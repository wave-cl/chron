//! Open-time recovery: validate segment headers, drop segments nothing was
//! committed into, truncate the unacknowledged or torn tail, and check the
//! index against the data it vouches for.

use std::fs;
use std::path::{Path, PathBuf};

use crate::db::Layout;
use crate::error::{Error, Result};
use crate::event;
use crate::index::Index;
use crate::options::Options;
use crate::segment::{self, HEADER_LEN, ScanItem, Scanner, SegmentWriter};

pub(crate) struct Recovered {
    pub index: Index,
    pub head: u64,
    pub tail_base: u64,
    pub tail_path: PathBuf,
    /// Byte length of the tail after truncation; the writer continues here.
    pub tail_len: u64,
}

/// One good record found by a scan.
struct Found {
    offset: u64,
    next_offset: u64,
}

struct Scanned {
    records: Vec<Found>,
    /// First byte not covered by a good record.
    end_offset: u64,
    reason: String,
}

fn scan_segment(path: &Path, base: u64, max_record: usize) -> Result<Scanned> {
    let mut scanner = Scanner::open(path, HEADER_LEN, max_record)?;
    let mut records = Vec::new();
    let mut expected = base;
    loop {
        match scanner.next()? {
            ScanItem::Record { offset, body } => {
                let next_offset = offset + (segment::FRAME_LEN + body.len()) as u64;
                let ev = match event::decode_body(&body) {
                    Ok(ev) => ev,
                    Err(e) => {
                        return Ok(Scanned {
                            records,
                            end_offset: offset,
                            reason: e.to_string(),
                        });
                    }
                };
                if ev.position != expected {
                    return Ok(Scanned {
                        records,
                        end_offset: offset,
                        reason: format!(
                            "record carries position {} where {expected} was expected",
                            ev.position
                        ),
                    });
                }
                records.push(Found {
                    offset,
                    next_offset,
                });
                expected += 1;
            }
            ScanItem::Tail { offset, reason } => {
                return Ok(Scanned {
                    records,
                    end_offset: offset,
                    reason: reason.to_string(),
                });
            }
        }
    }
}

fn remove(path: &Path, dir: &Path) -> Result<()> {
    fs::remove_file(path).map_err(|e| Error::io(path, "remove", e))?;
    segment::sync_dir(dir)
}

pub(crate) fn recover(layout: &Layout, opts: &Options) -> Result<Recovered> {
    let dir = layout.segments_dir();
    let listed = segment::list(&dir)?;
    let n = listed.len();
    let mut segments = Vec::with_capacity(n);
    for (k, (base, path)) in listed.into_iter().enumerate() {
        match segment::read_header(&path)? {
            Ok(Some(h)) if h.base == base => segments.push((base, path)),
            Ok(Some(h)) => {
                return Err(Error::corrupt(
                    &path,
                    0,
                    format!("header says base {} but the file name says {base}", h.base),
                ));
            }
            // A roll that crashed before its header was complete.
            Ok(None) if k + 1 == n => remove(&path, &dir)?,
            Ok(None) => return Err(Error::corrupt(&path, 0, "segment shorter than a header")),
            Err(reason) => return Err(Error::corrupt(&path, 0, reason)),
        }
    }

    let index = Index::open(&layout.index_file(), opts.fsync)?;
    let head = index.head()?;

    // Segments nothing was ever committed into.
    while segments.last().is_some_and(|(base, _)| *base > head) {
        let (_, path) = segments.pop().unwrap();
        remove(&path, &dir)?;
    }
    let Some((tail_base, tail_path)) = segments.pop() else {
        if head != 0 {
            return Err(Error::corrupt(
                layout.index_file(),
                0,
                format!("head is {head} but no segment starts at or before it"),
            ));
        }
        let w = SegmentWriter::create(&dir, 0, true)?;
        drop(w);
        return Ok(Recovered {
            index,
            head: 0,
            tail_base: 0,
            tail_path: layout.segment(0),
            tail_len: HEADER_LEN,
        });
    };

    let scanned = scan_segment(&tail_path, tail_base, opts.max_record_bytes)?;
    let data_end = tail_base + scanned.records.len() as u64;
    if data_end < head {
        return Err(Error::corrupt(
            &tail_path,
            scanned.end_offset,
            format!(
                "index head is {head} but the data ends at {data_end} ({})",
                scanned.reason
            ),
        ));
    }
    let truncate_to = if data_end > head {
        scanned.records[(head - tail_base) as usize].offset
    } else {
        scanned.end_offset
    };
    let file_len = fs::metadata(&tail_path)
        .map_err(|e| Error::io(&tail_path, "metadata", e))?
        .len();
    if truncate_to < file_len {
        segment::truncate(&tail_path, truncate_to)?;
    }

    if head > tail_base {
        let last = &scanned.records[(head - 1 - tail_base) as usize];
        let want = (
            tail_base,
            last.offset,
            (last.next_offset - last.offset) as u32,
        );
        let got = index
            .locate(head - 1)?
            .map(|l| (l.base, l.offset, l.frame_len));
        if got != Some(want) {
            return Err(Error::corrupt(
                &tail_path,
                last.offset,
                format!(
                    "index places position {} at {got:?}, the data has it at {want:?}",
                    head - 1
                ),
            ));
        }
    }

    Ok(Recovered {
        index,
        head,
        tail_base,
        tail_path,
        tail_len: truncate_to,
    })
}
