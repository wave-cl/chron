//! Open-time recovery: validate segment headers, drop segments nothing was
//! committed into, truncate the unacknowledged or torn tail, and check the
//! index against the data it vouches for.

use std::fs;
use std::path::{Path, PathBuf};

use crate::db::Layout;
use crate::error::{Error, Result};
use crate::event::{self, Event};
use crate::index::{Index, POS};
use crate::options::Options;
use crate::segment::{self, HEADER_LEN, ScanItem, Scanner, SegmentWriter};

pub(crate) struct Recovered {
    pub index: Index,
    pub head: u64,
    pub floor: u64,
    pub tail_base: u64,
    pub tail_path: PathBuf,
    /// Byte length of the tail after truncation; the writer continues here.
    pub tail_len: u64,
}

/// One good record found by a scan.
pub(crate) struct Record {
    pub position: u64,
    pub offset: u64,
    pub frame_len: u32,
    pub event: Event,
}

pub(crate) enum Item {
    Record(Record),
    /// The scan stopped at `offset`; `clean` when the file ends exactly on
    /// a record boundary.
    Tail {
        offset: u64,
        clean: bool,
        reason: String,
    },
}

/// Walks one segment's records in order: whole, crc-valid, decodable, at
/// consecutive positions from the header's base. Stops at the first fault
/// and reports it as a [`Item::Tail`].
pub(crate) struct Records {
    scanner: Scanner,
    expected: u64,
}

impl Records {
    pub(crate) fn open(path: &Path, base: u64, max_record: usize) -> Result<Self> {
        Ok(Records {
            scanner: Scanner::open(path, HEADER_LEN, max_record)?,
            expected: base,
        })
    }

    /// The position after the last good record so far.
    pub(crate) fn reached(&self) -> u64 {
        self.expected
    }

    pub(crate) fn next(&mut self) -> Result<Item> {
        match self.scanner.next()? {
            ScanItem::Record { offset, body } => {
                let frame_len = (segment::FRAME_LEN + body.len()) as u32;
                let event = match event::decode_body(&body) {
                    Ok(ev) => ev,
                    Err(e) => {
                        return Ok(Item::Tail {
                            offset,
                            clean: false,
                            reason: e.to_string(),
                        });
                    }
                };
                if event.position != self.expected {
                    return Ok(Item::Tail {
                        offset,
                        clean: false,
                        reason: format!(
                            "record carries position {} where {} was expected",
                            event.position, self.expected
                        ),
                    });
                }
                self.expected += 1;
                Ok(Item::Record(Record {
                    position: event.position,
                    offset,
                    frame_len,
                    event,
                }))
            }
            ScanItem::Tail { offset, reason } => Ok(Item::Tail {
                offset,
                clean: reason == segment::TailReason::Clean,
                reason: reason.to_string(),
            }),
        }
    }
}

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
    let mut records = Records::open(path, base, max_record)?;
    let mut found = Vec::new();
    loop {
        match records.next()? {
            Item::Record(r) => found.push(Found {
                offset: r.offset,
                next_offset: r.offset + u64::from(r.frame_len),
            }),
            Item::Tail { offset, reason, .. } => {
                return Ok(Scanned {
                    records: found,
                    end_offset: offset,
                    reason,
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
    let index = Index::open(&layout.index_file(), opts.fsync)?;
    let head = index.head()?;
    let floor = index.floor()?;

    // Segments a retention dropped from the index before a crash stopped
    // it removing the files.
    let mut listed = Vec::new();
    for (base, path) in segment::list(&dir)? {
        if base < floor {
            remove(&path, &dir)?;
        } else {
            listed.push((base, path));
        }
    }
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
            // A roll that crashed before its header was complete, or
            // whose header never reached the disk whole.
            Ok(None) if k + 1 == n => remove(&path, &dir)?,
            Err(_) if k + 1 == n && base >= head => remove(&path, &dir)?,
            Ok(None) => {
                return Err(Error::corrupt(&path, 0, "segment shorter than a header"));
            }
            Err(reason) => return Err(Error::corrupt(&path, 0, reason)),
        }
    }

    // Segments nothing was ever committed into.
    while segments.last().is_some_and(|(base, _)| *base > head) {
        let (_, path) = segments.pop().unwrap();
        remove(&path, &dir)?;
    }
    let Some((tail_base, tail_path)) = segments.last().cloned() else {
        if head != floor {
            return Err(Error::corrupt(
                layout.index_file(),
                0,
                format!("head is {head} but no segment starts at or before it"),
            ));
        }
        let w = SegmentWriter::create(&dir, floor, true)?;
        drop(w);
        return Ok(Recovered {
            index,
            head,
            floor,
            tail_base: floor,
            tail_path: layout.segment(floor),
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

    if opts.verify_all_segments {
        verify_all(&segments, head, &index, opts.max_record_bytes)?;
    }

    Ok(Recovered {
        index,
        head,
        floor,
        tail_base,
        tail_path,
        tail_len: truncate_to,
    })
}

/// Reads every record of every segment, checking crc, consecutive
/// positions reaching exactly the next segment's base (or the head), and
/// that the index places each record where it is.
fn verify_all(
    segments: &[(u64, PathBuf)],
    head: u64,
    index: &Index,
    max_record: usize,
) -> Result<()> {
    let txn = index.begin_read()?;
    let pos = txn.open_table(POS)?;
    for (k, (base, path)) in segments.iter().enumerate() {
        let end = segments.get(k + 1).map_or(head, |(next, _)| *next);
        let mut records = Records::open(path, *base, max_record)?;
        loop {
            if records.reached() == end && *base < end {
                // Everything the index knows is checked; bytes after the
                // last committed record are a failed commit's leftovers.
                break;
            }
            match records.next()? {
                Item::Record(r) => {
                    if r.position >= end {
                        return Err(Error::corrupt(
                            path,
                            r.offset,
                            format!(
                                "record at position {} past the segment's end {end}",
                                r.position
                            ),
                        ));
                    }
                    let want = (*base, r.offset, r.frame_len);
                    let got = pos.get(r.position)?.map(|v| v.value());
                    if got != Some(want) {
                        return Err(Error::corrupt(
                            path,
                            r.offset,
                            format!(
                                "index places position {} at {got:?}, the data has it at {want:?}",
                                r.position
                            ),
                        ));
                    }
                }
                Item::Tail {
                    offset,
                    clean,
                    reason,
                } => {
                    if !clean {
                        return Err(Error::corrupt(path, offset, reason));
                    }
                    let reached = records.reached();
                    if reached != end {
                        return Err(Error::corrupt(
                            path,
                            offset,
                            format!("segment ends at position {reached}, {end} expected"),
                        ));
                    }
                    break;
                }
            }
        }
    }
    Ok(())
}
