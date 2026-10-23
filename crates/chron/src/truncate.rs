//! Truncation: dropping every record from a position to the head, so a
//! copy of a database that took writes of its own can cut back to the
//! history it shares with another copy and follow it again.

use std::fs;
use std::sync::atomic::Ordering;

use uuid::Uuid;

use crate::db::{Inner, Layout};
use crate::error::{Error, Result};
use crate::event::FLAG_FIRST_IN_TXN;
use redb::ReadableTable;

use crate::index::STREAM_POS;
use crate::index::{
    AGG_INDEX, AGG_LEN, HEAD_KEY, Index, META, POS, TAG, TRUNCATE_INTENT_KEY, TXN_ID,
};
use crate::recover::{Item, Records};
use crate::segment::{self, HEADER_LEN, SegmentWriter};

/// What [`Db::truncate_to`](crate::Db::truncate_to) did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Truncated {
    /// The head now in force.
    pub head: u64,
    /// Records dropped.
    pub removed: u64,
    /// Segment files deleted.
    pub segments_removed: usize,
}

/// Drops the records at positions `to..head`. `to` must be a transaction
/// boundary. The index is updated and committed first, with a marker
/// saying the files are not cut yet; then the segment holding `to` is
/// cut back to it and later segments are removed, and the marker is
/// cleared. A crash in between is finished by the next open from the
/// marker, before anything past the durable head is replayed.
pub(crate) fn truncate(inner: &Inner, to: u64) -> Result<Truncated> {
    truncate_with(inner, to, false)
}

/// `truncate`, stopping after the index commit when `marker_only`: the
/// state a crash between the two steps leaves (for tests).
pub(crate) fn truncate_with(inner: &Inner, to: u64, marker_only: bool) -> Result<Truncated> {
    let _m = inner.maintenance.lock().unwrap_or_else(|p| p.into_inner());
    let mut w = inner.lock_writer();
    let head = inner.head.load(Ordering::Acquire);
    let floor = inner.floor.load(Ordering::Acquire);
    if to > head {
        return Err(Error::PositionOutOfRange { position: to, head });
    }
    // The ring holds bytes about to be cut.
    inner
        .recent
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .clear();
    if to < floor {
        return Err(Error::BelowFloor {
            position: to,
            floor,
        });
    }
    if to == head {
        return Ok(Truncated {
            head,
            removed: 0,
            segments_removed: 0,
        });
    }
    let Some(loc) = inner.index.locate(to)? else {
        return Err(Error::Inconsistent {
            position: to,
            reason: "the index has no record there".into(),
        });
    };

    // Every record from `to` on, read from the segments: the keys to
    // drop and each touched aggregate's new length (its smallest
    // dropped index).
    let dir = inner.layout.segments_dir();
    let listed = segment::list(&dir)?;
    let mut positions: Vec<u64> = Vec::new();
    let mut agg_keys: Vec<(Uuid, u64)> = Vec::new();
    let mut agg_len: std::collections::HashMap<Uuid, u64> = std::collections::HashMap::new();
    let mut tag_keys: Vec<(Vec<u8>, u64)> = Vec::new();
    let mut ids: Vec<[u8; 16]> = Vec::new();
    let mut boundary_checked = false;
    for (base, path) in listed.iter().filter(|(b, _)| *b >= loc.base) {
        let mut records = Records::open(path, *base, inner.opts.max_record_bytes)?;
        loop {
            if records.reached() >= head {
                break;
            }
            match records.next()? {
                Item::Record(r) => {
                    if r.position < to {
                        continue;
                    }
                    if !boundary_checked {
                        if r.event.flags & FLAG_FIRST_IN_TXN == 0 {
                            return Err(Error::Inconsistent {
                                position: to,
                                reason: "not the start of a transaction".into(),
                            });
                        }
                        boundary_checked = true;
                    }
                    positions.push(r.position);
                    if let Some(id) = r.event.commit_id {
                        ids.push(id);
                    }
                    if let Some(id) = r.event.aggregate {
                        agg_keys.push((id, r.event.index));
                        agg_len
                            .entry(id)
                            .and_modify(|l| *l = (*l).min(r.event.index))
                            .or_insert(r.event.index);
                    }
                    for t in &r.event.tags {
                        tag_keys.push((t.to_vec(), r.position));
                    }
                }
                Item::Tail { .. } => break,
            }
        }
    }
    if positions.len() as u64 != head - to {
        return Err(Error::Inconsistent {
            position: to,
            reason: format!(
                "the segments hold {} records from {to} but the head is {head}",
                positions.len()
            ),
        });
    }

    // The index first, in one durable transaction, with the segment
    // bytes synced before it as every index write is.
    w.sync_data()?;
    let txn = inner.index.begin_write_durable()?;
    {
        let mut pos = txn.open_table(POS)?;
        let mut agg_index = txn.open_table(AGG_INDEX)?;
        let mut lens = txn.open_table(AGG_LEN)?;
        let mut tag = txn.open_table(TAG)?;
        for p in &positions {
            pos.remove(*p)?;
        }
        for (id, index) in &agg_keys {
            agg_index.remove((id.as_bytes(), *index))?;
        }
        for (id, len) in &agg_len {
            if *len == 0 {
                lens.remove(id.as_bytes())?;
            } else {
                lens.insert(id.as_bytes(), *len)?;
            }
        }
        for (t, p) in &tag_keys {
            tag.remove((t.as_slice(), *p))?;
        }
        // Stream entries only ever name positions below the head, so
        // everything from the cut on goes, per stream, by range.
        let streams = inner.streams();
        if !streams.is_empty() {
            let mut stream_pos = txn.open_table(STREAM_POS)?;
            for s in streams.iter() {
                stream_pos.retain_in((s.id, to)..=(s.id, u64::MAX), |_, _| false)?;
            }
        }
        // The cut transactions' ids go with them: a resend lands again.
        let mut txn_ids = txn.open_table(TXN_ID)?;
        for id in &ids {
            txn_ids.remove(id)?;
        }
        // A cursor past the cut points at records being rewritten: it
        // restarts at the cut.
        let mut cursors = txn.open_table(crate::index::CURSOR)?;
        let past: Vec<(String, u64)> = cursors
            .iter()?
            .filter_map(|item| item.ok())
            .filter(|(_, v)| v.value().0 > to)
            .map(|(k, v)| (k.value().to_string(), v.value().1))
            .collect();
        for (name, session) in &past {
            cursors.insert(name.as_str(), (to, *session))?;
        }
        let mut meta = txn.open_table(META)?;
        meta.insert(HEAD_KEY, to)?;
        // Until the files are cut, the records past `to` are still there
        // and must not be replayed by an open in between.
        meta.insert(TRUNCATE_INTENT_KEY, to)?;
    }
    txn.commit()?;
    inner.note_index_commit(true);

    // The index is the commit point: from here the head is `to` and the
    // writer continues at the cut, whatever the file operations below
    // manage; bytes they leave behind are overwritten by later writes
    // or dropped by the next open.
    let kept = inner.layout.segment(loc.base);
    *w = SegmentWriter::open(&kept, loc.base, loc.offset)?;
    inner.publish_head(to);
    let later: Vec<_> = listed.iter().filter(|(b, _)| *b > loc.base).collect();
    if marker_only {
        return Ok(Truncated {
            head: to,
            removed: head - to,
            segments_removed: 0,
        });
    }
    // Then the files: the segment holding `to` is cut back to its
    // frame, later ones go.
    let bases: Vec<u64> = later.iter().map(|(b, _)| *b).collect();
    inner.forget_files(&bases);
    segment::truncate(&kept, loc.offset)?;
    for (_, path) in &later {
        fs::remove_file(path).map_err(|e| Error::io(path, "remove", e))?;
    }
    segment::sync_dir(&dir)?;
    let txn = inner.index.begin_write_durable()?;
    txn.open_table(META)?.remove(TRUNCATE_INTENT_KEY)?;
    txn.commit()?;
    inner.note_index_commit(true);
    Ok(Truncated {
        head: to,
        removed: head - to,
        segments_removed: later.len(),
    })
}

/// Finishes a truncation to `to` whose index commit is durable but
/// whose file work was interrupted: cuts the files from the index's own
/// locations (the records past `to` may already be gone, so nothing is
/// scanned) and clears the marker.
pub(crate) fn finish(index: &Index, layout: &Layout, to: u64) -> Result<()> {
    let dir = layout.segments_dir();
    let floor = index.floor()?;
    // Where the kept data ends: after the record at `to - 1`, when the
    // index still has one above the floor.
    let cut = if to > floor {
        match index.locate(to - 1)? {
            Some(l) => Some((l.base, l.offset + u64::from(l.frame_len))),
            None => {
                return Err(Error::corrupt(
                    layout.index_file(),
                    0,
                    format!(
                        "a truncation to {to} is pending but position {} is not indexed",
                        to - 1
                    ),
                ));
            }
        }
    } else {
        None
    };
    for (base, path) in segment::list(&dir)? {
        match cut {
            Some((kept, end)) if base == kept => {
                let len = fs::metadata(&path)
                    .map_err(|e| Error::io(&path, "metadata", e))?
                    .len();
                if len > end {
                    segment::truncate(&path, end)?;
                }
            }
            Some((kept, _)) if base < kept => {}
            _ if base == to => {
                // A segment that begins exactly at the cut keeps its
                // header: the writer continues in it.
                let len = fs::metadata(&path)
                    .map_err(|e| Error::io(&path, "metadata", e))?
                    .len();
                if len > HEADER_LEN {
                    segment::truncate(&path, HEADER_LEN)?;
                }
            }
            _ if base > to => {
                fs::remove_file(&path).map_err(|e| Error::io(&path, "remove", e))?;
            }
            _ => {}
        }
    }
    segment::sync_dir(&dir)?;
    let txn = index.begin_write_durable()?;
    txn.open_table(META)?.remove(TRUNCATE_INTENT_KEY)?;
    txn.commit()?;
    Ok(())
}
