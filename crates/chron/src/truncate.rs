//! Truncation: dropping every record from a position to the head, so a
//! copy of a database that took writes of its own can cut back to the
//! history it shares with another copy and follow it again.

use std::fs;
use std::sync::atomic::Ordering;

use uuid::Uuid;

use crate::db::Inner;
use crate::error::{Error, Result};
use crate::event::FLAG_FIRST_IN_TXN;
use crate::index::{AGG_INDEX, AGG_LEN, HEAD_KEY, META, POS, TAG};
use crate::recover::{Item, Records};
use crate::segment::{self, SegmentWriter};

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
/// boundary. The index is updated and committed first, then the segment
/// holding `to` is cut back to it and later segments are removed; a
/// crash in between is repaired by the next open, which drops what the
/// index no longer vouches for.
pub(crate) fn truncate(inner: &Inner, to: u64) -> Result<Truncated> {
    let _m = inner.maintenance.lock().unwrap_or_else(|p| p.into_inner());
    let mut w = inner.lock_writer();
    let head = inner.head.load(Ordering::Acquire);
    let floor = inner.floor.load(Ordering::Acquire);
    if to > head {
        return Err(Error::PositionOutOfRange { position: to, head });
    }
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
        txn.open_table(META)?.insert(HEAD_KEY, to)?;
    }
    txn.commit()?;

    // Then the files: the segment holding `to` is cut back to its
    // frame, later ones go. The writer continues at the cut.
    let kept = inner.layout.segment(loc.base);
    segment::truncate(&kept, loc.offset)?;
    let later: Vec<_> = listed.iter().filter(|(b, _)| *b > loc.base).collect();
    for (_, path) in &later {
        fs::remove_file(path).map_err(|e| Error::io(path, "remove", e))?;
    }
    segment::sync_dir(&dir)?;
    let bases: Vec<u64> = later.iter().map(|(b, _)| *b).collect();
    inner.forget_files(&bases);
    *w = SegmentWriter::open(&kept, loc.base, loc.offset)?;
    inner.publish_head(to);
    Ok(Truncated {
        head: to,
        removed: head - to,
        segments_removed: later.len(),
    })
}
