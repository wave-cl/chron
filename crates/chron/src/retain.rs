//! Retention: dropping whole segments below a floor while every surviving
//! position keeps its number.

use std::fs;
use std::sync::atomic::Ordering;

use crate::db::Inner;
use crate::error::{Error, Result};
use crate::index::{AGG_INDEX, FLOOR_KEY, META, POS, TAG};
use crate::recover::{Item, Records};
use crate::segment;

/// What [`Db::retain_from`](crate::Db::retain_from) did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Retained {
    /// The floor now in force: the base of the first kept segment.
    pub floor: u64,
    pub segments_removed: usize,
    pub bytes_freed: u64,
}

/// Drops every segment that lies wholly below `floor`. The index is
/// updated and committed first; a crash before the files are removed is
/// repaired by the next open. With `remove_files` false the files are
/// left in place: the state that crash leaves.
pub(crate) fn retain(inner: &Inner, floor: u64, remove_files: bool) -> Result<Retained> {
    let _m = inner.maintenance.lock().unwrap_or_else(|p| p.into_inner());
    let head = inner.head.load(Ordering::Acquire);
    let old = inner.floor.load(Ordering::Acquire);
    if floor > head {
        return Err(Error::PositionOutOfRange {
            position: floor,
            head,
        });
    }
    let unchanged = Retained {
        floor: old,
        segments_removed: 0,
        bytes_freed: 0,
    };
    if floor <= old {
        return Ok(unchanged);
    }
    let dir = inner.layout.segments_dir();
    let listed = segment::list(&dir)?;
    // Rounded down to a segment boundary; the tail segment's base is at
    // most `head`, so it is never dropped.
    let Some(new_floor) = listed.iter().map(|(b, _)| *b).filter(|b| *b <= floor).max() else {
        return Ok(unchanged);
    };
    if new_floor <= old {
        return Ok(unchanged);
    }
    let dropped: Vec<_> = listed
        .into_iter()
        .filter(|(base, _)| *base < new_floor)
        .collect();

    // The index first, in one durable transaction.
    let txn = inner.index.begin_write_durable()?;
    {
        let mut pos = txn.open_table(POS)?;
        let mut agg_index = txn.open_table(AGG_INDEX)?;
        let mut tag = txn.open_table(TAG)?;
        for (base, path) in &dropped {
            let mut records = Records::open(path, *base, inner.opts.max_record_bytes)?;
            loop {
                match records.next()? {
                    Item::Record(r) => {
                        pos.remove(r.position)?;
                        if let Some(id) = &r.event.aggregate {
                            agg_index.remove((id.as_bytes(), r.event.index))?;
                        }
                        for t in &r.event.tags {
                            tag.remove((t.as_ref(), r.position))?;
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
                        break;
                    }
                }
            }
        }
        txn.open_table(META)?.insert(FLOOR_KEY, new_floor)?;
    }
    txn.commit()?;
    inner.floor.store(new_floor, Ordering::Release);
    if !remove_files {
        return Ok(Retained {
            floor: new_floor,
            segments_removed: 0,
            bytes_freed: 0,
        });
    }

    let bases: Vec<u64> = dropped.iter().map(|(b, _)| *b).collect();
    inner.forget_files(&bases);
    let mut bytes_freed = 0;
    for (_, path) in &dropped {
        bytes_freed += fs::metadata(path)
            .map_err(|e| Error::io(path, "metadata", e))?
            .len();
        fs::remove_file(path).map_err(|e| Error::io(path, "remove", e))?;
    }
    segment::sync_dir(&dir)?;
    Ok(Retained {
        floor: new_floor,
        segments_removed: dropped.len(),
        bytes_freed,
    })
}
