//! Replication: whole transactions as the segments hold them, pulled by a
//! copy of this database and landed through the normal commit path.

use std::path::Path;

use crate::backup;
use crate::db::Inner;
use crate::error::{Error, Result};
use crate::event::{FLAG_FIRST_IN_TXN, FLAG_LAST_IN_TXN};
use crate::segment::{FRAME_LEN, Frames};
use crate::txn;

/// Positions `from..to` as back-to-back frames; `to` ends a transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Chunk {
    pub from: u64,
    pub to: u64,
    pub frames: Vec<u8>,
}

pub(crate) fn chunk(inner: &Inner, from: u64, max_events: usize) -> Result<Option<Chunk>> {
    use std::sync::atomic::Ordering;
    let _m = inner.maintenance.lock().unwrap_or_else(|p| p.into_inner());
    let head = inner.head.load(Ordering::Acquire);
    let floor = inner.floor.load(Ordering::Acquire);
    if from < floor {
        return Err(Error::BelowFloor {
            position: from,
            floor,
        });
    }
    if from > head {
        return Err(Error::PositionOutOfRange {
            position: from,
            head,
        });
    }
    if from == head {
        return Ok(None);
    }
    let step = (max_events.max(1)) as u64;
    let mut to = from.saturating_add(step).min(head);
    let mut frames = backup::frames_between(inner, from, to)?;
    let path = Path::new("replication chunk");
    let max_record = inner.opts.max_record_bytes;
    // Only the flags byte of each record is looked at: where the last
    // complete transaction ends, in bytes and in positions.
    let mut cut: Option<(usize, u64)> = None;
    let mut scanned = 0usize;
    let mut position = from;
    loop {
        let mut walker = Frames::new(&frames[scanned..]);
        while let Some((offset, frame)) = walker.next(path, max_record)? {
            let Some(&flags) = frame.get(FRAME_LEN + 1) else {
                return Err(Error::corrupt(path, offset, "record too short for a body"));
            };
            if position == from && flags & FLAG_FIRST_IN_TXN == 0 {
                return Err(Error::corrupt(
                    inner.layout.index_file(),
                    from,
                    format!("position {from} is not the start of a transaction"),
                ));
            }
            position += 1;
            if flags & FLAG_LAST_IN_TXN != 0 {
                cut = Some((scanned + offset as usize + frame.len(), position));
            }
        }
        scanned = frames.len();
        if cut.is_some() || to >= head {
            break;
        }
        // One transaction longer than the window: extend until it ends.
        // The head is always a boundary, so this terminates.
        let next = to.saturating_add(step).min(head);
        frames.extend(backup::frames_between(inner, to, next)?);
        to = next;
    }
    let Some((bytes, to)) = cut else {
        return Err(Error::corrupt(
            inner.layout.index_file(),
            from,
            "no transaction ends before the head",
        ));
    };
    frames.truncate(bytes);
    Ok(Some(Chunk { from, to, frames }))
}

pub(crate) fn apply(inner: &Inner, from: u64, to: u64, frames: &[u8]) -> Result<u64> {
    let _m = inner.maintenance.lock().unwrap_or_else(|p| p.into_inner());
    txn::import_frames(inner, Path::new("replication stream"), frames, from, to).map_err(
        |e| match e {
            Error::WrongBaseHead {
                base_head, head, ..
            } => Error::ReplicationGap {
                from: base_head,
                head,
            },
            e => e,
        },
    )
}
