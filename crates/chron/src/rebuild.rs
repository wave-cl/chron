//! Rebuilding the index from the segment files alone.

use std::fs;
use std::path::{Path, PathBuf};

use bytes::Bytes;
use redb::Durability;
use uuid::Uuid;

use crate::db::{Layout, Lock};
use crate::error::{Error, Result};
use crate::index::{DB_ID_KEY, FLOOR_KEY, HEAD_KEY, Index, META, META_BYTES};
use crate::options::{Fsync, Options};
use crate::recover::{FLUSH_EVERY, Item, Records, Replayer, later_record_exists};
use crate::segment;
use crate::stream;

/// What [`Db::rebuild_index`](crate::Db::rebuild_index) found.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct Rebuilt {
    /// The new head: one past the last event of the last complete
    /// transaction.
    pub head: u64,
    /// Base of the first segment: the retention floor the data carries.
    pub floor: u64,
    /// Segment files read.
    pub segments: usize,
    /// Decodable events of an incomplete trailing transaction, left out of
    /// the index. The next open truncates their bytes.
    pub dropped: u64,
    /// Bytes after the last complete transaction in the last segment,
    /// which the next open truncates.
    pub tail_bytes_dropped: u64,
    /// Why the last segment's scan stopped before its end, if it did.
    pub tail_reason: Option<String>,
    /// Whether the identity came from the existing index or the backup
    /// (`false`: the next open mints a fresh one, and replicas of the old
    /// identity will refuse it).
    pub identity_kept: bool,
}

fn validated_segments(dir: &Path) -> Result<Vec<(u64, PathBuf)>> {
    let listed = segment::list(dir)?;
    let n = listed.len();
    let mut out = Vec::with_capacity(n);
    for (k, (base, path)) in listed.into_iter().enumerate() {
        match segment::read_header(&path)? {
            Ok(Some(h)) if h.base == base => out.push((base, path)),
            Ok(Some(h)) => {
                return Err(Error::corrupt(
                    &path,
                    0,
                    format!("header says base {} but the file name says {base}", h.base),
                ));
            }
            // A roll that crashed before its header was complete; the next
            // open removes it.
            Ok(None) if k + 1 == n => {}
            Ok(None) => return Err(Error::corrupt(&path, 0, "segment shorter than a header")),
            Err(reason) => return Err(Error::corrupt(&path, 0, reason)),
        }
    }
    Ok(out)
}

/// Where the rebuilt index's identity comes from.
enum IdSource {
    /// The existing index's identity, if it has one.
    Existing,
    /// This one; `None` lets the next open mint a fresh identity.
    Given(Option<Uuid>),
}

/// Rebuilds, keeping the identity and the aggregate lengths the existing
/// index holds (if any): lengths of aggregates whose events all lie
/// below the floor exist nowhere else.
pub(crate) fn rebuild(path: &Path, opts: &Options) -> Result<Rebuilt> {
    rebuild_from(
        path,
        opts,
        IdSource::Existing,
        Vec::new(),
        Vec::new(),
        Vec::new(),
    )
}

/// Rebuilds from a backup: `id`, `lens`, `cursors` and `streams` come
/// from the archive; without an id the next open mints a fresh identity.
pub(crate) fn rebuild_seeded(
    path: &Path,
    opts: &Options,
    id: Option<Uuid>,
    lens: Vec<(Uuid, u64)>,
    cursors: Vec<(String, u64, u64)>,
    streams: Vec<(String, Vec<Bytes>)>,
) -> Result<Rebuilt> {
    rebuild_from(path, opts, IdSource::Given(id), lens, cursors, streams)
}

fn rebuild_from(
    path: &Path,
    _opts: &Options,
    id: IdSource,
    seed: Vec<(Uuid, u64)>,
    given_cursors: Vec<(String, u64, u64)>,
    given_streams: Vec<(String, Vec<Bytes>)>,
) -> Result<Rebuilt> {
    let layout = Layout::new(path);
    let dir = layout.segments_dir();
    fs::create_dir_all(&dir).map_err(|e| Error::io(&dir, "mkdir", e))?;
    let _lock = Lock::acquire(&layout.lock_file())?;

    let tmp = layout.root().join("index.redb.rebuild");
    if tmp.exists() {
        fs::remove_file(&tmp).map_err(|e| Error::io(&tmp, "remove", e))?;
    }
    let segments = validated_segments(&dir)?;
    // Only after the segments passed: opening the old index may rewrite
    // its file, and a refused rebuild must leave it untouched.
    // The old index is the only place cursors and stream definitions
    // live: they are copied with the identity and the aggregate lengths
    // (a stream half filled is rebuilt whole). A seeded rebuild (a
    // restore) takes the archive's.
    let (id, seed, cursors, stream_defs) = match id {
        IdSource::Given(id) => (id, seed, given_cursors, given_streams),
        IdSource::Existing => {
            let index_file = layout.index_file();
            let old = if index_file.exists() {
                Index::open(&index_file, Fsync::Never).ok()
            } else {
                None
            };
            match old {
                Some(old) => (
                    old.db_id().unwrap_or(None),
                    old.aggregate_lens().unwrap_or_default(),
                    old.cursors().unwrap_or_default(),
                    old.stream_rows()
                        .unwrap_or_default()
                        .into_iter()
                        .map(|(name, row)| (name, stream::decode_tagset(&row.tagset)))
                        .collect(),
                ),
                None => (None, Vec::new(), Vec::new(), Vec::new()),
            }
        }
    };
    let identity_kept = id.is_some();

    let index = Index::create(&tmp, Fsync::Never)?;
    if !cursors.is_empty() {
        index.set_cursors(&cursors)?;
    }
    let streams = stream::seed(&index, &stream_defs)?;
    let floor = segments.first().map_or(0, |(b, _)| *b);
    // An aggregate first seen above a floor may start at any index: its
    // earlier events were dropped.
    let mut replayer = Replayer::new(floor, floor > 0, streams);
    let mut unknown = |_: &Uuid| Ok(None);
    let mut next_base = floor;
    let mut tail_bytes_dropped = 0u64;
    let mut tail_reason = None;
    let last = segments.len().saturating_sub(1);

    for (k, (base, seg_path)) in segments.iter().enumerate() {
        if *base != next_base {
            return Err(Error::corrupt(
                seg_path,
                0,
                format!("segment starts at position {base}, {next_base} expected"),
            ));
        }
        // The fixed frame bound, not the configured record limit: a limit
        // lowered since the data was written must not read as damage.
        let mut records = Records::open(seg_path, *base, u32::MAX as usize)?;
        replayer.begin_segment();
        loop {
            match records.next()? {
                Item::Record(r) => {
                    replayer.record(r, *base, seg_path, &mut unknown)?;
                    if replayer.batch.len() >= FLUSH_EVERY {
                        replayer.flush(&index, Durability::None)?;
                    }
                }
                Item::Tail {
                    offset,
                    clean,
                    reason,
                } => {
                    if k != last {
                        if !clean {
                            return Err(Error::corrupt(seg_path, offset, reason));
                        }
                        if replayer.open_txn() {
                            return Err(Error::corrupt(
                                seg_path,
                                offset,
                                "transaction still open at the end of a segment that is not the last",
                            ));
                        }
                    } else if !clean || replayer.open_txn() {
                        // A torn tail is cut; damage with committed records
                        // after it is not.
                        if later_record_exists(seg_path, offset, records.reached())? {
                            return Err(Error::corrupt(
                                seg_path,
                                offset,
                                format!("{reason}; committed records follow the damage"),
                            ));
                        }
                        let file_len = fs::metadata(seg_path)
                            .map_err(|e| Error::io(seg_path, "metadata", e))?
                            .len();
                        tail_bytes_dropped = file_len.saturating_sub(replayer.last_complete_end);
                        tail_reason = Some(if clean {
                            "transaction open at the end of the file".to_string()
                        } else {
                            reason
                        });
                    }
                    next_base = records.reached();
                    break;
                }
            }
        }
    }
    let head = replayer.head;
    let dropped = replayer.decoded - (head - floor);

    // Seeded lengths stand for events below the floor. An aggregate with
    // survivors counted its true length (indices are absolute); one whose
    // events were all dropped has only the seed.
    for (id, len) in seed {
        let entry = replayer.lens.entry(id).or_insert(0);
        if len > *entry {
            *entry = len;
        }
    }
    let all: Vec<Uuid> = replayer.lens.keys().copied().collect();
    replayer.touched.extend(all);
    let txn = index.begin_write_durable()?;
    replayer.flush_into(&txn)?;
    {
        let mut meta = txn.open_table(META)?;
        meta.insert(HEAD_KEY, head)?;
        meta.insert(FLOOR_KEY, floor)?;
        let mut bytes = txn.open_table(META_BYTES)?;
        match id {
            Some(id) => {
                bytes.insert(DB_ID_KEY, id.as_bytes().as_slice())?;
            }
            None => {
                bytes.remove(DB_ID_KEY)?;
            }
        }
    }
    txn.commit()?;
    drop(index);

    let target = layout.index_file();
    fs::rename(&tmp, &target).map_err(|e| Error::io(&tmp, "rename", e))?;
    segment::sync_dir(layout.root())?;
    Ok(Rebuilt {
        head,
        floor,
        segments: segments.len(),
        dropped,
        tail_bytes_dropped,
        tail_reason,
        identity_kept,
    })
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;
    use crate::event::{self, Event, FLAG_FIRST_IN_TXN, FLAG_LAST_IN_TXN};
    use crate::segment::SegmentWriter;
    use bytes::Bytes;

    /// Writes one segment of hand-made records: `(position, aggregate,
    /// index, flags)`.
    fn write_segment(dir: &Path, base: u64, records: &[(u64, Option<Uuid>, u64, u8)]) {
        let mut w = SegmentWriter::create(dir, base, false).unwrap();
        for (position, aggregate, index, flags) in records {
            let ev = Event {
                position: *position,
                aggregate: *aggregate,
                index: *index,
                recorded_at: 1,
                flags: *flags
                    | if aggregate.is_none() {
                        event::FLAG_FREE
                    } else {
                        0
                    },
                commit_id: None,
                tags: vec![],
                metadata: Bytes::new(),
                payload: Bytes::from_static(b"p"),
            };
            let mut body = Vec::new();
            event::encode_body(&ev, &mut body);
            let mut buf = Vec::new();
            segment::frame_record(&body, &mut buf);
            w.write_at_end(&buf).unwrap();
            w.advance(buf.len() as u64);
        }
    }

    const BOTH: u8 = FLAG_FIRST_IN_TXN | FLAG_LAST_IN_TXN;

    fn setup() -> (tempfile::TempDir, PathBuf) {
        let d = tempfile::tempdir().unwrap();
        let dir = d.path().join("segments");
        fs::create_dir_all(&dir).unwrap();
        (d, dir)
    }

    #[test]
    fn hand_made_segments_rebuild() {
        let (d, dir) = setup();
        let a = Some(Uuid::from_u128(1));
        write_segment(
            &dir,
            0,
            &[
                (0, a, 0, BOTH),
                (1, a, 1, FLAG_FIRST_IN_TXN),
                (2, None, 0, FLAG_LAST_IN_TXN),
            ],
        );
        write_segment(&dir, 3, &[(3, a, 2, BOTH)]);
        let r = rebuild(d.path(), &Options::new()).unwrap();
        assert_eq!((r.head, r.floor, r.segments, r.dropped), (4, 0, 2, 0));
        let index = Index::open(&d.path().join("index.redb"), Fsync::Never).unwrap();
        assert_eq!(index.head().unwrap(), 4);
        assert_eq!(index.aggregate_len(&Uuid::from_u128(1)).unwrap(), 3);
        assert_eq!(index.locate(3).unwrap().unwrap().base, 3);
    }

    #[test]
    fn non_dense_aggregate_index_is_corrupt() {
        let (d, dir) = setup();
        let a = Some(Uuid::from_u128(1));
        write_segment(&dir, 0, &[(0, a, 0, BOTH), (1, a, 2, BOTH)]);
        assert!(
            matches!(rebuild(d.path(), &Options::new()), Err(Error::Corrupt { offset, .. }) if offset > 64)
        );
        assert!(!d.path().join("index.redb").exists());
    }

    #[test]
    fn open_transaction_in_a_middle_segment_is_corrupt() {
        let (d, dir) = setup();
        let a = Some(Uuid::from_u128(1));
        write_segment(&dir, 0, &[(0, a, 0, FLAG_FIRST_IN_TXN)]);
        write_segment(&dir, 1, &[(1, a, 1, FLAG_LAST_IN_TXN)]);
        assert!(matches!(
            rebuild(d.path(), &Options::new()),
            Err(Error::Corrupt { .. })
        ));
    }

    #[test]
    fn a_gap_between_segments_is_corrupt() {
        let (d, dir) = setup();
        let a = Some(Uuid::from_u128(1));
        write_segment(&dir, 0, &[(0, a, 0, BOTH)]);
        write_segment(&dir, 5, &[(5, a, 1, BOTH)]);
        assert!(matches!(
            rebuild(d.path(), &Options::new()),
            Err(Error::Corrupt { offset: 0, .. })
        ));
    }

    #[test]
    fn a_first_segment_above_zero_is_a_floor() {
        let (d, dir) = setup();
        let a = Some(Uuid::from_u128(1));
        write_segment(&dir, 7, &[(7, a, 5, BOTH), (8, a, 6, BOTH)]);
        let r = rebuild(d.path(), &Options::new()).unwrap();
        assert_eq!((r.head, r.floor, r.segments, r.dropped), (9, 7, 1, 0));
        let index = Index::open(&d.path().join("index.redb"), Fsync::Never).unwrap();
        assert_eq!(index.floor().unwrap(), 7);
        assert_eq!(index.aggregate_len(&Uuid::from_u128(1)).unwrap(), 7);
    }

    #[test]
    fn first_inside_an_open_transaction_is_corrupt() {
        let (d, dir) = setup();
        let a = Some(Uuid::from_u128(1));
        write_segment(&dir, 0, &[(0, a, 0, FLAG_FIRST_IN_TXN), (1, a, 1, BOTH)]);
        assert!(matches!(
            rebuild(d.path(), &Options::new()),
            Err(Error::Corrupt { .. })
        ));
    }
}
