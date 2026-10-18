//! The database handle: open, read, and the shared state the writer uses.

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions, TryLockError};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use uuid::Uuid;

use bytes::Bytes;

use crate::backup::{self, BackupMeta};
use crate::error::{Error, Result};
use crate::event::{self, Event};
use crate::index::{self, Index, Location};
use crate::options::{Fsync, Options};
use crate::rebuild::Rebuilt;
use crate::recover;
use crate::replicate::{self, Chunk};
use crate::retain::{self, Retained};
use crate::segment::{self, SegmentWriter};
use crate::subscribe::Subscription;
use crate::txn::{self, Committed, Mode, Transaction};

pub(crate) struct Layout {
    root: PathBuf,
}

impl Layout {
    pub(crate) fn new(root: &Path) -> Self {
        Layout {
            root: root.to_path_buf(),
        }
    }

    pub(crate) fn root(&self) -> &Path {
        &self.root
    }

    pub(crate) fn index_file(&self) -> PathBuf {
        self.root.join("index.redb")
    }

    pub(crate) fn lock_file(&self) -> PathBuf {
        self.root.join("LOCK")
    }

    pub(crate) fn segments_dir(&self) -> PathBuf {
        self.root.join("segments")
    }

    pub(crate) fn segment(&self, base: u64) -> PathBuf {
        self.segments_dir().join(segment::file_name(base))
    }
}

/// Longest single positional read `read_all` makes when records sit back
/// to back.
const COALESCE_MAX_BYTES: u64 = 1 << 20;

/// Exclusive hold on the directory for as long as the `Db` (or a rebuild)
/// lives.
pub(crate) struct Lock {
    _file: File,
}

impl Lock {
    pub(crate) fn acquire(path: &Path) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)
            .map_err(|e| Error::io(path, "open", e))?;
        match file.try_lock() {
            Ok(()) => Ok(Lock { _file: file }),
            Err(TryLockError::WouldBlock) => Err(Error::Locked {
                path: path.to_path_buf(),
            }),
            Err(TryLockError::Error(e)) => Err(Error::io(path, "lock", e)),
        }
    }
}

pub(crate) struct Inner {
    pub(crate) layout: Layout,
    pub(crate) opts: Options,
    pub(crate) index: Index,
    pub(crate) writer: Mutex<SegmentWriter>,
    pub(crate) head: AtomicU64,
    /// Base of the first kept segment; positions below it were dropped.
    pub(crate) floor: AtomicU64,
    /// The database's identity; replicas and backups carry it.
    pub(crate) db_id: Mutex<Uuid>,
    /// The fencing epoch a leader writes under.
    pub(crate) epoch: AtomicU64,
    /// Serialises backup and retention against each other. Writers never
    /// take it.
    pub(crate) maintenance: Mutex<()>,
    pub(crate) fail_next_commit: std::sync::atomic::AtomicBool,
    head_watch: (Mutex<u64>, Condvar),
    #[cfg(feature = "tokio")]
    pub(crate) head_tx: tokio::sync::watch::Sender<u64>,
    files: Mutex<HashMap<u64, Arc<File>>>,
    _lock: Lock,
}

/// A database. Cheap to clone; every clone shares the same files, index
/// and writer. Dropping the last clone flushes under [`Fsync::Never`].
#[derive(Clone)]
pub struct Db {
    pub(crate) inner: Arc<Inner>,
}

impl Db {
    /// Opens the database at `path`, creating it if there is none, and
    /// recovers it: anything in the segment files past the committed head
    /// is truncated. Holds `LOCK` until dropped.
    pub fn open(path: impl AsRef<Path>, opts: Options) -> Result<Db> {
        let layout = Layout::new(path.as_ref());
        let segments_dir = layout.segments_dir();
        fs::create_dir_all(&segments_dir).map_err(|e| Error::io(&segments_dir, "mkdir", e))?;
        let lock = Lock::acquire(&layout.lock_file())?;
        let sync = opts.fsync == Fsync::Always;

        let index_file = layout.index_file();
        let (index, head, floor, writer) = if index_file.exists() {
            let r = recover::recover(&layout, &opts)?;
            let writer = SegmentWriter::open(&r.tail_path, r.tail_base, r.tail_len)?;
            (r.index, r.head, r.floor, writer)
        } else {
            if let Some((_, stray)) = segment::list(&segments_dir)?.first() {
                return Err(Error::corrupt(
                    stray,
                    0,
                    "segment files without an index; run Db::rebuild_index first",
                ));
            }
            let index = Index::create(&index_file, opts.fsync)?;
            let writer = SegmentWriter::create(&segments_dir, 0, sync)?;
            if sync {
                segment::sync_dir(layout.root())?;
            }
            (index, 0, 0, writer)
        };
        // Databases written before identities existed get one here; the
        // durable commit that stores it also persists every earlier index
        // commit, so the segment bytes are synced first.
        if index.db_id()?.is_none() {
            writer.sync_data()?;
        }
        let db_id = index.ensure_db_id()?;
        let epoch = index.epoch()?;

        Ok(Db {
            inner: Arc::new(Inner {
                layout,
                opts,
                index,
                writer: Mutex::new(writer),
                head: AtomicU64::new(head),
                floor: AtomicU64::new(floor),
                db_id: Mutex::new(db_id),
                epoch: AtomicU64::new(epoch),
                maintenance: Mutex::new(()),
                fail_next_commit: std::sync::atomic::AtomicBool::new(false),
                head_watch: (Mutex::new(head), Condvar::new()),
                #[cfg(feature = "tokio")]
                head_tx: tokio::sync::watch::channel(head).0,
                files: Mutex::new(HashMap::new()),
                _lock: lock,
            }),
        })
    }

    /// The directory the database lives in.
    pub fn path(&self) -> &Path {
        self.inner.layout.root()
    }

    /// The options the database was opened with.
    pub fn options(&self) -> &Options {
        &self.inner.opts
    }

    /// The next global position: one past the last committed event.
    pub fn head(&self) -> u64 {
        self.inner.head.load(Ordering::Acquire)
    }

    /// Base of the first kept segment: positions below it were dropped by
    /// [`retain_from`](Db::retain_from). 0 until then.
    pub fn floor(&self) -> u64 {
        self.inner.floor.load(Ordering::Acquire)
    }

    /// The database's identity: minted when it was created, carried by
    /// its backups, shared by its replicas.
    pub fn id(&self) -> Uuid {
        *self.inner.db_id.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// The fencing epoch (0 until a promotion). Daemons compare it with
    /// the epoch a writer sends; the library only stores it.
    pub fn epoch(&self) -> u64 {
        self.inner.epoch.load(Ordering::Acquire)
    }

    /// Durably sets the epoch. An epoch below the current one is
    /// [`Error::EpochNotNewer`]; the current one is a no-op.
    pub fn set_epoch(&self, epoch: u64) -> Result<()> {
        self.inner.durable_index(|inner, txn| {
            let current = inner.epoch.load(Ordering::Acquire);
            if epoch < current {
                return Err(Error::EpochNotNewer { epoch, current });
            }
            txn.open_table(crate::index::META)?
                .insert(crate::index::EPOCH_KEY, epoch)?;
            Ok(())
        })?;
        self.inner.epoch.store(epoch, Ordering::Release);
        Ok(())
    }

    /// Makes an empty database take on another's identity, so it can
    /// replicate from it. [`Error::NotEmpty`] once anything was written.
    pub fn adopt_id(&self, id: Uuid) -> Result<()> {
        self.inner.durable_index(|inner, txn| {
            let head = inner.head.load(Ordering::Acquire);
            if head != 0 || inner.floor.load(Ordering::Acquire) != 0 {
                return Err(Error::NotEmpty { head });
            }
            txn.open_table(crate::index::META_BYTES)?
                .insert(crate::index::DB_ID_KEY, id.as_bytes().as_slice())?;
            Ok(())
        })?;
        *self.inner.db_id.lock().unwrap_or_else(|p| p.into_inner()) = id;
        Ok(())
    }

    /// The stored crc32 of the record at `position`: a cheap way for two
    /// copies of a database to check they hold the same history up to
    /// there. `None` past the head or below the floor.
    pub fn fingerprint(&self, position: u64) -> Result<Option<u32>> {
        let Some(loc) = self.inner.index.locate(position)? else {
            return Ok(None);
        };
        let path = self.inner.layout.segment(loc.base);
        let file = self.inner.file(loc.base)?;
        let frame = segment::read_bytes_at(&file, &path, loc.offset, segment::FRAME_LEN)?;
        Ok(Some(u32::from_be_bytes(frame[4..8].try_into().unwrap())))
    }

    /// The next chunk a replica at `from` should apply: whole transactions
    /// from `from` on, at least one and about `max_events` events (a single
    /// longer transaction is sent whole). `None` when `from` is the head.
    pub fn replication_chunk(&self, from: u64, max_events: usize) -> Result<Option<Chunk>> {
        replicate::chunk(&self.inner, from, max_events)
    }

    /// Appends a chunk taken from another copy of this database. `from`
    /// must be the head ([`Error::ReplicationGap`] otherwise); returns the
    /// new head, which is `to`.
    pub fn apply_chunk(&self, chunk: &Chunk) -> Result<u64> {
        replicate::apply(&self.inner, chunk.from, chunk.to, &chunk.frames)
    }

    /// Removes the identity: the state of a database written before
    /// identities existed. For tests of the upgrade path; reopen after.
    #[doc(hidden)]
    pub fn debug_forget_id(&self) -> Result<()> {
        self.inner.index.forget_db_id()
    }

    /// Head, floor, and what the segment directory holds.
    pub fn info(&self) -> Result<Info> {
        let dir = self.inner.layout.segments_dir();
        let listed = segment::list(&dir)?;
        let mut bytes = 0;
        for (_, path) in &listed {
            bytes += fs::metadata(path)
                .map_err(|e| Error::io(path, "metadata", e))?
                .len();
        }
        Ok(Info {
            head: self.head(),
            floor: self.floor(),
            segments: listed.len(),
            bytes,
            db_id: self.id(),
            epoch: self.epoch(),
            fsync: self.inner.opts.fsync,
        })
    }

    /// Drops every segment that lies wholly below `floor`, freeing its
    /// space. The floor in force becomes the base of the first kept
    /// segment (so it is at most `floor`); positions never renumber.
    /// Reads below the floor fail with [`Error::BelowFloor`]; aggregate
    /// and tag reads return what survives; aggregate lengths are unchanged,
    /// so [`Expected::Exact`](crate::Expected::Exact) keeps working.
    /// `floor` past the head is [`Error::PositionOutOfRange`]; a floor at
    /// or below the current one is a no-op.
    pub fn retain_from(&self, floor: u64) -> Result<Retained> {
        retain::retain(&self.inner, floor, true)
    }

    /// Writes a full backup of everything committed so far to `archive`
    /// while the database stays in use. The archive holds exactly the
    /// events below the head at the moment it was taken; restore it with
    /// [`restore_backup`](crate::restore_backup).
    pub fn backup(&self, archive: impl AsRef<Path>) -> Result<BackupMeta> {
        backup::write_full(&self.inner, archive.as_ref())
    }

    /// Writes an incremental backup holding the events at positions
    /// `since..head`, to be applied with [`apply_backup`](Db::apply_backup)
    /// onto a database whose head is `since`. `since` past the head is
    /// [`Error::PositionOutOfRange`]; below the floor,
    /// [`Error::BelowFloor`].
    pub fn backup_since(&self, archive: impl AsRef<Path>, since: u64) -> Result<BackupMeta> {
        backup::write_incremental(&self.inner, archive.as_ref(), since)
    }

    /// Appends the events of an incremental backup. The archive is read
    /// and checked in full before anything is written; the head must equal
    /// the archive's base head, else [`Error::WrongBaseHead`] and nothing
    /// changes.
    pub fn apply_backup(&self, archive: impl AsRef<Path>) -> Result<BackupMeta> {
        backup::apply(&self.inner, archive.as_ref())
    }

    /// How many events the aggregate has; 0 for one never written.
    pub fn aggregate_len(&self, id: Uuid) -> Result<u64> {
        self.inner.index.aggregate_len(&id)
    }

    /// The aggregate's events from `from_index`, at most `limit`, in index
    /// order. Empty for an unknown aggregate or a `from_index` at or past
    /// its length.
    pub fn read_aggregate(&self, id: Uuid, from_index: u64, limit: usize) -> Result<Vec<Event>> {
        let limit = all_if_zero(limit);
        let txn = self.inner.index.begin_read()?;
        let found = self
            .inner
            .index
            .aggregate_positions(&txn, &id, from_index, limit)?;
        self.assemble_aggregate(&txn, id, found)
    }

    /// The aggregate's events at indices `<= from_index`, newest first, at
    /// most `limit`. `from_index` is clamped to the aggregate's last event,
    /// so `u64::MAX` means "from the end".
    pub fn read_aggregate_backward(
        &self,
        id: Uuid,
        from_index: u64,
        limit: usize,
    ) -> Result<Vec<Event>> {
        let limit = all_if_zero(limit);
        let txn = self.inner.index.begin_read()?;
        let found = self
            .inner
            .index
            .aggregate_positions_rev(&txn, &id, from_index, limit)?;
        self.assemble_aggregate(&txn, id, found)
    }

    fn assemble_aggregate(
        &self,
        txn: &redb::ReadTransaction,
        id: Uuid,
        found: Vec<(u64, u64)>,
    ) -> Result<Vec<Event>> {
        let positions: Vec<u64> = found.iter().map(|(_, p)| *p).collect();
        let locations = Index::locate_many(txn, &positions)?;
        let mut out = Vec::with_capacity(found.len());
        for ((index, position), loc) in found.into_iter().zip(locations) {
            let ev = self.read_located(position, loc)?;
            if ev.aggregate != Some(id) || ev.index != index {
                return Err(self.mismatch(
                    position,
                    format!(
                        "index lists it as {id} #{index}, record says {:?} #{}",
                        ev.aggregate, ev.index
                    ),
                ));
            }
            out.push(ev);
        }
        Ok(out)
    }

    /// Every event from global position `from`, at most `limit`. Empty at
    /// the head; [`Error::PositionOutOfRange`] past it.
    pub fn read_all(&self, from: u64, limit: usize) -> Result<Vec<Event>> {
        self.read_all_with(from, limit, || {})
    }

    /// `read_all` with a hook run between the head check and the read
    /// snapshot; the floor is read inside that snapshot, so a retention
    /// landing in the hook is seen. For tests of that ordering.
    #[doc(hidden)]
    pub fn read_all_with(
        &self,
        from: u64,
        limit: usize,
        between: impl FnOnce(),
    ) -> Result<Vec<Event>> {
        let head = self.head();
        if from > head {
            return Err(Error::PositionOutOfRange {
                position: from,
                head,
            });
        }
        between();
        let txn = self.inner.index.begin_read()?;
        // The floor the snapshot holds, not the atomic: a retention that
        // committed after the atomic was read would otherwise hide the
        // positions it dropped as if they had been read.
        let floor = Index::floor_in(&txn)?;
        if from < floor {
            return Err(Error::BelowFloor {
                position: from,
                floor,
            });
        }
        if from == head {
            return Ok(Vec::new());
        }
        let found = self
            .inner
            .index
            .positions_from(&txn, from, all_if_zero(limit))?;
        self.read_runs(&found)
    }

    /// Events at positions `<= from`, newest first, at most `limit`. `from`
    /// is clamped to the newest event, so `u64::MAX` means "from the end";
    /// empty when there are no events.
    pub fn read_all_backward(&self, from: u64, limit: usize) -> Result<Vec<Event>> {
        let limit = all_if_zero(limit);
        let txn = self.inner.index.begin_read()?;
        let floor = Index::floor_in(&txn)?;
        let mut found = self.inner.index.positions_to(&txn, floor, from, limit)?;
        found.reverse();
        let mut out = self.read_runs(&found)?;
        out.reverse();
        Ok(out)
    }

    /// Every event carrying `tag` from global position `from`, at most
    /// `limit`, in global order.
    pub fn read_tag(&self, tag: impl AsRef<[u8]>, from: u64, limit: usize) -> Result<Vec<Event>> {
        let tag = tag.as_ref();
        let limit = all_if_zero(limit);
        let txn = self.inner.index.begin_read()?;
        let positions = self.inner.index.tag_positions(&txn, tag, from, limit)?;
        self.assemble_tag(&txn, tag, positions)
    }

    /// Events carrying `tag` at positions `<= from`, newest first, at most
    /// `limit`. `u64::MAX` means "from the end".
    pub fn read_tag_backward(
        &self,
        tag: impl AsRef<[u8]>,
        from: u64,
        limit: usize,
    ) -> Result<Vec<Event>> {
        let tag = tag.as_ref();
        let limit = all_if_zero(limit);
        let txn = self.inner.index.begin_read()?;
        let positions = self.inner.index.tag_positions_rev(&txn, tag, from, limit)?;
        self.assemble_tag(&txn, tag, positions)
    }

    fn assemble_tag(
        &self,
        txn: &redb::ReadTransaction,
        tag: &[u8],
        positions: Vec<u64>,
    ) -> Result<Vec<Event>> {
        let locations = Index::locate_many(txn, &positions)?;
        let mut out = Vec::with_capacity(positions.len());
        for (position, loc) in positions.into_iter().zip(locations) {
            let ev = self.read_located(position, loc)?;
            if !ev.has_tag(tag) {
                return Err(self.mismatch(
                    position,
                    "tag index lists a record that does not carry the tag",
                ));
            }
            out.push(ev);
        }
        Ok(out)
    }

    /// Committed events in global order from `from`, then new ones as they
    /// are committed. `from` may be past the head: the subscription then
    /// waits for that position.
    pub fn subscribe(&self, from: u64) -> Subscription {
        Subscription::all(self.clone(), from)
    }

    /// The events carrying `tag`, in global order from `from`, then new
    /// ones as they are committed.
    pub fn subscribe_tag(&self, tag: impl AsRef<[u8]>, from: u64) -> Subscription {
        Subscription::tag(self.clone(), Bytes::copy_from_slice(tag.as_ref()), from)
    }

    /// Every event from global position `from` to the head, a page at a
    /// time. Ends at the head; a position past it is
    /// [`Error::PositionOutOfRange`] on the first item and below the floor
    /// [`Error::BelowFloor`].
    pub fn iter_all(&self, from: u64) -> EventIter {
        EventIter::new(self.clone(), Paged::All, from)
    }

    /// The aggregate's events from `from_index` to its end, a page at a time.
    pub fn iter_aggregate(&self, id: Uuid, from_index: u64) -> EventIter {
        EventIter::new(self.clone(), Paged::Aggregate(id), from_index)
    }

    /// The events carrying `tag` from `from` to the head, a page at a time.
    pub fn iter_tag(&self, tag: impl AsRef<[u8]>, from: u64) -> EventIter {
        EventIter::new(
            self.clone(),
            Paged::Tag(Bytes::copy_from_slice(tag.as_ref())),
            from,
        )
    }

    /// Starts a transaction. Nothing happens until `commit`.
    pub fn transaction(&self) -> Transaction {
        Transaction::new(self.clone())
    }

    /// Forces everything committed so far to disk. Only matters under
    /// [`Fsync::Never`]; also run when the last handle is dropped.
    pub fn flush(&self) -> Result<()> {
        self.inner.durable_index(|_, _| Ok(()))
    }

    /// Blocks until the head is at least `min_head` or `timeout` passes.
    /// Returns the head seen last.
    pub fn wait_for(&self, min_head: u64, timeout: Duration) -> u64 {
        let deadline = Instant::now().checked_add(timeout);
        let (lock, cv) = &self.inner.head_watch;
        let mut head = lock.lock().unwrap_or_else(|p| p.into_inner());
        while *head < min_head {
            let wait = match deadline {
                Some(d) => {
                    let now = Instant::now();
                    if now >= d {
                        break;
                    }
                    d - now
                }
                None => Duration::from_secs(60),
            };
            head = cv
                .wait_timeout(head, wait)
                .unwrap_or_else(|p| p.into_inner())
                .0;
        }
        *head
    }

    /// Waits until the head is at least `min_head`. Returns the head seen
    /// last. Cancel-safe.
    #[cfg(feature = "tokio")]
    pub async fn wait_for_async(&self, min_head: u64) -> u64 {
        let mut rx = self.inner.head_tx.subscribe();
        match rx.wait_for(|h| *h >= min_head).await {
            Ok(h) => *h,
            // The sender lives in `Inner`, which `self` keeps alive.
            Err(_) => self.head(),
        }
    }

    /// Writes (and fsyncs) the transaction's records without committing
    /// them to the index: the state a crash between the segment write and
    /// the index commit leaves. For tests of recovery only; the `Db` must be
    /// dropped afterwards.
    #[doc(hidden)]
    pub fn debug_write_without_commit(&self, txn: Transaction) -> Result<Committed> {
        let (_db, appends) = txn.into_parts();
        txn::commit(&self.inner, appends, Mode::WriteWithoutCommit)
    }

    /// Rebuilds `index.redb` from the segment files alone, for a database
    /// whose index is missing or damaged. Takes `LOCK`, so no `Db` may be
    /// open on `path`. The new index is written beside the old one and
    /// renamed into place only when complete, so a crash mid-rebuild leaves
    /// the old index (if any) untouched. Segment files are never modified:
    /// the next [`Db::open`] truncates whatever the new head excludes.
    ///
    /// Every complete transaction in the segments is indexed, including a
    /// trailing one whose commit was never acknowledged; an incomplete
    /// trailing transaction is dropped (see [`Rebuilt::dropped`]). Damage
    /// anywhere but the trailing transaction is [`Error::Corrupt`].
    pub fn rebuild_index(path: impl AsRef<Path>, opts: &Options) -> Result<Rebuilt> {
        crate::rebuild::rebuild(path.as_ref(), opts)
    }

    /// Retention's index update without the file removal: the state a crash
    /// between the two leaves. For tests of recovery only; the `Db` must be
    /// dropped afterwards.
    #[doc(hidden)]
    pub fn debug_retain_index_only(&self, floor: u64) -> Result<Retained> {
        retain::retain(&self.inner, floor, false)
    }

    /// The next commit fails after writing its bytes and before its index
    /// commit. For tests of the cleanup that follows.
    #[doc(hidden)]
    pub fn debug_fail_next_commit(&self) {
        self.inner.debug_fail_next_commit();
    }

    /// Where the index places `position`: `(segment base, offset, frame
    /// length)`. For tests of segment rotation.
    #[doc(hidden)]
    pub fn debug_locate(&self, position: u64) -> Result<Option<(u64, u64, u32)>> {
        Ok(self
            .inner
            .index
            .locate(position)?
            .map(|l| (l.base, l.offset, l.frame_len)))
    }

    fn mismatch(&self, position: u64, reason: impl Into<String>) -> Error {
        Error::Inconsistent {
            position,
            reason: reason.into(),
        }
    }

    /// Reads ascending, index-ordered records, one positional read per run
    /// of back-to-back records. Every record is still checked on its own.
    fn read_runs(&self, found: &[(u64, Location)]) -> Result<Vec<Event>> {
        let max_record = self.inner.opts.max_record_bytes;
        let mut out = Vec::with_capacity(found.len());
        for run in index::coalesce(found, COALESCE_MAX_BYTES) {
            let first = &found[run.start].1;
            let path = self.inner.layout.segment(first.base);
            let file = self.inner.file(first.base)?;
            let len: u64 = found[run.clone()]
                .iter()
                .map(|(_, l)| u64::from(l.frame_len))
                .sum();
            let bytes = segment::read_bytes_at(&file, &path, first.offset, len as usize)?;
            let mut at = 0usize;
            for (position, loc) in &found[run] {
                let frame = &bytes[at..at + loc.frame_len as usize];
                let body = segment::check_frame(&path, loc.offset, frame, max_record)?;
                out.push(self.decoded(&path, loc.offset, *position, body)?);
                at += loc.frame_len as usize;
            }
        }
        Ok(out)
    }

    fn decoded(&self, path: &Path, offset: u64, position: u64, body: &[u8]) -> Result<Event> {
        let ev =
            event::decode_body(body).map_err(|e| Error::corrupt(path, offset, e.to_string()))?;
        if ev.position != position {
            return Err(Error::corrupt(
                path,
                offset,
                format!(
                    "index points position {position} at a record carrying {}",
                    ev.position
                ),
            ));
        }
        Ok(ev)
    }

    fn read_located(&self, position: u64, loc: Option<Location>) -> Result<Event> {
        let loc = loc
            .ok_or_else(|| self.mismatch(position, "listed but missing from the position table"))?;
        let path = self.inner.layout.segment(loc.base);
        let file = self.inner.file(loc.base)?;
        let body = segment::read_record_at(
            &file,
            &path,
            loc.offset,
            loc.frame_len,
            self.inner.opts.max_record_bytes,
        )?;
        self.decoded(&path, loc.offset, position, &body)
    }
}

/// What [`Db::info`] reports.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct Info {
    /// The next global position.
    pub head: u64,
    /// The retention floor: positions below it were dropped.
    pub floor: u64,
    /// Segment files present.
    pub segments: usize,
    /// Bytes those files take.
    pub bytes: u64,
    /// The database's identity.
    pub db_id: Uuid,
    /// The fencing epoch.
    pub epoch: u64,
    /// Whether commits sync to disk.
    pub fsync: Fsync,
}

/// A read, usize::MAX when the caller asked for everything with 0.
fn all_if_zero(limit: usize) -> usize {
    if limit == 0 { usize::MAX } else { limit }
}

enum Paged {
    All,
    Aggregate(Uuid),
    Tag(Bytes),
}

/// A finite, paged walk over committed events; created by
/// [`Db::iter_all`], [`Db::iter_aggregate`] and [`Db::iter_tag`]. Reads
/// 256 events at a time and ends at the head (or the aggregate's end).
pub struct EventIter {
    db: Db,
    what: Paged,
    cursor: u64,
    buffer: std::collections::VecDeque<Event>,
    done: bool,
}

const PAGE: usize = 256;

impl EventIter {
    fn new(db: Db, what: Paged, from: u64) -> Self {
        EventIter {
            db,
            what,
            cursor: from,
            buffer: std::collections::VecDeque::new(),
            done: false,
        }
    }

    fn fill(&mut self) -> Result<()> {
        let page = match &self.what {
            Paged::All => self.db.read_all(self.cursor, PAGE)?,
            Paged::Aggregate(id) => self.db.read_aggregate(*id, self.cursor, PAGE)?,
            Paged::Tag(tag) => self.db.read_tag(tag, self.cursor, PAGE)?,
        };
        if page.len() < PAGE {
            self.done = true;
        }
        if let Some(last) = page.last() {
            self.cursor = match &self.what {
                Paged::Aggregate(_) => last.index + 1,
                _ => last.position + 1,
            };
        }
        self.buffer.extend(page);
        Ok(())
    }
}

impl Iterator for EventIter {
    type Item = Result<Event>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(ev) = self.buffer.pop_front() {
                return Some(Ok(ev));
            }
            if self.done {
                return None;
            }
            if let Err(e) = self.fill() {
                self.done = true;
                return Some(Err(e));
            }
        }
    }
}

impl std::fmt::Debug for Db {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Db")
            .field("path", &self.inner.layout.root())
            .field("head", &self.head())
            .field("floor", &self.floor())
            .finish()
    }
}

impl Inner {
    /// A shared read handle on a segment. Never seek on it: readers use
    /// positional reads only.
    pub(crate) fn file(&self, base: u64) -> Result<Arc<File>> {
        let mut files = self.files.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(f) = files.get(&base) {
            return Ok(f.clone());
        }
        let path = self.layout.segment(base);
        let f = match File::open(&path) {
            Ok(f) => Arc::new(f),
            // A segment that retention removed under an older snapshot.
            Err(e)
                if e.kind() == std::io::ErrorKind::NotFound
                    && base < self.floor.load(Ordering::Acquire) =>
            {
                return Err(Error::BelowFloor {
                    position: base,
                    floor: self.floor.load(Ordering::Acquire),
                });
            }
            Err(e) => return Err(Error::io(&path, "open", e)),
        };
        files.insert(base, f.clone());
        Ok(f)
    }

    /// A durable index write with the segment bytes synced first, so the
    /// index never vouches for data that is not on disk. Takes the writer
    /// lock; `f` gets the transaction and this `Inner`.
    pub(crate) fn durable_index<T>(
        &self,
        f: impl FnOnce(&Inner, &redb::WriteTransaction) -> Result<T>,
    ) -> Result<T> {
        let w = self.writer.lock().unwrap_or_else(|p| p.into_inner());
        w.sync_data()?;
        let txn = self.index.begin_write_durable()?;
        let out = f(self, &txn)?;
        txn.commit()?;
        drop(w);
        Ok(out)
    }

    /// Marks the next commit to fail after its bytes are written but
    /// before its index commit. For tests of the cleanup that follows.
    #[doc(hidden)]
    pub fn debug_fail_next_commit(&self) {
        self.fail_next_commit.store(true, Ordering::Release);
    }

    /// Drops cached handles on segments about to be removed.
    pub(crate) fn forget_files(&self, bases: &[u64]) {
        let mut files = self.files.lock().unwrap_or_else(|p| p.into_inner());
        for b in bases {
            files.remove(b);
        }
    }

    pub(crate) fn publish_head(&self, head: u64) {
        self.head.store(head, Ordering::Release);
        let (lock, cv) = &self.head_watch;
        *lock.lock().unwrap_or_else(|p| p.into_inner()) = head;
        cv.notify_all();
        #[cfg(feature = "tokio")]
        self.head_tx.send_replace(head);
    }

    /// Starts a new segment at `base`. The commit that filled the old one
    /// already succeeded, so a failure here just keeps writing to it.
    pub(crate) fn roll(&self, w: &mut MutexGuard<'_, SegmentWriter>, base: u64) {
        // The old segment is closed for good: cut any leftovers of a
        // failed commit and make it durable, whatever the fsync policy,
        // since nothing will sync it later. A new segment's header and
        // directory entry are synced for the same reason.
        if w.truncate_to_len().is_err() || w.sync_data().is_err() {
            return;
        }
        if let Ok(next) = SegmentWriter::create(&self.layout.segments_dir(), base, true) {
            **w = next;
        }
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        if self.opts.fsync == Fsync::Never {
            let w = self.writer.get_mut().unwrap_or_else(|p| p.into_inner());
            if w.sync_data().is_err() {
                return;
            }
            if let Ok(txn) = self.index.begin_write_durable() {
                let _ = txn.commit();
            }
        }
    }
}
