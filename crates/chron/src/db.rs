//! The database handle: open, read, and the shared state the writer uses.

use std::collections::{HashMap, VecDeque};
use std::fs::{self, File, OpenOptions, TryLockError};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, RwLock};
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
use crate::stream::{self, StreamDef, StreamInfo};
use crate::subscribe::Subscription;
use crate::truncate::{self, Truncated};
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
    /// Batches since the last durable index commit, and when that was.
    /// Taken under the writer lock only.
    pub(crate) checkpoint: Mutex<Checkpoint>,
    /// Transactions that joined a commit and wait for the writer lock's
    /// holder to land them. Lock order: the writer lock, then this.
    pub(crate) commits: Mutex<VecDeque<txn::Pending>>,
    pub(crate) counters: Counters,
    /// The last landed batches' bytes, so a replication chunk cut right
    /// after a land reads memory rather than the segment the next batch
    /// is flushing (a read of a file under `F_FULLFSYNC` waits for the
    /// flush). Filled under the writer lock, cleared by a truncation.
    pub(crate) recent: Mutex<Recent>,
    /// Every stream definition, ready or not: what the commit path indexes
    /// for. Replaced under the writer lock (`stream::begin`, `delete`),
    /// read once per batch.
    pub(crate) streams: RwLock<Arc<Vec<Arc<StreamDef>>>>,
    head_watch: (Mutex<u64>, Condvar),
    #[cfg(feature = "tokio")]
    pub(crate) head_tx: tokio::sync::watch::Sender<u64>,
    /// Bumped when a stream is begun or deleted: wakes stream subscribers.
    #[cfg(feature = "tokio")]
    pub(crate) streams_tx: tokio::sync::watch::Sender<u64>,
    files: Mutex<FileCache>,
    _lock: Lock,
}

/// One landed batch kept in memory for the replication chunk cut.
pub(crate) struct RecentBatch {
    pub(crate) from: u64,
    pub(crate) to: u64,
    /// Each position's offset into `frames`, in order.
    pub(crate) offsets: Vec<u64>,
    pub(crate) frames: Arc<[u8]>,
}

/// A ring of the last landed batches, contiguous in position, bounded in
/// bytes.
pub(crate) struct Recent {
    batches: VecDeque<RecentBatch>,
    bytes: usize,
    cap: usize,
}

impl Recent {
    pub(crate) fn new(cap: usize) -> Recent {
        Recent {
            batches: VecDeque::new(),
            bytes: 0,
            cap,
        }
    }

    /// Keeps `b`; a gap before it (a truncation landed between) empties
    /// the ring first, and the oldest batches go once the cap is passed.
    pub(crate) fn push(&mut self, b: RecentBatch) {
        if self.batches.back().is_some_and(|l| l.to != b.from) {
            self.clear();
        }
        if b.frames.len() > self.cap {
            self.clear();
            return;
        }
        self.bytes += b.frames.len();
        self.batches.push_back(b);
        while self.bytes > self.cap {
            if let Some(f) = self.batches.pop_front() {
                self.bytes -= f.frames.len();
            }
        }
    }

    /// The frames of `from..to`, when the ring holds all of them.
    pub(crate) fn read(&self, from: u64, to: u64) -> Option<Vec<u8>> {
        if from >= to {
            return Some(Vec::new());
        }
        let start = self
            .batches
            .iter()
            .position(|b| b.from <= from && from < b.to)?;
        let mut out = Vec::new();
        let mut pos = from;
        for b in self.batches.range(start..) {
            let lo = pos.max(b.from);
            let hi = to.min(b.to);
            let at = b.offsets[(lo - b.from) as usize] as usize;
            let end = if hi == b.to {
                b.frames.len()
            } else {
                b.offsets[(hi - b.from) as usize] as usize
            };
            out.extend_from_slice(&b.frames[at..end]);
            pos = hi;
            if pos == to {
                return Some(out);
            }
        }
        None
    }

    pub(crate) fn clear(&mut self) {
        self.batches.clear();
        self.bytes = 0;
    }
}

/// Where the index stands against the disk.
pub(crate) struct Checkpoint {
    pub(crate) batches_since: u64,
    pub(crate) last: Instant,
}

/// A database. Cheap to clone; every clone shares the same files, index
/// and writer. Dropping the last clone checkpoints the index and, under
/// [`Fsync::Never`], syncs the segment too.
#[derive(Clone)]
pub struct Db {
    pub(crate) inner: Arc<Inner>,
}

/// A durable cursor as [`Db::set_cursor`] stores it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CursorState {
    /// The cursor's name.
    pub name: String,
    /// The position after the last event the consumer processed.
    pub next: u64,
    /// The session that owns it; a new subscription takes the next one.
    pub session: u64,
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
        let streams = stream::load(&index)?;

        let opts_max_open = opts.max_open_segments.max(1);
        // The chunk ring: four batches' worth, a mebibyte at least.
        let recent_cap = opts.group_commit_max_bytes.saturating_mul(4).max(1 << 20);
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
                checkpoint: Mutex::new(Checkpoint {
                    batches_since: 0,
                    last: Instant::now(),
                }),
                commits: Mutex::new(VecDeque::new()),
                counters: Counters::default(),
                recent: Mutex::new(Recent::new(recent_cap)),
                streams: RwLock::new(Arc::new(streams)),
                head_watch: (Mutex::new(head), Condvar::new()),
                #[cfg(feature = "tokio")]
                head_tx: tokio::sync::watch::channel(head).0,
                #[cfg(feature = "tokio")]
                streams_tx: tokio::sync::watch::channel(0).0,
                files: Mutex::new(FileCache::new(opts_max_open)),
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
    /// from `from` on, at least one, about `max_events` events and at most
    /// `max_bytes` (a single longer transaction is sent whole, so a chunk
    /// may exceed `max_bytes` by itself). `None` when `from` is the head.
    pub fn replication_chunk(
        &self,
        from: u64,
        max_events: usize,
        max_bytes: usize,
    ) -> Result<Option<Chunk>> {
        replicate::chunk(&self.inner, from, max_events, max_bytes)
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

    /// Removes the cursor table: the state of an index written before
    /// cursors existed. For tests of the upgrade path.
    #[doc(hidden)]
    pub fn debug_drop_cursor_table(&self) -> Result<()> {
        self.inner.index.drop_cursor_table()
    }

    /// Removes the stream tables: the state of an index written before
    /// streams existed. For tests of the upgrade path.
    #[doc(hidden)]
    pub fn debug_drop_stream_tables(&self) -> Result<()> {
        self.inner.index.drop_stream_tables()?;
        self.inner.replace_streams(|v| v.clear());
        Ok(())
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
            streams: self.inner.streams().len(),
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

    /// Drops every record at positions `to..head`: the way a copy of a
    /// database that took writes of its own cuts back to the history it
    /// shares with another copy, so it can follow that copy again. `to`
    /// must start a transaction ([`Error::Inconsistent`] otherwise), be at
    /// most the head ([`Error::PositionOutOfRange`]) and at least the
    /// floor ([`Error::BelowFloor`]); the head itself is a no-op. Aggregate
    /// lengths and tag indexes shrink with the records. Positions are
    /// reused by what is committed next, so subscriptions past `to` must
    /// be restarted; the daemon ends them. Takes the writer lock: commits
    /// queued behind it land after the cut.
    pub fn truncate_to(&self, to: u64) -> Result<Truncated> {
        truncate::truncate(&self.inner, to)
    }

    /// Writes a full backup of everything committed so far to `archive`
    /// while the database stays in use. The archive holds exactly the
    /// events below the head at the moment it was taken; restore it with
    /// [`restore_backup`](crate::restore_backup).
    pub fn backup(&self, archive: impl AsRef<Path>) -> Result<BackupMeta> {
        backup::write_full(&self.inner, archive.as_ref())
    }

    /// [`backup`](Db::backup) to any sink: the archive's bytes, in order,
    /// as the file would hold them, with `bytes` counting them. Nothing is
    /// fsynced. The database's maintenance lock is held until the sink has
    /// taken the last byte, so a slow sink holds up retention and other
    /// backups of this database, not commits.
    pub fn backup_to(&self, out: &mut impl std::io::Write) -> Result<BackupMeta> {
        backup::write_full_to(&self.inner, out, Path::new("<backup stream>"))
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
            .aggregate_positions(&txn, &id, from_index, limit, u64::MAX)?;
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
        self.read_aggregate_backward_upto(id, from_index, limit, u64::MAX)
    }

    /// [`read_aggregate_backward`](Db::read_aggregate_backward) over the
    /// events at positions below `upto` only.
    ///
    /// The `_upto` reads take an exclusive cap on the global position,
    /// `u64::MAX` for none: a daemon serving only what a majority of its
    /// group holds caps every read at that bound. The cap is applied
    /// inside the index snapshot, so a position the cap excludes is never
    /// returned even though the snapshot holds it.
    pub fn read_aggregate_backward_upto(
        &self,
        id: Uuid,
        from_index: u64,
        limit: usize,
        upto: u64,
    ) -> Result<Vec<Event>> {
        let limit = all_if_zero(limit);
        let txn = self.inner.index.begin_read()?;
        let found = self
            .inner
            .index
            .aggregate_positions_rev(&txn, &id, from_index, limit, upto)?;
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
        self.assemble_aggregate_located(id, found, locations)
    }

    fn assemble_aggregate_located(
        &self,
        id: Uuid,
        found: Vec<(u64, u64)>,
        locations: Vec<Option<Location>>,
    ) -> Result<Vec<Event>> {
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

    /// [`read_all`](Db::read_all) cut at the record that would take the
    /// page past `max_bytes` (at least one record is returned): a page
    /// bounded in memory whatever the records' sizes. The next page starts
    /// after the last event returned.
    pub fn read_all_bounded(
        &self,
        from: u64,
        limit: usize,
        max_bytes: usize,
    ) -> Result<Vec<Event>> {
        self.read_all_bounded_upto(from, limit, max_bytes, u64::MAX)
    }

    /// [`read_all_bounded`](Db::read_all_bounded) over the positions
    /// below `upto` only (see
    /// [`read_aggregate_backward_upto`](Db::read_aggregate_backward_upto)).
    /// `from` past `min(head, upto)` is [`Error::PositionOutOfRange`] with
    /// that bound as its `head`.
    pub fn read_all_bounded_upto(
        &self,
        from: u64,
        limit: usize,
        max_bytes: usize,
        upto: u64,
    ) -> Result<Vec<Event>> {
        Ok(self.all_page(from, limit, max_bytes, upto)?.events)
    }

    /// [`read_aggregate`](Db::read_aggregate) cut at `max_bytes` like
    /// [`read_all_bounded`](Db::read_all_bounded).
    pub fn read_aggregate_bounded(
        &self,
        id: Uuid,
        from_index: u64,
        limit: usize,
        max_bytes: usize,
    ) -> Result<Vec<Event>> {
        self.read_aggregate_bounded_upto(id, from_index, limit, max_bytes, u64::MAX)
    }

    /// [`read_aggregate_bounded`](Db::read_aggregate_bounded) over the
    /// events at positions below `upto` only.
    pub fn read_aggregate_bounded_upto(
        &self,
        id: Uuid,
        from_index: u64,
        limit: usize,
        max_bytes: usize,
        upto: u64,
    ) -> Result<Vec<Event>> {
        Ok(self
            .aggregate_page(id, from_index, limit, max_bytes, upto)?
            .events)
    }

    /// [`read_tag`](Db::read_tag) cut at `max_bytes` like
    /// [`read_all_bounded`](Db::read_all_bounded).
    pub fn read_tag_bounded(
        &self,
        tag: impl AsRef<[u8]>,
        from: u64,
        limit: usize,
        max_bytes: usize,
    ) -> Result<Vec<Event>> {
        self.read_tag_bounded_upto(tag, from, limit, max_bytes, u64::MAX)
    }

    /// [`read_tag_bounded`](Db::read_tag_bounded) over the events at
    /// positions below `upto` only.
    pub fn read_tag_bounded_upto(
        &self,
        tag: impl AsRef<[u8]>,
        from: u64,
        limit: usize,
        max_bytes: usize,
        upto: u64,
    ) -> Result<Vec<Event>> {
        Ok(self
            .tag_page(tag.as_ref(), from, limit, max_bytes, upto)?
            .events)
    }

    /// A forward page of the global order, bounded by `limit` records,
    /// `max_bytes` and the positions below `upto`. `exhausted` says the
    /// read reached the end of what the snapshot held below
    /// `min(head, upto)` (no cut by the record or byte bound).
    pub(crate) fn all_page(
        &self,
        from: u64,
        limit: usize,
        max_bytes: usize,
        upto: u64,
    ) -> Result<Page> {
        let limit = all_if_zero(limit);
        let head = self.head().min(upto);
        if from > head {
            return Err(Error::PositionOutOfRange {
                position: from,
                head,
            });
        }
        let txn = self.inner.index.begin_read()?;
        let floor = Index::floor_in(&txn)?;
        if from < floor {
            return Err(Error::BelowFloor {
                position: from,
                floor,
            });
        }
        if from == head {
            return Ok(Page {
                events: Vec::new(),
                exhausted: true,
            });
        }
        let mut found = self.inner.index.positions_from(&txn, from, limit, upto)?;
        let short = found.len() < limit;
        let cut = cut_by_bytes(&mut found, |(_, l)| u64::from(l.frame_len), max_bytes);
        Ok(Page {
            events: self.read_runs(&found)?,
            exhausted: short && !cut,
        })
    }

    /// A forward page of one aggregate, bounded like [`Db::all_page`].
    pub(crate) fn aggregate_page(
        &self,
        id: Uuid,
        from_index: u64,
        limit: usize,
        max_bytes: usize,
        upto: u64,
    ) -> Result<Page> {
        let limit = all_if_zero(limit);
        let txn = self.inner.index.begin_read()?;
        let found = self
            .inner
            .index
            .aggregate_positions(&txn, &id, from_index, limit, upto)?;
        let short = found.len() < limit;
        let positions: Vec<u64> = found.iter().map(|(_, p)| *p).collect();
        let locations = Index::locate_many(&txn, &positions)?;
        let mut items: Vec<((u64, u64), Option<Location>)> =
            found.into_iter().zip(locations).collect();
        let cut = cut_by_bytes(
            &mut items,
            |(_, l)| l.as_ref().map_or(0, |l| u64::from(l.frame_len)),
            max_bytes,
        );
        let (found, locations): (Vec<(u64, u64)>, Vec<Option<Location>>) =
            items.into_iter().unzip();
        Ok(Page {
            events: self.assemble_aggregate_located(id, found, locations)?,
            exhausted: short && !cut,
        })
    }

    /// A forward page of one tag, bounded like [`Db::all_page`].
    pub(crate) fn tag_page(
        &self,
        tag: &[u8],
        from: u64,
        limit: usize,
        max_bytes: usize,
        upto: u64,
    ) -> Result<Page> {
        let limit = all_if_zero(limit);
        let txn = self.inner.index.begin_read()?;
        let positions = self
            .inner
            .index
            .tag_positions(&txn, tag, from, limit, upto)?;
        let short = positions.len() < limit;
        let locations = Index::locate_many(&txn, &positions)?;
        let mut items: Vec<(u64, Option<Location>)> =
            positions.into_iter().zip(locations).collect();
        let cut = cut_by_bytes(
            &mut items,
            |(_, l)| l.as_ref().map_or(0, |l| u64::from(l.frame_len)),
            max_bytes,
        );
        let (positions, locations): (Vec<u64>, Vec<Option<Location>>) = items.into_iter().unzip();
        Ok(Page {
            events: self.assemble_tag_located(tag, positions, locations)?,
            exhausted: short && !cut,
        })
    }

    /// A forward page of one stream, bounded like [`Db::all_page`].
    pub(crate) fn stream_page(
        &self,
        def: &StreamDef,
        from: u64,
        limit: usize,
        max_bytes: usize,
        upto: u64,
    ) -> Result<Page> {
        let limit = all_if_zero(limit);
        let txn = self.inner.index.begin_read()?;
        let positions = Index::stream_positions(&txn, def.id, from, limit, upto)?;
        let short = positions.len() < limit;
        let locations = Index::locate_many(&txn, &positions)?;
        let mut items: Vec<(u64, Option<Location>)> =
            positions.into_iter().zip(locations).collect();
        let cut = cut_by_bytes(
            &mut items,
            |(_, l)| l.as_ref().map_or(0, |l| u64::from(l.frame_len)),
            max_bytes,
        );
        let (positions, locations): (Vec<u64>, Vec<Option<Location>>) = items.into_iter().unzip();
        Ok(Page {
            events: self.assemble_stream_located(def, positions, locations)?,
            exhausted: short && !cut,
        })
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
            .positions_from(&txn, from, all_if_zero(limit), u64::MAX)?;
        self.read_runs(&found)
    }

    /// Events at positions `<= from`, newest first, at most `limit`. `from`
    /// is clamped to the newest event, so `u64::MAX` means "from the end";
    /// empty when there are no events.
    pub fn read_all_backward(&self, from: u64, limit: usize) -> Result<Vec<Event>> {
        self.read_all_backward_upto(from, limit, u64::MAX)
    }

    /// [`read_all_backward`](Db::read_all_backward) over the positions
    /// below `upto` only.
    pub fn read_all_backward_upto(&self, from: u64, limit: usize, upto: u64) -> Result<Vec<Event>> {
        let limit = all_if_zero(limit);
        let txn = self.inner.index.begin_read()?;
        let floor = Index::floor_in(&txn)?;
        let mut found = self
            .inner
            .index
            .positions_to(&txn, floor, from, limit, upto)?;
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
        let positions = self
            .inner
            .index
            .tag_positions(&txn, tag, from, limit, u64::MAX)?;
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
        self.read_tag_backward_upto(tag, from, limit, u64::MAX)
    }

    /// [`read_tag_backward`](Db::read_tag_backward) over the events at
    /// positions below `upto` only.
    pub fn read_tag_backward_upto(
        &self,
        tag: impl AsRef<[u8]>,
        from: u64,
        limit: usize,
        upto: u64,
    ) -> Result<Vec<Event>> {
        let tag = tag.as_ref();
        let limit = all_if_zero(limit);
        let txn = self.inner.index.begin_read()?;
        let positions = self
            .inner
            .index
            .tag_positions_rev(&txn, tag, from, limit, upto)?;
        self.assemble_tag(&txn, tag, positions)
    }

    fn assemble_tag(
        &self,
        txn: &redb::ReadTransaction,
        tag: &[u8],
        positions: Vec<u64>,
    ) -> Result<Vec<Event>> {
        let locations = Index::locate_many(txn, &positions)?;
        self.assemble_tag_located(tag, positions, locations)
    }

    fn assemble_tag_located(
        &self,
        tag: &[u8],
        positions: Vec<u64>,
        locations: Vec<Option<Location>>,
    ) -> Result<Vec<Event>> {
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

    fn assemble_stream_located(
        &self,
        def: &StreamDef,
        positions: Vec<u64>,
        locations: Vec<Option<Location>>,
    ) -> Result<Vec<Event>> {
        let mut out = Vec::with_capacity(positions.len());
        for (position, loc) in positions.into_iter().zip(locations) {
            let ev = self.read_located(position, loc)?;
            if !def.member(&ev.tags) {
                return Err(self.mismatch(
                    position,
                    "stream index lists a record that carries none of the stream's tags",
                ));
            }
            out.push(ev);
        }
        Ok(out)
    }

    /// The members of the stream `name` from global position `from`, at
    /// most `limit`, in global order; from the floor when `from` is below
    /// it. [`Error::NoStream`] without one, [`Error::StreamNotReady`]
    /// while its fill runs.
    pub fn read_stream(&self, name: &str, from: u64, limit: usize) -> Result<Vec<Event>> {
        self.read_stream_bounded_upto(name, from, limit, usize::MAX, u64::MAX)
    }

    /// [`read_stream`](Db::read_stream) cut at `max_bytes` like
    /// [`read_all_bounded`](Db::read_all_bounded).
    pub fn read_stream_bounded(
        &self,
        name: &str,
        from: u64,
        limit: usize,
        max_bytes: usize,
    ) -> Result<Vec<Event>> {
        self.read_stream_bounded_upto(name, from, limit, max_bytes, u64::MAX)
    }

    /// [`read_stream_bounded`](Db::read_stream_bounded) over the events
    /// at positions below `upto` only.
    pub fn read_stream_bounded_upto(
        &self,
        name: &str,
        from: u64,
        limit: usize,
        max_bytes: usize,
        upto: u64,
    ) -> Result<Vec<Event>> {
        let def = self.inner.resolve_stream(name)?;
        Ok(self.stream_page(&def, from, limit, max_bytes, upto)?.events)
    }

    /// Members of the stream at positions `<= from`, newest first, at
    /// most `limit`. `u64::MAX` means "from the end".
    pub fn read_stream_backward(&self, name: &str, from: u64, limit: usize) -> Result<Vec<Event>> {
        self.read_stream_backward_upto(name, from, limit, u64::MAX)
    }

    /// [`read_stream_backward`](Db::read_stream_backward) over the events
    /// at positions below `upto` only.
    pub fn read_stream_backward_upto(
        &self,
        name: &str,
        from: u64,
        limit: usize,
        upto: u64,
    ) -> Result<Vec<Event>> {
        let def = self.inner.resolve_stream(name)?;
        let limit = all_if_zero(limit);
        let txn = self.inner.index.begin_read()?;
        let positions = Index::stream_positions_rev(&txn, def.id, from, limit, upto)?;
        let locations = Index::locate_many(&txn, &positions)?;
        self.assemble_stream_located(&def, positions, locations)
    }

    /// The stream named `name`, if one is defined (ready or not).
    pub fn stream(&self, name: &str) -> Result<Option<StreamInfo>> {
        Ok(self
            .inner
            .streams()
            .iter()
            .find(|d| d.name == name)
            .map(|d| d.info()))
    }

    /// Every stream, by name, ready or not.
    pub fn streams(&self) -> Result<Vec<StreamInfo>> {
        let mut out: Vec<StreamInfo> = self.inner.streams().iter().map(|d| d.info()).collect();
        out.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(out)
    }

    /// Defines the stream `name` over `tags` and indexes its members: every
    /// event, committed before or after, that carries any of the tags. The
    /// definition is written first (every record landing from then on is
    /// indexed for it), then the earlier positions are filled a chunk at a
    /// time without holding up commits, backups or chunk cuts; the call
    /// returns once the stream is ready. Tags are sorted and deduplicated;
    /// at most [`MAX_STREAM_TAGS`](crate::MAX_STREAM_TAGS) of them, the
    /// name at most [`MAX_STREAM_NAME_BYTES`](crate::MAX_STREAM_NAME_BYTES)
    /// bytes ([`Error::InvalidStream`]). An existing name is
    /// [`Error::StreamExists`]. A creation interrupted by a crash leaves
    /// nothing behind.
    pub fn create_stream<T: Into<Bytes>>(
        &self,
        name: &str,
        tags: impl IntoIterator<Item = T>,
    ) -> Result<StreamInfo> {
        let def = stream::begin(
            &self.inner,
            name,
            tags.into_iter().map(Into::into).collect(),
        )?;
        if !stream::fill(&self.inner, &def, None)? {
            return Err(Error::NoStream(name.to_string()));
        }
        Ok(def.info())
    }

    /// The first half of [`create_stream`](Db::create_stream): the
    /// definition, durable and in force for new commits, not yet filled.
    #[doc(hidden)]
    pub fn begin_stream<T: Into<Bytes>>(
        &self,
        name: &str,
        tags: impl IntoIterator<Item = T>,
    ) -> Result<StreamInfo> {
        Ok(stream::begin(
            &self.inner,
            name,
            tags.into_iter().map(Into::into).collect(),
        )?
        .info())
    }

    /// The second half of [`create_stream`](Db::create_stream): fills a
    /// begun stream and marks it ready. `Ok(false)` if it was deleted
    /// meanwhile; [`Error::NoStream`] if it was never begun.
    #[doc(hidden)]
    pub fn fill_stream(&self, name: &str) -> Result<bool> {
        let def = self
            .inner
            .streams()
            .iter()
            .find(|d| d.name == name)
            .cloned()
            .ok_or_else(|| Error::NoStream(name.to_string()))?;
        stream::fill(&self.inner, &def, None)
    }

    /// [`fill_stream`](Db::fill_stream) with `between` run after each
    /// chunk of the fill, no lock held: for tests of a retention or a
    /// truncation landing in the middle of a fill.
    #[doc(hidden)]
    pub fn debug_fill_stream_with(&self, name: &str, mut between: impl FnMut()) -> Result<bool> {
        let def = self
            .inner
            .streams()
            .iter()
            .find(|d| d.name == name)
            .cloned()
            .ok_or_else(|| Error::NoStream(name.to_string()))?;
        stream::fill(&self.inner, &def, Some(&mut between))
    }

    /// Every index entry of the stream, below the floor included: what
    /// the table holds, not what a read serves. For tests of retention.
    #[doc(hidden)]
    pub fn debug_stream_entries(&self, name: &str) -> Result<Vec<u64>> {
        let def = self
            .inner
            .streams()
            .iter()
            .find(|d| d.name == name)
            .cloned()
            .ok_or_else(|| Error::NoStream(name.to_string()))?;
        self.inner.index.stream_entries(def.id)
    }

    /// Removes the stream and its index entries; whether it existed. A
    /// subscription on it ends with [`Error::NoStream`].
    pub fn delete_stream(&self, name: &str) -> Result<bool> {
        stream::delete(&self.inner, name)
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

    /// The members of the stream `name`, in global order from `from`, then
    /// new ones as they are committed. [`Error::NoStream`] without one,
    /// [`Error::StreamNotReady`] while its fill runs.
    pub fn subscribe_stream(&self, name: &str, from: u64) -> Result<Subscription> {
        let def = self.inner.resolve_stream(name)?;
        Ok(Subscription::stream(self.clone(), def, from))
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

    /// The members of the stream `name` from `from` to the head, a page at
    /// a time.
    pub fn iter_stream(&self, name: &str, from: u64) -> Result<EventIter> {
        let def = self.inner.resolve_stream(name)?;
        Ok(EventIter::new(self.clone(), Paged::Stream(def), from))
    }

    /// Starts a transaction. Nothing happens until `commit`.
    pub fn transaction(&self) -> Transaction {
        Transaction::new(self.clone())
    }

    /// Forces everything committed so far to disk: the segment bytes and
    /// a durable index commit (a checkpoint). Under [`Fsync::Always`] the
    /// bytes already are, so this only brings the index file up to date;
    /// also run when the last handle is dropped.
    pub fn flush(&self) -> Result<()> {
        self.inner.durable_index(|_, _| Ok(()))
    }

    /// A truncation stopped after its index commit and before its file
    /// work: the state a crash there leaves. For tests of recovery only;
    /// the `Db` must be dropped afterwards.
    #[doc(hidden)]
    pub fn debug_truncate_marker_only(&self, to: u64) -> Result<Truncated> {
        truncate::truncate_with(&self.inner, to, true)
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
        let (_db, appends, commit_id) = txn.into_parts();
        txn::commit(
            &self.inner,
            appends,
            commit_id,
            None,
            Mode::WriteWithoutCommit,
        )
    }

    /// Where the transaction committed under `id` landed, `(first, next)`,
    /// if one did and retention or truncation has not dropped it.
    pub fn lookup_commit(&self, id: [u8; 16]) -> Result<Option<(u64, u64)>> {
        self.inner.index.lookup_commit(&id)
    }

    /// The cursor named `name`, if one was stored (see
    /// [`set_cursor`](Db::set_cursor)).
    pub fn cursor(&self, name: &str) -> Result<Option<CursorState>> {
        Ok(self
            .inner
            .index
            .cursor(name)?
            .map(|(next, session)| CursorState {
                name: name.to_string(),
                next,
                session,
            }))
    }

    /// Stores a durable cursor: a consumer's position (`next`, the one
    /// after the last event it processed) and the session that owns it.
    /// Cursors live in the index: a daemon keeps and replicates them so a
    /// consumer resumes after a crash or a failover. The write is made
    /// durable by the next durable index commit or the close, which is
    /// enough since a lost cursor update means a redelivery, never a loss.
    /// Truncation clamps a cursor to the cut; retention leaves it (a
    /// consumer below the floor restarts at the floor); a rebuilt index
    /// keeps cursors; a full backup carries them and a restore keeps
    /// them, clamped to the archived head.
    pub fn set_cursor(&self, name: &str, next: u64, session: u64) -> Result<()> {
        self.inner
            .index
            .set_cursors(&[(name.to_string(), next, session)])
    }

    /// Removes a cursor; whether it existed.
    pub fn remove_cursor(&self, name: &str) -> Result<bool> {
        self.inner.index.remove_cursor(name)
    }

    /// Every cursor, by name.
    pub fn cursors(&self) -> Result<Vec<CursorState>> {
        Ok(self
            .inner
            .index
            .cursors()?
            .into_iter()
            .map(|(name, next, session)| CursorState {
                name,
                next,
                session,
            })
            .collect())
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

    /// Running totals of the commit path: batches, transactions, events,
    /// fsyncs and the time they took, index checkpoints. Concurrent
    /// commits land together, so `batches` is at most `transactions`.
    pub fn stats(&self) -> Stats {
        self.inner.stats()
    }

    /// Holds the writer lock until the returned value is dropped;
    /// commits from other threads queue behind it and land as one batch
    /// when it is released. For tests.
    #[doc(hidden)]
    pub fn debug_hold_writer(&self) -> WriterHold<'_> {
        WriterHold(self.inner.lock_writer())
    }

    /// How many segment files are open for reading.
    #[doc(hidden)]
    pub fn debug_open_segments(&self) -> usize {
        self.inner.open_segments()
    }

    /// How many transactions are queued for the writer lock's holder.
    #[doc(hidden)]
    pub fn debug_pending_commits(&self) -> usize {
        self.inner
            .commits
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .len()
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
    /// Streams defined, ready or not.
    pub streams: usize,
}

/// A read, usize::MAX when the caller asked for everything with 0.
fn all_if_zero(limit: usize) -> usize {
    if limit == 0 { usize::MAX } else { limit }
}

/// One page of a bounded read.
pub(crate) struct Page {
    pub(crate) events: Vec<Event>,
    /// Nothing more below the snapshot's end: neither bound cut the page.
    pub(crate) exhausted: bool,
}

/// Truncates `items` at the first element that would take the running
/// total of `len` past `max_bytes`, keeping at least one. Returns whether
/// anything was cut.
fn cut_by_bytes<T>(items: &mut Vec<T>, len: impl Fn(&T) -> u64, max_bytes: usize) -> bool {
    let max = max_bytes as u64;
    let mut total = 0u64;
    let mut keep = 0usize;
    for item in items.iter() {
        let l = len(item);
        if keep > 0 && total.saturating_add(l) > max {
            break;
        }
        total = total.saturating_add(l);
        keep += 1;
    }
    if keep < items.len() {
        items.truncate(keep);
        true
    } else {
        false
    }
}

enum Paged {
    All,
    Aggregate(Uuid),
    Tag(Bytes),
    Stream(Arc<StreamDef>),
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
        let max_bytes = self.db.inner.opts.page_max_bytes;
        let page = match &self.what {
            Paged::All => self.db.all_page(self.cursor, PAGE, max_bytes, u64::MAX)?,
            Paged::Aggregate(id) => {
                self.db
                    .aggregate_page(*id, self.cursor, PAGE, max_bytes, u64::MAX)?
            }
            Paged::Tag(tag) => self
                .db
                .tag_page(tag, self.cursor, PAGE, max_bytes, u64::MAX)?,
            Paged::Stream(def) => {
                self.db
                    .stream_page(def, self.cursor, PAGE, max_bytes, u64::MAX)?
            }
        };
        if page.exhausted {
            self.done = true;
        }
        if let Some(last) = page.events.last() {
            self.cursor = match &self.what {
                Paged::Aggregate(_) => last.index + 1,
                _ => last.position + 1,
            };
        }
        self.buffer.extend(page.events);
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
        if let Some(f) = files.get(base) {
            return Ok(f);
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

    /// How many segment files are open for reading right now.
    pub(crate) fn open_segments(&self) -> usize {
        self.files.lock().unwrap_or_else(|p| p.into_inner()).len()
    }

    /// The writer lock. Every holder releases it through [`WriterGuard`],
    /// which hands the lock to the next queued committer.
    pub(crate) fn lock_writer(&self) -> WriterGuard<'_> {
        WriterGuard {
            inner: self,
            w: Some(self.writer.lock().unwrap_or_else(|p| p.into_inner())),
        }
    }

    /// The writer lock if nobody holds it.
    pub(crate) fn try_lock_writer(&self) -> Option<WriterGuard<'_>> {
        let w = match self.writer.try_lock() {
            Ok(w) => w,
            Err(std::sync::TryLockError::Poisoned(p)) => p.into_inner(),
            Err(std::sync::TryLockError::WouldBlock) => return None,
        };
        Some(WriterGuard {
            inner: self,
            w: Some(w),
        })
    }

    /// The stream definitions in force, ready or not.
    pub(crate) fn streams(&self) -> Arc<Vec<Arc<StreamDef>>> {
        self.streams
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
    }

    /// Replaces the definitions with `f`'s edit of them. Called under the
    /// writer lock, so a batch sees either the old set or the new one.
    pub(crate) fn replace_streams(&self, f: impl FnOnce(&mut Vec<Arc<StreamDef>>)) {
        let mut guard = self.streams.write().unwrap_or_else(|p| p.into_inner());
        let mut v = (**guard).clone();
        f(&mut v);
        *guard = Arc::new(v);
    }

    /// Wakes stream subscribers after a definition change.
    pub(crate) fn notify_streams(&self) {
        #[cfg(feature = "tokio")]
        self.streams_tx.send_modify(|g| *g += 1);
    }

    /// Whether a stream with this id is still defined.
    pub(crate) fn stream_alive(&self, id: u32) -> bool {
        self.streams().iter().any(|d| d.id == id)
    }

    /// The ready stream `name`, for a read.
    pub(crate) fn resolve_stream(&self, name: &str) -> Result<Arc<StreamDef>> {
        let def = self
            .streams()
            .iter()
            .find(|d| d.name == name)
            .cloned()
            .ok_or_else(|| Error::NoStream(name.to_string()))?;
        if !def.is_ready() {
            return Err(Error::StreamNotReady(name.to_string()));
        }
        Ok(def)
    }

    /// A durable index write with the segment bytes synced first, so the
    /// index never vouches for data that is not on disk. Takes the writer
    /// lock; `f` gets the transaction and this `Inner`.
    pub(crate) fn durable_index<T>(
        &self,
        f: impl FnOnce(&Inner, &redb::WriteTransaction) -> Result<T>,
    ) -> Result<T> {
        let w = self.lock_writer();
        w.sync_data()?;
        let txn = self.index.begin_write_durable()?;
        let out = f(self, &txn)?;
        txn.commit()?;
        self.note_index_commit(true);
        drop(w);
        Ok(out)
    }

    /// Marks the next commit to fail after its bytes are written but
    /// before its index commit. For tests of the cleanup that follows.
    #[doc(hidden)]
    pub fn debug_fail_next_commit(&self) {
        self.fail_next_commit.store(true, Ordering::Release);
    }

    pub(crate) fn stats(&self) -> Stats {
        let c = &self.counters;
        Stats {
            batches: c.batches.load(Ordering::Relaxed),
            transactions: c.transactions.load(Ordering::Relaxed),
            events: c.events.load(Ordering::Relaxed),
            fsyncs: c.fsyncs.load(Ordering::Relaxed),
            fsync_nanos: c.fsync_nanos.load(Ordering::Relaxed),
            duplicates: c.duplicates.load(Ordering::Relaxed),
            index_checkpoints: c.index_checkpoints.load(Ordering::Relaxed),
            fsync_buckets: std::array::from_fn(|i| c.fsync_buckets[i].load(Ordering::Relaxed)),
            chunk_ring_hits: c.chunk_ring_hits.load(Ordering::Relaxed),
            chunk_ring_misses: c.chunk_ring_misses.load(Ordering::Relaxed),
        }
    }

    /// The durability of the next batch's index commit: durable when
    /// the checkpoint is due, under [`Fsync::Always`]; never under
    /// [`Fsync::Never`], where the close syncs. Called under the writer
    /// lock.
    pub(crate) fn checkpoint_durability(&self) -> redb::Durability {
        if self.opts.fsync == Fsync::Never {
            return redb::Durability::None;
        }
        let cp = self.checkpoint.lock().unwrap_or_else(|p| p.into_inner());
        if cp.batches_since + 1 >= self.opts.index_checkpoint_batches as u64
            || cp.last.elapsed() >= self.opts.index_checkpoint_interval
        {
            redb::Durability::Immediate
        } else {
            redb::Durability::None
        }
    }

    /// Records an index commit on the commit path: a durable one resets
    /// the checkpoint clock.
    pub(crate) fn note_index_commit(&self, durable: bool) {
        let mut cp = self.checkpoint.lock().unwrap_or_else(|p| p.into_inner());
        if durable {
            cp.batches_since = 0;
            cp.last = Instant::now();
            self.counters
                .index_checkpoints
                .fetch_add(1, Ordering::Relaxed);
        } else {
            cp.batches_since += 1;
        }
    }

    /// Accounts one segment sync of `nanos`.
    pub(crate) fn note_fsync(&self, nanos: u64) {
        self.counters.fsyncs.fetch_add(1, Ordering::Relaxed);
        self.counters
            .fsync_nanos
            .fetch_add(nanos, Ordering::Relaxed);
        let secs = nanos as f64 / 1e9;
        let bucket = FSYNC_BUCKET_BOUNDS
            .iter()
            .position(|b| secs <= *b)
            .unwrap_or(FSYNC_BUCKET_BOUNDS.len() - 1);
        self.counters.fsync_buckets[bucket].fetch_add(1, Ordering::Relaxed);
    }

    /// Drops cached handles on segments about to be removed.
    pub(crate) fn forget_files(&self, bases: &[u64]) {
        let mut files = self.files.lock().unwrap_or_else(|p| p.into_inner());
        for b in bases {
            files.remove(*b);
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
    pub(crate) fn roll(&self, w: &mut SegmentWriter, base: u64) {
        // The old segment is closed for good: cut any leftovers of a
        // failed commit and make it durable, whatever the fsync policy,
        // since nothing will sync it later. A new segment's header and
        // directory entry are synced for the same reason.
        if w.truncate_to_len().is_err() || w.sync_data().is_err() {
            return;
        }
        if let Ok(next) = SegmentWriter::create(&self.layout.segments_dir(), base, true) {
            *w = next;
        }
    }
}

/// Open segment files for reading, keyed by base, bounded in number: the
/// least recently used is closed when another is opened. Handles already
/// handed out stay valid.
struct FileCache {
    files: HashMap<u64, (Arc<File>, u64)>,
    tick: u64,
    cap: usize,
}

impl FileCache {
    fn new(cap: usize) -> Self {
        FileCache {
            files: HashMap::new(),
            tick: 0,
            cap,
        }
    }

    fn get(&mut self, base: u64) -> Option<Arc<File>> {
        self.tick += 1;
        let tick = self.tick;
        let (f, last) = self.files.get_mut(&base)?;
        *last = tick;
        Some(f.clone())
    }

    fn insert(&mut self, base: u64, f: Arc<File>) {
        while self.files.len() >= self.cap {
            let Some(oldest) = self
                .files
                .iter()
                .min_by_key(|(_, (_, t))| *t)
                .map(|(b, _)| *b)
            else {
                break;
            };
            self.files.remove(&oldest);
        }
        self.tick += 1;
        self.files.insert(base, (f, self.tick));
    }

    fn remove(&mut self, base: u64) {
        self.files.remove(&base);
    }

    fn len(&self) -> usize {
        self.files.len()
    }
}

/// Holds the writer lock. On release it nominates the first queued
/// committer, under the queue lock, before the mutex is freed: a
/// committer that joins the queue after that check finds the lock free
/// and takes it itself, so nobody waits for a leader that is not coming.
pub(crate) struct WriterGuard<'a> {
    inner: &'a Inner,
    w: Option<MutexGuard<'a, SegmentWriter>>,
}

impl std::ops::Deref for WriterGuard<'_> {
    type Target = SegmentWriter;

    fn deref(&self) -> &SegmentWriter {
        self.w.as_ref().expect("held until drop")
    }
}

impl std::ops::DerefMut for WriterGuard<'_> {
    fn deref_mut(&mut self) -> &mut SegmentWriter {
        self.w.as_mut().expect("held until drop")
    }
}

impl Drop for WriterGuard<'_> {
    fn drop(&mut self) {
        let queue = self.inner.commits.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(front) = queue.front() {
            front.nominate();
        }
        drop(self.w.take());
        drop(queue);
    }
}

/// Running totals of the commit path, read by [`Db::stats`].
#[derive(Default)]
pub(crate) struct Counters {
    pub(crate) batches: AtomicU64,
    pub(crate) transactions: AtomicU64,
    pub(crate) events: AtomicU64,
    pub(crate) fsyncs: AtomicU64,
    pub(crate) fsync_nanos: AtomicU64,
    pub(crate) duplicates: AtomicU64,
    pub(crate) index_checkpoints: AtomicU64,
    pub(crate) fsync_buckets: [AtomicU64; FSYNC_BUCKET_BOUNDS.len()],
    pub(crate) chunk_ring_hits: AtomicU64,
    pub(crate) chunk_ring_misses: AtomicU64,
}

/// Upper bounds, in seconds, of the buckets [`Stats::fsync_buckets`]
/// counts segment syncs into: from 100 µs to a second, the last bucket
/// taking everything above.
pub const FSYNC_BUCKET_BOUNDS: [f64; 12] = [
    1e-4, 2.5e-4, 5e-4, 1e-3, 2.5e-3, 5e-3, 1e-2, 2.5e-2, 5e-2, 0.1, 0.25, 1.0,
];

/// What the commit path has done since the database was opened.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub struct Stats {
    /// Index commits that landed transactions: one per batch. An applied
    /// backup or replication chunk lands as one batch (several only when
    /// it crosses the byte cap or the segment limit).
    pub batches: u64,
    /// Transactions landed through those batches.
    pub transactions: u64,
    /// Events landed through them.
    pub events: u64,
    /// Segment syncs on the commit path (one per batch under
    /// [`Fsync::Always`], none under [`Fsync::Never`]).
    pub fsyncs: u64,
    /// Time spent in those syncs, in nanoseconds.
    pub fsync_nanos: u64,
    /// Commits answered with an earlier landing because their commit id
    /// was seen before; nothing was written for them.
    pub duplicates: u64,
    /// Durable index commits: the checkpoints of the commit path plus
    /// every maintenance write. Each is one fsync of the index file.
    pub index_checkpoints: u64,
    /// How many segment syncs took at most each of
    /// [`FSYNC_BUCKET_BOUNDS`] (not cumulative; the last bucket counts
    /// the rest).
    pub fsync_buckets: [u64; FSYNC_BUCKET_BOUNDS.len()],
    /// Replication chunks cut from the in-memory ring of the last landed
    /// batches (no segment read).
    pub chunk_ring_hits: u64,
    /// Replication chunks that read the segment files.
    pub chunk_ring_misses: u64,
}

/// Holds the writer lock from a test, so that commits from other threads
/// queue up and land as one batch when it is dropped.
#[doc(hidden)]
pub struct WriterHold<'a>(#[allow(dead_code)] WriterGuard<'a>);

impl Drop for Inner {
    fn drop(&mut self) {
        // The closing checkpoint: the segment synced (already so under
        // Always), then a durable index commit, so the next open has
        // nothing to replay. (redb persists pending commits on a clean
        // close too; a crash is what leaves the index behind, and a copy
        // of the index file taken while open is the image it leaves.)
        let w = self.writer.get_mut().unwrap_or_else(|p| p.into_inner());
        if w.sync_data().is_err() {
            return;
        }
        if let Ok(txn) = self.index.begin_write_durable() {
            let _ = txn.commit();
        }
    }
}
