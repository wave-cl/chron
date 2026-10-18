//! The database handle: open, read, and the shared state the writer uses.

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions, TryLockError};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use uuid::Uuid;

use crate::error::{Error, Result};
use crate::event::{self, Event};
use crate::index::{Index, Location};
use crate::options::{Fsync, Options};
use crate::recover;
use crate::segment::{self, SegmentWriter};
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

/// Exclusive hold on the directory for as long as the `Db` lives.
struct Lock {
    _file: File,
}

impl Lock {
    fn acquire(path: &Path) -> Result<Self> {
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
    head_watch: (Mutex<u64>, Condvar),
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
        let (index, head, writer) = if index_file.exists() {
            let r = recover::recover(&layout, &opts)?;
            let writer = SegmentWriter::open(&r.tail_path, r.tail_base, r.tail_len)?;
            (r.index, r.head, writer)
        } else {
            if let Some((_, stray)) = segment::list(&segments_dir)?.first() {
                return Err(Error::corrupt(
                    stray,
                    0,
                    "segment files without an index; rebuilding the index is not supported",
                ));
            }
            let index = Index::create(&index_file, opts.fsync)?;
            let writer = SegmentWriter::create(&segments_dir, 0, sync)?;
            if sync {
                segment::sync_dir(layout.root())?;
            }
            (index, 0, writer)
        };

        Ok(Db {
            inner: Arc::new(Inner {
                layout,
                opts,
                index,
                writer: Mutex::new(writer),
                head: AtomicU64::new(head),
                head_watch: (Mutex::new(head), Condvar::new()),
                files: Mutex::new(HashMap::new()),
                _lock: lock,
            }),
        })
    }

    pub fn path(&self) -> &Path {
        self.inner.layout.root()
    }

    /// The next global position: one past the last committed event.
    pub fn head(&self) -> u64 {
        self.inner.head.load(Ordering::Acquire)
    }

    /// How many events the aggregate has; 0 for one never written.
    pub fn aggregate_len(&self, id: Uuid) -> Result<u64> {
        self.inner.index.aggregate_len(&id)
    }

    /// The aggregate's events from `from_index`, at most `limit`, in index
    /// order. Empty for an unknown aggregate or a `from_index` at or past
    /// its length.
    pub fn read_aggregate(&self, id: Uuid, from_index: u64, limit: usize) -> Result<Vec<Event>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
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
        if limit == 0 {
            return Ok(Vec::new());
        }
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
        let head = self.head();
        if from > head {
            return Err(Error::PositionOutOfRange {
                position: from,
                head,
            });
        }
        if limit == 0 || from == head {
            return Ok(Vec::new());
        }
        let txn = self.inner.index.begin_read()?;
        let found = self.inner.index.positions_from(&txn, from, limit)?;
        found
            .into_iter()
            .map(|(position, loc)| self.read_located(position, Some(loc)))
            .collect()
    }

    /// Events at positions `<= from`, newest first, at most `limit`. `from`
    /// is clamped to the newest event, so `u64::MAX` means "from the end";
    /// empty when there are no events.
    pub fn read_all_backward(&self, from: u64, limit: usize) -> Result<Vec<Event>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let txn = self.inner.index.begin_read()?;
        let found = self.inner.index.positions_to(&txn, from, limit)?;
        found
            .into_iter()
            .map(|(position, loc)| self.read_located(position, Some(loc)))
            .collect()
    }

    /// Every event carrying `tag` from global position `from`, at most
    /// `limit`, in global order.
    pub fn read_tag(&self, tag: impl AsRef<[u8]>, from: u64, limit: usize) -> Result<Vec<Event>> {
        let tag = tag.as_ref();
        if limit == 0 {
            return Ok(Vec::new());
        }
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
        if limit == 0 {
            return Ok(Vec::new());
        }
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

    /// Starts a transaction. Nothing happens until `commit`.
    pub fn transaction(&self) -> Transaction {
        Transaction::new(self.clone())
    }

    /// Forces everything committed so far to disk. Only matters under
    /// [`Fsync::Never`]; also run when the last handle is dropped.
    pub fn flush(&self) -> Result<()> {
        let w = self.inner.writer.lock().unwrap_or_else(|p| p.into_inner());
        w.sync_data()?;
        self.inner.index.begin_write_durable()?.commit()?;
        drop(w);
        Ok(())
    }

    /// Blocks until the head is at least `min_head` or `timeout` passes.
    /// Returns the head seen last.
    pub fn wait_for(&self, min_head: u64, timeout: Duration) -> u64 {
        let deadline = Instant::now() + timeout;
        let (lock, cv) = &self.inner.head_watch;
        let mut head = lock.lock().unwrap_or_else(|p| p.into_inner());
        while *head < min_head {
            let now = Instant::now();
            if now >= deadline {
                break;
            }
            head = cv
                .wait_timeout(head, deadline - now)
                .unwrap_or_else(|p| p.into_inner())
                .0;
        }
        *head
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
        Error::corrupt(
            self.inner.layout.index_file(),
            position,
            format!("position {position}: {}", reason.into()),
        )
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
        let ev = event::decode_body(&body)
            .map_err(|e| Error::corrupt(&path, loc.offset, e.to_string()))?;
        if ev.position != position {
            return Err(Error::corrupt(
                &path,
                loc.offset,
                format!(
                    "index points position {position} at a record carrying {}",
                    ev.position
                ),
            ));
        }
        Ok(ev)
    }
}

impl Inner {
    /// A shared read handle on a segment. Never seek on it: readers use
    /// positional reads only.
    fn file(&self, base: u64) -> Result<Arc<File>> {
        let mut files = self.files.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(f) = files.get(&base) {
            return Ok(f.clone());
        }
        let path = self.layout.segment(base);
        let f = Arc::new(File::open(&path).map_err(|e| Error::io(&path, "open", e))?);
        files.insert(base, f.clone());
        Ok(f)
    }

    pub(crate) fn publish_head(&self, head: u64) {
        self.head.store(head, Ordering::Release);
        let (lock, cv) = &self.head_watch;
        *lock.lock().unwrap_or_else(|p| p.into_inner()) = head;
        cv.notify_all();
    }

    /// Starts a new segment at `base`. The commit that filled the old one
    /// already succeeded, so a failure here just keeps writing to it.
    pub(crate) fn roll(&self, w: &mut MutexGuard<'_, SegmentWriter>, base: u64) {
        let sync = self.opts.fsync == Fsync::Always;
        if let Ok(next) = SegmentWriter::create(&self.layout.segments_dir(), base, sync) {
            **w = next;
        }
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        if self.opts.fsync == Fsync::Never {
            if let Ok(w) = self.writer.get_mut() {
                let _ = w.sync_data();
            }
            if let Ok(txn) = self.index.begin_write_durable() {
                let _ = txn.commit();
            }
        }
    }
}
