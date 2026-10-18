/// When segment writes and index commits reach the disk.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum Fsync {
    /// Every commit fdatasyncs the segment and commits the index durably
    /// before returning. A committed transaction survives power loss.
    #[default]
    Always,
    /// Nothing is synced on commit. A committed transaction survives a
    /// process crash but not power loss; [`crate::Db::flush`] (also run on
    /// drop) syncs everything committed so far.
    Never,
}

/// How a [`crate::Db`] is opened.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct Options {
    /// A new segment file starts after the commit that makes the current
    /// one reach this size. A transaction is never split across segments, so
    /// a segment may exceed this by up to one transaction.
    pub segment_max_bytes: u64,
    /// Largest frame plus body of one event.
    pub max_record_bytes: usize,
    /// When commits reach the disk.
    pub fsync: Fsync,
    /// Read every segment on open, checking each record's crc, that
    /// positions run consecutively across segments, and that the index
    /// places every record where it is. Costs a full read of the data;
    /// off by default, when only the tail is checked.
    pub verify_all_segments: bool,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            segment_max_bytes: 256 * 1024 * 1024,
            max_record_bytes: 16 * 1024 * 1024,
            fsync: Fsync::Always,
            verify_all_segments: false,
        }
    }
}

impl Options {
    /// The defaults: 256 MiB segments, 16 MiB records, [`Fsync::Always`],
    /// no verification on open.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the segment size at which the writer rolls.
    pub fn segment_max_bytes(mut self, bytes: u64) -> Self {
        self.segment_max_bytes = bytes;
        self
    }

    /// Sets the largest frame plus body of one event.
    pub fn max_record_bytes(mut self, bytes: usize) -> Self {
        self.max_record_bytes = bytes;
        self
    }

    /// Sets when commits reach the disk.
    pub fn fsync(mut self, fsync: Fsync) -> Self {
        self.fsync = fsync;
        self
    }

    /// Sets whether every segment is read and checked on open.
    pub fn verify_all_segments(mut self, verify: bool) -> Self {
        self.verify_all_segments = verify;
        self
    }
}
