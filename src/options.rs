/// When segment writes and index commits reach the disk.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
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
pub struct Options {
    /// A new segment file starts after the commit that makes the current
    /// one reach this size. A transaction is never split across segments, so
    /// a segment may exceed this by up to one transaction.
    pub segment_max_bytes: u64,
    /// Largest frame plus body of one event.
    pub max_record_bytes: usize,
    pub fsync: Fsync,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            segment_max_bytes: 256 * 1024 * 1024,
            max_record_bytes: 16 * 1024 * 1024,
            fsync: Fsync::Always,
        }
    }
}

impl Options {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn segment_max_bytes(mut self, bytes: u64) -> Self {
        self.segment_max_bytes = bytes;
        self
    }

    pub fn max_record_bytes(mut self, bytes: usize) -> Self {
        self.max_record_bytes = bytes;
        self
    }

    pub fn fsync(mut self, fsync: Fsync) -> Self {
        self.fsync = fsync;
        self
    }
}
