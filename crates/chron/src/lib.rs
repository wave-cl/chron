//! `chron`: an embeddable event-sourcing database.
//!
//! Events live in append-only logs called **aggregates**, each keyed by a
//! [`Uuid`]. A [`Transaction`] appends to any number of aggregates
//! atomically, each append stating what length it [`Expected`] the aggregate
//! to have. Every event also takes a dense position in one global order, and
//! carries any number of **tags**; reading a tag yields its events in global
//! order. An event may also belong to no aggregate at all (a **free**
//! event, appended with [`Transaction::append_free`]); it still takes a
//! global position and tags. A [`Subscription`] follows the global order,
//! or one tag's slice of it, live.
//!
//! On disk, event bytes go to append-only segment files and every index to
//! one redb file. **The segment is the source of truth once synced; the
//! redb commit is the commit point in memory.** A batch's bytes are
//! written (and under [`Fsync::Always`] synced) first, then one redb
//! transaction records their positions and advances the head; that
//! transaction reaches the disk every [`Options::index_checkpoint_batches`]
//! batches or [`Options::index_checkpoint_interval`], on close, and on
//! every maintenance write. On open, complete transactions the segments
//! hold past the durable index are replayed into it and a torn trailing
//! transaction is cut; a transaction that was written and synced but
//! whose result its committer never received is therefore kept, as
//! [`Db::rebuild_index`] has always done.

#![deny(missing_docs)]

mod backup;
mod db;
mod error;
mod event;
mod index;
mod options;
mod rebuild;
mod recover;
mod replicate;
mod retain;
mod segment;
mod subscribe;
mod truncate;
mod txn;

pub use backup::{BackupKind, BackupMeta, inspect_backup, restore_backup, restore_backup_from};
pub use bytes::Bytes;
pub use db::{Db, EventIter, FSYNC_BUCKET_BOUNDS, Info, Stats, WriterHold};
pub use error::{Error, Result};
pub use event::{
    Event, FLAG_COMMIT_ID, FLAG_FIRST_IN_TXN, FLAG_FREE, FLAG_LAST_IN_TXN, MAX_TAG_BYTES,
    MAX_TAGS_PER_EVENT, NewEvent,
};
pub use options::{Fsync, Options};
pub use rebuild::Rebuilt;
pub use replicate::Chunk;
pub use retain::Retained;
pub use subscribe::Subscription;
pub use truncate::Truncated;
pub use txn::{AppendResult, Committed, Expected, Transaction};
pub use uuid::Uuid;
