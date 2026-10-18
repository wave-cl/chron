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
//! one redb file. **The redb commit is the commit point**: the segment bytes
//! are written (and under [`Fsync::Always`] synced) first, then one redb
//! transaction records their positions and advances the head. On open,
//! anything in the segment files at or past the committed head was never
//! acknowledged and is truncated.

mod db;
mod error;
mod event;
mod index;
mod options;
mod rebuild;
mod recover;
mod retain;
mod segment;
mod subscribe;
mod txn;

pub use db::{Db, Info};
pub use error::{Error, Result};
pub use event::{
    Event, FLAG_FIRST_IN_TXN, FLAG_FREE, FLAG_LAST_IN_TXN, MAX_TAG_BYTES, MAX_TAGS_PER_EVENT,
    NewEvent,
};
pub use options::{Fsync, Options};
pub use rebuild::Rebuilt;
pub use retain::Retained;
pub use subscribe::Subscription;
pub use txn::{AppendResult, Committed, Expected, Transaction};
pub use uuid::Uuid;
