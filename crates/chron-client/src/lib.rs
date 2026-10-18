//! A typed client for a `chrond` daemon. It speaks the library's
//! vocabulary ([`Event`], [`NewEvent`], [`Expected`], [`Committed`], …),
//! so code written against an embedded [`chron::Db`] moves to a daemon by
//! swapping the handle.
//!
//! ```no_run
//! # async fn demo() -> Result<(), chron_client::ClientError> {
//! use chron_client::{Client, ClientConfig, Expected, NewEvent, Transaction, Uuid};
//!
//! let client = Client::connect("http://127.0.0.1:4242", ClientConfig::default()).await?;
//! let order = Uuid::new_v4();
//! let mut txn = Transaction::new();
//! txn.append_one(order, Expected::New, NewEvent::new("placed").tag("orders"));
//! let committed = client.commit(txn).await?;
//! let events = client.read_aggregate(order, 0, 0, false).await?;
//! assert_eq!(events.len(), 1);
//! assert_eq!(committed.first, events[0].position);
//! # Ok(()) }
//! ```

mod client;
mod error;
mod txn;

pub use chron::{
    AppendResult, BackupMeta, Committed, Event, Expected, NewEvent, Options, Retained, Uuid,
};
pub use client::{Client, ClientConfig, EventStream, Info, Role, TlsConfig};
pub use error::ClientError;
pub use txn::{Append, Transaction};
