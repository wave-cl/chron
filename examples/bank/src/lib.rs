//! A bank of accounts on chron: each account is an aggregate rebuilt by
//! replaying its events, every command is guarded by the aggregate's
//! length, and a balances projection follows the `account` tag.
//!
//! The same code runs against an embedded [`chron::Db`] or a daemon
//! through [`chron_client::Client`]: see [`Store`].

pub mod account;
pub mod commands;
pub mod projection;
pub mod store;

pub use account::{AccountEvent, AccountState, BankError, TAG};
pub use projection::Balances;
pub use store::{AnyStore, Embedded, Store, StoreError};
