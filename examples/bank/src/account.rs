use chron::{Event, NewEvent};
use serde::{Deserialize, Serialize};

/// Every account event carries this tag, so one subscription feeds the
/// balances projection.
pub const TAG: &str = "account";

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AccountEvent {
    Opened { owner: String },
    Deposited { amount: i64 },
    Withdrawn { amount: i64 },
    Closed,
}

impl AccountEvent {
    pub fn encode(&self) -> NewEvent {
        NewEvent::new(serde_json::to_vec(self).expect("serialisable")).tag(TAG)
    }

    pub fn decode(ev: &Event) -> Result<AccountEvent, BankError> {
        Ok(serde_json::from_slice(&ev.payload)?)
    }
}

/// What replaying an account's events gives.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct AccountState {
    pub owner: Option<String>,
    pub balance: i64,
    pub closed: bool,
    /// Events applied: the length to expect when appending.
    pub version: u64,
}

impl AccountState {
    pub fn exists(&self) -> bool {
        self.owner.is_some()
    }

    /// Applies a recorded event; the version follows the event's index,
    /// so a replay that skipped nothing ends at the aggregate's length.
    pub fn apply(&mut self, ev: &Event) -> Result<(), BankError> {
        self.apply_event(&AccountEvent::decode(ev)?);
        self.version = ev.index + 1;
        Ok(())
    }

    /// Applies an event this process decided, right after it committed at
    /// the version the state holds.
    pub fn apply_event(&mut self, ev: &AccountEvent) {
        match ev {
            AccountEvent::Opened { owner } => self.owner = Some(owner.clone()),
            AccountEvent::Deposited { amount } => self.balance += amount,
            AccountEvent::Withdrawn { amount } => self.balance -= amount,
            AccountEvent::Closed => self.closed = true,
        }
        self.version += 1;
    }
}

#[derive(thiserror::Error, Debug)]
pub enum BankError {
    #[error("no such account")]
    NoSuchAccount,
    #[error("the account is already open")]
    AlreadyOpen,
    #[error("the account is closed")]
    Closed,
    #[error("insufficient funds: balance {balance}, requested {requested}")]
    InsufficientFunds { balance: i64, requested: i64 },
    #[error("amounts must be positive")]
    InvalidAmount,
    #[error("the account changed under two attempts; try again")]
    Contended,
    #[error(transparent)]
    Store(#[from] crate::store::StoreError),
    #[error("bad event payload: {0}")]
    Json(#[from] serde_json::Error),
    #[error("cannot read or write the projection file: {0}")]
    Checkpoint(std::io::Error),
}
