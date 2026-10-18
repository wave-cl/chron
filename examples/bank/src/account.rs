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

    pub fn apply(&mut self, ev: &Event) -> Result<(), BankError> {
        match AccountEvent::decode(ev)? {
            AccountEvent::Opened { owner } => self.owner = Some(owner),
            AccountEvent::Deposited { amount } => self.balance += amount,
            AccountEvent::Withdrawn { amount } => self.balance -= amount,
            AccountEvent::Closed => self.closed = true,
        }
        self.version += 1;
        Ok(())
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
}
