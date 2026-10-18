//! Commands: load the account, decide, append under `Expected::Exact`.

use chron::{Expected, Uuid};
use chron_client::Transaction;

use crate::account::{AccountEvent, AccountState, BankError};
use crate::store::{Store, StoreError};

/// Replays the account's events.
pub async fn load<S: Store>(store: &S, id: Uuid) -> Result<AccountState, BankError> {
    let mut state = AccountState::default();
    for ev in store.read_aggregate(id, 0, 0).await? {
        state.apply(&ev)?;
    }
    Ok(state)
}

pub async fn open<S: Store>(store: &S, owner: &str) -> Result<Uuid, BankError> {
    let id = Uuid::new_v4();
    let mut txn = Transaction::new();
    txn.append_one(
        id,
        Expected::New,
        AccountEvent::Opened {
            owner: owner.to_string(),
        }
        .encode(),
    );
    match store.commit(txn).await {
        Ok(_) => Ok(id),
        Err(StoreError::Conflict { .. }) => Err(BankError::AlreadyOpen),
        Err(e) => Err(e.into()),
    }
}

/// Loads, decides, appends with the length just seen; on a conflict,
/// loads and decides once more (the second decision may now refuse: that
/// is the guard), and gives up after a second conflict. The state
/// returned is the one the command decided on, with its own event
/// applied: no second read.
async fn execute<S: Store>(
    store: &S,
    id: Uuid,
    decide: impl Fn(&AccountState) -> Result<AccountEvent, BankError>,
) -> Result<AccountState, BankError> {
    let mut retried = false;
    loop {
        let mut state = load(store, id).await?;
        if !state.exists() {
            return Err(BankError::NoSuchAccount);
        }
        let event = decide(&state)?;
        let mut txn = Transaction::new();
        txn.append_one(id, Expected::Exact(state.version), event.encode());
        match store.commit(txn).await {
            Ok(c) => {
                state.apply_event(&event);
                debug_assert_eq!(c.appends[0].new_len, state.version);
                return Ok(state);
            }
            Err(StoreError::Conflict { .. }) if !retried => retried = true,
            Err(StoreError::Conflict { .. }) => return Err(BankError::Contended),
            Err(e) => return Err(e.into()),
        }
    }
}

fn positive(amount: i64) -> Result<(), BankError> {
    if amount <= 0 {
        Err(BankError::InvalidAmount)
    } else {
        Ok(())
    }
}

pub async fn deposit<S: Store>(
    store: &S,
    id: Uuid,
    amount: i64,
) -> Result<AccountState, BankError> {
    positive(amount)?;
    execute(store, id, |s| {
        if s.closed {
            return Err(BankError::Closed);
        }
        Ok(AccountEvent::Deposited { amount })
    })
    .await
}

pub async fn withdraw<S: Store>(
    store: &S,
    id: Uuid,
    amount: i64,
) -> Result<AccountState, BankError> {
    positive(amount)?;
    execute(store, id, |s| {
        if s.closed {
            return Err(BankError::Closed);
        }
        if s.balance < amount {
            return Err(BankError::InsufficientFunds {
                balance: s.balance,
                requested: amount,
            });
        }
        Ok(AccountEvent::Withdrawn { amount })
    })
    .await
}

pub async fn close<S: Store>(store: &S, id: Uuid) -> Result<AccountState, BankError> {
    execute(store, id, |s| {
        if s.closed {
            return Err(BankError::Closed);
        }
        Ok(AccountEvent::Closed)
    })
    .await
}
