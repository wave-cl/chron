//! The balances projection: every account's balance, fed by the
//! `account` tag and checkpointed by global position.

use std::collections::HashMap;

use chron::{Event, Uuid};
use futures::StreamExt;

use crate::account::{AccountEvent, BankError, TAG};
use crate::store::Store;

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Balances {
    pub balances: HashMap<Uuid, i64>,
    /// The global position to resume from.
    pub checkpoint: u64,
}

impl Balances {
    pub fn apply(&mut self, ev: &Event) -> Result<(), BankError> {
        let Some(id) = ev.aggregate else {
            self.checkpoint = ev.position + 1;
            return Ok(());
        };
        let entry = self.balances.entry(id).or_insert(0);
        match AccountEvent::decode(ev)? {
            AccountEvent::Opened { .. } => {}
            AccountEvent::Deposited { amount } => *entry += amount,
            AccountEvent::Withdrawn { amount } => *entry -= amount,
            AccountEvent::Closed => {}
        }
        self.checkpoint = ev.position + 1;
        Ok(())
    }

    /// Applies everything committed so far past the checkpoint.
    pub async fn catch_up<S: Store>(&mut self, store: &S) -> Result<(), BankError> {
        for ev in store.read_tag(TAG, self.checkpoint, 0).await? {
            self.apply(&ev)?;
        }
        Ok(())
    }

    /// Follows the tag live from the checkpoint, calling `on_change` after
    /// every event. Never returns on its own.
    pub async fn follow<S: Store>(
        &mut self,
        store: &S,
        mut on_change: impl FnMut(&Balances),
    ) -> Result<(), BankError> {
        let mut stream = store.subscribe_tag(TAG, self.checkpoint).await?;
        while let Some(item) = stream.next().await {
            self.apply(&item?)?;
            on_change(self);
        }
        Ok(())
    }
}
