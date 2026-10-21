//! The balances projection: every open account's balance, fed by the
//! `account` tag and checkpointed by global position. It persists itself
//! to a small JSON file, so `balances` resumes from where it stopped
//! instead of replaying everything.

use std::collections::HashMap;
use std::path::Path;

use chron::{Event, Uuid};
use futures::StreamExt;
use serde::{Deserialize, Serialize};

use crate::account::{AccountEvent, BankError, TAG};
use crate::store::{Store, StoreError};

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Balances {
    pub balances: HashMap<Uuid, i64>,
    /// The global position to resume from.
    pub checkpoint: u64,
}

impl Balances {
    /// Applies one event. A closed account leaves the map; an event that
    /// does not decode changes nothing, not even the checkpoint.
    pub fn apply(&mut self, ev: &Event) -> Result<(), BankError> {
        // An event already applied (delivered again after a crash between
        // a save and its acknowledgment) changes nothing.
        if ev.position < self.checkpoint {
            return Ok(());
        }
        if let Some(id) = ev.aggregate {
            let event = AccountEvent::decode(ev)?;
            match event {
                AccountEvent::Opened { .. } => {
                    self.balances.entry(id).or_insert(0);
                }
                AccountEvent::Deposited { amount } => {
                    *self.balances.entry(id).or_insert(0) += amount;
                }
                AccountEvent::Withdrawn { amount } => {
                    *self.balances.entry(id).or_insert(0) -= amount;
                }
                AccountEvent::Closed => {
                    self.balances.remove(&id);
                }
            }
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
    /// every event. Returns only when the stream ends (the daemon fenced
    /// or stopped).
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

    /// Follows the tag through the store's durable cursor `name` (see
    /// `Store::subscribe_cursor`), acknowledging every event once applied,
    /// so a restart resumes where this run stopped without a file. On a
    /// store without cursors it is `follow` from the checkpoint.
    pub async fn follow_durable<S: Store>(
        &mut self,
        store: &S,
        name: &str,
        mut on_change: impl FnMut(&Balances),
    ) -> Result<(), BankError> {
        let (mut stream, cursor) = store.subscribe_cursor(name, TAG, self.checkpoint).await?;
        if let Some(c) = &cursor {
            // The cursor's position is the checkpoint; what lies between
            // the file's and it was processed by an earlier run.
            self.checkpoint = self.checkpoint.max(c.from);
        }
        while let Some(item) = stream.next().await {
            let ev = item?;
            self.apply(&ev)?;
            if let Some(c) = &cursor {
                c.ack(ev.position + 1).await.map_err(StoreError::from)?;
            }
            on_change(self);
        }
        Ok(())
    }

    /// The projection saved at `path`, or an empty one when there is none.
    pub fn load(path: &Path) -> Result<Balances, BankError> {
        match std::fs::read(path) {
            Ok(bytes) => Ok(serde_json::from_slice(&bytes)?),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Balances::default()),
            Err(e) => Err(BankError::Checkpoint(e)),
        }
    }

    /// Saves the projection atomically (write beside, then rename).
    pub fn save(&self, path: &Path) -> Result<(), BankError> {
        let tmp = path.with_extension("json.tmp");
        std::fs::write(&tmp, serde_json::to_vec_pretty(self)?).map_err(BankError::Checkpoint)?;
        std::fs::rename(&tmp, path).map_err(BankError::Checkpoint)
    }
}
