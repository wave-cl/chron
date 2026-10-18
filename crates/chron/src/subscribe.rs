//! Live subscriptions: committed events in global order, then new ones as
//! they land.

use std::collections::VecDeque;
use std::time::{Duration, Instant};

use bytes::Bytes;

use crate::db::Db;
use crate::error::Result;
use crate::event::Event;

/// Events read from the index per catch-up step.
const DEFAULT_BATCH: usize = 256;
/// How long one blocking wait lasts before the loop re-arms it.
const WAIT_ROUND: Duration = Duration::from_secs(60);

enum Filter {
    All,
    Tag(Bytes),
}

/// Yields committed events from a position onward, in global order,
/// blocking for new ones once caught up. Created by [`Db::subscribe`] and
/// [`Db::subscribe_tag`].
///
/// A subscription holds the database open: `Inner` lives (and its `LOCK`
/// stays held) until every `Db` and every `Subscription` is dropped. The
/// [`Iterator`] never ends; use [`next_timeout`](Subscription::next_timeout)
/// from a thread that must also watch a stop flag.
pub struct Subscription {
    db: Db,
    filter: Filter,
    /// Global position of the next event to fetch from the index.
    cursor: u64,
    buffer: VecDeque<Event>,
    batch: usize,
    #[cfg(feature = "tokio")]
    head_rx: tokio::sync::watch::Receiver<u64>,
}

impl Subscription {
    pub(crate) fn all(db: Db, from: u64) -> Self {
        Self::new(db, Filter::All, from)
    }

    pub(crate) fn tag(db: Db, tag: Bytes, from: u64) -> Self {
        Self::new(db, Filter::Tag(tag), from)
    }

    fn new(db: Db, filter: Filter, from: u64) -> Self {
        #[cfg(feature = "tokio")]
        let head_rx = db.inner.head_tx.subscribe();
        Subscription {
            db,
            filter,
            cursor: from,
            buffer: VecDeque::new(),
            batch: DEFAULT_BATCH,
            #[cfg(feature = "tokio")]
            head_rx,
        }
    }

    /// Events read from the index per catch-up step (default 256; 0 reads
    /// as 1).
    pub fn batch(mut self, n: usize) -> Self {
        self.batch = n.max(1);
        self
    }

    /// Global position of the next event this subscription would deliver.
    /// Resuming later with `subscribe(position())` (or `subscribe_tag`)
    /// skips and repeats nothing.
    pub fn position(&self) -> u64 {
        self.buffer.front().map_or(self.cursor, |e| e.position)
    }

    /// The next event, or `None` if none is committed before `timeout`
    /// passes.
    pub fn next_timeout(&mut self, timeout: Duration) -> Result<Option<Event>> {
        self.next_until(Instant::now().checked_add(timeout))
    }

    /// The next event, awaiting a commit once caught up. Never ends: the
    /// database lives as long as this subscription. Cancel-safe at its one
    /// await, so `tokio::time::timeout(d, sub.next_async())` loses nothing
    /// when it elapses. The catch-up reads run inline on the calling task;
    /// lower [`batch`](Subscription::batch) when records are very large.
    #[cfg(feature = "tokio")]
    pub async fn next_async(&mut self) -> Result<Event> {
        loop {
            if let Some(ev) = self.buffer.pop_front() {
                return Ok(ev);
            }
            self.fill()?;
            if !self.buffer.is_empty() {
                continue;
            }
            let min = self.cursor.saturating_add(1);
            // Err only if the sender is gone, which `self.db` prevents.
            let _ = self.head_rx.wait_for(|h| *h >= min).await;
        }
    }

    /// One catch-up step: reads the next batch into the buffer and moves
    /// the cursor past what it covered.
    fn fill(&mut self) -> Result<()> {
        // The head is read before the index: the read snapshot then holds
        // every position below it, so a short batch proves there is nothing
        // more to deliver below `head`.
        let floor = self.db.floor();
        if self.cursor < floor {
            return Err(crate::Error::BelowFloor {
                position: self.cursor,
                floor,
            });
        }
        let head = self.db.head();
        if self.cursor >= head {
            return Ok(());
        }
        let got = match &self.filter {
            Filter::All => self.db.read_all(self.cursor, self.batch)?,
            Filter::Tag(tag) => self.db.read_tag(tag, self.cursor, self.batch)?,
        };
        if let Some(last) = got.last() {
            self.cursor = last.position + 1;
        }
        if got.len() < self.batch {
            self.cursor = self.cursor.max(head);
        }
        self.buffer.extend(got);
        Ok(())
    }

    fn next_until(&mut self, deadline: Option<Instant>) -> Result<Option<Event>> {
        loop {
            if let Some(ev) = self.buffer.pop_front() {
                return Ok(Some(ev));
            }
            self.fill()?;
            if !self.buffer.is_empty() {
                continue;
            }
            let wait = match deadline {
                None => WAIT_ROUND,
                Some(d) => {
                    let now = Instant::now();
                    if now >= d {
                        return Ok(None);
                    }
                    (d - now).min(WAIT_ROUND)
                }
            };
            self.db.wait_for(self.cursor.saturating_add(1), wait);
        }
    }
}

impl Iterator for Subscription {
    type Item = Result<Event>;

    /// Blocks until an event is committed. Never returns `None`; an error
    /// is yielded and the subscription stays usable.
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.next_until(None) {
                Ok(Some(ev)) => return Some(Ok(ev)),
                Ok(None) => continue,
                Err(e) => return Some(Err(e)),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subscription_is_send_and_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<Subscription>();
    }
}
