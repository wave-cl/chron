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
/// [`Iterator`] ends only when retention dropped the positions it would
/// deliver next (see [`Iterator::next`] below); use
/// [`next_timeout`](Subscription::next_timeout) from a thread that must
/// also watch a stop flag. A tag subscription from below the floor starts
/// at the floor, as [`Db::read_tag`] does.
pub struct Subscription {
    db: Db,
    filter: Filter,
    /// Global position of the next event to fetch from the index.
    cursor: u64,
    buffer: VecDeque<Event>,
    batch: usize,
    /// Set once a position below the floor was asked for: nothing more can
    /// be delivered in order.
    ended: bool,
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
            ended: false,
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

    /// One catch-up step: reads the next batch into the buffer. Returns
    /// how many events are buffered afterwards; 0 means the subscription
    /// is at the head. Blocking (a read); pair it with
    /// [`pop`](Subscription::pop) and, with the `tokio` feature,
    /// [`wait_async`](Subscription::wait_async) to drive a subscription
    /// from an async task without blocking it on reads.
    pub fn catch_up(&mut self) -> Result<usize> {
        if self.buffer.is_empty() {
            self.fill()?;
        }
        Ok(self.buffer.len())
    }

    /// The next buffered event, if any; see
    /// [`catch_up`](Subscription::catch_up).
    pub fn pop(&mut self) -> Option<Event> {
        self.buffer.pop_front()
    }

    /// Waits until the head moves past this subscription's cursor.
    /// Cancel-safe.
    #[cfg(feature = "tokio")]
    pub async fn wait_async(&mut self) {
        let min = self.cursor.saturating_add(1);
        let _ = self.head_rx.wait_for(|h| *h >= min).await;
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
            match self.filter {
                // Like `read_tag`: what survives, from the floor on.
                Filter::Tag(_) => self.cursor = floor,
                Filter::All => {
                    self.ended = true;
                    return Err(crate::Error::BelowFloor {
                        position: self.cursor,
                        floor,
                    });
                }
            }
        }
        let head = self.db.head();
        if self.cursor >= head {
            return Ok(());
        }
        let max_bytes = self.db.inner.opts.page_max_bytes;
        let page = match &self.filter {
            Filter::All => self.db.all_page(self.cursor, self.batch, max_bytes)?,
            Filter::Tag(tag) => self.db.tag_page(tag, self.cursor, self.batch, max_bytes)?,
        };
        if let Some(last) = page.events.last() {
            self.cursor = last.position + 1;
        }
        if page.exhausted {
            self.cursor = self.cursor.max(head);
        }
        self.buffer.extend(page.events);
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

    /// Blocks until an event is committed. Ends only after yielding
    /// [`Error::BelowFloor`](crate::Error::BelowFloor): the positions it
    /// would deliver next were dropped by retention, so in-order delivery
    /// is impossible. Any other error is yielded and the subscription
    /// stays usable.
    fn next(&mut self) -> Option<Self::Item> {
        if self.ended {
            return None;
        }
        loop {
            match self.next_until(None) {
                Ok(Some(ev)) => return Some(Ok(ev)),
                Ok(None) => continue,
                Err(e) => return Some(Err(e)),
            }
        }
    }
}

impl std::fmt::Debug for Subscription {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Subscription")
            .field("position", &self.position())
            .field("buffered", &self.buffer.len())
            .field(
                "tag",
                &match &self.filter {
                    Filter::All => None,
                    Filter::Tag(t) => Some(t),
                },
            )
            .finish()
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
