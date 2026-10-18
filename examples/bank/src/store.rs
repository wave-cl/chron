//! Where events live: an embedded database or a daemon, behind one trait.

use std::future::Future;
use std::pin::Pin;

use chron::{Committed, Event, Uuid};
use chron_client::{Client, ClientError, Transaction};
use futures::Stream;

pub type EventStream = Pin<Box<dyn Stream<Item = Result<Event, StoreError>> + Send>>;

#[derive(thiserror::Error, Debug)]
pub enum StoreError {
    /// An expectation failed: the aggregate has `actual` events.
    #[error("aggregate {aggregate} has {actual} events")]
    Conflict { aggregate: Uuid, actual: u64 },
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

impl From<chron::Error> for StoreError {
    fn from(e: chron::Error) -> Self {
        match e {
            chron::Error::ExpectedLength {
                aggregate, actual, ..
            } => StoreError::Conflict { aggregate, actual },
            e => StoreError::Other(e.into()),
        }
    }
}

impl From<ClientError> for StoreError {
    fn from(e: ClientError) -> Self {
        match e {
            ClientError::Conflict {
                aggregate, actual, ..
            } => StoreError::Conflict { aggregate, actual },
            e => StoreError::Other(e.into()),
        }
    }
}

/// The five calls the bank needs. Both implementations are async; the
/// embedded one runs the blocking library calls on the blocking pool.
pub trait Store: Clone + Send + Sync + 'static {
    fn commit(
        &self,
        txn: Transaction,
    ) -> impl Future<Output = Result<Committed, StoreError>> + Send;
    fn read_aggregate(
        &self,
        id: Uuid,
        from: u64,
        limit: u64,
    ) -> impl Future<Output = Result<Vec<Event>, StoreError>> + Send;
    fn read_tag(
        &self,
        tag: &str,
        from: u64,
        limit: u64,
    ) -> impl Future<Output = Result<Vec<Event>, StoreError>> + Send;
    fn head(&self) -> impl Future<Output = Result<u64, StoreError>> + Send;
    fn subscribe_tag(
        &self,
        tag: &str,
        from: u64,
    ) -> impl Future<Output = Result<EventStream, StoreError>> + Send;
}

/// An embedded database.
#[derive(Clone)]
pub struct Embedded(pub chron::Db);

async fn blocking<T: Send + 'static>(
    f: impl FnOnce() -> Result<T, StoreError> + Send + 'static,
) -> Result<T, StoreError> {
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| StoreError::Other(anyhow::anyhow!("task: {e}")))?
}

impl Store for Embedded {
    async fn commit(&self, txn: Transaction) -> Result<Committed, StoreError> {
        let db = self.0.clone();
        blocking(move || {
            let mut t = db.transaction();
            for a in txn.into_appends() {
                match a.aggregate {
                    Some(id) => t.append(id, a.expected, a.events),
                    None => t.append_free(a.events),
                };
            }
            Ok(t.commit()?)
        })
        .await
    }

    async fn read_aggregate(
        &self,
        id: Uuid,
        from: u64,
        limit: u64,
    ) -> Result<Vec<Event>, StoreError> {
        let db = self.0.clone();
        let n = if limit == 0 {
            usize::MAX
        } else {
            limit as usize
        };
        blocking(move || Ok(db.read_aggregate(id, from, n)?)).await
    }

    async fn read_tag(&self, tag: &str, from: u64, limit: u64) -> Result<Vec<Event>, StoreError> {
        let db = self.0.clone();
        let tag = tag.to_string();
        let n = if limit == 0 {
            usize::MAX
        } else {
            limit as usize
        };
        blocking(move || Ok(db.read_tag(tag, from, n)?)).await
    }

    async fn head(&self) -> Result<u64, StoreError> {
        Ok(self.0.head())
    }

    async fn subscribe_tag(&self, tag: &str, from: u64) -> Result<EventStream, StoreError> {
        let sub = self.0.subscribe_tag(tag, from);
        Ok(Box::pin(futures::stream::unfold(
            sub,
            |mut sub| async move {
                let item = sub.next_async().await.map_err(StoreError::from);
                Some((item, sub))
            },
        )))
    }
}

impl Store for Client {
    async fn commit(&self, txn: Transaction) -> Result<Committed, StoreError> {
        Ok(Client::commit(self, txn).await?)
    }

    async fn read_aggregate(
        &self,
        id: Uuid,
        from: u64,
        limit: u64,
    ) -> Result<Vec<Event>, StoreError> {
        Ok(Client::read_aggregate(self, id, from, limit as usize).await?)
    }

    async fn read_tag(&self, tag: &str, from: u64, limit: u64) -> Result<Vec<Event>, StoreError> {
        Ok(Client::read_tag(self, tag.to_string(), from, limit as usize).await?)
    }

    async fn head(&self) -> Result<u64, StoreError> {
        Ok(Client::head(self).await?)
    }

    async fn subscribe_tag(&self, tag: &str, from: u64) -> Result<EventStream, StoreError> {
        use futures::StreamExt;
        let stream = Client::subscribe_tag(self, tag.to_string(), from).await?;
        Ok(Box::pin(stream.map(|r| r.map_err(StoreError::from))))
    }
}

/// Whichever the binary was pointed at.
#[derive(Clone)]
pub enum AnyStore {
    Embedded(Embedded),
    Remote(Box<Client>),
}
