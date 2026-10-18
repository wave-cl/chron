use chron::{Expected, NewEvent, Uuid};

/// One append of a transaction. `aggregate` `None` is a free append.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Append {
    /// The aggregate, or `None` for events that belong to none.
    pub aggregate: Option<Uuid>,
    /// What the append asserts about the aggregate's length.
    pub expected: Expected,
    /// The events, in order.
    pub events: Vec<NewEvent>,
}

/// Appends to any number of aggregates, committed atomically by
/// [`Client::commit`](crate::Client::commit). Plain data: build it
/// anywhere, send it once.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Transaction {
    appends: Vec<Append>,
}

impl Transaction {
    /// An empty transaction (which the daemon refuses to commit).
    pub fn new() -> Self {
        Self::default()
    }

    /// Appends `events` to `aggregate`, asserting `expected` about its
    /// length before them.
    pub fn append(
        &mut self,
        aggregate: Uuid,
        expected: Expected,
        events: Vec<NewEvent>,
    ) -> &mut Self {
        self.appends.push(Append {
            aggregate: Some(aggregate),
            expected,
            events,
        });
        self
    }

    /// [`append`](Transaction::append) with one event.
    pub fn append_one(
        &mut self,
        aggregate: Uuid,
        expected: Expected,
        event: NewEvent,
    ) -> &mut Self {
        self.append(aggregate, expected, vec![event])
    }

    /// Events that belong to no aggregate.
    pub fn append_free(&mut self, events: Vec<NewEvent>) -> &mut Self {
        self.appends.push(Append {
            aggregate: None,
            expected: Expected::Any,
            events,
        });
        self
    }

    /// Whether nothing has been appended, free appends included.
    pub fn is_empty(&self) -> bool {
        self.appends.is_empty()
    }

    /// The appends in order.
    pub fn appends(&self) -> &[Append] {
        &self.appends
    }

    /// The appends, consuming the transaction.
    pub fn into_appends(self) -> Vec<Append> {
        self.appends
    }
}
