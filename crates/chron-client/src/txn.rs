use chron::{Expected, NewEvent, Uuid};

/// One append of a transaction. `aggregate` `None` is a free append.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Append {
    pub aggregate: Option<Uuid>,
    pub expected: Expected,
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
    pub fn new() -> Self {
        Self::default()
    }

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

    pub fn is_empty(&self) -> bool {
        self.appends.is_empty()
    }

    pub fn appends(&self) -> &[Append] {
        &self.appends
    }

    pub fn into_appends(self) -> Vec<Append> {
        self.appends
    }
}
