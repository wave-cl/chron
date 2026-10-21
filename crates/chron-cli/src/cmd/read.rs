use std::time::Duration;

use chron_client::{EventStream, ReadOptions};
use clap::Args;
use tokio_stream::StreamExt;

use crate::client::{self, Conn};
use crate::cmd::aggregate_id;
use crate::output::{Format, print_event};

#[derive(Args, Debug)]
pub struct Range {
    /// First index or position; backward, the inclusive upper bound
    /// (default: the end).
    #[arg(long)]
    from: Option<u64>,
    /// At most this many events; 0 for all.
    #[arg(long, default_value_t = 0)]
    limit: u64,
    /// Newest first.
    #[arg(long)]
    backward: bool,
    /// Wait until the node's bound (under --local/--confirmed) reaches
    /// this position before reading: a commit's `next`, to read your
    /// own write.
    #[arg(long)]
    at_least: Option<u64>,
    /// Seconds --at-least may wait (default: the daemon's sync timeout;
    /// 0 fails at once).
    #[arg(long, requires = "at_least")]
    wait: Option<f64>,
}

impl Range {
    fn from(&self) -> u64 {
        self.from
            .unwrap_or(if self.backward { u64::MAX } else { 0 })
    }

    fn options(&self) -> ReadOptions {
        read_options(self.at_least, self.wait)
    }
}

/// The options of a read waiting for `at_least`, `wait` seconds at most.
pub fn read_options(at_least: Option<u64>, wait: Option<f64>) -> ReadOptions {
    ReadOptions {
        consistency: None,
        at_least,
        wait_timeout: wait.map(|s| Duration::from_secs_f64(s.max(0.0))),
    }
}

#[derive(Args, Debug)]
pub struct AggregateArgs {
    aggregate: String,
    #[command(flatten)]
    range: Range,
}

#[derive(Args, Debug)]
pub struct AllArgs {
    #[command(flatten)]
    range: Range,
}

#[derive(Args, Debug)]
pub struct TagArgs {
    tag: String,
    #[command(flatten)]
    range: Range,
}

pub async fn aggregate(conn: &Conn, format: Format, a: AggregateArgs) -> anyhow::Result<()> {
    let id = aggregate_id(&a.aggregate)?;
    let c = client::connect(conn).await?;
    let limit = a.range.limit as usize;
    let stream = if a.range.backward {
        c.read_aggregate_backward_stream(id, a.range.from(), limit)
            .await?
    } else if a.range.at_least.is_some() {
        let events = c
            .read_aggregate_with(id, a.range.from(), limit, a.range.options())
            .await?;
        for ev in &events {
            print_event(format, ev);
        }
        return Ok(());
    } else {
        c.read_aggregate_stream(id, a.range.from(), limit).await?
    };
    print_stream(format, stream).await
}

pub async fn all(conn: &Conn, format: Format, a: AllArgs) -> anyhow::Result<()> {
    let c = client::connect(conn).await?;
    let limit = a.range.limit as usize;
    let stream = if a.range.backward {
        c.read_all_backward_stream(a.range.from(), limit).await?
    } else {
        c.read_all_stream_with(a.range.from(), limit, a.range.options())
            .await?
    };
    print_stream(format, stream).await
}

pub async fn tag(conn: &Conn, format: Format, a: TagArgs) -> anyhow::Result<()> {
    let c = client::connect(conn).await?;
    let limit = a.range.limit as usize;
    let stream = if a.range.backward {
        c.read_tag_backward_stream(a.tag, a.range.from(), limit)
            .await?
    } else if a.range.at_least.is_some() {
        let events = c
            .read_tag_with(a.tag, a.range.from(), limit, a.range.options())
            .await?;
        for ev in &events {
            print_event(format, ev);
        }
        return Ok(());
    } else {
        c.read_tag_stream(a.tag, a.range.from(), limit).await?
    };
    print_stream(format, stream).await
}

async fn print_stream(format: Format, mut stream: EventStream) -> anyhow::Result<()> {
    while let Some(item) = stream.next().await {
        print_event(format, &item?);
    }
    Ok(())
}

#[derive(Args, Debug)]
pub struct TailArgs {
    /// Start at this position (default: the floor, the oldest readable
    /// position).
    #[arg(long, conflicts_with = "from_head")]
    from: Option<u64>,
    /// Start at the current head: only new events.
    #[arg(long)]
    from_head: bool,
    /// Only events carrying this tag.
    #[arg(long)]
    tag: Option<String>,
}

pub async fn tail(conn: &Conn, format: Format, a: TailArgs) -> anyhow::Result<()> {
    let c = client::connect(conn).await?;
    let from = match (a.from_head, a.from) {
        (true, _) => c.head().await?,
        (false, Some(from)) => from,
        (false, None) => c.info().await?.floor,
    };
    let stream = match a.tag {
        Some(tag) => c.subscribe_tag(tag, from).await?,
        None => c.subscribe(from).await?,
    };
    print_stream(format, stream).await
}
