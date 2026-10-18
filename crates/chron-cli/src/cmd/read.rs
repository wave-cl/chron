use chron_client::EventStream;
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
}

impl Range {
    fn from(&self) -> u64 {
        self.from
            .unwrap_or(if self.backward { u64::MAX } else { 0 })
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
    for e in c
        .read_aggregate(id, a.range.from(), a.range.limit, a.range.backward)
        .await?
    {
        print_event(format, &e);
    }
    Ok(())
}

pub async fn all(conn: &Conn, format: Format, a: AllArgs) -> anyhow::Result<()> {
    let c = client::connect(conn).await?;
    for e in c
        .read_all(a.range.from(), a.range.limit, a.range.backward)
        .await?
    {
        print_event(format, &e);
    }
    Ok(())
}

pub async fn tag(conn: &Conn, format: Format, a: TagArgs) -> anyhow::Result<()> {
    let c = client::connect(conn).await?;
    for e in c
        .read_tag(a.tag, a.range.from(), a.range.limit, a.range.backward)
        .await?
    {
        print_event(format, &e);
    }
    Ok(())
}

#[derive(Args, Debug)]
pub struct TailArgs {
    /// Start at this position (default 0).
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
    let from = if a.from_head {
        c.head().await?
    } else {
        a.from.unwrap_or(0)
    };
    let mut stream: EventStream = c.subscribe(from, a.tag.map(bytes::Bytes::from)).await?;
    while let Some(item) = stream.next().await {
        print_event(format, &item?);
    }
    Ok(())
}
