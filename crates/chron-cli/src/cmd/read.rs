use chron_proto::v1 as pb;
use clap::Args;
use tokio_stream::StreamExt;

use crate::client;
use crate::cmd::aggregate_bytes;
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

async fn print_stream(
    format: Format,
    mut stream: tonic::Streaming<pb::Event>,
) -> anyhow::Result<()> {
    while let Some(item) = stream.next().await {
        print_event(format, &item?);
    }
    Ok(())
}

pub async fn aggregate(addr: &str, format: Format, a: AggregateArgs) -> anyhow::Result<()> {
    let aggregate = aggregate_bytes(&a.aggregate)?;
    let mut c = client::connect(addr).await?;
    let stream = c
        .read_aggregate(pb::ReadAggregateRequest {
            aggregate,
            from_index: a.range.from(),
            limit: a.range.limit,
            backward: a.range.backward,
        })
        .await?
        .into_inner();
    print_stream(format, stream).await
}

pub async fn all(addr: &str, format: Format, a: AllArgs) -> anyhow::Result<()> {
    let mut c = client::connect(addr).await?;
    let stream = c
        .read_all(pb::ReadAllRequest {
            from: a.range.from(),
            limit: a.range.limit,
            backward: a.range.backward,
        })
        .await?
        .into_inner();
    print_stream(format, stream).await
}

pub async fn tag(addr: &str, format: Format, a: TagArgs) -> anyhow::Result<()> {
    let mut c = client::connect(addr).await?;
    let stream = c
        .read_tag(pb::ReadTagRequest {
            tag: a.tag.into_bytes(),
            from: a.range.from(),
            limit: a.range.limit,
            backward: a.range.backward,
        })
        .await?
        .into_inner();
    print_stream(format, stream).await
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

pub async fn tail(addr: &str, format: Format, a: TailArgs) -> anyhow::Result<()> {
    let mut c = client::connect(addr).await?;
    let from = if a.from_head {
        c.head(pb::HeadRequest {}).await?.into_inner().head
    } else {
        a.from.unwrap_or(0)
    };
    let stream = c
        .subscribe(pb::SubscribeRequest {
            from,
            tag: a.tag.map(String::into_bytes),
        })
        .await?
        .into_inner();
    print_stream(format, stream).await
}
