use std::io::Read;

use anyhow::Context;
use base64::Engine;
use chron_proto::v1 as pb;
use clap::Args;
use serde::Deserialize;

use crate::client;
use crate::cmd::{parse_aggregate, parse_expect};
use crate::output::{Format, Usage, print_commit};

#[derive(Args, Debug)]
pub struct AppendArgs {
    /// The aggregate's UUID, or `free`.
    aggregate: String,
    /// any, new, or the exact length the aggregate must have.
    #[arg(long, default_value = "any")]
    expect: String,
    /// A tag; repeatable.
    #[arg(long = "tag")]
    tags: Vec<String>,
    /// Metadata bytes.
    #[arg(long)]
    meta: Option<String>,
    /// The payload.
    #[arg(short = 'd', long = "data", conflicts_with = "stdin")]
    data: Option<String>,
    /// Read the payload from stdin.
    #[arg(long)]
    stdin: bool,
}

pub async fn append(addr: &str, format: Format, a: AppendArgs) -> anyhow::Result<()> {
    let aggregate = parse_aggregate(&a.aggregate)?;
    let expected = parse_expect(&a.expect)?;
    if aggregate.is_none() && !matches!(expected.kind, Some(pb::expected::Kind::Any(_))) {
        return Err(Usage("free events have no length to expect; use --expect any".into()).into());
    }
    let payload = match (a.data, a.stdin) {
        (Some(d), _) => d.into_bytes(),
        (None, true) => {
            let mut buf = Vec::new();
            std::io::stdin()
                .read_to_end(&mut buf)
                .context("read stdin")?;
            buf
        }
        (None, false) => return Err(Usage("give the payload with -d or --stdin".into()).into()),
    };
    let event = pb::NewEvent {
        tags: a.tags.into_iter().map(String::into_bytes).collect(),
        metadata: a.meta.map(String::into_bytes).unwrap_or_default(),
        payload,
    };
    let append = pb::Append {
        aggregate: aggregate.map(|u| u.as_bytes().to_vec()),
        expected: Some(expected),
        events: vec![event],
    };
    let mut c = client::connect(addr).await?;
    let resp = c
        .commit(pb::CommitRequest {
            appends: vec![append],
        })
        .await?
        .into_inner();
    print_commit(format, &resp);
    Ok(())
}

#[derive(Args, Debug)]
pub struct CommitArgs {
    /// A JSON file: {"appends":[{"aggregate":"<uuid>"|null,"expect":"any"|"new"|<n>,
    /// "events":[{"payload":"…","tags":["…"],"metadata":"…"}]}]}. `payload_base64`
    /// and `metadata_base64` carry binary values.
    #[arg(short = 'f', long, conflicts_with = "stdin")]
    file: Option<std::path::PathBuf>,
    /// Read the JSON from stdin.
    #[arg(long)]
    stdin: bool,
}

#[derive(Deserialize)]
struct TxnJson {
    appends: Vec<AppendJson>,
}

#[derive(Deserialize)]
struct AppendJson {
    #[serde(default)]
    aggregate: Option<String>,
    #[serde(default)]
    expect: Option<ExpectJson>,
    events: Vec<EventJson>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum ExpectJson {
    Word(String),
    Exact(u64),
}

#[derive(Deserialize)]
struct EventJson {
    #[serde(default)]
    payload: Option<String>,
    #[serde(default)]
    payload_base64: Option<String>,
    #[serde(default)]
    tags: Vec<String>,
    #[serde(default)]
    metadata: Option<String>,
    #[serde(default)]
    metadata_base64: Option<String>,
}

fn bytes(text: Option<String>, b64: Option<String>, what: &str) -> anyhow::Result<Vec<u8>> {
    match (text, b64) {
        (Some(t), None) => Ok(t.into_bytes()),
        (None, Some(b)) => base64::engine::general_purpose::STANDARD
            .decode(b)
            .with_context(|| format!("{what}_base64 is not base64")),
        (None, None) => Ok(Vec::new()),
        (Some(_), Some(_)) => Err(Usage(format!("give {what} or {what}_base64, not both")).into()),
    }
}

pub async fn commit(addr: &str, format: Format, a: CommitArgs) -> anyhow::Result<()> {
    let text = match (a.file, a.stdin) {
        (Some(f), _) => {
            std::fs::read_to_string(&f).with_context(|| format!("read {}", f.display()))?
        }
        (None, true) => {
            let mut s = String::new();
            std::io::stdin()
                .read_to_string(&mut s)
                .context("read stdin")?;
            s
        }
        (None, false) => return Err(Usage("give the transaction with -f or --stdin".into()).into()),
    };
    let txn: TxnJson =
        serde_json::from_str(&text).map_err(|e| Usage(format!("bad transaction JSON: {e}")))?;
    let mut appends = Vec::with_capacity(txn.appends.len());
    for a in txn.appends {
        let aggregate = match a.aggregate {
            Some(s) => parse_aggregate(&s)?,
            None => None,
        };
        let expected = match a.expect {
            None => None,
            Some(ExpectJson::Word(w)) => Some(parse_expect(&w)?),
            Some(ExpectJson::Exact(n)) => Some(parse_expect(&n.to_string())?),
        };
        let mut events = Vec::with_capacity(a.events.len());
        for e in a.events {
            events.push(pb::NewEvent {
                tags: e.tags.into_iter().map(String::into_bytes).collect(),
                metadata: bytes(e.metadata, e.metadata_base64, "metadata")?,
                payload: bytes(e.payload, e.payload_base64, "payload")?,
            });
        }
        appends.push(pb::Append {
            aggregate: aggregate.map(|u| u.as_bytes().to_vec()),
            expected,
            events,
        });
    }
    let mut c = client::connect(addr).await?;
    let resp = c.commit(pb::CommitRequest { appends }).await?.into_inner();
    print_commit(format, &resp);
    Ok(())
}
