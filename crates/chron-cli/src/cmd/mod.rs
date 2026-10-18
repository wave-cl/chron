pub mod admin;
pub mod offline;
pub mod read;
pub mod write;

use chron_proto::v1::{self as pb, expected};
use uuid::Uuid;

use crate::output::Usage;

/// `free`, or a UUID.
pub fn parse_aggregate(s: &str) -> anyhow::Result<Option<Uuid>> {
    if s == "free" {
        return Ok(None);
    }
    Uuid::parse_str(s)
        .map(Some)
        .map_err(|_| Usage(format!("{s} is neither a UUID nor `free`")).into())
}

pub fn aggregate_bytes(s: &str) -> anyhow::Result<Vec<u8>> {
    match parse_aggregate(s)? {
        Some(u) => Ok(u.as_bytes().to_vec()),
        None => Err(Usage("this command needs an aggregate, not `free`".into()).into()),
    }
}

/// `any`, `new`, or a number.
pub fn parse_expect(s: &str) -> anyhow::Result<pb::Expected> {
    let kind = match s {
        "any" => expected::Kind::Any(expected::Any {}),
        "new" => expected::Kind::New(expected::New {}),
        n => expected::Kind::Exact(
            n.parse()
                .map_err(|_| Usage(format!("--expect takes any, new or a number, not {n}")))?,
        ),
    };
    Ok(pb::Expected { kind: Some(kind) })
}
