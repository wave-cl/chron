pub mod admin;
pub mod offline;
pub mod read;
pub mod write;

use chron::{Expected, Uuid};

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

pub fn aggregate_id(s: &str) -> anyhow::Result<Uuid> {
    parse_aggregate(s)?.ok_or_else(|| Usage("this command takes an aggregate UUID".into()).into())
}

/// `any`, `new`, or a number.
pub fn parse_expect(s: &str) -> anyhow::Result<Expected> {
    Ok(match s {
        "any" => Expected::Any,
        "new" => Expected::New,
        n => Expected::Exact(
            n.parse()
                .map_err(|_| Usage(format!("--expect takes any, new or a number, not {n}")))?,
        ),
    })
}
