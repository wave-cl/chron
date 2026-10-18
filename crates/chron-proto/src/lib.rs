//! The `chron.v1` gRPC contract: generated messages, the `Chron` service
//! client and server, and the response metadata keys the daemon sets.

pub mod v1 {
    #![allow(clippy::all, missing_docs)]
    // Generated from `proto/chron/v1/chron.proto` and committed, so no
    // `protoc` is needed to build; `CHRON_PROTO_REGEN=1 cargo build -p
    // chron-proto` regenerates it (see build.rs).
    include!("generated/chron.v1.rs");
}

#[cfg(feature = "convert")]
pub mod convert;

/// On a `FAILED_PRECONDITION` from `Commit`: the conflicting aggregate's
/// actual length, as decimal text.
pub const CONFLICT_ACTUAL_LENGTH_HEADER: &str = "chron-actual-length";
/// On the same status: the conflicting aggregate's hyphenated UUID.
pub const CONFLICT_AGGREGATE_HEADER: &str = "chron-aggregate";
/// On a `FAILED_PRECONDITION` for a write the node's role or epoch
/// refused: the node's epoch, as decimal text.
pub const EPOCH_HEADER: &str = "chron-epoch";
/// On an `OUT_OF_RANGE`: the position asked for, as decimal text.
pub const POSITION_HEADER: &str = "chron-position";
/// On an `OUT_OF_RANGE` for a position below the floor: the floor.
pub const FLOOR_HEADER: &str = "chron-floor";
/// On an `OUT_OF_RANGE` for a position past the head: the head.
pub const HEAD_HEADER: &str = "chron-head";
/// Request metadata naming the database a call is for; absent means
/// [`DEFAULT_DATABASE`].
pub const DATABASE_HEADER: &str = "chron-db";
/// The database every daemon has and a call without [`DATABASE_HEADER`]
/// goes to.
pub const DEFAULT_DATABASE: &str = "default";

/// Whether `name` may name a database: 1 to 64 bytes of `a-z`, `0-9`,
/// `_` and `-`.
pub fn valid_database_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 64
        && name
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_' || b == b'-')
}
