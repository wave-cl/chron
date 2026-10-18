//! The `chron.v1` gRPC contract: generated messages, the `Chron` service
//! client and server, and the response metadata keys the daemon sets.

pub mod v1 {
    #![allow(clippy::all, missing_docs)]
    tonic::include_proto!("chron.v1");
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
