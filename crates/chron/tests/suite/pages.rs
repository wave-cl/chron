//! Pages bounded in bytes, and the bounded cache of open segment files.

use chron::{Expected, Fsync};

use crate::common::*;

const RECORD: usize = 250_000;
const MIB: usize = 1024 * 1024;

/// 40 records of a quarter megabyte, tagged, on one aggregate.
fn big_db(d: &tempfile::TempDir, page_max_bytes: usize) -> chron::Db {
    let db = open_with(d.path(), opts().page_max_bytes(page_max_bytes));
    let payload = "x".repeat(RECORD);
    for _ in 0..40 {
        let mut t = db.transaction();
        t.append_one(agg(1), Expected::Any, tagged(&payload, &["big"]));
        t.commit().unwrap();
    }
    db
}

#[test]
fn a_bounded_read_stops_at_the_byte_limit_but_returns_at_least_one() {
    let d = tmp();
    let db = big_db(&d, MIB);
    assert_eq!(db.read_all_bounded(0, 0, MIB).unwrap().len(), 4);
    assert_eq!(db.read_tag_bounded("big", 0, 0, MIB).unwrap().len(), 4);
    assert_eq!(
        db.read_aggregate_bounded(agg(1), 0, 0, MIB).unwrap().len(),
        4
    );
    assert_eq!(db.read_all_bounded(0, 0, 1).unwrap().len(), 1);
    assert_eq!(
        db.read_all_bounded(0, 2, MIB).unwrap().len(),
        2,
        "the record bound still applies"
    );
    assert_eq!(
        db.read_all(0, 0).unwrap().len(),
        40,
        "an unbounded read is unchanged"
    );
    let page = db.read_all_bounded(38, 0, MIB).unwrap();
    assert_eq!(positions(&page), [38, 39]);
}

#[test]
fn iterators_and_subscriptions_page_by_bytes_and_still_see_everything() {
    let d = tmp();
    let db = big_db(&d, MIB);
    let all: Vec<_> = db.iter_all(0).map(|r| r.unwrap()).collect();
    assert_eq!(positions(&all), (0..40).collect::<Vec<_>>());
    assert_eq!(db.iter_tag("big", 0).count(), 40);
    assert_eq!(
        indices(
            &db.iter_aggregate(agg(1), 0)
                .map(|r| r.unwrap())
                .collect::<Vec<_>>()
        ),
        (0..40).collect::<Vec<_>>()
    );
    let got: Vec<_> = db.subscribe(0).take(40).map(|r| r.unwrap()).collect();
    assert_eq!(positions(&got), (0..40).collect::<Vec<_>>());
    let mut tagged_sub = db.subscribe_tag("big", 0);
    for i in 0..40 {
        assert_eq!(tagged_sub.next().unwrap().unwrap().position, i);
    }
    // Live delivery after the catch-up still works.
    let mut t = db.transaction();
    t.append_one(agg(1), Expected::Any, tagged("live", &["big"]));
    t.commit().unwrap();
    assert_eq!(tagged_sub.next().unwrap().unwrap().position, 40);
}

#[test]
fn open_segment_handles_are_bounded() {
    let d = tmp();
    // Every commit rolls: one segment per event.
    let db = open_with(
        d.path(),
        opts()
            .fsync(Fsync::Never)
            .segment_max_bytes(1)
            .max_open_segments(16),
    );
    fill(&db, agg(1), 100);
    assert!(segments(d.path()).len() >= 100);
    let all = db.read_all(0, 0).unwrap();
    assert_eq!(all.len(), 100);
    assert!(
        db.debug_open_segments() <= 16,
        "{} segment files open",
        db.debug_open_segments()
    );
    // Reads through evicted handles reopen them and read correctly.
    assert_eq!(positions(&db.read_all(0, 0).unwrap()), positions(&all));
    assert_eq!(db.read_tag("t", 0, 0).unwrap().len(), 0);
    assert_eq!(db.read_aggregate(agg(1), 0, 0).unwrap().len(), 100);
    assert!(db.debug_open_segments() <= 16);
}
