use chron::{Db, Error, Expected};

use crate::common::*;

/// A db of several segments: aggregates 1 and 2, tag `t` on every third
/// event, a free event every fifth transaction.
fn multi_segment(dir: &std::path::Path) -> Db {
    let db = open_with(dir, opts().segment_max_bytes(300));
    for i in 0..60u64 {
        let mut t = db.transaction();
        let tags: &[&str] = if i % 3 == 0 { &["t"] } else { &[] };
        t.append_one(
            agg(1 + i as u128 % 2),
            Expected::Any,
            tagged(&format!("e{i}"), tags),
        );
        if i % 5 == 0 {
            t.append_free(vec![tagged("free", &["t"])]);
        }
        t.commit().unwrap();
    }
    assert!(segments(dir).len() > 3);
    db
}

/// The segment base the floor rounds down to for `position`.
fn base_of(db: &Db, position: u64) -> u64 {
    db.debug_locate(position).unwrap().unwrap().0
}

#[test]
fn retain_rounds_down_and_positions_are_stable() {
    let d = tmp();
    let db = multi_segment(d.path());
    let head = db.head();
    let x = head / 2;
    let expected_floor = base_of(&db, x);
    assert!(
        expected_floor > 0,
        "the midpoint is not in the first segment"
    );
    let before = segments(d.path()).len();
    let r = db.retain_from(x).unwrap();
    assert_eq!(r.floor, expected_floor);
    assert!(r.segments_removed >= 1);
    assert!(r.bytes_freed > 0);
    assert_eq!(segments(d.path()).len(), before - r.segments_removed);
    assert_eq!(db.floor(), expected_floor);
    assert_eq!(db.head(), head);
    let all = db.read_all(expected_floor, 10_000).unwrap();
    assert_eq!(positions(&all), (expected_floor..head).collect::<Vec<_>>());
    let info = db.info().unwrap();
    assert_eq!(
        (info.head, info.floor, info.segments),
        (head, expected_floor, before - r.segments_removed)
    );
    // Again at the same floor: a no-op.
    let again = db.retain_from(x).unwrap();
    assert_eq!(
        (again.floor, again.segments_removed, again.bytes_freed),
        (expected_floor, 0, 0)
    );
}

#[test]
fn reads_below_the_floor_error_and_backward_clamps() {
    let d = tmp();
    let db = multi_segment(d.path());
    let floor = db.retain_from(db.head() / 2).unwrap().floor;
    assert!(matches!(
        db.read_all(floor - 1, 1),
        Err(Error::BelowFloor { position, floor: f }) if position == floor - 1 && f == floor
    ));
    assert!(matches!(db.read_all(0, 1), Err(Error::BelowFloor { .. })));
    let back = db.read_all_backward(u64::MAX, 10_000).unwrap();
    assert_eq!(back.last().unwrap().position, floor);
    assert_eq!(back.len() as u64, db.head() - floor);
    let mut sub = db.subscribe(0);
    assert!(matches!(
        sub.next_timeout(std::time::Duration::from_millis(50)),
        Err(Error::BelowFloor { .. })
    ));
    let mut sub = db.subscribe(floor);
    assert_eq!(
        sub.next_timeout(std::time::Duration::from_secs(10))
            .unwrap()
            .unwrap()
            .position,
        floor
    );
}

#[test]
fn aggregate_and_tag_reads_return_survivors_and_exact_still_works() {
    let d = tmp();
    let db = multi_segment(d.path());
    let len_before = db.aggregate_len(agg(1)).unwrap();
    let floor = db.retain_from(db.head() / 2).unwrap().floor;
    let a = db.read_aggregate(agg(1), 0, 10_000).unwrap();
    assert!(a[0].index > 0, "the aggregate's earliest events are gone");
    assert!(a.iter().all(|e| e.position >= floor));
    let first = a[0].index;
    assert_eq!(
        indices(&a),
        (first..first + a.len() as u64).collect::<Vec<_>>()
    );
    let mut back = db
        .read_aggregate_backward(agg(1), u64::MAX, 10_000)
        .unwrap();
    back.reverse();
    assert_eq!(back, a);
    let t = db.read_tag("t", 0, 10_000).unwrap();
    assert!(!t.is_empty() && t.iter().all(|e| e.position >= floor && e.has_tag(b"t")));
    assert_eq!(db.aggregate_len(agg(1)).unwrap(), len_before);
    let mut txn = db.transaction();
    txn.append_one(agg(1), Expected::Exact(len_before), ev("after"));
    let c = txn.commit().unwrap();
    assert_eq!(c.appends[0].first_index, len_before);
}

#[test]
fn reopen_keeps_the_floor_and_verify_all_passes() {
    let d = tmp();
    let db = multi_segment(d.path());
    let floor = db.retain_from(db.head() / 2).unwrap().floor;
    let before = db.read_all(floor, 10_000).unwrap();
    let head = db.head();
    drop(db);
    let db = open_with(
        d.path(),
        opts().segment_max_bytes(300).verify_all_segments(true),
    );
    assert_eq!(db.floor(), floor);
    assert_eq!(db.head(), head);
    assert_eq!(db.read_all(floor, 10_000).unwrap(), before);
    assert!(matches!(db.read_all(0, 1), Err(Error::BelowFloor { .. })));
}

#[test]
fn a_crash_between_index_commit_and_file_removal_is_repaired_on_open() {
    let d = tmp();
    let db = multi_segment(d.path());
    let before = segments(d.path()).len();
    let x = db.head() / 2;
    let expected_floor = base_of(&db, x);
    let r = db.debug_retain_index_only(x).unwrap();
    assert_eq!((r.floor, r.segments_removed), (expected_floor, 0));
    drop(db);
    assert_eq!(
        segments(d.path()).len(),
        before,
        "the files are still there"
    );
    let db = open_with(
        d.path(),
        opts().segment_max_bytes(300).verify_all_segments(true),
    );
    assert!(segments(d.path()).len() < before, "open removed them");
    assert_eq!(
        segments(d.path())[0].file_name().unwrap().to_str().unwrap(),
        format!("{expected_floor:020}.seg")
    );
    assert_eq!(db.floor(), expected_floor);
    let all = db.read_all(expected_floor, 10_000).unwrap();
    assert_eq!(
        positions(&all),
        (expected_floor..db.head()).collect::<Vec<_>>()
    );
    assert!(db.read_aggregate(agg(1), 0, 10).unwrap()[0].index > 0);
}

#[test]
fn floor_past_head_is_refused_and_the_tail_is_never_removed() {
    let d = tmp();
    let db = multi_segment(d.path());
    let head = db.head();
    assert!(matches!(
        db.retain_from(head + 1),
        Err(Error::PositionOutOfRange { .. })
    ));
    let r = db.retain_from(head).unwrap();
    assert!(r.floor <= head);
    assert!(!segments(d.path()).is_empty());
    assert_eq!(
        db.read_all(r.floor, 10_000).unwrap().len() as u64,
        head - r.floor
    );
    fill(&db, agg(1), 1);
    assert_eq!(db.head(), head + 1);
    assert_eq!(db.read_all(head, 10).unwrap()[0].position, head);
}

#[test]
fn rebuild_after_retention_keeps_the_floor() {
    let d = tmp();
    let db = multi_segment(d.path());
    let floor = db.retain_from(db.head() / 2).unwrap().floor;
    let before = (
        db.head(),
        db.read_all(floor, 10_000).unwrap(),
        db.aggregate_len(agg(1)).unwrap(),
    );
    drop(db);
    std::fs::remove_file(d.path().join("index.redb")).unwrap();
    let r = Db::rebuild_index(d.path(), &opts()).unwrap();
    assert_eq!((r.head, r.floor, r.dropped), (before.0, floor, 0));
    let db = open_with(d.path(), opts().verify_all_segments(true));
    assert_eq!(db.floor(), floor);
    assert_eq!(db.read_all(floor, 10_000).unwrap(), before.1);
    let a = db.read_aggregate(agg(1), 0, 10_000).unwrap();
    assert!(a[0].index > 0);
    assert_eq!(
        db.aggregate_len(agg(1)).unwrap(),
        before.2,
        "the length counts the dropped events too"
    );
}
