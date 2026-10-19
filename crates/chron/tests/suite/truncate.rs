//! Truncation: cutting the tail off a database so it can follow another
//! copy again.

use chron::{Error, Expected};

use crate::common::*;

#[test]
fn truncate_drops_the_tail_and_the_database_goes_on() {
    let d = tmp();
    let db = open_with(d.path(), opts().segment_max_bytes(300));
    for _ in 0..10 {
        let mut t = db.transaction();
        t.append(agg(1), Expected::Any, vec![tagged("a", &["t"]), ev("b")]);
        t.commit().unwrap();
    }
    for _ in 0..5 {
        let mut t = db.transaction();
        t.append_free(vec![tagged("f", &["t"])]);
        t.commit().unwrap();
    }
    assert_eq!(db.head(), 25);
    assert!(segments(d.path()).len() >= 3);
    assert!(matches!(
        db.truncate_to(1),
        Err(Error::Inconsistent { position: 1, .. })
    ));
    assert!(matches!(
        db.truncate_to(26),
        Err(Error::PositionOutOfRange { .. })
    ));
    let r = db.truncate_to(10).unwrap();
    assert_eq!((r.head, r.removed), (10, 15));
    assert!(r.segments_removed >= 1);
    assert_eq!(db.head(), 10);
    assert_eq!(db.aggregate_len(agg(1)).unwrap(), 10);
    assert_eq!(
        positions(&db.read_all(0, 0).unwrap()),
        (0..10).collect::<Vec<_>>()
    );
    assert_eq!(positions(&db.read_tag("t", 0, 0).unwrap()), [0, 2, 4, 6, 8]);
    assert_eq!(
        indices(&db.read_aggregate(agg(1), 0, 0).unwrap()),
        (0..10).collect::<Vec<_>>()
    );
    assert!(db.read_all(10, 0).unwrap().is_empty());
    let c = {
        let mut t = db.transaction();
        t.append_one(agg(1), Expected::Exact(10), ev("new"));
        t.commit().unwrap()
    };
    assert_eq!((c.first, c.appends[0].first_index), (10, 10));
    assert_eq!(db.truncate_to(11).unwrap().removed, 0);
    drop(db);
    let db = open_with(
        d.path(),
        opts().segment_max_bytes(300).verify_all_segments(true),
    );
    assert_eq!(db.head(), 11);
    assert_eq!(db.aggregate_len(agg(1)).unwrap(), 11);
    assert_eq!(payloads(&db.read_all(9, 0).unwrap()), ["b", "new"]);
    assert_eq!(db.read_tag("t", 0, 0).unwrap().len(), 5);
    // Below the floor is refused before anything else.
    fill(&db, agg(2), 40);
    let r = db.retain_from(30).unwrap();
    if r.floor > 0 {
        assert!(matches!(
            db.truncate_to(r.floor - 1),
            Err(Error::BelowFloor { .. })
        ));
    }
    db.truncate_to(db.head()).unwrap();
}

#[test]
fn truncate_at_a_segment_base_leaves_an_empty_tail_segment() {
    let d = tmp();
    let db = open_with(d.path(), opts().segment_max_bytes(300));
    fill(&db, agg(1), 40);
    let segs = segments(d.path());
    assert!(segs.len() >= 3, "{segs:?}");
    let second = &segs[1];
    let base: u64 = second
        .file_name()
        .unwrap()
        .to_str()
        .unwrap()
        .trim_end_matches(".seg")
        .parse()
        .unwrap();
    let r = db.truncate_to(base).unwrap();
    assert_eq!(r.head, base);
    assert_eq!(r.segments_removed, segs.len() - 2);
    let now = segments(d.path());
    assert_eq!(now.len(), 2);
    assert_eq!(file_len(&now[1]), 64, "header only");
    let c = {
        let mut t = db.transaction();
        t.append_one(agg(1), Expected::Exact(base), ev("after"));
        t.commit().unwrap()
    };
    assert_eq!(c.first, base);
    drop(db);
    let db = open_with(
        d.path(),
        opts().segment_max_bytes(300).verify_all_segments(true),
    );
    assert_eq!(db.head(), base + 1);
    assert_eq!(payloads(&db.read_all(base, 0).unwrap()), ["after"]);
}

#[test]
fn a_cut_inside_the_tail_segment_removes_no_file_and_an_aggregate_can_vanish() {
    let d = tmp();
    let db = open_with(d.path(), opts());
    fill(&db, agg(1), 4);
    fill(&db, agg(2), 3);
    assert_eq!(segments(d.path()).len(), 1);
    let r = db.truncate_to(4).unwrap();
    assert_eq!((r.head, r.removed, r.segments_removed), (4, 3, 0));
    assert_eq!(db.aggregate_len(agg(2)).unwrap(), 0, "wholly cut");
    assert_eq!(db.aggregate_len(agg(1)).unwrap(), 4);
    assert!(db.read_aggregate(agg(2), 0, 0).unwrap().is_empty());
    assert!(db.read_all(4, 0).unwrap().is_empty());
    fill(&db, agg(2), 2);
    assert_eq!(db.aggregate_len(agg(2)).unwrap(), 2);
    assert_eq!(indices(&db.read_aggregate(agg(2), 0, 0).unwrap()), [0, 1]);
    drop(db);
    let db = open_with(d.path(), opts().verify_all_segments(true));
    assert_eq!(db.head(), 6);
    assert_eq!(db.aggregate_len(agg(2)).unwrap(), 2);
}

#[test]
fn a_cut_at_a_floor_above_zero_and_a_subscription_across_a_cut() {
    let d = tmp();
    let db = open_with(d.path(), opts().segment_max_bytes(300));
    fill(&db, agg(1), 40);
    let r = db.retain_from(20).unwrap();
    assert!(r.floor > 0);
    let floor = r.floor;
    // A subscription that has seen the tail notices the cut.
    let mut sub = db.subscribe(floor);
    let seen: Vec<_> = (0..(40 - floor))
        .map(|_| sub.next().unwrap().unwrap())
        .collect();
    assert_eq!(seen.last().unwrap().position, 39);
    let r = db.truncate_to(floor).unwrap();
    assert_eq!((r.head, db.floor()), (floor, floor));
    let err = sub.next_timeout(std::time::Duration::from_millis(50));
    assert!(
        matches!(err, Err(Error::Inconsistent { .. })),
        "a subscription across the cut: {err:?}"
    );
    fill(&db, agg(1), 2);
    assert_eq!(db.aggregate_len(agg(1)).unwrap(), floor + 2);
    // The subscription keeps the database open; both go before the reopen.
    drop(sub);
    drop(db);
    let db = open_with(
        d.path(),
        opts().segment_max_bytes(300).verify_all_segments(true),
    );
    assert_eq!((db.head(), db.floor()), (floor + 2, floor));
}

/// The index put back from a copy taken right after a truncation (a
/// durable commit): the commits since are replayed at the reused
/// positions.
#[test]
fn reopen_after_truncate_with_a_lagging_index() {
    let d = tmp();
    let db = open(d.path());
    for i in 0..6 {
        let mut t = db.transaction();
        t.append_one(agg(1), Expected::Any, tagged(&format!("old{i}"), &["t"]));
        t.commit().unwrap();
    }
    db.truncate_to(3).unwrap();
    let stale = snapshot_index(d.path());
    for i in 0..4 {
        let mut t = db.transaction();
        t.append_one(agg(1), Expected::Exact(3 + i), ev(&format!("new{i}")));
        t.commit().unwrap();
    }
    let before = db.read_all(0, 0).unwrap();
    drop(db);
    restore_index(d.path(), &stale);
    let db = open_with(d.path(), opts().verify_all_segments(true));
    assert_eq!(db.head(), 7);
    assert_eq!(db.read_all(0, 0).unwrap(), before);
    assert_eq!(
        payloads(&db.read_all(3, 0).unwrap()),
        ["new0", "new1", "new2", "new3"]
    );
    assert_eq!(positions(&db.read_tag("t", 0, 0).unwrap()), [0, 1, 2]);
    assert_eq!(db.aggregate_len(agg(1)).unwrap(), 7);
}

/// A crash between the truncation's index commit and its file work: the
/// marker makes the next open finish the cut instead of replaying the
/// cut records as committed.
#[test]
fn a_crash_after_the_truncate_marker_is_finished_on_open() {
    for at_base in [false, true] {
        let d = tmp();
        let db = open_with(d.path(), opts().segment_max_bytes(300));
        for i in 0..30 {
            let mut t = db.transaction();
            t.append_one(agg(1), Expected::Any, tagged(&format!("e{i}"), &["t"]));
            t.commit().unwrap();
        }
        let segs = segments(d.path()).len();
        assert!(segs > 3);
        let to = if at_base {
            // The base of the second segment: the cut leaves it empty.
            segments(d.path())[1]
                .file_stem()
                .unwrap()
                .to_str()
                .unwrap()
                .parse::<u64>()
                .unwrap()
        } else {
            10
        };
        let r = db.debug_truncate_marker_only(to).unwrap();
        assert_eq!((r.head, r.removed), (to, 30 - to));
        drop(db);
        assert_eq!(segments(d.path()).len(), segs, "the files are untouched");
        let db = open_with(d.path(), opts().verify_all_segments(true));
        assert_eq!(db.head(), to, "at_base {at_base}");
        assert_eq!(db.aggregate_len(agg(1)).unwrap(), to);
        assert_eq!(db.read_all(0, 0).unwrap().len() as u64, to);
        assert_eq!(positions(&db.read_tag("t", 0, 0).unwrap()).len() as u64, to);
        assert!(db.read_all(to, 0).unwrap().is_empty());
        assert!(segments(d.path()).len() < segs, "later segments are gone");
        let c = {
            let mut t = db.transaction();
            t.append_one(agg(1), Expected::Exact(to), ev("after"));
            t.commit().unwrap()
        };
        assert_eq!(c.first, to);
        drop(db);
        let db = open_with(d.path(), opts().verify_all_segments(true));
        assert_eq!(db.head(), to + 1, "a second open finds nothing pending");
        assert_eq!(payloads(&db.read_all(to, 0).unwrap()), ["after"]);
    }
}
