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
    assert!(db.read_all(10, 0).unwrap().is_empty() || db.read_all(10, 0).is_err());
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
