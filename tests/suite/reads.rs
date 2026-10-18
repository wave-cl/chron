use chron::{Error, Expected};

use crate::common::*;

#[test]
fn round_trip_one_transaction() {
    let d = tmp();
    let db = open(d.path());
    let a = agg(1);
    let mut t = db.transaction();
    t.append(
        a,
        Expected::New,
        vec![
            tagged("one", &["x"]).metadata("m1"),
            ev("two"),
            tagged("three", &["x", "y"]),
        ],
    );
    let c = t.commit().unwrap();
    assert_eq!((c.first, c.last, c.next), (0, 2, 3));
    assert_eq!(c.appends.len(), 1);
    assert_eq!(c.appends[0].aggregate, Some(a));
    assert_eq!(c.appends[0].first_index, 0);
    assert_eq!(c.appends[0].new_len, 3);

    let got = db.read_aggregate(a, 0, 10).unwrap();
    assert_eq!(payloads(&got), ["one", "two", "three"]);
    assert_eq!(indices(&got), [0, 1, 2]);
    assert_eq!(positions(&got), [0, 1, 2]);
    assert!(got[0].is_first_in_txn() && !got[0].is_last_in_txn());
    assert!(!got[1].is_first_in_txn() && !got[1].is_last_in_txn());
    assert!(!got[2].is_first_in_txn() && got[2].is_last_in_txn());
    assert!(got.iter().all(|e| e.recorded_at == got[0].recorded_at));
    assert!(got[0].recorded_at > 0);
    assert_eq!(got[0].metadata.as_ref(), b"m1");
    assert!(got[0].has_tag(b"x") && !got[0].has_tag(b"y"));
    assert!(got[2].has_tag(b"x") && got[2].has_tag(b"y"));
    assert!(got.iter().all(|e| e.aggregate == Some(a)));
    assert_eq!(db.aggregate_len(a).unwrap(), 3);
    assert_eq!(db.head(), 3);
}

#[test]
fn read_from_index() {
    let d = tmp();
    let db = open(d.path());
    let a = agg(1);
    fill(&db, a, 3);
    let got = db.read_aggregate(a, 1, 1).unwrap();
    assert_eq!(indices(&got), [1]);
    assert_eq!(payloads(&got), ["e1"]);
    assert_eq!(indices(&db.read_aggregate(a, 1, 10).unwrap()), [1, 2]);
    assert!(db.read_aggregate(a, 3, 10).unwrap().is_empty());
    assert!(db.read_aggregate(a, 0, 0).unwrap().is_empty());
    assert!(db.read_aggregate(agg(9), 0, 10).unwrap().is_empty());
    assert_eq!(db.aggregate_len(agg(9)).unwrap(), 0);
}

#[test]
fn read_all_from_position() {
    let d = tmp();
    let db = open(d.path());
    fill(&db, agg(1), 2);
    fill(&db, agg(2), 2);
    let got = db.read_all(2, 10).unwrap();
    assert_eq!(positions(&got), [2, 3]);
    assert_eq!(got[0].aggregate, Some(agg(2)));
    assert_eq!(positions(&db.read_all(0, 3).unwrap()), [0, 1, 2]);
    assert!(db.read_all(4, 1).unwrap().is_empty());
    assert!(db.read_all(0, 0).unwrap().is_empty());
    assert!(matches!(
        db.read_all(5, 1),
        Err(Error::PositionOutOfRange {
            position: 5,
            head: 4
        })
    ));
}

#[test]
fn tag_order_across_aggregates() {
    let d = tmp();
    let db = open(d.path());
    let steps: [(u128, &str, &[&str]); 5] = [
        (1, "a0", &["t"]),
        (2, "b0", &[]),
        (1, "a1", &["t", "u"]),
        (3, "c0", &["t", "t"]),
        (2, "b1", &["u"]),
    ];
    for (id, p, tags) in steps {
        let mut t = db.transaction();
        t.append_one(agg(id), Expected::Any, tagged(p, tags));
        t.commit().unwrap();
    }
    let got = db.read_tag("t", 0, 100).unwrap();
    assert_eq!(payloads(&got), ["a0", "a1", "c0"]);
    assert_eq!(positions(&got), [0, 2, 3]);
    assert_eq!(got[2].tags.len(), 1, "a tag given twice is stored once");
    assert_eq!(payloads(&db.read_tag("t", 1, 100).unwrap()), ["a1", "c0"]);
    assert_eq!(payloads(&db.read_tag("t", 0, 2).unwrap()), ["a0", "a1"]);
    assert_eq!(payloads(&db.read_tag("u", 0, 100).unwrap()), ["a1", "b1"]);
    assert!(db.read_tag("v", 0, 100).unwrap().is_empty());
    assert!(db.read_tag("t", 0, 0).unwrap().is_empty());
}

#[test]
fn read_all_backward() {
    let d = tmp();
    let db = open(d.path());
    assert!(db.read_all_backward(u64::MAX, 10).unwrap().is_empty());
    fill(&db, agg(1), 4);
    assert_eq!(
        positions(&db.read_all_backward(u64::MAX, 10).unwrap()),
        [3, 2, 1, 0]
    );
    let got = db.read_all_backward(2, 2).unwrap();
    assert_eq!(positions(&got), [2, 1]);
    assert_eq!(payloads(&got), ["e2", "e1"]);
    assert_eq!(positions(&db.read_all_backward(0, 5).unwrap()), [0]);
    assert_eq!(positions(&db.read_all_backward(99, 1).unwrap()), [3]);
    assert!(db.read_all_backward(3, 0).unwrap().is_empty());
}

#[test]
fn read_aggregate_backward() {
    let d = tmp();
    let db = open(d.path());
    let (a, b) = (agg(1), agg(2));
    for i in 0..6u64 {
        let mut t = db.transaction();
        t.append_one(
            if i % 2 == 0 { a } else { b },
            Expected::Any,
            ev(&format!("e{i}")),
        );
        t.commit().unwrap();
    }
    let got = db.read_aggregate_backward(a, u64::MAX, 10).unwrap();
    assert_eq!(indices(&got), [2, 1, 0]);
    assert_eq!(payloads(&got), ["e4", "e2", "e0"]);
    assert_eq!(positions(&got), [4, 2, 0]);
    assert_eq!(
        indices(&db.read_aggregate_backward(b, 1, 10).unwrap()),
        [1, 0]
    );
    assert_eq!(indices(&db.read_aggregate_backward(b, 1, 1).unwrap()), [1]);
    assert!(
        db.read_aggregate_backward(agg(9), u64::MAX, 10)
            .unwrap()
            .is_empty()
    );
    assert!(db.read_aggregate_backward(a, 2, 0).unwrap().is_empty());
}

#[test]
fn read_tag_backward() {
    let d = tmp();
    let db = open(d.path());
    let steps: [(&str, &[&str]); 4] = [
        ("a0", &["t"]),
        ("b0", &[]),
        ("a1", &["t", "u"]),
        ("c0", &["t"]),
    ];
    for (p, tags) in steps {
        let mut t = db.transaction();
        t.append_one(agg(1), Expected::Any, tagged(p, tags));
        t.commit().unwrap();
    }
    assert_eq!(
        positions(&db.read_tag_backward("t", u64::MAX, 10).unwrap()),
        [3, 2, 0]
    );
    assert_eq!(
        payloads(&db.read_tag_backward("t", 2, 10).unwrap()),
        ["a1", "a0"]
    );
    assert_eq!(positions(&db.read_tag_backward("t", 1, 10).unwrap()), [0]);
    assert_eq!(positions(&db.read_tag_backward("t", 3, 1).unwrap()), [3]);
    assert!(db.read_tag_backward("v", u64::MAX, 10).unwrap().is_empty());
    assert!(db.read_tag_backward("t", 3, 0).unwrap().is_empty());
}

#[test]
fn empty_transaction_and_empty_append() {
    let d = tmp();
    let db = open(d.path());
    assert!(matches!(
        db.transaction().commit(),
        Err(Error::EmptyTransaction)
    ));
    let mut t = db.transaction();
    assert!(t.is_empty());
    t.append(agg(1), Expected::Any, vec![]);
    assert!(!t.is_empty());
    assert!(matches!(
        t.commit(),
        Err(Error::EmptyAppend { aggregate: Some(_) })
    ));
    let mut t = db.transaction();
    t.append_one(agg(1), Expected::Any, ev("ok"))
        .append_free(vec![]);
    assert!(matches!(
        t.commit(),
        Err(Error::EmptyAppend { aggregate: None })
    ));
    assert_eq!(db.head(), 0);
    assert_eq!(db.aggregate_len(agg(1)).unwrap(), 0);
}
