use chron::{Error, Expected};

use crate::common::*;

#[test]
fn exact_conflict_fails_the_whole_transaction() {
    let d = tmp();
    let db = open(d.path());
    let (a, b) = (agg(1), agg(2));
    fill(&db, a, 1);
    let before = db.read_all(0, 100).unwrap();
    let seg_len = file_len(&last_segment(d.path()));

    let mut t = db.transaction();
    t.append_one(a, Expected::Exact(5), ev("a?"));
    t.append_one(b, Expected::New, ev("b?"));
    let err = t.commit().unwrap_err();
    match err {
        Error::ExpectedLength {
            aggregate,
            expected,
            actual,
        } => {
            assert_eq!(aggregate, a);
            assert_eq!(expected, Expected::Exact(5));
            assert_eq!(actual, 1);
        }
        other => panic!("unexpected {other:?}"),
    }
    assert_eq!(
        db.aggregate_len(b).unwrap(),
        0,
        "the other aggregate was not written"
    );
    assert!(db.read_aggregate(b, 0, 10).unwrap().is_empty());
    assert_eq!(db.head(), 1);
    assert_eq!(db.read_all(0, 100).unwrap(), before);
    assert_eq!(
        file_len(&last_segment(d.path())),
        seg_len,
        "nothing reached the segment"
    );

    let mut t = db.transaction();
    t.append_one(b, Expected::New, ev("b0"));
    assert_eq!(t.commit().unwrap().first, 1);
}

#[test]
fn new_on_existing_fails() {
    let d = tmp();
    let db = open(d.path());
    fill(&db, agg(1), 1);
    let mut t = db.transaction();
    t.append_one(agg(1), Expected::New, ev("x"));
    assert!(matches!(
        t.commit(),
        Err(Error::ExpectedLength {
            expected: Expected::New,
            actual: 1,
            ..
        })
    ));
    assert_eq!(db.head(), 1);
}

#[test]
fn exact_zero_is_new_and_exact_matches() {
    let d = tmp();
    let db = open(d.path());
    let mut t = db.transaction();
    t.append_one(agg(1), Expected::Exact(0), ev("x"));
    t.commit().unwrap();
    let mut t = db.transaction();
    t.append_one(agg(1), Expected::Exact(1), ev("y"));
    t.commit().unwrap();
    assert_eq!(db.aggregate_len(agg(1)).unwrap(), 2);
}

#[test]
fn any_skips_the_check() {
    let d = tmp();
    let db = open(d.path());
    fill(&db, agg(1), 2);
    let mut t = db.transaction();
    t.append_one(agg(1), Expected::Any, ev("x"));
    t.append_one(agg(2), Expected::Any, ev("y"));
    let c = t.commit().unwrap();
    assert_eq!(c.appends[0].first_index, 2);
    assert_eq!(c.appends[1].first_index, 0);
    assert_eq!(
        indices(&db.read_aggregate(agg(1), 0, 10).unwrap()),
        [0, 1, 2]
    );
}

#[test]
fn same_aggregate_twice_in_one_transaction() {
    let d = tmp();
    let db = open(d.path());
    let a = agg(1);
    let mut t = db.transaction();
    t.append_one(a, Expected::New, ev("0"));
    t.append(a, Expected::Exact(1), vec![ev("1"), ev("2")]);
    let c = t.commit().unwrap();
    assert_eq!((c.first, c.last), (0, 2));
    assert_eq!(c.appends[1].first_index, 1);
    assert_eq!(c.appends[1].new_len, 3);
    assert_eq!(db.aggregate_len(a).unwrap(), 3);
    assert_eq!(indices(&db.read_aggregate(a, 0, 10).unwrap()), [0, 1, 2]);

    let b = agg(2);
    let mut t = db.transaction();
    t.append_one(b, Expected::New, ev("0"));
    t.append_one(b, Expected::New, ev("1"));
    assert!(matches!(
        t.commit(),
        Err(Error::ExpectedLength { aggregate, expected: Expected::New, actual: 1 }) if aggregate == b
    ));
    assert_eq!(db.aggregate_len(b).unwrap(), 0);
    assert_eq!(db.head(), 3);
}

#[test]
fn multi_aggregate_commit_is_contiguous() {
    let d = tmp();
    let db = open(d.path());
    fill(&db, agg(9), 1);
    let mut t = db.transaction();
    t.append(agg(1), Expected::New, vec![ev("a0"), ev("a1")]);
    t.append_one(agg(2), Expected::New, ev("b0"));
    t.append(agg(3), Expected::New, vec![ev("c0"), ev("c1"), ev("c2")]);
    let c = t.commit().unwrap();
    assert_eq!((c.first, c.last, c.next), (1, 6, 7));
    let firsts: Vec<u64> = c.appends.iter().map(|a| a.first_position).collect();
    assert_eq!(firsts, [1, 3, 4]);
    let counts: Vec<u64> = c.appends.iter().map(|a| a.count).collect();
    assert_eq!(counts, [2, 1, 3]);
    let all = db.read_all(1, 100).unwrap();
    assert_eq!(payloads(&all), ["a0", "a1", "b0", "c0", "c1", "c2"]);
    let firsts: Vec<bool> = all.iter().map(|e| e.is_first_in_txn()).collect();
    let lasts: Vec<bool> = all.iter().map(|e| e.is_last_in_txn()).collect();
    assert_eq!(firsts, [true, false, false, false, false, false]);
    assert_eq!(lasts, [false, false, false, false, false, true]);
    assert!(all.iter().all(|e| e.recorded_at == all[0].recorded_at));
    assert_eq!(db.head(), 7);
}
