use chron::{Expected, FLAG_FREE};

use crate::common::*;

#[test]
fn free_events_take_positions_tags_and_flags() {
    let d = tmp();
    let db = open(d.path());
    fill(&db, agg(1), 1);
    let mut t = db.transaction();
    t.append_free(vec![tagged("f0", &["t"]), ev("f1")]);
    t.append_one(agg(1), Expected::Exact(1), tagged("a1", &["t"]));
    t.append_free(vec![tagged("f2", &["t"])]);
    let c = t.commit().unwrap();
    assert_eq!((c.first, c.last), (1, 4));
    assert_eq!(c.appends[0].aggregate, None);
    assert_eq!(c.appends[0].first_position, 1);
    assert_eq!(c.appends[0].count, 2);
    assert_eq!(c.appends[2].first_position, 4);

    let all = db.read_all(0, 100).unwrap();
    assert_eq!(payloads(&all), ["e0", "f0", "f1", "a1", "f2"]);
    assert!(all[1].is_free() && all[2].is_free() && all[4].is_free());
    assert_eq!(all[1].aggregate, None);
    assert_eq!(all[1].index, 0);
    assert_eq!(all[1].flags & FLAG_FREE, FLAG_FREE);
    assert!(all[1].is_first_in_txn() && !all[1].is_last_in_txn());
    assert!(all[4].is_last_in_txn());
    assert_eq!(all[3].aggregate, Some(agg(1)));
    assert_eq!(all[3].index, 1);

    assert_eq!(
        payloads(&db.read_tag("t", 0, 100).unwrap()),
        ["f0", "a1", "f2"]
    );
    assert_eq!(db.aggregate_len(agg(1)).unwrap(), 2);
    assert_eq!(indices(&db.read_aggregate(agg(1), 0, 10).unwrap()), [0, 1]);
    assert_eq!(db.head(), 5);
}

#[test]
fn free_events_do_not_rescue_a_failed_transaction() {
    let d = tmp();
    let db = open(d.path());
    let mut t = db.transaction();
    t.append_free(vec![ev("f")]);
    t.append_one(agg(1), Expected::Exact(3), ev("x"));
    assert!(t.commit().is_err());
    assert_eq!(db.head(), 0);
    assert!(db.read_all(0, 10).unwrap().is_empty());
}

#[test]
fn free_events_survive_reopen() {
    let d = tmp();
    {
        let db = open(d.path());
        let mut t = db.transaction();
        t.append_free(vec![tagged("f", &["t"])]);
        t.commit().unwrap();
    }
    let db = open(d.path());
    let all = db.read_all(0, 10).unwrap();
    assert_eq!(payloads(&all), ["f"]);
    assert!(all[0].is_free());
    assert_eq!(payloads(&db.read_tag("t", 0, 10).unwrap()), ["f"]);
}
