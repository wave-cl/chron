use chron::{Db, Error, Expected, Fsync, Options};

use crate::common::*;

#[test]
fn reopen_preserves_everything() {
    let d = tmp();
    let snapshot = |db: &Db| {
        (
            db.head(),
            db.read_all(0, 1000).unwrap(),
            db.read_aggregate(agg(1), 0, 1000).unwrap(),
            db.read_aggregate(agg(2), 0, 1000).unwrap(),
            db.read_tag("t", 0, 1000).unwrap(),
            db.aggregate_len(agg(1)).unwrap(),
        )
    };
    let before = {
        let db = open(d.path());
        for i in 0..20 {
            let mut t = db.transaction();
            t.append_one(
                agg(1 + i % 2),
                Expected::Any,
                tagged(&format!("e{i}"), if i % 3 == 0 { &["t"] } else { &[] }),
            );
            t.commit().unwrap();
        }
        snapshot(&db)
    };
    let db = open(d.path());
    assert_eq!(snapshot(&db), before);
    assert_eq!(db.head(), 20);
    let mut t = db.transaction();
    t.append_one(agg(1), Expected::Exact(10), ev("more"));
    assert_eq!(t.commit().unwrap().first, 20);
    drop(db);
    let db = open(d.path());
    assert_eq!(db.head(), 21);
    assert_eq!(payloads(&db.read_all(20, 10).unwrap()), ["more"]);
    assert_eq!(db.aggregate_len(agg(1)).unwrap(), 11);
}

#[test]
fn lock_refuses_a_second_open() {
    let d = tmp();
    let db = open(d.path());
    assert!(matches!(
        Db::open(d.path(), opts()),
        Err(Error::Locked { .. })
    ));
    drop(db);
    open(d.path());
}

#[test]
fn fsync_never_flush_and_drop_persist() {
    let d = tmp();
    let o = Options::new().fsync(Fsync::Never);
    let db = open_with(d.path(), o.clone());
    fill(&db, agg(1), 2);
    db.flush().unwrap();
    fill(&db, agg(1), 1);
    drop(db);
    let db = open_with(d.path(), o);
    assert_eq!(db.head(), 3);
    assert_eq!(db.aggregate_len(agg(1)).unwrap(), 3);
}

#[test]
fn wait_for_sees_a_commit_from_another_thread() {
    let d = tmp();
    let db = open(d.path());
    assert_eq!(db.wait_for(1, std::time::Duration::from_millis(50)), 0);
    let writer = db.clone();
    let h = std::thread::spawn(move || {
        std::thread::sleep(std::time::Duration::from_millis(50));
        fill(&writer, agg(1), 1);
    });
    assert_eq!(db.wait_for(1, std::time::Duration::from_secs(10)), 1);
    h.join().unwrap();
}
