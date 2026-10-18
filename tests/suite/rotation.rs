use chron::Expected;

use crate::common::*;

#[test]
fn segments_rotate_and_reads_cross_them() {
    let d = tmp();
    let o = opts().segment_max_bytes(700);
    let db = open_with(d.path(), o.clone());
    for i in 0..300u64 {
        let mut t = db.transaction();
        t.append_one(
            agg(1 + i as u128 % 3),
            Expected::Any,
            tagged(&format!("e{i}"), &["t"]),
        );
        t.commit().unwrap();
    }
    assert!(segments(d.path()).len() > 3);
    let all = db.read_all(0, 1000).unwrap();
    assert_eq!(positions(&all), (0..300).collect::<Vec<_>>());
    assert_eq!(db.read_aggregate(agg(1), 0, 1000).unwrap().len(), 100);
    assert_eq!(db.read_tag("t", 0, 1000).unwrap().len(), 300);
    let bases: std::collections::BTreeSet<u64> = (0..300)
        .map(|p| db.debug_locate(p).unwrap().unwrap().0)
        .collect();
    assert!(bases.len() > 3);

    // A multi-event transaction lands wholly in one segment.
    let mut t = db.transaction();
    t.append(
        agg(7),
        Expected::New,
        (0..40).map(|i| ev(&format!("big{i}"))).collect(),
    );
    let c = t.commit().unwrap();
    let first_base = db.debug_locate(c.first).unwrap().unwrap().0;
    let last_base = db.debug_locate(c.last).unwrap().unwrap().0;
    assert_eq!(first_base, last_base);
    drop(db);

    let db = open_with(d.path(), o);
    assert_eq!(db.head(), 340);
    let n = segments(d.path()).len();
    fill(&db, agg(1), 1);
    assert_eq!(db.head(), 341);
    assert!(segments(d.path()).len() >= n);
    assert_eq!(db.read_all(0, 1000).unwrap().len(), 341);
}
