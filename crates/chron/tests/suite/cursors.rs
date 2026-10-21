//! Durable cursors: stored in the index, kept across a reopen and a
//! rebuild, clamped by a truncation, absent from a restored backup.

use chron::CursorState;

use crate::common::*;

#[test]
fn cursors_persist_reopen_and_clamp_on_truncate() {
    let d = tmp();
    let db = open(d.path());
    fill(&db, agg(1), 10);
    assert_eq!(db.cursor("p").unwrap(), None);
    assert!(db.cursors().unwrap().is_empty());
    db.set_cursor("p", 7, 1).unwrap();
    db.set_cursor("q", 2, 3).unwrap();
    assert_eq!(
        db.cursor("p").unwrap(),
        Some(CursorState {
            name: "p".into(),
            next: 7,
            session: 1
        })
    );
    drop(db);
    let db = open(d.path());
    let names: Vec<String> = db.cursors().unwrap().into_iter().map(|c| c.name).collect();
    assert_eq!(names, ["p", "q"], "a reopen keeps them");
    db.truncate_to(5).unwrap();
    assert_eq!(
        db.cursor("p").unwrap().unwrap().next,
        5,
        "clamped to the cut"
    );
    assert_eq!(db.cursor("p").unwrap().unwrap().session, 1);
    assert_eq!(
        db.cursor("q").unwrap().unwrap().next,
        2,
        "below the cut, untouched"
    );
    assert!(db.remove_cursor("q").unwrap());
    assert!(!db.remove_cursor("q").unwrap());
    assert_eq!(db.cursors().unwrap().len(), 1);
    drop(db);
    // A rebuilt index keeps them; a restored backup starts without.
    let archive = d.path().join("full.chronbak");
    {
        let db = open(d.path());
        db.backup(&archive).unwrap();
    }
    chron::Db::rebuild_index(d.path(), &opts()).unwrap();
    let db = open(d.path());
    assert_eq!(
        db.cursor("p").unwrap().unwrap().next,
        5,
        "the rebuild kept it"
    );
    drop(db);
    let dst = d.path().join("restored");
    chron::restore_backup(&archive, &dst, &opts()).unwrap();
    let restored = open(&dst);
    assert!(restored.cursors().unwrap().is_empty());
}

/// An index from before cursors existed reads as having none.
#[test]
fn an_old_index_without_the_table_reads_as_empty() {
    let d = tmp();
    let db = open(d.path());
    fill(&db, agg(1), 3);
    db.debug_drop_cursor_table().unwrap();
    assert_eq!(db.cursor("p").unwrap(), None);
    assert!(db.cursors().unwrap().is_empty());
    db.set_cursor("p", 1, 1).unwrap();
    assert_eq!(db.cursor("p").unwrap().unwrap().next, 1);
}
