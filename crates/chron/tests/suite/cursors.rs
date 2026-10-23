//! Durable cursors: stored in the index, kept across a reopen and a
//! rebuild, clamped by a truncation, carried by a full backup.

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
    // A rebuilt index keeps them; so does a restored backup.
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
    assert_eq!(
        restored.cursors().unwrap(),
        [CursorState {
            name: "p".into(),
            next: 5,
            session: 1
        }],
        "the restore kept it"
    );
}

/// A full backup carries every cursor; a restore keeps them, clamping
/// one past the archived head (a follower stores the leader's unclamped).
#[test]
fn a_backup_carries_cursors_and_a_restore_clamps_them() {
    let d = tmp();
    let db = open(d.path());
    fill(&db, agg(1), 10);
    db.set_cursor("a", 4, 2).unwrap();
    db.set_cursor("z", 15, 7).unwrap();
    let archive = d.path().join("full.chronbak");
    let meta = db.backup(&archive).unwrap();
    assert_eq!(meta.format, 2, "an archive holding cursors says so");
    assert_eq!(chron::inspect_backup(&archive).unwrap().format, 2);
    let dst = d.path().join("restored");
    chron::restore_backup(&archive, &dst, &opts()).unwrap();
    let got: Vec<(String, u64, u64)> = open(&dst)
        .cursors()
        .unwrap()
        .into_iter()
        .map(|c| (c.name, c.next, c.session))
        .collect();
    assert_eq!(
        got,
        [("a".to_string(), 4, 2), ("z".to_string(), 10, 7)],
        "kept; the one past the head clamped to it"
    );
}

/// A database without cursors writes the first format, which every
/// reader knows; the archive restores without cursors.
#[test]
fn an_archive_without_cursors_keeps_the_first_format() {
    let d = tmp();
    let db = open(d.path());
    fill(&db, agg(1), 3);
    let archive = d.path().join("full.chronbak");
    assert_eq!(db.backup(&archive).unwrap().format, 1);
    let dst = d.path().join("restored");
    chron::restore_backup(&archive, &dst, &opts()).unwrap();
    assert!(open(&dst).cursors().unwrap().is_empty());
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
