use std::fs;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;

use chron::{BackupKind, Db, Error, Expected, Options};

use crate::common::*;

/// Everything a reader can observe.
fn snapshot(
    db: &Db,
) -> (
    u64,
    u64,
    Vec<chron::Event>,
    Vec<chron::Event>,
    Vec<chron::Event>,
    u64,
    u64,
) {
    (
        db.head(),
        db.floor(),
        db.read_all(db.floor(), 100_000).unwrap(),
        db.read_aggregate(agg(1), 0, 100_000).unwrap(),
        db.read_tag("t", 0, 100_000).unwrap(),
        db.aggregate_len(agg(1)).unwrap(),
        db.aggregate_len(agg(2)).unwrap(),
    )
}

fn small() -> Options {
    opts().segment_max_bytes(300)
}

fn write_some(db: &Db, from: u64, n: u64) {
    for i in from..from + n {
        let mut t = db.transaction();
        let tags: &[&str] = if i % 3 == 0 { &["t"] } else { &[] };
        t.append_one(
            agg(1 + i as u128 % 2),
            Expected::Any,
            tagged(&format!("e{i}"), tags),
        );
        if i % 5 == 0 {
            t.append(agg(1), Expected::Any, vec![ev("x"), tagged("y", &["t"])]);
            t.append_free(vec![tagged("free", &["t"])]);
        }
        t.commit().unwrap();
    }
}

#[test]
fn a_full_backup_restores_an_identical_snapshot() {
    let d = tmp();
    let db = open_with(d.path().join("src"), small());
    write_some(&db, 0, 40);
    assert!(segments(&d.path().join("src")).len() > 3);
    let before = snapshot(&db);
    let archive = d.path().join("full.chronbak");
    let meta = db.backup(&archive).unwrap();
    assert_eq!(
        (meta.kind, meta.head, meta.floor, meta.base_head),
        (BackupKind::Full, before.0, 0, None)
    );
    assert_eq!(
        meta.files as usize,
        segments(&d.path().join("src")).len() + 1
    );
    assert!(meta.bytes > 0);
    let inspected = chron::inspect_backup(&archive).unwrap();
    assert_eq!(inspected, meta);
    assert!(!d.path().join("full.chronbak.tmp").exists());

    let restored = chron::restore_backup(&archive, d.path().join("dst"), &small()).unwrap();
    assert_eq!(restored.head, meta.head);
    assert!(!d.path().join(".dst.restore").exists());
    let dst = open_with(d.path().join("dst"), small().verify_all_segments(true));
    assert_eq!(snapshot(&dst), before);
    let len = dst.aggregate_len(agg(1)).unwrap();
    let mut t = dst.transaction();
    t.append_one(agg(1), Expected::Exact(len), ev("after"));
    let c = t.commit().unwrap();
    assert_eq!((c.first, c.appends[0].first_index), (before.0, len));
}

#[test]
fn a_backup_taken_during_appends_is_consistent_at_its_head() {
    let d = tmp();
    let src = d.path().join("src");
    let db = open_with(&src, small());
    let stop = Arc::new(AtomicBool::new(false));
    let writer = {
        let db = db.clone();
        let stop = stop.clone();
        thread::spawn(move || {
            let mut i = 0u64;
            while !stop.load(Ordering::Relaxed) {
                write_some(&db, i, 1);
                i += 1;
            }
        })
    };
    let mut archives = Vec::new();
    for k in 0..5 {
        thread::sleep(std::time::Duration::from_millis(20));
        let archive = d.path().join(format!("b{k}.chronbak"));
        let meta = db.backup(&archive).unwrap();
        archives.push((archive, meta));
    }
    stop.store(true, Ordering::Relaxed);
    writer.join().unwrap();
    assert!(
        archives.last().unwrap().1.head > archives[0].1.head,
        "the writer was running"
    );
    let source = db.read_all(0, 1_000_000).unwrap();
    for (k, (archive, meta)) in archives.iter().enumerate() {
        let dir = d.path().join(format!("r{k}"));
        chron::restore_backup(archive, &dir, &small()).unwrap();
        let r = open_with(&dir, small().verify_all_segments(true));
        assert_eq!(r.head(), meta.head, "backup {k}");
        assert_eq!(
            r.read_all(0, 1_000_000).unwrap(),
            source[..meta.head as usize],
            "backup {k}"
        );
        assert!(r.read_all(meta.head, 10).unwrap().is_empty());
        let mut t = r.transaction();
        t.append_free(vec![ev("after")]);
        assert_eq!(t.commit().unwrap().first, meta.head);
    }
}

#[test]
fn increments_chain_across_a_segment_roll() {
    let d = tmp();
    let src = d.path().join("src");
    let db = open_with(&src, small());
    write_some(&db, 0, 10);
    let full = d.path().join("full.chronbak");
    let h0 = db.backup(&full).unwrap().head;
    write_some(&db, 10, 20);
    let inc1 = d.path().join("inc1.chronbak");
    let m1 = db.backup_since(&inc1, h0).unwrap();
    assert_eq!((m1.kind, m1.base_head), (BackupKind::Incremental, Some(h0)));
    let h1 = m1.head;
    write_some(&db, 30, 20);
    let inc2 = d.path().join("inc2.chronbak");
    let h2 = db.backup_since(&inc2, h1).unwrap().head;
    assert!(segments(&src).len() > 4);
    let source = snapshot(&db);
    assert!(matches!(
        db.backup_since(d.path().join("x"), h2 + 1),
        Err(Error::PositionOutOfRange { .. })
    ));
    let empty = db
        .backup_since(d.path().join("empty.chronbak"), h2)
        .unwrap();
    assert_eq!((empty.base_head, empty.head), (Some(h2), h2));

    let dst = d.path().join("dst");
    chron::restore_backup(&full, &dst, &small()).unwrap();
    let r = open_with(&dst, small());
    assert_eq!(r.head(), h0);
    assert!(
        matches!(r.apply_backup(&inc2), Err(Error::WrongBaseHead { base_head, head, .. }) if base_head == h1 && head == h0)
    );
    assert_eq!(r.apply_backup(&inc1).unwrap().head, h1);
    assert_eq!(r.head(), h1);
    assert!(matches!(
        r.apply_backup(&inc1),
        Err(Error::WrongBaseHead { .. })
    ));
    assert_eq!(r.apply_backup(&inc2).unwrap().head, h2);
    r.apply_backup(d.path().join("empty.chronbak")).unwrap();
    assert_eq!(snapshot(&r), source);
    drop(r);
    let r = open_with(&dst, small().verify_all_segments(true));
    assert_eq!(snapshot(&r), source);
    assert!(
        segments(&dst).len() > 4,
        "the applied frames rolled segments too"
    );
}

#[test]
fn apply_refuses_a_damaged_archive_and_the_wrong_kind() {
    let d = tmp();
    let db = open(d.path().join("src"));
    write_some(&db, 0, 5);
    let full = d.path().join("full.chronbak");
    let h0 = db.backup(&full).unwrap().head;
    write_some(&db, 5, 5);
    let inc = d.path().join("inc.chronbak");
    db.backup_since(&inc, h0).unwrap();

    let dst = d.path().join("dst");
    chron::restore_backup(&full, &dst, &opts()).unwrap();
    let r = open(&dst);
    let before = snapshot(&r);
    let e = r.apply_backup(&full).unwrap_err();
    assert!(
        matches!(
            e,
            Error::WrongBackupKind {
                kind: BackupKind::Full,
                ..
            }
        ),
        "{e:?}"
    );
    let damaged = d.path().join("damaged.chronbak");
    fs::copy(&inc, &damaged).unwrap();
    flip(&damaged, file_len(&damaged) - 40);
    assert!(matches!(
        r.apply_backup(&damaged),
        Err(Error::Corrupt { .. })
    ));
    assert_eq!(snapshot(&r), before, "nothing was written");
    chop(&damaged, 2);
    assert!(matches!(
        r.apply_backup(&damaged),
        Err(Error::Corrupt { .. })
    ));
    assert_eq!(snapshot(&r), before);
    // Intact frames, missing trailer: only the archive crc can tell.
    let cut = d.path().join("cut.chronbak");
    fs::copy(&inc, &cut).unwrap();
    chop(&cut, 2);
    assert!(matches!(r.apply_backup(&cut), Err(Error::Corrupt { .. })));
    assert_eq!(snapshot(&r), before);
    r.apply_backup(&inc).unwrap();
    assert_eq!(r.head(), db.head());
}

#[test]
fn restore_refuses_an_existing_dir_and_a_damaged_archive_leaves_no_dir() {
    let d = tmp();
    let db = open(d.path().join("src"));
    write_some(&db, 0, 5);
    let full = d.path().join("full.chronbak");
    db.backup(&full).unwrap();
    let inc = d.path().join("inc.chronbak");
    db.backup_since(&inc, 0).unwrap();

    assert!(matches!(
        chron::restore_backup(&full, d.path().join("src"), &opts()),
        Err(Error::AlreadyExists { .. })
    ));
    assert!(matches!(
        chron::restore_backup(&inc, d.path().join("dst"), &opts()),
        Err(Error::WrongBackupKind {
            kind: BackupKind::Incremental,
            ..
        })
    ));
    let damaged = d.path().join("damaged.chronbak");
    fs::copy(&full, &damaged).unwrap();
    flip(&damaged, file_len(&damaged) - 30);
    assert!(matches!(
        chron::restore_backup(&damaged, d.path().join("dst"), &opts()),
        Err(Error::Corrupt { .. })
    ));
    assert!(!d.path().join("dst").exists());
    assert!(!d.path().join(".dst.restore").exists());
    chop(&damaged, 3);
    assert!(matches!(
        chron::restore_backup(&damaged, d.path().join("dst"), &opts()),
        Err(Error::Corrupt { .. })
    ));
    assert!(!d.path().join("dst").exists());
    assert!(matches!(
        chron::restore_backup(d.path().join("missing"), d.path().join("dst"), &opts()),
        Err(Error::Io { .. })
    ));
}

#[test]
fn inspect_reads_the_header_only() {
    let d = tmp();
    let db = open(d.path().join("src"));
    write_some(&db, 0, 3);
    let full = d.path().join("full.chronbak");
    let meta = db.backup(&full).unwrap();
    let bytes = fs::read(&full).unwrap();
    let header_len = u32::from_be_bytes(bytes[8..12].try_into().unwrap()) as usize;
    let short = d.path().join("short.chronbak");
    fs::write(&short, &bytes[..12 + header_len + 1]).unwrap();
    let inspected = chron::inspect_backup(&short).unwrap();
    assert_eq!(
        (inspected.head, inspected.kind),
        (meta.head, BackupKind::Full)
    );
    assert!(matches!(
        chron::restore_backup(&short, d.path().join("dst"), &opts()),
        Err(Error::Corrupt { .. })
    ));
    fs::write(&short, b"NOTABACKUP").unwrap();
    assert!(matches!(
        chron::inspect_backup(&short),
        Err(Error::Corrupt { .. })
    ));
}

#[test]
fn restore_of_a_retained_db_keeps_the_floor_and_an_empty_db_round_trips() {
    let d = tmp();
    let db = open_with(d.path().join("src"), small());
    write_some(&db, 0, 40);
    let floor = db.retain_from(db.head() / 2).unwrap().floor;
    assert!(floor > 0);
    let before = snapshot(&db);
    let archive = d.path().join("retained.chronbak");
    let meta = db.backup(&archive).unwrap();
    assert_eq!(meta.floor, floor);
    chron::restore_backup(&archive, d.path().join("dst"), &small()).unwrap();
    let r = open_with(d.path().join("dst"), small().verify_all_segments(true));
    assert_eq!(r.floor(), floor);
    assert_eq!(snapshot(&r), before);
    assert!(matches!(r.read_all(0, 1), Err(Error::BelowFloor { .. })));
    assert!(matches!(
        db.backup_since(d.path().join("x"), 0),
        Err(Error::BelowFloor { .. })
    ));

    let empty = open(d.path().join("empty"));
    let archive = d.path().join("empty.chronbak");
    let meta = empty.backup(&archive).unwrap();
    assert_eq!(
        (meta.head, meta.files),
        (0, 2),
        "one segment entry and the lengths entry"
    );
    chron::restore_backup(&archive, d.path().join("empty2"), &opts()).unwrap();
    let r = open(d.path().join("empty2"));
    assert_eq!(r.head(), 0);
    fill(&r, agg(1), 1);
    assert_eq!(r.head(), 1);
}

#[test]
fn a_streamed_backup_matches_the_file_and_restores_from_a_reader() {
    let d = tmp();
    let db = open_with(d.path(), small());
    write_some(&db, 0, 40);
    let file = d.path().join("full.chronbak");
    let meta_file = db.backup(&file).unwrap();
    let mut bytes = Vec::new();
    let meta_stream = db.backup_to(&mut bytes).unwrap();
    assert_eq!(meta_stream.bytes, bytes.len() as u64);
    assert_eq!(
        (
            meta_stream.head,
            meta_stream.floor,
            meta_stream.db_id,
            meta_stream.files
        ),
        (
            meta_file.head,
            meta_file.floor,
            meta_file.db_id,
            meta_file.files
        )
    );
    // Identical between the header (which carries its own timestamp) and
    // the crc (which covers the header).
    let on_disk = std::fs::read(&file).unwrap();
    let skip = |b: &[u8]| 12 + u32::from_be_bytes(b[8..12].try_into().unwrap()) as usize;
    assert_eq!(
        &on_disk[skip(&on_disk)..on_disk.len() - 4],
        &bytes[skip(&bytes)..bytes.len() - 4]
    );
    let target = d.path().join("restored");
    let meta = chron::restore_backup_from(std::io::Cursor::new(bytes), &target, &small()).unwrap();
    assert_eq!(meta.head, meta_file.head);
    let restored = open_with(&target, small());
    assert_eq!(snapshot(&restored), snapshot(&db));
}

#[test]
fn a_damaged_or_truncated_stream_is_refused_and_leaves_nothing() {
    let d = tmp();
    let db = open_with(d.path(), small());
    write_some(&db, 0, 40);
    let mut bytes = Vec::new();
    db.backup_to(&mut bytes).unwrap();
    let target = d.path().join("restored");
    let staging = d.path().join(".restored.restore");
    let refused = |bytes: Vec<u8>, what: &str| {
        let r = chron::restore_backup_from(std::io::Cursor::new(bytes), &target, &small());
        assert!(matches!(r, Err(Error::Corrupt { .. })), "{what}: {r:?}");
        assert!(
            !target.exists() && !staging.exists(),
            "{what} left a directory"
        );
    };
    // A flipped byte inside the agg_len entry: only the crc can tell.
    let mut flipped = bytes.clone();
    let n = flipped.len();
    flipped[n - 30] ^= 0x01;
    refused(flipped, "a flipped byte");
    refused(bytes[..bytes.len() - 3].to_vec(), "a chopped trailer");
    let mut bad_header = bytes.clone();
    bad_header[8..12].copy_from_slice(&0xFFFF_FFF0u32.to_be_bytes());
    refused(bad_header, "an implausible header length");
    // An entry claiming u64::MAX bytes: refused when the stream ends, not
    // by allocating that much.
    let header_len = u32::from_be_bytes(bytes[8..12].try_into().unwrap()) as usize;
    let first_entry = 12 + header_len;
    let name_len =
        u16::from_be_bytes(bytes[first_entry + 1..first_entry + 3].try_into().unwrap()) as usize;
    let len_at = first_entry + 3 + name_len;
    let mut huge = bytes.clone();
    huge[len_at..len_at + 8].copy_from_slice(&u64::MAX.to_be_bytes());
    refused(huge, "a huge entry length");
}
