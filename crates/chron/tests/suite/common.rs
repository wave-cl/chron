#![allow(dead_code)]

use std::fs;
use std::path::{Path, PathBuf};

use chron::{Db, Event, Fsync, NewEvent, Options, Uuid};

pub fn tmp() -> tempfile::TempDir {
    tempfile::tempdir().unwrap()
}

/// `Fsync::Never`: fast, and a process crash is the only crash these tests
/// simulate.
pub fn opts() -> Options {
    Options::new().fsync(Fsync::Never)
}

pub fn open(dir: impl AsRef<Path>) -> Db {
    Db::open(dir, opts()).unwrap()
}

pub fn open_with(dir: impl AsRef<Path>, opts: Options) -> Db {
    Db::open(dir, opts).unwrap()
}

pub fn ev(payload: &str) -> NewEvent {
    NewEvent::new(payload.to_owned())
}

pub fn tagged(payload: &str, tags: &[&str]) -> NewEvent {
    ev(payload).tags(tags.iter().map(|t| t.to_string()))
}

pub fn agg(n: u128) -> Uuid {
    Uuid::from_u128(n)
}

pub fn payloads(events: &[Event]) -> Vec<String> {
    events
        .iter()
        .map(|e| String::from_utf8(e.payload.to_vec()).unwrap())
        .collect()
}

pub fn positions(events: &[Event]) -> Vec<u64> {
    events.iter().map(|e| e.position).collect()
}

pub fn indices(events: &[Event]) -> Vec<u64> {
    events.iter().map(|e| e.index).collect()
}

pub fn segments(dir: &Path) -> Vec<PathBuf> {
    let mut out: Vec<PathBuf> = fs::read_dir(dir.join("segments"))
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|x| x == "seg"))
        .collect();
    out.sort();
    out
}

pub fn last_segment(dir: &Path) -> PathBuf {
    segments(dir).pop().unwrap()
}

pub fn file_len(path: &Path) -> u64 {
    fs::metadata(path).unwrap().len()
}

/// Removes the last `n` bytes of a file.
pub fn chop(path: &Path, n: u64) {
    let len = file_len(path);
    let f = fs::OpenOptions::new().write(true).open(path).unwrap();
    f.set_len(len - n).unwrap();
}

/// Flips one byte of a file.
pub fn flip(path: &Path, at: u64) {
    let mut bytes = fs::read(path).unwrap();
    bytes[at as usize] ^= 0xff;
    fs::write(path, bytes).unwrap();
}

/// Appends `n` single-event transactions to `id` with `Expected::Any`.
pub fn fill(db: &Db, id: Uuid, n: usize) {
    for i in 0..n {
        let mut t = db.transaction();
        t.append_one(id, chron::Expected::Any, ev(&format!("e{i}")));
        t.commit().unwrap();
    }
}
