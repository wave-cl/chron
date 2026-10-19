//! Backups: one archive file holding segment bytes cut exactly at the
//! archived head (full) or the frames of a position range (incremental).
//! A restore writes the segments back and rebuilds the index from them.
//!
//! ```text
//! "CHRONBAK" | u32 header_len | header: JSON BackupMeta
//! entries: u8 kind | u16 name_len | name | u64 len | bytes
//!   kind 1  a segment file, "segments/<base:020>.seg"   (full backups)
//!   kind 2  "records": frames of positions [base_head, head)   (incremental)
//!   kind 3  "agg_len": (uuid, u64 length) pairs                  (full backups)
//! u8 0xFF | u32 crc32 over everything before it
//! ```

use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::db::{Inner, Layout};
use crate::error::{Error, Result};
use crate::options::Options;
use crate::rebuild;
use crate::segment::{self, HEADER_LEN, SegmentHeader};
use crate::txn;

const MAGIC: &[u8; 8] = b"CHRONBAK";
const FORMAT: u32 = 1;
const KIND_SEGMENT: u8 = 1;
const KIND_RECORDS: u8 = 2;
/// Every aggregate's length, (16-byte uuid, u64) pairs: lengths of
/// aggregates whose events all lie below the floor exist nowhere else.
const KIND_AGG_LEN: u8 = 3;
const KIND_END: u8 = 0xFF;
const MAX_HEADER: u32 = 16 * 1024 * 1024;
const COPY_CHUNK: usize = 1 << 20;

/// Full: a snapshot to restore into a new directory. Incremental: a range
/// of events to apply onto a database at the archive's base head.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum BackupKind {
    #[default]
    /// A snapshot to restore into a new directory.
    Full,
    /// A range of events to apply onto a database at the archive's base head.
    Incremental,
}

/// What an archive holds; written in its header.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackupMeta {
    /// The archive format version.
    pub format: u32,
    #[serde(default)]
    /// Full or incremental.
    pub kind: BackupKind,
    /// Incremental only: the head the target must be at.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_head: Option<u64>,
    /// Every position below it (and at or above the floor, or the base
    /// head) is in the archive.
    pub head: u64,
    #[serde(default)]
    /// The retention floor at the time: nothing below it is in the archive.
    pub floor: u64,
    /// When the archive was written.
    pub created_at_unix_nanos: i64,
    /// Entries in the archive.
    #[serde(default)]
    pub files: u64,
    /// Size of the archive file; 0 until it is complete.
    #[serde(default)]
    pub bytes: u64,
    /// The identity of the database the archive came from.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub db_id: Option<Uuid>,
}

fn now_nanos() -> i64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_nanos() as i64,
        Err(e) => -(e.duration().as_nanos() as i64),
    }
}

/// Hashes and counts everything written through it.
struct Crc<W: Write> {
    inner: W,
    hasher: crc32fast::Hasher,
    written: u64,
}

impl<W: Write> Write for Crc<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let n = self.inner.write(buf)?;
        self.hasher.update(&buf[..n]);
        self.written += n as u64;
        Ok(n)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

/// Writes an archive to any sink: the file-based callers wrap a
/// temporary file around it and rename it into place when done.
struct Writer<W: Write> {
    /// What errors name: the file, or "<backup stream>".
    what: PathBuf,
    out: Crc<W>,
}

/// A temporary file removed on drop unless kept.
struct TmpFile {
    path: PathBuf,
    keep: bool,
}

impl Drop for TmpFile {
    fn drop(&mut self) {
        if !self.keep {
            let _ = fs::remove_file(&self.path);
        }
    }
}

impl<W: Write> Writer<W> {
    fn start(out: W, meta: &BackupMeta, what: &Path) -> Result<Self> {
        let header = serde_json::to_vec(meta)
            .map_err(|e| Error::corrupt(what, 0, format!("cannot encode the header: {e}")))?;
        let mut w = Writer {
            what: what.to_path_buf(),
            out: Crc {
                inner: out,
                hasher: crc32fast::Hasher::new(),
                written: 0,
            },
        };
        w.bytes(MAGIC)?;
        w.bytes(&(header.len() as u32).to_be_bytes())?;
        w.bytes(&header)?;
        Ok(w)
    }

    fn entry_header(&mut self, kind: u8, name: &str, len: u64) -> Result<()> {
        self.bytes(&[kind])?;
        self.bytes(&(name.len() as u16).to_be_bytes())?;
        self.bytes(name.as_bytes())?;
        self.bytes(&len.to_be_bytes())
    }

    fn bytes(&mut self, bytes: &[u8]) -> Result<()> {
        let what = self.what.clone();
        self.out
            .write_all(bytes)
            .map_err(|e| Error::io(&what, "write", e))
    }

    /// Copies exactly `len` bytes of `file` from offset 0, by positional
    /// reads (the file may be the live tail segment, which keeps growing).
    fn copy_file(&mut self, file: &File, path: &Path, len: u64) -> Result<()> {
        use std::os::unix::fs::FileExt;
        let mut buf = vec![0u8; COPY_CHUNK];
        let mut at = 0u64;
        while at < len {
            let n = ((len - at) as usize).min(COPY_CHUNK);
            file.read_exact_at(&mut buf[..n], at).map_err(|e| {
                if e.kind() == std::io::ErrorKind::UnexpectedEof {
                    Error::corrupt(path, at, "segment shorter than the index says")
                } else {
                    Error::io(path, "read", e)
                }
            })?;
            self.bytes(&buf[..n])?;
            at += n as u64;
        }
        Ok(())
    }

    /// Writes the end marker and the crc, flushes, and returns the bytes
    /// written in all.
    fn finish(mut self) -> Result<u64> {
        self.bytes(&[KIND_END])?;
        let crc = self.out.hasher.clone().finalize();
        let what = self.what.clone();
        self.out
            .inner
            .write_all(&crc.to_be_bytes())
            .and_then(|()| self.out.inner.flush())
            .map_err(|e| Error::io(&what, "write", e))?;
        Ok(self.out.written + 4)
    }
}

/// Writes the archive `write` produces to `archive` through a temporary
/// file beside it, fsyncs it and renames it into place; a failure leaves
/// nothing behind.
fn write_file(
    archive: &Path,
    write: impl FnOnce(&mut BufWriter<File>, &Path) -> Result<BackupMeta>,
) -> Result<BackupMeta> {
    let tmp = TmpFile {
        path: PathBuf::from(format!(
            "{}.{}.{}.tmp",
            archive.display(),
            std::process::id(),
            now_nanos()
        )),
        keep: false,
    };
    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&tmp.path)
        .map_err(|e| Error::io(&tmp.path, "create", e))?;
    let mut out = BufWriter::new(file);
    let mut meta = write(&mut out, &tmp.path)?;
    let file = out
        .into_inner()
        .map_err(|e| Error::io(&tmp.path, "flush", e.into_error()))?;
    file.sync_all()
        .map_err(|e| Error::io(&tmp.path, "fsync", e))?;
    meta.bytes = file
        .metadata()
        .map_err(|e| Error::io(&tmp.path, "metadata", e))?
        .len();
    drop(file);
    fs::rename(&tmp.path, archive).map_err(|e| Error::io(&tmp.path, "rename", e))?;
    let mut tmp = tmp;
    tmp.keep = true;
    if let Some(parent) = archive.parent().filter(|p| !p.as_os_str().is_empty()) {
        segment::sync_dir(parent)?;
    }
    Ok(meta)
}

/// Where the committed bytes of each segment end, with the head and floor
/// they were cut at.
struct Plan {
    head: u64,
    floor: u64,
    segments: Vec<(u64, PathBuf, u64)>,
}

/// Which segments a full backup takes and how much of each: every one with
/// a base in `floor..=head`, cut where the index places its last record
/// below the head (a segment at the head is header-only). The index, not
/// the file length, decides: bytes past the last committed record may be a
/// failed write's leftovers.
fn plan(inner: &Inner) -> Result<Plan> {
    let head = inner.head.load(Ordering::Acquire);
    let floor = inner.floor.load(Ordering::Acquire);
    let listed = segment::list(&inner.layout.segments_dir())?;
    let kept: Vec<_> = listed
        .into_iter()
        .filter(|(base, _)| *base >= floor && *base <= head)
        .collect();
    let mut segments = Vec::with_capacity(kept.len());
    for (k, (base, path)) in kept.iter().enumerate() {
        let next = kept.get(k + 1).map_or(u64::MAX, |(b, _)| *b);
        let end_excl = next.min(head);
        let len = if end_excl <= *base {
            HEADER_LEN
        } else {
            let loc = inner
                .index
                .locate(end_excl - 1)?
                .ok_or_else(|| Error::Inconsistent {
                    position: end_excl - 1,
                    reason: "position below the head is missing from the index".into(),
                })?;
            if loc.base != *base {
                return Err(Error::corrupt(
                    path,
                    0,
                    format!(
                        "index places position {} in segment {}, not here",
                        end_excl - 1,
                        loc.base
                    ),
                ));
            }
            loc.offset + u64::from(loc.frame_len)
        };
        segments.push((*base, path.clone(), len));
    }
    Ok(Plan {
        head,
        floor,
        segments,
    })
}

pub(crate) fn write_full(inner: &Inner, archive: &Path) -> Result<BackupMeta> {
    write_file(archive, |out, what| write_full_to(inner, out, what))
}

/// A full backup's bytes to any sink; `meta.bytes` counts them. Holds the
/// maintenance lock for the duration, so a slow sink holds up retention
/// and other backups of this database, not commits.
pub(crate) fn write_full_to(
    inner: &Inner,
    out: &mut impl Write,
    what: &Path,
) -> Result<BackupMeta> {
    let _m = inner.maintenance.lock().unwrap_or_else(|p| p.into_inner());
    let plan = plan(inner)?;
    let mut meta = BackupMeta {
        format: FORMAT,
        kind: BackupKind::Full,
        base_head: None,
        head: plan.head,
        floor: plan.floor,
        created_at_unix_nanos: now_nanos(),
        files: plan.segments.len() as u64 + 1,
        bytes: 0,
        db_id: Some(*inner.db_id.lock().unwrap_or_else(|p| p.into_inner())),
    };
    let mut w = Writer::start(out, &meta, what)?;
    let lens = inner.index.aggregate_lens()?;
    for (base, path, len) in &plan.segments {
        let name = format!("segments/{}", segment::file_name(*base));
        w.entry_header(KIND_SEGMENT, &name, *len)?;
        if *len == HEADER_LEN {
            // A segment holding no committed record: the writer may have
            // created the file and not yet written its header, so the
            // header is produced here rather than read.
            w.bytes(&SegmentHeader { base: *base }.encode())?;
            continue;
        }
        let file = inner.file(*base)?;
        w.copy_file(&file, path, *len)?;
    }
    w.entry_header(KIND_AGG_LEN, "agg_len", lens.len() as u64 * 24)?;
    for (id, len) in &lens {
        w.bytes(id.as_bytes())?;
        w.bytes(&len.to_be_bytes())?;
    }
    meta.bytes = w.finish()?;
    Ok(meta)
}

/// Decodes an `agg_len` entry.
fn parse_lens(bytes: &[u8], path: &Path, at: u64) -> Result<Vec<(Uuid, u64)>> {
    if !bytes.len().is_multiple_of(24) {
        return Err(Error::corrupt(
            path,
            at,
            "agg_len entry is not a whole number of pairs",
        ));
    }
    Ok(bytes
        .as_chunks::<24>()
        .0
        .iter()
        .map(|c| {
            (
                Uuid::from_bytes(c[..16].try_into().unwrap()),
                u64::from_be_bytes(c[16..].try_into().unwrap()),
            )
        })
        .collect())
}

/// The frames of positions `from..to`, back to back.
pub(crate) fn frames_between(inner: &Inner, from: u64, to: u64) -> Result<Vec<u8>> {
    use std::os::unix::fs::FileExt;
    let mut out = Vec::new();
    if from >= to {
        return Ok(out);
    }
    let listed = segment::list(&inner.layout.segments_dir())?;
    for (k, (base, path)) in listed.iter().enumerate() {
        let next = listed.get(k + 1).map_or(u64::MAX, |(b, _)| *b);
        let lo = from.max(*base);
        let hi = to.min(next);
        if lo >= hi {
            continue;
        }
        let locate = |p: u64| -> Result<crate::index::Location> {
            let loc = inner.index.locate(p)?.ok_or_else(|| Error::Inconsistent {
                position: p,
                reason: "position missing from the index".into(),
            })?;
            if loc.base != *base {
                return Err(Error::corrupt(
                    path,
                    0,
                    format!(
                        "index places position {p} in segment {}, not here",
                        loc.base
                    ),
                ));
            }
            Ok(loc)
        };
        let first = locate(lo)?;
        let last = locate(hi - 1)?;
        let start = first.offset;
        let end = last.offset + u64::from(last.frame_len);
        let file = inner.file(*base)?;
        let span = end.checked_sub(start).ok_or_else(|| Error::Inconsistent {
            position: lo,
            reason: format!("positions {lo}..{hi} are placed out of order in segment {base}"),
        })?;
        let at = out.len();
        out.resize(at + span as usize, 0);
        file.read_exact_at(&mut out[at..], start)
            .map_err(|e| Error::io(path, "read", e))?;
    }
    Ok(out)
}

pub(crate) fn write_incremental(inner: &Inner, archive: &Path, since: u64) -> Result<BackupMeta> {
    write_file(archive, |out, what| {
        write_incremental_to(inner, out, what, since)
    })
}

/// An incremental backup's bytes to any sink (the frames are gathered in
/// memory first).
fn write_incremental_to(
    inner: &Inner,
    out: &mut impl Write,
    what: &Path,
    since: u64,
) -> Result<BackupMeta> {
    let _m = inner.maintenance.lock().unwrap_or_else(|p| p.into_inner());
    let head = inner.head.load(Ordering::Acquire);
    let floor = inner.floor.load(Ordering::Acquire);
    if since > head {
        return Err(Error::PositionOutOfRange {
            position: since,
            head,
        });
    }
    if since < floor {
        return Err(Error::BelowFloor {
            position: since,
            floor,
        });
    }
    let frames = frames_between(inner, since, head)?;
    let mut meta = BackupMeta {
        format: FORMAT,
        kind: BackupKind::Incremental,
        base_head: Some(since),
        head,
        floor,
        created_at_unix_nanos: now_nanos(),
        files: 1,
        bytes: 0,
        db_id: Some(*inner.db_id.lock().unwrap_or_else(|p| p.into_inner())),
    };
    let mut w = Writer::start(out, &meta, what)?;
    w.entry_header(KIND_RECORDS, "records", frames.len() as u64)?;
    w.bytes(&frames)?;
    meta.bytes = w.finish()?;
    Ok(meta)
}

/// Reads an archive front to back from any source, hashing everything
/// after the header; the crc trailer is the integrity check, so nothing
/// here needs the total length.
struct Reader<R: Read> {
    path: PathBuf,
    src: R,
    hasher: crc32fast::Hasher,
    at: u64,
}

impl Reader<BufReader<File>> {
    fn open(archive: &Path) -> Result<(Self, BackupMeta)> {
        let file = File::open(archive).map_err(|e| Error::io(archive, "open", e))?;
        Reader::from_read(BufReader::new(file), archive)
    }
}

impl<R: Read> Reader<R> {
    fn from_read(src: R, what: &Path) -> Result<(Self, BackupMeta)> {
        let mut r = Reader {
            path: what.to_path_buf(),
            src,
            hasher: crc32fast::Hasher::new(),
            at: 0,
        };
        let mut magic = [0u8; 8];
        r.read_hashed(&mut magic)?;
        if &magic != MAGIC {
            return Err(Error::corrupt(what, 0, "not a chron backup"));
        }
        let mut len = [0u8; 4];
        r.read_hashed(&mut len)?;
        let len = u32::from_be_bytes(len);
        if len == 0 || len > MAX_HEADER {
            return Err(Error::corrupt(
                what,
                8,
                format!("implausible header length {len}"),
            ));
        }
        let mut header = vec![0u8; len as usize];
        r.read_hashed(&mut header)?;
        let meta: BackupMeta = serde_json::from_slice(&header)
            .map_err(|e| Error::corrupt(what, 12, format!("bad header: {e}")))?;
        if meta.format != FORMAT {
            return Err(Error::corrupt(
                what,
                12,
                format!("unknown format {}", meta.format),
            ));
        }
        Ok((r, meta))
    }

    fn read_raw(&mut self, buf: &mut [u8]) -> Result<()> {
        self.src.read_exact(buf).map_err(|e| {
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                Error::corrupt(&self.path, self.at, "archive ends early")
            } else {
                Error::io(&self.path, "read", e)
            }
        })?;
        self.at += buf.len() as u64;
        Ok(())
    }

    fn read_hashed(&mut self, buf: &mut [u8]) -> Result<()> {
        self.read_raw(buf)?;
        self.hasher.update(buf);
        Ok(())
    }

    /// The next entry's `(kind, name, len)`, or `None` at the end marker.
    fn next_entry(&mut self) -> Result<Option<(u8, String, u64)>> {
        let mut kind = [0u8; 1];
        self.read_hashed(&mut kind)?;
        if kind[0] == KIND_END {
            return Ok(None);
        }
        let mut name_len = [0u8; 2];
        self.read_hashed(&mut name_len)?;
        let mut name = vec![0u8; u16::from_be_bytes(name_len) as usize];
        self.read_hashed(&mut name)?;
        let name = String::from_utf8(name)
            .map_err(|_| Error::corrupt(&self.path, self.at, "entry name is not UTF-8"))?;
        let mut len = [0u8; 8];
        self.read_hashed(&mut len)?;
        Ok(Some((kind[0], name, u64::from_be_bytes(len))))
    }

    /// Streams `len` entry bytes into `out`.
    fn copy_entry(&mut self, len: u64, out: &mut impl Write, out_path: &Path) -> Result<()> {
        let mut buf = vec![0u8; COPY_CHUNK];
        let mut left = len;
        while left > 0 {
            let n = (left as usize).min(COPY_CHUNK);
            self.read_hashed(&mut buf[..n])?;
            out.write_all(&buf[..n])
                .map_err(|e| Error::io(out_path, "write", e))?;
            left -= n as u64;
        }
        Ok(())
    }

    /// An entry's bytes, read in pieces as they arrive: a bogus length
    /// fails with "archive ends early" before any large allocation.
    fn read_entry(&mut self, len: u64) -> Result<Vec<u8>> {
        let mut out = Vec::with_capacity((len as usize).min(COPY_CHUNK));
        let mut left = len;
        let mut buf = vec![0u8; COPY_CHUNK];
        while left > 0 {
            let n = (left as usize).min(COPY_CHUNK);
            self.read_hashed(&mut buf[..n])?;
            out.extend_from_slice(&buf[..n]);
            left -= n as u64;
        }
        Ok(out)
    }

    /// Checks the trailing crc against everything read since the header.
    fn finish(mut self) -> Result<()> {
        let want = self.hasher.clone().finalize();
        let mut crc = [0u8; 4];
        self.read_raw(&mut crc)?;
        let got = u32::from_be_bytes(crc);
        if got != want {
            return Err(Error::corrupt(
                &self.path,
                self.at - 4,
                format!("archive crc {got:#x} != {want:#x}"),
            ));
        }
        Ok(())
    }
}

/// Reads an archive's header without reading its data.
pub fn inspect_backup(archive: impl AsRef<Path>) -> Result<BackupMeta> {
    let archive = archive.as_ref();
    let (_, mut meta) = Reader::open(archive)?;
    meta.bytes = fs::metadata(archive)
        .map_err(|e| Error::io(archive, "metadata", e))?
        .len();
    Ok(meta)
}

/// Restores a full backup into `dir`, which must not exist yet. The
/// segment files are written to a staging directory beside `dir`, the
/// archive's crc is checked, the index is rebuilt from the segments, and
/// only then is the staging directory renamed into place; any failure
/// leaves no `dir` behind. `opts` bounds the rebuild's record size.
pub fn restore_backup(
    archive: impl AsRef<Path>,
    dir: impl AsRef<Path>,
    opts: &Options,
) -> Result<BackupMeta> {
    let archive = archive.as_ref();
    let (reader, meta) = Reader::open(archive)?;
    restore_with(reader, meta, dir.as_ref(), opts)
}

/// [`restore_backup`] from a stream of the archive's bytes (a backup
/// fetched over the daemon, for one): nothing is written to `dir` before
/// the whole stream has arrived and its crc has been checked.
pub fn restore_backup_from(
    reader: impl Read,
    dir: impl AsRef<Path>,
    opts: &Options,
) -> Result<BackupMeta> {
    let (reader, meta) = Reader::from_read(reader, Path::new("<backup stream>"))?;
    restore_with(reader, meta, dir.as_ref(), opts)
}

fn restore_with<R: Read>(
    reader: Reader<R>,
    meta: BackupMeta,
    dir: &Path,
    opts: &Options,
) -> Result<BackupMeta> {
    let archive = reader.path.clone();
    if meta.kind != BackupKind::Full {
        return Err(Error::WrongBackupKind {
            archive,
            kind: meta.kind,
        });
    }
    if dir.exists() {
        return Err(Error::AlreadyExists {
            path: dir.to_path_buf(),
        });
    }
    let parent = match dir.parent() {
        Some(p) if !p.as_os_str().is_empty() => p.to_path_buf(),
        _ => PathBuf::from("."),
    };
    let name = dir.file_name().and_then(|n| n.to_str()).ok_or_else(|| {
        Error::io(
            dir,
            "restore",
            std::io::Error::other("directory has no name"),
        )
    })?;
    let staging = parent.join(format!(".{name}.restore"));
    if staging.exists() {
        fs::remove_dir_all(&staging).map_err(|e| Error::io(&staging, "remove", e))?;
    }
    let result = restore_into(reader, &meta, &staging, opts).and_then(|()| {
        fs::rename(&staging, dir).map_err(|e| Error::io(&staging, "rename", e))?;
        segment::sync_dir(&parent)
    });
    if result.is_err() {
        let _ = fs::remove_dir_all(&staging);
    }
    result.map(|()| meta)
}

fn restore_into<R: Read>(
    mut reader: Reader<R>,
    meta: &BackupMeta,
    staging: &Path,
    opts: &Options,
) -> Result<()> {
    let layout = Layout::new(staging);
    let segments_dir = layout.segments_dir();
    fs::create_dir_all(&segments_dir).map_err(|e| Error::io(&segments_dir, "mkdir", e))?;
    let mut lens: Vec<(Uuid, u64)> = Vec::new();
    while let Some((kind, name, len)) = reader.next_entry()? {
        if kind == KIND_AGG_LEN {
            let at = reader.at;
            let bytes = reader.read_entry(len)?;
            lens = parse_lens(&bytes, &reader.path, at)?;
            continue;
        }
        if kind != KIND_SEGMENT {
            return Err(Error::corrupt(
                &reader.path,
                reader.at,
                format!("unexpected entry kind {kind} in a full backup"),
            ));
        }
        let base = name
            .strip_prefix("segments/")
            .and_then(segment::parse_file_name)
            .ok_or_else(|| {
                Error::corrupt(
                    &reader.path,
                    reader.at,
                    format!("unexpected entry name {name}"),
                )
            })?;
        let path = layout.segment(base);
        let mut file = File::create(&path).map_err(|e| Error::io(&path, "create", e))?;
        reader.copy_entry(len, &mut file, &path)?;
        file.sync_all().map_err(|e| Error::io(&path, "fsync", e))?;
    }
    reader.finish()?;
    // The index last: a damaged archive never leaves a half-built database.
    let rebuilt = rebuild::rebuild_seeded(staging, opts, meta.db_id, lens)?;
    if rebuilt.head != meta.head || rebuilt.floor != meta.floor || rebuilt.dropped != 0 {
        return Err(Error::corrupt(
            staging,
            0,
            format!(
                "the segments rebuild to head {} floor {} ({} dropped); the archive says head {} floor {}",
                rebuilt.head, rebuilt.floor, rebuilt.dropped, meta.head, meta.floor
            ),
        ));
    }
    segment::sync_dir(&segments_dir)?;
    segment::sync_dir(staging)
}

pub(crate) fn apply(inner: &Inner, archive: &Path) -> Result<BackupMeta> {
    let (mut reader, meta) = Reader::open(archive)?;
    let Some(base_head) = meta
        .base_head
        .filter(|_| meta.kind == BackupKind::Incremental)
    else {
        return Err(Error::WrongBackupKind {
            archive: archive.to_path_buf(),
            kind: meta.kind,
        });
    };
    // Everything is read and checked before the database is touched.
    let mut frames = None;
    while let Some((kind, name, len)) = reader.next_entry()? {
        if kind != KIND_RECORDS || name != "records" || frames.is_some() {
            return Err(Error::corrupt(
                archive,
                reader.at,
                format!("unexpected entry {name} (kind {kind}) in an incremental backup"),
            ));
        }
        frames = Some(reader.read_entry(len)?);
    }
    let frames = frames.ok_or_else(|| Error::corrupt(archive, reader.at, "no records entry"))?;
    reader.finish()?;

    let ours = *inner.db_id.lock().unwrap_or_else(|p| p.into_inner());
    if let Some(theirs) = meta.db_id
        && theirs != ours
    {
        return Err(Error::WrongDatabase {
            what: archive.display().to_string(),
            theirs,
            ours,
        });
    }
    let _m = inner.maintenance.lock().unwrap_or_else(|p| p.into_inner());
    txn::import_frames(inner, archive, &frames, base_head, meta.head)?;
    Ok(meta)
}
