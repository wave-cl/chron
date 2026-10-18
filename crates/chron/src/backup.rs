//! Backups: one archive file holding segment bytes cut exactly at the
//! archived head (full) or the frames of a position range (incremental).
//! A restore writes the segments back and rebuilds the index from them.
//!
//! ```text
//! "CHRONBAK" | u32 header_len | header: JSON BackupMeta
//! entries: u8 kind | u16 name_len | name | u64 len | bytes
//!   kind 1  a segment file, "segments/<base:020>.seg"   (full backups)
//!   kind 2  "records": frames of positions [base_head, head)   (incremental)
//! u8 0xFF | u32 crc32 over everything after the header
//! ```

use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::db::{Inner, Layout};
use crate::error::{Error, Result};
use crate::options::Options;
use crate::rebuild;
use crate::segment::{self, HEADER_LEN};
use crate::txn;

const MAGIC: &[u8; 8] = b"CHRONBAK";
const FORMAT: u32 = 1;
const KIND_SEGMENT: u8 = 1;
const KIND_RECORDS: u8 = 2;
const KIND_END: u8 = 0xFF;
const MAX_HEADER: u32 = 16 * 1024 * 1024;
const COPY_CHUNK: usize = 1 << 20;

/// Full: a snapshot to restore into a new directory. Incremental: a range
/// of events to apply onto a database at the archive's base head.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum BackupKind {
    #[default]
    Full,
    Incremental,
}

/// What an archive holds; written in its header.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackupMeta {
    pub format: u32,
    #[serde(default)]
    pub kind: BackupKind,
    /// Incremental only: the head the target must be at.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_head: Option<u64>,
    /// Every position below it (and at or above the floor, or the base
    /// head) is in the archive.
    pub head: u64,
    #[serde(default)]
    pub floor: u64,
    pub created_at_unix_nanos: i64,
    /// Entries in the archive.
    #[serde(default)]
    pub files: u64,
    /// Size of the archive file; 0 until it is complete.
    #[serde(default)]
    pub bytes: u64,
}

fn now_nanos() -> i64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_nanos() as i64,
        Err(e) => -(e.duration().as_nanos() as i64),
    }
}

/// Hashes everything written through it.
struct Crc<W: Write> {
    inner: W,
    hasher: crc32fast::Hasher,
}

impl<W: Write> Write for Crc<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let n = self.inner.write(buf)?;
        self.hasher.update(&buf[..n]);
        Ok(n)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

struct Writer {
    path: PathBuf,
    tmp: PathBuf,
    out: Crc<BufWriter<File>>,
}

impl Writer {
    fn create(archive: &Path, meta: &BackupMeta) -> Result<Self> {
        let tmp = archive.with_extension("chronbak.tmp");
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp)
            .map_err(|e| Error::io(&tmp, "create", e))?;
        let mut out = BufWriter::new(file);
        let header = serde_json::to_vec(meta)
            .map_err(|e| Error::corrupt(archive, 0, format!("cannot encode the header: {e}")))?;
        out.write_all(MAGIC)
            .and_then(|()| out.write_all(&(header.len() as u32).to_be_bytes()))
            .and_then(|()| out.write_all(&header))
            .map_err(|e| Error::io(&tmp, "write", e))?;
        Ok(Writer {
            path: archive.to_path_buf(),
            tmp,
            out: Crc {
                inner: out,
                hasher: crc32fast::Hasher::new(),
            },
        })
    }

    fn io(&self, op: &'static str, e: std::io::Error) -> Error {
        Error::io(&self.tmp, op, e)
    }

    fn entry_header(&mut self, kind: u8, name: &str, len: u64) -> Result<()> {
        self.out
            .write_all(&[kind])
            .and_then(|()| self.out.write_all(&(name.len() as u16).to_be_bytes()))
            .and_then(|()| self.out.write_all(name.as_bytes()))
            .and_then(|()| self.out.write_all(&len.to_be_bytes()))
            .map_err(|e| self.io("write", e))
    }

    fn bytes(&mut self, bytes: &[u8]) -> Result<()> {
        self.out.write_all(bytes).map_err(|e| self.io("write", e))
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

    fn finish(mut self, mut meta: BackupMeta) -> Result<BackupMeta> {
        self.out
            .write_all(&[KIND_END])
            .map_err(|e| self.io("write", e))?;
        let crc = self.out.hasher.clone().finalize();
        let mut file = self.out.inner;
        file.write_all(&crc.to_be_bytes())
            .and_then(|()| file.flush())
            .map_err(|e| Error::io(&self.tmp, "write", e))?;
        let file = file
            .into_inner()
            .map_err(|e| Error::io(&self.tmp, "flush", e.into_error()))?;
        file.sync_all()
            .map_err(|e| Error::io(&self.tmp, "fsync", e))?;
        meta.bytes = file
            .metadata()
            .map_err(|e| Error::io(&self.tmp, "metadata", e))?
            .len();
        drop(file);
        fs::rename(&self.tmp, &self.path).map_err(|e| Error::io(&self.tmp, "rename", e))?;
        if let Some(parent) = self.path.parent().filter(|p| !p.as_os_str().is_empty()) {
            segment::sync_dir(parent)?;
        }
        Ok(meta)
    }
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
            let loc = inner.index.locate(end_excl - 1)?.ok_or_else(|| {
                Error::corrupt(
                    inner.layout.index_file(),
                    end_excl - 1,
                    "position below the head is missing from the index",
                )
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
    let _m = inner.maintenance.lock().unwrap_or_else(|p| p.into_inner());
    let plan = plan(inner)?;
    let meta = BackupMeta {
        format: FORMAT,
        kind: BackupKind::Full,
        base_head: None,
        head: plan.head,
        floor: plan.floor,
        created_at_unix_nanos: now_nanos(),
        files: plan.segments.len() as u64,
        bytes: 0,
    };
    let mut w = Writer::create(archive, &meta)?;
    for (base, path, len) in &plan.segments {
        let name = format!("segments/{}", segment::file_name(*base));
        w.entry_header(KIND_SEGMENT, &name, *len)?;
        let file = inner.file(*base)?;
        w.copy_file(&file, path, *len)?;
    }
    w.finish(meta)
}

/// The frames of positions `from..to`, back to back.
fn frames_between(inner: &Inner, from: u64, to: u64) -> Result<Vec<u8>> {
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
            let loc = inner.index.locate(p)?.ok_or_else(|| {
                Error::corrupt(
                    inner.layout.index_file(),
                    p,
                    "position missing from the index",
                )
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
        let at = out.len();
        out.resize(at + (end - start) as usize, 0);
        file.read_exact_at(&mut out[at..], start)
            .map_err(|e| Error::io(path, "read", e))?;
    }
    Ok(out)
}

pub(crate) fn write_incremental(inner: &Inner, archive: &Path, since: u64) -> Result<BackupMeta> {
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
    let meta = BackupMeta {
        format: FORMAT,
        kind: BackupKind::Incremental,
        base_head: Some(since),
        head,
        floor,
        created_at_unix_nanos: now_nanos(),
        files: 1,
        bytes: 0,
    };
    let mut w = Writer::create(archive, &meta)?;
    w.entry_header(KIND_RECORDS, "records", frames.len() as u64)?;
    w.bytes(&frames)?;
    w.finish(meta)
}

/// Reads an archive front to back, hashing everything after the header.
struct Reader {
    path: PathBuf,
    file: BufReader<File>,
    hasher: crc32fast::Hasher,
    at: u64,
}

impl Reader {
    fn open(archive: &Path) -> Result<(Self, BackupMeta)> {
        let file = File::open(archive).map_err(|e| Error::io(archive, "open", e))?;
        let mut r = Reader {
            path: archive.to_path_buf(),
            file: BufReader::new(file),
            hasher: crc32fast::Hasher::new(),
            at: 0,
        };
        let mut magic = [0u8; 8];
        r.read_raw(&mut magic)?;
        if &magic != MAGIC {
            return Err(Error::corrupt(archive, 0, "not a chron backup"));
        }
        let mut len = [0u8; 4];
        r.read_raw(&mut len)?;
        let len = u32::from_be_bytes(len);
        if len == 0 || len > MAX_HEADER {
            return Err(Error::corrupt(
                archive,
                8,
                format!("implausible header length {len}"),
            ));
        }
        let mut header = vec![0u8; len as usize];
        r.read_raw(&mut header)?;
        let meta: BackupMeta = serde_json::from_slice(&header)
            .map_err(|e| Error::corrupt(archive, 12, format!("bad header: {e}")))?;
        if meta.format != FORMAT {
            return Err(Error::corrupt(
                archive,
                12,
                format!("unknown format {}", meta.format),
            ));
        }
        Ok((r, meta))
    }

    fn read_raw(&mut self, buf: &mut [u8]) -> Result<()> {
        self.file.read_exact(buf).map_err(|e| {
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

    fn read_entry(&mut self, len: u64) -> Result<Vec<u8>> {
        let mut out = vec![0u8; len as usize];
        self.read_hashed(&mut out)?;
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
    let dir = dir.as_ref();
    let (reader, meta) = Reader::open(archive)?;
    if meta.kind != BackupKind::Full {
        return Err(Error::corrupt(
            archive,
            12,
            "an incremental backup; apply it with Db::apply_backup",
        ));
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

fn restore_into(
    mut reader: Reader,
    meta: &BackupMeta,
    staging: &Path,
    opts: &Options,
) -> Result<()> {
    let layout = Layout::new(staging);
    let segments_dir = layout.segments_dir();
    fs::create_dir_all(&segments_dir).map_err(|e| Error::io(&segments_dir, "mkdir", e))?;
    while let Some((kind, name, len)) = reader.next_entry()? {
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
    let rebuilt = rebuild::rebuild(staging, opts)?;
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
        return Err(Error::corrupt(
            archive,
            12,
            "a full backup; restore it with restore_backup",
        ));
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

    let _m = inner.maintenance.lock().unwrap_or_else(|p| p.into_inner());
    txn::import_frames(inner, archive, &frames, base_head, meta.head)?;
    Ok(meta)
}
