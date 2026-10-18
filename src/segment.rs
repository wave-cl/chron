//! Segment files: a 64-byte header followed by framed records.
//!
//! ```text
//! header (64 bytes)
//!   [0..8)   magic  "CHRONSG\0"
//!   [8..12)  u32 format (1)
//!   [12..16) u32 crc32 of the header with this field zeroed
//!   [16..24) u64 base position (position of the first record)
//!   [24..64) zero
//! record
//!   u32 body_len, u32 crc32(body), body
//! ```

use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, Read, Seek, SeekFrom, Write};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};

use crate::error::{Error, Result};

pub(crate) const HEADER_LEN: u64 = 64;
pub(crate) const MAGIC: &[u8; 8] = b"CHRONSG\0";
pub(crate) const FORMAT: u32 = 1;
/// Bytes of framing before each body.
pub(crate) const FRAME_LEN: usize = 8;

/// File name of the segment whose first record has `base`.
pub(crate) fn file_name(base: u64) -> String {
    format!("{base:020}.seg")
}

/// Parses a segment file name back to its base position.
pub(crate) fn parse_file_name(name: &str) -> Option<u64> {
    let stem = name.strip_suffix(".seg")?;
    if stem.is_empty() || !stem.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    stem.parse().ok()
}

/// Segment files in `dir`, sorted by base. Other names are ignored.
pub(crate) fn list(dir: &Path) -> Result<Vec<(u64, PathBuf)>> {
    let mut out = Vec::new();
    for entry in fs::read_dir(dir).map_err(|e| Error::io(dir, "readdir", e))? {
        let entry = entry.map_err(|e| Error::io(dir, "readdir", e))?;
        if let Some(base) = entry.file_name().to_str().and_then(parse_file_name) {
            out.push((base, entry.path()));
        }
    }
    out.sort();
    Ok(out)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SegmentHeader {
    pub base: u64,
}

impl SegmentHeader {
    pub(crate) fn encode(&self) -> [u8; HEADER_LEN as usize] {
        let mut h = [0u8; HEADER_LEN as usize];
        h[0..8].copy_from_slice(MAGIC);
        h[8..12].copy_from_slice(&FORMAT.to_be_bytes());
        h[16..24].copy_from_slice(&self.base.to_be_bytes());
        let crc = crc32fast::hash(&h);
        h[12..16].copy_from_slice(&crc.to_be_bytes());
        h
    }

    pub(crate) fn decode(h: &[u8; HEADER_LEN as usize]) -> std::result::Result<Self, String> {
        if &h[0..8] != MAGIC {
            return Err("bad magic".into());
        }
        let format = u32::from_be_bytes(h[8..12].try_into().unwrap());
        if format != FORMAT {
            return Err(format!("unknown segment format {format}"));
        }
        let stored = u32::from_be_bytes(h[12..16].try_into().unwrap());
        let mut zeroed = *h;
        zeroed[12..16].fill(0);
        let actual = crc32fast::hash(&zeroed);
        if stored != actual {
            return Err(format!("header crc {stored:#x} != {actual:#x}"));
        }
        if h[24..].iter().any(|&b| b != 0) {
            return Err("non-zero header padding".into());
        }
        Ok(SegmentHeader {
            base: u64::from_be_bytes(h[16..24].try_into().unwrap()),
        })
    }
}

/// Appends `len, crc, body` to `out`.
pub(crate) fn frame_record(body: &[u8], out: &mut Vec<u8>) {
    out.extend_from_slice(&(body.len() as u32).to_be_bytes());
    out.extend_from_slice(&crc32fast::hash(body).to_be_bytes());
    out.extend_from_slice(body);
}

/// Why a sequential scan stopped.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum TailReason {
    /// The file ends exactly on a record boundary.
    Clean,
    /// Fewer than 8 bytes left.
    ShortFrame,
    /// The frame claims more body bytes than the file has.
    ShortBody,
    /// The body's crc does not match.
    BadCrc,
    /// The frame claims a body larger than `max_record_bytes` (or zero).
    BadLength(u32),
}

impl std::fmt::Display for TailReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TailReason::Clean => write!(f, "end of file"),
            TailReason::ShortFrame => write!(f, "short frame"),
            TailReason::ShortBody => write!(f, "short body"),
            TailReason::BadCrc => write!(f, "crc mismatch"),
            TailReason::BadLength(n) => write!(f, "implausible body length {n}"),
        }
    }
}

pub(crate) enum ScanItem {
    Record { offset: u64, body: Vec<u8> },
    Tail { offset: u64, reason: TailReason },
}

/// Reads records sequentially from an offset on its own file handle (it
/// seeks, so it must never share a handle with the readers). A malformed
/// record is not an error: it is reported as a [`ScanItem::Tail`] so
/// recovery can truncate there.
pub(crate) struct Scanner {
    reader: BufReader<File>,
    path: PathBuf,
    offset: u64,
    len: u64,
    max_record: usize,
    done: bool,
}

impl Scanner {
    pub(crate) fn open(path: &Path, start: u64, max_record: usize) -> Result<Self> {
        let file = File::open(path).map_err(|e| Error::io(path, "open", e))?;
        let len = file
            .metadata()
            .map_err(|e| Error::io(path, "metadata", e))?
            .len();
        let mut reader = BufReader::with_capacity(256 * 1024, file);
        reader
            .seek(SeekFrom::Start(start))
            .map_err(|e| Error::io(path, "seek", e))?;
        Ok(Scanner {
            reader,
            path: path.to_owned(),
            offset: start,
            len,
            max_record,
            done: false,
        })
    }

    fn stop(&mut self, reason: TailReason) -> ScanItem {
        self.done = true;
        ScanItem::Tail {
            offset: self.offset,
            reason,
        }
    }

    pub(crate) fn next(&mut self) -> Result<ScanItem> {
        if self.done {
            return Ok(self.stop(TailReason::Clean));
        }
        let remaining = self.len.saturating_sub(self.offset);
        if remaining == 0 {
            return Ok(self.stop(TailReason::Clean));
        }
        if remaining < FRAME_LEN as u64 {
            return Ok(self.stop(TailReason::ShortFrame));
        }
        let mut frame = [0u8; FRAME_LEN];
        self.reader
            .read_exact(&mut frame)
            .map_err(|e| Error::io(&self.path, "read", e))?;
        let body_len = u32::from_be_bytes(frame[0..4].try_into().unwrap());
        let crc = u32::from_be_bytes(frame[4..8].try_into().unwrap());
        if body_len == 0 || body_len as usize + FRAME_LEN > self.max_record {
            return Ok(self.stop(TailReason::BadLength(body_len)));
        }
        if remaining - (FRAME_LEN as u64) < u64::from(body_len) {
            return Ok(self.stop(TailReason::ShortBody));
        }
        let mut body = vec![0u8; body_len as usize];
        self.reader
            .read_exact(&mut body)
            .map_err(|e| Error::io(&self.path, "read", e))?;
        if crc32fast::hash(&body) != crc {
            return Ok(self.stop(TailReason::BadCrc));
        }
        let offset = self.offset;
        self.offset += (FRAME_LEN + body_len as usize) as u64;
        Ok(ScanItem::Record { offset, body })
    }
}

/// Reads the record the index placed at `offset` with a frame of `frame_len`
/// bytes, in one positional read. Everything wrong is `Corrupt`: the caller
/// only asks for records the index vouches for.
pub(crate) fn read_record_at(
    file: &File,
    path: &Path,
    offset: u64,
    frame_len: u32,
    max_record: usize,
) -> Result<Vec<u8>> {
    let frame_len = frame_len as usize;
    if frame_len <= FRAME_LEN || frame_len > max_record {
        return Err(Error::corrupt(
            path,
            offset,
            format!("index claims an implausible frame length {frame_len}"),
        ));
    }
    let mut buf = vec![0u8; frame_len];
    file.read_exact_at(&mut buf, offset).map_err(|e| {
        if e.kind() == std::io::ErrorKind::UnexpectedEof {
            Error::corrupt(path, offset, "record past end of file")
        } else {
            Error::io(path, "read", e)
        }
    })?;
    let body_len = u32::from_be_bytes(buf[0..4].try_into().unwrap()) as usize;
    let crc = u32::from_be_bytes(buf[4..8].try_into().unwrap());
    if body_len + FRAME_LEN != frame_len {
        return Err(Error::corrupt(
            path,
            offset,
            format!(
                "frame says body {body_len}, index says {}",
                frame_len - FRAME_LEN
            ),
        ));
    }
    buf.drain(..FRAME_LEN);
    if crc32fast::hash(&buf) != crc {
        return Err(Error::corrupt(path, offset, "crc mismatch"));
    }
    Ok(buf)
}

/// The writer's handle on the current segment.
pub(crate) struct SegmentWriter {
    file: File,
    path: PathBuf,
    base: u64,
    /// Logical end of committed data; bytes past it are garbage from a
    /// failed commit and are overwritten by the next one.
    len: u64,
}

impl SegmentWriter {
    /// Creates a fresh segment holding only a header.
    pub(crate) fn create(segments_dir: &Path, base: u64, sync: bool) -> Result<Self> {
        let path = segments_dir.join(file_name(base));
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)
            .map_err(|e| Error::io(&path, "create", e))?;
        file.write_all(&SegmentHeader { base }.encode())
            .map_err(|e| Error::io(&path, "write", e))?;
        if sync {
            file.sync_all().map_err(|e| Error::io(&path, "fsync", e))?;
            sync_dir(segments_dir)?;
        }
        Ok(SegmentWriter {
            file,
            path,
            base,
            len: HEADER_LEN,
        })
    }

    /// Opens an existing, already recovered segment for appending at `len`.
    pub(crate) fn open(path: &Path, base: u64, len: u64) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .map_err(|e| Error::io(path, "open", e))?;
        Ok(SegmentWriter {
            file,
            path: path.to_owned(),
            base,
            len,
        })
    }

    pub(crate) fn base(&self) -> u64 {
        self.base
    }

    pub(crate) fn len(&self) -> u64 {
        self.len
    }

    /// Writes `buf` at the logical end and returns the offset it starts at.
    /// The logical end moves only when the caller [`advance`]s, so a failed
    /// commit leaves the bytes to be overwritten.
    ///
    /// [`advance`]: SegmentWriter::advance
    pub(crate) fn write_at_end(&mut self, buf: &[u8]) -> Result<u64> {
        self.file
            .write_all_at(buf, self.len)
            .map_err(|e| Error::io(&self.path, "write", e))?;
        Ok(self.len)
    }

    pub(crate) fn advance(&mut self, bytes: u64) {
        self.len += bytes;
    }

    pub(crate) fn sync_data(&self) -> Result<()> {
        self.file
            .sync_data()
            .map_err(|e| Error::io(&self.path, "fdatasync", e))
    }
}

/// Reads and validates a segment header; `Err(String)` is the reason it is
/// not one, `Ok(None)` means the file is shorter than a header.
pub(crate) fn read_header(
    path: &Path,
) -> Result<std::result::Result<Option<SegmentHeader>, String>> {
    let mut file = File::open(path).map_err(|e| Error::io(path, "open", e))?;
    let mut h = [0u8; HEADER_LEN as usize];
    match file.read_exact(&mut h) {
        Ok(()) => Ok(SegmentHeader::decode(&h).map(Some)),
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => Ok(Ok(None)),
        Err(e) => Err(Error::io(path, "read", e)),
    }
}

/// Truncates a segment file to `len` and fsyncs it.
pub(crate) fn truncate(path: &Path, len: u64) -> Result<()> {
    let file = OpenOptions::new()
        .write(true)
        .open(path)
        .map_err(|e| Error::io(path, "open", e))?;
    file.set_len(len)
        .map_err(|e| Error::io(path, "truncate", e))?;
    file.sync_all().map_err(|e| Error::io(path, "fsync", e))
}

/// fsyncs a directory so a created or removed entry is durable.
pub(crate) fn sync_dir(dir: &Path) -> Result<()> {
    let d = File::open(dir).map_err(|e| Error::io(dir, "open", e))?;
    d.sync_all().map_err(|e| Error::io(dir, "fsync", e))
}

#[cfg(test)]
mod tests {
    use super::*;

    const MAX: usize = 1024;

    fn tmp() -> tempfile::TempDir {
        tempfile::tempdir().unwrap()
    }

    fn write_records(dir: &Path, bodies: &[&[u8]]) -> (PathBuf, Vec<u64>) {
        let mut w = SegmentWriter::create(dir, 0, false).unwrap();
        let mut offsets = Vec::new();
        for b in bodies {
            let mut buf = Vec::new();
            frame_record(b, &mut buf);
            offsets.push(w.write_at_end(&buf).unwrap());
            w.advance(buf.len() as u64);
        }
        (w.path.clone(), offsets)
    }

    fn scan_all(path: &Path) -> (Vec<(u64, Vec<u8>)>, u64, TailReason) {
        let mut s = Scanner::open(path, HEADER_LEN, MAX).unwrap();
        let mut out = Vec::new();
        loop {
            match s.next().unwrap() {
                ScanItem::Record { offset, body } => out.push((offset, body)),
                ScanItem::Tail { offset, reason } => return (out, offset, reason),
            }
        }
    }

    #[test]
    fn file_names_are_zero_padded_and_parse_back() {
        assert_eq!(file_name(0), "00000000000000000000.seg");
        assert_eq!(file_name(1234), "00000000000000001234.seg");
        assert_eq!(parse_file_name("00000000000000001234.seg"), Some(1234));
        assert_eq!(parse_file_name("7.seg"), Some(7));
        assert_eq!(parse_file_name("x.seg"), None);
        assert_eq!(parse_file_name(".seg"), None);
        assert_eq!(parse_file_name("1.tmp"), None);
    }

    #[test]
    fn list_sorts_numerically_and_ignores_strangers() {
        let d = tmp();
        for name in [
            "00000000000000000010.seg",
            "00000000000000000002.seg",
            "notes.txt",
            "x.seg",
        ] {
            fs::write(d.path().join(name), b"").unwrap();
        }
        let bases: Vec<u64> = list(d.path())
            .unwrap()
            .into_iter()
            .map(|(b, _)| b)
            .collect();
        assert_eq!(bases, vec![2, 10]);
    }

    #[test]
    fn header_round_trip_crc_magic_and_padding() {
        let h = SegmentHeader { base: 42 };
        let bytes = h.encode();
        assert_eq!(&bytes[0..8], MAGIC);
        assert_eq!(SegmentHeader::decode(&bytes).unwrap(), h);
        let mut flipped = bytes;
        flipped[20] ^= 1;
        assert!(SegmentHeader::decode(&flipped).unwrap_err().contains("crc"));
        let mut magic = bytes;
        magic[0] = b'X';
        assert_eq!(SegmentHeader::decode(&magic).unwrap_err(), "bad magic");
        let mut padded = bytes;
        padded[63] = 1;
        padded[12..16].fill(0);
        let crc = crc32fast::hash(&padded);
        padded[12..16].copy_from_slice(&crc.to_be_bytes());
        assert!(
            SegmentHeader::decode(&padded)
                .unwrap_err()
                .contains("padding")
        );
    }

    #[test]
    fn framing_offsets_are_exact_and_scan_back() {
        let d = tmp();
        let (path, offsets) = write_records(d.path(), &[b"one", b"second"]);
        assert_eq!(offsets, vec![HEADER_LEN, HEADER_LEN + 8 + 3]);
        let (records, end, reason) = scan_all(&path);
        assert_eq!(
            records,
            vec![
                (offsets[0], b"one".to_vec()),
                (offsets[1], b"second".to_vec())
            ]
        );
        assert_eq!(end, HEADER_LEN + 11 + 14);
        assert_eq!(reason, TailReason::Clean);
    }

    #[test]
    fn read_record_at_checks_length_and_crc() {
        let d = tmp();
        let (path, offsets) = write_records(d.path(), &[b"one", b"second"]);
        let f = File::open(&path).unwrap();
        assert_eq!(
            read_record_at(&f, &path, offsets[1], 14, MAX).unwrap(),
            b"second"
        );
        assert!(matches!(
            read_record_at(&f, &path, offsets[1], 13, MAX),
            Err(Error::Corrupt { .. })
        ));
        assert!(matches!(
            read_record_at(&f, &path, offsets[1], 8, MAX),
            Err(Error::Corrupt { .. })
        ));
        assert!(matches!(
            read_record_at(&f, &path, offsets[1], 15, MAX),
            Err(Error::Corrupt { .. })
        ));
        let mut bytes = fs::read(&path).unwrap();
        bytes[offsets[0] as usize + 9] ^= 0xff;
        fs::write(&path, &bytes).unwrap();
        let f = File::open(&path).unwrap();
        assert!(matches!(
            read_record_at(&f, &path, offsets[0], 11, MAX),
            Err(Error::Corrupt { .. })
        ));
        let (records, end, reason) = scan_all(&path);
        assert!(records.is_empty());
        assert_eq!(end, offsets[0]);
        assert_eq!(reason, TailReason::BadCrc);
    }

    #[test]
    fn torn_tail_reasons() {
        let d = tmp();
        let (path, offsets) = write_records(d.path(), &[b"one", b"second"]);
        let full = fs::read(&path).unwrap();
        let cases: [(usize, TailReason); 4] = [
            (offsets[1] as usize + 3, TailReason::ShortFrame),
            (offsets[1] as usize + 8, TailReason::ShortBody),
            (offsets[1] as usize + 12, TailReason::ShortBody),
            (offsets[1] as usize, TailReason::Clean),
        ];
        for (cut, want) in cases {
            fs::write(&path, &full[..cut]).unwrap();
            let (records, end, reason) = scan_all(&path);
            assert_eq!(records.len(), 1, "cut {cut}");
            assert_eq!(end, offsets[1], "cut {cut}");
            assert_eq!(reason, want, "cut {cut}");
        }
        // Zero-filled space after a record reads as a zero length.
        let mut zeroed = full[..offsets[1] as usize].to_vec();
        zeroed.extend_from_slice(&[0u8; 32]);
        fs::write(&path, &zeroed).unwrap();
        assert_eq!(scan_all(&path).2, TailReason::BadLength(0));
    }

    #[test]
    fn oversized_length_is_rejected_without_allocating() {
        let d = tmp();
        let (path, offsets) = write_records(d.path(), &[b"one"]);
        let mut bytes = fs::read(&path).unwrap();
        let huge = u32::MAX.to_be_bytes();
        bytes[offsets[0] as usize..offsets[0] as usize + 4].copy_from_slice(&huge);
        fs::write(&path, &bytes).unwrap();
        let (records, _, reason) = scan_all(&path);
        assert!(records.is_empty());
        assert_eq!(reason, TailReason::BadLength(u32::MAX));
        let f = File::open(&path).unwrap();
        assert!(matches!(
            read_record_at(&f, &path, offsets[0], u32::MAX, MAX),
            Err(Error::Corrupt { .. })
        ));
    }

    #[test]
    fn read_header_short_vs_bad() {
        let d = tmp();
        let p = d.path().join("a.seg");
        fs::write(&p, b"short").unwrap();
        assert_eq!(read_header(&p).unwrap(), Ok(None));
        fs::write(&p, [0u8; 64]).unwrap();
        assert!(read_header(&p).unwrap().is_err());
        let w = SegmentWriter::create(d.path(), 9, true).unwrap();
        assert_eq!(
            read_header(&w.path).unwrap(),
            Ok(Some(SegmentHeader { base: 9 }))
        );
        assert!(SegmentWriter::create(d.path(), 9, false).is_err());
    }

    #[test]
    fn truncate_cuts_and_write_at_end_overwrites_garbage() {
        let d = tmp();
        let (path, offsets) = write_records(d.path(), &[b"one"]);
        let mut w = SegmentWriter::open(&path, 0, offsets[0] + 11).unwrap();
        let mut ghost = Vec::new();
        frame_record(b"ghost", &mut ghost);
        w.write_at_end(&ghost).unwrap();
        assert_eq!(fs::metadata(&path).unwrap().len(), offsets[0] + 11 + 13);
        let mut real = Vec::new();
        frame_record(b"real!", &mut real);
        assert_eq!(w.write_at_end(&real).unwrap(), offsets[0] + 11);
        w.advance(real.len() as u64);
        let (records, _, _) = scan_all(&path);
        assert_eq!(records[1].1, b"real!");
        truncate(&path, offsets[0] + 11).unwrap();
        assert_eq!(scan_all(&path).0.len(), 1);
    }
}
