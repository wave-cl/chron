//! Streams: a named subset of the global order, defined by a set of tags.
//! An event is a member when it carries any tag of the set, so membership
//! is derived from the record: every copy of the database computes the
//! same stream whenever the definition reaches it. The index keeps a
//! `(stream id, position)` entry per member, filled beside the tag entries
//! as records land and, at creation, from the tag entries already there.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use bytes::Bytes;
use redb::{Durability, ReadableTable};

use crate::db::Inner;
use crate::error::{Error, Result};
use crate::event::MAX_TAG_BYTES;
use crate::index::{FLOOR_KEY, Index, META, STREAM, STREAM_NEXT_ID_KEY, STREAM_POS, TAG};

/// Most tags one stream may be defined over.
pub const MAX_STREAM_TAGS: usize = 32;
/// Longest stream name, in bytes.
pub const MAX_STREAM_NAME_BYTES: usize = 255;
/// Positions inserted per index transaction while a stream is filled.
const FILL_CHUNK: usize = 50_000;

/// A stream as [`Db::stream`](crate::Db::stream) reports it.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct StreamInfo {
    /// The stream's name.
    pub name: String,
    /// The tags it is defined over, sorted, without duplicates.
    pub tags: Vec<Bytes>,
    /// Whether the fill at creation has finished: until then the stream
    /// cannot be read.
    pub ready: bool,
}

/// A stream definition as the commit path and the readers hold it.
pub(crate) struct StreamDef {
    pub(crate) id: u32,
    pub(crate) name: String,
    /// Sorted and deduplicated: membership is a binary search per tag of
    /// the record, whatever order the record's tags are in.
    pub(crate) tags: Vec<Bytes>,
    pub(crate) ready: AtomicBool,
}

impl StreamDef {
    /// Whether a record carrying `tags` belongs to the stream.
    pub(crate) fn member(&self, tags: &[Bytes]) -> bool {
        tags.iter().any(|t| self.tags.binary_search(t).is_ok())
    }

    pub(crate) fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Acquire)
    }

    pub(crate) fn info(&self) -> StreamInfo {
        StreamInfo {
            name: self.name.clone(),
            tags: self.tags.clone(),
            ready: self.is_ready(),
        }
    }
}

/// Encodes a tag set as the `STREAM` row stores it: `(u8 len, bytes)` per
/// tag, in order.
pub(crate) fn encode_tagset(tags: &[Bytes]) -> Vec<u8> {
    let mut out = Vec::with_capacity(tags.iter().map(|t| 1 + t.len()).sum());
    for t in tags {
        out.push(t.len() as u8);
        out.extend_from_slice(t);
    }
    out
}

pub(crate) fn decode_tagset(mut bytes: &[u8]) -> Vec<Bytes> {
    let mut out = Vec::new();
    while let Some((&len, rest)) = bytes.split_first() {
        let len = usize::from(len).min(rest.len());
        out.push(Bytes::copy_from_slice(&rest[..len]));
        bytes = &rest[len..];
    }
    out
}

/// Checks a name and a tag set and returns the tags sorted and
/// deduplicated.
pub(crate) fn normalise(name: &str, tags: Vec<Bytes>) -> Result<Vec<Bytes>> {
    if name.is_empty() {
        return Err(Error::InvalidStream("the name is empty".into()));
    }
    if name.len() > MAX_STREAM_NAME_BYTES {
        return Err(Error::InvalidStream(format!(
            "the name is {} bytes, over {MAX_STREAM_NAME_BYTES}",
            name.len()
        )));
    }
    let mut tags = tags;
    tags.sort();
    tags.dedup();
    if tags.is_empty() {
        return Err(Error::InvalidStream("no tags".into()));
    }
    if tags.len() > MAX_STREAM_TAGS {
        return Err(Error::InvalidStream(format!(
            "{} tags, over {MAX_STREAM_TAGS}",
            tags.len()
        )));
    }
    if let Some(t) = tags
        .iter()
        .find(|t| t.is_empty() || t.len() > MAX_TAG_BYTES)
    {
        return Err(Error::InvalidStream(format!(
            "a tag of {} bytes (1 to {MAX_TAG_BYTES})",
            t.len()
        )));
    }
    Ok(tags)
}

/// Every definition the index holds, ready or not.
pub(crate) fn load(index: &Index) -> Result<Vec<Arc<StreamDef>>> {
    Ok(index
        .stream_rows()?
        .into_iter()
        .map(|(name, row)| {
            Arc::new(StreamDef {
                id: row.id,
                name,
                tags: decode_tagset(&row.tagset),
                ready: AtomicBool::new(row.ready),
            })
        })
        .collect())
}

/// Phase one of a creation: the definition, written durably and made
/// known to the commit path, so every record landing from now on is
/// indexed for it. The stream is not ready until [`fill`] runs.
pub(crate) fn begin(inner: &Inner, name: &str, tags: Vec<Bytes>) -> Result<Arc<StreamDef>> {
    let tags = normalise(name, tags)?;
    let tagset = encode_tagset(&tags);
    let _m = inner.maintenance.lock().unwrap_or_else(|p| p.into_inner());
    let def = inner.durable_index(|inner, txn| {
        let mut table = txn.open_table(STREAM)?;
        if table.get(name)?.is_some() {
            return Err(Error::StreamExists(name.to_string()));
        }
        let mut meta = txn.open_table(META)?;
        let next = meta.get(STREAM_NEXT_ID_KEY)?.map_or(0, |v| v.value());
        let id =
            u32::try_from(next).map_err(|_| Error::InvalidStream("too many streams".into()))?;
        meta.insert(STREAM_NEXT_ID_KEY, next + 1)?;
        // The head under the writer lock: every position from here on
        // lands through the cache, every one below is the fill's.
        let head = inner.head.load(Ordering::Acquire);
        table.insert(name, (id, false, head, tagset.as_slice()))?;
        let def = Arc::new(StreamDef {
            id,
            name: name.to_string(),
            tags,
            ready: AtomicBool::new(false),
        });
        inner.replace_streams(|v| v.push(def.clone()));
        Ok(def)
    })?;
    inner.notify_streams();
    Ok(def)
}

/// Phase two: indexes the positions below the head the definition was
/// written at, a chunk per index transaction under the maintenance lock
/// (never across the whole fill: chunk cuts, backups and retention take
/// turns between chunks), then marks the stream ready. `Ok(false)` when
/// the stream was deleted meanwhile.
pub(crate) fn fill(inner: &Inner, def: &Arc<StreamDef>) -> Result<bool> {
    let name = def.name.as_str();
    // Per tag, the next position to scan from; a tag leaves the list once
    // its range is exhausted.
    let mut pending: Vec<(Bytes, u64)> = def.tags.iter().map(|t| (t.clone(), 0)).collect();
    loop {
        let _m = inner.maintenance.lock().unwrap_or_else(|p| p.into_inner());
        let txn = inner.index.begin_write_with(Durability::None)?;
        let row = {
            let table = txn.open_table(STREAM)?;
            table.get(name)?.map(|v| {
                let (id, ready, upto, _) = v.value();
                (id, ready, upto)
            })
        };
        let Some((id, ready, upto)) = row else {
            return Ok(false);
        };
        if id != def.id {
            return Ok(false);
        }
        if ready {
            def.ready.store(true, Ordering::Release);
            return Ok(true);
        }
        if pending.is_empty() {
            drop(txn);
            drop(_m);
            break;
        }
        // The floor as of this chunk: positions retention dropped since
        // the last chunk are not indexed (their tag entries are gone too).
        let floor = txn
            .open_table(META)?
            .get(FLOOR_KEY)?
            .map_or(0, |v| v.value());
        {
            let tags = txn.open_table(TAG)?;
            let mut positions = txn.open_table(STREAM_POS)?;
            let mut budget = FILL_CHUNK;
            let mut i = 0;
            while i < pending.len() && budget > 0 {
                let tag = pending[i].0.clone();
                let from = pending[i].1.max(floor);
                let mut found = Vec::new();
                if from < upto {
                    for item in tags
                        .range((tag.as_ref(), from)..(tag.as_ref(), upto))?
                        .take(budget)
                    {
                        let (k, _) = item?;
                        found.push(k.value().1);
                    }
                }
                for p in &found {
                    positions.insert((id, *p), ())?;
                }
                budget -= found.len();
                match found.last() {
                    Some(last) if budget == 0 => {
                        pending[i].1 = last + 1;
                        i += 1;
                    }
                    _ => {
                        pending.remove(i);
                    }
                }
            }
        }
        txn.commit()?;
    }
    inner
        .durable_index(|_, txn| {
            let mut table = txn.open_table(STREAM)?;
            let row = table.get(name)?.map(|v| {
                let (id, _, upto, tagset) = v.value();
                (id, upto, tagset.to_vec())
            });
            match row {
                Some((id, upto, tagset)) if id == def.id => {
                    table.insert(name, (id, true, upto, tagset.as_slice()))?;
                    Ok(true)
                }
                _ => Ok(false),
            }
        })
        .inspect(|&done| {
            if done {
                def.ready.store(true, Ordering::Release);
            }
        })
}

/// Removes a stream and its entries; whether it existed. A fill in
/// progress stops at its next chunk.
pub(crate) fn delete(inner: &Inner, name: &str) -> Result<bool> {
    let _m = inner.maintenance.lock().unwrap_or_else(|p| p.into_inner());
    let existed = inner.durable_index(|inner, txn| {
        let mut table = txn.open_table(STREAM)?;
        let Some(id) = table.remove(name)?.map(|v| v.value().0) else {
            return Ok(false);
        };
        txn.open_table(STREAM_POS)?
            .retain_in((id, 0)..=(id, u64::MAX), |_, _| false)?;
        inner.replace_streams(|v| v.retain(|d| d.id != id));
        Ok(true)
    })?;
    if existed {
        inner.notify_streams();
    }
    Ok(existed)
}
