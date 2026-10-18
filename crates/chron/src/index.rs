//! The redb index: where every position lives, each aggregate's events,
//! each tag's events, and the committed head.

use std::path::Path;

use redb::{
    Database, Durability, ReadTransaction, ReadableDatabase, TableDefinition, WriteTransaction,
};
use uuid::Uuid;

use crate::error::Result;
use crate::options::Fsync;

/// `"head"` → next global position.
pub(crate) const META: TableDefinition<&str, u64> = TableDefinition::new("meta");
/// aggregate uuid → number of events.
pub(crate) const AGG_LEN: TableDefinition<&[u8; 16], u64> = TableDefinition::new("agg_len");
/// (aggregate uuid, index) → position.
pub(crate) const AGG_INDEX: TableDefinition<(&[u8; 16], u64), u64> =
    TableDefinition::new("agg_index");
/// position → (segment base, frame offset, frame length).
pub(crate) const POS: TableDefinition<u64, (u64, u64, u32)> = TableDefinition::new("pos");
/// (tag, position) → ().
pub(crate) const TAG: TableDefinition<(&[u8], u64), ()> = TableDefinition::new("tag");

pub(crate) const HEAD_KEY: &str = "head";
/// `"floor"` → base of the first kept segment; absent means 0.
pub(crate) const FLOOR_KEY: &str = "floor";

/// Where a record's frame starts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Location {
    pub base: u64,
    pub offset: u64,
    pub frame_len: u32,
}

impl From<(u64, u64, u32)> for Location {
    fn from((base, offset, frame_len): (u64, u64, u32)) -> Self {
        Location {
            base,
            offset,
            frame_len,
        }
    }
}

/// Groups ascending `(position, location)` entries into runs of records
/// that sit back to back in one segment, each run at most `max_bytes`
/// long (a single record larger than that is a run of its own). Each
/// range indexes into `found`.
pub(crate) fn coalesce(found: &[(u64, Location)], max_bytes: u64) -> Vec<std::ops::Range<usize>> {
    let mut runs = Vec::new();
    let mut start = 0;
    let mut bytes = 0u64;
    for i in 0..found.len() {
        let loc = &found[i].1;
        let extends = i > start && {
            let prev = &found[i - 1].1;
            prev.base == loc.base
                && prev.offset + u64::from(prev.frame_len) == loc.offset
                && bytes + u64::from(loc.frame_len) <= max_bytes
        };
        if extends {
            bytes += u64::from(loc.frame_len);
        } else {
            if i > start {
                runs.push(start..i);
            }
            start = i;
            bytes = u64::from(loc.frame_len);
        }
    }
    if start < found.len() {
        runs.push(start..found.len());
    }
    runs
}

pub(crate) struct Index {
    db: Database,
    durability: Durability,
}

impl Index {
    fn durability_for(fsync: Fsync) -> Durability {
        match fsync {
            Fsync::Always => Durability::Immediate,
            Fsync::Never => Durability::None,
        }
    }

    /// Creates the index with every table and `head = 0`.
    pub(crate) fn create(path: &Path, fsync: Fsync) -> Result<Self> {
        let index = Index {
            db: Database::create(path)?,
            durability: Self::durability_for(fsync),
        };
        let txn = index.begin_write_durable()?;
        {
            txn.open_table(META)?.insert(HEAD_KEY, 0)?;
            txn.open_table(AGG_LEN)?;
            txn.open_table(AGG_INDEX)?;
            txn.open_table(POS)?;
            txn.open_table(TAG)?;
        }
        txn.commit()?;
        Ok(index)
    }

    pub(crate) fn open(path: &Path, fsync: Fsync) -> Result<Self> {
        Ok(Index {
            db: Database::open(path)?,
            durability: Self::durability_for(fsync),
        })
    }

    pub(crate) fn begin_read(&self) -> Result<ReadTransaction> {
        Ok(self.db.begin_read()?)
    }

    /// A write transaction at the configured durability.
    pub(crate) fn begin_write(&self) -> Result<WriteTransaction> {
        let mut txn = self.db.begin_write()?;
        txn.set_durability(self.durability)?;
        Ok(txn)
    }

    /// A write transaction that reaches the disk before `commit` returns.
    pub(crate) fn begin_write_durable(&self) -> Result<WriteTransaction> {
        let mut txn = self.db.begin_write()?;
        txn.set_durability(Durability::Immediate)?;
        Ok(txn)
    }

    pub(crate) fn head(&self) -> Result<u64> {
        let txn = self.begin_read()?;
        Ok(txn
            .open_table(META)?
            .get(HEAD_KEY)?
            .map_or(0, |v| v.value()))
    }

    pub(crate) fn floor(&self) -> Result<u64> {
        let txn = self.begin_read()?;
        Ok(txn
            .open_table(META)?
            .get(FLOOR_KEY)?
            .map_or(0, |v| v.value()))
    }

    pub(crate) fn aggregate_len(&self, id: &Uuid) -> Result<u64> {
        let txn = self.begin_read()?;
        Ok(txn
            .open_table(AGG_LEN)?
            .get(id.as_bytes())?
            .map_or(0, |v| v.value()))
    }

    pub(crate) fn locate(&self, position: u64) -> Result<Option<Location>> {
        let txn = self.begin_read()?;
        Self::locate_in(&txn, position)
    }

    pub(crate) fn locate_in(txn: &ReadTransaction, position: u64) -> Result<Option<Location>> {
        Ok(txn
            .open_table(POS)?
            .get(position)?
            .map(|v| v.value().into()))
    }

    /// Resolves several positions through one table handle.
    pub(crate) fn locate_many(
        txn: &ReadTransaction,
        positions: &[u64],
    ) -> Result<Vec<Option<Location>>> {
        let table = txn.open_table(POS)?;
        positions
            .iter()
            .map(|p| Ok(table.get(*p)?.map(|v| v.value().into())))
            .collect()
    }

    /// `(index, position)` of the aggregate's events from `from_index`.
    pub(crate) fn aggregate_positions(
        &self,
        txn: &ReadTransaction,
        id: &Uuid,
        from_index: u64,
        limit: usize,
    ) -> Result<Vec<(u64, u64)>> {
        let table = txn.open_table(AGG_INDEX)?;
        let bytes = id.as_bytes();
        let mut out = Vec::new();
        for item in table
            .range((bytes, from_index)..=(bytes, u64::MAX))?
            .take(limit)
        {
            let (k, v) = item?;
            out.push((k.value().1, v.value()));
        }
        Ok(out)
    }

    /// `(position, location)` of every event from `from`, at most `limit`.
    pub(crate) fn positions_from(
        &self,
        txn: &ReadTransaction,
        from: u64,
        limit: usize,
    ) -> Result<Vec<(u64, Location)>> {
        let table = txn.open_table(POS)?;
        let end = from.saturating_add(limit as u64);
        let mut out = Vec::new();
        for item in table.range(from..end)? {
            let (k, v) = item?;
            out.push((k.value(), v.value().into()));
        }
        Ok(out)
    }

    /// `(position, location)` of the events at positions in `floor..=from`,
    /// newest first, at most `limit`.
    pub(crate) fn positions_to(
        &self,
        txn: &ReadTransaction,
        floor: u64,
        from: u64,
        limit: usize,
    ) -> Result<Vec<(u64, Location)>> {
        let table = txn.open_table(POS)?;
        let mut out = Vec::new();
        for item in table.range(floor..=from)?.rev().take(limit) {
            let (k, v) = item?;
            out.push((k.value(), v.value().into()));
        }
        Ok(out)
    }

    /// `(index, position)` of the aggregate's events at indices
    /// `<= from_index`, newest first.
    pub(crate) fn aggregate_positions_rev(
        &self,
        txn: &ReadTransaction,
        id: &Uuid,
        from_index: u64,
        limit: usize,
    ) -> Result<Vec<(u64, u64)>> {
        let table = txn.open_table(AGG_INDEX)?;
        let bytes = id.as_bytes();
        let mut out = Vec::new();
        for item in table
            .range((bytes, 0)..=(bytes, from_index))?
            .rev()
            .take(limit)
        {
            let (k, v) = item?;
            out.push((k.value().1, v.value()));
        }
        Ok(out)
    }

    /// Positions of the events carrying `tag` at positions `<= from`,
    /// newest first.
    pub(crate) fn tag_positions_rev(
        &self,
        txn: &ReadTransaction,
        tag: &[u8],
        from: u64,
        limit: usize,
    ) -> Result<Vec<u64>> {
        let table = txn.open_table(TAG)?;
        let mut out = Vec::new();
        for item in table.range((tag, 0)..=(tag, from))?.rev().take(limit) {
            let (k, _) = item?;
            out.push(k.value().1);
        }
        Ok(out)
    }

    /// Positions of the events carrying `tag` from `from`, ascending.
    pub(crate) fn tag_positions(
        &self,
        txn: &ReadTransaction,
        tag: &[u8],
        from: u64,
        limit: usize,
    ) -> Result<Vec<u64>> {
        let table = txn.open_table(TAG)?;
        let mut out = Vec::new();
        for item in table.range((tag, from)..=(tag, u64::MAX))?.take(limit) {
            let (k, _) = item?;
            out.push(k.value().1);
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn index() -> (tempfile::TempDir, Index) {
        let d = tempfile::tempdir().unwrap();
        let i = Index::create(&d.path().join("index.redb"), Fsync::Never).unwrap();
        (d, i)
    }

    #[test]
    fn fresh_index_has_head_zero_and_empty_tables() {
        let (_d, i) = index();
        assert_eq!(i.head().unwrap(), 0);
        assert_eq!(i.aggregate_len(&Uuid::from_u128(1)).unwrap(), 0);
        assert_eq!(i.locate(0).unwrap(), None);
        let txn = i.begin_read().unwrap();
        assert!(i.positions_from(&txn, 0, 10).unwrap().is_empty());
        assert!(i.tag_positions(&txn, b"t", 0, 10).unwrap().is_empty());
    }

    #[test]
    fn tag_range_isolates_one_tag() {
        let (_d, i) = index();
        let txn = i.begin_write().unwrap();
        {
            let mut t = txn.open_table(TAG).unwrap();
            let tags: [&[u8]; 4] = [b"a", b"a\0", b"ab", b"b"];
            for (pos, tag) in (0u64..12).zip(tags.iter().cycle()) {
                t.insert((*tag, pos), ()).unwrap();
            }
        }
        txn.commit().unwrap();
        let txn = i.begin_read().unwrap();
        assert_eq!(i.tag_positions(&txn, b"a", 0, 100).unwrap(), vec![0, 4, 8]);
        assert_eq!(
            i.tag_positions(&txn, b"a\0", 0, 100).unwrap(),
            vec![1, 5, 9]
        );
        assert_eq!(
            i.tag_positions(&txn, b"ab", 0, 100).unwrap(),
            vec![2, 6, 10]
        );
        assert_eq!(i.tag_positions(&txn, b"b", 0, 100).unwrap(), vec![3, 7, 11]);
        assert_eq!(i.tag_positions(&txn, b"a", 5, 100).unwrap(), vec![8]);
        assert_eq!(i.tag_positions(&txn, b"a", 0, 2).unwrap(), vec![0, 4]);
        assert!(i.tag_positions(&txn, b"", 0, 100).unwrap().is_empty());
        assert_eq!(
            i.tag_positions_rev(&txn, b"a", u64::MAX, 100).unwrap(),
            vec![8, 4, 0]
        );
        assert_eq!(i.tag_positions_rev(&txn, b"a", 7, 100).unwrap(), vec![4, 0]);
        assert_eq!(i.tag_positions_rev(&txn, b"a\0", 9, 1).unwrap(), vec![9]);
        assert!(
            i.tag_positions_rev(&txn, b"c", u64::MAX, 100)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn aggregate_range_respects_from_index_and_id() {
        let (_d, i) = index();
        let a = Uuid::from_u128(1);
        let b = Uuid::from_u128(2);
        let txn = i.begin_write().unwrap();
        {
            let mut t = txn.open_table(AGG_INDEX).unwrap();
            for idx in 0..5u64 {
                t.insert((a.as_bytes(), idx), idx * 2).unwrap();
                t.insert((b.as_bytes(), idx), idx * 2 + 1).unwrap();
            }
        }
        txn.commit().unwrap();
        let txn = i.begin_read().unwrap();
        assert_eq!(
            i.aggregate_positions(&txn, &a, 3, 10).unwrap(),
            vec![(3, 6), (4, 8)]
        );
        assert_eq!(
            i.aggregate_positions(&txn, &b, 0, 2).unwrap(),
            vec![(0, 1), (1, 3)]
        );
        assert!(i.aggregate_positions(&txn, &a, 5, 10).unwrap().is_empty());
        assert!(
            i.aggregate_positions(&txn, &Uuid::from_u128(3), 0, 10)
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            i.aggregate_positions_rev(&txn, &a, u64::MAX, 2).unwrap(),
            vec![(4, 8), (3, 6)]
        );
        assert_eq!(
            i.aggregate_positions_rev(&txn, &b, 1, 10).unwrap(),
            vec![(1, 3), (0, 1)]
        );
        assert!(
            i.aggregate_positions_rev(&txn, &Uuid::from_u128(3), u64::MAX, 10)
                .unwrap()
                .is_empty()
        );
    }

    fn loc(base: u64, offset: u64, frame_len: u32) -> (u64, Location) {
        (
            offset,
            Location {
                base,
                offset,
                frame_len,
            },
        )
    }

    #[test]
    fn coalesce_runs() {
        assert!(coalesce(&[], 100).is_empty());
        let contiguous = [loc(0, 64, 10), loc(0, 74, 10), loc(0, 84, 10)];
        assert_eq!(coalesce(&contiguous, 100), vec![0..3]);
        let gap = [loc(0, 64, 10), loc(0, 80, 10), loc(0, 90, 10)];
        assert_eq!(coalesce(&gap, 100), vec![0..1, 1..3]);
        let base_change = [loc(0, 64, 10), loc(7, 74, 10)];
        assert_eq!(coalesce(&base_change, 100), vec![0..1, 1..2]);
        let capped = [
            loc(0, 64, 10),
            loc(0, 74, 10),
            loc(0, 84, 10),
            loc(0, 94, 10),
        ];
        assert_eq!(coalesce(&capped, 25), vec![0..2, 2..4]);
        let huge = [loc(0, 64, 10), loc(0, 74, 500), loc(0, 574, 10)];
        assert_eq!(coalesce(&huge, 100), vec![0..1, 1..2, 2..3]);
        let one = [loc(0, 64, 500)];
        assert_eq!(coalesce(&one, 100), vec![0..1]);
    }

    #[test]
    fn positions_from_is_dense_and_bounded() {
        let (_d, i) = index();
        let txn = i.begin_write().unwrap();
        {
            let mut t = txn.open_table(POS).unwrap();
            for p in 0..6u64 {
                t.insert(p, (0u64, 64 + p * 10, 10u32)).unwrap();
            }
        }
        txn.commit().unwrap();
        let txn = i.begin_read().unwrap();
        let got = i.positions_from(&txn, 2, 3).unwrap();
        assert_eq!(
            got.iter().map(|(p, _)| *p).collect::<Vec<_>>(),
            vec![2, 3, 4]
        );
        assert_eq!(
            got[0].1,
            Location {
                base: 0,
                offset: 84,
                frame_len: 10
            }
        );
        assert!(i.positions_from(&txn, u64::MAX, 5).unwrap().is_empty());
        let back = |from, limit| -> Vec<u64> {
            i.positions_to(&txn, 0, from, limit)
                .unwrap()
                .iter()
                .map(|(p, _)| *p)
                .collect()
        };
        assert_eq!(back(u64::MAX, 2), vec![5, 4]);
        assert_eq!(back(3, 10), vec![3, 2, 1, 0]);
        assert_eq!(back(0, 1), vec![0]);
        let floored: Vec<u64> = i
            .positions_to(&txn, 3, u64::MAX, 10)
            .unwrap()
            .iter()
            .map(|(p, _)| *p)
            .collect();
        assert_eq!(floored, vec![5, 4, 3]);
        assert_eq!(
            i.locate(5).unwrap(),
            Some(Location {
                base: 0,
                offset: 114,
                frame_len: 10
            })
        );
    }
}
