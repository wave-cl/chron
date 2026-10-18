//! `cargo bench` (or `cargo bench -- append`). Short runs: the point is a
//! number to compare across commits, not a precise one.

use std::time::Duration;

use chron::{Db, Expected, Fsync, NewEvent, Options, Uuid};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

fn db(fsync: Fsync) -> (tempfile::TempDir, Db) {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), Options::new().fsync(fsync)).unwrap();
    (dir, db)
}

fn event(i: u64) -> NewEvent {
    NewEvent::new(format!("{{\"n\":{i}}}")).tag(if i.is_multiple_of(2) { "even" } else { "odd" })
}

fn append(c: &mut Criterion) {
    let mut g = c.benchmark_group("append");
    for (fsync, name) in [(Fsync::Never, "never"), (Fsync::Always, "always")] {
        for per_txn in [1u64, 10] {
            g.throughput(Throughput::Elements(per_txn));
            g.bench_with_input(
                BenchmarkId::new(format!("{per_txn}-events"), name),
                &per_txn,
                |b, &per_txn| {
                    let (_dir, db) = db(fsync);
                    let agg = Uuid::from_u128(1);
                    b.iter(|| {
                        let mut t = db.transaction();
                        t.append(agg, Expected::Any, (0..per_txn).map(event).collect());
                        t.commit().unwrap()
                    })
                },
            );
        }
    }
    g.finish();
}

fn reads(c: &mut Criterion) {
    let (_dir, db) = db(Fsync::Never);
    let agg = Uuid::from_u128(1);
    for i in 0..10_000u64 {
        let mut t = db.transaction();
        t.append_one(agg, Expected::Any, event(i));
        t.commit().unwrap();
    }
    let mut g = c.benchmark_group("read");
    g.throughput(Throughput::Elements(1000));
    g.bench_function("all/1000", |b| b.iter(|| db.read_all(4000, 1000).unwrap()));
    g.bench_function("tag/1000", |b| {
        b.iter(|| db.read_tag("even", 2000, 1000).unwrap())
    });
    g.bench_function("aggregate/1000", |b| {
        b.iter(|| db.read_aggregate(agg, 4000, 1000).unwrap())
    });
    g.finish();
}

fn short() -> Criterion {
    Criterion::default()
        .sample_size(10)
        .warm_up_time(Duration::from_millis(300))
        .measurement_time(Duration::from_secs(1))
}

criterion_group! {
    name = benches;
    config = short();
    targets = append, reads
}
criterion_main!(benches);
