//! A real crash: a child process commits with `Fsync::Always` and reports
//! each acknowledged position; the parent kills it with SIGKILL at a random
//! moment and checks that reopening keeps every acknowledged commit.

use std::io::{BufRead, BufReader, Write};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use chron::{Db, Expected, Fsync, Options};

use crate::common::*;

const CHILD_DIR: &str = "CHRON_CRASH_CHILD_DIR";
/// Batches between durable index commits in the child; `max` holds
/// every checkpoint back, so the whole run is replayed on open.
const CHILD_CHECKPOINT: &str = "CHRON_CRASH_CHECKPOINT_EVERY";

fn child_options() -> Options {
    let every = match std::env::var(CHILD_CHECKPOINT).as_deref() {
        Ok("max") => usize::MAX,
        Ok(n) => n.parse().unwrap(),
        Err(_) => 256,
    };
    Options::new()
        .fsync(Fsync::Always)
        .index_checkpoint_batches(every)
        .index_checkpoint_interval(Duration::from_secs(3600))
}

/// The child. Only the re-executed test binary has the variable set; in a
/// normal run this returns at once.
#[test]
fn child_writer() {
    let Ok(dir) = std::env::var(CHILD_DIR) else {
        return;
    };
    let db = Db::open(&dir, child_options()).unwrap();
    let mut out = std::io::stdout().lock();
    loop {
        let mut t = db.transaction();
        t.append_one(agg(1), Expected::Any, tagged("x", &["t"]));
        t.append_free(vec![ev("f")]);
        let c = t.commit().unwrap();
        // Acknowledged only once this line is out.
        writeln!(out, "{}", c.last).unwrap();
        out.flush().unwrap();
    }
}

struct KillOnDrop(Child);

impl Drop for KillOnDrop {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[test]
fn kill_dash_nine_keeps_every_acknowledged_commit() {
    for every in ["1", "3", "max"] {
        kill_dash_nine_rounds(every);
    }
}

/// `every`: the child's checkpoint setting; with `max` nothing but the
/// replay on open can keep the acknowledged commits.
fn kill_dash_nine_rounds(every: &str) {
    let d = tmp();
    let exe = std::env::current_exe().unwrap();
    for round in 0..4 {
        let seed = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .subsec_nanos() as u64;
        let child = Command::new(&exe)
            .args(["--exact", "crash::child_writer", "--nocapture"])
            .env(CHILD_DIR, d.path())
            .env(CHILD_CHECKPOINT, every)
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap();
        let mut child = KillOnDrop(child);
        let stdout = child.0.stdout.take().unwrap();
        let mut lines = BufReader::new(stdout).lines();
        let want = 3 + (seed % 25) as usize;
        let mut acked = None;
        let mut seen = 0;
        while seen < want {
            let line = lines.next().expect("the child keeps writing").unwrap();
            if let Ok(p) = line.trim().parse::<u64>() {
                acked = Some(p);
                seen += 1;
            }
        }
        let acked = acked.unwrap();
        std::thread::sleep(Duration::from_micros(seed % 3000));
        child.0.kill().unwrap();
        child.0.wait().unwrap();
        drop(child);

        let db = Db::open(
            d.path(),
            Options::new()
                .fsync(Fsync::Always)
                .verify_all_segments(true),
        )
        .unwrap_or_else(|e| panic!("every {every} round {round} seed {seed}: reopen failed: {e}"));
        let head = db.head();
        assert!(
            head > acked,
            "every {every} round {round} seed {seed}: head {head} lost acknowledged {acked}"
        );
        assert_eq!(
            head % 2,
            0,
            "round {round} seed {seed}: head {head} is not a transaction boundary"
        );
        let all = db.read_all(0, usize::MAX).unwrap();
        assert_eq!(
            positions(&all),
            (0..head).collect::<Vec<_>>(),
            "round {round} seed {seed}"
        );
        assert_eq!(
            db.aggregate_len(agg(1)).unwrap(),
            head / 2,
            "round {round} seed {seed}"
        );
        assert_eq!(
            db.read_tag("t", 0, usize::MAX).unwrap().len() as u64,
            head / 2,
            "round {round} seed {seed}"
        );
        drop(db);
    }
}
