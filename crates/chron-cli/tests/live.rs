//! The `chron` binary against an in-process daemon.

use std::io::{BufRead, BufReader};
use std::process::Stdio;
use std::time::Duration;

use assert_cmd::Command;
use predicates::prelude::*;

struct Daemon {
    dir: tempfile::TempDir,
    rt: tokio::runtime::Runtime,
    running: Option<chrond::Running>,
    url: String,
}

impl Daemon {
    fn start() -> Daemon {
        let dir = tempfile::tempdir().unwrap();
        let rt = tokio::runtime::Runtime::new().unwrap();
        let mut opts =
            chrond::Options::new(dir.path().join("data"), "127.0.0.1:0".parse().unwrap());
        opts.fsync = false;
        let running = rt.block_on(chrond::start(opts)).expect("the daemon starts");
        let url = format!("http://{}", running.local_addr);
        Daemon {
            dir,
            rt,
            running: Some(running),
            url,
        }
    }

    fn chron(&self) -> Command {
        let mut c = Command::cargo_bin("chron").unwrap();
        c.env("CHRON_ADDR", &self.url);
        c
    }

    fn db(&self) -> chron::Db {
        self.running.as_ref().unwrap().db.clone()
    }
}

impl Drop for Daemon {
    fn drop(&mut self) {
        if let Some(r) = self.running.take() {
            self.rt.block_on(r.shutdown()).unwrap();
        }
    }
}

const A: &str = "00000000-0000-0000-0000-000000000001";

#[test]
fn append_then_read_prints_human_lines() {
    let d = Daemon::start();
    d.chron()
        .args([
            "append", A, "--expect", "new", "--tag", "orders", "-d", "placed",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("committed 0..0 (next 1)"));
    d.chron()
        .args(["append", A, "--expect", "1", "-d", "shipped"])
        .assert()
        .success();
    d.chron()
        .args(["read", A])
        .assert()
        .success()
        .stdout(predicate::str::contains(format!(
            "0  {A}#0  [orders]  placed"
        )))
        .stdout(predicate::str::contains(format!("1  {A}#1  []  shipped")));
    d.chron()
        .args(["read", A, "--backward", "--limit", "1"])
        .assert()
        .success()
        .stdout(predicate::str::contains("shipped").and(predicate::str::contains("placed").not()));
    d.chron().args(["len", A]).assert().success().stdout("2\n");
    d.chron().args(["head"]).assert().success().stdout("2\n");
}

#[test]
fn free_events_and_json_output() {
    let d = Daemon::start();
    d.chron()
        .args(["append", "free", "--tag", "audit", "-d", "note"])
        .assert()
        .success();
    let out = d
        .chron()
        .args(["--json", "all"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let text = String::from_utf8(out).unwrap();
    let lines: Vec<serde_json::Value> = text
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    assert_eq!(lines.len(), 1);
    assert_eq!(lines[0]["aggregate"], serde_json::Value::Null);
    assert_eq!(lines[0]["payload"], "note");
    assert_eq!(lines[0]["tags"][0], "audit");
    assert_eq!(lines[0]["metadata"], serde_json::Value::Null);
    d.chron()
        .args(["tag", "audit"])
        .assert()
        .success()
        .stdout(predicate::str::contains("0  free  [audit]  note"));
    d.chron()
        .args(["append", "free", "--expect", "new", "-d", "x"])
        .assert()
        .code(2)
        .stderr(predicate::str::contains("free events have no length"));
}

#[test]
fn commit_from_a_json_file_and_conflict_exit_codes() {
    let d = Daemon::start();
    let file = d.dir.path().join("txn.json");
    std::fs::write(
        &file,
        format!(
            r#"{{"appends":[
                {{"aggregate":"{A}","expect":"new","events":[{{"payload":"a0","tags":["t"]}},{{"payload":"a1"}}]}},
                {{"aggregate":null,"events":[{{"payload_base64":"/w=="}}]}}
            ]}}"#
        ),
    )
    .unwrap();
    d.chron()
        .args(["commit", "-f"])
        .arg(&file)
        .assert()
        .success()
        .stdout(predicate::str::contains("committed 0..2 (next 3)"));
    let out = d
        .chron()
        .args(["--json", "all"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let last: serde_json::Value =
        serde_json::from_str(String::from_utf8(out).unwrap().lines().last().unwrap()).unwrap();
    assert_eq!(last["payload_base64"], "/w==");
    d.chron()
        .args(["append", A, "--expect", "7", "-d", "x"])
        .assert()
        .code(1)
        .stderr(
            predicate::str::contains("FailedPrecondition")
                .and(predicate::str::contains("actual length 2")),
        );
    d.chron().args(["head"]).assert().success().stdout("3\n");
}

#[test]
fn unreachable_address_exits_2() {
    Command::cargo_bin("chron")
        .unwrap()
        .args(["--addr", "http://127.0.0.1:1", "head"])
        .assert()
        .code(2)
        .stderr(predicate::str::contains("cannot reach chrond"));
}

#[test]
fn tail_streams_live_until_killed() {
    let d = Daemon::start();
    let mut child = std::process::Command::new(env!("CARGO_BIN_EXE_chron"))
        .env("CHRON_ADDR", &d.url)
        .args(["tail", "--tag", "t"])
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let db = d.db();
    let writer = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(200));
        for i in 0..3 {
            let mut t = db.transaction();
            t.append_one(
                chron::Uuid::from_u128(1),
                chron::Expected::Any,
                chron::NewEvent::new(format!("live{i}")).tag("t"),
            );
            t.append_free(vec![chron::NewEvent::new("untagged")]);
            t.commit().unwrap();
        }
    });
    let stdout = child.stdout.take().unwrap();
    let mut lines = BufReader::new(stdout).lines();
    let got: Vec<String> = (0..3).map(|_| lines.next().unwrap().unwrap()).collect();
    writer.join().unwrap();
    child.kill().unwrap();
    child.wait().unwrap();
    for (i, line) in got.iter().enumerate() {
        assert!(line.contains(&format!("live{i}")), "{line}");
        assert!(!line.contains("untagged"));
    }
}

#[test]
fn info_flush_backup_restore_inspect_and_retain() {
    let d = Daemon::start();
    d.chron()
        .args(["append", A, "-d", "one"])
        .assert()
        .success();
    d.chron()
        .args(["info"])
        .assert()
        .success()
        .stdout(predicate::str::contains("head 1").and(predicate::str::contains("segments 1")));
    d.chron()
        .args(["flush"])
        .assert()
        .success()
        .stdout("flushed\n");
    let archive = d.dir.path().join("full.chronbak");
    d.chron()
        .args(["backup"])
        .arg(&archive)
        .assert()
        .success()
        .stdout(predicate::str::contains("full backup to head 1"));
    d.chron()
        .args(["append", A, "-d", "two"])
        .assert()
        .success();
    let inc = d.dir.path().join("inc.chronbak");
    d.chron()
        .args(["backup", "--since", "1"])
        .arg(&inc)
        .assert()
        .success()
        .stdout(predicate::str::contains("incremental backup 1..2"));
    d.chron()
        .args(["apply"])
        .arg(&inc)
        .assert()
        .code(1)
        .stderr(predicate::str::contains("FailedPrecondition"));
    d.chron()
        .args(["inspect"])
        .arg(&archive)
        .assert()
        .success()
        .stdout(predicate::str::contains("full backup to head 1"));
    let restored = d.dir.path().join("restored");
    d.chron()
        .args(["restore"])
        .arg(&archive)
        .arg(&restored)
        .assert()
        .success()
        .stdout(predicate::str::contains("to head 1"));
    d.chron()
        .args(["verify"])
        .arg(&restored)
        .assert()
        .success()
        .stdout(predicate::str::contains("ok; head 1"));
    std::fs::remove_file(restored.join("index.redb")).unwrap();
    d.chron()
        .args(["rebuild-index"])
        .arg(&restored)
        .assert()
        .success()
        .stdout(predicate::str::contains("rebuilt; head 1"));
    d.chron()
        .args(["verify"])
        .arg(d.dir.path().join("data"))
        .assert()
        .code(1)
        .stderr(predicate::str::contains("stop the daemon first"));
    d.chron()
        .args(["retain", "2"])
        .assert()
        .success()
        .stdout(predicate::str::contains("floor 0 (0 segments"));
    d.chron()
        .args(["retain", "99"])
        .assert()
        .code(1)
        .stderr(predicate::str::contains("OutOfRange"));
}
