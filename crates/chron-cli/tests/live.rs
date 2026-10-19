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
        Self::start_with(|_| {})
    }

    fn start_with(f: impl FnOnce(&mut chrond::Options)) -> Daemon {
        let dir = tempfile::tempdir().unwrap();
        let rt = tokio::runtime::Runtime::new().unwrap();
        let mut opts =
            chrond::Options::new(dir.path().join("data"), "127.0.0.1:0".parse().unwrap());
        opts.fsync = false;
        f(&mut opts);
        let tls = opts.tls.is_some();
        let running = rt.block_on(chrond::start(opts)).expect("the daemon starts");
        let scheme = if tls { "https" } else { "http" };
        let url = format!("{scheme}://{}", running.local_addr);
        Daemon {
            dir,
            rt,
            running: Some(running),
            url,
        }
    }

    /// A replica of `leader` in its own directory and runtime.
    fn start_replica_of(leader: &Daemon) -> Daemon {
        let url = leader.url.clone();
        Self::start_with(move |o| o.replicate_from = Some(url))
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
    // Errors under --json are one JSON object on stderr.
    let err = d
        .chron()
        .args(["--json", "read", "free"])
        .assert()
        .code(2)
        .get_output()
        .stderr
        .clone();
    let err: serde_json::Value = serde_json::from_slice(&err).unwrap();
    assert_eq!(err["code"], 2);
    assert!(
        err["error"]
            .as_str()
            .unwrap()
            .contains("takes an aggregate UUID"),
        "{err}"
    );
    // An empty payload and binary tags.
    d.chron()
        .args(["append", "free", "--metadata", "m", "-d", ""])
        .assert()
        .success();
    let file = d.dir.path().join("bin.json");
    std::fs::write(
        &file,
        r#"{"appends":[{"events":[{"payload_base64":"/w==","tags_base64":["/w=="],"tags":["t"]}]}]}"#,
    )
    .unwrap();
    d.chron()
        .args(["commit", "-f"])
        .arg(&file)
        .assert()
        .success();
    let out = d
        .chron()
        .args(["--json", "all", "--from", "1"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let lines: Vec<serde_json::Value> = String::from_utf8(out)
        .unwrap()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    assert_eq!(lines[0]["payload"], serde_json::Value::Null);
    assert_eq!(lines[0]["metadata"], "m");
    assert_eq!(lines[1]["payload_base64"], "/w==");
    assert_eq!(lines[1]["tags"][0], "t");
    assert_eq!(lines[1]["tags_base64"][0], "/w==");
    std::fs::write(
        &file,
        r#"{"appends":[{"events":[{"payload_base64":"not base64!"}]}]}"#,
    )
    .unwrap();
    d.chron()
        .args(["commit", "-f"])
        .arg(&file)
        .assert()
        .code(2)
        .stderr(predicate::str::contains("not base64"));
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
fn health_exits_0_when_serving_and_2_when_unreachable() {
    let d = Daemon::start();
    d.chron()
        .args(["health"])
        .assert()
        .success()
        .stdout("SERVING\n");
    d.chron()
        .args(["--json", "health", "--service", ""])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"serving\":true"));
    Command::cargo_bin("chron")
        .unwrap()
        .args(["--addr", "http://127.0.0.1:1", "health"])
        .assert()
        .code(2);
}

#[test]
fn token_required_exits_1_with_a_hint_and_the_env_token_works() {
    let d = Daemon::start_with(|o| o.token = Some("s3cret".into()));
    d.chron().args(["head"]).assert().code(1).stderr(
        predicate::str::contains("Unauthenticated").and(predicate::str::contains("CHRON_TOKEN")),
    );
    d.chron()
        .env("CHRON_TOKEN", "s3cret")
        .args(["head"])
        .assert()
        .success()
        .stdout("0\n");
    d.chron()
        .args(["--token", "s3cret", "health"])
        .assert()
        .success();
}

#[test]
fn tls_and_mtls_flags() {
    let p = chrond::testing::pki();
    let tls = chrond::Tls {
        cert_pem: p.server_cert_pem.clone().into_bytes(),
        key_pem: p.server_key_pem.clone().into_bytes(),
        client_ca_pem: None,
    };
    let d = Daemon::start_with(move |o| o.tls = Some(tls));
    let paths = p.write_to(d.dir.path());
    d.chron()
        .args(["--tls-ca"])
        .arg(&paths.ca)
        .args(["--tls-domain", "localhost", "head"])
        .assert()
        .success()
        .stdout("0\n");
    d.chron().args(["head"]).assert().code(2);
    d.chron()
        .args(["--tls-ca", "/nonexistent/ca.pem", "head"])
        .assert()
        .code(2)
        .stderr(predicate::str::contains("cannot read the CA"));

    let p2 = chrond::testing::pki();
    let tls = chrond::Tls {
        cert_pem: p2.server_cert_pem.clone().into_bytes(),
        key_pem: p2.server_key_pem.clone().into_bytes(),
        client_ca_pem: Some(p2.ca_pem.clone().into_bytes()),
    };
    let m = Daemon::start_with(move |o| o.tls = Some(tls));
    let paths = p2.write_to(m.dir.path());
    // Without a client certificate the handshake fails after the connect;
    // which status reports it depends on timing, so only the refusal is
    // asserted.
    m.chron()
        .args(["--tls-ca"])
        .arg(&paths.ca)
        .args(["--tls-domain", "localhost", "head"])
        .assert()
        .failure()
        .stdout("");
    m.chron()
        .args(["--tls-ca"])
        .arg(&paths.ca)
        .args(["--tls-cert"])
        .arg(&paths.client_cert)
        .args(["--tls-key"])
        .arg(&paths.client_key)
        .args(["--tls-domain", "localhost", "head"])
        .assert()
        .success()
        .stdout("0\n");
}

#[test]
fn promote_fence_and_epoch_flags() {
    let leader = Daemon::start();
    leader
        .chron()
        .args(["append", A, "-d", "one"])
        .assert()
        .success();
    let replica = Daemon::start_replica_of(&leader);
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let out = replica
            .chron()
            .args(["head"])
            .assert()
            .success()
            .get_output()
            .stdout
            .clone();
        if String::from_utf8(out).unwrap().trim() == "1" {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the replica never caught up"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    replica.chron().args(["info"]).assert().success().stdout(
        predicate::str::contains("role replica").and(predicate::str::contains("replicating_from")),
    );
    replica
        .chron()
        .args(["append", A, "-d", "nope"])
        .assert()
        .code(1)
        .stderr(predicate::str::contains("read-only replica"));
    replica
        .chron()
        .args(["promote"])
        .assert()
        .success()
        .stdout(predicate::str::contains("promoted; epoch 1 head 1"));
    replica
        .chron()
        .args(["append", A, "--epoch", "0", "-d", "stale"])
        .assert()
        .code(1)
        .stderr(
            predicate::str::contains("stale epoch")
                .and(predicate::str::contains("(daemon epoch 1)")),
        );
    replica
        .chron()
        .args(["append", A, "--epoch", "1", "-d", "fresh"])
        .assert()
        .success();
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let out = leader
            .chron()
            .args(["info"])
            .assert()
            .success()
            .get_output()
            .stdout
            .clone();
        if String::from_utf8(out).unwrap().contains("role fenced") {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the old leader was never fenced"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    leader
        .chron()
        .args(["fence", "5"])
        .assert()
        .success()
        .stdout(predicate::str::contains("fenced; epoch 0"));
    leader
        .chron()
        .args(["append", A, "-d", "late"])
        .assert()
        .code(1)
        .stderr(predicate::str::contains("fenced"));
}

#[test]
fn info_flush_backup_restore_inspect_and_retain() {
    let d =
        Daemon::start_with(|o| o.backup_dir = Some(o.data_dir.parent().unwrap().join("backups")));
    d.chron()
        .args(["append", A, "-d", "one"])
        .assert()
        .success();
    d.chron().args(["info"]).assert().success().stdout(
        predicate::str::contains("head 1")
            .and(predicate::str::contains("segments 1"))
            .and(predicate::str::contains("fsync never")),
    );
    d.chron()
        .args(["--json", "info"])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"fsync\":\"never\""));
    d.chron()
        .args(["flush"])
        .assert()
        .success()
        .stdout("flushed\n");
    let archive = d.dir.path().join("backups/default/full.chronbak");
    d.chron()
        .args(["backup", "full.chronbak"])
        .assert()
        .success()
        .stdout(predicate::str::contains("full backup to head 1"));
    d.chron()
        .args(["append", A, "-d", "two"])
        .assert()
        .success();
    d.chron()
        .args(["backup", "--since", "1", "inc.chronbak"])
        .assert()
        .success()
        .stdout(predicate::str::contains("incremental backup 1..2"));
    d.chron()
        .args(["apply", "inc.chronbak"])
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
    d.chron()
        .args(["verify", "--max-record-bytes", "1048576"])
        .arg(&restored)
        .assert()
        .success();
    let empty = d.dir.path().join("empty");
    std::fs::create_dir(&empty).unwrap();
    d.chron()
        .args(["verify"])
        .arg(&empty)
        .assert()
        .code(2)
        .stderr(predicate::str::contains("holds no chron database"));
    assert!(
        std::fs::read_dir(&empty).unwrap().next().is_none(),
        "verify created nothing"
    );
    d.chron()
        .args(["rebuild-index"])
        .arg(d.dir.path().join("missing"))
        .assert()
        .code(2);
    std::fs::remove_file(restored.join("index.redb")).unwrap();
    d.chron()
        .args(["rebuild-index"])
        .arg(&restored)
        .assert()
        .success()
        .stdout(predicate::str::contains("rebuilt; head 1"));
    d.chron()
        .args(["verify"])
        .arg(d.dir.path().join("data/default"))
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

#[test]
fn admin_token_flag_and_env() {
    let d = Daemon::start_with(|o| {
        o.token = Some("user".into());
        o.admin_token = Some("admin".into());
    });
    d.chron()
        .env("CHRON_TOKEN", "user")
        .args(["retain", "0"])
        .assert()
        .code(1)
        .stderr(
            predicate::str::contains("PermissionDenied")
                .and(predicate::str::contains("--admin-token")),
        );
    d.chron()
        .env("CHRON_TOKEN", "user")
        .args(["--admin-token", "admin", "retain", "0"])
        .assert()
        .success();
    d.chron()
        .env("CHRON_TOKEN", "user")
        .env("CHRON_ADMIN_TOKEN", "admin")
        .args(["retain", "0"])
        .assert()
        .success();
}

#[test]
fn db_subcommands_and_the_db_flag() {
    let d = Daemon::start();
    d.chron()
        .args(["db", "create", "orders"])
        .assert()
        .success()
        .stdout("created orders\n");
    d.chron()
        .args(["--db", "orders", "append", A, "-d", "o0"])
        .assert()
        .success();
    d.chron().args(["head"]).assert().success().stdout("0\n");
    d.chron()
        .args(["--db", "orders", "head"])
        .assert()
        .success()
        .stdout("1\n");
    d.chron()
        .env("CHRON_DB", "orders")
        .args(["head"])
        .assert()
        .success()
        .stdout("1\n");
    d.chron().args(["db", "list"]).assert().success().stdout(
        predicate::str::contains("default  head 0").and(predicate::str::contains("orders  head 1")),
    );
    d.chron()
        .args(["--json", "db", "list"])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"name\":\"orders\""));
    d.chron()
        .args(["info"])
        .assert()
        .success()
        .stdout(predicate::str::contains("database default"));
    d.chron()
        .args(["--db", "Orders", "head"])
        .assert()
        .code(1)
        .stderr(predicate::str::contains("InvalidArgument"));
    d.chron()
        .args(["db", "drop", "orders"])
        .assert()
        .success()
        .stdout("dropped orders\n");
    d.chron()
        .args(["--db", "orders", "head"])
        .assert()
        .code(1)
        .stderr(
            predicate::str::contains("NotFound").and(predicate::str::contains("chron db list")),
        );
    d.chron()
        .args(["db", "drop", "default"])
        .assert()
        .code(1)
        .stderr(predicate::str::contains("cannot be dropped"));
}

#[test]
fn status_exit_codes() {
    let leader = Daemon::start();
    leader.chron().args(["status"]).assert().success().stdout(
        predicate::str::contains("role leader").and(predicate::str::contains("status healthy")),
    );
    leader
        .chron()
        .args(["--json", "status", "--max-lag", "0"])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"healthy\":true"));
    leader
        .chron()
        .args(["append", A, "-d", "one"])
        .assert()
        .success();
    let replica = Daemon::start_replica_of(&leader);
    // An unknown lag counts as healthy, so wait for the head itself.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let out = replica
            .chron()
            .args(["head"])
            .assert()
            .success()
            .get_output()
            .stdout
            .clone();
        if String::from_utf8(out).unwrap().trim() == "1" {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the replica never caught up"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    replica
        .chron()
        .args(["status", "--max-lag", "0"])
        .assert()
        .success();
    replica
        .running
        .as_ref()
        .unwrap()
        .node
        .default_db()
        .record_replica(|r| r.leader_head = Some(11));
    replica
        .chron()
        .args(["status", "--max-lag", "5"])
        .assert()
        .code(1)
        .stdout(predicate::str::contains("lag 10"))
        .stderr(predicate::str::contains("degraded: lag 10 above 5"));
    replica
        .chron()
        .args(["status", "--max-lag", "20"])
        .assert()
        .success();
    Command::cargo_bin("chron")
        .unwrap()
        .args(["--addr", "http://127.0.0.1:1", "status"])
        .assert()
        .code(2);
    drop(leader);
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let out = replica
            .chron()
            .args(["status"])
            .assert()
            .get_output()
            .clone();
        if out.status.code() == Some(1)
            && String::from_utf8_lossy(&out.stderr).contains("replica disconnected")
        {
            break;
        }
        assert!(std::time::Instant::now() < deadline, "never degraded");
        std::thread::sleep(Duration::from_millis(50));
    }
}

#[test]
fn append_takes_several_payloads_in_one_transaction() {
    let d = Daemon::start();
    d.chron()
        .args([
            "append", A, "--expect", "new", "--tag", "t", "-d", "one", "-d", "two", "-d", "three",
        ])
        .assert()
        .success()
        .stdout(
            predicate::str::contains("committed 0..2 (next 3)")
                .and(predicate::str::contains("count 3")),
        );
    d.chron().args(["len", A]).assert().success().stdout("3\n");
    d.chron().args(["read", A]).assert().success().stdout(
        predicate::str::contains(format!("0  {A}#0  [t]  one"))
            .and(predicate::str::contains(format!("2  {A}#2  [t]  three"))),
    );
    d.chron()
        .args(["append", "free", "--stdin"])
        .write_stdin("four\nfive\n")
        .assert()
        .success()
        .stdout(predicate::str::contains("committed 3..4 (next 5)"));
    d.chron()
        .args(["append", "free", "--stdin"])
        .write_stdin("")
        .assert()
        .code(2)
        .stderr(predicate::str::contains("at least one payload"));
    d.chron().args(["append", A]).assert().code(2);
}

#[test]
fn a_comma_list_of_addresses_finds_the_leader_and_discover_names_it() {
    use chrond::testing::Cluster;
    let rt = tokio::runtime::Runtime::new().unwrap();
    let ttl = Duration::from_secs(1);
    let mut c = rt.block_on(Cluster::start(3, ttl));
    let l = rt.block_on(c.wait_for_leader(Duration::from_secs(6)));
    for i in (0..3).filter(|i| *i != l) {
        rt.block_on(c.wait_following(i, l, Duration::from_secs(6)));
    }
    let f = (l + 1) % 3;
    let list = format!("{},{}", c.addr(f), c.addr(l));
    // A write through the list lands on the leader even though a
    // follower is named first.
    Command::cargo_bin("chron")
        .unwrap()
        .args(["--addr", &list, "append", "free", "-d", "via-list"])
        .assert()
        .success()
        .stdout(predicate::str::contains("committed 0"));
    // Discover on the follower names the leader; info shows the group.
    // Discover asks every listed node and prints one line each.
    Command::cargo_bin("chron")
        .unwrap()
        .args([
            "--addr",
            &format!("{},{}", c.addr(f), c.addr(l)),
            "discover",
        ])
        .assert()
        .success()
        .stdout(
            predicate::str::contains(format!("{}: leader {}", c.addr(f), c.addr(l)))
                .and(predicate::str::contains("role replica"))
                .and(predicate::str::contains(format!(
                    "{}: leader {}",
                    c.addr(l),
                    c.addr(l)
                )))
                .and(predicate::str::contains("role leader")),
        );
    Command::cargo_bin("chron")
        .unwrap()
        .args(["--addr", &c.addr(l), "--json", "discover"])
        .assert()
        .success()
        .stdout(
            predicate::str::contains("\"role\":\"leader\"")
                .and(predicate::str::contains("lease_remaining_ms")),
        );
    Command::cargo_bin("chron")
        .unwrap()
        .args(["--addr", &c.addr(l), "info"])
        .assert()
        .success()
        .stdout(
            predicate::str::contains("lease_remaining").and(predicate::str::contains("peers ")),
        );
    // status: a follower is healthy once its puller is connected.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let out = Command::cargo_bin("chron")
            .unwrap()
            .args(["--addr", &c.addr(f), "status"])
            .assert()
            .get_output()
            .clone();
        if out.status.success() && String::from_utf8_lossy(&out.stdout).contains("status healthy") {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the follower never became healthy: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        std::thread::sleep(Duration::from_millis(100));
    }
    // A single dead address first in the list still finds the leader.
    let list = format!("http://127.0.0.1:1,{}", c.addr(f));
    Command::cargo_bin("chron")
        .unwrap()
        .args(["--addr", &list, "head"])
        .assert()
        .success()
        .stdout(predicate::eq("1\n"));
    // Mixed schemes are a usage error.
    Command::cargo_bin("chron")
        .unwrap()
        .args(["--addr", &format!("https://x:1,{}", c.addr(f)), "head"])
        .assert()
        .code(2)
        .stderr(predicate::str::contains("one scheme"));
    // Stop two of three: the survivor is a degraded candidate, and the
    // list finds no leader (exit 2).
    rt.block_on(c.stop(l));
    rt.block_on(c.stop(f));
    let third = (0..3).find(|i| *i != l && *i != f).unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let out = Command::cargo_bin("chron")
            .unwrap()
            .args(["--addr", &c.addr(third), "status"])
            .assert()
            .get_output()
            .clone();
        let err = String::from_utf8_lossy(&out.stderr);
        if out.status.code() == Some(1)
            && (err.contains("candidate") || err.contains("replica disconnected"))
        {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "never degraded: {err}"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
    Command::cargo_bin("chron")
        .unwrap()
        .args([
            "--addr",
            &format!("{},{}", c.addr(third), c.addr(l)),
            "head",
        ])
        .assert()
        .code(2)
        .stderr(predicate::str::contains("no leader"));
    rt.block_on(c.stop_all());
}

#[test]
fn append_sync_and_async_flags() {
    let d = Daemon::start();
    d.chron()
        .args(["append", "free", "-d", "a", "--sync"])
        .assert()
        .success()
        .stdout(predicate::str::contains("committed 0"));
    d.chron()
        .args(["append", "free", "-d", "b", "--async"])
        .assert()
        .success()
        .stdout(predicate::str::contains("committed 1"));
    d.chron()
        .args(["append", "free", "-d", "c", "--sync", "--async"])
        .assert()
        .code(2);
    d.chron()
        .args(["info"])
        .assert()
        .success()
        .stdout(predicate::str::contains("head 2"));
}
