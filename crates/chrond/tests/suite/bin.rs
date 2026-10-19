//! The `chrond` binary's flags that the library cannot exercise.

use std::io::{BufRead, BufReader};
use std::process::{Command, Stdio};
use std::time::Duration;

fn spawn(args: &[&str]) -> std::process::Child {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    std::mem::forget(dir);
    Command::new(env!("CARGO_BIN_EXE_chrond"))
        .arg("--data-dir")
        .arg(&data)
        .args(["--listen", "127.0.0.1:0", "--no-fsync"])
        .args(args)
        .stderr(Stdio::piped())
        .stdout(Stdio::null())
        .spawn()
        .unwrap()
}

#[test]
fn json_logs_carry_the_listening_line_as_an_object() {
    let mut child = spawn(&["--log-format", "json"]);
    let stderr = child.stderr.take().unwrap();
    let mut lines = BufReader::new(stderr).lines();
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    let line = loop {
        assert!(std::time::Instant::now() < deadline, "no listening line");
        let line = lines.next().expect("stderr open").unwrap();
        if line.contains("listening") {
            break line;
        }
    };
    child.kill().unwrap();
    child.wait().unwrap();
    let v: serde_json::Value = serde_json::from_str(&line).expect("a JSON line");
    assert_eq!(v["message"], "listening");
    assert_eq!(v["level"], "INFO");
    assert_eq!(v["role"], "Leader");
    assert!(v["local_addr"].as_str().unwrap().starts_with("127.0.0.1:"));
    assert_eq!(v["databases"], "default");
}

#[test]
fn text_logs_are_not_json_and_a_bad_format_is_usage() {
    let mut child = spawn(&["--log-format", "text"]);
    let stderr = child.stderr.take().unwrap();
    let mut lines = BufReader::new(stderr).lines();
    let line = loop {
        let line = lines.next().expect("stderr open").unwrap();
        if line.contains("listening") {
            break line;
        }
    };
    child.kill().unwrap();
    child.wait().unwrap();
    assert!(serde_json::from_str::<serde_json::Value>(&line).is_err());

    let out = Command::new(env!("CARGO_BIN_EXE_chrond"))
        .args(["--data-dir", "/nonexistent", "--log-format", "yaml"])
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(2));
}

#[test]
fn group_commit_and_checkpoint_flags_take_at_least_one() {
    for flag in [
        "--group-commit-max-txns",
        "--group-commit-max-bytes",
        "--index-checkpoint-batches",
    ] {
        let dir = tempfile::tempdir().unwrap();
        let out = Command::new(env!("CARGO_BIN_EXE_chrond"))
            .arg("--data-dir")
            .arg(dir.path().join("data"))
            .args(["--listen", "127.0.0.1:0", flag, "0"])
            .output()
            .unwrap();
        assert_eq!(out.status.code(), Some(2), "{flag}");
        let err = String::from_utf8_lossy(&out.stderr);
        assert!(
            err.contains("1..") || err.contains("not in"),
            "{flag}: {err}"
        );
    }
}
