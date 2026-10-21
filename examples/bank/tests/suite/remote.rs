use std::time::Duration;

use bank::{Balances, TAG, commands};

use crate::common::*;

#[tokio::test(flavor = "multi_thread")]
async fn scenarios_through_a_daemon() {
    let (running, client, _d) = remote().await;
    open_deposit_withdraw_history(&client).await;
    overdraft_is_refused_and_nothing_written(&client).await;
    closed_refuses_and_open_is_guarded(&client).await;
    concurrent_withdrawals_never_overdraw(&client).await;
    balances_projection_equals_replayed_states(&client).await;
    running.shutdown().await.unwrap();
}

/// Followed through the daemon's durable cursor, the projection resumes
/// where the last run acknowledged: a second run starts at the cursor,
/// not at zero, and ends equal to a fresh catch-up.
#[tokio::test(flavor = "multi_thread")]
async fn balances_follow_through_a_cursor_resumes_where_it_acked() {
    use std::sync::{Arc, Mutex};
    let (running, client, _d) = remote().await;
    let ids = [
        commands::open(&client, "x").await.unwrap(),
        commands::open(&client, "y").await.unwrap(),
        commands::open(&client, "z").await.unwrap(),
    ];
    for (i, id) in ids.iter().enumerate() {
        commands::deposit(&client, *id, 10 * (i as i64 + 1))
            .await
            .unwrap();
    }
    let head = client.head().await.unwrap();
    // Run 1: three events, then the process is gone.
    let saved = Arc::new(Mutex::new(Balances::default()));
    let (tx, mut rx) = tokio::sync::mpsc::channel(32);
    let first = tokio::spawn({
        let client = client.clone();
        let saved = saved.clone();
        async move {
            let mut b = Balances::default();
            b.follow_durable(&client, "balances", |b| {
                *saved.lock().unwrap() = b.clone();
                let _ = tx.try_send(b.checkpoint);
            })
            .await
        }
    });
    let mut reached = 0;
    for _ in 0..3 {
        reached = tokio::time::timeout(Duration::from_secs(10), rx.recv())
            .await
            .unwrap()
            .unwrap();
    }
    first.abort();
    let _ = first.await;
    let mut b = saved.lock().unwrap().clone();
    assert_eq!(b.checkpoint, reached);
    // Run 2: the cursor resumes at the acknowledgment, not at zero.
    let (tx, mut rx) = tokio::sync::mpsc::channel(32);
    let second = tokio::spawn({
        let client = client.clone();
        async move {
            let r = b
                .follow_durable(&client, "balances", |b| {
                    let _ = tx.try_send(b.clone());
                })
                .await;
            (r, b)
        }
    });
    let last = loop {
        let b = tokio::time::timeout(Duration::from_secs(10), rx.recv())
            .await
            .unwrap()
            .unwrap();
        if b.checkpoint >= head {
            break b;
        }
    };
    second.abort();
    let _ = second.await;
    let cursors = client.cursors().await.unwrap();
    assert_eq!(cursors.cursors[0].next, head, "every event acknowledged");
    let resumed_from = reached;
    let mut caught = Balances::default();
    caught.catch_up(&client).await.unwrap();
    assert_eq!(last, caught);
    // A fresh subscription on the cursor starts where the runs got to,
    // which is past where the first run stopped.
    let (_stream, cursor) = client
        .subscribe_cursor("balances", 0, Some(TAG.to_string().into()))
        .await
        .unwrap();
    assert!(
        cursor.from >= resumed_from && cursor.from == head,
        "{}",
        cursor.from
    );
    running.shutdown().await.unwrap();
}
