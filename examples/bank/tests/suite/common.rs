//! Scenarios generic over the store, run embedded and through a daemon.

use std::time::Duration;

use bank::{AccountEvent, Balances, BankError, Embedded, Store, TAG, commands};
use chron::{Fsync, Options};
use futures::StreamExt;

pub fn embedded() -> (Embedded, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let db = chron::Db::open(dir.path().join("bank"), Options::new().fsync(Fsync::Never)).unwrap();
    (Embedded(db), dir)
}

pub async fn remote() -> (chrond::Running, chron_client::Client, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let mut opts = chrond::Options::new(dir.path().join("data"), "127.0.0.1:0".parse().unwrap());
    opts.fsync = false;
    let running = chrond::start(opts).await.unwrap();
    let client =
        chron_client::Client::connect(format!("http://{}", running.local_addr), Default::default())
            .await
            .unwrap();
    (running, client, dir)
}

pub async fn open_deposit_withdraw_history<S: Store>(s: &S) {
    let id = commands::open(s, "alice").await.unwrap();
    assert_eq!(commands::deposit(s, id, 100).await.unwrap().balance, 100);
    let state = commands::withdraw(s, id, 30).await.unwrap();
    assert_eq!(
        (state.balance, state.version, state.owner.as_deref()),
        (70, 3, Some("alice"))
    );
    let history: Vec<AccountEvent> = s
        .read_aggregate(id, 0, 0)
        .await
        .unwrap()
        .iter()
        .map(|e| AccountEvent::decode(e).unwrap())
        .collect();
    assert_eq!(
        history,
        [
            AccountEvent::Opened {
                owner: "alice".into()
            },
            AccountEvent::Deposited { amount: 100 },
            AccountEvent::Withdrawn { amount: 30 },
        ]
    );
    assert_eq!(commands::load(s, id).await.unwrap(), state);
    assert!(matches!(
        commands::deposit(s, id, 0).await,
        Err(BankError::InvalidAmount)
    ));
    assert!(matches!(
        commands::deposit(s, chron::Uuid::new_v4(), 5).await,
        Err(BankError::NoSuchAccount)
    ));
}

pub async fn overdraft_is_refused_and_nothing_written<S: Store>(s: &S) {
    let id = commands::open(s, "bob").await.unwrap();
    commands::deposit(s, id, 10).await.unwrap();
    assert!(matches!(
        commands::withdraw(s, id, 500).await,
        Err(BankError::InsufficientFunds {
            balance: 10,
            requested: 500
        })
    ));
    assert_eq!(s.read_aggregate(id, 0, 0).await.unwrap().len(), 2);
}

pub async fn closed_refuses_and_open_is_guarded<S: Store>(s: &S) {
    let id = commands::open(s, "carol").await.unwrap();
    commands::close(s, id).await.unwrap();
    assert!(matches!(
        commands::deposit(s, id, 5).await,
        Err(BankError::Closed)
    ));
    assert!(matches!(
        commands::close(s, id).await,
        Err(BankError::Closed)
    ));
}

pub async fn concurrent_withdrawals_never_overdraw<S: Store>(s: &S) {
    for round in 0..20 {
        let id = commands::open(s, "dave").await.unwrap();
        commands::deposit(s, id, 100).await.unwrap();
        let (a, b) = (s.clone(), s.clone());
        let x = tokio::spawn(async move { commands::withdraw(&a, id, 60).await });
        let y = tokio::spawn(async move { commands::withdraw(&b, id, 60).await });
        let (x, y) = (x.await.unwrap(), y.await.unwrap());
        let oks = [x.is_ok(), y.is_ok()].iter().filter(|b| **b).count();
        assert_eq!(
            oks, 1,
            "round {round}: exactly one withdrawal succeeds: {x:?} {y:?}"
        );
        for r in [x, y] {
            if let Err(e) = r {
                assert!(
                    matches!(
                        e,
                        BankError::InsufficientFunds {
                            balance: 40,
                            requested: 60
                        }
                    ),
                    "round {round}: {e}"
                );
            }
        }
        let state = commands::load(s, id).await.unwrap();
        assert_eq!((state.balance, state.version), (40, 3), "round {round}");
    }
}

pub async fn balances_projection_equals_replayed_states<S: Store>(s: &S) {
    let ids = [
        commands::open(s, "x").await.unwrap(),
        commands::open(s, "y").await.unwrap(),
        commands::open(s, "z").await.unwrap(),
    ];
    for (i, id) in ids.iter().enumerate() {
        commands::deposit(s, *id, 10 * (i as i64 + 1))
            .await
            .unwrap();
        commands::withdraw(s, *id, 1).await.unwrap();
    }
    let head = s.head().await.unwrap();
    let mut live = Balances::default();
    let mut stream = s.subscribe_tag(TAG, 0).await.unwrap();
    for _ in 0..head {
        let ev = tokio::time::timeout(Duration::from_secs(10), stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        live.apply(&ev).unwrap();
    }
    let mut caught = Balances::default();
    caught.catch_up(s).await.unwrap();
    assert_eq!(live, caught);
    assert_eq!(caught.checkpoint, head);
    for id in ids {
        assert_eq!(
            caught.balances[&id],
            commands::load(s, id).await.unwrap().balance
        );
    }
    // A closed account leaves the projection; the checkpoint still moves.
    commands::close(s, ids[0]).await.unwrap();
    caught.catch_up(s).await.unwrap();
    assert!(!caught.balances.contains_key(&ids[0]));
    assert_eq!(caught.checkpoint, s.head().await.unwrap());
    // Saved and loaded, it resumes from its checkpoint rather than from 0.
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("balances.json");
    caught.save(&file).unwrap();
    let mut resumed = Balances::load(&file).unwrap();
    assert_eq!(resumed, caught);
    commands::deposit(s, ids[1], 5).await.unwrap();
    resumed.catch_up(s).await.unwrap();
    assert_eq!(resumed.balances[&ids[1]], caught.balances[&ids[1]] + 5);
    assert_eq!(resumed.checkpoint, s.head().await.unwrap());
    assert_eq!(
        Balances::load(&dir.path().join("none.json")).unwrap(),
        Balances::default()
    );
}
