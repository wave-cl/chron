//! Certificates for tests (feature `test-util`): a CA, a server
//! certificate for `localhost` and `127.0.0.1`, a client certificate, and
//! a client certificate from an unrelated CA.

use std::path::{Path, PathBuf};

use rcgen::{BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, KeyPair};

pub struct Pki {
    pub ca_pem: String,
    pub server_cert_pem: String,
    pub server_key_pem: String,
    pub client_cert_pem: String,
    pub client_key_pem: String,
    pub other_ca_client_cert_pem: String,
    pub other_ca_client_key_pem: String,
}

pub struct PkiPaths {
    pub ca: PathBuf,
    pub server_cert: PathBuf,
    pub server_key: PathBuf,
    pub client_cert: PathBuf,
    pub client_key: PathBuf,
}

fn ca() -> (rcgen::Certificate, KeyPair) {
    let key = KeyPair::generate().expect("key");
    let mut params = CertificateParams::new(Vec::<String>::new()).expect("params");
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    (params.self_signed(&key).expect("ca"), key)
}

fn leaf(
    sans: &[&str],
    purpose: ExtendedKeyUsagePurpose,
    ca: &(rcgen::Certificate, KeyPair),
) -> (String, String) {
    let key = KeyPair::generate().expect("key");
    let mut params = CertificateParams::new(sans.iter().map(|s| s.to_string()).collect::<Vec<_>>())
        .expect("params");
    params.extended_key_usages = vec![purpose];
    let cert = params.signed_by(&key, &ca.0, &ca.1).expect("leaf");
    (cert.pem(), key.serialize_pem())
}

/// Fresh certificates every call.
pub fn pki() -> Pki {
    let root = ca();
    let other = ca();
    let (server_cert_pem, server_key_pem) = leaf(
        &["localhost", "127.0.0.1"],
        ExtendedKeyUsagePurpose::ServerAuth,
        &root,
    );
    let (client_cert_pem, client_key_pem) =
        leaf(&["client"], ExtendedKeyUsagePurpose::ClientAuth, &root);
    let (other_ca_client_cert_pem, other_ca_client_key_pem) =
        leaf(&["client"], ExtendedKeyUsagePurpose::ClientAuth, &other);
    Pki {
        ca_pem: root.0.pem(),
        server_cert_pem,
        server_key_pem,
        client_cert_pem,
        client_key_pem,
        other_ca_client_cert_pem,
        other_ca_client_key_pem,
    }
}

impl Pki {
    /// Writes `ca.pem`, `server.pem`, `server.key`, `client.pem`,
    /// `client.key` into `dir`.
    pub fn write_to(&self, dir: &Path) -> PkiPaths {
        let paths = PkiPaths {
            ca: dir.join("ca.pem"),
            server_cert: dir.join("server.pem"),
            server_key: dir.join("server.key"),
            client_cert: dir.join("client.pem"),
            client_key: dir.join("client.key"),
        };
        std::fs::write(&paths.ca, &self.ca_pem).expect("write");
        std::fs::write(&paths.server_cert, &self.server_cert_pem).expect("write");
        std::fs::write(&paths.server_key, &self.server_key_pem).expect("write");
        std::fs::write(&paths.client_cert, &self.client_cert_pem).expect("write");
        std::fs::write(&paths.client_key, &self.client_key_pem).expect("write");
        paths
    }
}

// ---- a failover group for tests

use std::sync::Arc;
use std::time::Duration;

use crate::node::{Node, Role};
use crate::{Options, Running};

/// One member of a [`Cluster`]: its directory, its fixed port and, while
/// it runs, the daemon.
pub struct ClusterNode {
    pub dir: tempfile::TempDir,
    pub port: u16,
    pub running: Option<Running>,
}

impl ClusterNode {
    pub fn addr(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }

    /// The daemon's node; panics while stopped.
    pub fn node(&self) -> Arc<Node> {
        self.running.as_ref().expect("running").node.clone()
    }

    pub fn is_running(&self) -> bool {
        self.running.is_some()
    }
}

/// `n` in-process daemons on fixed loopback ports, each with the others
/// as peers, `fsync` off, one lease ttl.
pub struct Cluster {
    pub nodes: Vec<ClusterNode>,
    pub ttl: Duration,
}

/// A free TCP port: bound then released, so a daemon can be restarted on it.
pub fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("bind")
        .local_addr()
        .expect("addr")
        .port()
}

impl Cluster {
    pub async fn start(n: usize, ttl: Duration) -> Cluster {
        Self::start_with(n, ttl, |_| {}).await
    }

    /// `start` with `f` adjusting every node's options.
    pub async fn start_with(n: usize, ttl: Duration, f: impl Fn(&mut Options)) -> Cluster {
        let ports: Vec<u16> = (0..n).map(|_| free_port()).collect();
        let mut cluster = Cluster {
            nodes: Vec::new(),
            ttl,
        };
        for port in &ports {
            cluster.nodes.push(ClusterNode {
                dir: tempfile::tempdir().expect("tempdir"),
                port: *port,
                running: None,
            });
        }
        for i in 0..n {
            cluster.restart_with(i, &f).await;
        }
        cluster
    }

    /// Holds (or releases) every apply on node `i`: its acknowledgments
    /// stop without its pullers stopping.
    pub fn hold_apply(&self, i: usize, held: bool) {
        self.node(i).hold_apply.send_replace(held);
    }

    /// The followers node `i` sees on `default`, when it leads.
    pub fn followers(&self, i: usize) -> Vec<chron_proto::v1::FollowerStatus> {
        self.node(i).default_db().sync.snapshot()
    }

    /// The majority-applied position node `i` has for `default`.
    pub fn majority_applied(&self, i: usize) -> u64 {
        self.node(i).default_db().sync.majority()
    }

    /// A client with no request timeout, for watching a commit pend.
    pub async fn client_patient(&self, i: usize) -> chron_client::Client {
        let config = chron_client::ClientConfig {
            connect_timeout: Duration::from_millis(500),
            ..Default::default()
        };
        chron_client::Client::connect(self.addr(i), config)
            .await
            .expect("connect")
    }

    /// The options node `i` starts with: its directory and port, every
    /// other node as a peer.
    pub fn options(&self, i: usize) -> Options {
        let me = &self.nodes[i];
        let mut o = Options::new(
            me.dir.path().join("data"),
            format!("127.0.0.1:{}", me.port).parse().expect("addr"),
        );
        o.fsync = false;
        o.lease_ttl = self.ttl;
        // Databases created on the leader show up on followers within a
        // second, not the ten of the default.
        o.discover_interval = Duration::from_secs(1);
        // Below the 2 s request timeout of `client`, above the time a
        // follower takes to attach after an election.
        o.sync_timeout = Some(Duration::from_millis(1500));
        o.peers = self
            .nodes
            .iter()
            .enumerate()
            .filter(|(j, _)| *j != i)
            .map(|(_, n)| n.addr())
            .collect();
        o
    }

    /// Starts (or restarts) node `i` with `f` adjusting its options.
    pub async fn restart_with(&mut self, i: usize, f: impl FnOnce(&mut Options)) {
        assert!(self.nodes[i].running.is_none(), "node {i} is running");
        let mut o = self.options(i);
        f(&mut o);
        let running = crate::start(o).await.expect("the node starts");
        self.nodes[i].running = Some(running);
    }

    pub async fn restart(&mut self, i: usize) {
        self.restart_with(i, |_| {}).await
    }

    /// Stops node `i` (a graceful shutdown: its heartbeats just stop).
    pub async fn stop(&mut self, i: usize) {
        let r = self.nodes[i].running.take().expect("running");
        r.shutdown().await.expect("shutdown");
    }

    pub fn node(&self, i: usize) -> Arc<Node> {
        self.nodes[i].node()
    }

    pub fn addr(&self, i: usize) -> String {
        self.nodes[i].addr()
    }

    /// The role of node `i`, `None` while it is stopped.
    pub fn role(&self, i: usize) -> Option<Role> {
        self.nodes[i].running.as_ref().map(|r| r.node.role())
    }

    /// A client for node `i` with short timeouts, so a dead node fails
    /// fast.
    pub async fn client(&self, i: usize) -> chron_client::Client {
        let config = chron_client::ClientConfig {
            connect_timeout: Duration::from_millis(500),
            request_timeout: Some(Duration::from_secs(2)),
            ..Default::default()
        };
        chron_client::Client::connect(self.addr(i), config)
            .await
            .expect("connect")
    }

    /// Cuts node `i` off on the control plane: heartbeats, votes and the
    /// other cluster calls neither leave it nor are answered by it, in
    /// both directions. Replication streams, Fingerprints, the fencer and
    /// clients still pass, so a leader cut off this way loses its lease
    /// but its followers keep pulling from it until they switch.
    pub fn partition(&self, i: usize) {
        let me = self.addr(i);
        for (j, n) in self.nodes.iter().enumerate() {
            let Some(r) = &n.running else { continue };
            let mut set = r.node.partition.lock().unwrap_or_else(|p| p.into_inner());
            if j == i {
                for (k, o) in self.nodes.iter().enumerate() {
                    if k != i {
                        set.insert(o.addr());
                    }
                }
            } else {
                set.insert(me.clone());
            }
        }
    }

    /// Cuts the link between nodes `i` and `j` only.
    pub fn cut(&self, i: usize, j: usize) {
        for (a, b) in [(i, j), (j, i)] {
            if let Some(r) = &self.nodes[a].running {
                r.node
                    .partition
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .insert(self.addr(b));
            }
        }
    }

    /// Undoes [`cut`](Cluster::cut).
    pub fn uncut(&self, i: usize, j: usize) {
        for (a, b) in [(i, j), (j, i)] {
            if let Some(r) = &self.nodes[a].running {
                r.node
                    .partition
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .remove(&self.addr(b));
            }
        }
    }

    /// One line per node: role, epoch, head, leader, replication error.
    pub fn describe(&self) -> String {
        self.nodes
            .iter()
            .enumerate()
            .map(|(i, n)| match &n.running {
                None => format!("node {i} ({}): stopped", n.addr()),
                Some(r) => {
                    let st = r.node.default_db().replica_status();
                    format!(
                        "node {i} ({}): {:?} epoch {} floor {} head {} leader {:?} lease {:?} connected {} error {:?}",
                        n.addr(),
                        r.node.role(),
                        r.node.data_epoch(),
                        r.node.floor_epoch(),
                        r.node.default_db().db.head(),
                        r.node.leader_addr(),
                        r.node.lease_remaining(),
                        st.connected,
                        st.last_error
                    )
                }
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    /// Undoes [`partition`](Cluster::partition).
    pub fn heal(&self, i: usize) {
        let me = self.addr(i);
        for (j, n) in self.nodes.iter().enumerate() {
            let Some(r) = &n.running else { continue };
            let mut set = r.node.partition.lock().unwrap_or_else(|p| p.into_inner());
            if j == i {
                set.clear();
            } else {
                set.remove(&me);
            }
        }
    }

    /// The one running node that leads with a live lease, if exactly one
    /// does.
    pub fn leader(&self) -> Option<usize> {
        let leaders: Vec<usize> = self
            .nodes
            .iter()
            .enumerate()
            .filter(|(_, n)| {
                n.running
                    .as_ref()
                    .is_some_and(|r| r.node.role() == Role::Leader && r.node.lease_held())
            })
            .map(|(i, _)| i)
            .collect();
        match leaders.as_slice() {
            [one] => Some(*one),
            _ => None,
        }
    }

    /// Polls `f` every 20 ms until it is true; panics after `within`.
    pub async fn wait_until(what: &str, within: Duration, mut f: impl FnMut() -> bool) {
        let deadline = std::time::Instant::now() + within;
        while !f() {
            assert!(
                std::time::Instant::now() < deadline,
                "timed out after {within:?} waiting for {what}"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// [`wait_until`](Cluster::wait_until) that describes every node on
    /// timeout.
    pub async fn wait(&self, what: &str, within: Duration, mut f: impl FnMut() -> bool) {
        let deadline = std::time::Instant::now() + within;
        while !f() {
            assert!(
                std::time::Instant::now() < deadline,
                "timed out after {within:?} waiting for {what}:\n{}",
                self.describe()
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// Waits until exactly one node leads with a live lease and returns it.
    pub async fn wait_for_leader(&self, within: Duration) -> usize {
        let mut found = None;
        self.wait("a leader", within, || {
            found = self.leader();
            found.is_some()
        })
        .await;
        found.expect("a leader")
    }

    /// Waits until node `i` is a replica following `leader`.
    pub async fn wait_following(&self, i: usize, leader: usize, within: Duration) {
        let want = self.addr(leader);
        self.wait(&format!("node {i} following node {leader}"), within, || {
            self.nodes[i].running.as_ref().is_some_and(|r| {
                r.node.role() == Role::Replica && r.node.leader_addr().as_deref() == Some(&want)
            })
        })
        .await;
    }

    /// Waits until every running node's `default` head is `head`.
    pub async fn wait_heads(&self, head: u64, within: Duration) {
        self.wait(&format!("every head at {head}"), within, || {
            self.nodes
                .iter()
                .filter_map(|n| n.running.as_ref())
                .all(|r| r.node.default_db().db.head() == head)
        })
        .await;
    }

    /// Stops every running node.
    pub async fn stop_all(&mut self) {
        for i in 0..self.nodes.len() {
            if self.nodes[i].running.is_some() {
                self.stop(i).await;
            }
        }
    }
}

impl Node {
    /// `chron_elections_total` as a number, for tests.
    pub fn metrics_elections(&self) -> u64 {
        self.metrics.elections.get()
    }
}

impl Node {
    /// `chron_sync_commit_wait_seconds` observations on `default`.
    pub fn metrics_sync_waits(&self) -> u64 {
        self.metrics
            .sync_wait
            .with_label_values(&["default"])
            .get_sample_count()
    }

    /// `chron_sync_commit_timeouts_total{reason}` on `default`.
    pub fn metrics_sync_timeouts(&self, reason: &str) -> u64 {
        self.metrics
            .sync_timeouts
            .with_label_values(&["default", reason])
            .get()
    }
}
