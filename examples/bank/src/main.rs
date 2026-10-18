use std::path::PathBuf;

use anyhow::Context;
use bank::{AnyStore, Balances, Embedded, Store, commands};
use chron::Uuid;
use chron_client::{Client, ClientConfig, TlsConfig};
use clap::{Parser, Subcommand};

/// Accounts on chron, embedded (default) or through a chrond daemon.
#[derive(Parser)]
#[command(name = "bank", version)]
struct Cli {
    /// The embedded database directory.
    #[arg(
        long,
        env = "BANK_DATA_DIR",
        default_value = "./bank.chron",
        conflicts_with = "addr"
    )]
    data_dir: PathBuf,
    /// Use the daemon at this address instead of an embedded database.
    #[arg(long, env = "BANK_ADDR")]
    addr: Option<String>,
    /// Bearer token the daemon requires.
    #[arg(long, env = "CHRON_TOKEN", hide_env_values = true)]
    token: Option<String>,
    /// PEM of a private CA to verify an https:// daemon with.
    #[arg(long, env = "CHRON_TLS_CA")]
    tls_ca: Option<PathBuf>,
    /// The name to verify the daemon's certificate against.
    #[arg(long, env = "CHRON_TLS_DOMAIN")]
    tls_domain: Option<String>,
    /// Where the balances projection keeps its state between runs.
    #[arg(long, env = "BANK_PROJECTION", default_value = "./bank.balances.json")]
    projection: PathBuf,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Open an account; prints its id.
    Open {
        /// The account holder's name.
        owner: String,
    },
    /// Add money to an account.
    Deposit {
        /// The account's id, as `open` printed it.
        id: Uuid,
        /// A positive amount.
        amount: i64,
    },
    /// Take money out of an account; refused beyond its balance.
    Withdraw {
        /// The account's id.
        id: Uuid,
        /// A positive amount.
        amount: i64,
    },
    /// Close an account; nothing can be done with it afterwards.
    Close {
        /// The account's id.
        id: Uuid,
    },
    /// Every open account's balance, from the projection (resumed from
    /// the projection file, then caught up).
    Balances {
        /// Keep following and reprint on every change.
        #[arg(long)]
        follow: bool,
    },
    /// An account's events.
    History {
        /// The account's id.
        id: Uuid,
    },
}

async fn run<S: Store>(
    store: S,
    command: Command,
    projection: &std::path::Path,
) -> anyhow::Result<()> {
    match command {
        Command::Open { owner } => println!("{}", commands::open(&store, &owner).await?),
        Command::Deposit { id, amount } => {
            let s = commands::deposit(&store, id, amount).await?;
            println!("balance {}", s.balance);
        }
        Command::Withdraw { id, amount } => {
            let s = commands::withdraw(&store, id, amount).await?;
            println!("balance {}", s.balance);
        }
        Command::Close { id } => {
            commands::close(&store, id).await?;
            println!("closed");
        }
        Command::Balances { follow } => {
            let mut b = Balances::load(projection)?;
            b.catch_up(&store).await?;
            b.save(projection)?;
            print_balances(&b);
            if follow {
                b.follow(&store, |b| {
                    print_balances(b);
                    if let Err(e) = b.save(projection) {
                        eprintln!("bank: {e}");
                    }
                })
                .await?;
            }
        }
        Command::History { id } => {
            for ev in store.read_aggregate(id, 0, 0).await? {
                let e = bank::AccountEvent::decode(&ev)?;
                println!("{:>4}  {:?}  {}", ev.index, e, ev.recorded_at);
            }
        }
    }
    Ok(())
}

fn print_balances(b: &Balances) {
    let mut rows: Vec<_> = b.balances.iter().collect();
    rows.sort();
    for (id, balance) in rows {
        println!("{id}  {balance}");
    }
    println!("-- checkpoint {}", b.checkpoint);
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let store = match &cli.addr {
        Some(addr) => {
            let tls = if addr.starts_with("https://") {
                let ca_pem = match &cli.tls_ca {
                    Some(p) => Some(
                        std::fs::read(p)
                            .with_context(|| format!("cannot read the CA at {}", p.display()))?,
                    ),
                    None => None,
                };
                Some(TlsConfig {
                    ca_pem,
                    identity: None,
                    domain: cli.tls_domain.clone(),
                })
            } else {
                None
            };
            let config = ClientConfig {
                tls,
                token: cli.token.clone(),
                ..ClientConfig::default()
            };
            AnyStore::Remote(Box::new(Client::connect(addr.clone(), config).await?))
        }
        None => AnyStore::Embedded(Embedded(chron::Db::open(
            &cli.data_dir,
            chron::Options::new(),
        )?)),
    };
    match store {
        AnyStore::Embedded(s) => run(s, cli.command, &cli.projection).await,
        AnyStore::Remote(c) => run(*c, cli.command, &cli.projection).await,
    }
}
