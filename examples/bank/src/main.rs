use std::path::PathBuf;

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
    #[arg(long, env = "CHRON_TOKEN", hide_env_values = true)]
    token: Option<String>,
    #[arg(long, env = "CHRON_TLS_CA")]
    tls_ca: Option<PathBuf>,
    #[arg(long, env = "CHRON_TLS_DOMAIN")]
    tls_domain: Option<String>,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Open an account; prints its id.
    Open {
        owner: String,
    },
    Deposit {
        id: Uuid,
        amount: i64,
    },
    Withdraw {
        id: Uuid,
        amount: i64,
    },
    Close {
        id: Uuid,
    },
    /// Every account's balance, from the projection.
    Balances {
        /// Keep following and reprint on every change.
        #[arg(long)]
        follow: bool,
    },
    /// An account's events.
    History {
        id: Uuid,
    },
}

async fn run<S: Store>(store: S, command: Command) -> anyhow::Result<()> {
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
            let mut b = Balances::default();
            b.catch_up(&store).await?;
            print_balances(&b);
            if follow {
                b.follow(&store, print_balances).await?;
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
            let tls = addr.starts_with("https://").then(|| TlsConfig {
                ca_pem: cli
                    .tls_ca
                    .as_ref()
                    .map(std::fs::read)
                    .transpose()
                    .ok()
                    .flatten(),
                identity_pem: None,
                domain: cli.tls_domain.clone(),
            });
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
        AnyStore::Embedded(s) => run(s, cli.command).await,
        AnyStore::Remote(c) => run(*c, cli.command).await,
    }
}
