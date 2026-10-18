//! `chron`: talk to a `chrond` daemon, or maintain a database directory
//! offline.

mod client;
mod cmd;
mod output;

use clap::{Parser, Subcommand};

use crate::output::Format;

#[derive(Parser, Debug)]
#[command(
    name = "chron",
    version,
    about = "Client for a chrond daemon and offline maintenance"
)]
struct Cli {
    /// The daemon's address.
    #[arg(
        long,
        global = true,
        env = "CHRON_ADDR",
        default_value = "http://127.0.0.1:4242"
    )]
    addr: String,
    /// One JSON object per line instead of human lines.
    #[arg(long, global = true)]
    json: bool,
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Append events to one aggregate (or `free`) in one transaction.
    Append(cmd::write::AppendArgs),
    /// Commit a transaction described in JSON.
    Commit(cmd::write::CommitArgs),
    /// Read an aggregate's events.
    Read(cmd::read::AggregateArgs),
    /// Read the whole database from a position.
    All(cmd::read::AllArgs),
    /// Read the events carrying a tag.
    Tag(cmd::read::TagArgs),
    /// Follow the database (or one tag) live until stopped.
    Tail(cmd::read::TailArgs),
    /// How many events an aggregate has.
    Len { aggregate: String },
    /// The next global position.
    Head,
    /// Path, head, floor, segments and options of the served database.
    Info,
    /// Make everything committed so far durable.
    Flush,
    /// Write a backup to a path on the daemon's filesystem.
    Backup(cmd::admin::BackupArgs),
    /// Apply an incremental backup from a path on the daemon's filesystem.
    Apply { archive: String },
    /// Drop segments wholly below a position.
    Retain { floor: u64 },
    /// Offline: rebuild a database's index from its segment files.
    RebuildIndex { path: std::path::PathBuf },
    /// Offline: open a database checking every segment and the index.
    Verify { path: std::path::PathBuf },
    /// Offline: restore a full backup into a new directory.
    Restore {
        archive: std::path::PathBuf,
        dir: std::path::PathBuf,
    },
    /// Offline: print a backup's header.
    Inspect { archive: std::path::PathBuf },
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let format = if cli.json {
        Format::Json
    } else {
        Format::Human
    };
    let code = match run(cli, format).await {
        Ok(()) => 0,
        Err(e) => {
            eprintln!("chron: {}", output::render_error(&e));
            output::exit_code(&e)
        }
    };
    std::process::exit(code);
}

async fn run(cli: Cli, format: Format) -> anyhow::Result<()> {
    match cli.command {
        Commands::Append(a) => cmd::write::append(&cli.addr, format, a).await,
        Commands::Commit(a) => cmd::write::commit(&cli.addr, format, a).await,
        Commands::Read(a) => cmd::read::aggregate(&cli.addr, format, a).await,
        Commands::All(a) => cmd::read::all(&cli.addr, format, a).await,
        Commands::Tag(a) => cmd::read::tag(&cli.addr, format, a).await,
        Commands::Tail(a) => cmd::read::tail(&cli.addr, format, a).await,
        Commands::Len { aggregate } => cmd::admin::len(&cli.addr, format, &aggregate).await,
        Commands::Head => cmd::admin::head(&cli.addr, format).await,
        Commands::Info => cmd::admin::info(&cli.addr, format).await,
        Commands::Flush => cmd::admin::flush(&cli.addr, format).await,
        Commands::Backup(a) => cmd::admin::backup(&cli.addr, format, a).await,
        Commands::Apply { archive } => cmd::admin::apply(&cli.addr, format, archive).await,
        Commands::Retain { floor } => cmd::admin::retain(&cli.addr, format, floor).await,
        Commands::RebuildIndex { path } => cmd::offline::rebuild_index(format, &path),
        Commands::Verify { path } => cmd::offline::verify(format, &path),
        Commands::Restore { archive, dir } => cmd::offline::restore(format, &archive, &dir),
        Commands::Inspect { archive } => cmd::offline::inspect(format, &archive),
    }
}
