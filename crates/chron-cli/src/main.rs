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
    #[command(flatten)]
    conn: client::Conn,
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
    /// Apply an incremental backup by its name under the daemon's backup
    /// directory.
    Apply { archive: String },
    /// Drop segments wholly below a position.
    Retain { floor: u64 },
    /// Whether the daemon reports SERVING (exit 0) or not (exit 1).
    Health {
        #[arg(long, default_value = "chron.v1.Chron")]
        service: String,
    },
    /// Make a replica the leader under a new epoch.
    Promote,
    /// Tell a leader that a newer epoch exists; it refuses writes from then on.
    Fence { epoch: u64 },
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
    let conn = &cli.conn;
    match cli.command {
        Commands::Append(a) => cmd::write::append(conn, format, a).await,
        Commands::Commit(a) => cmd::write::commit(conn, format, a).await,
        Commands::Read(a) => cmd::read::aggregate(conn, format, a).await,
        Commands::All(a) => cmd::read::all(conn, format, a).await,
        Commands::Tag(a) => cmd::read::tag(conn, format, a).await,
        Commands::Tail(a) => cmd::read::tail(conn, format, a).await,
        Commands::Len { aggregate } => cmd::admin::len(conn, format, &aggregate).await,
        Commands::Head => cmd::admin::head(conn, format).await,
        Commands::Info => cmd::admin::info(conn, format).await,
        Commands::Flush => cmd::admin::flush(conn, format).await,
        Commands::Backup(a) => cmd::admin::backup(conn, format, a).await,
        Commands::Apply { archive } => cmd::admin::apply(conn, format, archive).await,
        Commands::Retain { floor } => cmd::admin::retain(conn, format, floor).await,
        Commands::Health { service } => cmd::admin::health(conn, format, &service).await,
        Commands::Promote => cmd::admin::promote(conn, format).await,
        Commands::Fence { epoch } => cmd::admin::fence(conn, format, epoch).await,
        Commands::RebuildIndex { path } => cmd::offline::rebuild_index(format, &path),
        Commands::Verify { path } => cmd::offline::verify(format, &path),
        Commands::Restore { archive, dir } => cmd::offline::restore(format, &archive, &dir),
        Commands::Inspect { archive } => cmd::offline::inspect(format, &archive),
    }
}
