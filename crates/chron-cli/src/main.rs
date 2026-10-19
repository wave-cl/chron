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
    /// Who leads, as the daemon at --addr knows it (any node of a group).
    Discover,
    /// One screen of health; exit 1 when degraded (a replica disconnected,
    /// a fenced node, a candidate, an expired lease, lag above --max-lag),
    /// 2 when unreachable.
    Status(cmd::admin::StatusArgs),
    /// Make everything committed so far durable.
    Flush,
    /// Write a backup by name under the daemon's backup directory, or
    /// --fetch one to a local file.
    Backup(cmd::admin::BackupArgs),
    /// Apply an incremental backup by its name under the daemon's backup
    /// directory.
    Apply { archive: String },
    /// Drop segments wholly below a position.
    Retain { floor: u64 },
    /// Whether the daemon reports SERVING (exit 0), NOT_SERVING (exit 1)
    /// or cannot be reached (exit 2).
    Health {
        #[arg(long, default_value = "chron.v1.Chron")]
        service: String,
    },
    /// Make a replica the leader under a new epoch.
    Promote,
    /// Tell a leader that a newer epoch exists; it refuses writes from then on.
    Fence { epoch: u64 },
    /// The failover group's membership; add or remove one node (on the
    /// leader).
    Members {
        #[command(subcommand)]
        cmd: Option<MembersCommands>,
    },
    /// Create, list or drop the daemon's databases.
    Db {
        #[command(subcommand)]
        cmd: DbCommands,
    },
    /// Offline: rebuild a database's index from its segment files.
    RebuildIndex {
        path: std::path::PathBuf,
        #[command(flatten)]
        opts: cmd::offline::OfflineOpts,
    },
    /// Offline: open a database checking every segment and the index.
    Verify {
        path: std::path::PathBuf,
        #[command(flatten)]
        opts: cmd::offline::OfflineOpts,
    },
    /// Offline: restore a full backup into a new directory.
    Restore {
        archive: std::path::PathBuf,
        dir: std::path::PathBuf,
        #[command(flatten)]
        opts: cmd::offline::OfflineOpts,
    },
    /// Offline: print a backup's header.
    Inspect { archive: std::path::PathBuf },
}

#[derive(Subcommand, Debug)]
enum MembersCommands {
    /// The membership as the daemon holds it (the default).
    List,
    /// Add a node by its advertised address; it must be following the
    /// leader unless --force.
    Add {
        /// The node's advertised address (http://host:port).
        #[arg(value_name = "ADDRESS")]
        member: String,
        #[arg(long)]
        force: bool,
    },
    /// Remove a node by its advertised address (the leader itself steps
    /// down once the change commits).
    Remove {
        /// The node's advertised address (http://host:port).
        #[arg(value_name = "ADDRESS")]
        member: String,
    },
}

#[derive(Subcommand, Debug)]
enum DbCommands {
    /// Create an empty database (1 to 64 of a-z, 0-9, _ and -).
    Create { name: String },
    /// Every database with its head, floor, epoch and id.
    List,
    /// Drop a database and its data; `default` cannot be dropped.
    Drop { name: String },
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
            let code = output::exit_code(&e);
            let text = output::render_error(&e);
            match format {
                Format::Json => eprintln!("{}", serde_json::json!({ "error": text, "code": code })),
                Format::Human => eprintln!("chron: {text}"),
            }
            code
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
        Commands::Discover => cmd::admin::discover(conn, format).await,
        Commands::Status(a) => cmd::admin::status(conn, format, a).await,
        Commands::Flush => cmd::admin::flush(conn, format).await,
        Commands::Backup(a) => cmd::admin::backup(conn, format, a).await,
        Commands::Apply { archive } => cmd::admin::apply(conn, format, archive).await,
        Commands::Retain { floor } => cmd::admin::retain(conn, format, floor).await,
        Commands::Health { service } => cmd::admin::health(conn, format, &service).await,
        Commands::Promote => cmd::admin::promote(conn, format).await,
        Commands::Fence { epoch } => cmd::admin::fence(conn, format, epoch).await,
        Commands::Members { cmd } => match cmd.unwrap_or(MembersCommands::List) {
            MembersCommands::List => cmd::admin::members(conn, format).await,
            MembersCommands::Add { member, force } => {
                cmd::admin::members_add(conn, format, &member, force).await
            }
            MembersCommands::Remove { member } => {
                cmd::admin::members_remove(conn, format, &member).await
            }
        },
        Commands::Db { cmd } => match cmd {
            DbCommands::Create { name } => cmd::admin::db_create(conn, format, &name).await,
            DbCommands::List => cmd::admin::db_list(conn, format).await,
            DbCommands::Drop { name } => cmd::admin::db_drop(conn, format, &name).await,
        },
        Commands::RebuildIndex { path, opts } => cmd::offline::rebuild_index(format, &path, &opts),
        Commands::Verify { path, opts } => cmd::offline::verify(format, &path, &opts),
        Commands::Restore { archive, dir, opts } => {
            cmd::offline::restore(format, &archive, &dir, &opts)
        }
        Commands::Inspect { archive } => cmd::offline::inspect(format, &archive),
    }
}
