use clap::Parser;
use nexus_common::db::redact_connection_url;
use nexus_common::types::DynError;
use nexus_watcher::events::handlers::listing::prune_stale_listings;
use nexus_watcher::service::NexusWatcher;
use nexus_webapi::mock::MockDb;
use nexus_webapi::NexusApi;
use nexusd::cli::{
    ApiArgs, Cli, DbCommands, MigrationCommands, NexusCommands, PruneStaleListingsArgs, WatcherArgs,
};
use nexusd::migrations::{import_migrations, MigrationBuilder, MigrationManager};
use nexusd::prune::{prune_outcome, render_prune_summary};
use nexusd::DaemonLauncher;

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("Error: {}", redact_connection_url(&format!("{error:?}")));
        std::process::exit(1);
    }
}

async fn run() -> Result<(), DynError> {
    let cli = Cli::parse();
    let command = Cli::receive_command(cli);
    match command {
        NexusCommands::Db(db_command) => match db_command {
            DbCommands::Clear => MockDb::clear_database().await,
            DbCommands::Mock(args) => MockDb::run(args.mock_type).await,
            DbCommands::Migration(migration_command) => match migration_command {
                MigrationCommands::New(args) => MigrationManager::new_migration(args.name).await?,
                MigrationCommands::Run => {
                    let builder = MigrationBuilder::default().await?;
                    let mut mm = builder.init_stack().await?;
                    import_migrations(&mut mm);
                    mm.run(&builder.migrations_backfill_ready()).await?;
                }
            },
            DbCommands::PruneStaleListings(args) => prune_stale_listings_command(args).await?,
        },
        NexusCommands::Api(ApiArgs { config_dir }) => {
            NexusApi::start_from_daemon(config_dir, None).await?;
        }
        NexusCommands::Watcher(WatcherArgs { config_dir }) => {
            NexusWatcher::start_from_daemon(config_dir, None).await?;
        }
        NexusCommands::Run { config_dir } => {
            DaemonLauncher::start(config_dir, None).await?;
        }
    }

    Ok(())
}

async fn prune_stale_listings_command(args: PruneStaleListingsArgs) -> Result<(), DynError> {
    let builder = MigrationBuilder::default().await?;
    builder.init_stack().await?;
    let summary = prune_stale_listings(args.apply, args.max_prune).await?;
    println!("{}", render_prune_summary(&summary, args.apply));
    prune_outcome(&summary)
}
