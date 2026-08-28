use async_trait::async_trait;

use crate::migrations::manager::Migration;
use nexus_common::types::DynError;
use nexus_watcher::events::handlers::review::backfill_unindexed_reviews;

/// Indexes marketplace reviews published BEFORE the deployed watcher's replay
/// cursor, which the events feed will never deliver. Candidates are
/// discovered from the canonical source (each indexed user's reviews
/// directory is LISTed on their homeserver), already-indexed ids are
/// skipped, and each remaining record runs through the normal review ingest
/// — offline attestation verification and reputation recompute included.
///
/// Requires the Pubky client, which `MigrationBuilder::init_stack`
/// initialises from the migration config (`testnet` / `testnet_host`,
/// mainnet by default).
pub struct ReviewBackfill1787905961;

#[async_trait]
impl Migration for ReviewBackfill1787905961 {
    fn id(&self) -> &'static str {
        "ReviewBackfill1787905961"
    }

    fn is_multi_staged(&self) -> bool {
        false
    }

    async fn dual_write(_data: Box<dyn std::any::Any + Send + 'static>) -> Result<(), DynError> {
        Ok(())
    }

    async fn backfill(&self) -> Result<(), DynError> {
        let summary = backfill_unindexed_reviews().await?;
        tracing::info!(
            "Review backfill: {} newly indexed, {} already indexed, {} failed",
            summary.indexed,
            summary.already_indexed,
            summary.failed
        );
        // Stay in the backfill phase when anything failed: indexed reviews
        // drop out via the already-indexed skip, so a re-run only retries
        // the failures.
        if summary.failed > 0 {
            return Err(format!(
                "{} review(s) could not be indexed from their homeserver; re-run the migration to retry them",
                summary.failed
            )
            .into());
        }
        Ok(())
    }

    async fn cutover(&self) -> Result<(), DynError> {
        Ok(())
    }

    async fn cleanup(&self) -> Result<(), DynError> {
        Ok(())
    }
}
