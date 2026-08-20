use async_trait::async_trait;

use crate::migrations::manager::Migration;
use nexus_common::types::DynError;
use nexus_watcher::events::handlers::listing::backfill_missing_auction_terms;

/// Backfills the auction term fields (`auction_starts_at`, `auction_ends_at`,
/// reserve/buy-now/minimum-increment prices) for marketplace listings indexed
/// before the index carried them. The homeserver stays canonical for listing
/// records, so the backfill re-reads each pre-term auction row's record from
/// its seller's homeserver and re-runs the normal listing ingest, which also
/// rescoring the listing in the auction end-time sorted set.
///
/// Requires the Pubky client, which `MigrationBuilder::init_stack` initialises
/// from the migration config (`testnet` / `testnet_host`, mainnet by default).
pub struct ListingAuctionTermsReindex1787256279;

#[async_trait]
impl Migration for ListingAuctionTermsReindex1787256279 {
    fn id(&self) -> &'static str {
        "ListingAuctionTermsReindex1787256279"
    }

    fn is_multi_staged(&self) -> bool {
        false
    }

    async fn dual_write(_data: Box<dyn std::any::Any + Send + 'static>) -> Result<(), DynError> {
        Ok(())
    }

    async fn backfill(&self) -> Result<(), DynError> {
        let summary = backfill_missing_auction_terms().await?;
        tracing::info!(
            "Auction terms backfill: {} reindexed, {} gone from their homeserver, {} failed",
            summary.reindexed,
            summary.gone,
            summary.failed
        );
        // Keep the migration in the backfill phase when any listing failed:
        // reindexed listings drop out of the candidate query, so a re-run
        // only retries the failed ones.
        if summary.failed > 0 {
            return Err(format!(
                "{} listing(s) could not be reindexed from their homeserver; re-run the migration to retry them",
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
