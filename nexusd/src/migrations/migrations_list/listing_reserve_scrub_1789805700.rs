use async_trait::async_trait;

use crate::migrations::manager::Migration;
use nexus_common::types::DynError;
use nexus_watcher::events::handlers::listing::scrub_legacy_listing_reserves;

/// Removes private auction reserve data persisted by older Nexus releases.
///
/// The scrub removes the legacy graph property and rewrites every listing
/// details entry in Redis through the reserve-free model. It is idempotent.
pub struct ListingReserveScrub1789805700;

#[async_trait]
impl Migration for ListingReserveScrub1789805700 {
    fn id(&self) -> &'static str {
        "ListingReserveScrub1789805700"
    }

    fn is_multi_staged(&self) -> bool {
        false
    }

    async fn dual_write(_data: Box<dyn std::any::Any + Send + 'static>) -> Result<(), DynError> {
        Ok(())
    }

    async fn backfill(&self) -> Result<(), DynError> {
        let summary = scrub_legacy_listing_reserves().await?;
        tracing::info!(
            scanned = summary.scanned,
            rewritten = summary.rewritten,
            disappeared = summary.disappeared,
            "Finished legacy listing reserve scrub"
        );
        Ok(())
    }

    async fn cutover(&self) -> Result<(), DynError> {
        Ok(())
    }

    async fn cleanup(&self) -> Result<(), DynError> {
        Ok(())
    }
}
