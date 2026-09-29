use nexus_common::db::GraphTxn;
use nexus_common::models::event::EventProcessorError;
use nexus_common::models::post::PostRelationships;
use tracing::warn;

/// Points of a post or user deletion where integration tests inject a
/// failure or a concurrent event.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TargetDeleteStep {
    /// The target's write lock is held and it was found free of
    /// relationships; nothing is deleted yet.
    Checked,
}

/// Internal deterministic seam for integration-testing a post or user
/// deletion against tag writes on the target.
#[doc(hidden)]
#[async_trait::async_trait]
pub trait TargetDeleteHook: Sync {
    async fn at(&self, _step: TargetDeleteStep) -> Result<(), EventProcessorError> {
        Ok(())
    }
}

pub struct NoopTargetDeleteHook;

#[async_trait::async_trait]
impl TargetDeleteHook for NoopTargetDeleteHook {}

/// Checks if a post is a reply based on its relationships.
/// # Arguments
/// * `author_id` - The ID of the author of the post
/// * `post_id` - The ID of the post to check
///
pub async fn post_relationships_is_reply(
    author_id: &str,
    post_id: &str,
) -> Result<bool, EventProcessorError> {
    match PostRelationships::get_by_id(author_id, post_id).await? {
        Some(relationship) => Ok(relationship.replied.is_some()),
        // If the post does not exist, it is treated as a reply to avoid incorrect assumptions
        None => Ok(true),
    }
}

/// Ends the transaction a handler ran its graph and Redis writes in: commits
/// it when `result` is `Ok`, rolls it back otherwise and hands back the
/// handler's error. The write locks the handler took are released either
/// way, only after its Redis writes.
pub async fn finish_txn<T>(
    txn: GraphTxn,
    result: Result<T, EventProcessorError>,
) -> Result<T, EventProcessorError> {
    match result {
        Ok(value) => {
            txn.commit()
                .await
                .map_err(EventProcessorError::graph_query_failed)?;
            Ok(value)
        }
        Err(error) => {
            if let Err(rollback_error) = txn.rollback().await {
                warn!("Rolling back the graph transaction failed: {rollback_error}");
            }
            Err(error)
        }
    }
}
