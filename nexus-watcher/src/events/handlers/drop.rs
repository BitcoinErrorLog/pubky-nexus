use crate::events::retry::event::RetryEvent;
use crate::events::EventProcessorError;
use nexus_common::db::OperationOutcome;
use nexus_common::models::marketplace::DropDetails;
use pubky_app_specs::{PubkyAppMarketplaceDrop, PubkyId};
use tracing::debug;

pub async fn sync_put(
    drop: PubkyAppMarketplaceDrop,
    user_id: PubkyId,
    drop_id: String,
) -> Result<(), EventProcessorError> {
    debug!("Indexing new drop: {}/{}", user_id, drop_id);

    // Create DropDetails object
    let drop_details = DropDetails::from_homeserver(drop, &user_id, &drop_id);

    // SAVE TO GRAPH: only if the seller user exists
    if let OperationOutcome::MissingDependency = drop_details.put_to_graph().await? {
        let key = RetryEvent::generate_index_key_from_uri(&user_id.to_uri());
        return Err(EventProcessorError::missing_dependencies(vec![key]));
    }

    // SAVE TO INDEX: the stream sorted sets are scored by the declared start
    // time and upserted on every write, so an edit that reschedules the drop
    // moves it in the stream instead of keeping a stale position
    drop_details.put_to_index().await?;

    Ok(())
}

pub async fn del(user_id: PubkyId, drop_id: String) -> Result<(), EventProcessorError> {
    debug!("Deleting drop: {}/{}", user_id, drop_id);

    DropDetails::delete(&user_id, &drop_id).await?;

    Ok(())
}
