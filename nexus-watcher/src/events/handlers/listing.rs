use crate::events::retry::event::RetryEvent;
use crate::events::EventProcessorError;
use nexus_common::db::OperationOutcome;
use nexus_common::models::marketplace::ListingDetails;
use pubky_app_specs::{PubkyAppListing, PubkyId};
use tracing::debug;

pub async fn sync_put(
    listing: PubkyAppListing,
    user_id: PubkyId,
    listing_id: String,
) -> Result<(), EventProcessorError> {
    debug!("Indexing new listing: {}/{}", user_id, listing_id);

    // Create ListingDetails object
    let listing_details = ListingDetails::from_homeserver(listing, &user_id, &listing_id);

    // SAVE TO GRAPH: only if the seller user exists
    let existed = match listing_details.put_to_graph().await? {
        OperationOutcome::CreatedOrDeleted => false,
        OperationOutcome::Updated => true,
        OperationOutcome::MissingDependency => {
            let key = RetryEvent::generate_index_key_from_uri(&user_id.to_uri());
            return Err(EventProcessorError::missing_dependencies(vec![key]));
        }
    };

    // SAVE TO INDEX: on an edit only the details JSON is refreshed, the listing
    // keeps its original position in the stream sorted sets
    listing_details.put_to_index(existed).await?;

    Ok(())
}

pub async fn del(user_id: PubkyId, listing_id: String) -> Result<(), EventProcessorError> {
    debug!("Deleting listing: {}/{}", user_id, listing_id);

    ListingDetails::delete(&user_id, &listing_id).await?;

    Ok(())
}
