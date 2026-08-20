use crate::events::retry::event::RetryEvent;
use crate::events::EventProcessorError;
use nexus_common::db::OperationOutcome;
use nexus_common::models::marketplace::ShopDetails;
use pubky_app_specs::{PubkyAppShop, PubkyId};
use tracing::debug;

pub async fn sync_put(shop: PubkyAppShop, user_id: PubkyId) -> Result<(), EventProcessorError> {
    debug!("Indexing new shop: {}", user_id);

    // Create ShopDetails object
    let shop_details = ShopDetails::from_homeserver(shop, &user_id);

    // SAVE TO GRAPH: only if the owner user exists
    match shop_details.put_to_graph().await? {
        OperationOutcome::CreatedOrDeleted | OperationOutcome::Updated => (),
        OperationOutcome::MissingDependency => {
            let key = RetryEvent::generate_index_key_from_uri(&user_id.to_uri());
            return Err(EventProcessorError::missing_dependencies(vec![key]));
        }
    }

    // SAVE TO INDEX
    shop_details.put_to_index().await?;

    Ok(())
}

pub async fn del(user_id: PubkyId) -> Result<(), EventProcessorError> {
    debug!("Deleting shop: {}", user_id);

    ShopDetails::delete(&user_id).await?;

    Ok(())
}
