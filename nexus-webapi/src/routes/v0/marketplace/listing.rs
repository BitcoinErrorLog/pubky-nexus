use crate::routes::v0::endpoints::LISTING_ROUTE;
use crate::{Error, Result};
use axum::extract::Path;
use axum::Json;
use nexus_common::models::marketplace::{ListingDetails, ListingSaleFormat};
use pubky_app_specs::{PubkyAppFulfillmentMethod, PubkyAppListingCondition, PubkyAppListingState};
use tracing::debug;
use utoipa::OpenApi;

#[utoipa::path(
    get,
    path = LISTING_ROUTE,
    description = "Listing details",
    tag = "Marketplace",
    params(
        ("seller_id" = String, Path, description = "Seller Pubky ID"),
        ("listing_id" = String, Path, description = "Listing Crockford32 ID")
    ),
    responses(
        (status = 200, description = "Listing details", body = ListingDetails),
        (status = 404, description = "Listing not found"),
        (status = 500, description = "Internal server error")
    )
)]
pub async fn listing_details_handler(
    Path((seller_id, listing_id)): Path<(String, String)>,
) -> Result<Json<ListingDetails>> {
    debug!("GET {LISTING_ROUTE} seller_id:{seller_id}, listing_id:{listing_id}");

    match ListingDetails::get_by_id(&seller_id, &listing_id).await? {
        Some(listing) => Ok(Json(listing)),
        None => Err(Error::ListingNotFound {
            seller_id,
            listing_id,
        }),
    }
}

#[derive(OpenApi)]
#[openapi(
    paths(listing_details_handler),
    components(schemas(
        ListingDetails,
        ListingSaleFormat,
        PubkyAppListingState,
        PubkyAppListingCondition,
        PubkyAppFulfillmentMethod
    ))
)]
pub struct ListingDetailsApiDoc;
