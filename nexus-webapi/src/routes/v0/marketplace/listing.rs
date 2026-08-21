use crate::routes::v0::endpoints::LISTING_ROUTE;
use crate::{Error, Result};
use axum::extract::Path;
use axum::Json;
use nexus_common::models::marketplace::{
    ListingDetails, ListingSaleFormat, ListingStreamEntry, ReputationSummary,
};
use pubky_app_specs::{
    PubkyAppFulfillmentMethod, PubkyAppListingCondition, PubkyAppListingState, PubkyAppReviewRole,
};
use tracing::debug;
use utoipa::OpenApi;

#[utoipa::path(
    get,
    path = LISTING_ROUTE,
    description = "Listing details together with the compact seller and listing reputation objects. \
Reputation fields are absent (not zero) when no review is indexed for the scope.",
    tag = "Marketplace",
    params(
        ("seller_id" = String, Path, description = "Seller Pubky ID"),
        ("listing_id" = String, Path, description = "Listing Crockford32 ID")
    ),
    responses(
        (status = 200, description = "Listing details", body = ListingStreamEntry),
        (status = 404, description = "Listing not found"),
        (status = 500, description = "Internal server error")
    )
)]
pub async fn listing_details_handler(
    Path((seller_id, listing_id)): Path<(String, String)>,
) -> Result<Json<ListingStreamEntry>> {
    debug!("GET {LISTING_ROUTE} seller_id:{seller_id}, listing_id:{listing_id}");

    let Some(details) = ListingDetails::get_by_id(&seller_id, &listing_id).await? else {
        return Err(Error::ListingNotFound {
            seller_id,
            listing_id,
        });
    };

    let (seller_reputation, listing_reputation) = tokio::join!(
        ReputationSummary::get_by_subject(&seller_id, PubkyAppReviewRole::BuyerReviewingSeller),
        ReputationSummary::get_by_listing(&seller_id, &listing_id),
    );

    Ok(Json(ListingStreamEntry {
        details,
        reputation: seller_reputation?.map(|summary| summary.snippet()),
        listing_reputation: listing_reputation?.map(|summary| summary.snippet()),
    }))
}

#[derive(OpenApi)]
#[openapi(
    paths(listing_details_handler),
    components(schemas(
        ListingDetails,
        ListingStreamEntry,
        ListingSaleFormat,
        PubkyAppListingState,
        PubkyAppListingCondition,
        PubkyAppFulfillmentMethod
    ))
)]
pub struct ListingDetailsApiDoc;
