use crate::routes::v0::endpoints::DROP_ROUTE;
use crate::{Error, Result};
use axum::extract::Path;
use axum::Json;
use nexus_common::models::marketplace::DropDetails;
use pubky_app_specs::{PubkyAppDropFormat, PubkyAppDropStockDisplay};
use tracing::debug;
use utoipa::OpenApi;

#[utoipa::path(
    get,
    path = DROP_ROUTE,
    description = "Drop details as indexed from the seller's public record. \
`starts_at`/`ends_at` are the seller's DECLARED schedule and any liveness derived from them is a \
time-window estimate: the marketplace transaction service holds the authoritative drop state \
(remaining stock, cancellation, sold-out), which is deliberately not indexed here.",
    tag = "Marketplace",
    params(
        ("owner_id" = String, Path, description = "Seller Pubky ID"),
        ("drop_id" = String, Path, description = "Drop entity ID")
    ),
    responses(
        (status = 200, description = "Drop details", body = DropDetails),
        (status = 404, description = "Drop not found"),
        (status = 500, description = "Internal server error")
    )
)]
pub async fn drop_details_handler(
    Path((owner_id, drop_id)): Path<(String, String)>,
) -> Result<Json<DropDetails>> {
    debug!("GET {DROP_ROUTE} owner_id:{owner_id}, drop_id:{drop_id}");

    let Some(details) = DropDetails::get_by_id(&owner_id, &drop_id).await? else {
        return Err(Error::DropNotFound { owner_id, drop_id });
    };

    Ok(Json(details))
}

#[derive(OpenApi)]
#[openapi(
    paths(drop_details_handler),
    components(schemas(DropDetails, PubkyAppDropFormat, PubkyAppDropStockDisplay))
)]
pub struct DropDetailsApiDoc;
