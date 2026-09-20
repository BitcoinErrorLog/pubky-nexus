use crate::routes::v0::endpoints::SHOP_ROUTE;
use crate::{Error, Result};
use axum::extract::{Path, Query};
use axum::Json;
use nexus_common::models::marketplace::{ShopDetails, ShopView};
use nexus_common::types::Pagination;
use serde::Deserialize;
use tracing::debug;
use utoipa::{OpenApi, ToSchema};

#[derive(Deserialize, Debug, ToSchema)]
pub struct ShopViewQuery {
    #[serde(flatten)]
    pub pagination: Pagination,
}

impl ShopViewQuery {
    pub fn initialize_defaults(&mut self) {
        self.pagination.skip.get_or_insert(0);
        self.pagination.limit = Some(self.pagination.limit.unwrap_or(10).min(30));
    }
}

#[utoipa::path(
    get,
    path = SHOP_ROUTE,
    description = "Shop view: retrieve the marketplace shop of a seller together with a page of the seller's listings",
    tag = "Marketplace",
    params(
        ("seller_id" = String, Path, description = "Seller Pubky ID"),
        ("skip" = Option<usize>, Query, description = "Skip N listings"),
        ("limit" = Option<usize>, Query, description = "Retrieve N listings"),
        ("start" = Option<usize>, Query, description = "The start of the listings timeframe. Listings with a timestamp greater than this value will be excluded from the results"),
        ("end" = Option<usize>, Query, description = "The end of the listings timeframe. Listings with a timestamp less than this value will be excluded from the results"),
    ),
    responses(
        (status = 200, description = "Shop view", body = ShopView),
        (status = 404, description = "Shop not found"),
        (status = 500, description = "Internal server error")
    )
)]
pub async fn shop_view_handler(
    Path(seller_id): Path<String>,
    Query(mut query): Query<ShopViewQuery>,
) -> Result<Json<ShopView>> {
    debug!("GET {SHOP_ROUTE} seller_id:{seller_id}");

    query.initialize_defaults();

    match ShopView::get_by_id(&seller_id, query.pagination).await? {
        Some(shop_view) => Ok(Json(shop_view)),
        None => Err(Error::ShopNotFound { seller_id }),
    }
}

#[derive(OpenApi)]
#[openapi(paths(shop_view_handler), components(schemas(ShopView, ShopDetails)))]
pub struct ShopViewApiDoc;
