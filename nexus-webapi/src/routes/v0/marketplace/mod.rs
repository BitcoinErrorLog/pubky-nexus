use crate::routes::v0::endpoints::{LISTING_ROUTE, SHOP_ROUTE};
use crate::routes::AppState;
use axum::routing::get;
use axum::Router;
use utoipa::OpenApi;

mod listing;
mod shop;

pub fn routes() -> Router<AppState> {
    Router::new()
        .route(SHOP_ROUTE, get(shop::shop_view_handler))
        .route(LISTING_ROUTE, get(listing::listing_details_handler))
}

#[derive(OpenApi)]
#[openapi()]
pub struct MarketplaceApiDoc;

impl MarketplaceApiDoc {
    pub fn merge_docs() -> utoipa::openapi::OpenApi {
        let mut combined = shop::ShopViewApiDoc::openapi();
        combined.merge(listing::ListingDetailsApiDoc::openapi());
        combined
    }
}
