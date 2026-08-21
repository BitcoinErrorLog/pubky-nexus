use crate::routes::v0::endpoints::{
    LISTING_ROUTE, LISTING_TAGGERS_ROUTE, LISTING_TAGS_ROUTE, SHOP_ROUTE, SHOP_TAGGERS_ROUTE,
    SHOP_TAGS_ROUTE,
};
use crate::routes::AppState;
use axum::routing::get;
use axum::Router;
use utoipa::OpenApi;

mod listing;
mod shop;
mod tags;

pub fn routes() -> Router<AppState> {
    Router::new()
        .route(SHOP_ROUTE, get(shop::shop_view_handler))
        .route(SHOP_TAGS_ROUTE, get(tags::shop_tags_handler))
        .route(SHOP_TAGGERS_ROUTE, get(tags::shop_taggers_handler))
        .route(LISTING_ROUTE, get(listing::listing_details_handler))
        .route(LISTING_TAGS_ROUTE, get(tags::listing_tags_handler))
        .route(LISTING_TAGGERS_ROUTE, get(tags::listing_taggers_handler))
}

#[derive(OpenApi)]
#[openapi()]
pub struct MarketplaceApiDoc;

impl MarketplaceApiDoc {
    pub fn merge_docs() -> utoipa::openapi::OpenApi {
        let mut combined = shop::ShopViewApiDoc::openapi();
        combined.merge(listing::ListingDetailsApiDoc::openapi());
        combined.merge(tags::MarketplaceTagsApiDoc::openapi());
        combined
    }
}
