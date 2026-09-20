use crate::routes::v0::endpoints::{
    DROP_ROUTE, LISTING_REVIEWS_ROUTE, LISTING_ROUTE, LISTING_TAGGERS_ROUTE, LISTING_TAGS_ROUTE,
    SHOP_REPUTATION_ROUTE, SHOP_REVIEWS_ROUTE, SHOP_ROUTE, SHOP_TAGGERS_ROUTE, SHOP_TAGS_ROUTE,
};
use crate::routes::AppState;
use axum::routing::get;
use axum::Router;
use utoipa::OpenApi;

mod drop;
mod listing;
mod reviews;
mod shop;
mod tags;

pub fn routes() -> Router<AppState> {
    Router::new()
        .route(SHOP_ROUTE, get(shop::shop_view_handler))
        .route(SHOP_TAGS_ROUTE, get(tags::shop_tags_handler))
        .route(SHOP_TAGGERS_ROUTE, get(tags::shop_taggers_handler))
        .route(SHOP_REVIEWS_ROUTE, get(reviews::shop_reviews_handler))
        .route(SHOP_REPUTATION_ROUTE, get(reviews::shop_reputation_handler))
        .route(LISTING_ROUTE, get(listing::listing_details_handler))
        .route(LISTING_TAGS_ROUTE, get(tags::listing_tags_handler))
        .route(LISTING_TAGGERS_ROUTE, get(tags::listing_taggers_handler))
        .route(LISTING_REVIEWS_ROUTE, get(reviews::listing_reviews_handler))
        .route(DROP_ROUTE, get(drop::drop_details_handler))
}

#[derive(OpenApi)]
#[openapi()]
pub struct MarketplaceApiDoc;

impl MarketplaceApiDoc {
    pub fn merge_docs() -> utoipa::openapi::OpenApi {
        let mut combined = shop::ShopViewApiDoc::openapi();
        combined.merge(listing::ListingDetailsApiDoc::openapi());
        combined.merge(drop::DropDetailsApiDoc::openapi());
        combined.merge(tags::MarketplaceTagsApiDoc::openapi());
        combined.merge(reviews::MarketplaceReviewsApiDoc::openapi());
        combined
    }
}
