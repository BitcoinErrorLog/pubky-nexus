use crate::routes::v0::endpoints::{
    LISTING_TAGGERS_ROUTE, LISTING_TAGS_ROUTE, SHOP_TAGGERS_ROUTE, SHOP_TAGS_ROUTE,
};
use crate::routes::v0::user::tags::TaggersQuery;
use crate::routes::v0::{TaggersInfoResponse, TagsQuery};
use crate::{Error, Result};
use axum::extract::{Path, Query};
use axum::Json;
use nexus_common::models::tag::listing::TagListing;
use nexus_common::models::tag::shop::TagShop;
use nexus_common::models::tag::traits::{TagCollection, TaggersCollection};
use nexus_common::models::tag::TagDetails;
use tracing::debug;
use utoipa::OpenApi;

#[utoipa::path(
    get,
    path = LISTING_TAGS_ROUTE,
    description = "Marketplace listing community tags",
    tag = "Marketplace",
    params(
        ("seller_id" = String, Path, description = "Seller Pubky ID"),
        ("listing_id" = String, Path, description = "Listing Crockford32 ID"),
        ("viewer_id" = Option<String>, Query, description = "Viewer Pubky ID"),
        ("skip_tags" = Option<usize>, Query, description = "Skip N tags. Defaults to `0`"),
        ("limit_tags" = Option<usize>, Query, description = "Upper limit on the number of tags for the listing. Defaults to `5`"),
        ("limit_taggers" = Option<usize>, Query, description = "Upper limit on the number of taggers per tag. Defaults to `5`"),
    ),
    responses(
        (status = 404, description = "Listing not found"),
        (status = 200, description = "Listing tags", body = Vec<TagDetails>),
        (status = 500, description = "Internal server error")
    )
)]
pub async fn listing_tags_handler(
    Path((seller_id, listing_id)): Path<(String, String)>,
    Query(query): Query<TagsQuery>,
) -> Result<Json<Vec<TagDetails>>> {
    debug!(
        "GET {LISTING_TAGS_ROUTE} seller_id:{}, listing_id:{}, skip_tags:{:?}, limit_tags:{:?}, limit_taggers:{:?}",
        seller_id, listing_id, query.skip_tags, query.limit_tags, query.limit_taggers
    );
    match TagListing::get_by_id(
        &seller_id,
        Some(&listing_id),
        query.skip_tags,
        query.limit_tags,
        query.limit_taggers,
        query.viewer_id.as_deref(),
        None, // WoT tag filtering only applies to User nodes
    )
    .await?
    {
        Some(tags) => Ok(Json(tags)),
        None => Err(Error::ListingNotFound {
            seller_id,
            listing_id,
        }),
    }
}

#[utoipa::path(
    get,
    path = LISTING_TAGGERS_ROUTE,
    description = "Marketplace listing specific label taggers",
    tag = "Marketplace",
    params(
        ("seller_id" = String, Path, description = "Seller Pubky ID"),
        ("listing_id" = String, Path, description = "Listing Crockford32 ID"),
        ("label" = String, Path, description = "Tag name"),
        ("viewer_id" = Option<String>, Query, description = "Viewer Pubky ID"),
        ("skip" = Option<usize>, Query, description = "Number of taggers to skip for pagination. Defaults to `0`"),
        ("limit" = Option<usize>, Query, description = "Number of taggers to return for pagination. Defaults to `40`")
    ),
    responses(
        (status = 200, description = "Listing tag taggers", body = TaggersInfoResponse),
        (status = 500, description = "Internal server error")
    )
)]
pub async fn listing_taggers_handler(
    Path((seller_id, listing_id, label)): Path<(String, String, String)>,
    Query(taggers_query): Query<TaggersQuery>,
) -> Result<Json<TaggersInfoResponse>> {
    debug!(
        "GET {LISTING_TAGGERS_ROUTE} seller_id:{}, listing_id:{}, label:{}, viewer_id:{:?}, skip:{:?}, limit:{:?}",
        seller_id,
        listing_id,
        label,
        taggers_query.tags_query.viewer_id,
        taggers_query.pagination.skip,
        taggers_query.pagination.limit
    );
    let taggers = TagListing::get_tagger_by_id(
        &seller_id,
        Some(&listing_id),
        &label,
        taggers_query.pagination,
        taggers_query.tags_query.viewer_id.as_deref(),
        None,
    )
    .await?;
    Ok(Json(TaggersInfoResponse::from(taggers)))
}

#[utoipa::path(
    get,
    path = SHOP_TAGS_ROUTE,
    description = "Marketplace shop community tags",
    tag = "Marketplace",
    params(
        ("seller_id" = String, Path, description = "Shop owner Pubky ID"),
        ("viewer_id" = Option<String>, Query, description = "Viewer Pubky ID"),
        ("skip_tags" = Option<usize>, Query, description = "Skip N tags. Defaults to `0`"),
        ("limit_tags" = Option<usize>, Query, description = "Upper limit on the number of tags for the shop. Defaults to `5`"),
        ("limit_taggers" = Option<usize>, Query, description = "Upper limit on the number of taggers per tag. Defaults to `5`"),
    ),
    responses(
        (status = 404, description = "Shop not found"),
        (status = 200, description = "Shop tags", body = Vec<TagDetails>),
        (status = 500, description = "Internal server error")
    )
)]
pub async fn shop_tags_handler(
    Path(seller_id): Path<String>,
    Query(query): Query<TagsQuery>,
) -> Result<Json<Vec<TagDetails>>> {
    debug!(
        "GET {SHOP_TAGS_ROUTE} seller_id:{}, skip_tags:{:?}, limit_tags:{:?}, limit_taggers:{:?}",
        seller_id, query.skip_tags, query.limit_tags, query.limit_taggers
    );
    match TagShop::get_by_id(
        &seller_id,
        None,
        query.skip_tags,
        query.limit_tags,
        query.limit_taggers,
        query.viewer_id.as_deref(),
        None, // WoT tag filtering only applies to User nodes
    )
    .await?
    {
        Some(tags) => Ok(Json(tags)),
        None => Err(Error::ShopNotFound { seller_id }),
    }
}

#[utoipa::path(
    get,
    path = SHOP_TAGGERS_ROUTE,
    description = "Marketplace shop specific label taggers",
    tag = "Marketplace",
    params(
        ("seller_id" = String, Path, description = "Shop owner Pubky ID"),
        ("label" = String, Path, description = "Tag name"),
        ("viewer_id" = Option<String>, Query, description = "Viewer Pubky ID"),
        ("skip" = Option<usize>, Query, description = "Number of taggers to skip for pagination. Defaults to `0`"),
        ("limit" = Option<usize>, Query, description = "Number of taggers to return for pagination. Defaults to `40`")
    ),
    responses(
        (status = 200, description = "Shop tag taggers", body = TaggersInfoResponse),
        (status = 500, description = "Internal server error")
    )
)]
pub async fn shop_taggers_handler(
    Path((seller_id, label)): Path<(String, String)>,
    Query(taggers_query): Query<TaggersQuery>,
) -> Result<Json<TaggersInfoResponse>> {
    debug!(
        "GET {SHOP_TAGGERS_ROUTE} seller_id:{}, label:{}, viewer_id:{:?}, skip:{:?}, limit:{:?}",
        seller_id,
        label,
        taggers_query.tags_query.viewer_id,
        taggers_query.pagination.skip,
        taggers_query.pagination.limit
    );
    let taggers = TagShop::get_tagger_by_id(
        &seller_id,
        None,
        &label,
        taggers_query.pagination,
        taggers_query.tags_query.viewer_id.as_deref(),
        None,
    )
    .await?;
    Ok(Json(TaggersInfoResponse::from(taggers)))
}

#[derive(OpenApi)]
#[openapi(
    paths(
        listing_tags_handler,
        listing_taggers_handler,
        shop_tags_handler,
        shop_taggers_handler
    ),
    components(schemas(TagDetails, TaggersInfoResponse))
)]
pub struct MarketplaceTagsApiDoc;
