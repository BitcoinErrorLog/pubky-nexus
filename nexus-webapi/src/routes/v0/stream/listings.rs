use crate::routes::v0::endpoints::STREAM_LISTINGS_ROUTE;
use crate::{Error, Result as AppResult};
use axum::{extract::Query, Json};
use nexus_common::db::kv::SortOrder;
use nexus_common::models::marketplace::{
    ListingSaleFormat, ListingStream, ListingStreamFilters, ListingStreamSorting,
};
use nexus_common::types::Pagination;
use pubky_app_specs::{PubkyAppListingCondition, PubkyAppListingState};
use serde::Deserialize;
use tracing::debug;
use utoipa::{OpenApi, ToSchema};

#[derive(Deserialize, Debug, ToSchema)]
pub struct ListingStreamQuery {
    #[serde(flatten)]
    pub filters: ListingStreamFilters,
    #[serde(flatten)]
    pub pagination: Pagination,
    pub order: Option<SortOrder>,
    pub sorting: Option<ListingStreamSorting>,
}

impl ListingStreamQuery {
    pub fn initialize_defaults(&mut self) {
        self.pagination.skip.get_or_insert(0);
        self.pagination.limit = Some(self.pagination.limit.unwrap_or(10).min(30));
    }

    pub fn validate_price_filters(&self) -> AppResult<()> {
        if (self.filters.min_price.is_some() || self.filters.max_price.is_some())
            && self.filters.currency.is_none()
        {
            return Err(Error::invalid_input(
                "The min_price and max_price filters require the currency parameter",
            ));
        }
        Ok(())
    }
}

#[utoipa::path(
    get,
    path = STREAM_LISTINGS_ROUTE,
    tag = "Stream",
    params(
        ("seller_id" = Option<String>, Query, description = "Filter listings by a specific seller Pubky ID"),
        ("category" = Option<String>, Query, description = "Filter listings by their kebab-case category identifier. E.g., `apparel-shoes`"),
        ("condition" = Option<PubkyAppListingCondition>, Query, description = "Filter listings by item condition: new, like_new, excellent, good, fair or for_parts"),
        ("sale_format" = Option<ListingSaleFormat>, Query, description = "Filter listings by sale format: fixed_price or auction"),
        ("state" = Option<PubkyAppListingState>, Query, description = "Filter listings by lifecycle state: active, paused, ended or removed"),
        ("min_price" = Option<f64>, Query, description = "Filter listings with a price greater than or equal to this value, expressed in major units of `currency`. Requires the currency parameter"),
        ("max_price" = Option<f64>, Query, description = "Filter listings with a price less than or equal to this value, expressed in major units of `currency`. Requires the currency parameter"),
        ("currency" = Option<String>, Query, description = "Filter listings by their uppercase asset code. E.g., `USD` or `SAT`"),
        ("order" = Option<SortOrder>, Query, description = "Ordering of response list. Either 'ascending' or 'descending'. Defaults to descending."),
        ("sorting" = Option<ListingStreamSorting>, Query, description = "Property the stream is sorted by. Either 'timeline' (indexing time, the default) or 'ends_at' (auction end time). Sorting by 'ends_at' excludes fixed-price listings; combine with order 'ascending' to retrieve auctions ending soonest first"),
        ("skip" = Option<usize>, Query, description = "Skip N listings"),
        ("limit" = Option<usize>, Query, description = "Retrieve N listings"),
        ("start" = Option<usize>, Query, description = "The start of the stream timeframe. Listings with a sorting timestamp greater than this value will be excluded from the results"),
        ("end" = Option<usize>, Query, description = "The end of the stream timeframe. Listings with a sorting timestamp less than this value will be excluded from the results"),
    ),
    responses(
        (status = 200, description = "Listings stream", body = ListingStream),
        (status = 400, description = "Invalid input"),
        (status = 500, description = "Internal server error")
    ),
    description = r#"Stream Listings: Retrieve a stream of marketplace listings sorted by indexing timeline or by auction end time.

Listings can be filtered by seller, category, condition, sale format, lifecycle state and price range.
The price range filters (`min_price`, `max_price`) are expressed in major units and require the `currency` parameter.
Sorting by `ends_at` returns only auction listings; use `order=ascending` for an "ending soon" stream."#
)]
pub async fn stream_listings_handler(
    Query(mut query): Query<ListingStreamQuery>,
) -> AppResult<Json<ListingStream>> {
    debug!("GET {STREAM_LISTINGS_ROUTE}");

    query.initialize_defaults();
    query.validate_price_filters()?;
    let order = query.order.unwrap_or_default();
    let sorting = query.sorting.unwrap_or_default();

    match ListingStream::get_listings(query.filters, query.pagination, order, sorting).await? {
        Some(stream) => Ok(Json(stream)),
        None => Ok(Json(ListingStream::default())),
    }
}

#[derive(OpenApi)]
#[openapi(
    paths(stream_listings_handler),
    components(schemas(ListingStream, ListingStreamFilters, ListingStreamSorting, SortOrder))
)]
pub struct StreamListingsApiDocs;
