use crate::routes::v0::endpoints::STREAM_DROPS_ROUTE;
use crate::Result as AppResult;
use axum::{extract::Query, Json};
use nexus_common::db::kv::SortOrder;
use nexus_common::models::marketplace::{DropStream, DropStreamBucket, DropStreamFilters};
use nexus_common::types::Pagination;
use serde::Deserialize;
use tracing::debug;
use utoipa::{OpenApi, ToSchema};

#[derive(Deserialize, Debug, ToSchema)]
pub struct DropStreamQuery {
    #[serde(flatten)]
    pub filters: DropStreamFilters,
    #[serde(flatten)]
    pub pagination: Pagination,
    pub order: Option<SortOrder>,
}

impl DropStreamQuery {
    pub fn initialize_defaults(&mut self) {
        self.pagination.skip.get_or_insert(0);
        self.pagination.limit = Some(self.pagination.limit.unwrap_or(10).min(30));
    }
}

#[utoipa::path(
    get,
    path = STREAM_DROPS_ROUTE,
    tag = "Stream",
    params(
        ("owner" = Option<String>, Query, description = "Filter drops by a specific seller Pubky ID"),
        ("bucket" = Option<DropStreamBucket>, Query, description = "Filter drops by their declared time window: `upcoming` (starts_at > now), `live_window` (starts_at <= now AND (ends_at IS NULL OR ends_at > now)) or `ended_window` (ends_at <= now). These buckets are TIME-WINDOW ESTIMATES computed from the indexed record, not the transaction service's authoritative drop state: a drop can be cancelled or sold out inside its window, so clients must hydrate the service projection before claiming a drop is live"),
        ("order" = Option<SortOrder>, Query, description = "Ordering of response list by the declared start time. Either 'ascending' or 'descending'. Defaults to ascending (soonest starts_at first)."),
        ("skip" = Option<usize>, Query, description = "Skip N drops"),
        ("limit" = Option<usize>, Query, description = "Retrieve N drops"),
        ("start" = Option<usize>, Query, description = "The start of the stream timeframe. Drops with a declared start time greater than this value will be excluded from the results"),
        ("end" = Option<usize>, Query, description = "The end of the stream timeframe. Drops with a declared start time less than this value will be excluded from the results"),
    ),
    responses(
        (status = 200, description = "Drops stream", body = DropStream),
        (status = 400, description = "Invalid input"),
        (status = 500, description = "Internal server error")
    ),
    description = r#"Stream Drops: Retrieve a stream of marketplace drops sorted by their declared start time, soonest first by default.

Drops can be filtered by owner (seller pubky) and by a time-window `bucket` (`upcoming`, `live_window`, `ended_window`).
The buckets are TIME-WINDOW ESTIMATES computed from the indexed record's declared `starts_at`/`ends_at`, not the
transaction service's authoritative drop state: a drop can be cancelled or sold out inside its window. Clients must
hydrate the transaction service projection before claiming a drop is live."#
)]
pub async fn stream_drops_handler(
    Query(mut query): Query<DropStreamQuery>,
) -> AppResult<Json<DropStream>> {
    debug!("GET {STREAM_DROPS_ROUTE}");

    query.initialize_defaults();
    // Soonest declared start first is the drop stream's default ordering
    let order = query.order.unwrap_or(SortOrder::Ascending);

    match DropStream::get_drops(query.filters, query.pagination, order).await? {
        Some(stream) => Ok(Json(stream)),
        None => Ok(Json(DropStream::default())),
    }
}

#[derive(OpenApi)]
#[openapi(
    paths(stream_drops_handler),
    components(schemas(DropStream, DropStreamFilters, DropStreamBucket, SortOrder))
)]
pub struct StreamDropsApiDocs;
