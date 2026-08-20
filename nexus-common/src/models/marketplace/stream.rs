use super::{ListingDetails, ListingSaleFormat};
use crate::db::kv::{RedisResult, SortOrder};
use crate::db::{get_neo4j_graph, queries, GraphError, GraphResult, RedisOps};
use crate::models::error::{ModelError, ModelResult};
use crate::types::Pagination;
use futures::TryStreamExt;
use pubky_app_specs::{PubkyAppListingCondition, PubkyAppListingState};
use serde::{Deserialize, Serialize};
use tokio::task::spawn;
use tokio::time::{timeout, Duration};
use tracing::warn;
use utoipa::ToSchema;

pub const LISTING_TIMELINE_KEY_PARTS: [&str; 3] = ["Listings", "Global", "Timeline"];
pub const LISTING_PER_SELLER_KEY_PARTS: [&str; 2] = ["Listings", "Seller"];

/// Filters supported by the listing stream. Any filter besides `seller_id`
/// requires falling back to a graph query.
#[derive(Deserialize, ToSchema, Debug, Clone, Default)]
pub struct ListingStreamFilters {
    pub seller_id: Option<String>,
    pub category: Option<String>,
    pub condition: Option<PubkyAppListingCondition>,
    pub sale_format: Option<ListingSaleFormat>,
    pub state: Option<PubkyAppListingState>,
    #[serde(default, deserialize_with = "parse_string_to_f64")]
    pub min_price: Option<f64>,
    #[serde(default, deserialize_with = "parse_string_to_f64")]
    pub max_price: Option<f64>,
    pub currency: Option<String>,
}

// Parses strings into f64. Needed because query string values always arrive as
// strings when this struct is flattened into an axum Query extractor.
fn parse_string_to_f64<'de, D>(deserializer: D) -> Result<Option<f64>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s: Option<String> = Option::deserialize(deserializer)?;
    match s {
        Some(s) => s.parse::<f64>().map(Some).map_err(serde::de::Error::custom),
        None => Ok(None),
    }
}

impl ListingStreamFilters {
    fn has_graph_only_filters(&self) -> bool {
        self.category.is_some()
            || self.condition.is_some()
            || self.sale_format.is_some()
            || self.state.is_some()
            || self.min_price.is_some()
            || self.max_price.is_some()
            || self.currency.is_some()
    }
}

#[derive(Serialize, Deserialize, ToSchema, Debug, Default)]
pub struct ListingStream(pub Vec<ListingDetails>);

impl RedisOps for ListingStream {}

impl ListingStream {
    pub async fn get_listings(
        filters: ListingStreamFilters,
        pagination: Pagination,
        order: SortOrder,
    ) -> ModelResult<Option<Self>> {
        let listing_keys = Self::collect_listing_keys(filters, pagination, order).await?;

        if listing_keys.is_empty() {
            return Ok(None);
        }

        Self::from_listed_listing_keys(&listing_keys).await
    }

    async fn collect_listing_keys(
        filters: ListingStreamFilters,
        pagination: Pagination,
        order: SortOrder,
    ) -> ModelResult<Vec<String>> {
        // Any filter besides the seller requires querying the graph
        match filters.has_graph_only_filters() {
            false => Ok(Self::get_from_index(&filters, pagination, order).await?),
            true => Ok(Self::get_from_graph(&filters, pagination).await?),
        }
    }

    // Fetch listing keys from the Redis sorted sets
    async fn get_from_index(
        filters: &ListingStreamFilters,
        pagination: Pagination,
        order: SortOrder,
    ) -> RedisResult<Vec<String>> {
        let Pagination {
            skip,
            limit,
            start,
            end,
        } = pagination;

        match &filters.seller_id {
            Some(seller_id) => {
                let key_parts = [&LISTING_PER_SELLER_KEY_PARTS[..], &[seller_id]].concat();
                let listing_ids = Self::try_from_index_sorted_set(
                    &key_parts, start, end, skip, limit, order, None,
                )
                .await?
                .unwrap_or_default();
                Ok(listing_ids
                    .into_iter()
                    .map(|(listing_id, _)| format!("{seller_id}:{listing_id}"))
                    .collect())
            }
            None => {
                let listing_keys = Self::try_from_index_sorted_set(
                    &LISTING_TIMELINE_KEY_PARTS,
                    start,
                    end,
                    skip,
                    limit,
                    order,
                    None,
                )
                .await?
                .unwrap_or_default();
                Ok(listing_keys.into_iter().map(|(key, _)| key).collect())
            }
        }
    }

    // Fetch listing keys from the graph when filters cannot be resolved from the index
    async fn get_from_graph(
        filters: &ListingStreamFilters,
        pagination: Pagination,
    ) -> GraphResult<Vec<String>> {
        let mut result;
        {
            let graph = get_neo4j_graph()?;
            let query = queries::get::listing_stream(filters, pagination)?;

            // Set a 10-second timeout for the query execution
            result = match timeout(Duration::from_secs(10), graph.execute(query)).await {
                Ok(Ok(res)) => res,
                Ok(Err(e)) => return Err(GraphError::QueryFailed(e)),
                Err(_) => return Err(GraphError::QueryTimeout),
            };
        }

        let mut listing_keys = Vec::new();
        while let Some(row) = result.try_next().await? {
            let owner_id: String = row.get("owner_id")?;
            let listing_id: String = row.get("listing_id")?;
            listing_keys.push(format!("{owner_id}:{listing_id}"));
        }

        Ok(listing_keys)
    }

    pub async fn from_listed_listing_keys(listing_keys: &[String]) -> ModelResult<Option<Self>> {
        let mut handles = Vec::with_capacity(listing_keys.len());

        for listing_key in listing_keys {
            let Some((owner_id, listing_id)) = listing_key.split_once(':') else {
                warn!("Invalid listing_key format (missing ':'): {listing_key}");
                continue;
            };
            let owner_id = owner_id.to_string();
            let listing_id = listing_id.to_string();
            let handle =
                spawn(async move { ListingDetails::get_by_id(&owner_id, &listing_id).await });
            handles.push(handle);
        }

        let mut listings = Vec::with_capacity(listing_keys.len());

        for handle in handles {
            if let Some(listing) = handle.await.map_err(ModelError::from_generic)?? {
                listings.push(listing);
            }
        }

        Ok(Some(Self(listings)))
    }

    /// Adds the listing to the global timeline sorted set using `indexed_at` as the score.
    pub async fn add_to_timeline_sorted_set(details: &ListingDetails) -> RedisResult<()> {
        let element = format!("{}:{}", details.owner_id, details.id);
        let score = details.indexed_at as f64;
        Self::put_index_sorted_set(
            &LISTING_TIMELINE_KEY_PARTS,
            &[(score, element.as_str())],
            None,
            None,
        )
        .await
    }

    /// Removes the listing from the global timeline sorted set.
    pub async fn remove_from_timeline_sorted_set(
        owner_id: &str,
        listing_id: &str,
    ) -> RedisResult<()> {
        let element = format!("{owner_id}:{listing_id}");
        Self::remove_from_index_sorted_set(None, &LISTING_TIMELINE_KEY_PARTS, &[element.as_str()])
            .await
    }

    /// Adds the listing to the per-seller sorted set using `indexed_at` as the score.
    pub async fn add_to_per_seller_sorted_set(details: &ListingDetails) -> RedisResult<()> {
        let key_parts = [
            &LISTING_PER_SELLER_KEY_PARTS[..],
            &[details.owner_id.as_str()],
        ]
        .concat();
        let score = details.indexed_at as f64;
        Self::put_index_sorted_set(&key_parts, &[(score, details.id.as_str())], None, None).await
    }

    /// Removes the listing from the per-seller sorted set.
    pub async fn remove_from_per_seller_sorted_set(
        owner_id: &str,
        listing_id: &str,
    ) -> RedisResult<()> {
        let key_parts = [&LISTING_PER_SELLER_KEY_PARTS[..], &[owner_id]].concat();
        Self::remove_from_index_sorted_set(None, &key_parts, &[listing_id]).await
    }
}
