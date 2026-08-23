use crate::db::kv::{RedisResult, SortOrder};
use crate::db::{
    exec_single_row, execute_graph_operation, fetch_row_from_graph, get_neo4j_graph, queries,
    GraphError, GraphResult, OperationOutcome, RedisOps,
};
use crate::models::error::{ModelError, ModelResult};
use crate::types::Pagination;
use chrono::Utc;
use futures::TryStreamExt;
use pubky_app_specs::{
    drop_uri_builder, PubkyAppDropFormat, PubkyAppDropStockDisplay, PubkyAppMarketplaceDrop,
    PubkyId,
};
use serde::{Deserialize, Serialize};
use tokio::task::spawn;
use tokio::time::{timeout, Duration};
use tracing::warn;
use utoipa::ToSchema;

pub const DROP_STARTS_KEY_PARTS: [&str; 3] = ["Drops", "Global", "StartsAt"];
pub const DROP_PER_OWNER_KEY_PARTS: [&str; 2] = ["Drops", "Owner"];

/// Represents the indexed details of a marketplace drop: a seller's
/// scheduled, limited-quantity release bundling one or more of their listings.
///
/// Everything here is the seller's PUBLIC record data, indexed verbatim.
/// The live drop state (remaining stock, cancellation, sold-out) belongs to
/// the marketplace transaction service and is deliberately NOT indexed:
/// `starts_at`/`ends_at` are the seller's declared schedule intent, so any
/// liveness derived from them is a time-window estimate, not authoritative
/// drop state.
#[derive(Serialize, Deserialize, ToSchema, Clone, Debug, PartialEq)]
pub struct DropDetails {
    pub id: String,
    pub uri: String,
    pub owner_id: String,
    pub indexed_at: i64,
    pub revision: i64,
    pub title: String,
    pub description: String,
    pub media_urls: Vec<String>,
    pub format: PubkyAppDropFormat,
    pub starts_at: String,
    pub ends_at: Option<String>,
    /// The seller's own listing entity ids bundled into the drop.
    pub listing_ids: Vec<String>,
    pub total_quantity: i64,
    pub per_buyer_limit: i64,
    pub stock_display: PubkyAppDropStockDisplay,
    pub created_at: String,
    pub updated_at: String,
}

impl RedisOps for DropDetails {}

impl DropDetails {
    pub fn from_homeserver(
        homeserver_drop: PubkyAppMarketplaceDrop,
        owner_id: &PubkyId,
        drop_id: &str,
    ) -> Self {
        DropDetails {
            id: drop_id.to_string(),
            uri: drop_uri_builder(owner_id.to_string(), drop_id.to_string()),
            owner_id: owner_id.to_string(),
            indexed_at: Utc::now().timestamp_millis(),
            revision: homeserver_drop.revision,
            title: homeserver_drop.title,
            description: homeserver_drop.description,
            media_urls: homeserver_drop.media,
            format: homeserver_drop.format,
            starts_at: homeserver_drop.starts_at,
            ends_at: homeserver_drop.ends_at,
            listing_ids: homeserver_drop.listing_ids,
            total_quantity: homeserver_drop.total_quantity,
            per_buyer_limit: homeserver_drop.per_buyer_limit,
            stock_display: homeserver_drop.stock_display,
            created_at: homeserver_drop.created_at,
            updated_at: homeserver_drop.updated_at,
        }
    }

    /// The declared start time as epoch milliseconds, used as the score of the
    /// drop sorted sets and for start-time sorting in the graph. `None` only
    /// for an unparseable timestamp, which specs validation rules out.
    pub fn starts_at_ms(&self) -> Option<i64> {
        chrono::DateTime::parse_from_rfc3339(&self.starts_at)
            .ok()
            .map(|datetime| datetime.timestamp_millis())
    }

    /// The declared end time as epoch milliseconds, used for the time-window
    /// bucket filters in the graph. `None` for open-ended drops.
    pub fn ends_at_ms(&self) -> Option<i64> {
        let ends_at = self.ends_at.as_deref()?;
        chrono::DateTime::parse_from_rfc3339(ends_at)
            .ok()
            .map(|datetime| datetime.timestamp_millis())
    }

    /// Retrieves drop details by owner ID and drop ID, first trying Redis,
    /// then falling back to Neo4j.
    pub async fn get_by_id(owner_id: &str, drop_id: &str) -> ModelResult<Option<DropDetails>> {
        match Self::get_from_index(owner_id, drop_id).await? {
            Some(details) => Ok(Some(details)),
            None => {
                let maybe_details = Self::get_from_graph(owner_id, drop_id).await?;
                if let Some(details) = maybe_details {
                    details.put_to_index().await?;
                    return Ok(Some(details));
                }
                Ok(None)
            }
        }
    }

    pub async fn get_from_index(owner_id: &str, drop_id: &str) -> RedisResult<Option<DropDetails>> {
        Self::try_from_index_json(&[owner_id, drop_id], None).await
    }

    /// Retrieves the drop fields from Neo4j.
    pub async fn get_from_graph(owner_id: &str, drop_id: &str) -> GraphResult<Option<DropDetails>> {
        let query = queries::get::get_drop_by_id(owner_id, drop_id);
        let maybe_row = fetch_row_from_graph(query).await?;

        let Some(row) = maybe_row else {
            return Ok(None);
        };

        let drop: DropDetails = row.get("details")?;
        Ok(Some(drop))
    }

    // Save new graph node
    pub async fn put_to_graph(&self) -> GraphResult<OperationOutcome> {
        let query = queries::put::create_drop(self)?;
        execute_graph_operation(query).await
    }

    /// Stores the drop details JSON and refreshes the drop sorted sets. Both
    /// sets are scored by the declared start time, so every write upserts the
    /// score: an edit that reschedules the drop moves it in the stream.
    pub async fn put_to_index(&self) -> RedisResult<()> {
        self.put_index_json(&[&self.owner_id, &self.id], None, None)
            .await?;
        DropStream::upsert_sorted_sets(self).await?;
        Ok(())
    }

    pub async fn delete(owner_id: &str, drop_id: &str) -> ModelResult<()> {
        // Delete drop graph node
        exec_single_row(queries::del::delete_drop(owner_id, drop_id)).await?;
        // Delete drop details on Redis
        Self::remove_from_index_multiple_json(&[&[owner_id, drop_id]]).await?;
        // Remove from stream sorted sets
        DropStream::remove_from_sorted_sets(owner_id, drop_id).await?;
        Ok(())
    }
}

/// Time-window bucket of the drop stream, computed from the indexed record's
/// declared `starts_at`/`ends_at` relative to the query time.
///
/// These buckets are TIME-WINDOW ESTIMATES, not the transaction service's
/// authoritative drop state: a drop can be cancelled or sold out inside its
/// window. Clients must hydrate the service projection before claiming a
/// drop is live.
#[derive(Serialize, Deserialize, ToSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DropStreamBucket {
    /// The declared start is still in the future (`starts_at > now`).
    Upcoming,
    /// The declared window is open (`starts_at <= now` and the drop is
    /// open-ended or `ends_at > now`).
    LiveWindow,
    /// The declared window has closed (`ends_at <= now`). Open-ended drops
    /// never enter this bucket; they end only by sell-out or cancellation,
    /// which the transaction service tracks.
    EndedWindow,
}

/// Filters supported by the drop stream. The `bucket` filter compares the
/// declared schedule against the current time, which the sorted sets cannot
/// resolve alone, so it always falls back to a graph query.
#[derive(Deserialize, ToSchema, Debug, Clone, Default)]
pub struct DropStreamFilters {
    /// Seller pubky of the drops.
    pub owner: Option<String>,
    pub bucket: Option<DropStreamBucket>,
}

#[derive(Serialize, Deserialize, ToSchema, Debug, Default)]
pub struct DropStream(pub Vec<DropDetails>);

impl RedisOps for DropStream {}

impl DropStream {
    pub async fn get_drops(
        filters: DropStreamFilters,
        pagination: Pagination,
        order: SortOrder,
    ) -> ModelResult<Option<Self>> {
        let drop_keys = Self::collect_drop_keys(filters, pagination, order).await?;

        if drop_keys.is_empty() {
            return Ok(None);
        }

        Self::from_listed_drop_keys(&drop_keys).await
    }

    async fn collect_drop_keys(
        filters: DropStreamFilters,
        pagination: Pagination,
        order: SortOrder,
    ) -> ModelResult<Vec<String>> {
        // The bucket filter compares against the current time, which requires
        // querying the graph; owner-only and unfiltered streams are served
        // from the start-time sorted sets.
        match filters.bucket.is_some() {
            true => Ok(Self::get_from_graph(&filters, pagination, order).await?),
            false => Ok(Self::get_from_index(&filters, pagination, order).await?),
        }
    }

    // Fetch drop keys from the Redis sorted sets scored by the declared start time
    async fn get_from_index(
        filters: &DropStreamFilters,
        pagination: Pagination,
        order: SortOrder,
    ) -> RedisResult<Vec<String>> {
        let Pagination {
            skip,
            limit,
            start,
            end,
        } = pagination;

        match &filters.owner {
            Some(owner_id) => {
                let key_parts = [&DROP_PER_OWNER_KEY_PARTS[..], &[owner_id]].concat();
                let drop_ids = Self::try_from_index_sorted_set(
                    &key_parts, start, end, skip, limit, order, None,
                )
                .await?
                .unwrap_or_default();
                Ok(drop_ids
                    .into_iter()
                    .map(|(drop_id, _)| format!("{owner_id}:{drop_id}"))
                    .collect())
            }
            None => {
                let drop_keys = Self::try_from_index_sorted_set(
                    &DROP_STARTS_KEY_PARTS,
                    start,
                    end,
                    skip,
                    limit,
                    order,
                    None,
                )
                .await?
                .unwrap_or_default();
                Ok(drop_keys.into_iter().map(|(key, _)| key).collect())
            }
        }
    }

    // Fetch drop keys from the graph when the bucket filter cannot be
    // resolved from the index
    async fn get_from_graph(
        filters: &DropStreamFilters,
        pagination: Pagination,
        order: SortOrder,
    ) -> GraphResult<Vec<String>> {
        let now_ms = Utc::now().timestamp_millis();
        let mut result;
        {
            let graph = get_neo4j_graph()?;
            let query = queries::get::drop_stream(filters, pagination, order, now_ms);

            // Set a 10-second timeout for the query execution
            result = match timeout(Duration::from_secs(10), graph.execute(query)).await {
                Ok(Ok(res)) => res,
                Ok(Err(e)) => return Err(GraphError::QueryFailed(e)),
                Err(_) => return Err(GraphError::QueryTimeout),
            };
        }

        let mut drop_keys = Vec::new();
        while let Some(row) = result.try_next().await? {
            let owner_id: String = row.get("owner_id")?;
            let drop_id: String = row.get("drop_id")?;
            drop_keys.push(format!("{owner_id}:{drop_id}"));
        }

        Ok(drop_keys)
    }

    pub async fn from_listed_drop_keys(drop_keys: &[String]) -> ModelResult<Option<Self>> {
        let mut handles = Vec::with_capacity(drop_keys.len());

        for drop_key in drop_keys {
            let Some((owner_id, drop_id)) = drop_key.split_once(':') else {
                warn!("Invalid drop_key format (missing ':'): {drop_key}");
                continue;
            };
            let owner_id = owner_id.to_string();
            let drop_id = drop_id.to_string();
            let handle = spawn(async move { DropDetails::get_by_id(&owner_id, &drop_id).await });
            handles.push(handle);
        }

        let mut drops = Vec::with_capacity(drop_keys.len());

        for handle in handles {
            if let Some(drop) = handle.await.map_err(ModelError::from_generic)?? {
                drops.push(drop);
            }
        }

        Ok(Some(Self(drops)))
    }

    /// Keeps the global and per-owner sorted sets in sync with the drop
    /// details, scored by the declared start time. The score is refreshed on
    /// every write so a rescheduled drop moves in the stream; a drop whose
    /// start time cannot be parsed (impossible for validated records) is
    /// removed rather than fabricated a position.
    pub async fn upsert_sorted_sets(details: &DropDetails) -> RedisResult<()> {
        let Some(starts_at_ms) = details.starts_at_ms() else {
            return Self::remove_from_sorted_sets(&details.owner_id, &details.id).await;
        };
        let score = starts_at_ms as f64;

        let element = format!("{}:{}", details.owner_id, details.id);
        Self::put_index_sorted_set(
            &DROP_STARTS_KEY_PARTS,
            &[(score, element.as_str())],
            None,
            None,
        )
        .await?;

        let key_parts = [&DROP_PER_OWNER_KEY_PARTS[..], &[details.owner_id.as_str()]].concat();
        Self::put_index_sorted_set(&key_parts, &[(score, details.id.as_str())], None, None).await
    }

    /// Removes the drop from the global and per-owner sorted sets.
    pub async fn remove_from_sorted_sets(owner_id: &str, drop_id: &str) -> RedisResult<()> {
        let element = format!("{owner_id}:{drop_id}");
        Self::remove_from_index_sorted_set(None, &DROP_STARTS_KEY_PARTS, &[element.as_str()])
            .await?;

        let key_parts = [&DROP_PER_OWNER_KEY_PARTS[..], &[owner_id]].concat();
        Self::remove_from_index_sorted_set(None, &key_parts, &[drop_id]).await
    }
}
