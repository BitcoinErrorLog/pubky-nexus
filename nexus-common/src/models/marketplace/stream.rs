use super::{
    ListingDetails, ListingSaleFormat, ListingsByTagSearch, ReputationSnippet, ReputationSummary,
};
use crate::db::kv::{RedisResult, SortOrder};
use crate::db::{get_neo4j_graph, queries, GraphError, GraphResult, RedisOps};
use crate::models::error::{ModelError, ModelResult};
use crate::types::Pagination;
use futures::TryStreamExt;
use pubky_app_specs::{PubkyAppListingCondition, PubkyAppListingState};
use serde::{Deserialize, Serialize};
use std::ops::Deref;
use tokio::task::spawn;
use tokio::time::{timeout, Duration};
use tracing::warn;
use utoipa::ToSchema;

pub const LISTING_TIMELINE_KEY_PARTS: [&str; 3] = ["Listings", "Global", "Timeline"];
pub const LISTING_PER_SELLER_KEY_PARTS: [&str; 2] = ["Listings", "Seller"];
pub const LISTING_AUCTION_ENDS_KEY_PARTS: [&str; 3] = ["Listings", "Auctions", "EndsAt"];

/// Property the listing stream is sorted by.
#[derive(Serialize, Deserialize, ToSchema, Debug, Clone, Copy, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ListingStreamSorting {
    /// Sort listings by the time they were indexed.
    #[default]
    Timeline,
    /// Sort auction listings by their auction end time. Listings without an
    /// auction end time (fixed-price listings) are excluded from the stream.
    EndsAt,
}

/// Filters supported by the listing stream. Any filter besides `seller_id`
/// (or a single community tag on its own) requires falling back to a graph query.
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
    /// Seller-declared item location: the listing record's uppercase
    /// ISO-3166-1 alpha-2 country code (e.g. `HR`).
    pub country: Option<String>,
    /// Community tag labels (comma-separated in the query string). A listing
    /// matches when any user has tagged it with one of the labels. Mirrors
    /// the post stream's `tags` filter: a single label with no other filters
    /// is served from the by-tag index; anything else falls back to the graph.
    #[serde(default, deserialize_with = "parse_comma_separated_tags")]
    pub tags: Option<Vec<String>>,
}

// Parses a comma-separated tag list. Query string values always arrive as one
// string when this struct is flattened into an axum Query extractor.
fn parse_comma_separated_tags<'de, D>(deserializer: D) -> Result<Option<Vec<String>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s: Option<String> = Option::deserialize(deserializer)?;
    match s {
        Some(s) if !s.trim().is_empty() => Ok(Some(
            s.split(',').map(|tag| tag.trim().to_string()).collect(),
        )),
        _ => Ok(None),
    }
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
            || self.country.is_some()
    }

    /// A single tag label with no other filters can be served from the
    /// by-tag Redis sorted set (`Sorted:Tags:Global:Listing:Timeline:{label}`).
    fn single_tag_index_label(&self) -> Option<&str> {
        match &self.tags {
            Some(tags)
                if tags.len() == 1
                    && self.seller_id.is_none()
                    && !self.has_graph_only_filters() =>
            {
                Some(tags[0].as_str())
            }
            _ => None,
        }
    }
}

/// One listing stream entry: the full card projection plus the compact
/// reputation objects (ADR 0024 §9 — anything a card renders must be in the
/// stream projection; per-card hydration is a bug, not a pattern).
///
/// Both reputation fields are additive and optional: a seller or listing
/// without any indexed review carries no object at all — honest absence,
/// which clients must render as "New seller", never as a fabricated 0.0.
#[derive(Serialize, Deserialize, ToSchema, Debug, PartialEq)]
pub struct ListingStreamEntry {
    #[serde(flatten)]
    pub details: ListingDetails,
    /// Seller-scoped reputation (reviews about the seller in the
    /// buyer-reviewing-seller role, across all their listings).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reputation: Option<ReputationSnippet>,
    /// Listing-scoped reputation (buyer reviews of this listing).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub listing_reputation: Option<ReputationSnippet>,
}

impl Deref for ListingStreamEntry {
    type Target = ListingDetails;

    fn deref(&self) -> &Self::Target {
        &self.details
    }
}

#[derive(Serialize, Deserialize, ToSchema, Debug, Default)]
pub struct ListingStream(pub Vec<ListingStreamEntry>);

impl RedisOps for ListingStream {}

impl ListingStream {
    pub async fn get_listings(
        filters: ListingStreamFilters,
        pagination: Pagination,
        order: SortOrder,
        sorting: ListingStreamSorting,
    ) -> ModelResult<Option<Self>> {
        let listing_keys = Self::collect_listing_keys(filters, pagination, order, sorting).await?;

        if listing_keys.is_empty() {
            return Ok(None);
        }

        Self::from_listed_listing_keys(&listing_keys).await
    }

    async fn collect_listing_keys(
        filters: ListingStreamFilters,
        pagination: Pagination,
        order: SortOrder,
        sorting: ListingStreamSorting,
    ) -> ModelResult<Vec<String>> {
        match sorting {
            // A single community tag on its own is served by the by-tag index
            ListingStreamSorting::Timeline if filters.single_tag_index_label().is_some() => {
                Ok(Self::get_by_tag_from_index(&filters, pagination).await?)
            }
            // Any filter besides the seller requires querying the graph
            ListingStreamSorting::Timeline => {
                match filters.has_graph_only_filters() || filters.tags.is_some() {
                    false => Ok(Self::get_from_index(&filters, pagination, order).await?),
                    true => Ok(Self::get_from_graph(&filters, pagination, order, sorting).await?),
                }
            }
            // The auction end-time sorted set is global, so any filter
            // (including the seller) requires querying the graph
            ListingStreamSorting::EndsAt => {
                match filters.seller_id.is_none()
                    && !filters.has_graph_only_filters()
                    && filters.tags.is_none()
                {
                    true => Ok(Self::get_auction_keys_from_index(pagination, order).await?),
                    false => Ok(Self::get_from_graph(&filters, pagination, order, sorting).await?),
                }
            }
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

    // Fetch listing keys from the by-tag Redis sorted set (single label, no other filters)
    async fn get_by_tag_from_index(
        filters: &ListingStreamFilters,
        pagination: Pagination,
    ) -> RedisResult<Vec<String>> {
        let label = filters
            .single_tag_index_label()
            .expect("caller checked single_tag_index_label");
        let results = ListingsByTagSearch::get_by_label(label, pagination)
            .await?
            .unwrap_or_default();
        Ok(results.into_iter().map(|entry| entry.listing_key).collect())
    }

    // Fetch auction listing keys from the Redis sorted set scored by auction end time
    async fn get_auction_keys_from_index(
        pagination: Pagination,
        order: SortOrder,
    ) -> RedisResult<Vec<String>> {
        let Pagination {
            skip,
            limit,
            start,
            end,
        } = pagination;

        let listing_keys = Self::try_from_index_sorted_set(
            &LISTING_AUCTION_ENDS_KEY_PARTS,
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

    // Fetch listing keys from the graph when filters cannot be resolved from the index
    async fn get_from_graph(
        filters: &ListingStreamFilters,
        pagination: Pagination,
        order: SortOrder,
        sorting: ListingStreamSorting,
    ) -> GraphResult<Vec<String>> {
        let mut result;
        {
            let graph = get_neo4j_graph()?;
            let query = queries::get::listing_stream(filters, pagination, order, sorting)?;

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

        let entries = Self::hydrate_reputation(listings).await?;
        Ok(Some(Self(entries)))
    }

    /// Attaches the compact reputation objects to the card projections with
    /// two batched Redis reads (one per scope) — the stream stays a single
    /// index round-trip regardless of page size. Missing aggregates stay
    /// `None`: absence is the honest state, not a zero.
    async fn hydrate_reputation(
        listings: Vec<ListingDetails>,
    ) -> ModelResult<Vec<ListingStreamEntry>> {
        if listings.is_empty() {
            return Ok(Vec::new());
        }

        let seller_ids: Vec<&str> = listings
            .iter()
            .map(|listing| listing.owner_id.as_str())
            .collect();
        let listing_keys: Vec<(&str, &str)> = listings
            .iter()
            .map(|listing| (listing.owner_id.as_str(), listing.id.as_str()))
            .collect();

        let (seller_snippets, listing_snippets) = tokio::join!(
            ReputationSummary::snippets_by_subjects(&seller_ids),
            ReputationSummary::snippets_by_listings(&listing_keys),
        );
        let seller_snippets = seller_snippets?;
        let listing_snippets = listing_snippets?;

        Ok(listings
            .into_iter()
            .zip(seller_snippets)
            .zip(listing_snippets)
            .map(
                |((details, reputation), listing_reputation)| ListingStreamEntry {
                    details,
                    reputation,
                    listing_reputation,
                },
            )
            .collect())
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

    /// Keeps the auction end-time sorted set in sync with the listing details.
    /// An auction listing is added (or its score refreshed) using the auction
    /// end time as the score; a listing without an auction end time is removed,
    /// covering edits that change the sale format.
    pub async fn upsert_auction_ends_sorted_set(details: &ListingDetails) -> RedisResult<()> {
        match details.auction_ends_at_ms() {
            Some(ends_at_ms) => {
                let element = format!("{}:{}", details.owner_id, details.id);
                Self::put_index_sorted_set(
                    &LISTING_AUCTION_ENDS_KEY_PARTS,
                    &[(ends_at_ms as f64, element.as_str())],
                    None,
                    None,
                )
                .await
            }
            None => Self::remove_from_auction_ends_sorted_set(&details.owner_id, &details.id).await,
        }
    }

    /// Removes the listing from the auction end-time sorted set.
    pub async fn remove_from_auction_ends_sorted_set(
        owner_id: &str,
        listing_id: &str,
    ) -> RedisResult<()> {
        let element = format!("{owner_id}:{listing_id}");
        Self::remove_from_index_sorted_set(
            None,
            &LISTING_AUCTION_ENDS_KEY_PARTS,
            &[element.as_str()],
        )
        .await
    }
}
