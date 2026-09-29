use super::ListingStream;
use crate::db::kv::RedisResult;
use crate::db::{
    exec_single_row, execute_graph_operation, fetch_row_from_graph, queries, GraphResult,
    OperationOutcome, RedisOps,
};
use crate::models::error::ModelResult;
use chrono::Utc;
use pubky_app_specs::{
    listing_uri_builder, PubkyAppFulfillmentMethod, PubkyAppListing, PubkyAppListingCondition,
    PubkyAppListingSale, PubkyAppListingState, PubkyId,
};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// Discriminator of the sale mechanism of a listing (fixed price or auction).
#[derive(Serialize, Deserialize, ToSchema, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ListingSaleFormat {
    FixedPrice,
    Auction,
}

impl From<&PubkyAppListingSale> for ListingSaleFormat {
    fn from(sale: &PubkyAppListingSale) -> Self {
        match sale {
            PubkyAppListingSale::FixedPrice { .. } => ListingSaleFormat::FixedPrice,
            PubkyAppListingSale::Auction { .. } => ListingSaleFormat::Auction,
        }
    }
}

/// Represents the indexed details of a marketplace listing.
///
/// The `auction_*` fields carry the public auction sale terms and are `null`
/// for fixed-price listings. The auction money terms are expressed in minor
/// units of the listing's primary asset (`price_currency` /
/// `price_exponent`); the specs validation guarantees all auction prices
/// share that asset. Private reserve terms are deliberately not indexed.
#[derive(Serialize, Deserialize, ToSchema, Clone, Debug, PartialEq)]
pub struct ListingDetails {
    pub id: String,
    pub uri: String,
    pub owner_id: String,
    pub indexed_at: i64,
    /// Lifecycle state as served by the index. Stored records carry the state
    /// the seller published; reads report an `active` auction whose
    /// `auction_ends_at` has passed as `ended` (see [`Self::effective_state_at`]).
    pub state: PubkyAppListingState,
    pub title: String,
    pub description: String,
    pub category_id: String,
    pub condition: PubkyAppListingCondition,
    pub tags: Vec<String>,
    pub country_code: String,
    pub region: Option<String>,
    pub media_urls: Vec<String>,
    pub sale_format: ListingSaleFormat,
    pub price_amount_minor: i64,
    pub price_currency: String,
    pub price_exponent: i64,
    pub auction_starts_at: Option<String>,
    pub auction_ends_at: Option<String>,
    pub auction_buy_now_price_minor: Option<i64>,
    pub auction_minimum_increment_minor: Option<i64>,
    pub fulfillment_methods: Vec<PubkyAppFulfillmentMethod>,
    pub adult_only: bool,
    pub created_at: String,
    pub updated_at: String,
    pub revision: i64,
}

impl RedisOps for ListingDetails {}

impl ListingDetails {
    pub fn from_homeserver(
        homeserver_listing: PubkyAppListing,
        owner_id: &PubkyId,
        listing_id: &str,
    ) -> Self {
        let primary_price = homeserver_listing.sale.primary_price().clone();
        let (
            auction_starts_at,
            auction_ends_at,
            auction_buy_now_price_minor,
            auction_minimum_increment_minor,
        ) = match &homeserver_listing.sale {
            PubkyAppListingSale::FixedPrice { .. } => (None, None, None, None),
            PubkyAppListingSale::Auction {
                buy_now_price,
                minimum_increment,
                starts_at,
                ends_at,
                ..
            } => (
                Some(starts_at.clone()),
                Some(ends_at.clone()),
                buy_now_price.as_ref().map(|price| price.amount_minor),
                Some(minimum_increment.amount_minor),
            ),
        };
        ListingDetails {
            id: listing_id.to_string(),
            uri: listing_uri_builder(owner_id.to_string(), listing_id.into()),
            owner_id: owner_id.to_string(),
            indexed_at: Utc::now().timestamp_millis(),
            state: homeserver_listing.state,
            title: homeserver_listing.title,
            description: homeserver_listing.description,
            category_id: homeserver_listing.category_id,
            condition: homeserver_listing.condition,
            tags: homeserver_listing.tags,
            country_code: homeserver_listing.location.country_code,
            region: homeserver_listing.location.region,
            media_urls: homeserver_listing
                .media
                .iter()
                .map(|media| media.url.clone())
                .collect(),
            sale_format: ListingSaleFormat::from(&homeserver_listing.sale),
            price_amount_minor: primary_price.amount_minor,
            price_currency: primary_price.currency,
            price_exponent: primary_price.exponent,
            auction_starts_at,
            auction_ends_at,
            auction_buy_now_price_minor,
            auction_minimum_increment_minor,
            fulfillment_methods: homeserver_listing.fulfillment_methods,
            adult_only: homeserver_listing.adult_only,
            created_at: homeserver_listing.created_at,
            updated_at: homeserver_listing.updated_at,
            revision: homeserver_listing.revision,
        }
    }

    /// The price expressed in major units, used for range filtering in the graph.
    pub fn price_major(&self) -> f64 {
        self.price_amount_minor as f64 / 10f64.powi(self.price_exponent as i32)
    }

    /// The auction end time as epoch milliseconds, used as the score of the
    /// auction end-time sorted set and for end-time sorting in the graph.
    /// `None` for fixed-price listings.
    pub fn auction_ends_at_ms(&self) -> Option<i64> {
        let ends_at = self.auction_ends_at.as_deref()?;
        chrono::DateTime::parse_from_rfc3339(ends_at)
            .ok()
            .map(|datetime| datetime.timestamp_millis())
    }

    /// The lifecycle state a reader sees at `now_ms` (epoch milliseconds).
    ///
    /// An auction is closed to bidding once its end time is reached, but the
    /// seller's record keeps `active` until the seller edits it. The stored
    /// state is left alone (only the seller can change it, and the index
    /// cannot rewrite it); an `active` auction whose end time is at or before
    /// `now_ms` reads as `ended`. `paused` and `removed` are the seller's
    /// explicit choices and are never overridden.
    pub fn effective_state_at(&self, now_ms: i64) -> PubkyAppListingState {
        match (self.state, self.auction_ends_at_ms()) {
            (PubkyAppListingState::Active, Some(ends_at_ms)) if ends_at_ms <= now_ms => {
                PubkyAppListingState::Ended
            }
            (state, _) => state,
        }
    }

    /// Returns the details with [`Self::effective_state_at`] applied. Only for
    /// values on their way out to a reader; never write the result back to
    /// the graph or the Redis cache.
    pub fn with_effective_state_at(mut self, now_ms: i64) -> Self {
        self.state = self.effective_state_at(now_ms);
        self
    }

    /// Retrieves listing details by seller ID and listing ID, first trying Redis,
    /// then falling back to Neo4j. The returned state is the effective state at
    /// the current time.
    pub async fn get_by_id(
        owner_id: &str,
        listing_id: &str,
    ) -> ModelResult<Option<ListingDetails>> {
        Self::get_by_id_at(owner_id, listing_id, Utc::now().timestamp_millis()).await
    }

    /// Like [`Self::get_by_id`], with the effective state evaluated at `now_ms`
    /// so a caller that also filtered by state uses one clock for both.
    pub async fn get_by_id_at(
        owner_id: &str,
        listing_id: &str,
        now_ms: i64,
    ) -> ModelResult<Option<ListingDetails>> {
        let stored = match Self::get_from_index(owner_id, listing_id).await? {
            Some(details) => Some(details),
            None => {
                let maybe_details = Self::get_from_graph(owner_id, listing_id).await?;
                if let Some(details) = &maybe_details {
                    details.put_to_index(false).await?;
                }
                maybe_details
            }
        };
        Ok(stored.map(|details| details.with_effective_state_at(now_ms)))
    }

    pub async fn get_from_index(
        owner_id: &str,
        listing_id: &str,
    ) -> RedisResult<Option<ListingDetails>> {
        Self::try_from_index_json(&[owner_id, listing_id], None).await
    }

    /// Retrieves the listing fields from Neo4j.
    pub async fn get_from_graph(
        owner_id: &str,
        listing_id: &str,
    ) -> GraphResult<Option<ListingDetails>> {
        let query = queries::get::get_listing_by_id(owner_id, listing_id);
        let maybe_row = fetch_row_from_graph(query).await?;

        let Some(row) = maybe_row else {
            return Ok(None);
        };

        let listing: ListingDetails = row.get("details")?;
        Ok(Some(listing))
    }

    // Save new graph node
    pub async fn put_to_graph(&self) -> GraphResult<OperationOutcome> {
        let query = queries::put::create_listing(self)?;
        execute_graph_operation(query).await
    }

    /// Stores the listing details JSON and, unless this is an edit of an already
    /// indexed listing, adds the listing to the stream sorted sets. The auction
    /// end-time sorted set is refreshed on every write because an edit can
    /// change the auction end time or the sale format.
    pub async fn put_to_index(&self, is_edit: bool) -> RedisResult<()> {
        self.put_index_json(&[&self.owner_id, &self.id], None, None)
            .await?;
        ListingStream::upsert_auction_ends_sorted_set(self).await?;
        if is_edit {
            return Ok(());
        }
        ListingStream::add_to_timeline_sorted_set(self).await?;
        ListingStream::add_to_per_seller_sorted_set(self).await?;
        Ok(())
    }

    pub async fn delete(owner_id: &str, listing_id: &str) -> ModelResult<()> {
        // Delete listing graph node
        exec_single_row(queries::del::delete_listing(owner_id, listing_id)).await?;
        Self::delete_indexes(owner_id, listing_id).await
    }

    /// Deletes the listing's Redis details and stream memberships. The
    /// watcher deletes the graph node with its tags first.
    pub async fn delete_indexes(owner_id: &str, listing_id: &str) -> ModelResult<()> {
        // Delete listing details on Redis
        Self::remove_from_index_multiple_json(&[&[owner_id, listing_id]]).await?;
        // Remove from stream sorted sets
        ListingStream::remove_from_timeline_sorted_set(owner_id, listing_id).await?;
        ListingStream::remove_from_per_seller_sorted_set(owner_id, listing_id).await?;
        ListingStream::remove_from_auction_ends_sorted_set(owner_id, listing_id).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::ListingDetails;
    use crate::models::marketplace::ListingStream;
    use pubky_app_specs::PubkyAppListingState;
    use serde_json::Value;

    const PRODUCTION_DETAIL_BEFORE_CLOSE: &str = include_str!(
        "../../../../nexus-webapi/tests/marketplace/fixtures/production-nexus-detail-before-close.txt"
    );
    const PRODUCTION_STREAM_BEFORE_CLOSE: &str = include_str!(
        "../../../../nexus-webapi/tests/marketplace/fixtures/production-nexus-stream-before-close.txt"
    );

    fn captured_response_body(capture: &str) -> Value {
        serde_json::from_str(
            capture
                .lines()
                .rev()
                .find(|line| !line.trim().is_empty())
                .expect("captured response body"),
        )
        .expect("captured production JSON")
    }

    fn contains_forbidden_reserve_key(value: &Value) -> bool {
        match value {
            Value::Object(object) => object.iter().any(|(key, child)| {
                matches!(
                    key.as_str(),
                    "auction_reserve_price_minor"
                        | "reservePrice"
                        | "reserve_price"
                        | "reserveMet"
                        | "reserve_met"
                ) || contains_forbidden_reserve_key(child)
            }),
            Value::Array(array) => array.iter().any(contains_forbidden_reserve_key),
            _ => false,
        }
    }

    fn captured_ended_auction() -> ListingDetails {
        let mut detail: ListingDetails =
            serde_json::from_value(captured_response_body(PRODUCTION_DETAIL_BEFORE_CLOSE))
                .expect("captured detail shape");
        detail.state = PubkyAppListingState::Active;
        detail
    }

    #[test]
    fn captured_production_auction_reads_as_ended_after_its_end_time() {
        // Production listing 7dd7e427…: state `active` in the record, auction
        // ended 2026-09-20T14:34:39.384Z.
        let detail = captured_ended_auction();
        assert_eq!(detail.state, PubkyAppListingState::Active);
        let ends_at_ms = detail.auction_ends_at_ms().expect("auction end time");
        assert_eq!(ends_at_ms, 1_789_914_879_384);

        let now_ms = 1_790_640_000_000; // 2026-09-29T00:00:00Z
        assert_eq!(
            detail.effective_state_at(now_ms),
            PubkyAppListingState::Ended
        );
        let served = detail.clone().with_effective_state_at(now_ms);
        assert_eq!(
            serde_json::to_value(&served).expect("serialized detail")["state"],
            "ended"
        );
        // The stored value is untouched by the read-side derivation.
        assert_eq!(detail.state, PubkyAppListingState::Active);
    }

    #[test]
    fn auction_state_flips_exactly_at_the_end_time() {
        let detail = captured_ended_auction();
        let ends_at_ms = detail.auction_ends_at_ms().expect("auction end time");

        assert_eq!(
            detail.effective_state_at(ends_at_ms - 1),
            PubkyAppListingState::Active,
            "one millisecond before the end time the auction is still open"
        );
        assert_eq!(
            detail.effective_state_at(ends_at_ms),
            PubkyAppListingState::Ended,
            "at the end time the auction is ended"
        );
        assert_eq!(
            detail.effective_state_at(ends_at_ms + 1),
            PubkyAppListingState::Ended
        );
    }

    #[test]
    fn seller_choices_and_fixed_price_listings_are_never_overridden() {
        let mut detail = captured_ended_auction();
        let after_end = detail.auction_ends_at_ms().expect("auction end time") + 1;

        for state in [
            PubkyAppListingState::Paused,
            PubkyAppListingState::Removed,
            PubkyAppListingState::Ended,
        ] {
            detail.state = state;
            assert_eq!(detail.effective_state_at(after_end), state);
        }

        detail.state = PubkyAppListingState::Active;
        detail.auction_ends_at = None;
        assert_eq!(
            detail.effective_state_at(i64::MAX),
            PubkyAppListingState::Active,
            "a listing without an auction end time never expires"
        );
    }

    #[test]
    fn captured_detail_and_stream_serialize_without_reserve_keys() {
        let detail_wire = captured_response_body(PRODUCTION_DETAIL_BEFORE_CLOSE);
        assert!(detail_wire.get("auction_reserve_price_minor").is_some());
        let detail: ListingDetails =
            serde_json::from_value(detail_wire).expect("captured detail shape");
        let serialized_detail = serde_json::to_value(detail).expect("serialized detail");
        assert!(
            !contains_forbidden_reserve_key(&serialized_detail),
            "detail must omit reserve keys rather than serializing null"
        );

        let stream_wire = captured_response_body(PRODUCTION_STREAM_BEFORE_CLOSE);
        assert!(stream_wire
            .as_array()
            .expect("captured stream")
            .iter()
            .any(|listing| listing.get("auction_reserve_price_minor").is_some()));
        let stream: ListingStream =
            serde_json::from_value(stream_wire).expect("captured stream shape");
        let serialized_stream = serde_json::to_value(stream).expect("serialized stream");
        assert!(
            !contains_forbidden_reserve_key(&serialized_stream),
            "stream must omit reserve keys rather than serializing null"
        );
    }
}
