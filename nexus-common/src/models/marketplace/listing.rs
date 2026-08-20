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
/// The `auction_*` fields carry the auction sale terms and are `null` for
/// fixed-price listings. The auction money terms are expressed in minor units
/// of the listing's primary asset (`price_currency` / `price_exponent`); the
/// specs validation guarantees all auction prices share that asset.
#[derive(Serialize, Deserialize, ToSchema, Clone, Debug, PartialEq)]
pub struct ListingDetails {
    pub id: String,
    pub uri: String,
    pub owner_id: String,
    pub indexed_at: i64,
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
    pub auction_reserve_price_minor: Option<i64>,
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
            auction_reserve_price_minor,
            auction_buy_now_price_minor,
            auction_minimum_increment_minor,
        ) = match &homeserver_listing.sale {
            PubkyAppListingSale::FixedPrice { .. } => (None, None, None, None, None),
            PubkyAppListingSale::Auction {
                reserve_price,
                buy_now_price,
                minimum_increment,
                starts_at,
                ends_at,
                ..
            } => (
                Some(starts_at.clone()),
                Some(ends_at.clone()),
                reserve_price.as_ref().map(|price| price.amount_minor),
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
            auction_reserve_price_minor,
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

    /// Retrieves listing details by seller ID and listing ID, first trying Redis,
    /// then falling back to Neo4j.
    pub async fn get_by_id(
        owner_id: &str,
        listing_id: &str,
    ) -> ModelResult<Option<ListingDetails>> {
        match Self::get_from_index(owner_id, listing_id).await? {
            Some(details) => Ok(Some(details)),
            None => {
                let maybe_details = Self::get_from_graph(owner_id, listing_id).await?;
                if let Some(details) = maybe_details {
                    details.put_to_index(false).await?;
                    return Ok(Some(details));
                }
                Ok(None)
            }
        }
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
        // Delete listing details on Redis
        Self::remove_from_index_multiple_json(&[&[owner_id, listing_id]]).await?;
        // Remove from stream sorted sets
        ListingStream::remove_from_timeline_sorted_set(owner_id, listing_id).await?;
        ListingStream::remove_from_per_seller_sorted_set(owner_id, listing_id).await?;
        ListingStream::remove_from_auction_ends_sorted_set(owner_id, listing_id).await?;
        Ok(())
    }
}
