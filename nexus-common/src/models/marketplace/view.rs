use super::{
    ListingStream, ListingStreamFilters, ListingStreamSorting, ReputationSnippet,
    ReputationSummary, ShopDetails,
};
use crate::db::kv::SortOrder;
use crate::models::error::ModelResult;
use crate::types::Pagination;
use pubky_app_specs::PubkyAppReviewRole;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// Represents a seller's shop together with a page of their listings.
#[derive(Serialize, Deserialize, ToSchema, Debug)]
pub struct ShopView {
    pub details: ShopDetails,
    pub listings: ListingStream,
    /// Compact seller reputation (buyer reviews across all listings).
    /// Absent when the seller has no indexed review — the honest "New
    /// seller" state, never a fabricated 0.0.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reputation: Option<ReputationSnippet>,
}

impl ShopView {
    /// Retrieves the shop of a seller and a page of the seller's listings.
    /// Returns `None` when the seller has no indexed shop.
    pub async fn get_by_id(seller_id: &str, pagination: Pagination) -> ModelResult<Option<Self>> {
        let Some(details) = ShopDetails::get_by_id(seller_id).await? else {
            return Ok(None);
        };

        let filters = ListingStreamFilters {
            seller_id: Some(seller_id.to_string()),
            ..Default::default()
        };
        let (listings, reputation) = tokio::join!(
            ListingStream::get_listings(
                filters,
                pagination,
                SortOrder::Descending,
                ListingStreamSorting::Timeline,
            ),
            ReputationSummary::get_by_subject(seller_id, PubkyAppReviewRole::BuyerReviewingSeller),
        );
        let listings = listings?.unwrap_or_default();
        let reputation = reputation?.map(|summary| summary.snippet());

        Ok(Some(ShopView {
            details,
            listings,
            reputation,
        }))
    }
}
