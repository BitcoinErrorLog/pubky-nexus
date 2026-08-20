use super::{ListingStream, ListingStreamFilters, ShopDetails};
use crate::db::kv::SortOrder;
use crate::models::error::ModelResult;
use crate::types::Pagination;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// Represents a seller's shop together with a page of their listings.
#[derive(Serialize, Deserialize, ToSchema, Debug)]
pub struct ShopView {
    pub details: ShopDetails,
    pub listings: ListingStream,
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
        let listings = ListingStream::get_listings(filters, pagination, SortOrder::Descending)
            .await?
            .unwrap_or_default();

        Ok(Some(ShopView { details, listings }))
    }
}
