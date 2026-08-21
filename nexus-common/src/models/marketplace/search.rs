use crate::db::kv::RedisResult;
use crate::db::RedisOps;
use crate::models::marketplace::ListingDetails;
use crate::models::tag::listing::TagListing;
use crate::models::tag::traits::TaggersCollection;
use crate::types::Pagination;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

pub const TAG_GLOBAL_LISTING_TIMELINE: [&str; 4] = ["Tags", "Global", "Listing", "Timeline"];

/// Represents a single result of a "listings by tag" search, returning the
/// listing keys (`owner_id:listing_id`) and score. Mirrors
/// [`crate::models::post::PostsByTagSearch`] for the marketplace: listings
/// have no engagement scoring, so only the timeline sorted set exists.
#[derive(Serialize, Deserialize, ToSchema, Default)]
pub struct ListingsByTagSearch {
    pub listing_key: String,
    pub score: usize,
}

impl From<(String, f64)> for ListingsByTagSearch {
    fn from(tuple: (String, f64)) -> Self {
        ListingsByTagSearch {
            listing_key: tuple.0,
            score: tuple.1 as usize,
        }
    }
}

impl RedisOps for ListingsByTagSearch {}

impl ListingsByTagSearch {
    /// Retrieves the listing keys tagged with the label, newest-indexed first.
    pub async fn get_by_label(
        label: &str,
        pagination: Pagination,
    ) -> RedisResult<Option<Vec<ListingsByTagSearch>>> {
        let listing_score_list = Self::try_from_index_sorted_set(
            &[&TAG_GLOBAL_LISTING_TIMELINE[..], &[label]].concat(),
            pagination.start,
            pagination.end,
            pagination.skip,
            pagination.limit,
            crate::db::kv::SortOrder::Descending,
            None,
        )
        .await?;

        match listing_score_list {
            Some(list) => Ok(Some(list.into_iter().map(|t| t.into()).collect())),
            None => Ok(None),
        }
    }

    /// Adds the listing to the label's global timeline sorted set, scored by
    /// the listing's `indexed_at`, unless it is already a member.
    pub async fn put_to_index(
        owner_id: &str,
        listing_id: &str,
        tag_label: &str,
    ) -> RedisResult<()> {
        let listing_key_slice: &[&str] = &[owner_id, listing_id];
        let key_parts = [&TAG_GLOBAL_LISTING_TIMELINE[..], &[tag_label]].concat();
        let tag_search = Self::check_sorted_set_member(None, &key_parts, listing_key_slice).await?;
        if tag_search.is_none() {
            let option = ListingDetails::try_from_index_json(listing_key_slice, None).await?;
            if let Some(listing_details) = option {
                let member_key = listing_key_slice.join(":");
                Self::put_index_sorted_set(
                    &key_parts,
                    &[(listing_details.indexed_at as f64, &member_key)],
                    None,
                    None,
                )
                .await?;
            }
        }
        Ok(())
    }

    /// Removes the listing from the label's global timeline sorted set when
    /// its last tagger with that label is gone.
    pub async fn del_from_index(
        owner_id: &str,
        listing_id: &str,
        tag_label: &str,
    ) -> RedisResult<()> {
        let listing_label_key = vec![owner_id, listing_id, tag_label];
        let (taggers, _) =
            TagListing::get_from_index(listing_label_key, None, None, None, None).await?;
        // Make sure the listing has no more taggers with that label: Listing:Taggers:owner_id:listing_id:label
        if taggers.is_empty() {
            let key_parts = [&TAG_GLOBAL_LISTING_TIMELINE[..], &[tag_label]].concat();
            let listing_key = format!("{owner_id}:{listing_id}");
            Self::remove_from_index_sorted_set(None, &key_parts, &[&listing_key]).await?;
        }
        Ok(())
    }
}
