use crate::db::graph::lock::LockTarget;
use crate::db::graph::Query;
use crate::db::kv::{RedisResult, ScoreAction};
use crate::db::{
    execute_graph_operation_in, queries, GraphResult, GraphTxn, OperationOutcome, RedisOps,
};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use super::traits::{TagCollection, TaggersCollection};

pub const LISTING_TAGS_KEY_PARTS: [&str; 2] = ["Listings", "Tag"];

/// Community tags on a marketplace listing. Mirrors [`super::post::TagPost`]:
/// the tagged target is identified by the seller id (`user_id` in trait terms)
/// plus the listing id (`extra_param`).
#[derive(Serialize, Deserialize, Debug, Clone, ToSchema, Default)]
pub struct TagListing(pub Vec<String>);

impl AsRef<[String]> for TagListing {
    fn as_ref(&self) -> &[String] {
        &self.0
    }
}

#[async_trait]
impl RedisOps for TagListing {
    async fn prefix() -> String {
        String::from("Listing:Taggers")
    }
}

#[async_trait]
impl TagCollection for TagListing {
    fn get_tag_prefix<'a>() -> [&'a str; 2] {
        LISTING_TAGS_KEY_PARTS
    }

    fn lock_target<'a>(user_id: &'a str, extra_param: Option<&'a str>) -> LockTarget<'a> {
        LockTarget::Listing {
            owner_id: user_id,
            listing_id: extra_param.unwrap_or_default(),
        }
    }

    /// The trait default resolves the sorted-set key to the post/user
    /// prefixes; listings have their own.
    async fn update_index_score(
        author_id: &str,
        extra_param: Option<&str>,
        label: &str,
        score_action: ScoreAction,
    ) -> RedisResult<()> {
        let listing_id = extra_param.unwrap_or_default();
        let key: Vec<&str> = [&LISTING_TAGS_KEY_PARTS[..], &[author_id, listing_id]].concat();
        Self::put_score_index_sorted_set(&key, &[label], score_action).await
    }

    /// The trait default writes post/user tag edges; listings tag the
    /// `Listing` node instead.
    async fn put_to_graph(
        txn: &mut GraphTxn,
        tagger_user_id: &str,
        tagged_user_id: &str,
        extra_param: Option<&str>,
        tag_id: &str,
        label: &str,
        indexed_at: i64,
    ) -> GraphResult<OperationOutcome> {
        let listing_id = extra_param.unwrap_or_default();
        let query = queries::put::create_listing_tag(
            tagger_user_id,
            tagged_user_id,
            listing_id,
            tag_id,
            label,
            indexed_at,
        );
        let outcome = execute_graph_operation_in(txn, query).await?;
        if !matches!(outcome, OperationOutcome::MissingDependency) {
            txn.hold(&Self::lock_target(tagged_user_id, extra_param).key());
            txn.hold(&LockTarget::User(tagger_user_id).key());
        }
        Ok(outcome)
    }

    fn read_graph_query(user_id: &str, extra_param: Option<&str>) -> Query {
        queries::get::listing_tags(user_id, extra_param.unwrap_or_default())
    }
}

impl TaggersCollection for TagListing {}
