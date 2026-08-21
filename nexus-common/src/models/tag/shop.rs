use crate::db::graph::Query;
use crate::db::kv::{RedisResult, ScoreAction};
use crate::db::{execute_graph_operation, queries, GraphResult, OperationOutcome, RedisOps};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use super::traits::{TagCollection, TaggersCollection};

pub const SHOP_TAGS_KEY_PARTS: [&str; 2] = ["Shops", "Tag"];

/// Community tags on a marketplace shop. Mirrors [`super::user::TagUser`]:
/// the tagged target is identified by the shop owner's pubky alone (a shop is
/// a singleton per user), so `extra_param` is always `None`.
#[derive(Serialize, Deserialize, Debug, Clone, ToSchema, Default)]
pub struct TagShop(pub Vec<String>);

impl AsRef<[String]> for TagShop {
    fn as_ref(&self) -> &[String] {
        &self.0
    }
}

#[async_trait]
impl RedisOps for TagShop {
    async fn prefix() -> String {
        String::from("Shop:Taggers")
    }
}

#[async_trait]
impl TagCollection for TagShop {
    fn get_tag_prefix<'a>() -> [&'a str; 2] {
        SHOP_TAGS_KEY_PARTS
    }

    /// The trait default resolves the sorted-set key to the post/user
    /// prefixes; shops have their own.
    async fn update_index_score(
        author_id: &str,
        _extra_param: Option<&str>,
        label: &str,
        score_action: ScoreAction,
    ) -> RedisResult<()> {
        let key: Vec<&str> = [&SHOP_TAGS_KEY_PARTS[..], &[author_id]].concat();
        Self::put_score_index_sorted_set(&key, &[label], score_action).await
    }

    /// The trait default writes post/user tag edges; shops tag the `Shop`
    /// node instead.
    async fn put_to_graph(
        tagger_user_id: &str,
        tagged_user_id: &str,
        _extra_param: Option<&str>,
        tag_id: &str,
        label: &str,
        indexed_at: i64,
    ) -> GraphResult<OperationOutcome> {
        let query = queries::put::create_shop_tag(
            tagger_user_id,
            tagged_user_id,
            tag_id,
            label,
            indexed_at,
        );
        execute_graph_operation(query).await
    }

    fn read_graph_query(user_id: &str, _extra_param: Option<&str>) -> Query {
        queries::get::shop_tags(user_id)
    }
}

impl TaggersCollection for TagShop {}
