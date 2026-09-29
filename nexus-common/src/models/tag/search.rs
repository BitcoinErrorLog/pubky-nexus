use crate::db::kv::RedisResult;
use crate::db::queries::get::get_tags;
use crate::db::{fetch_key_from_graph, GraphTxn, RedisOps};
use crate::models::create_zero_score_tuples;
use crate::models::error::ModelResult;
use crate::types::Pagination;

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

pub const TAGS_LABEL: [&str; 2] = ["Tags", "Label"];

/// Represents a single search result of a tag search
#[derive(Serialize, Deserialize, ToSchema, Default)]
pub struct TagSearch(String);

impl RedisOps for TagSearch {}

impl TagSearch {
    /// Retrieves tags from the Neo4j graph and updates global sorted set
    pub async fn reindex() -> ModelResult<()> {
        let tag_labels_opt = fetch_key_from_graph(get_tags(), "tag_labels").await?;
        let tag_labels: Vec<String> = tag_labels_opt.unwrap_or_default();
        Self::put_to_index(&tag_labels).await.map_err(Into::into)
    }

    pub async fn get_by_label(
        label_prefix: &str,
        pagination: &Pagination,
    ) -> RedisResult<Option<Vec<TagSearch>>> {
        let label_prefix_lowercase = label_prefix.to_lowercase();
        let min_inclusive = format!("[{label_prefix_lowercase}");

        // We mark the end of the label prefix upper bound with the maximum possible Unicode code point
        // Any valid Unicode string will be lexicographically smaller than a string ending with this character
        let max_unicode_char = char::MAX;
        let max_exclusive = format!("({label_prefix_lowercase}{max_unicode_char}");

        Self::try_from_index_sorted_set_lex(
            &TAGS_LABEL,
            &min_inclusive,
            &max_exclusive,
            pagination.skip,
            pagination.limit,
        )
        .await
        .map(|opt| opt.map(|list| list.into_iter().map(TagSearch).collect()))
    }

    pub async fn put_to_index(tag_labels: &[String]) -> RedisResult<()> {
        let elements: Vec<(f64, &str)> = create_zero_score_tuples(tag_labels);
        Self::put_index_sorted_set(&TAGS_LABEL, &elements, None, None).await
    }

    pub async fn del_from_index(tag_label: &str) -> RedisResult<()> {
        Self::remove_from_index_sorted_set(None, &TAGS_LABEL, &[tag_label]).await
    }

    /// Removes the label from the suggestions once no `TAGGED` edge on any
    /// target type (post, user, listing, shop) still carries it. Call after
    /// the deleted edge is gone from the graph.
    pub async fn del_from_index_if_unused(tag_label: &str) -> ModelResult<()> {
        Self::del_from_index_if_unused_with_hook(tag_label, async {}).await
    }

    /// [`Self::del_from_index_if_unused`] for an untag whose edge deletion
    /// is still uncommitted in `txn`: the in-use checks run in `txn`, which
    /// sees its own deletion, where another connection would still see the
    /// edge.
    pub async fn del_from_index_if_unused_in(
        txn: &mut GraphTxn,
        tag_label: &str,
    ) -> ModelResult<()> {
        if Self::label_in_use_in(txn, tag_label).await? {
            return Ok(());
        }
        Self::del_from_index(tag_label).await?;
        if Self::label_in_use_in(txn, tag_label).await? {
            Self::put_to_index(&[tag_label.to_string()]).await?;
        }
        Ok(())
    }

    /// [`Self::del_from_index_if_unused`] with `between` awaited after the
    /// first in-use check and before the removal. Production callers use
    /// [`Self::del_from_index_if_unused`].
    #[doc(hidden)]
    pub async fn del_from_index_if_unused_with_hook(
        tag_label: &str,
        between: impl std::future::Future<Output = ()>,
    ) -> ModelResult<()> {
        if Self::label_in_use(tag_label).await? {
            return Ok(());
        }
        between.await;
        Self::del_from_index(tag_label).await?;
        // A tag PUT adds the label only after its edge commits. An edge that
        // committed before the removal is seen here and the label goes back;
        // a PUT that indexes after the removal re-adds the label itself.
        if Self::label_in_use(tag_label).await? {
            Self::put_to_index(&[tag_label.to_string()]).await?;
        }
        Ok(())
    }

    async fn label_in_use_in(txn: &mut GraphTxn, tag_label: &str) -> ModelResult<bool> {
        let row = txn
            .fetch_row(crate::db::queries::get::tag_label_in_use(tag_label))
            .await?;
        let in_use: Option<bool> = row.map(|row| row.get("in_use")).transpose()?;
        Ok(in_use == Some(true))
    }

    async fn label_in_use(tag_label: &str) -> ModelResult<bool> {
        let in_use: Option<bool> = fetch_key_from_graph(
            crate::db::queries::get::tag_label_in_use(tag_label),
            "in_use",
        )
        .await?;
        Ok(in_use == Some(true))
    }
}
