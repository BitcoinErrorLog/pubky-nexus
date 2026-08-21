use crate::db::{fetch_row_from_graph, queries, RedisOps};
use crate::models::error::ModelResult;
use pubky_app_specs::PubkyAppReviewRole;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use utoipa::ToSchema;

/// The compact reputation object embedded in listing stream entries and shop
/// views (ADR 0024 §9): cards render stars with zero additional requests.
///
/// Truth basis: `count` covers every indexed review of the scope;
/// `verified_count` covers the subset whose embedded purchase attestation
/// cryptographically verified (parse + Ed25519 signature against the `iss`
/// pubky + review binding). Which attestors to *trust* is the consumer's
/// policy — the full [`ReputationSummary`] breaks verified counts down per
/// attestor pubky. `avg` averages `ratings.overall` over ALL indexed
/// reviews (verified and labeled-unverified alike, ratified D5).
#[derive(Serialize, Deserialize, ToSchema, Clone, Debug, PartialEq)]
pub struct ReputationSnippet {
    pub avg: f64,
    pub count: u64,
    pub verified_count: u64,
}

/// The full reputation aggregate of one scope: a subject in one review role
/// (`Subject:{pubky}:{role}`) or a listing (`Listing:{owner}:{listing_id}`,
/// buyer reviews only).
///
/// Recomputed from the graph's `REVIEWED` edges on every review/response
/// event — the whole aggregate is reproducible from public records by any
/// third party (design §9); this index is a cache, not an authority.
#[derive(Serialize, Deserialize, ToSchema, Clone, Debug, PartialEq)]
pub struct ReputationSummary {
    /// Total indexed reviews in this scope, verified and unverified.
    pub count: u64,
    /// Reviews whose purchase attestation cryptographically verified.
    pub verified_count: u64,
    /// Mean of `ratings.overall` over all indexed reviews.
    pub avg: f64,
    /// Star histogram of `ratings.overall`: index 0 holds 1-star counts.
    pub histogram: [u64; 5],
    pub avg_item_accuracy: Option<f64>,
    pub avg_shipping: Option<f64>,
    pub avg_communication: Option<f64>,
    /// Reviews carrying a subject response record.
    pub response_count: u64,
    /// Reviews edited beyond the marketplace's 24h edit window.
    pub edited_late_count: u64,
    /// Verified reviews per attestor pubky. Verification proves the signer;
    /// whether a signer is a trusted attestor is the consumer's decision.
    pub attestors: BTreeMap<String, u64>,
    /// `created_at` of the most recent review in this scope.
    pub last_reviewed_at: Option<String>,
}

impl RedisOps for ReputationSummary {}

pub const REPUTATION_SUBJECT_KEY: &str = "Subject";
pub const REPUTATION_LISTING_KEY: &str = "Listing";

impl ReputationSummary {
    pub fn snippet(&self) -> ReputationSnippet {
        ReputationSnippet {
            avg: self.avg,
            count: self.count,
            verified_count: self.verified_count,
        }
    }

    pub async fn get_by_subject(
        subject_id: &str,
        role: PubkyAppReviewRole,
    ) -> ModelResult<Option<Self>> {
        Ok(
            Self::try_from_index_json(&[REPUTATION_SUBJECT_KEY, subject_id, role.as_str()], None)
                .await?,
        )
    }

    pub async fn get_by_listing(
        listing_owner_id: &str,
        listing_id: &str,
    ) -> ModelResult<Option<Self>> {
        Ok(Self::try_from_index_json(
            &[REPUTATION_LISTING_KEY, listing_owner_id, listing_id],
            None,
        )
        .await?)
    }

    /// Recomputes the subject-scoped aggregate from the graph and stores it,
    /// removing the key when the last review of the scope is gone.
    pub async fn recompute_subject(
        subject_id: &str,
        role: PubkyAppReviewRole,
    ) -> ModelResult<Option<Self>> {
        let query = queries::get::subject_reputation(subject_id, role.as_str());
        Self::recompute(query, &[REPUTATION_SUBJECT_KEY, subject_id, role.as_str()]).await
    }

    /// Recomputes the listing-scoped aggregate (buyer reviews only) from the
    /// graph and stores it, removing the key when empty.
    pub async fn recompute_listing(
        listing_owner_id: &str,
        listing_id: &str,
    ) -> ModelResult<Option<Self>> {
        let query = queries::get::listing_reputation(listing_owner_id, listing_id);
        Self::recompute(
            query,
            &[REPUTATION_LISTING_KEY, listing_owner_id, listing_id],
        )
        .await
    }

    async fn recompute(
        query: crate::db::graph::Query,
        key_parts: &[&str],
    ) -> ModelResult<Option<Self>> {
        let Some(row) = fetch_row_from_graph(query).await? else {
            Self::remove_from_index_multiple_json(&[key_parts]).await?;
            return Ok(None);
        };

        let count: i64 = row.get("total")?;
        if count == 0 {
            Self::remove_from_index_multiple_json(&[key_parts]).await?;
            return Ok(None);
        }

        let verified_count: i64 = row.get("verified_count")?;
        let avg: f64 = row.get::<Option<f64>>("average")?.unwrap_or(0.0);
        let histogram = [
            row.get::<i64>("stars_1")? as u64,
            row.get::<i64>("stars_2")? as u64,
            row.get::<i64>("stars_3")? as u64,
            row.get::<i64>("stars_4")? as u64,
            row.get::<i64>("stars_5")? as u64,
        ];
        let attestor_list: Vec<String> = row.get("attestors")?;
        let mut attestors: BTreeMap<String, u64> = BTreeMap::new();
        for attestor in attestor_list {
            *attestors.entry(attestor).or_default() += 1;
        }

        let summary = ReputationSummary {
            count: count as u64,
            verified_count: verified_count as u64,
            avg,
            histogram,
            avg_item_accuracy: row.get("avg_item_accuracy")?,
            avg_shipping: row.get("avg_shipping")?,
            avg_communication: row.get("avg_communication")?,
            response_count: row.get::<i64>("response_count")? as u64,
            edited_late_count: row.get::<i64>("edited_late_count")? as u64,
            attestors,
            last_reviewed_at: row.get("last_reviewed_at")?,
        };

        summary.put_index_json(key_parts, None, None).await?;
        Ok(Some(summary))
    }

    /// Batch-fetches subject-scoped snippets (buyer-reviewing-seller role)
    /// for stream hydration. Returns entries aligned with the input order;
    /// scopes without an aggregate yield `None` (honest absence, never a
    /// fabricated 0.0).
    pub async fn snippets_by_subjects(
        subject_ids: &[&str],
    ) -> ModelResult<Vec<Option<ReputationSnippet>>> {
        let role = PubkyAppReviewRole::BuyerReviewingSeller.as_str();
        let key_slices: Vec<Vec<&str>> = subject_ids
            .iter()
            .map(|subject| vec![REPUTATION_SUBJECT_KEY, *subject, role])
            .collect();
        let key_refs: Vec<&[&str]> = key_slices.iter().map(|parts| &parts[..]).collect();
        let summaries = Self::try_from_index_multiple_json(&key_refs).await?;
        Ok(summaries
            .into_iter()
            .map(|summary| summary.map(|summary| summary.snippet()))
            .collect())
    }

    /// Batch-fetches listing-scoped snippets for stream hydration.
    pub async fn snippets_by_listings(
        listing_keys: &[(&str, &str)],
    ) -> ModelResult<Vec<Option<ReputationSnippet>>> {
        let key_slices: Vec<Vec<&str>> = listing_keys
            .iter()
            .map(|(owner, listing)| vec![REPUTATION_LISTING_KEY, *owner, *listing])
            .collect();
        let key_refs: Vec<&[&str]> = key_slices.iter().map(|parts| &parts[..]).collect();
        let summaries = Self::try_from_index_multiple_json(&key_refs).await?;
        Ok(summaries
            .into_iter()
            .map(|summary| summary.map(|summary| summary.snippet()))
            .collect())
    }
}
