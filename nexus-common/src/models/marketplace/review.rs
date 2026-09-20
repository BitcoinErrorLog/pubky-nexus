use super::ReviewResponseDetails;
use crate::db::kv::{RedisResult, SortOrder};
use crate::db::{
    exec_single_row, execute_graph_operation, queries, GraphResult, OperationOutcome, RedisOps,
};
use crate::models::error::ModelResult;
use crate::types::Pagination;
use chrono::Utc;
use pubky_app_specs::{
    marketplace_review_uri_builder, PubkyAppMarketplaceReview, PubkyAppPurchaseAttestation,
    PubkyAppReviewRole, PubkyId,
};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

pub const REVIEW_SUBJECT_KEY_PARTS: [&str; 2] = ["Reviews", "Subject"];
pub const REVIEW_LISTING_KEY_PARTS: [&str; 2] = ["Reviews", "Listing"];

/// The marketplace transaction service's review edit window. Records whose
/// `updated_at` moved beyond this window after `created_at` are flagged
/// `edited_late`: the window is app policy, the record is user property, and
/// the index surfaces the divergence instead of pretending the window is
/// protocol-enforced.
const REVIEW_EDIT_WINDOW_SECONDS: i64 = 24 * 60 * 60;

/// The indexed details of a marketplace review record.
///
/// Reviews live on the **reviewer's** homeserver; the embedded purchase
/// attestation (compact JWS) is verified at ingest. `verified` is a
/// cryptographic fact — the attestation parsed, its Ed25519 signature
/// verified against the self-certifying `iss` pubky, and its claims bind to
/// this exact review — never a trust statement. WHO is a trusted attestor is
/// client policy: the index records `attestor_id` on every verified review
/// and lets consumers apply their own attestor trust list at display time.
#[derive(Serialize, Deserialize, ToSchema, Clone, Debug, PartialEq)]
pub struct ReviewDetails {
    pub review_id: String,
    pub uri: String,
    /// The reviewer (record owner; the record lives on their homeserver).
    pub reviewer_id: String,
    /// The reviewed counterparty; aggregates key on this user.
    pub subject_id: String,
    pub listing_owner_id: String,
    pub listing_id: String,
    pub role: PubkyAppReviewRole,
    pub rating_overall: i64,
    pub rating_item_accuracy: Option<i64>,
    pub rating_shipping: Option<i64>,
    pub rating_communication: Option<i64>,
    pub text: String,
    /// True iff the embedded attestation parsed, signature-verified against
    /// its `iss` pubky, and bound to this review. Signer identity is in
    /// `attestor_id`; trusting that signer is the consumer's decision.
    pub verified: bool,
    /// The attestor pubky (`iss` claim) of a verified attestation.
    pub attestor_id: Option<String>,
    /// The attestor-salted order reference of a verified attestation.
    /// Distinct refs on one living review witness distinct attested orders.
    pub order_ref: Option<String>,
    /// True when `updated_at` moved more than the marketplace's 24h edit
    /// window after `created_at` (see [`REVIEW_EDIT_WINDOW_SECONDS`]).
    pub edited_late: bool,
    pub created_at: String,
    pub updated_at: String,
    pub revision: i64,
    pub indexed_at: i64,
}

impl RedisOps for ReviewDetails {}

impl ReviewDetails {
    /// Builds the indexed details from a validated homeserver record,
    /// running the offline attestation verification recipe (parse →
    /// signature → binding). A failed verification never rejects the record:
    /// unverified reviews are indexed and labeled (ratified D5).
    pub fn from_homeserver(review: &PubkyAppMarketplaceReview, reviewer_id: &PubkyId) -> Self {
        let (verified, attestor_id, order_ref) =
            match PubkyAppPurchaseAttestation::verify_for_review(review) {
                Ok(attestation) => (
                    true,
                    Some(attestation.claims.iss.clone()),
                    Some(attestation.claims.order_ref.clone()),
                ),
                Err(_) => (false, None, None),
            };

        ReviewDetails {
            review_id: review.review_id.clone(),
            uri: marketplace_review_uri_builder(reviewer_id.to_string(), review.review_id.clone()),
            reviewer_id: reviewer_id.to_string(),
            subject_id: review.subject_pubky.clone(),
            listing_owner_id: review.listing_owner_pubky.clone(),
            listing_id: review.listing_id.clone(),
            role: review.role,
            rating_overall: review.ratings.overall,
            rating_item_accuracy: review.ratings.item_accuracy,
            rating_shipping: review.ratings.shipping,
            rating_communication: review.ratings.communication,
            text: review.text.clone(),
            verified,
            attestor_id,
            order_ref,
            edited_late: is_edited_late(&review.created_at, &review.updated_at),
            created_at: review.created_at.clone(),
            updated_at: review.updated_at.clone(),
            revision: review.revision,
            indexed_at: Utc::now().timestamp_millis(),
        }
    }

    pub async fn get_from_index(
        reviewer_id: &str,
        review_id: &str,
    ) -> RedisResult<Option<ReviewDetails>> {
        Self::try_from_index_json(&[reviewer_id, review_id], None).await
    }

    /// Writes the `REVIEWED` edge between the reviewer and the subject.
    /// Returns `MissingDependency` when either user is not indexed yet.
    pub async fn put_to_graph(&self) -> GraphResult<OperationOutcome> {
        let query = queries::put::create_review(self)?;
        execute_graph_operation(query).await
    }

    /// Stores the details JSON and, unless this is an edit of an already
    /// indexed review, adds the review to the per-subject (and, for buyer
    /// reviews, per-listing) sorted sets. Edits keep the original position.
    pub async fn put_to_index(&self, is_edit: bool) -> RedisResult<()> {
        self.put_index_json(&[&self.reviewer_id, &self.review_id], None, None)
            .await?;
        if is_edit {
            return Ok(());
        }
        let member = format!("{}:{}", self.reviewer_id, self.review_id);
        let score = self.indexed_at as f64;
        let subject_key = [
            &REVIEW_SUBJECT_KEY_PARTS[..],
            &[self.subject_id.as_str(), self.role.as_str()],
        ]
        .concat();
        Self::put_index_sorted_set(&subject_key, &[(score, member.as_str())], None, None).await?;
        // A listing's star rating comes from buyers; seller-side reviews of
        // buyers reference the listing but do not rate it.
        if self.role == PubkyAppReviewRole::BuyerReviewingSeller {
            let listing_key = [
                &REVIEW_LISTING_KEY_PARTS[..],
                &[self.listing_owner_id.as_str(), self.listing_id.as_str()],
            ]
            .concat();
            Self::put_index_sorted_set(&listing_key, &[(score, member.as_str())], None, None)
                .await?;
        }
        Ok(())
    }

    /// Removes the review from the graph and every Redis index.
    pub async fn delete(&self) -> ModelResult<()> {
        exec_single_row(queries::del::delete_review(
            &self.reviewer_id,
            &self.review_id,
        ))
        .await?;
        Self::remove_from_index_multiple_json(&[&[&self.reviewer_id, &self.review_id]]).await?;
        let member = format!("{}:{}", self.reviewer_id, self.review_id);
        let subject_key = [
            &REVIEW_SUBJECT_KEY_PARTS[..],
            &[self.subject_id.as_str(), self.role.as_str()],
        ]
        .concat();
        Self::remove_from_index_sorted_set(None, &subject_key, &[member.as_str()]).await?;
        if self.role == PubkyAppReviewRole::BuyerReviewingSeller {
            let listing_key = [
                &REVIEW_LISTING_KEY_PARTS[..],
                &[self.listing_owner_id.as_str(), self.listing_id.as_str()],
            ]
            .concat();
            Self::remove_from_index_sorted_set(None, &listing_key, &[member.as_str()]).await?;
        }
        Ok(())
    }
}

/// One entry of a paged review list: the review plus, when the subject has
/// published one, their response record (subject-only, one per review —
/// ratified D7; authorization is structural, `owner == subjectPubky`).
#[derive(Serialize, Deserialize, ToSchema, Debug)]
pub struct ReviewView {
    pub review: ReviewDetails,
    pub response: Option<ReviewResponseDetails>,
}

/// A page of reviews with joined responses.
#[derive(Serialize, Deserialize, ToSchema, Debug, Default)]
pub struct ReviewStream(pub Vec<ReviewView>);

impl RedisOps for ReviewStream {}

impl ReviewStream {
    /// Retrieves a page of reviews about a subject in the given role,
    /// newest-indexed first, with responses joined.
    pub async fn get_by_subject(
        subject_id: &str,
        role: PubkyAppReviewRole,
        pagination: Pagination,
    ) -> ModelResult<Option<Self>> {
        let key_parts = [&REVIEW_SUBJECT_KEY_PARTS[..], &[subject_id, role.as_str()]].concat();
        Self::from_sorted_set(&key_parts, pagination).await
    }

    /// Retrieves a page of buyer reviews about a listing, newest-indexed
    /// first, with responses joined.
    pub async fn get_by_listing(
        listing_owner_id: &str,
        listing_id: &str,
        pagination: Pagination,
    ) -> ModelResult<Option<Self>> {
        let key_parts = [
            &REVIEW_LISTING_KEY_PARTS[..],
            &[listing_owner_id, listing_id],
        ]
        .concat();
        Self::from_sorted_set(&key_parts, pagination).await
    }

    async fn from_sorted_set(
        key_parts: &[&str],
        pagination: Pagination,
    ) -> ModelResult<Option<Self>> {
        let Pagination {
            skip,
            limit,
            start,
            end,
        } = pagination;

        let members = ReviewDetails::try_from_index_sorted_set(
            key_parts,
            start,
            end,
            skip,
            limit,
            SortOrder::Descending,
            None,
        )
        .await?
        .unwrap_or_default();

        if members.is_empty() {
            return Ok(None);
        }

        let review_keys: Vec<(String, String)> = members
            .into_iter()
            .filter_map(|(member, _)| {
                member
                    .split_once(':')
                    .map(|(reviewer, review_id)| (reviewer.to_string(), review_id.to_string()))
            })
            .collect();

        let review_key_slices: Vec<Vec<&str>> = review_keys
            .iter()
            .map(|(reviewer, review_id)| vec![reviewer.as_str(), review_id.as_str()])
            .collect();
        let review_key_refs: Vec<&[&str]> =
            review_key_slices.iter().map(|parts| &parts[..]).collect();
        let reviews: Vec<ReviewDetails> =
            ReviewDetails::try_from_index_multiple_json(&review_key_refs)
                .await?
                .into_iter()
                .flatten()
                .collect();

        if reviews.is_empty() {
            return Ok(None);
        }

        // Responses live under [responder == subject, review_id].
        let response_key_slices: Vec<Vec<&str>> = reviews
            .iter()
            .map(|review| vec![review.subject_id.as_str(), review.review_id.as_str()])
            .collect();
        let response_key_refs: Vec<&[&str]> =
            response_key_slices.iter().map(|parts| &parts[..]).collect();
        let responses =
            ReviewResponseDetails::try_from_index_multiple_json(&response_key_refs).await?;

        let views = reviews
            .into_iter()
            .zip(responses)
            .map(|(review, response)| ReviewView { review, response })
            .collect();

        Ok(Some(Self(views)))
    }
}

/// True when `updated_at` is more than the marketplace edit window after
/// `created_at`. Unparseable timestamps (already spec-validated upstream)
/// degrade to `false` rather than mislabeling the record.
fn is_edited_late(created_at: &str, updated_at: &str) -> bool {
    let (Ok(created), Ok(updated)) = (
        chrono::DateTime::parse_from_rfc3339(created_at),
        chrono::DateTime::parse_from_rfc3339(updated_at),
    ) else {
        return false;
    };
    (updated - created).num_seconds() > REVIEW_EDIT_WINDOW_SECONDS
}
