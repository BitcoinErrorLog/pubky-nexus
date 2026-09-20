use crate::db::kv::RedisResult;
use crate::db::RedisOps;
use crate::models::error::ModelResult;
use chrono::Utc;
use pubky_app_specs::{PubkyAppReviewResponse, PubkyId};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// The indexed details of a review response record.
///
/// Responses live on the **subject's** homeserver (the responder owns their
/// words, symmetrically to the reviewer). The path ID equals the subject
/// review's ID, structurally capping responses at one revisable response per
/// review (ratified D7). Authorization is structural, not cryptographic: the
/// ingest pipeline accepts a response only when its owner equals the subject
/// review's `subjectPubky`.
#[derive(Serialize, Deserialize, ToSchema, Clone, Debug, PartialEq)]
pub struct ReviewResponseDetails {
    /// The subject review's ID (also this record's path ID).
    pub review_id: String,
    /// The responder (record owner; always the review's subject).
    pub responder_id: String,
    /// The reviewer who owns the subject review record.
    pub reviewer_id: String,
    /// Canonical URI of the subject review on the reviewer's homeserver.
    pub review_uri: String,
    pub text: String,
    pub created_at: String,
    pub updated_at: String,
    pub revision: i64,
    pub indexed_at: i64,
}

impl RedisOps for ReviewResponseDetails {}

impl ReviewResponseDetails {
    pub fn from_homeserver(
        response: &PubkyAppReviewResponse,
        responder_id: &PubkyId,
        reviewer_id: &str,
    ) -> Self {
        ReviewResponseDetails {
            review_id: response.review_id.clone(),
            responder_id: responder_id.to_string(),
            reviewer_id: reviewer_id.to_string(),
            review_uri: response.review_uri.clone(),
            text: response.text.clone(),
            created_at: response.created_at.clone(),
            updated_at: response.updated_at.clone(),
            revision: response.revision,
            indexed_at: Utc::now().timestamp_millis(),
        }
    }

    pub async fn get_from_index(
        responder_id: &str,
        review_id: &str,
    ) -> RedisResult<Option<ReviewResponseDetails>> {
        Self::try_from_index_json(&[responder_id, review_id], None).await
    }

    pub async fn put_to_index(&self) -> RedisResult<()> {
        self.put_index_json(&[&self.responder_id, &self.review_id], None, None)
            .await
    }

    pub async fn delete(responder_id: &str, review_id: &str) -> ModelResult<()> {
        Self::remove_from_index_multiple_json(&[&[responder_id, review_id]]).await?;
        Ok(())
    }
}
