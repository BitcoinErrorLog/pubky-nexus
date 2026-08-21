use crate::events::retry::event::RetryEvent;
use crate::events::EventProcessorError;
use nexus_common::db::OperationOutcome;
use nexus_common::models::homeserver::Homeserver;
use nexus_common::models::marketplace::{ReputationSummary, ReviewDetails};
use nexus_common::models::user::UserDetails;
use pubky_app_specs::{PubkyAppMarketplaceReview, PubkyAppReviewRole, PubkyId};
use tracing::debug;

/// Indexes a marketplace review record published on the reviewer's
/// homeserver.
///
/// The embedded purchase attestation is verified offline at ingest (parse →
/// Ed25519 signature against the self-certifying `iss` pubky → binding to
/// this exact review). Verification failure never rejects the record: the
/// review is indexed as unverified and labeled (ratified D5). WHO counts as
/// a trusted attestor is client policy — the index stores each verified
/// review's attestor pubky and lets consumers decide display.
pub async fn sync_put(
    review: PubkyAppMarketplaceReview,
    user_id: PubkyId,
    review_id: String,
) -> Result<(), EventProcessorError> {
    debug!("Indexing new marketplace review: {}/{}", user_id, review_id);

    // The record owner must be the homeserver user the event came from;
    // a record claiming another reviewer's identity is rejected outright.
    if review.owner_pubky != user_id.as_ref() {
        return Err(EventProcessorError::generic(format!(
            "Review record owner {} does not match the event homeserver user {}",
            review.owner_pubky, user_id
        )));
    }

    let details = ReviewDetails::from_homeserver(&review, &user_id);

    // SAVE TO GRAPH: requires both the reviewer and the subject users
    let existed = match details.put_to_graph().await? {
        OperationOutcome::CreatedOrDeleted => false,
        OperationOutcome::Updated => true,
        OperationOutcome::MissingDependency => {
            return Err(missing_user_dependencies(&details).await);
        }
    };

    // SAVE TO INDEX: an edit refreshes the details JSON only; the review
    // keeps its original position in the review sorted sets
    details.put_to_index(existed).await?;

    // AGGREGATES: recompute the affected scopes from the graph
    recompute_reputation(&details).await?;

    Ok(())
}

pub async fn del(user_id: PubkyId, review_id: String) -> Result<(), EventProcessorError> {
    debug!("Deleting marketplace review: {}/{}", user_id, review_id);

    let Some(details) = ReviewDetails::get_from_index(&user_id, &review_id).await? else {
        return Err(EventProcessorError::SkipIndexing);
    };

    details.delete().await?;
    recompute_reputation(&details).await?;

    Ok(())
}

/// Recomputes the subject-scoped aggregate and, for buyer reviews, the
/// listing-scoped aggregate of the review's listing.
pub async fn recompute_reputation(details: &ReviewDetails) -> Result<(), EventProcessorError> {
    ReputationSummary::recompute_subject(&details.subject_id, details.role).await?;
    if details.role == PubkyAppReviewRole::BuyerReviewingSeller {
        ReputationSummary::recompute_listing(&details.listing_owner_id, &details.listing_id)
            .await?;
    }
    Ok(())
}

/// Builds the missing-dependency error for a review whose graph write found
/// no user nodes: reports exactly the users that are absent and nudges the
/// homeserver ingest for the subject (who may live on an unmonitored
/// homeserver, mirroring the tag pipeline).
async fn missing_user_dependencies(details: &ReviewDetails) -> EventProcessorError {
    let mut dependency = Vec::new();
    for candidate in [&details.reviewer_id, &details.subject_id] {
        match UserDetails::get_by_id(candidate).await {
            Ok(Some(_)) => {}
            _ => {
                if candidate == &details.subject_id {
                    if let Err(e) = Homeserver::maybe_ingest_for_user(candidate).await {
                        tracing::error!("Failed to ingest homeserver: {e}");
                    }
                }
                if let Ok(user_id) = PubkyId::try_from(candidate.as_str()) {
                    dependency.push(RetryEvent::generate_index_key_from_uri(&user_id.to_uri()));
                }
            }
        }
    }
    if dependency.is_empty() {
        // The graph reported a missing node but both users resolve now:
        // a benign race; retry against the reviewer to re-run the event.
        if let Ok(user_id) = PubkyId::try_from(details.reviewer_id.as_str()) {
            dependency.push(RetryEvent::generate_index_key_from_uri(&user_id.to_uri()));
        }
    }
    EventProcessorError::missing_dependencies(dependency)
}
