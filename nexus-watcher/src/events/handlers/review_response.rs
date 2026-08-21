use crate::events::handlers::review::recompute_reputation;
use crate::events::retry::event::RetryEvent;
use crate::events::EventProcessorError;
use nexus_common::db::{exec_single_row, queries};
use nexus_common::models::marketplace::{ReviewDetails, ReviewResponseDetails};
use pubky_app_specs::{ParsedUri, PubkyAppReviewResponse, PubkyId};
use tracing::debug;

/// Indexes a review response record published on the subject's homeserver.
///
/// Authorization is structural, not cryptographic (ratified D7): the
/// response is accepted only when its owner equals the subject review's
/// `subjectPubky`. An impostor's response is shape-valid but fails that
/// check and is rejected without any signature machinery. The subject
/// review must already be indexed; until it is, the event parks in the
/// missing-dependency retry queue keyed by the review.
pub async fn sync_put(
    response: PubkyAppReviewResponse,
    user_id: PubkyId,
    review_id: String,
) -> Result<(), EventProcessorError> {
    debug!("Indexing new review response: {}/{}", user_id, review_id);

    if response.owner_pubky != user_id.as_ref() {
        return Err(EventProcessorError::generic(format!(
            "Response record owner {} does not match the event homeserver user {}",
            response.owner_pubky, user_id
        )));
    }

    // The reviewer (and their homeserver) comes from the response's
    // canonical review URI, already spec-validated to name this review ID.
    let review_uri =
        ParsedUri::try_from(response.review_uri.as_str()).map_err(EventProcessorError::generic)?;
    let reviewer_id = review_uri.user_id.clone();

    let Some(review) = ReviewDetails::get_from_index(&reviewer_id, &review_id).await? else {
        let key = RetryEvent::generate_index_key_from_uri(&review_uri);
        return Err(EventProcessorError::missing_dependencies(vec![key]));
    };

    // Structural authorization: only the review's subject may respond.
    if review.subject_id != user_id.as_ref() {
        return Err(EventProcessorError::generic(format!(
            "Response owner {} is not the subject {} of review {}",
            user_id, review.subject_id, review_id
        )));
    }

    let details = ReviewResponseDetails::from_homeserver(&response, &user_id, &review.reviewer_id);
    details.put_to_index().await?;

    // Feed the aggregate response count through the review edge flag.
    exec_single_row(queries::put::set_review_response_flag(
        &review.reviewer_id,
        &review_id,
        true,
    ))
    .await?;
    recompute_reputation(&review).await?;

    Ok(())
}

pub async fn del(user_id: PubkyId, review_id: String) -> Result<(), EventProcessorError> {
    debug!("Deleting review response: {}/{}", user_id, review_id);

    let Some(details) = ReviewResponseDetails::get_from_index(&user_id, &review_id).await? else {
        return Err(EventProcessorError::SkipIndexing);
    };

    ReviewResponseDetails::delete(&user_id, &review_id).await?;

    // The subject review may already be gone (its DEL recomputed the
    // aggregates); only touch the edge and aggregates when it still exists.
    if let Some(review) = ReviewDetails::get_from_index(&details.reviewer_id, &review_id).await? {
        exec_single_row(queries::put::set_review_response_flag(
            &review.reviewer_id,
            &review_id,
            false,
        ))
        .await?;
        recompute_reputation(&review).await?;
    }

    Ok(())
}
