use crate::events::retry::event::RetryEvent;
use crate::events::EventProcessorError;
use nexus_common::db::reindex::{get_all_user_ids, get_indexed_review_ids};
use nexus_common::db::{OperationOutcome, PubkyConnector};
use nexus_common::models::homeserver::Homeserver;
use nexus_common::models::marketplace::{ReputationSummary, ReviewDetails};
use nexus_common::models::user::UserDetails;
use nexus_common::types::DynError;
use pubky_app_specs::{
    marketplace_review_uri_builder, PubkyAppMarketplaceReview, PubkyAppObject, PubkyAppReviewRole,
    PubkyId, Resource,
};
use tracing::{debug, info, warn};

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

/// Outcome tally of one review-backfill pass.
#[derive(Debug, Default)]
pub struct ReviewBackfill {
    /// Reviews discovered on a homeserver and newly indexed.
    pub indexed: usize,
    /// Reviews already present in the index (skipped without a fetch).
    pub already_indexed: usize,
    /// Reviews that could not be fetched, parsed, or ingested this pass.
    pub failed: usize,
}

/// Discovers and indexes marketplace reviews published BEFORE the deployed
/// watcher's replay cursor, which the events feed will never deliver.
///
/// Unlike the listing-terms backfill (whose candidates exist as incomplete
/// graph rows), a pre-cursor review has NO index row at all, so candidates
/// are discovered from the canonical source: for every indexed user, the
/// reviewer-owned `/pub/pubky.app/marketplace/v1/reviews/` directory is
/// LISTed on the homeserver (public, unauthenticated), already-indexed ids
/// are skipped, and each remaining record is fetched and run through the
/// normal ingest ([`sync_put`]) — offline attestation verification,
/// reputation recompute, and all.
///
/// Failures never abort the pass: they are tallied and the migration re-run
/// retries them (indexed reviews drop out via the already-indexed skip).
pub async fn backfill_unindexed_reviews() -> Result<ReviewBackfill, DynError> {
    let user_ids = get_all_user_ids().await?;
    let total = user_ids.len();
    info!(
        "Review backfill: scanning the reviews directory of {} indexed user(s)",
        total
    );

    // Bounded concurrency: the scan is network-bound (one homeserver LIST
    // per user, pkarr resolution included), and a sequential pass over
    // thousands of users outlives operator sessions. Sixteen in flight
    // keeps the staging homeserver comfortable and the wall time in
    // minutes.
    const CONCURRENCY: usize = 16;
    let mut summary = ReviewBackfill::default();
    let mut scanned = 0usize;
    let mut tasks = tokio::task::JoinSet::new();
    let mut queue = user_ids.into_iter();

    loop {
        while tasks.len() < CONCURRENCY {
            let Some(user_id) = queue.next() else { break };
            tasks.spawn(async move {
                let result = backfill_reviews_for_user(&user_id).await;
                (user_id, result)
            });
        }
        let Some(joined) = tasks.join_next().await else {
            break;
        };
        scanned += 1;
        if scanned % 250 == 0 {
            info!("Review backfill: scanned {}/{} user(s)", scanned, total);
        }
        match joined {
            Ok((_, Ok(user_summary))) => {
                summary.indexed += user_summary.indexed;
                summary.already_indexed += user_summary.already_indexed;
                summary.failed += user_summary.failed;
            }
            Ok((user_id, Err(e))) => {
                warn!("Review backfill for user {} failed: {:?}", user_id, e);
                summary.failed += 1;
            }
            Err(e) => {
                warn!("Review backfill task panicked: {:?}", e);
                summary.failed += 1;
            }
        }
    }
    Ok(summary)
}

/// One user's backfill pass: LIST their reviews directory
/// (cursor-paginated), skip ids the graph already carries, ingest the rest.
pub async fn backfill_reviews_for_user(user_id: &str) -> Result<ReviewBackfill, DynError> {
    let mut summary = ReviewBackfill::default();
    backfill_user_reviews(user_id, &mut summary).await?;
    Ok(summary)
}

async fn backfill_user_reviews(
    user_id: &str,
    summary: &mut ReviewBackfill,
) -> Result<(), DynError> {
    let indexed: std::collections::HashSet<String> =
        get_indexed_review_ids(user_id).await?.into_iter().collect();

    let pubky = PubkyConnector::get()?;
    let storage = pubky.public_storage();
    let directory = format!("pubky://{user_id}/pub/pubky.app/marketplace/v1/reviews/");

    let mut cursor: Option<String> = None;
    loop {
        let mut request = storage.list(directory.as_str())?.shallow(true).limit(500);
        if let Some(cursor_value) = &cursor {
            request = request.cursor(cursor_value);
        }
        let entries = match request.send().await {
            Ok(entries) => entries,
            // A user with no reviews directory answers 404: nothing to do.
            Err(_) => return Ok(()),
        };
        if entries.is_empty() {
            return Ok(());
        }

        for entry in &entries {
            let url = entry.to_pubky_url();
            let Some(review_id) = url.rsplit('/').next().filter(|id| !id.is_empty()) else {
                continue;
            };
            if indexed.contains(review_id) {
                summary.already_indexed += 1;
                continue;
            }
            match ingest_review_from_homeserver(user_id, review_id).await {
                Ok(()) => summary.indexed += 1,
                Err(e) => {
                    warn!(
                        "Review backfill could not ingest {}/{}: {:?}",
                        user_id, review_id, e
                    );
                    summary.failed += 1;
                }
            }
        }

        if entries.len() < 500 {
            return Ok(());
        }
        cursor = entries.last().map(|entry| entry.to_pubky_url());
    }
}

/// Fetches one review record from the reviewer's homeserver (the canonical
/// source) and runs the normal ingest.
async fn ingest_review_from_homeserver(
    user_id: &str,
    review_id: &str,
) -> Result<(), EventProcessorError> {
    let reviewer = PubkyId::try_from(user_id).map_err(EventProcessorError::generic)?;
    let uri = marketplace_review_uri_builder(user_id.to_string(), review_id.to_string());

    let pubky = PubkyConnector::get()?;
    let response = pubky.public_storage().get(&uri).await?;
    if !response.status().is_success() {
        return Err(EventProcessorError::client_error(format!(
            "Fetch resource failed {uri}: HTTP {}",
            response.status()
        )));
    }
    let blob = response
        .bytes()
        .await
        .map_err(|e| EventProcessorError::client_error(e.to_string()))?;

    let resource = Resource::MarketplaceReview(review_id.to_string());
    let pubky_object =
        PubkyAppObject::from_resource(&resource, &blob).map_err(EventProcessorError::generic)?;
    match pubky_object {
        PubkyAppObject::MarketplaceReview(review) => {
            sync_put(review, reviewer, review_id.to_string()).await
        }
        _ => Err(EventProcessorError::generic(format!(
            "Expected a marketplace review record at {uri}"
        ))),
    }
}
