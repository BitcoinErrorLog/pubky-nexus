use crate::events::retry::event::RetryEvent;
use crate::events::EventProcessorError;
use nexus_common::db::graph::Query;
use nexus_common::db::reindex::get_auction_listings_missing_terms;
use nexus_common::db::{fetch_all_rows_from_graph, OperationOutcome, PubkyConnector, RedisOps};
use nexus_common::models::marketplace::ListingDetails;
use nexus_common::types::DynError;
use pubky_app_specs::{listing_uri_builder, PubkyAppListing, PubkyAppObject, PubkyId, Resource};
use serde_json::Value;
use tracing::{debug, info, warn};

const FORBIDDEN_PUBLIC_RESERVE_KEYS: [&str; 5] = [
    "auction_reserve_price_minor",
    "reservePrice",
    "reserve_price",
    "reserveMet",
    "reserve_met",
];

/// Rejects public listing documents that contain private reserve information.
///
/// The app-spec parser is intentionally open to older record shapes, so this
/// check runs on the raw JSON before parsing and recursively covers extension
/// objects and arrays. A rejected document never reaches graph or Redis writes.
pub fn validate_public_listing_blob(blob: &[u8]) -> Result<(), EventProcessorError> {
    let value: Value = serde_json::from_slice(blob).map_err(EventProcessorError::generic)?;
    reject_public_reserve_keys(&value)
}

fn reject_public_reserve_keys(value: &Value) -> Result<(), EventProcessorError> {
    match value {
        Value::Object(object) => {
            for (key, child) in object {
                if FORBIDDEN_PUBLIC_RESERVE_KEYS.contains(&key.as_str()) {
                    return Err(EventProcessorError::InvalidEventLine(format!(
                        "Public marketplace listing contains forbidden field {key}"
                    )));
                }
                reject_public_reserve_keys(child)?;
            }
        }
        Value::Array(array) => {
            for child in array {
                reject_public_reserve_keys(child)?;
            }
        }
        _ => {}
    }
    Ok(())
}

pub(crate) async fn sync_put(
    listing: PubkyAppListing,
    user_id: PubkyId,
    listing_id: String,
) -> Result<(), EventProcessorError> {
    debug!("Indexing new listing: {}/{}", user_id, listing_id);

    // Create ListingDetails object
    let listing_details = ListingDetails::from_homeserver(listing, &user_id, &listing_id);

    // SAVE TO GRAPH: only if the seller user exists
    let existed = match listing_details.put_to_graph().await? {
        OperationOutcome::CreatedOrDeleted => false,
        OperationOutcome::Updated => true,
        OperationOutcome::MissingDependency => {
            let key = RetryEvent::generate_index_key_from_uri(&user_id.to_uri());
            return Err(EventProcessorError::missing_dependencies(vec![key]));
        }
    };

    // SAVE TO INDEX: on an edit only the details JSON is refreshed, the listing
    // keeps its original position in the stream sorted sets
    listing_details.put_to_index(existed).await?;

    Ok(())
}

pub async fn del(user_id: PubkyId, listing_id: String) -> Result<(), EventProcessorError> {
    debug!("Deleting listing: {}/{}", user_id, listing_id);

    ListingDetails::delete(&user_id, &listing_id).await?;

    Ok(())
}

/// Outcome counts of one [`backfill_missing_auction_terms`] run.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct AuctionTermsBackfill {
    /// Listings re-read from their homeserver and upserted with full details.
    pub reindexed: usize,
    /// Listings whose canonical record no longer exists on the homeserver;
    /// left untouched because removals belong to the DEL event pipeline.
    pub gone: usize,
    /// Listings that could not be reindexed (fetch or indexing error). The
    /// backfill keeps going and reports them so a re-run can retry; a failed
    /// listing still lacks its terms and stays a candidate.
    pub failed: usize,
}

/// One-shot backfill for auction listings indexed before the index carried
/// the auction term fields: finds every auction row without terms in the
/// graph and reindexes each one from its seller's homeserver via
/// [`reindex_from_homeserver`]. Idempotent — reindexed listings gain their
/// terms and drop out of the candidate query, so a re-run only retries the
/// ones that failed.
pub async fn backfill_missing_auction_terms() -> Result<AuctionTermsBackfill, DynError> {
    let candidates = get_auction_listings_missing_terms().await?;
    info!(
        "Backfilling auction terms for {} listing(s) indexed without them",
        candidates.len()
    );

    let mut summary = AuctionTermsBackfill::default();
    for (owner_id, listing_id) in candidates {
        match reindex_from_homeserver(&owner_id, &listing_id).await {
            Ok(true) => summary.reindexed += 1,
            Ok(false) => {
                warn!(
                    "Listing {}/{} is no longer on its homeserver; leaving the index row to the DEL pipeline",
                    owner_id, listing_id
                );
                summary.gone += 1;
            }
            Err(e) => {
                warn!(
                    "Failed to reindex listing {}/{} from its homeserver: {:?}",
                    owner_id, listing_id, e
                );
                summary.failed += 1;
            }
        }
    }
    Ok(summary)
}

/// Removes reserve data written by older Nexus versions from graph and Redis.
///
/// The graph mutation selects every listing so rerunning this scrub also
/// rewrites every listing details cache entry with the reserve-free model.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct ListingReserveScrub {
    /// Listing rows returned after the graph-wide reserve-property removal.
    pub scanned: usize,
    /// Listing details successfully rewritten in Redis.
    pub rewritten: usize,
    /// Listings deleted after the graph mutation returned their identifiers;
    /// their stale Redis details JSON entries were removed.
    pub disappeared: usize,
}

pub async fn scrub_legacy_listing_reserves() -> Result<ListingReserveScrub, DynError> {
    let rows = fetch_all_rows_from_graph(Query::new(
        "scrub_legacy_listing_reserves",
        "MATCH (listing:Listing)
         REMOVE listing.auction_reserve_price_minor
         RETURN listing.owner_id AS owner_id, listing.id AS listing_id",
    ))
    .await?;

    let mut summary = ListingReserveScrub {
        scanned: rows.len(),
        ..Default::default()
    };
    for row in rows {
        let owner_id: Option<String> = row.get("owner_id")?;
        let listing_id: Option<String> = row.get("listing_id")?;
        let (Some(owner_id), Some(listing_id)) = (owner_id, listing_id) else {
            return Err(
                "Listing row is missing owner_id or listing_id during reserve scrub".into(),
            );
        };
        let Some(details) = ListingDetails::get_from_graph(&owner_id, &listing_id).await? else {
            ListingDetails::remove_from_index_multiple_json(&[&[
                owner_id.as_str(),
                listing_id.as_str(),
            ]])
            .await?;
            warn!(
                "Listing {}/{} disappeared during reserve scrub; removed stale Redis details JSON",
                owner_id, listing_id
            );
            summary.disappeared += 1;
            continue;
        };
        details.put_to_index(true).await?;
        summary.rewritten += 1;
    }
    Ok(summary)
}

/// Re-reads the canonical listing record from the seller's homeserver
/// (the homeserver stays canonical for marketplace records) and re-runs the
/// normal ingest ([`sync_put`]), upserting the full [`ListingDetails`] —
/// including fields added to the index after the row was first written.
/// Returns `false` without touching the index when the record no longer
/// exists on the homeserver.
pub async fn reindex_from_homeserver(
    owner_id: &str,
    listing_id: &str,
) -> Result<bool, EventProcessorError> {
    let user_id = PubkyId::try_from(owner_id).map_err(EventProcessorError::generic)?;
    let uri = listing_uri_builder(owner_id.to_string(), listing_id.to_string());

    let pubky = PubkyConnector::get()?;
    let response = pubky.public_storage().get(&uri).await?;

    if response.status().as_u16() == 404 {
        return Ok(false);
    }
    if !response.status().is_success() {
        let status = response.status();
        let body = response
            .text()
            .await
            .unwrap_or_else(|_| "<unable to read body>".to_string());
        return Err(EventProcessorError::client_error(format!(
            "Fetch resource failed {uri}: HTTP {status} - {body}"
        )));
    }

    let blob = response
        .bytes()
        .await
        .map_err(|e| EventProcessorError::client_error(e.to_string()))?;
    validate_public_listing_blob(&blob)?;
    let resource = Resource::Listing(listing_id.to_string());
    let pubky_object =
        PubkyAppObject::from_resource(&resource, &blob).map_err(EventProcessorError::generic)?;

    match pubky_object {
        PubkyAppObject::Listing(listing) => {
            sync_put(*listing, user_id, listing_id.to_string()).await?;
            Ok(true)
        }
        _ => Err(EventProcessorError::generic(format!(
            "Expected a listing record at {uri}"
        ))),
    }
}
