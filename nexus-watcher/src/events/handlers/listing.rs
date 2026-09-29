use crate::events::retry::event::RetryEvent;
use crate::events::EventProcessorError;
use async_trait::async_trait;
use nexus_common::db::graph::Query;
use nexus_common::db::kv::sets;
use nexus_common::db::reindex::{get_all_listing_ids, get_auction_listings_missing_terms};
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

/// Phase boundaries of [`del`], for deterministic interruption tests.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ListingDelStep {
    /// Nothing removed yet.
    Start,
    /// Redis details and stream memberships removed, graph node still there.
    IndexesRemoved,
    /// Tags and graph node removed, Redis not yet swept a second time.
    GraphDeleted,
}

#[doc(hidden)]
#[async_trait]
pub trait ListingDelHook: Sync {
    async fn at(&self, _step: ListingDelStep) -> Result<(), EventProcessorError> {
        Ok(())
    }

    /// Passed through to the tag cleanup that runs between the two steps.
    fn tag_cleanup(&self) -> Option<&dyn super::tag::TargetTagCleanupHook> {
        None
    }
}

struct NoopListingDelHook;

#[async_trait]
impl ListingDelHook for NoopListingDelHook {}

/// Removes a listing everywhere Nexus keeps it. Every step is idempotent and
/// the graph node goes last, so a DEL that stops anywhere is finished by
/// running it again, and a listing still in the graph is still found by the
/// next prune. The Redis indexes are removed before the tags and the graph
/// node, and swept again after them: a details read between the two
/// refills the cache from the still-present graph row, and a republish
/// indexed meanwhile re-adds its memberships.
pub async fn del(user_id: PubkyId, listing_id: String) -> Result<(), EventProcessorError> {
    del_with_hook(user_id, listing_id, &NoopListingDelHook).await
}

#[doc(hidden)]
pub async fn del_with_hook(
    user_id: PubkyId,
    listing_id: String,
    hook: &dyn ListingDelHook,
) -> Result<(), EventProcessorError> {
    debug!("Deleting listing: {}/{}", user_id, listing_id);

    hook.at(ListingDelStep::Start).await?;
    ListingDetails::delete_indexes(&user_id, &listing_id).await?;
    hook.at(ListingDelStep::IndexesRemoved).await?;

    let target = super::tag::TagTarget::Listing {
        owner_id: &user_id,
        listing_id: &listing_id,
    };
    match hook.tag_cleanup() {
        Some(tag_hook) => super::tag::del_tagged_target_with_hook(target, tag_hook).await?,
        None => super::tag::del_tagged_target(target).await?,
    }
    hook.at(ListingDelStep::GraphDeleted).await?;
    ListingDetails::delete_indexes(&user_id, &listing_id).await?;

    Ok(())
}

/// Whether a listing's canonical record is on its seller's homeserver.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HomeserverRecord {
    Present,
    /// The homeserver answered 404 for the record.
    Gone,
}

/// Asks the seller's homeserver whether the listing record exists. Only a
/// definitive answer counts: a 404 from the homeserver is `Gone`, a success
/// is `Present`, and every other outcome (unresolvable homeserver, rate
/// limit, 5xx, timeout) is an error, so an unreachable homeserver can never
/// read as a deletion. The Pubky client reports a non-2xx status as an
/// error, so the 404 is read from that error.
pub async fn listing_record_on_homeserver(
    owner_id: &str,
    listing_id: &str,
) -> Result<HomeserverRecord, EventProcessorError> {
    let uri = listing_uri_builder(owner_id.to_string(), listing_id.to_string());
    let pubky = PubkyConnector::get()?;
    match pubky.public_storage().get(&uri).await {
        Ok(_) => Ok(HomeserverRecord::Present),
        Err(pubky::Error::Request(pubky::errors::RequestError::Server { status, .. }))
            if status == pubky::StatusCode::NOT_FOUND =>
        {
            Ok(HomeserverRecord::Gone)
        }
        Err(e) => Err(e.into()),
    }
}

/// Homeserver checks running at once. Sellers on a slow or unresolvable
/// homeserver only cost their own timeout instead of holding up the rest.
const HOMESERVER_CHECK_CONCURRENCY: usize = 8;

async fn check_homeservers(
    listings: Vec<(String, String)>,
) -> Vec<(
    String,
    String,
    Result<HomeserverRecord, EventProcessorError>,
)> {
    let mut checked = Vec::with_capacity(listings.len());
    for chunk in listings.chunks(HOMESERVER_CHECK_CONCURRENCY) {
        let mut tasks = tokio::task::JoinSet::new();
        for (index, (owner_id, listing_id)) in chunk.iter().cloned().enumerate() {
            tasks.spawn(async move {
                let result = listing_record_on_homeserver(&owner_id, &listing_id).await;
                (index, result)
            });
        }
        let mut results: Vec<Option<Result<HomeserverRecord, EventProcessorError>>> =
            chunk.iter().map(|_| None).collect();
        while let Some(joined) = tasks.join_next().await {
            if let Ok((index, result)) = joined {
                results[index] = Some(result);
            }
        }
        for ((owner_id, listing_id), result) in chunk.iter().cloned().zip(results) {
            let result = result.unwrap_or_else(|| {
                Err(EventProcessorError::generic(
                    "homeserver check task did not finish",
                ))
            });
            checked.push((owner_id, listing_id, result));
        }
    }
    checked
}

/// Redis set of `owner_id:listing_id` rows a prune has started deleting and
/// not yet confirmed. It is what lets a rerun finish a delete that was
/// interrupted after the graph node was already gone.
const PRUNE_PENDING_PREFIX: &str = "Prune";
const PRUNE_PENDING_KEY: &str = "StaleListings";

fn pending_member(owner_id: &str, listing_id: &str) -> String {
    format!("{owner_id}:{listing_id}")
}

async fn pending_prunes() -> Result<Vec<(String, String)>, DynError> {
    let members = sets::get_range(PRUNE_PENDING_PREFIX, PRUNE_PENDING_KEY, None, Some(100_000))
        .await?
        .unwrap_or_default();
    Ok(members
        .into_iter()
        .filter_map(|member| {
            member
                .split_once(':')
                .map(|(owner, listing)| (owner.to_string(), listing.to_string()))
        })
        .collect())
}

async fn mark_pending(owner_id: &str, listing_id: &str) -> Result<(), DynError> {
    let member = pending_member(owner_id, listing_id);
    sets::put(PRUNE_PENDING_PREFIX, PRUNE_PENDING_KEY, &[&member], None).await?;
    Ok(())
}

async fn clear_pending(owner_id: &str, listing_id: &str) -> Result<(), DynError> {
    let member = pending_member(owner_id, listing_id);
    sets::del(PRUNE_PENDING_PREFIX, PRUNE_PENDING_KEY, &[&member]).await?;
    Ok(())
}

/// Result of one [`prune_stale_listings`] run.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct StaleListingPrune {
    /// Rows checked: every listing in the graph plus every row an earlier
    /// interrupted run left pending.
    pub scanned: usize,
    /// Rows whose record is still on the seller's homeserver.
    pub present: usize,
    /// `(owner_id, listing_id)` of rows the homeserver reports as gone.
    pub stale: Vec<(String, String)>,
    /// Rows an earlier interrupted prune left pending whose record is on the
    /// homeserver again; an apply run re-indexes them.
    pub to_restore: Vec<(String, String)>,
    /// Stale rows deleted and confirmed gone (always 0 in a dry run).
    pub pruned: usize,
    /// Listings re-indexed from their homeserver because the record came
    /// back while, or after, they were being deleted.
    pub restored: usize,
    /// Rows the homeserver could not answer for, or whose delete or restore
    /// failed. They are left in place, still pending when a delete was
    /// started, and a re-run retries them.
    pub failed: usize,
}

/// Finds listing rows whose canonical record is gone from the seller's
/// homeserver (missed DEL events) and, when `apply` is set, removes them
/// through the same path a DEL event takes ([`del`]): tags, graph node,
/// Redis details and stream memberships.
///
/// Each row is checked against its homeserver and only a 404 marks it stale.
/// More than `max_prune` stale rows aborts the run before anything is
/// deleted, so a misbehaving homeserver cannot empty the marketplace index.
///
/// The delete of one row is a recoverable protocol:
/// 1. the file is checked again and the row is recorded as pending in Redis;
/// 2. [`del`] runs (every step idempotent, graph node last);
/// 3. the file is checked once more. If the seller republished meanwhile and
///    the watcher indexed it before the delete finished, the record is read
///    back and re-indexed; otherwise the row is confirmed gone.
///
/// The pending record is removed only after step 3, and the next run also
/// works through pending rows even when their graph node is already gone, so
/// an interruption at any point is repaired by running the command again.
/// Tags a republished listing had before the delete are not restored.
pub async fn prune_stale_listings(
    apply: bool,
    max_prune: usize,
) -> Result<StaleListingPrune, DynError> {
    let mut listings = get_all_listing_ids().await?;
    for row in pending_prunes().await? {
        if !listings.contains(&row) {
            listings.push(row);
        }
    }
    prune_stale_listings_among(listings, apply, max_prune).await
}

/// [`prune_stale_listings`] over an explicit set of `(owner_id, listing_id)`
/// rows instead of every listing in the graph.
#[doc(hidden)]
pub async fn prune_stale_listings_among(
    listings: Vec<(String, String)>,
    apply: bool,
    max_prune: usize,
) -> Result<StaleListingPrune, DynError> {
    prune_stale_listings_among_with_hook(listings, apply, max_prune, &NoopStalePruneHook).await
}

/// Deterministic seams for testing the windows between the steps of one
/// row's delete. An `Err` from a hook stops the whole run as a killed
/// process would.
#[doc(hidden)]
#[async_trait]
pub trait StalePruneHook: Sync {
    /// After the scan marked the row stale, before the recheck.
    async fn before_recheck(&self, _owner_id: &str, _listing_id: &str) -> Result<(), DynError> {
        Ok(())
    }

    /// After the recheck said gone and the row was recorded as pending,
    /// before the delete starts.
    async fn after_recheck(&self, _owner_id: &str, _listing_id: &str) -> Result<(), DynError> {
        Ok(())
    }

    /// Between the phases of the listing DEL.
    async fn del_step(&self, _step: ListingDelStep) -> Result<(), EventProcessorError> {
        Ok(())
    }

    /// Inside the tag cleanup of the listing DEL.
    fn tag_cleanup(&self) -> Option<&dyn super::tag::TargetTagCleanupHook> {
        None
    }

    /// After the delete finished, before the post-delete check.
    async fn after_delete(&self, _owner_id: &str, _listing_id: &str) -> Result<(), DynError> {
        Ok(())
    }
}

struct NoopStalePruneHook;

#[async_trait]
impl StalePruneHook for NoopStalePruneHook {}

struct PruneDelHook<'a>(&'a dyn StalePruneHook);

#[async_trait]
impl ListingDelHook for PruneDelHook<'_> {
    async fn at(&self, step: ListingDelStep) -> Result<(), EventProcessorError> {
        self.0.del_step(step).await
    }

    fn tag_cleanup(&self) -> Option<&dyn super::tag::TargetTagCleanupHook> {
        self.0.tag_cleanup()
    }
}

/// Reads the record back from the seller's homeserver and indexes it again.
/// The graph row may already exist (a republish indexed it before the delete
/// finished), and then the ingest treats the write as an edit and leaves the
/// stream memberships alone, so the memberships are written again from the
/// graph row; adding them is idempotent.
async fn restore_listing(owner_id: &str, listing_id: &str) -> Result<(), String> {
    match reindex_from_homeserver(owner_id, listing_id).await {
        Ok(true) => {}
        Ok(false) => return Err("record is gone again".to_string()),
        Err(e) => return Err(format!("{e:?}")),
    }
    let details = ListingDetails::get_from_graph(owner_id, listing_id)
        .await
        .map_err(|e| format!("{e:?}"))?
        .ok_or_else(|| "listing is not in the graph after the re-index".to_string())?;
    details
        .put_to_index(false)
        .await
        .map_err(|e| format!("{e:?}"))
}

/// [`prune_stale_listings_among`] with the interleaving seams. Production
/// callers use [`prune_stale_listings`].
#[doc(hidden)]
pub async fn prune_stale_listings_among_with_hook(
    listings: Vec<(String, String)>,
    apply: bool,
    max_prune: usize,
    hook: &dyn StalePruneHook,
) -> Result<StaleListingPrune, DynError> {
    let pending = pending_prunes().await?;
    let mut summary = StaleListingPrune {
        scanned: listings.len(),
        ..Default::default()
    };
    info!(
        "Checking {} listing row(s) against their homeservers",
        listings.len()
    );

    for (owner_id, listing_id, checked) in check_homeservers(listings).await {
        match checked {
            Ok(HomeserverRecord::Present) => {
                summary.present += 1;
                if pending.contains(&(owner_id.clone(), listing_id.clone())) {
                    summary.to_restore.push((owner_id, listing_id));
                }
            }
            Ok(HomeserverRecord::Gone) => summary.stale.push((owner_id, listing_id)),
            Err(e) => {
                warn!(
                    "Could not check listing {}/{} on its homeserver, leaving it: {:?}",
                    owner_id, listing_id, e
                );
                summary.failed += 1;
            }
        }
    }

    if summary.stale.len() > max_prune {
        return Err(format!(
            "{} stale listing(s) found, more than the limit of {max_prune}; nothing was deleted. Check the homeservers, then re-run with a higher limit",
            summary.stale.len()
        )
        .into());
    }

    for (owner_id, listing_id) in &summary.stale {
        let title = ListingDetails::get_from_graph(owner_id, listing_id)
            .await?
            .map(|details| details.title)
            .unwrap_or_default();
        info!(
            "Stale listing {}/{} ({:?}): gone from its homeserver",
            owner_id, listing_id, title
        );
    }
    if !apply {
        return Ok(summary);
    }

    for (owner_id, listing_id) in summary.to_restore.clone() {
        match restore_listing(&owner_id, &listing_id).await {
            Ok(()) => {
                clear_pending(&owner_id, &listing_id).await?;
                info!(
                    "Restored listing {}/{}: its record is back after an interrupted prune",
                    owner_id, listing_id
                );
                summary.restored += 1;
            }
            Err(e) => {
                warn!(
                    "Could not restore listing {}/{}, leaving it pending: {}",
                    owner_id, listing_id, e
                );
                summary.failed += 1;
            }
        }
    }

    for (owner_id, listing_id) in summary.stale.clone() {
        hook.before_recheck(&owner_id, &listing_id).await?;
        match listing_record_on_homeserver(&owner_id, &listing_id).await {
            Ok(HomeserverRecord::Gone) => {}
            Ok(HomeserverRecord::Present) => {
                warn!(
                    "Listing {}/{} is back on its homeserver; keeping it",
                    owner_id, listing_id
                );
                summary
                    .stale
                    .retain(|(o, l)| !(o == &owner_id && l == &listing_id));
                summary.present += 1;
                continue;
            }
            Err(e) => {
                warn!(
                    "Could not re-check listing {}/{}, leaving it: {:?}",
                    owner_id, listing_id, e
                );
                summary.failed += 1;
                continue;
            }
        }

        mark_pending(&owner_id, &listing_id).await?;
        hook.after_recheck(&owner_id, &listing_id).await?;

        let deleted = match PubkyId::try_from(owner_id.as_str()) {
            Ok(user_id) => del_with_hook(user_id, listing_id.clone(), &PruneDelHook(hook))
                .await
                .map_err(|e| e.to_string()),
            Err(e) => Err(e.to_string()),
        };
        if let Err(e) = deleted {
            warn!(
                "Failed to prune stale listing {}/{}, left pending for the next run: {}",
                owner_id, listing_id, e
            );
            summary.failed += 1;
            continue;
        }
        hook.after_delete(&owner_id, &listing_id).await?;

        match listing_record_on_homeserver(&owner_id, &listing_id).await {
            Ok(HomeserverRecord::Gone) => {
                clear_pending(&owner_id, &listing_id).await?;
                info!("Pruned stale listing {}/{}", owner_id, listing_id);
                summary.pruned += 1;
            }
            Ok(HomeserverRecord::Present) => match restore_listing(&owner_id, &listing_id).await {
                Ok(()) => {
                    clear_pending(&owner_id, &listing_id).await?;
                    warn!(
                        "Listing {}/{} was republished during its delete; re-indexed it",
                        owner_id, listing_id
                    );
                    summary
                        .stale
                        .retain(|(o, l)| !(o == &owner_id && l == &listing_id));
                    summary.restored += 1;
                }
                Err(e) => {
                    warn!(
                        "Listing {}/{} was republished during its delete and could not be re-indexed, left pending: {}",
                        owner_id, listing_id, e
                    );
                    summary.failed += 1;
                }
            },
            Err(e) => {
                warn!(
                    "Could not confirm the delete of {}/{}, left pending: {:?}",
                    owner_id, listing_id, e
                );
                summary.failed += 1;
            }
        }
    }
    Ok(summary)
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
    /// their details keys were already removed by the upfront cache eviction.
    pub disappeared: usize,
}

/// Internal operation object exposed only to support deterministic integration
/// tests of scrub/event-pipeline interleavings.
#[doc(hidden)]
pub struct ListingReserveScrubOps;

impl ListingReserveScrubOps {
    /// Re-publishes a listing through the production graph-then-cache path.
    pub async fn sync_put(
        &self,
        listing: PubkyAppListing,
        user_id: PubkyId,
        listing_id: String,
    ) -> Result<(), EventProcessorError> {
        sync_put(listing, user_id, listing_id).await
    }
}

/// Internal deterministic seam for integration-testing scrub interleavings.
#[doc(hidden)]
#[async_trait]
pub trait ListingReserveScrubHook: Sync {
    async fn after_cache_eviction(
        &self,
        _listing_ids: &[(String, String)],
    ) -> Result<(), DynError> {
        Ok(())
    }

    async fn after_missing_re_read(
        &self,
        _ops: &ListingReserveScrubOps,
        _owner_id: &str,
        _listing_id: &str,
    ) -> Result<(), DynError> {
        Ok(())
    }
}

struct NoopListingReserveScrubHook;

#[async_trait]
impl ListingReserveScrubHook for NoopListingReserveScrubHook {}

pub async fn scrub_legacy_listing_reserves() -> Result<ListingReserveScrub, DynError> {
    scrub_legacy_listing_reserves_with_hook(&NoopListingReserveScrubHook).await
}

/// Runs the reserve scrub with deterministic integration-test interleaving
/// points. Production callers use [`scrub_legacy_listing_reserves`].
#[doc(hidden)]
pub async fn scrub_legacy_listing_reserves_with_hook(
    hook: &dyn ListingReserveScrubHook,
) -> Result<ListingReserveScrub, DynError> {
    let rows = fetch_all_rows_from_graph(Query::new(
        "scrub_legacy_listing_reserves",
        "MATCH (listing:Listing)
         REMOVE listing.auction_reserve_price_minor
         RETURN listing.owner_id AS owner_id, listing.id AS listing_id",
    ))
    .await?;

    let mut listing_ids = Vec::with_capacity(rows.len());
    for row in rows {
        let owner_id: Option<String> = row.get("owner_id")?;
        let listing_id: Option<String> = row.get("listing_id")?;
        let (Some(owner_id), Some(listing_id)) = (owner_id, listing_id) else {
            return Err(
                "Listing row is missing owner_id or listing_id during reserve scrub".into(),
            );
        };
        listing_ids.push((owner_id, listing_id));
    }

    let mut summary = ListingReserveScrub {
        scanned: listing_ids.len(),
        ..Default::default()
    };

    // Evict every captured details key before any graph re-read. A concurrent
    // reserve-free republish before this point may be evicted, but its surviving
    // graph row is rewritten below. A republish after this point is never
    // followed by a scrub delete.
    if !listing_ids.is_empty() {
        let key_parts: Vec<[&str; 2]> = listing_ids
            .iter()
            .map(|(owner_id, listing_id)| [owner_id.as_str(), listing_id.as_str()])
            .collect();
        let keys: Vec<&[&str]> = key_parts.iter().map(|parts| parts.as_slice()).collect();
        ListingDetails::remove_from_index_multiple_json(&keys).await?;
    }
    hook.after_cache_eviction(&listing_ids).await?;

    let ops = ListingReserveScrubOps;
    for (owner_id, listing_id) in listing_ids {
        let Some(details) = ListingDetails::get_from_graph(&owner_id, &listing_id).await? else {
            hook.after_missing_re_read(&ops, &owner_id, &listing_id)
                .await?;
            warn!(
                "Listing {}/{} disappeared after the reserve scrub cache eviction; no later cache delete will run",
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
