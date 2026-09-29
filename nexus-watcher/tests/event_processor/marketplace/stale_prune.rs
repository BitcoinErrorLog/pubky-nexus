use super::tag_cleanup::{listing_is_untagged, suggested, tag, user};
use super::utils::{test_auction_listing, test_listing};
use crate::event_processor::users::utils::find_user_counts;
use crate::event_processor::utils::watcher::WatcherTest;
use anyhow::Result;
use async_trait::async_trait;
use nexus_common::db::kv::{sets, SortOrder};
use nexus_common::db::{PubkyConnector, RedisOps};
use nexus_common::models::event::EventProcessorError;
use nexus_common::models::marketplace::{
    ListingDetails, ListingStream, ListingStreamFilters, ListingStreamSorting, ListingsByTagSearch,
    LISTING_AUCTION_ENDS_KEY_PARTS,
};
use nexus_common::models::tag::listing::TagListing;
use nexus_common::models::tag::traits::TaggersCollection;
use nexus_common::types::{DynError, Pagination};
use nexus_watcher::events::handlers::listing::{
    del_with_hook, prune_stale_listings, prune_stale_listings_among,
    prune_stale_listings_among_with_hook, reindex_from_homeserver, ListingDelHook, ListingDelStep,
    StalePruneHook,
};
use nexus_watcher::events::handlers::tag::{TargetTagCleanupHook, TargetTagCleanupStep};
use pubky::{Keypair, ResourcePath};
use pubky_app_specs::{
    listing_uri_builder, user_uri_builder, PubkyAppListing, PubkyAppListingCondition, PubkyId,
};
use std::sync::atomic::{AtomicBool, Ordering};

const STARTS_AT: &str = "2025-02-01T00:00:00Z";
const ENDS_AT: &str = "2025-02-08T00:00:00Z";

fn seller_filters(seller_id: &str) -> ListingStreamFilters {
    ListingStreamFilters {
        seller_id: Some(seller_id.to_string()),
        ..Default::default()
    }
}

async fn seller_stream_ids(seller_id: &str) -> Vec<String> {
    ListingStream::get_listings(
        seller_filters(seller_id),
        Pagination::default(),
        SortOrder::Descending,
        ListingStreamSorting::Timeline,
    )
    .await
    .unwrap()
    .map(|stream| stream.0.into_iter().map(|entry| entry.id.clone()).collect())
    .unwrap_or_default()
}

/// Reads whether the row exists in the graph and in the Redis index, and
/// fails when the two stores disagree.
async fn listing_is_indexed(seller_id: &str, listing_id: &str) -> bool {
    let in_graph = ListingDetails::get_from_graph(seller_id, listing_id)
        .await
        .unwrap()
        .is_some();
    let in_index = ListingDetails::get_from_index(seller_id, listing_id)
        .await
        .unwrap()
        .is_some();
    assert_eq!(in_graph, in_index, "graph and index must agree");
    in_graph
}

async fn by_tag_timeline_has(label: &str, seller_id: &str, listing_id: &str) -> bool {
    let key = format!("{seller_id}:{listing_id}");
    ListingsByTagSearch::get_by_label(label, Pagination::default())
        .await
        .unwrap()
        .unwrap_or_default()
        .iter()
        .any(|entry| entry.listing_key == key)
}

async fn in_auction_set(seller_id: &str, listing_id: &str) -> bool {
    let ends_ms = chrono::DateTime::parse_from_rfc3339(ENDS_AT)
        .unwrap()
        .timestamp_millis() as f64;
    let member = format!("{seller_id}:{listing_id}");
    ListingStream::try_from_index_sorted_set(
        &LISTING_AUCTION_ENDS_KEY_PARTS,
        Some(ends_ms),
        Some(ends_ms),
        None,
        None,
        SortOrder::Descending,
        None,
    )
    .await
    .unwrap()
    .unwrap_or_default()
    .iter()
    .any(|(key, _)| key == &member)
}

async fn is_pending(seller_id: &str, listing_id: &str) -> bool {
    let member = format!("{seller_id}:{listing_id}");
    sets::get_range("Prune", "StaleListings", None, Some(10_000))
        .await
        .unwrap()
        .unwrap_or_default()
        .contains(&member)
}

/// Deletes the record on the homeserver without letting the watcher process
/// the DEL event: the row Nexus keeps is now stale.
async fn delete_record_silently(kp: &Keypair, path: &ResourcePath) -> Result<()> {
    let pubky = PubkyConnector::get()?;
    let session = pubky.signer(kp.clone()).signin().await?;
    session.storage().delete(path).await?;
    Ok(())
}

async fn publish_record(
    kp: &Keypair,
    path: &ResourcePath,
    listing: &PubkyAppListing,
) -> Result<()> {
    let pubky = PubkyConnector::get()?;
    let session = pubky.signer(kp.clone()).signin().await?;
    session
        .storage()
        .put(path, serde_json::to_string(listing)?)
        .await?;
    Ok(())
}

/// A tagged auction listing whose record was then deleted on the homeserver
/// behind Nexus's back.
struct StaleListing {
    seller_kp: Keypair,
    seller_id: String,
    tagger_kp: Keypair,
    tagger_id: String,
    listing: PubkyAppListing,
    listing_id: String,
    path: ResourcePath,
    labels: [String; 2],
}

async fn stale_listing(test: &mut WatcherTest, name: &str, round: usize) -> Result<StaleListing> {
    let (seller_kp, seller_id) = user(test, &format!("{name}:Seller")).await?;
    let (tagger_kp, tagger_id) = user(test, &format!("{name}:Tagger")).await?;
    let mut listing = test_auction_listing(
        &seller_id,
        "Stale auction",
        "collectibles",
        STARTS_AT,
        ENDS_AT,
    );
    let (listing_id, path) = test.create_listing(&seller_kp, &listing).await?;
    listing.listing_id = listing_id.clone();
    let labels = [
        format!("ma{round}{}", &seller_id[..6]),
        format!("mb{round}{}", &seller_id[..6]),
    ];
    let uri = listing_uri_builder(seller_id.clone(), listing_id.clone());
    for label in &labels {
        tag(test, &tagger_kp, uri.clone(), label).await?;
    }
    assert!(listing_is_indexed(&seller_id, &listing_id).await);
    assert!(in_auction_set(&seller_id, &listing_id).await);
    assert_eq!(find_user_counts(&tagger_id).await.tagged, 2);

    delete_record_silently(&seller_kp, &path).await?;
    Ok(StaleListing {
        seller_kp,
        seller_id,
        tagger_kp,
        tagger_id,
        listing,
        listing_id,
        path,
        labels,
    })
}

impl StaleListing {
    fn row(&self) -> (String, String) {
        (self.seller_id.clone(), self.listing_id.clone())
    }

    async fn assert_fully_removed(&self, context: &str) -> Result<()> {
        assert!(
            !listing_is_indexed(&self.seller_id, &self.listing_id).await,
            "{context}: graph node and Redis details"
        );
        assert!(
            seller_stream_ids(&self.seller_id).await.is_empty(),
            "{context}: timeline and per-seller sets"
        );
        assert!(
            !in_auction_set(&self.seller_id, &self.listing_id).await,
            "{context}: auction end-time set"
        );
        let labels: Vec<&str> = self.labels.iter().map(String::as_str).collect();
        listing_is_untagged(&self.seller_id, &self.listing_id, &labels).await?;
        for label in &labels {
            assert!(!suggested(label).await?, "{context}: autocomplete {label}");
        }
        assert_eq!(
            find_user_counts(&self.tagger_id).await.tagged,
            0,
            "{context}: tagger counts"
        );
        assert!(
            !is_pending(&self.seller_id, &self.listing_id).await,
            "{context}: pending record"
        );
        Ok(())
    }

    async fn cleanup(self, test: &mut WatcherTest) -> Result<()> {
        test.cleanup_user(&self.tagger_kp).await?;
        test.cleanup_user(&self.seller_kp).await?;
        Ok(())
    }
}

#[tokio_shared_rt::test(shared)]
async fn prune_removes_only_stale_listings_and_cleans_every_index() -> Result<()> {
    let mut test = WatcherTest::setup().await?;
    let (seller_kp, seller_id) = user(&mut test, "Prune:Seller").await?;
    let (tagger_kp, tagger_id) = user(&mut test, "Prune:Tagger").await?;

    let live = test_listing(
        &seller_id,
        "Still on the homeserver",
        "fashion",
        PubkyAppListingCondition::New,
        5_000,
    );
    let (live_id, live_path) = test.create_listing(&seller_kp, &live).await?;
    let stale = test_auction_listing(
        &seller_id,
        "Deleted while Nexus missed the event",
        "collectibles",
        STARTS_AT,
        ENDS_AT,
    );
    let (stale_id, stale_path) = test.create_listing(&seller_kp, &stale).await?;

    let shared = format!("ps{}", &seller_id[..8]);
    let only_stale = format!("po{}", &seller_id[..8]);
    let profile_only = format!("pk{}", &seller_id[..8]);
    let live_uri = listing_uri_builder(seller_id.clone(), live_id.clone());
    let stale_uri = listing_uri_builder(seller_id.clone(), stale_id.clone());
    tag(&mut test, &tagger_kp, live_uri, &shared).await?;
    tag(&mut test, &tagger_kp, stale_uri.clone(), &shared).await?;
    tag(&mut test, &tagger_kp, stale_uri, &only_stale).await?;
    tag(
        &mut test,
        &tagger_kp,
        user_uri_builder(seller_id.clone()),
        &profile_only,
    )
    .await?;
    assert_eq!(find_user_counts(&tagger_id).await.tagged, 4);
    assert_eq!(seller_stream_ids(&seller_id).await.len(), 2);
    assert!(in_auction_set(&seller_id, &stale_id).await);

    delete_record_silently(&seller_kp, &stale_path).await?;

    let rows = vec![
        (seller_id.clone(), live_id.clone()),
        (seller_id.clone(), stale_id.clone()),
    ];

    // Dry run: reports the stale listing and changes nothing.
    let dry_run = prune_stale_listings_among(rows.clone(), false, 50)
        .await
        .unwrap();
    assert_eq!(dry_run.scanned, 2);
    assert_eq!(dry_run.present, 1);
    assert_eq!(
        dry_run.stale,
        vec![(seller_id.clone(), stale_id.clone())],
        "only the deleted record is stale"
    );
    assert_eq!(
        (dry_run.pruned, dry_run.restored, dry_run.failed),
        (0, 0, 0)
    );
    assert!(listing_is_indexed(&seller_id, &stale_id).await);
    assert!(in_auction_set(&seller_id, &stale_id).await);
    assert!(!is_pending(&seller_id, &stale_id).await);
    assert_eq!(find_user_counts(&tagger_id).await.tagged, 4);
    assert_eq!(seller_stream_ids(&seller_id).await.len(), 2);

    // More stale rows than the limit aborts before anything is deleted.
    assert!(prune_stale_listings_among(rows.clone(), true, 0)
        .await
        .is_err());
    assert!(listing_is_indexed(&seller_id, &stale_id).await);
    assert_eq!(find_user_counts(&tagger_id).await.tagged, 4);

    // Real run: only the stale listing goes, with every index cleaned.
    let applied = prune_stale_listings_among(rows, true, 50).await.unwrap();
    assert_eq!(
        (applied.pruned, applied.restored, applied.failed),
        (1, 0, 0)
    );
    assert!(!listing_is_indexed(&seller_id, &stale_id).await);
    assert!(!in_auction_set(&seller_id, &stale_id).await);
    assert!(!is_pending(&seller_id, &stale_id).await);
    assert_eq!(
        seller_stream_ids(&seller_id).await,
        vec![live_id.clone()],
        "timeline and per-seller sets keep only the live listing"
    );
    listing_is_untagged(&seller_id, &stale_id, &[&shared, &only_stale]).await?;
    assert!(
        !suggested(&only_stale).await?,
        "a label no listing uses leaves autocomplete"
    );
    assert!(
        suggested(&shared).await?,
        "a label the live listing still carries stays in autocomplete"
    );
    assert_eq!(
        find_user_counts(&tagger_id).await.tagged,
        2,
        "the tagger's count drops by the stale listing's two tags only"
    );

    assert!(listing_is_indexed(&seller_id, &live_id).await);
    let (live_taggers, _) = <TagListing as TaggersCollection>::get_from_index(
        vec![&seller_id, &live_id, &shared],
        None,
        None,
        None,
        None,
    )
    .await?;
    assert_eq!(live_taggers.len(), 1, "the live listing keeps its tag");
    assert!(by_tag_timeline_has(&shared, &seller_id, &live_id).await);

    // Second run, driven by the graph as the command is: nothing to do.
    let rerun = prune_stale_listings(true, 1_000).await.unwrap();
    assert!(!rerun.stale.iter().any(|(_, id)| id == &stale_id));
    assert!(rerun.present >= 1);
    assert_eq!((rerun.pruned, rerun.restored), (0, 0));
    assert!(listing_is_indexed(&seller_id, &live_id).await);
    assert_eq!(find_user_counts(&tagger_id).await.tagged, 2);

    test.del(&seller_kp, &live_path).await?;
    test.cleanup_user(&tagger_kp).await?;
    test.cleanup_user(&seller_kp).await?;
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Point {
    BeforeRecheck,
    AfterRecheck,
    DelStep(ListingDelStep),
    TagStep(TargetTagCleanupStep),
    AfterDelete,
}

/// Puts the record back on the homeserver and indexes it, as a republish
/// the watcher fully processed, the first time `point` is reached.
struct RepublishAt {
    point: Point,
    kp: Keypair,
    path: ResourcePath,
    listing: PubkyAppListing,
    fired: AtomicBool,
}

impl RepublishAt {
    async fn at(&self, point: Point, owner_id: &str, listing_id: &str) -> Result<(), DynError> {
        if point == self.point && !self.fired.swap(true, Ordering::SeqCst) {
            publish_record(&self.kp, &self.path, &self.listing).await?;
            reindex_from_homeserver(owner_id, listing_id).await?;
        }
        Ok(())
    }
}

#[async_trait]
impl StalePruneHook for RepublishAt {
    async fn before_recheck(&self, owner_id: &str, listing_id: &str) -> Result<(), DynError> {
        self.at(Point::BeforeRecheck, owner_id, listing_id).await
    }

    async fn after_recheck(&self, owner_id: &str, listing_id: &str) -> Result<(), DynError> {
        self.at(Point::AfterRecheck, owner_id, listing_id).await
    }

    async fn del_step(&self, step: ListingDelStep) -> Result<(), EventProcessorError> {
        let (owner_id, listing_id) = (
            self.listing.owner_pubky.clone(),
            self.listing.listing_id.clone(),
        );
        self.at(Point::DelStep(step), &owner_id, &listing_id)
            .await
            .map_err(|e| EventProcessorError::generic(e.to_string()))
    }

    fn tag_cleanup(&self) -> Option<&dyn TargetTagCleanupHook> {
        Some(self)
    }

    async fn after_delete(&self, owner_id: &str, listing_id: &str) -> Result<(), DynError> {
        self.at(Point::AfterDelete, owner_id, listing_id).await
    }
}

#[async_trait]
impl TargetTagCleanupHook for RepublishAt {
    async fn at(&self, step: TargetTagCleanupStep) -> Result<(), EventProcessorError> {
        let (owner_id, listing_id) = (
            self.listing.owner_pubky.clone(),
            self.listing.listing_id.clone(),
        );
        RepublishAt::at(self, Point::TagStep(step), &owner_id, &listing_id)
            .await
            .map_err(|e| EventProcessorError::generic(e.to_string()))
    }
}

#[tokio_shared_rt::test(shared)]
async fn prune_keeps_a_listing_whose_file_reappears_before_the_delete() -> Result<()> {
    let mut test = WatcherTest::setup().await?;
    let stale = stale_listing(&mut test, "Recheck", 0).await?;
    let hook = RepublishAt {
        point: Point::BeforeRecheck,
        kp: stale.seller_kp.clone(),
        path: stale.path.clone(),
        listing: stale.listing.clone(),
        fired: AtomicBool::new(false),
    };
    let summary = prune_stale_listings_among_with_hook(vec![stale.row()], true, 50, &hook)
        .await
        .unwrap();

    assert_eq!(
        (
            summary.pruned,
            summary.restored,
            summary.failed,
            summary.present
        ),
        (0, 0, 0, 1),
        "the recheck finds the file again and keeps the listing untouched"
    );
    assert!(summary.stale.is_empty());
    assert!(listing_is_indexed(&stale.seller_id, &stale.listing_id).await);
    assert_eq!(
        seller_stream_ids(&stale.seller_id).await,
        vec![stale.listing_id.clone()]
    );
    assert_eq!(
        find_user_counts(&stale.tagger_id).await.tagged,
        2,
        "no tag was removed"
    );
    assert!(by_tag_timeline_has(&stale.labels[0], &stale.seller_id, &stale.listing_id).await);
    assert!(!is_pending(&stale.seller_id, &stale.listing_id).await);

    test.del(&stale.seller_kp, &stale.path).await?;
    stale.cleanup(&mut test).await
}

/// A republish the watcher fully indexes at any point after the recheck said
/// gone must not leave the listing deleted: the post-delete check reads the
/// record back and re-indexes it.
#[tokio_shared_rt::test(shared)]
async fn prune_restores_a_listing_republished_at_any_point_of_its_delete() -> Result<()> {
    let mut test = WatcherTest::setup().await?;
    let points = [
        Point::AfterRecheck,
        Point::DelStep(ListingDelStep::Start),
        Point::DelStep(ListingDelStep::IndexesRemoved),
        Point::TagStep(TargetTagCleanupStep::EdgeDeleted),
        Point::DelStep(ListingDelStep::GraphDeleted),
        Point::AfterDelete,
    ];
    for (round, point) in points.into_iter().enumerate() {
        let stale = stale_listing(&mut test, "Republish", round).await?;
        let hook = RepublishAt {
            point,
            kp: stale.seller_kp.clone(),
            path: stale.path.clone(),
            listing: stale.listing.clone(),
            fired: AtomicBool::new(false),
        };
        let summary = prune_stale_listings_among_with_hook(vec![stale.row()], true, 50, &hook)
            .await
            .unwrap();
        assert!(
            hook.fired.load(Ordering::SeqCst),
            "{point:?}: never reached"
        );
        assert_eq!(
            (summary.pruned, summary.restored, summary.failed),
            (0, 1, 0),
            "{point:?}: the republished listing is re-indexed, not left deleted"
        );
        assert!(summary.stale.is_empty(), "{point:?}");
        assert!(
            listing_is_indexed(&stale.seller_id, &stale.listing_id).await,
            "{point:?}: graph and Redis hold the republished listing"
        );
        assert_eq!(
            seller_stream_ids(&stale.seller_id).await,
            vec![stale.listing_id.clone()],
            "{point:?}"
        );
        assert!(
            in_auction_set(&stale.seller_id, &stale.listing_id).await,
            "{point:?}: back in the auction end-time set"
        );
        assert!(
            !is_pending(&stale.seller_id, &stale.listing_id).await,
            "{point:?}: nothing left pending"
        );

        test.del(&stale.seller_kp, &stale.path).await?;
        stale.cleanup(&mut test).await?;
    }
    Ok(())
}

/// Stops the run at one point as a killed process or a failing store would,
/// once. With `refill`, a details read lands between the Redis removal and
/// the graph delete, as an API request would.
struct InterruptAt {
    point: Point,
    refill: bool,
    fired: AtomicBool,
    row: (String, String),
}

impl InterruptAt {
    fn hit(&self, point: Point) -> bool {
        point == self.point && !self.fired.swap(true, Ordering::SeqCst)
    }
}

#[async_trait]
impl StalePruneHook for InterruptAt {
    async fn before_recheck(&self, _: &str, _: &str) -> Result<(), DynError> {
        if self.hit(Point::BeforeRecheck) {
            return Err("interrupted before the recheck".into());
        }
        Ok(())
    }

    async fn after_recheck(&self, _: &str, _: &str) -> Result<(), DynError> {
        if self.hit(Point::AfterRecheck) {
            return Err("interrupted after the recheck".into());
        }
        Ok(())
    }

    async fn del_step(&self, step: ListingDelStep) -> Result<(), EventProcessorError> {
        if step == ListingDelStep::IndexesRemoved && self.refill {
            let (owner_id, listing_id) = &self.row;
            ListingDetails::get_by_id(owner_id, listing_id)
                .await
                .map_err(|e| EventProcessorError::generic(e.to_string()))?;
        }
        if self.hit(Point::DelStep(step)) {
            return Err(EventProcessorError::generic(format!(
                "interrupted at {step:?}"
            )));
        }
        Ok(())
    }

    fn tag_cleanup(&self) -> Option<&dyn TargetTagCleanupHook> {
        Some(self)
    }

    async fn after_delete(&self, _: &str, _: &str) -> Result<(), DynError> {
        if self.hit(Point::AfterDelete) {
            return Err("interrupted after the delete".into());
        }
        Ok(())
    }
}

#[async_trait]
impl TargetTagCleanupHook for InterruptAt {
    async fn at(&self, step: TargetTagCleanupStep) -> Result<(), EventProcessorError> {
        if self.hit(Point::TagStep(step)) {
            return Err(EventProcessorError::generic(format!(
                "interrupted at tag cleanup {step:?}"
            )));
        }
        Ok(())
    }
}

/// Interrupts one prune at every phase of the listing delete, then runs the
/// production entry point again: every graph and Redis index must end clean
/// and nothing may stay pending.
#[tokio_shared_rt::test(shared)]
async fn a_prune_interrupted_at_any_phase_is_finished_by_the_next_run() -> Result<()> {
    let mut test = WatcherTest::setup().await?;
    let scenarios = [
        (Point::BeforeRecheck, false),
        (Point::AfterRecheck, false),
        (Point::DelStep(ListingDelStep::Start), false),
        (Point::DelStep(ListingDelStep::IndexesRemoved), true),
        (Point::TagStep(TargetTagCleanupStep::EdgesRead), false),
        (Point::TagStep(TargetTagCleanupStep::EdgeDeleted), false),
        (Point::TagStep(TargetTagCleanupStep::MarkersRead), false),
        (Point::TagStep(TargetTagCleanupStep::TaggerCounted), false),
        (Point::TagStep(TargetTagCleanupStep::TaggersPurged), false),
        (Point::TagStep(TargetTagCleanupStep::TimelinePurged), false),
        (Point::TagStep(TargetTagCleanupStep::SearchPruned), false),
        (
            Point::TagStep(TargetTagCleanupStep::FinalCheckPassed),
            false,
        ),
        (Point::DelStep(ListingDelStep::GraphDeleted), true),
        (Point::AfterDelete, false),
    ];
    for (round, (point, refill)) in scenarios.into_iter().enumerate() {
        let stale = stale_listing(&mut test, "Interrupt", round).await?;
        let hook = InterruptAt {
            point,
            refill,
            fired: AtomicBool::new(false),
            row: stale.row(),
        };
        let first = prune_stale_listings_among_with_hook(vec![stale.row()], true, 50, &hook).await;
        assert!(
            hook.fired.load(Ordering::SeqCst),
            "{point:?}: never reached"
        );
        match point {
            Point::BeforeRecheck | Point::AfterRecheck | Point::AfterDelete => {
                assert!(first.is_err(), "{point:?}: the run stops like a kill");
            }
            _ => {
                let summary = first.unwrap();
                assert_eq!(summary.failed, 1, "{point:?}");
                assert_eq!(summary.pruned, 0, "{point:?}");
            }
        }
        let started = !matches!(point, Point::BeforeRecheck);
        assert_eq!(
            is_pending(&stale.seller_id, &stale.listing_id).await,
            started,
            "{point:?}: the delete is recorded before it starts and kept until confirmed"
        );

        // Second run through the production entry point: graph rows plus
        // pending rows, so a listing whose graph node is already gone is
        // still found.
        let rerun = prune_stale_listings(true, 1_000).await.unwrap();
        assert!(
            rerun.stale.contains(&stale.row()),
            "{point:?}: the rerun finds the row"
        );
        stale
            .assert_fully_removed(&format!("{point:?} then rerun"))
            .await?;

        stale.cleanup(&mut test).await?;
    }
    Ok(())
}

/// A details read between the Redis removal and the graph delete refills the
/// cache; the second sweep must remove it even when nothing is interrupted.
#[tokio_shared_rt::test(shared)]
async fn a_details_read_during_the_delete_leaves_no_redis_index_behind() -> Result<()> {
    let mut test = WatcherTest::setup().await?;
    let stale = stale_listing(&mut test, "Refill", 0).await?;
    let hook = InterruptAt {
        point: Point::BeforeRecheck,
        refill: true,
        fired: AtomicBool::new(true),
        row: stale.row(),
    };
    let summary = prune_stale_listings_among_with_hook(vec![stale.row()], true, 50, &hook)
        .await
        .unwrap();
    assert_eq!((summary.pruned, summary.failed), (1, 0));
    stale
        .assert_fully_removed("refill without interruption")
        .await?;
    stale.cleanup(&mut test).await
}

/// The record comes back after an interrupted prune already removed the
/// listing: the dry run reports it, and the next apply re-indexes it.
#[tokio_shared_rt::test(shared)]
async fn a_pending_listing_whose_record_came_back_is_restored_by_the_next_run() -> Result<()> {
    let mut test = WatcherTest::setup().await?;
    let stale = stale_listing(&mut test, "Resume", 0).await?;
    let hook = InterruptAt {
        point: Point::AfterDelete,
        refill: false,
        fired: AtomicBool::new(false),
        row: stale.row(),
    };
    assert!(
        prune_stale_listings_among_with_hook(vec![stale.row()], true, 50, &hook)
            .await
            .is_err()
    );
    assert!(!listing_is_indexed(&stale.seller_id, &stale.listing_id).await);
    assert!(is_pending(&stale.seller_id, &stale.listing_id).await);

    publish_record(&stale.seller_kp, &stale.path, &stale.listing).await?;

    let dry = prune_stale_listings(false, 1_000).await.unwrap();
    assert!(dry.to_restore.contains(&stale.row()));
    assert_eq!(dry.restored, 0);
    assert!(!listing_is_indexed(&stale.seller_id, &stale.listing_id).await);
    assert!(is_pending(&stale.seller_id, &stale.listing_id).await);

    let applied = prune_stale_listings(true, 1_000).await.unwrap();
    assert!(applied.restored >= 1);
    assert!(listing_is_indexed(&stale.seller_id, &stale.listing_id).await);
    assert!(in_auction_set(&stale.seller_id, &stale.listing_id).await);
    assert!(!is_pending(&stale.seller_id, &stale.listing_id).await);

    test.del(&stale.seller_kp, &stale.path).await?;
    stale.cleanup(&mut test).await
}

#[tokio_shared_rt::test(shared)]
async fn prune_leaves_a_listing_it_cannot_check() -> Result<()> {
    let mut test = WatcherTest::setup().await?;
    let (seller_kp, seller_id) = user(&mut test, "Unreachable:Seller").await?;
    let listing = test_listing(
        &seller_id,
        "Seller resolves, stranger does not",
        "fashion",
        PubkyAppListingCondition::New,
        1_000,
    );
    let (listing_id, listing_path) = test.create_listing(&seller_kp, &listing).await?;

    // A seller with no homeserver record cannot be resolved: not a deletion.
    let stranger = Keypair::random().public_key().to_string();
    let summary = prune_stale_listings_among(
        vec![
            (stranger, "0000000000000".to_string()),
            (seller_id.clone(), listing_id.clone()),
        ],
        true,
        50,
    )
    .await
    .unwrap();
    assert_eq!((summary.failed, summary.present, summary.pruned), (1, 1, 0));
    assert!(summary.stale.is_empty());
    assert!(listing_is_indexed(&seller_id, &listing_id).await);

    test.del(&seller_kp, &listing_path).await?;
    test.cleanup_user(&seller_kp).await?;
    Ok(())
}

struct FailAt(ListingDelStep);

#[async_trait]
impl ListingDelHook for FailAt {
    async fn at(&self, step: ListingDelStep) -> Result<(), EventProcessorError> {
        if step == self.0 {
            return Err(EventProcessorError::generic(format!("stopped at {step:?}")));
        }
        Ok(())
    }
}

/// The watcher's own DEL has no pending record to resume from, so the Redis
/// indexes must already be gone when it stops after the graph node was
/// deleted.
#[tokio_shared_rt::test(shared)]
async fn a_listing_del_stopped_after_the_graph_delete_leaves_no_redis_index() -> Result<()> {
    let mut test = WatcherTest::setup().await?;
    let stale = stale_listing(&mut test, "DelStop", 0).await?;
    let owner = PubkyId::try_from(stale.seller_id.as_str()).map_err(anyhow::Error::msg)?;

    let stopped = del_with_hook(
        owner.clone(),
        stale.listing_id.clone(),
        &FailAt(ListingDelStep::GraphDeleted),
    )
    .await;
    assert!(stopped.is_err());
    assert!(
        ListingDetails::get_from_graph(&stale.seller_id, &stale.listing_id)
            .await?
            .is_none(),
        "the graph node is gone"
    );
    assert!(
        ListingDetails::get_from_index(&stale.seller_id, &stale.listing_id)
            .await?
            .is_none(),
        "the details key is already gone"
    );
    assert!(seller_stream_ids(&stale.seller_id).await.is_empty());
    assert!(!in_auction_set(&stale.seller_id, &stale.listing_id).await);

    stale.cleanup(&mut test).await
}
