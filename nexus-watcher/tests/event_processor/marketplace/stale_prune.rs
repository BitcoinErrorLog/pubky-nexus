use super::tag_cleanup::{listing_is_untagged, suggested, tag, user};
use super::utils::test_listing;
use crate::event_processor::users::utils::find_user_counts;
use crate::event_processor::utils::watcher::WatcherTest;
use anyhow::Result;
use async_trait::async_trait;
use nexus_common::db::kv::SortOrder;
use nexus_common::db::PubkyConnector;
use nexus_common::models::marketplace::{
    ListingDetails, ListingStream, ListingStreamFilters, ListingStreamSorting, ListingsByTagSearch,
};
use nexus_common::models::tag::listing::TagListing;
use nexus_common::models::tag::traits::TaggersCollection;
use nexus_common::types::{DynError, Pagination};
use nexus_watcher::events::handlers::listing::{
    prune_stale_listings, prune_stale_listings_among, prune_stale_listings_among_with_hook,
    StalePruneHook,
};
use pubky::{Keypair, ResourcePath};
use pubky_app_specs::{listing_uri_builder, PubkyAppListing, PubkyAppListingCondition};

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

/// Deletes the record on the homeserver without letting the watcher process
/// the DEL event: the row Nexus keeps is now stale.
async fn delete_record_silently(kp: &Keypair, path: &ResourcePath) -> Result<()> {
    let pubky = PubkyConnector::get()?;
    let session = pubky.signer(kp.clone()).signin().await?;
    session.storage().delete(path).await?;
    Ok(())
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
    let stale = test_listing(
        &seller_id,
        "Deleted while Nexus missed the event",
        "fashion",
        PubkyAppListingCondition::New,
        7_000,
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
        pubky_app_specs::user_uri_builder(seller_id.clone()),
        &profile_only,
    )
    .await?;
    assert_eq!(find_user_counts(&tagger_id).await.tagged, 4);
    assert_eq!(seller_stream_ids(&seller_id).await.len(), 2);

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
    assert_eq!((dry_run.pruned, dry_run.failed), (0, 0));
    assert!(listing_is_indexed(&seller_id, &stale_id).await);
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
    assert_eq!((applied.pruned, applied.failed), (1, 0));
    assert!(!listing_is_indexed(&seller_id, &stale_id).await);
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
    assert_eq!(rerun.pruned, 0);
    assert!(listing_is_indexed(&seller_id, &live_id).await);
    assert_eq!(find_user_counts(&tagger_id).await.tagged, 2);

    test.del(&seller_kp, &live_path).await?;
    test.cleanup_user(&tagger_kp).await?;
    test.cleanup_user(&seller_kp).await?;
    Ok(())
}

/// Puts the record back on the homeserver after the scan marked the row
/// stale and before the delete, as a seller re-publishing would.
struct RepublishBeforeRecheck {
    kp: Keypair,
    path: ResourcePath,
    listing: PubkyAppListing,
}

#[async_trait]
impl StalePruneHook for RepublishBeforeRecheck {
    async fn before_recheck(&self, _owner_id: &str, _listing_id: &str) -> Result<(), DynError> {
        let pubky = PubkyConnector::get()?;
        let session = pubky.signer(self.kp.clone()).signin().await?;
        session
            .storage()
            .put(&self.path, serde_json::to_string(&self.listing)?)
            .await?;
        Ok(())
    }
}

#[tokio_shared_rt::test(shared)]
async fn prune_keeps_a_listing_whose_file_reappears_before_the_delete() -> Result<()> {
    let mut test = WatcherTest::setup().await?;
    let (seller_kp, seller_id) = user(&mut test, "Recheck:Seller").await?;
    let (tagger_kp, tagger_id) = user(&mut test, "Recheck:Tagger").await?;

    let listing = test_listing(
        &seller_id,
        "Republished mid-run",
        "fashion",
        PubkyAppListingCondition::New,
        3_000,
    );
    let (listing_id, listing_path) = test.create_listing(&seller_kp, &listing).await?;
    let label = format!("rc{}", &seller_id[..8]);
    tag(
        &mut test,
        &tagger_kp,
        listing_uri_builder(seller_id.clone(), listing_id.clone()),
        &label,
    )
    .await?;

    delete_record_silently(&seller_kp, &listing_path).await?;

    let mut republished = listing.clone();
    republished.listing_id = listing_id.clone();
    let hook = RepublishBeforeRecheck {
        kp: seller_kp.clone(),
        path: listing_path.clone(),
        listing: republished,
    };
    let summary = prune_stale_listings_among_with_hook(
        vec![(seller_id.clone(), listing_id.clone())],
        true,
        50,
        &hook,
    )
    .await
    .unwrap();

    assert_eq!(
        (summary.pruned, summary.failed, summary.present),
        (0, 0, 1),
        "the recheck finds the file again and keeps the listing"
    );
    assert!(summary.stale.is_empty());
    assert!(listing_is_indexed(&seller_id, &listing_id).await);
    assert_eq!(
        seller_stream_ids(&seller_id).await,
        vec![listing_id.clone()]
    );
    assert_eq!(find_user_counts(&tagger_id).await.tagged, 1);
    assert!(by_tag_timeline_has(&label, &seller_id, &listing_id).await);

    test.del(&seller_kp, &listing_path).await?;
    test.cleanup_user(&tagger_kp).await?;
    test.cleanup_user(&seller_kp).await?;
    Ok(())
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
