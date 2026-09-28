use super::utils::{test_listing, test_shop};
use crate::event_processor::users::utils::find_user_counts;
use crate::event_processor::utils::watcher::{HomeserverHashIdPath, HomeserverPath, WatcherTest};
use anyhow::Result;
use async_trait::async_trait;
use chrono::Utc;
use nexus_common::db::graph::Query;
use nexus_common::db::kv::sets;
use nexus_common::db::{fetch_key_from_graph, Neo4JConfig, RedisOps};
use nexus_common::models::event::EventProcessorError;
use nexus_common::models::marketplace::ListingsByTagSearch;
use nexus_common::models::tag::listing::TagListing;
use nexus_common::models::tag::search::TagSearch;
use nexus_common::models::tag::shop::TagShop;
use nexus_common::models::tag::traits::{TagCollection, TaggersCollection};
use nexus_common::models::user::UserCounts;
use nexus_common::types::Pagination;
use nexus_watcher::events::handlers::tag::{
    del_tagged_target, del_tagged_target_with_hook, TagTarget, TargetTagCleanupHook,
    TargetTagCleanupStep,
};
use pubky::Keypair;
use pubky_app_specs::traits::HashId;
use pubky_app_specs::{
    listing_uri_builder, user_uri_builder, PubkyAppListingCondition, PubkyAppShop, PubkyAppTag,
    PubkyAppUser, PubkyId,
};
use std::sync::atomic::{AtomicBool, Ordering};

/// Fails the cleanup once, as a Redis or graph error would, at one step.
struct FailOnceAt {
    step: TargetTagCleanupStep,
    fired: AtomicBool,
}

#[async_trait]
impl TargetTagCleanupHook for FailOnceAt {
    async fn at(&self, step: TargetTagCleanupStep) -> Result<(), EventProcessorError> {
        if step == self.step && !self.fired.swap(true, Ordering::SeqCst) {
            return Err(EventProcessorError::IndexOperationFailed(format!(
                "injected failure at {step:?}"
            )));
        }
        Ok(())
    }
}

/// Indexes a new tag on the target through the tag PUT handler, as a
/// concurrently processed tag event would, the first time the step is hit.
struct TagLandsAt {
    step: TargetTagCleanupStep,
    tagger_id: PubkyId,
    tag: PubkyAppTag,
    fired: AtomicBool,
}

#[async_trait]
impl TargetTagCleanupHook for TagLandsAt {
    async fn at(&self, step: TargetTagCleanupStep) -> Result<(), EventProcessorError> {
        if step == self.step && !self.fired.swap(true, Ordering::SeqCst) {
            nexus_watcher::events::handlers::tag::sync_put(
                self.tag.clone(),
                self.tagger_id.clone(),
                self.tag.create_id(),
            )
            .await?;
        }
        Ok(())
    }
}

/// Runs the tagger's own untag of one tag through the tag DEL handler, as
/// a concurrently processed DEL event would, the first time the step is hit.
struct UntagAt {
    step: TargetTagCleanupStep,
    tagger_id: PubkyId,
    tag_id: String,
    fired: AtomicBool,
}

#[async_trait]
impl TargetTagCleanupHook for UntagAt {
    async fn at(&self, step: TargetTagCleanupStep) -> Result<(), EventProcessorError> {
        if step == self.step && !self.fired.swap(true, Ordering::SeqCst) {
            match nexus_watcher::events::handlers::tag::del(
                self.tagger_id.clone(),
                self.tag_id.clone(),
            )
            .await
            {
                Ok(()) | Err(EventProcessorError::SkipIndexing) => {}
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }
}

async fn user(test: &mut WatcherTest, name: &str) -> Result<(Keypair, String)> {
    let kp = Keypair::random();
    let user = PubkyAppUser {
        bio: Some("target tag cleanup".to_string()),
        image: None,
        links: None,
        name: name.to_string(),
        status: None,
    };
    let id = test.create_user(&kp, &user).await?;
    Ok((kp, id))
}

async fn tag(
    test: &mut WatcherTest,
    kp: &Keypair,
    uri: String,
    label: &str,
) -> Result<PubkyAppTag> {
    let tag = PubkyAppTag {
        uri,
        label: label.to_string(),
        created_at: Utc::now().timestamp_millis(),
    };
    test.put(kp, &tag.hs_path(), tag.clone()).await?;
    Ok(tag)
}

async fn listing_is_untagged(seller_id: &str, listing_id: &str, labels: &[&str]) -> Result<()> {
    let scores = <TagListing as TagCollection>::get_from_index(
        seller_id,
        Some(listing_id),
        None,
        None,
        None,
        None,
        false,
    )
    .await?
    .unwrap_or_default();
    assert!(scores.is_empty(), "label scores left: {scores:?}");
    let listing_key = format!("{seller_id}:{listing_id}");
    for label in labels {
        let (taggers, _) = <TagListing as TaggersCollection>::get_from_index(
            vec![seller_id, listing_id, label],
            None,
            None,
            None,
            None,
        )
        .await?;
        assert!(taggers.is_empty(), "taggers left for {label}");
        let timeline = ListingsByTagSearch::get_by_label(label, Pagination::default())
            .await?
            .unwrap_or_default();
        assert!(
            !timeline
                .iter()
                .any(|entry| entry.listing_key == listing_key),
            "timeline member left for {label}"
        );
    }
    Ok(())
}

async fn suggested(label: &str) -> Result<bool> {
    Ok(TagSearch::get_by_label(label, &Pagination::default())
        .await?
        .is_some_and(|found| {
            found
                .iter()
                .any(|tag| serde_json::to_value(tag).ok() == Some(serde_json::json!(label)))
        }))
}

#[tokio_shared_rt::test(shared)]
async fn listing_tag_cleanup_retried_after_a_failure_at_any_step_ends_clean() -> Result<()> {
    let mut test = WatcherTest::setup().await?;

    for (round, step) in [
        TargetTagCleanupStep::EdgesRead,
        TargetTagCleanupStep::EdgeDeleted,
        TargetTagCleanupStep::MarkersRead,
        TargetTagCleanupStep::TaggerCounted,
        TargetTagCleanupStep::TaggersPurged,
        TargetTagCleanupStep::TimelinePurged,
        TargetTagCleanupStep::SearchPruned,
        TargetTagCleanupStep::FinalCheckPassed,
    ]
    .into_iter()
    .enumerate()
    {
        let (seller_kp, seller_id) = user(&mut test, "Cleanup:Seller").await?;
        let (a_kp, a_id) = user(&mut test, "Cleanup:TaggerA").await?;
        let (b_kp, b_id) = user(&mut test, "Cleanup:TaggerB").await?;
        let listing = test_listing(
            &seller_id,
            "Cleanup boots",
            "fashion",
            PubkyAppListingCondition::New,
            1_000,
        );
        let (listing_id, listing_path) = test.create_listing(&seller_kp, &listing).await?;
        let listing_uri = listing_uri_builder(seller_id.clone(), listing_id.clone());
        let shared = format!("cs{round}{}", &seller_id[..6]);
        let only = format!("co{round}{}", &seller_id[..6]);
        let keep = format!("ck{round}{}", &seller_id[..6]);

        // A: two listing labels and one unrelated tag; B: the shared label on
        // the listing and on the seller's profile.
        tag(&mut test, &a_kp, listing_uri.clone(), &shared).await?;
        tag(&mut test, &a_kp, listing_uri.clone(), &only).await?;
        tag(&mut test, &a_kp, user_uri_builder(seller_id.clone()), &keep).await?;
        tag(&mut test, &b_kp, listing_uri.clone(), &shared).await?;
        tag(
            &mut test,
            &b_kp,
            user_uri_builder(seller_id.clone()),
            &shared,
        )
        .await?;
        assert_eq!(find_user_counts(&a_id).await.tagged, 3);
        assert_eq!(find_user_counts(&b_id).await.tagged, 2);

        let target = TagTarget::Listing {
            owner_id: &seller_id,
            listing_id: &listing_id,
        };
        let failing = FailOnceAt {
            step,
            fired: AtomicBool::new(false),
        };
        let first = del_tagged_target_with_hook(target, &failing).await;
        assert!(
            first.is_err(),
            "{step:?}: the injected failure must surface"
        );
        del_tagged_target(target).await?;

        // Exactly one decrement per listing tag, whatever failed.
        assert_eq!(find_user_counts(&a_id).await.tagged, 1, "{step:?}");
        assert_eq!(find_user_counts(&b_id).await.tagged, 1, "{step:?}");
        listing_is_untagged(&seller_id, &listing_id, &[&shared, &only]).await?;
        assert!(
            suggested(&shared).await?,
            "{step:?}: still used on a profile"
        );
        assert!(!suggested(&only).await?, "{step:?}: unused label must go");
        assert!(suggested(&keep).await?);

        // The claims are gone: a re-created listing tagged again by A under
        // the same tag id is counted down again when it is deleted.
        test.del(&seller_kp, &listing_path).await?;
        let mut recreated = listing.clone();
        recreated.listing_id = listing_id.clone();
        test.put(&seller_kp, &listing_path, &recreated).await?;
        tag(&mut test, &a_kp, listing_uri.clone(), &only).await?;
        assert_eq!(find_user_counts(&a_id).await.tagged, 2);
        test.del(&seller_kp, &listing_path).await?;
        assert_eq!(find_user_counts(&a_id).await.tagged, 1, "{step:?}");

        test.cleanup_user(&a_kp).await?;
        test.cleanup_user(&b_kp).await?;
        test.cleanup_user(&seller_kp).await?;
    }
    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn a_tag_landing_during_listing_cleanup_is_swept_too() -> Result<()> {
    let mut test = WatcherTest::setup().await?;
    let (seller_kp, seller_id) = user(&mut test, "Race:Seller").await?;
    let (a_kp, a_id) = user(&mut test, "Race:TaggerA").await?;
    let (c_kp, c_id) = user(&mut test, "Race:TaggerC").await?;
    let listing = test_listing(
        &seller_id,
        "Race boots",
        "fashion",
        PubkyAppListingCondition::New,
        1_000,
    );
    let (listing_id, listing_path) = test.create_listing(&seller_kp, &listing).await?;
    let listing_uri = listing_uri_builder(seller_id.clone(), listing_id.clone());
    let first = format!("ra{}", &seller_id[..8]);
    let late = format!("rl{}", &seller_id[..8]);
    tag(&mut test, &a_kp, listing_uri.clone(), &first).await?;

    let landing = TagLandsAt {
        step: TargetTagCleanupStep::EdgeDeleted,
        tagger_id: PubkyId::try_from(c_id.as_str()).map_err(anyhow::Error::msg)?,
        tag: PubkyAppTag {
            uri: listing_uri.clone(),
            label: late.clone(),
            created_at: Utc::now().timestamp_millis(),
        },
        fired: AtomicBool::new(false),
    };
    del_tagged_target_with_hook(
        TagTarget::Listing {
            owner_id: &seller_id,
            listing_id: &listing_id,
        },
        &landing,
    )
    .await?;
    assert!(landing.fired.load(Ordering::SeqCst));

    assert_eq!(find_user_counts(&a_id).await.tagged, 0);
    assert_eq!(find_user_counts(&c_id).await.tagged, 0);
    listing_is_untagged(&seller_id, &listing_id, &[&first, &late]).await?;
    assert!(!suggested(&late).await?);

    test.del(&seller_kp, &listing_path).await?;
    test.cleanup_user(&a_kp).await?;
    test.cleanup_user(&c_kp).await?;
    test.cleanup_user(&seller_kp).await?;
    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn shop_tag_cleanup_retried_after_a_failure_ends_clean() -> Result<()> {
    let mut test = WatcherTest::setup().await?;
    let (owner_kp, owner_id) = user(&mut test, "ShopCleanup:Owner").await?;
    let (a_kp, a_id) = user(&mut test, "ShopCleanup:Tagger").await?;
    let shop_path = PubkyAppShop::hs_path();
    test.put(&owner_kp, &shop_path, &test_shop(&owner_id))
        .await?;
    let label = format!("sc{}", &owner_id[..8]);
    let shop_uri = pubky_app_specs::shop_uri_builder(owner_id.clone());
    tag(&mut test, &a_kp, shop_uri, &label).await?;
    assert!(suggested(&label).await?);

    let target = TagTarget::Shop {
        owner_id: &owner_id,
    };
    let failing = FailOnceAt {
        step: TargetTagCleanupStep::TaggersPurged,
        fired: AtomicBool::new(false),
    };
    assert!(del_tagged_target_with_hook(target, &failing).await.is_err());
    del_tagged_target(target).await?;

    assert_eq!(find_user_counts(&a_id).await.tagged, 0);
    let scores =
        <TagShop as TagCollection>::get_from_index(&owner_id, None, None, None, None, None, false)
            .await?
            .unwrap_or_default();
    assert!(scores.is_empty());
    let (taggers, _) = <TagShop as TaggersCollection>::get_from_index(
        vec![owner_id.as_str(), label.as_str()],
        None,
        None,
        None,
        None,
    )
    .await?;
    assert!(taggers.is_empty());
    assert!(
        !suggested(&label).await?,
        "shop-only label must leave autocomplete"
    );

    test.del(&owner_kp, &shop_path).await?;
    test.cleanup_user(&a_kp).await?;
    test.cleanup_user(&owner_kp).await?;
    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn untagging_a_shop_or_listing_prunes_autocomplete_only_when_unused() -> Result<()> {
    let mut test = WatcherTest::setup().await?;
    let (owner_kp, owner_id) = user(&mut test, "Untag:Owner").await?;
    let (a_kp, _) = user(&mut test, "Untag:Tagger").await?;
    let shop_path = PubkyAppShop::hs_path();
    test.put(&owner_kp, &shop_path, &test_shop(&owner_id))
        .await?;
    let listing = test_listing(
        &owner_id,
        "Untag boots",
        "fashion",
        PubkyAppListingCondition::New,
        1_000,
    );
    let (listing_id, listing_path) = test.create_listing(&owner_kp, &listing).await?;
    let shop_only = format!("us{}", &owner_id[..8]);
    let shared = format!("uv{}", &owner_id[..8]);

    let shop_tag = tag(
        &mut test,
        &a_kp,
        pubky_app_specs::shop_uri_builder(owner_id.clone()),
        &shop_only,
    )
    .await?;
    let listing_tag = tag(
        &mut test,
        &a_kp,
        listing_uri_builder(owner_id.clone(), listing_id.clone()),
        &shared,
    )
    .await?;
    tag(
        &mut test,
        &a_kp,
        user_uri_builder(owner_id.clone()),
        &shared,
    )
    .await?;

    // The tagger's own DEL of a shop tag prunes a label nothing else uses.
    test.del(&a_kp, &shop_tag.hs_path()).await?;
    assert!(!suggested(&shop_only).await?);
    // Untagging the listing keeps a label a profile tag still uses.
    test.del(&a_kp, &listing_tag.hs_path()).await?;
    assert!(suggested(&shared).await?);

    test.del(&owner_kp, &listing_path).await?;
    test.del(&owner_kp, &shop_path).await?;
    test.cleanup_user(&a_kp).await?;
    test.cleanup_user(&owner_kp).await?;
    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn a_taggers_untag_racing_the_cleanup_is_counted_once() -> Result<()> {
    let mut test = WatcherTest::setup().await?;
    for (round, step) in [
        TargetTagCleanupStep::EdgesRead,
        TargetTagCleanupStep::EdgeDeleted,
    ]
    .into_iter()
    .enumerate()
    {
        let (seller_kp, seller_id) = user(&mut test, "Untag:Seller").await?;
        let (a_kp, a_id) = user(&mut test, "Untag:TaggerA").await?;
        let listing = test_listing(
            &seller_id,
            "Untag race boots",
            "fashion",
            PubkyAppListingCondition::New,
            1_000,
        );
        let (listing_id, listing_path) = test.create_listing(&seller_kp, &listing).await?;
        let label = format!("ur{round}{}", &seller_id[..6]);
        let keep = format!("uk{round}{}", &seller_id[..6]);
        let listing_tag = tag(
            &mut test,
            &a_kp,
            listing_uri_builder(seller_id.clone(), listing_id.clone()),
            &label,
        )
        .await?;
        tag(&mut test, &a_kp, user_uri_builder(seller_id.clone()), &keep).await?;
        assert_eq!(find_user_counts(&a_id).await.tagged, 2);

        // The tagger's own DEL of the listing tag lands before the cleanup
        // deletes the edge, or right after.
        let untag = UntagAt {
            step,
            tagger_id: PubkyId::try_from(a_id.as_str()).map_err(anyhow::Error::msg)?,
            tag_id: listing_tag.create_id(),
            fired: AtomicBool::new(false),
        };
        del_tagged_target_with_hook(
            TagTarget::Listing {
                owner_id: &seller_id,
                listing_id: &listing_id,
            },
            &untag,
        )
        .await?;
        assert!(untag.fired.load(Ordering::SeqCst));
        assert_eq!(find_user_counts(&a_id).await.tagged, 1, "{step:?}");
        listing_is_untagged(&seller_id, &listing_id, &[&label]).await?;

        test.del(&seller_kp, &listing_path).await?;
        test.cleanup_user(&a_kp).await?;
        test.cleanup_user(&seller_kp).await?;
    }
    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn a_retag_during_a_multi_round_cleanup_is_counted() -> Result<()> {
    let mut test = WatcherTest::setup().await?;
    let (seller_kp, seller_id) = user(&mut test, "Retag:Seller").await?;
    let (a_kp, a_id) = user(&mut test, "Retag:TaggerA").await?;
    let listing = test_listing(
        &seller_id,
        "Retag race boots",
        "fashion",
        PubkyAppListingCondition::New,
        1_000,
    );
    let (listing_id, listing_path) = test.create_listing(&seller_kp, &listing).await?;
    let label = format!("rt{}", &seller_id[..8]);
    let keep = format!("rk{}", &seller_id[..8]);
    let listing_tag = tag(
        &mut test,
        &a_kp,
        listing_uri_builder(seller_id.clone(), listing_id.clone()),
        &label,
    )
    .await?;
    tag(&mut test, &a_kp, user_uri_builder(seller_id.clone()), &keep).await?;
    assert_eq!(find_user_counts(&a_id).await.tagged, 2);

    // After the first edge is counted down, the same tagger tags the same
    // label again: the same deterministic tag id, a new edge.
    let retag = TagLandsAt {
        step: TargetTagCleanupStep::TaggerCounted,
        tagger_id: PubkyId::try_from(a_id.as_str()).map_err(anyhow::Error::msg)?,
        tag: listing_tag,
        fired: AtomicBool::new(false),
    };
    del_tagged_target_with_hook(
        TagTarget::Listing {
            owner_id: &seller_id,
            listing_id: &listing_id,
        },
        &retag,
    )
    .await?;
    assert!(retag.fired.load(Ordering::SeqCst));
    assert_eq!(find_user_counts(&a_id).await.tagged, 1);
    listing_is_untagged(&seller_id, &listing_id, &[&label]).await?;

    test.del(&seller_kp, &listing_path).await?;
    test.cleanup_user(&a_kp).await?;
    test.cleanup_user(&seller_kp).await?;
    Ok(())
}

/// Runs a whole second cleanup of the same target, as an overlapping
/// retry of the DEL would, the first time the step is hit.
struct CleanupRunsAt {
    step: TargetTagCleanupStep,
    owner_id: String,
    listing_id: String,
    fired: AtomicBool,
}

#[async_trait]
impl TargetTagCleanupHook for CleanupRunsAt {
    async fn at(&self, step: TargetTagCleanupStep) -> Result<(), EventProcessorError> {
        if step == self.step && !self.fired.swap(true, Ordering::SeqCst) {
            del_tagged_target(TagTarget::Listing {
                owner_id: &self.owner_id,
                listing_id: &self.listing_id,
            })
            .await?;
        }
        Ok(())
    }
}

async fn graph_count(query: Query) -> Result<i64> {
    Ok(fetch_key_from_graph::<i64>(query, "n")
        .await?
        .unwrap_or_default())
}

async fn listing_nodes(owner_id: &str, listing_id: &str) -> Result<i64> {
    graph_count(
        Query::new(
            "test_listing_nodes",
            "MATCH (l:Listing {id: $listing_id, owner_id: $owner_id}) RETURN count(l) AS n",
        )
        .param("owner_id", owner_id)
        .param("listing_id", listing_id),
    )
    .await
}

async fn shop_nodes(owner_id: &str) -> Result<i64> {
    graph_count(
        Query::new(
            "test_shop_nodes",
            "MATCH (s:Shop {owner_id: $owner_id}) RETURN count(s) AS n",
        )
        .param("owner_id", owner_id),
    )
    .await
}

#[tokio_shared_rt::test(shared)]
async fn a_tag_committed_after_the_final_check_refuses_the_listing_deletion() -> Result<()> {
    let mut test = WatcherTest::setup().await?;
    let (seller_kp, seller_id) = user(&mut test, "Final:Seller").await?;
    let (c_kp, c_id) = user(&mut test, "Final:TaggerC").await?;
    let listing = test_listing(
        &seller_id,
        "Final check boots",
        "fashion",
        PubkyAppListingCondition::New,
        1_000,
    );
    let (listing_id, _) = test.create_listing(&seller_kp, &listing).await?;
    let listing_uri = listing_uri_builder(seller_id.clone(), listing_id.clone());
    let late = format!("fl{}", &seller_id[..8]);
    let tag = PubkyAppTag {
        uri: listing_uri.clone(),
        label: late.clone(),
        created_at: Utc::now().timestamp_millis(),
    };
    let tagger_id = PubkyId::try_from(c_id.as_str()).map_err(anyhow::Error::msg)?;

    // The untagged listing passes the empty check; the tag commits before
    // the deletion statement runs.
    let landing = TagLandsAt {
        step: TargetTagCleanupStep::FinalCheckPassed,
        tagger_id: tagger_id.clone(),
        tag: tag.clone(),
        fired: AtomicBool::new(false),
    };
    del_tagged_target_with_hook(
        TagTarget::Listing {
            owner_id: &seller_id,
            listing_id: &listing_id,
        },
        &landing,
    )
    .await?;
    assert!(landing.fired.load(Ordering::SeqCst));
    assert_eq!(listing_nodes(&seller_id, &listing_id).await?, 0);
    assert_eq!(find_user_counts(&c_id).await.tagged, 0);
    listing_is_untagged(&seller_id, &listing_id, &[&late]).await?;
    assert!(!suggested(&late).await?);

    // After the deletion the same tag finds no target and indexes nothing.
    let after =
        nexus_watcher::events::handlers::tag::sync_put(tag.clone(), tagger_id, tag.create_id())
            .await;
    assert!(
        matches!(after, Err(EventProcessorError::MissingDependency { .. })),
        "{after:?}"
    );
    assert_eq!(find_user_counts(&c_id).await.tagged, 0);
    listing_is_untagged(&seller_id, &listing_id, &[&late]).await?;
    assert!(!suggested(&late).await?);

    test.cleanup_user(&c_kp).await?;
    test.cleanup_user(&seller_kp).await?;
    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn a_tag_committed_after_the_final_check_refuses_the_shop_deletion() -> Result<()> {
    let mut test = WatcherTest::setup().await?;
    let (owner_kp, owner_id) = user(&mut test, "FinalShop:Owner").await?;
    let (c_kp, c_id) = user(&mut test, "FinalShop:Tagger").await?;
    let shop_path = PubkyAppShop::hs_path();
    test.put(&owner_kp, &shop_path, &test_shop(&owner_id))
        .await?;
    let late = format!("fs{}", &owner_id[..8]);
    let tag = PubkyAppTag {
        uri: pubky_app_specs::shop_uri_builder(owner_id.clone()),
        label: late.clone(),
        created_at: Utc::now().timestamp_millis(),
    };
    let tagger_id = PubkyId::try_from(c_id.as_str()).map_err(anyhow::Error::msg)?;

    let landing = TagLandsAt {
        step: TargetTagCleanupStep::FinalCheckPassed,
        tagger_id: tagger_id.clone(),
        tag: tag.clone(),
        fired: AtomicBool::new(false),
    };
    del_tagged_target_with_hook(
        TagTarget::Shop {
            owner_id: &owner_id,
        },
        &landing,
    )
    .await?;
    assert!(landing.fired.load(Ordering::SeqCst));
    assert_eq!(shop_nodes(&owner_id).await?, 0);
    assert_eq!(find_user_counts(&c_id).await.tagged, 0);
    let scores =
        <TagShop as TagCollection>::get_from_index(&owner_id, None, None, None, None, None, false)
            .await?
            .unwrap_or_default();
    assert!(scores.is_empty(), "shop label scores left: {scores:?}");
    let (taggers, _) = <TagShop as TaggersCollection>::get_from_index(
        vec![owner_id.as_str(), late.as_str()],
        None,
        None,
        None,
        None,
    )
    .await?;
    assert!(taggers.is_empty());
    assert!(!suggested(&late).await?);

    let after =
        nexus_watcher::events::handlers::tag::sync_put(tag.clone(), tagger_id, tag.create_id())
            .await;
    assert!(
        matches!(after, Err(EventProcessorError::MissingDependency { .. })),
        "{after:?}"
    );
    assert_eq!(find_user_counts(&c_id).await.tagged, 0);
    assert!(!suggested(&late).await?);

    test.cleanup_user(&c_kp).await?;
    test.cleanup_user(&owner_kp).await?;
    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn an_uncommitted_tag_holds_the_listing_deletion_until_it_commits() -> Result<()> {
    let mut test = WatcherTest::setup().await?;
    let (seller_kp, seller_id) = user(&mut test, "Lock:Seller").await?;
    let (a_kp, a_id) = user(&mut test, "Lock:TaggerA").await?;
    let listing = test_listing(
        &seller_id,
        "Lock boots",
        "fashion",
        PubkyAppListingCondition::New,
        1_000,
    );
    let (listing_id, _) = test.create_listing(&seller_kp, &listing).await?;
    let label = format!("lk{}", &seller_id[..8]);

    // A tag PUT's graph write, still uncommitted on its own connection. It
    // holds the listing's lock; its Redis writes follow its commit.
    let config = Neo4JConfig::default();
    let graph = neo4rs::Graph::new(config.uri.as_str(), &config.user, &config.password).await?;
    let mut txn = graph.start_txn().await?;
    txn.run(
        neo4rs::query(
            "MATCH (u:User {id: $tagger_id})
             MATCH (l:Listing {id: $listing_id, owner_id: $owner_id})
             CREATE (u)-[:TAGGED {id: $tag_id, label: $label, indexed_at: 0}]->(l)",
        )
        .param("tagger_id", a_id.as_str())
        .param("listing_id", listing_id.as_str())
        .param("owner_id", seller_id.as_str())
        .param("tag_id", format!("lock{}", &seller_id[..8]))
        .param("label", label.as_str()),
    )
    .await?;

    let (owner, id) = (seller_id.clone(), listing_id.clone());
    let cleanup = tokio::spawn(async move {
        del_tagged_target(TagTarget::Listing {
            owner_id: &owner,
            listing_id: &id,
        })
        .await
    });
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(20);
    loop {
        let blocked = graph_count(Query::new(
            "test_blocked_finalize",
            "SHOW TRANSACTIONS YIELD currentQuery, status
             WHERE currentQuery CONTAINS 'DETACH DELETE listing'
               AND status STARTS WITH 'Blocked'
             RETURN count(*) AS n",
        ))
        .await?;
        if blocked >= 1 {
            break;
        }
        assert!(
            !cleanup.is_finished(),
            "the deletion did not wait for the uncommitted tag"
        );
        assert!(
            tokio::time::Instant::now() < deadline,
            "the deletion never blocked on the listing"
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    // The deletion holds the node lock the tag's commit needs, so Neo4j
    // either refuses the deletion (another round, or a deadlock abort and
    // the event's retry, sweeps the committed tag) or aborts the tag, which
    // then indexes nothing. The deletion never removes a committed tag
    // silently.
    let committed = txn.commit().await.is_ok();
    if committed {
        UserCounts::increment(&a_id, "tagged", None).await?;
    }
    if let Err(error) = cleanup.await? {
        assert!(
            committed,
            "only a committed tag can fail the deletion: {error}"
        );
        assert!(
            error.to_string().contains("DeadlockDetected"),
            "unexpected deletion failure: {error}"
        );
        del_tagged_target(TagTarget::Listing {
            owner_id: &seller_id,
            listing_id: &listing_id,
        })
        .await?;
    }

    assert_eq!(listing_nodes(&seller_id, &listing_id).await?, 0);
    assert_eq!(find_user_counts(&a_id).await.tagged, 0);
    let edges = graph_count(
        Query::new(
            "test_orphan_tag",
            "MATCH (:User {id: $tagger_id})-[t:TAGGED {label: $label}]->() RETURN count(t) AS n",
        )
        .param("tagger_id", a_id.as_str())
        .param("label", label.as_str()),
    )
    .await?;
    assert_eq!(edges, 0);

    test.cleanup_user(&a_kp).await?;
    test.cleanup_user(&seller_kp).await?;
    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn an_overlapping_cleanup_that_read_a_settled_marker_counts_nothing() -> Result<()> {
    let mut test = WatcherTest::setup().await?;
    let (seller_kp, seller_id) = user(&mut test, "Overlap:Seller").await?;
    let (a_kp, a_id) = user(&mut test, "Overlap:TaggerA").await?;
    let listing = test_listing(
        &seller_id,
        "Overlap boots",
        "fashion",
        PubkyAppListingCondition::New,
        1_000,
    );
    let (listing_id, _) = test.create_listing(&seller_kp, &listing).await?;
    let label = format!("ov{}", &seller_id[..8]);
    let keep = format!("ok{}", &seller_id[..8]);
    tag(
        &mut test,
        &a_kp,
        listing_uri_builder(seller_id.clone(), listing_id.clone()),
        &label,
    )
    .await?;
    tag(&mut test, &a_kp, user_uri_builder(seller_id.clone()), &keep).await?;
    assert_eq!(find_user_counts(&a_id).await.tagged, 2);

    // This cleanup reads its marker; another settles it and deletes the
    // listing before this one counts it.
    let overlap = CleanupRunsAt {
        step: TargetTagCleanupStep::MarkersRead,
        owner_id: seller_id.clone(),
        listing_id: listing_id.clone(),
        fired: AtomicBool::new(false),
    };
    del_tagged_target_with_hook(
        TagTarget::Listing {
            owner_id: &seller_id,
            listing_id: &listing_id,
        },
        &overlap,
    )
    .await?;
    assert!(overlap.fired.load(Ordering::SeqCst));
    assert_eq!(find_user_counts(&a_id).await.tagged, 1);
    assert_eq!(listing_nodes(&seller_id, &listing_id).await?, 0);
    listing_is_untagged(&seller_id, &listing_id, &[&label]).await?;

    test.cleanup_user(&a_kp).await?;
    test.cleanup_user(&seller_kp).await?;
    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn a_label_tagged_during_the_autocomplete_prune_stays_suggested() -> Result<()> {
    let mut test = WatcherTest::setup().await?;
    let (owner_kp, owner_id) = user(&mut test, "Prune:Owner").await?;
    let (a_kp, a_id) = user(&mut test, "Prune:Tagger").await?;
    let label = format!("pr{}", &owner_id[..8]);
    TagSearch::put_to_index(std::slice::from_ref(&label)).await?;
    let tag = PubkyAppTag {
        uri: user_uri_builder(owner_id.clone()),
        label: label.clone(),
        created_at: Utc::now().timestamp_millis(),
    };
    let tagger_id = PubkyId::try_from(a_id.as_str()).map_err(anyhow::Error::msg)?;

    // The label is unused when checked; a profile tag is indexed with it
    // before the prune removes it.
    TagSearch::del_from_index_if_unused_with_hook(&label, async {
        nexus_watcher::events::handlers::tag::sync_put(
            tag.clone(),
            tagger_id.clone(),
            tag.create_id(),
        )
        .await
        .expect("profile tag indexes");
    })
    .await?;
    assert!(suggested(&label).await?, "a used label must stay suggested");

    nexus_watcher::events::handlers::tag::del(tagger_id, tag.create_id()).await?;
    assert!(!suggested(&label).await?, "unused again once untagged");
    test.cleanup_user(&a_kp).await?;
    test.cleanup_user(&owner_kp).await?;
    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn a_corrupt_counts_document_fails_before_its_claim() -> Result<()> {
    WatcherTest::setup().await?;
    let user_id = Keypair::random().public_key().to_string();
    let counts_prefix = <UserCounts as RedisOps>::prefix().await;
    // A counts key holding a set, not a JSON document.
    sets::put(&counts_prefix, &user_id, &["corrupt"], Some(60)).await?;
    let claim_key = format!("Cleanup:Tags:corrupt:{user_id}");

    let result = UserCounts::decrement_once(&user_id, "tagged", &claim_key, "marker", 60).await;
    assert!(result.is_err(), "{result:?}");
    let (_, claimed) =
        sets::check_member("Cleanup:Tags", &format!("corrupt:{user_id}"), "marker").await?;
    assert!(!claimed, "a failed decrement must not consume its claim");

    sets::del(&counts_prefix, &user_id, &["corrupt"]).await?;
    Ok(())
}
