use super::utils::{test_listing, test_shop};
use crate::event_processor::users::utils::find_user_counts;
use crate::event_processor::utils::watcher::{HomeserverHashIdPath, HomeserverPath, WatcherTest};
use anyhow::Result;
use async_trait::async_trait;
use chrono::Utc;
use nexus_common::models::event::EventProcessorError;
use nexus_common::models::marketplace::ListingsByTagSearch;
use nexus_common::models::tag::listing::TagListing;
use nexus_common::models::tag::search::TagSearch;
use nexus_common::models::tag::shop::TagShop;
use nexus_common::models::tag::traits::{TagCollection, TaggersCollection};
use nexus_common::types::Pagination;
use nexus_watcher::events::handlers::tag::{
    del_target_tags, del_target_tags_with_hook, TagTarget, TargetTagCleanupHook,
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
        TargetTagCleanupStep::TaggerCounted,
        TargetTagCleanupStep::EdgeDeleted,
        TargetTagCleanupStep::TaggersPurged,
        TargetTagCleanupStep::TimelinePurged,
        TargetTagCleanupStep::SearchPruned,
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
        let first = del_target_tags_with_hook(target, &failing).await;
        assert!(
            first.is_err(),
            "{step:?}: the injected failure must surface"
        );
        del_target_tags(target).await?;

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
    del_target_tags_with_hook(
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
    assert!(del_target_tags_with_hook(target, &failing).await.is_err());
    del_target_tags(target).await?;

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
