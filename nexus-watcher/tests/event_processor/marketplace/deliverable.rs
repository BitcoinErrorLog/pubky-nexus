use super::utils::test_listing;
use crate::event_processor::utils::watcher::{retrieve_and_handle_event_line, WatcherTest};
use anyhow::Result;
use nexus_common::models::event::{Event, EventType, MARKETPLACE_DELIVERABLES_PATH};
use nexus_common::models::marketplace::ListingDetails;
use nexus_watcher::events::retry::event::RetryEvent;
use pubky::{Keypair, ResourcePath};
use pubky_app_specs::{
    traits::{HasIdPath, TimestampId},
    PubkyAppFulfillmentMethod, PubkyAppListing, PubkyAppListingCondition, PubkyAppListingPackage,
    PubkyAppShippingOption, PubkyAppUser,
};

fn test_user(name: &str) -> PubkyAppUser {
    PubkyAppUser {
        bio: None,
        image: None,
        links: None,
        name: name.to_string(),
        status: None,
    }
}

async fn events_mention(uri: &str) -> Result<bool> {
    let (lines, _) = Event::get_events_from_redis(None, 100_000).await?;
    Ok(lines.iter().any(|line| line.contains(uri)))
}

#[tokio_shared_rt::test(shared)]
async fn deliverable_put_skipped_without_body_read() -> Result<()> {
    let mut test = WatcherTest::setup().await?;
    let seller_kp = Keypair::random();
    let seller_id = test
        .create_user(&seller_kp, &test_user("Watcher:Deliverable:Seller"))
        .await?;
    let moderation = test.event_processor_runner.moderation.clone();

    // A path with nothing behind it: any GET would come back 404 and fail the
    // event. Calibrate that on a listing path first.
    let absent_listing =
        format!("PUT pubky://{seller_id}/pub/pubky.app/marketplace/v1/listings/0000000000001");
    assert!(
        retrieve_and_handle_event_line(&absent_listing, moderation.clone())
            .await
            .is_err(),
        "calibration: a fetched path with no body fails"
    );
    let absent_deliverable =
        format!("PUT pubky://{seller_id}{MARKETPLACE_DELIVERABLES_PATH}absent0000000000/1");
    retrieve_and_handle_event_line(&absent_deliverable, moderation.clone())
        .await
        .expect("a deliverable PUT succeeds without a GET");

    // A real ciphertext-sized blob through the running watcher.
    let deliverable_path = format!("{MARKETPLACE_DELIVERABLES_PATH}9f1c0e7a2b6d4c85/1");
    let deliverable_uri = format!("pubky://{seller_id}{deliverable_path}");
    let blob: Vec<u8> = (0..(1024 * 1024)).map(|i| (i % 251) as u8).collect();
    test.create_file_from_body(&seller_kp, &deliverable_path, blob)
        .await?;
    test.ensure_event_processing_complete().await?;

    let retry_key = format!(
        "{}:{}",
        EventType::Put,
        RetryEvent::generate_index_key(&deliverable_uri).expect("deliverable retry key")
    );
    assert!(
        RetryEvent::get_from_index(&retry_key).await?.is_none(),
        "a deliverable PUT leaves no retry row"
    );
    assert!(
        !events_mention(&deliverable_uri).await?,
        "a deliverable PUT is not stored as a handled event"
    );

    let resource_path: ResourcePath = deliverable_path.parse()?;
    test.del(&seller_kp, &resource_path).await?;
    test.cleanup_user(&seller_kp).await?;
    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn deliverable_del_skipped() -> Result<()> {
    let mut test = WatcherTest::setup().await?;
    let seller_kp = Keypair::random();
    let seller_id = test
        .create_user(&seller_kp, &test_user("Watcher:DeliverableDel:Seller"))
        .await?;
    let moderation = test.event_processor_runner.moderation.clone();

    let deliverable_path = format!("{MARKETPLACE_DELIVERABLES_PATH}5a0d3e9c1f7b2468/2");
    let deliverable_uri = format!("pubky://{seller_id}{deliverable_path}");
    test.create_file_from_body(&seller_kp, &deliverable_path, vec![7u8; 4096])
        .await?;
    let resource_path: ResourcePath = deliverable_path.parse()?;
    test.del(&seller_kp, &resource_path).await?;

    retrieve_and_handle_event_line(&format!("DEL {deliverable_uri}"), moderation)
        .await
        .expect("a deliverable DEL succeeds");
    let retry_key = format!(
        "{}:{}",
        EventType::Del,
        RetryEvent::generate_index_key(&deliverable_uri).expect("deliverable retry key")
    );
    assert!(RetryEvent::get_from_index(&retry_key).await?.is_none());
    assert!(!events_mention(&deliverable_uri).await?);

    test.cleanup_user(&seller_kp).await?;
    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn digital_listing_without_lock_indexes() -> Result<()> {
    let mut test = WatcherTest::setup().await?;
    let seller_kp = Keypair::random();
    let seller_id = test
        .create_user(&seller_kp, &test_user("Watcher:DigitalListing:Seller"))
        .await?;

    let mut digital = test_listing(
        &seller_id,
        "Digital-only listing",
        "other",
        PubkyAppListingCondition::New,
        1_500,
    );
    digital.fulfillment_methods = vec![PubkyAppFulfillmentMethod::Digital];
    assert!(digital.digital_lock.is_none());
    digital.listing_id = digital.create_id();
    let digital_path: ResourcePath = PubkyAppListing::create_path(&digital.listing_id).parse()?;
    test.put(&seller_kp, &digital_path, &digital).await?;

    let indexed = ListingDetails::get_from_index(&seller_id, &digital.listing_id)
        .await?
        .expect("the digital listing was indexed");
    assert_eq!(
        indexed.fulfillment_methods,
        vec![PubkyAppFulfillmentMethod::Digital]
    );

    let mut all_four = test_listing(
        &seller_id,
        "Ship, pickup or digital",
        "other",
        PubkyAppListingCondition::New,
        2_500,
    );
    all_four.fulfillment_methods = vec![
        PubkyAppFulfillmentMethod::Physical,
        PubkyAppFulfillmentMethod::Shipping,
        PubkyAppFulfillmentMethod::Pickup,
        PubkyAppFulfillmentMethod::Digital,
    ];
    all_four.package = Some(PubkyAppListingPackage {
        weight_grams: 500,
        length_millimeters: 200,
        width_millimeters: 150,
        height_millimeters: 30,
    });
    all_four.shipping_options = vec![PubkyAppShippingOption::Free {
        id: "ship_01".to_string(),
        label: "Free".to_string(),
        estimated_min_days: 2,
        estimated_max_days: 7,
    }];
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    all_four.listing_id = all_four.create_id();
    assert_ne!(all_four.listing_id, digital.listing_id);
    let all_four_path: ResourcePath = PubkyAppListing::create_path(&all_four.listing_id).parse()?;
    test.put(&seller_kp, &all_four_path, &all_four).await?;

    let indexed = ListingDetails::get_from_index(&seller_id, &all_four.listing_id)
        .await?
        .expect("the four-method listing was indexed");
    assert_eq!(indexed.fulfillment_methods, all_four.fulfillment_methods);

    test.del(&seller_kp, &digital_path).await?;
    test.del(&seller_kp, &all_four_path).await?;
    test.cleanup_user(&seller_kp).await?;
    Ok(())
}
