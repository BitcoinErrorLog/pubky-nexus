use super::utils::{test_listing, test_shop};
use crate::event_processor::users::utils::find_user_counts;
use crate::event_processor::utils::watcher::{HomeserverHashIdPath, HomeserverPath, WatcherTest};
use anyhow::Result;
use chrono::Utc;
use nexus_common::db::kv::SortOrder;
use nexus_common::models::marketplace::{
    ListingStream, ListingStreamFilters, ListingStreamSorting, ListingsByTagSearch,
};
use nexus_common::models::tag::listing::TagListing;
use nexus_common::models::tag::search::TagSearch;
use nexus_common::models::tag::shop::TagShop;
use nexus_common::models::tag::traits::TagCollection;
use nexus_common::types::Pagination;
use pubky::Keypair;
use pubky_app_specs::{
    listing_uri_builder, shop_uri_builder, PubkyAppListingCondition, PubkyAppShop, PubkyAppTag,
    PubkyAppUser,
};

#[tokio_shared_rt::test(shared)]
async fn test_homeserver_tag_listing_lifecycle() -> Result<()> {
    let mut test = WatcherTest::setup().await?;

    // Step 1: Create a user
    let user_kp = Keypair::random();
    let user = PubkyAppUser {
        bio: Some("test_homeserver_tag_listing_lifecycle".to_string()),
        image: None,
        links: None,
        name: "Watcher:TagListing:User".to_string(),
        status: None,
    };
    let user_id = test.create_user(&user_kp, &user).await?;

    // Step 2: Publish a listing record
    let listing = test_listing(
        &user_id,
        "Tagged boots",
        "fashion",
        PubkyAppListingCondition::New,
        12_000,
    );
    let (listing_id, listing_path) = test.create_listing(&user_kp, &listing).await?;

    // Step 3: Tag the listing (community layer)
    let label = "handmade-listing";
    let tag = PubkyAppTag {
        uri: listing_uri_builder(user_id.clone(), listing_id.clone()),
        label: label.to_string(),
        created_at: Utc::now().timestamp_millis(),
    };
    let tag_path = tag.hs_path();
    test.put(&user_kp, &tag_path, tag).await?;

    // GRAPH_OP + CACHE_OP: the tag round-trips through TagListing::get_by_id
    let tag_details =
        TagListing::get_by_id(&user_id, Some(&listing_id), None, None, None, None, None)
            .await
            .unwrap()
            .expect("The listing tag collection should exist");
    assert_eq!(tag_details.len(), 1);
    assert_eq!(tag_details[0].label, label);
    assert_eq!(tag_details[0].taggers_count, 1);
    assert_eq!(tag_details[0].taggers[0], user_id);

    // CACHE_OP: the tag is served straight from the Redis index
    let cached =
        TagListing::get_from_index(&user_id, Some(&listing_id), None, None, None, None, false)
            .await
            .unwrap()
            .expect("The listing tag should be cached");
    assert_eq!(cached.len(), 1);
    assert_eq!(cached[0].label, label);
    assert_eq!(cached[0].taggers_count, 1);

    // INDEX_OP: the listing is in the label's global timeline
    // Sorted:Tags:Global:Listing:Timeline:{label}
    let by_tag = ListingsByTagSearch::get_by_label(label, Pagination::default())
        .await
        .unwrap()
        .expect("The by-tag listing timeline should exist");
    let listing_key = format!("{user_id}:{listing_id}");
    assert!(by_tag.iter().any(|entry| entry.listing_key == listing_key));

    // INDEX_OP: the tagger's global "tagged" count is incremented
    let user_counts = find_user_counts(&user_id).await;
    assert_eq!(user_counts.tagged, 1);

    // INDEX_OP: the label joined the tag autocomplete suggestions
    let suggestions = TagSearch::get_by_label(label, &Pagination::default()).await?;
    assert!(suggestions.is_some_and(|x| !x.is_empty()));

    // STREAM_OP: the single-tag index path of the listing stream finds the listing
    let filters = ListingStreamFilters {
        tags: Some(vec![label.to_string()]),
        ..Default::default()
    };
    let stream = ListingStream::get_listings(
        filters,
        Pagination::default(),
        SortOrder::Descending,
        ListingStreamSorting::Timeline,
    )
    .await
    .unwrap()
    .expect("The by-tag listing stream should not be empty");
    assert!(stream.0.iter().any(|entry| entry.id == listing_id));

    // STREAM_OP: combined with another filter the stream resolves through the graph
    let filters = ListingStreamFilters {
        tags: Some(vec![label.to_string()]),
        condition: Some(PubkyAppListingCondition::New),
        ..Default::default()
    };
    let stream = ListingStream::get_listings(
        filters,
        Pagination::default(),
        SortOrder::Descending,
        ListingStreamSorting::Timeline,
    )
    .await
    .unwrap()
    .expect("The graph-resolved by-tag stream should not be empty");
    assert!(stream.0.iter().any(|entry| entry.id == listing_id));

    // Step 4: Delete the tag; every index is cleaned up
    test.del(&user_kp, &tag_path).await?;

    let cached =
        TagListing::get_from_index(&user_id, Some(&listing_id), None, None, None, None, false)
            .await
            .unwrap()
            .unwrap_or_default();
    assert!(
        cached.is_empty(),
        "The listing tag should be removed from the index"
    );

    let by_tag = ListingsByTagSearch::get_by_label(label, Pagination::default())
        .await
        .unwrap()
        .unwrap_or_default();
    assert!(
        !by_tag.iter().any(|entry| entry.listing_key == listing_key),
        "The listing should leave the label's global timeline"
    );

    let user_counts = find_user_counts(&user_id).await;
    assert_eq!(user_counts.tagged, 0);

    let suggestions = TagSearch::get_by_label(label, &Pagination::default()).await?;
    assert!(
        suggestions.is_none_or(|x| x.is_empty()),
        "The last use of the label should remove it from suggestions"
    );

    // Cleanup
    test.del(&user_kp, &listing_path).await?;
    test.cleanup_user(&user_kp).await?;

    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn test_homeserver_tag_shop_lifecycle() -> Result<()> {
    let mut test = WatcherTest::setup().await?;

    // Step 1: Create a user with a shop
    let user_kp = Keypair::random();
    let user = PubkyAppUser {
        bio: Some("test_homeserver_tag_shop_lifecycle".to_string()),
        image: None,
        links: None,
        name: "Watcher:TagShop:User".to_string(),
        status: None,
    };
    let user_id = test.create_user(&user_kp, &user).await?;

    let shop = test_shop(&user_id);
    let shop_path = PubkyAppShop::hs_path();
    test.put(&user_kp, &shop_path, &shop).await?;

    // Step 2: Tag the shop (community layer)
    let label = "trusted-shop";
    let tag = PubkyAppTag {
        uri: shop_uri_builder(user_id.clone()),
        label: label.to_string(),
        created_at: Utc::now().timestamp_millis(),
    };
    let tag_path = tag.hs_path();
    test.put(&user_kp, &tag_path, tag).await?;

    // GRAPH_OP + CACHE_OP: the tag round-trips through TagShop::get_by_id
    let tag_details = TagShop::get_by_id(&user_id, None, None, None, None, None, None)
        .await
        .unwrap()
        .expect("The shop tag collection should exist");
    assert_eq!(tag_details.len(), 1);
    assert_eq!(tag_details[0].label, label);
    assert_eq!(tag_details[0].taggers_count, 1);
    assert_eq!(tag_details[0].taggers[0], user_id);

    // CACHE_OP: the tag is served straight from the Redis index
    let cached = TagShop::get_from_index(&user_id, None, None, None, None, None, false)
        .await
        .unwrap()
        .expect("The shop tag should be cached");
    assert_eq!(cached.len(), 1);
    assert_eq!(cached[0].label, label);

    // INDEX_OP: the tagger's global "tagged" count is incremented
    let user_counts = find_user_counts(&user_id).await;
    assert_eq!(user_counts.tagged, 1);

    // Step 3: Delete the tag; the indexes are cleaned up
    test.del(&user_kp, &tag_path).await?;

    let cached = TagShop::get_from_index(&user_id, None, None, None, None, None, false)
        .await
        .unwrap()
        .unwrap_or_default();
    assert!(
        cached.is_empty(),
        "The shop tag should be removed from the index"
    );

    let user_counts = find_user_counts(&user_id).await;
    assert_eq!(user_counts.tagged, 0);

    // Cleanup
    test.del(&user_kp, &shop_path).await?;
    test.cleanup_user(&user_kp).await?;

    Ok(())
}
