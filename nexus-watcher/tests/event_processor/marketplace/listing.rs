use super::utils::test_listing;
use crate::event_processor::utils::watcher::WatcherTest;
use anyhow::Result;
use nexus_common::db::kv::SortOrder;
use nexus_common::models::marketplace::{
    ListingDetails, ListingSaleFormat, ListingStream, ListingStreamFilters,
};
use nexus_common::types::Pagination;
use pubky::Keypair;
use pubky_app_specs::{PubkyAppListingCondition, PubkyAppListingState, PubkyAppUser};

fn seller_filters(seller_id: &str) -> ListingStreamFilters {
    ListingStreamFilters {
        seller_id: Some(seller_id.to_string()),
        ..Default::default()
    }
}

#[tokio_shared_rt::test(shared)]
async fn test_homeserver_listing_lifecycle() -> Result<()> {
    let mut test = WatcherTest::setup().await?;

    // Step 1: Create a user
    let user_kp = Keypair::random();
    let user = PubkyAppUser {
        bio: Some("test_homeserver_listing_lifecycle".to_string()),
        image: None,
        links: None,
        name: "Watcher:Listing:User".to_string(),
        status: None,
    };
    let user_id = test.create_user(&user_kp, &user).await?;

    // Step 2: Publish a listing record
    let listing = test_listing(
        &user_id,
        "Hiking boots",
        "fashion",
        PubkyAppListingCondition::New,
        12_000,
    );
    let (listing_id, listing_path) = test.create_listing(&user_kp, &listing).await?;

    // GRAPH_OP: Assert the listing node was written to the graph
    let graph_listing = ListingDetails::get_from_graph(&user_id, &listing_id)
        .await
        .unwrap()
        .expect("The listing was not saved in the graph");
    assert_eq!(graph_listing.id, listing_id);
    assert_eq!(graph_listing.owner_id, user_id);
    assert_eq!(graph_listing.title, listing.title);
    assert_eq!(graph_listing.description, listing.description);
    assert_eq!(graph_listing.category_id, listing.category_id);
    assert_eq!(graph_listing.condition, PubkyAppListingCondition::New);
    assert_eq!(graph_listing.state, PubkyAppListingState::Active);
    assert_eq!(graph_listing.sale_format, ListingSaleFormat::FixedPrice);
    assert_eq!(graph_listing.price_amount_minor, 12_000);
    assert_eq!(graph_listing.price_currency, "USD");
    assert_eq!(graph_listing.price_exponent, 2);
    assert_eq!(graph_listing.tags, listing.tags);
    assert_eq!(graph_listing.media_urls.len(), 1);
    assert_eq!(graph_listing.country_code, listing.location.country_code);
    assert!(!graph_listing.adult_only);

    // INDEX_OP: Assert the listing details were indexed in Redis
    let indexed_listing = ListingDetails::get_from_index(&user_id, &listing_id)
        .await
        .unwrap()
        .expect("The listing details were not indexed");
    assert_eq!(indexed_listing.title, listing.title);

    // STREAM_OP: Assert the listing shows up in the per-seller stream
    let stream = ListingStream::get_listings(
        seller_filters(&user_id),
        Pagination::default(),
        SortOrder::Descending,
    )
    .await
    .unwrap()
    .expect("The listing stream should not be empty");
    assert_eq!(stream.0.len(), 1);
    assert_eq!(stream.0[0].id, listing_id);

    // Step 3: Update the listing record
    let mut updated_listing = listing.clone();
    updated_listing.listing_id = listing_id.clone();
    updated_listing.title = "Hiking boots v2".to_string();
    updated_listing.state = PubkyAppListingState::Paused;
    updated_listing.revision = 2;
    updated_listing.updated_at = "2025-01-02T00:00:00Z".to_string();
    test.put(&user_kp, &listing_path, &updated_listing).await?;

    let graph_listing = ListingDetails::get_from_graph(&user_id, &listing_id)
        .await
        .unwrap()
        .expect("The updated listing was not found in the graph");
    assert_eq!(graph_listing.title, updated_listing.title);
    assert_eq!(graph_listing.state, PubkyAppListingState::Paused);
    assert_eq!(graph_listing.revision, 2);

    let indexed_listing = ListingDetails::get_from_index(&user_id, &listing_id)
        .await
        .unwrap()
        .expect("The updated listing details were not indexed");
    assert_eq!(indexed_listing.title, updated_listing.title);
    assert_eq!(indexed_listing.state, PubkyAppListingState::Paused);

    // The edit must not duplicate the listing in the stream
    let stream = ListingStream::get_listings(
        seller_filters(&user_id),
        Pagination::default(),
        SortOrder::Descending,
    )
    .await
    .unwrap()
    .expect("The listing stream should not be empty");
    assert_eq!(stream.0.len(), 1);
    assert_eq!(stream.0[0].title, updated_listing.title);

    // Step 4: Delete the listing record
    test.del(&user_kp, &listing_path).await?;

    let graph_listing = ListingDetails::get_from_graph(&user_id, &listing_id)
        .await
        .unwrap();
    assert!(
        graph_listing.is_none(),
        "The listing node should be deleted from the graph"
    );
    let indexed_listing = ListingDetails::get_from_index(&user_id, &listing_id)
        .await
        .unwrap();
    assert!(
        indexed_listing.is_none(),
        "The listing details should be deleted from the index"
    );
    let stream = ListingStream::get_listings(
        seller_filters(&user_id),
        Pagination::default(),
        SortOrder::Descending,
    )
    .await
    .unwrap();
    assert!(
        stream.is_none(),
        "The listing should be removed from the stream"
    );

    // Cleanup user
    test.cleanup_user(&user_kp).await?;

    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn test_stream_listings_filters_and_pagination() -> Result<()> {
    let mut test = WatcherTest::setup().await?;

    let user_kp = Keypair::random();
    let user = PubkyAppUser {
        bio: Some("test_stream_listings_filters_and_pagination".to_string()),
        image: None,
        links: None,
        name: "Watcher:ListingStream:User".to_string(),
        status: None,
    };
    let user_id = test.create_user(&user_kp, &user).await?;

    // A category unique to this test run so graph-filter assertions are isolated
    let category = format!("cat-{}", chrono::Utc::now().timestamp_micros());

    let fixtures = [
        ("Boots", PubkyAppListingCondition::New, 12_000),
        ("Jacket", PubkyAppListingCondition::Good, 30_000),
        ("Hat", PubkyAppListingCondition::New, 5_000),
    ];

    let mut listing_ids = Vec::new();
    for (title, condition, amount_minor) in fixtures {
        let listing = test_listing(&user_id, title, &category, condition, amount_minor);
        let (listing_id, _) = test.create_listing(&user_kp, &listing).await?;
        listing_ids.push(listing_id);
        // Guarantee unique indexed_at scores so the stream ordering is deterministic
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }

    // Per-seller stream from the index: newest first
    let stream = ListingStream::get_listings(
        seller_filters(&user_id),
        Pagination::default(),
        SortOrder::Descending,
    )
    .await
    .unwrap()
    .expect("The seller stream should not be empty");
    assert_eq!(stream.0.len(), 3);
    assert_eq!(stream.0[0].title, "Hat");
    assert_eq!(stream.0[1].title, "Jacket");
    assert_eq!(stream.0[2].title, "Boots");

    // Pagination on the per-seller stream: skip the newest, take one
    let pagination = Pagination {
        skip: Some(1),
        limit: Some(1),
        ..Default::default()
    };
    let stream =
        ListingStream::get_listings(seller_filters(&user_id), pagination, SortOrder::Descending)
            .await
            .unwrap()
            .expect("The paginated stream should not be empty");
    assert_eq!(stream.0.len(), 1);
    assert_eq!(stream.0[0].title, "Jacket");

    // Condition filter resolved through the graph
    let filters = ListingStreamFilters {
        category: Some(category.clone()),
        condition: Some(PubkyAppListingCondition::New),
        ..Default::default()
    };
    let stream = ListingStream::get_listings(filters, Pagination::default(), SortOrder::Descending)
        .await
        .unwrap()
        .expect("The condition-filtered stream should not be empty");
    assert_eq!(stream.0.len(), 2);
    assert!(stream
        .0
        .iter()
        .all(|listing| listing.condition == PubkyAppListingCondition::New));

    // Price range filter resolved through the graph: 100.00 - 400.00 USD
    let filters = ListingStreamFilters {
        category: Some(category.clone()),
        currency: Some("USD".to_string()),
        min_price: Some(100.0),
        max_price: Some(400.0),
        ..Default::default()
    };
    let stream = ListingStream::get_listings(filters, Pagination::default(), SortOrder::Descending)
        .await
        .unwrap()
        .expect("The price-filtered stream should not be empty");
    assert_eq!(stream.0.len(), 2);
    let titles: Vec<&str> = stream
        .0
        .iter()
        .map(|listing| listing.title.as_str())
        .collect();
    assert_eq!(titles, vec!["Jacket", "Boots"]);

    // Pagination on the graph-filtered stream
    let filters = ListingStreamFilters {
        category: Some(category.clone()),
        ..Default::default()
    };
    let pagination = Pagination {
        skip: Some(1),
        limit: Some(1),
        ..Default::default()
    };
    let stream = ListingStream::get_listings(filters, pagination, SortOrder::Descending)
        .await
        .unwrap()
        .expect("The paginated graph stream should not be empty");
    assert_eq!(stream.0.len(), 1);
    assert_eq!(stream.0[0].title, "Jacket");

    // Cleanup user
    test.cleanup_user(&user_kp).await?;

    Ok(())
}
