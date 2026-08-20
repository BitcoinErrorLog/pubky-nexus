use super::utils::{test_auction_listing, test_listing};
use crate::event_processor::utils::watcher::WatcherTest;
use anyhow::Result;
use nexus_common::db::kv::SortOrder;
use nexus_common::db::RedisOps;
use nexus_common::models::marketplace::{
    ListingDetails, ListingSaleFormat, ListingStream, ListingStreamFilters, ListingStreamSorting,
    LISTING_AUCTION_ENDS_KEY_PARTS,
};
use nexus_common::types::Pagination;
use pubky::Keypair;
use pubky_app_specs::{
    PubkyAppListingCondition, PubkyAppListingSale, PubkyAppListingState, PubkyAppMoney,
    PubkyAppUser,
};

fn seller_filters(seller_id: &str) -> ListingStreamFilters {
    ListingStreamFilters {
        seller_id: Some(seller_id.to_string()),
        ..Default::default()
    }
}

fn rfc3339_ms(value: &str) -> i64 {
    chrono::DateTime::parse_from_rfc3339(value)
        .unwrap()
        .timestamp_millis()
}

/// Reads the auction end-time sorted set entries within the given
/// inclusive score window (epoch milliseconds).
async fn auction_set_entries(min_ms: i64, max_ms: i64) -> Vec<(String, f64)> {
    ListingStream::try_from_index_sorted_set(
        &LISTING_AUCTION_ENDS_KEY_PARTS,
        Some(max_ms as f64),
        Some(min_ms as f64),
        None,
        None,
        SortOrder::Descending,
        None,
    )
    .await
    .unwrap()
    .unwrap_or_default()
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
        ListingStreamSorting::Timeline,
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
        ListingStreamSorting::Timeline,
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
        ListingStreamSorting::Timeline,
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
        ListingStreamSorting::Timeline,
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
    let stream = ListingStream::get_listings(
        seller_filters(&user_id),
        pagination,
        SortOrder::Descending,
        ListingStreamSorting::Timeline,
    )
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
    let stream = ListingStream::get_listings(
        filters,
        Pagination::default(),
        SortOrder::Descending,
        ListingStreamSorting::Timeline,
    )
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
    let stream = ListingStream::get_listings(
        filters,
        Pagination::default(),
        SortOrder::Descending,
        ListingStreamSorting::Timeline,
    )
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
    let stream = ListingStream::get_listings(
        filters,
        pagination,
        SortOrder::Descending,
        ListingStreamSorting::Timeline,
    )
    .await
    .unwrap()
    .expect("The paginated graph stream should not be empty");
    assert_eq!(stream.0.len(), 1);
    assert_eq!(stream.0[0].title, "Jacket");

    // Cleanup user
    test.cleanup_user(&user_kp).await?;

    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn test_homeserver_auction_listing_roundtrip() -> Result<()> {
    let mut test = WatcherTest::setup().await?;

    let user_kp = Keypair::random();
    let user = PubkyAppUser {
        bio: Some("test_homeserver_auction_listing_roundtrip".to_string()),
        image: None,
        links: None,
        name: "Watcher:AuctionListing:User".to_string(),
        status: None,
    };
    let user_id = test.create_user(&user_kp, &user).await?;

    let starts_at = "2025-01-03T00:00:00Z";
    let ends_at = "2025-01-10T00:00:00Z";
    let listing = test_auction_listing(
        &user_id,
        "Vintage watch",
        "collectibles",
        starts_at,
        ends_at,
    );
    let (listing_id, listing_path) = test.create_listing(&user_kp, &listing).await?;

    // GRAPH_OP: the auction terms are written to the graph node
    let graph_listing = ListingDetails::get_from_graph(&user_id, &listing_id)
        .await
        .unwrap()
        .expect("The auction listing was not saved in the graph");
    assert_eq!(graph_listing.sale_format, ListingSaleFormat::Auction);
    assert_eq!(
        graph_listing.price_amount_minor, 1_000,
        "The primary price of an auction is its starting price"
    );
    assert_eq!(graph_listing.auction_starts_at.as_deref(), Some(starts_at));
    assert_eq!(graph_listing.auction_ends_at.as_deref(), Some(ends_at));
    assert_eq!(graph_listing.auction_reserve_price_minor, Some(2_000));
    assert_eq!(graph_listing.auction_buy_now_price_minor, Some(10_000));
    assert_eq!(graph_listing.auction_minimum_increment_minor, Some(100));

    // INDEX_OP: the auction terms round-trip through the Redis details JSON
    let indexed_listing = ListingDetails::get_from_index(&user_id, &listing_id)
        .await
        .unwrap()
        .expect("The auction listing details were not indexed");
    assert_eq!(indexed_listing, graph_listing);

    let ends_at_ms = rfc3339_ms(ends_at);
    assert_eq!(indexed_listing.auction_ends_at_ms(), Some(ends_at_ms));

    // STREAM_OP: the listing is scored by its end time in the auction sorted set
    let member = format!("{user_id}:{listing_id}");
    let entries = auction_set_entries(ends_at_ms, ends_at_ms).await;
    assert!(
        entries
            .iter()
            .any(|(key, score)| key == &member && *score == ends_at_ms as f64),
        "The auction listing should be scored by its end time in the auction sorted set"
    );

    // Step: extend the auction; the sorted-set score must follow the new end time
    let extended_ends_at = "2025-01-12T00:00:00Z";
    let mut updated_listing = listing.clone();
    updated_listing.listing_id = listing_id.clone();
    if let PubkyAppListingSale::Auction { ends_at, .. } = &mut updated_listing.sale {
        *ends_at = extended_ends_at.to_string();
    }
    updated_listing.revision = 2;
    updated_listing.updated_at = "2025-01-02T00:00:00Z".to_string();
    test.put(&user_kp, &listing_path, &updated_listing).await?;

    let indexed_listing = ListingDetails::get_from_index(&user_id, &listing_id)
        .await
        .unwrap()
        .expect("The updated auction listing details were not indexed");
    assert_eq!(
        indexed_listing.auction_ends_at.as_deref(),
        Some(extended_ends_at)
    );
    let extended_ends_at_ms = rfc3339_ms(extended_ends_at);
    let entries = auction_set_entries(extended_ends_at_ms, extended_ends_at_ms).await;
    assert!(
        entries
            .iter()
            .any(|(key, score)| key == &member && *score == extended_ends_at_ms as f64),
        "Editing the auction end time should refresh the sorted-set score"
    );

    // Step: edit into a fixed-price sale; the auction terms are cleared and the
    // listing leaves the auction sorted set
    let mut fixed_listing = updated_listing.clone();
    fixed_listing.sale = PubkyAppListingSale::FixedPrice {
        unit_price: PubkyAppMoney {
            amount_minor: 5_000,
            currency: "USD".to_string(),
            exponent: 2,
        },
        accepts_offers: false,
    };
    fixed_listing.revision = 3;
    test.put(&user_kp, &listing_path, &fixed_listing).await?;

    let indexed_listing = ListingDetails::get_from_index(&user_id, &listing_id)
        .await
        .unwrap()
        .expect("The fixed-price listing details were not indexed");
    assert_eq!(indexed_listing.sale_format, ListingSaleFormat::FixedPrice);
    assert!(indexed_listing.auction_starts_at.is_none());
    assert!(indexed_listing.auction_ends_at.is_none());
    assert!(indexed_listing.auction_reserve_price_minor.is_none());
    assert!(indexed_listing.auction_buy_now_price_minor.is_none());
    assert!(indexed_listing.auction_minimum_increment_minor.is_none());
    assert!(indexed_listing.auction_ends_at_ms().is_none());

    // The absent auction terms serialize as null, consistently with `region`
    let json = serde_json::to_value(&indexed_listing)?;
    for field in [
        "auction_starts_at",
        "auction_ends_at",
        "auction_reserve_price_minor",
        "auction_buy_now_price_minor",
        "auction_minimum_increment_minor",
    ] {
        assert_eq!(
            json.get(field),
            Some(&serde_json::Value::Null),
            "Field {field} should serialize as null for fixed-price listings"
        );
    }
    assert_eq!(json.get("region"), Some(&serde_json::Value::Null));

    let entries = auction_set_entries(ends_at_ms, extended_ends_at_ms).await;
    assert!(
        !entries.iter().any(|(key, _)| key == &member),
        "A listing edited into a fixed-price sale should leave the auction sorted set"
    );

    // Step: a deleted auction listing leaves the auction sorted set
    let second_listing =
        test_auction_listing(&user_id, "Second watch", "collectibles", starts_at, ends_at);
    let (second_listing_id, second_listing_path) =
        test.create_listing(&user_kp, &second_listing).await?;
    let second_member = format!("{user_id}:{second_listing_id}");
    let entries = auction_set_entries(ends_at_ms, ends_at_ms).await;
    assert!(entries.iter().any(|(key, _)| key == &second_member));

    test.del(&user_kp, &second_listing_path).await?;
    let entries = auction_set_entries(ends_at_ms, ends_at_ms).await;
    assert!(
        !entries.iter().any(|(key, _)| key == &second_member),
        "A deleted auction listing should leave the auction sorted set"
    );

    // Cleanup
    test.del(&user_kp, &listing_path).await?;
    test.cleanup_user(&user_kp).await?;

    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn test_stream_listings_sorted_by_auction_end() -> Result<()> {
    let mut test = WatcherTest::setup().await?;

    let user_kp = Keypair::random();
    let user = PubkyAppUser {
        bio: Some("test_stream_listings_sorted_by_auction_end".to_string()),
        image: None,
        links: None,
        name: "Watcher:AuctionStream:User".to_string(),
        status: None,
    };
    let user_id = test.create_user(&user_kp, &user).await?;

    // A category unique to this test run so graph-filter assertions are isolated
    let category = format!("cat-{}", chrono::Utc::now().timestamp_micros());

    // A unique far-future end-time window so the global auction stream can be
    // asserted deterministically while other tests share the sorted set
    let base_ms = chrono::Utc::now().timestamp_millis()
        + 9_000_000_000
        + chrono::Utc::now().timestamp_micros() % 1_000_000;
    let fixtures = [
        ("Middle", base_ms + 2_000),
        ("Soonest", base_ms + 500),
        ("Latest", base_ms + 3_500),
    ];

    for (title, ends_at_ms) in fixtures {
        let ends_at = chrono::DateTime::from_timestamp_millis(ends_at_ms)
            .unwrap()
            .to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
        let listing =
            test_auction_listing(&user_id, title, &category, "2025-01-01T00:00:00Z", &ends_at);
        test.create_listing(&user_kp, &listing).await?;
    }

    // A fixed-price listing in the same category must be excluded from end-time sorting
    let fixed_listing = test_listing(
        &user_id,
        "Fixed",
        &category,
        PubkyAppListingCondition::New,
        12_000,
    );
    test.create_listing(&user_kp, &fixed_listing).await?;

    let stream_titles = |stream: &ListingStream| -> Vec<String> {
        stream
            .0
            .iter()
            .map(|listing| listing.title.clone())
            .collect()
    };

    // Graph path: category filter with end-time sorting, ending soonest first
    let filters = ListingStreamFilters {
        category: Some(category.clone()),
        ..Default::default()
    };
    let stream = ListingStream::get_listings(
        filters.clone(),
        Pagination::default(),
        SortOrder::Ascending,
        ListingStreamSorting::EndsAt,
    )
    .await
    .unwrap()
    .expect("The end-time sorted stream should not be empty");
    assert_eq!(stream_titles(&stream), vec!["Soonest", "Middle", "Latest"]);

    // Graph path honors the descending order as well
    let stream = ListingStream::get_listings(
        filters,
        Pagination::default(),
        SortOrder::Descending,
        ListingStreamSorting::EndsAt,
    )
    .await
    .unwrap()
    .expect("The end-time sorted stream should not be empty");
    assert_eq!(stream_titles(&stream), vec!["Latest", "Middle", "Soonest"]);

    // A seller filter also resolves end-time sorting through the graph
    let stream = ListingStream::get_listings(
        seller_filters(&user_id),
        Pagination::default(),
        SortOrder::Ascending,
        ListingStreamSorting::EndsAt,
    )
    .await
    .unwrap()
    .expect("The seller end-time sorted stream should not be empty");
    assert_eq!(stream_titles(&stream), vec!["Soonest", "Middle", "Latest"]);

    // Redis path: no filters, bounded to this test's unique end-time window
    let pagination = Pagination {
        start: Some((base_ms + 4_000) as f64),
        end: Some(base_ms as f64),
        ..Default::default()
    };
    let stream = ListingStream::get_listings(
        ListingStreamFilters::default(),
        pagination,
        SortOrder::Ascending,
        ListingStreamSorting::EndsAt,
    )
    .await
    .unwrap()
    .expect("The global auction stream should not be empty");
    assert_eq!(stream_titles(&stream), vec!["Soonest", "Middle", "Latest"]);

    // Cleanup user
    test.cleanup_user(&user_kp).await?;

    Ok(())
}
