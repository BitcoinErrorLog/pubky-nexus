mod drops;

use crate::utils::{get_request, invalid_get_request};
use anyhow::Result;
use axum::http::StatusCode;
use nexus_common::db::{exec_single_row, queries, OperationOutcome};
use nexus_common::models::marketplace::{ListingDetails, ListingSaleFormat, ShopDetails};
use nexus_common::models::user::UserDetails;
use pubky::Keypair;
use pubky_app_specs::{
    listing_uri_builder, shop_uri_builder, PubkyAppFulfillmentMethod, PubkyAppListingCondition,
    PubkyAppListingState, PubkyId,
};

/// Seeds a user node in the graph so shop and listing writes can attach to it.
async fn seed_user(seller_id: &str) -> Result<()> {
    let user_details = UserDetails {
        name: "WebApi:Marketplace:Seller".to_string(),
        bio: None,
        id: PubkyId::try_from(seller_id).map_err(anyhow::Error::msg)?,
        links: None,
        status: None,
        image: None,
        indexed_at: chrono::Utc::now().timestamp_millis(),
    };
    exec_single_row(queries::put::create_user(&user_details)?).await?;
    Ok(())
}

fn shop_details(seller_id: &str) -> ShopDetails {
    ShopDetails {
        owner_id: seller_id.to_string(),
        uri: shop_uri_builder(seller_id.to_string()),
        indexed_at: chrono::Utc::now().timestamp_millis(),
        name: "WebApi Marketplace Shop".to_string(),
        bio: "Test shop for the marketplace endpoint tests.".to_string(),
        country_code: "US".to_string(),
        region: Some("Oregon".to_string()),
        avatar_url: None,
        banner_url: None,
        shipping_policy: "Ships within 3 business days.".to_string(),
        return_policy: "Returns accepted within 30 days.".to_string(),
        vacation_mode: false,
        created_at: "2025-01-01T00:00:00Z".to_string(),
        updated_at: "2025-01-01T00:00:00Z".to_string(),
        revision: 1,
    }
}

#[allow(clippy::too_many_arguments)]
fn listing_details(
    seller_id: &str,
    listing_id: &str,
    title: &str,
    category_id: &str,
    condition: PubkyAppListingCondition,
    amount_minor: i64,
    indexed_at: i64,
) -> ListingDetails {
    ListingDetails {
        id: listing_id.to_string(),
        uri: listing_uri_builder(seller_id.to_string(), listing_id.to_string()),
        owner_id: seller_id.to_string(),
        indexed_at,
        state: PubkyAppListingState::Active,
        title: title.to_string(),
        description: "Test listing for the marketplace endpoint tests.".to_string(),
        category_id: category_id.to_string(),
        condition,
        tags: vec!["webapi-test".to_string()],
        country_code: "US".to_string(),
        region: None,
        media_urls: vec![format!(
            "pubky://{seller_id}/pub/pubky.app/marketplace/v1/media/image_01"
        )],
        sale_format: ListingSaleFormat::FixedPrice,
        price_amount_minor: amount_minor,
        price_currency: "USD".to_string(),
        price_exponent: 2,
        auction_starts_at: None,
        auction_ends_at: None,
        auction_reserve_price_minor: None,
        auction_buy_now_price_minor: None,
        auction_minimum_increment_minor: None,
        fulfillment_methods: vec![PubkyAppFulfillmentMethod::Pickup],
        adult_only: false,
        created_at: "2025-01-01T00:00:00Z".to_string(),
        updated_at: "2025-01-01T00:00:00Z".to_string(),
        revision: 1,
    }
}

/// Turns a fixed-price listing fixture into an auction with the given start
/// and end times. The auction money terms share the listing's USD asset.
fn with_auction_terms(
    mut listing: ListingDetails,
    starts_at: &str,
    ends_at: &str,
) -> ListingDetails {
    listing.sale_format = ListingSaleFormat::Auction;
    listing.auction_starts_at = Some(starts_at.to_string());
    listing.auction_ends_at = Some(ends_at.to_string());
    listing.auction_reserve_price_minor = Some(2_000);
    listing.auction_buy_now_price_minor = Some(10_000);
    listing.auction_minimum_increment_minor = Some(100);
    listing
}

/// Seeds a seller with a shop and three listings, returning the seller ID,
/// the unique category used by the fixtures and the listing IDs (oldest first).
async fn seed_marketplace() -> Result<(String, String, Vec<String>)> {
    // Make sure the test server (and its database connectors) are initialized
    crate::utils::host_url().await;

    let seller_id = Keypair::random().public_key().to_z32();
    let category = format!("cat-{}", chrono::Utc::now().timestamp_micros());
    seed_user(&seller_id).await?;

    let shop = shop_details(&seller_id);
    assert!(
        !matches!(
            shop.put_to_graph().await?,
            OperationOutcome::MissingDependency
        ),
        "The seller user should exist in the graph"
    );
    shop.put_to_index().await?;

    let base_indexed_at = chrono::Utc::now().timestamp_millis();
    let fixtures = [
        (
            "A0AAAAAAAAAAA",
            "Boots",
            PubkyAppListingCondition::New,
            12_000,
        ),
        (
            "B0AAAAAAAAAAA",
            "Jacket",
            PubkyAppListingCondition::Good,
            30_000,
        ),
        ("C0AAAAAAAAAAA", "Hat", PubkyAppListingCondition::New, 5_000),
    ];

    let mut listing_ids = Vec::new();
    for (i, (listing_id, title, condition, amount_minor)) in fixtures.into_iter().enumerate() {
        let listing = listing_details(
            &seller_id,
            listing_id,
            title,
            &category,
            condition,
            amount_minor,
            base_indexed_at + i as i64,
        );
        assert!(
            !matches!(
                listing.put_to_graph().await?,
                OperationOutcome::MissingDependency
            ),
            "The seller user should exist in the graph"
        );
        listing.put_to_index(false).await?;
        listing_ids.push(listing_id.to_string());
    }

    Ok((seller_id, category, listing_ids))
}

#[tokio_shared_rt::test(shared)]
async fn test_stream_listings_by_seller_with_pagination() -> Result<()> {
    let (seller_id, _, listing_ids) = seed_marketplace().await?;

    // Full seller stream, newest first
    let body = get_request(&format!("/v0/stream/listings?seller_id={seller_id}")).await?;
    let listings = body.as_array().expect("Listing stream should be an array");
    assert_eq!(listings.len(), 3);
    assert_eq!(listings[0]["title"], "Hat");
    assert_eq!(listings[1]["title"], "Jacket");
    assert_eq!(listings[2]["title"], "Boots");
    assert_eq!(listings[0]["id"], listing_ids[2].as_str());

    // Paginated seller stream: skip the newest, take one
    let body = get_request(&format!(
        "/v0/stream/listings?seller_id={seller_id}&skip=1&limit=1"
    ))
    .await?;
    let listings = body.as_array().expect("Listing stream should be an array");
    assert_eq!(listings.len(), 1);
    assert_eq!(listings[0]["title"], "Jacket");
    assert_eq!(listings[0]["id"], listing_ids[1].as_str());

    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn test_stream_listings_filters() -> Result<()> {
    let (_, category, _) = seed_marketplace().await?;

    // Condition filter is resolved through the graph
    let body = get_request(&format!(
        "/v0/stream/listings?category={category}&condition=new"
    ))
    .await?;
    let listings = body.as_array().expect("Listing stream should be an array");
    assert_eq!(listings.len(), 2);
    for listing in listings {
        assert_eq!(listing["condition"], "new");
        assert_eq!(listing["category_id"], category.as_str());
    }

    // Price range filter: 100.00 - 400.00 USD
    let body = get_request(&format!(
        "/v0/stream/listings?category={category}&currency=USD&min_price=100&max_price=400"
    ))
    .await?;
    let listings = body.as_array().expect("Listing stream should be an array");
    assert_eq!(listings.len(), 2);
    assert_eq!(listings[0]["title"], "Jacket");
    assert_eq!(listings[1]["title"], "Boots");

    // Sale format filter
    let body = get_request(&format!(
        "/v0/stream/listings?category={category}&sale_format=fixed_price"
    ))
    .await?;
    let listings = body.as_array().expect("Listing stream should be an array");
    assert_eq!(listings.len(), 3);

    // Price filters without a currency are rejected
    invalid_get_request("/v0/stream/listings?min_price=100", StatusCode::BAD_REQUEST).await?;

    // Country filter: the seller-declared item location, case-insensitive.
    // The fixtures all declare US, so US matches everything in the category
    // and HR matches nothing.
    let body = get_request(&format!(
        "/v0/stream/listings?category={category}&country=us"
    ))
    .await?;
    let listings = body.as_array().expect("Listing stream should be an array");
    assert_eq!(listings.len(), 3);
    for listing in listings {
        assert_eq!(listing["country_code"], "US");
    }
    let body = get_request(&format!(
        "/v0/stream/listings?category={category}&country=HR"
    ))
    .await?;
    assert_eq!(body.as_array().expect("array").len(), 0);

    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn test_shop_view() -> Result<()> {
    let (seller_id, _, _) = seed_marketplace().await?;

    let body = get_request(&format!("/v0/shop/{seller_id}")).await?;
    assert_eq!(body["details"]["owner_id"], seller_id.as_str());
    assert_eq!(body["details"]["name"], "WebApi Marketplace Shop");
    assert_eq!(body["details"]["country_code"], "US");

    let listings = body["listings"]
        .as_array()
        .expect("Shop view listings should be an array");
    assert_eq!(listings.len(), 3);
    assert_eq!(listings[0]["title"], "Hat");

    // Paginated shop view
    let body = get_request(&format!("/v0/shop/{seller_id}?skip=2&limit=1")).await?;
    let listings = body["listings"]
        .as_array()
        .expect("Shop view listings should be an array");
    assert_eq!(listings.len(), 1);
    assert_eq!(listings[0]["title"], "Boots");

    // Unknown seller yields 404
    let unknown_seller = Keypair::random().public_key().to_z32();
    invalid_get_request(&format!("/v0/shop/{unknown_seller}"), StatusCode::NOT_FOUND).await?;

    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn test_stream_listings_sorted_by_auction_end() -> Result<()> {
    // Make sure the test server (and its database connectors) are initialized
    crate::utils::host_url().await;

    let seller_id = Keypair::random().public_key().to_z32();
    let category = format!("cat-{}", chrono::Utc::now().timestamp_micros());
    seed_user(&seller_id).await?;

    // A unique far-future end-time window so the global auction stream can be
    // asserted deterministically while other tests share the sorted set
    let base_ms = chrono::Utc::now().timestamp_millis()
        + 18_000_000_000
        + chrono::Utc::now().timestamp_micros() % 1_000_000;
    let auction_fixtures = [
        ("D0AAAAAAAAAAA", "Middle", base_ms + 2_000),
        ("E0AAAAAAAAAAA", "Soonest", base_ms + 500),
        ("F0AAAAAAAAAAA", "Latest", base_ms + 3_500),
    ];

    let base_indexed_at = chrono::Utc::now().timestamp_millis();
    for (i, (listing_id, title, ends_at_ms)) in auction_fixtures.into_iter().enumerate() {
        let starts_at = "2025-01-01T00:00:00Z";
        let ends_at = chrono::DateTime::from_timestamp_millis(ends_at_ms)
            .unwrap()
            .to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
        let listing = with_auction_terms(
            listing_details(
                &seller_id,
                listing_id,
                title,
                &category,
                PubkyAppListingCondition::New,
                1_000,
                base_indexed_at + i as i64,
            ),
            starts_at,
            &ends_at,
        );
        assert!(
            !matches!(
                listing.put_to_graph().await?,
                OperationOutcome::MissingDependency
            ),
            "The seller user should exist in the graph"
        );
        listing.put_to_index(false).await?;
    }

    // A fixed-price listing in the same category must be excluded from end-time sorting
    let fixed_listing = listing_details(
        &seller_id,
        "G0AAAAAAAAAAA",
        "Fixed",
        &category,
        PubkyAppListingCondition::New,
        12_000,
        base_indexed_at + 3,
    );
    fixed_listing.put_to_graph().await?;
    fixed_listing.put_to_index(false).await?;

    // Graph path: category filter with end-time sorting, ending soonest first
    let body = get_request(&format!(
        "/v0/stream/listings?category={category}&sorting=ends_at&order=ascending"
    ))
    .await?;
    let listings = body.as_array().expect("Listing stream should be an array");
    assert_eq!(listings.len(), 3);
    assert_eq!(listings[0]["title"], "Soonest");
    assert_eq!(listings[1]["title"], "Middle");
    assert_eq!(listings[2]["title"], "Latest");

    // The auction terms are carried by the stream payload
    assert_eq!(listings[0]["sale_format"], "auction");
    assert_eq!(listings[0]["auction_reserve_price_minor"], 2_000);
    assert_eq!(listings[0]["auction_buy_now_price_minor"], 10_000);
    assert_eq!(listings[0]["auction_minimum_increment_minor"], 100);
    assert_eq!(listings[0]["auction_starts_at"], "2025-01-01T00:00:00Z");

    // Redis path: no filters, bounded to this test's unique end-time window
    let body = get_request(&format!(
        "/v0/stream/listings?sorting=ends_at&order=ascending&start={}&end={}",
        base_ms + 4_000,
        base_ms
    ))
    .await?;
    let listings = body.as_array().expect("Listing stream should be an array");
    assert_eq!(listings.len(), 3);
    assert_eq!(listings[0]["title"], "Soonest");
    assert_eq!(listings[1]["title"], "Middle");
    assert_eq!(listings[2]["title"], "Latest");

    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn test_listing_details_auction_terms() -> Result<()> {
    // Make sure the test server (and its database connectors) are initialized
    crate::utils::host_url().await;

    let seller_id = Keypair::random().public_key().to_z32();
    let category = format!("cat-{}", chrono::Utc::now().timestamp_micros());
    seed_user(&seller_id).await?;

    let starts_at = "2025-01-03T00:00:00Z";
    let ends_at = "2025-01-10T00:00:00Z";
    let auction_listing = with_auction_terms(
        listing_details(
            &seller_id,
            "H0AAAAAAAAAAA",
            "Vintage watch",
            &category,
            PubkyAppListingCondition::Excellent,
            1_000,
            chrono::Utc::now().timestamp_millis(),
        ),
        starts_at,
        ends_at,
    );
    auction_listing.put_to_graph().await?;
    auction_listing.put_to_index(false).await?;

    let body = get_request(&format!("/v0/listing/{seller_id}/H0AAAAAAAAAAA")).await?;
    assert_eq!(body["sale_format"], "auction");
    assert_eq!(body["auction_starts_at"], starts_at);
    assert_eq!(body["auction_ends_at"], ends_at);
    assert_eq!(body["auction_reserve_price_minor"], 2_000);
    assert_eq!(body["auction_buy_now_price_minor"], 10_000);
    assert_eq!(body["auction_minimum_increment_minor"], 100);

    // Fixed-price listings serialize the auction terms as null, like `region`
    let fixed_listing = listing_details(
        &seller_id,
        "J0AAAAAAAAAAA",
        "Boots",
        &category,
        PubkyAppListingCondition::New,
        12_000,
        chrono::Utc::now().timestamp_millis(),
    );
    fixed_listing.put_to_graph().await?;
    fixed_listing.put_to_index(false).await?;

    let body = get_request(&format!("/v0/listing/{seller_id}/J0AAAAAAAAAAA")).await?;
    assert_eq!(body["sale_format"], "fixed_price");
    for field in [
        "auction_starts_at",
        "auction_ends_at",
        "auction_reserve_price_minor",
        "auction_buy_now_price_minor",
        "auction_minimum_increment_minor",
    ] {
        assert_eq!(
            body.get(field),
            Some(&serde_json::Value::Null),
            "Field {field} should serialize as null for fixed-price listings"
        );
    }
    assert_eq!(body.get("region"), Some(&serde_json::Value::Null));

    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn test_listing_details_and_delete() -> Result<()> {
    let (seller_id, _, listing_ids) = seed_marketplace().await?;
    let listing_id = &listing_ids[0];

    let body = get_request(&format!("/v0/listing/{seller_id}/{listing_id}")).await?;
    assert_eq!(body["id"], listing_id.as_str());
    assert_eq!(body["owner_id"], seller_id.as_str());
    assert_eq!(body["title"], "Boots");
    assert_eq!(body["sale_format"], "fixed_price");
    assert_eq!(body["price_amount_minor"], 12_000);
    assert_eq!(body["price_currency"], "USD");

    // Delete the listing and verify it can no longer be read back
    ListingDetails::delete(&seller_id, listing_id).await?;
    invalid_get_request(
        &format!("/v0/listing/{seller_id}/{listing_id}"),
        StatusCode::NOT_FOUND,
    )
    .await?;

    // The deleted listing must also disappear from the seller stream
    let body = get_request(&format!("/v0/stream/listings?seller_id={seller_id}")).await?;
    let listings = body.as_array().expect("Listing stream should be an array");
    assert_eq!(listings.len(), 2);
    assert!(listings
        .iter()
        .all(|listing| listing["id"] != listing_id.as_str()));

    Ok(())
}
