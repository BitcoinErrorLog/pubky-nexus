use super::seed_user;
use crate::utils::{get_request, invalid_get_request};
use anyhow::Result;
use axum::http::StatusCode;
use nexus_common::db::OperationOutcome;
use nexus_common::models::marketplace::DropDetails;
use pubky::Keypair;
use pubky_app_specs::{drop_uri_builder, PubkyAppDropFormat, PubkyAppDropStockDisplay};

fn rfc3339_from_ms(value_ms: i64) -> String {
    chrono::DateTime::from_timestamp_millis(value_ms)
        .unwrap()
        .to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

fn drop_details(
    owner_id: &str,
    drop_id: &str,
    title: &str,
    starts_at: &str,
    ends_at: Option<&str>,
) -> DropDetails {
    DropDetails {
        id: drop_id.to_string(),
        uri: drop_uri_builder(owner_id.to_string(), drop_id.to_string()),
        owner_id: owner_id.to_string(),
        indexed_at: chrono::Utc::now().timestamp_millis(),
        revision: 1,
        title: title.to_string(),
        description: "Test drop for the marketplace endpoint tests.".to_string(),
        media_urls: vec![format!(
            "pubky://{owner_id}/pub/pubky.app/marketplace/v1/media/drop_banner"
        )],
        format: PubkyAppDropFormat::Fcfs,
        starts_at: starts_at.to_string(),
        ends_at: ends_at.map(str::to_string),
        listing_ids: vec!["listing_01".to_string(), "listing_02".to_string()],
        total_quantity: 500,
        per_buyer_limit: 2,
        stock_display: PubkyAppDropStockDisplay::Bands,
        created_at: "2025-01-01T00:00:00Z".to_string(),
        updated_at: "2025-01-01T00:00:00Z".to_string(),
    }
}

async fn index_drop(drop: &DropDetails) -> Result<()> {
    assert!(
        !matches!(
            drop.put_to_graph().await?,
            OperationOutcome::MissingDependency
        ),
        "The owner user should exist in the graph"
    );
    drop.put_to_index().await?;
    Ok(())
}

/// Seeds an owner with four drops whose declared schedules cover every
/// time-window bucket, returning the owner ID. Titles by schedule:
/// - "Ended":       starts now-3h, ends now-1h  -> ended_window
/// - "LiveOpen":    starts now-2h, open-ended   -> live_window
/// - "LiveBounded": starts now-1h, ends now+1h  -> live_window
/// - "Upcoming":    starts now+1h, ends now+2h  -> upcoming
async fn seed_drops() -> Result<String> {
    // Make sure the test server (and its database connectors) are initialized
    crate::utils::host_url().await;

    let owner_id = Keypair::random().public_key().to_z32();
    seed_user(&owner_id).await?;

    let now_ms = chrono::Utc::now().timestamp_millis();
    let hour_ms = 3_600_000;
    let fixtures = [
        (
            "A0AAAAAAAAAAA",
            "Ended",
            now_ms - 3 * hour_ms,
            Some(now_ms - hour_ms),
        ),
        ("B0AAAAAAAAAAA", "LiveOpen", now_ms - 2 * hour_ms, None),
        (
            "C0AAAAAAAAAAA",
            "LiveBounded",
            now_ms - hour_ms,
            Some(now_ms + hour_ms),
        ),
        (
            "D0AAAAAAAAAAA",
            "Upcoming",
            now_ms + hour_ms,
            Some(now_ms + 2 * hour_ms),
        ),
    ];

    for (drop_id, title, starts_ms, ends_ms) in fixtures {
        let starts_at = rfc3339_from_ms(starts_ms);
        let ends_at = ends_ms.map(rfc3339_from_ms);
        let drop = drop_details(&owner_id, drop_id, title, &starts_at, ends_at.as_deref());
        index_drop(&drop).await?;
    }

    Ok(owner_id)
}

#[tokio_shared_rt::test(shared)]
async fn test_stream_drops_by_owner_with_pagination() -> Result<()> {
    let owner_id = seed_drops().await?;

    // Full owner stream: soonest declared start first is the default order
    let body = get_request(&format!("/v0/stream/drops?owner={owner_id}")).await?;
    let drops = body.as_array().expect("Drop stream should be an array");
    assert_eq!(drops.len(), 4);
    assert_eq!(drops[0]["title"], "Ended");
    assert_eq!(drops[1]["title"], "LiveOpen");
    assert_eq!(drops[2]["title"], "LiveBounded");
    assert_eq!(drops[3]["title"], "Upcoming");

    // Explicit descending order flips the stream
    let body = get_request(&format!(
        "/v0/stream/drops?owner={owner_id}&order=descending"
    ))
    .await?;
    let drops = body.as_array().expect("Drop stream should be an array");
    assert_eq!(drops[0]["title"], "Upcoming");
    assert_eq!(drops[3]["title"], "Ended");

    // Paginated owner stream: skip the soonest start, take two
    let body = get_request(&format!("/v0/stream/drops?owner={owner_id}&skip=1&limit=2")).await?;
    let drops = body.as_array().expect("Drop stream should be an array");
    assert_eq!(drops.len(), 2);
    assert_eq!(drops[0]["title"], "LiveOpen");
    assert_eq!(drops[1]["title"], "LiveBounded");

    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn test_stream_drops_bucket_filters() -> Result<()> {
    let owner_id = seed_drops().await?;

    // The buckets are time-window estimates computed from the declared
    // schedule of the indexed record, not the transaction service's
    // authoritative drop state.
    let body = get_request(&format!(
        "/v0/stream/drops?owner={owner_id}&bucket=upcoming"
    ))
    .await?;
    let drops = body.as_array().expect("Drop stream should be an array");
    assert_eq!(drops.len(), 1);
    assert_eq!(drops[0]["title"], "Upcoming");

    let body = get_request(&format!(
        "/v0/stream/drops?owner={owner_id}&bucket=live_window"
    ))
    .await?;
    let drops = body.as_array().expect("Drop stream should be an array");
    assert_eq!(drops.len(), 2);
    assert_eq!(drops[0]["title"], "LiveOpen");
    assert_eq!(drops[1]["title"], "LiveBounded");
    // The open-ended live drop serializes its absent end time as null
    assert_eq!(drops[0].get("ends_at"), Some(&serde_json::Value::Null));

    let body = get_request(&format!(
        "/v0/stream/drops?owner={owner_id}&bucket=ended_window"
    ))
    .await?;
    let drops = body.as_array().expect("Drop stream should be an array");
    assert_eq!(drops.len(), 1);
    assert_eq!(drops[0]["title"], "Ended");

    // Pagination applies to bucket-filtered streams too
    let body = get_request(&format!(
        "/v0/stream/drops?owner={owner_id}&bucket=live_window&skip=1&limit=1"
    ))
    .await?;
    let drops = body.as_array().expect("Drop stream should be an array");
    assert_eq!(drops.len(), 1);
    assert_eq!(drops[0]["title"], "LiveBounded");

    // An unknown bucket value is rejected as invalid input
    invalid_get_request(
        &format!("/v0/stream/drops?owner={owner_id}&bucket=live"),
        StatusCode::BAD_REQUEST,
    )
    .await?;

    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn test_stream_drops_global_window() -> Result<()> {
    // Make sure the test server (and its database connectors) are initialized
    crate::utils::host_url().await;

    let owner_id = Keypair::random().public_key().to_z32();
    seed_user(&owner_id).await?;

    // A unique far-future start-time window so the global stream can be
    // asserted deterministically while other tests share the sorted set
    let base_ms = chrono::Utc::now().timestamp_millis()
        + 36_000_000_000
        + chrono::Utc::now().timestamp_micros() % 1_000_000;
    let fixtures = [
        ("E0AAAAAAAAAAA", "Middle", base_ms + 2_000),
        ("F0AAAAAAAAAAA", "Soonest", base_ms + 500),
        ("G0AAAAAAAAAAA", "Latest", base_ms + 3_500),
    ];

    for (drop_id, title, starts_ms) in fixtures {
        let starts_at = rfc3339_from_ms(starts_ms);
        let drop = drop_details(&owner_id, drop_id, title, &starts_at, None);
        index_drop(&drop).await?;
    }

    // Global stream (no owner filter), bounded to this test's unique window
    let body = get_request(&format!(
        "/v0/stream/drops?start={}&end={}",
        base_ms + 4_000,
        base_ms
    ))
    .await?;
    let drops = body.as_array().expect("Drop stream should be an array");
    assert_eq!(drops.len(), 3);
    assert_eq!(drops[0]["title"], "Soonest");
    assert_eq!(drops[1]["title"], "Middle");
    assert_eq!(drops[2]["title"], "Latest");

    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn test_drop_details_and_delete() -> Result<()> {
    let owner_id = seed_drops().await?;

    let body = get_request(&format!("/v0/drop/{owner_id}/D0AAAAAAAAAAA")).await?;
    assert_eq!(body["id"], "D0AAAAAAAAAAA");
    assert_eq!(body["owner_id"], owner_id.as_str());
    assert_eq!(body["title"], "Upcoming");
    assert_eq!(
        body["uri"],
        format!("pubky://{owner_id}/pub/pubky.app/marketplace/v1/drops/D0AAAAAAAAAAA")
    );
    assert_eq!(body["format"], "fcfs");
    assert_eq!(body["stock_display"], "bands");
    assert_eq!(body["total_quantity"], 500);
    assert_eq!(body["per_buyer_limit"], 2);
    assert_eq!(body["revision"], 1);
    let listing_ids = body["listing_ids"]
        .as_array()
        .expect("listing_ids should be an array");
    assert_eq!(listing_ids.len(), 2);
    assert_eq!(listing_ids[0], "listing_01");
    assert!(body["starts_at"].is_string());
    assert!(body["ends_at"].is_string());

    // The open-ended drop serves a null ends_at
    let body = get_request(&format!("/v0/drop/{owner_id}/B0AAAAAAAAAAA")).await?;
    assert_eq!(body["title"], "LiveOpen");
    assert_eq!(body.get("ends_at"), Some(&serde_json::Value::Null));

    // An unknown drop yields 404
    invalid_get_request(
        &format!("/v0/drop/{owner_id}/Z0AAAAAAAAAAA"),
        StatusCode::NOT_FOUND,
    )
    .await?;

    // Delete the drop and verify it can no longer be read back
    DropDetails::delete(&owner_id, "D0AAAAAAAAAAA").await?;
    invalid_get_request(
        &format!("/v0/drop/{owner_id}/D0AAAAAAAAAAA"),
        StatusCode::NOT_FOUND,
    )
    .await?;

    // The deleted drop must also disappear from the owner stream
    let body = get_request(&format!("/v0/stream/drops?owner={owner_id}")).await?;
    let drops = body.as_array().expect("Drop stream should be an array");
    assert_eq!(drops.len(), 3);
    assert!(drops.iter().all(|drop| drop["id"] != "D0AAAAAAAAAAA"));

    Ok(())
}
