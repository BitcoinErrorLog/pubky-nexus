use super::utils::test_drop;
use crate::event_processor::utils::watcher::WatcherTest;
use anyhow::Result;
use nexus_common::db::kv::SortOrder;
use nexus_common::db::RedisOps;
use nexus_common::models::marketplace::{
    DropDetails, DropStream, DropStreamBucket, DropStreamFilters, DROP_STARTS_KEY_PARTS,
};
use nexus_common::types::Pagination;
use pubky::Keypair;
use pubky_app_specs::{
    traits::HasIdPath, PubkyAppDropFormat, PubkyAppDropStockDisplay, PubkyAppMarketplaceDrop,
    PubkyAppObject, PubkyAppUser, Resource,
};

fn owner_filters(owner_id: &str) -> DropStreamFilters {
    DropStreamFilters {
        owner: Some(owner_id.to_string()),
        ..Default::default()
    }
}

fn bucket_filters(owner_id: &str, bucket: DropStreamBucket) -> DropStreamFilters {
    DropStreamFilters {
        owner: Some(owner_id.to_string()),
        bucket: Some(bucket),
    }
}

fn rfc3339_from_ms(value_ms: i64) -> String {
    chrono::DateTime::from_timestamp_millis(value_ms)
        .unwrap()
        .to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

/// Reads the global start-time sorted set entries within the given
/// inclusive score window (epoch milliseconds).
async fn starts_set_entries(min_ms: i64, max_ms: i64) -> Vec<(String, f64)> {
    DropStream::try_from_index_sorted_set(
        &DROP_STARTS_KEY_PARTS,
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
async fn test_homeserver_drop_lifecycle() -> Result<()> {
    let mut test = WatcherTest::setup().await?;

    // Step 1: Create a user
    let user_kp = Keypair::random();
    let user = PubkyAppUser {
        bio: Some("test_homeserver_drop_lifecycle".to_string()),
        image: None,
        links: None,
        name: "Watcher:Drop:User".to_string(),
        status: None,
    };
    let user_id = test.create_user(&user_kp, &user).await?;

    // A unique far-future start-time window so the global stream sorted set
    // can be asserted deterministically while other tests share it
    let base_ms = chrono::Utc::now().timestamp_millis()
        + 27_000_000_000
        + chrono::Utc::now().timestamp_micros() % 1_000_000;
    let starts_at = rfc3339_from_ms(base_ms);
    let ends_at = rfc3339_from_ms(base_ms + 86_400_000);

    // Step 2: Publish a drop record
    let drop = test_drop(&user_id, "Spring boot drop", &starts_at, Some(&ends_at));
    let (drop_id, drop_path) = test.create_drop(&user_kp, &drop).await?;

    // GRAPH_OP: Assert the drop node was written to the graph
    let graph_drop = DropDetails::get_from_graph(&user_id, &drop_id)
        .await
        .unwrap()
        .expect("The drop was not saved in the graph");
    assert_eq!(graph_drop.id, drop_id);
    assert_eq!(graph_drop.owner_id, user_id);
    assert_eq!(graph_drop.title, drop.title);
    assert_eq!(graph_drop.description, drop.description);
    assert_eq!(graph_drop.media_urls, drop.media);
    assert_eq!(graph_drop.format, PubkyAppDropFormat::Fcfs);
    assert_eq!(graph_drop.starts_at, starts_at);
    assert_eq!(graph_drop.ends_at.as_deref(), Some(ends_at.as_str()));
    assert_eq!(graph_drop.listing_ids, drop.listing_ids);
    assert_eq!(graph_drop.total_quantity, 500);
    assert_eq!(graph_drop.per_buyer_limit, 2);
    assert_eq!(graph_drop.stock_display, PubkyAppDropStockDisplay::Bands);
    assert_eq!(graph_drop.revision, 1);
    assert_eq!(graph_drop.created_at, drop.created_at);
    assert_eq!(graph_drop.updated_at, drop.updated_at);

    // INDEX_OP: Assert the drop details were indexed in Redis and round-trip
    // exactly to the graph projection
    let indexed_drop = DropDetails::get_from_index(&user_id, &drop_id)
        .await
        .unwrap()
        .expect("The drop details were not indexed");
    assert_eq!(indexed_drop, graph_drop);

    // STREAM_OP: the drop is scored by its declared start time in the global set
    let member = format!("{user_id}:{drop_id}");
    let entries = starts_set_entries(base_ms, base_ms).await;
    assert!(
        entries
            .iter()
            .any(|(key, score)| key == &member && *score == base_ms as f64),
        "The drop should be scored by its declared start time in the global sorted set"
    );

    // STREAM_OP: Assert the drop shows up in the per-owner stream
    let stream = DropStream::get_drops(
        owner_filters(&user_id),
        Pagination::default(),
        SortOrder::Ascending,
    )
    .await
    .unwrap()
    .expect("The drop stream should not be empty");
    assert_eq!(stream.0.len(), 1);
    assert_eq!(stream.0[0].id, drop_id);

    // Step 3: Update the drop record — retitle and reschedule it
    let rescheduled_ms = base_ms + 3_600_000;
    let mut updated_drop = drop.clone();
    updated_drop.drop_id = drop_id.clone();
    updated_drop.title = "Spring boot drop v2".to_string();
    updated_drop.starts_at = rfc3339_from_ms(rescheduled_ms);
    updated_drop.revision = 2;
    updated_drop.updated_at = "2025-01-02T00:00:00Z".to_string();
    test.put(&user_kp, &drop_path, &updated_drop).await?;

    let graph_drop = DropDetails::get_from_graph(&user_id, &drop_id)
        .await
        .unwrap()
        .expect("The updated drop was not found in the graph");
    assert_eq!(graph_drop.title, updated_drop.title);
    assert_eq!(graph_drop.revision, 2);

    let indexed_drop = DropDetails::get_from_index(&user_id, &drop_id)
        .await
        .unwrap()
        .expect("The updated drop details were not indexed");
    assert_eq!(indexed_drop.title, updated_drop.title);
    assert_eq!(indexed_drop.starts_at_ms(), Some(rescheduled_ms));

    // Rescheduling must move the sorted-set score, not duplicate the member
    let entries = starts_set_entries(base_ms, rescheduled_ms).await;
    let member_entries: Vec<&(String, f64)> =
        entries.iter().filter(|(key, _)| key == &member).collect();
    assert_eq!(
        member_entries.len(),
        1,
        "The edit must not duplicate the drop in the sorted set"
    );
    assert_eq!(member_entries[0].1, rescheduled_ms as f64);

    // The edit must not duplicate the drop in the per-owner stream either
    let stream = DropStream::get_drops(
        owner_filters(&user_id),
        Pagination::default(),
        SortOrder::Ascending,
    )
    .await
    .unwrap()
    .expect("The drop stream should not be empty");
    assert_eq!(stream.0.len(), 1);
    assert_eq!(stream.0[0].title, updated_drop.title);

    // Step 4: Delete the drop record
    test.del(&user_kp, &drop_path).await?;

    let graph_drop = DropDetails::get_from_graph(&user_id, &drop_id)
        .await
        .unwrap();
    assert!(
        graph_drop.is_none(),
        "The drop node should be deleted from the graph"
    );
    let indexed_drop = DropDetails::get_from_index(&user_id, &drop_id)
        .await
        .unwrap();
    assert!(
        indexed_drop.is_none(),
        "The drop details should be deleted from the index"
    );
    let stream = DropStream::get_drops(
        owner_filters(&user_id),
        Pagination::default(),
        SortOrder::Ascending,
    )
    .await
    .unwrap();
    assert!(
        stream.is_none(),
        "The drop should be removed from the stream"
    );
    let entries = starts_set_entries(base_ms, rescheduled_ms).await;
    assert!(
        !entries.iter().any(|(key, _)| key == &member),
        "A deleted drop should leave the global sorted set"
    );

    // Cleanup user
    test.cleanup_user(&user_kp).await?;

    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn test_drop_invalid_record_rejected() -> Result<()> {
    let mut test = WatcherTest::setup().await?;

    let user_kp = Keypair::random();
    let user = PubkyAppUser {
        bio: Some("test_drop_invalid_record_rejected".to_string()),
        image: None,
        links: None,
        name: "Watcher:InvalidDrop:User".to_string(),
        status: None,
    };
    let user_id = test.create_user(&user_kp, &user).await?;

    // A drop violating the title bound (1..=120 characters)
    let mut invalid_drop = test_drop(
        &user_id,
        "placeholder",
        "2026-02-01T00:00:00Z",
        Some("2026-02-02T00:00:00Z"),
    );
    let drop_id = format!("drop-{}", chrono::Utc::now().timestamp_micros());
    invalid_drop.drop_id = drop_id.clone();
    invalid_drop.title = "x".repeat(121);

    // The specs importer — the exact validation gate the event processor
    // runs — must reject the record for the bound violation
    let blob = serde_json::to_vec(&invalid_drop)?;
    let imported = PubkyAppObject::from_resource(&Resource::Drop(drop_id.clone()), &blob);
    assert!(
        imported.is_err(),
        "A drop with a 121-character title must fail validation"
    );

    // Publish it through the pipeline anyway; whether the processor surfaces
    // the ingest failure or parks it for retry, nothing may be indexed
    let drop_path: pubky::ResourcePath = PubkyAppMarketplaceDrop::create_path(&drop_id).parse()?;
    let _ = test.put(&user_kp, &drop_path, &invalid_drop).await;

    assert!(
        DropDetails::get_from_graph(&user_id, &drop_id)
            .await
            .unwrap()
            .is_none(),
        "An invalid drop must not reach the graph"
    );
    assert!(
        DropDetails::get_from_index(&user_id, &drop_id)
            .await
            .unwrap()
            .is_none(),
        "An invalid drop must not reach the index"
    );
    let stream = DropStream::get_drops(
        owner_filters(&user_id),
        Pagination::default(),
        SortOrder::Ascending,
    )
    .await
    .unwrap();
    assert!(
        stream.is_none(),
        "An invalid drop must not appear in the stream"
    );

    // Cleanup
    let _ = test.del(&user_kp, &drop_path).await;
    test.cleanup_user(&user_kp).await?;

    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn test_stream_drops_buckets_owner_filter_and_pagination() -> Result<()> {
    let mut test = WatcherTest::setup().await?;

    let user_kp = Keypair::random();
    let user = PubkyAppUser {
        bio: Some("test_stream_drops_buckets_owner_filter_and_pagination".to_string()),
        image: None,
        links: None,
        name: "Watcher:DropStream:User".to_string(),
        status: None,
    };
    let user_id = test.create_user(&user_kp, &user).await?;

    let now_ms = chrono::Utc::now().timestamp_millis();
    let hour_ms = 3_600_000;

    // Declared schedules relative to now covering every bucket:
    // - "Ended":       starts now-3h, ends now-1h  -> ended_window
    // - "LiveOpen":    starts now-2h, open-ended   -> live_window
    // - "LiveBounded": starts now-1h, ends now+1h  -> live_window
    // - "Upcoming":    starts now+1h, ends now+2h  -> upcoming
    let fixtures = [
        ("Ended", now_ms - 3 * hour_ms, Some(now_ms - hour_ms)),
        ("LiveOpen", now_ms - 2 * hour_ms, None),
        ("LiveBounded", now_ms - hour_ms, Some(now_ms + hour_ms)),
        ("Upcoming", now_ms + hour_ms, Some(now_ms + 2 * hour_ms)),
    ];

    for (title, starts_ms, ends_ms) in fixtures {
        let starts_at = rfc3339_from_ms(starts_ms);
        let ends_at = ends_ms.map(rfc3339_from_ms);
        let drop = test_drop(&user_id, title, &starts_at, ends_at.as_deref());
        test.create_drop(&user_kp, &drop).await?;
        // Guarantee unique publish micros for the generated drop ids
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }

    let stream_titles = |stream: &DropStream| -> Vec<String> {
        stream.0.iter().map(|drop| drop.title.clone()).collect()
    };

    // Owner stream from the index: soonest declared start first
    let stream = DropStream::get_drops(
        owner_filters(&user_id),
        Pagination::default(),
        SortOrder::Ascending,
    )
    .await
    .unwrap()
    .expect("The owner stream should not be empty");
    assert_eq!(
        stream_titles(&stream),
        vec!["Ended", "LiveOpen", "LiveBounded", "Upcoming"]
    );

    // The owner stream honors the descending order as well
    let stream = DropStream::get_drops(
        owner_filters(&user_id),
        Pagination::default(),
        SortOrder::Descending,
    )
    .await
    .unwrap()
    .expect("The owner stream should not be empty");
    assert_eq!(
        stream_titles(&stream),
        vec!["Upcoming", "LiveBounded", "LiveOpen", "Ended"]
    );

    // Pagination on the owner stream: skip the soonest start, take two
    let pagination = Pagination {
        skip: Some(1),
        limit: Some(2),
        ..Default::default()
    };
    let stream = DropStream::get_drops(owner_filters(&user_id), pagination, SortOrder::Ascending)
        .await
        .unwrap()
        .expect("The paginated owner stream should not be empty");
    assert_eq!(stream_titles(&stream), vec!["LiveOpen", "LiveBounded"]);

    // Bucket filters resolved through the graph. These are time-window
    // estimates computed from the declared schedule, so a record with a past
    // window lands in ended_window and an open-ended started drop stays in
    // live_window.
    let stream = DropStream::get_drops(
        bucket_filters(&user_id, DropStreamBucket::Upcoming),
        Pagination::default(),
        SortOrder::Ascending,
    )
    .await
    .unwrap()
    .expect("The upcoming bucket should not be empty");
    assert_eq!(stream_titles(&stream), vec!["Upcoming"]);

    let stream = DropStream::get_drops(
        bucket_filters(&user_id, DropStreamBucket::LiveWindow),
        Pagination::default(),
        SortOrder::Ascending,
    )
    .await
    .unwrap()
    .expect("The live_window bucket should not be empty");
    assert_eq!(stream_titles(&stream), vec!["LiveOpen", "LiveBounded"]);

    let stream = DropStream::get_drops(
        bucket_filters(&user_id, DropStreamBucket::EndedWindow),
        Pagination::default(),
        SortOrder::Ascending,
    )
    .await
    .unwrap()
    .expect("The ended_window bucket should not be empty");
    assert_eq!(stream_titles(&stream), vec!["Ended"]);

    // Pagination applies to graph-resolved bucket streams too
    let pagination = Pagination {
        skip: Some(1),
        limit: Some(1),
        ..Default::default()
    };
    let stream = DropStream::get_drops(
        bucket_filters(&user_id, DropStreamBucket::LiveWindow),
        pagination,
        SortOrder::Ascending,
    )
    .await
    .unwrap()
    .expect("The paginated live_window bucket should not be empty");
    assert_eq!(stream_titles(&stream), vec!["LiveBounded"]);

    // Another owner's drop never leaks into the owner-filtered stream
    let other_kp = Keypair::random();
    let other_user = PubkyAppUser {
        bio: Some("test_stream_drops_buckets_owner_filter_and_pagination_other".to_string()),
        image: None,
        links: None,
        name: "Watcher:DropStream:OtherUser".to_string(),
        status: None,
    };
    let other_id = test.create_user(&other_kp, &other_user).await?;
    let other_drop = test_drop(
        &other_id,
        "OtherOwners",
        &rfc3339_from_ms(now_ms + hour_ms),
        None,
    );
    test.create_drop(&other_kp, &other_drop).await?;

    let stream = DropStream::get_drops(
        owner_filters(&user_id),
        Pagination::default(),
        SortOrder::Ascending,
    )
    .await
    .unwrap()
    .expect("The owner stream should not be empty");
    assert_eq!(stream.0.len(), 4);
    assert!(stream.0.iter().all(|drop| drop.owner_id == user_id));

    let stream = DropStream::get_drops(
        bucket_filters(&user_id, DropStreamBucket::Upcoming),
        Pagination::default(),
        SortOrder::Ascending,
    )
    .await
    .unwrap()
    .expect("The upcoming bucket should not be empty");
    assert_eq!(stream_titles(&stream), vec!["Upcoming"]);

    // Cleanup users
    test.cleanup_user(&user_kp).await?;
    test.cleanup_user(&other_kp).await?;

    Ok(())
}
