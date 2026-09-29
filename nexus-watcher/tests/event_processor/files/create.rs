use crate::event_processor::utils::watcher::{
    assert_file_details, retrieve_and_handle_event_line, WatcherTest,
};
use anyhow::Result;
use chrono::Utc;
use nexus_common::models::event::Event;
use nexus_common::models::file::FileDetails;
use nexus_common::models::traits::Collection;
use pubky::Keypair;
use pubky_app_specs::traits::{HasIdPath, HashId};
use pubky_app_specs::{blob_uri_builder, PubkyAppBlob, PubkyAppFile, PubkyAppUser};
use std::path::Path;

#[tokio_shared_rt::test(shared)]
async fn test_put_pubkyapp_file() -> Result<()> {
    // Arrange
    let mut test = WatcherTest::setup().await?;

    let user_kp = Keypair::random();
    let user = PubkyAppUser {
        bio: None,
        image: None,
        links: None,
        name: "Test User".to_string(),
        status: None,
    };

    let user_id = test.create_user(&user_kp, &user).await?;

    let blob_data = "Hello World!".to_string();
    let blob = PubkyAppBlob::new(blob_data.as_bytes().to_vec());
    let blob_id = blob.create_id();
    let blob_relative_url = PubkyAppBlob::create_path(&blob_id);
    let blob_absolute_url = blob_uri_builder(user_id.clone(), blob_id);

    let (_, events_in_redis_before) = Event::get_events_from_redis(None, 100_000).await.unwrap();

    test.create_file_from_body(&user_kp, blob_relative_url.as_str(), blob.0.clone())
        .await?;

    // Act
    let file = PubkyAppFile {
        name: "myfile".to_string(),
        content_type: "text/plain".to_string(),
        src: blob_absolute_url.clone(),
        size: blob.0.len(),
        created_at: Utc::now().timestamp_millis(),
    };

    let (file_id, _) = test.create_file(&user_kp, &file).await?;

    let result_file = assert_file_details(&user_id, &file_id, &blob_absolute_url, &file).await;

    // Assert: Ensure it's created
    let blob_static_path = format!("./static/files/{}", result_file.urls.main.clone());
    assert!(
        Path::new(&blob_static_path).exists(),
        "File have to exist after PUT event"
    );
    let (_, events_in_redis_after) = Event::get_events_from_redis(None, 100_000).await.unwrap();
    assert!(events_in_redis_after > events_in_redis_before);

    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn missing_file_blob_is_gone_not_a_retryable_put_failure() -> Result<()> {
    let mut test = WatcherTest::setup().await?;
    let user_kp = Keypair::random();
    let user_id = test
        .create_user(
            &user_kp,
            &PubkyAppUser {
                bio: None,
                image: None,
                links: None,
                name: "Watcher:File:GoneBlob".to_string(),
                status: None,
            },
        )
        .await?;

    let missing_blob = PubkyAppBlob::new(b"never published".to_vec());
    let missing_blob_uri = blob_uri_builder(user_id.clone(), missing_blob.create_id());
    let file = PubkyAppFile {
        name: "gone.txt".to_string(),
        content_type: "text/plain".to_string(),
        src: missing_blob_uri,
        size: 15,
        created_at: Utc::now().timestamp_millis(),
    };
    let moderation = test.event_processor_runner.moderation.clone();
    let mut test = test.remove_event_processing().await;
    let (file_id, file_path) = test.create_file(&user_kp, &file).await?;
    let event_line = format!("PUT pubky://{user_id}{file_path}");

    retrieve_and_handle_event_line(&event_line, moderation)
        .await
        .expect("a missing file blob is skipped");
    assert!(
        FileDetails::get_by_ids(&[&[user_id.as_str(), file_id.as_str()]])
            .await?
            .into_iter()
            .all(|details| details.is_none()),
        "a file whose blob is gone must not be indexed"
    );

    test.del(&user_kp, &file_path).await?;
    test.cleanup_user(&user_kp).await?;
    Ok(())
}
