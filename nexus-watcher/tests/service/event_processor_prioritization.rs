use crate::event_processor::utils::default_moderation_tests;
use crate::service::utils::HS_IDS;
use crate::service::utils::{create_mock_event_processors, setup, MockEventProcessorRunner};
use anyhow::Result;
use nexus_common::models::homeserver::Homeserver;
use nexus_common::types::DynError;
use nexus_watcher::service::TEventProcessorRunner;
use nexus_watcher::service::{EventProcessorRunner, HomeserverPollBackoff};
use pubky_app_specs::PubkyId;
use std::path::PathBuf;
use std::sync::Arc;

#[tokio_shared_rt::test(shared)]
async fn test_event_processor_runner_default_homeserver_prioritization() -> Result<(), DynError> {
    // Initialize the test
    setup().await?;

    let runner = EventProcessorRunner {
        default_homeserver: PubkyId::try_from(HS_IDS[3]).unwrap(),
        shutdown_rx: tokio::sync::watch::channel(false).1,
        limit: 1000,
        monitored_homeservers_limit: HS_IDS.len(),
        files_path: PathBuf::from("/tmp/nexus-watcher-test"),
        tracer_name: String::from("unit-test-hs-list-test"),
        moderation: Arc::new(default_moderation_tests()),
        poll_backoff: Arc::new(HomeserverPollBackoff::default()),
    };

    // Persist the homeservers
    for hs_id in HS_IDS {
        let hs = Homeserver::new(PubkyId::try_from(hs_id).unwrap());
        hs.put_to_graph().await.unwrap();
    }

    // Prioritize the default homeserver
    let hs_ids = runner.homeservers_by_priority().await?;
    assert_eq!(hs_ids[0], HS_IDS[3]);

    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn rate_limited_homeserver_does_not_consume_the_next_poll_slot() -> Result<(), DynError> {
    setup().await?;

    let runner = EventProcessorRunner {
        default_homeserver: PubkyId::try_from(HS_IDS[0]).unwrap(),
        shutdown_rx: tokio::sync::watch::channel(false).1,
        limit: 1000,
        monitored_homeservers_limit: 100,
        files_path: PathBuf::from("/tmp/nexus-watcher-test"),
        tracer_name: String::from("unit-test-rate-limit-backoff"),
        moderation: Arc::new(default_moderation_tests()),
        poll_backoff: Arc::new(HomeserverPollBackoff::default()),
    };

    for hs_id in &HS_IDS[..2] {
        Homeserver::new(PubkyId::try_from(*hs_id).unwrap())
            .put_to_graph()
            .await
            .unwrap();
    }

    runner
        .poll_backoff
        .defer(HS_IDS[0], std::time::Duration::from_secs(5));

    let scheduled = runner.pre_run_all().await?;
    assert!(!scheduled.contains(&HS_IDS[0].to_string()));
    assert!(scheduled.contains(&HS_IDS[1].to_string()));
    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn test_mock_event_processor_runner_default_homeserver_prioritization() -> Result<(), DynError>
{
    // Initialize the test
    setup().await?;

    let event_processors = create_mock_event_processors(None, tokio::sync::watch::channel(false).1)
        .into_iter()
        .map(Arc::new)
        .collect();

    let runner = MockEventProcessorRunner {
        event_processors,
        monitored_homeservers_limit: 100,
        shutdown_rx: tokio::sync::watch::channel(false).1,
    };

    // Persist the homeservers
    for hs_id in HS_IDS {
        let hs = Homeserver::new(PubkyId::try_from(hs_id).unwrap());
        hs.put_to_graph().await.unwrap();
    }

    // Prioritize the default homeserver
    let hs_ids = runner.homeservers_by_priority().await?;
    assert_eq!(hs_ids[0], HS_IDS[0]);

    Ok(())
}
