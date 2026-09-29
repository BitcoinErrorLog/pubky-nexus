use crate::events::EventProcessorError;

use nexus_common::db::queries::get::user_is_safe_to_delete;
use nexus_common::db::{execute_graph_operation_in, start_graph_txn, OperationOutcome};
use nexus_common::models::{
    traits::Collection,
    user::{UserCounts, UserDetails, UserSearch, USER_DELETED_SENTINEL},
};
use pubky_app_specs::{PubkyAppUser, PubkyId};
use tracing::debug;

use super::utils::{finish_txn, NoopTargetDeleteHook, TargetDeleteHook, TargetDeleteStep};

pub async fn sync_put(user: PubkyAppUser, user_id: PubkyId) -> Result<(), EventProcessorError> {
    debug!("Indexing new user profile: {}", user_id);

    // Step 1: Create `UserDetails` object
    let user_details = UserDetails::from_homeserver(user, &user_id);

    // Step 2: Save to graph
    user_details.put_to_graph().await?;

    // Step 3: Run in parallel the cache process: SAVE TO INDEX
    let indexing_results = tokio::join!(
        async {
            UserSearch::put_to_index(&[&user_details]).await?;
            Ok::<(), EventProcessorError>(())
        },
        async {
            // TODO: Use SCARD on a set for unique tag count to avoid race conditions in parallel processing
            // If new user (no existing counts), save a new `UserCounts`
            if UserCounts::get_from_index(&user_id).await?.is_none() {
                UserCounts::default().put_to_index(&user_id).await?;
            }
            Ok::<(), EventProcessorError>(())
        },
        async {
            UserDetails::put_to_index(&[&user_details.id], vec![Some(user_details.clone())])
                .await?;
            Ok::<(), EventProcessorError>(())
        }
    );

    indexing_results.0?;
    indexing_results.1?;
    indexing_results.2?;
    Ok(())
}

pub async fn del(user_id: PubkyId) -> Result<(), EventProcessorError> {
    del_with_hook(user_id, &NoopTargetDeleteHook).await
}

/// [`del`] with a deterministic interleaving point. Production callers use
/// [`del`].
#[doc(hidden)]
pub async fn del_with_hook(
    user_id: PubkyId,
    hook: &dyn TargetDeleteHook,
) -> Result<(), EventProcessorError> {
    debug!("Deleting user profile:  {}", user_id);

    // 1. Graph query to check if there is any edge at all to this user. It takes the user's write lock
    // and the transaction holds it until the user is deleted: a tag that commits first is counted,
    // and one that starts later waits and finds no user.
    let query = user_is_safe_to_delete(&user_id);
    let mut txn = start_graph_txn().await?;
    let outcome = execute_graph_operation_in(&mut txn, query)
        .await
        .map_err(EventProcessorError::graph_query_failed);

    // 2. If there is no relationships (OperationOutcome::CreatedOrDeleted), delete from graph and redis.
    // 3. But if there is any relationship (OperationOutcome::Updated), then we simply update the user with empty profile
    // and keyword username [DELETED].
    // A deleted user is a user whose profile is empty and has username `"[DELETED]"`
    match outcome {
        Ok(OperationOutcome::CreatedOrDeleted) => {
            let result = async {
                hook.at(TargetDeleteStep::Checked).await?;
                // UserSearch::delete reads UserDetails from the index to find the username,
                // so it must complete before UserDetails::delete runs.
                UserSearch::delete(&user_id).await?;
                super::tag::purge_deleted_user_tags(&mut txn, &user_id).await?;
                let indexing_results = tokio::join!(
                    UserDetails::delete(&mut txn, &user_id),
                    UserCounts::delete(&user_id)
                );
                indexing_results.0?;
                indexing_results.1?;
                Ok::<(), EventProcessorError>(())
            }
            .await;
            finish_txn(txn, result).await?;
        }
        Ok(OperationOutcome::Updated) => {
            finish_txn(txn, Ok(())).await?;
            let deleted_user = PubkyAppUser {
                name: USER_DELETED_SENTINEL.to_string(),
                bio: None,
                status: None,
                links: None,
                image: None,
            };

            sync_put(deleted_user, user_id).await?;
        }
        Ok(OperationOutcome::MissingDependency) => {
            finish_txn(txn, Err::<(), _>(EventProcessorError::SkipIndexing)).await?;
        }
        Err(error) => {
            finish_txn(txn, Err::<(), _>(error)).await?;
        }
    }

    // TODO notifications for deleted user

    Ok(())
}
