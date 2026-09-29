use crate::events::retry::event::RetryEvent;
use crate::events::EventProcessorError;

use nexus_common::db::queries::get::post_is_safe_to_delete;
use nexus_common::db::{exec_single_row, OperationOutcome};
use nexus_common::db::{execute_graph_operation_in, queries, start_graph_txn, GraphTxn, RedisOps};
use nexus_common::models::homeserver::Homeserver;
use nexus_common::models::notification::{Notification, PostChangedSource, PostChangedType};
use nexus_common::models::post::{
    PostCounts, PostDetails, PostRelationships, PostStream, POST_TOTAL_ENGAGEMENT_KEY_PARTS,
};
use nexus_common::models::user::UserCounts;
use pubky_app_specs::{
    post_uri_builder, ParsedUri, PubkyAppPost, PubkyAppPostKind, PubkyId, Resource,
};
use tracing::debug;

use super::recovery::{recover, PostDeletion, Recovery};
use super::tag_index::{self, OwnedTarget, StepHook, Steps};
use super::utils::{
    finish_txn, post_relationships_is_reply, post_relationships_is_reply_in, NoopTargetDeleteHook,
    TargetDeleteHook, TargetDeleteStep,
};

pub async fn sync_put(
    post: PubkyAppPost,
    author_id: PubkyId,
    post_id: String,
) -> Result<(), EventProcessorError> {
    debug!("Indexing new post: {}/{}", author_id, post_id);
    // Create PostDetails object
    let post_details = PostDetails::from_homeserver(post.clone(), &author_id, &post_id);
    // We avoid indexing replies into global feed sorted sets
    let is_reply = post.parent.is_some();
    // PRE-INDEX operation, identify the post relationship
    let mut post_relationships = PostRelationships::from_homeserver(&post);

    let existed = match post_details.put_to_graph(&post_relationships).await? {
        OperationOutcome::CreatedOrDeleted => false,
        OperationOutcome::Updated => true,
        OperationOutcome::MissingDependency => {
            let mut dependency_event_keys = Vec::new();
            if let Some(replied_to_uri) = &post_relationships.replied {
                let reply_dependency = RetryEvent::generate_index_key_from_uri(replied_to_uri);
                dependency_event_keys.push(reply_dependency);

                if let Err(e) = Homeserver::maybe_ingest_for_post(replied_to_uri).await {
                    tracing::error!("Failed to ingest homeserver: {e}");
                }
            }
            if let Some(reposted_uri) = &post_relationships.reposted {
                let reply_dependency = RetryEvent::generate_index_key_from_uri(reposted_uri);
                dependency_event_keys.push(reply_dependency);

                if let Err(e) = Homeserver::maybe_ingest_for_post(reposted_uri).await {
                    tracing::error!("Failed to ingest homeserver: {e}");
                }
            }
            if dependency_event_keys.is_empty() {
                let key = RetryEvent::generate_index_key_from_uri(&author_id.to_uri());
                dependency_event_keys.push(key);
            }
            return Err(EventProcessorError::missing_dependencies(
                dependency_event_keys,
            ));
        }
    };

    if existed {
        // If the post existed, let's confirm this is an edit. Is the content different?
        let existing_details = PostDetails::get_from_index(&author_id, &post_id)
            .await?
            .ok_or("An existing post in graph, could not be retrieved from index")
            .map_err(EventProcessorError::generic)?;
        if existing_details.content != post_details.content {
            sync_edit(post, author_id, post_id, post_details).await?;
        }
        return Ok(());
    }

    // IMPORTANT: Handle the mentions before traverse the graph (reindex_post) for that post
    // Handle "MENTIONED" relationships
    put_mentioned_relationships(
        &author_id,
        &post_id,
        &post_details.content,
        &mut post_relationships,
    )
    .await?;

    // We only consider the first mentioned (tagged) user, to mitigate DoS attacks against Nexus
    // whereby posts with many (inexistent) tagged PKs can cause Nexus to spend a lot of time trying to resolve them
    if let Some(mentioned_user_id) = &post_relationships.mentioned.first() {
        if let Err(e) = Homeserver::maybe_ingest_for_user(mentioned_user_id).await {
            tracing::error!("Failed to ingest homeserver: {e}");
        }
    }

    // SAVE TO INDEX - PHASE 1, update post counts
    let indexing_results = tokio::join!(
        // TODO: Use SCARD on a set for unique tag count to avoid race conditions in parallel processing
        async {
            // Create post counts index
            // If new post (no existing counts) save a new PostCounts.
            if PostCounts::get_from_index(&author_id, &post_id)
                .await?
                .is_none()
            {
                PostCounts::default()
                    .put_to_index(&author_id, &post_id, is_reply)
                    .await?
            }
            Ok::<(), EventProcessorError>(())
        },
        // TODO: Use SCARD on a set for unique tag count to avoid race conditions in parallel processing
        // Update user counts with the new post
        UserCounts::increment(&author_id, "posts", None),
        async {
            if is_reply {
                UserCounts::increment(&author_id, "replies", None).await?;
            };
            Ok::<(), EventProcessorError>(())
        }
    );

    indexing_results.0?;
    indexing_results.1?;
    indexing_results.2?;

    // Use that index wrapper to add a post reply
    let mut reply_parent_post_key_wrapper: Option<(String, String)> = None;

    // PHASE 2: Process POST REPLIES indexes
    if let Some(replied_uri) = &post_relationships.replied {
        let parent_author_id = replied_uri.user_id.clone();
        let parent_post_id = match replied_uri.resource.clone() {
            Resource::Post(id) => id,
            _ => {
                return Err(EventProcessorError::generic(
                    "Replied URI is not a Post resource",
                ))
            }
        };
        let replied_uri_str = replied_uri
            .try_to_uri_str()
            .map_err(EventProcessorError::generic)?;

        // Define the reply parent key to index the reply later
        reply_parent_post_key_wrapper =
            Some((parent_author_id.to_string(), parent_post_id.clone()));

        let parent_post_key_parts: &[&str; 2] = &[&parent_author_id, &parent_post_id];

        let indexing_results = tokio::join!(
            PostCounts::increment_index_field(parent_post_key_parts, "replies", None),
            async {
                if !post_relationships_is_reply(&parent_author_id, &parent_post_id).await? {
                    PostStream::increment_score_index_sorted_set(
                        &POST_TOTAL_ENGAGEMENT_KEY_PARTS,
                        parent_post_key_parts,
                    )
                    .await
                    .map_err(EventProcessorError::index_operation_failed)?;
                }
                Ok::<(), EventProcessorError>(())
            },
            PostStream::add_to_post_reply_sorted_set(
                parent_post_key_parts,
                &author_id,
                &post_id,
                post_details.indexed_at,
            ),
            Notification::new_post_reply(
                &author_id,
                &replied_uri_str,
                &post_details.uri,
                &parent_author_id,
            )
        );

        indexing_results.0?;
        indexing_results.1?;
        indexing_results.2?;
        indexing_results.3?;
    }

    // PHASE 3: Process POST REPOSTS indexes
    if let Some(reposted_uri) = &post_relationships.reposted {
        let parent_author_id = reposted_uri.user_id.clone();
        let parent_post_id = match reposted_uri.resource.clone() {
            Resource::Post(id) => id,
            _ => {
                return Err(EventProcessorError::generic(
                    "Reposted uri is not a Post resource",
                ))
            }
        };
        let reposted_uri_str = reposted_uri
            .try_to_uri_str()
            .map_err(EventProcessorError::generic)?;

        let parent_post_key_parts: &[&str; 2] = &[&parent_author_id, &parent_post_id];
        let indexing_results = tokio::join!(
            PostCounts::increment_index_field(parent_post_key_parts, "reposts", None),
            async {
                // Post replies cannot be included in the total engagement index after they receive a reply
                if !post_relationships_is_reply(&parent_author_id, &parent_post_id).await? {
                    PostStream::increment_score_index_sorted_set(
                        &POST_TOTAL_ENGAGEMENT_KEY_PARTS,
                        parent_post_key_parts,
                    )
                    .await
                    .map_err(EventProcessorError::index_operation_failed)?;
                }
                Ok::<(), EventProcessorError>(())
            },
            Notification::new_repost(
                &author_id,
                &reposted_uri_str,
                &post_details.uri,
                &parent_author_id,
            )
        );

        indexing_results.0?;
        indexing_results.1?;
        indexing_results.2?;
    }

    // PHASE 4: Add post related content
    let indexing_results = tokio::join!(
        post_relationships.put_to_index(&author_id, &post_id),
        post_details.put_to_index(&author_id, reply_parent_post_key_wrapper, false)
    );

    indexing_results.0?;
    indexing_results.1?;

    Ok(())
}

async fn sync_edit(
    post: PubkyAppPost,
    author_id: PubkyId,
    post_id: String,
    post_details: PostDetails,
) -> Result<(), EventProcessorError> {
    // Construct the URI of the post that changed
    let changed_uri = post_uri_builder(author_id.to_string(), post_id.clone());

    // Update content of PostDetails!
    post_details.put_to_index(&author_id, None, true).await?;

    // Notifications
    // Determine the change type
    let change_type = if post_details.content == *"[DELETED]" {
        PostChangedType::Deleted
    } else {
        PostChangedType::Edited
    };

    // Send notifications to users who interacted with the post
    Notification::changed_post(&author_id, &post_id, &changed_uri, &change_type).await?;

    // Handle "A reply to your post was edited/deleted"
    if let Some(parent) = post.parent {
        let parsed_parent =
            ParsedUri::try_from(parent.as_str()).map_err(EventProcessorError::generic)?;
        Notification::post_children_changed(
            &author_id,
            &parent,
            &parsed_parent.user_id,
            &changed_uri,
            PostChangedSource::Reply,
            &change_type,
        )
        .await?;
    };

    Ok(())
}

/// Helper function to handle "MENTIONED" relationships on the post content
pub async fn put_mentioned_relationships(
    author_id: &PubkyId,
    post_id: &str,
    content: &str,
    relationships: &mut PostRelationships,
) -> Result<(), EventProcessorError> {
    // TODO Deprecate, drop support for pk: support in an upcoming release
    // Backwards compatibility: identify user references with "pk:" prefix
    put_mentioned_relationships_for_prefix(author_id, post_id, content, relationships, "pk:")
        .await?;

    // Support new pubkey display: identify user references with "pubky" prefix
    put_mentioned_relationships_for_prefix(author_id, post_id, content, relationships, "pubky")
        .await?;

    Ok(())
}

async fn put_mentioned_relationships_for_prefix(
    author_id: &PubkyId,
    post_id: &str,
    content: &str,
    relationships: &mut PostRelationships,
    prefix: &str,
) -> Result<(), EventProcessorError> {
    let user_id_len = 52;

    let found_pubky_ids = content.match_indices(prefix).filter_map(|(start_idx, _)| {
        let user_id_start = start_idx + prefix.len();
        content
            .get(user_id_start..user_id_start + user_id_len)
            .and_then(|candidate| PubkyId::try_from(candidate).ok())
    });

    for pubky_id in found_pubky_ids {
        // Create the MENTIONED relationship in the graph
        let query = queries::put::create_mention_relationship(author_id, post_id, &pubky_id);
        exec_single_row(query)
            .await
            .map_err(EventProcessorError::graph_query_failed)?;

        let maybe_mentioned_id = Notification::new_mention(author_id, &pubky_id, post_id).await?;
        if let Some(mentioned_user_id) = maybe_mentioned_id {
            relationships.mentioned.push(mentioned_user_id);
        }
    }

    Ok(())
}

pub async fn del(author_id: PubkyId, post_id: String) -> Result<(), EventProcessorError> {
    del_with_hook(author_id, post_id, &NoopTargetDeleteHook).await
}

/// [`del`] with deterministic interleaving points. Production callers use
/// [`del`].
#[doc(hidden)]
pub async fn del_with_hook(
    author_id: PubkyId,
    post_id: String,
    hook: &dyn TargetDeleteHook,
) -> Result<(), EventProcessorError> {
    debug!("Deleting post: {}/{}", author_id, post_id);

    // Graph query to check if there is any edge at all to this post other than AUTHORED, is a reply or is a repost.
    // The check takes the post's write lock and the transaction holds it until the post is deleted: a
    // tag PUT that commits first is counted, and one that starts later waits and finds no post.
    let query = post_is_safe_to_delete(&author_id, &post_id);
    let mut txn = start_graph_txn().await?;
    let outcome = execute_graph_operation_in(&mut txn, query)
        .await
        .map_err(EventProcessorError::graph_query_failed);

    // If there is none other relationship (OperationOutcome::CreatedOrDeleted), we delete from graph and redis.
    // But if there is any (OperationOutcome::Updated), then we simply update the post with keyword content [DELETED].
    // A deleted post is a post whose content is EXACTLY `"[DELETED]"`
    match outcome {
        Ok(OperationOutcome::CreatedOrDeleted) => {
            let checked = hook.at(TargetDeleteStep::Checked).await;
            delete_post(txn, hook, &author_id, &post_id, checked).await?;
        }
        Ok(OperationOutcome::Updated) => {
            finish_txn(txn, Ok(())).await?;
            let existing_relationships = PostRelationships::get_by_id(&author_id, &post_id).await?;
            let parent = existing_relationships
                .and_then(|rel| rel.replied)
                .and_then(|replied_uri| replied_uri.try_to_uri_str().ok());

            // We store a dummy that is still a reply if it was one already.
            let dummy_deleted_post = PubkyAppPost {
                content: "[DELETED]".to_string(),
                parent,
                embed: None,
                kind: PubkyAppPostKind::Short,
                attachments: None,
                lock: None,
            };

            sync_put(dummy_deleted_post, author_id, post_id).await?;
        }
        Ok(OperationOutcome::MissingDependency) => {
            finish_txn(txn, Err::<(), _>(EventProcessorError::SkipIndexing)).await?;
        }
        Err(error) => {
            finish_txn(txn, Err::<(), _>(error)).await?;
        }
    };

    Ok(())
}

/// Deletes a post from the graph and Redis in one transaction that holds the
/// post's write lock until every index is cleaned. A tag PUT or untag on the
/// post either finished first or starts after the deletion and finds no post.
pub async fn sync_del(author_id: PubkyId, post_id: String) -> Result<(), EventProcessorError> {
    sync_del_with_hook(author_id, post_id, &NoopTargetDeleteHook).await
}

/// [`sync_del`] with deterministic interleaving points.
#[doc(hidden)]
pub async fn sync_del_with_hook(
    author_id: PubkyId,
    post_id: String,
    hook: &dyn TargetDeleteHook,
) -> Result<(), EventProcessorError> {
    let mut txn = start_graph_txn().await?;
    let locked = txn
        .fetch_row(queries::del::lock_post(&author_id, &post_id))
        .await
        .map(|_| ())
        .map_err(EventProcessorError::from);
    delete_post(txn, hook, &author_id, &post_id, locked).await
}

/// What a post deletion has touched, for its recovery.
#[derive(Default)]
struct Touched {
    taggers: Vec<String>,
    labels: Vec<String>,
    deletion: PostDeletion,
}

/// A notification that a deleted post's parent or reposted post sends, once
/// the deletion has committed: a notification cannot be taken back.
struct ChildDeleted {
    author_id: String,
    parent_uri: String,
    parent_user_id: String,
    deleted_uri: String,
    source: PostChangedSource,
}

impl ChildDeleted {
    async fn send(self) {
        if let Err(error) = Notification::post_children_changed(
            &self.author_id,
            &self.parent_uri,
            &self.parent_user_id,
            &self.deleted_uri,
            self.source,
            &PostChangedType::Deleted,
        )
        .await
        {
            tracing::warn!("Sending a post deletion notification failed: {error}");
        }
    }
}

/// Deletes the post in `txn`, which holds its lock, and commits. `ready` is
/// the outcome of what came before (the check or the lock) and stops the
/// deletion when it failed. If the deletion or its commit fails after a Redis
/// write started, Redis is rebuilt from the committed graph under the locks
/// of the post and the users and posts whose counters it moved (see
/// [`recover`]).
async fn delete_post(
    mut txn: GraphTxn,
    hook: &dyn TargetDeleteHook,
    author_id: &PubkyId,
    post_id: &str,
    ready: Result<(), EventProcessorError>,
) -> Result<(), EventProcessorError> {
    let mut steps = Steps::new(StepHook::Delete(hook));
    let mut touched = Touched::default();
    let result = match ready {
        Ok(()) => sync_del_in_txn(&mut txn, &mut steps, &mut touched, author_id, post_id).await,
        Err(error) => Err(error),
    };
    let result = match result {
        Ok(notifications) => hook
            .at(TargetDeleteStep::BeforeCommit)
            .await
            .map(|_| notifications),
        Err(error) => Err(error),
    };
    let outcome = finish_txn(txn, result).await;
    if outcome.is_err() && steps.started() {
        let recovery = Recovery {
            target: OwnedTarget::Post(author_id.to_string(), post_id.to_string()),
            taggers: touched.taggers,
            labels: touched.labels,
            post: Some(touched.deletion),
        };
        if let Err(error) = recover(&recovery, StepHook::Delete(hook)).await {
            tracing::error!("Recovering a failed post deletion failed: {error}");
        }
    }
    for notification in outcome? {
        notification.send().await;
    }
    Ok(())
}

async fn sync_del_in_txn(
    txn: &mut GraphTxn,
    steps: &mut Steps<'_>,
    touched: &mut Touched,
    author_id: &PubkyId,
    post_id: &str,
) -> Result<Vec<ChildDeleted>, EventProcessorError> {
    let deleted_uri = post_uri_builder(author_id.to_string(), post_id.to_string());
    let mut notifications = Vec::new();

    let post_relationships = PostRelationships::get_by_id_in(txn, author_id, post_id).await?;
    // If the post is reply, cannot delete from the main feeds
    // In the main feed, we just include the root posts and reposts
    // It could be a situation that relationship would not exist and we will treat the post as a not reply
    let is_reply =
        matches!(&post_relationships, Some(relationship) if relationship.replied.is_some());
    let mut replied = None;
    let mut reposted = None;
    if let Some(relationships) = &post_relationships {
        if let Some(uri) = &relationships.replied {
            replied = Some(post_of(uri, "Replied")?);
        }
        if let Some(uri) = &relationships.reposted {
            reposted = Some(post_of(uri, "Reposted")?);
        }
    }
    touched.deletion.replied = replied.clone();
    touched.deletion.reposted = reposted.clone();

    let (taggers, labels) = super::tag::delete_post_tag_edges(txn, author_id, post_id).await?;
    touched.taggers = taggers.clone();
    touched.labels = labels.clone();
    super::tag::purge_deleted_post_tags(txn, steps, author_id, post_id, &labels).await?;

    // DELETE TO INDEX - PHASE 1, decrease post counts
    steps
        .run(async {
            PostCounts::delete(author_id, post_id, !is_reply).await?;
            Ok(())
        })
        .await?;
    steps
        .run(tag_index::user_counter(author_id, "posts", false))
        .await?;
    if is_reply {
        steps
            .run(tag_index::user_counter(author_id, "replies", false))
            .await?;
    }

    // Use that index wrapper to delete a post reply
    let mut reply_parent_post_key_wrapper: Option<[String; 2]> = None;

    // PHASE 2: Process POST REPLIES indexes
    // Decrement counts for parent post if replied
    if let Some((parent_user_id, parent_post_id)) = &replied {
        reply_parent_post_key_wrapper = Some([parent_user_id.clone(), parent_post_id.clone()]);
        decrement_parent(txn, steps, parent_user_id, parent_post_id, "replies").await?;
        notifications.push(ChildDeleted {
            author_id: author_id.to_string(),
            parent_uri: post_uri_builder(parent_user_id.clone(), parent_post_id.clone()),
            parent_user_id: parent_user_id.clone(),
            deleted_uri: deleted_uri.clone(),
            source: PostChangedSource::Reply,
        });
    }
    // PHASE 3: Process POST REPOSTED indexes
    // Decrement counts for resposted post if existed
    if let Some((parent_user_id, parent_post_id)) = &reposted {
        decrement_parent(txn, steps, parent_user_id, parent_post_id, "reposts").await?;
        notifications.push(ChildDeleted {
            author_id: author_id.to_string(),
            parent_uri: post_uri_builder(parent_user_id.clone(), parent_post_id.clone()),
            parent_user_id: parent_user_id.clone(),
            deleted_uri: deleted_uri.clone(),
            source: PostChangedSource::Repost,
        });
    }

    steps
        .run(async {
            PostDetails::remove_from_index_multiple_json(&[&[author_id, post_id]]).await?;
            Ok(())
        })
        .await?;
    // Delete post graph node
    txn.run(queries::del::delete_post(author_id, post_id))
        .await?;
    steps
        .run(async {
            PostDetails::delete_stream_entries(author_id, post_id, reply_parent_post_key_wrapper)
                .await?;
            Ok(())
        })
        .await?;
    steps
        .run(async {
            PostRelationships::delete(author_id, post_id).await?;
            Ok(())
        })
        .await?;

    for tagger_id in &taggers {
        steps
            .run(tag_index::user_counter(tagger_id, "tagged", false))
            .await?;
    }

    Ok(notifications)
}

/// The `(author_id, post_id)` a relationship URI points at.
fn post_of(uri: &ParsedUri, what: &str) -> Result<(String, String), EventProcessorError> {
    match &uri.resource {
        Resource::Post(post_id) => Ok((uri.user_id.to_string(), post_id.clone())),
        _ => Err(EventProcessorError::generic(format!(
            "{what} uri is not a Post resource"
        ))),
    }
}

/// Lowers the parent post's reply or repost count and, when the parent is
/// not a reply, its total engagement.
async fn decrement_parent(
    txn: &mut GraphTxn,
    steps: &mut Steps<'_>,
    parent_user_id: &str,
    parent_post_id: &str,
    field: &'static str,
) -> Result<(), EventProcessorError> {
    steps
        .run(tag_index::post_counter(
            parent_user_id,
            parent_post_id,
            field,
            false,
        ))
        .await?;
    // Post replies cannot be included in the total engagement index after the reply or repost is deleted
    if !post_relationships_is_reply_in(txn, parent_user_id, parent_post_id).await? {
        steps
            .run(tag_index::post_engagement(
                parent_user_id,
                parent_post_id,
                false,
            ))
            .await?;
    }
    Ok(())
}
