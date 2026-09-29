use crate::events::retry::event::RetryEvent;
use crate::events::EventProcessorError;

use chrono::Utc;
use nexus_common::db::graph::Query;
use nexus_common::db::kv::{ScoreAction, SortOrder};
use nexus_common::db::{
    exec_single_row, fetch_all_rows_from_graph, queries, start_graph_txn, GraphTxn,
    OperationOutcome, RedisOps,
};
use nexus_common::models::homeserver::Homeserver;
use nexus_common::models::marketplace::ListingsByTagSearch;
use nexus_common::models::notification::Notification;
use nexus_common::models::post::search::PostsByTagSearch;
use nexus_common::models::post::{PostCounts, PostStream};
use nexus_common::models::tag::listing::{TagListing, LISTING_TAGS_KEY_PARTS};
use nexus_common::models::tag::post::{TagPost, POST_TAGS_KEY_PARTS};
use nexus_common::models::tag::search::TagSearch;
use nexus_common::models::tag::shop::{TagShop, SHOP_TAGS_KEY_PARTS};
use nexus_common::models::tag::traits::{TagCollection, TaggersCollection};
use nexus_common::models::tag::user::{TagUser, USER_TAGS_KEY_PARTS};
use nexus_common::models::user::UserCounts;
use pubky_app_specs::{post_uri_builder, ParsedUri, PubkyAppTag, PubkyId, Resource};
use tracing::debug;

use super::utils::{finish_txn, post_relationships_is_reply};

/// Points of a tag write where integration tests inject a failure or a
/// concurrent event.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TagWriteStep {
    /// The tag PUT wrote its edge in its open transaction, holding the
    /// target's write lock; no Redis index is written yet.
    GraphWritten,
    /// The untag deleted the edge in its open transaction, holding the
    /// target's write lock; no Redis index is changed yet.
    EdgeDeleted,
}

/// Internal deterministic seam for integration-testing tag writes against a
/// deletion of their target.
#[doc(hidden)]
#[async_trait::async_trait]
pub trait TagWriteHook: Sync {
    async fn at(&self, _step: TagWriteStep) -> Result<(), EventProcessorError> {
        Ok(())
    }
}

struct NoopTagWriteHook;

#[async_trait::async_trait]
impl TagWriteHook for NoopTagWriteHook {}

/// Indexes a tag PUT.
///
/// The edge and every Redis index of the tag are written inside one graph
/// transaction that holds the write lock of the tagged target. A deletion of
/// the target locks the same node: it waits for the
/// tag's Redis writes and then sees the committed edge, and a deletion that
/// finished first leaves no target, so the PUT writes nothing. Any failure
/// rolls the edge back, so the retried event starts from a clean state. The
/// global autocomplete entry is written once the edge is committed, where a
/// concurrent untag's unused-label check finds the edge.
pub async fn sync_put(
    tag: PubkyAppTag,
    tagger_id: PubkyId,
    tag_id: String,
) -> Result<(), EventProcessorError> {
    sync_put_with_hook(tag, tagger_id, tag_id, &NoopTagWriteHook).await
}

/// [`sync_put`] with deterministic interleaving points. Production callers
/// use [`sync_put`].
#[doc(hidden)]
pub async fn sync_put_with_hook(
    tag: PubkyAppTag,
    tagger_id: PubkyId,
    tag_id: String,
    hook: &dyn TagWriteHook,
) -> Result<(), EventProcessorError> {
    debug!("Indexing new tag: {} -> {}", tagger_id, tag_id);

    // Parse the embeded URI to extract author_id and post_id using parse_tagged_post_uri
    let parsed_uri = ParsedUri::try_from(tag.uri.as_str()).map_err(EventProcessorError::generic)?;
    let user_id = parsed_uri.user_id.clone();
    // Retry-manager key of the tagged target, used as the missing-dependency
    // key for marketplace targets (matches how their PUT events are keyed).
    let target_dependency_key = RetryEvent::generate_index_key_from_uri(&parsed_uri);
    let indexed_at = Utc::now().timestamp_millis();

    let mut txn = start_graph_txn().await?;
    let result = match parsed_uri.resource {
        // If post_id is in the tagged URI, we place tag to a post.
        Resource::Post(post_id) => {
            // Place the tag on post
            put_sync_post(
                &mut txn, hook, tagger_id, user_id, &post_id, &tag_id, &tag.label, &tag.uri,
                indexed_at,
            )
            .await
        }
        // If no post_id in the tagged URI, we place tag to a user.
        Resource::User => {
            put_sync_user(
                &mut txn, hook, tagger_id, user_id, &tag_id, &tag.label, indexed_at,
            )
            .await
        }
        // Marketplace targets: tags on listings and shops (community layer).
        Resource::Listing(listing_id) => {
            put_sync_listing(
                &mut txn,
                hook,
                tagger_id,
                user_id,
                &listing_id,
                &tag_id,
                &tag.label,
                target_dependency_key,
                indexed_at,
            )
            .await
        }
        Resource::Shop => {
            put_sync_shop(
                &mut txn,
                hook,
                tagger_id,
                user_id,
                &tag_id,
                &tag.label,
                target_dependency_key,
                indexed_at,
            )
            .await
        }
        other => Err(EventProcessorError::generic(format!(
            "The tagged resource is not Post, User, Listing or Shop, instead is: {other:?}"
        ))),
    };
    finish_txn(txn, result).await?;

    // Add tag to search index
    TagSearch::put_to_index(&[tag.label]).await?;
    Ok(())
}

/// Handles the synchronization of a tagged post by updating the graph, indexes, and related counts.
/// # Arguments
/// - `tagger_user_id` - The `PubkyId` of the user tagging the post.
/// - `author_id` - The `PubkyId` of the author of the tagged post.
/// - `post_id` - A `String` representing the unique identifier of the post being tagged.
/// - `tag_id` - A `String` representing the unique identifier of the tag.
/// - `tag_label` - A `String` representing the label of the tag.
/// - `post_uri` - A `String` representing the homeserver URI of the tagged post.
/// - `indexed_at` - A 64-bit integer representing the timestamp when the post was indexed.
///
#[allow(clippy::too_many_arguments)]
async fn put_sync_post(
    txn: &mut GraphTxn,
    hook: &dyn TagWriteHook,
    tagger_user_id: PubkyId,
    author_id: PubkyId,
    post_id: &str,
    tag_id: &str,
    tag_label: &str,
    post_uri: &str,
    indexed_at: i64,
) -> Result<(), EventProcessorError> {
    match TagPost::put_to_graph(
        txn,
        &tagger_user_id,
        &author_id,
        Some(post_id),
        tag_id,
        tag_label,
        indexed_at,
    )
    .await?
    {
        OperationOutcome::Updated => Ok(()),
        OperationOutcome::MissingDependency => {
            // Ensure that dependencies follow the same format as the RetryManager keys
            let dependency = vec![format!("{author_id}:posts:{post_id}")];
            if let Ok(referenced_post_uri) = ParsedUri::try_from(post_uri) {
                if let Err(e) = Homeserver::maybe_ingest_for_post(&referenced_post_uri).await {
                    tracing::error!("Failed to ingest homeserver: {e}");
                }
            }
            Err(EventProcessorError::MissingDependency { dependency })
        }
        OperationOutcome::CreatedOrDeleted => {
            hook.at(TagWriteStep::GraphWritten).await?;
            // SAVE TO INDEXES
            let post_key_slice: &[&str] = &[&author_id, post_id];

            let indexing_results = tokio::join!(
                // Update user counts for tagger
                UserCounts::increment(&tagger_user_id, "tagged", None),
                // Increment in one the post tags
                PostCounts::increment_index_field(post_key_slice, "tags", None),
                async {
                    // Increase unique_tags if the tag does not exist already
                    // NOTE: To update that field, it cannot exist in TagPost SORTED SET the tag. Thats why it has to be executed
                    // before TagPost operation
                    PostCounts::increment_index_field(
                        post_key_slice,
                        "unique_tags",
                        Some(tag_label),
                    )
                    .await?;
                    // Increment the label count to post
                    TagPost::update_index_score(
                        &author_id,
                        Some(post_id),
                        tag_label,
                        ScoreAction::Increment(1.0),
                    )
                    .await?;
                    Ok::<(), EventProcessorError>(())
                },
                // Add user tag in post
                TagPost::add_tagger_to_index(&author_id, Some(post_id), &tagger_user_id, tag_label),
                // Add post to label total engagement
                PostsByTagSearch::update_index_score(
                    &author_id,
                    post_id,
                    tag_label,
                    ScoreAction::Increment(1.0),
                ),
                async {
                    // Post replies cannot be included in the total engagement index once they have been tagged
                    if !post_relationships_is_reply(&author_id, post_id).await? {
                        // Increment in one post global engagement
                        PostStream::update_index_score(
                            &author_id,
                            post_id,
                            ScoreAction::Increment(1.0),
                        )
                        .await
                        .map_err(EventProcessorError::index_operation_failed)?;
                    }
                    Ok::<(), EventProcessorError>(())
                },
                // Add post to global label timeline
                PostsByTagSearch::put_to_index(&author_id, post_id, tag_label),
                // Save new notification
                Notification::new_post_tag(&tagger_user_id, &author_id, tag_label, post_uri)
            );

            indexing_results.0?;
            indexing_results.1?;
            indexing_results.2?;
            indexing_results.3?;
            indexing_results.4?;
            indexing_results.5?;
            indexing_results.6?;
            indexing_results.7?;

            Ok(())
        }
    }
}

/// Handles the synchronization of a tagged user by updating the graph, indexes, and related counts.
///
/// # Arguments
/// - `tagger_user_id` - The `PubkyId` of the user tagging the user.
/// - `tagged_user_id` - The `PubkyId` of the user being tagged.
/// - `tag_id` - A `String` representing the unique identifier of the tag.
/// - `tag_label` - A `String` representing the label of the tag.
/// - `indexed_at` - A 64-bit integer representing the timestamp when the user was indexed.
async fn put_sync_user(
    txn: &mut GraphTxn,
    hook: &dyn TagWriteHook,
    tagger_user_id: PubkyId,
    tagged_user_id: PubkyId,
    tag_id: &str,
    tag_label: &str,
    indexed_at: i64,
) -> Result<(), EventProcessorError> {
    match TagUser::put_to_graph(
        txn,
        &tagger_user_id,
        &tagged_user_id,
        None,
        tag_id,
        tag_label,
        indexed_at,
    )
    .await?
    {
        OperationOutcome::Updated => Ok(()),
        OperationOutcome::MissingDependency => {
            if let Err(e) = Homeserver::maybe_ingest_for_user(tagged_user_id.as_ref()).await {
                tracing::error!("Failed to ingest homeserver: {e}");
            }

            let key = RetryEvent::generate_index_key_from_uri(&tagged_user_id.to_uri());
            let dependency = vec![key];
            Err(EventProcessorError::MissingDependency { dependency })
        }
        OperationOutcome::CreatedOrDeleted => {
            hook.at(TagWriteStep::GraphWritten).await?;

            // SAVE TO INDEX
            let indexing_results = tokio::join!(
                // Update user counts for the tagged user
                UserCounts::increment(&tagged_user_id, "tags", None),
                // Update user counts for the tagger user
                UserCounts::increment(&tagger_user_id, "tagged", None),
                async {
                    // Increase unique_tags if the tag does not exist already
                    // NOTE: To update that field, it cannot exist in TagUser SORTED SET the tag. Thats why it has to be executed
                    // before TagUser operation
                    UserCounts::increment(&tagged_user_id, "unique_tags", Some(tag_label)).await?;
                    // Add label count to the user profile tag
                    TagUser::update_index_score(
                        &tagged_user_id,
                        None,
                        tag_label,
                        ScoreAction::Increment(1.0),
                    )
                    .await?;
                    Ok::<(), EventProcessorError>(())
                },
                // Add tagger to the user taggers list
                TagUser::add_tagger_to_index(&tagged_user_id, None, &tagger_user_id, tag_label),
                // Save new notification
                Notification::new_user_tag(&tagger_user_id, &tagged_user_id, tag_label)
            );

            indexing_results.0?;
            indexing_results.1?;
            indexing_results.2?;
            indexing_results.3?;
            indexing_results.4?;

            Ok(())
        }
    }
}

/// Handles the synchronization of a tagged marketplace listing by updating the
/// graph and Redis indexes. Mirrors [`put_sync_post`] minus the pieces that do
/// not exist for listings: there are no per-listing counts models, listings
/// carry no engagement score, and tag notifications for marketplace targets
/// are deliberately not emitted (out of scope of the social layer).
///
/// # Arguments
/// - `tagger_user_id` - The `PubkyId` of the user tagging the listing.
/// - `seller_id` - The `PubkyId` of the listing owner.
/// - `listing_id` - The unique identifier of the listing being tagged.
/// - `tag_id` - The unique identifier of the tag.
/// - `tag_label` - The label of the tag.
/// - `dependency_key` - Retry-manager key of the listing, used when the
///   listing is not indexed yet.
/// - `indexed_at` - Timestamp (ms) when the tag was indexed.
#[allow(clippy::too_many_arguments)]
async fn put_sync_listing(
    txn: &mut GraphTxn,
    hook: &dyn TagWriteHook,
    tagger_user_id: PubkyId,
    seller_id: PubkyId,
    listing_id: &str,
    tag_id: &str,
    tag_label: &str,
    dependency_key: String,
    indexed_at: i64,
) -> Result<(), EventProcessorError> {
    match TagListing::put_to_graph(
        txn,
        &tagger_user_id,
        &seller_id,
        Some(listing_id),
        tag_id,
        tag_label,
        indexed_at,
    )
    .await?
    {
        OperationOutcome::Updated => Ok(()),
        OperationOutcome::MissingDependency => Err(EventProcessorError::MissingDependency {
            dependency: vec![dependency_key],
        }),
        OperationOutcome::CreatedOrDeleted => {
            hook.at(TagWriteStep::GraphWritten).await?;

            let indexing_results = tokio::join!(
                // Update user counts for tagger
                UserCounts::increment(&tagger_user_id, "tagged", None),
                // Increment the label count on the listing
                TagListing::update_index_score(
                    &seller_id,
                    Some(listing_id),
                    tag_label,
                    ScoreAction::Increment(1.0),
                ),
                // Add user tag in listing
                TagListing::add_tagger_to_index(
                    &seller_id,
                    Some(listing_id),
                    &tagger_user_id,
                    tag_label
                ),
                // Add listing to the global label timeline
                ListingsByTagSearch::put_to_index(&seller_id, listing_id, tag_label)
            );

            indexing_results.0?;
            indexing_results.1?;
            indexing_results.2?;
            indexing_results.3?;

            Ok(())
        }
    }
}

/// Handles the synchronization of a tagged marketplace shop by updating the
/// graph and Redis indexes. Mirrors [`put_sync_user`] minus per-target counts
/// (shops have no counts model) and notifications (deliberately not emitted).
///
/// # Arguments
/// - `tagger_user_id` - The `PubkyId` of the user tagging the shop.
/// - `owner_id` - The `PubkyId` of the shop owner.
/// - `tag_id` - The unique identifier of the tag.
/// - `tag_label` - The label of the tag.
/// - `dependency_key` - Retry-manager key of the shop, used when the shop is
///   not indexed yet.
/// - `indexed_at` - Timestamp (ms) when the tag was indexed.
#[allow(clippy::too_many_arguments)]
async fn put_sync_shop(
    txn: &mut GraphTxn,
    hook: &dyn TagWriteHook,
    tagger_user_id: PubkyId,
    owner_id: PubkyId,
    tag_id: &str,
    tag_label: &str,
    dependency_key: String,
    indexed_at: i64,
) -> Result<(), EventProcessorError> {
    match TagShop::put_to_graph(
        txn,
        &tagger_user_id,
        &owner_id,
        None,
        tag_id,
        tag_label,
        indexed_at,
    )
    .await?
    {
        OperationOutcome::Updated => Ok(()),
        OperationOutcome::MissingDependency => Err(EventProcessorError::MissingDependency {
            dependency: vec![dependency_key],
        }),
        OperationOutcome::CreatedOrDeleted => {
            hook.at(TagWriteStep::GraphWritten).await?;

            let indexing_results = tokio::join!(
                // Update user counts for tagger
                UserCounts::increment(&tagger_user_id, "tagged", None),
                // Increment the label count on the shop
                TagShop::update_index_score(
                    &owner_id,
                    None,
                    tag_label,
                    ScoreAction::Increment(1.0)
                ),
                // Add tagger to the shop taggers list
                TagShop::add_tagger_to_index(&owner_id, None, &tagger_user_id, tag_label)
            );

            indexing_results.0?;
            indexing_results.1?;
            indexing_results.2?;

            Ok(())
        }
    }
}

/// Indexes a tag DEL (an untag).
///
/// Like [`sync_put`], the edge deletion and every Redis index change run in
/// one graph transaction holding the target's write lock, so a deletion of
/// the target cannot interleave: it waits for the
/// untag, or it finished first and the untag finds no edge.
pub async fn del(user_id: PubkyId, tag_id: String) -> Result<(), EventProcessorError> {
    del_with_hook(user_id, tag_id, &NoopTagWriteHook).await
}

/// [`del`] with deterministic interleaving points. Production callers use
/// [`del`].
#[doc(hidden)]
pub async fn del_with_hook(
    user_id: PubkyId,
    tag_id: String,
    hook: &dyn TagWriteHook,
) -> Result<(), EventProcessorError> {
    debug!("Deleting tag: {} -> {}", user_id, tag_id);
    let mut txn = start_graph_txn().await?;
    let result = del_in_txn(&mut txn, hook, user_id, tag_id).await;
    finish_txn(txn, result).await
}

async fn del_in_txn(
    txn: &mut GraphTxn,
    hook: &dyn TagWriteHook,
    user_id: PubkyId,
    tag_id: String,
) -> Result<(), EventProcessorError> {
    let tag_details = TagUser::del_from_graph(txn, &user_id, &tag_id).await?;
    // CHOOSE THE EVENT TYPE
    if let Some(target) = tag_details {
        hook.at(TagWriteStep::EdgeDeleted).await?;
        let label = target.label.clone();
        match (
            target.user_id,
            target.post_id,
            target.author_id,
            target.listing_id,
            target.listing_owner_id,
            target.shop_owner_id,
        ) {
            // Delete user related indexes
            (Some(tagged_id), None, None, None, None, None) => {
                del_sync_user(txn, user_id, &tagged_id, &label).await?;
            }
            // Delete post related indexes
            (None, Some(post_id), Some(author_id), None, None, None) => {
                del_sync_post(txn, user_id, &post_id, &author_id, &label).await?;
            }
            // Delete marketplace listing related indexes
            (None, None, None, Some(listing_id), Some(listing_owner_id), None) => {
                del_sync_listing(txn, user_id, &listing_id, &listing_owner_id, &label).await?;
            }
            // Delete marketplace shop related indexes
            (None, None, None, None, None, Some(shop_owner_id)) => {
                del_sync_shop(txn, user_id, &shop_owner_id, &label).await?;
            }
            // Handle other unexpected cases
            _ => {
                debug!("DEL-Tag: Unexpected combination of tag details");
            }
        }
    } else {
        return Err(EventProcessorError::SkipIndexing);
    }
    Ok(())
}

/// A marketplace target whose own DEL is removing its community tags.
#[derive(Clone, Copy)]
pub enum TagTarget<'a> {
    Listing {
        owner_id: &'a str,
        listing_id: &'a str,
    },
    Shop {
        owner_id: &'a str,
    },
}

/// Points of [`del_tagged_target_with_hook`] where integration tests inject
/// a failure or a concurrent write.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TargetTagCleanupStep {
    EdgesRead,
    EdgeDeleted,
    MarkersRead,
    TaggerCounted,
    TaggersPurged,
    TimelinePurged,
    SearchPruned,
    /// The round saw no edge, marker or indexed label; the target is not
    /// deleted yet.
    FinalCheckPassed,
    /// The target's write lock is held and the edges, markers and indexed
    /// labels were read empty under it; the node is about to be deleted.
    FinalLocked,
}

/// Internal deterministic seam for integration-testing cleanup retries.
#[doc(hidden)]
#[async_trait::async_trait]
pub trait TargetTagCleanupHook: Sync {
    async fn at(&self, _step: TargetTagCleanupStep) -> Result<(), EventProcessorError> {
        Ok(())
    }
}

#[doc(hidden)]
pub struct NoopTargetTagCleanupHook;

#[async_trait::async_trait]
impl TargetTagCleanupHook for NoopTargetTagCleanupHook {}

/// Rounds of [`del_tagged_target`] before it gives up and lets the event
/// retry: each round only repeats because a tag landed on the target
/// meanwhile.
const TARGET_TAG_ROUNDS: usize = 5;

/// Lifetime of a target's claim set. Claims are never deleted once the
/// target is gone: a cleanup still holding a marker it read before another
/// cleanup settled it must find the claim. Marker ids are fresh per edge, so
/// a re-created target never collides with an old claim.
const CLAIM_TTL_SECONDS: i64 = 30 * 24 * 60 * 60;

impl TagTarget<'_> {
    fn edges_query(self) -> Query {
        match self {
            TagTarget::Listing {
                owner_id,
                listing_id,
            } => queries::get::listing_tag_edges(owner_id, listing_id),
            TagTarget::Shop { owner_id } => queries::get::shop_tag_edges(owner_id),
        }
    }

    /// Names the target on its `TagCleanup` markers.
    fn marker_target(self) -> String {
        match self {
            TagTarget::Listing {
                owner_id,
                listing_id,
            } => format!("listing:{owner_id}:{listing_id}"),
            TagTarget::Shop { owner_id } => format!("shop:{owner_id}"),
        }
    }

    /// Takes the target's write lock; see [`queries::del::lock_listing`].
    fn lock_query(self) -> Query {
        match self {
            TagTarget::Listing {
                owner_id,
                listing_id,
            } => queries::del::lock_listing(owner_id, listing_id),
            TagTarget::Shop { owner_id } => queries::del::lock_shop(owner_id),
        }
    }

    fn delete_query(self) -> Query {
        match self {
            TagTarget::Listing {
                owner_id,
                listing_id,
            } => queries::del::delete_listing(owner_id, listing_id),
            TagTarget::Shop { owner_id } => queries::del::delete_shop(owner_id),
        }
    }

    fn edges_to_markers_query(self) -> Query {
        let target = self.marker_target();
        match self {
            TagTarget::Listing {
                owner_id,
                listing_id,
            } => queries::del::listing_tags_to_cleanup_markers(owner_id, listing_id, &target),
            TagTarget::Shop { owner_id } => {
                queries::del::shop_tags_to_cleanup_markers(owner_id, &target)
            }
        }
    }

    /// The set of marker ids whose tagger count this cleanup already
    /// decremented.
    fn claim_key(self) -> String {
        format!("Cleanup:Tags:{}", self.marker_target())
    }

    fn score_key(self) -> Vec<String> {
        match self {
            TagTarget::Listing {
                owner_id,
                listing_id,
            } => [&LISTING_TAGS_KEY_PARTS[..], &[owner_id, listing_id]]
                .concat()
                .into_iter()
                .map(str::to_string)
                .collect(),
            TagTarget::Shop { owner_id } => [&SHOP_TAGS_KEY_PARTS[..], &[owner_id]]
                .concat()
                .into_iter()
                .map(str::to_string)
                .collect(),
        }
    }

    /// Labels still in the target's label-score set. The set survives until
    /// every other index of a label is gone, so a retry can find labels
    /// whose graph edges were already deleted.
    async fn indexed_labels(self) -> Result<Vec<String>, EventProcessorError> {
        let key = self.score_key();
        let key: Vec<&str> = key.iter().map(String::as_str).collect();
        let labels = match self {
            TagTarget::Listing { .. } => {
                TagListing::try_from_index_sorted_set(
                    &key,
                    None,
                    None,
                    None,
                    None,
                    SortOrder::Descending,
                    None,
                )
                .await?
            }
            TagTarget::Shop { .. } => {
                TagShop::try_from_index_sorted_set(
                    &key,
                    None,
                    None,
                    None,
                    None,
                    SortOrder::Descending,
                    None,
                )
                .await?
            }
        };
        Ok(labels
            .unwrap_or_default()
            .into_iter()
            .map(|(label, _)| label)
            .collect())
    }

    /// Deletes the target node if it has no edge, marker or indexed label,
    /// read under its write lock. Returns whether the node is gone.
    ///
    /// Tag PUTs and untags hold the same lock from their graph write through
    /// their Redis writes, so under it none is half done: each either
    /// finished, and its edge or indexed label is seen here and refuses the
    /// deletion, or it starts after the deletion and finds no target.
    async fn delete_if_settled(
        self,
        hook: &dyn TargetTagCleanupHook,
    ) -> Result<bool, EventProcessorError> {
        let mut txn = start_graph_txn().await?;
        let result = self.delete_if_settled_in(&mut txn, hook).await;
        finish_txn(txn, result).await
    }

    async fn delete_if_settled_in(
        self,
        txn: &mut GraphTxn,
        hook: &dyn TargetTagCleanupHook,
    ) -> Result<bool, EventProcessorError> {
        if txn.fetch_row(self.lock_query()).await?.is_none() {
            return Ok(true);
        }
        let edges = txn.fetch_all(self.edges_query()).await?;
        let markers = txn
            .fetch_all(queries::get::tag_cleanup_markers(&self.marker_target()))
            .await?;
        let labels = self.indexed_labels().await?;
        if !(edges.is_empty() && markers.is_empty() && labels.is_empty()) {
            return Ok(false);
        }
        hook.at(TargetTagCleanupStep::FinalLocked).await?;
        txn.run(self.delete_query()).await?;
        Ok(true)
    }

    /// Removes one label's target indexes: taggers set, the listing's
    /// global-timeline membership, the autocomplete entry when the label is
    /// unused, and last the label score that lets a retry find the label.
    /// Every step is idempotent.
    async fn purge_label(
        self,
        label: &str,
        hook: &dyn TargetTagCleanupHook,
    ) -> Result<(), EventProcessorError> {
        match self {
            TagTarget::Listing {
                owner_id,
                listing_id,
            } => loop {
                let key = vec![owner_id, listing_id, label];
                let (taggers, _) =
                    <TagListing as TaggersCollection>::get_from_index(key, None, None, None, None)
                        .await?;
                if taggers.is_empty() {
                    break;
                }
                TagListing(taggers)
                    .del_from_index(owner_id, Some(listing_id), label)
                    .await?;
            },
            TagTarget::Shop { owner_id } => loop {
                let key = vec![owner_id, label];
                let (taggers, _) =
                    <TagShop as TaggersCollection>::get_from_index(key, None, None, None, None)
                        .await?;
                if taggers.is_empty() {
                    break;
                }
                TagShop(taggers)
                    .del_from_index(owner_id, None, label)
                    .await?;
            },
        }
        hook.at(TargetTagCleanupStep::TaggersPurged).await?;
        if let TagTarget::Listing {
            owner_id,
            listing_id,
        } = self
        {
            ListingsByTagSearch::del_from_index(owner_id, listing_id, label).await?;
        }
        hook.at(TargetTagCleanupStep::TimelinePurged).await?;
        TagSearch::del_from_index_if_unused(label).await?;
        hook.at(TargetTagCleanupStep::SearchPruned).await?;
        let key = self.score_key();
        let key: Vec<&str> = key.iter().map(String::as_str).collect();
        match self {
            TagTarget::Listing { .. } => {
                TagListing::remove_from_index_sorted_set(None, &key, &[label]).await?
            }
            TagTarget::Shop { .. } => {
                TagShop::remove_from_index_sorted_set(None, &key, &[label]).await?
            }
        }
        Ok(())
    }
}

/// Deletes a marketplace target (listing or shop) node whose own DEL is
/// being processed, together with every community tag on it. `DETACH
/// DELETE` of the target removes the `TAGGED` edges from the graph but
/// cannot reach their Redis indexes (label scores, taggers, the global label
/// timeline, tag search, tagger counts); left behind, they would reattach to
/// a record re-created at the same id. The caller deletes the target's own
/// Redis details afterwards.
///
/// Retry-safe at every step, and exact under racing tag events:
/// - One Cypher statement, taken under the target's write lock, deletes the
///   target's edges and leaves a
///   `TagCleanup` marker, with a fresh id, per edge it deleted. A tagger's
///   own untag that removes an edge first leaves no marker and counts itself
///   down; one that arrives after finds no edge and counts nothing.
/// - Each marker's tagger `tagged` count is decremented once through a
///   claim on the marker id, then the marker is deleted. A re-tag during the
///   cleanup is a new edge, so a later round gives it a new marker. Claims
///   outlive the target (see [`CLAIM_TTL_SECONDS`]), so an overlapping
///   cleanup that read a marker before another settled it counts nothing.
/// - Each label's target indexes are purged idempotently and its score entry
///   goes last, so a retry still finds the label once its edges are gone.
///
/// Rounds repeat until the target has no edge, no marker and no indexed
/// label, which also sweeps a tag that landed while the cleanup ran. The
/// node is then deleted in a transaction that takes its write lock and
/// re-reads edges, markers and indexed labels under it. Tag PUTs and untags
/// hold that lock from their graph write through their Redis writes: one
/// still running is waited for, and what it wrote is seen and refuses the
/// deletion, which then gets another round; one that starts after the
/// deletion finds no target and writes nothing.
pub async fn del_tagged_target(target: TagTarget<'_>) -> Result<(), EventProcessorError> {
    del_tagged_target_with_hook(target, &NoopTargetTagCleanupHook).await
}

/// [`del_tagged_target`] with deterministic interleaving points. Production
/// callers use [`del_tagged_target`].
#[doc(hidden)]
pub async fn del_tagged_target_with_hook(
    target: TagTarget<'_>,
    hook: &dyn TargetTagCleanupHook,
) -> Result<(), EventProcessorError> {
    let claim_key = target.claim_key();
    let marker_target = target.marker_target();
    for _ in 0..TARGET_TAG_ROUNDS {
        let edges = fetch_all_rows_from_graph(target.edges_query()).await?;
        let mut markers =
            fetch_all_rows_from_graph(queries::get::tag_cleanup_markers(&marker_target)).await?;
        let mut labels = target.indexed_labels().await?;
        if edges.is_empty() && markers.is_empty() && labels.is_empty() {
            hook.at(TargetTagCleanupStep::FinalCheckPassed).await?;
            if target.delete_if_settled(hook).await? {
                return Ok(());
            }
            continue;
        }
        for row in &edges {
            let label: String = row
                .get("label")
                .map_err(EventProcessorError::graph_query_failed)?;
            if !labels.contains(&label) {
                labels.push(label);
            }
        }
        hook.at(TargetTagCleanupStep::EdgesRead).await?;
        if !edges.is_empty() {
            exec_single_row(target.edges_to_markers_query()).await?;
            hook.at(TargetTagCleanupStep::EdgeDeleted).await?;
            markers = fetch_all_rows_from_graph(queries::get::tag_cleanup_markers(&marker_target))
                .await?;
        }
        hook.at(TargetTagCleanupStep::MarkersRead).await?;
        for marker in markers {
            let id: String = marker
                .get("id")
                .map_err(EventProcessorError::graph_query_failed)?;
            let tagger_id: String = marker
                .get("tagger_id")
                .map_err(EventProcessorError::graph_query_failed)?;
            let label: String = marker
                .get("label")
                .map_err(EventProcessorError::graph_query_failed)?;
            UserCounts::decrement_once(&tagger_id, "tagged", &claim_key, &id, CLAIM_TTL_SECONDS)
                .await?;
            hook.at(TargetTagCleanupStep::TaggerCounted).await?;
            exec_single_row(queries::del::delete_tag_cleanup_marker(&id)).await?;
            if !labels.contains(&label) {
                labels.push(label);
            }
        }
        for label in &labels {
            target.purge_label(label, hook).await?;
        }
    }
    Err(EventProcessorError::generic(
        "Marketplace target kept gaining tags while its DEL removed them",
    ))
}

/// Removes the tag indexes of a post that is being deleted, in the
/// deletion's transaction (which holds the post's write lock, so no tag
/// write is half done): every label's taggers, its global timeline and
/// engagement entries, its score entry and, once no edge carries the label,
/// its autocomplete entry. Tag edges still on the post (a moderated post can
/// have them) are deleted first. Returns the tagger of each deleted edge;
/// the caller decrements their `tagged` count as its last write.
pub async fn purge_deleted_post_tags(
    txn: &mut GraphTxn,
    author_id: &str,
    post_id: &str,
) -> Result<Vec<String>, EventProcessorError> {
    let mut taggers = Vec::new();
    let mut labels = Vec::new();
    for row in txn
        .fetch_all(queries::del::delete_post_tags(author_id, post_id))
        .await?
    {
        taggers.push(
            row.get::<String>("tagger_id")
                .map_err(EventProcessorError::graph_query_failed)?,
        );
        let label: String = row
            .get("label")
            .map_err(EventProcessorError::graph_query_failed)?;
        if !labels.contains(&label) {
            labels.push(label);
        }
    }
    let score_key: Vec<&str> = [&POST_TAGS_KEY_PARTS[..], &[author_id, post_id]].concat();
    for (label, _) in TagPost::try_from_index_sorted_set(
        &score_key,
        None,
        None,
        None,
        None,
        SortOrder::Descending,
        None,
    )
    .await?
    .unwrap_or_default()
    {
        if !labels.contains(&label) {
            labels.push(label);
        }
    }
    for label in &labels {
        loop {
            let (label_taggers, _) = <TagPost as TaggersCollection>::get_from_index(
                vec![author_id, post_id, label],
                None,
                None,
                None,
                None,
            )
            .await?;
            if label_taggers.is_empty() {
                break;
            }
            TagPost(label_taggers)
                .del_from_index(author_id, Some(post_id), label)
                .await?;
        }
        PostsByTagSearch::purge_post(author_id, post_id, label).await?;
        TagSearch::del_from_index_if_unused_in(txn, label).await?;
        TagPost::remove_from_index_sorted_set(None, &score_key, &[label]).await?;
    }
    Ok(taggers)
}

/// [`purge_deleted_post_tags`] for a user that is being deleted. A user is
/// deleted outright only when it has no relationship, so no edge is left to
/// delete.
pub async fn purge_deleted_user_tags(
    txn: &mut GraphTxn,
    user_id: &str,
) -> Result<(), EventProcessorError> {
    let score_key: Vec<&str> = [&USER_TAGS_KEY_PARTS[..], &[user_id]].concat();
    let labels: Vec<String> = TagUser::try_from_index_sorted_set(
        &score_key,
        None,
        None,
        None,
        None,
        SortOrder::Descending,
        None,
    )
    .await?
    .unwrap_or_default()
    .into_iter()
    .map(|(label, _)| label)
    .collect();
    for label in &labels {
        loop {
            let (label_taggers, _) = <TagUser as TaggersCollection>::get_from_index(
                vec![user_id, label],
                None,
                None,
                None,
                None,
            )
            .await?;
            if label_taggers.is_empty() {
                break;
            }
            TagUser(label_taggers)
                .del_from_index(user_id, None, label)
                .await?;
        }
        TagSearch::del_from_index_if_unused_in(txn, label).await?;
        TagUser::remove_from_index_sorted_set(None, &score_key, &[label]).await?;
    }
    Ok(())
}

async fn del_sync_user(
    txn: &mut GraphTxn,
    tagger_id: PubkyId,
    tagged_id: &str,
    tag_label: &str,
) -> Result<(), EventProcessorError> {
    let indexing_results = tokio::join!(
        // Update user counts in the tagged
        UserCounts::decrement(tagged_id, "tags", None),
        // Update user counts in the tagger
        UserCounts::decrement(&tagger_id, "tagged", None),
        async {
            // Decrement label count to the user profile tag
            TagUser::update_index_score(tagged_id, None, tag_label, ScoreAction::Decrement(1.0))
                .await?;
            // Decrease unique_tags
            // NOTE: To update that field, we first need to decrement the value in the TagUser SORTED SET associated with that tag
            UserCounts::decrement(tagged_id, "unique_tags", Some(tag_label)).await?;
            Ok::<(), EventProcessorError>(())
        },
        async {
            // Remove tagger to the user taggers list
            TagUser(vec![tagger_id.to_string()])
                .del_from_index(tagged_id, None, tag_label)
                .await?;
            Ok::<(), EventProcessorError>(())
        },
        // Save new notification
        Notification::new_user_untag(&tagger_id, tagged_id, tag_label),
        // Drop the label from autocomplete once no target uses it
        TagSearch::del_from_index_if_unused_in(txn, tag_label)
    );

    indexing_results.0?;
    indexing_results.1?;
    indexing_results.2?;
    indexing_results.3?;
    indexing_results.4?;
    indexing_results.5?;

    Ok(())
}

/// Removes a deleted listing tag from the Redis indexes: the tagger's global
/// "tagged" count, the listing's label score and taggers set, the global
/// label timeline, and — when the label's last listing is gone — the tag
/// search suggestions. Mirrors [`del_sync_post`] minus per-listing counts,
/// engagement scoring, and notifications.
async fn del_sync_listing(
    txn: &mut GraphTxn,
    tagger_id: PubkyId,
    listing_id: &str,
    owner_id: &str,
    tag_label: &str,
) -> Result<(), EventProcessorError> {
    let indexing_results = tokio::join!(
        // Update user counts for tagger
        UserCounts::decrement(&tagger_id, "tagged", None),
        // Decrement label score in the listing
        TagListing::update_index_score(
            owner_id,
            Some(listing_id),
            tag_label,
            ScoreAction::Decrement(1.0),
        ),
        async {
            // Delete the tagger from the tag list
            TagListing(vec![tagger_id.to_string()])
                .del_from_index(owner_id, Some(listing_id), tag_label)
                .await?;
            // NOTE: The by-tag timeline depends on the listing taggers collection to delete
            // Delete listing from the global label timeline
            ListingsByTagSearch::del_from_index(owner_id, listing_id, tag_label).await?;
            Ok::<(), EventProcessorError>(())
        },
        // Drop the label from autocomplete once no target uses it
        TagSearch::del_from_index_if_unused_in(txn, tag_label)
    );

    indexing_results.0?;
    indexing_results.1?;
    indexing_results.2?;
    indexing_results.3?;

    Ok(())
}

/// Removes a deleted shop tag from the Redis indexes. Mirrors
/// [`del_sync_user`] minus per-target counts and notifications.
async fn del_sync_shop(
    txn: &mut GraphTxn,
    tagger_id: PubkyId,
    owner_id: &str,
    tag_label: &str,
) -> Result<(), EventProcessorError> {
    let indexing_results = tokio::join!(
        // Update user counts for tagger
        UserCounts::decrement(&tagger_id, "tagged", None),
        // Decrement label score on the shop
        TagShop::update_index_score(owner_id, None, tag_label, ScoreAction::Decrement(1.0)),
        async {
            // Remove tagger from the shop taggers list
            TagShop(vec![tagger_id.to_string()])
                .del_from_index(owner_id, None, tag_label)
                .await?;
            Ok::<(), EventProcessorError>(())
        },
        // Drop the label from autocomplete once no target uses it
        TagSearch::del_from_index_if_unused_in(txn, tag_label)
    );

    indexing_results.0?;
    indexing_results.1?;
    indexing_results.2?;
    indexing_results.3?;

    Ok(())
}

async fn del_sync_post(
    txn: &mut GraphTxn,
    tagger_id: PubkyId,
    post_id: &str,
    author_id: &str,
    tag_label: &str,
) -> Result<(), EventProcessorError> {
    // SAVE TO INDEXES
    let post_key_slice: &[&str] = &[author_id, post_id];
    let tag_post = TagPost(vec![tagger_id.to_string()]);
    let post_uri = post_uri_builder(author_id.to_string(), post_id.to_string());

    let indexing_results = tokio::join!(
        // Update user counts for tagger
        UserCounts::decrement(&tagger_id, "tagged", None),
        // Decrement in one the post tags
        PostCounts::decrement_index_field(post_key_slice, "tags", None),
        async {
            // Decrement label score in the post
            TagPost::update_index_score(
                author_id,
                Some(post_id),
                tag_label,
                ScoreAction::Decrement(1.0),
            )
            .await?;
            // Decrease unique_tag
            // NOTE: To update that field, we first need to decrement the value in the SORTED SET associated with that tag
            PostCounts::decrement_index_field(post_key_slice, "unique_tags", Some(tag_label))
                .await?;
            Ok::<(), EventProcessorError>(())
        },
        // Decrease post from label total engagement
        PostsByTagSearch::update_index_score(
            author_id,
            post_id,
            tag_label,
            ScoreAction::Decrement(1.0),
        ),
        async {
            // Post replies cannot be included in the total engagement index once the tag have been deleted
            if !post_relationships_is_reply(author_id, post_id).await? {
                // Decrement in one post global engagement
                PostStream::update_index_score(author_id, post_id, ScoreAction::Decrement(1.0))
                    .await
                    .map_err(EventProcessorError::index_operation_failed)?;
            }
            Ok::<(), EventProcessorError>(())
        },
        async {
            // Delete the tagger from the tag list
            tag_post
                .del_from_index(author_id, Some(post_id), tag_label)
                .await?;
            // NOTE: The tag search index, depends on the post taggers collection to delete
            // Delete post from global label timeline
            PostsByTagSearch::del_from_index(author_id, post_id, tag_label).await?;
            Ok::<(), EventProcessorError>(())
        },
        // Save new notification
        Notification::new_post_untag(&tagger_id, author_id, tag_label, &post_uri),
        // Drop the label from autocomplete once no target uses it
        TagSearch::del_from_index_if_unused_in(txn, tag_label)
    );

    indexing_results.0?;
    indexing_results.1?;
    indexing_results.2?;
    indexing_results.3?;
    indexing_results.4?;
    indexing_results.5?;
    indexing_results.6?;
    indexing_results.7?;

    Ok(())
}
