//! Recovery of a failed tag write, untag or post deletion.
//!
//! A tag write runs its graph changes in a transaction and its Redis writes
//! outside it. When the write fails, or its commit fails, the graph is back to
//! (or, for a commit whose reply was lost, ahead at) the committed state, while
//! Redis holds whichever commands applied: a command that reported failure may
//! or may not have applied, so the writes cannot be told apart, let alone taken
//! back one by one. Recovery instead rebuilds what the write touched from the
//! committed graph, and does it under the write locks of the target and of the
//! users whose counters it rebuilds, so no other write on them interleaves.
//! Because it reads the graph and writes absolute values, it gives the same
//! result whether the failed write applied none, some or all of its commands,
//! and whether or not the commit took effect, and running it twice changes
//! nothing.
//!
//! Rebuilt from the graph: the target's label scores, taggers sets, timeline
//! and engagement entries, its own tag counters, the tagger's `tagged` counter,
//! and the autocomplete entry of each label involved. For a post deletion also
//! the post's details, counts and relationships (or their absence, when the
//! deletion committed), the author's and the parents' counters, and the
//! taggers'.
//!
//! Counters other events move (a user's `posts`, a post's `replies`) are set
//! field by field to the graph's count, so a recovery replaces only the fields
//! a tag write or post deletion changes.

use std::collections::BTreeSet;

use crate::events::handlers::tag::TagWriteStep;
use crate::events::handlers::tag_index::{
    label_engagement_key, label_timeline_key, refs, timeline, OwnedTarget, RawIndex, StepHook,
    Target,
};
use crate::events::handlers::utils::{finish_txn, TargetDeleteStep};
use crate::events::EventProcessorError;
use nexus_common::db::{queries, start_graph_txn, GraphTxn, RedisOps};
use nexus_common::models::post::{PostCounts, PostDetails, PostRelationships, PostStream};
use nexus_common::models::tag::search::TagSearch;
use nexus_common::models::user::{UserCounts, UserStream};

/// What a failed write touched.
#[derive(Clone, Debug)]
pub struct Recovery {
    pub target: OwnedTarget,
    /// Users whose `tagged` counter the write moved.
    pub taggers: Vec<String>,
    /// Labels the write moved, besides the ones the graph and Redis hold.
    pub labels: Vec<String>,
    /// Set for a post deletion.
    pub post: Option<PostDeletion>,
}

/// The relationships a post deletion touched.
#[derive(Clone, Debug, Default)]
pub struct PostDeletion {
    pub replied: Option<(String, String)>,
    pub reposted: Option<(String, String)>,
}

impl Recovery {
    pub fn tag_write(target: OwnedTarget, tagger_id: &str, label: &str) -> Self {
        Self {
            target,
            taggers: vec![tagger_id.to_string()],
            labels: vec![label.to_string()],
            post: None,
        }
    }
}

/// Rebuilds what `spec` names from the committed graph, in a transaction that
/// holds their locks.
pub async fn recover(spec: &Recovery, hook: StepHook<'_>) -> Result<(), EventProcessorError> {
    let mut txn = start_graph_txn().await?;
    let result = lock_and_rebuild(&mut txn, spec, hook).await;
    finish_txn(txn, result).await
}

async fn lock(
    txn: &mut GraphTxn,
    query: nexus_common::db::graph::Query,
) -> Result<(), EventProcessorError> {
    txn.fetch_row(query).await?;
    Ok(())
}

async fn lock_and_rebuild(
    txn: &mut GraphTxn,
    spec: &Recovery,
    hook: StepHook<'_>,
) -> Result<(), EventProcessorError> {
    // The target first, as every tag write and deletion does, then the users
    // and posts whose counters are rebuilt, each group in id order.
    let target = spec.target.as_target();
    match target {
        Target::Post { author_id, post_id } => {
            lock(txn, queries::del::lock_post(author_id, post_id)).await?
        }
        Target::User { user_id } => lock(txn, queries::del::lock_user(user_id)).await?,
        Target::Listing {
            owner_id,
            listing_id,
        } => lock(txn, queries::del::lock_listing(owner_id, listing_id)).await?,
        Target::Shop { owner_id } => lock(txn, queries::del::lock_shop(owner_id)).await?,
    }
    let mut users: BTreeSet<&str> = spec.taggers.iter().map(String::as_str).collect();
    if let OwnedTarget::Post(author_id, _) = &spec.target {
        if spec.post.is_some() {
            users.insert(author_id);
        }
    }
    for user_id in &users {
        lock(txn, queries::del::lock_user(user_id)).await?;
    }
    if let Some(deletion) = &spec.post {
        let posts: BTreeSet<&(String, String)> = deletion
            .replied
            .iter()
            .chain(deletion.reposted.iter())
            .collect();
        for (author_id, post_id) in posts {
            lock(txn, queries::del::lock_post(author_id, post_id)).await?;
        }
    }
    match hook {
        StepHook::Tag(hook) => hook.at(TagWriteStep::Recovering).await?,
        StepHook::Delete(hook) => hook.at(TargetDeleteStep::Recovering).await?,
    }

    let mut labels: BTreeSet<String> = spec.labels.iter().cloned().collect();
    if let Some(deletion) = &spec.post {
        rebuild_post(txn, &spec.target, deletion).await?;
    }
    labels.extend(rebuild_tags(txn, target).await?);
    match target {
        Target::Post { author_id, post_id } if spec.post.is_none() => {
            rebuild_post_counters(txn, author_id, post_id, &["tags", "unique_tags"]).await?
        }
        Target::User { user_id } => {
            rebuild_user_counters(txn, user_id, &["tags", "unique_tags"]).await?
        }
        _ => {}
    }
    for tagger_id in &spec.taggers {
        rebuild_user_counters(txn, tagger_id, &["tagged"]).await?;
    }
    for label in &labels {
        TagSearch::sync_label_in(txn, label).await?;
    }
    Ok(())
}

/// Makes the target's tag indexes match the graph: the label scores, each
/// label's taggers set, and, for posts and listings, each label's timeline and
/// (posts) engagement entry. Returns every label it looked at.
async fn rebuild_tags(
    txn: &mut GraphTxn,
    target: Target<'_>,
) -> Result<BTreeSet<String>, EventProcessorError> {
    let tags = target.graph_tags(txn).await?;
    let mut labels: BTreeSet<String> = tags.iter().map(|tag| tag.label.clone()).collect();
    labels.extend(target.indexed_labels().await?);

    let score_key = target.score_key();
    RawIndex::delete_sorted_set_index(&refs(&score_key)).await?;
    let scores: Vec<(f64, &str)> = tags
        .iter()
        .map(|tag| (tag.taggers_count as f64, tag.label.as_str()))
        .collect();
    if !scores.is_empty() {
        RawIndex::put_index_sorted_set(&refs(&score_key), &scores, None, None).await?;
    }

    let engagement = match target {
        Target::Post { author_id, post_id } => {
            let row = txn
                .fetch_row(queries::get::post_tag_engagement(author_id, post_id))
                .await?;
            row.and_then(|row| row.get::<f64>("score").ok())
        }
        _ => None,
    };
    for label in &labels {
        let taggers: Vec<String> = tags
            .iter()
            .find(|tag| &tag.label == label)
            .map(|tag| tag.taggers.clone())
            .unwrap_or_default();
        target.replace_taggers(label, &taggers).await?;
        let present = !taggers.is_empty();
        if let Some((key, member)) = label_timeline_key(&target, label) {
            if present {
                timeline(target, label, true).await?;
            } else {
                RawIndex::remove_from_index_sorted_set(None, &refs(&key), &[&member]).await?;
            }
        }
        if let Target::Post { author_id, post_id } = target {
            let key = label_engagement_key(label);
            let member = format!("{author_id}:{post_id}");
            match (present, engagement) {
                (true, Some(score)) => {
                    RawIndex::put_index_sorted_set(&refs(&key), &[(score, &member)], None, None)
                        .await?
                }
                _ => RawIndex::remove_from_index_sorted_set(None, &refs(&key), &[&member]).await?,
            }
        }
    }
    Ok(labels)
}

fn counter(counts: &UserCounts, field: &str) -> i64 {
    match field {
        "tagged" => counts.tagged,
        "tags" => counts.tags,
        "unique_tags" => counts.unique_tags,
        "posts" => counts.posts,
        "replies" => counts.replies,
        other => unreachable!("no user counter {other} is rebuilt"),
    }
    .into()
}

fn post_counter(counts: &PostCounts, field: &str) -> i64 {
    match field {
        "tags" => counts.tags,
        "unique_tags" => counts.unique_tags,
        "replies" => counts.replies,
        "reposts" => counts.reposts,
        other => unreachable!("no post counter {other} is rebuilt"),
    }
    .into()
}

/// Sets the named counters of a user to the graph's count and refreshes the
/// influencer ranking they feed.
async fn rebuild_user_counters(
    txn: &mut GraphTxn,
    user_id: &str,
    fields: &[&str],
) -> Result<(), EventProcessorError> {
    let Some(from_graph) = UserCounts::get_from_graph_in(txn, user_id).await? else {
        return Ok(());
    };
    for field in fields {
        UserCounts::set_json_field(&[user_id], field, counter(&from_graph, field)).await?;
    }
    if let Some(counts) = UserCounts::get_from_index(user_id).await? {
        UserStream::add_to_influencers_sorted_set(user_id, &counts).await?;
    }
    Ok(())
}

/// Sets the named counters of a post to the graph's count and refreshes its
/// total engagement.
async fn rebuild_post_counters(
    txn: &mut GraphTxn,
    author_id: &str,
    post_id: &str,
    fields: &[&str],
) -> Result<(), EventProcessorError> {
    let Some((from_graph, is_reply)) =
        PostCounts::get_from_graph_in(txn, author_id, post_id).await?
    else {
        return Ok(());
    };
    for field in fields {
        PostCounts::set_json_field(
            &[author_id, post_id],
            field,
            post_counter(&from_graph, field),
        )
        .await?;
    }
    if !is_reply {
        if let Some(counts) = PostCounts::get_from_index(author_id, post_id).await? {
            PostStream::add_to_engagement_sorted_set(&counts, author_id, post_id).await?;
        }
    }
    Ok(())
}

/// Rebuilds a post a deletion may have removed: its details, counts and
/// relationships when it is in the graph, their absence when it is not, and
/// the counters the deletion moved on its author and its parents.
async fn rebuild_post(
    txn: &mut GraphTxn,
    target: &OwnedTarget,
    deletion: &PostDeletion,
) -> Result<(), EventProcessorError> {
    let OwnedTarget::Post(author_id, post_id) = target else {
        return Ok(());
    };
    match PostDetails::get_from_graph_in(txn, author_id, post_id).await? {
        Some((details, reply)) => {
            details.put_to_index(author_id, reply, false).await?;
            if let Some((counts, is_reply)) =
                PostCounts::get_from_graph_in(txn, author_id, post_id).await?
            {
                counts.put_to_index(author_id, post_id, !is_reply).await?;
            }
            if let Some(relationships) =
                PostRelationships::get_from_graph_in(txn, author_id, post_id).await?
            {
                relationships.put_to_index(author_id, post_id).await?;
            }
        }
        None => {
            PostDetails::remove_from_index_multiple_json(&[&[author_id, post_id]]).await?;
            PostCounts::delete(author_id, post_id, deletion.replied.is_none()).await?;
            PostRelationships::delete(author_id, post_id).await?;
            let parent = deletion
                .replied
                .as_ref()
                .map(|(parent_author, parent_post)| [parent_author.clone(), parent_post.clone()]);
            PostDetails::delete_stream_entries(author_id, post_id, parent).await?;
        }
    }
    rebuild_user_counters(txn, author_id, &["posts", "replies"]).await?;
    if let Some((parent_author, parent_post)) = &deletion.replied {
        rebuild_post_counters(txn, parent_author, parent_post, &["replies", "reposts"]).await?;
    }
    if let Some((parent_author, parent_post)) = &deletion.reposted {
        rebuild_post_counters(txn, parent_author, parent_post, &["replies", "reposts"]).await?;
    }
    Ok(())
}
