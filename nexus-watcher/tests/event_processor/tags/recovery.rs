//! A tag PUT, an untag or a post deletion that fails after a Redis write
//! started, or whose commit fails, rebuilds what it touched from the committed
//! graph. After every failure the Redis state equals what a fresh reindex of
//! the graph produces.
//!
//! Failures are injected between the write's Redis commands: before the n-th
//! (a command that never applied), after it (a command that applied and
//! reported failure), before the commit, at the commit (rolled back) and at the
//! commit with a lost reply (committed, reported failed).

use super::delete_race::{
    graph_count, new_user, redis_state, wait_until_blocked, world, Gate, Kind, World,
};
use crate::event_processor::posts::utils::find_post_counts;
use crate::event_processor::utils::watcher::WatcherTest;
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use nexus_common::db::graph::Query;
use nexus_common::db::{get_redis_conn, RedisOps};
use nexus_common::models::event::EventProcessorError;
use nexus_common::models::post::search::PostsByTagSearch;
use nexus_common::models::post::{
    PostCounts, PostDetails, PostRelationships, POST_PER_USER_KEY_PARTS,
    POST_REPLIES_PER_POST_KEY_PARTS, POST_REPLIES_PER_USER_KEY_PARTS, POST_TIMELINE_KEY_PARTS,
    POST_TOTAL_ENGAGEMENT_KEY_PARTS,
};
use nexus_common::models::tag::listing::TagListing;
use nexus_common::models::tag::post::TagPost;
use nexus_common::models::tag::search::TagSearch;
use nexus_common::models::tag::shop::TagShop;
use nexus_common::models::tag::traits::TagCollection;
use nexus_common::models::tag::user::TagUser;
use nexus_common::models::user::UserCounts;
use nexus_watcher::events::handlers::post;
use nexus_watcher::events::handlers::tag::{self, TagWriteHook, TagWriteStep};
use nexus_watcher::events::handlers::utils::{TargetDeleteHook, TargetDeleteStep};
use pubky_app_specs::traits::HashId;
use pubky_app_specs::{
    post_uri_builder, user_uri_builder, PubkyAppPost, PubkyAppPostKind, PubkyAppTag, PubkyId,
};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

type OpResult = Result<(), EventProcessorError>;

#[derive(Clone, Copy, Debug)]
enum Mode {
    /// Runs every step, then fails before the commit; counts the steps.
    Count,
    BeforeStep(u32),
    AfterStep(u32),
    /// The commit fails without reaching the server.
    CommitFails,
    /// The commit is applied and its reply is lost.
    CommitReplyLost,
}

struct Inject {
    mode: Mode,
    steps: AtomicU32,
}

impl Inject {
    fn new(mode: Mode) -> Self {
        Self {
            mode,
            steps: AtomicU32::new(0),
        }
    }

    fn steps(&self) -> u32 {
        self.steps.load(Ordering::SeqCst)
    }

    fn injected() -> EventProcessorError {
        EventProcessorError::IndexOperationFailed("injected failure".to_string())
    }

    fn step(&self, before: bool, n: u32) -> OpResult {
        if before {
            self.steps.fetch_max(n, Ordering::SeqCst);
        }
        match (self.mode, before) {
            (Mode::BeforeStep(k), true) if k == n => Err(Self::injected()),
            (Mode::AfterStep(k), false) if k == n => Err(Self::injected()),
            _ => Ok(()),
        }
    }

    fn commit(&self) -> OpResult {
        match self.mode {
            Mode::Count => Err(Self::injected()),
            Mode::CommitFails => {
                nexus_common::db::fail_next_commit(false);
                Ok(())
            }
            Mode::CommitReplyLost => {
                nexus_common::db::fail_next_commit(true);
                Ok(())
            }
            _ => Ok(()),
        }
    }
}

#[async_trait]
impl TagWriteHook for Inject {
    async fn at(&self, step: TagWriteStep) -> OpResult {
        match step {
            TagWriteStep::BeforeIndexStep(n) => self.step(true, n),
            TagWriteStep::IndexStep(n) => self.step(false, n),
            TagWriteStep::BeforeCommit => self.commit(),
            _ => Ok(()),
        }
    }
}

#[async_trait]
impl TargetDeleteHook for Inject {
    async fn at(&self, step: TargetDeleteStep) -> OpResult {
        match step {
            TargetDeleteStep::BeforeIndexStep(n) => self.step(true, n),
            TargetDeleteStep::IndexStep(n) => self.step(false, n),
            TargetDeleteStep::BeforeCommit => self.commit(),
            _ => Ok(()),
        }
    }
}

/// Redis values named by `patterns` and the score of each `(sorted set,
/// member)`.
async fn state_of(patterns: &[String], scores: &[(String, String)]) -> Result<Vec<String>> {
    use deadpool_redis::redis::cmd;
    let mut conn = get_redis_conn().await?;
    let mut keys: Vec<String> = Vec::new();
    for pattern in patterns {
        let mut found: Vec<String> = cmd("KEYS").arg(pattern).query_async(&mut conn).await?;
        keys.append(&mut found);
    }
    keys.sort();
    keys.dedup();
    let mut state = Vec::new();
    for key in &keys {
        state.push(super::delete_race::dump_key(&mut conn, key).await?);
    }
    for (key, member) in scores {
        let score: Option<f64> = cmd("ZSCORE")
            .arg(key)
            .arg(member)
            .query_async(&mut conn)
            .await?;
        state.push(format!("{key}[{member}] = {score:?}"));
    }
    Ok(state)
}

async fn delete_keys(
    patterns: &[String],
    exact: &[String],
    members: &[(String, String)],
) -> Result<()> {
    use deadpool_redis::redis::cmd;
    let mut conn = get_redis_conn().await?;
    let mut keys: Vec<String> = exact.to_vec();
    for pattern in patterns {
        let mut found: Vec<String> = cmd("KEYS").arg(pattern).query_async(&mut conn).await?;
        keys.append(&mut found);
    }
    for key in keys {
        cmd("DEL").arg(key).exec_async(&mut conn).await?;
    }
    for (key, member) in members {
        cmd("ZREM")
            .arg(key)
            .arg(member)
            .exec_async(&mut conn)
            .await?;
    }
    Ok(())
}

fn joined(parts: &[&str], tail: &[&str]) -> String {
    [parts, tail].concat().join(":")
}

async fn counts_key<T: RedisOps>(parts: &[&str]) -> String {
    format!("{}:{}", T::prefix().await, parts.join(":"))
}

/// Deletes the Redis entries a reindex of the graph rebuilds for the world's
/// target and its tagger, then reindexes them from the graph.
async fn reindex_from_graph(w: &World) -> Result<()> {
    let (owner, id) = (w.owner_id.as_str(), w.target_id.as_str());
    let mut patterns = vec![format!("*{}*", w.label)];
    let mut exact: Vec<String> = Vec::new();
    let mut members = vec![
        ("Sorted:Tags:Label".to_string(), w.label.clone()),
        ("Sorted:Users:Influencers".to_string(), w.tagger_id.clone()),
        ("Sorted:Users:MostFollowed".to_string(), w.tagger_id.clone()),
    ];
    exact.push(counts_key::<UserCounts>(&[&w.tagger_id]).await);
    match w.kind {
        Kind::Post => {
            exact.push(format!("Sorted:Posts:Tag:{owner}:{id}"));
            exact.push(counts_key::<PostCounts>(&[owner, id]).await);
            members.push((
                joined(&POST_TOTAL_ENGAGEMENT_KEY_PARTS, &[]),
                format!("{owner}:{id}"),
            ));
        }
        Kind::User => {
            exact.push(format!("Sorted:Users:Tag:{owner}"));
            exact.push(counts_key::<UserCounts>(&[owner]).await);
            members.push(("Sorted:Users:Influencers".to_string(), owner.to_string()));
            members.push(("Sorted:Users:MostFollowed".to_string(), owner.to_string()));
        }
        Kind::Listing => {
            exact.push(format!("Sorted:Listings:Tag:{owner}:{id}"));
            // Listings have no by-tag reindex; their timeline is kept.
            patterns = vec![format!("*Listing:Taggers*{}*", w.label)];
        }
        Kind::Shop => exact.push(format!("Sorted:Shops:Tag:{owner}")),
    }
    delete_keys(&patterns, &exact, &members).await?;
    match w.kind {
        Kind::Post => {
            TagPost::reindex(owner, Some(id)).await?;
            PostCounts::reindex(owner, id).await?;
            PostsByTagSearch::reindex().await?;
        }
        Kind::User => {
            TagUser::reindex(owner, None).await?;
            UserCounts::reindex(owner).await?;
        }
        Kind::Listing => TagListing::reindex(owner, Some(id)).await?,
        Kind::Shop => TagShop::reindex(owner, None).await?,
    }
    UserCounts::reindex(&w.tagger_id).await?;
    TagSearch::reindex().await?;
    Ok(())
}

/// Asserts the world's Redis state equals a fresh reindex of the graph.
async fn assert_matches_reindex(w: &World, context: &str) -> Result<()> {
    let actual = redis_state(w).await?;
    reindex_from_graph(w).await?;
    let oracle = redis_state(w).await?;
    if actual != oracle {
        let only_actual: Vec<&String> = actual
            .iter()
            .filter(|line| !oracle.contains(line))
            .collect();
        let only_oracle: Vec<&String> = oracle
            .iter()
            .filter(|line| !actual.contains(line))
            .collect();
        panic!(
            "{context}: Redis differs from a fresh reindex.\nonly in Redis: {only_actual:#?}\nonly in the reindex: {only_oracle:#?}"
        );
    }
    Ok(())
}

async fn put_with(w: &World, mode: Mode) -> Result<()> {
    let hook = Inject::new(mode);
    let outcome = tag::sync_put_with_hook(w.tag.clone(), w.tagger(), w.tag_id(), &hook).await;
    assert!(
        outcome.is_err(),
        "{:?}: the injected failure must surface",
        mode
    );
    Ok(())
}

async fn untag_with(w: &World, mode: Mode) -> Result<()> {
    let hook = Inject::new(mode);
    let outcome = tag::del_with_hook(w.tagger(), w.tag_id(), &hook).await;
    assert!(
        outcome.is_err(),
        "{:?}: the injected failure must surface",
        mode
    );
    Ok(())
}

async fn steps_of_put(w: &World) -> Result<u32> {
    let hook = Inject::new(Mode::Count);
    let outcome = tag::sync_put_with_hook(w.tag.clone(), w.tagger(), w.tag_id(), &hook).await;
    assert!(outcome.is_err());
    Ok(hook.steps())
}

async fn steps_of_untag(w: &World) -> Result<u32> {
    let hook = Inject::new(Mode::Count);
    let outcome = tag::del_with_hook(w.tagger(), w.tag_id(), &hook).await;
    assert!(outcome.is_err());
    Ok(hook.steps())
}

/// A tag PUT and an untag fail at every step, at the commit and with a lost
/// commit reply, for every target kind; each time Redis matches the graph.
#[tokio_shared_rt::test(shared)]
async fn a_failed_tag_write_leaves_redis_equal_to_a_reindex_of_the_graph() -> Result<()> {
    let mut test = WatcherTest::setup().await?;
    for kind in Kind::ALL {
        let w = world(&mut test, kind).await?;
        let context = format!("{kind:?}");

        // PUT, from the untagged state.
        let steps = steps_of_put(&w).await?;
        assert!(steps >= 3, "{context}: only {steps} Redis steps ran");
        assert_eq!(w.edge_count().await?, 0, "{context}");
        assert_matches_reindex(&w, &format!("{context}: PUT failed before its commit")).await?;
        for step in 1..=steps {
            for mode in [Mode::BeforeStep(step), Mode::AfterStep(step)] {
                put_with(&w, mode).await?;
                assert_eq!(w.edge_count().await?, 0, "{context} {mode:?}");
                assert_matches_reindex(&w, &format!("{context}: PUT {mode:?}")).await?;
            }
        }
        let statements = nexus_common::db::autocommit_statement_count();
        put_with(&w, Mode::CommitFails).await?;
        assert_eq!(
            nexus_common::db::autocommit_statement_count(),
            statements,
            "{context}: the write and its recovery use only their transactions' connections"
        );
        assert_eq!(w.edge_count().await?, 0, "{context}");
        assert_matches_reindex(&w, &format!("{context}: PUT commit failed")).await?;
        assert_eq!(nexus_common::db::open_txn_count(), 0, "{context}");

        // The retried PUT indexes the tag once.
        w.put_now().await?;
        w.assert_kept_with_tag(&context).await?;

        // Untag, from the tagged state.
        let steps = steps_of_untag(&w).await?;
        assert!(steps >= 3, "{context}: only {steps} Redis steps ran");
        assert_eq!(w.edge_count().await?, 1, "{context}");
        assert_matches_reindex(&w, &format!("{context}: untag failed before its commit")).await?;
        for step in 1..=steps {
            for mode in [Mode::BeforeStep(step), Mode::AfterStep(step)] {
                untag_with(&w, mode).await?;
                assert_eq!(w.edge_count().await?, 1, "{context} untag {mode:?}");
                assert_matches_reindex(&w, &format!("{context}: untag {mode:?}")).await?;
            }
        }
        let statements = nexus_common::db::autocommit_statement_count();
        untag_with(&w, Mode::CommitFails).await?;
        assert_eq!(
            nexus_common::db::autocommit_statement_count(),
            statements,
            "{context}: the untag and its recovery use only their transactions' connections"
        );
        assert_eq!(w.edge_count().await?, 1, "{context}");
        assert_matches_reindex(&w, &format!("{context}: untag commit failed")).await?;
        w.assert_kept_with_tag(&context).await?;

        // A commit that took effect but reported failure: the graph is the
        // untagged one, and so is Redis.
        untag_with(&w, Mode::CommitReplyLost).await?;
        assert_eq!(w.edge_count().await?, 0, "{context}");
        assert_eq!(w.tagged().await, 1, "{context}: the tagger is counted once");
        assert_matches_reindex(&w, &format!("{context}: untag commit reply lost")).await?;

        // ... and the same for a PUT.
        put_with(&w, Mode::CommitReplyLost).await?;
        assert_eq!(w.edge_count().await?, 1, "{context}");
        w.assert_kept_with_tag(&context).await?;
        assert_matches_reindex(&w, &format!("{context}: PUT commit reply lost")).await?;
        w.cleanup(&mut test).await?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Post deletion
// ---------------------------------------------------------------------------

/// What the Redis state of a post deletion covers: the post, its label, its
/// author, its taggers and its parent.
struct DeletionWorld {
    author: String,
    post: String,
    parent: Option<(String, String)>,
    taggers: Vec<String>,
    label: String,
}

impl DeletionWorld {
    fn post_key(&self) -> String {
        format!("{}:{}", self.author, self.post)
    }

    fn patterns(&self) -> Vec<String> {
        let mut patterns = vec![
            format!("*{}:{}*", self.author, self.post),
            format!("*{}*", self.label),
        ];
        for user in std::iter::once(&self.author).chain(self.taggers.iter()) {
            patterns.push(format!("*UserCounts:{user}*"));
        }
        if let Some((parent_author, parent_post)) = &self.parent {
            patterns.push(format!("*{parent_author}:{parent_post}*"));
        }
        patterns
    }

    fn scores(&self) -> Vec<(String, String)> {
        let post_key = self.post_key();
        let mut scores = vec![
            (joined(&POST_TIMELINE_KEY_PARTS, &[]), post_key.clone()),
            (
                joined(&POST_TOTAL_ENGAGEMENT_KEY_PARTS, &[]),
                post_key.clone(),
            ),
            (
                joined(&POST_PER_USER_KEY_PARTS, &[&self.author]),
                self.post.clone(),
            ),
            (
                joined(&POST_REPLIES_PER_USER_KEY_PARTS, &[&self.author]),
                self.post.clone(),
            ),
            ("Sorted:Tags:Label".to_string(), self.label.clone()),
        ];
        if let Some((parent_author, parent_post)) = &self.parent {
            scores.push((
                joined(
                    &POST_REPLIES_PER_POST_KEY_PARTS,
                    &[parent_author, parent_post],
                ),
                post_key,
            ));
            scores.push((
                joined(&POST_TOTAL_ENGAGEMENT_KEY_PARTS, &[]),
                format!("{parent_author}:{parent_post}"),
            ));
        }
        for user in std::iter::once(&self.author).chain(self.taggers.iter()) {
            scores.push(("Sorted:Users:Influencers".to_string(), user.clone()));
        }
        scores
    }

    async fn state(&self) -> Result<Vec<String>> {
        state_of(&self.patterns(), &self.scores()).await
    }

    /// Deletes what a reindex of the graph rebuilds for the post, then
    /// reindexes it.
    async fn reindex_from_graph(&self) -> Result<()> {
        let mut exact = vec![
            format!("Sorted:Posts:Tag:{}:{}", self.author, self.post),
            counts_key::<PostCounts>(&[&self.author, &self.post]).await,
            counts_key::<UserCounts>(&[&self.author]).await,
        ];
        for tagger in &self.taggers {
            exact.push(counts_key::<UserCounts>(&[tagger]).await);
        }
        if let Some((parent_author, parent_post)) = &self.parent {
            exact.push(counts_key::<PostCounts>(&[parent_author, parent_post]).await);
        }
        let mut members = self.scores();
        members.retain(|(key, _)| !key.starts_with("Sorted:Users:MostFollowed"));
        let patterns = vec![
            format!("*{}*", self.label),
            format!("*Post:Taggers:{}:{}*", self.author, self.post),
            format!("*PostDetails:{}:{}", self.author, self.post),
            format!("*PostRelationships:{}:{}", self.author, self.post),
        ];
        delete_keys(&patterns, &exact, &members).await?;
        PostDetails::reindex(&self.author, &self.post).await?;
        PostCounts::reindex(&self.author, &self.post).await?;
        PostRelationships::reindex(&self.author, &self.post).await?;
        TagPost::reindex(&self.author, Some(&self.post)).await?;
        PostsByTagSearch::reindex().await?;
        UserCounts::reindex(&self.author).await?;
        for tagger in &self.taggers {
            UserCounts::reindex(tagger).await?;
        }
        if let Some((parent_author, parent_post)) = &self.parent {
            PostCounts::reindex(parent_author, parent_post).await?;
        }
        TagSearch::reindex().await?;
        Ok(())
    }

    async fn assert_matches_reindex(&self, context: &str) -> Result<()> {
        let actual = self.state().await?;
        self.reindex_from_graph().await?;
        let oracle = self.state().await?;
        if actual != oracle {
            let only_actual: Vec<&String> = actual
                .iter()
                .filter(|line| !oracle.contains(line))
                .collect();
            let only_oracle: Vec<&String> = oracle
                .iter()
                .filter(|line| !actual.contains(line))
                .collect();
            panic!(
                "{context}: Redis differs from a fresh reindex.\nonly in Redis: {only_actual:#?}\nonly in the reindex: {only_oracle:#?}"
            );
        }
        Ok(())
    }

    async fn post_nodes(&self) -> Result<i64> {
        graph_count(
            Query::new(
                "recovery_post_nodes",
                "MATCH (:User {id: $author})-[:AUTHORED]->(p:Post {id: $post}) RETURN count(p) AS n",
            )
            .param("author", self.author.as_str())
            .param("post", self.post.as_str()),
        )
        .await
    }
}

#[derive(Clone, Copy, Debug)]
enum Deletion {
    /// `post::del`: a post without relationships is deleted outright.
    Regular,
    /// `post::sync_del`, as a moderation tag runs it: tags on the post go too.
    Moderated,
}

async fn delete_with(d: &DeletionWorld, how: Deletion, mode: Mode) -> OpResult {
    let hook = Inject::new(mode);
    let author = PubkyId::try_from(d.author.as_str()).expect("a valid pubky");
    match how {
        Deletion::Regular => post::del_with_hook(author, d.post.clone(), &hook).await,
        Deletion::Moderated => post::sync_del_with_hook(author, d.post.clone(), &hook).await,
    }
}

async fn steps_of_deletion(d: &DeletionWorld, how: Deletion) -> Result<u32> {
    let hook = Inject::new(Mode::Count);
    let author = PubkyId::try_from(d.author.as_str()).expect("a valid pubky");
    let outcome = match how {
        Deletion::Regular => post::del_with_hook(author, d.post.clone(), &hook).await,
        Deletion::Moderated => post::sync_del_with_hook(author, d.post.clone(), &hook).await,
    };
    assert!(outcome.is_err());
    Ok(hook.steps())
}

/// A post deletion fails at every step, at the commit and with a lost commit
/// reply. The post is still there after every failure that did not commit, and
/// Redis matches the graph; after the one that committed the post is gone, and
/// so is everything Redis kept for it.
async fn every_deletion_failure(
    test: &mut WatcherTest,
    d: &DeletionWorld,
    how: Deletion,
    context: &str,
) -> Result<()> {
    let steps = steps_of_deletion(d, how).await?;
    assert!(steps >= 4, "{context}: only {steps} Redis steps ran");
    assert_eq!(d.post_nodes().await?, 1, "{context}");
    d.assert_matches_reindex(&format!("{context}: failed before the commit"))
        .await?;
    for step in 1..=steps {
        for mode in [Mode::BeforeStep(step), Mode::AfterStep(step)] {
            let outcome = delete_with(d, how, mode).await;
            assert!(outcome.is_err(), "{context} {mode:?}");
            assert_eq!(d.post_nodes().await?, 1, "{context} {mode:?}");
            d.assert_matches_reindex(&format!("{context}: {mode:?}"))
                .await?;
        }
    }
    let statements = nexus_common::db::autocommit_statement_count();
    let outcome = delete_with(d, how, Mode::CommitFails).await;
    assert!(outcome.is_err(), "{context}");
    assert_eq!(
        nexus_common::db::autocommit_statement_count(),
        statements,
        "{context}: the deletion and its recovery use only their transactions' connections"
    );
    assert_eq!(d.post_nodes().await?, 1, "{context}");
    d.assert_matches_reindex(&format!("{context}: commit failed"))
        .await?;

    // The commit took effect and its reply was lost.
    let outcome = delete_with(d, how, Mode::CommitReplyLost).await;
    assert!(outcome.is_err(), "{context}");
    assert_eq!(d.post_nodes().await?, 0, "{context}: the post must be gone");
    let keys = state_of(&[format!("*{}:{}*", d.author, d.post)], &[]).await?;
    assert!(
        keys.is_empty(),
        "{context}: Redis still holds the deleted post: {keys:?}"
    );
    for (key, member) in d.scores() {
        if key.contains("Timeline") || key.contains("TotalEngagement") || key.contains("Replies") {
            let left = state_of(&[], &[(key.clone(), member.clone())]).await?;
            let is_parent = d
                .parent
                .as_ref()
                .is_some_and(|(pa, pp)| member == format!("{pa}:{pp}"));
            if !is_parent {
                assert!(
                    left[0].ends_with("None"),
                    "{context}: {key} still holds {member}"
                );
            }
        }
    }
    // Counters equal the graph's.
    for user in std::iter::once(&d.author).chain(d.taggers.iter()) {
        let from_graph = graph_counts(user).await?;
        let in_redis = UserCounts::get_from_index(user)
            .await?
            .ok_or_else(|| anyhow!("no counts for {user}"))?;
        assert_eq!(in_redis.posts, from_graph.posts, "{context}: {user} posts");
        assert_eq!(
            in_redis.replies, from_graph.replies,
            "{context}: {user} replies"
        );
        assert_eq!(
            in_redis.tagged, from_graph.tagged,
            "{context}: {user} tagged"
        );
    }
    if let Some((parent_author, parent_post)) = &d.parent {
        let in_redis = PostCounts::get_from_index(parent_author, parent_post)
            .await?
            .ok_or_else(|| anyhow!("no counts for the parent"))?;
        let mut txn = nexus_common::db::start_graph_txn().await?;
        let (from_graph, _) = PostCounts::get_from_graph_in(&mut txn, parent_author, parent_post)
            .await?
            .ok_or_else(|| anyhow!("no parent"))?;
        txn.rollback().await?;
        assert_eq!(
            in_redis.replies, from_graph.replies,
            "{context}: parent replies"
        );
        assert_eq!(
            in_redis.reposts, from_graph.reposts,
            "{context}: parent reposts"
        );
    }
    let _ = test;
    Ok(())
}

async fn graph_counts(user_id: &str) -> Result<UserCounts> {
    let mut txn = nexus_common::db::start_graph_txn().await?;
    let counts = UserCounts::get_from_graph_in(&mut txn, user_id)
        .await?
        .ok_or_else(|| anyhow!("no user {user_id}"))?;
    txn.rollback().await?;
    Ok(counts)
}

/// Moderated deletion of a tagged post, and regular deletion of an untagged one.
#[tokio_shared_rt::test(shared)]
async fn a_failed_post_deletion_leaves_redis_equal_to_a_reindex_of_the_graph() -> Result<()> {
    let mut test = WatcherTest::setup().await?;

    // Moderated: the post carries a tag.
    let w = world(&mut test, Kind::Post).await?;
    w.put_now().await?;
    let d = DeletionWorld {
        author: w.owner_id.clone(),
        post: w.target_id.clone(),
        parent: None,
        taggers: vec![w.tagger_id.clone()],
        label: w.label.clone(),
    };
    every_deletion_failure(&mut test, &d, Deletion::Moderated, "moderated post").await?;
    w.cleanup(&mut test).await?;

    // Regular: a post nobody tagged.
    let w = world(&mut test, Kind::Post).await?;
    let d = DeletionWorld {
        author: w.owner_id.clone(),
        post: w.target_id.clone(),
        parent: None,
        taggers: vec![w.tagger_id.clone()],
        label: w.label.clone(),
    };
    every_deletion_failure(&mut test, &d, Deletion::Regular, "regular post").await?;
    w.cleanup(&mut test).await?;
    Ok(())
}

/// The deleted post is a reply: its parent's reply count and engagement, and
/// its author's counters, are rebuilt too.
#[tokio_shared_rt::test(shared)]
async fn a_failed_reply_deletion_leaves_redis_equal_to_a_reindex_of_the_graph() -> Result<()> {
    let mut test = WatcherTest::setup().await?;
    let w = world(&mut test, Kind::Post).await?;
    let (reply_kp, reply_author) = new_user(&mut test, "Recovery:Replier").await?;
    let (reply_id, _) = test
        .create_post(
            &reply_kp,
            &PubkyAppPost {
                content: "A reply".to_string(),
                kind: PubkyAppPostKind::Short,
                parent: Some(post_uri_builder(w.owner_id.clone(), w.target_id.clone())),
                embed: None,
                attachments: None,
                lock: None,
            },
        )
        .await?;
    assert_eq!(find_post_counts(&w.owner_id, &w.target_id).await.replies, 1);
    let d = DeletionWorld {
        author: reply_author,
        post: reply_id,
        parent: Some((w.owner_id.clone(), w.target_id.clone())),
        taggers: vec![],
        label: w.label.clone(),
    };
    every_deletion_failure(&mut test, &d, Deletion::Regular, "reply").await?;
    w.cleanup(&mut test).await?;
    test.cleanup_user(&reply_kp).await.ok();
    Ok(())
}

/// The recovery holds the target's lock: a deletion of the target waits for it.
#[tokio_shared_rt::test(shared)]
async fn a_recovery_holds_the_targets_lock_while_it_rebuilds() -> Result<()> {
    struct PauseAtRecovery {
        gate: Arc<Gate>,
    }

    #[async_trait]
    impl TagWriteHook for PauseAtRecovery {
        async fn at(&self, step: TagWriteStep) -> OpResult {
            match step {
                TagWriteStep::Recovering => {
                    self.gate.hold_once().await;
                    Ok(())
                }
                TagWriteStep::BeforeCommit => Err(Inject::injected()),
                _ => Ok(()),
            }
        }
    }

    let mut test = WatcherTest::setup().await?;
    for kind in Kind::ALL {
        let w = world(&mut test, kind).await?;
        let gate = Gate::new();
        let hook = Arc::new(PauseAtRecovery { gate: gate.clone() });
        let (tag_, tagger, id) = (w.tag.clone(), w.tagger(), w.tag_id());
        let failing =
            tokio::spawn(async move { tag::sync_put_with_hook(tag_, tagger, id, &*hook).await });
        gate.reached.notified().await;
        let delete = w.spawn_delete_now();
        wait_until_blocked(&delete).await?;
        // Another write by the same tagger waits too: the recovery rebuilds
        // the tagger's counter and holds the tagger's lock for it.
        let probe = if kind == Kind::User {
            None
        } else {
            let tag = PubkyAppTag {
                uri: user_uri_builder(w.owner_id.clone()),
                label: format!("{}p", w.label),
                created_at: chrono::Utc::now().timestamp_millis(),
            };
            let id = tag.create_id();
            let tagger = w.tagger();
            let probe_id = id.clone();
            let handle = tokio::spawn(async move { tag::sync_put(tag, tagger, probe_id).await });
            wait_until_blocked(&handle).await?;
            Some((handle, id))
        };
        gate.release.notify_one();
        assert!(failing.await?.is_err(), "{kind:?}");
        delete.await??;
        if let Some((handle, id)) = probe {
            handle.await??;
            tag::del(w.tagger(), id).await?;
        }
        w.assert_deleted_and_clean(&format!("{kind:?}")).await?;
        w.cleanup(&mut test).await?;
    }
    Ok(())
}
