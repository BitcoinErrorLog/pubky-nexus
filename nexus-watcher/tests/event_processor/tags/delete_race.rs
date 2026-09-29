//! A tag write (PUT or untag) and the deletion of its target can interleave
//! between the tag's graph write and its Redis writes. The tag write holds the
//! target's write lock from its graph statement through its Redis writes, and
//! the deletion takes the same lock before it checks or removes anything, so
//! each interleaving below ends in one of two states: the target is gone and
//! no index names it, or the target is kept and its tag indexes match its
//! edges.
//!
//! Every interleaving is deterministic: a hook holds one side at a chosen
//! point, the other side is observed `Blocked` on the lock in
//! `SHOW TRANSACTIONS`, and the hook releases.

use crate::event_processor::marketplace::utils::{test_listing, test_shop};
use crate::event_processor::posts::utils::find_post_counts;
use crate::event_processor::users::utils::find_user_counts;
use crate::event_processor::utils::watcher::{HomeserverHashIdPath, HomeserverPath, WatcherTest};
use anyhow::{anyhow, bail, Result};
use async_trait::async_trait;
use chrono::Utc;
use nexus_common::db::graph::Query;
use nexus_common::db::{fetch_key_from_graph, get_redis_conn};
use nexus_common::models::event::EventProcessorError;
use nexus_common::models::marketplace::ListingsByTagSearch;
use nexus_common::models::post::search::PostsByTagSearch;
use nexus_common::models::tag::search::TagSearch;
use nexus_common::types::Pagination;
use nexus_watcher::events::handlers::tag::{
    TagWriteHook, TagWriteStep, TargetTagCleanupHook, TargetTagCleanupStep,
};
use nexus_watcher::events::handlers::utils::{TargetDeleteHook, TargetDeleteStep};
use nexus_watcher::events::handlers::{listing, post, shop, tag, user};
use pubky::{Keypair, ResourcePath};
use pubky_app_specs::traits::HashId;
use pubky_app_specs::{
    listing_uri_builder, post_uri_builder, shop_uri_builder, user_uri_builder, PubkyAppListing,
    PubkyAppListingCondition, PubkyAppPost, PubkyAppShop, PubkyAppTag, PubkyAppUser, PubkyId,
};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;
use tokio::task::JoinHandle;

type OpResult = Result<(), EventProcessorError>;

/// Holds one side of an interleaving until the test releases it.
struct Gate {
    reached: Notify,
    release: Notify,
    fired: AtomicBool,
}

impl Gate {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            reached: Notify::new(),
            release: Notify::new(),
            fired: AtomicBool::new(false),
        })
    }

    async fn hold_once(&self) {
        if !self.fired.swap(true, Ordering::SeqCst) {
            self.reached.notify_one();
            self.release.notified().await;
        }
    }
}

struct PauseWrite {
    step: TagWriteStep,
    gate: Arc<Gate>,
}

#[async_trait]
impl TagWriteHook for PauseWrite {
    async fn at(&self, step: TagWriteStep) -> OpResult {
        if step == self.step {
            self.gate.hold_once().await;
        }
        Ok(())
    }
}

struct FailWrite {
    step: TagWriteStep,
    fired: AtomicBool,
}

#[async_trait]
impl TagWriteHook for FailWrite {
    async fn at(&self, step: TagWriteStep) -> OpResult {
        if step == self.step && !self.fired.swap(true, Ordering::SeqCst) {
            return Err(EventProcessorError::IndexOperationFailed(format!(
                "injected failure at {step:?}"
            )));
        }
        Ok(())
    }
}

struct PauseDelete {
    gate: Arc<Gate>,
}

#[async_trait]
impl TargetDeleteHook for PauseDelete {
    async fn at(&self, step: TargetDeleteStep) -> OpResult {
        match step {
            TargetDeleteStep::Checked => self.gate.hold_once().await,
        }
        Ok(())
    }
}

struct PauseCleanup {
    gate: Arc<Gate>,
}

#[async_trait]
impl TargetTagCleanupHook for PauseCleanup {
    async fn at(&self, step: TargetTagCleanupStep) -> OpResult {
        if step == TargetTagCleanupStep::FinalLocked {
            self.gate.hold_once().await;
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kind {
    Post,
    User,
    Listing,
    Shop,
}

impl Kind {
    const ALL: [Kind; 4] = [Kind::Post, Kind::User, Kind::Listing, Kind::Shop];

    fn code(self) -> &'static str {
        match self {
            Kind::Post => "p",
            Kind::User => "u",
            Kind::Listing => "l",
            Kind::Shop => "s",
        }
    }

    /// A post or a user that has an edge is kept as a `[DELETED]` record;
    /// a listing or a shop is deleted together with its tags.
    fn keeps_tagged_target(self) -> bool {
        matches!(self, Kind::Post | Kind::User)
    }
}

/// A target, its owner, a tagger with one unrelated tag, and the tag the
/// tests write and delete.
struct World {
    kind: Kind,
    owner_kp: Keypair,
    owner_id: String,
    /// Post or listing id; unused for user and shop targets.
    target_id: String,
    target_path: Option<ResourcePath>,
    tagger_kp: Keypair,
    tagger_id: String,
    label: String,
    tag: PubkyAppTag,
    baseline_label: String,
}

async fn new_user(test: &mut WatcherTest, name: &str) -> Result<(Keypair, String)> {
    let kp = Keypair::random();
    let id = test
        .create_user(
            &kp,
            &PubkyAppUser {
                bio: Some("tag delete race".to_string()),
                image: None,
                links: None,
                name: name.to_string(),
                status: None,
            },
        )
        .await?;
    Ok((kp, id))
}

async fn world(test: &mut WatcherTest, kind: Kind) -> Result<World> {
    let (owner_kp, owner_id) = new_user(test, "Race:Owner").await?;
    let (tagger_kp, tagger_id) = new_user(test, "Race:Tagger").await?;
    let (bystander_kp, bystander_id) = new_user(test, "Race:Bystander").await?;
    let unique = format!("{}{}", kind.code(), &owner_id[..8]);

    let mut target_id = String::new();
    let mut target_path = None;
    let target_uri = match kind {
        Kind::Post => {
            let (post_id, path) = test
                .create_post(
                    &owner_kp,
                    &PubkyAppPost {
                        content: "A post that gets tagged".to_string(),
                        kind: PubkyAppPost::default().kind,
                        parent: None,
                        embed: None,
                        attachments: None,
                        lock: None,
                    },
                )
                .await?;
            target_id = post_id.clone();
            target_path = Some(path);
            post_uri_builder(owner_id.clone(), post_id)
        }
        Kind::User => user_uri_builder(owner_id.clone()),
        Kind::Listing => {
            let listing: PubkyAppListing = test_listing(
                &owner_id,
                "Race boots",
                "fashion",
                PubkyAppListingCondition::New,
                1_000,
            );
            let (listing_id, path) = test.create_listing(&owner_kp, &listing).await?;
            target_id = listing_id.clone();
            target_path = Some(path);
            listing_uri_builder(owner_id.clone(), listing_id)
        }
        Kind::Shop => {
            test.put(&owner_kp, &PubkyAppShop::hs_path(), &test_shop(&owner_id))
                .await?;
            shop_uri_builder(owner_id.clone())
        }
    };

    let baseline_label = format!("db{unique}");
    let baseline = PubkyAppTag {
        uri: user_uri_builder(bystander_id.clone()),
        label: baseline_label.clone(),
        created_at: Utc::now().timestamp_millis(),
    };
    test.put(&tagger_kp, &baseline.hs_path(), baseline).await?;
    assert_eq!(find_user_counts(&tagger_id).await.tagged, 1);
    drop(bystander_kp);

    let label = format!("dr{unique}");
    Ok(World {
        kind,
        owner_kp,
        owner_id,
        target_id,
        target_path,
        tagger_kp,
        tagger_id,
        label: label.clone(),
        tag: PubkyAppTag {
            uri: target_uri,
            label,
            created_at: Utc::now().timestamp_millis(),
        },
        baseline_label,
    })
}

impl World {
    fn tagger(&self) -> PubkyId {
        PubkyId::try_from(self.tagger_id.as_str()).expect("a valid pubky")
    }

    fn owner(&self) -> PubkyId {
        PubkyId::try_from(self.owner_id.as_str()).expect("a valid pubky")
    }

    fn tag_id(&self) -> String {
        self.tag.create_id()
    }

    /// The tag PUT handler, as a concurrently processed event runs it.
    fn spawn_put(&self, hook: Arc<dyn TagWriteHook + Send>) -> JoinHandle<OpResult> {
        let (tag, tagger, tag_id) = (self.tag.clone(), self.tagger(), self.tag_id());
        tokio::spawn(async move { tag::sync_put_with_hook(tag, tagger, tag_id, &*hook).await })
    }

    /// The tag DEL handler.
    fn spawn_untag(&self, hook: Arc<dyn TagWriteHook + Send>) -> JoinHandle<OpResult> {
        let (tagger, tag_id) = (self.tagger(), self.tag_id());
        tokio::spawn(async move { tag::del_with_hook(tagger, tag_id, &*hook).await })
    }

    /// The target's DEL handler, paused where it holds the target's lock.
    fn spawn_delete(&self, gate: Arc<Gate>) -> JoinHandle<OpResult> {
        let (owner, target_id) = (self.owner(), self.target_id.clone());
        match self.kind {
            Kind::Post => tokio::spawn(async move {
                post::del_with_hook(owner, target_id, &PauseDelete { gate }).await
            }),
            Kind::User => {
                tokio::spawn(async move { user::del_with_hook(owner, &PauseDelete { gate }).await })
            }
            Kind::Listing => tokio::spawn(async move {
                listing::del_with_hook(owner, target_id, &PauseCleanup { gate }).await
            }),
            Kind::Shop => {
                tokio::spawn(
                    async move { shop::del_with_hook(owner, &PauseCleanup { gate }).await },
                )
            }
        }
    }

    async fn delete_now(&self) -> OpResult {
        let (owner, target_id) = (self.owner(), self.target_id.clone());
        match self.kind {
            Kind::Post => post::del(owner, target_id).await,
            Kind::User => user::del(owner).await,
            Kind::Listing => listing::del(owner, target_id).await,
            Kind::Shop => shop::del(owner).await,
        }
    }

    async fn put_now(&self) -> OpResult {
        tag::sync_put(self.tag.clone(), self.tagger(), self.tag_id()).await
    }

    async fn node_count(&self) -> Result<i64> {
        let query = match self.kind {
            Kind::Post => Query::new(
                "race_post_nodes",
                "MATCH (:User {id: $owner})-[:AUTHORED]->(t:Post {id: $id}) RETURN count(t) AS n",
            ),
            Kind::User => Query::new(
                "race_user_nodes",
                "MATCH (t:User {id: $owner}) RETURN count(t) AS n",
            ),
            Kind::Listing => Query::new(
                "race_listing_nodes",
                "MATCH (t:Listing {id: $id, owner_id: $owner}) RETURN count(t) AS n",
            ),
            Kind::Shop => Query::new(
                "race_shop_nodes",
                "MATCH (t:Shop {owner_id: $owner}) RETURN count(t) AS n",
            ),
        }
        .param("owner", self.owner_id.as_str())
        .param("id", self.target_id.as_str());
        graph_count(query).await
    }

    /// `TAGGED` edges of the tagger on the target that carry the label.
    async fn edge_count(&self) -> Result<i64> {
        let query = match self.kind {
            Kind::Post => Query::new(
                "race_post_edges",
                "MATCH (:User {id: $tagger})-[e:TAGGED {label: $label}]->(:Post {id: $id}) RETURN count(e) AS n",
            ),
            Kind::User => Query::new(
                "race_user_edges",
                "MATCH (:User {id: $tagger})-[e:TAGGED {label: $label}]->(:User {id: $owner}) RETURN count(e) AS n",
            ),
            Kind::Listing => Query::new(
                "race_listing_edges",
                "MATCH (:User {id: $tagger})-[e:TAGGED {label: $label}]->(:Listing {id: $id}) RETURN count(e) AS n",
            ),
            Kind::Shop => Query::new(
                "race_shop_edges",
                "MATCH (:User {id: $tagger})-[e:TAGGED {label: $label}]->(:Shop {owner_id: $owner}) RETURN count(e) AS n",
            ),
        }
        .param("tagger", self.tagger_id.as_str())
        .param("label", self.label.as_str())
        .param("owner", self.owner_id.as_str())
        .param("id", self.target_id.as_str());
        graph_count(query).await
    }

    /// Redis keys that name the target's tag indexes.
    async fn tag_index_keys(&self) -> Result<Vec<String>> {
        let (owner, id) = (&self.owner_id, &self.target_id);
        let patterns = match self.kind {
            Kind::Post => vec![format!("*{owner}:{id}*")],
            Kind::Listing => vec![format!("*{owner}:{id}*")],
            Kind::Shop => vec![format!("*Shop*{owner}*")],
            Kind::User => vec![
                format!("*Users:Tag:{owner}*"),
                format!("*Taggers:{owner}:*"),
            ],
        };
        let mut conn = get_redis_conn().await?;
        let mut keys = Vec::new();
        for pattern in patterns {
            let mut found: Vec<String> = deadpool_redis::redis::cmd("KEYS")
                .arg(&pattern)
                .query_async(&mut conn)
                .await?;
            keys.append(&mut found);
        }
        keys.sort();
        keys.dedup();
        Ok(keys)
    }

    /// Whether the target still sits in a label's global timeline or
    /// engagement set.
    async fn label_member_left(&self) -> Result<bool> {
        let key = format!("{}:{}", self.owner_id, self.target_id);
        Ok(match self.kind {
            Kind::Post => {
                let by_time =
                    PostsByTagSearch::get_by_label(&self.label, None, Pagination::default())
                        .await?
                        .unwrap_or_default();
                let by_engagement = PostsByTagSearch::get_by_label(
                    &self.label,
                    Some(nexus_common::types::StreamSorting::TotalEngagement),
                    Pagination::default(),
                )
                .await?
                .unwrap_or_default();
                by_time
                    .iter()
                    .chain(by_engagement.iter())
                    .any(|e| e.post_key == key)
            }
            Kind::Listing => ListingsByTagSearch::get_by_label(&self.label, Pagination::default())
                .await?
                .unwrap_or_default()
                .iter()
                .any(|entry| entry.listing_key == key),
            Kind::User | Kind::Shop => false,
        })
    }

    async fn tagged(&self) -> u32 {
        find_user_counts(&self.tagger_id).await.tagged
    }

    /// The target is gone and nothing indexes it or the tag.
    async fn assert_deleted_and_clean(&self, context: &str) -> Result<()> {
        assert_eq!(self.node_count().await?, 0, "{context}: node left");
        assert_eq!(self.edge_count().await?, 0, "{context}: edge left");
        // The cleanup's claim set is a ledger, not an index: it outlives the
        // target on purpose, so an overlapping cleanup cannot count a tagger
        // twice, and it expires.
        let (ledger, keys): (Vec<String>, Vec<String>) = self
            .tag_index_keys()
            .await?
            .into_iter()
            .partition(|key| key.starts_with("Cleanup:Tags:"));
        assert!(keys.is_empty(), "{context}: index keys left: {keys:?}");
        let mut conn = get_redis_conn().await?;
        for key in ledger {
            let ttl: i64 = deadpool_redis::redis::cmd("TTL")
                .arg(&key)
                .query_async(&mut conn)
                .await?;
            assert!(ttl > 0, "{context}: {key} does not expire");
        }
        assert!(
            !self.label_member_left().await?,
            "{context}: label timeline member left"
        );
        assert!(
            !suggested(&self.label).await?,
            "{context}: label still suggested"
        );
        assert_eq!(
            self.tagged().await,
            1,
            "{context}: tagger count is not the baseline"
        );
        assert_eq!(
            graph_count(
                Query::new(
                    "race_cleanup_markers",
                    "MATCH (c:TagCleanup) WHERE c.target ENDS WITH $owner RETURN count(c) AS n"
                )
                .param("owner", self.owner_id.as_str())
            )
            .await?,
            0,
            "{context}: cleanup markers left"
        );
        Ok(())
    }

    /// The target is kept with exactly one tag, and its indexes match it.
    async fn assert_kept_with_tag(&self, context: &str) -> Result<()> {
        assert_eq!(self.node_count().await?, 1, "{context}: node lost");
        assert_eq!(self.edge_count().await?, 1, "{context}: edge lost");
        assert!(
            !self.tag_index_keys().await?.is_empty(),
            "{context}: tag indexes missing"
        );
        assert_eq!(self.tagged().await, 2, "{context}: tagger count");
        assert!(suggested(&self.label).await?, "{context}: not suggested");
        match self.kind {
            Kind::Post => {
                assert_eq!(
                    find_post_counts(&self.owner_id, &self.target_id).await.tags,
                    1
                )
            }
            Kind::User => assert_eq!(find_user_counts(&self.owner_id).await.tags, 1),
            Kind::Listing | Kind::Shop => {}
        }
        Ok(())
    }

    async fn cleanup(self, test: &mut WatcherTest) -> Result<()> {
        if let Some(path) = &self.target_path {
            test.del(&self.owner_kp, path).await.ok();
        }
        test.cleanup_user(&self.tagger_kp).await.ok();
        test.cleanup_user(&self.owner_kp).await.ok();
        Ok(())
    }
}

async fn graph_count(query: Query) -> Result<i64> {
    Ok(fetch_key_from_graph::<i64>(query, "n")
        .await?
        .unwrap_or_default())
}

async fn suggested(label: &str) -> Result<bool> {
    Ok(TagSearch::get_by_label(label, &Pagination::default())
        .await?
        .is_some_and(|found| {
            found
                .iter()
                .any(|tag| serde_json::to_value(tag).ok() == Some(serde_json::json!(label)))
        }))
}

/// Waits until the operation is waiting on a write lock some other
/// transaction holds, and asserts it has not finished.
async fn wait_until_blocked<T>(operation: &JoinHandle<T>) -> Result<()> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        if operation.is_finished() {
            bail!("the operation finished instead of waiting on the lock");
        }
        let blocked = graph_count(Query::new(
            "race_blocked_on_lock",
            "SHOW TRANSACTIONS YIELD currentQuery, status
             WHERE currentQuery CONTAINS 'tag_cleanup_lock'
               AND status STARTS WITH 'Blocked'
             RETURN count(*) AS n",
        ))
        .await?;
        if blocked >= 1 {
            return Ok(());
        }
        if tokio::time::Instant::now() > deadline {
            bail!("the operation never blocked on the lock");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn joined(operation: JoinHandle<OpResult>) -> Result<OpResult> {
    operation.await.map_err(|e| anyhow!("task failed: {e}"))
}

/// A tag PUT holds the target's lock through its Redis writes. The target's
/// deletion waits, then sees the tag: a listing or shop is deleted with the
/// tag swept exactly; a post or user is kept as a `[DELETED]` record whose
/// tag indexes match its edge.
#[tokio_shared_rt::test(shared)]
async fn a_deletion_waits_for_a_tag_put_that_holds_the_target() -> Result<()> {
    let mut test = WatcherTest::setup().await?;
    for kind in Kind::ALL {
        let w = world(&mut test, kind).await?;
        let put_gate = Gate::new();
        let put = w.spawn_put(Arc::new(PauseWrite {
            step: TagWriteStep::GraphWritten,
            gate: put_gate.clone(),
        }));
        put_gate.reached.notified().await;

        let delete_gate = Gate::new();
        let delete = w.spawn_delete(delete_gate.clone());
        wait_until_blocked(&delete).await?;
        put_gate.release.notify_one();

        joined(put).await??;
        if !kind.keeps_tagged_target() {
            delete_gate.release.notify_one();
        }
        joined(delete).await??;

        if kind.keeps_tagged_target() {
            w.assert_kept_with_tag(&format!("{kind:?}")).await?;
        } else {
            w.assert_deleted_and_clean(&format!("{kind:?}")).await?;
        }
        w.cleanup(&mut test).await?;
    }
    Ok(())
}

/// A deletion holds the target's lock after it decided to delete. A tag PUT
/// that starts meanwhile waits, then finds no target: it fails before any
/// Redis write, and the same PUT again is a missing dependency that writes
/// nothing.
#[tokio_shared_rt::test(shared)]
async fn a_tag_put_after_a_deletion_writes_nothing() -> Result<()> {
    let mut test = WatcherTest::setup().await?;
    for kind in Kind::ALL {
        let w = world(&mut test, kind).await?;
        let delete_gate = Gate::new();
        let delete = w.spawn_delete(delete_gate.clone());
        delete_gate.reached.notified().await;

        let put = w.spawn_put(Arc::new(NoHook));
        wait_until_blocked(&put).await?;
        delete_gate.release.notify_one();

        joined(delete).await??;
        let landed = joined(put).await?;
        assert!(
            landed.is_err(),
            "{kind:?}: a PUT after the deletion must not succeed"
        );
        let again = w.put_now().await;
        assert!(
            matches!(again, Err(EventProcessorError::MissingDependency { .. })),
            "{kind:?}: {again:?}"
        );
        w.assert_deleted_and_clean(&format!("{kind:?}")).await?;
        w.cleanup(&mut test).await?;
    }
    Ok(())
}

struct NoHook;

#[async_trait]
impl TagWriteHook for NoHook {}

/// A tag PUT fails between its graph write and its Redis writes. The edge is
/// rolled back with it, so a deletion of the target finds nothing to sweep
/// and leaves nothing, and the retried PUT writes nothing.
#[tokio_shared_rt::test(shared)]
async fn a_tag_put_that_fails_after_its_graph_write_leaves_nothing_to_purge() -> Result<()> {
    let mut test = WatcherTest::setup().await?;
    for kind in Kind::ALL {
        let w = world(&mut test, kind).await?;
        let failing = FailWrite {
            step: TagWriteStep::GraphWritten,
            fired: AtomicBool::new(false),
        };
        let failed = tag::sync_put_with_hook(w.tag.clone(), w.tagger(), w.tag_id(), &failing).await;
        assert!(
            failed.is_err(),
            "{kind:?}: the injected failure must surface"
        );
        assert_eq!(
            w.edge_count().await?,
            0,
            "{kind:?}: the edge outlived its failed PUT"
        );
        assert!(
            !w.label_member_left().await? && !suggested(&w.label).await?,
            "{kind:?}: the failed PUT left label indexes"
        );
        assert_eq!(w.tagged().await, 1, "{kind:?}: tagger counted a failed PUT");

        w.delete_now().await?;
        let retried = w.put_now().await;
        assert!(
            matches!(retried, Err(EventProcessorError::MissingDependency { .. })),
            "{kind:?}: {retried:?}"
        );
        w.assert_deleted_and_clean(&format!("{kind:?}")).await?;
        w.cleanup(&mut test).await?;
    }
    Ok(())
}

/// The same failure with the target alive: the retried PUT indexes the tag
/// exactly once.
#[tokio_shared_rt::test(shared)]
async fn a_tag_put_that_fails_after_its_graph_write_is_indexed_once_when_retried() -> Result<()> {
    let mut test = WatcherTest::setup().await?;
    for kind in Kind::ALL {
        let w = world(&mut test, kind).await?;
        let failing = FailWrite {
            step: TagWriteStep::GraphWritten,
            fired: AtomicBool::new(false),
        };
        let failed = tag::sync_put_with_hook(w.tag.clone(), w.tagger(), w.tag_id(), &failing).await;
        assert!(failed.is_err());
        w.put_now().await?;
        w.assert_kept_with_tag(&format!("{kind:?}")).await?;
        w.cleanup(&mut test).await?;
    }
    Ok(())
}

/// An untag holds the target's lock through its Redis writes. The target's
/// deletion waits, then finds the tag gone: the tagger's count went down
/// once, and once the target is deleted nothing indexes it, the label's
/// zero-score leftovers included.
#[tokio_shared_rt::test(shared)]
async fn a_deletion_waits_for_an_untag_that_holds_the_target() -> Result<()> {
    let mut test = WatcherTest::setup().await?;
    for kind in Kind::ALL {
        let w = world(&mut test, kind).await?;
        w.put_now().await?;
        w.assert_kept_with_tag(&format!("{kind:?} tagged")).await?;

        let untag_gate = Gate::new();
        let untag = w.spawn_untag(Arc::new(PauseWrite {
            step: TagWriteStep::EdgeDeleted,
            gate: untag_gate.clone(),
        }));
        untag_gate.reached.notified().await;

        let delete_gate = Gate::new();
        let delete = w.spawn_delete(delete_gate.clone());
        wait_until_blocked(&delete).await?;
        untag_gate.release.notify_one();

        joined(untag).await??;
        delete_gate.release.notify_one();
        joined(delete).await??;
        w.assert_deleted_and_clean(&format!("{kind:?}")).await?;
        w.cleanup(&mut test).await?;
    }
    Ok(())
}

/// A moderated post is deleted with its tags on it: the deletion removes
/// their edges and every index of them, and counts each tagger down.
#[tokio_shared_rt::test(shared)]
async fn a_moderated_post_is_deleted_with_every_tag_index() -> Result<()> {
    let mut test = WatcherTest::setup().await?;
    let w = world(&mut test, Kind::Post).await?;
    w.put_now().await?;
    w.assert_kept_with_tag("tagged").await?;

    post::sync_del(w.owner(), w.target_id.clone()).await?;
    w.assert_deleted_and_clean("moderated").await?;
    w.cleanup(&mut test).await?;
    Ok(())
}

/// The baseline tag on the bystander is untouched by all of the above.
#[tokio_shared_rt::test(shared)]
async fn a_target_deletion_leaves_the_taggers_other_tags_alone() -> Result<()> {
    let mut test = WatcherTest::setup().await?;
    let w = world(&mut test, Kind::Listing).await?;
    w.put_now().await?;
    listing::del(w.owner(), w.target_id.clone()).await?;
    w.assert_deleted_and_clean("listing").await?;
    assert!(suggested(&w.baseline_label).await?);
    w.cleanup(&mut test).await?;
    Ok(())
}

/// A tag write whose task is cancelled between its graph write and its
/// Redis writes releases its locks and leaves no edge: the target's
/// deletion, which needs the same lock, completes.
#[tokio_shared_rt::test(shared)]
async fn a_cancelled_tag_put_releases_its_locks_and_leaves_no_edge() -> Result<()> {
    let mut test = WatcherTest::setup().await?;
    for kind in Kind::ALL {
        let w = world(&mut test, kind).await?;
        let gate = Gate::new();
        let put = w.spawn_put(Arc::new(PauseWrite {
            step: TagWriteStep::GraphWritten,
            gate: gate.clone(),
        }));
        gate.reached.notified().await;
        put.abort();
        assert!(put.await.is_err_and(|e| e.is_cancelled()));

        tokio::time::timeout(Duration::from_secs(30), w.delete_now())
            .await
            .map_err(|_| {
                anyhow!("{kind:?}: the deletion is still waiting on the cancelled PUT")
            })??;
        w.assert_deleted_and_clean(&format!("{kind:?}")).await?;
        w.cleanup(&mut test).await?;
    }
    Ok(())
}
