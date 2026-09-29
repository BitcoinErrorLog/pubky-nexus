//! Serializes a target's Redis index writes against its deletion.
//!
//! A target (user, post, listing, shop) is deleted, and its tags written and
//! removed, under the target node's write lock (`tag_cleanup_lock`, taken in
//! a graph transaction that stays open through the Redis writes). A cache fill
//! or reindex reads the target from the graph and then writes Redis; without
//! the lock the deletion can finish between the two and the fill re-indexes a
//! deleted target. [`under_target_lock`] takes the same lock around the read
//! and the write.

use std::cell::RefCell;
use std::collections::HashSet;
use std::future::Future;
use std::sync::Mutex;

use super::error::GraphError;
use super::ops::GraphTxn;
use super::query::Query;
use crate::db::start_graph_txn;

/// A node whose deletion takes the target write lock.
#[derive(Debug, Clone, Copy)]
pub enum LockTarget<'a> {
    User(&'a str),
    Post {
        author_id: &'a str,
        post_id: &'a str,
    },
    Listing {
        owner_id: &'a str,
        listing_id: &'a str,
    },
    Shop {
        owner_id: &'a str,
    },
    Drop {
        owner_id: &'a str,
        drop_id: &'a str,
    },
    File {
        owner_id: &'a str,
        file_id: &'a str,
    },
}

impl LockTarget<'_> {
    /// Names the node in the per-task registry of locks already held.
    pub fn key(&self) -> String {
        match self {
            LockTarget::User(id) => format!("user:{id}"),
            LockTarget::Post { author_id, post_id } => format!("post:{author_id}:{post_id}"),
            LockTarget::Listing {
                owner_id,
                listing_id,
            } => format!("listing:{owner_id}:{listing_id}"),
            LockTarget::Shop { owner_id } => format!("shop:{owner_id}"),
            LockTarget::Drop { owner_id, drop_id } => format!("drop:{owner_id}:{drop_id}"),
            LockTarget::File { owner_id, file_id } => format!("file:{owner_id}:{file_id}"),
        }
    }

    /// Takes the node's write lock and returns one row when the node exists.
    pub fn lock_query(&self) -> Query {
        use super::queries::del;
        match self {
            LockTarget::User(id) => del::lock_user(id),
            LockTarget::Post { author_id, post_id } => del::lock_post(author_id, post_id),
            LockTarget::Listing {
                owner_id,
                listing_id,
            } => del::lock_listing(owner_id, listing_id),
            LockTarget::Shop { owner_id } => del::lock_shop(owner_id),
            LockTarget::Drop { owner_id, drop_id } => del::lock_drop(owner_id, drop_id),
            LockTarget::File { owner_id, file_id } => del::lock_file(owner_id, file_id),
        }
    }
}

tokio::task_local! {
    /// Keys of the locks held by the open transactions of the code running
    /// inside [`lock_scope`].
    static HELD: RefCell<Vec<String>>;
}

/// Runs `work` with a registry of the locks its transactions hold. A cache
/// fill that runs inside a transaction which already holds the target's lock
/// (a tag write reading the post's relationships, a deletion reading them
/// before it deletes) must not take the lock again: it would wait for its
/// own transaction. An enclosing scope is reused, so nested scopes see the
/// locks of the outer ones.
pub async fn lock_scope<F: Future>(work: F) -> F::Output {
    // Boxed: the handlers' futures are large, and nesting them unboxed
    // overflows the stack of a debug build.
    let work = Box::pin(work);
    if HELD.try_with(|_| ()).is_ok() {
        work.await
    } else {
        HELD.scope(RefCell::new(Vec::new()), work).await
    }
}

pub(super) fn register_held(key: &str) -> bool {
    HELD.try_with(|held| held.borrow_mut().push(key.to_string()))
        .is_ok()
}

pub(super) fn release_held(key: &str) {
    let _ = HELD.try_with(|held| {
        let mut held = held.borrow_mut();
        if let Some(position) = held.iter().position(|held_key| held_key == key) {
            held.swap_remove(position);
        }
    });
}

/// Whether a transaction inside the current [`lock_scope`] holds the node's
/// lock.
pub fn is_held_by_current_task(key: &str) -> bool {
    HELD.try_with(|held| held.borrow().iter().any(|held_key| held_key == key))
        .unwrap_or(false)
}

/// Test seam: pauses the next fill after it holds the lock.
#[doc(hidden)]
pub struct FillPause {
    pub reached: tokio::sync::Notify,
    pub release: tokio::sync::Notify,
}

static FILL_PAUSE: Mutex<Option<std::sync::Arc<FillPause>>> = Mutex::new(None);

/// Installs (or clears) the pause the next [`under_target_lock`] fill takes
/// once it holds the lock. Integration tests only.
#[doc(hidden)]
pub fn set_fill_pause(pause: Option<std::sync::Arc<FillPause>>) {
    *FILL_PAUSE.lock().expect("fill pause") = pause;
}

async fn pause_if_installed() {
    let pause = FILL_PAUSE.lock().expect("fill pause").take();
    if let Some(pause) = pause {
        pause.reached.notify_one();
        pause.release.notified().await;
    }
}

/// Runs `work`, a graph read followed by the Redis writes it feeds, while
/// holding the target's write lock in a transaction that ends after `work`.
///
/// Returns `Ok(None)` without running `work` when the target is gone: nothing
/// is read and nothing is written for a deleted target. A deletion that holds
/// the lock finishes first; one that starts later waits for `work`.
pub async fn under_target_lock<T, E, Fut>(target: LockTarget<'_>, work: Fut) -> Result<Option<T>, E>
where
    E: From<GraphError>,
    Fut: Future<Output = Result<Option<T>, E>>,
{
    let key = target.key();
    if is_held_by_current_task(&key) {
        return work.await;
    }
    let mut txn = start_graph_txn().await?;
    let present = match txn.fetch_row(target.lock_query()).await {
        Ok(row) => row.is_some(),
        // The target was deleted while this statement waited for its lock.
        Err(error) if error.is_entity_not_found() => false,
        Err(error) => {
            rollback_quietly(txn).await;
            return Err(error.into());
        }
    };
    if !present {
        rollback_quietly(txn).await;
        return Ok(None);
    }
    let result = lock_scope(async {
        txn.hold(&key);
        pause_if_installed().await;
        work.await
    })
    .await;
    finish_txn(txn, result).await
}

/// Records whose deletion takes a node lock, fetched and indexed in a batch.
#[derive(Debug, Clone)]
pub enum BatchLock {
    /// User ids.
    Users(Vec<String>),
    /// File keys, `owner_id:file_id`.
    Files(Vec<String>),
}

impl BatchLock {
    fn kind(&self) -> &'static str {
        match self {
            BatchLock::Users(_) => "user",
            BatchLock::Files(_) => "file",
        }
    }

    fn ids(&self) -> &[String] {
        match self {
            BatchLock::Users(ids) | BatchLock::Files(ids) => ids,
        }
    }

    fn lock_query(&self, ids: &[&str]) -> Query {
        match self {
            BatchLock::Users(_) => super::queries::del::lock_users(ids),
            BatchLock::Files(_) => super::queries::del::lock_files(ids),
        }
    }
}

/// [`under_target_lock`] for a batch. `work` receives the ids whose nodes
/// exist and are locked; it must write only those.
pub async fn under_batch_lock<T, E, F, Fut>(batch: BatchLock, work: F) -> Result<T, E>
where
    E: From<GraphError>,
    F: FnOnce(HashSet<String>) -> Fut,
    Fut: Future<Output = Result<T, E>>,
{
    let kind = batch.kind();
    let key_of = |id: &str| format!("{kind}:{id}");
    let mut sorted: Vec<&str> = batch.ids().iter().map(String::as_str).collect();
    sorted.sort_unstable();
    sorted.dedup();
    let (held, unheld): (Vec<&str>, Vec<&str>) = sorted
        .iter()
        .copied()
        .partition(|id| is_held_by_current_task(&key_of(id)));
    let held: HashSet<String> = held.iter().map(|id| id.to_string()).collect();
    if unheld.is_empty() {
        return work(held).await;
    }
    // A statement that matched a node another transaction then deleted fails
    // as a whole; the retry no longer matches the deleted node.
    let mut attempts = 0;
    let (txn, locked) = loop {
        attempts += 1;
        let mut txn = start_graph_txn().await?;
        match txn.fetch_all(batch.lock_query(&unheld)).await {
            Ok(rows) => {
                let mut locked = Vec::with_capacity(rows.len());
                for row in rows {
                    // A node another transaction deleted while the statement
                    // waited for its lock comes back without its id.
                    match row.get::<Option<String>>("id") {
                        Ok(Some(id)) => locked.push(id),
                        Ok(None) => {}
                        Err(error) => {
                            rollback_quietly(txn).await;
                            return Err(GraphError::from(error).into());
                        }
                    }
                }
                break (txn, locked);
            }
            Err(error) if error.is_entity_not_found() && attempts < 5 => {
                rollback_quietly(txn).await;
            }
            Err(error) => {
                rollback_quietly(txn).await;
                return Err(error.into());
            }
        }
    };
    let mut txn = txn;
    let result = lock_scope(async {
        let mut present = held;
        for id in locked {
            txn.hold(&key_of(&id));
            present.insert(id);
        }
        pause_if_installed().await;
        work(present).await
    })
    .await;
    finish_txn(txn, result).await
}

/// Ends a transaction that ran a target's deletion: commits it when `result`
/// is `Ok`, rolls it back otherwise.
pub async fn finish_txn<T, E>(txn: GraphTxn, result: Result<T, E>) -> Result<T, E>
where
    E: From<GraphError>,
{
    match result {
        Ok(value) => {
            txn.commit().await?;
            Ok(value)
        }
        Err(error) => {
            rollback_quietly(txn).await;
            Err(error)
        }
    }
}

async fn rollback_quietly(txn: GraphTxn) {
    if let Err(error) = txn.rollback().await {
        tracing::warn!("Rolling back a cache-fill transaction failed: {error}");
    }
}
