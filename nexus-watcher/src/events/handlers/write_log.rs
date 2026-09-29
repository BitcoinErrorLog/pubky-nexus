//! The Redis writes of one tag PUT or untag, each recorded with its undo.
//!
//! A tag write changes the graph inside a transaction and Redis outside it.
//! When the transaction is rolled back, or its commit fails, the Redis writes
//! that already happened must not survive: the retried event would repeat them
//! and count the tag twice. Every write here runs on its own, in order, and
//! records how to take it back. [`WriteLog::rollback`] replays the undos
//! newest first.
//!
//! Undo kinds:
//! - values only the tag flows of the locked target change (label scores,
//!   `unique_tags`, taggers sets, per-label timelines) are put back to the
//!   value read just before the write, so an undo is correct whether or not
//!   the failed write reached Redis;
//! - counters other events change too (the tagger's `tagged`, the target's
//!   `tags`, the post's total engagement) are undone by the inverse
//!   increment.

use std::future::Future;
use std::pin::Pin;

use crate::events::handlers::tag::{TagWriteHook, TagWriteStep};
use crate::events::EventProcessorError;
use nexus_common::db::kv::ScoreAction;
use nexus_common::db::{GraphTxn, RedisOps};
use nexus_common::models::marketplace::{ListingsByTagSearch, TAG_GLOBAL_LISTING_TIMELINE};
use nexus_common::models::post::search::{
    PostsByTagSearch, TAG_GLOBAL_POST_ENGAGEMENT, TAG_GLOBAL_POST_TIMELINE,
};
use nexus_common::models::post::{PostCounts, PostStream, POST_TOTAL_ENGAGEMENT_KEY_PARTS};
use nexus_common::models::tag::listing::{TagListing, LISTING_TAGS_KEY_PARTS};
use nexus_common::models::tag::post::{TagPost, POST_TAGS_KEY_PARTS};
use nexus_common::models::tag::search::{TagSearch, TAGS_LABEL};
use nexus_common::models::tag::shop::{TagShop, SHOP_TAGS_KEY_PARTS};
use nexus_common::models::tag::traits::{TagCollection, TaggersCollection};
use nexus_common::models::tag::user::{TagUser, USER_TAGS_KEY_PARTS};
use nexus_common::models::user::UserCounts;

type UndoFuture = Pin<Box<dyn Future<Output = Result<(), EventProcessorError>> + Send>>;
type Undo = Box<dyn FnOnce() -> UndoFuture + Send>;

/// Sorted sets and JSON fields need a `RedisOps` type to be addressed; the
/// key layout does not depend on which one.
#[derive(serde::Serialize, serde::Deserialize)]
struct RawIndex;
impl RedisOps for RawIndex {}

/// The node a tag is written on, with the ids that address its indexes.
#[derive(Clone, Copy)]
pub enum Target<'a> {
    Post {
        author_id: &'a str,
        post_id: &'a str,
    },
    User {
        user_id: &'a str,
    },
    Listing {
        owner_id: &'a str,
        listing_id: &'a str,
    },
    Shop {
        owner_id: &'a str,
    },
}

impl Target<'_> {
    /// The target's label-score sorted set.
    fn score_key(&self) -> Vec<String> {
        let parts: Vec<&str> = match self {
            Target::Post { author_id, post_id } => {
                [&POST_TAGS_KEY_PARTS[..], &[author_id, post_id]].concat()
            }
            Target::User { user_id } => [&USER_TAGS_KEY_PARTS[..], &[user_id]].concat(),
            Target::Listing {
                owner_id,
                listing_id,
            } => [&LISTING_TAGS_KEY_PARTS[..], &[owner_id, listing_id]].concat(),
            Target::Shop { owner_id } => [&SHOP_TAGS_KEY_PARTS[..], &[owner_id]].concat(),
        };
        parts.into_iter().map(str::to_string).collect()
    }

    /// The `(author_id, extra)` pair the tag models take.
    fn model_ids(&self) -> (&str, Option<&str>) {
        match self {
            Target::Post { author_id, post_id } => (author_id, Some(post_id)),
            Target::User { user_id } => (user_id, None),
            Target::Listing {
                owner_id,
                listing_id,
            } => (owner_id, Some(listing_id)),
            Target::Shop { owner_id } => (owner_id, None),
        }
    }

    /// The target's taggers set of a label.
    fn taggers_key(&self, label: &str) -> Vec<String> {
        let (id, extra) = self.model_ids();
        let mut key = vec![id.to_string()];
        key.extend(extra.map(str::to_string));
        key.push(label.to_string());
        key
    }

    /// Whether the tagger is in the label's taggers set.
    async fn is_tagger(&self, key: &[&str], tagger_id: &str) -> Result<bool, EventProcessorError> {
        Ok(match self {
            Target::Post { .. } => TagPost::check_set_member(key, tagger_id).await?.1,
            Target::User { .. } => TagUser::check_set_member(key, tagger_id).await?.1,
            Target::Listing { .. } => TagListing::check_set_member(key, tagger_id).await?.1,
            Target::Shop { .. } => TagShop::check_set_member(key, tagger_id).await?.1,
        })
    }

    async fn put_tagger(&self, key: &[&str], tagger_id: &str) -> Result<(), EventProcessorError> {
        match self {
            Target::Post { .. } => TagPost::put_index_set(key, &[tagger_id], None, None).await?,
            Target::User { .. } => TagUser::put_index_set(key, &[tagger_id], None, None).await?,
            Target::Listing { .. } => {
                TagListing::put_index_set(key, &[tagger_id], None, None).await?
            }
            Target::Shop { .. } => TagShop::put_index_set(key, &[tagger_id], None, None).await?,
        }
        Ok(())
    }

    async fn remove_tagger(&self, label: &str, tagger_id: &str) -> Result<(), EventProcessorError> {
        let (id, extra) = self.model_ids();
        let tagger = vec![tagger_id.to_string()];
        match self {
            Target::Post { .. } => TagPost(tagger).del_from_index(id, extra, label).await?,
            Target::User { .. } => TagUser(tagger).del_from_index(id, extra, label).await?,
            Target::Listing { .. } => TagListing(tagger).del_from_index(id, extra, label).await?,
            Target::Shop { .. } => TagShop(tagger).del_from_index(id, extra, label).await?,
        }
        Ok(())
    }

    async fn apply_score(
        &self,
        label: &str,
        action: ScoreAction,
    ) -> Result<(), EventProcessorError> {
        let (id, extra) = self.model_ids();
        match self {
            Target::Post { .. } => TagPost::update_index_score(id, extra, label, action).await?,
            Target::User { .. } => TagUser::update_index_score(id, extra, label, action).await?,
            Target::Listing { .. } => {
                TagListing::update_index_score(id, extra, label, action).await?
            }
            Target::Shop { .. } => TagShop::update_index_score(id, extra, label, action).await?,
        }
        Ok(())
    }
}

fn refs(parts: &[String]) -> Vec<&str> {
    parts.iter().map(String::as_str).collect()
}

#[derive(Default)]
pub struct WriteLog {
    undo: Vec<Undo>,
    steps: u32,
}

impl WriteLog {
    pub fn new() -> Self {
        Self::default()
    }

    fn record<F, Fut>(&mut self, undo: F)
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), EventProcessorError>> + Send + 'static,
    {
        self.undo.push(Box::new(move || Box::pin(undo())));
    }

    /// Marks the end of one write; integration tests inject a failure here.
    async fn step_done(&mut self, hook: &dyn TagWriteHook) -> Result<(), EventProcessorError> {
        self.steps += 1;
        hook.at(TagWriteStep::IndexStep(self.steps)).await
    }

    pub fn is_empty(&self) -> bool {
        self.undo.is_empty()
    }

    /// Undoes every recorded write, newest first. A failing undo is logged
    /// and does not stop the rest.
    pub async fn rollback(&mut self) {
        while let Some(undo) = self.undo.pop() {
            if let Err(error) = undo().await {
                tracing::error!("Undoing a tag index write failed: {error}");
            }
        }
    }

    /// The tagger's or the tagged user's counter, moved up or down.
    pub async fn user_counter(
        &mut self,
        hook: &dyn TagWriteHook,
        user_id: &str,
        field: &'static str,
        up: bool,
    ) -> Result<(), EventProcessorError> {
        if up {
            UserCounts::increment(user_id, field, None).await?;
        } else {
            UserCounts::decrement(user_id, field, None).await?;
        }
        let user_id = user_id.to_string();
        self.record(move || async move {
            if up {
                UserCounts::decrement(&user_id, field, None).await?;
            } else {
                UserCounts::increment(&user_id, field, None).await?;
            }
            Ok(())
        });
        self.step_done(hook).await
    }

    pub async fn post_counter(
        &mut self,
        hook: &dyn TagWriteHook,
        author_id: &str,
        post_id: &str,
        field: &'static str,
        up: bool,
    ) -> Result<(), EventProcessorError> {
        let key = [author_id, post_id];
        if up {
            PostCounts::increment_index_field(&key, field, None).await?;
        } else {
            PostCounts::decrement_index_field(&key, field, None).await?;
        }
        let (author_id, post_id) = (author_id.to_string(), post_id.to_string());
        self.record(move || async move {
            let key = [author_id.as_str(), post_id.as_str()];
            if up {
                PostCounts::decrement_index_field(&key, field, None).await?;
            } else {
                PostCounts::increment_index_field(&key, field, None).await?;
            }
            Ok(())
        });
        self.step_done(hook).await
    }

    /// `unique_tags` of the target moves only when the label's score crosses
    /// zero, so the write is undone by restoring the field.
    pub async fn unique_tags(
        &mut self,
        hook: &dyn TagWriteHook,
        target: Target<'_>,
        label: &str,
        up: bool,
    ) -> Result<(), EventProcessorError> {
        match target {
            Target::Post { author_id, post_id } => {
                let key = [author_id, post_id];
                if let Some(before) = PostCounts::get_from_index(author_id, post_id).await? {
                    let restore = (author_id.to_string(), post_id.to_string());
                    self.record(move || async move {
                        let key = [restore.0.as_str(), restore.1.as_str()];
                        PostCounts::set_json_field(&key, "unique_tags", before.unique_tags.into())
                            .await?;
                        Ok(())
                    });
                }
                if up {
                    PostCounts::increment_index_field(&key, "unique_tags", Some(label)).await?;
                } else {
                    PostCounts::decrement_index_field(&key, "unique_tags", Some(label)).await?;
                }
            }
            Target::User { user_id } => {
                if let Some(before) = UserCounts::get_from_index(user_id).await? {
                    let restore = user_id.to_string();
                    self.record(move || async move {
                        UserCounts::set_json_field(
                            &[&restore],
                            "unique_tags",
                            before.unique_tags.into(),
                        )
                        .await?;
                        Ok(())
                    });
                }
                if up {
                    UserCounts::increment(user_id, "unique_tags", Some(label)).await?;
                } else {
                    UserCounts::decrement(user_id, "unique_tags", Some(label)).await?;
                }
            }
            Target::Listing { .. } | Target::Shop { .. } => {}
        }
        self.step_done(hook).await
    }

    /// The label's score on the target.
    pub async fn score(
        &mut self,
        hook: &dyn TagWriteHook,
        target: Target<'_>,
        label: &str,
        action: ScoreAction,
    ) -> Result<(), EventProcessorError> {
        let key = target.score_key();
        let before = RawIndex::check_sorted_set_member(None, &refs(&key), &[label]).await?;
        let label_owned = label.to_string();
        self.record(move || async move {
            RawIndex::restore_sorted_set_member(&refs(&key), &[&label_owned], before, false)
                .await?;
            Ok(())
        });
        target.apply_score(label, action).await?;
        self.step_done(hook).await
    }

    /// The tagger's membership in the label's taggers set.
    pub async fn tagger(
        &mut self,
        hook: &dyn TagWriteHook,
        target: Target<'_>,
        tagger_id: &str,
        label: &str,
        add: bool,
    ) -> Result<(), EventProcessorError> {
        let key = target.taggers_key(label);
        let was_member = target.is_tagger(&refs(&key), tagger_id).await?;
        let (target_owned, tagger_owned, label_owned) = (
            OwnedTarget::from(target),
            tagger_id.to_string(),
            label.to_string(),
        );
        self.record(move || async move {
            let target = target_owned.as_target();
            if was_member {
                target.put_tagger(&refs(&key), &tagger_owned).await
            } else {
                target.remove_tagger(&label_owned, &tagger_owned).await
            }
        });
        if add {
            match target {
                Target::Post { author_id, post_id } => {
                    TagPost::add_tagger_to_index(author_id, Some(post_id), tagger_id, label).await?
                }
                Target::User { user_id } => {
                    TagUser::add_tagger_to_index(user_id, None, tagger_id, label).await?
                }
                Target::Listing {
                    owner_id,
                    listing_id,
                } => {
                    TagListing::add_tagger_to_index(owner_id, Some(listing_id), tagger_id, label)
                        .await?
                }
                Target::Shop { owner_id } => {
                    TagShop::add_tagger_to_index(owner_id, None, tagger_id, label).await?
                }
            }
        } else {
            target.remove_tagger(label, tagger_id).await?;
        }
        self.step_done(hook).await
    }

    /// The target's entry in the label's global timeline (posts, listings).
    pub async fn timeline(
        &mut self,
        hook: &dyn TagWriteHook,
        target: Target<'_>,
        label: &str,
        add: bool,
    ) -> Result<(), EventProcessorError> {
        let (key, member, apply): (Vec<String>, String, _) = match target {
            Target::Post { author_id, post_id } => (
                [&TAG_GLOBAL_POST_TIMELINE[..], &[label]]
                    .concat()
                    .into_iter()
                    .map(str::to_string)
                    .collect(),
                format!("{author_id}:{post_id}"),
                true,
            ),
            Target::Listing {
                owner_id,
                listing_id,
            } => (
                [&TAG_GLOBAL_LISTING_TIMELINE[..], &[label]]
                    .concat()
                    .into_iter()
                    .map(str::to_string)
                    .collect(),
                format!("{owner_id}:{listing_id}"),
                true,
            ),
            _ => (Vec::new(), String::new(), false),
        };
        if !apply {
            return self.step_done(hook).await;
        }
        let before = RawIndex::check_sorted_set_member(None, &refs(&key), &[&member]).await?;
        let recreate = !add;
        self.record(move || async move {
            RawIndex::restore_sorted_set_member(&refs(&key), &[&member], before, recreate).await?;
            Ok(())
        });
        match (target, add) {
            (Target::Post { author_id, post_id }, true) => {
                PostsByTagSearch::put_to_index(author_id, post_id, label).await?
            }
            (Target::Post { author_id, post_id }, false) => {
                PostsByTagSearch::del_from_index(author_id, post_id, label).await?
            }
            (
                Target::Listing {
                    owner_id,
                    listing_id,
                },
                true,
            ) => ListingsByTagSearch::put_to_index(owner_id, listing_id, label).await?,
            (
                Target::Listing {
                    owner_id,
                    listing_id,
                },
                false,
            ) => ListingsByTagSearch::del_from_index(owner_id, listing_id, label).await?,
            _ => {}
        }
        self.step_done(hook).await
    }

    /// The post's entry in the label's engagement sorted set.
    pub async fn label_engagement(
        &mut self,
        hook: &dyn TagWriteHook,
        author_id: &str,
        post_id: &str,
        label: &str,
        action: ScoreAction,
    ) -> Result<(), EventProcessorError> {
        let key: Vec<String> = [&TAG_GLOBAL_POST_ENGAGEMENT[..], &[label]]
            .concat()
            .into_iter()
            .map(str::to_string)
            .collect();
        let member = format!("{author_id}:{post_id}");
        let before = RawIndex::check_sorted_set_member(None, &refs(&key), &[&member]).await?;
        self.record(move || async move {
            RawIndex::restore_sorted_set_member(&refs(&key), &[&member], before, false).await?;
            Ok(())
        });
        PostsByTagSearch::update_index_score(author_id, post_id, label, action).await?;
        self.step_done(hook).await
    }

    /// The post's total engagement, which replies and reposts move too.
    pub async fn post_engagement(
        &mut self,
        hook: &dyn TagWriteHook,
        author_id: &str,
        post_id: &str,
        up: bool,
    ) -> Result<(), EventProcessorError> {
        let action = if up {
            ScoreAction::Increment(1.0)
        } else {
            ScoreAction::Decrement(1.0)
        };
        PostStream::update_index_score(author_id, post_id, action).await?;
        let member = format!("{author_id}:{post_id}");
        self.record(move || async move {
            let delta = if up { -1.0 } else { 1.0 };
            RawIndex::put_score_index_sorted_set_if_present(
                &POST_TOTAL_ENGAGEMENT_KEY_PARTS,
                &[&member],
                delta,
            )
            .await?;
            Ok(())
        });
        self.step_done(hook).await
    }

    /// Drops the label from autocomplete once no edge carries it, judged
    /// inside the untag's transaction, which sees its own edge deletion.
    pub async fn prune_label(
        &mut self,
        hook: &dyn TagWriteHook,
        txn: &mut GraphTxn,
        label: &str,
    ) -> Result<(), EventProcessorError> {
        let before = RawIndex::check_sorted_set_member(None, &TAGS_LABEL, &[label]).await?;
        let label_owned = label.to_string();
        self.record(move || async move {
            RawIndex::restore_sorted_set_member(&TAGS_LABEL, &[&label_owned], before, true).await?;
            Ok(())
        });
        TagSearch::del_from_index_if_unused_in(txn, label).await?;
        self.step_done(hook).await
    }
}

/// An owned copy of a [`Target`] for an undo that outlives the borrow.
enum OwnedTarget {
    Post(String, String),
    User(String),
    Listing(String, String),
    Shop(String),
}

impl From<Target<'_>> for OwnedTarget {
    fn from(target: Target<'_>) -> Self {
        match target {
            Target::Post { author_id, post_id } => {
                OwnedTarget::Post(author_id.to_string(), post_id.to_string())
            }
            Target::User { user_id } => OwnedTarget::User(user_id.to_string()),
            Target::Listing {
                owner_id,
                listing_id,
            } => OwnedTarget::Listing(owner_id.to_string(), listing_id.to_string()),
            Target::Shop { owner_id } => OwnedTarget::Shop(owner_id.to_string()),
        }
    }
}

impl OwnedTarget {
    fn as_target(&self) -> Target<'_> {
        match self {
            OwnedTarget::Post(author_id, post_id) => Target::Post { author_id, post_id },
            OwnedTarget::User(user_id) => Target::User { user_id },
            OwnedTarget::Listing(owner_id, listing_id) => Target::Listing {
                owner_id,
                listing_id,
            },
            OwnedTarget::Shop(owner_id) => Target::Shop { owner_id },
        }
    }
}
