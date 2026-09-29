//! The Redis writes of one tag PUT, untag or post deletion, run as numbered
//! steps, and the typed access to a target's tag indexes that both the writes
//! and the recovery in [`super::recovery`] use.
//!
//! A tag write changes the graph inside a transaction and Redis outside it, so
//! a failure leaves the two apart: the graph rolls back while some Redis
//! commands have applied, and a command whose reply was lost may or may not
//! have applied. The writes here do not try to take themselves back. After a
//! failure, [`super::recovery`] rebuilds what the write touched from the
//! committed graph.

use std::future::Future;

use crate::events::handlers::tag::{TagWriteHook, TagWriteStep};
use crate::events::handlers::utils::{TargetDeleteHook, TargetDeleteStep};
use crate::events::EventProcessorError;
use nexus_common::db::kv::{ScoreAction, SortOrder};
use nexus_common::db::{GraphTxn, RedisOps};
use nexus_common::models::marketplace::{ListingsByTagSearch, TAG_GLOBAL_LISTING_TIMELINE};
use nexus_common::models::post::search::{
    PostsByTagSearch, TAG_GLOBAL_POST_ENGAGEMENT, TAG_GLOBAL_POST_TIMELINE,
};
use nexus_common::models::post::{PostCounts, PostStream};
use nexus_common::models::tag::listing::{TagListing, LISTING_TAGS_KEY_PARTS};
use nexus_common::models::tag::post::{TagPost, POST_TAGS_KEY_PARTS};
use nexus_common::models::tag::shop::{TagShop, SHOP_TAGS_KEY_PARTS};
use nexus_common::models::tag::traits::{TagCollection, TaggersCollection};
use nexus_common::models::tag::user::{TagUser, USER_TAGS_KEY_PARTS};
use nexus_common::models::tag::TagDetails;
use nexus_common::models::user::UserCounts;

/// Sorted sets and JSON fields need a `RedisOps` type to be addressed; the
/// key layout does not depend on which one.
#[derive(serde::Serialize, serde::Deserialize)]
pub(super) struct RawIndex;
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

/// A [`Target`] that owns its ids, for recovery that outlives the write.
#[derive(Clone, Debug)]
pub enum OwnedTarget {
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
    pub fn as_target(&self) -> Target<'_> {
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

pub(super) fn refs(parts: &[String]) -> Vec<&str> {
    parts.iter().map(String::as_str).collect()
}

impl Target<'_> {
    /// The target's label-score sorted set.
    pub fn score_key(&self) -> Vec<String> {
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
    pub fn model_ids(&self) -> (&str, Option<&str>) {
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

    /// The labels the target's score set holds in Redis.
    pub async fn indexed_labels(&self) -> Result<Vec<String>, EventProcessorError> {
        let key = self.score_key();
        let scores = RawIndex::try_from_index_sorted_set(
            &refs(&key),
            None,
            None,
            None,
            None,
            SortOrder::Descending,
            None,
        )
        .await?
        .unwrap_or_default();
        Ok(scores.into_iter().map(|(label, _)| label).collect())
    }

    /// The tags on the target in the committed graph, read through `txn`.
    pub async fn graph_tags(
        &self,
        txn: &mut GraphTxn,
    ) -> Result<Vec<TagDetails>, EventProcessorError> {
        let (id, extra) = self.model_ids();
        let tags = match self {
            Target::Post { .. } => TagPost::get_from_graph_in(txn, id, extra).await?,
            Target::User { .. } => TagUser::get_from_graph_in(txn, id, extra).await?,
            Target::Listing { .. } => TagListing::get_from_graph_in(txn, id, extra).await?,
            Target::Shop { .. } => TagShop::get_from_graph_in(txn, id, extra).await?,
        };
        Ok(tags.unwrap_or_default())
    }

    /// Makes the label's taggers set hold exactly `taggers`.
    pub async fn replace_taggers(
        &self,
        label: &str,
        taggers: &[String],
    ) -> Result<(), EventProcessorError> {
        let key = self.taggers_key(label);
        let key = refs(&key);
        let members: Vec<&str> = taggers.iter().map(String::as_str).collect();
        match self {
            Target::Post { .. } => {
                TagPost::delete_set_index(&key).await?;
                if !members.is_empty() {
                    TagPost::put_index_set(&key, &members, None, None).await?;
                }
            }
            Target::User { .. } => {
                TagUser::delete_set_index(&key).await?;
                if !members.is_empty() {
                    TagUser::put_index_set(&key, &members, None, None).await?;
                }
            }
            Target::Listing { .. } => {
                TagListing::delete_set_index(&key).await?;
                if !members.is_empty() {
                    TagListing::put_index_set(&key, &members, None, None).await?;
                }
            }
            Target::Shop { .. } => {
                TagShop::delete_set_index(&key).await?;
                if !members.is_empty() {
                    TagShop::put_index_set(&key, &members, None, None).await?;
                }
            }
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

    async fn add_tagger(&self, label: &str, tagger_id: &str) -> Result<(), EventProcessorError> {
        let (id, extra) = self.model_ids();
        match self {
            Target::Post { .. } => {
                TagPost::add_tagger_to_index(id, extra, tagger_id, label).await?
            }
            Target::User { .. } => {
                TagUser::add_tagger_to_index(id, extra, tagger_id, label).await?
            }
            Target::Listing { .. } => {
                TagListing::add_tagger_to_index(id, extra, tagger_id, label).await?
            }
            Target::Shop { .. } => {
                TagShop::add_tagger_to_index(id, extra, tagger_id, label).await?
            }
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

// ---------------------------------------------------------------------------
// The Redis writes of a tag PUT, untag or post deletion. Each is one step.
// ---------------------------------------------------------------------------

/// The tagger's or the tagged user's counter, moved up or down.
pub async fn user_counter(
    user_id: &str,
    field: &'static str,
    up: bool,
) -> Result<(), EventProcessorError> {
    if up {
        UserCounts::increment(user_id, field, None).await?;
    } else {
        UserCounts::decrement(user_id, field, None).await?;
    }
    Ok(())
}

pub async fn post_counter(
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
    Ok(())
}

/// `unique_tags` of the target moves only when the label's score crosses
/// zero, which the counters read from the label's score set.
pub async fn unique_tags(
    target: Target<'_>,
    label: &str,
    up: bool,
) -> Result<(), EventProcessorError> {
    match target {
        Target::Post { author_id, post_id } => {
            let key = [author_id, post_id];
            if up {
                PostCounts::increment_index_field(&key, "unique_tags", Some(label)).await?;
            } else {
                PostCounts::decrement_index_field(&key, "unique_tags", Some(label)).await?;
            }
        }
        Target::User { user_id } => {
            if up {
                UserCounts::increment(user_id, "unique_tags", Some(label)).await?;
            } else {
                UserCounts::decrement(user_id, "unique_tags", Some(label)).await?;
            }
        }
        Target::Listing { .. } | Target::Shop { .. } => {}
    }
    Ok(())
}

pub async fn score(
    target: Target<'_>,
    label: &str,
    action: ScoreAction,
) -> Result<(), EventProcessorError> {
    target.apply_score(label, action).await
}

pub async fn tagger(
    target: Target<'_>,
    tagger_id: &str,
    label: &str,
    add: bool,
) -> Result<(), EventProcessorError> {
    if add {
        target.add_tagger(label, tagger_id).await
    } else {
        target.remove_tagger(label, tagger_id).await
    }
}

/// The target's entry in the label's global timeline (posts, listings).
pub async fn timeline(
    target: Target<'_>,
    label: &str,
    add: bool,
) -> Result<(), EventProcessorError> {
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
    Ok(())
}

/// The post's entry in the label's engagement sorted set.
pub async fn label_engagement(
    author_id: &str,
    post_id: &str,
    label: &str,
    action: ScoreAction,
) -> Result<(), EventProcessorError> {
    PostsByTagSearch::update_index_score(author_id, post_id, label, action).await?;
    Ok(())
}

/// The post's total engagement, which replies and reposts move too.
pub async fn post_engagement(
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
    Ok(())
}

/// Keys of the global sets a label's tags live in.
pub(super) fn label_timeline_key(
    target: &Target<'_>,
    label: &str,
) -> Option<(Vec<String>, String)> {
    let (key, member): (&[&str], String) = match target {
        Target::Post { author_id, post_id } => (
            &TAG_GLOBAL_POST_TIMELINE[..],
            format!("{author_id}:{post_id}"),
        ),
        Target::Listing {
            owner_id,
            listing_id,
        } => (
            &TAG_GLOBAL_LISTING_TIMELINE[..],
            format!("{owner_id}:{listing_id}"),
        ),
        _ => return None,
    };
    let parts = [key, &[label]].concat();
    Some((parts.into_iter().map(str::to_string).collect(), member))
}

pub(super) fn label_engagement_key(label: &str) -> Vec<String> {
    [&TAG_GLOBAL_POST_ENGAGEMENT[..], &[label]]
        .concat()
        .into_iter()
        .map(str::to_string)
        .collect()
}

// ---------------------------------------------------------------------------
// Steps
// ---------------------------------------------------------------------------

/// The hook of the write whose steps are counted.
#[derive(Clone, Copy)]
pub enum StepHook<'a> {
    Tag(&'a dyn TagWriteHook),
    Delete(&'a dyn TargetDeleteHook),
}

impl StepHook<'_> {
    async fn fire(&self, before: bool, step: u32) -> Result<(), EventProcessorError> {
        match self {
            StepHook::Tag(hook) => {
                hook.at(if before {
                    TagWriteStep::BeforeIndexStep(step)
                } else {
                    TagWriteStep::IndexStep(step)
                })
                .await
            }
            StepHook::Delete(hook) => {
                hook.at(if before {
                    TargetDeleteStep::BeforeIndexStep(step)
                } else {
                    TargetDeleteStep::IndexStep(step)
                })
                .await
            }
        }
    }
}

/// Counts the Redis writes of one tag write and lets tests fail it before and
/// after each. A failure after a step is a command that applied and reported
/// failure; one before it is a command that never applied.
pub struct Steps<'a> {
    hook: StepHook<'a>,
    step: u32,
    started: bool,
}

impl<'a> Steps<'a> {
    pub fn new(hook: StepHook<'a>) -> Self {
        Self {
            hook,
            step: 0,
            started: false,
        }
    }

    /// Whether a Redis write has started, so Redis may differ from the graph.
    pub fn started(&self) -> bool {
        self.started
    }

    pub async fn run<T>(
        &mut self,
        write: impl Future<Output = Result<T, EventProcessorError>>,
    ) -> Result<T, EventProcessorError> {
        self.started = true;
        self.step += 1;
        self.hook.fire(true, self.step).await?;
        let value = write.await?;
        self.hook.fire(false, self.step).await?;
        Ok(value)
    }
}
