//! A cache fill or reindex reads a target from the graph and then writes
//! Redis. The target's deletion holds the target's write lock until it has
//! cleaned Redis, and every fill takes the same lock around its read and its
//! write, so a fill either finishes before the deletion or starts after it
//! and writes nothing. A fill that runs inside a transaction which already
//! holds the lock (a tag write reading the post's relationships) does not
//! take it again.

use super::delete_race::{
    graph_count, joined, new_user, wait_until_blocked, wait_until_blocked_n, world, Gate, Kind,
    World,
};
use crate::event_processor::marketplace::utils::test_drop;
use crate::event_processor::utils::watcher::WatcherTest;
use anyhow::{anyhow, Result};
use nexus_common::db::graph::lock::{set_fill_pause, FillPause, LockTarget};
use nexus_common::db::graph::Query;
use nexus_common::db::{start_graph_txn, RedisOps};
use nexus_common::get_files_dir_test_pathbuf;
use nexus_common::models::file::FileDetails;
use nexus_common::models::follow::{Followers, Following, UserFollows};
use nexus_common::models::marketplace::{DropDetails, ListingDetails, ShopDetails};
use nexus_common::models::post::{Bookmark, PostCounts, PostDetails, PostRelationships};
use nexus_common::models::tag::listing::TagListing;
use nexus_common::models::tag::post::TagPost;
use nexus_common::models::tag::shop::TagShop;
use nexus_common::models::tag::traits::TagCollection;
use nexus_common::models::tag::user::TagUser;
use nexus_common::models::traits::Collection;
use nexus_common::models::user::{UserCounts, UserDetails};
use nexus_watcher::events::handlers::{drop as drop_handler, file as file_handler, post};
use pubky_app_specs::traits::HasIdPath;
use pubky_app_specs::traits::HashId;
use pubky_app_specs::{blob_uri_builder, PubkyAppBlob, PubkyAppFile, PubkyId};
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;

#[derive(Clone, Copy, Debug)]
enum Fill {
    Tags,
    TagsTrustNetwork,
    TagsReindex,
    PostCounts,
    PostCountsReindex,
    PostDetails,
    PostDetailsReindex,
    PostRelationships,
    PostRelationshipsReindex,
    Bookmark,
    UserCounts,
    UserCountsReindex,
    Followers,
    Following,
    FollowersReindex,
    UserDetails,
    UserDetailsReindex,
    ListingDetails,
    ShopDetails,
}

impl Fill {
    fn for_kind(kind: Kind) -> Vec<Fill> {
        use Fill::*;
        match kind {
            Kind::Post => vec![
                Tags,
                TagsReindex,
                PostCounts,
                PostCountsReindex,
                PostDetails,
                PostDetailsReindex,
                PostRelationships,
                PostRelationshipsReindex,
                Bookmark,
            ],
            Kind::User => vec![
                Tags,
                TagsTrustNetwork,
                TagsReindex,
                UserCounts,
                UserCountsReindex,
                Followers,
                Following,
                FollowersReindex,
                UserDetails,
                UserDetailsReindex,
            ],
            Kind::Listing => vec![Tags, TagsReindex, ListingDetails],
            Kind::Shop => vec![Tags, TagsReindex, ShopDetails],
        }
    }

    /// Empties the Redis record the fill would read first, so it takes the
    /// graph path.
    async fn clear(self, w: &World) -> Result<()> {
        let (owner, id) = (w.owner_id.as_str(), w.target_id.as_str());
        match self {
            Fill::PostCounts => {
                PostCounts::remove_from_index_multiple_json(&[&[owner, id]]).await?
            }
            Fill::PostDetails => {
                PostDetails::remove_from_index_multiple_json(&[&[owner, id]]).await?
            }
            Fill::PostRelationships => {
                PostRelationships::remove_from_index_multiple_json(&[&[owner, id]]).await?
            }
            Fill::Bookmark => {
                Bookmark::remove_from_index_multiple_json(&[&[owner, id, &w.tagger_id]]).await?
            }
            Fill::UserCounts => UserCounts::remove_from_index_multiple_json(&[&[owner]]).await?,
            Fill::UserDetails => UserDetails::remove_from_index_multiple_json(&[&[owner]]).await?,
            Fill::ListingDetails => {
                ListingDetails::remove_from_index_multiple_json(&[&[owner, id]]).await?
            }
            Fill::ShopDetails => ShopDetails::remove_from_index_multiple_json(&[&[owner]]).await?,
            _ => {}
        }
        Ok(())
    }

    async fn run(self, kind: Kind, owner: &str, id: &str, viewer: &str) -> Result<()> {
        let extra = (!id.is_empty() && matches!(kind, Kind::Post | Kind::Listing)).then_some(id);
        match self {
            Fill::Tags => match kind {
                Kind::Post => {
                    TagPost::get_by_id(owner, extra, None, None, None, None, None).await?;
                }
                Kind::User => {
                    TagUser::get_by_id(owner, None, None, None, None, None, None).await?;
                }
                Kind::Listing => {
                    TagListing::get_by_id(owner, extra, None, None, None, None, None).await?;
                }
                Kind::Shop => {
                    TagShop::get_by_id(owner, None, None, None, None, None, None).await?;
                }
            },
            Fill::TagsTrustNetwork => {
                TagUser::get_by_id(owner, None, None, None, None, Some(viewer), Some(2)).await?;
            }
            Fill::TagsReindex => match kind {
                Kind::Post => TagPost::reindex(owner, extra).await?,
                Kind::User => TagUser::reindex(owner, None).await?,
                Kind::Listing => TagListing::reindex(owner, extra).await?,
                Kind::Shop => TagShop::reindex(owner, None).await?,
            },
            Fill::PostCounts => {
                PostCounts::get_by_id(owner, id).await?;
            }
            Fill::PostCountsReindex => PostCounts::reindex(owner, id).await?,
            Fill::PostDetails => {
                PostDetails::get_by_id(owner, id).await?;
            }
            Fill::PostDetailsReindex => PostDetails::reindex(owner, id).await?,
            Fill::PostRelationships => {
                PostRelationships::get_by_id(owner, id).await?;
            }
            Fill::PostRelationshipsReindex => PostRelationships::reindex(owner, id).await?,
            Fill::Bookmark => {
                Bookmark::get_by_id(owner, id, Some(viewer)).await?;
            }
            Fill::UserCounts => {
                UserCounts::get_by_id(owner).await?;
            }
            Fill::UserCountsReindex => UserCounts::reindex(owner).await?,
            Fill::Followers => {
                Followers::get_by_id(owner, None, None).await?;
            }
            Fill::Following => {
                Following::get_by_id(owner, None, None).await?;
            }
            Fill::FollowersReindex => Followers::reindex(owner).await?,
            Fill::UserDetails => {
                UserDetails::get_by_ids(&[owner]).await?;
            }
            Fill::UserDetailsReindex => UserDetails::reindex(&[owner]).await?,
            Fill::ListingDetails => {
                ListingDetails::get_by_id(owner, id).await?;
            }
            Fill::ShopDetails => {
                ShopDetails::get_by_id(owner).await?;
            }
        }
        Ok(())
    }

    fn spawn(self, w: &World) -> JoinHandle<Result<()>> {
        let (kind, owner, id, viewer) = (
            w.kind,
            w.owner_id.clone(),
            w.target_id.clone(),
            w.tagger_id.clone(),
        );
        tokio::spawn(async move { self.run(kind, &owner, &id, &viewer).await })
    }
}

/// The target's deletion holds the target's lock after it decided to
/// delete. Every fill of the target waits, then writes nothing.
#[tokio_shared_rt::test(shared)]
async fn a_cache_fill_waits_for_a_deletion_that_holds_the_target() -> Result<()> {
    let mut test = WatcherTest::setup().await?;
    for kind in Kind::ALL {
        let w = world(&mut test, kind).await?;
        let fills = Fill::for_kind(kind);
        for fill in &fills {
            fill.clear(&w).await?;
        }

        let gate = Gate::new();
        let delete = w.spawn_delete(gate.clone());
        gate.reached.notified().await;

        let running: Vec<(Fill, JoinHandle<Result<()>>)> =
            fills.iter().map(|fill| (*fill, fill.spawn(&w))).collect();
        let handles: Vec<&JoinHandle<Result<()>>> = running.iter().map(|(_, h)| h).collect();
        wait_until_blocked_n(&handles, fills.len() as i64).await?;
        gate.release.notify_one();

        joined(delete).await??;
        for (fill, handle) in running {
            handle
                .await
                .map_err(|e| anyhow!("{kind:?} {fill:?}: task failed: {e}"))?
                .map_err(|e| anyhow!("{kind:?} {fill:?}: {e}"))?;
        }
        w.assert_deleted_and_clean(&format!("{kind:?}")).await?;
        w.cleanup(&mut test).await?;
    }
    Ok(())
}

/// A fill holds the target's lock through its Redis write. The target's
/// deletion waits for it and then cleans what it wrote.
#[tokio_shared_rt::test(shared)]
async fn a_deletion_waits_for_a_cache_fill_that_holds_the_target() -> Result<()> {
    let mut test = WatcherTest::setup().await?;
    for (kind, fill) in [
        (Kind::Post, Fill::PostCounts),
        (Kind::Post, Fill::Tags),
        (Kind::User, Fill::Followers),
        (Kind::User, Fill::UserDetails),
        (Kind::Listing, Fill::ListingDetails),
        (Kind::Shop, Fill::ShopDetails),
    ] {
        let w = world(&mut test, kind).await?;
        fill.clear(&w).await?;
        let pause = Arc::new(FillPause {
            reached: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
        });
        set_fill_pause(Some(pause.clone()));
        let running = fill.spawn(&w);
        pause.reached.notified().await;

        let delete = w.spawn_delete_now();
        wait_until_blocked(&delete).await?;
        pause.release.notify_one();

        running.await??;
        joined(delete).await??;
        w.assert_deleted_and_clean(&format!("{kind:?} {fill:?}"))
            .await?;
        w.cleanup(&mut test).await?;
    }
    Ok(())
}

/// A cache miss inside a transaction that holds the target's lock reads the
/// graph and writes Redis without taking the lock again, which would wait
/// for its own transaction: a tag PUT on a post whose relationships are not
/// cached, and the post's own hard deletion.
#[tokio_shared_rt::test(shared)]
async fn a_cache_miss_inside_a_locked_write_does_not_wait_for_its_own_lock() -> Result<()> {
    let mut test = WatcherTest::setup().await?;
    let w = world(&mut test, Kind::Post).await?;
    Fill::PostRelationships.clear(&w).await?;
    tokio::time::timeout(Duration::from_secs(30), w.put_now())
        .await
        .map_err(|_| anyhow!("the tag PUT waits for its own lock"))??;
    w.assert_kept_with_tag("tagged").await?;

    tokio::time::timeout(
        Duration::from_secs(30),
        nexus_watcher::events::handlers::tag::del(w.tagger(), w.tag_id()),
    )
    .await
    .map_err(|_| anyhow!("the untag waits for its own lock"))??;

    Fill::PostRelationships.clear(&w).await?;
    tokio::time::timeout(
        Duration::from_secs(30),
        post::del(w.owner(), w.target_id.clone()),
    )
    .await
    .map_err(|_| anyhow!("the post deletion waits for its own lock"))??;
    w.assert_deleted_and_clean("post").await?;
    w.cleanup(&mut test).await?;
    Ok(())
}

/// Drops and files are deleted under the lock their fills take: a fill
/// waits for a deletion that holds it, and a deletion waits for a fill.
#[tokio_shared_rt::test(shared)]
async fn drop_and_file_fills_and_deletions_share_a_lock() -> Result<()> {
    let mut test = WatcherTest::setup().await?;
    let (kp, owner_id) = new_user(&mut test, "Race:DropFileOwner").await?;
    let owner = PubkyId::try_from(owner_id.as_str()).map_err(anyhow::Error::msg)?;

    let (drop_id, _) = test
        .create_drop(
            &kp,
            &test_drop(&owner_id, "Race drop", "2026-01-01T00:00:00Z", None),
        )
        .await?;
    let blob = PubkyAppBlob::new(b"Hello World!".to_vec());
    let blob_id = blob.create_id();
    test.create_file_from_body(
        &kp,
        PubkyAppBlob::create_path(&blob_id).as_str(),
        blob.0.clone(),
    )
    .await?;
    let (file_id, _) = test
        .create_file(
            &kp,
            &PubkyAppFile {
                name: "race".to_string(),
                content_type: "text/plain".to_string(),
                src: blob_uri_builder(owner_id.clone(), blob_id),
                size: 12,
                created_at: chrono::Utc::now().timestamp_millis(),
            },
        )
        .await?;

    // A deletion holds the lock: the fills wait and write nothing.
    let mut holder = start_graph_txn().await?;
    for target in [
        LockTarget::Drop {
            owner_id: &owner_id,
            drop_id: &drop_id,
        },
        LockTarget::File {
            owner_id: &owner_id,
            file_id: &file_id,
        },
    ] {
        holder.fetch_row(target.lock_query()).await?;
    }
    DropDetails::remove_from_index_multiple_json(&[&[&owner_id, &drop_id]]).await?;
    FileDetails::remove_from_index_multiple_json(&[&[&owner_id, &file_id]]).await?;
    let (o, d, f) = (owner_id.clone(), drop_id.clone(), file_id.clone());
    let drop_fill = tokio::spawn(async move { DropDetails::get_by_id(&o, &d).await.map(|_| ()) });
    let (o, f2) = (owner_id.clone(), f.clone());
    let file_fill =
        tokio::spawn(async move { FileDetails::get_by_ids(&[&[&o, &f2]]).await.map(|_| ()) });
    wait_until_blocked_n(&[&drop_fill, &file_fill], 2).await?;
    holder.rollback().await?;
    drop_fill.await??;
    file_fill.await??;

    // A fill holds the lock: the deletions wait for it.
    for is_drop in [true, false] {
        if is_drop {
            DropDetails::remove_from_index_multiple_json(&[&[&owner_id, &drop_id]]).await?;
        } else {
            FileDetails::remove_from_index_multiple_json(&[&[&owner_id, &file_id]]).await?;
        }
        let pause = Arc::new(FillPause {
            reached: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
        });
        set_fill_pause(Some(pause.clone()));
        let (o, d, f) = (owner_id.clone(), drop_id.clone(), file_id.clone());
        let fill = tokio::spawn(async move {
            if is_drop {
                DropDetails::get_by_id(&o, &d).await.map(|_| ())
            } else {
                FileDetails::get_by_ids(&[&[&o, &f]]).await.map(|_| ())
            }
        });
        pause.reached.notified().await;
        let (owner, d, f) = (owner.clone(), drop_id.clone(), file_id.clone());
        let delete = tokio::spawn(async move {
            if is_drop {
                drop_handler::del(owner, d).await
            } else {
                file_handler::del(&owner, f, get_files_dir_test_pathbuf()).await
            }
        });
        wait_until_blocked(&delete).await?;
        pause.release.notify_one();
        fill.await??;
        delete.await??;
    }

    assert_eq!(
        graph_count(
            Query::new(
                "race_drop_file_nodes",
                "MATCH (n) WHERE (n:Drop OR n:File) AND n.owner_id = $owner RETURN count(n) AS n"
            )
            .param("owner", owner_id.as_str())
        )
        .await?,
        0
    );
    assert!(DropDetails::get_from_index(&owner_id, &drop_id)
        .await?
        .is_none());
    assert!(
        FileDetails::get_from_index(vec![&[owner_id.as_str(), file_id.as_str()][..]])
            .await?
            .into_iter()
            .all(|file| file.is_none())
    );
    test.cleanup_user(&kp).await?;
    Ok(())
}
