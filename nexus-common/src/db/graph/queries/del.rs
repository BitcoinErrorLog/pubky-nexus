use crate::db::graph::Query;

/// Deletes a user node and all its relationships
/// # Arguments
/// * `user_id` - The unique identifier of the user to be deleted
pub fn delete_user(user_id: &str) -> Query {
    Query::new(
        "delete_user",
        "MATCH (u:User {id: $id})
         DETACH DELETE u;",
    )
    .param("id", user_id.to_string())
}

/// Deletes a post node authored by a specific user, along with all its relationships
/// # Arguments
/// * `author_id` - The unique identifier of the user who authored the post.
/// * `post_id` - The unique identifier of the post to be deleted.
pub fn delete_post(author_id: &str, post_id: &str) -> Query {
    Query::new(
        "delete_post",
        "MATCH (u:User {id: $author_id})-[:AUTHORED]->(p:Post {id: $post_id})
         DETACH DELETE p;",
    )
    .param("author_id", author_id.to_string())
    .param("post_id", post_id.to_string())
}

/// Takes a post's write lock and returns one row when the post exists. Run
/// it in the deletion's transaction; see [`lock_listing`].
pub fn lock_post(author_id: &str, post_id: &str) -> Query {
    Query::new(
        "lock_post",
        "MATCH (:User {id: $author_id})-[:AUTHORED]->(post:Post {id: $post_id})
         SET post.tag_cleanup_lock = true
         REMOVE post.tag_cleanup_lock
         RETURN true AS locked",
    )
    .param("author_id", author_id.to_string())
    .param("post_id", post_id.to_string())
}

/// Deletes a "follows" relationship between two users
/// # Arguments
/// * `follower_id` - The unique identifier of the user who is following another user.
/// * `followee_id` - The unique identifier of the user being followed
pub fn delete_follow(follower_id: &str, followee_id: &str) -> Query {
    Query::new(
        "delete_follow",
        "// Important that MATCH to check if both users are in the graph
        MATCH (follower:User {id: $follower_id}), (followee:User {id: $followee_id})
        // Check if follow already exist
        OPTIONAL MATCH (follower)-[existing:FOLLOWS]->(followee)
        DELETE existing
        // Returns true if the relationship does not exist as 'flag'
        RETURN existing IS NULL AS flag;",
    )
    .param("follower_id", follower_id.to_string())
    .param("followee_id", followee_id.to_string())
}

/// Deletes a bookmark relationship between a user and a post
/// # Arguments
/// * `user_id` - The unique identifier of the user who created the bookmark.
/// * `bookmark_id` - The unique identifier of the bookmark relationship to be deleted.
pub fn delete_bookmark(user_id: &str, bookmark_id: &str) -> Query {
    Query::new(
        "delete_bookmark",
        "MATCH (u:User {id: $user_id})-[b:BOOKMARKED {id: $bookmark_id}]->(post:Post)<-[:AUTHORED]-(author:User)
         WITH post.id as post_id, author.id as author_id, b
         DELETE b
         RETURN post_id, author_id",
    )
    .param("user_id", user_id)
    .param("bookmark_id", bookmark_id)
}

/// Takes the write locks a tag's untag needs, target first and then the
/// tagger, and returns one row when the tag exists. Run it in the untag's
/// transaction before [`delete_tag`]: the locks stay held until the
/// transaction ends, so a cleanup that removes the same edge waits for the
/// untag's Redis writes, and [`delete_tag`] reads the edge after any such
/// cleanup finished.
pub fn lock_tag_target(user_id: &str, tag_id: &str) -> Query {
    Query::new(
        "lock_tag_target",
        "MATCH (user:User {id: $user_id})-[:TAGGED {id: $tag_id}]->(target)
         SET target.tag_cleanup_lock = true
         REMOVE target.tag_cleanup_lock
         WITH user
         SET user.tag_cleanup_lock = true
         REMOVE user.tag_cleanup_lock
         RETURN true AS locked",
    )
    .param("user_id", user_id)
    .param("tag_id", tag_id)
}

/// Deletes a tag relationship created by a user and retrieves relevant details about the tag's target
/// # Arguments
/// * `user_id` - The unique identifier of the user who created the tag.
/// * `tag_id` - The unique identifier of the `TAGGED` relationship to be deleted.
pub fn delete_tag(user_id: &str, tag_id: &str) -> Query {
    Query::new(
        "delete_tag",
        "MATCH (user:User {id: $user_id})-[tag:TAGGED {id: $tag_id}]->(target)
         OPTIONAL MATCH (target)<-[:AUTHORED]-(author:User)
         WITH CASE WHEN target:User THEN target.id ELSE null END AS user_id,
              CASE WHEN target:Post THEN target.id ELSE null END AS post_id,
              CASE WHEN target:Post THEN author.id ELSE null END AS author_id,
              CASE WHEN target:Listing THEN target.id ELSE null END AS listing_id,
              CASE WHEN target:Listing THEN target.owner_id ELSE null END AS listing_owner_id,
              CASE WHEN target:Shop THEN target.owner_id ELSE null END AS shop_owner_id,
              tag.label AS label,
              tag
         DELETE tag
         RETURN user_id, post_id, author_id, listing_id, listing_owner_id, shop_owner_id, label",
    )
    .param("user_id", user_id)
    .param("tag_id", tag_id)
}

/// Deletes every `TAGGED` edge on a marketplace listing and, in the same
/// statement, leaves one `TagCleanup` marker per deleted edge naming its
/// tagger and label. The marker id is fresh per deleted edge. The
/// statement takes the listing's write lock before it reads the edges, so
/// an untag holding the lock has finished, Redis writes included, and its
/// edge is already gone; a tag PUT holding it has committed its edge.
pub fn listing_tags_to_cleanup_markers(owner_id: &str, listing_id: &str, target: &str) -> Query {
    Query::new(
        "listing_tags_to_cleanup_markers",
        "MATCH (listing:Listing {id: $listing_id, owner_id: $owner_id})
         SET listing.tag_cleanup_lock = true
         REMOVE listing.tag_cleanup_lock
         WITH listing
         MATCH (tagger:User)-[tag:TAGGED]->(listing)
         CREATE (:TagCleanup {id: randomUUID(), target: $target, tagger_id: tagger.id,
                              label: tag.label})
         DELETE tag",
    )
    .param("owner_id", owner_id)
    .param("listing_id", listing_id)
    .param("target", target)
}

/// [`listing_tags_to_cleanup_markers`] for a marketplace shop.
pub fn shop_tags_to_cleanup_markers(owner_id: &str, target: &str) -> Query {
    Query::new(
        "shop_tags_to_cleanup_markers",
        "MATCH (shop:Shop {owner_id: $owner_id})
         SET shop.tag_cleanup_lock = true
         REMOVE shop.tag_cleanup_lock
         WITH shop
         MATCH (tagger:User)-[tag:TAGGED]->(shop)
         CREATE (:TagCleanup {id: randomUUID(), target: $target, tagger_id: tagger.id,
                              label: tag.label})
         DELETE tag",
    )
    .param("owner_id", owner_id)
    .param("target", target)
}

/// Takes a marketplace listing's write lock and returns one row when the
/// listing exists. Run it in a transaction: the lock stays held until the
/// transaction ends, so tag PUTs and untags of the listing wait, and each
/// one that finished before the lock was granted has committed its edge
/// and its Redis writes.
pub fn lock_listing(owner_id: &str, listing_id: &str) -> Query {
    Query::new(
        "lock_listing",
        "MATCH (listing:Listing {id: $listing_id, owner_id: $owner_id})
         SET listing.tag_cleanup_lock = true
         REMOVE listing.tag_cleanup_lock
         RETURN true AS locked",
    )
    .param("owner_id", owner_id)
    .param("listing_id", listing_id)
}

/// [`lock_listing`] for a marketplace shop.
pub fn lock_shop(owner_id: &str) -> Query {
    Query::new(
        "lock_shop",
        "MATCH (shop:Shop {owner_id: $owner_id})
         SET shop.tag_cleanup_lock = true
         REMOVE shop.tag_cleanup_lock
         RETURN true AS locked",
    )
    .param("owner_id", owner_id)
}

/// Deletes a shop node and all its relationships
/// # Arguments
/// * `owner_id` - The unique identifier of the user who owns the shop
pub fn delete_shop(owner_id: &str) -> Query {
    Query::new(
        "delete_shop",
        "MATCH (shop:Shop {owner_id: $owner_id})
         DETACH DELETE shop;",
    )
    .param("owner_id", owner_id.to_string())
}

/// Deletes every `TAGGED` edge on a post and returns the tagger and label
/// of each. Run it in the post deletion's transaction, after the post's
/// write lock is taken.
pub fn delete_post_tags(author_id: &str, post_id: &str) -> Query {
    Query::new(
        "delete_post_tags",
        "MATCH (:User {id: $author_id})-[:AUTHORED]->(post:Post {id: $post_id})
         MATCH (tagger:User)-[tag:TAGGED]->(post)
         WITH tagger.id AS tagger_id, tag.label AS label, tag
         DELETE tag
         RETURN tagger_id, label",
    )
    .param("author_id", author_id)
    .param("post_id", post_id)
}

/// Deletes one `TagCleanup` marker once its tagger count is settled.
pub fn delete_tag_cleanup_marker(id: &str) -> Query {
    Query::new(
        "delete_tag_cleanup_marker",
        "MATCH (c:TagCleanup {id: $id}) DELETE c",
    )
    .param("id", id)
}

/// Deletes a listing node and all its relationships
/// # Arguments
/// * `owner_id` - The unique identifier of the user who owns the listing
/// * `listing_id` - The unique identifier of the listing to be deleted
pub fn delete_listing(owner_id: &str, listing_id: &str) -> Query {
    Query::new(
        "delete_listing",
        "MATCH (listing:Listing {id: $listing_id, owner_id: $owner_id})
         DETACH DELETE listing;",
    )
    .param("owner_id", owner_id.to_string())
    .param("listing_id", listing_id.to_string())
}

/// Deletes a drop node and all its relationships
/// # Arguments
/// * `owner_id` - The unique identifier of the user who owns the drop
/// * `drop_id` - The unique identifier of the drop to be deleted
pub fn delete_drop(owner_id: &str, drop_id: &str) -> Query {
    Query::new(
        "delete_drop",
        "MATCH (drop:Drop {id: $drop_id, owner_id: $owner_id})
         DETACH DELETE drop;",
    )
    .param("owner_id", owner_id.to_string())
    .param("drop_id", drop_id.to_string())
}

/// Deletes a review edge between a reviewer and its subject
/// # Arguments
/// * `reviewer_id` - The unique identifier of the user who authored the review
/// * `review_id` - The deterministic identifier of the review to be deleted
pub fn delete_review(reviewer_id: &str, review_id: &str) -> Query {
    Query::new(
        "delete_review",
        "MATCH (reviewer:User {id: $reviewer_id})-[r:REVIEWED {review_id: $review_id}]->(:User)
         DELETE r;",
    )
    .param("reviewer_id", reviewer_id.to_string())
    .param("review_id", review_id.to_string())
}

/// Deletes a file node and all its relationships
/// # Arguments
/// * `owner_id` - The unique identifier of the user who owns the file
/// * `file_id` - The unique identifier of the file to be deleted
pub fn delete_file(owner_id: &str, file_id: &str) -> Query {
    Query::new(
        "delete_file",
        "MATCH (f:File {id: $id, owner_id: $owner_id})
         DETACH DELETE f;",
    )
    .param("id", file_id.to_string())
    .param("owner_id", owner_id.to_string())
}
