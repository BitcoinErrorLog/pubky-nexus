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
/// tagger and label. The marker id is fresh per deleted edge.
pub fn listing_tags_to_cleanup_markers(owner_id: &str, listing_id: &str, target: &str) -> Query {
    Query::new(
        "listing_tags_to_cleanup_markers",
        "MATCH (tagger:User)-[tag:TAGGED]->(:Listing {id: $listing_id, owner_id: $owner_id})
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
        "MATCH (tagger:User)-[tag:TAGGED]->(:Shop {owner_id: $owner_id})
         CREATE (:TagCleanup {id: randomUUID(), target: $target, tagger_id: tagger.id,
                              label: tag.label})
         DELETE tag",
    )
    .param("owner_id", owner_id)
    .param("target", target)
}

/// Deletes a marketplace listing node only if no `TAGGED` edge reaches it.
/// The write lock on the node is taken before the check (creating a
/// relationship locks both of its nodes), so a tag that commits first is
/// seen and refuses the deletion, and a tag that comes after finds no node.
///
/// Returns one row, `blocked`, when the node exists; no row when it is
/// already gone.
pub fn delete_untagged_listing(owner_id: &str, listing_id: &str) -> Query {
    Query::new(
        "delete_untagged_listing",
        "MATCH (listing:Listing {id: $listing_id, owner_id: $owner_id})
         SET listing.tag_cleanup_lock = true
         REMOVE listing.tag_cleanup_lock
         WITH listing,
              EXISTS { MATCH ()-[:TAGGED]->(listing) } AS blocked
         FOREACH (_ IN CASE WHEN blocked THEN [] ELSE [1] END | DETACH DELETE listing)
         RETURN blocked",
    )
    .param("owner_id", owner_id)
    .param("listing_id", listing_id)
}

/// [`delete_untagged_listing`] for a marketplace shop.
pub fn delete_untagged_shop(owner_id: &str) -> Query {
    Query::new(
        "delete_untagged_shop",
        "MATCH (shop:Shop {owner_id: $owner_id})
         SET shop.tag_cleanup_lock = true
         REMOVE shop.tag_cleanup_lock
         WITH shop,
              EXISTS { MATCH ()-[:TAGGED]->(shop) } AS blocked
         FOREACH (_ IN CASE WHEN blocked THEN [] ELSE [1] END | DETACH DELETE shop)
         RETURN blocked",
    )
    .param("owner_id", owner_id)
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
