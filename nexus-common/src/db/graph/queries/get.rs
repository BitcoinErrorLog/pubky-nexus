use crate::db::graph::error::{GraphError, GraphResult};
use crate::db::graph::Query;
use crate::db::kv::SortOrder;
use crate::models::marketplace::{
    DropStreamBucket, DropStreamFilters, ListingStreamFilters, ListingStreamSorting,
};
use crate::models::post::StreamSource;
use crate::types::routes::HotTagsInputDTO;
use crate::types::Pagination;
use crate::types::StreamReach;
use crate::types::StreamSorting;
use crate::types::Timeframe;
use pubky_app_specs::PubkyAppPostKind;

// Retrieve post node by post id and author id
pub fn get_post_by_id(author_id: &str, post_id: &str) -> Query {
    Query::new(
        "get_post_by_id",
        "
            MATCH (u:User {id: $author_id})-[:AUTHORED]->(p:Post {id: $post_id})
            OPTIONAL MATCH (p)-[replied:REPLIED]->(parent_post:Post)<-[:AUTHORED]-(author:User)
            WITH u, p, parent_post, author
            RETURN {
                uri: 'pubky://' + u.id + '/pub/pubky.app/posts/' + p.id,
                content: p.content,
                id: p.id,
                indexed_at: p.indexed_at,
                author: u.id,
                // default value when the specified property is null
                // Avoids enum deserialization ERROR
                kind: COALESCE(p.kind, 'short'),
                attachments: p.attachments
            } as details,
            COLLECT([author.id, parent_post.id]) AS reply

        ",
    )
    .param("author_id", author_id)
    .param("post_id", post_id)
}

pub fn post_counts(author_id: &str, post_id: &str) -> Query {
    Query::new(
        "post_counts",
        "
        MATCH (u:User {id: $author_id})-[:AUTHORED]->(p:Post {id: $post_id})
        WITH p
        OPTIONAL MATCH (p)<-[t:TAGGED]-()
        WITH p, COUNT (t) AS tags_count, COUNT(DISTINCT t.label) AS unique_tags_count
        RETURN p IS NOT NULL AS exists,
            {
                tags: tags_count,
                unique_tags: unique_tags_count,
                replies: COUNT { (p)<-[:REPLIED]-() },
                reposts: COUNT { (p)<-[:REPOSTED]-() }
            } AS counts,
            EXISTS { (p)-[:REPLIED]->(:Post) } AS is_reply
    ",
    )
    .param("author_id", author_id)
    .param("post_id", post_id)
}

// Check if the viewer_id has a bookmark in the post
pub fn post_bookmark(author_id: &str, post_id: &str, viewer_id: &str) -> Query {
    Query::new(
        "post_bookmark",
        "MATCH (u:User {id: $author_id})-[:AUTHORED]->(p:Post {id: $post_id})
         MATCH (viewer:User {id: $viewer_id})-[b:BOOKMARKED]->(p)
         RETURN b",
    )
    .param("author_id", author_id)
    .param("post_id", post_id)
    .param("viewer_id", viewer_id)
}

// Check all the bookmarks that user creates
pub fn user_bookmarks(user_id: &str) -> Query {
    Query::new(
        "user_bookmarks",
        "MATCH (u:User {id: $user_id})-[b:BOOKMARKED]->(p:Post)<-[:AUTHORED]-(author:User)
         RETURN b, p.id AS post_id, author.id AS author_id",
    )
    .param("user_id", user_id)
}

// Get all the bookmarks that a post has received (used for edit/delete notifications)
pub fn get_post_bookmarks(author_id: &str, post_id: &str) -> Query {
    Query::new(
        "get_post_bookmarks",
        "MATCH (bookmarker:User)-[b:BOOKMARKED]->(p:Post {id: $post_id})<-[:AUTHORED]-(author:User {id: $author_id})
         RETURN b.id AS bookmark_id, bookmarker.id AS bookmarker_id",
    )
    .param("author_id", author_id)
    .param("post_id", post_id)
}

// Get all the reposts that a post has received (used for edit/delete notifications)
pub fn get_post_reposts(author_id: &str, post_id: &str) -> Query {
    Query::new(
        "get_post_reposts",
        "MATCH (reposter:User)-[:AUTHORED]->(repost:Post)-[:REPOSTED]->(p:Post {id: $post_id})<-[:AUTHORED]-(author:User {id: $author_id})
         RETURN reposter.id AS reposter_id, repost.id AS repost_id",
    )
    .param("author_id", author_id)
    .param("post_id", post_id)
}

// Get all the replies that a post has received (used for edit/delete notifications)
pub fn get_post_replies(author_id: &str, post_id: &str) -> Query {
    Query::new(
        "get_post_replies",
        "MATCH (replier:User)-[:AUTHORED]->(reply:Post)-[:REPLIED]->(p:Post {id: $post_id})<-[:AUTHORED]-(author:User {id: $author_id})
         RETURN replier.id AS replier_id, reply.id AS reply_id",
    )
    .param("author_id", author_id)
    .param("post_id", post_id)
}

// Get all the tags/taggers that a post has received (used for edit/delete notifications)
pub fn get_post_tags(author_id: &str, post_id: &str) -> Query {
    Query::new(
        "get_post_tags",
        "MATCH (tagger:User)-[t:TAGGED]->(p:Post {id: $post_id})<-[:AUTHORED]-(author:User {id: $author_id})
         RETURN tagger.id AS tagger_id, t.id AS tag_id",
    )
    .param("author_id", author_id)
    .param("post_id", post_id)
}

pub fn post_relationships(author_id: &str, post_id: &str) -> Query {
    Query::new(
        "post_relationships",
        "MATCH (u:User {id: $author_id})-[:AUTHORED]->(p:Post {id: $post_id})
        OPTIONAL MATCH (p)-[:REPLIED]->(replied_post:Post)<-[:AUTHORED]-(replied_author:User)
        OPTIONAL MATCH (p)-[:REPOSTED]->(reposted_post:Post)<-[:AUTHORED]-(reposted_author:User)
        OPTIONAL MATCH (p)-[:MENTIONED]->(mentioned_user:User)
        RETURN
          replied_post.id AS replied_post_id,
          replied_author.id AS replied_author_id,
          reposted_post.id AS reposted_post_id,
          reposted_author.id AS reposted_author_id,
          COLLECT(mentioned_user.id) AS mentioned_user_ids",
    )
    .param("author_id", author_id)
    .param("post_id", post_id)
}

// Retrieve many users by id
// We return also id if not we will not get not found users
pub fn get_users_details_by_ids(user_ids: &[&str]) -> Query {
    Query::new(
        "get_users_details_by_ids",
        "
        UNWIND $ids AS id
        OPTIONAL MATCH (record:User {id: id})
        RETURN
            id,
            CASE
                WHEN record IS NOT NULL
                    THEN record
                    ELSE null
                END AS record
        ",
    )
    .param("ids", user_ids)
}

/// Retrieves unique global tags for posts, returning a list of `post_ids` and `timestamp` pairs for each tag label.
pub fn global_tags_by_post() -> Query {
    Query::new(
        "global_tags_by_post",
        "
        MATCH (tagger:User)-[t:TAGGED]->(post:Post)<-[:AUTHORED]-(author:User)
        WITH t.label AS label, author.id + ':' + post.id AS post_id, post.indexed_at AS score
        WITH DISTINCT post_id, label, score
        WITH label, COLLECT([toFloat(score), post_id ]) AS sorted_set
        RETURN label, sorted_set
        ",
    )
}

// TODO: Do not traverse all the graph again to get the engagement score. Rethink how to share that info in the indexer
/// Retrieves unique global tags for posts, calculating an engagement score based on tag counts,
/// replies, reposts and mentions. The query returns a `key` by combining author's ID
/// and post's ID, along with a sorted set of engagement scores for each tag label.
pub fn global_tags_by_post_engagement() -> Query {
    Query::new(
        "global_tags_by_post_engagement",
        "
        MATCH (author:User)-[:AUTHORED]->(post:Post)<-[tag:TAGGED]-(tagger:User)
        WITH post, COUNT(tag) AS tags_count, tag.label AS label, author.id + ':' + post.id AS key
        WITH DISTINCT key, label, post, tags_count
        WHERE tags_count > 0
        OPTIONAL MATCH (post)<-[reply:REPLIED]-()
        OPTIONAL MATCH (post)<-[repost:REPOSTED]-()
        OPTIONAL MATCH (post)-[mention:MENTIONED]->()
        OPTIONAL MATCH (post)<-[tagged:TAGGED]-()
        WITH COUNT(DISTINCT tagged) AS taggers, COUNT(DISTINCT reply) AS replies_count, COUNT(DISTINCT repost) AS reposts_count, COUNT(DISTINCT mention) AS mention_count, key, label
        WITH label, COLLECT([toFloat(taggers + replies_count + reposts_count + mention_count), key ]) AS sorted_set
        RETURN label, sorted_set
        order by label
        ",
    )
}

// Retrieve all the tags of the post
pub fn post_tags(user_id: &str, post_id: &str) -> Query {
    Query::new(
        "post_tags",
        "
        MATCH (u:User {id: $user_id})-[:AUTHORED]->(p:Post {id: $post_id})
        CALL {
            WITH p
            MATCH (tagger:User)-[tag:TAGGED]->(p)
            WITH tag.label AS name, collect(DISTINCT tagger.id) AS tagger_ids
            RETURN collect({
                label: name,
                taggers: tagger_ids,
                taggers_count: SIZE(tagger_ids)
            }) AS tags
        }
        RETURN
            u IS NOT NULL AS exists,
            tags
    ",
    )
    .param("user_id", user_id)
    .param("post_id", post_id)
}

// Retrieve all the tags of the user
pub fn user_tags(user_id: &str) -> Query {
    Query::new(
        "user_tags",
        "
        MATCH (u:User {id: $user_id})
        CALL {
            WITH u
            MATCH (p:User)-[t:TAGGED]->(u)
            WITH t.label AS name, collect(DISTINCT p.id) AS tagger_ids
            RETURN collect({
                label: name,
                taggers: tagger_ids,
                taggers_count: SIZE(tagger_ids)
            }) AS tags
        }
        RETURN
            u IS NOT NULL AS exists,
            tags
    ",
    )
    .param("user_id", user_id)
}

// Retrieve all the tags of a marketplace listing
pub fn listing_tags(owner_id: &str, listing_id: &str) -> Query {
    Query::new(
        "listing_tags",
        "
        MATCH (l:Listing {id: $listing_id, owner_id: $owner_id})
        CALL {
            WITH l
            MATCH (tagger:User)-[tag:TAGGED]->(l)
            WITH tag.label AS name, collect(DISTINCT tagger.id) AS tagger_ids
            RETURN collect({
                label: name,
                taggers: tagger_ids,
                taggers_count: SIZE(tagger_ids)
            }) AS tags
        }
        RETURN
            l IS NOT NULL AS exists,
            tags
    ",
    )
    .param("owner_id", owner_id)
    .param("listing_id", listing_id)
}

// Retrieve all the tags of a marketplace shop
pub fn shop_tags(owner_id: &str) -> Query {
    Query::new(
        "shop_tags",
        "
        MATCH (:User {id: $owner_id})-[:HAS_SHOP]->(s:Shop {owner_id: $owner_id})
        CALL {
            WITH s
            MATCH (tagger:User)-[tag:TAGGED]->(s)
            WITH tag.label AS name, collect(DISTINCT tagger.id) AS tagger_ids
            RETURN collect({
                label: name,
                taggers: tagger_ids,
                taggers_count: SIZE(tagger_ids)
            }) AS tags
        }
        RETURN
            s IS NOT NULL AS exists,
            tags
    ",
    )
    .param("owner_id", owner_id)
}

/// Retrieve a homeserver by ID
pub fn get_homeserver_by_id(id: &str) -> Query {
    Query::new(
        "get_homeserver_by_id",
        "MATCH (hs:Homeserver {id: $id})
        WITH hs.id AS id
        RETURN id",
    )
    .param("id", id)
}

/// Retrieves all homeserver IDs
pub fn get_all_homeservers() -> Query {
    Query::new(
        "get_all_homeservers",
        "MATCH (hs:Homeserver)
        WITH collect(hs.id) AS homeservers_list
        RETURN homeservers_list",
    )
}

/// Retrieve tags for a user within the viewer's trusted network
/// # Arguments
///
/// - `user_id` - A string slice representing the ID of the user whose tags are being queried.
/// - `viewer_id` - A string slice representing the ID of the viewer whose trusted network is used as a filter.
/// - `depth` - A `u8` value specifying the depth of the viewer's trusted network (e.g., 1 for direct connections,
///   2 for connections of connections, and so on).
///
/// # Cypher Query Behavior
///
/// - **Nodes and Relationships**:
///   - Finds the `viewer` node with the given `viewer_id`.
///   - Finds the `tagged` user node with the given `user_id`.
///   - Traverses the `FOLLOWS` relationships up to the specified `depth` from the viewer to find trusted `tagger` users.
///   - Matches `TAGGED` relationships between taggers and the tagged user.
/// - **Return Values**:
///   - `exists`: A boolean indicating whether any taggers were found.
///   - `tags`: A collection of objects, each containing:
///       - `label`: The tag label.
///       - `taggers`: A list of tagger user IDs who applied the tag.
///       - `taggers_count`: The number of taggers who applied the tag.
pub fn get_viewer_trusted_network_tags(user_id: &str, viewer_id: &str, depth: u8) -> Query {
    let graph_query = format!(
        "
        MATCH (viewer:User {{id: $viewer_id}})
        MATCH (tagged:User {{id: $user_id}})
        CALL {{
            WITH viewer
            MATCH (viewer)-[:FOLLOWS*1..{depth}]->(tagger:User)
            RETURN DISTINCT tagger
        }}
        MATCH (tagger)-[tag:TAGGED]->(tagged)
        WITH tag.label AS label, collect(tagger.id) AS taggerIds
        RETURN 
            taggerIds IS NOT NULL AS exists,
            collect({{
                label: label,
                taggers: taggerIds,
                taggers_count: SIZE(taggerIds)
        }}) AS tags
        "
    );

    // Add to the query the params
    Query::new("get_viewer_trusted_network_tags", graph_query.as_str())
        .param("user_id", user_id)
        .param("viewer_id", viewer_id)
}

pub fn user_counts(user_id: &str) -> Query {
    Query::new(
        "user_counts",
        "
        MATCH (u:User {id: $user_id})
        // tags that reference this user
        OPTIONAL MATCH (u)<-[t:TAGGED]-(:User)
        WITH u, COUNT(DISTINCT t.label) AS unique_tags,

        // Count relationships to users
        COUNT { (u)-[:FOLLOWS]->(:User) } AS following,
        COUNT { (:User)-[:FOLLOWS]->(u) } AS followers,
        COUNT { (u)-[:FOLLOWS]->(friend:User) WHERE (friend)-[:FOLLOWS]->(u) } AS friends,

        // Count relationships to posts
        COUNT { (u)-[:AUTHORED]->(:Post) } AS posts,
        COUNT { (u)-[:AUTHORED]->(:Post)-[:REPLIED]->(:Post) } AS replies,
        COUNT { (u)-[:BOOKMARKED]->(:Post) } AS bookmarks,

        // Count user and post tagging
        COUNT { (u)-[:TAGGED]->(:User) } AS user_tags,
        COUNT { (u)-[:TAGGED]->(:Post) } AS post_tags,
        COUNT { (:User)-[:TAGGED]->(u) } AS tags

        RETURN
            u IS NOT NULL AS exists,
            {
                following: following,
                followers: followers,
                friends: friends,
                posts: posts,
                replies: replies,
                tagged: user_tags + post_tags,
                tags: tags,
                unique_tags: unique_tags,
                bookmarks: bookmarks
            } AS counts;
        ",
    )
    .param("user_id", user_id)
}

pub fn get_user_followers(user_id: &str, skip: Option<usize>, limit: Option<usize>) -> Query {
    let mut query_string = String::from(
        "MATCH (u:User {id: $user_id}) 
         OPTIONAL MATCH (u)<-[:FOLLOWS]-(follower:User)
         RETURN COUNT(u) > 0 AS user_exists, 
                COLLECT(follower.id) AS follower_ids",
    );
    if let Some(skip_value) = skip {
        query_string.push_str(&format!(" SKIP {skip_value}"));
    }
    if let Some(limit_value) = limit {
        query_string.push_str(&format!(" LIMIT {limit_value}"));
    }
    Query::new("get_user_followers", &query_string).param("user_id", user_id)
}

pub fn get_user_following(user_id: &str, skip: Option<usize>, limit: Option<usize>) -> Query {
    let mut query_string = String::from(
        "MATCH (u:User {id: $user_id}) 
         OPTIONAL MATCH (u)-[:FOLLOWS]->(following:User)
         RETURN COUNT(u) > 0 AS user_exists, 
                COLLECT(following.id) AS following_ids",
    );
    if let Some(skip_value) = skip {
        query_string.push_str(&format!(" SKIP {skip_value}"));
    }
    if let Some(limit_value) = limit {
        query_string.push_str(&format!(" LIMIT {limit_value}"));
    }
    Query::new("get_user_following", &query_string).param("user_id", user_id)
}

fn stream_reach_to_graph_subquery(reach: &StreamReach) -> String {
    match reach {
        StreamReach::Followers => "MATCH (user:User)<-[:FOLLOWS]-(reach:User)".to_string(),
        StreamReach::Following => "MATCH (user:User)-[:FOLLOWS]->(reach:User)".to_string(),
        StreamReach::Friends => {
            "MATCH (user:User)-[:FOLLOWS]->(reach:User), (user)<-[:FOLLOWS]-(reach)".to_string()
        }
        StreamReach::Wot(depth) => {
            format!("MATCH (user:User)-[:FOLLOWS*1..{depth}]->(reach:User)")
        }
    }
}

pub fn get_tags_by_label_prefix(label_prefix: &str) -> Query {
    Query::new(
        "get_tags_by_label_prefix",
        "
        MATCH ()-[t:TAGGED]->()
        WHERE t.label STARTS WITH $label_prefix
        RETURN COLLECT(DISTINCT t.label) AS tag_labels
        ",
    )
    .param("label_prefix", label_prefix)
}

pub fn get_tags() -> Query {
    Query::new(
        "get_tags",
        "
        MATCH ()-[t:TAGGED]->()
        RETURN COLLECT(DISTINCT t.label) AS tag_labels
        ",
    )
}

pub fn get_tag_taggers_by_reach(
    label: &str,
    user_id: &str,
    reach: StreamReach,
    skip: usize,
    limit: usize,
) -> Query {
    let cypher = format!(
        "
            {}
            // The tagged node can be generic, representing either a Post, a User, or both.
            // For now, it will be a Post to align with UX requirements.
            MATCH (reach)-[tag:TAGGED]->(tagged:Post)
            WHERE user.id = $user_id AND tag.label = $label

            // Get the latest tagged timestamp per `reach` user
            WITH DISTINCT reach, MAX(tag.indexed_at) AS latest_tag_time
            ORDER BY latest_tag_time DESC

            // Use slice notation instead of SKIP and LIMIT
            WITH COLLECT({{ reach_id: reach.id }})[$skip..$skip + $limit] AS paginated
            UNWIND paginated AS row

            RETURN COLLECT(row.reach_id) AS tagger_ids
            ",
        stream_reach_to_graph_subquery(&reach)
    );
    Query::new("get_tag_taggers_by_reach", &cypher)
        .param("label", label)
        .param("user_id", user_id)
        .param("skip", skip as i64)
        .param("limit", limit as i64)
}

pub fn get_hot_tags_by_reach(
    user_id: &str,
    reach: StreamReach,
    tags_query: &HotTagsInputDTO,
) -> Query {
    let input_tagged_type = match &tags_query.tagged_type {
        Some(tagged_type) => tagged_type.to_string(),
        None => String::from("Post|User"),
    };

    let (from, to) = tags_query.timeframe.to_timestamp_range();
    let cypher = format!(
        "
        {}
        MATCH (reach)-[tag:TAGGED]->(tagged:{})
        WHERE user.id = $user_id AND tag.indexed_at >= $from AND tag.indexed_at < $to
        WITH
            tag.label AS label,
            COLLECT(DISTINCT reach.id)[..{}] AS taggers,
            COUNT(DISTINCT tagged) AS uniqueTaggedCount,
            COUNT(DISTINCT reach.id) AS taggers_count
        WITH {{
            label: label,
            taggers_id: taggers,
            tagged_count: uniqueTaggedCount,
            taggers_count: taggers_count
        }} AS hot_tag
        ORDER BY hot_tag.tagged_count DESC, hot_tag.label ASC
        SKIP $skip LIMIT $limit
        RETURN COLLECT(hot_tag) as hot_tags
    ",
        stream_reach_to_graph_subquery(&reach),
        input_tagged_type,
        tags_query.taggers_limit
    );
    Query::new("get_hot_tags_by_reach", &cypher)
        .param("user_id", user_id)
        .param("skip", tags_query.skip as i64)
        .param("limit", tags_query.limit as i64)
        .param("from", from)
        .param("to", to)
}

pub fn get_global_hot_tags(tags_query: &HotTagsInputDTO) -> Query {
    let input_tagged_type = match &tags_query.tagged_type {
        Some(tagged_type) => tagged_type.to_string(),
        None => String::from("Post|User"),
    };
    let (from, to) = tags_query.timeframe.to_timestamp_range();
    let cypher = format!(
        "
        MATCH (user: User)-[tag:TAGGED]->(tagged:{})
        WHERE tag.indexed_at >= $from AND tag.indexed_at < $to
        WITH
            tag.label AS label,
            COLLECT(DISTINCT user.id)[..{}] AS taggers,
            COUNT(DISTINCT tagged) AS uniqueTaggedCount,
            COUNT(DISTINCT user.id) AS taggers_count
        WITH {{
            label: label,
            taggers_id: taggers,
            tagged_count: uniqueTaggedCount,
            taggers_count: taggers_count
        }} AS hot_tag
        ORDER BY hot_tag.tagged_count DESC, hot_tag.label ASC
        SKIP $skip LIMIT $limit
        RETURN COLLECT(hot_tag) as hot_tags
    ",
        input_tagged_type, tags_query.taggers_limit
    );
    Query::new("get_global_hot_tags", &cypher)
        .param("skip", tags_query.skip as i64)
        .param("limit", tags_query.limit as i64)
        .param("from", from)
        .param("to", to)
}

pub fn get_influencers_by_reach(
    user_id: &str,
    reach: StreamReach,
    skip: usize,
    limit: usize,
    timeframe: &Timeframe,
) -> Query {
    let (from, to) = timeframe.to_timestamp_range();
    let cypher = format!(
        "
        {}
        WHERE user.id = $user_id
        WITH DISTINCT reach
        WHERE reach.name <> '[DELETED]'

        CALL (reach) {{
            MATCH (others:User)-[follow:FOLLOWS]->(reach)
            RETURN count(DISTINCT follow) as followers_count
        }}
        CALL (reach) {{
            MATCH (reach)-[tag:TAGGED]->(:Post)
            WHERE tag.indexed_at >= $from AND tag.indexed_at < $to
            RETURN count(DISTINCT tag) as tags_count
        }}
        CALL (reach) {{
            MATCH (reach)-[:AUTHORED]->(post:Post)
            WHERE post.indexed_at >= $from AND post.indexed_at < $to
            RETURN count(DISTINCT post) as posts_count
        }}

        WITH reach, followers_count, tags_count, posts_count
        WITH {{
            id: reach.id,
            score: (tags_count + posts_count) * sqrt(followers_count)
        }} AS influencer
        ORDER BY influencer.score DESC
        SKIP $skip
        LIMIT $limit
        RETURN COLLECT([influencer.id, influencer.score]) as influencers
    ",
        stream_reach_to_graph_subquery(&reach),
    );
    Query::new("get_influencers_by_reach", &cypher)
        .param("user_id", user_id)
        .param("skip", skip as i64)
        .param("limit", limit as i64)
        .param("from", from)
        .param("to", to)
}

pub fn get_global_influencers(skip: usize, limit: usize, timeframe: &Timeframe) -> Query {
    let (from, to) = timeframe.to_timestamp_range();
    Query::new(
        "get_global_influencers",
        "
        MATCH (user:User)
        WHERE user.name <> '[DELETED]'
        WITH DISTINCT user

        OPTIONAL MATCH (others:User)-[follow:FOLLOWS]->(user)
        WHERE follow.indexed_at >= $from AND follow.indexed_at < $to

        OPTIONAL MATCH (user)-[tag:TAGGED]->(tagged:Post)
        WHERE tag.indexed_at >= $from AND tag.indexed_at < $to

        OPTIONAL MATCH (user)-[authored:AUTHORED]->(post:Post)
        WHERE authored.indexed_at >= $from AND authored.indexed_at < $to

        WITH user, COUNT(DISTINCT follow) AS followers_count, COUNT(DISTINCT tag) AS tags_count,
             COUNT(DISTINCT post) AS posts_count
        WITH {
            id: user.id,
            score: (tags_count + posts_count) * sqrt(followers_count)
        } AS influencer
        WHERE influencer.id IS NOT NULL

        ORDER BY influencer.score DESC, influencer.id ASC
        SKIP $skip
        LIMIT $limit
        RETURN COLLECT([influencer.id, influencer.score]) as influencers
    ",
    )
    .param("skip", skip as i64)
    .param("limit", limit as i64)
    .param("from", from)
    .param("to", to)
}

pub fn get_files_by_ids(key_pair: &[&[&str]]) -> Query {
    Query::new(
        "get_files_by_ids",
        "
        UNWIND $pairs AS pair
        OPTIONAL MATCH (record:File {owner_id: pair[0], id: pair[1]})
        RETURN record
        ",
    )
    .param("pairs", key_pair)
}

// Build the graph query based on parameters
pub fn post_stream(
    source: StreamSource,
    sorting: StreamSorting,
    tags: &Option<Vec<String>>,
    pagination: Pagination,
    kind: Option<PubkyAppPostKind>,
) -> Query {
    // Initialize the cypher query
    let mut cypher = String::new();

    // Initialize where_clause_applied to false
    let mut where_clause_applied = false;

    // Start with the observer node if needed
    // Needed that one for source pattern matching
    if source.get_observer().is_some() {
        cypher.push_str("MATCH (observer:User {id: $observer_id})\n");
    }

    // Base match for posts and authors
    cypher.push_str("MATCH (p:Post)<-[:AUTHORED]-(author:User)\n");

    // Apply source MATCH clause
    if let Some(query) = match source {
        StreamSource::Following { .. } => Some("MATCH (observer)-[:FOLLOWS]->(author)\n"),
        StreamSource::Followers { .. } => Some("MATCH (observer)<-[:FOLLOWS]-(author)\n"),
        StreamSource::Friends { .. } => {
            Some("MATCH (observer)-[:FOLLOWS]->(author)-[:FOLLOWS]->(observer)\n")
        }
        StreamSource::Bookmarks { .. } => Some("MATCH (observer)-[:BOOKMARKED]->(p)\n"),
        _ => None,
    } {
        cypher.push_str(query);
    }

    // Apply tags
    if tags.is_some() {
        cypher.push_str("MATCH (User)-[tag:TAGGED]->(p)\n");
        append_condition(
            &mut cypher,
            "tag.label IN $labels",
            &mut where_clause_applied,
        );
    }

    // If source has an author, add where clause. It is related with source pattern matching
    // If the source is Author, it is enough adding where clause. Not need to relate nodes
    if source.get_author().is_some() {
        append_condition(
            &mut cypher,
            "author.id = $author_id",
            &mut where_clause_applied,
        );
    }

    // If post kind is provided, add the corresponding condition
    if kind.is_some() {
        append_condition(&mut cypher, "p.kind = $kind", &mut where_clause_applied);
    }

    // Filter just the parent posts: StreamSource:PostReplies and StreamSource:AuthorReplies do not reach that query
    // so we do not need any condition to filter just parent nodes
    append_condition(
        &mut cypher,
        "NOT ( (p)-[:REPLIED]->(:Post) )",
        &mut where_clause_applied,
    );

    // Apply time interval conditions. Only can be applied with timeline sorting
    // The engagament score has to be computed
    if sorting == StreamSorting::Timeline {
        if pagination.start.is_some() {
            append_condition(
                &mut cypher,
                "p.indexed_at <= $start",
                &mut where_clause_applied,
            );
        }

        if pagination.end.is_some() {
            append_condition(
                &mut cypher,
                "p.indexed_at >= $end",
                &mut where_clause_applied,
            );
        }
    }

    // Make unique the posts, cannot be repeated
    cypher.push_str("WITH DISTINCT p, author\n");

    // Apply StreamSorting
    // Conditionally compute engagement counts only for TotalEngagement sorting
    let order_clause = match sorting {
        StreamSorting::Timeline => "ORDER BY p.indexed_at DESC".to_string(),
        StreamSorting::TotalEngagement => {
            // TODO: These optional matches could potentially be combined/collected to improve performance
            cypher.push_str(
                "
                // Count tags
                OPTIONAL MATCH (p)<-[tag:TAGGED]-(:User)  
                // Count replies
                OPTIONAL MATCH (p)<-[reply:REPLIED]-(:Post)
                // Count reposts
                OPTIONAL MATCH (p)<-[repost:REPOSTED]-(:Post)

                WITH p, author, 
                    COUNT(DISTINCT tag) AS tags_count,
                    COUNT(DISTINCT reply) AS replies_count,
                    COUNT(DISTINCT repost) AS reposts_count,
                    (COUNT(DISTINCT tag) + COUNT(DISTINCT reply) + COUNT(DISTINCT repost)) AS total_engagement
                ",
            );

            // Initialise again
            where_clause_applied = false;

            // Add total_engagement to filter by engagement the post
            if pagination.start.is_some() {
                append_condition(
                    &mut cypher,
                    "total_engagement <= $start",
                    &mut where_clause_applied,
                );
            }

            if pagination.end.is_some() {
                append_condition(
                    &mut cypher,
                    "total_engagement >= $end",
                    &mut where_clause_applied,
                );
            }

            "ORDER BY total_engagement DESC".to_string()
        }
    };

    // Final return statement
    cypher.push_str(&format!(
        "RETURN author.id AS author_id, p.id AS post_id, p.indexed_at AS indexed_at\n{order_clause}\n"
    ));

    // Apply skip and limit
    if let Some(skip) = pagination.skip {
        cypher.push_str(&format!("SKIP {skip}\n"));
    }
    if let Some(limit) = pagination.limit {
        cypher.push_str(&format!("LIMIT {limit}\n"));
    }

    // Build the query and apply parameters using `param` method
    let query = Query::new(
        match &source {
            StreamSource::Following { .. } => "post_stream_following",
            StreamSource::Followers { .. } => "post_stream_followers",
            StreamSource::Friends { .. } => "post_stream_friends",
            StreamSource::Bookmarks { .. } => "post_stream_bookmarks",
            StreamSource::Author { .. } => "post_stream_author",
            StreamSource::AuthorReplies { .. } => "post_stream_author_replies",
            StreamSource::PostReplies { .. } => "post_stream_post_replies",
            StreamSource::All => "post_stream_all",
        },
        &cypher,
    );
    build_query_with_params(query, &source, tags, kind, &pagination)
}

// Retrieve the shop node of a seller
pub fn get_shop_by_owner(owner_id: &str) -> Query {
    Query::new(
        "get_shop_by_owner",
        "
            MATCH (owner:User {id: $owner_id})-[:HAS_SHOP]->(shop:Shop)
            RETURN {
                owner_id: owner.id,
                uri: shop.uri,
                indexed_at: shop.indexed_at,
                name: shop.name,
                bio: shop.bio,
                country_code: shop.country_code,
                region: shop.region,
                avatar_url: shop.avatar_url,
                banner_url: shop.banner_url,
                shipping_policy: shop.shipping_policy,
                return_policy: shop.return_policy,
                vacation_mode: shop.vacation_mode,
                created_at: shop.created_at,
                updated_at: shop.updated_at,
                revision: shop.revision
            } AS details
        ",
    )
    .param("owner_id", owner_id)
}

// Retrieve a listing node by seller id and listing id
pub fn get_listing_by_id(owner_id: &str, listing_id: &str) -> Query {
    Query::new(
        "get_listing_by_id",
        "
            MATCH (seller:User {id: $owner_id})-[:SELLS]->(listing:Listing {id: $listing_id})
            RETURN {
                id: listing.id,
                uri: listing.uri,
                owner_id: seller.id,
                indexed_at: listing.indexed_at,
                state: listing.state,
                title: listing.title,
                description: listing.description,
                category_id: listing.category_id,
                condition: listing.condition,
                tags: COALESCE(listing.tags, []),
                country_code: listing.country_code,
                region: listing.region,
                media_urls: COALESCE(listing.media_urls, []),
                sale_format: listing.sale_format,
                price_amount_minor: listing.price_amount_minor,
                price_currency: listing.price_currency,
                price_exponent: listing.price_exponent,
                auction_starts_at: listing.auction_starts_at,
                auction_ends_at: listing.auction_ends_at,
                auction_reserve_price_minor: listing.auction_reserve_price_minor,
                auction_buy_now_price_minor: listing.auction_buy_now_price_minor,
                auction_minimum_increment_minor: listing.auction_minimum_increment_minor,
                fulfillment_methods: COALESCE(listing.fulfillment_methods, []),
                adult_only: listing.adult_only,
                created_at: listing.created_at,
                updated_at: listing.updated_at,
                revision: listing.revision
            } AS details
        ",
    )
    .param("owner_id", owner_id)
    .param("listing_id", listing_id)
}

// Retrieve a drop node by owner id and drop id
pub fn get_drop_by_id(owner_id: &str, drop_id: &str) -> Query {
    Query::new(
        "get_drop_by_id",
        "
            MATCH (owner:User {id: $owner_id})-[:OFFERS]->(drop:Drop {id: $drop_id})
            RETURN {
                id: drop.id,
                uri: drop.uri,
                owner_id: owner.id,
                indexed_at: drop.indexed_at,
                revision: drop.revision,
                title: drop.title,
                description: drop.description,
                media_urls: COALESCE(drop.media_urls, []),
                format: drop.format,
                starts_at: drop.starts_at,
                ends_at: drop.ends_at,
                listing_ids: COALESCE(drop.listing_ids, []),
                total_quantity: drop.total_quantity,
                per_buyer_limit: drop.per_buyer_limit,
                stock_display: drop.stock_display,
                created_at: drop.created_at,
                updated_at: drop.updated_at
            } AS details
        ",
    )
    .param("owner_id", owner_id)
    .param("drop_id", drop_id)
}

// Build the drop stream graph query based on the provided filters. The stream
// is ordered by the declared start time (`starts_at_ms`) and the pagination
// timeframe bounds apply to it. The bucket filter compares the declared
// schedule against `now_ms`, so the buckets are time-window estimates
// computed from the indexed record, never the transaction service's
// authoritative drop state.
pub fn drop_stream(
    filters: &DropStreamFilters,
    pagination: Pagination,
    order: SortOrder,
    now_ms: i64,
) -> Query {
    let mut cypher = String::new();
    let mut where_clause_applied = false;

    cypher.push_str("MATCH (owner:User)-[:OFFERS]->(drop:Drop)\n");

    if filters.owner.is_some() {
        append_condition(&mut cypher, "owner.id = $owner", &mut where_clause_applied);
    }
    if let Some(bucket) = &filters.bucket {
        let condition = match bucket {
            DropStreamBucket::Upcoming => "drop.starts_at_ms > $now",
            DropStreamBucket::LiveWindow => {
                "drop.starts_at_ms <= $now AND (drop.ends_at_ms IS NULL OR drop.ends_at_ms > $now)"
            }
            DropStreamBucket::EndedWindow => {
                "drop.ends_at_ms IS NOT NULL AND drop.ends_at_ms <= $now"
            }
        };
        append_condition(&mut cypher, condition, &mut where_clause_applied);
    }
    if pagination.start.is_some() {
        append_condition(
            &mut cypher,
            "drop.starts_at_ms <= $start",
            &mut where_clause_applied,
        );
    }
    if pagination.end.is_some() {
        append_condition(
            &mut cypher,
            "drop.starts_at_ms >= $end",
            &mut where_clause_applied,
        );
    }

    let sort_direction = match order {
        SortOrder::Ascending => "ASC",
        SortOrder::Descending => "DESC",
    };
    cypher.push_str(&format!(
        "WITH DISTINCT drop, owner
        RETURN owner.id AS owner_id, drop.id AS drop_id
        ORDER BY drop.starts_at_ms {sort_direction}\n",
    ));

    if let Some(skip) = pagination.skip {
        cypher.push_str(&format!("SKIP {skip}\n"));
    }
    if let Some(limit) = pagination.limit {
        cypher.push_str(&format!("LIMIT {limit}\n"));
    }

    let mut query = Query::new("drop_stream", &cypher);

    if let Some(owner) = &filters.owner {
        query = query.param("owner", owner.to_string());
    }
    if filters.bucket.is_some() {
        query = query.param("now", now_ms);
    }
    if let Some(start) = pagination.start {
        query = query.param("start", start);
    }
    if let Some(end) = pagination.end {
        query = query.param("end", end);
    }

    query
}

/// Serializes a unit enum variant into its snake_case string form for query parameters.
fn enum_query_param<T: serde::Serialize>(value: &T) -> GraphResult<String> {
    let json =
        serde_json::to_string(value).map_err(|e| GraphError::SerializationFailed(Box::new(e)))?;
    Ok(json.trim_matches('"').to_string())
}

// Build the listing stream graph query based on the provided filters. The
// stream is ordered by the property matching `sorting`; when sorting by the
// auction end time, listings without one (fixed-price listings) are excluded
// and the pagination timeframe bounds apply to the auction end time.
pub fn listing_stream(
    filters: &ListingStreamFilters,
    pagination: Pagination,
    order: SortOrder,
    sorting: ListingStreamSorting,
) -> GraphResult<Query> {
    let mut cypher = String::new();
    let mut where_clause_applied = false;

    let sort_property = match sorting {
        ListingStreamSorting::Timeline => "listing.indexed_at",
        ListingStreamSorting::EndsAt => "listing.auction_ends_at_ms",
    };

    cypher.push_str("MATCH (seller:User)-[:SELLS]->(listing:Listing)\n");

    if sorting == ListingStreamSorting::EndsAt {
        append_condition(
            &mut cypher,
            "listing.auction_ends_at_ms IS NOT NULL",
            &mut where_clause_applied,
        );
    }

    if filters.seller_id.is_some() {
        append_condition(
            &mut cypher,
            "seller.id = $seller_id",
            &mut where_clause_applied,
        );
    }
    if filters.category.is_some() {
        append_condition(
            &mut cypher,
            "listing.category_id = $category",
            &mut where_clause_applied,
        );
    }
    if filters.condition.is_some() {
        append_condition(
            &mut cypher,
            "listing.condition = $condition",
            &mut where_clause_applied,
        );
    }
    if filters.sale_format.is_some() {
        append_condition(
            &mut cypher,
            "listing.sale_format = $sale_format",
            &mut where_clause_applied,
        );
    }
    if filters.state.is_some() {
        append_condition(
            &mut cypher,
            "listing.state = $state",
            &mut where_clause_applied,
        );
    }
    if filters.currency.is_some() {
        append_condition(
            &mut cypher,
            "listing.price_currency = $currency",
            &mut where_clause_applied,
        );
    }
    if filters.min_price.is_some() {
        append_condition(
            &mut cypher,
            "listing.price_major >= $min_price",
            &mut where_clause_applied,
        );
    }
    if filters.max_price.is_some() {
        append_condition(
            &mut cypher,
            "listing.price_major <= $max_price",
            &mut where_clause_applied,
        );
    }
    if filters.tags.is_some() {
        // Community tags: a listing matches when any user has tagged it with
        // one of the requested labels.
        append_condition(
            &mut cypher,
            "EXISTS { MATCH (listing)<-[listing_tag:TAGGED]-(:User) WHERE listing_tag.label IN $tags }",
            &mut where_clause_applied,
        );
    }
    if pagination.start.is_some() {
        append_condition(
            &mut cypher,
            &format!("{sort_property} <= $start"),
            &mut where_clause_applied,
        );
    }
    if pagination.end.is_some() {
        append_condition(
            &mut cypher,
            &format!("{sort_property} >= $end"),
            &mut where_clause_applied,
        );
    }

    let sort_direction = match order {
        SortOrder::Ascending => "ASC",
        SortOrder::Descending => "DESC",
    };
    cypher.push_str(&format!(
        "WITH DISTINCT listing, seller
        RETURN seller.id AS owner_id, listing.id AS listing_id
        ORDER BY {sort_property} {sort_direction}\n",
    ));

    if let Some(skip) = pagination.skip {
        cypher.push_str(&format!("SKIP {skip}\n"));
    }
    if let Some(limit) = pagination.limit {
        cypher.push_str(&format!("LIMIT {limit}\n"));
    }

    let mut query = Query::new("listing_stream", &cypher);

    if let Some(seller_id) = &filters.seller_id {
        query = query.param("seller_id", seller_id.to_string());
    }
    if let Some(category) = &filters.category {
        query = query.param("category", category.to_string());
    }
    if let Some(condition) = &filters.condition {
        query = query.param("condition", enum_query_param(condition)?);
    }
    if let Some(sale_format) = &filters.sale_format {
        query = query.param("sale_format", enum_query_param(sale_format)?);
    }
    if let Some(state) = &filters.state {
        query = query.param("state", enum_query_param(state)?);
    }
    if let Some(currency) = &filters.currency {
        query = query.param("currency", currency.to_string());
    }
    if let Some(min_price) = filters.min_price {
        query = query.param("min_price", min_price);
    }
    if let Some(max_price) = filters.max_price {
        query = query.param("max_price", max_price);
    }
    if let Some(tags) = &filters.tags {
        query = query.param("tags", tags.clone());
    }
    if let Some(start) = pagination.start {
        query = query.param("start", start);
    }
    if let Some(end) = pagination.end {
        query = query.param("end", end);
    }

    Ok(query)
}

/// Appends a condition to the Cypher query, using `WHERE` if no `WHERE` clause
/// has been applied yet, or `AND` if a `WHERE` clause is already present.
///
/// # Arguments
///
/// * `cypher` - A mutable reference to the Cypher query string to which the condition will be appended
/// * `condition` - The condition to be added to the query
/// * `where_clause_applied` - A mutable reference to a boolean flag indicating whether a `WHERE` clause
///   has already been applied to the query.
fn append_condition(cypher: &mut String, condition: &str, where_clause_applied: &mut bool) {
    if *where_clause_applied {
        cypher.push_str(&format!("AND {condition}\n"));
    } else {
        cypher.push_str(&format!("WHERE {condition}\n"));
        *where_clause_applied = true;
    }
}

/// Applies the necessary parameters to an already-constructed `Query`.
///
/// # Arguments
///
/// * `query` - A `Query` already constructed with its label and cypher string.
/// * `source` - The `StreamSource` specifying the origin of the posts (e.g., Following, Followers).
/// * `tags` - An optional list of tag labels to filter the posts.
/// * `kind` - An optional `PubkyAppPostKind` to filter the posts by their kind.
/// * `pagination` - The `Pagination` object containing pagination parameters like `start`, `end`, `skip`, and `limit`.
fn build_query_with_params(
    mut query: Query,
    source: &StreamSource,
    tags: &Option<Vec<String>>,
    kind: Option<PubkyAppPostKind>,
    pagination: &Pagination,
) -> Query {
    if let Some(observer_id) = source.get_observer() {
        query = query.param("observer_id", observer_id.to_string());
    }
    if let Some(labels) = tags.clone() {
        query = query.param("labels", labels);
    }
    if let Some(author_id) = source.get_author() {
        query = query.param("author_id", author_id.to_string());
    }
    if let Some(post_kind) = kind {
        query = query.param("kind", post_kind.to_string());
    }
    if let Some(start_interval) = pagination.start {
        query = query.param("start", start_interval);
    }
    if let Some(end_interval) = pagination.end {
        query = query.param("end", end_interval);
    }

    query
}

/// Determines whether a user has any relationships
/// # Arguments
/// * `user_id` - The unique identifier of the user
pub fn user_is_safe_to_delete(user_id: &str) -> Query {
    Query::new(
        "user_is_safe_to_delete",
        "
        MATCH (u:User {id: $user_id})
        // Ensures all relationships to the user (u) are checked, counting as 0 if none exist
        OPTIONAL MATCH (u)-[r]-()
        // Checks if the user has any relationships
        WITH u, NOT (COUNT(r) = 0) AS flag
        RETURN flag
        ",
    )
    .param("user_id", user_id)
}

/// Checks if a post has any relationships that aren't in the set of allowed
/// relationships for post deletion. If the post has such relationships,
/// the query returns `true`; otherwise, `false`
/// If the user or post does not exist, the query returns no rows.
/// # Arguments
/// * `author_id` - The unique identifier of the user who authored the post
/// * `post_id` - The unique identifier of the post
pub fn post_is_safe_to_delete(author_id: &str, post_id: &str) -> Query {
    Query::new(
        "post_is_safe_to_delete",
        "
        MATCH (u:User {id: $author_id})-[:AUTHORED]->(p:Post {id: $post_id})
        // Ensures all relationships to the post (p) are checked, counting as 0 if none exist
        OPTIONAL MATCH (p)-[r]-()
        WHERE NOT (
            // Allowed relationships:
            // 1. Incoming AUTHORED relationship from the specified user
            (type(r) = 'AUTHORED' AND startNode(r).id = $author_id AND endNode(r) = p)
            OR
            // 2. Outgoing REPOSTED relationship to another post
            (type(r) = 'REPOSTED' AND startNode(r) = p)
            OR
            // 3. Outgoing REPLIED relationship to another post
            (type(r) = 'REPLIED' AND startNode(r) = p)
        )
        // Checks if any disallowed relationships exist for the post
        WITH p, NOT (COUNT(r) = 0) AS flag
        RETURN flag
        ",
    )
    .param("author_id", author_id)
    .param("post_id", post_id)
}

/// Find user recommendations: active users (with 5+ posts) who are 1-3 degrees of separation away
/// from the given user, but not directly followed by them
pub fn recommend_users(user_id: &str, limit: usize) -> Query {
    Query::new(
        "recommend_users",
        "
        MATCH (user:User {id: $user_id})
        MATCH (user)-[:FOLLOWS*1..3]->(potential:User)
        WHERE NOT (user)-[:FOLLOWS]->(potential)
        AND potential.id <> $user_id
        WITH DISTINCT potential
        MATCH (potential)-[:AUTHORED]->(post:Post)
        WITH potential, COUNT(post) AS post_count
        WHERE post_count >= 5
        RETURN potential.id AS recommended_user_id, potential.name AS recommended_user_name
        LIMIT $limit
    ",
    )
    .param("user_id", user_id.to_string())
    .param("limit", limit as i64)
}

/// Retrieve specific tag created by the user
pub fn get_tag_by_tagger_and_id(tagger_id: &str, tag_id: &str) -> Query {
    Query::new(
        "get_tag_by_tagger_and_id",
        "
        MATCH (tagger:User { id: $tagger_id})-[tag:TAGGED {id: $tag_id }]->(tagged)
        OPTIONAL MATCH (author:User)-[:AUTHORED]->(tagged)
        RETURN
            labels(tagged) as tagged_labels,
            tagged.id as tagged_id,
            author.id as author_id,
            tag.id as id,
            tag.indexed_at as indexed_at,
            tag.label as label
        ",
    )
    .param("tagger_id", tagger_id)
    .param("tag_id", tag_id)
}

/// The aggregate projection shared by the reputation queries. Averages skip
/// null sub-ratings (Cypher `avg` ignores nulls); the attestor list carries
/// one entry per verified review for per-attestor counting in Rust.
const REPUTATION_RETURN: &str = "
        RETURN count(r) AS total,
            sum(CASE WHEN r.verified THEN 1 ELSE 0 END) AS verified_count,
            avg(toFloat(r.rating_overall)) AS average,
            sum(CASE WHEN r.rating_overall = 1 THEN 1 ELSE 0 END) AS stars_1,
            sum(CASE WHEN r.rating_overall = 2 THEN 1 ELSE 0 END) AS stars_2,
            sum(CASE WHEN r.rating_overall = 3 THEN 1 ELSE 0 END) AS stars_3,
            sum(CASE WHEN r.rating_overall = 4 THEN 1 ELSE 0 END) AS stars_4,
            sum(CASE WHEN r.rating_overall = 5 THEN 1 ELSE 0 END) AS stars_5,
            avg(toFloat(r.rating_item_accuracy)) AS avg_item_accuracy,
            avg(toFloat(r.rating_shipping)) AS avg_shipping,
            avg(toFloat(r.rating_communication)) AS avg_communication,
            sum(CASE WHEN r.has_response THEN 1 ELSE 0 END) AS response_count,
            sum(CASE WHEN r.edited_late THEN 1 ELSE 0 END) AS edited_late_count,
            [x IN collect(CASE WHEN r.verified THEN r.attestor_id ELSE null END) WHERE x IS NOT NULL] AS attestors,
            max(r.created_at) AS last_reviewed_at
        ";

/// Aggregates every `REVIEWED` edge pointing at a subject in one role into
/// the reputation summary row.
pub fn subject_reputation(subject_id: &str, role: &str) -> Query {
    let cypher = format!(
        "MATCH (:User)-[r:REVIEWED]->(subject:User {{id: $subject_id}})
        WHERE r.role = $role
        {REPUTATION_RETURN}"
    );
    Query::new("subject_reputation", &cypher)
        .param("subject_id", subject_id.to_string())
        .param("role", role.to_string())
}

/// Aggregates the buyer reviews of one listing into the reputation summary
/// row. For `buyer_reviewing_seller` reviews the subject is the listing
/// owner, which anchors the scan.
pub fn listing_reputation(listing_owner_id: &str, listing_id: &str) -> Query {
    let cypher = format!(
        "MATCH (:User)-[r:REVIEWED]->(subject:User {{id: $listing_owner_id}})
        WHERE r.role = 'buyer_reviewing_seller' AND r.listing_id = $listing_id
        {REPUTATION_RETURN}"
    );
    Query::new("listing_reputation", &cypher)
        .param("listing_owner_id", listing_owner_id.to_string())
        .param("listing_id", listing_id.to_string())
}
