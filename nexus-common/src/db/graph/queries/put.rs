use crate::db::graph::error::{GraphError, GraphResult};
use crate::db::graph::Query;
use crate::models::marketplace::{ListingDetails, ShopDetails};
use crate::models::post::PostRelationships;
use crate::models::{file::FileDetails, post::PostDetails, user::UserDetails};
use pubky_app_specs::{ParsedUri, Resource};

/// Serializes a unit enum variant into its snake_case string form for graph storage.
fn enum_to_graph_string<T: serde::Serialize>(value: &T) -> GraphResult<String> {
    let json =
        serde_json::to_string(value).map_err(|e| GraphError::SerializationFailed(Box::new(e)))?;
    Ok(json.trim_matches('"').to_string())
}

/// Create a user node
pub fn create_user(user: &UserDetails) -> GraphResult<Query> {
    let links = serde_json::to_string(&user.links)
        .map_err(|e| GraphError::SerializationFailed(Box::new(e)))?;

    let query = Query::new(
        "create_user",
        "MERGE (u:User {id: $id})
         SET u.name = $name, u.bio = $bio, u.status = $status, u.links = $links, u.image = $image, u.indexed_at = $indexed_at;",
    )
    .param("id", user.id.to_string())
    .param("name", user.name.clone())
    .param("bio", user.bio.clone())
    .param("status", user.status.clone())
    .param("links", links)
    .param("image", user.image.clone())
    .param("indexed_at", user.indexed_at);

    Ok(query)
}

/// Creates a Cypher query to add or edit a post to the graph database and handles its relationships.
/// # Arguments
/// * `post` - A reference to a `PostDetails` struct containing information about the post to be created or edited
/// * `post_relationships` - A reference to a PostRelationships struct that define relationships
///   for the post (e.g., replies or reposts).
pub fn create_post(
    post: &PostDetails,
    post_relationships: &PostRelationships,
) -> GraphResult<Query> {
    let mut cypher = String::new();
    let mut new_relationships = Vec::new();

    // Check if all the dependencies are consistent in the graph
    if post_relationships.replied.is_some() {
        cypher.push_str("
            MATCH (reply_parent_author:User {id: $reply_parent_author_id})-[:AUTHORED]->(reply_parent_post:Post {id: $reply_parent_post_id})
        ");
        new_relationships.push("MERGE (new_post)-[:REPLIED]->(reply_parent_post)");
    };
    if post_relationships.reposted.is_some() {
        cypher.push_str("
            MATCH (repost_parent_author:User {id: $repost_parent_author_id})-[:AUTHORED]->(repost_parent_post:Post {id: $repost_parent_post_id})
        ");
        new_relationships.push("MERGE (new_post)-[:REPOSTED]->(repost_parent_post)");
    }
    // Create the new post
    cypher.push_str(
        "
        MATCH (author:User {id: $author_id})
        OPTIONAL MATCH (u)-[:AUTHORED]->(existing_post:Post {id: $post_id})
        MERGE (author)-[:AUTHORED]->(new_post:Post {id: $post_id})
    ",
    );

    // Add relationships to the query
    cypher.push_str(&new_relationships.join("\n"));

    cypher.push_str(
        "
        // Set indexed_at only on creation
        ON CREATE SET
            new_post.indexed_at = $indexed_at
        SET new_post.content = $content,
            new_post.kind = $kind,
            new_post.attachments = $attachments
        RETURN existing_post IS NOT NULL AS flag",
    );

    let kind = serde_json::to_string(&post.kind)
        .map_err(|e| GraphError::SerializationFailed(Box::new(e)))?;

    let mut cypher_query = Query::new("create_post", &cypher)
        .param("author_id", post.author.to_string())
        .param("post_id", post.id.to_string())
        .param("content", post.content.to_string())
        .param("indexed_at", post.indexed_at)
        .param("kind", kind.trim_matches('"'))
        .param("attachments", post.attachments.clone().unwrap_or_default());

    // Handle "replied" relationship
    cypher_query = add_relationship_params(
        cypher_query,
        &post_relationships
            .replied
            .clone()
            .and_then(|uri| uri.try_to_uri_str().ok()),
        "reply_parent_author_id",
        "reply_parent_post_id",
    )?;

    // Handle "reposted" relationship
    cypher_query = add_relationship_params(
        cypher_query,
        &post_relationships
            .reposted
            .clone()
            .and_then(|uri| uri.try_to_uri_str().ok()),
        "repost_parent_author_id",
        "repost_parent_post_id",
    )?;

    Ok(cypher_query)
}

fn add_relationship_params(
    cypher_query: Query,
    uri: &Option<String>,
    author_param: &str,
    post_param: &str,
) -> GraphResult<Query> {
    if let Some(uri) = uri {
        let parsed_uri = ParsedUri::try_from(uri.as_str()).map_err(GraphError::UriParseError)?;
        let parent_author_id = parsed_uri.user_id;
        let parent_post_id = match parsed_uri.resource {
            Resource::Post(id) => id,
            _ => {
                return Err(GraphError::InvalidResourceType(
                    "Reposted uri is not a Post resource".into(),
                ))
            }
        };

        return Ok(cypher_query
            .param(author_param, parent_author_id.as_ref() as &str)
            .param(post_param, parent_post_id.as_str()));
    }
    Ok(cypher_query)
}

/// Creates a `MENTIONED` relationship between a post and a user
/// # Arguments
/// * `author_id` - The unique identifier of the user who authored the post
/// * `post_id` - The unique identifier of the post where the mention occurs
/// * `mentioned_user_id` - The unique identifier of the user being mentioned in the post
pub fn create_mention_relationship(
    author_id: &str,
    post_id: &str,
    mentioned_user_id: &str,
) -> Query {
    Query::new(
        "create_mention_relationship",
        "MATCH (author:User {id: $author_id})-[:AUTHORED]->(post:Post {id: $post_id}),
              (mentioned_user:User {id: $mentioned_user_id})
         MERGE (post)-[:MENTIONED]->(mentioned_user)",
    )
    .param("author_id", author_id)
    .param("post_id", post_id)
    .param("mentioned_user_id", mentioned_user_id)
}

/// Create a follows relationship between two users. Before creating the relationship,
/// it validates that both users exist in the database
/// Validates that both users exist before creating the relationship
/// # Arguments
/// * `follower_id` - The unique identifier of the user who will follow another user.
/// * `followee_id` - The unique identifier of the user to be followed.
/// * `indexed_at` - A timestamp representing when the relationship was indexed or updated.
pub fn create_follow(follower_id: &str, followee_id: &str, indexed_at: i64) -> Query {
    Query::new(
        "create_follow",
        "MATCH (follower:User {id: $follower_id}), (followee:User {id: $followee_id})
         // Check if follow already existed
         OPTIONAL MATCH (follower)-[existing:FOLLOWS]->(followee)
         MERGE (follower)-[r:FOLLOWS]->(followee)
         SET r.indexed_at = $indexed_at
         // Returns true if the follow relationship already existed
         RETURN existing IS NOT NULL AS flag;",
    )
    .param("follower_id", follower_id.to_string())
    .param("followee_id", followee_id.to_string())
    .param("indexed_at", indexed_at)
}

/// Creates a "BOOKMARKED" relationship between a user and a post authored by another user
/// # Arguments
/// * `user_id` - The unique identifier of the user bookmarking the post.
/// * `author_id` - The unique identifier of the user who authored the post.
/// * `post_id` - The unique identifier of the post being bookmarked.
/// * `bookmark_id` - A unique identifier for the bookmark relationship.
/// * `indexed_at` - A timestamp representing when the bookmark relationship was created or last updated.
pub fn create_post_bookmark(
    user_id: &str,
    author_id: &str,
    post_id: &str,
    bookmark_id: &str,
    indexed_at: i64,
) -> Query {
    Query::new(
        "create_post_bookmark",
        "MATCH (u:User {id: $user_id})
        // We assume these nodes are already created. If not we would not be able to add a bookmark
        MATCH (author:User {id: $author_id})-[:AUTHORED]->(p:Post {id: $post_id})
        // Check if bookmark already existed
        OPTIONAL MATCH (u)-[existing:BOOKMARKED]->(p)
        MERGE (u)-[b:BOOKMARKED]->(p)
        SET b.indexed_at = $indexed_at,
            b.id = $bookmark_id
        // Returns true if the bookmark relationship already existed
        RETURN existing IS NOT NULL AS flag;",
    )
    .param("user_id", user_id)
    .param("author_id", author_id)
    .param("post_id", post_id)
    .param("bookmark_id", bookmark_id)
    .param("indexed_at", indexed_at)
}

/// Creates a `TAGGED` relationship between a user and a post authored by another user. The tag is uniquely
/// identified by a `label` and is associated with the post
/// # Arguments
/// * `user_id` - The unique identifier of the user tagging the post.
/// * `author_id` - The unique identifier of the user who authored the post.
/// * `post_id` - The unique identifier of the post being tagged.
/// * `tag_id` - A unique identifier for the tagging relationship.
/// * `label` - A string representing the label of the tag.
/// * `indexed_at` - A timestamp representing when the tagging relationship was created or last updated.
///
pub fn create_post_tag(
    user_id: &str,
    author_id: &str,
    post_id: &str,
    tag_id: &str,
    label: &str,
    indexed_at: i64,
) -> Query {
    Query::new(
        "create_post_tag",
        "MATCH (user:User {id: $user_id})
        // We assume these nodes are already created. If not we would not be able to add a tag
        MATCH (author:User {id: $author_id})-[:AUTHORED]->(post:Post {id: $post_id})
        // Check if tag already existed
        OPTIONAL MATCH (user)-[existing:TAGGED {label: $label}]->(post)
        MERGE (user)-[t:TAGGED {label: $label}]->(post)
        ON CREATE SET t.indexed_at = $indexed_at,
                      t.id = $tag_id
        // Returns true if the post tag relationship already existed
        RETURN existing IS NOT NULL AS flag;",
    )
    .param("user_id", user_id)
    .param("author_id", author_id)
    .param("post_id", post_id)
    .param("tag_id", tag_id)
    .param("label", label)
    .param("indexed_at", indexed_at)
}

/// Creates a `TAGGED` relationship between two users. The relationship is uniquely identified by a `label`
/// # Arguments
/// * `tagger_user_id` - The unique identifier of the user creating the tag.
/// * `tagged_user_id` - The unique identifier of the user being tagged.
/// * `tag_id` - A unique identifier for the tagging relationship.
/// * `label` - A string representing the label of the tag.
/// * `indexed_at` - A timestamp indicating when the tagging relationship was created or last updated.
pub fn create_user_tag(
    tagger_user_id: &str,
    tagged_user_id: &str,
    tag_id: &str,
    label: &str,
    indexed_at: i64,
) -> Query {
    Query::new(
        "create_user_tag",
        "MATCH (tagged_used:User {id: $tagged_user_id})
        MATCH (tagger:User {id: $tagger_user_id})
        // Check if tag already existed
        OPTIONAL MATCH (tagger)-[existing:TAGGED {label: $label}]->(tagged_used)
        MERGE (tagger)-[t:TAGGED {label: $label}]->(tagged_used)
        ON CREATE SET t.indexed_at = $indexed_at,
                      t.id = $tag_id
        // Returns true if the user tag relationship already existed
        RETURN existing IS NOT NULL AS flag;",
    )
    .param("tagger_user_id", tagger_user_id)
    .param("tagged_user_id", tagged_user_id)
    .param("tag_id", tag_id)
    .param("label", label)
    .param("indexed_at", indexed_at)
}

/// Create a file node
pub fn create_file(file: &FileDetails) -> GraphResult<Query> {
    let urls = serde_json::to_string(&file.urls)
        .map_err(|e| GraphError::SerializationFailed(Box::new(e)))?;

    let query = Query::new(
        "create_file",
        "MERGE (f:File {id: $id, owner_id: $owner_id})
         SET f.uri = $uri, f.indexed_at = $indexed_at, f.created_at = $created_at, f.size = $size,
            f.src = $src, f.name = $name, f.content_type = $content_type, f.urls = $urls;",
    )
    .param("id", file.id.to_string())
    .param("owner_id", file.owner_id.to_string())
    .param("uri", file.uri.to_string())
    .param("indexed_at", file.indexed_at)
    .param("created_at", file.created_at)
    .param("size", file.size)
    .param("src", file.src.to_string())
    .param("name", file.name.to_string())
    .param("content_type", file.content_type.to_string())
    .param("urls", urls);

    Ok(query)
}

/// Creates or updates the marketplace shop node of a seller.
/// The query returns no rows when the owner user is not yet indexed (missing dependency).
pub fn create_shop(shop: &ShopDetails) -> Query {
    Query::new(
        "create_shop",
        "MATCH (owner:User {id: $owner_id})
        OPTIONAL MATCH (owner)-[:HAS_SHOP]->(existing_shop:Shop)
        MERGE (owner)-[:HAS_SHOP]->(shop:Shop {owner_id: $owner_id})
        ON CREATE SET shop.indexed_at = $indexed_at
        SET shop.uri = $uri,
            shop.name = $name,
            shop.bio = $bio,
            shop.country_code = $country_code,
            shop.region = $region,
            shop.avatar_url = $avatar_url,
            shop.banner_url = $banner_url,
            shop.shipping_policy = $shipping_policy,
            shop.return_policy = $return_policy,
            shop.vacation_mode = $vacation_mode,
            shop.created_at = $created_at,
            shop.updated_at = $updated_at,
            shop.revision = $revision
        // Returns true if the shop node already existed
        RETURN existing_shop IS NOT NULL AS flag;",
    )
    .param("owner_id", shop.owner_id.to_string())
    .param("uri", shop.uri.to_string())
    .param("indexed_at", shop.indexed_at)
    .param("name", shop.name.to_string())
    .param("bio", shop.bio.to_string())
    .param("country_code", shop.country_code.to_string())
    .param("region", shop.region.clone())
    .param("avatar_url", shop.avatar_url.clone())
    .param("banner_url", shop.banner_url.clone())
    .param("shipping_policy", shop.shipping_policy.to_string())
    .param("return_policy", shop.return_policy.to_string())
    .param("vacation_mode", shop.vacation_mode)
    .param("created_at", shop.created_at.to_string())
    .param("updated_at", shop.updated_at.to_string())
    .param("revision", shop.revision)
}

/// Creates or updates a marketplace listing node of a seller.
/// The query returns no rows when the seller user is not yet indexed (missing dependency).
pub fn create_listing(listing: &ListingDetails) -> GraphResult<Query> {
    let state = enum_to_graph_string(&listing.state)?;
    let condition = enum_to_graph_string(&listing.condition)?;
    let sale_format = enum_to_graph_string(&listing.sale_format)?;
    let fulfillment_methods = listing
        .fulfillment_methods
        .iter()
        .map(enum_to_graph_string)
        .collect::<GraphResult<Vec<String>>>()?;

    let query = Query::new(
        "create_listing",
        "MATCH (seller:User {id: $owner_id})
        OPTIONAL MATCH (seller)-[:SELLS]->(existing_listing:Listing {id: $listing_id, owner_id: $owner_id})
        MERGE (seller)-[:SELLS]->(listing:Listing {id: $listing_id, owner_id: $owner_id})
        ON CREATE SET listing.indexed_at = $indexed_at
        SET listing.uri = $uri,
            listing.state = $state,
            listing.title = $title,
            listing.description = $description,
            listing.category_id = $category_id,
            listing.condition = $condition,
            listing.tags = $tags,
            listing.country_code = $country_code,
            listing.region = $region,
            listing.media_urls = $media_urls,
            listing.sale_format = $sale_format,
            listing.price_amount_minor = $price_amount_minor,
            listing.price_currency = $price_currency,
            listing.price_exponent = $price_exponent,
            listing.price_major = $price_major,
            listing.auction_starts_at = $auction_starts_at,
            listing.auction_ends_at = $auction_ends_at,
            listing.auction_ends_at_ms = $auction_ends_at_ms,
            listing.auction_reserve_price_minor = $auction_reserve_price_minor,
            listing.auction_buy_now_price_minor = $auction_buy_now_price_minor,
            listing.auction_minimum_increment_minor = $auction_minimum_increment_minor,
            listing.fulfillment_methods = $fulfillment_methods,
            listing.adult_only = $adult_only,
            listing.created_at = $created_at,
            listing.updated_at = $updated_at,
            listing.revision = $revision
        // Returns true if the listing node already existed
        RETURN existing_listing IS NOT NULL AS flag;",
    )
    .param("owner_id", listing.owner_id.to_string())
    .param("listing_id", listing.id.to_string())
    .param("uri", listing.uri.to_string())
    .param("indexed_at", listing.indexed_at)
    .param("state", state)
    .param("title", listing.title.to_string())
    .param("description", listing.description.to_string())
    .param("category_id", listing.category_id.to_string())
    .param("condition", condition)
    .param("tags", listing.tags.clone())
    .param("country_code", listing.country_code.to_string())
    .param("region", listing.region.clone())
    .param("media_urls", listing.media_urls.clone())
    .param("sale_format", sale_format)
    .param("price_amount_minor", listing.price_amount_minor)
    .param("price_currency", listing.price_currency.to_string())
    .param("price_exponent", listing.price_exponent)
    .param("price_major", listing.price_major())
    .param("auction_starts_at", listing.auction_starts_at.clone())
    .param("auction_ends_at", listing.auction_ends_at.clone())
    .param("auction_ends_at_ms", listing.auction_ends_at_ms())
    .param(
        "auction_reserve_price_minor",
        listing.auction_reserve_price_minor,
    )
    .param(
        "auction_buy_now_price_minor",
        listing.auction_buy_now_price_minor,
    )
    .param(
        "auction_minimum_increment_minor",
        listing.auction_minimum_increment_minor,
    )
    .param("fulfillment_methods", fulfillment_methods)
    .param("adult_only", listing.adult_only)
    .param("created_at", listing.created_at.to_string())
    .param("updated_at", listing.updated_at.to_string())
    .param("revision", listing.revision);

    Ok(query)
}

/// Create a homeserver
pub fn create_homeserver(homeserver_id: &str) -> Query {
    Query::new(
        "create_homeserver",
        "MERGE (hs:Homeserver {
          id: $id
        })
        RETURN hs;",
    )
    .param("id", homeserver_id)
}
