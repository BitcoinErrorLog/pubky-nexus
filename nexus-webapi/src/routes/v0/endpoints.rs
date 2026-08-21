use const_format::concatcp;

// Version routes
const VERSION_ROUTE: &str = "/v0";

// Info routes
pub const INFO_ROUTE: &str = concatcp!(VERSION_ROUTE, "/info");

// -- USER endpoints --
const USER_PREFIX: &str = concatcp!(VERSION_ROUTE, "/user");
pub const USER_ROUTE: &str = concatcp!(USER_PREFIX, "/{user_id}");
pub const RELATIONSHIP_ROUTE: &str = concatcp!(USER_ROUTE, "/relationship/{viewer_id}");
pub const USER_COUNTS_ROUTE: &str = concatcp!(USER_ROUTE, "/counts");
pub const USER_DETAILS_ROUTE: &str = concatcp!(USER_ROUTE, "/details");
pub const USER_TAGS_ROUTE: &str = concatcp!(USER_ROUTE, "/tags");
pub const USER_TAGGERS_ROUTE: &str = concatcp!(USER_ROUTE, "/taggers/{label}");
pub const USER_FOLLOWERS_ROUTE: &str = concatcp!(USER_ROUTE, "/followers");
pub const USER_FOLLOWING_ROUTE: &str = concatcp!(USER_ROUTE, "/following");
pub const USER_FRIENDS_ROUTE: &str = concatcp!(USER_ROUTE, "/friends");

// -- POST endpoints --
pub const POST_PREFIX: &str = concatcp!(VERSION_ROUTE, "/post");
pub const POST_ROUTE: &str = concatcp!(POST_PREFIX, "/{author_id}/{post_id}");
pub const POST_RELATIONSHIPS_ROUTE: &str = concatcp!(POST_ROUTE, "/relationships");
pub const POST_BOOKMARK_ROUTE: &str = concatcp!(POST_ROUTE, "/bookmark");
pub const POST_COUNTS_ROUTE: &str = concatcp!(POST_ROUTE, "/counts");
pub const POST_DETAILS_ROUTE: &str = concatcp!(POST_ROUTE, "/details");
pub const POST_TAGS_ROUTE: &str = concatcp!(POST_ROUTE, "/tags");
pub const POST_TAGGERS_ROUTE: &str = concatcp!(POST_ROUTE, "/taggers/{label}");

// -- MARKETPLACE endpoints --
const SHOP_PREFIX: &str = concatcp!(VERSION_ROUTE, "/shop");
pub const SHOP_ROUTE: &str = concatcp!(SHOP_PREFIX, "/{seller_id}");
pub const SHOP_TAGS_ROUTE: &str = concatcp!(SHOP_ROUTE, "/tags");
pub const SHOP_TAGGERS_ROUTE: &str = concatcp!(SHOP_ROUTE, "/taggers/{label}");
pub const SHOP_REVIEWS_ROUTE: &str = concatcp!(SHOP_ROUTE, "/reviews");
pub const SHOP_REPUTATION_ROUTE: &str = concatcp!(SHOP_ROUTE, "/reputation");
const LISTING_PREFIX: &str = concatcp!(VERSION_ROUTE, "/listing");
pub const LISTING_ROUTE: &str = concatcp!(LISTING_PREFIX, "/{seller_id}/{listing_id}");
pub const LISTING_TAGS_ROUTE: &str = concatcp!(LISTING_ROUTE, "/tags");
pub const LISTING_TAGGERS_ROUTE: &str = concatcp!(LISTING_ROUTE, "/taggers/{label}");
pub const LISTING_REVIEWS_ROUTE: &str = concatcp!(LISTING_ROUTE, "/reviews");

// -- STREAM endpoints --
const STREAM_PREFIX: &str = concatcp!(VERSION_ROUTE, "/stream");
// STREAM of UserView objects
pub const STREAM_USERS_ROUTE: &str = concatcp!(STREAM_PREFIX, "/users");
pub const STREAM_USER_IDS_ROUTE: &str = concatcp!(STREAM_USERS_ROUTE, "/ids");
pub const STREAM_USERS_USERNAME_SEARCH_ROUTE: &str = concatcp!(STREAM_USERS_ROUTE, "/username");
pub const STREAM_USERS_BY_IDS_ROUTE: &str = concatcp!(STREAM_USERS_ROUTE, "/by_ids");
// STREAM of PostView objects
pub const STREAM_POSTS_ROUTE: &str = concatcp!(STREAM_PREFIX, "/posts");
pub const STREAM_POSTS_BY_IDS_ROUTE: &str = concatcp!(STREAM_POSTS_ROUTE, "/by_ids");
pub const STREAM_POST_KEYS_ROUTE: &str = concatcp!(STREAM_POSTS_ROUTE, "/keys");
// STREAM of ListingDetails objects
pub const STREAM_LISTINGS_ROUTE: &str = concatcp!(STREAM_PREFIX, "/listings");
// STREAM of Tags for posts
pub const STREAM_TAGS_ROUTE: &str = concatcp!(STREAM_PREFIX, "/tags");
pub const STREAM_TAGS_GLOBAL_ROUTE: &str = concatcp!(STREAM_TAGS_ROUTE, "/global");
pub const STREAM_TAGS_REACH_ROUTE: &str =
    concatcp!(STREAM_TAGS_ROUTE, "/reached/{user_id}/{reach}");

// -- SEARCH endpoints --
const SEARCH_PREFIX: &str = concatcp!(VERSION_ROUTE, "/search");
const SEARCH_USERS_ROUTE: &str = concatcp!(SEARCH_PREFIX, "/users");
pub const SEARCH_USERS_BY_NAME_ROUTE: &str = concatcp!(SEARCH_USERS_ROUTE, "/by_name/{prefix}");
pub const SEARCH_USERS_BY_ID_ROUTE: &str = concatcp!(SEARCH_USERS_ROUTE, "/by_id/{prefix}");
pub const SEARCH_POSTS_BY_TAG_ROUTE: &str = concatcp!(SEARCH_PREFIX, "/posts/by_tag/{tag}");
pub const SEARCH_TAGS_BY_PREFIX_ROUTE: &str = concatcp!(SEARCH_PREFIX, "/tags/by_prefix/{prefix}");

// -- TAG endpoints --
const TAG_PREFIX: &str = concatcp!(VERSION_ROUTE, "/tags");
pub const TAGS_HOT_ROUTE: &str = concatcp!(TAG_PREFIX, "/hot");
pub const TAG_TAGGERS_ROUTE: &str = concatcp!(TAG_PREFIX, "/taggers/{label}");
pub const TAG_ROUTE: &str = concatcp!(TAG_PREFIX, "/{tagger_id}/{tag_id}");

// -- FILE endpoints --
const FILE_PREFIX: &str = concatcp!(VERSION_ROUTE, "/files");
pub const FILE_LIST_ROUTE: &str = concatcp!(FILE_PREFIX, "/by_ids");
pub const FILE_ROUTE: &str = concatcp!(FILE_PREFIX, "/file/{file_id}");

// -- NOTIFICATION endpoints -
pub const NOTIFICATION_ROUTE: &str = concatcp!(USER_ROUTE, "/notifications");

// -- BOOTSTRAP endpoints -
pub const BOOTSTRAP_ROUTE: &str = concatcp!(VERSION_ROUTE, "/bootstrap/{user_id}");
pub const PUT_HOMESERVER_ROUTE: &str = concatcp!(VERSION_ROUTE, "/ingest/{user_id}");

// -- EVENTS endpoints
pub const EVENTS_ROUTE: &str = concatcp!(VERSION_ROUTE, "/events");
