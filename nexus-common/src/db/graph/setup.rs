use crate::db::get_neo4j_graph;
use crate::db::graph::error::{GraphError, GraphResult};
use crate::db::graph::Query;
use tokio::sync::OnceCell;
use tracing::info;

static GRAPH_SETUP: OnceCell<()> = OnceCell::const_new();

/// Ensure the Neo4j graph has the required constraints and indexes.
///
/// Uses a `OnceCell` so that concurrent callers (e.g. API + watcher starting
/// in parallel) only execute DDL once; the second caller awaits the first.
pub async fn setup_graph() -> GraphResult<()> {
    GRAPH_SETUP
        .get_or_try_init(setup_graph_inner)
        .await
        .copied()
}

async fn setup_graph_inner() -> GraphResult<()> {
    // Define unique constraints
    let constraints = [
        "CREATE CONSTRAINT uniqueUserId IF NOT EXISTS FOR (u:User) REQUIRE u.id IS UNIQUE",
        "CREATE CONSTRAINT uniquePostId IF NOT EXISTS FOR (p:Post) REQUIRE p.id IS UNIQUE",
        "CREATE CONSTRAINT uniqueFileId IF NOT EXISTS FOR (f:File) REQUIRE (f.owner_id, f.id) IS UNIQUE",
        "CREATE CONSTRAINT uniqueHomeserverId IF NOT EXISTS FOR (hs:Homeserver) REQUIRE hs.id IS UNIQUE",
        "CREATE CONSTRAINT uniqueShopOwnerId IF NOT EXISTS FOR (s:Shop) REQUIRE s.owner_id IS UNIQUE",
        "CREATE CONSTRAINT uniqueListingId IF NOT EXISTS FOR (l:Listing) REQUIRE (l.owner_id, l.id) IS UNIQUE",
        "CREATE CONSTRAINT uniqueDropId IF NOT EXISTS FOR (d:Drop) REQUIRE (d.owner_id, d.id) IS UNIQUE",
    ];

    // Create indexes
    let indexes = [
        "CREATE INDEX userIdIndex IF NOT EXISTS FOR (u:User) ON (u.id)",
        "CREATE INDEX postIdIndex IF NOT EXISTS FOR (p:Post) ON (p.id)",
        "CREATE INDEX postTimestampIndex IF NOT EXISTS FOR (p:Post) ON (p.indexed_at)",
        "CREATE INDEX postKindIndex IF NOT EXISTS FOR (p:Post) ON (p.kind)",
        "CREATE INDEX taggedLabelIndex IF NOT EXISTS FOR ()-[r:TAGGED]-() ON (r.label)",
        "CREATE INDEX taggedTimestampIndex IF NOT EXISTS FOR ()-[r:TAGGED]-() ON (r.indexed_at)",
        "CREATE INDEX fileIdIndex IF NOT EXISTS FOR (f:File) ON (f.owner_id, f.id)",
        "CREATE INDEX homeserverIdIndex IF NOT EXISTS FOR (hs:Homeserver) ON (hs.id)",
        "CREATE INDEX shopOwnerIdIndex IF NOT EXISTS FOR (s:Shop) ON (s.owner_id)",
        "CREATE INDEX listingIdIndex IF NOT EXISTS FOR (l:Listing) ON (l.owner_id, l.id)",
        "CREATE INDEX listingTimestampIndex IF NOT EXISTS FOR (l:Listing) ON (l.indexed_at)",
        "CREATE INDEX listingCategoryIndex IF NOT EXISTS FOR (l:Listing) ON (l.category_id)",
        "CREATE INDEX listingConditionIndex IF NOT EXISTS FOR (l:Listing) ON (l.condition)",
        "CREATE INDEX listingSaleFormatIndex IF NOT EXISTS FOR (l:Listing) ON (l.sale_format)",
        "CREATE INDEX listingStateIndex IF NOT EXISTS FOR (l:Listing) ON (l.state)",
        "CREATE INDEX listingPriceIndex IF NOT EXISTS FOR (l:Listing) ON (l.price_currency, l.price_major)",
        "CREATE INDEX listingAuctionEndsAtIndex IF NOT EXISTS FOR (l:Listing) ON (l.auction_ends_at_ms)",
        "CREATE INDEX dropIdIndex IF NOT EXISTS FOR (d:Drop) ON (d.owner_id, d.id)",
        "CREATE INDEX dropStartsAtIndex IF NOT EXISTS FOR (d:Drop) ON (d.starts_at_ms)",
        "CREATE INDEX dropEndsAtIndex IF NOT EXISTS FOR (d:Drop) ON (d.ends_at_ms)",
    ];

    let queries = constraints.iter().chain(indexes.iter());

    let graph = get_neo4j_graph()?;

    for &ddl in queries {
        if let Err(e) = graph.run(Query::new("setup_ddl", ddl)).await {
            // `IF NOT EXISTS` does not protect against a concurrent process
            // creating the same rule between our existence check and create
            // (e.g. API + watcher, or parallel test binaries, booting at
            // once). Neo4j reports that race as an AlreadyExists schema
            // error, which means the rule is in place — the desired state.
            let message = e.to_string();
            if message.contains("AlreadyExists") {
                info!("Graph constraint/index already exists, skipping: {ddl}");
                continue;
            }
            return Err(GraphError::Generic(format!(
                "Failed to apply graph constraint/index '{ddl}': {e}"
            )));
        }
    }

    info!("Neo4j graph constraints and indexes have been applied successfully");

    Ok(())
}
