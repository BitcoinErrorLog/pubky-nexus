use crate::db::kv::RedisResult;
use crate::db::{
    exec_single_row, execute_graph_operation, fetch_row_from_graph, queries, GraphResult,
    OperationOutcome, RedisOps,
};
use crate::models::error::ModelResult;
use chrono::Utc;
use pubky_app_specs::{shop_uri_builder, PubkyAppShop, PubkyId};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// Represents the indexed details of a seller's marketplace shop (singleton per user).
#[derive(Serialize, Deserialize, ToSchema, Clone, Debug, PartialEq)]
pub struct ShopDetails {
    pub owner_id: String,
    pub uri: String,
    pub indexed_at: i64,
    pub name: String,
    pub bio: String,
    pub country_code: String,
    pub region: Option<String>,
    pub avatar_url: Option<String>,
    pub banner_url: Option<String>,
    pub shipping_policy: String,
    pub return_policy: String,
    pub vacation_mode: bool,
    pub created_at: String,
    pub updated_at: String,
    pub revision: i64,
}

impl RedisOps for ShopDetails {}

impl ShopDetails {
    pub fn from_homeserver(homeserver_shop: PubkyAppShop, owner_id: &PubkyId) -> Self {
        ShopDetails {
            owner_id: owner_id.to_string(),
            uri: shop_uri_builder(owner_id.to_string()),
            indexed_at: Utc::now().timestamp_millis(),
            name: homeserver_shop.name,
            bio: homeserver_shop.bio,
            country_code: homeserver_shop.location.country_code,
            region: homeserver_shop.location.region,
            avatar_url: homeserver_shop.avatar_url,
            banner_url: homeserver_shop.banner_url,
            shipping_policy: homeserver_shop.shipping_policy,
            return_policy: homeserver_shop.return_policy,
            vacation_mode: homeserver_shop.vacation_mode,
            created_at: homeserver_shop.created_at,
            updated_at: homeserver_shop.updated_at,
            revision: homeserver_shop.revision,
        }
    }

    /// Retrieves shop details by owner ID, first trying Redis and falling back to Neo4j.
    pub async fn get_by_id(owner_id: &str) -> ModelResult<Option<ShopDetails>> {
        match Self::get_from_index(owner_id).await? {
            Some(details) => Ok(Some(details)),
            None => {
                let maybe_details = Self::get_from_graph(owner_id).await?;
                if let Some(details) = maybe_details {
                    details.put_to_index().await?;
                    return Ok(Some(details));
                }
                Ok(None)
            }
        }
    }

    pub async fn get_from_index(owner_id: &str) -> RedisResult<Option<ShopDetails>> {
        Self::try_from_index_json(&[owner_id], None).await
    }

    /// Retrieves the shop fields from Neo4j.
    pub async fn get_from_graph(owner_id: &str) -> GraphResult<Option<ShopDetails>> {
        let query = queries::get::get_shop_by_owner(owner_id);
        let maybe_row = fetch_row_from_graph(query).await?;

        let Some(row) = maybe_row else {
            return Ok(None);
        };

        let shop: ShopDetails = row.get("details")?;
        Ok(Some(shop))
    }

    // Save new graph node
    pub async fn put_to_graph(&self) -> GraphResult<OperationOutcome> {
        let query = queries::put::create_shop(self);
        execute_graph_operation(query).await
    }

    pub async fn put_to_index(&self) -> RedisResult<()> {
        self.put_index_json(&[&self.owner_id], None, None).await
    }

    pub async fn delete(owner_id: &str) -> ModelResult<()> {
        // Delete shop graph node
        exec_single_row(queries::del::delete_shop(owner_id)).await?;
        // Delete shop details on Redis
        Self::remove_from_index_multiple_json(&[&[owner_id]]).await?;
        Ok(())
    }
}
