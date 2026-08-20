use super::utils::test_shop;
use crate::event_processor::utils::watcher::{HomeserverPath, WatcherTest};
use anyhow::Result;
use nexus_common::models::marketplace::ShopDetails;
use pubky::Keypair;
use pubky_app_specs::{PubkyAppShop, PubkyAppUser};

#[tokio_shared_rt::test(shared)]
async fn test_homeserver_shop_lifecycle() -> Result<()> {
    let mut test = WatcherTest::setup().await?;

    // Step 1: Create a user
    let user_kp = Keypair::random();
    let user = PubkyAppUser {
        bio: Some("test_homeserver_shop_lifecycle".to_string()),
        image: None,
        links: None,
        name: "Watcher:Shop:User".to_string(),
        status: None,
    };
    let user_id = test.create_user(&user_kp, &user).await?;

    // Step 2: Publish the shop record
    let shop = test_shop(&user_id);
    let shop_path = PubkyAppShop::hs_path();
    test.put(&user_kp, &shop_path, &shop).await?;

    // GRAPH_OP: Assert the shop node was written to the graph
    let graph_shop = ShopDetails::get_from_graph(&user_id)
        .await
        .unwrap()
        .expect("The shop was not saved in the graph");
    assert_eq!(graph_shop.owner_id, user_id);
    assert_eq!(graph_shop.name, shop.name);
    assert_eq!(graph_shop.bio, shop.bio);
    assert_eq!(graph_shop.country_code, shop.location.country_code);
    assert_eq!(graph_shop.region, shop.location.region);
    assert_eq!(graph_shop.shipping_policy, shop.shipping_policy);
    assert_eq!(graph_shop.return_policy, shop.return_policy);
    assert_eq!(graph_shop.vacation_mode, shop.vacation_mode);
    assert_eq!(graph_shop.revision, shop.revision);

    // INDEX_OP: Assert the shop details were indexed in Redis
    let indexed_shop = ShopDetails::get_from_index(&user_id)
        .await
        .unwrap()
        .expect("The shop details were not indexed");
    assert_eq!(indexed_shop.name, shop.name);
    assert_eq!(indexed_shop.owner_id, user_id);

    // Step 3: Update the shop record
    let mut updated_shop = shop.clone();
    updated_shop.name = "Watcher Marketplace Shop v2".to_string();
    updated_shop.vacation_mode = true;
    updated_shop.revision = 2;
    updated_shop.updated_at = "2025-01-02T00:00:00Z".to_string();
    test.put(&user_kp, &shop_path, &updated_shop).await?;

    let graph_shop = ShopDetails::get_from_graph(&user_id)
        .await
        .unwrap()
        .expect("The updated shop was not found in the graph");
    assert_eq!(graph_shop.name, updated_shop.name);
    assert!(graph_shop.vacation_mode);
    assert_eq!(graph_shop.revision, 2);

    let indexed_shop = ShopDetails::get_from_index(&user_id)
        .await
        .unwrap()
        .expect("The updated shop details were not indexed");
    assert_eq!(indexed_shop.name, updated_shop.name);
    assert!(indexed_shop.vacation_mode);

    // Step 4: Delete the shop record
    test.del(&user_kp, &shop_path).await?;

    let graph_shop = ShopDetails::get_from_graph(&user_id).await.unwrap();
    assert!(
        graph_shop.is_none(),
        "The shop node should be deleted from the graph"
    );
    let indexed_shop = ShopDetails::get_from_index(&user_id).await.unwrap();
    assert!(
        indexed_shop.is_none(),
        "The shop details should be deleted from the index"
    );

    // Cleanup user
    test.cleanup_user(&user_kp).await?;

    Ok(())
}
