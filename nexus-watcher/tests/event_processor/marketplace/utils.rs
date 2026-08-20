use anyhow::Result;
use pubky::{Keypair, ResourcePath};
use pubky_app_specs::{
    traits::{HasIdPath, TimestampId},
    PubkyAppFulfillmentMethod, PubkyAppListing, PubkyAppListingCondition, PubkyAppListingMedia,
    PubkyAppListingMediaKind, PubkyAppListingSale, PubkyAppListingState, PubkyAppListingVariant,
    PubkyAppMarketplaceLocation, PubkyAppMoney, PubkyAppReturnPolicy, PubkyAppShop,
};

use crate::event_processor::utils::watcher::WatcherTest;

/// Builds a valid shop record owned by the given user.
pub fn test_shop(owner_id: &str) -> PubkyAppShop {
    PubkyAppShop::new(
        owner_id.to_string(),
        1,
        "2025-01-01T00:00:00Z".to_string(),
        "2025-01-01T00:00:00Z".to_string(),
        "Watcher Marketplace Shop".to_string(),
        "Test shop for the marketplace watcher tests.".to_string(),
        PubkyAppMarketplaceLocation {
            country_code: "US".to_string(),
            region: Some("Oregon".to_string()),
        },
        None,
        None,
        "Ships within 3 business days.".to_string(),
        "Returns accepted within 30 days.".to_string(),
        false,
    )
}

/// Builds a valid pickup-fulfilled fixed-price listing record owned by the given user.
pub fn test_listing(
    owner_id: &str,
    title: &str,
    category_id: &str,
    condition: PubkyAppListingCondition,
    amount_minor: i64,
) -> PubkyAppListing {
    PubkyAppListing::new(
        owner_id.to_string(),
        1,
        "2025-01-01T00:00:00Z".to_string(),
        "2025-01-01T00:00:00Z".to_string(),
        // The caller assigns the listing_id right before publishing
        String::new(),
        PubkyAppListingState::Active,
        title.to_string(),
        "Test listing for the marketplace watcher tests.".to_string(),
        category_id.to_string(),
        condition,
        None,
        vec!["watcher-test".to_string()],
        PubkyAppMarketplaceLocation {
            country_code: "US".to_string(),
            region: None,
        },
        vec![PubkyAppListingMedia {
            id: "image_01".to_string(),
            kind: PubkyAppListingMediaKind::Image,
            url: format!("pubky://{owner_id}/pub/pubky.app/marketplace/v1/media/image_01"),
            content_hash: "a".repeat(64),
            mime_type: "image/png".to_string(),
            byte_size: 1024,
            width: 800,
            height: 600,
            duration_ms: None,
            alt_text: "A test image".to_string(),
        }],
        vec![PubkyAppListingVariant {
            id: "variant_01".to_string(),
            sku: None,
            options: Default::default(),
            price_override: None,
            quantity: 5,
            media_ids: vec![],
            enabled: true,
        }],
        PubkyAppListingSale::FixedPrice {
            unit_price: PubkyAppMoney {
                amount_minor,
                currency: "USD".to_string(),
                exponent: 2,
            },
            accepts_offers: false,
        },
        vec![PubkyAppFulfillmentMethod::Pickup],
        None,
        vec![],
        PubkyAppReturnPolicy {
            accepts_returns: false,
            return_window_days: None,
            buyer_pays_return_shipping: false,
            details: None,
        },
        None,
        false,
    )
}

impl WatcherTest {
    /// Publishes a listing with a freshly generated timestamp ID and returns
    /// the assigned ID together with the homeserver path.
    pub async fn create_listing(
        &mut self,
        user_kp: &Keypair,
        listing: &PubkyAppListing,
    ) -> Result<(String, ResourcePath)> {
        let mut listing = listing.clone();
        listing.listing_id = listing.create_id();
        let listing_id = listing.listing_id.clone();
        let listing_path: ResourcePath = PubkyAppListing::create_path(&listing_id).parse()?;

        self.put(user_kp, &listing_path, &listing).await?;

        Ok((listing_id, listing_path))
    }
}
