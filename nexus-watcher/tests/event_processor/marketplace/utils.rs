use anyhow::Result;
use base32::{encode as base32_encode, Alphabet};
use ed25519_dalek::{Signer, SigningKey};
use pubky::{Keypair, ResourcePath};
use pubky_app_specs::{
    base64url_encode, listing_uri_builder, marketplace_review_uri_builder,
    traits::{HasIdPath, HashId, TimestampId},
    PubkyAppFulfillmentMethod, PubkyAppListing, PubkyAppListingCondition, PubkyAppListingMedia,
    PubkyAppListingMediaKind, PubkyAppListingSale, PubkyAppListingState, PubkyAppListingVariant,
    PubkyAppMarketplaceLocation, PubkyAppMarketplaceReview, PubkyAppMoney,
    PubkyAppPurchaseAttestationClaims, PubkyAppReturnPolicy, PubkyAppReviewRatings,
    PubkyAppReviewResponse, PubkyAppReviewRole, PubkyAppShop, PURCHASE_ATTESTATION_TYP,
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

/// Builds a valid auction listing record owned by the given user with the
/// given start and end times (RFC 3339). The auction terms are fixed: a
/// starting price of 10.00 USD, a reserve of 20.00 USD, a buy-now price of
/// 100.00 USD and a minimum increment of 1.00 USD.
pub fn test_auction_listing(
    owner_id: &str,
    title: &str,
    category_id: &str,
    starts_at: &str,
    ends_at: &str,
) -> PubkyAppListing {
    let usd = |amount_minor: i64| PubkyAppMoney {
        amount_minor,
        currency: "USD".to_string(),
        exponent: 2,
    };
    let mut listing = test_listing(
        owner_id,
        title,
        category_id,
        PubkyAppListingCondition::New,
        1_000,
    );
    listing.sale = PubkyAppListingSale::Auction {
        starting_price: usd(1_000),
        reserve_price: Some(usd(2_000)),
        buy_now_price: Some(usd(10_000)),
        minimum_increment: usd(100),
        starts_at: starts_at.to_string(),
        ends_at: ends_at.to_string(),
        anti_sniping_window_seconds: 300,
        anti_sniping_extension_seconds: 300,
    };
    listing
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

    /// Publishes a review record (its deterministic hash ID is derived here)
    /// and returns the assigned ID together with the homeserver path.
    pub async fn create_review(
        &mut self,
        reviewer_kp: &Keypair,
        review: &PubkyAppMarketplaceReview,
    ) -> Result<(String, ResourcePath)> {
        let mut review = review.clone();
        review.review_id = review.create_id();
        let review_id = review.review_id.clone();
        let review_path: ResourcePath =
            PubkyAppMarketplaceReview::create_path(&review_id).parse()?;

        self.put(reviewer_kp, &review_path, &review).await?;

        Ok((review_id, review_path))
    }

    /// Publishes a review response record under the subject's homeserver.
    pub async fn create_review_response(
        &mut self,
        responder_kp: &Keypair,
        response: &PubkyAppReviewResponse,
    ) -> Result<ResourcePath> {
        let response_path: ResourcePath =
            PubkyAppReviewResponse::create_path(&response.review_id).parse()?;
        self.put(responder_kp, &response_path, response).await?;
        Ok(response_path)
    }
}

/// A deterministic test attestor: an Ed25519 key whose z-base-32 encoding is
/// the attestor pubky, exactly like the production attestor identity.
pub fn test_attestor_key() -> SigningKey {
    SigningKey::from_bytes(&[42u8; 32])
}

pub fn attestor_pubky(key: &SigningKey) -> String {
    base32_encode(Alphabet::Z, key.verifying_key().as_bytes())
}

/// Builds valid `v: 1` purchase attestation claims binding the given review
/// parties and listing.
pub fn test_attestation_claims(
    iss: &str,
    reviewer_id: &str,
    subject_id: &str,
    listing_owner_id: &str,
    listing_id: &str,
    order_ref_seed: char,
) -> PubkyAppPurchaseAttestationClaims {
    PubkyAppPurchaseAttestationClaims {
        v: 1,
        iss: iss.to_string(),
        sub: reviewer_id.to_string(),
        cpk: subject_id.to_string(),
        role: "buyer_reviewing_seller".to_string(),
        listing: listing_uri_builder(listing_owner_id.to_string(), listing_id.to_string()),
        order_ref: order_ref_seed.to_string().repeat(64),
        completed_on: "2026-08-20".to_string(),
        amount_band: None,
        iat: 1_787_000_000,
    }
}

/// Signs the claims into a compact JWS with the given key. Passing a key
/// other than the one `iss` names produces a structurally valid but
/// signature-forged attestation.
pub fn sign_attestation(claims: &PubkyAppPurchaseAttestationClaims, key: &SigningKey) -> String {
    let header = serde_json::json!({ "alg": "EdDSA", "typ": PURCHASE_ATTESTATION_TYP });
    let header_b64 = base64url_encode(serde_json::to_vec(&header).unwrap().as_slice());
    let payload_b64 = base64url_encode(serde_json::to_vec(claims).unwrap().as_slice());
    let signing_input = format!("{header_b64}.{payload_b64}");
    let signature = key.sign(signing_input.as_bytes());
    format!(
        "{signing_input}.{}",
        base64url_encode(&signature.to_bytes())
    )
}

/// Builds a valid buyer-reviewing-seller review record. The caller assigns
/// `review_id` (via `create_id`) right before publishing.
pub fn test_review(
    reviewer_id: &str,
    subject_id: &str,
    listing_owner_id: &str,
    listing_id: &str,
    overall: i64,
    text: &str,
    attestation: &str,
) -> PubkyAppMarketplaceReview {
    PubkyAppMarketplaceReview::new(
        reviewer_id.to_string(),
        1,
        "2026-08-20T12:00:00Z".to_string(),
        "2026-08-20T12:00:00Z".to_string(),
        String::new(),
        subject_id.to_string(),
        listing_owner_id.to_string(),
        listing_id.to_string(),
        PubkyAppReviewRole::BuyerReviewingSeller,
        PubkyAppReviewRatings {
            overall,
            item_accuracy: None,
            shipping: None,
            communication: None,
        },
        text.to_string(),
        attestation.to_string(),
    )
}

/// Builds the subject's response record to a review.
pub fn test_review_response(
    responder_id: &str,
    reviewer_id: &str,
    review_id: &str,
    text: &str,
) -> PubkyAppReviewResponse {
    PubkyAppReviewResponse::new(
        responder_id.to_string(),
        1,
        "2026-08-21T00:00:00Z".to_string(),
        "2026-08-21T00:00:00Z".to_string(),
        review_id.to_string(),
        marketplace_review_uri_builder(reviewer_id.to_string(), review_id.to_string()),
        text.to_string(),
    )
}
