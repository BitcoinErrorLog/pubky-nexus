use crate::routes::v0::endpoints::{
    LISTING_REVIEWS_ROUTE, SHOP_REPUTATION_ROUTE, SHOP_REVIEWS_ROUTE,
};
use crate::{Error, Result};
use axum::extract::{Path, Query};
use axum::Json;
use nexus_common::models::marketplace::{
    ReputationSnippet, ReputationSummary, ReviewDetails, ReviewResponseDetails, ReviewStream,
    ReviewView,
};
use nexus_common::types::Pagination;
use pubky_app_specs::PubkyAppReviewRole;
use serde::Deserialize;
use tracing::debug;
use utoipa::{OpenApi, ToSchema};

#[derive(Deserialize, Debug, ToSchema)]
pub struct ReviewStreamQuery {
    #[serde(flatten)]
    pub pagination: Pagination,
    /// Review direction. Defaults to `buyer_reviewing_seller` (the public
    /// seller-reputation surface). `seller_reviewing_buyer` serves
    /// negotiation contexts only (ratified D8) — clients must not build
    /// public buyer profiles from it.
    pub role: Option<PubkyAppReviewRole>,
}

impl ReviewStreamQuery {
    pub fn initialize_defaults(&mut self) {
        self.pagination.skip.get_or_insert(0);
        self.pagination.limit = Some(self.pagination.limit.unwrap_or(10).min(30));
    }
}

#[utoipa::path(
    get,
    path = SHOP_REVIEWS_ROUTE,
    description = r#"Paged reviews about a subject, newest-indexed first, with subject responses joined.

Every entry carries a `verified` flag: true iff the review's embedded purchase attestation parsed as a
compact JWS, its Ed25519 signature verified against the self-certifying `iss` pubky, and its claims bind
to the review (reviewer/subject/listing/role). Verification proves WHICH key signed — `attestor_id` names
it — never that the signer is legitimate; clients apply their own attestor trust list at display time.
Unverified reviews are included and must be labeled as such (ratified D5)."#,
    tag = "Marketplace",
    params(
        ("seller_id" = String, Path, description = "Subject Pubky ID (the reviewed user)"),
        ("role" = Option<PubkyAppReviewRole>, Query, description = "Review direction; defaults to buyer_reviewing_seller"),
        ("skip" = Option<usize>, Query, description = "Skip N reviews"),
        ("limit" = Option<usize>, Query, description = "Retrieve N reviews"),
    ),
    responses(
        (status = 200, description = "Review page (empty list when none indexed)", body = ReviewStream),
        (status = 500, description = "Internal server error")
    )
)]
pub async fn shop_reviews_handler(
    Path(seller_id): Path<String>,
    Query(mut query): Query<ReviewStreamQuery>,
) -> Result<Json<ReviewStream>> {
    debug!("GET {SHOP_REVIEWS_ROUTE} seller_id:{seller_id}");

    query.initialize_defaults();
    let role = query
        .role
        .unwrap_or(PubkyAppReviewRole::BuyerReviewingSeller);

    match ReviewStream::get_by_subject(&seller_id, role, query.pagination).await? {
        Some(stream) => Ok(Json(stream)),
        None => Ok(Json(ReviewStream::default())),
    }
}

#[utoipa::path(
    get,
    path = SHOP_REPUTATION_ROUTE,
    description = r#"Full reputation aggregate of a subject: counts, verified counts, star average and
histogram, sub-rating averages, response/edited-late counts, and the per-attestor breakdown of verified
reviews. The aggregate is recomputed from public records and reproducible by any third party.

404 means no review is indexed for the subject in that role — the explicit "New seller" state. Clients
must render absence, never a fabricated 0.0."#,
    tag = "Marketplace",
    params(
        ("seller_id" = String, Path, description = "Subject Pubky ID (the reviewed user)"),
        ("role" = Option<PubkyAppReviewRole>, Query, description = "Review direction; defaults to buyer_reviewing_seller"),
    ),
    responses(
        (status = 200, description = "Reputation summary", body = ReputationSummary),
        (status = 404, description = "No indexed reviews for this subject and role"),
        (status = 500, description = "Internal server error")
    )
)]
pub async fn shop_reputation_handler(
    Path(seller_id): Path<String>,
    Query(query): Query<ReviewStreamQuery>,
) -> Result<Json<ReputationSummary>> {
    debug!("GET {SHOP_REPUTATION_ROUTE} seller_id:{seller_id}");

    let role = query
        .role
        .unwrap_or(PubkyAppReviewRole::BuyerReviewingSeller);

    match ReputationSummary::get_by_subject(&seller_id, role).await? {
        Some(summary) => Ok(Json(summary)),
        None => Err(Error::ReputationNotFound {
            subject_id: seller_id,
        }),
    }
}

#[utoipa::path(
    get,
    path = LISTING_REVIEWS_ROUTE,
    description = "Paged buyer reviews of one listing, newest-indexed first, with seller responses joined. \
Same verification semantics as the subject review stream.",
    tag = "Marketplace",
    params(
        ("seller_id" = String, Path, description = "Seller Pubky ID"),
        ("listing_id" = String, Path, description = "Listing Crockford32 ID"),
        ("skip" = Option<usize>, Query, description = "Skip N reviews"),
        ("limit" = Option<usize>, Query, description = "Retrieve N reviews"),
    ),
    responses(
        (status = 200, description = "Review page (empty list when none indexed)", body = ReviewStream),
        (status = 500, description = "Internal server error")
    )
)]
pub async fn listing_reviews_handler(
    Path((seller_id, listing_id)): Path<(String, String)>,
    Query(mut query): Query<ReviewStreamQuery>,
) -> Result<Json<ReviewStream>> {
    debug!("GET {LISTING_REVIEWS_ROUTE} seller_id:{seller_id}, listing_id:{listing_id}");

    query.initialize_defaults();

    match ReviewStream::get_by_listing(&seller_id, &listing_id, query.pagination).await? {
        Some(stream) => Ok(Json(stream)),
        None => Ok(Json(ReviewStream::default())),
    }
}

#[derive(OpenApi)]
#[openapi(
    paths(shop_reviews_handler, shop_reputation_handler, listing_reviews_handler),
    components(schemas(
        ReviewStream,
        ReviewView,
        ReviewDetails,
        ReviewResponseDetails,
        ReputationSummary,
        ReputationSnippet,
        PubkyAppReviewRole
    ))
)]
pub struct MarketplaceReviewsApiDoc;
