use super::utils::{
    attestor_pubky, sign_attestation, test_attestation_claims, test_attestor_key, test_listing,
    test_review, test_review_response, test_shop,
};
use crate::event_processor::utils::watcher::WatcherTest;
use anyhow::Result;
use ed25519_dalek::SigningKey;
use nexus_common::db::kv::SortOrder;
use nexus_common::models::marketplace::{
    ListingStream, ListingStreamFilters, ListingStreamSorting, ReputationSummary, ReviewDetails,
    ReviewResponseDetails, ReviewStream, ShopView,
};
use nexus_common::types::Pagination;
use pubky::Keypair;
use pubky_app_specs::{
    traits::HasPath, PubkyAppListingCondition, PubkyAppReviewRole, PubkyAppShop, PubkyAppUser,
    PubkyId,
};

fn test_user(name: &str) -> PubkyAppUser {
    PubkyAppUser {
        bio: Some("marketplace review watcher test".to_string()),
        image: None,
        links: None,
        name: name.to_string(),
        status: None,
    }
}

fn seller_filters(seller_id: &str) -> ListingStreamFilters {
    ListingStreamFilters {
        seller_id: Some(seller_id.to_string()),
        ..Default::default()
    }
}

async fn seller_stream(seller_id: &str) -> Option<ListingStream> {
    ListingStream::get_listings(
        seller_filters(seller_id),
        Pagination::default(),
        SortOrder::Descending,
        ListingStreamSorting::Timeline,
    )
    .await
    .unwrap()
}

#[tokio_shared_rt::test(shared)]
async fn test_homeserver_review_lifecycle() -> Result<()> {
    let mut test = WatcherTest::setup().await?;

    // Cast: a seller with a shop and a listing, and two buyers.
    let seller_kp = Keypair::random();
    let seller_id = test
        .create_user(&seller_kp, &test_user("Watcher:Review:Seller"))
        .await?;
    let shop = test_shop(&seller_id);
    let shop_path: pubky::ResourcePath = PubkyAppShop::create_path().parse()?;
    test.put(&seller_kp, &shop_path, &shop).await?;
    let listing = test_listing(
        &seller_id,
        "Reviewed turntable",
        "electronics",
        PubkyAppListingCondition::Excellent,
        50_000,
    );
    let (listing_id, _listing_path) = test.create_listing(&seller_kp, &listing).await?;

    let buyer1_kp = Keypair::random();
    let buyer1_id = test
        .create_user(&buyer1_kp, &test_user("Watcher:Review:Buyer1"))
        .await?;
    let buyer2_kp = Keypair::random();
    let buyer2_id = test
        .create_user(&buyer2_kp, &test_user("Watcher:Review:Buyer2"))
        .await?;

    let attestor = test_attestor_key();
    let attestor_id = attestor_pubky(&attestor);

    // Step 1: buyer1 publishes a review carrying a VALID attestation:
    // honest key, claims bound to exactly this (reviewer, subject, listing).
    let claims = test_attestation_claims(
        &attestor_id,
        &buyer1_id,
        &seller_id,
        &seller_id,
        &listing_id,
        'a',
    );
    let jws = sign_attestation(&claims, &attestor);
    let review1 = test_review(
        &buyer1_id,
        &seller_id,
        &seller_id,
        &listing_id,
        5,
        "Flawless deck, shipped fast.",
        &jws,
    );
    let (review1_id, review1_path) = test.create_review(&buyer1_kp, &review1).await?;

    // INDEX_OP: the review details carry the verification verdict
    let indexed = ReviewDetails::get_from_index(&buyer1_id, &review1_id)
        .await
        .unwrap()
        .expect("The verified review was not indexed");
    assert!(
        indexed.verified,
        "A valid attestation must verify at ingest"
    );
    assert_eq!(indexed.attestor_id.as_deref(), Some(attestor_id.as_str()));
    assert_eq!(
        indexed.order_ref.as_deref(),
        Some(claims.order_ref.as_str())
    );
    assert_eq!(indexed.rating_overall, 5);
    assert_eq!(indexed.subject_id, seller_id);
    assert_eq!(indexed.listing_id, listing_id);
    assert!(!indexed.edited_late);

    // STREAM_OP: subject and listing review streams serve the review
    let stream = ReviewStream::get_by_subject(
        &seller_id,
        PubkyAppReviewRole::BuyerReviewingSeller,
        Pagination::default(),
    )
    .await
    .unwrap()
    .expect("The subject review stream should not be empty");
    assert_eq!(stream.0.len(), 1);
    assert_eq!(stream.0[0].review.review_id, review1_id);
    assert!(stream.0[0].response.is_none());

    let listing_stream_reviews =
        ReviewStream::get_by_listing(&seller_id, &listing_id, Pagination::default())
            .await
            .unwrap()
            .expect("The listing review stream should not be empty");
    assert_eq!(listing_stream_reviews.0.len(), 1);

    // AGGREGATE_OP: subject and listing summaries carry the verified basis
    let summary =
        ReputationSummary::get_by_subject(&seller_id, PubkyAppReviewRole::BuyerReviewingSeller)
            .await
            .unwrap()
            .expect("The subject reputation summary should exist");
    assert_eq!(summary.count, 1);
    assert_eq!(summary.verified_count, 1);
    assert_eq!(summary.avg, 5.0);
    assert_eq!(summary.histogram, [0, 0, 0, 0, 1]);
    assert_eq!(summary.attestors.get(&attestor_id), Some(&1));
    assert_eq!(summary.response_count, 0);

    let listing_summary = ReputationSummary::get_by_listing(&seller_id, &listing_id)
        .await
        .unwrap()
        .expect("The listing reputation summary should exist");
    assert_eq!(listing_summary.count, 1);
    assert_eq!(listing_summary.verified_count, 1);
    assert_eq!(listing_summary.avg, 5.0);

    // CARD_OP: the listing stream projection embeds both compact snippets —
    // cards render stars with zero additional requests
    let stream = seller_stream(&seller_id)
        .await
        .expect("The seller listing stream should not be empty");
    let entry = &stream.0[0];
    let seller_snippet = entry
        .reputation
        .as_ref()
        .expect("The stream entry should embed the seller reputation");
    assert_eq!(seller_snippet.count, 1);
    assert_eq!(seller_snippet.verified_count, 1);
    assert_eq!(seller_snippet.avg, 5.0);
    let listing_snippet = entry
        .listing_reputation
        .as_ref()
        .expect("The stream entry should embed the listing reputation");
    assert_eq!(listing_snippet.count, 1);

    // The shop view carries the same compact seller reputation
    let shop_view = ShopView::get_by_id(&seller_id, Pagination::default())
        .await
        .unwrap()
        .expect("The shop view should exist");
    let shop_snippet = shop_view
        .reputation
        .expect("The shop view should embed the seller reputation");
    assert_eq!(shop_snippet.count, 1);
    assert_eq!(shop_snippet.verified_count, 1);

    // Step 2: buyer2 publishes a review with a FORGED attestation — honest
    // claims naming the real attestor, but signed by a different key. It is
    // indexed unverified and labeled (ratified D5), never rejected.
    let forger = SigningKey::from_bytes(&[9u8; 32]);
    let forged_claims = test_attestation_claims(
        &attestor_id,
        &buyer2_id,
        &seller_id,
        &seller_id,
        &listing_id,
        'b',
    );
    let forged_jws = sign_attestation(&forged_claims, &forger);
    let review2 = test_review(
        &buyer2_id,
        &seller_id,
        &seller_id,
        &listing_id,
        3,
        "Decent, but the tonearm was scuffed.",
        &forged_jws,
    );
    let (review2_id, review2_path) = test.create_review(&buyer2_kp, &review2).await?;

    let indexed2 = ReviewDetails::get_from_index(&buyer2_id, &review2_id)
        .await
        .unwrap()
        .expect("The unverified review was not indexed");
    assert!(
        !indexed2.verified,
        "A forged signature must index as unverified"
    );
    assert!(indexed2.attestor_id.is_none());
    assert!(indexed2.order_ref.is_none());

    let summary =
        ReputationSummary::get_by_subject(&seller_id, PubkyAppReviewRole::BuyerReviewingSeller)
            .await
            .unwrap()
            .expect("The subject reputation summary should exist");
    assert_eq!(summary.count, 2);
    assert_eq!(
        summary.verified_count, 1,
        "Only the valid attestation counts"
    );
    assert_eq!(summary.avg, 4.0);
    assert_eq!(summary.histogram, [0, 0, 1, 0, 1]);

    // Step 3: buyer1 revises their review (one living review per listing —
    // ratified D1) far outside the 24h window: rating drops, no stream
    // duplicate, and the record is flagged edited_late.
    let mut revised = review1.clone();
    revised.review_id = review1_id.clone();
    revised.ratings.overall = 1;
    revised.text = "Died after a week; seller went silent.".to_string();
    revised.revision = 2;
    revised.updated_at = "2026-08-23T12:00:00Z".to_string();
    test.put(&buyer1_kp, &review1_path, &revised).await?;

    let indexed = ReviewDetails::get_from_index(&buyer1_id, &review1_id)
        .await
        .unwrap()
        .expect("The revised review was not indexed");
    assert_eq!(indexed.rating_overall, 1);
    assert_eq!(indexed.revision, 2);
    assert!(
        indexed.verified,
        "The attestation attests the purchase and survives revisions"
    );
    assert!(indexed.edited_late, "An edit beyond 24h must be flagged");

    let stream = ReviewStream::get_by_subject(
        &seller_id,
        PubkyAppReviewRole::BuyerReviewingSeller,
        Pagination::default(),
    )
    .await
    .unwrap()
    .expect("The subject review stream should not be empty");
    assert_eq!(stream.0.len(), 2, "An edit must not duplicate the review");

    let summary =
        ReputationSummary::get_by_subject(&seller_id, PubkyAppReviewRole::BuyerReviewingSeller)
            .await
            .unwrap()
            .expect("The subject reputation summary should exist");
    assert_eq!(summary.count, 2);
    assert_eq!(summary.avg, 2.0);
    assert_eq!(summary.histogram, [1, 0, 1, 0, 0]);
    assert_eq!(summary.edited_late_count, 1);

    // Step 4: the seller (the review's subject) responds — subject-only,
    // one revisable response (ratified D7), joined into the review stream.
    let response = test_review_response(
        &seller_id,
        &buyer1_id,
        &review1_id,
        "Sorry — a replacement unit is on its way.",
    );
    let response_path = test.create_review_response(&seller_kp, &response).await?;

    let indexed_response = ReviewResponseDetails::get_from_index(&seller_id, &review1_id)
        .await
        .unwrap()
        .expect("The seller response was not indexed");
    assert_eq!(indexed_response.responder_id, seller_id);
    assert_eq!(indexed_response.reviewer_id, buyer1_id);

    let stream = ReviewStream::get_by_subject(
        &seller_id,
        PubkyAppReviewRole::BuyerReviewingSeller,
        Pagination::default(),
    )
    .await
    .unwrap()
    .expect("The subject review stream should not be empty");
    let responded = stream
        .0
        .iter()
        .find(|view| view.review.review_id == review1_id)
        .expect("review1 should be in the stream");
    assert!(
        responded.response.is_some(),
        "The subject response should join its review"
    );

    let summary =
        ReputationSummary::get_by_subject(&seller_id, PubkyAppReviewRole::BuyerReviewingSeller)
            .await
            .unwrap()
            .expect("The subject reputation summary should exist");
    assert_eq!(summary.response_count, 1);

    // Step 5: an impostor response — buyer2 is not the review's subject.
    // Structural authorization rejects it without indexing anything.
    let impostor_response = test_review_response(
        &buyer2_id,
        &buyer1_id,
        &review1_id,
        "I am definitely the seller, trust me.",
    );
    let impostor_result = nexus_watcher::events::handlers::review_response::sync_put(
        impostor_response,
        PubkyId::try_from(buyer2_id.as_str()).unwrap(),
        review1_id.clone(),
    )
    .await;
    assert!(
        impostor_result.is_err(),
        "A response whose owner is not the review subject must be rejected"
    );
    assert!(
        ReviewResponseDetails::get_from_index(&buyer2_id, &review1_id)
            .await
            .unwrap()
            .is_none(),
        "The impostor response must not be indexed"
    );

    // Step 6: the seller deletes their response; the join and the count go.
    test.del(&seller_kp, &response_path).await?;
    assert!(
        ReviewResponseDetails::get_from_index(&seller_id, &review1_id)
            .await
            .unwrap()
            .is_none(),
        "The deleted response should leave the index"
    );
    let summary =
        ReputationSummary::get_by_subject(&seller_id, PubkyAppReviewRole::BuyerReviewingSeller)
            .await
            .unwrap()
            .expect("The subject reputation summary should exist");
    assert_eq!(summary.response_count, 0);

    // Step 7: deleting reviews walks the aggregates down to honest absence.
    test.del(&buyer2_kp, &review2_path).await?;
    let summary =
        ReputationSummary::get_by_subject(&seller_id, PubkyAppReviewRole::BuyerReviewingSeller)
            .await
            .unwrap()
            .expect("The subject reputation summary should exist");
    assert_eq!(summary.count, 1);
    assert_eq!(summary.verified_count, 1);

    test.del(&buyer1_kp, &review1_path).await?;
    assert!(
        ReputationSummary::get_by_subject(&seller_id, PubkyAppReviewRole::BuyerReviewingSeller)
            .await
            .unwrap()
            .is_none(),
        "The last deleted review must remove the aggregate, not zero it"
    );
    assert!(
        ReputationSummary::get_by_listing(&seller_id, &listing_id)
            .await
            .unwrap()
            .is_none(),
        "The listing aggregate must be removed too"
    );
    assert!(ReviewStream::get_by_subject(
        &seller_id,
        PubkyAppReviewRole::BuyerReviewingSeller,
        Pagination::default(),
    )
    .await
    .unwrap()
    .is_none());

    // The stream projection returns to honest absence — no reputation
    // object at all, never a fabricated 0.0
    let stream = seller_stream(&seller_id)
        .await
        .expect("The seller listing stream should not be empty");
    assert!(stream.0[0].reputation.is_none());
    assert!(stream.0[0].listing_reputation.is_none());

    // Cleanup
    test.cleanup_user(&seller_kp).await?;
    test.cleanup_user(&buyer1_kp).await?;
    test.cleanup_user(&buyer2_kp).await?;

    Ok(())
}

#[tokio_shared_rt::test(shared)]
async fn test_review_backfill_indexes_pre_cursor_reviews() -> Result<()> {
    let mut test = WatcherTest::setup().await?;

    // Cast indexed through the normal pipeline: seller, listing, buyer.
    let seller_kp = Keypair::random();
    let seller_id = test
        .create_user(&seller_kp, &test_user("Watcher:ReviewBackfill:Seller"))
        .await?;
    let listing = test_listing(
        &seller_id,
        "Backfilled reviewed radio",
        "electronics",
        PubkyAppListingCondition::Good,
        20_000,
    );
    let (listing_id, _listing_path) = test.create_listing(&seller_kp, &listing).await?;
    let buyer_kp = Keypair::random();
    let buyer_id = test
        .create_user(&buyer_kp, &test_user("Watcher:ReviewBackfill:Buyer"))
        .await?;

    // Publish a VALID attested review with event processing DISABLED: the
    // record exists on the homeserver but the watcher never saw its event —
    // exactly the shape of a review published before the deployed replay
    // cursor.
    let attestor = test_attestor_key();
    let attestor_id = attestor_pubky(&attestor);
    let claims = test_attestation_claims(
        &attestor_id,
        &buyer_id,
        &seller_id,
        &seller_id,
        &listing_id,
        'f',
    );
    let jws = sign_attestation(&claims, &attestor);
    let review = test_review(
        &buyer_id,
        &seller_id,
        &seller_id,
        &listing_id,
        4,
        "Backfilled: solid radio, honest seller.",
        &jws,
    );
    let mut test = test.remove_event_processing().await;
    let (review_id, _review_path) = test.create_review(&buyer_kp, &review).await?;

    assert!(
        ReviewDetails::get_from_index(&buyer_id, &review_id)
            .await
            .unwrap()
            .is_none(),
        "The pre-cursor review must start unindexed"
    );

    // The backfill discovers it from the buyer's homeserver directory and
    // runs the normal ingest. (The per-user entry point: the global
    // migration is this same pass looped over get_all_user_ids, and a
    // global scan cannot run concurrently with the other watcher tests'
    // deletions without racing them.)
    let summary = nexus_watcher::events::handlers::review::backfill_reviews_for_user(&buyer_id)
        .await
        .unwrap();
    assert!(
        summary.indexed >= 1,
        "The backfill should have indexed the pre-cursor review, got {summary:?}"
    );

    let indexed = ReviewDetails::get_from_index(&buyer_id, &review_id)
        .await
        .unwrap()
        .expect("The backfilled review should be indexed");
    assert!(
        indexed.verified,
        "The attestation must verify during the backfill ingest"
    );
    assert_eq!(indexed.rating_overall, 4);
    assert_eq!(indexed.subject_id, seller_id);

    // Reputation was recomputed from the backfilled review.
    let summary_row =
        ReputationSummary::get_by_subject(&seller_id, PubkyAppReviewRole::BuyerReviewingSeller)
            .await
            .unwrap()
            .expect("The subject reputation summary should exist after the backfill");
    assert!(summary_row.count >= 1);

    // A second pass skips it by id without refetching.
    let second = nexus_watcher::events::handlers::review::backfill_reviews_for_user(&buyer_id)
        .await
        .unwrap();
    assert_eq!(
        second.indexed, 0,
        "The re-run must not re-ingest, got {second:?}"
    );
    assert!(
        second.already_indexed >= 1,
        "The re-run should count the review as already indexed, got {second:?}"
    );
    assert!(
        ReviewDetails::get_from_index(&buyer_id, &review_id)
            .await
            .unwrap()
            .is_some(),
        "The review must survive the idempotent re-run"
    );

    Ok(())
}
