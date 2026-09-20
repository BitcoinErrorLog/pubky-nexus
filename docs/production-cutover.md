# Pubky Marketplace Nexus Production Cutover

Scope: this document covers the dedicated Pubky Marketplace ("Shop") Nexus fork at `BitcoinErrorLog/pubky-nexus`, branch `feat/marketplace-indexing`, HEAD `6fc2dbe3ac8cb81d137cd2fa7dd46b066b5a1adf`. The intended production homeserver is `8um71us3fyw6h8wbcxb5ar3rwusy1a6u49956ikzojg3gcwd1dty` (`https://homeserver.pubky.app`). No production homeserver requests were made while preparing this document.

## 1. Watcher Start Position

### Current behavior

On first boot, the watcher starts from cursor `0000000000000`.

The start cursor is not a CLI argument, not a Railway env var, and not present in `WatcherConfig`. It comes from the `Homeserver` model default:

- `nexus-common/src/models/homeserver.rs:32-38` defines `Homeserver::new(id)` with `cursor: "0000000000000"`.
- `nexus-watcher/src/service/mod.rs:74-77` calls `Homeserver::persist_if_unknown(config_hs)` before the event loop starts.
- `nexus-common/src/models/homeserver.rs:99-105` persists an unknown homeserver by creating `Homeserver::new(homeserver_id)`, writing it to Neo4j, then writing it to Redis.
- `nexus-common/src/models/homeserver.rs:70-81` stores the cursor in Redis through `Homeserver::put_to_index()`.
- `nexus-common/src/db/kv/traits.rs:20-36` derives the default Redis prefix from the type name, so the persisted cursor key is the `Homeserver` JSON key for the homeserver id, effectively `Homeserver:<homeserver_pubky>`.

On every poll, the watcher uses the persisted cursor:

- `nexus-watcher/src/service/processor_runner.rs:72-87` loads the `Homeserver` by id, including its Redis cursor if present.
- `nexus-watcher/src/service/processor.rs:70-73` requests `https://{homeserver_id}/events/?cursor={homeserver.cursor}&limit={limit}`.
- `nexus-watcher/src/service/processor.rs:130-135` handles `cursor: ...` lines from the homeserver by creating `Homeserver::try_from_cursor(id, cursor)` and writing that value back to Redis.
- `nexus-common/src/models/homeserver.rs:84-93` reads Redis first and falls back to the graph only when no Redis record exists; the graph fallback uses `Homeserver::new`, so it also falls back to cursor zero.

The Railway entrypoint exposes only homeserver id and replay tuning:

- `entrypoint-railway.sh:3-16` reads `NEXUS_HOMESERVER`, `NEXUS_EVENTS_LIMIT`, and `NEXUS_WATCHER_SLEEP`.
- `entrypoint-railway.sh:31-39` writes those values into `[watcher]` in `/data/config.toml`.
- `nexus-common/src/config/watcher.rs:34-50` defines `WatcherConfig`; there is no cursor, tail, or start-position field.
- `nexusd/src/cli.rs:35-68` exposes `api`, `watcher`, and `run` with only `--config-dir`; there is no cursor flag.

### Is tail-start or arbitrary cursor configurable?

No. This fork currently supports only "resume from the Redis cursor if already present; otherwise start from `0000000000000`." There is no env/config/CLI switch to start from the current tail or from a given cursor.

### Minimal change to add it

The smallest production-grade change would be:

1. Add an optional `initial_cursor: Option<String>` or `start_cursor: Option<String>` to `WatcherConfig` in `nexus-common/src/config/watcher.rs`.
2. Add `NEXUS_INITIAL_CURSOR` to `entrypoint-railway.sh` and write it into `[watcher]` only when set.
3. In `NexusWatcher::start` (`nexus-watcher/src/service/mod.rs:70-80`), before or inside `Homeserver::persist_if_unknown`, check whether `Homeserver::get_from_index(&config_hs).await?` already exists. If not, persist `Homeserver::try_from_cursor(config_hs.clone(), initial_cursor)?.put_to_index().await?` instead of letting `Homeserver::new` seed cursor zero.
4. Keep the persisted cursor key the same: the `Homeserver` Redis JSON entry keyed by the homeserver id (`Homeserver:<homeserver_pubky>`), written by `Homeserver::put_to_index()` in `nexus-common/src/models/homeserver.rs:75-81`.

"Start from current tail" still needs a reliable way to obtain the homeserver's current tail cursor. This repository does not document a tail endpoint or a special cursor value. If the homeserver supports returning a next cursor for an empty future cursor, that should be verified separately before implementing a `NEXUS_START_AT_TAIL=true` convenience flag.

## 2. What Marketplace Endpoints Need From History

### Routes this fork adds

The README states that this fork adds marketplace indexing over upstream and that the official/shared Nexus has none of these endpoints (`README.md:7-18`). The route constants are:

- Shops: `/v0/shop/{seller_id}`, `/tags`, `/taggers/{label}`, `/reviews`, `/reputation` in `nexus-webapi/src/routes/v0/endpoints.rs:31-37`.
- Listings: `/v0/listing/{seller_id}/{listing_id}`, `/tags`, `/taggers/{label}`, `/reviews` in `nexus-webapi/src/routes/v0/endpoints.rs:38-42`.
- Drops: `/v0/drop/{owner_id}/{drop_id}` in `nexus-webapi/src/routes/v0/endpoints.rs:43-44`.
- Streams: `/v0/stream/listings` and `/v0/stream/drops` in `nexus-webapi/src/routes/v0/endpoints.rs:57-60`.

`nexus-webapi/src/routes/v0/marketplace/mod.rs:16-28` registers shop, listing, drop, review, reputation, and marketplace tag routes. `nexus-webapi/src/routes/v0/stream/mod.rs:18-25` registers the listing and drop streams.

### Graph/write dependencies

Marketplace writes are not lazy about users. They require existing `User` nodes.

- Shop PUT: `nexus-watcher/src/events/handlers/shop.rs:13-20` writes the shop and returns `MissingDependency` when the owner user is absent. The Cypher in `nexus-common/src/db/graph/queries/put.rs:392-433` starts with `MATCH (owner:User {id: $owner_id})`, then creates `(:User)-[:HAS_SHOP]->(:Shop)`.
- Listing PUT: `nexus-watcher/src/events/handlers/listing.rs:17-27` returns `MissingDependency` when the seller user is absent. The Cypher in `nexus-common/src/db/graph/queries/put.rs:435-520` starts with `MATCH (seller:User {id: $owner_id})`, then creates `(:User)-[:SELLS]->(:Listing)`.
- Drop PUT: `nexus-watcher/src/events/handlers/drop.rs:15-23` returns `MissingDependency` when the seller user is absent. The Cypher in `nexus-common/src/db/graph/queries/put.rs:522-578` starts with `MATCH (owner:User {id: $owner_id})`, then creates `(:User)-[:OFFERS]->(:Drop)`.
- Review PUT: `nexus-watcher/src/events/handlers/review.rs:40-49` requires both reviewer and subject users. The Cypher in `nexus-common/src/db/graph/queries/put.rs:580-631` starts with `MATCH (reviewer:User {id: $reviewer_id})` and `MATCH (subject:User {id: $subject_id})`, then creates a `REVIEWED` edge.
- Review response PUT: `nexus-watcher/src/events/handlers/review_response.rs:31-40` requires the original review to already be indexed; it then sets a flag on that review edge in `nexus-common/src/db/graph/queries/put.rs:633-646`.
- Listing tags: `nexus-watcher/src/events/handlers/tag.rs:51-63` routes tags whose embedded URI is a listing to `put_sync_listing`; `nexus-common/src/db/graph/queries/put.rs:304-330` requires the tagger `User` and target `Listing`.
- Shop tags: `nexus-watcher/src/events/handlers/tag.rs:64-73` routes tags whose embedded URI is a shop to `put_sync_shop`; `nexus-common/src/db/graph/queries/put.rs:341-365` requires the tagger `User`, owner `User`, and target `Shop`.

The normal user profile event creates the `User` node:

- `nexus-watcher/src/events/mod.rs:57-60` dispatches `PubkyAppObject::User` on `Resource::User` to `handlers::user::sync_put`.
- `nexus-watcher/src/events/handlers/user.rs:11-20` builds `UserDetails` and writes it to the graph.
- `nexus-common/src/db/graph/queries/put.rs:15-33` uses `MERGE (u:User {id: $id})`.

Therefore, if the production watcher starts from the current tail with an empty database, sellers and reviewers whose `profile.json` events occurred before the cursor will be missing. Their pre-existing shop/listing/drop/review events will also be skipped, and new marketplace records by those users will park in the missing-dependency retry queue until a user profile PUT is seen or a backfill creates the user nodes.

### Read dependencies by endpoint

`/v0/stream/listings`

- Handler: `nexus-webapi/src/routes/v0/stream/listings.rs:76-88`.
- Query path: `ListingStream::get_listings` in `nexus-common/src/models/marketplace/stream.rs:148-160`.
- Dependencies: `ListingDetails` JSON in Redis and sorted sets (`Listings:Global:Timeline`, `Listings:Seller`, `Listings:Auctions:EndsAt`) populated by `ListingDetails::put_to_index` in `nexus-common/src/models/marketplace/listing.rs:200-213`, plus `Listing` nodes and `SELLS` relationships for graph-only filters in `nexus-common/src/db/graph/queries/get.rs:1056-1224`.
- Reputation snippets are optional Redis aggregates, batch-read in `nexus-common/src/models/marketplace/stream.rs:335-372`.
- Tail-start impact: empty for all pre-cursor listings. New listing PUTs by sellers whose `User` node is missing fail before they enter the stream.

`/v0/listing/{seller_id}/{listing_id}`

- Handler: `nexus-webapi/src/routes/v0/marketplace/listing.rs:30-51`.
- Query path: `ListingDetails::get_by_id` in `nexus-common/src/models/marketplace/listing.rs:151-168`.
- Dependencies: Redis listing details or graph fallback `MATCH (seller:User)-[:SELLS]->(listing:Listing)` in `nexus-common/src/db/graph/queries/get.rs:892-930`; optional seller and listing reputation.
- Tail-start impact: 404 for pre-cursor listings; new listings by missing sellers fail to index.

`/v0/shop/{seller_id}`

- Handler: `nexus-webapi/src/routes/v0/marketplace/shop.rs:42-53`.
- Query path: `ShopView::get_by_id` in `nexus-common/src/models/marketplace/view.rs:24-52`.
- Dependencies: `ShopDetails`, the seller's listing stream, and optional seller reputation. Graph fallback for the shop is `MATCH (owner:User)-[:HAS_SHOP]->(shop:Shop)` in `nexus-common/src/db/graph/queries/get.rs:864-890`.
- Tail-start impact: 404 for pre-cursor shops; new shop PUTs by missing sellers fail to index.

`/v0/drop/{owner_id}/{drop_id}`

- Handler: `nexus-webapi/src/routes/v0/marketplace/drop.rs:27-37`.
- Query path: `DropDetails::get_by_id` in `nexus-common/src/models/marketplace/drop.rs:100-114`.
- Dependencies: Redis drop details or graph fallback `MATCH (owner:User)-[:OFFERS]->(drop:Drop)` in `nexus-common/src/db/graph/queries/get.rs:933-961`.
- Tail-start impact: 404 for pre-cursor drops; new drops by missing sellers fail to index.

`/v0/stream/drops`

- Handler: `nexus-webapi/src/routes/v0/stream/drops.rs:52-63`.
- Query path: `DropStream::get_drops` in `nexus-common/src/models/marketplace/drop.rs:197-209`.
- Dependencies: Redis sorted sets (`Drops:Global:StartsAt`, `Drops:Owner`) written by `DropDetails::put_to_index` in `nexus-common/src/models/marketplace/drop.rs:140-147`, or graph fallback for bucket filters in `nexus-common/src/db/graph/queries/get.rs:970-1042`.
- Tail-start impact: empty for pre-cursor drops; new drops by missing sellers fail to index.

`/v0/shop/{seller_id}/reviews`

- Handler: `nexus-webapi/src/routes/v0/marketplace/reviews.rs:56-71`.
- Query path: `ReviewStream::get_by_subject` in `nexus-common/src/models/marketplace/review.rs:198-208`.
- Dependencies: Redis `Reviews:Subject:<subject_id>:<role>` sorted set, review details JSON, optional response JSON, and `REVIEWED` edges for recomputation.
- Tail-start impact: empty until reviews are indexed. Review PUT requires reviewer and subject `User` nodes.

`/v0/listing/{seller_id}/{listing_id}/reviews`

- Handler: `nexus-webapi/src/routes/v0/marketplace/reviews.rs:129-140`.
- Query path: `ReviewStream::get_by_listing` in `nexus-common/src/models/marketplace/review.rs:210-223`.
- Dependencies: Redis `Reviews:Listing:<listing_owner_id>:<listing_id>` sorted set and review details; buyer-reviewing-seller reviews only.
- Tail-start impact: empty until review records are indexed. It does not require the listing node at read time, but review ingest still requires reviewer and subject user nodes.

`/v0/shop/{seller_id}/reputation`

- Handler: `nexus-webapi/src/routes/v0/marketplace/reviews.rs:93-109`.
- Query path: `ReputationSummary::get_by_subject` in `nexus-common/src/models/marketplace/reputation.rs:70-78`.
- Dependencies: Redis `ReputationSummary:Subject:<subject_id>:<role>` recomputed from `REVIEWED` edges by `nexus-common/src/db/graph/queries/get.rs:1398-1409`.
- Tail-start impact: 404 until at least one review for that subject indexes and recomputes the aggregate.

`/v0/listing/{seller_id}/{listing_id}/tags`

- Handler: `nexus-webapi/src/routes/v0/marketplace/tags.rs:35-60`.
- Query path: `TagListing::get_by_id`.
- Dependencies: Redis listing tag sorted set and tagger sets (`nexus-common/src/models/tag/listing.rs:10-48`) or graph fallback `MATCH (l:Listing {id, owner_id})` plus incoming `TAGGED` edges in `nexus-common/src/db/graph/queries/get.rs:255-278`.
- Tail-start impact: 404 when the target listing is not indexed. A new tag for a missing listing goes to missing dependency keyed by the listing (`nexus-watcher/src/events/handlers/tag.rs:291-347`).

`/v0/listing/{seller_id}/{listing_id}/taggers/{label}`

- Handler: `nexus-webapi/src/routes/v0/marketplace/tags.rs:80-102`.
- Query path: `TaggersCollection::get_tagger_by_id` in `nexus-common/src/models/tag/traits/taggers.rs:36-59`.
- Dependencies: Redis tagger set for the listing label. `viewer_id` only checks whether the viewer is a member in this route because depth is passed as `None`.
- Tail-start impact: empty when the tag set was never populated.

`/v0/shop/{seller_id}/tags`

- Handler: `nexus-webapi/src/routes/v0/marketplace/tags.rs:122-143`.
- Query path: `TagShop::get_by_id`.
- Dependencies: Redis shop tag sorted set and tagger sets (`nexus-common/src/models/tag/shop.rs:10-47`) or graph fallback `MATCH (:User {id: $owner_id})-[:HAS_SHOP]->(s:Shop)` plus incoming `TAGGED` edges in `nexus-common/src/db/graph/queries/get.rs:280-302`.
- Tail-start impact: 404 when the shop is not indexed. A new tag for a missing shop goes to missing dependency keyed by the shop.

`/v0/shop/{seller_id}/taggers/{label}`

- Handler: `nexus-webapi/src/routes/v0/marketplace/tags.rs:164-185`.
- Query path: `TaggersCollection::get_tagger_by_id`.
- Dependencies: Redis tagger set for the shop label; no `FOLLOWS` dependency because depth is `None`.
- Tail-start impact: empty when the tag set was never populated.

### Does a marketplace PUT lazily create unknown users?

No. Marketplace PUT handlers do not create `User` nodes from `/pub/pubky.app/marketplace/v1/**` records.

They convert the record into marketplace details, attempt a graph write, and return `MissingDependency` if required users do not already exist. The retry event is then stored by `EventProcessor::handle_event` in `nexus-watcher/src/service/processor.rs:164-176`. The retry key generation is in `nexus-watcher/src/events/retry/event.rs:40-57`.

### Review backfill dependency

The review backfill depends on pre-existing indexed users.

- `nexusd/src/migrations/migrations_list/review_backfill_1787905961.rs:6-16` says it discovers reviews by listing each indexed user's reviews directory.
- `nexus-watcher/src/events/handlers/review.rs:140-147` calls `get_all_user_ids()` before scanning.
- `nexus-common/src/db/reindex.rs:123-134` defines `get_all_user_ids()` as `MATCH (u:User) RETURN u.id AS id`.

With a tail-start empty database, the review backfill has no users to scan, so it cannot recover historical reviews by itself.

## 3. Upstream Safety

There is no `upstream` remote configured in this checkout. `git merge-base HEAD upstream/main` and `git remote get-url upstream` returned no output, so I could not compare this worktree directly to upstream.

Reasoning from this fork:

- The README states this branch is a fork of official `pubky/pubky-nexus`, and that marketplace ingest/streams are "Added over upstream" (`README.md:7-18`).
- The current fork depends on `BitcoinErrorLog/pubky-app-specs` at `7d79e5e8fc61fd75a503268cdedba75038c9b4d4` (`Cargo.toml:23`), and that fork supplies marketplace resource/object variants.
- Event parsing rejects unknown resources before handler dispatch: `Event::parse_event` maps `Resource::Unknown` to `InvalidEventLine` in `nexus-common/src/models/event/mod.rs:69-80`; `process_event_lines` logs the parse error and drops the event when parsing returns no event in `nexus-watcher/src/service/processor.rs:136-140`.
- Known-but-unhandled PUT variants fall through to `other => debug!("Event type not handled, Resource: {other:?}")` in `nexus-watcher/src/events/mod.rs:104-105`; known-but-unhandled DEL variants fall through to a debug log in `nexus-watcher/src/events/mod.rs:140-141`.

Implication: marketplace writes to the production homeserver should not disturb the official production Nexus. On likely upstream code without marketplace resource variants in `pubky-app-specs`, marketplace URIs are unknown and are rejected/skipped during parsing. If upstream had the specs variants but not the marketplace handlers, they would fall through to the debug-only unhandled branch and not create marketplace graph nodes. In neither local code path is there partial `User`, `Post`, or relationship creation for marketplace records unless the explicit marketplace handler arms exist.

## 4. Replay Estimate

This repository does not provide a cheap exact production history count. The watcher uses only a cursor and limit against `/events`:

```text
GET https://<homeserver_pubky>/events/?cursor=<cursor>&limit=<limit>
```

The operator can cheaply inspect the stream shape with a very small request, for example:

```bash
curl "https://8um71us3fyw6h8wbcxb5ar3rwusy1a6u49956ikzojg3gcwd1dty/events/?cursor=0000000000000&limit=1"
```

or, if resolving through the public homeserver host is supported operationally:

```bash
curl "https://homeserver.pubky.app/events/?cursor=0000000000000&limit=1"
```

That confirms first-page behavior and the returned `cursor:` line, but it is not a count. Counting exactly requires walking pages until empty, which is effectively the replay. If the homeserver has an administrative count metric or a documented tail cursor endpoint, use that instead; it is not referenced in this repo.

Using the observed Railway staging replay throughput from `docs/railway-deploy.md:57-64` (100-160 events/minute), estimated cold replay wall time is:

| Production event count | At 160 events/min | At 100 events/min |
| ---: | ---: | ---: |
| 500,000 | ~52.1 hours / 2.2 days | ~83.3 hours / 3.5 days |
| 1,000,000 | ~104.2 hours / 4.3 days | ~166.7 hours / 6.9 days |
| 3,000,000 | ~312.5 hours / 13.0 days | ~500.0 hours / 20.8 days |

The staging note also matters qualitatively: each PUT fetches the public record, and the event stream has no path filter (`docs/railway-deploy.md:47-56`), so marketplace-only replay is not currently possible.

## 5. Recommendation

Recommendation: choose (a) full replay for the first production instance.

Rationale:

- Full replay is the only option supported by current code without introducing new behavior at cutover time.
- Marketplace graph writes require users to already exist. Tail-start with an empty DB would miss historical seller/reviewer `User` nodes and would also miss historical shop/listing/drop/review records.
- The existing review backfill cannot compensate for tail-start because it scans indexed users, and tail-start initially has no complete indexed user set.
- A targeted seller-backfill script could work, but no such script exists in this repo. It would need to discover sellers from Shop's production data source, fetch each seller profile/shop/listings/drops/reviews/tags in dependency order, and preserve the same validation paths. That is more operational risk than a slow cold replay.

### Production Railway service config

Set the production service to a fresh Neo4j volume and fresh Redis volume unless intentionally resuming an already verified replay.

Required:

```toml
NEXUS_HOMESERVER=8um71us3fyw6h8wbcxb5ar3rwusy1a6u49956ikzojg3gcwd1dty
NEXUS_TESTNET=false
NEXUS_NEO4J_URI=bolt://neo4j.railway.internal:7687
NEXUS_NEO4J_PASSWORD=<secret>
NEXUS_REDIS_URL=${{Redis.REDIS_URL}}
PORT=8080
```

Recommended replay tuning, matching the existing Railway entrypoint knobs:

```toml
NEXUS_EVENTS_LIMIT=1000
NEXUS_WATCHER_SLEEP=500
```

Homeserver URL for operator verification: `https://homeserver.pubky.app`.

Cursor flag: none exists today. Do not set an invented cursor env var. The first boot will seed Redis with cursor `0000000000000`; restarts resume from the `Homeserver:<production_homeserver_pubky>` Redis JSON cursor.

### Operational risks

- Runtime is unknown until production history size is measured. At staging speed, 1M events is roughly 4.3-6.9 days; 3M events is roughly 13.0-20.8 days.
- During replay, marketplace stream endpoints can return empty or incomplete results until the relevant user and marketplace records are reached.
- Because events are global and unfiltered, production replay will process profiles, posts, follows, files, tags, and marketplace records, not only Shop data.
- A restart should resume from Redis, but losing the Redis volume or replacing it with an empty one restarts from cursor zero.
- New marketplace records written during replay should eventually be reached in event order, but a record whose dependencies are not indexed yet can be parked in retry until its user/profile dependency exists.
- Review reputation is eventually consistent: aggregates recompute when reviews/responses index; pre-cursor review recovery via the existing migration only works after users exist.

### Proof checklist

Before pointing Shop at the production marketplace Nexus URL:

1. Health:
   - `GET https://<production-marketplace-nexus>/v0/info` returns 200.
   - Railway logs show `Processing N event lines` and `Received cursor for the next request`.
   - Redis contains the production homeserver cursor under the `Homeserver:<production_homeserver_pubky>` JSON key.
2. Empty-read readiness:
   - `GET https://<production-marketplace-nexus>/v0/stream/listings` returns 200 with an empty list or a valid listing stream, not 500.
   - `GET https://<production-marketplace-nexus>/v0/stream/drops` returns 200 with an empty list or a valid drop stream, not 500.
3. First indexed listing:
   - Publish or identify one production listing by a seller whose profile has indexed.
   - Confirm `GET /v0/listing/{seller_id}/{listing_id}` returns the listing details.
   - Confirm `GET /v0/stream/listings?seller_id={seller_id}` includes that listing.
   - Confirm Shop with `PUBKY_RUNTIME_MARKETPLACE_NEXUS_URL=<production-marketplace-nexus-url>` renders the listing while social reads continue to use the official Nexus.
4. Review/reputation:
   - After a review is published, confirm `GET /v0/shop/{seller_id}/reviews` includes it.
   - Confirm `GET /v0/shop/{seller_id}/reputation` returns 200 when at least one review exists, or 404 for an honestly new seller with no reviews.
5. Backfill migrations, if needed after replay:
   - Run `ListingAuctionTermsReindex1787256279` only if auction rows missing term fields are detected.
   - Run `ReviewBackfill1787905961` only after a substantial user set is indexed; it scans `MATCH (u:User)` and cannot discover users from an empty graph.

## Verification That Differed From The Brief

- No `upstream` remote exists in this checkout, so direct `git merge-base HEAD upstream/main` comparison could not be performed. The upstream-safety conclusion is reasoned from this fork's README, current dependency fork, and event dispatch/fallback code.
- I did not make any production homeserver requests. The replay-size request examples are operator instructions only.
- I did not find a tracked `status.md` file in this repo. The review backfill behavior was verified from `docs/railway-deploy.md`, `README.md`, `nexusd/src/migrations/migrations_list/review_backfill_1787905961.rs`, `nexus-watcher/src/events/handlers/review.rs`, and `nexus-common/src/db/reindex.rs`.
- Only `docs/production-cutover.md` was created; no other files were modified.
