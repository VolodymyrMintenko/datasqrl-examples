# Orders Revenue Pipeline

A DataSQRL streaming pipeline that ingests order events from Kafka, deduplicates
producer retries, and serves hourly revenue per product plus a deduplicated-order
lookup through a read-only GraphQL/REST/MCP API.

## Motivation and requirements

Merchandising and finance need near-real-time visibility into revenue per product per
hour, computed from a single source: the `orders` Kafka topic. The source is
at-least-once (producer retries resend the same `order_id`), JSON without a schema
registry, and carries no envelope. The full requirements are recorded in
`adr/requirements_20260930-135454.md` and the implementation plan in
`adr/plan_20261002-094302.md`; the key functional points are:

- Ingest `orders` continuously; an order is visible through the `Orders` lookup within
  seconds of being produced (no windowed wait on that path).
- Deduplicate retried order events on `order_id`, keeping the copy with the latest
  `ordered_at`.
- Flatten each order's `items` into order lines and compute `revenue = quantity *
  unit_price` per line.
- Aggregate `revenue`, `units` (`sum(quantity)`), and `orders` (`count(distinct
  order_id)`) per `product_id` per 1-hour tumbling window on `ordered_at`.
- Tolerate up to 5 minutes of lateness: an order that arrives within 5 minutes of the
  current watermark still updates its hour; a later order is dropped from the
  aggregate but remains visible through `Orders`.
- Never expose `customer_id` (pseudonymous PII) through the API.
- No mutations, no subscriptions — the API is read-only; the Kafka topic is an
  **external** connector source, not a DataSQRL-managed internal table.

Out of scope for this iteration (see `adr/requirements_20260930-135454.md` for the
full list): refunds/cancellations, currency conversion, customer/basket analytics,
historical backfill, and any write path.

## Architecture and implementation choices

**Pipeline stages** (`orders_revenue.sqrl`):

1. **Ingest** — `connectors/sources-prod.sqrl` declares `Orders` as an external
   `kafka-safe` source over the `orders` topic (`flexible-json`, ISO-8601 timestamps),
   watermarked on the payload's `ordered_at` with 5 minutes of out-of-orderness. That
   watermark is what implements the lateness tolerance (TS-2/FR-5/FR-6) — no separate
   filter is needed. `connectors/sources-test.sqrl` mirrors this exactly over a static
   JSONL file. The shared payload schema lives in `connectors/sources.sqrl`
   (`_Orders_schema`), with no connector or watermark details (those differ per
   environment).
2. **Dedup, served** — `_DedupedOrders` is a `DISTINCT ... ORDER BY ordered_at DESC`
   over the raw source, giving one row per `order_id` with the latest `ordered_at`
   (FR-2). Retained 30 days (NFR-3). Served through the `Orders` API function.
3. **Dedup, append-only** — `_FirstOrders` is a first-row (`ROW_NUMBER() = 1`, ordered
   ascending) dedup of the same key. The hourly aggregate's window table-valued
   function requires an insert-only input, which the latest-wins `DISTINCT` cannot
   provide (it emits retractions), so the revenue path uses this separate,
   append-only dedup instead. Retries are assumed to repeat the same order lines (the
   source describes immutable events), so both dedup forms are expected to agree on
   revenue-relevant fields.
4. **Order lines** — `_OrderLineAll` flattens `items` via `UNNEST`, computing
   `revenue = quantity * unit_price` per line. Lines with non-positive `quantity` or
   negative `unit_price` are excluded from the revenue path (`_OrderLine`) and
   captured instead by `DQ_OrderLine_Invalid_Amount`.
5. **Hourly aggregate** — `_HourlyProductRevenue` is a 1-hour `TUMBLE` on `ordered_at`,
   grouped by `product_id`, with `sum(revenue)`, `sum(quantity)` as `units`, and
   `count(distinct order_id)` as `orders`. Hour boundaries are UTC. Retained 395 days
   (~13 months, NFR-3 — the `ttl` hint's largest supported unit is days). A descending
   `(product_id, hour)` index matches the only query pattern (FR-4).

**API** (`compiler.api.protocols`: GraphQL, REST, MCP):

- `HourlyProductRevenue(product_id: String, from_hour: DateTime, to_hour: DateTime)` —
  optional filters, applied only when supplied; `from_hour`/`to_hour` bound `hour`
  inclusively. Ordered by `hour` descending, then `product_id` ascending. The `limit`
  parameter from the requirements is served by DataSQRL's generated `limit`/`offset`
  query arguments, not a declared function argument (it would collide with the
  generated one).

  ```graphql
  query { HourlyProductRevenue(product_id: "p-3391") { product_id hour revenue units orders } }
  ```

  This function is a **passthrough** definition executing literal Postgres SQL. Reason:
  the hourly aggregate stores `hour` as a plain `TIMESTAMP` (no time zone) so it always
  displays as the correct UTC wall-clock value, but the incoming `from_hour`/`to_hour`
  arguments arrive as zone-aware instants. Comparing the two needs an *explicit*
  `timezone('UTC', hour)` conversion — a plain `CAST` between the two types converts
  through the serving connection's session time zone and silently shifts the bound.
  The passthrough form is what lets this exact Postgres expression be written.

- `Orders(order_id: String)` — optional filter to a single order; omitted, returns
  every deduplicated order newest first. Returns `order_id`, `ordered_at`, `currency`,
  `items`; **never** `customer_id` (NFR-4) — it exists on the ingested table as a
  hidden column only.

  ```graphql
  query { Orders(order_id: "O-008") { order_id ordered_at currency items { product_id quantity unit_price } } }
  ```

Intermediate tables (`_DedupedOrders`, `_FirstOrders`, `_OrderLineAll`, `_OrderLine`,
`_HourlyProductRevenue`) are `_`-prefixed and absent from the API.

**Engines**: `flink` (stream processing), `postgres` (serves the two query endpoints
plus the DQ/OB tables), `vertx` (GraphQL/REST/MCP server). No `kafka` engine — the
topic is an external connector, not a DataSQRL-managed one — and no Iceberg, since 13
months of hourly rows per product fits comfortably in Postgres.

**Environment/package layout**:

| File | Role |
|---|---|
| `orders_revenue-shared-package.json` | base: engines, `script.main`, API protocols, `table.local-time-zone: UTC` |
| `orders_revenue-test-package.json` | test overlay: `environment=test`, fast source idle timeout, test-runner block |
| `orders_revenue-prod-package.json` | prod overlay: `environment=prod`, Flink state TTL (30 days, bounding dedup state), deployment sizing |
| `connectors/sources.sqrl` | shared payload schema (`_Orders_schema`) |
| `connectors/sources-prod.sqrl` | external `kafka-safe` connector over topic `orders` |
| `connectors/sources-test.sqrl` | filesystem connector over `connectors/test-data/orders.jsonl` |

## Event-time and lateness semantics

- Event-time column: `ordered_at` (RFC3339 with milliseconds, UTC).
- Watermark: `ordered_at - INTERVAL '5' MINUTE`, declared identically in the prod and
  test connectors.
- An order whose `ordered_at` lags the current watermark by **5 minutes or less**
  still updates its hour in `HourlyProductRevenue` (FR-5) — demonstrated by test
  record `O-007` (2 minutes late).
- An order that lags **more than 5 minutes** is dropped by the window operator — its
  revenue and units are excluded from every hour — but it **remains visible** through
  `Orders` (FR-6) — demonstrated by test record `O-008` (20 minutes late).
- Because of this 5-minute tolerance, a product-hour in `HourlyProductRevenue` only
  becomes stable ~5 minutes after the hour boundary passes, while `Orders` reflects a
  new order as soon as it is processed (checkpoint-bounded, not window-bounded). The
  30-second freshness requirement (NFR-1/FR-1) is met by the `Orders` path.

## Retention

- Deduplicated orders (`_DedupedOrders`): 30 days (NFR-3).
- Hourly revenue aggregate (`_HourlyProductRevenue`): 395 days (~13 months, NFR-3).
- The production overlay additionally bounds the Flink dedup operator's state to 30
  days (`table.exec.state.ttl`) so state does not grow unbounded at ~3.5M orders/day.

## PII

`customer_id` is pseudonymous and ingested (for possible future customer analytics)
but is **never** projected into any API-facing table, test snapshot, or DQ/OB table.

## Data quality and observability

- **`DQ_OrderLine_Invalid_Amount`** — one row is one order line with `quantity <= 0`
  or `unit_price < 0`, excluded from the revenue path. A non-empty table means some
  line would otherwise have silently distorted a product-hour sum; merchandising
  operations reconciles the row with the producing service.
- **`DQ_Orders_NonUSD_Currency`** — one row is one deduplicated order whose `currency`
  is present and not `USD`. Revenue is summed assuming every order is USD (currency
  conversion is out of scope); a non-empty table means that assumption is false and
  every affected product-hour is silently wrong. An **empty** table (the expected
  steady state) confirms the assumption holds.
- **`OB_OrderIngestionStats`** — one row is one hour of `ordered_at`, counting raw
  received records and distinct `order_id` values before dedup and before the revenue
  filters. The gap between the two counts is the duplicate rate of the at-least-once
  topic. A **missing** hour means ingestion stopped while the aggregate quietly keeps
  serving the last complete hours.

## Running the tests

```bash
./run-tests.sh --list-invocations   # show what would run
./run-tests.sh                      # compile + test the orders_revenue sub-project (env=test)
./run-tests.sh --compile --env prod # verify the production configuration compiles
```

The first run of a new or changed `/*+test */` table or API test creates its snapshot
and reports `BUILD FAILURE` by design (`Snapshot created ... Rerun to verify.`);
re-running the same command compares against the created snapshot and reports
`BUILD SUCCESS`. Snapshots live under `snapshots/orders_revenue/`; read and validate a
newly created snapshot before trusting it (see `connectors/test-data/orders.jsonl`,
which encodes the full set of edge cases — duplicate orders, guest checkout, a
multi-line order, a missing `currency`, on-time and late arrivals, an invalid line
amount, and a non-USD order — and the resulting hourly aggregate is the test oracle).

CI runs the same script on every push (`.github/workflows/build.yml`).

## Running the pipeline locally (outside the code agent)

```bash
docker run -it --rm -p 8888:8888 -p 8081:8081 -v $PWD:/build datasqrl/cmd run \
  orders_revenue-shared-package.json orders_revenue-test-package.json
```

- Flink WebUI: http://localhost:8081/
- GraphQL (GraphiQL): http://localhost:8888/v1/graphiql/
- REST (Swagger UI): http://localhost:8888/v1/swagger-ui
- OpenAPI JSON: http://localhost:8888/v1/openapi
- MCP: http://localhost:8888/v1/mcp/
