# Orders → Hourly Revenue per Product

A DataSQRL pipeline that ingests order data from Kafka in real time and serves
**hourly revenue per product** through a GraphQL / REST / MCP API.

## Pipeline

1. **Ingest** — order line items are read in real time from the Kafka topic `orders`
   (`kafka-safe` connector, event time from the Kafka record timestamp).
2. **Aggregate** — a 1-hour tumbling window sums `quantity * unit_price` per product
   (`HourlyRevenueByProduct`).
3. **Serve** — the aggregate is exposed through the API, backed by Postgres and the
   Vert.x server.

## Data model

Each order record (`connectors/sources.sqrl` → `_Orders_schema`):

| column       | type   | description                     |
|--------------|--------|---------------------------------|
| `order_id`   | STRING | order identifier                |
| `product_id` | STRING | product purchased               |
| `quantity`   | INT    | units purchased                 |
| `unit_price` | DOUBLE | price per unit                  |
| `event_time` | TIMESTAMP_LTZ(3) | Kafka record timestamp (prod) / data field (test) |

## API

- `HourlyRevenueByProduct` — query hourly revenue; optional filters `product_id`, `hour_start`
  (`revenue`, `order_count` per product per hour).
- `RevenueForProduct(product_id)` — hourly revenue for one product, ordered by hour.

## Project layout

```
orders.sqrl                     main pipeline + API
connectors/
  sources.sqrl                  shared schema (_Orders_schema)
  sources-prod.sqrl             production Kafka source (kafka-safe)
  sources-test.sqrl             test filesystem source
  test-data/Orders.jsonl        test fixtures
orders-api/tests/               API tests
orders-shared-package.json      base config (flink, postgres, vertx)
orders-test-package.json        test overlay
orders-prod-package.json        prod overlay
run-tests.sh                    test entry point
```

The main script selects the source per environment via
`IMPORT connectors.sources-{{environment}} AS sources;` driven by
`script.config.environment`.

## Build / test

```bash
# Compile for production
/opt/agent/cmd.sh compile -r . orders-shared-package.json orders-prod-package.json -b orders

# Run tests
./run-tests.sh
```

## Production configuration

Set these environment variables for the production deployment:

- `KAFKA_BOOTSTRAP_SERVERS` — Kafka broker list for the `orders` topic
- `DEPLOYMENT_ID` — consumer group id
