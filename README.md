# camunda-db-poller

> **Community MVP** — production-usable for PostgreSQL, MySQL, and H2. Oracle, SQL Server, and SQLite are supported (unit-tested dialect logic; not yet integration-tested against real instances) — see [Supported Dialects](#supported-dialects).

> For a full walkthrough — build, deploy to Camunda 8 SaaS, model a process, and watch a row flow end-to-end — see [DEPLOYMENT.md](DEPLOYMENT.md).

## Why

Camunda's ecosystem already ships outbound connectors that execute DML and stored procedures, but there is no first-class inbound connector that turns database rows into BPMN messages. Teams often resort to custom Java delegates or fragile Spring Batch jobs that are tightly coupled to the workflow engine. This connector fills that gap: configure a SELECT query, pick a watermark column, and every new or updated row automatically starts or continues a process instance — without a line of glue code.

## Polling Loop

```
┌─────────────────────────────────────────────────────────┐
│  Connector Runtime                                      │
│                                                         │
│  activate()                                             │
│      │                                                  │
│      ▼                                                  │
│  ┌───────────────────────────────────────────────────┐  │
│  │  Daemon Thread  "db-poller-<processId>"           │  │
│  │                                                   │  │
│  │  ┌─────────────────────────────────────────────┐  │  │
│  │  │  pollOnce()                                 │  │  │
│  │  │   1. read watermark  (InMemoryWatermarkStore)│  │  │
│  │  │   2. build SQL       (replace placeholder)  │  │  │
│  │  │   3. PreparedStatement.setXxx(watermark)    │  │  │
│  │  │   4. executeQuery → iterate ResultSet       │  │  │
│  │  │   5. correlateWithResult(RowPayload)  ──────┼──┼──┼──► Zeebe
│  │  │   6. write highestWatermarkSeen             │  │  │
│  │  └─────────────────────────────────────────────┘  │  │
│  │           ▲                                        │  │
│  │           │  fixed delay (pollingIntervalSeconds)  │  │
│  │           └────────────────────────────────────────┘  │
│  └───────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────┘
```

## Payload Shape

Every row is delivered to Zeebe as a `RowPayload`:

```json
{
  "row": {
    "id": 42,
    "status": "PENDING",
    "updated_at": "2024-01-15T10:30:00Z",
    "amount": 199.99
  },
  "metadata": {
    "dialect":    "POSTGRES",
    "watermark":  "2024-01-15T10:30:00Z",
    "polledAt":   "2024-01-15T10:30:01.123Z",
    "instanceId": "order-fulfillment"
  }
}
```

## Quick Start

### 1. Build

Requires JDK 17 (`java.version`/`maven.compiler.source`/`target` in `pom.xml`).

```bash
mvn -q clean package
```

This produces `target/camunda-db-poller-1.0.0-SNAPSHOT-with-dependencies.jar`.

`connector-core` is intentionally **not** `provided` scope: the `connectors-bundle`
image loads custom connector jars through an isolated classloader for the
Java `ServiceLoader`/SPI lookup, so the jar must carry its own copy of the
`io.camunda.connector.api.*` interfaces to link correctly. `protobuf-java` is
excluded from both `connector-core` and `mysql-connector-j` because it
transitively pulls an older version that conflicts with the protobuf runtime
already bundled in the image (`Detected incompatible Protobuf Gencode/Runtime
versions`). `jackson-databind`/`jackson-datatype-jsr310`/`slf4j-api` stay
`provided`, since bundling duplicates of those specifically breaks Spring
Boot's logging bootstrap and the client's JSON mapper.

### 2. Run with the Connector Runtime

Create a `.env` file (not committed — see `.gitignore`) with your Camunda 8
SaaS credentials, using the property names the Connector Runtime's Spring
Boot config actually binds to (`camunda.client.*`, relaxed-bound from
`CAMUNDA_CLIENT_*` env vars — see the [Spring Boot Starter config
docs](https://docs.camunda.io/docs/apis-tools/camunda-spring-boot-starter/configuration/)):

```
CAMUNDA_CLIENT_MODE=saas
CAMUNDA_CLIENT_CLOUD_REGION=<region-id>
CAMUNDA_CLIENT_CLOUD_CLUSTERID=<cluster-id>
CAMUNDA_CLIENT_AUTH_CLIENTID=<client-id>
CAMUNDA_CLIENT_AUTH_CLIENTSECRET=<client-secret>
CAMUNDA_CLIENT_AUTH_TOKENURL=https://login.cloud.camunda.io/oauth/token
CAMUNDA_CLIENT_AUTH_AUDIENCE=zeebe.camunda.io
```

```bash
docker run -d --name camunda-db-poller \
  --env-file .env \
  -v "$(pwd)/target/camunda-db-poller-1.0.0-SNAPSHOT-with-dependencies.jar:/opt/app/camunda-db-poller.jar" \
  camunda/connectors-bundle:latest
```

Verify it authenticated and connected:

```bash
docker exec camunda-db-poller wget -qO- http://localhost:8080/actuator/health/liveness
```

A healthy connection reports `"zeebeClient":{"status":"UP", ...}`.

> Note: the runtime's background "process definition import" job (used to
> discover deployed BPMN processes that reference this connector) may log
> repeated 404s if it derives the wrong REST endpoint for your cluster/region
> or your cluster runs an older Camunda version than the runtime image. This
> does not affect the gRPC connection above; the connector's actual message
> correlation happens over gRPC once a process using the element template is
> deployed.

### 3. Import the Element Template

1. Open Camunda Modeler.
2. **File → Import Element Template…** → select `element-templates/db-poller-inbound-connector.json`.
3. Place a **Message Start Event** or **Intermediate Catch Event** on the canvas.
4. Select the element, open the **Properties Panel → Template**, and pick **Database Polling Connector**.

### 4. Sample Configuration

| Property              | Example value                                                                |
|-----------------------|------------------------------------------------------------------------------|
| JDBC URL              | `jdbc:postgresql://db.example.com:5432/orders`                               |
| Username              | `camunda_poller`                                                             |
| Password              | `*****`                                                                      |
| Polling Query         | `SELECT * FROM orders WHERE updated_at > :lastWatermark ORDER BY updated_at` |
| Watermark Column      | `updated_at`                                                                 |
| Watermark Type        | `TIMESTAMP`                                                                  |
| Initial Watermark     | `1970-01-01T00:00:00Z`                                                       |
| Polling Interval (s)  | `30`                                                                         |
| Batch Size            | `100`                                                                        |
| Correlation Key       | `= row.orderId`                                                              |
| Result Variable       | `orderRow`                                                                   |

## Supported Dialects

| Dialect | JDBC URL prefix | Notes |
|---|---|---|
| PostgreSQL | `jdbc:postgresql:` | |
| MySQL | `jdbc:mysql:` | |
| H2 | `jdbc:h2:` | |
| Oracle | `jdbc:oracle:` | Uses `FETCH FIRST n ROWS ONLY` for auto-pagination. |
| SQL Server | `jdbc:sqlserver:` | Uses `OFFSET ... FETCH NEXT n ROWS ONLY`, which **requires an `ORDER BY`** in the Polling Query — SQL Server rejects `OFFSET`/`FETCH` without one. |
| SQLite | `jdbc:sqlite:` | |

Dialect is auto-detected from the JDBC URL prefix; override it via the **Dialect** dropdown if needed.

## Consumption Strategies

How a correlated row is marked so it isn't redelivered on the next poll:

| Strategy | Mechanism | Requires write access? |
|---|---|---|
| `WATERMARK` (default) | Tracks a high-water column value; the row itself is never modified. | No |
| `UPDATE_FLAG` | Flips a boolean column to `TRUE` immediately after each row is correlated. | Yes |
| `DELETE_AFTER_READ` | Deletes the row immediately after it's correlated. | Yes |

`UPDATE_FLAG` and `DELETE_AFTER_READ` mutate the row **immediately after that row's
correlation succeeds** — inside the same poll, before the next row is processed —
rather than at the end of the batch. This means a later row in the batch failing
to correlate does not cause an already-correlated earlier row to be redelivered.

### UPDATE_FLAG / DELETE_AFTER_READ configuration

These strategies replace the Watermark group entirely — no watermark column,
type, or placeholder is needed, and `:lastWatermark` does not have to appear in
the Polling Query. Instead, configure:

| Property | Purpose |
|---|---|
| Consumption Strategy | `UPDATE_FLAG` or `DELETE_AFTER_READ` |
| Target Table | Table the UPDATE/DELETE is issued against (the Polling Query itself may still join/filter freely) |
| Key Column | Single column that uniquely identifies a row, used to target the UPDATE/DELETE |
| Flag Column | *(`UPDATE_FLAG` only)* boolean-ish column flipped to "true" after correlation |

> The literal used for "true" is dialect-specific: `TRUE` on PostgreSQL/MySQL/H2/SQLite,
> `1` on SQL Server and Oracle. Neither T-SQL nor (pre-23c) Oracle SQL has a bare
> `TRUE` keyword, so on those two, **Flag Column should be a numeric/BIT column**
> (`BIT` on SQL Server, `NUMBER(1)` on Oracle), not a true boolean type.

Example — `UPDATE_FLAG`:

| Property | Example value |
|---|---|
| Polling Query | `SELECT * FROM orders WHERE processed = FALSE` |
| Target Table | `orders` |
| Key Column | `id` |
| Flag Column | `processed` |

Example — `DELETE_AFTER_READ`:

| Property | Example value |
|---|---|
| Polling Query | `SELECT * FROM orders` |
| Target Table | `orders` |
| Key Column | `id` |

Because these strategies issue `UPDATE`/`DELETE` statements, the connector's
database user needs the matching grant (see [Database Operator
Setup](#database-operator-setup)) — the connection pool is automatically
switched from read-only to writable when a strategy other than `WATERMARK` is
configured (or when **Watermark Storage** below is set to durable). With the
default `WATERMARK` strategy and in-memory storage, nothing changes: the pool
stays read-only exactly as before.

### Durable Watermark Storage

By default the watermark lives in memory and is **lost on connector
restart/redeployment**. Set **Watermark Storage** to `Durable (JDBC table)` to
persist it instead. The table is **not** auto-created — run this DDL once per
database before activating the connector (adjust types per dialect as needed;
this is ANSI-portable SQL that works across all six supported dialects
unmodified):

```sql
CREATE TABLE camunda_db_poller_watermark (
    poller_key       VARCHAR(255) PRIMARY KEY,
    watermark_value  VARCHAR(255) NOT NULL,
    updated_at       TIMESTAMP NOT NULL
);
```

> Identifiers are matched **case-sensitively, quoted** by the connector
> (`"camunda_db_poller_watermark"`, `"poller_key"`, etc.). Most dialects
> preserve the case you use in an unquoted `CREATE TABLE` as-is, but Oracle
> folds unquoted identifiers to UPPERCASE — on Oracle, quote the DDL above
> exactly as shown (`CREATE TABLE "camunda_db_poller_watermark" (...)`) so it
> matches what the connector queries.

The row key (`poller_key`) is the element's **Instance ID** if set, otherwise
a stable hash of the JDBC URL + Polling Query — so multiple connector
elements sharing one table don't collide. **Watermark Table Name** (default
`camunda_db_poller_watermark`) lets you point at a different table name if
needed.

## Database Operator Setup

Grants below cover the default read-only `WATERMARK` setup. If you're using
`UPDATE_FLAG`, `DELETE_AFTER_READ`, or durable (JDBC table) watermark storage,
add the corresponding `UPDATE` / `DELETE` / `INSERT`+`DELETE` grants noted
under each snippet.

### PostgreSQL

```sql
CREATE ROLE camunda_poller WITH LOGIN PASSWORD 'change_me' CONNECTION LIMIT 5;
GRANT CONNECT ON DATABASE orders TO camunda_poller;
GRANT USAGE  ON SCHEMA  public TO camunda_poller;
GRANT SELECT ON TABLE   orders TO camunda_poller;
-- If you add more tables later:
-- ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO camunda_poller;

-- UPDATE_FLAG:
-- GRANT UPDATE ON TABLE orders TO camunda_poller;
-- DELETE_AFTER_READ:
-- GRANT DELETE ON TABLE orders TO camunda_poller;
-- Durable (JDBC table) watermark storage:
-- GRANT SELECT, INSERT, DELETE ON TABLE camunda_db_poller_watermark TO camunda_poller;
```

### MySQL

```sql
CREATE USER 'camunda_poller'@'%' IDENTIFIED BY 'change_me'
    WITH MAX_USER_CONNECTIONS 5;
GRANT SELECT ON orders.* TO 'camunda_poller'@'%';
FLUSH PRIVILEGES;

-- UPDATE_FLAG:               GRANT UPDATE ON orders.* TO 'camunda_poller'@'%';
-- DELETE_AFTER_READ:         GRANT DELETE ON orders.* TO 'camunda_poller'@'%';
-- Durable watermark storage: GRANT SELECT, INSERT, DELETE ON camunda_db_poller_watermark.* TO 'camunda_poller'@'%';
```

### Oracle

```sql
CREATE USER camunda_poller IDENTIFIED BY change_me;
GRANT CREATE SESSION TO camunda_poller;
GRANT SELECT ON orders TO camunda_poller;

-- UPDATE_FLAG:               GRANT UPDATE ON orders TO camunda_poller;
-- DELETE_AFTER_READ:         GRANT DELETE ON orders TO camunda_poller;
-- Durable watermark storage: GRANT SELECT, INSERT, DELETE ON camunda_db_poller_watermark TO camunda_poller;
```

### SQL Server

```sql
CREATE LOGIN camunda_poller WITH PASSWORD = 'change_me';
CREATE USER camunda_poller FOR LOGIN camunda_poller;
GRANT SELECT ON dbo.orders TO camunda_poller;

-- UPDATE_FLAG:               GRANT UPDATE ON dbo.orders TO camunda_poller;
-- DELETE_AFTER_READ:         GRANT DELETE ON dbo.orders TO camunda_poller;
-- Durable watermark storage: GRANT SELECT, INSERT, DELETE ON dbo.camunda_db_poller_watermark TO camunda_poller;
```

### SQLite

SQLite has no user/grant model — file-level OS permissions control access.
Mount the database file read-only (e.g. a read-only volume/bind mount) unless
`UPDATE_FLAG`, `DELETE_AFTER_READ`, or durable watermark storage is in use.

## Operational Notes

### Threading Model

Each activated connector element gets exactly **one daemon thread** named `db-poller-<bpmnProcessId>`. Scheduling uses `scheduleWithFixedDelay`, so a slow poll does not cause overlapping executions. The thread is destroyed on `deactivate()`.

### Resilience

- Consecutive poll failures increment a counter. When the counter reaches `circuitBreakerThreshold`, the connector reports `Health.DOWN` to the runtime, which surfaces as an incident in Camunda Operate.
- The scheduler **keeps running** after health goes DOWN, enabling automatic self-healing once the database recovers.
- Per-row correlation failure aborts the current batch. The watermark is **not** advanced, so the same rows will be retried on the next poll.

### Security

- The watermark is always bound via `PreparedStatement` — no string concatenation of user data occurs. The `UPDATE`/`DELETE` statements issued by `UPDATE_FLAG`/`DELETE_AFTER_READ` bind the key value the same way; only the (operator-configured, not runtime-user-supplied) table/column *names* are interpolated, and those are always quoted via `DatabaseDialect#quoteIdentifier`.
- The polling query is a configured template (not user input at runtime), and the only dynamic value it receives is the watermark, which is bound as a typed JDBC parameter.
- The connection pool is **read-only by default** and only becomes writable when the configuration actually needs it — `UPDATE_FLAG`, `DELETE_AFTER_READ`, or durable (JDBC table) watermark storage (see [`DbPollerProperties#requiresWriteAccess`](src/main/java/io/camunda/connector/dbpoller/DbPollerProperties.java)). The default `WATERMARK` + in-memory setup is unaffected and stays read-only.
- Use a dedicated database user scoped to exactly the grants your configuration needs (see [Database Operator Setup](#database-operator-setup)), with `CONNECTION LIMIT` (Postgres) or `MAX_USER_CONNECTIONS` (MySQL).

### Clock Skew Warning

When using `TIMESTAMP` watermarks, ensure the clocks of the connector host and the database server are synchronised (e.g. via NTP). A skew larger than the polling interval can cause rows to be missed or duplicated.

## Roadmap

v1.0 is complete: Oracle/SQL Server/SQLite dialects, durable (JDBC table)
watermark storage, and the `UPDATE_FLAG`/`DELETE_AFTER_READ` consumption
strategies are all implemented — see [Supported Dialects](#supported-dialects)
and [Consumption Strategies](#consumption-strategies).

### v1.1

- Micrometer metrics (rows polled, latency, failure count).
- Native handling of JSON/JSONB columns (auto-deserialise to `Map`).

### v2.0

- Pluggable watermark backends: Redis, Camunda process variables.
- `LISTEN/NOTIFY` sibling connector for PostgreSQL push-based CDC.

## License

Apache License 2.0 — see [LICENSE](LICENSE).

---

> **Disclaimer:** This connector is a community contribution and is **not** part of Camunda's commercial product or covered by Camunda's enterprise support. Use it at your own risk. Contributions and bug reports are welcome via GitHub Issues.

