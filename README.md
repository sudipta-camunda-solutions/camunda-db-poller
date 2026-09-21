# camunda-db-poller

> **Community MVP** — production-usable for PostgreSQL, MySQL, and H2; additional dialects and watermark strategies are on the roadmap.

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

## Database Operator Setup

### PostgreSQL

```sql
CREATE ROLE camunda_poller WITH LOGIN PASSWORD 'change_me' CONNECTION LIMIT 5;
GRANT CONNECT ON DATABASE orders TO camunda_poller;
GRANT USAGE  ON SCHEMA  public TO camunda_poller;
GRANT SELECT ON TABLE   orders TO camunda_poller;
-- If you add more tables later:
-- ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO camunda_poller;
```

### MySQL

```sql
CREATE USER 'camunda_poller'@'%' IDENTIFIED BY 'change_me'
    WITH MAX_USER_CONNECTIONS 5;
GRANT SELECT ON orders.* TO 'camunda_poller'@'%';
FLUSH PRIVILEGES;
```

## Operational Notes

### Threading Model

Each activated connector element gets exactly **one daemon thread** named `db-poller-<bpmnProcessId>`. Scheduling uses `scheduleWithFixedDelay`, so a slow poll does not cause overlapping executions. The thread is destroyed on `deactivate()`.

### Resilience

- Consecutive poll failures increment a counter. When the counter reaches `circuitBreakerThreshold`, the connector reports `Health.DOWN` to the runtime, which surfaces as an incident in Camunda Operate.
- The scheduler **keeps running** after health goes DOWN, enabling automatic self-healing once the database recovers.
- Per-row correlation failure aborts the current batch. The watermark is **not** advanced, so the same rows will be retried on the next poll.

### Security

- The watermark is always bound via `PreparedStatement` — no string concatenation of user data occurs.
- The polling query is a configured template (not user input at runtime), and the only dynamic value it receives is the watermark, which is bound as a typed JDBC parameter.
- Use a dedicated read-only database user with `CONNECTION LIMIT` (Postgres) or `MAX_USER_CONNECTIONS` (MySQL).

### Clock Skew Warning

When using `TIMESTAMP` watermarks, ensure the clocks of the connector host and the database server are synchronised (e.g. via NTP). A skew larger than the polling interval can cause rows to be missed or duplicated.

## Roadmap

### v1.0

- Additional JDBC dialects: Oracle, SQL Server, SQLite.
- Durable watermark storage backed by a JDBC table.
- `UPDATE_FLAG` watermark strategy (flip a boolean column after read).
- `DELETE_AFTER_READ` watermark strategy (delete row after successful correlation).

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

