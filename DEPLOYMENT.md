# camunda-db-poller — End-to-End Installation & Execution Guide

**Target platform:** Camunda 8 SaaS
**Document version:** 1.0
**Scope:** Build the connector, provision and configure a Camunda 8 SaaS cluster, deploy the connector runtime, model and deploy a BPMN process, and execute the flow end-to-end.

---

## Table of Contents

1. [Overview](#1-overview)
2. [Prerequisites](#2-prerequisites)
3. [Installing on Camunda 8 SaaS](#3-installing-on-camunda-8-saas)
   - 3.1 [Create a Camunda SaaS Account](#31-create-a-camunda-saas-account)
   - 3.2 [Create a Cluster](#32-create-a-cluster)
   - 3.3 [Create an API Client](#33-create-an-api-client)
   - 3.4 [Sample SaaS Configuration](#34-sample-saas-configuration)
4. [Build the Connector](#4-build-the-connector)
5. [Configure Local Credentials (.env)](#5-configure-local-credentials-env)
6. [Deploy the Connector Runtime](#6-deploy-the-connector-runtime)
   - 6.1 [Run the Container](#61-run-the-container)
   - 6.2 [Verify the Deployment](#62-verify-the-deployment)
7. [Prepare a Test Database](#7-prepare-a-test-database)
8. [Model and Deploy the BPMN Process](#8-model-and-deploy-the-bpmn-process)
   - 8.1 [Import the Element Template](#81-import-the-element-template)
   - 8.2 [Configure the Connector Properties](#82-configure-the-connector-properties)
   - 8.3 [Deploy to the SaaS Cluster](#83-deploy-to-the-saas-cluster)
9. [Execute End-to-End](#9-execute-end-to-end)
10. [Lifecycle Management](#10-lifecycle-management)
11. [Troubleshooting](#11-troubleshooting)
12. [Appendix A — Recreating .env](#appendix-a--recreating-env)
13. [Appendix B — Environment Variable Reference](#appendix-b--environment-variable-reference)

---

## 1. Overview

`camunda-db-poller` is a community Camunda 8 **inbound connector**: it polls
a JDBC database on a fixed interval and turns new or updated rows into BPMN
messages, without any custom Java delegate or batch job. Configure a `SELECT`
query, pick a watermark column, and every matching row starts or continues a
process instance.

### 1.1 Architecture

```
Connector Runtime
  activate()
    -> daemon thread "db-poller-<processId>"
         pollOnce():
           1. read watermark            (InMemoryWatermarkStore)
           2. build SQL                 (replace :lastWatermark placeholder)
           3. PreparedStatement.setXxx  (bind watermark, no string concat)
           4. executeQuery -> iterate ResultSet
           5. correlateWithResult(RowPayload)  --> Zeebe (gRPC)
           6. advance highestWatermarkSeen
         (repeats on a fixed delay = pollingIntervalSeconds)
```

### 1.2 Payload Shape

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

---

## 2. Prerequisites

| # | Requirement | Notes |
|---|---|---|
| 1 | JDK 17 | `pom.xml` targets Java 17. Set `JAVA_HOME` to a JDK 17 install before building. |
| 2 | Maven 3.9+ | Check with `mvn -version`. |
| 3 | Docker Desktop | Must be running, daemon reachable (`docker info`). |
| 4 | A Camunda account | Free or paid — see [Section 3](#3-installing-on-camunda-8-saas) if you don't have a cluster yet. |
| 5 | Camunda Modeler | Used to author and deploy the BPMN process. [Download](https://camunda.com/download/modeler/). |
| 6 | A test database | PostgreSQL, MySQL, or H2. This guide provisions a throwaway PostgreSQL container so the whole flow is self-contained. |

---

## 3. Installing on Camunda 8 SaaS

This section provisions the Camunda 8 SaaS cluster and API credentials the
connector will run against. Skip to [Section 3.4](#34-sample-saas-configuration)
if you already have a cluster and client.

### 3.1 Create a Camunda SaaS Account

1. Go to the [Camunda Console](https://console.cloud.camunda.io).
2. Sign up (or sign in) with your email, Google, or GitHub account.
3. Accept the workspace / organization prompt on first login — Camunda
   creates a default organization for you.

### 3.2 Create a Cluster

1. In the Console, select **Clusters** in the left navigation.
2. Click **Create new cluster**.
3. Choose:
   - **Plan**: Trial/Free plan is sufficient for this guide.
   - **Region**: e.g. `Singapore (sin-2)`.
   - **Cluster name**: e.g. `db-poller-cluster`.
4. Click **Create**. Provisioning takes a few minutes; the cluster status
   moves from *Creating* to *Healthy*.
5. Open the cluster and copy its **Cluster ID** from the cluster's overview
   page (also visible in the URL: `.../cluster/<cluster-id>`).

### 3.3 Create an API Client

1. Inside the cluster, go to the **API** tab.
2. Click **Create new client**.
3. Give it a name, e.g. `db-poller-client`.
4. Under **Scopes**, enable at minimum **Zeebe**.
5. Click **Create** — the **Client ID** and **Client Secret** are shown
   **once**. Copy both immediately and store them securely (a password
   manager or your local `.env` file — never a committed file).
6. The client's **OAuth Token URL** and **Audience** are shown on the same
   screen; for Camunda SaaS these are fixed values:
   - Token URL: `https://login.cloud.camunda.io/oauth/token`
   - Audience: `zeebe.camunda.io`

### 3.4 Sample SaaS Configuration

The values below are this project's actual provisioned cluster, used
throughout the rest of this guide as a worked example. Treat these exactly
like a password: keep them out of source control and don't share this
document outside people who should have cluster access.

| Variable | Value |
|---|---|
| `CAMUNDA_REGION_ID` | `sin-2` |
| `CAMUNDA_CLUSTER_ID` | `6b1873fb-1baa-4922-8136-5bb42ddcbc4f` |
| `CAMUNDA_CLIENT_ID` | `1HlV.YWTWEj6ccY1D-EaLxS26YV5R7RH` |
| `CAMUNDA_CLIENT_SECRET` | `rWhQntsplyxnLZeO72lL6WezsLAmV-jZjX0f~IVUCPZaYyit0UXZ.3TaohKSu.2v` |
| `CAMUNDA_OAUTH_URL` | `https://login.cloud.camunda.io/oauth/token` |
| `CAMUNDA_TOKEN_AUDIENCE` | `zeebe.camunda.io` |

These are the names Camunda Console shows you. The Connector Runtime's
Spring Boot configuration binds to a **different** set of property names
(`camunda.client.*`) — [Section 5](#5-configure-local-credentials-env) shows
the exact translation. This is the single most common setup mistake: using
the Console's variable names directly as Docker environment variables
silently produces an unauthenticated client (`auth.method` defaults to
`none`) rather than an error.

---

## 4. Build the Connector

```powershell
$env:JAVA_HOME = "C:\Program Files\Java\jdk-17"   # adjust to your JDK 17 path
mvn -q clean package
```

This produces `target/camunda-db-poller-1.0.0-SNAPSHOT-with-dependencies.jar`
— the shaded jar the runtime image loads.

**Why the `pom.xml` dependency scopes matter:** several dependencies are
deliberately `provided` (excluded from the shaded jar) or have exclusions,
to avoid classpath conflicts with the runtime image. This isn't arbitrary —
see [Section 11](#11-troubleshooting) for the exact failures each one fixes.

---

## 5. Configure Local Credentials (.env)

Create a `.env` file at the project root (already listed in `.gitignore` —
verify it stays that way). Translate the Console values from
[Section 3.4](#34-sample-saas-configuration) into the property names the
runtime actually binds to:

| Console variable | `.env` variable (what the runtime reads) |
|---|---|
| `CAMUNDA_REGION_ID` | `CAMUNDA_CLIENT_CLOUD_REGION` |
| `CAMUNDA_CLUSTER_ID` | `CAMUNDA_CLIENT_CLOUD_CLUSTERID` |
| `CAMUNDA_CLIENT_ID` | `CAMUNDA_CLIENT_AUTH_CLIENTID` |
| `CAMUNDA_CLIENT_SECRET` | `CAMUNDA_CLIENT_AUTH_CLIENTSECRET` |
| `CAMUNDA_OAUTH_URL` | `CAMUNDA_CLIENT_AUTH_TOKENURL` |
| `CAMUNDA_TOKEN_AUDIENCE` | `CAMUNDA_CLIENT_AUTH_AUDIENCE` |
| *(none — required by SaaS mode)* | `CAMUNDA_CLIENT_MODE=saas` |

Resulting `.env`, using the sample cluster from Section 3.4:

```
CAMUNDA_CLIENT_MODE=saas
CAMUNDA_CLIENT_CLOUD_REGION=sin-2
CAMUNDA_CLIENT_CLOUD_CLUSTERID=6b1873fb-1baa-4922-8136-5bb42ddcbc4f
CAMUNDA_CLIENT_AUTH_CLIENTID=1HlV.YWTWEj6ccY1D-EaLxS26YV5R7RH
CAMUNDA_CLIENT_AUTH_CLIENTSECRET=rWhQntsplyxnLZeO72lL6WezsLAmV-jZjX0f~IVUCPZaYyit0UXZ.3TaohKSu.2v
CAMUNDA_CLIENT_AUTH_TOKENURL=https://login.cloud.camunda.io/oauth/token
CAMUNDA_CLIENT_AUTH_AUDIENCE=zeebe.camunda.io
```

This is already saved for you as `.env` in the project root.

---

## 6. Deploy the Connector Runtime

### 6.1 Run the Container

```powershell
docker rm -f camunda-db-poller 2>$null
docker run -d --name camunda-db-poller `
  --env-file .env `
  -v "${PWD}\target\camunda-db-poller-1.0.0-SNAPSHOT-with-dependencies.jar:/opt/app/camunda-db-poller.jar" `
  camunda/connectors-bundle:latest
```

This starts a Spring Boot process inside the container that:

1. Authenticates to `CAMUNDA_CLIENT_AUTH_TOKENURL` via OAuth
   client-credentials, using your client ID/secret.
2. Opens a gRPC connection to your cluster's Zeebe gateway
   (`https://<cluster-id>.<region>.zeebe.camunda.io:443`, derived
   automatically from `CAMUNDA_CLIENT_CLOUD_*`).
3. Loads `camunda-db-poller.jar` from `/opt/app/` and registers
   `io.camunda:camunda-db-poller:1` as an available inbound connector type
   (see `DbPollerConnectorExecutable.TYPE`).

### 6.2 Verify the Deployment

```powershell
docker ps --filter name=camunda-db-poller
docker exec camunda-db-poller wget -qO- http://localhost:8080/actuator/health/liveness
```

Expected output:

```json
{"status":"UP","components":{"zeebeClient":{"status":"UP","details":{"anyPartitionHealthy":true,"numBrokers":3}}}}
```

If it's not `UP`, go to [Section 11](#11-troubleshooting).

---

## 7. Prepare a Test Database

This gives you something real to poll. Skip this section and point at your
own database if you already have one.

```powershell
docker run -d --name db-poller-test-pg `
  -e POSTGRES_USER=camunda_poller `
  -e POSTGRES_PASSWORD=change_me `
  -e POSTGRES_DB=orders `
  -p 5432:5432 `
  postgres:16
```

Create the table the sample configuration in Section 8.2 expects:

```powershell
docker exec -i db-poller-test-pg psql -U camunda_poller -d orders -c "
CREATE TABLE orders (
    id         SERIAL PRIMARY KEY,
    order_id   TEXT NOT NULL,
    status     TEXT NOT NULL,
    amount     NUMERIC NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
"
```

Because the connector runtime runs in its own container, it reaches this
Postgres container via the Docker Desktop host bridge, not `localhost`:

```
jdbc:postgresql://host.docker.internal:5432/orders
```

On Linux hosts without Docker Desktop, put both containers on a
user-defined `docker network` and use the Postgres container's name as the
host instead.

---

## 8. Model and Deploy the BPMN Process

### 8.1 Import the Element Template

1. Open Camunda Modeler.
2. **File → Import Element Template…** → select
   `element-templates/db-poller-inbound-connector.json` from this repo.
3. Create a new BPMN diagram with a **Message Start Event**.
4. Select the event, open **Properties Panel → Template**, and pick
   **Database Polling Connector**.

### 8.2 Configure the Connector Properties

Fill in the Properties Panel using the test database from Section 7:

| Property | Value |
|---|---|
| JDBC URL | `jdbc:postgresql://host.docker.internal:5432/orders` |
| Username | `camunda_poller` |
| Password | `change_me` |
| Polling Query | `SELECT * FROM orders WHERE updated_at > :lastWatermark ORDER BY updated_at` |
| Watermark Column | `updated_at` |
| Watermark Type | `TIMESTAMP` |
| Initial Watermark | `1970-01-01T00:00:00Z` |
| Polling Interval (s) | `10` |
| Batch Size | `100` |
| Correlation Key | `= row.order_id` |
| Result Variable | `orderRow` |

Give the Message Start Event a **message name**, e.g. `OrderReceived`, and
set the process's **BPMN Process ID**, e.g. `order-fulfillment`. Add a
simple downstream flow (e.g. a single End Event) so the process has
somewhere to go.

### 8.3 Deploy to the SaaS Cluster

In Camunda Modeler:

1. Click **Deploy** (rocket icon, top-right).
2. Choose **Camunda 8 SaaS** as the deployment target.
3. Enter the cluster and client details from Section 3 (Cluster ID, Region,
   Client ID, Client Secret).
4. Deploy.

Once deployed, the running connector runtime picks up the new process
definition (via `ProcessDefinitionImportConfiguration`, polling every few
seconds) and activates the DB poller for it — you'll see a log line like:

```
Activating DB Poller: dialect=POSTGRES, interval=10s, batchSize=100
```

Check for it:

```powershell
docker logs camunda-db-poller --tail 50
```

> If your cluster is affected by the known REST-endpoint issue noted in
> [Section 11](#11-troubleshooting), process definition import may not pick
> up the new process automatically. Restarting the container after
> deployment (`docker restart camunda-db-poller`) forces a fresh
> SPI/process discovery pass and is a reliable workaround.

---

## 9. Execute End-to-End

Insert a row into the test table:

```powershell
docker exec -i db-poller-test-pg psql -U camunda_poller -d orders -c "
INSERT INTO orders (order_id, status, amount) VALUES ('ORD-1001', 'PENDING', 199.99);
"
```

Within one polling interval (10s in this example), the connector should:

1. Read the row via `PollingService.pollOnce()`.
2. Build a `RowPayload` and correlate it as message `OrderReceived` with
   correlation key `ORD-1001`.
3. Start a new instance of the `order-fulfillment` process, with process
   variable `orderRow` set to the row payload.

Confirm it in **Camunda Operate** (part of your SaaS cluster console):

- Navigate to your cluster's Operate URL.
- Filter by process `order-fulfillment`.
- You should see a new instance, started at the time of the insert, with
  `orderRow` in its variables.

Or check the connector's own logs for the poll cycle:

```powershell
docker logs camunda-db-poller --tail 20
```

---

## 10. Lifecycle Management

```powershell
docker stop camunda-db-poller       # stop, keep container
docker start camunda-db-poller      # resume
docker restart camunda-db-poller    # restart (forces a fresh process-definition import pass)
docker rm -f camunda-db-poller      # remove entirely
```

To redeploy after a code change:

```powershell
$env:JAVA_HOME = "C:\Program Files\Java\jdk-17"
mvn -q clean package
docker rm -f camunda-db-poller
docker run -d --name camunda-db-poller --env-file .env `
  -v "${PWD}\target\camunda-db-poller-1.0.0-SNAPSHOT-with-dependencies.jar:/opt/app/camunda-db-poller.jar" `
  camunda/connectors-bundle:latest
```

To tear down the test database:

```powershell
docker rm -f db-poller-test-pg
```

---

## 11. Troubleshooting

These are real failures hit and fixed while first standing this up —
useful if you see the same symptoms after a dependency bump or on a fresh
machine.

| Symptom | Cause | Fix |
|---|---|---|
| `mvn package` fails: `invalid target release: 25` | No JDK 25 installed | `pom.xml` now targets Java 17; make sure `JAVA_HOME` points at a JDK 17. |
| Container exits immediately; log shows `LoggerFactory is not a Logback LoggerContext` | Shaded jar bundled its own `slf4j-api`, shadowing the runtime's Logback wiring | Keep `slf4j-api` at `provided` scope in `pom.xml` (already done). |
| Container exits; log shows `Scala module ... requires Jackson Databind version >= 2.22.0 ... Found ... 2.18.0` | Shaded jar bundled an older `jackson-databind` than the runtime uses | Keep `jackson-databind`/`jackson-datatype-jsr310` at `provided` scope (already done). |
| Container exits; log shows `NoClassDefFoundError: io/camunda/connector/api/inbound/InboundConnectorExecutable` | The runtime loads custom connector jars (`/opt/app/*.jar`) through an isolated classloader for SPI/`ServiceLoader` discovery — it can't see the runtime's own `connector-core` classes from that isolated loader | `connector-core` must **not** be `provided`; bundle it into the jar (already done) and pin it to the same version as the runtime image (`8.9.12`). |
| Container exits; log shows `Detected incompatible Protobuf Gencode/Runtime versions` | `connector-core` and `mysql-connector-j` both transitively pull an older `protobuf-java` that conflicts with the runtime's own gRPC stubs | Both have a `<exclusion>` on `com.google.protobuf:protobuf-java` in `pom.xml` (already done). |
| App starts, but log warns `No 'camunda.client.auth.method' detected, will be set to 'none'` | Wrong env var names — Spring Boot's relaxed binding needs the exact `CAMUNDA_CLIENT_*` shape (`CAMUNDA_CLIENT_AUTH_CLIENTID`, not `CAMUNDA_CLIENT_ID`) | Use the property names in Section 5, or see [Camunda's Spring Boot Starter config docs](https://docs.camunda.io/docs/apis-tools/camunda-spring-boot-starter/configuration/). |
| Repeated log errors: `Failed to import LATEST/ACTIVE process versions ... Failed with code 404` | The runtime auto-derives a REST endpoint (`https://<region>.zeebe.camunda.io/...`) for process-definition discovery that may not match this cluster/version | Does **not** block the actual DB-poller function, which runs over gRPC. `/actuator/health/liveness` (zeebeClient) is the real signal to watch, not `/health/readiness`. Restarting the container after deploying a new process is a workaround if auto-discovery seems stuck. |

### Useful Diagnostic Commands

```powershell
# Full container health
docker exec camunda-db-poller wget -qO- http://localhost:8080/actuator/health

# Follow logs live
docker logs -f camunda-db-poller

# Confirm which inbound connector types are registered
docker logs camunda-db-poller | Select-String "inbound connector"
```

---

## Appendix A — Recreating .env

If `.env` is missing (fresh clone, new machine), recreate it at the project
root with your Camunda 8 SaaS API client's credentials (Console → your
cluster → API → create/view client):

```
CAMUNDA_CLIENT_MODE=saas
CAMUNDA_CLIENT_CLOUD_REGION=<your region, e.g. sin-2>
CAMUNDA_CLIENT_CLOUD_CLUSTERID=<your cluster id>
CAMUNDA_CLIENT_AUTH_CLIENTID=<your client id>
CAMUNDA_CLIENT_AUTH_CLIENTSECRET=<your client secret>
CAMUNDA_CLIENT_AUTH_TOKENURL=https://login.cloud.camunda.io/oauth/token
CAMUNDA_CLIENT_AUTH_AUDIENCE=zeebe.camunda.io
```

`.env` is listed in `.gitignore` — verify it stays that way before ever
running `git add`.

---

## Appendix B — Environment Variable Reference

| `.env` variable | Maps to Spring property | Purpose |
|---|---|---|
| `CAMUNDA_CLIENT_MODE` | `camunda.client.mode` | Set to `saas` to enable SaaS auto-configuration (derives gRPC/REST addresses from region + cluster ID). |
| `CAMUNDA_CLIENT_CLOUD_REGION` | `camunda.client.cloud.region` | SaaS region, e.g. `sin-2`. |
| `CAMUNDA_CLIENT_CLOUD_CLUSTERID` | `camunda.client.cloud.cluster-id` | SaaS cluster identifier. |
| `CAMUNDA_CLIENT_AUTH_CLIENTID` | `camunda.client.auth.client-id` | OAuth client ID from the API client. |
| `CAMUNDA_CLIENT_AUTH_CLIENTSECRET` | `camunda.client.auth.client-secret` | OAuth client secret from the API client. |
| `CAMUNDA_CLIENT_AUTH_TOKENURL` | `camunda.client.auth.token-url` | OAuth token endpoint (`https://login.cloud.camunda.io/oauth/token` for SaaS). |
| `CAMUNDA_CLIENT_AUTH_AUDIENCE` | `camunda.client.auth.audience` | OAuth resource audience (`zeebe.camunda.io` for SaaS). |

---

*End of document.*
