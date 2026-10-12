# Tenant Provisioning Control Plane

A control-plane service for a multi-tenant SaaS platform. It records tenants,
emits provisioning tasks to a message broker **after** the database commit
(transactional outbox), and consumes worker progress updates idempotently and
order-tolerantly. A worker simulator drives tasks through their lifecycle.

> **Build status** (remove before submission)
>
> - [x] Domain state machines + exhaustive tests (`internal/domain`)
> - [x] HTTP API skeleton: routes, middleware, error format, route test (`internal/api`)
> - [ ] HTTP handlers: decode, validate, call service, respond (currently 501 stubs)
> - [ ] Store: migrations, pgx repositories, conditional UPDATEs, concurrency tests
> - [ ] Outbox relay
> - [ ] Worker simulator
> - [ ] Progress consumer (idempotency, ordering, DLQ)
> - [ ] Dockerfile, `compose.yaml`, quality gates
> - [ ] Negative-scenario scripts

---

## Contents

- [Quick start](#quick-start)
- [Tech stack](#tech-stack)
- [Architecture](#architecture)
- [State machines](#state-machines)
- [REST API](#rest-api)
- [Messaging](#messaging)
- [Database schema](#database-schema)
- [Design decisions and trade-offs](#design-decisions-and-trade-offs)
- [Project layout](#project-layout)

---

## Quick start

**Prerequisite:** Docker with Compose v2. Nothing else (no Go toolchain, no
`make`) is needed on the host.

```bash
git clone <repo-url> && cd <repo>
docker compose up --build                             # boot: Postgres, RabbitMQ, control-plane, worker
docker compose --profile test run --rm --build test   # full suite: format check, lint, security scan, tests
```

| Service       | URL                                                         |
| ------------- | ----------------------------------------------------------- |
| API           | http://localhost:8080                                       |
| RabbitMQ UI   | http://localhost:15672 (credentials in `.env.example`)      |

Credentials in `.env.example` are local-development defaults, not secrets.

---

## Tech stack

| Concern               | Choice                                       | Why                                                                                   |
| --------------------- | -------------------------------------------- | ------------------------------------------------------------------------------------- |
| Language              | Go 1.24                                      | Static binary, small distroless image, first-class concurrency                        |
| HTTP routing          | `net/http` `ServeMux` (Go 1.22+ patterns)    | Method + wildcard routing with zero dependencies                                      |
| Database              | PostgreSQL 17                                | Transactions, `UNIQUE`/`CHECK` constraints, `FOR UPDATE SKIP LOCKED`                  |
| DB driver             | `jackc/pgx/v5` + `pgxpool`                   | Fast, idiomatic, exposes SQLSTATE codes (e.g. `23505` for unique violations)          |
| Migrations            | `pressly/goose`, embedded, run on startup    | Schema always matches the binary; no manual step                                      |
| Broker                | RabbitMQ 4.x (management image)             | Work-queue semantics, per-message ack/nack, native dead-lettering                     |
| AMQP client           | `rabbitmq/amqp091-go`                        | Official client; publisher confirms                                                   |
| IDs                   | `google/uuid` (UUIDv7)                       | App-generated (task ID exists before commit), time-ordered (index-friendly, cursor)   |
| Logging               | `log/slog` (JSON)                            | Standard library, structured                                                          |
| Lint / format / sec   | `golangci-lint` (incl. `gofumpt`, `gosec`), `govulncheck` | One command for static analysis, formatting and security             |
| Tests                 | `go test -race`                              | Unit tests + integration tests against real Postgres and RabbitMQ                     |
| Packaging             | Multi-stage Dockerfile, Docker Compose       | One image, two entrypoints (`control-plane`, `worker`)                                |

---

## Architecture

```mermaid
flowchart LR
    client([Client])

    subgraph cp["control-plane (one process)"]
        api["HTTP API"]
        relay["Outbox relay"]
        consumer["Progress consumer"]
    end

    db[("PostgreSQL<br/>tenants · tasks · processed_messages")]

    subgraph mq["RabbitMQ"]
        qt[["tasks"]]
        qu[["task-updates"]]
        dlq[["*.dlq"]]
    end

    worker["Worker simulator<br/>--fail-rate --min/max-delay-ms"]

    client -- "REST" --> api
    api -- "tenant + task<br/>one transaction" --> db
    relay -- "claim unpublished<br/>SKIP LOCKED" --> db
    relay -- "publish after commit<br/>publisher confirms" --> qt
    qt --> worker
    worker -- "in_progress / done / failed" --> qu
    qu --> consumer
    consumer -- "apply update<br/>idempotent, forward-only" --> db
    consumer -. "poison messages" .-> dlq
```

Four containers: `postgres`, `rabbitmq`, `control-plane`, `worker`. The
control-plane and worker are two entrypoints of **one image**. Inside the
control-plane process, three components run as goroutines sharing one DB pool:

| Component         | Responsibility                                                                 |
| ----------------- | ------------------------------------------------------------------------------ |
| HTTP API          | Validates requests, writes tenant + task in one transaction                     |
| Outbox relay      | Publishes committed, unpublished tasks to RabbitMQ in creation order            |
| Progress consumer | Applies worker updates to tasks (and to tenants on terminal outcomes)          |

### High-level workflow

```mermaid
sequenceDiagram
    autonumber
    actor C as Client
    participant API as HTTP API
    participant DB as PostgreSQL
    participant R as Outbox relay
    participant Q as RabbitMQ
    participant W as Worker simulator
    participant CO as Progress consumer

    C->>API: POST /v1/tenants {slug, name}
    API->>DB: BEGIN; INSERT tenant (provisioning)<br/>INSERT task (deploy, accepted, published_at NULL); COMMIT
    API-->>C: 202 {tenant, task}
    API--)R: nudge (non-blocking)

    R->>DB: SELECT unpublished tasks ORDER BY id<br/>FOR UPDATE SKIP LOCKED
    R->>Q: publish to "tasks" (await confirm)
    R->>DB: SET published_at = now()

    Q->>W: deliver task
    W->>Q: publish {update_id, task_id, in_progress} to "task-updates"
    Q->>CO: deliver update
    CO->>DB: record update_id + task → in_progress (one transaction)

    Note over W: simulated work<br/>(random delay, fail-rate)
    W->>Q: publish {update_id, task_id, done | failed}
    W->>Q: ack task
    Q->>CO: deliver update
    CO->>DB: record update_id + task → done<br/>tenant → active, version+1 (one transaction)

    C->>API: GET /v1/tenants/{id}
    API-->>C: 200 {status: "active", version: 2}
```

---

## State machines

The authoritative tables live in [`internal/domain/transitions.go`](internal/domain/transitions.go)
and are tested exhaustively (every status × every operation).

### Tenant

```mermaid
stateDiagram-v2
    [*] --> provisioning : POST
    provisioning --> active : deploy done
    provisioning --> failed : deploy failed
    active --> updating : PATCH
    updating --> active : update done
    updating --> failed : update failed
    active --> destroying : DELETE
    failed --> destroying : DELETE
    destroying --> destroyed : destroy done
    destroying --> failed : destroy failed
    destroyed --> [*]
```

### Task

```mermaid
stateDiagram-v2
    [*] --> accepted : created by tenant API call
    accepted --> in_progress
    accepted --> done : forward jump (late in_progress)
    accepted --> failed : forward jump
    in_progress --> done
    in_progress --> failed
    done --> [*]
    failed --> [*]
```

### Allowed operations by tenant status

| Tenant status  | PATCH                          | DELETE                           |
| -------------- | ------------------------------ | -------------------------------- |
| `provisioning` | 409 `tenant_update_not_allowed` | 409 `tenant_update_not_allowed` |
| `active`       | → `updating` + `update` task   | → `destroying` + `destroy` task  |
| `updating`     | 409                            | 409                              |
| `destroying`   | 409                            | 409                              |
| `failed`       | 409                            | → `destroying` + `destroy` task  |
| `destroyed`    | 409                            | 409                              |

Because only `active`/`failed` tenants accept new work and every accepted
call moves the tenant into a busy status, **at most one open task per tenant**
falls out of the guards. A partial unique index enforces it again as defence
in depth.

---

## REST API

Base path: `/v1`. All bodies are JSON. Timestamps are ISO 8601 UTC with
milliseconds, e.g. `2026-10-12T05:30:00.123Z`.

### Endpoints

| Method   | Path                  | Purpose                                 | Success | Errors                                   |
| -------- | --------------------- | --------------------------------------- | ------- | ---------------------------------------- |
| `POST`   | `/v1/tenants`         | Create a tenant (starts deploy)         | 202     | 400, 409 `tenant_already_exists`, 422    |
| `GET`    | `/v1/tenants`         | List tenants (paginated, filterable)    | 200     | 422                                      |
| `GET`    | `/v1/tenants/{id}`    | Get one tenant                          | 200     | 404 `tenant_not_found`                   |
| `PATCH`  | `/v1/tenants/{id}`    | Partial update (starts update)          | 202     | 400, 404, 409 `tenant_update_not_allowed` / `tenant_version_conflict`, 422 |
| `DELETE` | `/v1/tenants/{id}`    | Delete (starts destroy)                 | 202     | 404, 409 `tenant_update_not_allowed`     |
| `GET`    | `/v1/tasks`           | List tasks (paginated, filterable)      | 200     | 422                                      |
| `GET`    | `/v1/tasks/{id}`      | Get one task                            | 200     | 404 `task_not_found`                     |
| `GET`    | `/healthz`            | Liveness                                | 200     |                                          |
| `GET`    | `/readyz`             | Readiness (DB reachable)                | 200     | 503 `not_ready`                          |

Tasks are **read-only** over HTTP: `POST`/`PATCH`/`DELETE` on `/v1/tasks`
return 405. They are created only as a side effect of tenant calls and driven
only by the worker.

**Why 202 Accepted for mutations:** the change is asynchronous. The response
contains the tenant's new state and the task that will carry the work out;
poll the task or tenant to follow progress.

### Create a tenant

```bash
curl -s -X POST localhost:8080/v1/tenants \
  -H 'Content-Type: application/json' \
  -d '{"slug": "acme-eu", "name": "Acme Corporation (EU)"}'
```

`202 Accepted`, `Location: /v1/tenants/{id}`

```json
{
  "tenant": {
    "id": "0192f3a1-7c2e-7b4d-9f10-3a5c8e2d1b44",
    "slug": "acme-eu",
    "name": "Acme Corporation (EU)",
    "status": "provisioning",
    "version": 1,
    "created_at": "2026-10-12T05:30:00.123Z",
    "updated_at": "2026-10-12T05:30:00.123Z"
  },
  "task": {
    "id": "0192f3a1-7c2f-7000-8000-000000000001",
    "type": "deploy",
    "tenant_id": "0192f3a1-7c2e-7b4d-9f10-3a5c8e2d1b44",
    "status": "accepted",
    "created_at": "2026-10-12T05:30:00.123Z",
    "updated_at": "2026-10-12T05:30:00.123Z"
  }
}
```

Rules: `slug` is 3–28 chars matching `^[a-z][-a-z0-9]{1,26}[a-z0-9]$`
(Kubernetes-namespace compatible) and unique forever, including against
destroyed tenants. `name` must be non-empty.

### Get a tenant

```bash
curl -s localhost:8080/v1/tenants/0192f3a1-7c2e-7b4d-9f10-3a5c8e2d1b44
```

`200 OK` with the tenant object. A malformed or unknown ID returns
`404 tenant_not_found`.

### List tenants

```bash
curl -s 'localhost:8080/v1/tenants?status=active&limit=20'
curl -s 'localhost:8080/v1/tenants?limit=20&cursor=0192f3a1-7c2e-7b4d-9f10-3a5c8e2d1b44'
```

```json
{
  "data": [ { "id": "...", "slug": "acme-eu", "status": "active", "...": "..." } ],
  "next_cursor": "0192f3a1-7c2e-7b4d-9f10-3a5c8e2d1b44"
}
```

| Query param | Meaning                                                   | Default |
| ----------- | --------------------------------------------------------- | ------- |
| `status`    | Filter by tenant status                                   | all     |
| `limit`     | Page size, 1–100                                          | 20      |
| `cursor`    | `next_cursor` from the previous page                      | start   |

Pagination is keyset-based on `id` (UUIDv7, so creation order). It is stable
under concurrent inserts, unlike offset pagination. `next_cursor` is `null` on
the last page.

### Update a tenant

`version` is required (optimistic locking). Only `name` is mutable; sending
`slug` returns 422.

```bash
curl -s -X PATCH localhost:8080/v1/tenants/0192f3a1-7c2e-7b4d-9f10-3a5c8e2d1b44 \
  -H 'Content-Type: application/json' \
  -d '{"version": 2, "name": "Acme EU"}'
```

`202 Accepted` with `{tenant, task}`; the tenant is now `updating`, its
`version` is incremented and the new name is stored as desired state.

If the tenant changed since you read it: `409 tenant_version_conflict` (refetch
and retry). If it's not `active`: `409 tenant_update_not_allowed` (wait).

### Delete a tenant

```bash
curl -s -X DELETE localhost:8080/v1/tenants/0192f3a1-7c2e-7b4d-9f10-3a5c8e2d1b44
```

`202 Accepted` with `{tenant, task}`; the tenant is `destroying`, and becomes
`destroyed` when the worker finishes. Allowed from `active` and `failed`.
The tenant row is kept (and stays readable) so its slug remains reserved.

### Tasks

```bash
curl -s 'localhost:8080/v1/tasks?tenant_id=0192f3a1-7c2e-7b4d-9f10-3a5c8e2d1b44'
curl -s 'localhost:8080/v1/tasks?status=failed&type=deploy&limit=50'
curl -s localhost:8080/v1/tasks/0192f3a1-7c2f-7000-8000-000000000001
```

| Query param | Meaning                                      |
| ----------- | -------------------------------------------- |
| `tenant_id` | Tasks for one tenant                         |
| `status`    | `accepted`, `in_progress`, `done`, `failed`  |
| `type`      | `deploy`, `update`, `destroy`                |
| `limit`, `cursor` | As for tenants                         |

### Errors

Every error, including unknown routes and wrong methods, has the same shape:

```json
{
  "error": {
    "code": "tenant_version_conflict",
    "message": "tenant version does not match; refetch and retry",
    "details": { "field": "reason, for validation errors only" }
  }
}
```

| Code                        | HTTP | When                                                                |
| --------------------------- | ---- | ------------------------------------------------------------------- |
| `tenant_not_found`          | 404  | Unknown or malformed tenant ID                                       |
| `task_not_found`            | 404  | Unknown or malformed task ID                                         |
| `tenant_already_exists`     | 409  | Slug taken (including by a destroyed tenant)                         |
| `tenant_update_not_allowed` | 409  | PATCH or DELETE not allowed in the tenant's current status           |
| `tenant_version_conflict`   | 409  | PATCH `version` doesn't match the current version                    |
| `validation_error`          | 422  | Well-formed JSON with invalid values; `details` lists each field     |
| `invalid_request`           | 400  | Malformed JSON, unknown fields, wrong types, oversized body          |
| `route_not_found`           | 404  | No such path                                                         |
| `method_not_allowed`        | 405  | Path exists, method doesn't (`Allow` header lists valid methods)     |
| `not_ready`                 | 503  | Readiness check failed                                               |
| `internal_error`            | 500  | Unexpected failure; details are logged, never returned               |

The first five codes are the contract from the brief.
`tenant_update_not_allowed` covers DELETE as well as PATCH, because the
contract has no separate delete code.

**PATCH error precedence:** `tenant_not_found` → `tenant_update_not_allowed`
→ `tenant_version_conflict`. A busy tenant can't be updated even with a fresh
version, so "not allowed" is the more useful answer.

---

## Messaging

| Queue              | Direction             | Producer      | Consumer          |
| ------------------ | --------------------- | ------------- | ----------------- |
| `tasks`            | control-plane → worker | Outbox relay  | Worker simulator  |
| `task-updates`     | worker → control-plane | Worker        | Progress consumer |
| `tasks.dlq`, `task-updates.dlq` | dead letters | —        | Inspected manually |

All queues and messages are durable/persistent.

### Task message (`tasks`)

The task *is* the event; AMQP `message_id` = task `id`.

```json
{
  "id": "0192f3a1-7c2f-7000-8000-000000000001",
  "type": "deploy",
  "tenant_id": "0192f3a1-7c2e-7b4d-9f10-3a5c8e2d1b44",
  "status": "accepted",
  "created_at": "2026-10-12T05:30:00.123Z"
}
```

### Progress update (`task-updates`)

`update_id` is the idempotency key (also AMQP `message_id`).

```json
{
  "update_id": "0192f3a2-0000-7000-8000-00000000abcd",
  "task_id": "0192f3a1-7c2f-7000-8000-000000000001",
  "status": "done",
  "occurred_at": "2026-10-12T05:30:03.456Z"
}
```

### Consumer guarantees

| Requirement      | Mechanism                                                                                   |
| ---------------- | ------------------------------------------------------------------------------------------- |
| Idempotent       | `update_id` inserted into `processed_messages` in the same transaction as the state change; a redelivery hits the primary key and is acked as a no-op |
| Order-tolerant   | Forward-only conditional UPDATE (`WHERE status = ANY(predecessors)`); a stale update matches zero rows and is recorded as `stale_ignored` |
| Resilient        | Broker reconnect with exponential backoff; transient DB errors are nacked and retried; malformed messages or unknown tasks go to the DLQ and are acked so they never block the queue |

---

## Database schema

```mermaid
erDiagram
    tenants ||--o{ tasks : "has"
    tasks ||--o{ processed_messages : "updated by (logical, no FK)"

    tenants {
        uuid id PK "UUIDv7"
        text slug UK "k8s-namespace format"
        text name "non-empty"
        text status "CHECK enum"
        bigint version "optimistic lock"
        timestamptz created_at
        timestamptz updated_at
    }
    tasks {
        uuid id PK "UUIDv7, also the event id"
        text type "deploy, update, destroy"
        uuid tenant_id FK
        text status "accepted .. done, failed"
        timestamptz published_at "NULL = still in outbox"
        timestamptz created_at
        timestamptz updated_at
    }
    processed_messages {
        uuid update_id PK "idempotency key"
        uuid task_id "no FK, see below"
        text status "status the update claimed"
        text outcome "applied, stale_ignored"
        timestamptz processed_at
    }
```

### `tenants`

```sql
CREATE TABLE tenants (
    id          uuid           PRIMARY KEY,
    slug        text           NOT NULL,
    name        text           NOT NULL,
    status      text           NOT NULL,
    version     bigint         NOT NULL DEFAULT 1,
    created_at  timestamptz(3) NOT NULL,
    updated_at  timestamptz(3) NOT NULL,

    CONSTRAINT tenants_slug_key       UNIQUE (slug),
    CONSTRAINT tenants_slug_format    CHECK (slug ~ '^[a-z][-a-z0-9]{1,26}[a-z0-9]$'),
    CONSTRAINT tenants_name_nonempty  CHECK (length(btrim(name)) > 0),
    CONSTRAINT tenants_status_valid   CHECK (status IN
        ('provisioning','active','updating','destroying','destroyed','failed'))
);
CREATE INDEX tenants_status_id_idx ON tenants (status, id);
```

### `tasks` (also the outbox)

```sql
CREATE TABLE tasks (
    id           uuid           PRIMARY KEY,
    type         text           NOT NULL CHECK (type IN ('deploy','update','destroy')),
    tenant_id    uuid           NOT NULL REFERENCES tenants(id),
    status       text           NOT NULL CHECK (status IN ('accepted','in_progress','done','failed')),
    published_at timestamptz(3),
    created_at   timestamptz(3) NOT NULL,
    updated_at   timestamptz(3) NOT NULL
);
CREATE INDEX tasks_tenant_id_idx   ON tasks (tenant_id, id);
CREATE INDEX tasks_unpublished_idx ON tasks (id) WHERE published_at IS NULL;
CREATE UNIQUE INDEX tasks_one_open_per_tenant
    ON tasks (tenant_id) WHERE status IN ('accepted','in_progress');
```

### `processed_messages`

```sql
CREATE TABLE processed_messages (
    update_id    uuid           PRIMARY KEY,
    task_id      uuid           NOT NULL,
    status       text           NOT NULL,
    outcome      text           NOT NULL CHECK (outcome IN ('applied','stale_ignored')),
    processed_at timestamptz(3) NOT NULL
);
```

| Object                       | Purpose                                                                                  |
| ---------------------------- | ---------------------------------------------------------------------------------------- |
| `tenants_slug_key`           | Settles concurrent creates: exactly one insert wins; the loser gets SQLSTATE `23505` → `tenant_already_exists` |
| `tenants_slug_format`        | Slug rule enforced by the DB as well as the API                                          |
| `tenants.version`            | Optimistic lock, bumped on every mutation including worker-applied ones                 |
| `tasks.published_at`         | The outbox flag: `NULL` = committed but not yet sent                                     |
| `tasks_unpublished_idx`      | Partial index; stays tiny, so the relay poll is cheap regardless of history size         |
| `tasks_one_open_per_tenant`  | Defence in depth for "one open task per tenant" (the guards are the real mechanism)      |
| `processed_messages`         | Idempotency ledger; `outcome` makes duplicate/stale handling observable in tests         |

`processed_messages.task_id` has no foreign key on purpose: an update for an
unknown task is a poison message and is dead-lettered before it reaches this
table.

---

## Design decisions and trade-offs

| Decision | Rationale | Trade-off |
| -------- | --------- | --------- |
| **Transactional outbox, using the `tasks` table** | The brief says the task *is* the event; a `published_at` column avoids a second table. Publishing never happens before commit or on rollback, and nothing is lost on crash. | A crash between publish and marking re-publishes: at-least-once, handled by consumer idempotency. |
| **Relay = ticker + in-process nudge** | The nudge gives millisecond latency in the normal case; the ticker is the safety net for crashes, outages and other replicas. | Polling load (one cheap indexed query per tick). `LISTEN/NOTIFY` or CDC would be the next step at scale. |
| **Relay publishes in creation order** (`ORDER BY id`, `SKIP LOCKED`) | FIFO fairness; replicas never claim the same rows. | Not a strict global order (commit order ≠ ID order). Correctness doesn't depend on it: one open task per tenant + order-tolerant consumer. |
| **Invariants in the database** (unique slug, conditional UPDATEs) | Concurrency correctness without application locks; works across replicas. | Error disambiguation needs a re-read after a zero-row UPDATE. |
| **One conditional UPDATE per mutation** | `WHERE id=$1 AND version=$2 AND status = ANY($3)`: two concurrent PATCHes with the same version can't both succeed. | — |
| **RabbitMQ over Kafka** | Work-queue semantics, per-message ack/nack, native DLX for poison messages, easy manual publishing for tests. | No retained, replayable log. Kafka would win if task events became a shared stream for many consumers. |
| **Forward jump `accepted → done/failed`** | If `done` arrives before `in_progress`, the work did finish; rejecting it would strand the task forever. | Task history may skip `in_progress`. |
| **Only DELETE recovers a `failed` tenant** | Matches the brief exactly. | No "retry deploy"; a customer would sign up again with a new slug. |
| **Slug reserved forever, immutable** | Slugs become namespaces, DNS names and URLs; reuse could point stale references at a new customer. | Slugs are never reclaimed. |
| **UUIDv7 generated in Go** | Task ID must exist before commit (it's the event ID); time-ordered for index locality and cursor pagination. | 16-byte keys vs 8-byte `bigint`. |
| **`text` + `CHECK` instead of Postgres `ENUM`** | Adding a status is a one-line migration; scans into Go strings. | Slightly larger storage than enums. |
| **Single process for API, relay and consumer** | Simple to run, demo and reason about. | Components are flag-selectable and replica-safe, so they can be split into separate deployments without code changes. |
| **No retention for `processed_messages`** | Out of scope. | Would need pruning (e.g. older than the broker's max redelivery window) in production. |

---

## Project layout

```
cmd/
  control-plane/        main: API + outbox relay + progress consumer
  worker/               main: worker simulator
internal/
  domain/               state machines, entities, errors, validation (pure, no I/O)
  api/                  HTTP routing, handlers, DTOs, error mapping
  store/                pgx repositories, conditional UPDATEs, migrations
  outbox/               relay: unpublished tasks → broker
  consumer/             progress updates → tasks/tenants
  broker/               AMQP topology, reconnect/backoff
migrations/             goose SQL migrations (embedded)
scripts/                negative-scenario helpers (publish duplicate/stale/poison updates)
Dockerfile              multi-stage; one image, two binaries
compose.yaml            postgres, rabbitmq, control-plane, worker (+ test profile)
```
