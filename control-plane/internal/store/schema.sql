-- Control-plane schema. Applied by the control-plane on every startup, so
-- every statement must be idempotent (IF NOT EXISTS). For a POC this
-- replaces a migration tool; it cannot alter existing tables, so a schema
-- change means adding a new idempotent statement (or resetting the volume).

-- Tenants: customer environments.
CREATE TABLE IF NOT EXISTS tenants (
    id          uuid           PRIMARY KEY,                 -- UUIDv7, generated in Go
    slug        text           NOT NULL,
    name        text           NOT NULL,
    status      text           NOT NULL,
    version     bigint         NOT NULL DEFAULT 1,          -- optimistic lock
    created_at  timestamptz(3) NOT NULL,
    updated_at  timestamptz(3) NOT NULL,

    -- Unique forever, including destroyed tenants; settles concurrent creates.
    CONSTRAINT tenants_slug_key       UNIQUE (slug),
    CONSTRAINT tenants_slug_format    CHECK (slug ~ '^[a-z][-a-z0-9]{1,26}[a-z0-9]$'),
    CONSTRAINT tenants_name_nonempty  CHECK (length(btrim(name)) > 0),
    CONSTRAINT tenants_version_pos    CHECK (version >= 1),
    CONSTRAINT tenants_status_valid   CHECK (status IN
        ('provisioning', 'active', 'updating', 'destroying', 'destroyed', 'failed'))
);

-- List tenants filtered by status, paginated by id.
CREATE INDEX IF NOT EXISTS tenants_status_id_idx ON tenants (status, id);

-- Tasks: async operations on a tenant. Also the outbox (published_at).
CREATE TABLE IF NOT EXISTS tasks (
    id           uuid           PRIMARY KEY,                -- UUIDv7; also the event id
    type         text           NOT NULL,
    tenant_id    uuid           NOT NULL REFERENCES tenants (id),
    status       text           NOT NULL,
    published_at timestamptz(3),                            -- NULL = not yet sent to broker
    created_at   timestamptz(3) NOT NULL,
    updated_at   timestamptz(3) NOT NULL,

    CONSTRAINT tasks_type_valid   CHECK (type IN ('deploy', 'update', 'destroy')),
    CONSTRAINT tasks_status_valid CHECK (status IN ('accepted', 'in_progress', 'done', 'failed'))
);

-- Tasks for one tenant, paginated by id.
CREATE INDEX IF NOT EXISTS tasks_tenant_id_idx ON tasks (tenant_id, id);

-- Outbox relay poll: only unsent rows, so it stays small.
CREATE INDEX IF NOT EXISTS tasks_unpublished_idx ON tasks (id) WHERE published_at IS NULL;

-- Defence in depth: at most one open task per tenant. The tenant status
-- guards are the real mechanism; this makes a bug there impossible to persist.
CREATE UNIQUE INDEX IF NOT EXISTS tasks_one_open_per_tenant
    ON tasks (tenant_id) WHERE status IN ('accepted', 'in_progress');

-- Processed worker updates: the consumer's idempotency ledger.
CREATE TABLE IF NOT EXISTS processed_messages (
    update_id    uuid           PRIMARY KEY,                -- idempotency key
    task_id      uuid           NOT NULL,                   -- no FK: unknown tasks are dead-lettered first
    status       text           NOT NULL,                   -- status the update claimed
    outcome      text           NOT NULL,
    processed_at timestamptz(3) NOT NULL,

    CONSTRAINT processed_messages_outcome_valid CHECK (outcome IN ('applied', 'stale_ignored'))
);