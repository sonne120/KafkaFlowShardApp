-- Read-model schema for the CQRS query side.
-- Runs once on first container boot (empty data dir).

CREATE EXTENSION IF NOT EXISTS pg_ivm;

-- Append-only ledger: every projected packet, deduplicated by the cross-system business key.
-- The UNIQUE primary key is what actually guarantees exactly-once projection under at-least-once
-- Kafka delivery (Redis is only a fast-path filter in front of this).
CREATE TABLE IF NOT EXISTS packet_ledger (
    transaction_id  text PRIMARY KEY,
    client_id       text        NOT NULL,
    version         bigint      NOT NULL DEFAULT 0,
    proto           text        NOT NULL,
    source_ip       text,
    dest_ip         text,
    source_port     integer,
    dest_port       integer,
    stored_at       timestamptz,
    payload         jsonb,
    ingested_at     timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_packet_ledger_client ON packet_ledger (client_id);
CREATE INDEX IF NOT EXISTS ix_packet_ledger_proto  ON packet_ledger (proto);

-- Last-value state per client, protected by the version guard in the writer's UPSERT.
-- Demonstrates the ordering path; commutative count/sum views don't need it.
CREATE TABLE IF NOT EXISTS client_state (
    client_id   text PRIMARY KEY,
    version     bigint      NOT NULL,
    last_proto  text        NOT NULL,
    updated_at  timestamptz NOT NULL DEFAULT now()
);

-- Incrementally Maintainable Materialized View: per-protocol counts maintained on every INSERT
-- by a pg_ivm trigger — no REFRESH, no query-time aggregation. count/min/max are commutative,
-- so projection order is irrelevant here.
SELECT create_immv(
    'packet_stats_by_proto',
    'SELECT proto,
            count(*)        AS packet_count,
            min(stored_at)  AS first_seen,
            max(stored_at)  AS last_seen
     FROM packet_ledger
     GROUP BY proto'
);
