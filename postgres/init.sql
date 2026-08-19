CREATE EXTENSION IF NOT EXISTS pg_ivm;


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

CREATE TABLE IF NOT EXISTS client_state (
    client_id   text PRIMARY KEY,
    version     bigint      NOT NULL,
    last_proto  text        NOT NULL,
    updated_at  timestamptz NOT NULL DEFAULT now()
);

SELECT create_immv(
    'packet_stats_by_proto',
    'SELECT proto,
            count(*)        AS packet_count,
            min(stored_at)  AS first_seen,
            max(stored_at)  AS last_seen
     FROM packet_ledger
     GROUP BY proto'
);
