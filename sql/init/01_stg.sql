CREATE SCHEMA IF NOT EXISTS stg;
CREATE SCHEMA IF NOT EXISTS dds;
CREATE SCHEMA IF NOT EXISTS cdm;

-- Запись появляется только после атомарного сохранения всей загрузки.
CREATE TABLE IF NOT EXISTS stg.batches (
    batch_id text PRIMARY KEY,
    mode text NOT NULL CHECK (mode IN ('initial', 'incremental', 'backfill')),
    scope_start timestamptz NOT NULL,
    request_started_at timestamptz NOT NULL,
    completed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    response_count integer NOT NULL,
    event_count integer NOT NULL
);
CREATE TABLE IF NOT EXISTS stg.api_responses (
    id bigserial PRIMARY KEY,
    batch_id text NOT NULL REFERENCES stg.batches(batch_id),
    request_url text NOT NULL,
    request_params jsonb NOT NULL,
    payload jsonb NOT NULL,
    fetched_at timestamptz NOT NULL,
    UNIQUE (batch_id, request_url)
);
CREATE TABLE IF NOT EXISTS stg.earthquake_raw (
    id bigserial PRIMARY KEY,
    batch_id text NOT NULL REFERENCES stg.batches(batch_id),
    response_id bigint NOT NULL REFERENCES stg.api_responses(id),
    event_id text NOT NULL,
    source_updated_at timestamptz NOT NULL,
    payload_hash text NOT NULL,
    payload jsonb NOT NULL,
    loaded_at timestamptz NOT NULL,
    UNIQUE (batch_id, event_id, source_updated_at, payload_hash)
);
CREATE INDEX IF NOT EXISTS earthquake_raw_event_idx ON stg.earthquake_raw(event_id, source_updated_at);
-- Отдельный пользователь BI видит только CDM.
DO $$ BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'bi_reader') THEN
        CREATE ROLE bi_reader LOGIN PASSWORD 'bi_reader';
    END IF;
END $$;
GRANT CONNECT ON DATABASE earthquakes TO bi_reader;
GRANT USAGE ON SCHEMA cdm TO bi_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA cdm TO bi_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA cdm GRANT SELECT ON TABLES TO bi_reader;
