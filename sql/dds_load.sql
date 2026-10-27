-- Роль 2: DDL и загрузка STG -> DDS с SCD2.
-- Контракт: docs/contracts.md, модель: docs/dds.md.
-- Файл выполняется целиком в одной транзакции (storage.run_sql_file), COMMIT не пишем:
-- версии, справочники, отбраковка и watermark либо сохраняются вместе, либо не сохраняются.

---------------------------------------------------------------------------------------------
-- 1. DDL (идемпотентно: sql/init выполняется только на пустом томе)
---------------------------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS dds;
-- Нужен для EXCLUDE-ограничения "интервалы версий одного события не пересекаются".
CREATE EXTENSION IF NOT EXISTS btree_gist;

-- Безопасное приведение JSON-значения к числу: строка-не-число или объект дают NULL,
-- а не падение всей загрузки.
CREATE OR REPLACE FUNCTION dds.json_num(v jsonb) RETURNS numeric
LANGUAGE sql IMMUTABLE AS $$
    SELECT CASE jsonb_typeof(v)
        WHEN 'number' THEN (v #>> '{}')::numeric
        WHEN 'string' THEN CASE WHEN btrim(v #>> '{}') ~ '^[-+]?[0-9]+(\.[0-9]+)?([eE][-+]?[0-9]+)?$'
                                THEN btrim(v #>> '{}')::numeric END
    END
$$;

-- Состояние загрузки: последний обработанный id stg.earthquake_raw.
CREATE TABLE IF NOT EXISTS dds.etl_state (
    process      text PRIMARY KEY,
    last_stg_id  bigint NOT NULL DEFAULT 0,
    updated_at   timestamptz NOT NULL DEFAULT now()
);

-- Справочники (SCD1: только добавление новых кодов).
CREATE TABLE IF NOT EXISTS dds.dim_network (
    network_code   text PRIMARY KEY,
    first_seen_at  timestamptz NOT NULL
);
CREATE TABLE IF NOT EXISTS dds.dim_event_type (
    event_type     text PRIMARY KEY,
    first_seen_at  timestamptz NOT NULL
);

-- Сущность события (SCD0): неизменяемые атрибуты бизнес-ключа.
CREATE TABLE IF NOT EXISTS dds.events (
    event_id           text PRIMARY KEY,
    first_observed_at  timestamptz NOT NULL,
    first_stg_id       bigint NOT NULL
);

-- Версии состояния события (SCD2). Интервал действия версии [valid_from, valid_to).
CREATE TABLE IF NOT EXISTS dds.event_versions (
    version_id         bigserial PRIMARY KEY,
    event_id           text NOT NULL REFERENCES dds.events(event_id),
    event_time         timestamptz NOT NULL,
    magnitude          numeric,
    magnitude_type     text,
    depth_km           numeric,
    longitude          numeric NOT NULL,
    latitude           numeric NOT NULL,
    place              text,
    event_type         text NOT NULL REFERENCES dds.dim_event_type(event_type),
    review_status      text NOT NULL,
    network_code       text NOT NULL REFERENCES dds.dim_network(network_code),
    tsunami_flag       boolean NOT NULL,
    alert_level        text,
    source_updated_at  timestamptz NOT NULL,
    stg_id             bigint NOT NULL,
    valid_from         timestamptz NOT NULL,
    valid_to           timestamptz,
    is_current         boolean NOT NULL,
    row_hash           text NOT NULL,
    CHECK (latitude BETWEEN -90 AND 90 AND longitude BETWEEN -180 AND 180),
    CHECK (valid_to IS NULL OR valid_to > valid_from),
    CHECK (is_current = (valid_to IS NULL)),
    EXCLUDE USING gist (event_id WITH =,
                        tstzrange(valid_from, valid_to, '[)') WITH &&)
);
-- Ровно одна текущая версия на событие.
CREATE UNIQUE INDEX IF NOT EXISTS event_versions_current_uq
    ON dds.event_versions(event_id) WHERE is_current;
CREATE INDEX IF NOT EXISTS event_versions_event_time_idx ON dds.event_versions(event_time);

-- Наблюдения, не прошедшие проверки качества (не теряем, а откладываем с причиной).
CREATE TABLE IF NOT EXISTS dds.rejected_observations (
    stg_id        bigint PRIMARY KEY,
    event_id      text,
    reason        text NOT NULL,
    rejected_at   timestamptz NOT NULL DEFAULT now()
);

-- Журнал запусков: видно, что повтор ничего не меняет.
CREATE TABLE IF NOT EXISTS dds.load_log (
    load_id             bigserial PRIMARY KEY,
    started_at          timestamptz NOT NULL DEFAULT now(),
    stg_id_from         bigint NOT NULL,
    stg_id_to           bigint NOT NULL,
    rows_read           integer NOT NULL,
    rows_rejected       integer NOT NULL,
    rows_stale          integer NOT NULL,
    new_events          integer NOT NULL,
    versions_inserted   integer NOT NULL,
    versions_closed     integer NOT NULL
);

INSERT INTO dds.etl_state(process) VALUES ('stg_to_dds') ON CONFLICT DO NOTHING;

---------------------------------------------------------------------------------------------
-- 2. Загрузка
---------------------------------------------------------------------------------------------
-- Две загрузки DDS параллельно не идут; блокировка освобождается в конце транзакции.
SELECT pg_advisory_xact_lock(731004);
-- Состояние читаем под блокировкой строки.
SELECT last_stg_id FROM dds.etl_state WHERE process = 'stg_to_dds' FOR UPDATE;

-- 2.1 Новые строки STG, разобранные по типизированным колонкам.
CREATE TEMP TABLE tmp_parsed ON COMMIT DROP AS
SELECT r.id AS stg_id, r.event_id, r.source_updated_at, r.loaded_at,
       CASE WHEN dds.json_num(p->'time') IS NOT NULL
            THEN to_timestamp(dds.json_num(p->'time') / 1000) END            AS event_time,
       dds.json_num(p->'mag')                                               AS magnitude,
       NULLIF(btrim(p->>'magType'), '')                                     AS magnitude_type,
       dds.json_num(g->'coordinates'->2)                                    AS depth_km,
       dds.json_num(g->'coordinates'->0)                                    AS longitude,
       dds.json_num(g->'coordinates'->1)                                    AS latitude,
       NULLIF(btrim(p->>'place'), '')                                       AS place,
       COALESCE(NULLIF(lower(btrim(p->>'type')), ''), 'unknown')            AS event_type,
       COALESCE(NULLIF(lower(btrim(p->>'status')), ''), 'unknown')          AS review_status,
       COALESCE(NULLIF(lower(btrim(p->>'net')), ''), 'unknown')             AS network_code,
       COALESCE(dds.json_num(p->'tsunami'), 0) <> 0                         AS tsunami_flag,
       NULLIF(lower(btrim(p->>'alert')), '')                                AS alert_level
FROM stg.earthquake_raw r
CROSS JOIN LATERAL (SELECT r.payload->'properties' AS p, r.payload->'geometry' AS g) j
WHERE r.id > (SELECT last_stg_id FROM dds.etl_state WHERE process = 'stg_to_dds');

-- 2.2 Проверки качества: время, координаты и правдоподобные диапазоны.
CREATE TEMP TABLE tmp_checked ON COMMIT DROP AS
SELECT t.*,
       CASE
           WHEN event_time IS NULL                         THEN 'нет времени события'
           WHEN longitude IS NULL OR latitude IS NULL      THEN 'нет координат'
           WHEN latitude NOT BETWEEN -90 AND 90
             OR longitude NOT BETWEEN -180 AND 180         THEN 'координаты вне диапазона'
           WHEN magnitude IS NOT NULL
            AND magnitude NOT BETWEEN -3 AND 10.5          THEN 'магнитуда вне диапазона'
           WHEN depth_km IS NOT NULL
            AND depth_km NOT BETWEEN -15 AND 1000          THEN 'глубина вне диапазона'
       END AS reject_reason
FROM tmp_parsed t;

INSERT INTO dds.rejected_observations(stg_id, event_id, reason)
SELECT stg_id, event_id, reject_reason FROM tmp_checked WHERE reject_reason IS NOT NULL
ON CONFLICT (stg_id) DO NOTHING;

-- 2.3 Хеш значимых бизнес-атрибутов. loaded_at, URL, updated и прочие технические
-- поля в хеш не входят, поэтому сами по себе новую версию не создают.
CREATE TEMP TABLE tmp_valid ON COMMIT DROP AS
SELECT c.*,
       md5(jsonb_build_array(event_time, magnitude, magnitude_type, depth_km, longitude,
                             latitude, place, event_type, review_status, network_code,
                             tsunami_flag, alert_level)::text) AS row_hash
FROM tmp_checked c WHERE reject_reason IS NULL;

-- 2.4 Справочники и сущности.
INSERT INTO dds.dim_network(network_code, first_seen_at)
SELECT network_code, min(loaded_at) FROM tmp_valid GROUP BY network_code
ON CONFLICT DO NOTHING;
INSERT INTO dds.dim_event_type(event_type, first_seen_at)
SELECT event_type, min(loaded_at) FROM tmp_valid GROUP BY event_type
ON CONFLICT DO NOTHING;

CREATE TEMP TABLE tmp_new_events ON COMMIT DROP AS
SELECT DISTINCT ON (v.event_id) v.event_id, v.loaded_at, v.stg_id
FROM tmp_valid v
WHERE NOT EXISTS (SELECT 1 FROM dds.events e WHERE e.event_id = v.event_id)
ORDER BY v.event_id, v.stg_id;
INSERT INTO dds.events(event_id, first_observed_at, first_stg_id)
SELECT event_id, loaded_at, stg_id FROM tmp_new_events;

-- 2.5 Выбор наблюдений, которые становятся версиями.
-- Правила (docs/dds.md):
--  a) одно наблюдение на (event_id, loaded_at): берём с max(source_updated_at), затем max(stg_id);
--  b) наблюдения идут в порядке stg_id; устаревшее (source_updated_at меньше уже принятого,
--     включая текущую версию в DDS) пропускается — так backfill не откатывает данные назад;
--     при равном source_updated_at побеждает более позднее наблюдение;
--  c) новая версия создаётся только если row_hash отличается от предыдущей принятой.
CREATE TEMP TABLE tmp_one_per_moment ON COMMIT DROP AS
SELECT DISTINCT ON (event_id, loaded_at) *
FROM tmp_valid
ORDER BY event_id, loaded_at, source_updated_at DESC, stg_id DESC;

CREATE TEMP TABLE tmp_ordered ON COMMIT DROP AS
SELECT o.*,
       cur.row_hash          AS cur_hash,
       cur.source_updated_at AS cur_updated,
       max(o.source_updated_at) OVER (PARTITION BY o.event_id ORDER BY o.stg_id
                                     ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS prev_max_updated
FROM tmp_one_per_moment o
LEFT JOIN dds.event_versions cur ON cur.event_id = o.event_id AND cur.is_current;

CREATE TEMP TABLE tmp_accepted ON COMMIT DROP AS
SELECT *
FROM tmp_ordered
WHERE source_updated_at >= GREATEST(COALESCE(cur_updated, '-infinity'),
                                    COALESCE(prev_max_updated, '-infinity'));

CREATE TEMP TABLE tmp_changes ON COMMIT DROP AS
SELECT *
FROM (SELECT a.*,
             COALESCE(lag(row_hash) OVER (PARTITION BY event_id ORDER BY stg_id), cur_hash)
                 AS prev_hash
      FROM tmp_accepted a) x
WHERE prev_hash IS DISTINCT FROM row_hash;

-- 2.6 Закрываем текущие версии событий, у которых появилось новое состояние.
CREATE TEMP TABLE tmp_closed ON COMMIT DROP AS
WITH first_change AS (
    SELECT event_id, min(loaded_at) AS new_from FROM tmp_changes GROUP BY event_id
), closed AS (
    UPDATE dds.event_versions v
       SET valid_to = f.new_from, is_current = false
      FROM first_change f
     WHERE v.event_id = f.event_id AND v.is_current
    RETURNING v.version_id
)
SELECT version_id FROM closed;

-- 2.7 Вставляем новые версии; последняя по событию становится текущей.
INSERT INTO dds.event_versions (
    event_id, event_time, magnitude, magnitude_type, depth_km, longitude, latitude, place,
    event_type, review_status, network_code, tsunami_flag, alert_level,
    source_updated_at, stg_id, valid_from, valid_to, is_current, row_hash)
SELECT event_id, event_time, magnitude, magnitude_type, depth_km, longitude, latitude, place,
       event_type, review_status, network_code, tsunami_flag, alert_level,
       source_updated_at, stg_id, loaded_at,
       lead(loaded_at) OVER w,
       lead(loaded_at) OVER w IS NULL,
       row_hash
FROM tmp_changes
WINDOW w AS (PARTITION BY event_id ORDER BY stg_id);

-- 2.8 Журнал и watermark (в той же транзакции, что и версии).
INSERT INTO dds.load_log(stg_id_from, stg_id_to, rows_read, rows_rejected, rows_stale,
                         new_events, versions_inserted, versions_closed)
SELECT s.last_stg_id,
       COALESCE((SELECT max(stg_id) FROM tmp_parsed), s.last_stg_id),
       (SELECT count(*) FROM tmp_parsed),
       (SELECT count(*) FROM tmp_checked WHERE reject_reason IS NOT NULL),
       (SELECT count(*) FROM tmp_one_per_moment) - (SELECT count(*) FROM tmp_accepted),
       (SELECT count(*) FROM tmp_new_events),
       (SELECT count(*) FROM tmp_changes),
       (SELECT count(*) FROM tmp_closed)
FROM dds.etl_state s WHERE s.process = 'stg_to_dds';

UPDATE dds.etl_state
   SET last_stg_id = GREATEST(last_stg_id, COALESCE((SELECT max(stg_id) FROM tmp_parsed), 0)),
       updated_at = now()
 WHERE process = 'stg_to_dds';
