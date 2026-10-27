-- Роль 3: DDL и загрузка DDS -> CDM (витрины для Metabase).
-- Контракт: docs/contracts.md, описание витрин и карточек: docs/dashboard.md.
-- Источник — только схема dds. Файл выполняется целиком в одной транзакции
-- (storage.run_sql_file), COMMIT не пишем: витрины пересобираются все вместе или никак.
--
-- Общие правила:
--  * счётчики, карта и доли — только по текущим версиям (is_current), иначе SCD2 завысит числа;
--  * история версий используется в revision_summary и review_dynamics (time travel);
--  * аналитика — только event_type = 'earthquake', прочие типы видны в data_quality;
--  * NULL-магнитуда и NULL-глубина — отдельная категория 'нет данных';
--  * таблицы не пересоздаются (TRUNCATE + INSERT), чтобы Metabase не терял поля карточек.

---------------------------------------------------------------------------------------------
-- 1. DDL (идемпотентно)
---------------------------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS cdm;

-- Одно текущее землетрясение: карта и детальная таблица.
CREATE TABLE IF NOT EXISTS cdm.event_map (
    event_id           text PRIMARY KEY,
    event_time         timestamptz NOT NULL,
    event_date         date NOT NULL,
    latitude           numeric NOT NULL,
    longitude          numeric NOT NULL,
    magnitude          numeric,
    magnitude_type     text,
    mag_bin            numeric,
    mag_class          text NOT NULL,
    depth_km           numeric,
    depth_class        text NOT NULL,
    review_status      text NOT NULL,
    network_code       text NOT NULL,
    place              text,
    tsunami_flag       boolean NOT NULL,
    alert_level        text,
    versions_cnt       integer NOT NULL,
    first_observed_at  timestamptz NOT NULL,
    source_updated_at  timestamptz NOT NULL
);

-- Активность: дата события UTC × сеть × корзина магнитуды (шаг 0.5) × класс глубины.
-- Средние не хранятся (неаддитивны): avg = sum_* / *_filled_cnt после любой агрегации.
CREATE TABLE IF NOT EXISTS cdm.daily_activity (
    event_date         date NOT NULL,
    network_code       text NOT NULL,
    mag_bin            numeric,
    mag_class          text NOT NULL,
    depth_class        text NOT NULL,
    events_cnt         integer NOT NULL,
    reviewed_cnt       integer NOT NULL,
    automatic_cnt      integer NOT NULL,
    deleted_cnt        integer NOT NULL,
    mag_filled_cnt     integer NOT NULL,
    depth_filled_cnt   integer NOT NULL,
    place_filled_cnt   integer NOT NULL,
    revised_cnt        integer NOT NULL,
    tsunami_flag_cnt   integer NOT NULL,
    sum_mag            numeric,
    max_mag            numeric,
    sum_depth_km       numeric,
    max_depth_km       numeric,
    UNIQUE NULLS NOT DISTINCT (event_date, network_code, mag_bin, depth_class)
);

-- Проверка событий: сеть × возраст события (сутки до момента данных) × класс магнитуды.
-- Знаменатель доли reviewed — все события группы, включая automatic и deleted.
CREATE TABLE IF NOT EXISTS cdm.review_summary (
    network_code       text NOT NULL,
    event_age_days     integer NOT NULL,
    mag_class          text NOT NULL,
    events_cnt         integer NOT NULL,
    reviewed_cnt       integer NOT NULL,
    automatic_cnt      integer NOT NULL,
    deleted_cnt        integer NOT NULL,
    reviewed_share     numeric(5, 4) NOT NULL,
    data_as_of         timestamptz NOT NULL,
    PRIMARY KEY (network_code, event_age_days, mag_class)
);

-- Уточнения: текущее землетрясение, первая наблюдённая версия против текущей.
-- Относится только к периоду наблюдения нашим пайплайном.
CREATE TABLE IF NOT EXISTS cdm.revision_summary (
    event_id              text PRIMARY KEY,
    event_date            date NOT NULL,
    network_code          text NOT NULL,
    mag_class             text NOT NULL,
    versions_cnt          integer NOT NULL,
    is_revised            boolean NOT NULL,
    first_observed_at     timestamptz NOT NULL,
    last_changed_at       timestamptz NOT NULL,
    first_magnitude       numeric,
    current_magnitude     numeric,
    magnitude_delta       numeric,
    magnitude_changed     boolean NOT NULL,
    first_status          text NOT NULL,
    current_status        text NOT NULL,
    status_changed        boolean NOT NULL,
    first_depth_km        numeric,
    current_depth_km      numeric,
    depth_delta_km        numeric,
    location_shift_km     numeric NOT NULL,
    hours_to_review       numeric
);

-- Time travel по SCD2: состояние каталога на каждый час наблюдения.
-- Для среза t берутся версии с valid_from <= t < valid_to.
CREATE TABLE IF NOT EXISTS cdm.review_dynamics (
    snapshot_at        timestamptz NOT NULL,
    network_code       text NOT NULL,
    mag_class          text NOT NULL,
    events_cnt         integer NOT NULL,
    reviewed_cnt       integer NOT NULL,
    automatic_cnt      integer NOT NULL,
    mag_filled_cnt     integer NOT NULL,
    sum_mag            numeric,
    PRIMARY KEY (snapshot_at, network_code, mag_class)
);

-- Здоровье конвейера: запуски STG -> DDS из dds.load_log.
CREATE TABLE IF NOT EXISTS cdm.pipeline_health (
    load_id            bigint PRIMARY KEY,
    started_at         timestamptz NOT NULL,
    run_kind           text NOT NULL,
    stg_id_from        bigint NOT NULL,
    stg_id_to          bigint NOT NULL,
    rows_read          integer NOT NULL,
    rows_rejected      integer NOT NULL,
    rows_stale         integer NOT NULL,
    new_events         integer NOT NULL,
    versions_inserted  integer NOT NULL,
    versions_closed    integer NOT NULL
);

-- Качество данных: отбраковка, полнота полей, типы событий вне аналитики.
CREATE TABLE IF NOT EXISTS cdm.data_quality (
    check_group        text NOT NULL,
    check_name         text NOT NULL,
    records_cnt        integer NOT NULL,
    total_cnt          integer NOT NULL,
    share              numeric(7, 6),
    PRIMARY KEY (check_group, check_name)
);

---------------------------------------------------------------------------------------------
-- 2. Общие классы (временные функции живут только в этой сессии и не попадают в CDM)
---------------------------------------------------------------------------------------------
-- Префикс-номер нужен, чтобы Metabase сортировал классы по смыслу, а не по алфавиту.
CREATE OR REPLACE FUNCTION pg_temp.mag_class(m numeric) RETURNS text
LANGUAGE sql IMMUTABLE AS $$
    SELECT CASE WHEN m IS NULL THEN '6. нет данных'
                WHEN m < 2.5   THEN '1. M<2.5'
                WHEN m < 4     THEN '2. M2.5–4'
                WHEN m < 5     THEN '3. M4–5'
                WHEN m < 6     THEN '4. M5–6'
                ELSE                '5. M6+' END
$$;

CREATE OR REPLACE FUNCTION pg_temp.depth_class(d numeric) RETURNS text
LANGUAGE sql IMMUTABLE AS $$
    SELECT CASE WHEN d IS NULL THEN '4. нет данных'
                WHEN d < 70    THEN '1. мелкие (<70 км)'
                WHEN d < 300   THEN '2. средние (70–300 км)'
                ELSE                '3. глубокие (≥300 км)' END
$$;

---------------------------------------------------------------------------------------------
-- 3. Загрузка
---------------------------------------------------------------------------------------------
-- Две пересборки CDM параллельно не идут; блокировка освобождается в конце транзакции.
SELECT pg_advisory_xact_lock(731005);

TRUNCATE cdm.event_map, cdm.daily_activity, cdm.review_summary, cdm.revision_summary,
         cdm.review_dynamics, cdm.pipeline_health, cdm.data_quality;

-- Момент, на который актуальны данные: последний запуск DDS (или последнее наблюдение).
CREATE TEMP TABLE tmp_as_of ON COMMIT DROP AS
SELECT COALESCE((SELECT max(started_at) FROM dds.load_log),
                (SELECT max(valid_from) FROM dds.event_versions),
                now()) AS data_as_of;

-- 3.1 Текущие землетрясения — основа всех счётчиков.
CREATE TEMP TABLE tmp_cur ON COMMIT DROP AS
SELECT v.event_id, v.event_time, (v.event_time AT TIME ZONE 'UTC')::date AS event_date,
       v.latitude, v.longitude, v.magnitude, v.magnitude_type,
       floor(v.magnitude * 2) / 2               AS mag_bin,
       pg_temp.mag_class(v.magnitude)           AS mag_class,
       v.depth_km, pg_temp.depth_class(v.depth_km) AS depth_class,
       v.review_status, v.network_code, v.place, v.tsunami_flag, v.alert_level,
       v.valid_from, v.source_updated_at, e.first_observed_at, h.versions_cnt
FROM dds.event_versions v
JOIN dds.events e ON e.event_id = v.event_id
JOIN (SELECT event_id, count(*) AS versions_cnt FROM dds.event_versions GROUP BY event_id) h
  ON h.event_id = v.event_id
WHERE v.is_current AND v.event_type = 'earthquake';

INSERT INTO cdm.event_map
SELECT event_id, event_time, event_date, latitude, longitude, magnitude, magnitude_type,
       mag_bin, mag_class, depth_km, depth_class, review_status, network_code, place,
       tsunami_flag, alert_level, versions_cnt, first_observed_at, source_updated_at
FROM tmp_cur;

-- 3.2 Активность по дням.
INSERT INTO cdm.daily_activity
SELECT event_date, network_code, mag_bin, mag_class, depth_class,
       count(*),
       count(*) FILTER (WHERE review_status = 'reviewed'),
       count(*) FILTER (WHERE review_status = 'automatic'),
       count(*) FILTER (WHERE review_status = 'deleted'),
       count(magnitude), count(depth_km), count(place),
       count(*) FILTER (WHERE versions_cnt > 1),
       count(*) FILTER (WHERE tsunami_flag),
       sum(magnitude), max(magnitude), sum(depth_km), max(depth_km)
FROM tmp_cur
GROUP BY event_date, network_code, mag_bin, mag_class, depth_class;

-- 3.3 Доля reviewed в зависимости от возраста события.
INSERT INTO cdm.review_summary
SELECT c.network_code,
       GREATEST(floor(extract(epoch FROM a.data_as_of - c.event_time) / 86400), 0)::int,
       c.mag_class,
       count(*),
       count(*) FILTER (WHERE c.review_status = 'reviewed'),
       count(*) FILTER (WHERE c.review_status = 'automatic'),
       count(*) FILTER (WHERE c.review_status = 'deleted'),
       round(count(*) FILTER (WHERE c.review_status = 'reviewed')::numeric / count(*), 4),
       a.data_as_of
FROM tmp_cur c CROSS JOIN tmp_as_of a
GROUP BY 1, 2, 3, a.data_as_of;

-- 3.4 Уточнения: первая наблюдённая версия против текущей.
INSERT INTO cdm.revision_summary
SELECT c.event_id, c.event_date, c.network_code, c.mag_class, c.versions_cnt,
       c.versions_cnt > 1,
       c.first_observed_at, c.valid_from,
       f.magnitude, c.magnitude, c.magnitude - f.magnitude,
       f.magnitude IS DISTINCT FROM c.magnitude,
       f.review_status, c.review_status, f.review_status <> c.review_status,
       f.depth_km, c.depth_km, c.depth_km - f.depth_km,
       -- Равнопрямоугольное приближение: для сдвигов в километры точности достаточно.
       round(111.2 * sqrt(power(c.latitude - f.latitude, 2)
                          + power((c.longitude - f.longitude)
                                  * cos(radians((c.latitude + f.latitude) / 2)), 2))::numeric, 3),
       -- Часы от первого наблюдения до появления статуса reviewed (если он не был сразу).
       CASE WHEN f.review_status <> 'reviewed' AND r.reviewed_from IS NOT NULL
            THEN round(extract(epoch FROM r.reviewed_from - c.first_observed_at) / 3600, 2) END
FROM tmp_cur c
JOIN (SELECT DISTINCT ON (event_id) * FROM dds.event_versions
      ORDER BY event_id, valid_from) f ON f.event_id = c.event_id
LEFT JOIN (SELECT event_id, min(valid_from) AS reviewed_from FROM dds.event_versions
           WHERE review_status = 'reviewed' GROUP BY event_id) r ON r.event_id = c.event_id;

-- 3.5 Time travel: срез каждый час наблюдения и финальный срез на момент данных.
CREATE TEMP TABLE tmp_snap ON COMMIT DROP AS
SELECT DISTINCT snapshot_at FROM (
    SELECT generate_series(date_trunc('hour', min(v.valid_from)) + interval '1 hour',
                           max(a.data_as_of), interval '1 hour') AS snapshot_at
    FROM dds.event_versions v CROSS JOIN tmp_as_of a
    UNION ALL
    SELECT data_as_of FROM tmp_as_of WHERE EXISTS (SELECT 1 FROM dds.event_versions)
) s;

INSERT INTO cdm.review_dynamics
SELECT s.snapshot_at, v.network_code, pg_temp.mag_class(v.magnitude),
       count(*),
       count(*) FILTER (WHERE v.review_status = 'reviewed'),
       count(*) FILTER (WHERE v.review_status = 'automatic'),
       count(v.magnitude),
       sum(v.magnitude)
FROM tmp_snap s
JOIN dds.event_versions v
  ON tstzrange(v.valid_from, v.valid_to, '[)') @> s.snapshot_at
WHERE v.event_type = 'earthquake'
GROUP BY 1, 2, 3;

-- 3.6 Журнал конвейера.
INSERT INTO cdm.pipeline_health
SELECT load_id, started_at,
       CASE WHEN rows_read = 0         THEN 'нет новых строк'
            WHEN versions_inserted = 0 THEN 'без изменений'
            ELSE                            'есть изменения' END,
       stg_id_from, stg_id_to, rows_read, rows_rejected, rows_stale,
       new_events, versions_inserted, versions_closed
FROM dds.load_log;

-- 3.7 Качество данных.
INSERT INTO cdm.data_quality
SELECT 'Отбраковка наблюдений', r.reason, count(*), t.total,
       round(count(*)::numeric / NULLIF(t.total, 0), 6)
FROM dds.rejected_observations r
CROSS JOIN (SELECT COALESCE(sum(rows_read), 0)::int AS total FROM dds.load_log) t
GROUP BY r.reason, t.total;

INSERT INTO cdm.data_quality
SELECT 'Полнота полей землетрясений', x.name, x.cnt, t.total,
       round(x.cnt::numeric / NULLIF(t.total, 0), 6)
FROM (SELECT count(*)::int AS total FROM tmp_cur) t
CROSS JOIN LATERAL (VALUES
    ('нет магнитуды',      (SELECT count(*) FROM tmp_cur WHERE magnitude IS NULL)::int),
    ('нет типа магнитуды', (SELECT count(*) FROM tmp_cur WHERE magnitude_type IS NULL)::int),
    ('нет глубины',        (SELECT count(*) FROM tmp_cur WHERE depth_km IS NULL)::int),
    ('нет описания места', (SELECT count(*) FROM tmp_cur WHERE place IS NULL)::int)
) x(name, cnt)
WHERE t.total > 0;

INSERT INTO cdm.data_quality
SELECT 'Типы событий (текущие версии)', v.event_type, count(*), t.total,
       round(count(*)::numeric / NULLIF(t.total, 0), 6)
FROM dds.event_versions v
CROSS JOIN (SELECT count(*)::int AS total FROM dds.event_versions WHERE is_current) t
WHERE v.is_current
GROUP BY v.event_type, t.total;

-- Страховка к ALTER DEFAULT PRIVILEGES из sql/init: BI читает только CDM.
GRANT USAGE ON SCHEMA cdm TO bi_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA cdm TO bi_reader;
