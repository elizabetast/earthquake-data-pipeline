"""Сохранение STG и точки расширения для SQL участников 2 и 3."""
import hashlib
import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

LOG = logging.getLogger(__name__)


def connect():
    from airflow.hooks.base import BaseHook
    import psycopg2
    c = BaseHook.get_connection("warehouse")
    return psycopg2.connect(host=c.host, port=c.port, dbname=c.schema,
                            user=c.login, password=c.password)


def load(batch_id, days_back=None):
    from psycopg2.extras import Json, execute_values
    from earthquake_common.api import fetch_range
    if days_back is not None and (type(days_back) is not int or not 1 <= days_back <= 365):
        raise ValueError("days_back должен быть целым числом от 1 до 365")
    conn = connect()
    try:
        with conn, conn.cursor() as cur:
            # Защита от одновременной ручной загрузки и scheduled run.
            cur.execute("SELECT pg_advisory_xact_lock(731003)")
            cur.execute("SELECT event_count FROM stg.batches WHERE batch_id=%s", (batch_id,))
            existing = cur.fetchone()
            if existing:
                LOG.info("Batch %s уже сохранён: %s строк", batch_id, existing[0])
                return existing[0]
            now = datetime.now(timezone.utc)
            cur.execute("""SELECT scope_start, request_started_at FROM stg.batches
                           WHERE mode IN ('initial', 'incremental')
                           ORDER BY request_started_at DESC LIMIT 1""")
            previous = cur.fetchone()
            mode = "backfill" if days_back is not None else ("incremental" if previous else "initial")
            start = now - timedelta(days=days_back or 90)
            updated_after = None
            if mode == "incremental":
                start = previous[0]
                # Перекрытие на случай обновления во время предыдущего запроса.
                updated_after = previous[1] - timedelta(minutes=10)
            cur.execute("""INSERT INTO stg.batches
                           (batch_id,mode,scope_start,request_started_at,response_count,event_count)
                           VALUES (%s,%s,%s,%s,0,0)""", (batch_id, mode, start, now))
            LOG.info("USGS batch=%s mode=%s start=%s end=%s updatedafter=%s",
                     batch_id, mode, start, now, updated_after)
            response_count = event_count = 0
            for url, params, payload, fetched_at in fetch_range(start, now, updated_after):
                cur.execute("""INSERT INTO stg.api_responses
                               (batch_id,request_url,request_params,payload,fetched_at)
                               VALUES (%s,%s,%s,%s,%s) RETURNING id""",
                            (batch_id, url, Json(params), Json(payload), fetched_at))
                response_id = cur.fetchone()[0]
                rows = []
                for event in payload["features"]:
                    digest = hashlib.sha256(json.dumps(event, sort_keys=True,
                                             separators=(",", ":")).encode()).hexdigest()
                    updated = datetime.fromtimestamp(event["properties"]["updated"] / 1000, timezone.utc)
                    rows.append((batch_id, response_id, event["id"], updated, digest,
                                 Json(event), fetched_at))
                if rows:
                    # Одно событие может встретиться на общей границе двух окон.
                    result = execute_values(cur, """INSERT INTO stg.earthquake_raw
                        (batch_id,response_id,event_id,source_updated_at,payload_hash,payload,loaded_at)
                        VALUES %s ON CONFLICT DO NOTHING RETURNING id""", rows, fetch=True)
                    event_count += len(result)
                response_count += 1
                LOG.info("USGS: ответ %s, событий в ответе %s, сохранено всего %s",
                         response_count, len(payload["features"]), event_count)
            cur.execute("""UPDATE stg.batches SET response_count=%s,event_count=%s,
                           completed_at=clock_timestamp() WHERE batch_id=%s""",
                        (response_count, event_count, batch_id))
        return event_count
    finally:
        conn.close()


def run_sql_file(filename):
    from airflow.exceptions import AirflowSkipException
    sql = (Path(__file__).resolve().parents[2] / "sql" / filename).read_text()
    if not any(line.strip() and not line.lstrip().startswith("--") for line in sql.splitlines()):
        raise AirflowSkipException("SQL роли ещё не реализован: " + filename)
    conn = connect()
    try:
        with conn, conn.cursor() as cur:
            cur.execute(sql)
    finally:
        conn.close()
