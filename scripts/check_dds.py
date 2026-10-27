"""Проверка роли 2 (STG -> DDS, SCD2) в отдельной временной БД.

Реальные таблицы не меняются: создаётся БД lr3_dds_test_<uuid>, в STG кладутся
контролируемые наблюдения, затем выполняется sql/dds_load.sql так же, как в Airflow
(весь файл одной транзакцией). БД удаляется в конце.

Запуск в окружении:  docker compose exec -T airflow-scheduler python < scripts/check_dds.py
С ноутбука (нужен psycopg2): python scripts/check_dds.py --local
"""
import json
import os
import sys
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import psycopg2
from psycopg2 import sql
from psycopg2.extras import Json

root = Path('/opt/airflow') if Path('/opt/airflow/sql').exists() else Path(__file__).resolve().parents[1] \
    if '__file__' in globals() else Path.cwd()


def base_params():
    if '--local' in sys.argv:
        return dict(host=os.getenv('WAREHOUSE_HOST', '127.0.0.1'),
                    port=os.getenv('WAREHOUSE_PORT', '15433'), user='lab',
                    password=os.getenv('WAREHOUSE_PASSWORD', 'lab_password'))
    sys.path.insert(0, str(root / 'dags'))
    from airflow.hooks.base import BaseHook
    c = BaseHook.get_connection('warehouse')
    return dict(host=c.host, port=c.port, user=c.login, password=c.password)


params = base_params()
admin = psycopg2.connect(dbname='earthquakes', **params)
admin.autocommit = True
dbname = 'lr3_dds_test_' + uuid.uuid4().hex[:12]
with admin.cursor() as cur:
    cur.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(dbname)))


def connect(**override):
    return psycopg2.connect(**{**params, 'dbname': dbname, **override})


def run_file(name, extra=''):
    """Как storage.run_sql_file: весь файл в одной транзакции."""
    conn = connect()
    try:
        with conn, conn.cursor() as cur:
            cur.execute((root / 'sql' / name).read_text() + extra)
    finally:
        conn.close()


def q(query, *args):
    conn = connect()
    try:
        with conn, conn.cursor() as cur:
            cur.execute(query, args)
            return cur.fetchall()
    finally:
        conn.close()


T0 = datetime(2026, 10, 1, 12, 0, tzinfo=timezone.utc)
EVENT_TIME = int(datetime(2026, 9, 30, 8, 0, tzinfo=timezone.utc).timestamp() * 1000)


def feature(eid, updated_min, mag=3.0, status='automatic', lat=35.0, lon=139.0, depth=10.0,
            etype='earthquake', place='10 km N of Test', net='us'):
    return {'type': 'Feature', 'id': eid,
            'properties': {'mag': mag, 'place': place, 'time': EVENT_TIME,
                           'updated': int((T0 + timedelta(minutes=updated_min)).timestamp() * 1000),
                           'status': status, 'type': etype, 'net': net, 'magType': 'ml',
                           'tsunami': 0, 'alert': None, 'felt': None},
            'geometry': {'type': 'Point', 'coordinates': [lon, lat, depth]}}


def add_batch(batch_id, mode, loaded_at, observations):
    """observations: [(feature, минуты к loaded_at)] — разные ответы API внутри batch."""
    conn = connect()
    try:
        with conn, conn.cursor() as cur:
            cur.execute("""INSERT INTO stg.batches(batch_id,mode,scope_start,request_started_at,
                           response_count,event_count) VALUES (%s,%s,%s,%s,%s,%s)""",
                        (batch_id, mode, T0 - timedelta(days=90), loaded_at,
                         len(observations), len(observations)))
            for i, (f, shift) in enumerate(observations):
                fetched = loaded_at + timedelta(minutes=shift)
                cur.execute("""INSERT INTO stg.api_responses(batch_id,request_url,request_params,
                               payload,fetched_at) VALUES (%s,%s,'{}',%s,%s) RETURNING id""",
                            (batch_id, f'https://test.invalid/{batch_id}/{i}',
                             Json({'type': 'FeatureCollection', 'features': [f]}), fetched))
                rid = cur.fetchone()[0]
                updated = datetime.fromtimestamp(f['properties']['updated'] / 1000, timezone.utc)
                cur.execute("""INSERT INTO stg.earthquake_raw(batch_id,response_id,event_id,
                               source_updated_at,payload_hash,payload,loaded_at)
                               VALUES (%s,%s,%s,%s,md5(%s),%s,%s)""",
                            (batch_id, rid, f['id'], updated, json.dumps(f, sort_keys=True),
                             Json(f), fetched))
    finally:
        conn.close()


def versions(eid):
    return q("""SELECT magnitude::float, review_status, is_current, place
                FROM dds.event_versions WHERE event_id=%s ORDER BY valid_from""", eid)


def snapshot(schema):
    tables = [r[0] for r in q("SELECT table_name FROM information_schema.tables "
                              "WHERE table_schema=%s ORDER BY 1", schema)]
    return {t: sorted(map(repr, q(f'SELECT * FROM {schema}.{t}'))) for t in tables}


def check(name, cond, detail=''):
    print(('PASS ' if cond else 'FAIL ') + name + (f'  [{detail}]' if not cond else ''))
    if not cond:
        raise AssertionError(name)


try:
    conn = connect()
    with conn, conn.cursor() as cur:
        cur.execute((root / 'sql/init/01_stg.sql').read_text())
    conn.close()

    # --- 1. Пустой STG: скрипты проходят, ничего не создают.
    run_file('dds_load.sql')
    check('пустой STG', q('SELECT count(*) FROM dds.event_versions')[0][0] == 0)

    # --- 2. Первичная загрузка.
    add_batch('initial', 'initial', T0, [
        (feature('ev_a', 0, mag=3.0), 0),
        (feature('ev_b', 0, mag=5.1), 0),
        (feature('ev_x', 0, mag=2.0, etype='explosion'), 0),
        (feature('ev_nomag', 0, mag=None), 0),
        (feature('ev_bad', 0, lat=95.0), 0),            # координаты вне диапазона
    ])
    run_file('dds_load.sql')
    check('новые события: 4 версии', q('SELECT count(*) FROM dds.event_versions')[0][0] == 4)
    check('отбраковка с причиной',
          q('SELECT event_id, reason FROM dds.rejected_observations') ==
          [('ev_bad', 'координаты вне диапазона')])
    before = snapshot('dds')
    run_file('dds_load.sql')
    after = snapshot('dds')
    after.pop('load_log'); before.pop('load_log'); after.pop('etl_state'); before.pop('etl_state')
    check('повторный запуск DDS без изменений', before == after)

    # --- 3. Инкремент: техническое обновление, изменение, новое событие.
    add_batch('inc1', 'incremental', T0 + timedelta(hours=1), [
        (feature('ev_a', 30, mag=3.0), 0),                       # updated сменился, данные нет
        (feature('ev_b', 30, mag=5.3, status='reviewed'), 0),    # изменились магнитуда и статус
        (feature('ev_c', 30, mag=4.2), 0),                       # новое событие
    ])
    run_file('dds_load.sql')
    check('неизменившийся повтор не создаёт версию', len(versions('ev_a')) == 1)
    check('изменение закрывает старую версию',
          versions('ev_b') == [(5.1, 'automatic', False, '10 km N of Test'),
                               (5.3, 'reviewed', True, '10 km N of Test')], versions('ev_b'))
    check('новое событие', len(versions('ev_c')) == 1)

    # --- 4. Два изменения одного события в одной загрузке.
    add_batch('inc2', 'incremental', T0 + timedelta(hours=2), [
        (feature('ev_b', 60, mag=5.4, status='reviewed'), 0),
        (feature('ev_b', 70, mag=5.5, status='reviewed'), 1),
    ])
    run_file('dds_load.sql')
    check('два изменения в одном batch → две версии',
          [v[0] for v in versions('ev_b')] == [5.1, 5.3, 5.4, 5.5], versions('ev_b'))

    # --- 5. Backfill со старым состоянием и равное время обновления.
    add_batch('backfill', 'backfill', T0 + timedelta(hours=3), [
        (feature('ev_b', 0, mag=5.1), 0),                         # устарело: пропустить
        (feature('ev_a', 30, mag=3.0, place='12 km N of Test'), 0),  # то же updated, другой place
    ])
    run_file('dds_load.sql')
    check('поздняя старая запись не откатывает текущую',
          q("SELECT magnitude::float FROM dds.event_versions WHERE event_id='ev_b' AND is_current")
          == [(5.5,)])
    check('равный source_updated_at: побеждает более позднее наблюдение',
          versions('ev_a')[-1][3] == '12 km N of Test' and len(versions('ev_a')) == 2)
    check('в журнале учтена устаревшая строка',
          q('SELECT rows_stale FROM dds.load_log ORDER BY load_id DESC LIMIT 1') == [(1,)])

    # --- 6. Целостность SCD2.
    check('одна текущая версия на событие', q("""SELECT count(*) FROM (SELECT event_id FROM
          dds.event_versions WHERE is_current GROUP BY 1 HAVING count(*) <> 1) x""")[0][0] == 0
          and q("""SELECT count(*) FROM dds.events e WHERE NOT EXISTS (SELECT 1 FROM
          dds.event_versions v WHERE v.event_id=e.event_id AND v.is_current)""")[0][0] == 0)
    check('интервалы смыкаются без дыр', q("""SELECT count(*) FROM (SELECT valid_to,
          lead(valid_from) OVER (PARTITION BY event_id ORDER BY valid_from) nxt
          FROM dds.event_versions) x WHERE nxt IS NOT NULL AND nxt <> valid_to""")[0][0] == 0)
    check('time-travel запрос', q("""SELECT magnitude::float FROM dds.event_versions
          WHERE event_id='ev_b' AND %s >= valid_from AND %s < COALESCE(valid_to,'infinity')""",
          T0 + timedelta(minutes=90), T0 + timedelta(minutes=90)) == [(5.3,)])

    # --- 7. Ошибка внутри транзакции откатывает версии и watermark.
    add_batch('inc3', 'incremental', T0 + timedelta(hours=4), [(feature('ev_d', 90), 0)])
    wm = q("SELECT last_stg_id FROM dds.etl_state")[0][0]
    try:
        run_file('dds_load.sql', '\nSELECT 1/0;')
        raise RuntimeError('ожидалась ошибка')
    except psycopg2.errors.DivisionByZero:
        pass
    check('откат при ошибке', q("SELECT last_stg_id FROM dds.etl_state")[0][0] == wm
          and not versions('ev_d'))
    run_file('dds_load.sql')
    check('после исправления загрузка проходит', len(versions('ev_d')) == 1)

    # --- 8. Пользователь BI не читает DDS (доступ только к CDM).
    bi = connect(user='bi_reader', password='bi_reader')
    bi.autocommit = True
    with bi.cursor() as cur:
        try:
            cur.execute('SELECT 1 FROM dds.event_versions LIMIT 1')
            denied = False
        except psycopg2.errors.InsufficientPrivilege:
            denied = True
    bi.close()
    check('bi_reader не видит DDS', denied)
    print('Все проверки роли 2 пройдены.')
finally:
    with admin.cursor() as cur:
        cur.execute(sql.SQL('DROP DATABASE IF EXISTS {} WITH (FORCE)').format(sql.Identifier(dbname)))
    admin.close()
