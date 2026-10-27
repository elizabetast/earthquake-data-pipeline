"""Проверка роли 3 (DDS -> CDM) в отдельной временной БД.

Реальные таблицы не меняются: создаётся БД lr3_cdm_test_<uuid>, в STG кладутся
контролируемые наблюдения, затем dds_load.sql и cdm_load.sql выполняются так же,
как в Airflow (каждый файл одной транзакцией). БД удаляется в конце.

Запуск в окружении:  docker compose exec -T airflow-scheduler python < scripts/check_cdm.py
С ноутбука (нужен psycopg2): python scripts/check_cdm.py --local
"""
import json
import os
import re
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
dbname = 'lr3_cdm_test_' + uuid.uuid4().hex[:12]
with admin.cursor() as cur:
    cur.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(dbname)))


def connect(**override):
    return psycopg2.connect(**{**params, 'dbname': dbname, **override})


def run_file(name, extra=''):
    """Как storage.run_sql_file: весь файл в одной транзакции."""
    conn = connect()
    try:
        with conn, conn.cursor() as cur:
            cur.execute((root / 'sql' / name).read_text(encoding='utf-8') + extra)
    finally:
        conn.close()


def pipeline():
    run_file('dds_load.sql')
    run_file('cdm_load.sql')


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
    """observations: [feature] — каждый в своём ответе API внутри batch."""
    conn = connect()
    try:
        with conn, conn.cursor() as cur:
            cur.execute("""INSERT INTO stg.batches(batch_id,mode,scope_start,request_started_at,
                           response_count,event_count) VALUES (%s,%s,%s,%s,%s,%s)""",
                        (batch_id, mode, T0 - timedelta(days=90), loaded_at,
                         len(observations), len(observations)))
            for i, f in enumerate(observations):
                cur.execute("""INSERT INTO stg.api_responses(batch_id,request_url,request_params,
                               payload,fetched_at) VALUES (%s,%s,'{}',%s,%s) RETURNING id""",
                            (batch_id, f'https://test.invalid/{batch_id}/{i}',
                             Json({'type': 'FeatureCollection', 'features': [f]}), loaded_at))
                rid = cur.fetchone()[0]
                updated = datetime.fromtimestamp(f['properties']['updated'] / 1000, timezone.utc)
                cur.execute("""INSERT INTO stg.earthquake_raw(batch_id,response_id,event_id,
                               source_updated_at,payload_hash,payload,loaded_at)
                               VALUES (%s,%s,%s,%s,md5(%s),%s,%s)""",
                            (batch_id, rid, f['id'], updated, json.dumps(f, sort_keys=True),
                             Json(f), loaded_at))
    finally:
        conn.close()


def snapshot(skip=()):
    tables = [r[0] for r in q("SELECT table_name FROM information_schema.tables "
                              "WHERE table_schema='cdm' ORDER BY 1")]
    return {t: sorted(map(repr, q(f'SELECT * FROM cdm.{t}'))) for t in tables if t not in skip}


def scalar(query, *args):
    return q(query, *args)[0][0]


def check(name, cond, detail=''):
    print(('PASS ' if cond else 'FAIL ') + name + (f'  [{detail}]' if not cond else ''))
    if not cond:
        raise AssertionError(name)


CDM_TABLES = {'event_map', 'daily_activity', 'review_summary', 'revision_summary',
              'review_dynamics', 'pipeline_health', 'data_quality'}

try:
    conn = connect()
    with conn, conn.cursor() as cur:
        cur.execute((root / 'sql/init/01_stg.sql').read_text(encoding='utf-8'))
    conn.close()

    # --- 0. Витрины читают только DDS: в коде (без комментариев) нет обращений к stg.
    code = '\n'.join(line.split('--')[0] for line in
                     (root / 'sql/cdm_load.sql').read_text(encoding='utf-8').splitlines())
    check('cdm_load.sql не обращается к STG', not re.search(r'\bstg\s*\.', code, re.I))

    # --- 1. Пустой DDS: витрины создаются и пусты.
    pipeline()
    tables = {r[0] for r in q("SELECT table_name FROM information_schema.tables "
                              "WHERE table_schema='cdm'")}
    check('созданы все 7 витрин', tables == CDM_TABLES, tables)
    check('пустой DDS → пустые витрины',
          all(scalar(f'SELECT count(*) FROM cdm.{t}') == 0 for t in CDM_TABLES - {'pipeline_health'}))

    # --- 2. Первичная загрузка.
    add_batch('initial', 'initial', T0, [
        feature('ev_a', 0, mag=3.0),
        feature('ev_b', 0, mag=5.1, depth=120.0, net='ak'),
        feature('ev_x', 0, mag=2.0, etype='explosion'),
        feature('ev_nomag', 0, mag=None, depth=None),
        feature('ev_bad', 0, lat=95.0),                   # отбраковка в DDS
    ])
    pipeline()
    check('карта: только текущие землетрясения',
          sorted(r[0] for r in q('SELECT event_id FROM cdm.event_map')) == ['ev_a', 'ev_b', 'ev_nomag'])
    check('NULL-магнитуда и глубина — категория "нет данных"',
          q("SELECT mag_class, depth_class FROM cdm.event_map WHERE event_id='ev_nomag'")
          == [('6. нет данных', '4. нет данных')])
    check('классы магнитуды и глубины',
          q("SELECT mag_bin::float, mag_class, depth_class FROM cdm.event_map WHERE event_id='ev_b'")
          == [(5.0, '4. M5–6', '2. средние (70–300 км)')])
    check('daily_activity сходится с DDS (is_current, earthquake)',
          scalar('SELECT sum(events_cnt) FROM cdm.daily_activity') == scalar(
              "SELECT count(*) FROM dds.event_versions WHERE is_current AND event_type='earthquake'"))
    check('среднее магнитуды без NULL',
          scalar('SELECT round(sum(sum_mag) / sum(mag_filled_cnt), 2)::float FROM cdm.daily_activity')
          == 4.05)
    check('отбраковка и посторонние типы видны в data_quality',
          q("""SELECT check_name, records_cnt FROM cdm.data_quality
               WHERE check_group IN ('Отбраковка наблюдений', 'Типы событий (текущие версии)')
               ORDER BY 1""") == [('earthquake', 3), ('explosion', 1), ('координаты вне диапазона', 1)])

    # --- 3. Повторный запуск CDM без изменений, повторный конвейер без новых данных.
    before = snapshot()
    run_file('cdm_load.sql')
    check('повторный cdm_load даёт те же витрины', snapshot() == before)
    before = snapshot(skip={'pipeline_health', 'review_summary', 'review_dynamics'})
    pipeline()
    check('пустой запуск конвейера не меняет витрины',
          snapshot(skip={'pipeline_health', 'review_summary', 'review_dynamics'}) == before)
    check('журнал: пустой запуск помечен',
          q('SELECT run_kind FROM cdm.pipeline_health ORDER BY load_id DESC LIMIT 1')
          == [('нет новых строк',)])

    # --- 4. Уточнение события: в карте одна строка, история — в revision_summary.
    add_batch('inc1', 'incremental', T0 + timedelta(hours=1), [
        feature('ev_b', 30, mag=5.3, status='reviewed', depth=118.0, net='ak', lat=35.1),
        feature('ev_c', 30, mag=4.2),
    ])
    pipeline()
    check('уточнение не задваивает событие',
          scalar("SELECT count(*) FROM cdm.event_map WHERE event_id='ev_b'") == 1
          and scalar('SELECT sum(events_cnt) FROM cdm.daily_activity') == 4)
    check('карта показывает текущую версию',
          q("SELECT magnitude::float, review_status, versions_cnt FROM cdm.event_map "
            "WHERE event_id='ev_b'") == [(5.3, 'reviewed', 2)])
    rev = q("""SELECT versions_cnt, is_revised, first_magnitude::float, current_magnitude::float,
                      magnitude_delta::float, status_changed, depth_delta_km::float,
                      location_shift_km::float, hours_to_review::float
               FROM cdm.revision_summary WHERE event_id='ev_b'""")
    check('revision_summary: первая vs текущая версия',
          rev == [(2, True, 5.1, 5.3, 0.2, True, -2.0, 11.12, 1.0)], rev)
    check('неуточнённое событие', q("""SELECT is_revised, magnitude_delta::float, hours_to_review
          FROM cdm.revision_summary WHERE event_id='ev_a'""") == [(False, 0.0, None)])

    # --- 5. Доли reviewed.
    share = q("""SELECT sum(reviewed_cnt), sum(events_cnt) FROM cdm.review_summary""")
    check('review_summary: знаменатель — все события', share == [(1, 4)], share)
    check('reviewed_share по сети',
          q("SELECT reviewed_share::float FROM cdm.review_summary WHERE network_code='ak'") == [(1.0,)])

    # --- 6. Time travel: состояние на час наблюдения.
    def dyn(at):
        return q("""SELECT sum(events_cnt), sum(reviewed_cnt) FROM cdm.review_dynamics
                    WHERE snapshot_at = %s""", at)
    check('срез T0+1ч: ev_b уже reviewed, ev_c появилось', dyn(T0 + timedelta(hours=1)) == [(4, 1)],
          dyn(T0 + timedelta(hours=1)))
    add_batch('inc2', 'incremental', T0 + timedelta(hours=2, minutes=30), [
        feature('ev_a', 60, mag=3.1, status='reviewed'),
    ])
    pipeline()
    check('срез T0+2ч не видит более позднего уточнения', dyn(T0 + timedelta(hours=2)) == [(4, 1)])
    check('срез T0+3ч видит уточнение', dyn(T0 + timedelta(hours=3)) == [(4, 2)])
    check('последний срез совпадает с картой', q("""SELECT sum(events_cnt) FROM cdm.review_dynamics
          WHERE snapshot_at = (SELECT max(snapshot_at) FROM cdm.review_dynamics)""")[0][0]
          == scalar('SELECT count(*) FROM cdm.event_map'))

    # --- 7. Ошибка внутри транзакции оставляет прежние витрины.
    before = snapshot()
    try:
        run_file('cdm_load.sql', '\nSELECT 1/0;')
        raise RuntimeError('ожидалась ошибка')
    except psycopg2.errors.DivisionByZero:
        pass
    check('откат при ошибке', snapshot() == before)

    # --- 8. Пользователь BI читает CDM и не читает STG/DDS.
    bi = connect(user='bi_reader', password='bi_reader')
    bi.autocommit = True
    with bi.cursor() as cur:
        readable = all(cur.execute(f'SELECT count(*) FROM cdm.{t}') is None for t in CDM_TABLES)
        denied = []
        for table in ('dds.event_versions', 'stg.earthquake_raw'):
            try:
                cur.execute(f'SELECT 1 FROM {table} LIMIT 1')
                denied.append(False)
            except psycopg2.errors.InsufficientPrivilege:
                denied.append(True)
    bi.close()
    check('bi_reader читает все витрины CDM', readable)
    check('bi_reader не видит STG и DDS', all(denied), denied)
    print('Все проверки роли 3 пройдены.')
finally:
    with admin.cursor() as cur:
        cur.execute(sql.SQL('DROP DATABASE IF EXISTS {} WITH (FORCE)').format(sql.Identifier(dbname)))
    admin.close()
