"""Интеграция с PostgreSQL в отдельной временной БД, реальные данные не меняются.

Запуск: docker compose exec -T airflow-scheduler python < scripts/check_storage.py
"""
import sys
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

root = Path('/opt/airflow') if Path('/opt/airflow/dags').exists() else Path.cwd()
sys.path.insert(0, str(root / 'dags'))
from earthquake_common import api, storage
from psycopg2 import sql

if '--local' in sys.argv:
    import psycopg2
    def original_connect():
        return psycopg2.connect(host='127.0.0.1', port=os.getenv('WAREHOUSE_PORT', '15433'),
                               dbname='earthquakes', user='lab',
                               password=os.getenv('WAREHOUSE_PASSWORD', 'lab_password'))
else:
    original_connect = storage.connect
admin = original_connect()
admin.autocommit = True
dbname = 'lr3_test_' + uuid.uuid4().hex
with admin.cursor() as cur:
    cur.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(dbname)))


def test_connect():
    import psycopg2
    params = admin.get_dsn_parameters()
    return psycopg2.connect(host=params['host'], port=params['port'],
                           dbname=dbname, user=params['user'], password=admin.info.password)


def counts():
    conn = test_connect()
    try:
        with conn, conn.cursor() as cur:
            cur.execute('SELECT count(*) FROM stg.batches')
            batches = cur.fetchone()[0]
            cur.execute('SELECT count(*) FROM stg.earthquake_raw')
            return batches, cur.fetchone()[0]
    finally:
        conn.close()


now = datetime.now(timezone.utc)
event = {'type': 'Feature', 'id': 'test-event', 'properties': {'updated': 1000, 'mag': 3.0}}
response = {'type': 'FeatureCollection', 'features': [event, event]}


def fake_range(*args):
    yield 'https://test.invalid/1', {}, response, now
    yield 'https://test.invalid/2', {}, response, now


def broken_range(*args):
    yield 'https://test.invalid/1', {}, response, now
    raise RuntimeError('simulated API failure')

try:
    conn = test_connect()
    with conn, conn.cursor() as cur:
        cur.execute((root / 'sql/init/01_stg.sql').read_text())
    conn.close()
    with patch.object(storage, 'connect', test_connect):
        with patch.object(api, 'fetch_range', fake_range):
            assert storage.load('initial') == 1
            assert counts() == (1, 1), counts()
        with patch.object(api, 'fetch_range', side_effect=AssertionError('must not fetch again')):
            assert storage.load('initial') == 1
        assert counts() == (1, 1)
        with patch.object(api, 'fetch_range', broken_range):
            try:
                storage.load('broken')
                raise AssertionError('expected API error')
            except RuntimeError:
                pass
        assert counts() == (1, 1), 'failed load was not rolled back'
        with patch.object(api, 'fetch_range', side_effect=fake_range) as fetch:
            storage.load('incremental')
            assert fetch.call_args.args[2] is not None, 'updatedafter missing'
            assert counts() == (2, 2)
        with patch.object(api, 'fetch_range', side_effect=fake_range) as fetch:
            storage.load('backfill', 7)
            assert fetch.call_args.args[2] is None, 'backfill must not filter updates'
        conn = test_connect()
        with conn, conn.cursor() as cur:
            cur.execute("SELECT mode FROM stg.batches ORDER BY request_started_at")
            assert [r[0] for r in cur.fetchall()] == ['initial', 'incremental', 'backfill']
        conn.close()
        for bad in (0, 366, True, '30'):
            try:
                storage.load('bad', bad)
                raise AssertionError('expected validation error')
            except ValueError:
                pass
    print('PASS: deduplication, idempotency, rollback, incremental watermark, backfill, input validation')
finally:
    with admin.cursor() as cur:
        cur.execute(sql.SQL('DROP DATABASE {} WITH (FORCE)').format(sql.Identifier(dbname)))
    admin.close()
