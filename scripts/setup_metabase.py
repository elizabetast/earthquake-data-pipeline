"""Роль 3: воспроизводимая сборка Metabase через его API.

Создаёт (или пересоздаёт) в локальном Metabase:
  * администратора при первом запуске (учётные данные — METABASE_ADMIN_* из .env);
  * подключение «Earthquakes CDM» под bi_reader (видит только схему cdm);
  * коллекцию «LR3 Землетрясения» с карточками и двумя дашбордами:
    «Сейсмическая активность USGS» (4 вкладки, фильтры, кросс-фильтры по клику) и
    «История события (SCD2)» — куда ведёт клик по событию.
Повторный запуск архивирует прежние карточки коллекции и собирает всё заново.

Запуск с ноутбука после успешного dds_to_cdm (нужен только Python 3):
    python scripts/setup_metabase.py
"""
import json
import os
import sys
import time
import urllib.error
import urllib.request
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def read_env():
    env = {}
    for name in ('.env', '.env.example'):
        path = ROOT / name
        if path.exists():
            for line in path.read_text(encoding='utf-8').splitlines():
                if '=' in line and not line.lstrip().startswith('#'):
                    key, value = line.split('=', 1)
                    env.setdefault(key.strip(), value.strip())
    env.update({k: v for k, v in os.environ.items() if k.startswith(('METABASE_', 'MB_'))})
    return env


ENV = read_env()
BASE = ENV.get('MB_URL', f"http://localhost:{ENV.get('METABASE_PORT', '3334')}")
EMAIL, PASSWORD = ENV['METABASE_ADMIN_EMAIL'], ENV['METABASE_ADMIN_PASSWORD']
DB_NAME, COLLECTION = 'Earthquakes CDM', 'LR3 Землетрясения'
MAIN_DASH, HISTORY_DASH = 'Сейсмическая активность USGS', 'История события (SCD2)'
CDM_TABLES = {'event_map', 'daily_activity', 'review_summary', 'revision_summary',
              'event_history', 'review_dynamics', 'pipeline_health', 'data_quality'}
session = None


def api(method, path, body=None):
    req = urllib.request.Request(BASE + '/api' + path, method=method,
                                 data=None if body is None else json.dumps(body).encode())
    req.add_header('Content-Type', 'application/json')
    if session:
        req.add_header('X-Metabase-Session', session)
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            raw = resp.read()
            return json.loads(raw) if raw else None
    except urllib.error.HTTPError as e:
        sys.exit(f'{method} {path}: HTTP {e.code}\n{e.read().decode(errors="replace")[:2000]}')


# --------------------------------------------------------------------------------------------
# 1. Вход, подключение к CDM, синхронизация
# --------------------------------------------------------------------------------------------
def login():
    global session
    props = api('GET', '/session/properties')
    if not props.get('has-user-setup'):
        session = api('POST', '/setup', {
            'token': props['setup-token'],
            'user': {'email': EMAIL, 'password': PASSWORD, 'first_name': 'Admin',
                     'last_name': 'LR3', 'site_name': 'LR3 Earthquakes'},
            'prefs': {'site_name': 'LR3 Earthquakes', 'site_locale': 'ru', 'allow_tracking': False},
        })['id']
        print('Metabase: создан администратор', EMAIL)
    else:
        session = api('POST', '/session', {'username': EMAIL, 'password': PASSWORD})['id']


def ensure_database():
    dbs = api('GET', '/database')
    dbs = dbs['data'] if isinstance(dbs, dict) else dbs
    db = next((d for d in dbs if d['name'] == DB_NAME), None)
    if db is None:
        db = api('POST', '/database', {
            'engine': 'postgres', 'name': DB_NAME, 'is_full_sync': True,
            'details': {'host': 'warehouse', 'port': 5432, 'dbname': 'earthquakes',
                        'user': 'bi_reader', 'password': 'bi_reader', 'ssl': False}})
        print('Metabase: подключена БД', DB_NAME)
    api('POST', f"/database/{db['id']}/sync_schema")
    for _ in range(60):
        meta = api('GET', f"/database/{db['id']}/metadata")
        tables = {t['name']: t for t in meta['tables'] if t.get('schema') == 'cdm'}
        if CDM_TABLES <= set(tables) and all(tables[t]['fields'] for t in CDM_TABLES):
            break
        time.sleep(2)
    else:
        sys.exit('Схема cdm не синхронизировалась: сначала выполните DAG до dds_to_cdm')
    schemas = {t.get('schema') for t in meta['tables']}
    print('Metabase видит схемы:', ', '.join(sorted(s for s in schemas if s)))
    fields = {(t, f['name']): f['id'] for t in tables for f in tables[t]['fields']}
    # Типы координат нужны, чтобы Metabase рисовал карту.
    for table in ('event_map', 'event_history'):
        api('PUT', f"/field/{fields[(table, 'latitude')]}", {'semantic_type': 'type/Latitude'})
        api('PUT', f"/field/{fields[(table, 'longitude')]}", {'semantic_type': 'type/Longitude'})
    for table, field in (('event_map', 'network_code'), ('event_map', 'depth_class'),
                         ('daily_activity', 'network_code'), ('daily_activity', 'depth_class')):
        api('PUT', f'/field/{fields[(table, field)]}', {'has_field_values': 'list'})
    api('POST', f"/database/{db['id']}/rescan_values")
    return db['id'], fields


def ensure_collection():
    coll = next((c for c in api('GET', '/collection')
                 if c.get('name') == COLLECTION and not c.get('archived')), None)
    if coll is None:
        coll = api('POST', '/collection', {'name': COLLECTION,
                                           'description': 'ЛР3: витрины CDM и дашборды роли 3'})
    dashboards = {}
    items = api('GET', f"/collection/{coll['id']}/items")
    for item in items['data'] if isinstance(items, dict) else items:
        if item['model'] == 'card':
            api('PUT', f"/card/{item['id']}", {'archived': True})
        elif item['model'] == 'dashboard':
            dashboards[item['name']] = item['id']
    return coll['id'], dashboards


# --------------------------------------------------------------------------------------------
# 2. Карточки
# --------------------------------------------------------------------------------------------
TAGS = {'date': ('Дата', 'date/all-options'), 'mag': ('Магнитуда от', 'number/>='),
        'net': ('Сеть', 'string/='), 'depth': ('Глубина', 'string/='),
        'event': ('Событие', 'string/=')}
PARAMS = {'date': 'p_date', 'mag': 'p_mag', 'net': 'p_net', 'depth': 'p_depth', 'event': 'p_event'}

DA = {'date': ('daily_activity', 'event_date'), 'mag': ('daily_activity', 'mag_bin'),
      'net': ('daily_activity', 'network_code'), 'depth': ('daily_activity', 'depth_class')}
EM = {'date': ('event_map', 'event_date'), 'mag': ('event_map', 'magnitude'),
      'net': ('event_map', 'network_code'), 'depth': ('event_map', 'depth_class')}
F_ALL = '1=1 [[AND {{date}}]] [[AND {{mag}}]] [[AND {{net}}]] [[AND {{depth}}]]'


def pct(*cols):
    return {'column_settings': {json.dumps(['name', c], ensure_ascii=False):
                                {'number_style': 'percent', 'decimals': 1} for c in cols}}


def card(name, display, query, tags=None, viz=None, description=''):
    return dict(name=name, display=display, query=query, tags=tags or {}, viz=viz or {},
                description=description)


C = {
    # ---- KPI
    'k_events': card('Событий', 'scalar',
                     f'SELECT sum(events_cnt) AS "Событий" FROM cdm.daily_activity WHERE {F_ALL}', DA,
                     description='Число текущих землетрясений (is_current) с учётом фильтров.'),
    'k_max': card('Макс. магнитуда', 'scalar',
                  f'SELECT max(max_mag) AS "Макс. M" FROM cdm.daily_activity WHERE {F_ALL}', DA,
                  description='Максимальная магнитуда среди отфильтрованных событий.'),
    'k_avg': card('Средняя магнитуда', 'scalar',
                  f'SELECT round(sum(sum_mag) / NULLIF(sum(mag_filled_cnt), 0), 2) AS "Средняя M" '
                  f'FROM cdm.daily_activity WHERE {F_ALL}', DA,
                  description='sum(mag) / число событий с магнитудой: NULL не считается нулём.'),
    'k_reviewed': card('Доля reviewed', 'scalar',
                       'SELECT sum(reviewed_cnt)::numeric / NULLIF(sum(events_cnt), 0) AS "Доля reviewed" '
                       f'FROM cdm.daily_activity WHERE {F_ALL}', DA, pct('Доля reviewed'),
                       'Доля событий, проверенных сейсмологом USGS. Знаменатель — все события.'),
    'k_revised': card('Уточнено событий', 'scalar',
                      f'SELECT sum(revised_cnt) AS "Уточнено" FROM cdm.daily_activity WHERE {F_ALL}', DA,
                      description='События, у которых DDS сохранил больше одной версии (SCD2).'),
    'k_last': card('Последняя загрузка', 'scalar',
                   'SELECT max(started_at) AS "Последняя загрузка" FROM cdm.pipeline_health',
                   description='Время последнего запуска STG → DDS (UTC).'),
    # ---- Обзор
    'map': card('Карта землетрясений', 'map',
                'SELECT event_id, latitude, longitude, magnitude, depth_km, event_time, place,\n'
                '       review_status, network_code\n'
                f'FROM cdm.event_map WHERE {F_ALL}\n'
                'ORDER BY magnitude DESC NULLS LAST LIMIT 2000', EM,
                {'map.type': 'pin', 'map.latitude_column': 'latitude',
                 'map.longitude_column': 'longitude'},
                '2000 самых сильных событий по фильтрам. Наведите на точку — детали события.'),
    'daily': card('Событий в день по классам магнитуды', 'bar',
                  'SELECT event_date AS "Дата", mag_class AS "Класс", sum(events_cnt) AS "Событий"\n'
                  f'FROM cdm.daily_activity WHERE {F_ALL}\nGROUP BY 1, 2 ORDER BY 1, 2', DA,
                  {'graph.dimensions': ['Дата', 'Класс'], 'graph.metrics': ['Событий'],
                   'stackable.stack_type': 'stacked', 'graph.x_axis.title_text': 'Дата события (UTC)',
                   'graph.y_axis.title_text': 'Событий'},
                  'Клик по столбцу фильтрует весь дашборд по этому дню.'),
    'depth_pie': card('Глубина очагов', 'pie',
                      'SELECT depth_class AS "Класс глубины", sum(events_cnt) AS "Событий"\n'
                      f'FROM cdm.daily_activity WHERE {F_ALL}\nGROUP BY 1 ORDER BY 1', DA,
                      {'pie.dimension': 'Класс глубины', 'pie.metric': 'Событий'},
                      'Клик по сектору — фильтр «Глубина».'),
    'scatter': card('Магнитуда и глубина', 'scatter',
                    'SELECT depth_km AS "Глубина, км", magnitude AS "Магнитуда", network_code AS "Сеть"\n'
                    'FROM cdm.event_map WHERE magnitude IS NOT NULL AND depth_km IS NOT NULL\n'
                    f'  AND {F_ALL}\nORDER BY magnitude DESC LIMIT 2000', EM,
                    {'graph.dimensions': ['Глубина, км'], 'graph.metrics': ['Магнитуда'],
                     'graph.x_axis.title_text': 'Глубина, км', 'graph.y_axis.title_text': 'Магнитуда'},
                    'Сильные события бывают и мелкими, и глубокими (до ~700 км).'),
    'top': card('Сильнейшие землетрясения', 'table',
                'SELECT event_id, event_time AS "Время (UTC)", magnitude AS "M", magnitude_type AS "Тип M",\n'
                '       depth_km AS "Глубина, км", place AS "Место", review_status AS "Статус",\n'
                '       network_code AS "Сеть", versions_cnt AS "Версий"\n'
                f'FROM cdm.event_map WHERE magnitude IS NOT NULL AND {F_ALL}\n'
                'ORDER BY magnitude DESC LIMIT 20', EM,
                description='Клик по event_id — история версий события; по «Месту» — страница USGS.'),
    # ---- Проверка USGS
    'status_pie': card('Статусы проверки', 'pie',
                       'SELECT review_status AS "Статус", count(*) AS "Событий"\n'
                       f'FROM cdm.event_map WHERE {F_ALL}\nGROUP BY 1 ORDER BY 2 DESC', EM,
                       {'pie.dimension': 'Статус', 'pie.metric': 'Событий'},
                       'automatic — автоматическая оценка, reviewed — проверено сейсмологом.'),
    'by_net': card('Доля reviewed по сетям', 'bar',
                   'SELECT network_code AS "Сеть", sum(reviewed_cnt)::numeric / sum(events_cnt) AS "Доля reviewed",\n'
                   '       sum(events_cnt) AS "Событий"\n'
                   f'FROM cdm.daily_activity WHERE {F_ALL}\n'
                   'GROUP BY 1 HAVING sum(events_cnt) >= 20 ORDER BY 2 DESC', DA,
                   {'graph.dimensions': ['Сеть'], 'graph.metrics': ['Доля reviewed'],
                    'graph.max_categories_enabled': False, **pct('Доля reviewed')},
                   'Сеть — региональный сейсмоцентр (us, ak, nc, ci …). Клик — фильтр по сети.'),
    'by_age': card('Доля reviewed в зависимости от возраста события', 'line',
                   'SELECT event_age_days AS "Возраст, сут",\n'
                   '       sum(reviewed_cnt)::numeric / sum(events_cnt) AS "Доля reviewed"\n'
                   'FROM cdm.review_summary WHERE 1=1 [[AND {{net}}]]\nGROUP BY 1 ORDER BY 1',
                   {'net': ('review_summary', 'network_code')},
                   {'graph.dimensions': ['Возраст, сут'], 'graph.metrics': ['Доля reviewed'],
                    'graph.x_axis.title_text': 'Сколько суток прошло с события',
                    **pct('Доля reviewed')},
                   'Свежие события ещё automatic; со временем USGS переводит их в reviewed.'),
    'dynamics': card('Динамика доли reviewed по часам наблюдения (SCD2)', 'combo',
                     'SELECT snapshot_at AS "Срез", sum(reviewed_cnt)::numeric / sum(events_cnt) AS "Доля reviewed",\n'
                     '       sum(events_cnt) AS "Событий в каталоге"\n'
                     'FROM cdm.review_dynamics WHERE 1=1 [[AND {{net}}]]\nGROUP BY 1 ORDER BY 1',
                     {'net': ('review_dynamics', 'network_code')},
                     {'graph.dimensions': ['Срез'], 'graph.metrics': ['Доля reviewed', 'Событий в каталоге'],
                      'series_settings': {'Доля reviewed': {'display': 'line'},
                                          'Событий в каталоге': {'display': 'bar', 'axis': 'right'}},
                      **pct('Доля reviewed')},
                     'Time travel: каждая точка — каталог таким, каким DDS видел его в тот час.'),
    'completeness': card('Полнота данных по дням', 'line',
                         'SELECT event_date AS "Дата",\n'
                         '       sum(mag_filled_cnt)::numeric / sum(events_cnt)   AS "С магнитудой",\n'
                         '       sum(depth_filled_cnt)::numeric / sum(events_cnt) AS "С глубиной",\n'
                         '       sum(place_filled_cnt)::numeric / sum(events_cnt) AS "С описанием места",\n'
                         '       sum(reviewed_cnt)::numeric / sum(events_cnt)     AS "Reviewed"\n'
                         f'FROM cdm.daily_activity WHERE {F_ALL}\nGROUP BY 1 ORDER BY 1', DA,
                         {'graph.dimensions': ['Дата'],
                          'graph.metrics': ['С магнитудой', 'С глубиной', 'С описанием места', 'Reviewed'],
                          **pct('С магнитудой', 'С глубиной', 'С описанием места', 'Reviewed')},
                         'Аналог «доли заполненных аннотаций»: какая доля событий дня имеет поле.'),
    # ---- Уточнения
    'what_changed': card('Что меняется при уточнении', 'bar',
                         "SELECT 'магнитуда' AS \"Атрибут\", count(*) FILTER (WHERE magnitude_changed) AS \"Событий\"\n"
                         'FROM cdm.revision_summary WHERE is_revised [[AND {{date}}]] [[AND {{net}}]]\n'
                         "UNION ALL SELECT 'статус', count(*) FILTER (WHERE status_changed)\n"
                         'FROM cdm.revision_summary WHERE is_revised [[AND {{date}}]] [[AND {{net}}]]\n'
                         "UNION ALL SELECT 'глубина', count(*) FILTER (WHERE depth_delta_km <> 0)\n"
                         'FROM cdm.revision_summary WHERE is_revised [[AND {{date}}]] [[AND {{net}}]]\n'
                         "UNION ALL SELECT 'координаты', count(*) FILTER (WHERE location_shift_km > 0)\n"
                         'FROM cdm.revision_summary WHERE is_revised [[AND {{date}}]] [[AND {{net}}]]',
                         {'date': ('revision_summary', 'event_date'), 'net': ('revision_summary', 'network_code')},
                         {'graph.dimensions': ['Атрибут'], 'graph.metrics': ['Событий']},
                         'Среди уточнённых событий: у скольких изменился каждый атрибут.'),
    'mag_delta': card('Насколько меняется магнитуда', 'bar',
                      'SELECT round(magnitude_delta, 1) AS "Δ M (текущая − первая)", count(*) AS "Событий"\n'
                      'FROM cdm.revision_summary WHERE magnitude_changed AND magnitude_delta IS NOT NULL\n'
                      '  [[AND {{date}}]] [[AND {{net}}]]\nGROUP BY 1 ORDER BY 1',
                      {'date': ('revision_summary', 'event_date'), 'net': ('revision_summary', 'network_code')},
                      {'graph.dimensions': ['Δ M (текущая − первая)'], 'graph.metrics': ['Событий']},
                      'Распределение поправок магнитуды за время наблюдения пайплайном.'),
    'revisions': card('Уточнённые события', 'table',
                      'SELECT event_id, event_date AS "Дата", network_code AS "Сеть", versions_cnt AS "Версий",\n'
                      '       first_magnitude AS "M первая", current_magnitude AS "M текущая",\n'
                      '       magnitude_delta AS "Δ M", first_status AS "Статус был",\n'
                      '       current_status AS "Статус стал", hours_to_review AS "Часов до reviewed",\n'
                      '       location_shift_km AS "Сдвиг, км"\n'
                      'FROM cdm.revision_summary WHERE is_revised [[AND {{date}}]] [[AND {{net}}]]\n'
                      'ORDER BY abs(magnitude_delta) DESC NULLS LAST, versions_cnt DESC LIMIT 200',
                      {'date': ('revision_summary', 'event_date'), 'net': ('revision_summary', 'network_code')},
                      description='Клик по event_id открывает историю версий события.'),
    # ---- Качество и конвейер
    'runs': card('Запуски конвейера', 'table',
                 'SELECT started_at AS "Запуск (UTC)", run_kind AS "Итог", rows_read AS "Прочитано STG",\n'
                 '       rows_rejected AS "Отбраковано", rows_stale AS "Устарело", new_events AS "Новых событий",\n'
                 '       versions_inserted AS "Версий создано", versions_closed AS "Версий закрыто"\n'
                 'FROM cdm.pipeline_health ORDER BY load_id DESC LIMIT 48',
                 description='dds.load_log: повторный запуск без новых данных ничего не создаёт.'),
    'runs_chart': card('Новые версии по запускам', 'bar',
                       'SELECT started_at AS "Запуск", versions_inserted AS "Версий создано",\n'
                       '       versions_closed AS "Версий закрыто"\n'
                       'FROM cdm.pipeline_health WHERE load_id > 1 ORDER BY load_id',
                       viz={'graph.dimensions': ['Запуск'], 'graph.metrics': ['Версий создано', 'Версий закрыто']},
                       description='Без первичной загрузки: сколько версий SCD2 даёт каждый часовой запуск.'),
    'types': card('Исключено из аналитики: не землетрясения', 'row',
                  "SELECT check_name AS \"Тип\", records_cnt AS \"Событий\" FROM cdm.data_quality\n"
                  "WHERE check_group = 'Типы событий (текущие версии)' AND check_name <> 'earthquake'\n"
                  "ORDER BY 2 DESC",
                  viz={'graph.dimensions': ['Тип'], 'graph.metrics': ['Событий'],
                       'graph.max_categories_enabled': False},
                  description='USGS отдаёт и взрывы, карьеры, ледовые толчки — в аналитику идут только earthquake.'),
    'quality': card('Проверки качества', 'table',
                    'SELECT check_group AS "Группа", check_name AS "Проверка", records_cnt AS "Записей",\n'
                    '       total_cnt AS "Из", share AS "Доля"\n'
                    'FROM cdm.data_quality ORDER BY 1, 3 DESC',
                    viz={'column_settings': {'["name", "Доля"]': {'number_style': 'percent',
                                                                  'decimals': 3}}},
                    description='Отбраковка DDS (с причиной) и полнота полей текущих землетрясений.'),
    # ---- История события
    'h_list': card('Уточнённые события (выберите)', 'table',
                   'SELECT event_id, event_date AS "Дата", versions_cnt AS "Версий",\n'
                   '       first_magnitude AS "M первая", current_magnitude AS "M текущая", current_status AS "Статус"\n'
                   'FROM cdm.revision_summary WHERE is_revised\n'
                   'ORDER BY versions_cnt DESC, abs(magnitude_delta) DESC NULLS LAST LIMIT 200',
                   description='Клик по event_id выбирает событие на этом дашборде.'),
    'h_mag': card('Магнитуда по версиям', 'line',
                  'SELECT valid_from AS "Наблюдено", magnitude AS "Магнитуда"\n'
                  'FROM cdm.event_history WHERE {{event}} ORDER BY valid_from',
                  {'event': ('event_history', 'event_id')},
                  {'graph.dimensions': ['Наблюдено'], 'graph.metrics': ['Магнитуда'],
                   'line.interpolate': 'step-after', 'graph.show_values': True},
                  'Ступенька — момент, когда пайплайн увидел новую версию (valid_from).'),
    'h_versions': card('Версии события (SCD2)', 'table',
                       'SELECT version_no AS "№", valid_from AS "Действует с", valid_to AS "Действует до",\n'
                       '       is_current AS "Текущая", magnitude AS "M", magnitude_change AS "Δ M",\n'
                       '       review_status AS "Статус", status_change AS "Смена статуса",\n'
                       '       depth_km AS "Глубина, км", place AS "Место", source_updated_at AS "Правка USGS"\n'
                       'FROM cdm.event_history WHERE {{event}} ORDER BY event_id, version_no',
                       {'event': ('event_history', 'event_id')},
                       description='valid_from/valid_to — интервал [from, to); у текущей версии to пуст.'),
    'h_map': card('Положение эпицентра по версиям', 'map',
                  'SELECT version_no, latitude, longitude, magnitude, valid_from\n'
                  'FROM cdm.event_history WHERE {{event}}',
                  {'event': ('event_history', 'event_id')},
                  {'map.type': 'pin', 'map.latitude_column': 'latitude', 'map.longitude_column': 'longitude'},
                  'Если координаты уточнялись — видно несколько точек.'),
}


def create_cards(db_id, fields, coll_id):
    ids = {}
    for key, c in C.items():
        tags = {t: {'id': str(uuid.uuid4()), 'name': t, 'display-name': TAGS[t][0],
                    'type': 'dimension', 'dimension': ['field', fields[f], None],
                    'widget-type': TAGS[t][1]}
                for t, f in c['tags'].items()}
        if 'event' in tags:
            tags['event']['required'] = False
        ids[key] = api('POST', '/card', {
            'name': c['name'], 'description': c['description'] or None, 'display': c['display'],
            'collection_id': coll_id, 'visualization_settings': c['viz'],
            'dataset_query': {'type': 'native', 'database': db_id,
                              'native': {'query': c['query'], 'template-tags': tags}},
        })['id']
    print('Metabase: создано карточек', len(ids))
    return ids


# --------------------------------------------------------------------------------------------
# 3. Дашборды
# --------------------------------------------------------------------------------------------
def text(md):
    return {'card': None, 'text': md}


def link_param(param, column):
    return {param: {'id': param, 'source': {'type': 'column', 'id': column, 'name': column},
                    'target': {'type': 'parameter', 'id': param}}}


def crossfilter(param, column):
    return {'click_behavior': {'type': 'crossfilter', 'parameterMapping': link_param(param, column)}}


def column_click(column, behavior):
    return {'column_settings': {json.dumps(['name', column], ensure_ascii=False):
                                {'click_behavior': behavior}}}


def build_dashboard(dash_id, ids, params, tabs, layout):
    """layout: [(вкладка, key|text(), row, col, w, h, click)]"""
    dashcards = []
    for n, (tab, item, row, col, w, h, click) in enumerate(layout, start=1):
        dc = {'id': -n, 'row': row, 'col': col, 'size_x': w, 'size_y': h,
              'dashboard_tab_id': -tab if tabs else None, 'series': []}
        if isinstance(item, dict):
            dc.update(card_id=None, parameter_mappings=[], visualization_settings={
                'virtual_card': {'name': None, 'display': 'text', 'visualization_settings': {},
                                 'dataset_query': {}, 'archived': False},
                'text': item['text']})
        else:
            cid = ids[item]
            dc.update(card_id=cid, visualization_settings=click or {}, parameter_mappings=[
                {'parameter_id': PARAMS[t], 'card_id': cid, 'target': ['dimension', ['template-tag', t]]}
                for t in C[item]['tags'] if PARAMS[t] in {p['id'] for p in params}])
        dashcards.append(dc)
    body = {'parameters': params, 'dashcards': dashcards}
    if tabs:
        body['tabs'] = [{'id': -i, 'name': name} for i, name in enumerate(tabs, start=1)]
    api('PUT', f'/dashboard/{dash_id}', body)


MAIN_PARAMS = [
    {'id': 'p_date', 'name': 'Дата', 'slug': 'date', 'type': 'date/all-options', 'sectionId': 'date'},
    {'id': 'p_mag', 'name': 'Магнитуда от', 'slug': 'mag', 'type': 'number/>=', 'sectionId': 'number'},
    {'id': 'p_net', 'name': 'Сеть', 'slug': 'net', 'type': 'string/=', 'sectionId': 'string'},
    {'id': 'p_depth', 'name': 'Глубина', 'slug': 'depth', 'type': 'string/=', 'sectionId': 'string'},
]

INTRO = """## Сейсмическая активность USGS — 90 суток + наблюдение каждый час
**Конвейер:** USGS API → STG (сырой JSON) → DDS (SCD2) → **CDM** (витрины) → Metabase. Airflow, каждый час в :15 UTC.

**Как пользоваться:** фильтры сверху действуют на все вкладки. Клик по столбцу дня, сектору глубины
или сети добавляет фильтр; клик по `event_id` открывает историю версий события.
Считаются только **текущие версии** (`is_current`) и только `earthquake`."""

T_REVIEW = """### Как USGS проверяет события
Сначала событие приходит со статусом **automatic** (оценка алгоритма), затем сейсмолог переводит его
в **reviewed**. Доля reviewed = reviewed / все события группы. Нижний правый график — **time travel по SCD2**:
доля на каждый час наблюдения, по версиям, которые тогда были текущими."""

T_REVISIONS = """### Уточнения: что SCD2 сохранил за время наблюдения
Когда USGS меняет магнитуду, статус, глубину или координаты, DDS закрывает старую версию и открывает новую.
Здесь сравниваются **первая наблюдённая** и **текущая** версии. Прошлые правки до запуска пайплайна API не отдаёт —
чем дольше работает DAG, тем больше уточнений. Клик по `event_id` — вся цепочка версий."""

T_QUALITY = """### Качество данных и работа конвейера
Записи с невалидными координатами, магнитудой или глубиной DDS не теряет, а откладывает с причиной.
Журнал запусков показывает, что повторный запуск без новых данных не создаёт версий (идемпотентность)."""

T_HISTORY = """## История события (SCD2)
Выберите событие в фильтре **«Событие»**, кликом по `event_id` в таблице слева (там уточнённые события)
или по `event_id` на основном дашборде. Каждая строка внизу — версия из `dds.event_versions`
с интервалом действия `[valid_from, valid_to)`; у текущей версии `valid_to` пуст."""


def ensure_dashboard(existing, coll_id, name, description):
    """Существующий дашборд переиспользуется: ссылка на него не меняется."""
    if name in existing:
        return {'id': existing[name]}
    return api('POST', '/dashboard', {'name': name, 'collection_id': coll_id,
                                      'description': description})


def create_dashboards(coll_id, existing, ids, default_event):
    history = ensure_dashboard(existing, coll_id, HISTORY_DASH,
        'Цепочка версий одного землетрясения: как менялись магнитуда, статус и координаты.')
    main = ensure_dashboard(existing, coll_id, MAIN_DASH,
        'ЛР3: витрины CDM из DDS. Активность, проверка USGS, уточнения SCD2, качество.')

    to_history = {'type': 'link', 'linkType': 'dashboard', 'targetId': history['id'],
                  'parameterMapping': link_param('p_event', 'event_id')}
    to_usgs = {'type': 'link', 'linkType': 'url',
               'linkTemplate': 'https://earthquake.usgs.gov/earthquakes/eventpage/{{event_id}}'}
    top_click = column_click('event_id', to_history)
    top_click['column_settings'].update(column_click('Место', to_usgs)['column_settings'])

    build_dashboard(main['id'], ids, MAIN_PARAMS,
                    ['Обзор', 'Проверка USGS', 'Уточнения (SCD2)', 'Качество и конвейер'], [
        (1, text(INTRO), 0, 0, 24, 4, None),
        (1, 'k_events', 4, 0, 4, 3, None), (1, 'k_max', 4, 4, 4, 3, None),
        (1, 'k_avg', 4, 8, 4, 3, None), (1, 'k_reviewed', 4, 12, 4, 3, None),
        (1, 'k_revised', 4, 16, 4, 3, None), (1, 'k_last', 4, 20, 4, 3, None),
        (1, 'map', 7, 0, 24, 10, None),
        (1, 'daily', 17, 0, 14, 7, crossfilter('p_date', 'Дата')),
        (1, 'depth_pie', 17, 14, 10, 7, crossfilter('p_depth', 'Класс глубины')),
        (1, 'scatter', 24, 0, 10, 8, None),
        (1, 'top', 24, 10, 14, 8, top_click),
        (2, text(T_REVIEW), 0, 0, 24, 3, None),
        (2, 'k_reviewed', 3, 0, 6, 3, None), (2, 'status_pie', 3, 6, 8, 6, None),
        (2, 'by_net', 3, 14, 10, 6, crossfilter('p_net', 'Сеть')),
        (2, 'k_events', 6, 0, 6, 3, None),
        (2, 'by_age', 9, 0, 12, 7, None), (2, 'dynamics', 9, 12, 12, 7, None),
        (2, 'completeness', 16, 0, 24, 6, None),
        (3, text(T_REVISIONS), 0, 0, 24, 3, None),
        (3, 'k_revised', 3, 0, 6, 4, None), (3, 'what_changed', 3, 6, 9, 6, None),
        (3, 'mag_delta', 3, 15, 9, 6, None),
        (3, 'revisions', 9, 0, 24, 9, column_click('event_id', to_history)),
        (4, text(T_QUALITY), 0, 0, 24, 3, None),
        (4, 'k_last', 3, 0, 6, 3, None), (4, 'types', 3, 6, 9, 6, None),
        (4, 'runs_chart', 3, 15, 9, 6, None),
        (4, 'quality', 9, 0, 24, 6, None), (4, 'runs', 15, 0, 24, 8, None),
    ])

    event_param = {'id': 'p_event', 'name': 'Событие', 'slug': 'event_id', 'type': 'string/=',
                   'sectionId': 'string'}
    if default_event:
        event_param['default'] = [default_event]
    build_dashboard(history['id'], ids, [event_param], [], [
        (0, text(T_HISTORY), 0, 0, 24, 4, None),
        (0, 'h_list', 4, 0, 10, 14, column_click('event_id', {
            'type': 'crossfilter', 'parameterMapping': link_param('p_event', 'event_id')})),
        (0, 'h_mag', 4, 10, 14, 7, None),
        (0, 'h_map', 11, 10, 14, 7, None),
        (0, 'h_versions', 18, 0, 24, 6, None),
    ])
    return main['id'], history['id']


def default_event(db_id):
    res = api('POST', '/dataset', {'database': db_id, 'type': 'native', 'native': {'query':
              # Самое уточнённое событие, а пока уточнений нет — самое сильное.
              'SELECT event_id FROM cdm.revision_summary ORDER BY versions_cnt DESC, '
              'abs(magnitude_delta) DESC NULLS LAST, current_magnitude DESC NULLS LAST LIMIT 1'}})
    rows = res.get('data', {}).get('rows') or []
    return rows[0][0] if rows else None


if __name__ == '__main__':
    login()
    db_id, fields = ensure_database()
    coll_id, existing = ensure_collection()
    ids = create_cards(db_id, fields, coll_id)
    main_id, history_id = create_dashboards(coll_id, existing, ids, default_event(db_id))
    print(f'Готово: {BASE}/dashboard/{main_id}  и  {BASE}/dashboard/{history_id}')
