# Контракты STG → DDS → CDM

## Область данных

Источник: [USGS Earthquake Catalog](https://earthquake.usgs.gov/fdsnws/event/1/).
Первичная загрузка — все события каталога за 90 суток, без отсечения по магнитуде.
Это позволяет не терять уточнения с переходом через порог магнитуды. USGS возвращает
также взрывы и другие типы событий: фильтр `event_type = 'earthquake'` применяется
в аналитике, а STG сохраняет всё. Часовой сбор ищет обновления по `updatedafter`
с перекрытием 10 минут, начиная с фиксированной нижней границы первичной истории.
Таким образом новые события тоже попадают в запрос. Старые события до этой границы
вне области проекта. Ручной backfill не меняет watermark регулярного сбора.

STG хранит наблюдения источника, а не полную историю его внутренних правок.
Историческая выгрузка возвращает доступную сейчас версию события. Пропущенные
между опросами версии восстановить этим API нельзя. Исчезновение события из
выдачи не означает удаление: tombstone/слияния ID — вне базового объёма проекта.

## Готовый STG (роль 1)

### stg.batches

Одна строка на успешно закоммиченную загрузку.

| Поле | Тип | Значение |
|---|---|---|
| batch_id | text PK | Airflow run_id, ключ идемпотентности |
| mode | text | initial / incremental / backfill |
| scope_start | timestamptz | нижняя граница времени событий |
| request_started_at | timestamptz | начало запроса загрузки; основа следующего updatedafter |
| completed_at | timestamptz | окончание записи |
| response_count, event_count | integer | число сохранённых ответов и строк событий |

### stg.api_responses

Одна строка на ответ API. `id bigserial PK`, `batch_id text FK`,
`request_url text`, `request_params jsonb`, `payload jsonb` — полный ответ,
`fetched_at timestamptz` — время получения. Сохраняются и пустые ответы. HTTP 204 без тела представлен техническим JSON
`{"type":"FeatureCollection","features":[],"_http_status":204}`.

### stg.earthquake_raw

Одна строка на событие в конкретной загрузке (сырой GeoJSON Feature).

| Поле | Тип | Значение |
|---|---|---|
| id | bigserial PK | порядок загрузки / watermark роли 2 |
| batch_id | text FK | ссылка на stg.batches |
| response_id | bigint FK | ссылка на полный ответ |
| event_id | text | бизнес-ключ USGS Feature.id |
| source_updated_at | timestamptz | properties.updated из API |
| payload_hash | text | SHA-256 полного Feature, техническая дедупликация |
| payload | jsonb | исходный Feature, без изменения полей |
| loaded_at | timestamptz | время получения ответа |

STG id растёт, но может содержать пропуски после отката транзакции;
не считайте его непрерывной нумерацией.

Все времена — UTC. В GeoJSON `time` и `updated` — миллисекунды Unix epoch.
`coordinates` = **[долгота, широта, глубина в км]**. Значения `mag` и других
атрибутов могут быть null. Значение `tsunami` — флаг источника, его нельзя
интерпретировать как подтверждённое цунами.

Одинаковые события в разных batch допустимы. Повтор задачи с тем же run_id
возвращает существующий batch, не обращается к API и не добавляет строки.
Вся загрузка атомарна; ошибка API/БД откатывает batch, ответы и события.
Удаления STG пока нет: участник 2 может разрабатывать без риска потери входных данных.
Большие JSON не передаются через XCom: там только количество строк.

Пример разбора (не реализация DDS):

```sql
SELECT id AS stg_id, event_id, source_updated_at, loaded_at,
       to_timestamp((payload->'properties'->>'time')::double precision / 1000) AS event_time,
       (payload->'properties'->>'mag')::numeric AS magnitude,
       payload->'properties'->>'magType' AS magnitude_type,
       payload->'properties'->>'status' AS review_status,
       payload->'properties'->>'type' AS event_type,
       payload->'properties'->>'net' AS network_code,
       payload->'properties'->>'place' AS place,
       (payload->'geometry'->'coordinates'->>0)::numeric AS longitude,
       (payload->'geometry'->'coordinates'->>1)::numeric AS latitude,
       (payload->'geometry'->'coordinates'->>2)::numeric AS depth_km
FROM stg.earthquake_raw ORDER BY id;
```

## Предлагаемый интерфейс DDS (реализует роль 2)

До параллельной разработки подтвердите названия в команде. Менять контракт можно,
но нужно синхронно обновить этот документ и SQL роли 3.

`dds.event_versions`:

- `version_id bigint PK`, `event_id text`, `event_time timestamptz`;
- `magnitude numeric NULL`, `magnitude_type text NULL`, `depth_km numeric NULL`;
- `longitude numeric`, `latitude numeric`, `place text NULL`;
- `event_type text`, `review_status text`, `network_code text`;
- `source_updated_at timestamptz`, `stg_id bigint`;
- `valid_from timestamptz`, `valid_to timestamptz NULL`, `is_current boolean`;
- `row_hash text` — хеш значимых бизнес-атрибутов.

Также `dds.etl_state` с последним обработанным STG id и при необходимости
справочники типов/сетей. Для компактной лабы достаточно таблицы версий и небольших
справочников; общее число таблиц не является целью.

Правила SCD2:

1. Обрабатывать новые STG-строки по порядку id. Бизнес-ключ — event_id.
2. Новый event_id → первая версия; изменение бизнес-атрибутов → закрыть старую
   и создать новую; неизменившийся набор → новую версию не создавать.
3. `loaded_at`, URL и техническое `updated` не должны сами создавать новую версию.
   STG payload_hash нельзя бездумно использовать как DDS row_hash.
4. `valid_from` — момент наблюдения состояния (`loaded_at`), интервал [from,to).
   `source_updated_at` хранить отдельно: это время правки у источника.
5. Backfill может принести устаревшее состояние: не заменять текущую версию
   записью с меньшим source_updated_at. Обработку равных времён явно определить.
6. Изменение версий и watermark — одна транзакция. Одна текущая версия на event_id,
   непересекающиеся интервалы, повторный запуск без дублей.

Проверки роли 2: новая сущность; неизменившийся повтор; изменение магнитуды/статуса;
поздняя старая запись; два изменения в одной загрузке; откат при ошибке.
Контролируемые тестовые версии хранить в tests, отдельно от реального STG.

## Витрины CDM (реализовано ролью 3)

Итоговая модель; подробные поля и метрики — в [dashboard.md](dashboard.md).

| Таблица CDM | Зерно / назначение |
|---|---|
| daily_activity | дата UTC × сеть × корзина магнитуды 0,5 × класс глубины; число событий, reviewed, полнота, суммы и максимумы магнитуды и глубины |
| event_map | одно актуальное землетрясение; время, координаты, магнитуда, глубина, классы, статус, число версий |
| review_summary | сеть × возраст события в сутках × класс магнитуды; количество и доля reviewed (вместо «дата × сеть»: дату покрывает daily_activity) |
| revision_summary | событие; число наблюдавшихся версий, первая/текущая магнитуда, статус, глубина, сдвиг эпицентра, часы до reviewed |
| event_history | версия текущего землетрясения; цепочка SCD2 для дашборда «История события» |
| review_dynamics | час наблюдения × сеть × класс магнитуды; time travel по SCD2 |
| pipeline_health, data_quality | запуски STG → DDS; отбраковка, полнота полей, типы событий |

Только `event_type='earthquake'`. При необходимости порог M≥2.5 задаётся в витринах,
а не в STG. Число событий и карта считаются по текущим версиям, иначе SCD2 завысит
числа. Для null магнитуды выделить отдельную категорию; знаменатель доли reviewed —
все события соответствующей группы. Метрики уточнений относятся только к периоду
наблюдения нашим пайплайном. Географические разрезы — карта/координаты: `place`
является свободным текстом, надёжной страны в API нет.

CDM читает только DDS; полная пересборка витрин в одной транзакции допустима.
Роль 3 сохраняет SQL карточек, скриншоты и шаги создания дашборда в docs/dashboard.md.
Metabase хранит настройки в собственном PostgreSQL-томе, Git не переносит этот том.
