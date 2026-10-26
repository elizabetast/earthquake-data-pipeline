# LR3 — мониторинг землетрясений USGS

Лабораторная №3 по ППАД, команда из трёх человек.

```text
USGS API → STG (сырой JSON) → DDS (SCD2) → CDM (витрины) → Metabase
           роль 1             роль 2       роль 3          роль 3
Airflow и Docker Compose — роль 1
```

Цель: показать динамику сейсмической активности и уточнение магнитуды, глубины
и статуса событий. Источник — [USGS Earthquake Catalog API](https://earthquake.usgs.gov/fdsnws/event/1/),
без API-ключа. Версионирование реализуется в DDS, витрины читают только DDS.

## План и разделение работы

| Участник | Ответственность | Файлы / результат | Статус |
|---|---|---|---|
| Роль 1 | Окружение, оркестрация, сбор и STG; документация передачи | compose.yaml, dags/, sql/init/, тесты сборщика, README и контракт | Реализовано |
| Роль 2 | Проектирование DDS, STG → DDS, SCD2, идемпотентность и проверки | sql/dds_load.sql, тесты DDS, описание модели | Предстоит |
| Роль 3 | Проектирование CDM, DDS → CDM, метрики и Metabase | sql/cdm_load.sql, docs/dashboard.md, SQL карточек и скриншоты | Предстоит |

Каждый участник документирует и проверяет свой этап. Точную равную трудоёмкость
заранее гарантировать нельзя; объём сбалансирован самостоятельным моделированием,
загрузкой и проверками у каждой роли. Docker входит в роль 1, отдельной роли 4 нет.

Порядок работы:

1. Вместе подтвердить [контракт данных](docs/contracts.md) и набор метрик.
2. Роль 1 передаёт рабочий STG. Роли 2 и 3 параллельно проектируют свои модели;
   роль 3 может разрабатывать SQL по согласованному интерфейсу DDS.
3. Роль 2 подключает свой SQL и проверяет SCD2. Роль 3 подключает витрины и BI.
4. Команда проверяет сквозной запуск и повторный запуск без дублей, готовит защиту.

## Что сделано ролью 1

- Docker Compose: Airflow 2.10.5 / Python 3.11, PostgreSQL 16, Metabase 0.56.1.
- Раздельные служебные БД Airflow/Metabase и хранилище со схемами stg, dds, cdm.
- Начальная история за 90 суток; затем каждый час — новые и изменённые события.
- Ручная дозагрузка 1–365 суток, UTC, разбиение истории по дням и больших ответов
  пополам, HTTP timeout, два повтора временных HTTP-ошибок и три повтора задачи Airflow.
- Полные ответы API и исходный Feature каждого события в JSONB; метаданные запросов.
- Атомарная запись всей загрузки и идемпотентность по Airflow run_id.
- Точки подключения SQL следующих ролей и пользователь BI с доступом только к CDM.

DDS, SCD2, CDM и дашборды намеренно оставлены задачами участников 2 и 3.
Состояние фактических проверок записано в [docs/validation.md](docs/validation.md).

## Быстрый старт

Нужны Git и запущенный Docker Desktop с Docker Compose v2. Рекомендуется выделить
Docker 8 ГБ RAM (минимум Airflow — 4 ГБ). Локальный Python устанавливать не нужно.

Публичный репозиторий: [elizabetast/earthquake-data-pipeline](https://github.com/elizabetast/earthquake-data-pipeline).

```bash
git clone https://github.com/elizabetast/earthquake-data-pipeline.git
cd earthquake-data-pipeline
```

В папке проекта:

```bash
cp .env.example .env
# Windows PowerShell: Copy-Item .env.example .env
docker compose up -d
```

Первый запуск скачивает образы, создаёт БД и пользователя Airflow. Проверка:

```bash
docker compose ps
docker compose logs --tail=50 airflow-init airflow-scheduler
```

| Сервис | Адрес | Доступ по умолчанию |
|---|---|---|
| Airflow | http://localhost:8081 | admin / admin |
| Metabase | http://localhost:3334 | первичная регистрация при первом открытии |
| PostgreSQL с ноутбука | localhost:15433 | БД earthquakes, пользователь lab, пароль lab_password |

Порты сервисов можно изменить в `.env`.
В контейнерах хост хранилища — `warehouse`, порт — `5432`.
Пароли учебные, сервисы открыты только на localhost. `.env` в Git не загружается.
Смену пароля БД после создания тома нужно делать в самой БД, а не только в `.env`.

## Запуск сбора и проверка STG

DAG `earthquake_pipeline` включён при создании, расписание — каждый час в :15 UTC,
`catchup=False`: пропущенные часы автоматически не воспроизводятся.
Чтобы получить данные сразу, в Airflow откройте DAG и нажмите Trigger DAG.
Можно также выполнить:

```bash
docker compose exec airflow-scheduler airflow dags trigger earthquake_pipeline
```

Первый обычный запуск загружает 90 суток истории. Следующие ищут изменения
через `updatedafter` с перекрытием 10 минут; watermark хранится в `stg.batches`.
При длительном простое обычный запуск заберёт накопившиеся изменения в области
первичной истории. Для принудительной дозагрузки через UI задайте JSON-конфигурацию
`{"days_back": 30}` либо:

```bash
docker compose exec airflow-scheduler airflow dags trigger earthquake_pipeline --conf '{"days_back":30}'
```

Backfill не сдвигает watermark регулярного сбора. Исторические внутренние версии
USGS не восстанавливаются; SCD2 сохраняет состояния, которые увидел наш сборщик.
Полная область и ограничения описаны в [контракте](docs/contracts.md).

Проверить таблицы:

```bash
docker compose exec -T warehouse psql -U lab -d earthquakes < sql/check_stg.sql
```

Ожидается: batch с числом ответов, сырые события, отсутствие технических дублей.
Успешный пустой ответ API тоже сохраняется. При ошибке вся загрузка откатывается;
проверяйте Logs задачи `collect_to_stg` в Airflow.

## Инструкция для роли 2

1. Поднять окружение и запустить сбор. Изучить STG и [контракт DDS](docs/contracts.md).
2. Реализовать DDL и загрузку в `sql/dds_load.sql`. Скрипт запускается целиком в одной
   транзакции после STG, подключение уже настроено. Не писать собственный COMMIT.
3. Создавать таблицы через `CREATE TABLE IF NOT EXISTS` в этом же файле:
   `sql/init/` выполняется только при создании пустого тома PostgreSQL.
4. Реализовать SCD2, контроль старых версий и собственный watermark по STG id.
5. Проверить новое событие, изменение, неизменившийся повтор, позднюю запись,
   отсутствие нескольких current-версий и пересечений интервалов.
6. Закоммитить SQL, проверки и описание. Python/DAG менять обычно не требуется.

## Инструкция для роли 3

1. Согласовать DDS с ролью 2; модель предложена в контракте.
2. В `sql/cdm_load.sql` реализовать DDL и идемпотентную загрузку **только из DDS**.
3. Построить витрины активности по дням, карты событий, проверки статусов и уточнений.
4. В Metabase добавить PostgreSQL: host `warehouse`, port `5432`, database
   `earthquakes`, username `bi_reader`, password `bi_reader`, SSL выключен.
   Этот пользователь видит только CDM. Синхронизировать схему после появления витрин.
5. Сделать дашборд с фильтрами даты и магнитуды, описать метрики и сохранить
   SQL карточек, скриншоты и воспроизводимые шаги в `docs/dashboard.md`.

Пока `dds_load.sql` содержит только комментарии, `stg_to_dds` имеет статус skipped,
а `dds_to_cdm` тоже пропускается по зависимостям. Если DDS уже реализован, а CDM ещё
нет, пропускается только CDM. Это ожидаемо: данные STG уже сохранены.
После заполнения SQL достаточно повторно запустить DAG, контейнеры пересобирать
не нужно. Если SQL упал, можно очистить (Clear) только упавшую задачу и downstream:
повторный сбор не обязателен. Очистка `collect_to_stg` того же запуска использует
сохранённый batch; для нового запроса API создайте новый запуск DAG.

## Git: передача проекта команде

В Git храним код, SQL, тесты и документацию. Базы, история STG и настройки Metabase
находятся в локальных Docker-томах и через Git не передаются. Каждый участник
загружает данные у себя. Для общей защиты выберите один ноутбук и воспроизведите
дашборд по инструкции роли 3.

После первого размещения репозитория:

```bash
git clone <URL_РЕПОЗИТОРИЯ>
cd <ПАПКА_РЕПОЗИТОРИЯ>
git switch -c role2/dds    # участник 2
# участник 3 создаёт свою ветку: git switch -c role3/cdm-dashboard
```

Каждый меняет свои файлы, делает commit/push и Pull Request в main. Владельцу
репозитория нужно добавить обоих участников через Settings → Collaborators.
URL репозитория указан в разделе «Быстрый старт».

## Проверки и обслуживание

Быстрые локальные проверки без Airflow:

```bash
python3 -m unittest discover -s tests -v
docker compose config --quiet
```

Проверка DAG в настоящем окружении:

```bash
docker compose exec airflow-scheduler airflow dags list-import-errors
```

Остановка с сохранением данных:

```bash
docker compose stop
docker compose start
```

`docker compose down` тоже сохраняет тома. **Не добавляйте `-v`**, если нужно
сохранить историю: `down -v` удаляет БД и настройки дашбордов.
STG автоматически не очищается; для объёма учебного проекта сохраняем всю историю.

## Источники

- [Постановка исходной лабораторной](https://github.com/kinomant/ppa-for-da/tree/master/airflow_workshop).
- [USGS API](https://earthquake.usgs.gov/fdsnws/event/1/) и [GeoJSON](https://earthquake.usgs.gov/earthquakes/feed/v1.0/geojson.php).
- [Airflow 2.10.5: Docker Compose](https://airflow.apache.org/docs/apache-airflow/2.10.5/howto/docker-compose/index.html).
- [Metabase: Docker](https://www.metabase.com/docs/latest/installation-and-operation/running-metabase-on-docker).
