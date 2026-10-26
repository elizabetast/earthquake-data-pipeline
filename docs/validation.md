# Проверка реализации роли 1

Дата: 26 октября 2026.

- `python3 -m unittest discover -s tests -v`: 8 проверок прошли.
- `python3 -m compileall -q dags tests scripts`: синтаксических ошибок нет.
- `docker compose config --quiet`: конфигурация корректна.
- Реальный USGS запрос с конечными параметрами: HTTP 200, GeoJSON FeatureCollection.
- PostgreSQL поднят из Compose, все три таблицы STG созданы init-скриптом.
- `scripts/check_storage.py --local`: проверки на реальном PostgreSQL в отдельной
  временной БД прошли: дедупликация, повтор batch без обращения к API,
  откат загрузки при ошибке, watermark инкремента, backfill, валидация параметров.
  Тестовая БД удаляется скриптом; реальные таблицы не меняются.

## Проверка всего окружения

- Все шесть постоянных сервисов Compose запущены, airflow-init успешно завершён.
- `airflow dags list-import-errors`: ошибок импорта нет.
- Интеграционный скрипт внутри контейнера Airflow / Python 3.11: PASS.
- Airflow `/health`: scheduler и metadatabase healthy; Metabase `/api/health`: ok.
- Автоматический запуск `scheduled__2026-10-26T19:15:00+00:00`: success.
  За 90 суток получено 90 ответов API и 33 662 различных события.
- Повтор сохранённого batch вернул 33 662 и не добавил записей / запросов API.
- `manual__incremental_validation`: success, один ответ, 9 сырых наблюдений.
  Повторные наблюдения событий в новом batch допустимы, SCD2 будет задачей роли 2.
- `manual__backfill_validation` с `days_back=1`: один ответ, 200 наблюдений;
  watermark регулярного сбора не изменён.
- Пользователь bi_reader: USAGE на CDM разрешён, на STG и DDS запрещён.
- Проверка SQL на технические дубли внутри batch: 0 строк.

Первый отдельный запуск проверки с ноутбука встретил HTTP timeout и откатился;
после добавления HTTP-повторов загрузка через Airflow завершилась успешно.
Пропуски числовых STG id после отката ожидаемы.

DDS/CDM и дашборды ещё не реализованы: задачи stg_to_dds и dds_to_cdm
ожидаемо skipped. Это работа участников 2 и 3.

Повторить интеграционную проверку внутри окружения:

```bash
docker compose exec -T airflow-scheduler python < scripts/check_storage.py
```
