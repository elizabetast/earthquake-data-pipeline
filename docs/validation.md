# Проверка реализации роли 1

Дата: 26 октября 2026.

- `python3 -m unittest discover -s tests -v`: 6 проверок прошли.
- `python3 -m compileall -q dags tests scripts`: синтаксических ошибок нет.
- `docker compose config --quiet`: конфигурация корректна.
- Реальный USGS запрос с конечными параметрами: HTTP 200, GeoJSON FeatureCollection.
- PostgreSQL поднят из Compose, все три таблицы STG созданы init-скриптом.
- `scripts/check_storage.py --local`: проверки на реальном PostgreSQL в отдельной
  временной БД прошли: дедупликация, повтор batch без обращения к API,
  откат загрузки при ошибке, watermark инкремента, backfill, валидация параметров.
  Тестовая БД удаляется скриптом; реальные таблицы не меняются.

Полная проверка запуска Airflow, получения истории и Metabase выполняется;
результат будет добавлен после завершения. DDS/CDM и дашборды не реализованы:
это работа участников 2 и 3.

Повторить интеграционную проверку внутри окружения:

```bash
docker compose exec -T airflow-scheduler python < scripts/check_storage.py
```
