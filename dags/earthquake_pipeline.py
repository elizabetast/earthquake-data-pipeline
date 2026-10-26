"""Роль 1: история/изменения USGS -> STG -> SQL ролей 2 и 3."""
from datetime import datetime, timedelta, timezone

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


@dag(dag_id="earthquake_pipeline", schedule="15 * * * *",
     start_date=datetime(2026, 1, 1, tzinfo=timezone.utc), catchup=False,
     max_active_runs=1, dagrun_timeout=timedelta(hours=2),
     default_args={"retries": 3, "retry_delay": timedelta(minutes=2)},
     tags=["LR3", "USGS", "role-1"])
def earthquake_pipeline():
    @task(execution_timeout=timedelta(minutes=90))
    def collect_to_stg():
        from earthquake_common.storage import load
        ctx = get_current_context()
        # Ручной запуск {"days_back": 30} загружает историю за 30 суток.
        return load(ctx["run_id"], ctx["dag_run"].conf.get("days_back"))

    @task
    def stg_to_dds():
        from earthquake_common.storage import run_sql_file
        run_sql_file("dds_load.sql")

    @task
    def dds_to_cdm():
        from earthquake_common.storage import run_sql_file
        run_sql_file("cdm_load.sql")

    collect_to_stg() >> stg_to_dds() >> dds_to_cdm()


earthquake_pipeline()
