"""
Component 7: Airflow DAG — mfs_daily_pipeline

Schedule: daily at 01:00 UTC (07:00 Asia/Dhaka), processing the previous day.
Logical date (ds) = the ingest_date being processed.

Task flow:
  wait_for_bronze → bronze_to_silver → load_silver → dbt_build → publish_audit

  dbt_build is a placeholder until Week 4.
"""
from datetime import timedelta

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sensors.python import PythonSensor
from pendulum import datetime as pendulum_datetime

DATA_PATH    = "/data"
PG_CONN_ID   = "mfs_dw"
SPARK_JOBS   = "/opt/spark-jobs"
PG_URL       = "jdbc:postgresql://postgres-dw:5432/mfs_dw"


def _bronze_partition_exists(ds, **kwargs):
    """Return True when the bronze partition for `ds` exists and has Parquet files."""
    import os
    path = f"{DATA_PATH}/lake/bronze/transactions/ingest_date={ds}"
    if not os.path.isdir(path):
        return False
    return any(f.endswith(".parquet") for f in os.listdir(path))


@dag(
    dag_id="mfs_daily_pipeline",
    description="Bronze → Silver → Gold daily pipeline for MFS transactions",
    schedule="0 1 * * *",
    start_date=pendulum_datetime(2026, 11, 9, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["mfs", "batch", "silver"],
)
def mfs_daily_pipeline():

    # ------------------------------------------------------------------
    # Task 1: Wait until the bronze partition exists and has Parquet files.
    # Pokes every 60 s, times out after 1 hour.
    # ------------------------------------------------------------------
    wait_for_bronze = PythonSensor(
        task_id="wait_for_bronze",
        python_callable=_bronze_partition_exists,
        mode="poke",
        poke_interval=60,
        timeout=180,
    )

    # ------------------------------------------------------------------
    # Task 2: Spark batch — bronze → silver
    # Runs bronze_to_silver.py with local[2] mode inside this Airflow
    # container (PySpark + Java are pre-installed in the custom image).
    # The PostgreSQL connector JAR is downloaded on first run and cached
    # in ~/.ivy2 inside the container.
    # ------------------------------------------------------------------
    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command=(
            "/home/airflow/.local/bin/spark-submit "
            "--master local[2] "
            "--packages org.postgresql:postgresql:42.7.3 "
            f"--py-files {SPARK_JOBS}/schemas.py "
            f"{SPARK_JOBS}/bronze_to_silver.py "
            "--ingest-date {{ ds }}"
        ),
        env={
            "LAKE_BASE":    f"{DATA_PATH}/lake",
            "PG_URL":       PG_URL,
            "PG_USER":      "mfs",
            "PG_PASSWORD":  "mfs",
        },
    )

    # ------------------------------------------------------------------
    # Task 3: Upsert from staging → silver (idempotent)
    # ON CONFLICT DO NOTHING means rerunning the same day is safe:
    # existing rows are skipped, so row counts never double.
    # ------------------------------------------------------------------
    load_silver = SQLExecuteQueryOperator(
        task_id="load_silver",
        conn_id=PG_CONN_ID,
        sql="""
            INSERT INTO silver.transactions
            SELECT * FROM silver.transactions_stage
            ON CONFLICT (txn_id) DO NOTHING;
        """,
    )

    # ------------------------------------------------------------------
    # Task 4 (placeholder): dbt build — Week 4
    # ------------------------------------------------------------------
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command="echo '[dbt_build] Not yet implemented — Week 4 milestone'",
    )

    # ------------------------------------------------------------------
    # Task 5: Publish audit summary to Airflow logs
    # ------------------------------------------------------------------
    @task(task_id="publish_audit")
    def publish_audit(ds=None):
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        hook = PostgresHook(postgres_conn_id=PG_CONN_ID)  # hook still uses postgres provider
        rows = hook.get_records(
            """
            SELECT batch_run_id, rows_read, rows_valid, rows_rejected,
                   duplicates_removed, rows_inserted,
                   EXTRACT(EPOCH FROM (finished_at - started_at))::INT AS duration_secs
            FROM silver.batch_audit
            WHERE ingest_date = %s
            ORDER BY started_at DESC
            LIMIT 1
            """,
            parameters=[ds],
        )
        if rows:
            r = rows[0]
            print(
                f"\n{'='*52}\n"
                f"  Audit summary: {ds}\n"
                f"  batch_run_id      : {r[0]}\n"
                f"  rows_read         : {r[1]}\n"
                f"  rows_valid        : {r[2]}\n"
                f"  rows_rejected     : {r[3]}\n"
                f"  duplicates_removed: {r[4]}\n"
                f"  rows_inserted     : {r[5]}\n"
                f"  duration          : {r[6]}s\n"
                f"{'='*52}"
            )
        else:
            print(f"No audit record found for ingest_date={ds}")

    wait_for_bronze >> bronze_to_silver >> load_silver >> dbt_build >> publish_audit()


mfs_daily_pipeline()
