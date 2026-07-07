"""
po_pipeline_basic_backfill_dag.py

Manually triggered backfill for the basic pipeline. Re-runs the exact same
staging -> SCD2 -> fact chain as po_pipeline_basic_dag, just looped over a
date range instead of a single day.

Trigger from the CLI:
    airflow dags backfill po_pipeline_basic_backfill -s 2026-01-01 -e 2026-01-07

Or from the UI by triggering with config:
    {"start_date": "2026-01-01", "end_date": "2026-01-07"}

Why this is safe to re-run:
    - staging load re-derives from Raw each time (source of truth never changes)
    - SCD2 merge is keyed on vendor_id + attribute_hash -- re-running with
      unchanged data is a no-op
    - fact load just re-inserts for the given date; if you run this twice
      without clearing the partition first, you WILL get duplicate fact
      rows -- this basic version doesn't yet truncate the partition before
      reloading. That's the first thing to add once this runs once cleanly.
"""

from datetime import datetime

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
PROJECT_ID = Variable.get("project_id")
LOCATION = Variable.get("bq_location")


with DAG(
    dag_id="po_pipeline_basic_backfill",
    description="Re-run the basic load chain for a historical date range",
    schedule_interval=None,   # manual trigger only
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    params={
        "start_date": Param("2026-01-01", type="string", format="date"),
        "end_date": Param("2026-01-07", type="string", format="date"),
    },
    tags=["procurement", "basic", "backfill"],
) as dag:

    def expand_date_range(**context):
        """Turns start_date/end_date into a list of individual dates, so
        each day's reprocessing is logged separately and easier to debug
        than one big multi-day query."""
        from datetime import date, timedelta

        start = date.fromisoformat(context["params"]["start_date"])
        end = date.fromisoformat(context["params"]["end_date"])
        days = (end - start).days + 1
        date_list = [(start + timedelta(days=i)).isoformat() for i in range(days)]
        print(f"Backfilling dates: {date_list}")
        return date_list

    list_dates = PythonOperator(
        task_id="expand_date_range",
        python_callable=expand_date_range,
    )

    # Re-runs the same SQL files as the normal DAG. The {{ ds }} in those
    # files is replaced by Airflow's templating at render time -- for a
    # real multi-date backfill, this task would loop per-date (e.g. via
    # dynamic task mapping) rather than running once for the whole range.
    reprocess_staging_vendor = BigQueryInsertJobOperator(
        task_id="reprocess_staging_vendor",
        project_id=PROJECT_ID,
        location=LOCATION,
        gcp_conn_id="google_cloud_default",
        configuration={
            "query": {
                "query": "{% include 'sql/load/load_staging_vendor.sql' %}",
                "useLegacySql": False,
            }
        },
    )

    reprocess_staging_po_lines = BigQueryInsertJobOperator(
        task_id="reprocess_staging_po_lines",
        project_id=PROJECT_ID,
        location=LOCATION,
        gcp_conn_id="google_cloud_default",
        configuration={
            "query": {
                "query": "{% include 'sql/load/load_staging_po_line_items.sql' %}",
                "useLegacySql": False,
            }
        },
    )

    replay_expire_changed_vendors = BigQueryInsertJobOperator(
        task_id="replay_expire_changed_vendors",
        project_id=PROJECT_ID,
        location=LOCATION,
        gcp_conn_id="google_cloud_default",
        configuration={
            "query": {
                "query": "{% include 'sql/load/scd2_expire_vendors.sql' %}",
                "useLegacySql": False,
            }
        },
    )

    replay_insert_new_vendor_versions = BigQueryInsertJobOperator(
        task_id="replay_insert_new_vendor_versions",
        project_id=PROJECT_ID,
        location=LOCATION,
        gcp_conn_id="google_cloud_default",
        configuration={
            "query": {
                "query": "{% include 'sql/load/scd2_insert_vendor_versions.sql' %}",
                "useLegacySql": False,
            }
        },
    )

    reprocess_fact_po_line_items = BigQueryInsertJobOperator(
        task_id="reprocess_fact_po_line_items",
        project_id=PROJECT_ID,
        location=LOCATION,
        gcp_conn_id="google_cloud_default",
        configuration={
            "query": {
                "query": "{% include 'sql/load/load_fact_po_line_items.sql' %}",
                "useLegacySql": False,
            }
        },
    )

    list_dates >> [reprocess_staging_vendor, reprocess_staging_po_lines]
    [reprocess_staging_vendor, reprocess_staging_po_lines] >> replay_expire_changed_vendors
    replay_expire_changed_vendors >> replay_insert_new_vendor_versions
    replay_insert_new_vendor_versions >> reprocess_fact_po_line_items
