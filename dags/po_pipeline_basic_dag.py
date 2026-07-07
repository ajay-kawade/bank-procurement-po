"""
po_pipeline_basic_dag.py

End-to-end DAG: Raw -> Staging -> Curated, with DQ checks on dim_vendor
before the fact load runs on top of it.

Flow:
    load_staging_vendor   ┐
    load_staging_po_lines ┘
              ↓
    expire_changed_vendors  (SCD2 pass 1)
              ↓
    insert_new_vendor_versions  (SCD2 pass 2)
              ↓
    dq_check_dim_vendor   <-- hard stop if dim_vendor is corrupt
              ↓
    load_fact_po_line_items
"""

from datetime import datetime

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.models import Variable

PROJECT_ID = Variable.get("project_id")
LOCATION = Variable.get("bq_location")

with DAG(
    dag_id="po_pipeline_basic",
    description="Raw -> Staging -> Curated (dim_vendor SCD2 + fact_po_line_items)",
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["procurement", "basic"],
) as dag:

    load_staging_vendor = BigQueryInsertJobOperator(
        task_id="load_staging_vendor",
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

    load_staging_po_lines = BigQueryInsertJobOperator(
        task_id="load_staging_po_lines",
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

    expire_changed_vendors = BigQueryInsertJobOperator(
        task_id="expire_changed_vendors",
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

    insert_new_vendor_versions = BigQueryInsertJobOperator(
        task_id="insert_new_vendor_versions",
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

    # DQ check runs as a BigQuery scripting job. The SQL uses RAISE to
    # throw an error if checks fail, which makes this Airflow task fail
    # and stops the fact load running on top of corrupt dimension data.
    dq_check_dim_vendor = BigQueryInsertJobOperator(
        task_id="dq_check_dim_vendor",
        project_id=PROJECT_ID,
        location=LOCATION,
        gcp_conn_id="google_cloud_default",
        configuration={
            "query": {
                "query": "{% include 'sql/load/dq_check_dim_vendor.sql' %}",
                "useLegacySql": False,
            }
        },
    )

    load_fact_po_line_items = BigQueryInsertJobOperator(
        task_id="load_fact_po_line_items",
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

    # dimensions must load before the fact, so the fact join has current
    # surrogate keys to resolve against.
    # DQ check sits between SCD2 and fact load -- if dim_vendor is corrupt,
    # we stop here rather than propagating bad keys into the fact table.
    [load_staging_vendor, load_staging_po_lines] >> expire_changed_vendors
    expire_changed_vendors >> insert_new_vendor_versions
    insert_new_vendor_versions >> dq_check_dim_vendor
    dq_check_dim_vendor >> load_fact_po_line_items
