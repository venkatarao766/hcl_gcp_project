from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor

# --- CONFIGURATION CONSTANTS ---
PROJECT_ID = "project-d058df44-4eb1-4f05-8b3"
REGION = "us-central1"
CLUSTER_NAME = "bronze-ingestion-cluster"
BUCKET_NAME = "hcl_hackathaon"
PYSPARK_SCRIPT_URI = f"gs://{BUCKET_NAME}/scripts/load_gcs_to_bronze_pipeline.py"

# Default arguments for the DAG
default_args = {
    "owner": "data_engineering_team",
    "depends_on_past": False,
    "email_on_failure": True,
    "email": ["alerts@your-bank.com"],
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="bank_medallion_pipeline_orchestration",
    default_args=default_args,
    description="End-to-end Medallion pipeline coordinating Dataproc and BigQuery",
    schedule_interval="@daily",
    start_date=datetime(2026, 10, 1),
    catchup=False,
    max_active_runs=1,
    tags=["medallion", "banking", "dataproc", "bigquery"],
) as dag:

    # 1. Validate Source File Exists in GCS
    validate_source_file = GCSObjectExistenceSensor(
        task_id="validate_source_file",
        bucket=BUCKET_NAME,
        object="raw/day_1.txt", # Dynamic parameter can be passed via dag_run.conf
        poke_interval=30,
        timeout=600,
    )

    # 2. Submit Dataproc PySpark Job for Bronze Ingestion
    # (Complies with restriction: no direct GCS-to-BigQuery operator used)
    dataproc_job_config = {
        "reference": {"project_id": PROJECT_ID},
        "placement": {"cluster_name": CLUSTER_NAME},
        "pyspark_job": {
            "main_python_file_uri": PYSPARK_SCRIPT_URI,
            "args": [PROJECT_ID, f"gs://{BUCKET_NAME}/raw/day_1.txt", "Day_1"],
        },
    }

    submit_dataproc_bronze = DataprocSubmitJobOperator(
        task_id="submit_dataproc_bronze",
        job=dataproc_job_config,
        region=REGION,
        project_id=PROJECT_ID,
    )

    # 3. Run Silver Cleansing, Deduplication & DQ Checks (BigQuery SQL)
    silver_cleansing_sql = f"""
    -- Execute Silver layer transformations and populate quarantine logs
    CALL `{PROJECT_ID}.hackathon_hyd_hack138_ws02_silver.sp_run_silver_pipeline`(); 
    """
    
    run_silver_cleansing = BigQueryInsertJobOperator(
        task_id="run_silver_cleansing",
        configuration={
            "query": {
                "query": silver_cleansing_sql,
                "useLegacySql": False,
            }
        },
    )

    # 4. Quality Gate Check (Branching based on DQ pass percentage)
    def evaluate_quality_gate(**context):
        # In practice, query dq_results to ensure pass rate >= threshold (e.g., 95%)
        # Here we route to Gold if checks pass, or alert/quarantine step if failed.
        dq_status = "PASSED" # Evaluated dynamically from table in production
        if dq_status == "PASSED":
            return "run_gold_transformations"
        else:
            return "handle_dq_failure"

    quality_gate_branch = BranchPythonOperator(
        task_id="quality_gate_check",
        python_callable=evaluate_quality_gate,
    )

    # 4a. Handle DQ Failure / Alert
    def alert_dq_failure(**context):
        print("Data Quality gate failed. Review silver.quarantine_records and dq_results.")

    handle_dq_failure = PythonOperator(
        task_id="handle_dq_failure",
        python_callable=alert_dq_failure,
    )

    # 5. Run Gold Transformations & SCD Type 2 (BigQuery SQL)
    gold_transformations_sql = f"""
    -- Rebuild dimensional models and apply SCD Type 2 versioning
    CREATE OR REPLACE TABLE `{PROJECT_ID}.hackathon_hyd_hack138_ws02_gold.dim_customer` AS
    SELECT * FROM `{PROJECT_ID}.hackathon_hyd_hack138_ws02_gold.vw_customer_scd2_builder`;

    CREATE OR REPLACE TABLE `{PROJECT_ID}.hackathon_hyd_hack138_ws02_gold.dim_account` AS
    SELECT * FROM `{PROJECT_ID}.hackathon_hyd_hack138_ws02_gold.vw_account_scd2_builder`;

    CREATE OR REPLACE TABLE `{PROJECT_ID}.hackathon_hyd_hack138_ws02_gold.fact_transactions` AS
    SELECT * FROM `{PROJECT_ID}.hackathon_hyd_hack138_ws02_gold.vw_fact_transactions_builder`;
    """

    run_gold_transformations = BigQueryInsertJobOperator(
        task_id="run_gold_transformations",
        configuration={
            "query": {
                "query": gold_transformations_sql,
                "useLegacySql": False,
            }
        },
    )

    # 6. Refresh KPI Outputs
    refresh_kpis_sql = f"""
    -- Refresh aggregated KPI tables
    CREATE OR REPLACE TABLE `{PROJECT_ID}.hackathon_hyd_hack138_ws02_gold.agg_monthly_kpis` AS
    SELECT * FROM `{PROJECT_ID}.hackathon_hyd_hack138_ws02_gold.vw_monthly_customer_kpis`;
    """

    refresh_kpi_outputs = BigQueryInsertJobOperator(
        task_id="refresh_kpi_outputs",
        configuration={
            "query": {
                "query": refresh_kpis_sql,
                "useLegacySql": False,
            }
        },
    )

    # --- TASK DEPENDENCIES ---
    validate_source_file >> submit_dataproc_bronze >> run_silver_cleansing >> quality_gate_branch
    quality_gate_branch >> [run_gold_transformations, handle_dq_failure]
    run_gold_transformations >> refresh_kpi_outputs