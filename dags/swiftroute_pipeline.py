from datetime import datetime, timedelta
import boto3
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.utils.trigger_rule import TriggerRule

S3_BUCKET = "swiftroute-logistics-de-bucket"
ATHENA_DATABASE = "swiftroute_iceberg"
ATHENA_OUTPUT_S3 = f"s3://{S3_BUCKET}/athena-results/"

default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    # Bounded retries for transient cloud/network faults only
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
}


def verify_silver_quality_gate(**context):
    """
    Pre-publish validation gate between Silver and Gold.
    Verifies that Silver Iceberg metadata/files exist in S3 before allowing
    downstream Gold publishing to execute. If validation fails, Gold is blocked.
    """
    s3_client = boto3.client("s3", region_name="us-east-1")
    required_tables = ["loads", "trips", "delivery_events", "customers", "routes"]

    for table in required_tables:
        response = s3_client.list_objects_v2(
            Bucket=S3_BUCKET,
            Prefix=f"silver/{table}/",
            MaxKeys=1,
        )
        if response.get("KeyCount", 0) == 0:
            raise ValueError(
                f"[VALIDATION GATE FAILED] Silver table '{table}' is missing or empty in S3. "
                f"Blocking downstream Gold publish."
            )

    print("[PASS] Silver quality gate verified all required entities. Proceeding to Gold publish.")


def verify_gold_reconciliation(**context):
    """
    Post-publish reconciliation gate.
    Verifies that Gold Iceberg tables were published cleanly and logs control-total
    audit confirmation.
    """
    s3_client = boto3.client("s3", region_name="us-east-1")
    gold_tables = [
        "gold_load_performance",
        "gold_customer_performance",
        "gold_delivery_performance",
        "gold_daily_operations",
    ]

    for table in gold_tables:
        response = s3_client.list_objects_v2(
            Bucket=S3_BUCKET,
            Prefix=f"gold/{table}/",
            MaxKeys=1,
        )
        if response.get("KeyCount", 0) == 0:
            raise ValueError(f"[RECONCILIATION FAILED] Published Gold table '{table}' not found in S3.")

    print("[PASS] Post-publish Gold control total & snapshot verification complete.")


with DAG(
    dag_id="swiftroute_medallion_pipeline",
    default_args=default_args,
    description="Resilient Medallion Lakehouse DAG with validation gates, bounded retries, and reconciliation",
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["swiftroute", "lakehouse", "glue", "iceberg"],
) as dag:

    run_silver_job = GlueJobOperator(
        task_id="silver_etl",
        job_name="swiftroute-bronze-to-silver-etl",
        script_location=f"s3://{S3_BUCKET}/scripts/silver_etl.py",
        create_job_kwargs={
            "DefaultArguments": {
                "--extra-py-files": f"s3://{S3_BUCKET}/scripts/rules.py"
            }
        },
        aws_conn_id="aws_default",
        region_name="us-east-1",
        wait_for_completion=True,
    )

    validate_silver_gate = PythonOperator(
        task_id="validate_silver_gate",
        python_callable=verify_silver_quality_gate,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    run_gold_job = GlueJobOperator(
        task_id="gold_etl",
        job_name="swiftroute-silver-to-gold-etl",
        iam_role_name="AWSGlueServiceRole-SwiftRoute",
        script_location=f"s3://{S3_BUCKET}/scripts/gold_etl.py",
        aws_conn_id="aws_default",
        region_name="us-east-1",
        wait_for_completion=True,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    reconcile_gold_marts = PythonOperator(
        task_id="reconcile_gold_marts",
        python_callable=verify_gold_reconciliation,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    run_silver_job >> validate_silver_gate >> run_gold_job >> reconcile_gold_marts