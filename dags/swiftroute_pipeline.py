from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from datetime import datetime

default_args = {
    'owner': 'data_engineer',
    'start_date': datetime(2026, 1, 1), 
    'retries': 0,
}

with DAG(
    dag_id='swiftroute_medallion_pipeline',
    default_args=default_args,
    schedule_interval=None, 
    catchup=False,
    tags=['swiftroute', 'lakehouse', 'glue'],
) as dag:

    run_silver_job = GlueJobOperator(
        task_id='silver_etl',
        job_name='swiftroute-bronze-to-silver-etl',
        iam_role_name='AWSGlueServiceRole-SwiftRoute', 
        script_location='s3://swiftroute-logistics-de-bucket/scripts/silver_etl.py',
        aws_conn_id='aws_default',
        region_name='us-east-1',
        wait_for_completion=True 
    )

    run_gold_job = GlueJobOperator(
        task_id='gold_etl',
        job_name='swiftroute-silver-to-gold-etl',
        iam_role_name='YOUR_GLUE_IAM_ROLE_NAME', 
        script_location='s3://swiftroute-logistics-de-bucket/scripts/gold_etl.py',
        aws_conn_id='aws_default',
        region_name='us-east-1',
        wait_for_completion=True
    )

    run_silver_job >> run_gold_job