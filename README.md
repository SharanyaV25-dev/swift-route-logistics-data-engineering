# SwiftRoute Logistics: Cloud Lakehouse & Automated Data Pipeline

An end-to-end, production-grade cloud data engineering project simulating a high-throughput logistics operational platform. The pipeline ingests transactional operational data into an AWS-native Medallion Lakehouse using Apache Iceberg, enforces enterprise data quality with PySpark, orchestrates multi-stage ETL workflows with Apache Airflow, and automates continuous deployment via GitHub Actions.

---

## Architecture

### End-to-End Pipeline Flow

1. **Source & Bronze Layer:** Raw operational logistics events (loads, trips, delivery events, facilities, drivers) are ingested as immutable batch snapshots into Amazon S3 Bronze.
2. **Silver Layer (Validation & Cleansing):** PySpark jobs running on AWS Glue extract raw data, enforce schema constraints, perform entity deduplication, validate foreign-key relationships, and quarantine corrupted records into dedicated S3 quarantine prefixes before writing clean datasets.
3. **Gold Layer (Analytical Marts & Apache Iceberg):** Curated Silver datasets are aggregated into optimized analytical tables using the Apache Iceberg open table format on Amazon S3, enabling ACID transactions, time travel, and hidden partitioning.
4. **Ad-Hoc & BI Serving:** Business queries and operational performance KPIs are served directly through Amazon Athena backed by the AWS Glue Data Catalog.
5. **Orchestration:** Workflows are orchestrated end-to-end via an Apache Airflow DAG utilizing the `GlueJobOperator` to manage dependency resolution and task monitoring.
6. **DataOps / CI/CD:** Automated GitHub Actions workflows deploy PySpark scripts and Airflow orchestration logic directly to S3 and runtime environments upon branch merge.

---

## Tech Stack

| Domain | Technology | Purpose |
| --- | --- | --- |
| **Compute & Processing** | PySpark, AWS Glue (Serverless) | Distributed distributed data transformations, data cleaning, and aggregations |
| **Storage & Lakehouse** | Amazon S3, Apache Iceberg | Scalable object storage with ACID transactional table formats |
| **Catalog & Serving** | AWS Glue Data Catalog, Amazon Athena | Centralized metadata schema repository and serverless Presto/Trino SQL engine |
| **Orchestration** | Apache Airflow, Docker | Code-first dependency management, scheduling, monitoring, and state alerts |
| **DevOps & CI/CD** | GitHub Actions, Git | Automated deployment of pipeline code and DAG scripts to cloud targets |
| **Security & Identity** | AWS IAM | Principle of least privilege authentication for GitHub deployers and Airflow workers |
| **Language & Tooling** | Python 3.10+, SQL, Boto3 | Core script logic, data manipulation, and cloud SDK interaction |

---

## Medallion Data Architecture

```
                    ┌────────────────────────┐
                    │ Raw Logistics Data     │
                    │ (CSV / Ingestion API)  │
                    └───────────┬────────────┘
                                │
                                ▼
┌────────────────────────────────────────────────────────────────────────┐
│ BRONZE ZONE (Amazon S3)                                                │
│ • Raw, immutable source files preserved as received                     │
│ • Ingestion metadata appended (ingest_timestamp, batch_id)             │
└───────────────────────────────┬────────────────────────────────────────┘
                                │
                                ▼  [AWS Glue: PySpark Validation]
┌────────────────────────────────────────────────────────────────────────┐
│ SILVER ZONE (Amazon S3 - Parquet)                                      │
│ • Type casting & null handling                                         │
│ • Deduplication on primary business keys                               │
│ • Business rule enforcement (positive weights, valid timestamps)       │
│ • Malformed / orphan records routed to S3 Quarantine prefix            │
└───────────────────────────────┬────────────────────────────────────────┘
                                │
                                ▼  [AWS Glue: Iceberg Transformations]
┌────────────────────────────────────────────────────────────────────────┐
│ GOLD ZONE (Amazon S3 - Apache Iceberg Tables)                          │
│ • Dimensional models & business marts                                  │
│ • Route reliability & facility detention performance metrics           │
│ • On-time delivery SLA calculations                                    │
│ • Partitioned for high-performance Amazon Athena SQL analytics         │
└────────────────────────────────────────────────────────────────────────┘

```

---

## Intentional Data Quality Engineering

To simulate production edge cases, the ingestion stream tests defensive data-engineering logic with deliberate defects handled at the Silver transformation layer:

| Entity | Injected Issue | Pipeline Resolution Strategy |
| --- | --- | --- |
| `loads` | Duplicate Load IDs | Window function deduplication retaining latest ingestion timestamp |
| `loads` | Missing Customer IDs / Null Keys | Rejection & logging into `quarantine/loads/missing_customer_id/` |
| `loads` | Negative or Zero Weights | Filtered out via business rule assertion: `weight > 0` |
| `trips` | Missing Driver IDs | Quarantined; prevented from orphan joins downstream |
| `delivery_events` | Impossible Timestamps (`pickup_time > drop_time`) | Temporal boundary validation; routed to error catalog |
| `delivery_events` | Duplicate Event Transactions | Distinct state tracking across event UUIDs |

---

## Orchestration & Pipeline Management

Orchestration is managed programmatically via **Apache Airflow** using the `swiftroute_medallion_pipeline` DAG:

* **Task Decoupling:** Airflow delegates heavy compute to AWS Glue's serverless PySpark cluster via `GlueJobOperator`, maintaining a lightweight execution footprint.
* **Deterministic Sequencing:** Upstream dependencies strictly block downstream execution (`run_silver_job >> run_gold_job`) ensuring business marts reflect fully validated source states.
* **Idempotency & Restarts:** Task state tracking enables isolated retries of failed pipeline steps without re-running successfully ingested stages.

```python
# Core Airflow DAG Definition
from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from datetime import datetime

with DAG(
    dag_id='swiftroute_medallion_pipeline',
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['swiftroute', 'lakehouse', 'glue'],
) as dag:

    run_silver_job = GlueJobOperator(
        task_id='silver_etl',
        job_name='swiftroute-bronze-to-silver-etl',
        script_location='s3://swiftroute-logistics-de-bucket/scripts/silver_etl.py',
        aws_conn_id='aws_default',
        region_name='us-east-1',
        wait_for_completion=True
    )

    run_gold_job = GlueJobOperator(
        task_id='gold_etl',
        job_name='swiftroute-silver-to-gold-etl',
        script_location='s3://swiftroute-logistics-de-bucket/scripts/gold_etl.py',
        aws_conn_id='aws_default',
        region_name='us-east-1',
        wait_for_completion=True
    )

    run_silver_job >> run_gold_job

```

---

## Repository Structure

```
swift-route-logistics-data-engineering/
├── .github/
│   └── workflows/
│       └── deploy-to-s3.yml          # CI/CD deployment pipeline for Glue scripts
├── assets/
│   └── pipeline_architecture.png     # System architecture diagram
├── dags/
│   └── swiftroute_pipeline.py        # Master Airflow orchestration DAG
├── src/
│   ├── bronze_ingestion.py           # Ingestion script to raw S3 zones
│   ├── silver_transformation.py      # PySpark validation, deduplication, & quarantine logic
│   └── gold_iceberg_marts.py         # PySpark aggregation into Apache Iceberg tables
├── validation/
│   └── data_quality_checks.py        # Schema enforcement & validation assertions
├── docker-compose.yaml               # Airflow environment service definition
├── requirements.txt                  # Python dependencies
└── README.md

```

---

## Analytical Serving Layer (Sample Business Queries)

Once the pipeline processes data into the Gold Iceberg tables, analytics can be executed directly in **Amazon Athena**:

### 1. Delivery On-Time Performance (SLA Compliance)

```sql
SELECT 
    route_id,
    COUNT(trip_id) AS total_trips,
    ROUND(AVG(CASE WHEN is_delayed = 0 THEN 1.0 ELSE 0.0 END) * 100, 2) AS on_time_percentage,
    ROUND(AVG(delay_duration_minutes), 1) AS avg_delay_minutes
FROM 
    "glue_database"."gold_route_performance"
GROUP BY 
    route_id
HAVING 
    COUNT(trip_id) >= 50
ORDER BY 
    on_time_percentage ASC;

```

### 2. Facility Detention & Congestion Analysis

```sql
SELECT 
    facility_id,
    COUNT(event_id) AS total_checkins,
    ROUND(AVG(detention_time_minutes), 2) AS avg_detention_minutes,
    MAX(detention_time_minutes) AS peak_detention_minutes
FROM 
    "glue_database"."gold_facility_performance"
GROUP BY 
    facility_id
ORDER BY 
    avg_detention_minutes DESC
LIMIT 10;

```

---

## Running the Pipeline

### Local / Cloud Orchestration Setup

1. Clone the repository:
```bash
git clone https://github.com/SharanyaV25-dev/swift-route-logistics-data-engineering.git
cd swift-route-logistics-data-engineering

```


2. Launch Airflow locally or inside a Cloud Development Environment (CDE) using Docker:
```bash
docker run -d -p 8080:8080 \
  -v $(pwd)/dags:/opt/airflow/dags \
  --name airflow apache/airflow:2.10.1 standalone

```


3. Retrieve the webserver password:
```bash
docker exec airflow cat /opt/airflow/standalone_admin_password.txt

```


4. Access `http://localhost:8080`, configure the `aws_default` connection with scoped AWS credentials, unpause `swiftroute_medallion_pipeline`, and trigger the execution run.