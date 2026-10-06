# SwiftRoute Logistics: Cloud Lakehouse & Automated Data Pipeline

[![CI/CD Pipeline](https://github.com/SharanyaV25-dev/swift-route-logistics-data-engineering/actions/workflows/ci_cd_pipeline.yml/badge.svg)](https://github.com/SharanyaV25-dev/swift-route-logistics-data-engineering/actions)
![Python](https://img.shields.io/badge/Python-3.10-blue?logo=python)
![PySpark](https://img.shields.io/badge/Apache_Spark-PySpark-E25A1C?logo=apachespark)
![Apache Iceberg](https://img.shields.io/badge/Lakehouse-Apache_Iceberg-blue)
![AWS](https://img.shields.io/badge/AWS-S3_|_Glue_|_Athena-232F3E?logo=amazonwebservices)
![Apache Airflow](https://img.shields.io/badge/Orchestration-Apache_Airflow-017CEE?logo=apacheairflow)
![Testing](https://img.shields.io/badge/Testing-Pytest-0A9EDC?logo=pytest)

An enterprise-grade cloud data engineering pipeline simulating a high-throughput logistics and fleet management platform. The lakehouse implements a **Medallion Architecture** using **AWS Glue (PySpark)** and **Apache Iceberg**, orchestrated via **Apache Airflow**, protected by **Pytest automated unit tests**, and continuously deployed through **GitHub Actions**.

---

## Architecture Overview

```
                      ┌────────────────────────┐
                      │  Raw Logistics Data    │
                      │  (Batch CSV Streams)   │
                      └───────────┬────────────┘
                                  │
                                  ▼
┌────────────────────────────────────────────────────────────────────────┐
│ BRONZE ZONE (Amazon S3)                                                │
│ • Immutable, raw source landed as ingested                             │
│ • Isolated ingestion paths per operational domain entity              │
└───────────────────────────────┬────────────────────────────────────────┘
                                  │
                                  ▼  [AWS Glue: PySpark Data Quality Engine]
┌────────────────────────────────────────────────────────────────────────┐
│ SILVER ZONE (Amazon S3 / Apache Iceberg)                               │
│ • Deterministic window deduplication on primary keys                   │
│ • Defensive NULL validation preventing silent data drops               │
│ • Strict foreign key referential integrity checks                      │
│ • Corrupted & orphan records partitioned into S3 Quarantine prefixes   │
└───────────────────────────────┬────────────────────────────────────────┘
                                  │
                                  ▼  [AWS Glue: Aggregations & Analytical Marts]
┌────────────────────────────────────────────────────────────────────────┐
│ GOLD ZONE (Amazon S3 / Apache Iceberg Tables)                          │
│ • Business dimensional models and operational performance marts        │
│ • Driver productivity, detention minutes, and route SLA compliance     │
│ • ACID transactions, time travel, and hidden partitioning via Iceberg  │
└───────────────────────────────┬────────────────────────────────────────┘
                                  │
                                  ▼
┌────────────────────────────────────────────────────────────────────────┐
│ SERVING & ANALYTICS                                                    │
│ • Amazon Athena serverless SQL via AWS Glue Data Catalog               │
│ • Operational BI KPIs and business SLA analytics                       │
└────────────────────────────────────────────────────────────────────────┘
```

---

## Production Engineering Highlights

* **DRY Modular Architecture:** Centralized business logic, schema sanitization, and filtering constraints into a single shared `rules.py` module. Both local PySpark runs and AWS Glue jobs import identical transformation rules, eliminating pipeline drift.
* **Deterministic Deduplication:** Replaced non-deterministic `dropDuplicates()` with PySpark `Window.partitionBy(pk).orderBy(...)` row ranking. Ties across duplicate primary keys are broken deterministically by ordering across attribute columns.
* **Elimination of Silent Data Loss:** Standardized three-valued boolean logic in PySpark filters. Null values are explicitly caught and segregated (`isNull() | (val <= 0)`), ensuring corrupted records are reliably tracked in quarantine audit partitions rather than vanishing during writes.
* **Automated CI/CD Test Gating:** Configured GitHub Actions to initialize Java 11 runtime dependencies and run automated Pytest suites against transformation logic. AWS S3 deployments for Glue scripts and Airflow DAGs are strictly blocked if any transformation assertion fails.
* **Idempotent Silver/Gold Writes:** Configured table writes with Apache Iceberg format version 2 and transactional table replacements, ensuring failed task restarts do not leave partial or corrupted states.

---

## Tech Stack

| Domain | Technology | Implementation Details |
| :--- | :--- | :--- |
| **Compute & Processing** | PySpark, AWS Glue (Serverless) | Distributed data transformations, schema validation, and aggregations |
| **Storage & Lakehouse** | Amazon S3, Apache Iceberg | Scalable object storage with ACID transactional table formats |
| **Catalog & Serving** | AWS Glue Data Catalog, Amazon Athena | Centralized metadata schema repository and serverless Presto/Trino SQL engine |
| **Orchestration** | Apache Airflow, Docker | Code-first dependency management, scheduling, monitoring, and state alerts |
| **DevOps & CI/CD** | GitHub Actions, Git | Automated testing and deployment of pipeline code and DAG scripts to cloud targets |
| **Testing** | Pytest, Local PySpark Fixture | Automated test suite validating deduplication rules, schema constraints, and edge cases |
| **Security & Identity** | AWS IAM | Principle of least privilege authentication for GitHub deployers and Airflow workers |
| **Language & Tooling** | Python 3.10+, SQL, Boto3 | Core script logic, data manipulation, and cloud SDK interaction |

---

## Data Quality & Defensive Engineering

To mirror production real-world data issues, incoming records contain intentional edge cases handled at the transformation boundary:

| Entity | Edge Case | Mitigation & Routing Strategy |
| :--- | :--- | :--- |
| **Loads** | Duplicate `load_id` | Window function deduplication retaining deterministic survivor row |
| **Loads** | Null or negative `weight_lbs` / `revenue` | Quarantined to `s3://.../quarantine/loads/invalid_business_values/` |
| **Trips** | Missing parent `load_id` / `driver_id` | Quarantined to `s3://.../quarantine/trips/invalid_fk/` via left anti-join |
| **Trips** | `idle_time_hours > actual_duration_hours` | Quarantined to `s3://.../quarantine/trips/invalid_duration_metrics/` |
| **Delivery Events** | `actual_datetime < scheduled_datetime` | Temporal boundary check routed to `invalid_event_timestamps/` |
| **All Dimensions** | Empty string primary keys (`""`) | Sanitized to native `NULL` and routed to `missing_primary_key/` |

---

## CI/CD Pipeline Workflow

The repository uses GitHub Actions (`.github/workflows/ci_cd_pipeline.yml`) to enforce deployment quality:

```
[ Git Push / PR ] 
       │
       ▼
[ Job 1: Run Unit Tests ]
  ├── Checkout Code
  ├── Set up Java 11 (Temurin) & Python 3.10
  ├── Install requirements.txt
  └── Run Pytest Suite (tests/test_silver_rules.py)
       │
       ├─► (Fail) ──► Stop Pipeline & Block Deployment
       ▼
   (Pass)
       │
       ▼
[ Job 2: Deploy to AWS S3 ]
  ├── Authenticate AWS Credentials via IAM
  ├── Sync src/glue_jobs/ to S3 Glue Script Bucket
  └── Sync dags/ to S3 Airflow DAG Directory
```

---

## Repository Structure

```
swift-route-logistics-data-engineering/
├── .github/
│   └── workflows/
│       └── ci_cd_pipeline.yml        # Gated CI/CD workflow (Pytest -> AWS S3 sync)
├── dags/
│   └── swiftroute_pipeline.py        # Master Airflow orchestration DAG (GlueJobOperator)
├── src/
│   ├── rules.py                      # Centralized PySpark transformation & quality rules
│   ├── silver_transformation.py      # Local Silver processing pipeline
│   ├── gold_iceberg_marts.py         # PySpark aggregation into Apache Iceberg analytical tables
│   └── glue_jobs/
│       ├── silver_etl.py             # Production AWS Glue job for Silver layer
│       └── gold_etl.py               # Production AWS Glue job for Gold layer
├── tests/
│   ├── conftest.py                   # Session-scoped local PySpark test fixture
│   └── test_silver_rules.py          # Unit tests for data cleaning & quarantine logic
├── docker-compose.yaml               # Local Airflow deployment configuration
├── requirements.txt                  # Python dependencies
└── README.md
```

---

## Analytical Serving Layer (Sample Business Queries)

Once curated into Gold Apache Iceberg tables, operational metrics are queryable via Amazon Athena:

### 1. Route SLA & On-Time Performance Analysis

```sql
SELECT 
    route_id,
    COUNT(trip_id) AS total_trips,
    ROUND(AVG(CASE WHEN is_delayed = 0 THEN 1.0 ELSE 0.0 END) * 100, 2) AS on_time_percentage,
    ROUND(AVG(delay_duration_minutes), 1) AS avg_delay_minutes
FROM 
    "swiftroute_iceberg"."gold_route_performance"
GROUP BY 
    route_id
HAVING 
    COUNT(trip_id) >= 25
ORDER BY 
    on_time_percentage ASC;
```

### 2. Facility Congestion & Detention Bottlenecks

```sql
SELECT 
    facility_id,
    COUNT(event_id) AS total_events,
    ROUND(AVG(detention_minutes), 2) AS avg_detention_minutes,
    MAX(detention_minutes) AS peak_detention_minutes
FROM 
    "swiftroute_iceberg"."gold_facility_performance"
GROUP BY 
    facility_id
ORDER BY 
    avg_detention_minutes DESC
LIMIT 10;
```

---

## Local Setup & Testing

### 1. Clone & Configure Environment

```bash
git clone [https://github.com/SharanyaV25-dev/swift-route-logistics-data-engineering.git](https://github.com/SharanyaV25-dev/swift-route-logistics-data-engineering.git)
cd swift-route-logistics-data-engineering

python -m venv venv
# On Windows:
.\venv\Scripts\activate
# On Linux/macOS:
source venv/bin/activate

pip install -r requirements.txt
```

### 2. Run Automated PySpark Tests

```bash
python -m pytest tests/ -v
```

### 3. Run Silver Pipeline Locally

```bash
python src/silver_transformation.py
```