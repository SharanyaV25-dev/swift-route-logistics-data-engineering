# SwiftRoute Logistics Data Engineering

An end-to-end logistics data engineering portfolio project demonstrating a modern AWS data lake pipeline with Python, PySpark, SQL, data quality engineering and business-ready analytics.

## Objective

SwiftRoute simulates a logistics company's operational data platform. Messy source data is preserved in Bronze, cleaned and validated in Silver, and transformed into business-ready Gold datasets for delivery, fleet, route, facility and customer analytics.

## Architecture

```text
Synthetic Source Data
        |
        v
RAW / BRONZE  ---> preserve source as received
        |
        v
SILVER        ---> PySpark cleaning + validation + Parquet
        |
        v
GOLD          ---> business-ready analytics
        |
        v
Athena / SQL Analytics
```

Planned AWS implementation:

```text
CSV -> Amazon S3 Bronze -> AWS Glue/PySpark -> S3 Silver -> S3 Gold
                                      |
                                      v
                              Glue Data Catalog
                                      |
                                      v
                                   Athena
```

## Source Tables

| Table | Target Records |
|---|---:|
| customers | 500 |
| facilities | 50 |
| routes | 100 |
| drivers | 300 |
| trucks | 200 |
| loads | 50,000 |
| trips | 50,000 |
| delivery_events | 250,000 |

The clean synthetic baseline was validated before intentional data-quality defects were injected.

## Purposeful Data Quality Problems

| Table | Issue | Count |
|---|---|---:|
| loads | Duplicate loads | 500 |
| loads | Missing customer IDs | 250 |
| loads | Negative/zero weights | 200 |
| loads | Invalid revenue | 100 |
| trips | Duplicate trips | 300 |
| trips | Missing driver IDs | 100 |
| delivery_events | Invalid facility IDs | 150 |
| delivery_events | Impossible timestamps | 200 |
| delivery_events | Duplicate delivery events | 1,000 |
| delivery_events | Orphan events | 300 |

These defects are intentional. Bronze preserves them; Silver is responsible for detecting, cleaning and quarantining them.

## Medallion Layers

### Bronze

Preserves source data as received and records ingestion metadata. The local implementation copies messy CSV files into the Bronze layer. The cloud implementation will use Amazon S3.

### Silver

Uses PySpark to:

- standardize IDs and data types
- remove duplicate primary-key records
- reject missing primary keys
- validate numeric business rules
- validate timestamps
- validate foreign-key relationships
- quarantine rejected records
- write clean Parquet datasets

### Gold

Will provide business-ready marts for:

- on-time delivery
- delivery delays
- driver performance
- truck/fuel efficiency
- route performance
- facility performance
- revenue/profitability
- customer service reliability

## Project Structure

```text
SwiftRoute-Logistics-Data-Engineering/
├── architecture/
├── data/
│   ├── raw/messy/
│   ├── bronze/
│   └── silver/
├── docs/
│   ├── metadata/
│   └── silver_quality_report.csv
├── src/
│   ├── Synthetic_Data_Generator_Final.py
│   ├── Inject_Purposeful_Messiness.py
│   ├── bronze_ingestion.py
│   └── silver_transformation.py
├── validation/
├── .gitignore
├── README.md
└── requirements.txt
```

Generated bulk CSV/Parquet data should not be committed to GitHub. The repository stores reproducible code and documentation; cloud storage will hold the data.

## Current Progress

- [x] Synthetic data generation
- [x] Clean baseline validation
- [x] Purposeful messiness injection
- [x] Local Bronze ingestion
- [x] Bronze Git commit
- [ ] Local Silver transformation
- [ ] Silver validation
- [ ] Gold business marts
- [ ] AWS S3 deployment
- [ ] AWS Glue processing
- [ ] Glue Data Catalog
- [ ] Athena analytics
- [ ] Orchestration
- [ ] Monitoring/logging
- [ ] IAM/security
- [ ] Final architecture documentation

## Technology Stack

Python · Pandas · PySpark · SDV · Amazon S3 · AWS Glue · Glue Data Catalog · Amazon Athena · SQL · Git · GitHub

## Business Questions

1. What percentage of deliveries are on time?
2. Which routes have the highest delay rates?
3. Which facilities create the most detention time?
4. Which drivers have the best delivery performance?
5. Which trucks have poor fuel efficiency?
6. What is revenue by route, customer and period?
7. Which customers experience the most delivery issues?
8. Where are operational bottlenecks occurring?

## Local Run

From the project root:

```bash
python src/bronze_ingestion.py
python src/silver_transformation.py
```

Silver Parquet output:

```text
data/silver/
```

Rejected records:

```text
data/silver/quarantine/
```

Quality summary:

```text
docs/silver_quality_report.csv
```

## Cloud Deployment Plan

Once AWS account activation is complete, local Bronze ingestion will be extended to real Amazon S3. The final flow will be:

```text
Local messy CSV
      |
      v
S3 Bronze
      |
      v
AWS Glue / PySpark
      |
      v
S3 Silver
      |
      v
S3 Gold
      |
      v
Glue Catalog -> Athena
```

AWS services will be used selectively and cost-consciously.

## Why Synthetic Data?

Synthetic data allows controlled demonstration of data-quality engineering without exposing sensitive operational information. The intentional defects make the pipeline demonstrate realistic handling of duplicates, missing keys, invalid values, broken relationships, impossible timestamps and orphan records.

## Author

Hands-on Data Engineering portfolio project focused on AWS, PySpark, SQL and production-style data quality practices.
