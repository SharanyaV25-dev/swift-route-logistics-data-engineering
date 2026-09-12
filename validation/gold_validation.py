from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    countDistinct,
    sum as spark_sum,
    when,
    lit,
)


# ============================================================
# PATH CONFIGURATION
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parent.parent

GOLD_DIR = PROJECT_ROOT / "data" / "gold"
REPORT_DIR = PROJECT_ROOT / "docs"
REPORT_PATH = REPORT_DIR / "gold_quality_report.csv"


# ============================================================
# SPARK SESSION
# ============================================================

spark = (
    SparkSession.builder
    .appName("SwiftRoute-Gold-Validation")
    .getOrCreate()
)


# ============================================================
# GOLD TABLES AND PRIMARY KEYS
# ============================================================

gold_tables = {
    "gold_load_performance": "load_id",
    "gold_delivery_performance": "event_id",
    "gold_driver_performance": "driver_id",
    "gold_truck_performance": "truck_id",
    "gold_facility_performance": "facility_id",
    "gold_route_performance": "route_id",
    "gold_customer_performance": "customer_id",
    "gold_daily_operations": "event_date",
}


# ============================================================
# VALIDATION
# ============================================================

REPORT_DIR.mkdir(parents=True, exist_ok=True)

validation_results = []

print("=" * 70)
print("GOLD DATA QUALITY VALIDATION")
print("=" * 70)


for table_name, primary_key in gold_tables.items():

    table_path = GOLD_DIR / table_name

    if not table_path.exists():
        raise FileNotFoundError(
            f"Gold table not found: {table_path}"
        )

    df = spark.read.parquet(str(table_path))

    row_count = df.count()
    column_count = len(df.columns)

    # --------------------------------------------------------
    # Primary-key validation
    # --------------------------------------------------------

    null_key_count = (
        df.filter(col(primary_key).isNull())
        .count()
    )

    duplicate_key_count = (
        df.groupBy(primary_key)
        .count()
        .filter(col("count") > 1)
        .count()
    )

    # --------------------------------------------------------
    # Numeric quality validation
    # --------------------------------------------------------

    negative_numeric_count = 0

    numeric_columns = [
        field.name
        for field in df.schema.fields
        if field.dataType.typeName()
        in ["integer", "long", "double", "float", "decimal"]
    ]

    for numeric_column in numeric_columns:

        negative_numeric_count += (
            df.filter(col(numeric_column) < 0)
            .count()
        )

    # --------------------------------------------------------
    # Overall status
    # --------------------------------------------------------

    status = "PASS"

    if row_count == 0:
        status = "FAIL"

    if null_key_count > 0:
        status = "FAIL"

    if duplicate_key_count > 0:
        status = "FAIL"

    if negative_numeric_count > 0:
        status = "WARNING"

    validation_results.append({
        "table_name": table_name,
        "row_count": row_count,
        "column_count": column_count,
        "primary_key": primary_key,
        "null_primary_keys": null_key_count,
        "duplicate_primary_keys": duplicate_key_count,
        "negative_numeric_values": negative_numeric_count,
        "validation_status": status,
    })

    print(
        f"{status}: {table_name:<30} "
        f"rows={row_count:>8,} "
        f"columns={column_count:<3} "
        f"null_keys={null_key_count:<4} "
        f"duplicate_keys={duplicate_key_count:<4}"
    )


# ============================================================
# WRITE QUALITY REPORT
# ============================================================

report_df = spark.createDataFrame(validation_results)

(
    report_df
    .orderBy("table_name")
    .coalesce(1)
    .write
    .mode("overwrite")
    .option("header", True)
    .csv(str(REPORT_PATH))
)


# ============================================================
# FINAL STATUS
# ============================================================

failed_tables = [
    result
    for result in validation_results
    if result["validation_status"] == "FAIL"
]

print()
print(f"Quality report written to: {REPORT_PATH}")

if failed_tables:
    raise ValueError(
        f"Gold validation failed for "
        f"{len(failed_tables)} table(s)"
    )

print("=" * 70)
print("GOLD VALIDATION PASSED")
print("=" * 70)

spark.stop()