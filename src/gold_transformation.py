from pathlib import Path
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, concat_ws, count, sum as spark_sum, avg, round, when, lit, current_timestamp

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SILVER_DIR = PROJECT_ROOT / "data" / "silver"
GOLD_DIR = PROJECT_ROOT / "data" / "gold"

spark = (
    SparkSession.builder
    .appName("SwiftRoute-Gold-Layer")
    .getOrCreate()
)


def read_silver(table_name: str, primary_key: str = None) -> DataFrame:
    """
    Read Silver Parquet dataset and enforce idempotent grain deduplication
    so duplicate upstream batch deliveries never multiply Gold metrics.
    """
    path = SILVER_DIR / table_name

    if not path.exists():
        raise FileNotFoundError(f"Silver dataset not found: {path}")

    df = spark.read.parquet(str(path))

    if primary_key and primary_key in df.columns:
        df = df.dropDuplicates([primary_key])

    return df


def write_gold(df: DataFrame, table_name: str, primary_key: str = None):
    """
    Idempotently write Gold dataset with primary-key grain enforcement
    and ingestion timestamp tracking.
    """
    output_path = GOLD_DIR / table_name

    if primary_key and primary_key in df.columns:
        df = df.dropDuplicates([primary_key])

    df_with_meta = df.withColumn("last_ingested_at", current_timestamp())

    (
        df_with_meta.write
        .mode("overwrite")
        .parquet(str(output_path))
    )

    print(f"PASS: {table_name:<30} {df_with_meta.count():>8,} rows")


GOLD_DIR.mkdir(parents=True, exist_ok=True)

# ============================================================
# READ SILVER TABLES (WITH IDEMPOTENT GRAIN DEDUPLICATION)
# ============================================================

customers = read_silver("customers", primary_key="customer_id")
facilities = read_silver("facilities", primary_key="facility_id")
routes = read_silver("routes", primary_key="route_id")
drivers = read_silver("drivers", primary_key="driver_id")
trucks = read_silver("trucks", primary_key="truck_id")
loads = read_silver("loads", primary_key="load_id")
trips = read_silver("trips", primary_key="trip_id")
delivery_events = read_silver("delivery_events", primary_key="event_id")

print("=" * 70)
print("CREATING GOLD LAYER (IDEMPOTENT EXECUTION)")
print("=" * 70)


# ============================================================
# 1. GOLD LOAD PERFORMANCE
# ============================================================

load_performance = (
    loads
    .join(
        customers.select(
            "customer_id",
            "customer_name",
            "customer_type",
            "account_status"
        ),
        on="customer_id",
        how="left"
    )
    .join(
        routes.select(
            "route_id",
            "origin_city",
            "origin_state",
            "destination_city",
            "destination_state"
        ),
        on="route_id",
        how="left"
    )
    .select(
        "load_id",
        "customer_id",
        "customer_name",
        "customer_type",
        "account_status",
        "route_id",
        "origin_city",
        "origin_state",
        "destination_city",
        "destination_state",
        "load_date",
        "load_type",
        "weight_lbs",
        "pieces",
        "revenue",
        "load_status",
        "booking_type"
    )
)

write_gold(load_performance, "gold_load_performance", primary_key="load_id")

# ============================================================
# 2. GOLD DELIVERY PERFORMANCE
# ============================================================

delivery_performance = (
    delivery_events
    .join(
        loads.select(
            "load_id",
            "customer_id",
            "revenue"
        ),
        on="load_id",
        how="left"
    )
    .join(
        trips.select(
            "trip_id",
            "driver_id",
            "truck_id",
            "trip_status"
        ),
        on="trip_id",
        how="left"
    )
    .join(
        facilities.select(
            col("facility_id"),
            "facility_name",
            "facility_type"
        ),
        on="facility_id",
        how="left"
    )
    .select(
        "event_id",
        "load_id",
        "trip_id",
        "customer_id",
        "driver_id",
        "truck_id",
        "event_type",
        "facility_id",
        "facility_name",
        "facility_type",
        "location_city",
        "location_state",
        "scheduled_datetime",
        "actual_datetime",
        "detention_minutes",
        "on_time_flag",
        "revenue",
        "trip_status"
    )
)

write_gold(delivery_performance, "gold_delivery_performance", primary_key="event_id")

# ============================================================
# 3. GOLD DRIVER PERFORMANCE
# ============================================================

driver_performance = (
    trips
    .groupBy("driver_id")
    .agg(
        count("*").alias("total_trips"),
        spark_sum("actual_distance_miles").alias("total_distance_miles"),
        avg("actual_distance_miles").alias("avg_distance_miles"),
        avg("actual_duration_hours").alias("avg_duration_hours"),
        spark_sum("fuel_gallons_used").alias("total_fuel_gallons"),
        avg("fuel_gallons_used").alias("avg_fuel_gallons"),
        spark_sum("idle_time_hours").alias("total_idle_time_hours"),
        avg("idle_time_hours").alias("avg_idle_time_hours")
    )
    .join(
        drivers.select(
            "driver_id",
            concat_ws(" ", col("first_name"), col("last_name")).alias("driver_name"),
            "employment_status",
            "years_experience"
        ),
        on="driver_id",
        how="left"
    )
    .withColumn(
        "fuel_efficiency_miles_per_gallon",
        round(
            col("total_distance_miles") /
            col("total_fuel_gallons"),
            2
        )
    )
    .select(
        "driver_id",
        "driver_name",
        "employment_status",
        "years_experience",
        "total_trips",
        "total_distance_miles",
        "avg_distance_miles",
        "avg_duration_hours",
        "total_fuel_gallons",
        "avg_fuel_gallons",
        "fuel_efficiency_miles_per_gallon",
        "total_idle_time_hours",
        "avg_idle_time_hours"
    )
)

write_gold(driver_performance, "gold_driver_performance", primary_key="driver_id")

# ============================================================
# 4. GOLD TRUCK / FUEL PERFORMANCE
# ============================================================

truck_performance = (
    trips
    .groupBy("truck_id")
    .agg(
        count("*").alias("total_trips"),
        spark_sum("actual_distance_miles").alias("total_distance_miles"),
        avg("actual_distance_miles").alias("avg_distance_miles"),
        spark_sum("fuel_gallons_used").alias("total_fuel_gallons"),
        avg("fuel_gallons_used").alias("avg_fuel_per_trip_gallons"),
        spark_sum("idle_time_hours").alias("total_idle_time_hours"),
        avg("idle_time_hours").alias("avg_idle_time_hours")
    )
    .join(
        trucks.select(
            "truck_id",
            "unit_number",
            "make",
            "model_year",
            "fuel_type",
            "tank_capacity_gallons",
            "status"
        ),
        on="truck_id",
        how="left"
    )
    .withColumn(
        "fuel_efficiency_miles_per_gallon",
        when(
            col("total_fuel_gallons") > 0,
            round(
                col("total_distance_miles")
                / col("total_fuel_gallons"),
                2
            )
        ).otherwise(lit(0))
    )
    .select(
        "truck_id",
        "unit_number",
        "make",
        "model_year",
        "fuel_type",
        "tank_capacity_gallons",
        "status",
        "total_trips",
        "total_distance_miles",
        "avg_distance_miles",
        "total_fuel_gallons",
        "avg_fuel_per_trip_gallons",
        "fuel_efficiency_miles_per_gallon",
        "total_idle_time_hours",
        "avg_idle_time_hours"
    )
)

write_gold(truck_performance, "gold_truck_performance", primary_key="truck_id")

# ============================================================
# 5. GOLD FACILITY PERFORMANCE
# ============================================================

facility_performance = (
    delivery_events
    .groupBy("facility_id")
    .agg(
        count("*").alias("total_events"),
        spark_sum(
            when(col("on_time_flag") == True, 1).otherwise(0)
        ).alias("on_time_events"),
        avg("detention_minutes").alias("avg_detention_minutes")
    )
    .join(
        facilities.select(
            "facility_id",
            "facility_name",
            "city",
            "state"
        ),
        on="facility_id",
        how="left"
    )
    .withColumn(
        "on_time_rate",
        round(
            col("on_time_events") / col("total_events") * 100,
            2
        )
    )
    .select(
        "facility_id",
        "facility_name",
        "city",
        "state",
        "total_events",
        "on_time_events",
        "on_time_rate",
        "avg_detention_minutes"
    )
)

write_gold(facility_performance, "gold_facility_performance", primary_key="facility_id")

# ============================================================
# 6. GOLD ROUTE PERFORMANCE
# ============================================================

route_performance = (
    trips
    .join(
        loads.select("load_id", "route_id"),
        on="load_id",
        how="left"
    )
    .groupBy("route_id")
    .agg(
        count("*").alias("total_trips"),
        avg("actual_duration_hours").alias(
            "avg_trip_duration_hours"
        ),
        avg("actual_distance_miles").alias(
            "avg_actual_distance_miles"
        ),
        avg("fuel_gallons_used").alias(
            "avg_fuel_consumed_gallons"
        )
    )
    .join(
        routes.select(
            "route_id",
            "origin_city",
            "origin_state",
            "destination_city",
            "destination_state",
            "typical_distance_miles",
            "typical_transit_days"
        ),
        on="route_id",
        how="left"
    )
    .withColumn(
        "fuel_efficiency_miles_per_gallon",
        round(
            col("avg_actual_distance_miles") /
            col("avg_fuel_consumed_gallons"),
            2
        )
    )
    .select(
        "route_id",
        "origin_city",
        "origin_state",
        "destination_city",
        "destination_state",
        "typical_distance_miles",
        "typical_transit_days",
        "total_trips",
        "avg_trip_duration_hours",
        "avg_actual_distance_miles",
        "avg_fuel_consumed_gallons",
        "fuel_efficiency_miles_per_gallon"
    )
)

write_gold(route_performance, "gold_route_performance", primary_key="route_id")

# ============================================================
# 7. GOLD CUSTOMER PERFORMANCE
# ============================================================

customer_performance = (
    loads
    .groupBy("customer_id")
    .agg(
        count("*").alias("total_loads"),
        spark_sum("revenue").alias("total_revenue"),
        avg("weight_lbs").alias("avg_load_weight_lbs"),
        spark_sum("pieces").alias("total_pieces")
    )
    .join(
        customers.select(
            "customer_id",
            "customer_name",
            "customer_type",
            "account_status"
        ),
        on="customer_id",
        how="left"
    )
    .withColumn(
        "avg_revenue_per_load",
        round(
            col("total_revenue") / col("total_loads"),
            2
        )
    )
    .select(
        "customer_id",
        "customer_name",
        "customer_type",
        "account_status",
        "total_loads",
        "total_revenue",
        "avg_revenue_per_load",
        "avg_load_weight_lbs",
        "total_pieces"
    )
)

write_gold(customer_performance, "gold_customer_performance", primary_key="customer_id")

# ============================================================
# 8. GOLD DAILY OPERATIONS SUMMARY
# ============================================================

daily_operations = (
    delivery_events
    .groupBy(
        col("actual_datetime").cast("date").alias("event_date")
    )
    .agg(
        count("*").alias("total_events"),
        spark_sum(
            when(col("on_time_flag") == True, 1).otherwise(0)
        ).alias("on_time_events"),
        avg("detention_minutes").alias("avg_detention_minutes")
    )
    .withColumn(
        "on_time_rate",
        round(
            col("on_time_events") / col("total_events") * 100,
            2
        )
    )
    .select(
        "event_date",
        "total_events",
        "on_time_events",
        "on_time_rate",
        "avg_detention_minutes"
    )
    .orderBy("event_date")
)

write_gold(daily_operations, "gold_daily_operations", primary_key="event_date")

# ============================================================
# FINAL VALIDATION & GRAIN / CONTROL TOTAL RECONCILIATION
# ============================================================

gold_table_keys = {
    "gold_load_performance": "load_id",
    "gold_delivery_performance": "event_id",
    "gold_driver_performance": "driver_id",
    "gold_truck_performance": "truck_id",
    "gold_facility_performance": "facility_id",
    "gold_route_performance": "route_id",
    "gold_customer_performance": "customer_id",
    "gold_daily_operations": "event_date",
}

print()
print("=" * 70)
print("GOLD VALIDATION & IDEMPOTENCY GRAIN CHECK")
print("=" * 70)

for table_name, pk in gold_table_keys.items():
    path = GOLD_DIR / table_name
    df = spark.read.parquet(str(path))

    row_count = df.count()
    distinct_pk_count = df.select(pk).distinct().count()
    column_count = len(df.columns)

    if row_count == 0:
        raise ValueError(f"FAIL: {table_name} contains zero rows")

    if row_count != distinct_pk_count:
        raise ValueError(
            f"FAIL: Duplicate primary keys detected in {table_name}! "
            f"rows={row_count}, distinct({pk})={distinct_pk_count}"
        )

    print(
        f"PASS: {table_name:<30} "
        f"rows={row_count:>8,} "
        f"distinct_pk={distinct_pk_count:>8,} "
        f"columns={column_count}"
    )

# Control Total Reconciliation: Silver Loads Revenue vs Gold Mart Revenue
silver_total_rev = loads.agg(round(spark_sum("revenue"), 2)).collect()[0][0]
gold_load_rev = (
    spark.read.parquet(str(GOLD_DIR / "gold_load_performance"))
    .agg(round(spark_sum("revenue"), 2))
    .collect()[0][0]
)
gold_cust_rev = (
    spark.read.parquet(str(GOLD_DIR / "gold_customer_performance"))
    .agg(round(spark_sum("total_revenue"), 2))
    .collect()[0][0]
)

if abs(silver_total_rev - gold_load_rev) > 0.01 or abs(silver_total_rev - gold_cust_rev) > 0.01:
    raise ValueError(
        f"RECONCILIATION FAIL: Silver Revenue ({silver_total_rev}) != "
        f"Gold Load Revenue ({gold_load_rev}) / Customer Revenue ({gold_cust_rev})"
    )

print("-" * 70)
print(f"RECONCILIATION PASS: Control Total Revenue = ${silver_total_rev:,.2f}")
print("=" * 70)
print("GOLD TRANSFORMATION PASSED")
print("=" * 70)

spark.stop()