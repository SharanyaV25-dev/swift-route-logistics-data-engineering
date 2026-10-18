import sys
from datetime import datetime, timezone
from pyspark.context import SparkContext
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, BooleanType
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

# AWS Glue injects extra-py-files directly into the runtime path
from rules import (
    clean_id, deduplicate_deterministic, get_orphans, keep_valid_fk, 
    get_invalid_loads, get_valid_loads, get_invalid_trips, 
    get_valid_trips, get_invalid_events, get_valid_events
)

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
spark = SparkSession.builder \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.glue_catalog.warehouse", "s3://swiftroute-logistics-de-bucket/silver/") \
    .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
    .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .getOrCreate()

sc = spark.sparkContext
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

BRONZE_BUCKET = "s3://swiftroute-logistics-de-bucket/bronze"
QUARANTINE_BUCKET = "s3://swiftroute-logistics-de-bucket/silver/quarantine"
DATABASE = "swiftroute_iceberg"


def read_bronze(table):
    df = spark.read.option("header", True).csv(f"{BRONZE_BUCKET}/{table}/*.csv")
    if df.limit(1).count() == 0:
        print(f"[WARN] Empty Bronze input detected for {table}: valid header with 0 rows.")
    return df


def deduplicate_latest_state(df, pk_col, candidate_ts_cols=None):
    """
    Deduplicates records by primary key using a deterministic Window function
    (ROW_NUMBER() OVER (PARTITION BY pk_col ORDER BY updated_at DESC, ...)).
    Ensures late-arriving status corrections and updates always supersede prior states.
    """
    order_cols = []
    if candidate_ts_cols:
        for c in candidate_ts_cols:
            if c in df.columns:
                order_cols.append(F.col(c).desc_nulls_last())

    for c in sorted(df.columns):
        if c != pk_col and (not candidate_ts_cols or c not in candidate_ts_cols):
            order_cols.append(F.col(c).desc_nulls_last())

    window_spec = Window.partitionBy(pk_col).orderBy(*order_cols)
    return (
        df.withColumn("_rn", F.row_number().over(window_spec))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def quarantine(df, table, reason):
    if df.limit(1).count() > 0:
        out_path = f"{QUARANTINE_BUCKET}/{table}/{reason}"
        df.withColumn("quarantine_reason", F.lit(reason)) \
          .withColumn("quarantined_at_utc", F.lit(datetime.now(timezone.utc).isoformat())) \
          .write.mode("overwrite").parquet(out_path)


def write_iceberg(df, table_name):
    df.writeTo(f"glue_catalog.{DATABASE}.{table_name}") \
      .tableProperty("format-version", "2") \
      .createOrReplace()
    
# ============================================================
# DIMENSIONS
# ============================================================

# 1. Customers
customers = read_bronze("customers")
customers = customers.withColumn("annual_revenue_potential", F.col("annual_revenue_potential").cast(DoubleType())) \
                     .withColumn("customer_id", clean_id("customer_id")) \
                     .withColumn("contract_start_date", F.to_date("contract_start_date"))
quarantine(customers.filter(F.col("customer_id").isNull()), "customers", "missing_primary_key")
customers = deduplicate_latest_state(
    customers.filter(F.col("customer_id").isNotNull()),
    "customer_id",
    candidate_ts_cols=["updated_at", "contract_start_date"]
)
write_iceberg(customers, "customers")

# 2. Facilities
facilities = read_bronze("facilities")
facilities = facilities.withColumn("latitude", F.col("latitude").cast(DoubleType())) \
                       .withColumn("longitude", F.col("longitude").cast(DoubleType())) \
                       .withColumn("facility_id", clean_id("facility_id"))
quarantine(facilities.filter(F.col("facility_id").isNull()), "facilities", "missing_primary_key")
facilities = deduplicate_latest_state(
    facilities.filter(F.col("facility_id").isNotNull()),
    "facility_id",
    candidate_ts_cols=["updated_at"]
)
write_iceberg(facilities, "facilities")

# 3. Routes
routes = read_bronze("routes")
routes = routes.withColumn("typical_distance_miles", F.col("typical_distance_miles").cast(DoubleType())) \
               .withColumn("base_rate_per_mile", F.col("base_rate_per_mile").cast(DoubleType())) \
               .withColumn("route_id", clean_id("route_id"))
quarantine(routes.filter(F.col("route_id").isNull()), "routes", "missing_primary_key")
routes = deduplicate_latest_state(
    routes.filter(F.col("route_id").isNotNull()),
    "route_id",
    candidate_ts_cols=["updated_at"]
)
write_iceberg(routes, "routes")

# 4. Drivers
drivers = read_bronze("drivers")
drivers = drivers.withColumn("years_experience", F.col("years_experience").cast(IntegerType())) \
                 .withColumn("driver_id", clean_id("driver_id"))
quarantine(drivers.filter(F.col("driver_id").isNull()), "drivers", "missing_primary_key")
drivers = deduplicate_latest_state(
    drivers.filter(F.col("driver_id").isNotNull()),
    "driver_id",
    candidate_ts_cols=["updated_at", "termination_date", "hire_date"]
)
write_iceberg(drivers, "drivers")

# 5. Trucks
trucks = read_bronze("trucks")
trucks = trucks.withColumn("model_year", F.col("model_year").cast(IntegerType())) \
               .withColumn("truck_id", clean_id("truck_id"))
quarantine(trucks.filter(F.col("truck_id").isNull()), "trucks", "missing_primary_key")
trucks = deduplicate_latest_state(
    trucks.filter(F.col("truck_id").isNotNull()),
    "truck_id",
    candidate_ts_cols=["updated_at", "acquisition_date"]
)
write_iceberg(trucks, "trucks")

# ============================================================
# FACTS (WITH WINDOW DEDUPLICATION & CANCELLATION POLICY)
# ============================================================

# 6. Loads
loads = read_bronze("loads")
loads = loads.withColumn("weight_lbs", F.col("weight_lbs").cast(DoubleType())).withColumn("revenue", F.col("revenue").cast(DoubleType())) \
             .withColumn("load_id", clean_id("load_id")).withColumn("customer_id", clean_id("customer_id")).withColumn("route_id", clean_id("route_id"))

if "updated_at" in loads.columns:
    loads = loads.withColumn("updated_at", F.to_timestamp("updated_at"))

quarantine(loads.filter(F.col("load_id").isNull()), "loads", "missing_primary_key")
loads = loads.filter(F.col("load_id").isNotNull())

quarantine(get_invalid_loads(loads), "loads", "invalid_business_values")
loads = get_valid_loads(loads)

loads = deduplicate_latest_state(loads, "load_id", candidate_ts_cols=["updated_at", "load_date"])

if "load_status" in loads.columns and "revenue" in loads.columns:
    loads = loads.withColumn(
        "revenue",
        F.when(F.upper(F.col("load_status")) == "CANCELLED", F.lit(0.0)).otherwise(F.col("revenue"))
    )

for fk_col, parent_df, reason in [("customer_id", customers, "invalid_customer_fk"), ("route_id", routes, "invalid_route_fk")]:
    quarantine(get_orphans(loads, fk_col, parent_df, fk_col), "loads", reason)
    loads = keep_valid_fk(loads, fk_col, parent_df, fk_col)
write_iceberg(loads, "loads")

# 7. Trips
trips = read_bronze("trips")
trips = trips.withColumn("actual_duration_hours", F.col("actual_duration_hours").cast(DoubleType())).withColumn("idle_time_hours", F.col("idle_time_hours").cast(DoubleType())) \
             .withColumn("trip_id", clean_id("trip_id")).withColumn("load_id", clean_id("load_id")).withColumn("driver_id", clean_id("driver_id")).withColumn("truck_id", clean_id("truck_id"))

if "updated_at" in trips.columns:
    trips = trips.withColumn("updated_at", F.to_timestamp("updated_at"))

quarantine(trips.filter(F.col("trip_id").isNull()), "trips", "missing_primary_key")
trips = trips.filter(F.col("trip_id").isNotNull())

quarantine(get_invalid_trips(trips), "trips", "invalid_duration_metrics")
trips = get_valid_trips(trips)

trips = deduplicate_latest_state(trips, "trip_id", candidate_ts_cols=["updated_at", "dispatch_date"])

for fk_col, parent_df, reason in [("load_id", loads, "invalid_load_fk"), ("driver_id", drivers, "invalid_driver_fk"), ("truck_id", trucks, "invalid_truck_fk")]:
    quarantine(get_orphans(trips, fk_col, parent_df, fk_col), "trips", reason)
    trips = keep_valid_fk(trips, fk_col, parent_df, fk_col)
write_iceberg(trips, "trips")

# 8. Delivery Events
events = read_bronze("delivery_events")
events = events.withColumn("scheduled_datetime", F.to_timestamp("scheduled_datetime")).withColumn("actual_datetime", F.to_timestamp("actual_datetime")) \
               .withColumn("event_id", clean_id("event_id")).withColumn("load_id", clean_id("load_id")).withColumn("trip_id", clean_id("trip_id")).withColumn("facility_id", clean_id("facility_id"))

if "updated_at" in events.columns:
    events = events.withColumn("updated_at", F.to_timestamp("updated_at"))

quarantine(events.filter(F.col("event_id").isNull()), "delivery_events", "missing_primary_key")
events = events.filter(F.col("event_id").isNotNull())

quarantine(get_invalid_events(events), "delivery_events", "invalid_event_timestamps")
events = get_valid_events(events)

events = deduplicate_latest_state(
    events,
    "event_id",
    candidate_ts_cols=["updated_at", "actual_datetime", "scheduled_datetime"]
)

for fk_col, parent_df, reason in [("load_id", loads, "orphan_load_fk"), ("trip_id", trips, "orphan_trip_fk"), ("facility_id", facilities, "invalid_facility_fk")]:
    quarantine(get_orphans(events, fk_col, parent_df, fk_col), "delivery_events", reason)
    events = keep_valid_fk(events, fk_col, parent_df, fk_col)
write_iceberg(events, "delivery_events")

job.commit()