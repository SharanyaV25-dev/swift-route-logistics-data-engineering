import sys
from datetime import datetime, timezone
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, BooleanType
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
# trigger CI/CD
# 1. Initialize Spark with AWS Glue Iceberg Extensions
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

# ============================================================
# HELPER FUNCTIONS
# ============================================================

def read_bronze(table):
    return spark.read.option("header", True).csv(f"{BRONZE_BUCKET}/{table}/*.csv")

def quarantine(df, table, reason):
    if df.limit(1).count() > 0:
        out_path = f"{QUARANTINE_BUCKET}/{table}/{reason}"
        df.withColumn("quarantine_reason", F.lit(reason)) \
          .withColumn("quarantined_at_utc", F.lit(datetime.now(timezone.utc).isoformat())) \
          .write.mode("append").parquet(out_path)

def write_iceberg(df, table_name):
    df.writeTo(f"glue_catalog.{DATABASE}.{table_name}") \
      .tableProperty("format-version", "2") \
      .createOrReplace()

def clean_id(c): 
    return F.when(F.trim(F.col(c)) == '', None).otherwise(F.trim(F.col(c)))

def keep_valid_fk(child_df, child_col, parent_df, parent_col, table_name, reason):
    valid_keys = parent_df.select(F.col(parent_col).alias('_valid_fk')).distinct()
    
    # Quarantine orphans
    orphans = child_df.join(valid_keys, child_df[child_col] == F.col('_valid_fk'), "left") \
                      .filter(F.col('_valid_fk').isNull()).drop('_valid_fk')
    quarantine(orphans, table_name, reason)
    
    # Return valid records
    return child_df.join(valid_keys, child_df[child_col] == F.col('_valid_fk'), "inner").drop('_valid_fk')

# ============================================================
# DIMENSIONS
# ============================================================

# 1. Customers
customers = read_bronze("customers")
customers = customers.withColumn("annual_revenue_potential", F.col("annual_revenue_potential").cast(DoubleType())) \
                     .withColumn("customer_id", clean_id("customer_id")) \
                     .withColumn("contract_start_date", F.to_date("contract_start_date"))
quarantine(customers.filter(F.col("customer_id").isNull()), "customers", "missing_primary_key")
customers = customers.filter(F.col("customer_id").isNotNull()).dropDuplicates(["customer_id"])
write_iceberg(customers, "customers")

# 2. Facilities
facilities = read_bronze("facilities")
facilities = facilities.withColumn("latitude", F.col("latitude").cast(DoubleType())) \
                       .withColumn("longitude", F.col("longitude").cast(DoubleType())) \
                       .withColumn("facility_id", clean_id("facility_id"))
quarantine(facilities.filter(F.col("facility_id").isNull()), "facilities", "missing_primary_key")
facilities = facilities.filter(F.col("facility_id").isNotNull()).dropDuplicates(["facility_id"])
write_iceberg(facilities, "facilities")

# 3. Routes
routes = read_bronze("routes")
routes = routes.withColumn("typical_distance_miles", F.col("typical_distance_miles").cast(DoubleType())) \
               .withColumn("base_rate_per_mile", F.col("base_rate_per_mile").cast(DoubleType())) \
               .withColumn("route_id", clean_id("route_id"))
quarantine(routes.filter(F.col("route_id").isNull()), "routes", "missing_primary_key")
routes = routes.filter(F.col("route_id").isNotNull()).dropDuplicates(["route_id"])
write_iceberg(routes, "routes")

# 4. Drivers
drivers = read_bronze("drivers")
drivers = drivers.withColumn("years_experience", F.col("years_experience").cast(IntegerType())) \
                 .withColumn("driver_id", clean_id("driver_id"))
quarantine(drivers.filter(F.col("driver_id").isNull()), "drivers", "missing_primary_key")
drivers = drivers.filter(F.col("driver_id").isNotNull()).dropDuplicates(["driver_id"])
write_iceberg(drivers, "drivers")

# 5. Trucks
trucks = read_bronze("trucks")
trucks = trucks.withColumn("model_year", F.col("model_year").cast(IntegerType())) \
               .withColumn("truck_id", clean_id("truck_id"))
quarantine(trucks.filter(F.col("truck_id").isNull()), "trucks", "missing_primary_key")
trucks = trucks.filter(F.col("truck_id").isNotNull()).dropDuplicates(["truck_id"])
write_iceberg(trucks, "trucks")

# ============================================================
# FACTS
# ============================================================

# 6. Loads
loads = read_bronze("loads")
loads = loads.withColumn("weight_lbs", F.col("weight_lbs").cast(DoubleType())) \
             .withColumn("revenue", F.col("revenue").cast(DoubleType())) \
             .withColumn("load_id", clean_id("load_id"))
quarantine(loads.filter(F.col("load_id").isNull()), "loads", "missing_primary_key")
loads = loads.filter(F.col("load_id").isNotNull()).dropDuplicates(["load_id"])
quarantine(loads.filter((F.col("weight_lbs") <= 0) | (F.col("revenue") < 0)), "loads", "invalid_business_values")
loads = loads.filter((F.col("weight_lbs") > 0) & (F.col("revenue") >= 0))

loads = keep_valid_fk(loads, "customer_id", customers, "customer_id", "loads", "invalid_customer_fk")
loads = keep_valid_fk(loads, "route_id", routes, "route_id", "loads", "invalid_route_fk")
write_iceberg(loads, "loads")

# 7. Trips
trips = read_bronze("trips")
trips = trips.withColumn("actual_duration_hours", F.col("actual_duration_hours").cast(DoubleType())) \
             .withColumn("idle_time_hours", F.col("idle_time_hours").cast(DoubleType())) \
             .withColumn("trip_id", clean_id("trip_id"))
quarantine(trips.filter(F.col("trip_id").isNull()), "trips", "missing_primary_key")
trips = trips.filter(F.col("trip_id").isNotNull()).dropDuplicates(["trip_id"])

bad_metrics = trips.filter((F.col("actual_duration_hours") < 0) | (F.col("idle_time_hours") < 0) | (F.col("idle_time_hours") > F.col("actual_duration_hours")))
quarantine(bad_metrics, "trips", "invalid_duration_metrics")
trips = trips.filter((F.col("actual_duration_hours") >= 0) & (F.col("idle_time_hours") >= 0) & (F.col("idle_time_hours") <= F.col("actual_duration_hours")))

trips = keep_valid_fk(trips, "load_id", loads, "load_id", "trips", "invalid_load_fk")
trips = keep_valid_fk(trips, "driver_id", drivers, "driver_id", "trips", "invalid_driver_fk")
trips = keep_valid_fk(trips, "truck_id", trucks, "truck_id", "trips", "invalid_truck_fk")
write_iceberg(trips, "trips")

# 8. Delivery Events
events = read_bronze("delivery_events")
events = events.withColumn("scheduled_datetime", F.to_timestamp("scheduled_datetime")) \
               .withColumn("actual_datetime", F.to_timestamp("actual_datetime")) \
               .withColumn("event_id", clean_id("event_id"))
quarantine(events.filter(F.col("event_id").isNull()), "delivery_events", "missing_primary_key")
events = events.filter(F.col("event_id").isNotNull()).dropDuplicates(["event_id"])

bad_times = events.filter(F.col("actual_datetime") < F.col("scheduled_datetime"))
quarantine(bad_times, "delivery_events", "invalid_event_timestamps")
events = events.filter(F.col("actual_datetime") >= F.col("scheduled_datetime"))

events = keep_valid_fk(events, "load_id", loads, "load_id", "delivery_events", "orphan_load_fk")
events = keep_valid_fk(events, "trip_id", trips, "trip_id", "delivery_events", "orphan_trip_fk")
events = keep_valid_fk(events, "facility_id", facilities, "facility_id", "delivery_events", "invalid_facility_fk")
write_iceberg(events, "delivery_events")

job.commit()