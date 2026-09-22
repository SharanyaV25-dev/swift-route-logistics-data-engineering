import sys
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as spark_sum, avg, round, when, lit, concat_ws
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Initialize Spark with AWS Glue Iceberg Extensions
spark = SparkSession.builder \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.glue_catalog.warehouse", "s3://swiftroute-logistics-de-bucket/gold/") \
    .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
    .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .getOrCreate()

glueContext = GlueContext(spark.sparkContext)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

DATABASE = "swiftroute_iceberg"

def read_silver(table_name):
    """Read the cleaned Silver Iceberg table directly from the catalog."""
    return spark.table(f"glue_catalog.{DATABASE}.{table_name}")

def write_gold(df, table_name):
    """Write aggregated Gold DataFrames as new Iceberg tables."""
    df.writeTo(f"glue_catalog.{DATABASE}.{table_name}") \
      .tableProperty("format-version", "2") \
      .createOrReplace()

# Read all 8 Silver tables
customers = read_silver("customers")
facilities = read_silver("facilities")
routes = read_silver("routes")
drivers = read_silver("drivers")
trucks = read_silver("trucks")
loads = read_silver("loads")
trips = read_silver("trips")
delivery_events = read_silver("delivery_events")

# 1. GOLD LOAD PERFORMANCE
load_performance = (
    loads
    .join(customers.select("customer_id", "customer_name", "customer_type", "account_status"), on="customer_id", how="left")
    .join(routes.select("route_id", "origin_city", "origin_state", "destination_city", "destination_state"), on="route_id", how="left")
    .select("load_id", "customer_id", "customer_name", "customer_type", "account_status", "route_id", "origin_city", "origin_state", "destination_city", "destination_state", "load_date", "load_type", "weight_lbs", "pieces", "revenue", "load_status", "booking_type")
)
write_gold(load_performance, "gold_load_performance")

# 2. GOLD DELIVERY PERFORMANCE
delivery_performance = (
    delivery_events
    .join(loads.select("load_id", "customer_id", "revenue"), on="load_id", how="left")
    .join(trips.select("trip_id", "driver_id", "truck_id", "trip_status"), on="trip_id", how="left")
    .join(facilities.select(col("facility_id"), "facility_name", "facility_type"), on="facility_id", how="left")
    .select("event_id", "load_id", "trip_id", "customer_id", "driver_id", "truck_id", "event_type", "facility_id", "facility_name", "facility_type", "location_city", "location_state", "scheduled_datetime", "actual_datetime", "detention_minutes", "on_time_flag", "revenue", "trip_status")
)
write_gold(delivery_performance, "gold_delivery_performance")

# 3. GOLD DRIVER PERFORMANCE
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
    .join(drivers.select("driver_id", concat_ws(" ", col("first_name"), col("last_name")).alias("driver_name"), "employment_status", "years_experience"), on="driver_id", how="left")
    .withColumn("fuel_efficiency_miles_per_gallon", round(col("total_distance_miles") / col("total_fuel_gallons"), 2))
    .select("driver_id", "driver_name", "employment_status", "years_experience", "total_trips", "total_distance_miles", "avg_distance_miles", "avg_duration_hours", "total_fuel_gallons", "avg_fuel_gallons", "fuel_efficiency_miles_per_gallon", "total_idle_time_hours", "avg_idle_time_hours")
)
write_gold(driver_performance, "gold_driver_performance")

# 4. GOLD TRUCK PERFORMANCE
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
    .join(trucks.select("truck_id", "unit_number", "make", "model_year", "fuel_type", "tank_capacity_gallons", "status"), on="truck_id", how="left")
    .withColumn("fuel_efficiency_miles_per_gallon", when(col("total_fuel_gallons") > 0, round(col("total_distance_miles") / col("total_fuel_gallons"), 2)).otherwise(lit(0)))
    .select("truck_id", "unit_number", "make", "model_year", "fuel_type", "tank_capacity_gallons", "status", "total_trips", "total_distance_miles", "avg_distance_miles", "total_fuel_gallons", "avg_fuel_per_trip_gallons", "fuel_efficiency_miles_per_gallon", "total_idle_time_hours", "avg_idle_time_hours")
)
write_gold(truck_performance, "gold_truck_performance")

# 5. GOLD FACILITY PERFORMANCE
facility_performance = (
    delivery_events
    .groupBy("facility_id")
    .agg(
        count("*").alias("total_events"),
        spark_sum(when(col("on_time_flag") == True, 1).otherwise(0)).alias("on_time_events"),
        avg("detention_minutes").alias("avg_detention_minutes")
    )
    .join(facilities.select("facility_id", "facility_name", "city", "state"), on="facility_id", how="left")
    .withColumn("on_time_rate", round(col("on_time_events") / col("total_events") * 100, 2))
    .select("facility_id", "facility_name", "city", "state", "total_events", "on_time_events", "on_time_rate", "avg_detention_minutes")
)
write_gold(facility_performance, "gold_facility_performance")

# 6. GOLD ROUTE PERFORMANCE
route_performance = (
    trips
    .join(loads.select("load_id", "route_id"), on="load_id", how="left")
    .groupBy("route_id")
    .agg(
        count("*").alias("total_trips"),
        avg("actual_duration_hours").alias("avg_trip_duration_hours"),
        avg("actual_distance_miles").alias("avg_actual_distance_miles"),
        avg("fuel_gallons_used").alias("avg_fuel_consumed_gallons")
    )
    .join(routes.select("route_id", "origin_city", "origin_state", "destination_city", "destination_state", "typical_distance_miles", "typical_transit_days"), on="route_id", how="left")
    .withColumn("fuel_efficiency_miles_per_gallon", round(col("avg_actual_distance_miles") / col("avg_fuel_consumed_gallons"), 2))
    .select("route_id", "origin_city", "origin_state", "destination_city", "destination_state", "typical_distance_miles", "typical_transit_days", "total_trips", "avg_trip_duration_hours", "avg_actual_distance_miles", "avg_fuel_consumed_gallons", "fuel_efficiency_miles_per_gallon")
)
write_gold(route_performance, "gold_route_performance")

# 7. GOLD CUSTOMER PERFORMANCE
customer_performance = (
    loads
    .groupBy("customer_id")
    .agg(
        count("*").alias("total_loads"),
        spark_sum("revenue").alias("total_revenue"),
        avg("weight_lbs").alias("avg_load_weight_lbs"),
        spark_sum("pieces").alias("total_pieces")
    )
    .join(customers.select("customer_id", "customer_name", "customer_type", "account_status"), on="customer_id", how="left")
    .withColumn("avg_revenue_per_load", round(col("total_revenue") / col("total_loads"), 2))
    .select("customer_id", "customer_name", "customer_type", "account_status", "total_loads", "total_revenue", "avg_revenue_per_load", "avg_load_weight_lbs", "total_pieces")
)
write_gold(customer_performance, "gold_customer_performance")

# 8. GOLD DAILY OPERATIONS
daily_operations = (
    delivery_events
    .groupBy(col("actual_datetime").cast("date").alias("event_date"))
    .agg(
        count("*").alias("total_events"),
        spark_sum(when(col("on_time_flag") == True, 1).otherwise(0)).alias("on_time_events"),
        avg("detention_minutes").alias("avg_detention_minutes")
    )
    .withColumn("on_time_rate", round(col("on_time_events") / col("total_events") * 100, 2))
    .select("event_date", "total_events", "on_time_events", "on_time_rate", "avg_detention_minutes")
    .orderBy("event_date")
)
write_gold(daily_operations, "gold_daily_operations")

job.commit()