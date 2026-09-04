from pathlib import Path
from datetime import datetime, timezone
import csv, shutil
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import BooleanType, IntegerType, LongType, DoubleType

PROJECT_ROOT = Path(__file__).resolve().parent.parent
BRONZE_DIR = PROJECT_ROOT / 'data' / 'bronze'
SILVER_DIR = PROJECT_ROOT / 'data' / 'silver'
QUARANTINE_DIR = SILVER_DIR / 'quarantine'
DOCS_DIR = PROJECT_ROOT / 'docs'
for p in (SILVER_DIR, QUARANTINE_DIR, DOCS_DIR): p.mkdir(parents=True, exist_ok=True)

TABLES = ['customers','facilities','routes','drivers','trucks','loads','trips','delivery_events']
spark = (SparkSession.builder.appName('SwiftRoute-Silver-Transformation').master('local[*]')
         .config('spark.sql.legacy.timeParserPolicy','LEGACY').getOrCreate())
spark.sparkContext.setLogLevel('WARN')

def read_bronze(table):
    files = list((BRONZE_DIR / table).glob('*.csv'))
    if not files: raise FileNotFoundError(f'No Bronze CSV found for {table}: {BRONZE_DIR/table}')
    return spark.read.option('header',True).option('inferSchema',False).option('mode','PERMISSIVE').csv(str(files[0]))

def quarantine(df, table, reason):
    if df.limit(1).count() == 0: return
    out = QUARANTINE_DIR / table / reason
    if out.exists(): shutil.rmtree(out)
    (df.withColumn('quarantine_reason',F.lit(reason))
       .withColumn('quarantined_at_utc',F.lit(datetime.now(timezone.utc).isoformat()))
       .write.mode('overwrite').parquet(str(out)))

def clean_id(c): return F.when(F.trim(F.col(c))=='',None).otherwise(F.trim(F.col(c)))
def cast(df, mapping):
    for c,t in mapping.items(): df=df.withColumn(c,F.col(c).cast(t))
    return df

def keep_valid_fk(child, child_col, parent, parent_col, table, reason):
    valid = parent.select(F.col(parent_col).alias('_valid_fk')).distinct()
    bad = (child.join(valid, child[child_col]==F.col('_valid_fk'),'left')
           .filter(F.col('_valid_fk').isNull()).drop('_valid_fk'))
    quarantine(bad, table, reason)
    return child.join(valid, child[child_col]==F.col('_valid_fk'),'inner').drop('_valid_fk')

print('\n'+'='*70+'\nSWIFTROUTE - SILVER TRANSFORMATION\n'+'='*70)
bronze={t:read_bronze(t) for t in TABLES}
for t,df in bronze.items(): print(f'Bronze loaded: {t:20s} {df.count():,} rows')

# ============================================================
# DIMENSION TABLES
# ============================================================

# Customers
customers = (
    cast(
        bronze["customers"],
        {"annual_revenue_potential": DoubleType()}
    )
    .withColumn("customer_id", clean_id("customer_id"))
    .withColumn("contract_start_date", F.to_date("contract_start_date"))
)

quarantine(
    customers.filter(F.col("customer_id").isNull()),
    "customers",
    "missing_primary_key"
)

customers = (
    customers
    .filter(F.col("customer_id").isNotNull())
    .dropDuplicates(["customer_id"])
)


# Facilities
facilities = (
    cast(
        bronze["facilities"],
        {
            "latitude": DoubleType(),
            "longitude": DoubleType()
        }
    )
    .withColumn("facility_id", clean_id("facility_id"))
)

quarantine(
    facilities.filter(F.col("facility_id").isNull()),
    "facilities",
    "missing_primary_key"
)

facilities = (
    facilities
    .filter(F.col("facility_id").isNotNull())
    .dropDuplicates(["facility_id"])
)


# Routes
routes = (
    cast(
        bronze["routes"],
        {
            "typical_distance_miles": DoubleType(),
            "base_rate_per_mile": DoubleType(),
            "fuel_surcharge_rate": DoubleType(),
            "typical_transit_days": DoubleType()
        }
    )
    .withColumn("route_id", clean_id("route_id"))
)

quarantine(
    routes.filter(F.col("route_id").isNull()),
    "routes",
    "missing_primary_key"
)

routes = (
    routes
    .filter(F.col("route_id").isNotNull())
    .dropDuplicates(["route_id"])
)


# Drivers
drivers = (
    cast(
        bronze["drivers"],
        {"years_experience": IntegerType()}
    )
    .withColumn("driver_id", clean_id("driver_id"))
    .withColumn("hire_date", F.to_date("hire_date"))
    .withColumn("termination_date", F.to_date("termination_date"))
)

quarantine(
    drivers.filter(F.col("driver_id").isNull()),
    "drivers",
    "missing_primary_key"
)

drivers = (
    drivers
    .filter(F.col("driver_id").isNotNull())
    .dropDuplicates(["driver_id"])
)


# Trucks
trucks = (
    cast(
        bronze["trucks"],
        {
            "model_year": IntegerType(),
            "acquisition_mileage": LongType(),
            "tank_capacity_gallons": DoubleType()
        }
    )
    .withColumn("truck_id", clean_id("truck_id"))
    .withColumn("acquisition_date", F.to_date("acquisition_date"))
)

quarantine(
    trucks.filter(F.col("truck_id").isNull()),
    "trucks",
    "missing_primary_key"
)

trucks = (
    trucks
    .filter(F.col("truck_id").isNotNull())
    .dropDuplicates(["truck_id"])
)


# ============================================================
# FACT TABLES
# ============================================================

# Loads
loads = (
    cast(
        bronze["loads"],
        {
            "weight_lbs": DoubleType(),
            "pieces": IntegerType(),
            "revenue": DoubleType()
        }
    )
    .withColumn("load_id", clean_id("load_id"))
    .withColumn("customer_id", clean_id("customer_id"))
    .withColumn("route_id", clean_id("route_id"))
    .withColumn("load_date", F.to_date("load_date"))
)

quarantine(
    loads.filter(F.col("load_id").isNull()),
    "loads",
    "missing_primary_key"
)

loads = loads.filter(F.col("load_id").isNotNull())

dup = (
    loads
    .groupBy("load_id")
    .count()
    .filter(F.col("count") > 1)
)

quarantine(
    loads.join(dup.select("load_id"), "load_id"),
    "loads",
    "duplicate_load_id"
)

loads = loads.dropDuplicates(["load_id"])

bad = loads.filter(
    F.col("weight_lbs").isNull()
    | (F.col("weight_lbs") <= 0)
    | F.col("revenue").isNull()
    | (F.col("revenue") < 0)
)

quarantine(
    bad,
    "loads",
    "invalid_business_values"
)

loads = loads.filter(
    (F.col("weight_lbs") > 0)
    & (F.col("revenue") >= 0)
)

loads = keep_valid_fk(
    loads, "customer_id",
    customers, "customer_id",
    "loads", "invalid_customer_fk"
)

loads = keep_valid_fk(
    loads, "route_id",
    routes, "route_id",
    "loads", "invalid_route_fk"
)


# Trips
trips = (
    cast(
        bronze["trips"],
        {
            "actual_distance_miles": DoubleType(),
            "actual_duration_hours": DoubleType(),
            "fuel_gallons_used": DoubleType(),
            "idle_time_hours": DoubleType()
        }
    )
    .withColumn("trip_id", clean_id("trip_id"))
    .withColumn("load_id", clean_id("load_id"))
    .withColumn("driver_id", clean_id("driver_id"))
    .withColumn("truck_id", clean_id("truck_id"))
    .withColumn("dispatch_date", F.to_date("dispatch_date"))
)

quarantine(
    trips.filter(F.col("trip_id").isNull()),
    "trips",
    "missing_primary_key"
)

trips = trips.filter(F.col("trip_id").isNotNull())

dup = (
    trips
    .groupBy("trip_id")
    .count()
    .filter(F.col("count") > 1)
)

quarantine(
    trips.join(dup.select("trip_id"), "trip_id"),
    "trips",
    "duplicate_trip_id"
)

trips = trips.dropDuplicates(["trip_id"])

quarantine(
    trips.filter(F.col("driver_id").isNull()),
    "trips",
    "missing_driver_fk"
)

trips = trips.filter(F.col("driver_id").isNotNull())

trips = keep_valid_fk(
    trips, "load_id",
    loads, "load_id",
    "trips", "invalid_load_fk"
)

trips = keep_valid_fk(
    trips, "driver_id",
    drivers, "driver_id",
    "trips", "invalid_driver_fk"
)

trips = keep_valid_fk(
    trips, "truck_id",
    trucks, "truck_id",
    "trips", "invalid_truck_fk"
)

bad = trips.filter(
    F.col("actual_duration_hours").isNull()
    | F.col("idle_time_hours").isNull()
    | (F.col("actual_duration_hours") < 0)
    | (F.col("idle_time_hours") < 0)
    | (F.col("idle_time_hours") > F.col("actual_duration_hours"))
)

quarantine(
    bad,
    "trips",
    "invalid_duration_metrics"
)

trips = trips.filter(
    (F.col("actual_duration_hours") >= 0)
    & (F.col("idle_time_hours") >= 0)
    & (F.col("idle_time_hours") <= F.col("actual_duration_hours"))
)


# Delivery Events
events = (
    cast(
        bronze["delivery_events"],
        {
            "detention_minutes": IntegerType(),
            "on_time_flag": BooleanType()
        }
    )
    .withColumn("event_id", clean_id("event_id"))
    .withColumn("load_id", clean_id("load_id"))
    .withColumn("trip_id", clean_id("trip_id"))
    .withColumn("facility_id", clean_id("facility_id"))
    .withColumn(
        "scheduled_datetime",
        F.to_timestamp("scheduled_datetime")
    )
    .withColumn(
        "actual_datetime",
        F.to_timestamp("actual_datetime")
    )
)

quarantine(
    events.filter(F.col("event_id").isNull()),
    "delivery_events",
    "missing_primary_key"
)

events = events.filter(F.col("event_id").isNotNull())

dup = (
    events
    .groupBy("event_id")
    .count()
    .filter(F.col("count") > 1)
)

quarantine(
    events.join(dup.select("event_id"), "event_id"),
    "delivery_events",
    "duplicate_event_id"
)

events = events.dropDuplicates(["event_id"])

bad = events.filter(
    F.col("scheduled_datetime").isNull()
    | F.col("actual_datetime").isNull()
    | (
        F.col("actual_datetime")
        < F.col("scheduled_datetime")
    )
)

quarantine(
    bad,
    "delivery_events",
    "invalid_event_timestamps"
)

events = events.filter(
    F.col("scheduled_datetime").isNotNull()
    & F.col("actual_datetime").isNotNull()
    & (
        F.col("actual_datetime")
        >= F.col("scheduled_datetime")
    )
)

events = keep_valid_fk(
    events, "load_id",
    loads, "load_id",
    "delivery_events", "orphan_load_fk"
)

events = keep_valid_fk(
    events, "trip_id",
    trips, "trip_id",
    "delivery_events", "orphan_trip_fk"
)

events = keep_valid_fk(
    events, "facility_id",
    facilities, "facility_id",
    "delivery_events", "invalid_facility_fk"
)

silver={'customers':customers,'facilities':facilities,'routes':routes,'drivers':drivers,'trucks':trucks,'loads':loads,'trips':trips,'delivery_events':events}
print('\n'+'='*70+'\nWRITING SILVER PARQUET\n'+'='*70)
report=[]
for t,df in silver.items():
    out=SILVER_DIR/t
    if out.exists(): shutil.rmtree(out)
    df.write.mode('overwrite').parquet(str(out))
    n=df.count(); report.append({'table_name':t,'silver_row_count':n,'processed_at_utc':datetime.now(timezone.utc).isoformat()})
    print(f'PASS: {t:20s} {n:>10,} rows')

report_path=DOCS_DIR/'silver_quality_report.csv'
with report_path.open('w',newline='',encoding='utf-8') as f:
    w=csv.DictWriter(f,fieldnames=['table_name','silver_row_count','processed_at_utc']); w.writeheader(); w.writerows(report)
print(f'\nQuality report: {report_path}')
print(f'Silver output:  {SILVER_DIR}')
print('\n'+'='*70+'\nSILVER TRANSFORMATION PASSED\n'+'='*70)
spark.stop()
