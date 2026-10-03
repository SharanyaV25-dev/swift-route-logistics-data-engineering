import sys
import csv
import shutil
from pathlib import Path
from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, IntegerType, LongType, DoubleType

# Ensure Python can find rules.py in the same directory
sys.path.append(str(Path(__file__).resolve().parent))
from rules import (
    clean_id, deduplicate_deterministic, get_orphans, keep_valid_fk, 
    get_invalid_loads, get_valid_loads, get_invalid_trips, 
    get_valid_trips, get_invalid_events, get_valid_events
)

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

def cast(df, mapping):
    for c,t in mapping.items(): df=df.withColumn(c,F.col(c).cast(t))
    return df

print('\n'+'='*70+'\nSWIFTROUTE - SILVER TRANSFORMATION\n'+'='*70)
bronze={t:read_bronze(t) for t in TABLES}
for t,df in bronze.items(): print(f'Bronze loaded: {t:20s} {df.count():,} rows')

# ============================================================
# DIMENSION TABLES
# ============================================================

# Customers
customers = cast(bronze["customers"], {"annual_revenue_potential": DoubleType()}) \
            .withColumn("customer_id", clean_id("customer_id")) \
            .withColumn("contract_start_date", F.to_date("contract_start_date"))
quarantine(customers.filter(F.col("customer_id").isNull()), "customers", "missing_primary_key")
customers = deduplicate_deterministic(customers.filter(F.col("customer_id").isNotNull()), "customer_id")

# Facilities
facilities = cast(bronze["facilities"], {"latitude": DoubleType(), "longitude": DoubleType()}) \
             .withColumn("facility_id", clean_id("facility_id"))
quarantine(facilities.filter(F.col("facility_id").isNull()), "facilities", "missing_primary_key")
facilities = deduplicate_deterministic(facilities.filter(F.col("facility_id").isNotNull()), "facility_id")

# Routes
routes = cast(bronze["routes"], {"typical_distance_miles": DoubleType(), "base_rate_per_mile": DoubleType(), "fuel_surcharge_rate": DoubleType(), "typical_transit_days": DoubleType()}) \
         .withColumn("route_id", clean_id("route_id"))
quarantine(routes.filter(F.col("route_id").isNull()), "routes", "missing_primary_key")
routes = deduplicate_deterministic(routes.filter(F.col("route_id").isNotNull()), "route_id")

# Drivers
drivers = cast(bronze["drivers"], {"years_experience": IntegerType()}) \
          .withColumn("driver_id", clean_id("driver_id")) \
          .withColumn("hire_date", F.to_date("hire_date")) \
          .withColumn("termination_date", F.to_date("termination_date"))
quarantine(drivers.filter(F.col("driver_id").isNull()), "drivers", "missing_primary_key")
drivers = deduplicate_deterministic(drivers.filter(F.col("driver_id").isNotNull()), "driver_id")

# Trucks
trucks = cast(bronze["trucks"], {"model_year": IntegerType(), "acquisition_mileage": LongType(), "tank_capacity_gallons": DoubleType()}) \
         .withColumn("truck_id", clean_id("truck_id")) \
         .withColumn("acquisition_date", F.to_date("acquisition_date"))
quarantine(trucks.filter(F.col("truck_id").isNull()), "trucks", "missing_primary_key")
trucks = deduplicate_deterministic(trucks.filter(F.col("truck_id").isNotNull()), "truck_id")


# ============================================================
# FACT TABLES
# ============================================================

# Loads
loads = cast(bronze["loads"], {"weight_lbs": DoubleType(), "pieces": IntegerType(), "revenue": DoubleType()}) \
    .withColumn("load_id", clean_id("load_id")) \
    .withColumn("customer_id", clean_id("customer_id")) \
    .withColumn("route_id", clean_id("route_id")) \
    .withColumn("load_date", F.to_date("load_date"))

quarantine(loads.filter(F.col("load_id").isNull()), "loads", "missing_primary_key")
loads = deduplicate_deterministic(loads.filter(F.col("load_id").isNotNull()), "load_id")

quarantine(get_invalid_loads(loads), "loads", "invalid_business_values")
loads = get_valid_loads(loads)

for fk_col, parent_df, reason in [("customer_id", customers, "invalid_customer_fk"), ("route_id", routes, "invalid_route_fk")]:
    quarantine(get_orphans(loads, fk_col, parent_df, fk_col), "loads", reason)
    loads = keep_valid_fk(loads, fk_col, parent_df, fk_col)

# Trips
trips = cast(bronze["trips"], {"actual_distance_miles": DoubleType(), "actual_duration_hours": DoubleType(), "fuel_gallons_used": DoubleType(), "idle_time_hours": DoubleType()}) \
    .withColumn("trip_id", clean_id("trip_id")).withColumn("load_id", clean_id("load_id")).withColumn("driver_id", clean_id("driver_id")).withColumn("truck_id", clean_id("truck_id")).withColumn("dispatch_date", F.to_date("dispatch_date"))

quarantine(trips.filter(F.col("trip_id").isNull()), "trips", "missing_primary_key")
trips = deduplicate_deterministic(trips.filter(F.col("trip_id").isNotNull()), "trip_id")

quarantine(get_invalid_trips(trips), "trips", "invalid_duration_metrics")
trips = get_valid_trips(trips)

for fk_col, parent_df, reason in [("load_id", loads, "invalid_load_fk"), ("driver_id", drivers, "invalid_driver_fk"), ("truck_id", trucks, "invalid_truck_fk")]:
    quarantine(get_orphans(trips, fk_col, parent_df, fk_col), "trips", reason)
    trips = keep_valid_fk(trips, fk_col, parent_df, fk_col)

# Delivery Events
events = cast(bronze["delivery_events"], {"detention_minutes": IntegerType(), "on_time_flag": BooleanType()}) \
    .withColumn("event_id", clean_id("event_id")).withColumn("load_id", clean_id("load_id")).withColumn("trip_id", clean_id("trip_id")).withColumn("facility_id", clean_id("facility_id")).withColumn("scheduled_datetime", F.to_timestamp("scheduled_datetime")).withColumn("actual_datetime", F.to_timestamp("actual_datetime"))

quarantine(events.filter(F.col("event_id").isNull()), "delivery_events", "missing_primary_key")
events = deduplicate_deterministic(events.filter(F.col("event_id").isNotNull()), "event_id")

quarantine(get_invalid_events(events), "delivery_events", "invalid_event_timestamps")
events = get_valid_events(events)

for fk_col, parent_df, reason in [("load_id", loads, "orphan_load_fk"), ("trip_id", trips, "orphan_trip_fk"), ("facility_id", facilities, "invalid_facility_fk")]:
    quarantine(get_orphans(events, fk_col, parent_df, fk_col), "delivery_events", reason)
    events = keep_valid_fk(events, fk_col, parent_df, fk_col)

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