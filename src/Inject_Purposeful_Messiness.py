import os
import random
import pandas as pd


# ============================================================
# CONFIGURATION
# ============================================================

BASE_DIR = r"D:\Interview_Preparation_FinalG\VS Codes\SourceTables"

INPUT_DIR = os.path.join(
    BASE_DIR,
    "Synthetic_Generated_Table_Data"
)

OUTPUT_DIR = os.path.join(
    BASE_DIR,
    "Synthetic_Messy_Table_Data"
)

AUDIT_FILE = os.path.join(
    OUTPUT_DIR,
    "messiness_audit.csv"
)

RANDOM_SEED = 42

random.seed(RANDOM_SEED)

os.makedirs(OUTPUT_DIR, exist_ok=True)


# ============================================================
# MESSINESS COUNTS
# ============================================================

MESSINESS_COUNTS = {
    "duplicate_loads": 500,
    "missing_customer_ids": 250,
    "invalid_facility_ids": 150,
    "invalid_weights": 200,
    "invalid_revenue": 100,
    "duplicate_trips": 300,
    "missing_driver_ids": 100,
    "impossible_timestamps": 200,
    "duplicate_delivery_events": 1000,
    "orphan_events": 300
}


audit_records = []


def record_audit(
    table_name,
    issue_type,
    affected_count
):
    audit_records.append({
        "table_name": table_name,
        "issue_type": issue_type,
        "affected_count": affected_count
    })


# ============================================================
# LOAD CLEAN CSV FILES
# ============================================================

tables = {}

for filename in os.listdir(INPUT_DIR):

    if filename.endswith("_synthetic.csv"):

        table_name = filename.replace(
            "_synthetic.csv",
            ""
        )

        filepath = os.path.join(
            INPUT_DIR,
            filename
        )

        tables[table_name] = pd.read_csv(
            filepath
        )


print("=" * 75)
print("PURPOSEFUL DATA MESSINESS INJECTION")
print("=" * 75)


# ============================================================
# 1. DUPLICATE LOADS
# ============================================================

loads = tables["loads"]

duplicate_rows = loads.sample(
    n=MESSINESS_COUNTS["duplicate_loads"],
    random_state=RANDOM_SEED
).copy()

loads = pd.concat(
    [loads, duplicate_rows],
    ignore_index=True
)

record_audit(
    "loads",
    "duplicate_loads",
    MESSINESS_COUNTS["duplicate_loads"]
)


# ============================================================
# 2. MISSING CUSTOMER IDs
# ============================================================

rows = random.sample(
    list(loads.index),
    MESSINESS_COUNTS["missing_customer_ids"]
)

loads.loc[
    rows,
    "customer_id"
] = pd.NA

record_audit(
    "loads",
    "missing_customer_ids",
    MESSINESS_COUNTS["missing_customer_ids"]
)


# ============================================================
# 3. INVALID FACILITY IDs
# ============================================================
# Our current LOAD table uses route_id rather than
# origin/destination facility IDs.
#
# Therefore, we will NOT inject invalid facility IDs here.
# Facility-related corruption will be handled through
# delivery_events below.


# ============================================================
# 4. INVALID WEIGHTS
# ============================================================

rows = random.sample(
    list(loads.index),
    MESSINESS_COUNTS["invalid_weights"]
)

half = len(rows) // 2

loads.loc[
    rows[:half],
    "weight_lbs"
] = 0

loads.loc[
    rows[half:],
    "weight_lbs"
] = -100

record_audit(
    "loads",
    "negative_or_zero_weights",
    MESSINESS_COUNTS["invalid_weights"]
)


# ============================================================
# 5. INVALID REVENUE
# ============================================================

rows = random.sample(
    list(loads.index),
    MESSINESS_COUNTS["invalid_revenue"]
)

loads.loc[
    rows,
    "revenue"
] = -500

record_audit(
    "loads",
    "invalid_revenue",
    MESSINESS_COUNTS["invalid_revenue"]
)


tables["loads"] = loads


# ============================================================
# 6. DUPLICATE TRIPS
# ============================================================

trips = tables["trips"]

duplicate_rows = trips.sample(
    n=MESSINESS_COUNTS["duplicate_trips"],
    random_state=RANDOM_SEED
).copy()

trips = pd.concat(
    [trips, duplicate_rows],
    ignore_index=True
)

record_audit(
    "trips",
    "duplicate_trips",
    MESSINESS_COUNTS["duplicate_trips"]
)


# ============================================================
# 7. MISSING DRIVER IDs
# ============================================================

rows = random.sample(
    list(trips.index),
    MESSINESS_COUNTS["missing_driver_ids"]
)

trips.loc[
    rows,
    "driver_id"
] = pd.NA

record_audit(
    "trips",
    "missing_driver_ids",
    MESSINESS_COUNTS["missing_driver_ids"]
)

tables["trips"] = trips


# ============================================================
# 8. DELIVERY EVENT MESSINESS
# ============================================================

events = tables["delivery_events"]


# ------------------------------------------------------------
# 8A. DUPLICATE DELIVERY EVENTS
# ------------------------------------------------------------

duplicate_rows = events.sample(
    n=MESSINESS_COUNTS["duplicate_delivery_events"],
    random_state=RANDOM_SEED
).copy()

events = pd.concat(
    [events, duplicate_rows],
    ignore_index=True
)

record_audit(
    "delivery_events",
    "duplicate_delivery_events",
    MESSINESS_COUNTS["duplicate_delivery_events"]
)


# ------------------------------------------------------------
# 8B. IMPOSSIBLE TIMESTAMPS
# ------------------------------------------------------------

events["scheduled_datetime"] = pd.to_datetime(
    events["scheduled_datetime"]
)

events["actual_datetime"] = pd.to_datetime(
    events["actual_datetime"]
)

rows = random.sample(
    list(events.index),
    MESSINESS_COUNTS["impossible_timestamps"]
)

events.loc[
    rows,
    "actual_datetime"
] = (
    events.loc[
        rows,
        "scheduled_datetime"
    ]
    - pd.Timedelta(hours=24)
)

record_audit(
    "delivery_events",
    "impossible_timestamps",
    MESSINESS_COUNTS["impossible_timestamps"]
)


# ------------------------------------------------------------
# 8C. ORPHAN EVENTS
# ------------------------------------------------------------

orphan_rows = random.sample(
    list(events.index),
    MESSINESS_COUNTS["orphan_events"]
)

events.loc[
    orphan_rows,
    "trip_id"
] = "TRIP_INVALID"

record_audit(
    "delivery_events",
    "orphan_events",
    MESSINESS_COUNTS["orphan_events"]
)


# ------------------------------------------------------------
# 8D. INVALID FACILITY IDs
# ------------------------------------------------------------

invalid_facility_rows = random.sample(
    list(events.index),
    MESSINESS_COUNTS["invalid_facility_ids"]
)

events.loc[
    invalid_facility_rows,
    "facility_id"
] = "FAC_INVALID"

record_audit(
    "delivery_events",
    "invalid_facility_ids",
    MESSINESS_COUNTS["invalid_facility_ids"]
)


tables["delivery_events"] = events


# ============================================================
# SAVE MESSY DATA
# ============================================================

print("\nSaving messy datasets...")

for table_name, df in tables.items():

    output_file = os.path.join(
        OUTPUT_DIR,
        f"{table_name}_messy.csv"
    )

    df.to_csv(
        output_file,
        index=False
    )

    print(
        f"{table_name:20s} "
        f"{len(df):,} rows"
    )


# ============================================================
# SAVE MESSINESS AUDIT
# ============================================================

audit_df = pd.DataFrame(
    audit_records
)

audit_df.to_csv(
    AUDIT_FILE,
    index=False
)


# ============================================================
# SUMMARY
# ============================================================

print("\n" + "=" * 75)
print("MESSINESS INJECTION COMPLETED")
print("=" * 75)

print(
    audit_df.to_string(index=False)
)

print(
    f"\nAudit report saved to:\n{AUDIT_FILE}"
)

print(
    f"\nMessy datasets saved to:\n{OUTPUT_DIR}"
)