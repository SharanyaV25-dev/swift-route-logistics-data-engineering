import os
import random
import numpy as np
import pandas as pd
import sdv

from sdv.metadata import Metadata
from sdv.metadata import SingleTableMetadata
from sdv.single_table import GaussianCopulaSynthesizer

# ============================================================
# CONFIGURATION
# ============================================================

SOURCE_DIR = r"D:\\Interview_Preparation_FinalG\\VS Codes\\SourceTables"

OUTPUT_DIR = r"D:\\Interview_Preparation_FinalG\\VS Codes\\Synthetic_Generated_Table_Data"

METADATA_DIR = r"D:\\Interview_Preparation_FinalG\\VS Codes\\Metadata"


# Target number of records required for your project
TARGET_ROWS = {
    "customers": 500,
    "facilities": 50,
    "drivers": 300,
    "trucks": 200,
    "loads": 50000,
    "routes": 100,
    "trips": 50000,
    "delivery_events": 250000
}


# Actual source files
SOURCE_FILES = {
    "customers": "customers.csv",
    "facilities": "facilities.csv",
    "drivers": "drivers.csv",
    "trucks": "trucks.csv",
    "loads": "loads.csv",
    "routes": "routes.csv",
    "trips": "trips.csv",
    "delivery_events": "delivery_events.csv"
}


# ============================================================
# CREATE OUTPUT DIRECTORIES
# ============================================================

os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(METADATA_DIR, exist_ok=True)


print("=" * 70)
print("SWIFTROUTE - MULTI-TABLE SYNTHETIC DATA GENERATOR")
print("=" * 70)

print(f"SDV version: {sdv.__version__}")


# ============================================================
# 1. LOAD ALL 7 SOURCE TABLES
# ============================================================

data = {}

for table_name, filename in SOURCE_FILES.items():

    filepath = os.path.join(SOURCE_DIR, filename)

    if not os.path.exists(filepath):
        raise FileNotFoundError(
            f"Source file not found:\n{filepath}"
        )

    df = pd.read_csv(filepath)

    data[table_name] = df

    print(
        f"{table_name:20s} "
        f"{len(df):>10,} rows "
        f"{len(df.columns):>3} columns"
    )


print("\nSource tables loaded successfully.")


# ============================================================
# 2. BASIC DATA CHECKS
# ============================================================

print("\n" + "=" * 70)
print("BASIC SOURCE DATA CHECKS")
print("=" * 70)


for table_name, df in data.items():

    print(
        f"{table_name:20s} "
        f"rows={len(df):>10,} "
        f"duplicates={df.duplicated().sum():>6,}"
    )


# ============================================================
# 3. CREATE MULTI-TABLE METADATA
# ============================================================

print("\n" + "=" * 70)
print("CREATING MULTI-TABLE METADATA")
print("=" * 70)


metadata = Metadata.detect_from_dataframes(data)
metadata.save_to_json(os.path.join(METADATA_DIR, "metadata_V1.json"))
print(f"\nAutomatically detected metadata saved to {os.path.join(METADATA_DIR, 'metadata_V1.json')}")

# ============================================================
# 4. ENSURE PRIMARY KEYS
# ============================================================

PRIMARY_KEYS = {
    "customers": "customer_id",
    "facilities": "facility_id",
    "drivers": "driver_id",
    "trucks": "truck_id",
    "loads": "load_id",
    "routes": "route_id",
    "trips": "trip_id",
    "delivery_events": "event_id"
}
print("\n" + "=" * 75)
print("SETTING PRIMARY KEYS")
print("=" * 75)
for table_name, pk_column in PRIMARY_KEYS.items():
    metadata.set_primary_key(
        table_name=table_name,
        column_name=pk_column
    )


# ============================================================
# 5. ADD RELATIONSHIPS ONLY IF NOT ALREADY DETECTED
# ============================================================

print("\n" + "=" * 75)
print("SETTING RELATIONSHIPS")
print("=" * 75)

RELATIONSHIPS = [

    # Customers -> Loads
    (
        "customers",
        "loads",
        "customer_id",
        "customer_id"
    ),

    # Routes -> Loads
    (
        "routes",
        "loads",
        "route_id",
        "route_id"
    ),

    # Loads -> Trips
    (
        "loads",
        "trips",
        "load_id",
        "load_id"
    ),

    # Drivers -> Trips
    (
        "drivers",
        "trips",
        "driver_id",
        "driver_id"
    ),

    # Trucks -> Trips
    (
        "trucks",
        "trips",
        "truck_id",
        "truck_id"
    ),

    # Loads -> Delivery Events
    (
        "loads",
        "delivery_events",
        "load_id",
        "load_id"
    ),

    # Trips -> Delivery Events
    (
        "trips",
        "delivery_events",
        "trip_id",
        "trip_id"
    ),

    # Facilities -> Delivery Events
    (
        "facilities",
        "delivery_events",
        "facility_id",
        "facility_id"
    )
]


for (
    parent_table,
    child_table,
    parent_pk,
    child_fk
) in RELATIONSHIPS:
    try:
        metadata.add_relationship(
            parent_table_name=parent_table,
            child_table_name=child_table,
            parent_primary_key=parent_pk,
            child_foreign_key=child_fk
        )
        print(
            f"ADDED: {parent_table}.{parent_pk}"
            f" -> "
            f"{child_table}.{child_fk}"
        )
    except Exception as e:
        if "already been added" in str(e):
            print(
                f"ALREADY EXISTS: "
                f"{parent_table}.{parent_pk}"
                f" -> "
                f"{child_table}.{child_fk}"
            )
        else:
            raise
   

# ============================================================
# 6. VALIDATE METADATA
# ============================================================

print("\n" + "=" * 75)
print("VALIDATING FINAL METADATA")
print("=" * 75)
metadata.validate()
print("\nMetadata validation PASSED!")


# ============================================================
# 7. SAVE METADATA
# ============================================================

metadata_file = os.path.join(
    METADATA_DIR,
    "swift_route_multi_table_metadata_V1.json"
)

metadata.save_to_json(metadata_file)

print(
    f"Metadata saved to:\n{metadata_file}"
)


# ============================================================
# 8. VALIDATE SOURCE FOREIGN KEYS
# ============================================================

print("\n" + "=" * 70)
print("FOREIGN KEY VALIDATION")
print("=" * 70)


def check_fk(
    parent_df,
    parent_pk,
    child_df,
    child_fk,
    relationship_name
):

    parent_values = set(
        parent_df[parent_pk]
        .dropna()
        .astype(str)
    )

    child_values = set(
        child_df[child_fk]
        .dropna()
        .astype(str)
    )

    orphan_values = child_values - parent_values

    child_non_null_count = (
        child_df[child_fk]
        .notna()
        .sum()
    )

    if len(orphan_values) == 0:

        print(
            f"PASS  {relationship_name}"
        )

    else:

        print(
            f"WARNING {relationship_name}: "
            f"{len(orphan_values):,} orphan FK values "
            f"out of {child_non_null_count:,}"
        )

        # Do NOT silently fix the data.
        # We want to know if the real Kaggle data contains
        # broken relationships before training.


for (
    parent_table,
    child_table,
    parent_pk,
    child_fk
) in RELATIONSHIPS:

    check_fk(
        data[parent_table],
        parent_pk,
        data[child_table],
        child_fk,
        f"{parent_table}.{parent_pk} -> "
        f"{child_table}.{child_fk}"
    )


# ============================================================
# 9. SINGLE-TABLE SYNTHESIS + RELATIONSHIP CONTROL
# ============================================================
# ============================================================
# REPRODUCIBILITY
# ============================================================

SEED = 42

random.seed(SEED)
np.random.seed(SEED)


# ============================================================
# OUTPUT DIRECTORIES
# ============================================================

SYNTHETIC_DIR = os.path.join(SOURCE_DIR, "Synthetic_Generated_Table_Data")

os.makedirs(SYNTHETIC_DIR, exist_ok=True)

# ============================================================
# HELPER: CREATE DETERMINISTIC IDS
# ============================================================

def create_ids(prefix, count):
    return [
        f"{prefix}{i:08d}"
        for i in range(1, count + 1)
    ]
# ============================================================
# HELPER: SINGLE-TABLE SYNTHESIS
# ============================================================

def synthesize_single_table(
    table_name,
    source_df,
    selected_columns,
    target_rows,
    pk_column,
    pk_prefix
):

    print("\n" + "=" * 75)
    print(f"SYNTHESIZING: {table_name.upper()}")
    print("=" * 75)

    df = source_df[selected_columns].copy()

    # --------------------------------------------------------
    # Remove PK from model training.
    # We create deterministic synthetic PKs afterwards.
    # --------------------------------------------------------

    model_columns = [
        col for col in selected_columns
        if col != pk_column
    ]

    model_df = df[model_columns].copy()

    # --------------------------------------------------------
    # Create single-table metadata
    # --------------------------------------------------------

    metadata = SingleTableMetadata()
    metadata.detect_from_dataframe(data=model_df)

    metadata.validate()

    # --------------------------------------------------------
    # Train synthesizer
    # --------------------------------------------------------

    synthesizer = GaussianCopulaSynthesizer(
        metadata
    )

    print(f"Training synthesizer for {table_name}...")

    synthesizer.fit(model_df)

    # --------------------------------------------------------
    # Generate exact number of rows
    # --------------------------------------------------------

    generated = synthesizer.sample(
        num_rows=target_rows
    )

    # --------------------------------------------------------
    # Add deterministic primary key
    # --------------------------------------------------------

    generated.insert(
        0,
        pk_column,
        create_ids(pk_prefix, target_rows)
    )

    print(
        f"Generated {len(generated):,} rows "
        f"for {table_name}"
    )

    return generated

# ============================================================
# 10. GENERATE DIMENSION TABLES
# ============================================================
synthetic = {}
# ------------------------------------------------------------
# CUSTOMERS
# ------------------------------------------------------------
synthetic["customers"] = synthesize_single_table(
    table_name="customers",
    source_df=data["customers"],
    selected_columns=[
        "customer_id",
        "customer_name",
        "customer_type",
        "account_status",
        "contract_start_date",
        "annual_revenue_potential"
    ],
    target_rows=TARGET_ROWS["customers"],
    pk_column="customer_id",
    pk_prefix="CUST"
)
# ------------------------------------------------------------
# FACILITIES
# ------------------------------------------------------------
synthetic["facilities"] = synthesize_single_table(
    table_name="facilities",
    source_df=data["facilities"],
    selected_columns=[
        "facility_id",
        "facility_name",
        "facility_type",
        "city",
        "state",
        "latitude",
        "longitude",
        "operating_hours"
    ],
    target_rows=TARGET_ROWS["facilities"],
    pk_column="facility_id",
    pk_prefix="FAC"
)
# ------------------------------------------------------------
# DRIVERS
# ------------------------------------------------------------
synthetic["drivers"] = synthesize_single_table(
    table_name="drivers",
    source_df=data["drivers"],
    selected_columns=[
        "driver_id",
        "first_name",
        "last_name",
        "hire_date",
        "termination_date",
        "employment_status",
        "years_experience"
    ],
    target_rows=TARGET_ROWS["drivers"],
    pk_column="driver_id",
    pk_prefix="DRIV"
)
# ------------------------------------------------------------
# TRUCKS
# ------------------------------------------------------------
synthetic["trucks"] = synthesize_single_table(
    table_name="trucks",
    source_df=data["trucks"],
    selected_columns=[
        "truck_id",
        "unit_number",
        "make",
        "model_year",
        "acquisition_date",
        "acquisition_mileage",
        "fuel_type",
        "tank_capacity_gallons",
        "status"
    ],
    target_rows=TARGET_ROWS["trucks"],
    pk_column="truck_id",
    pk_prefix="TRK"
)
# ------------------------------------------------------------
# ROUTES
# ------------------------------------------------------------
synthetic["routes"] = synthesize_single_table(
    table_name="routes",
    source_df=data["routes"],
    selected_columns=[
        "route_id",
        "origin_city",
        "origin_state",
        "destination_city",
        "destination_state",
        "typical_distance_miles",
        "base_rate_per_mile",
        "fuel_surcharge_rate",
        "typical_transit_days"
    ],
    target_rows=TARGET_ROWS["routes"],
    pk_column="route_id",
    pk_prefix="RTE"
)
# ============================================================
# 11. GENERATE LOADS
# ============================================================

print("\n" + "=" * 75)
print("GENERATING LOADS")
print("=" * 75)


load_source = data["loads"].copy()


load_columns = [
    "load_id",
    "customer_id",
    "route_id",
    "load_date",
    "load_type",
    "weight_lbs",
    "pieces",
    "revenue",
    "load_status",
    "booking_type"
]


# ------------------------------------------------------------
# Train only on non-FK attributes
# ------------------------------------------------------------

load_model_columns = [
    "load_date",
    "load_type",
    "weight_lbs",
    "pieces",
    "revenue",
    "load_status",
    "booking_type"
]


load_metadata = SingleTableMetadata()
load_metadata.detect_from_dataframe(data = load_source[load_model_columns])
load_metadata.validate()


load_synthesizer = GaussianCopulaSynthesizer(
    load_metadata
)

print("Training Loads synthesizer...")

load_synthesizer.fit(
    load_source[load_model_columns]
)


loads = load_synthesizer.sample(
    num_rows=TARGET_ROWS["loads"]
)


# ------------------------------------------------------------
# Generate deterministic Load IDs
# ------------------------------------------------------------

loads.insert(
    0,
    "load_id",
    create_ids(
        "LOAD",
        TARGET_ROWS["loads"]
    )
)


# ------------------------------------------------------------
# Learn customer/route relationship distribution
# from the real source data
# ------------------------------------------------------------

customer_route_pairs = (
    load_source[
        ["customer_id", "route_id"]
    ]
    .dropna()
    .value_counts(
        normalize=True
    )
)


pairs = customer_route_pairs.index.tolist()
probabilities = customer_route_pairs.values


selected_pairs = np.random.choice(
    len(pairs),
    size=TARGET_ROWS["loads"],
    p=probabilities
)


loads["customer_id"] = [
    pairs[i][0]
    for i in selected_pairs
]

loads["route_id"] = [
    pairs[i][1]
    for i in selected_pairs
]
synthetic["loads"] = loads
# ============================================================
# 12. REMAP FOREIGN KEYS TO SYNTHETIC DIMENSION IDs
# ============================================================

def remap_fk_values(
    generated_df,
    column,
    generated_parent_df,
    parent_pk,
    source_values
):

    unique_source_values = (
        pd.Series(source_values)
        .dropna()
        .drop_duplicates()
        .tolist()
    )

    generated_ids = generated_parent_df[
        parent_pk
    ].tolist()

    if len(unique_source_values) > len(generated_ids):
        raise ValueError(
            f"Not enough synthetic {parent_pk} values "
            f"to map {len(unique_source_values)} "
            f"source values."
        )

    mapping = dict(
        zip(
            unique_source_values,
            generated_ids
        )
    )

    generated_df[column] = (
        generated_df[column]
        .map(mapping)
    )

    return generated_df


loads = remap_fk_values(
    loads,
    "customer_id",
    synthetic["customers"],
    "customer_id",
    load_source["customer_id"]
)


loads = remap_fk_values(
    loads,
    "route_id",
    synthetic["routes"],
    "route_id",
    load_source["route_id"]
)
synthetic["loads"] = loads
# ============================================================
# 13. GENERATE TRIPS
# ============================================================

print("\n" + "=" * 75)
print("GENERATING TRIPS")
print("=" * 75)


trip_source = data["trips"].copy()


trip_model_columns = [
    "dispatch_date",
    "actual_distance_miles",
    "actual_duration_hours",
    "fuel_gallons_used",
    "idle_time_hours",
    "trip_status"
]


trip_metadata = SingleTableMetadata()
trip_metadata.detect_from_dataframe(data = trip_source[trip_model_columns])
trip_metadata.validate()


trip_synthesizer = GaussianCopulaSynthesizer(
    trip_metadata
)

print("Training Trips synthesizer...")

trip_synthesizer.fit(
    trip_source[trip_model_columns]
)


trips = trip_synthesizer.sample(
    num_rows=TARGET_ROWS["trips"]
)


# ------------------------------------------------------------
# IDs
# ------------------------------------------------------------

trips.insert(
    0,
    "trip_id",
    create_ids(
        "TRIP",
        TARGET_ROWS["trips"]
    )
)


# ------------------------------------------------------------
# One-to-one Load → Trip relationship
# ------------------------------------------------------------

trips["load_id"] = synthetic["loads"][
    "load_id"
].values


# ------------------------------------------------------------
# Learn driver/truck usage distributions
# ------------------------------------------------------------

driver_distribution = (
    trip_source["driver_id"]
    .dropna()
    .value_counts(normalize=True)
)

truck_distribution = (
    trip_source["truck_id"]
    .dropna()
    .value_counts(normalize=True)
)


trips["driver_source_id"] = np.random.choice(
    driver_distribution.index,
    size=len(trips),
    p=driver_distribution.values
)


trips["truck_source_id"] = np.random.choice(
    truck_distribution.index,
    size=len(trips),
    p=truck_distribution.values
)


# ------------------------------------------------------------
# Map source driver IDs → synthetic driver IDs
# ------------------------------------------------------------

trips = remap_fk_values(
    trips,
    "driver_source_id",
    synthetic["drivers"],
    "driver_id",
    trip_source["driver_id"]
)

trips.rename(
    columns={
        "driver_source_id": "driver_id"
    },
    inplace=True
)


# ------------------------------------------------------------
# Map source truck IDs → synthetic truck IDs
# ------------------------------------------------------------

trips = remap_fk_values(
    trips,
    "truck_source_id",
    synthetic["trucks"],
    "truck_id",
    trip_source["truck_id"]
)

trips.rename(
    columns={
        "truck_source_id": "truck_id"
    },
    inplace=True
)
# ------------------------------------------------------------
# Enforce business rule:
# idle_time_hours must not exceed actual_duration_hours
# ------------------------------------------------------------

trips["idle_time_hours"] = np.minimum(
    trips["idle_time_hours"],
    trips["actual_duration_hours"]
)

# Prevent negative values caused by synthetic generation
trips["idle_time_hours"] = trips["idle_time_hours"].clip(
    lower=0
)

trips["actual_duration_hours"] = trips[
    "actual_duration_hours"
].clip(
    lower=0.01
)

trips["actual_distance_miles"] = trips[
    "actual_distance_miles"
].clip(
    lower=0.01
)

trips["fuel_gallons_used"] = trips[
    "fuel_gallons_used"
].clip(
    lower=0.01
)
synthetic["trips"] = trips
# ============================================================
# 14. GENERATE DELIVERY EVENTS
# ============================================================

print("\n" + "=" * 75)
print("GENERATING DELIVERY EVENTS")
print("=" * 75)


event_source = data["delivery_events"].copy()


event_model_columns = [
    "event_type",
    "scheduled_datetime",
    "actual_datetime",
    "detention_minutes",
    "on_time_flag",
    "location_city",
    "location_state"
]


event_metadata = SingleTableMetadata()
event_metadata.detect_from_dataframe(data =event_source[event_model_columns])
event_metadata.validate()

event_synthesizer = GaussianCopulaSynthesizer(
    event_metadata
)

print("Training Delivery Events synthesizer...")

event_synthesizer.fit(
    event_source[event_model_columns]
)


events = event_synthesizer.sample(
    num_rows=TARGET_ROWS["delivery_events"]
)


events.insert(
    0,
    "event_id",
    create_ids(
        "EVT",
        TARGET_ROWS["delivery_events"]
    )
)
# ------------------------------------------------------------
# 5 events per load/trip
# ------------------------------------------------------------

load_ids = np.repeat(
    synthetic["loads"]["load_id"].values,
    5
)

trip_ids = np.repeat(
    synthetic["trips"]["trip_id"].values,
    5
)


events["load_id"] = load_ids
events["trip_id"] = trip_ids


# ------------------------------------------------------------
# Facility distribution from source
# ------------------------------------------------------------

facility_distribution = (
    event_source["facility_id"]
    .dropna()
    .value_counts(normalize=True)
)


events["facility_source_id"] = np.random.choice(
    facility_distribution.index,
    size=len(events),
    p=facility_distribution.values
)


events = remap_fk_values(
    events,
    "facility_source_id",
    synthetic["facilities"],
    "facility_id",
    event_source["facility_id"]
)


events.rename(
    columns={
        "facility_source_id": "facility_id"
    },
    inplace=True
)
# ------------------------------------------------------------
# Enforce business rule:
# actual_datetime must not be earlier than scheduled_datetime
# ------------------------------------------------------------

events["scheduled_datetime"] = pd.to_datetime(
    events["scheduled_datetime"],
    errors="coerce"
)

events["actual_datetime"] = pd.to_datetime(
    events["actual_datetime"],
    errors="coerce"
)

invalid_event_times = (
    events["actual_datetime"]
    < events["scheduled_datetime"]
)

events.loc[
    invalid_event_times,
    "actual_datetime"
] = events.loc[
    invalid_event_times,
    "scheduled_datetime"
]

# Recalculate on-time flag after enforcing timestamp rule
events["on_time_flag"] = (
    events["actual_datetime"]
    <= events["scheduled_datetime"]
)
synthetic["delivery_events"] = events
# ============================================================
# 15. FINAL COLUMN ORDER
# ============================================================

synthetic["customers"] = synthetic["customers"][
    [
        "customer_id",
        "customer_name",
        "customer_type",
        "account_status",
        "contract_start_date",
        "annual_revenue_potential"
    ]
]


synthetic["facilities"] = synthetic["facilities"][
    [
        "facility_id",
        "facility_name",
        "facility_type",
        "city",
        "state",
        "latitude",
        "longitude",
        "operating_hours"
    ]
]


synthetic["drivers"] = synthetic["drivers"][
    [
        "driver_id",
        "first_name",
        "last_name",
        "hire_date",
        "termination_date",
        "employment_status",
        "years_experience"
    ]
]


synthetic["trucks"] = synthetic["trucks"][
    [
        "truck_id",
        "unit_number",
        "make",
        "model_year",
        "acquisition_date",
        "acquisition_mileage",
        "fuel_type",
        "tank_capacity_gallons",
        "status"
    ]
]


synthetic["routes"] = synthetic["routes"][
    [
        "route_id",
        "origin_city",
        "origin_state",
        "destination_city",
        "destination_state",
        "typical_distance_miles",
        "base_rate_per_mile",
        "fuel_surcharge_rate",
        "typical_transit_days"
    ]
]


synthetic["loads"] = synthetic["loads"][
    [
        "load_id",
        "customer_id",
        "route_id",
        "load_date",
        "load_type",
        "weight_lbs",
        "pieces",
        "revenue",
        "load_status",
        "booking_type"
    ]
]


synthetic["trips"] = synthetic["trips"][
    [
        "trip_id",
        "load_id",
        "driver_id",
        "truck_id",
        "dispatch_date",
        "actual_distance_miles",
        "actual_duration_hours",
        "fuel_gallons_used",
        "idle_time_hours",
        "trip_status"
    ]
]


synthetic["delivery_events"] = synthetic["delivery_events"][
    [
        "event_id",
        "load_id",
        "trip_id",
        "event_type",
        "facility_id",
        "scheduled_datetime",
        "actual_datetime",
        "detention_minutes",
        "on_time_flag",
        "location_city",
        "location_state"
    ]
]

# ============================================================
# 16. FINAL SYNTHETIC DATA VALIDATION
# ============================================================

print("\n" + "=" * 75)
print("VALIDATING FINAL SYNTHETIC DATA")
print("=" * 75)


# ------------------------------------------------------------
# Row counts
# ------------------------------------------------------------

for table_name, expected_count in TARGET_ROWS.items():

    actual_count = len(
        synthetic[table_name]
    )

    print(
        f"{table_name:20s} "
        f"{actual_count:,} / "
        f"{expected_count:,}"
    )

    assert actual_count == expected_count, (
        f"{table_name}: expected "
        f"{expected_count}, got {actual_count}"
    )


# ------------------------------------------------------------
# Primary key uniqueness
# ------------------------------------------------------------

PRIMARY_KEYS = {
    "customers": "customer_id",
    "facilities": "facility_id",
    "drivers": "driver_id",
    "trucks": "truck_id",
    "routes": "route_id",
    "loads": "load_id",
    "trips": "trip_id",
    "delivery_events": "event_id"
}


for table_name, pk in PRIMARY_KEYS.items():

    df = synthetic[table_name]

    assert df[pk].notna().all(), (
        f"{table_name}.{pk} contains NULL values"
    )

    assert df[pk].is_unique, (
        f"{table_name}.{pk} contains duplicates"
    )


# ------------------------------------------------------------
# Foreign key validation helper
# ------------------------------------------------------------

def validate_fk(
    child_table,
    child_column,
    parent_table,
    parent_column
):

    child_values = set(
        synthetic[child_table][
            child_column
        ].dropna()
    )

    parent_values = set(
        synthetic[parent_table][
            parent_column
        ].dropna()
    )

    invalid = child_values - parent_values

    assert not invalid, (
        f"Invalid FK: "
        f"{child_table}.{child_column} "
        f"contains {len(invalid)} "
        f"values missing from "
        f"{parent_table}.{parent_column}"
    )

    print(
        f"PASS: "
        f"{child_table}.{child_column} "
        f"-> "
        f"{parent_table}.{parent_column}"
    )


# ------------------------------------------------------------
# Validate relationships
# ------------------------------------------------------------

validate_fk(
    "loads",
    "customer_id",
    "customers",
    "customer_id"
)

validate_fk(
    "loads",
    "route_id",
    "routes",
    "route_id"
)

validate_fk(
    "trips",
    "load_id",
    "loads",
    "load_id"
)

validate_fk(
    "trips",
    "driver_id",
    "drivers",
    "driver_id"
)

validate_fk(
    "trips",
    "truck_id",
    "trucks",
    "truck_id"
)

validate_fk(
    "delivery_events",
    "load_id",
    "loads",
    "load_id"
)

validate_fk(
    "delivery_events",
    "trip_id",
    "trips",
    "trip_id"
)

validate_fk(
    "delivery_events",
    "facility_id",
    "facilities",
    "facility_id"
)
# ============================================================
# BUSINESS RULE VALIDATION
# ============================================================

print("\n" + "=" * 75)
print("BUSINESS RULE VALIDATION")
print("=" * 75)


# ------------------------------------------------------------
# LOADS
# ------------------------------------------------------------

loads_df = synthetic["loads"]

invalid_weight_count = (
    (loads_df["weight_lbs"] <= 0)
    .sum()
)

invalid_revenue_count = (
    (loads_df["revenue"] <= 0)
    .sum()
)

assert invalid_weight_count == 0, (
    f"Loads contain {invalid_weight_count:,} "
    f"invalid weight values."
)

assert invalid_revenue_count == 0, (
    f"Loads contain {invalid_revenue_count:,} "
    f"invalid revenue values."
)

print("PASS: Loads business rules")


# ------------------------------------------------------------
# TRIPS
# ------------------------------------------------------------

trips_df = synthetic["trips"]

invalid_distance_count = (
    (trips_df["actual_distance_miles"] <= 0)
    .sum()
)

invalid_duration_count = (
    (trips_df["actual_duration_hours"] <= 0)
    .sum()
)

invalid_fuel_count = (
    (trips_df["fuel_gallons_used"] <= 0)
    .sum()
)

invalid_idle_count = (
    (trips_df["idle_time_hours"] < 0)
    .sum()
)

idle_exceeds_duration_count = (
    trips_df["idle_time_hours"]
    > trips_df["actual_duration_hours"]
).sum()


assert invalid_distance_count == 0, (
    f"Trips contain {invalid_distance_count:,} "
    f"invalid distance values."
)

assert invalid_duration_count == 0, (
    f"Trips contain {invalid_duration_count:,} "
    f"invalid duration values."
)

assert invalid_fuel_count == 0, (
    f"Trips contain {invalid_fuel_count:,} "
    f"invalid fuel consumption values."
)

assert invalid_idle_count == 0, (
    f"Trips contain {invalid_idle_count:,} "
    f"negative idle-time values."
)

assert idle_exceeds_duration_count == 0, (
    f"Trips contain {idle_exceeds_duration_count:,} "
    f"records where idle time exceeds trip duration."
)

print("PASS: Trips business rules")


# ------------------------------------------------------------
# DELIVERY EVENTS
# ------------------------------------------------------------

events_df = synthetic["delivery_events"].copy()

events_df["scheduled_datetime"] = pd.to_datetime(
    events_df["scheduled_datetime"],
    errors="coerce"
)

events_df["actual_datetime"] = pd.to_datetime(
    events_df["actual_datetime"],
    errors="coerce"
)


invalid_scheduled_datetime_count = (
    events_df["scheduled_datetime"]
    .isna()
    .sum()
)

invalid_actual_datetime_count = (
    events_df["actual_datetime"]
    .isna()
    .sum()
)

actual_before_scheduled_count = (
    events_df["actual_datetime"]
    < events_df["scheduled_datetime"]
).sum()


assert invalid_scheduled_datetime_count == 0, (
    f"Delivery events contain "
    f"{invalid_scheduled_datetime_count:,} "
    f"invalid scheduled datetime values."
)

assert invalid_actual_datetime_count == 0, (
    f"Delivery events contain "
    f"{invalid_actual_datetime_count:,} "
    f"invalid actual datetime values."
)

assert actual_before_scheduled_count == 0, (
    f"Delivery events contain "
    f"{actual_before_scheduled_count:,} records where "
    f"actual_datetime is earlier than scheduled_datetime."
)

print("PASS: Delivery event business rules")


# ------------------------------------------------------------
# FINAL VALIDATION SUCCESS
# ------------------------------------------------------------

print("\n" + "=" * 75)
print("FINAL SYNTHETIC DATA VALIDATION PASSED")
print("=" * 75)
# ============================================================
# 17. SAVE FINAL SYNTHETIC DATA
# ============================================================

print("\n" + "=" * 75)
print("SAVING SYNTHETIC DATA")
print("=" * 75)


for table_name, df in synthetic.items():

    output_file = os.path.join(
        SYNTHETIC_DIR,
        f"{table_name}_synthetic.csv"
    )

    df.to_csv(
        output_file,
        index=False
    )

    print(
        f"Saved: {output_file}"
    )
print("\nALL SYNTHETIC TABLES GENERATED SUCCESSFULLY.")