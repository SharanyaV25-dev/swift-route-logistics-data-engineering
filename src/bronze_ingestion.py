from pathlib import Path
from datetime import datetime, timezone
import shutil
import json
import csv
import re


# ============================================================
# CONFIGURATION
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Folder containing the intentionally messy CSV files
SOURCE_DIR = PROJECT_ROOT / "data" / "raw" / "messy"

# Local Bronze layer
BRONZE_DIR = PROJECT_ROOT / "data" / "bronze"

# Metadata/manifest
METADATA_DIR = PROJECT_ROOT / "docs" / "metadata"


EXPECTED_TABLES = {
    "customers",
    "facilities",
    "routes",
    "drivers",
    "trucks",
    "loads",
    "trips",
    "delivery_events",
}
# ============================================================
# LOGGING
# ============================================================
def log(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")

# ============================================================
# VALIDATE SOURCE DIRECTORY
# ============================================================
def validate_source_directory():
    if not SOURCE_DIR.exists():
        raise FileNotFoundError(
            f"Source directory does not exist:\n{SOURCE_DIR}"
        )
    csv_files = list(SOURCE_DIR.glob("*.csv"))
    if not csv_files:
        raise FileNotFoundError(
            f"No CSV files found in:\n{SOURCE_DIR}"
        )
    return csv_files

# ============================================================
# IDENTIFY TABLE NAME
# ============================================================
def get_table_name(file_path):
    table_name = file_path.stem.lower().strip()
    return re.sub(r'\W+', '_', table_name)

# ============================================================
# VALIDATE EXPECTED TABLES
# ============================================================

def validate_expected_tables(csv_files):

    actual_tables = {
        get_table_name(file)
        for file in csv_files
    }
    missing_tables = EXPECTED_TABLES - actual_tables
    if missing_tables:
        raise ValueError(
            f"Missing expected tables: {sorted(missing_tables)}"
        )
    unexpected_tables = actual_tables - EXPECTED_TABLES
    if unexpected_tables:
        log(
            f"WARNING: Unexpected CSV files found: "
            f"{sorted(unexpected_tables)}"
        )
# ============================================================
# COUNT CSV RECORDS
# ============================================================
def get_row_count(file_path):
    with open(
        file_path,
        mode="r",
        encoding="utf-8-sig",
        newline=""
    ) as file:

        reader = csv.reader(file)

        # Header
        next(reader, None)

        return sum(1 for _ in reader)


# ============================================================
# GET COLUMN INFORMATION
# ============================================================

def get_columns(file_path):

    with open(
        file_path,
        mode="r",
        encoding="utf-8-sig",
        newline=""
    ) as file:

        reader = csv.reader(file)

        header = next(reader, [])

    return header


# ============================================================
# COPY FILE TO BRONZE
# ============================================================

def ingest_to_bronze(source_file):

    table_name = get_table_name(source_file)

    table_bronze_dir = BRONZE_DIR / table_name

    table_bronze_dir.mkdir(
        parents=True,
        exist_ok=True
    )

    destination_file = table_bronze_dir / source_file.name

    # Preserve the original file exactly
    shutil.copy2(
        source_file,
        destination_file
    )

    return destination_file


# ============================================================
# CREATE INGESTION METADATA
# ============================================================

def create_metadata(source_file, bronze_file):

    table_name = get_table_name(source_file)

    metadata = {
        "table_name": table_name,
        "source_file": source_file.name,
        "bronze_file": str(
            bronze_file.relative_to(PROJECT_ROOT)
        ),
        "ingestion_timestamp_utc": datetime.now(
            timezone.utc
        ).isoformat(),

        "source_file_size_bytes": source_file.stat().st_size,

        "row_count": get_row_count(source_file),

        "column_count": len(
            get_columns(source_file)
        ),

        "columns": get_columns(source_file),

        "layer": "bronze",

        "data_preserved_as_source": True
    }

    return metadata


# ============================================================
# SAVE METADATA
# ============================================================

def save_metadata(metadata):

    METADATA_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    table_name = metadata["table_name"]

    metadata_file = (
        METADATA_DIR /
        f"{table_name}_bronze_metadata.json"
    )

    with open(
        metadata_file,
        mode="w",
        encoding="utf-8"
    ) as file:

        json.dump(
            metadata,
            file,
            indent=4
        )


# ============================================================
# MAIN INGESTION PIPELINE
# ============================================================

def run_bronze_ingestion():

    log("=" * 60)
    log("SwiftRoute Logistics - Bronze Ingestion")
    log("=" * 60)

    log(f"Source directory : {SOURCE_DIR}")
    log(f"Bronze directory : {BRONZE_DIR}")

    # Step 1
    csv_files = validate_source_directory()

    log(
        f"Found {len(csv_files)} CSV files."
    )

    # Step 2
    validate_expected_tables(csv_files)

    log("Expected table validation PASSED.")

    successful_tables = 0

    # Step 3
    for source_file in sorted(csv_files):

        table_name = get_table_name(source_file)

        log("")
        log(f"Processing: {table_name}")

        try:

            # Copy raw file unchanged
            bronze_file = ingest_to_bronze(
                source_file
            )

            # Create metadata
            metadata = create_metadata(
                source_file,
                bronze_file
            )

            # Save metadata
            save_metadata(metadata)

            log(
                f"SUCCESS: {table_name}"
            )

            log(
                f"Rows: {metadata['row_count']}"
            )

            log(
                f"Columns: {metadata['column_count']}"
            )

            successful_tables += 1

        except Exception as error:

            log(
                f"FAILED: {table_name}"
            )

            log(
                f"Reason: {error}"
            )

    # Step 4
    log("")
    log("=" * 60)
    log("BRONZE INGESTION SUMMARY")
    log("=" * 60)

    log(
        f"Successful tables : "
        f"{successful_tables}/{len(csv_files)}"
    )

    if successful_tables == len(csv_files):

        log(
            "STATUS: BRONZE INGESTION PASSED"
        )

    else:

        log(
            "STATUS: BRONZE INGESTION COMPLETED "
            "WITH ERRORS"
        )


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    run_bronze_ingestion()