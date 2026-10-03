import sys
from pathlib import Path

# Ensure Python can find your src folder
sys.path.append(str(Path(__file__).resolve().parent.parent / "src"))

from rules import deduplicate_deterministic, get_invalid_loads

def test_deduplicate_deterministic(spark):
    """Test that duplicates are removed deterministically."""
    data = [
        ("L1", 100.0), # Duplicate 1
        ("L1", 200.0), # Duplicate 2
        ("L2", 300.0)  # Unique
    ]
    df = spark.createDataFrame(data, ["load_id", "revenue"])
    
    deduped_df = deduplicate_deterministic(df, "load_id")
    
    # Should only keep 2 rows total (one L1, one L2)
    assert deduped_df.count() == 2

def test_get_invalid_loads(spark):
    """Test that negative weights and revenues are correctly flagged."""
    data = [
        ("L1", 5000.0, 1500.0),  # Valid
        ("L2", -10.0, 1500.0),   # Invalid weight
        ("L3", 5000.0, -50.0),   # Invalid revenue
        ("L4", None, 1500.0)     # Null weight
    ]
    df = spark.createDataFrame(data, ["load_id", "weight_lbs", "revenue"])
    
    invalid_df = get_invalid_loads(df)
    
    # Should flag exactly 3 rows (L2, L3, L4)
    assert invalid_df.count() == 3
    
    # Verify exactly which IDs were flagged
    flagged_ids = {row.load_id for row in invalid_df.collect()}
    assert flagged_ids == {"L2", "L3", "L4"}