from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ============================================================
# UTILITY FUNCTIONS
# ============================================================

def clean_id(c):
    """Replaces empty strings with proper NULLs."""
    return F.when(F.trim(F.col(c)) == '', None).otherwise(F.trim(F.col(c)))

def deduplicate_deterministic(df, pk_col):
    """Deterministically retains the first row ordered by all columns."""
    w = Window.partitionBy(pk_col).orderBy(*[F.col(c).asc_nulls_last() for c in df.columns])
    return df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")

def get_orphans(child_df, child_col, parent_df, parent_col):
    """Returns records from the child table that do not have a matching parent key."""
    valid_keys = parent_df.select(F.col(parent_col).alias('_valid_fk')).distinct()
    return child_df.join(valid_keys, child_df[child_col] == F.col('_valid_fk'), "left") \
                   .filter(F.col('_valid_fk').isNull()).drop('_valid_fk')

def keep_valid_fk(child_df, child_col, parent_df, parent_col):
    """Returns records from the child table that successfully match a parent key."""
    valid_keys = parent_df.select(F.col(parent_col).alias('_valid_fk')).distinct()
    return child_df.join(valid_keys, child_df[child_col] == F.col('_valid_fk'), "inner").drop('_valid_fk')

# ============================================================
# SILVER BUSINESS RULES (FACT TABLES)
# ============================================================

def get_invalid_loads(df):
    return df.filter(
        F.col("weight_lbs").isNull() | (F.col("weight_lbs") <= 0) |
        F.col("revenue").isNull() | (F.col("revenue") < 0)
    )

def get_valid_loads(df):
    return df.filter(
        F.col("weight_lbs").isNotNull() & (F.col("weight_lbs") > 0) &
        F.col("revenue").isNotNull() & (F.col("revenue") >= 0)
    )

def get_invalid_trips(df):
    return df.filter(
        F.col("actual_duration_hours").isNull() |
        F.col("idle_time_hours").isNull() |
        (F.col("actual_duration_hours") < 0) |
        (F.col("idle_time_hours") < 0) |
        (F.col("idle_time_hours") > F.col("actual_duration_hours"))
    )

def get_valid_trips(df):
    return df.filter(
        F.col("actual_duration_hours").isNotNull() &
        F.col("idle_time_hours").isNotNull() &
        (F.col("actual_duration_hours") >= 0) &
        (F.col("idle_time_hours") >= 0) &
        (F.col("idle_time_hours") <= F.col("actual_duration_hours"))
    )

def get_invalid_events(df):
    return df.filter(
        F.col("scheduled_datetime").isNull() |
        F.col("actual_datetime").isNull() |
        (F.col("actual_datetime") < F.col("scheduled_datetime"))
    )

def get_valid_events(df):
    return df.filter(
        F.col("scheduled_datetime").isNotNull() &
        F.col("actual_datetime").isNotNull() &
        (F.col("actual_datetime") >= F.col("scheduled_datetime"))
    )