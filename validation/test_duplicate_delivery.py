"""
Test 1: Duplicate Delivery & Pipeline Idempotency
Verifies that re-running the same batch does not corrupt table grain or revenue metrics
across SwiftRoute's loads and customer performance marts.
"""
import pandas as pd


def transform_to_gold(staged_loads_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Simulates SwiftRoute's idempotent Gold transformation logic:
    1. Enforces primary-key deduplication on `load_id` (simulating MERGE INTO / dropDuplicates).
    2. Computes `gold_load_performance` (1 row per load_id) and `gold_customer_performance`
       (1 row per customer_id).
    """
    # Defensive grain gate: deduplicate by primary key `load_id` keep latest
    deduped_loads = staged_loads_df.drop_duplicates(subset=["load_id"], keep="last").copy()

    # Gold Table 1: gold_load_performance
    gold_loads = deduped_loads[
        ["load_id", "customer_id", "route_id", "weight_lbs", "pieces", "revenue", "load_status"]
    ].copy()

    # Gold Table 2: gold_customer_performance
    gold_customers = (
        deduped_loads.groupby("customer_id", as_index=False)
        .agg(
            total_loads=("load_id", "count"),
            total_revenue=("revenue", "sum"),
            avg_load_weight_lbs=("weight_lbs", "mean"),
            total_pieces=("pieces", "sum"),
        )
    )
    gold_customers["avg_revenue_per_load"] = (
        gold_customers["total_revenue"] / gold_customers["total_loads"]
    ).round(2)

    return gold_loads, gold_customers


def run_idempotency_simulation():
    print("=" * 68)
    print("SWIFTROUTE TEST 1: DUPLICATE INGRESS & IDEMPOTENCY SIMULATION")
    print("=" * 68)

    # 1. Create a realistic sample batch of 100 SwiftRoute loads across 10 customers
    sample_batch = [
        {
            "load_id": f"LD{i:05d}",
            "customer_id": f"CUST{(i % 10) + 1:03d}",
            "route_id": f"RT{(i % 5) + 1:03d}",
            "weight_lbs": 12000 + (i * 50),
            "pieces": 20 + (i % 15),
            "revenue": 250.0 + (i * 5.0),
            "load_status": "DELIVERED",
        }
        for i in range(1, 101)
    ]
    df_batch_a = pd.DataFrame(sample_batch)
    expected_rows = len(df_batch_a)
    expected_revenue = round(df_batch_a["revenue"].sum(), 2)

    print(f"[*] Batch A Prepared: {expected_rows} loads | Control Total Revenue: ${expected_revenue:,.2f}")

    # Run 1: Normal Initial Ingestion
    gold_loads_run1, gold_cust_run1 = transform_to_gold(df_batch_a)
    run1_count = len(gold_loads_run1)
    run1_rev = round(gold_loads_run1["revenue"].sum(), 2)
    run1_cust_rev = round(gold_cust_run1["total_revenue"].sum(), 2)

    print(
        f"[Run 1 Complete] gold_load_performance rows: {run1_count} | "
        f"Load Revenue: ${run1_rev:,.2f} | Customer Mart Revenue: ${run1_cust_rev:,.2f}"
    )

    assert run1_count == expected_rows, "Run 1 row count mismatch!"
    assert run1_rev == expected_revenue, "Run 1 revenue mismatch!"

    # Run 2: Simulate Duplicate Delivery (Upstream retries & sends Batch A twice)
    print("\n[!] Upstream system re-delivers identical Batch A (200 raw rows arriving)...")
    duplicated_ingress = pd.concat([df_batch_a, df_batch_a], ignore_index=True)
    print(f"[*] Raw Staged Input Rows Before Idempotency Gate: {len(duplicated_ingress)}")

    gold_loads_run2, gold_cust_run2 = transform_to_gold(duplicated_ingress)
    run2_count = len(gold_loads_run2)
    run2_rev = round(gold_loads_run2["revenue"].sum(), 2)
    run2_cust_rev = round(gold_cust_run2["total_revenue"].sum(), 2)

    print(
        f"[Run 2 Complete] gold_load_performance rows: {run2_count} | "
        f"Load Revenue: ${run2_rev:,.2f} | Customer Mart Revenue: ${run2_cust_rev:,.2f}"
    )

    # Strict Engineering Assertions
    assert run2_count == run1_count, f"FAIL: Row count inflated from {run1_count} to {run2_count}!"
    assert run2_rev == run1_rev, f"FAIL: Load revenue doubled from ${run1_rev} to ${run2_rev}!"
    assert run2_cust_rev == run1_cust_rev, (
        f"FAIL: Customer mart revenue doubled from ${run1_cust_rev} to ${run2_cust_rev}!"
    )

    print("\n[PASS] Zero duplicate records generated. Revenue invariance confirmed across all Gold marts.")
    print("=" * 68)


if __name__ == "__main__":
    run_idempotency_simulation()