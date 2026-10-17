"""
Test 2: Child-Table Fan-Out Prevention (Grain Protection)
Reproduces the classic 1-to-Many join fan-out bug between `loads` (parent) and
`delivery_events` / `trips` (child tables), and verifies that pre-aggregating
child tables at the `load_id` grain prevents multiplied revenue and row explosion.
"""
import pandas as pd


def naive_join_with_fanout(loads_df: pd.DataFrame, events_df: pd.DataFrame) -> pd.DataFrame:
    """
    ANTI-PATTERN (What breaks in interviews):
    Joining parent `loads` (1 row per load_id) directly to child `delivery_events`
    (multiple events per load_id) before aggregating multiplies load revenue by
    the number of events per load.
    """
    joined = pd.merge(loads_df, events_df, on="load_id", how="left")
    return joined


def defensive_preaggregated_join(loads_df: pd.DataFrame, events_df: pd.DataFrame) -> pd.DataFrame:
    """
    SENIOR DE PATTERN (Pre-Aggregation Before Join):
    1. Aggregate child `delivery_events` to the parent `load_id` grain first.
    2. Join the 1-row-per-load summary back to `loads`.
    Preserves exact 1-row-per-load grain and prevents revenue multiplication.
    """
    events_preagg = (
        events_df.groupby("load_id", as_index=False)
        .agg(
            total_events=("event_id", "count"),
            total_detention_minutes=("detention_minutes", "sum"),
            on_time_events=("on_time_flag", lambda s: int(s.sum())),
        )
    )

    safe_joined = pd.merge(loads_df, events_preagg, on="load_id", how="left")
    safe_joined["total_events"] = safe_joined["total_events"].fillna(0).astype(int)
    safe_joined["total_detention_minutes"] = safe_joined["total_detention_minutes"].fillna(0.0)
    safe_joined["on_time_events"] = safe_joined["on_time_events"].fillna(0).astype(int)
    return safe_joined


def run_fanout_simulation():
    print("=" * 70)
    print("SWIFTROUTE TEST 2: CHILD-TABLE FAN-OUT PREVENTION")
    print("=" * 70)

    # 1. Parent Table: 50 Loads (1 row per load_id)
    loads_data = [
        {
            "load_id": f"LD{i:05d}",
            "customer_id": f"CUST{(i % 5) + 1:03d}",
            "revenue": 1000.00,
            "weight_lbs": 15000,
        }
        for i in range(1, 51)
    ]
    loads_df = pd.DataFrame(loads_data)
    expected_load_count = len(loads_df)
    expected_revenue = round(loads_df["revenue"].sum(), 2)

    # 2. Child Table: 5 delivery_events per load (Pickup, Depart, Hub, Arrive, Dropoff = 250 rows)
    events_data = []
    event_counter = 1
    for i in range(1, 51):
        for step in ["PICKUP", "DEPART", "HUB_SCAN", "ARRIVE", "DELIVERED"]:
            events_data.append(
                {
                    "event_id": f"EVT{event_counter:06d}",
                    "load_id": f"LD{i:05d}",
                    "event_type": step,
                    "detention_minutes": 15.0,
                    "on_time_flag": True,
                }
            )
            event_counter += 1
    events_df = pd.DataFrame(events_data)

    print(
        f"[*] Input State: {expected_load_count} parent loads (${expected_revenue:,.2f} true revenue) "
        f"| {len(events_df)} child delivery events (5 events/load)"
    )

    # 3. Reproduce the Naive Fan-Out Failure
    naive_df = naive_join_with_fanout(loads_df, events_df)
    naive_rows = len(naive_df)
    naive_revenue = round(naive_df["revenue"].sum(), 2)
    inflation_factor = naive_revenue / expected_revenue

    print(
        f"\n[!] NAIVE JOIN (BUG REPRODUCED): Rows exploded to {naive_rows} | "
        f"Summed Revenue: ${naive_revenue:,.2f} ({inflation_factor:.1f}x artificial inflation!)"
    )
    assert naive_revenue > expected_revenue, "Expected naive join to inflate revenue!"

    # 4. Run the Defensive Pre-Aggregated Fix
    safe_df = defensive_preaggregated_join(loads_df, events_df)
    safe_rows = len(safe_df)
    safe_revenue = round(safe_df["revenue"].sum(), 2)
    total_detention = round(safe_df["total_detention_minutes"].sum(), 2)

    print(
        f"[*] PRE-AGGREGATED JOIN (FIX VERIFIED): Rows preserved at {safe_rows} | "
        f"Summed Revenue: ${safe_revenue:,.2f} | Total Detention: {total_detention:,.0f} mins"
    )

    # Strict Engineering Assertions
    assert safe_rows == expected_load_count, (
        f"FAIL: Expected {expected_load_count} rows, got {safe_rows}!"
    )
    assert safe_revenue == expected_revenue, (
        f"FAIL: Revenue corrupted! Expected ${expected_revenue:,.2f}, got ${safe_revenue:,.2f}!"
    )
    assert total_detention == 3750.0, f"FAIL: Expected 3750 detention mins, got {total_detention}!"

    print("\n[PASS] Child-table fan-out prevented. Pre-aggregation preserved 1:1 load grain and exact revenue.")
    print("=" * 70)


if __name__ == "__main__":
    run_fanout_simulation()