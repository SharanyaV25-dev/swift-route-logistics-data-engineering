"""
Test 3 & Test 4: Order Corrections & Late-Arriving Event Restatements
Proves two production data engineering capabilities:
1. Test 3 (Status Correction): When a prior load changes from 'COMPLETED' to 'CANCELLED'
   in a later update, window deduplication (ROW_NUMBER() OVER (PARTITION BY load_id ORDER BY updated_at DESC))
   selects the latest state and restates recognized revenue to $0.00.
2. Test 4 (Late-Arriving Telemetry): When a delivery event or detention fee arrives 7 days
   after the original event_date, the pipeline distinguishes event time from processing time
   and accurately restates the historical business date mart.
"""
import pandas as pd


def apply_silver_latest_state_loads(raw_loads_df: pd.DataFrame) -> pd.DataFrame:
    """
    Simulates SwiftRoute's Silver Window deduplication and Cancelled Load Policy:
    - Sorts by `updated_at` descending per `load_id` and keeps row_number == 1.
    - Sets `revenue = 0.0` if the latest `load_status` is 'CANCELLED'.
    """
    sorted_df = raw_loads_df.sort_values(by=["load_id", "updated_at"], ascending=[True, False])
    latest_df = sorted_df.drop_duplicates(subset=["load_id"], keep="first").copy()

    # Apply Order Correction Policy: cancelled loads cannot recognize revenue
    latest_df.loc[latest_df["load_status"].str.upper() == "CANCELLED", "revenue"] = 0.0
    return latest_df


def apply_daily_operations_restatement(raw_events_df: pd.DataFrame) -> pd.DataFrame:
    """
    Simulates SwiftRoute's Silver -> Gold daily operations restatement:
    - Deduplicates `event_id` by latest `updated_at`.
    - Aggregates by `event_date` (Event Time), NOT `ingested_at` (Processing Time).
    """
    sorted_events = raw_events_df.sort_values(by=["event_id", "updated_at"], ascending=[True, False])
    deduped_events = sorted_events.drop_duplicates(subset=["event_id"], keep="first").copy()

    daily_mart = (
        deduped_events.groupby("event_date", as_index=False)
        .agg(
            total_events=("event_id", "count"),
            on_time_events=("on_time_flag", lambda s: int(s.sum())),
            avg_detention_minutes=("detention_minutes", "mean"),
        )
    )
    daily_mart["on_time_rate"] = (
        (daily_mart["on_time_events"] / daily_mart["total_events"]) * 100.0
    ).round(2)
    daily_mart["avg_detention_minutes"] = daily_mart["avg_detention_minutes"].round(2)
    return daily_mart


def run_corrections_and_late_events_simulation():
    print("=" * 72)
    print("SWIFTROUTE TEST 3 & 4: STATUS CORRECTIONS & LATE EVENT RESTATEMENTS")
    print("=" * 72)

    # =========================================================================
    # PART A: TEST 3 - PRIOR LOAD CHANGED FROM COMPLETED TO CANCELLED
    # =========================================================================
    print("\n--- [TEST 3] ORDER / LOAD STATUS CORRECTION (COMPLETED -> CANCELLED) ---")
    initial_loads = [
        {"load_id": "LD00001", "customer_id": "CUST001", "revenue": 1200.00, "load_status": "COMPLETED", "updated_at": "2026-10-01 09:00:00"},
        {"load_id": "LD00002", "customer_id": "CUST001", "revenue": 1500.00, "load_status": "COMPLETED", "updated_at": "2026-10-01 10:00:00"},
        {"load_id": "LD00003", "customer_id": "CUST002", "revenue": 1800.00, "load_status": "COMPLETED", "updated_at": "2026-10-01 11:00:00"},
        {"load_id": "LD00004", "customer_id": "CUST002", "revenue": 2000.00, "load_status": "COMPLETED", "updated_at": "2026-10-01 12:00:00"},
    ]
    df_day1 = pd.DataFrame(initial_loads)
    silver_day1 = apply_silver_latest_state_loads(df_day1)
    day1_rev = round(silver_day1["revenue"].sum(), 2)
    print(f"[*] Day 1 Published Revenue (4 Completed Loads): ${day1_rev:,.2f}")
    assert day1_rev == 6500.00, "Expected Day 1 revenue to be $6,500.00"

    # Next day: Carrier sends a status correction for LD00003 (CANCELLED) and a price adjustment for LD00002
    correction_batch = [
        {"load_id": "LD00003", "customer_id": "CUST002", "revenue": 1800.00, "load_status": "CANCELLED", "updated_at": "2026-10-02 08:30:00"},
        {"load_id": "LD00002", "customer_id": "CUST001", "revenue": 1650.00, "load_status": "COMPLETED", "updated_at": "2026-10-02 09:15:00"},
    ]
    df_combined = pd.concat([df_day1, pd.DataFrame(correction_batch)], ignore_index=True)
    silver_day2 = apply_silver_latest_state_loads(df_combined)
    day2_rev = round(silver_day2["revenue"].sum(), 2)
    ld3_status = silver_day2.loc[silver_day2["load_id"] == "LD00003", "load_status"].values[0]
    ld3_rev = silver_day2.loc[silver_day2["load_id"] == "LD00003", "revenue"].values[0]

    print(
        f"[!] Correction Ingested: LD00003 -> {ld3_status} (Recognized Rev: ${ld3_rev:,.2f}) "
        f"| LD00002 -> $1,650.00"
    )
    print(f"[*] Day 2 Restated Revenue (Expected $4,850.00): ${day2_rev:,.2f}")

    assert len(silver_day2) == 4, f"FAIL: Expected 4 unique loads, got {len(silver_day2)}!"
    assert ld3_status == "CANCELLED" and ld3_rev == 0.0, "FAIL: Cancelled load revenue was not zeroed!"
    assert day2_rev == 4850.00, f"FAIL: Expected $4,850.00, got ${day2_rev:,.2f}!"

    # =========================================================================
    # PART B: TEST 4 - LATE-ARRIVING EVENT (7 DAYS AFTER EVENT DATE)
    # =========================================================================
    print("\n--- [TEST 4] LATE-ARRIVING DELIVERY EVENT (7 DAYS AFTER BUSINESS DATE) ---")
    initial_events = [
        {"event_id": "EVT001", "load_id": "LD00001", "event_date": "2026-10-01", "detention_minutes": 20, "on_time_flag": True,  "updated_at": "2026-10-01 14:00:00", "ingested_at": "2026-10-01"},
        {"event_id": "EVT002", "load_id": "LD00002", "event_date": "2026-10-01", "detention_minutes": 10, "on_time_flag": True,  "updated_at": "2026-10-01 15:00:00", "ingested_at": "2026-10-01"},
    ]
    df_events_init = pd.DataFrame(initial_events)
    mart_before = apply_daily_operations_restatement(df_events_init)
    row_before = mart_before.iloc[0]
    print(
        f"[*] Initial Mart (2026-10-01): total_events={row_before['total_events']} | "
        f"on_time_rate={row_before['on_time_rate']}% | avg_detention={row_before['avg_detention_minutes']} mins"
    )

    # 7 days later (2026-10-08): A late facility scan from 2026-10-01 arrives with heavy detention (90 mins, late)
    late_events = [
        {"event_id": "EVT003", "load_id": "LD00004", "event_date": "2026-10-01", "detention_minutes": 90, "on_time_flag": False, "updated_at": "2026-10-08 10:00:00", "ingested_at": "2026-10-08"},
    ]
    df_events_after = pd.concat([df_events_init, pd.DataFrame(late_events)], ignore_index=True)
    mart_after = apply_daily_operations_restatement(df_events_after)
    row_after = mart_after.loc[mart_after["event_date"] == "2026-10-01"].iloc[0]

    print(
        f"[!] Late Event Arrived on 2026-10-08 for Business Date 2026-10-01 (EVT003: 90m detention, late)"
    )
    print(
        f"[*] Restated Mart (2026-10-01): total_events={row_after['total_events']} | "
        f"on_time_rate={row_after['on_time_rate']}% | avg_detention={row_after['avg_detention_minutes']} mins"
    )

    assert len(mart_after) == 1, "FAIL: Late event created a wrong processing-date partition instead of restating event_date!"
    assert row_after["total_events"] == 3, "FAIL: Restated event count should be 3!"
    assert row_after["avg_detention_minutes"] == 40.0, "FAIL: Restated average detention should be 40.0 mins!"
    assert row_after["on_time_rate"] == 66.67, "FAIL: Restated on-time rate should be 66.67%!"

    print("\n[PASS] Status corrections and 7-day late event restatements verified.")
    print("=" * 72)


if __name__ == "__main__":
    run_corrections_and_late_events_simulation()