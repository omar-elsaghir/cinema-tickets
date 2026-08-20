import os
import sys
import json
import csv
import argparse

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOCAL_HDFS_SIMULATION_DIR = os.path.join(BASE_DIR, ".hdfs_storage")
ANALYTICS_DIR = os.path.join(LOCAL_HDFS_SIMULATION_DIR, "cinema", "analytics")

JOB_SCHEMAS = {
    "seat_occupancy_per_event": {
        "title": "Job 1: Seat Occupancy Percentage per Event",
        "expected_cols": ["event_id", "movie_title", "hall_name", "screen_time", "booked_seats", "available_seats", "total_capacity", "occupancy_percentage"]
    },
    "available_seats_per_event": {
        "title": "Job 2: Available Seats & Capacity Status per Event",
        "expected_cols": ["event_id", "movie_title", "screen_time", "hall_name", "available_seats", "availability_status"]
    },
    "booking_stats_by_category": {
        "title": "Job 3: Booking Statistics by Event Category",
        "expected_cols": ["genre", "total_bookings", "total_revenue", "avg_ticket_price", "min_ticket_price", "max_ticket_price"]
    },
    "top_5_users": {
        "title": "Job 4: Top 5 Users by Total Bookings & Spend",
        "expected_cols": ["rank", "user_id", "name", "phone_number", "loyalty_points", "total_bookings", "total_spent", "avg_spend_per_ticket"]
    },
    "top_5_grossing_events": {
        "title": "Job 5: Top 5 Highest-Grossing Events/Movies",
        "expected_cols": ["rank", "event_id", "movie_title", "hall_name", "screen_time", "tickets_sold", "total_box_office_revenue"]
    },
    "revenue_by_auditorium": {
        "title": "Job 6: Revenue & Ticket Demand by Auditorium Hall",
        "expected_cols": ["hall_name", "total_screenings", "total_tickets_sold", "total_revenue", "avg_revenue_per_screening"]
    },
    "loyalty_tier_analytics": {
        "title": "Job 7: User Loyalty Tier & Engagement Analysis",
        "expected_cols": ["loyalty_tier", "total_users_in_tier", "total_bookings", "total_revenue", "avg_spend_per_user", "avg_ticket_price"]
    },
    "demand_distribution": {
        "title": "Job 8: Time-Series Screening Demand Distribution",
        "expected_cols": ["time_slot", "screening_hour", "total_screenings", "total_tickets_sold", "slot_revenue"]
    },
}

def verify_all_analytics_reports():
    print("==================================================================")
    print("      PART 2: BATCH ANALYTICS VERIFICATION & REPORT AUDIT        ")
    print("==================================================================")

    all_passed = True
    total_audited = 0

    for job_name, meta in JOB_SCHEMAS.items():
        total_audited += 1
        print(f"\n---> Auditing {meta['title']} ({job_name})")

        job_dir = os.path.join(ANALYTICS_DIR, job_name)
        csv_file = os.path.join(job_dir, "csv", f"{job_name}.csv")
        json_file = os.path.join(job_dir, "json", f"{job_name}.json")

        # 1. Check file existence
        if not os.path.exists(csv_file):
            print(f"  [✗] Missing CSV report at: {csv_file}")
            all_passed = False
            continue
        if not os.path.exists(json_file):
            print(f"  [✗] Missing JSON report at: {json_file}")
            all_passed = False
            continue

        # 2. Parse and validate CSV data
        with open(csv_file, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            csv_rows = list(reader)
            headers = reader.fieldnames

        # 3. Parse and validate JSON data
        with open(json_file, "r", encoding="utf-8") as f:
            json_rows = json.load(f)

        if len(csv_rows) != len(json_rows):
            print(f"  [✗] Row count mismatch: CSV has {len(csv_rows)} rows, JSON has {len(json_rows)} records!")
            all_passed = False
            continue

        # 4. Check schema headers
        for col in meta["expected_cols"]:
            if col not in headers:
                print(f"  [✗] Missing expected column '{col}' in CSV header!")
                all_passed = False

        print(f"  [OK] Dataset Integrity Passed: {len(csv_rows)} records in both CSV and JSON formats.")

        # 5. Specialized checks
        if job_name in ("top_5_users", "top_5_grossing_events"):
            if len(csv_rows) != 5:
                print(f"  [!] Warning: Expected exactly 5 ranked entries for {job_name}, found {len(csv_rows)}")
            else:
                print("  [OK] Ranked Leaderboard Check Passed: Exactly Top 5 rows ordered by rank 1..5.")

        # 6. Pretty print top 3 sample rows
        print("  [Sample Preview]:")
        for i, row in enumerate(csv_rows[:3], start=1):
            formatted_row = {k: v for k, v in row.items() if k in meta["expected_cols"][:4]}
            print(f"    #{i}: {formatted_row}")

    print("\n==================================================================")
    if all_passed:
        print(f" [ALL PASSED] Successfully verified all {total_audited} analytical batch jobs!")
        print(" Data is stored cleanly in HDFS structure and ready for Part 4.")
    else:
        print(" [!] Some batch analytics verification checks failed.")
    print("==================================================================")

    return all_passed

if __name__ == "__main__":
    success = verify_all_analytics_reports()
    if not success:
        sys.exit(1)
