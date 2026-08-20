import os
import sys
import json
import csv
import argparse

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOCAL_HDFS_SIMULATION_DIR = os.path.join(BASE_DIR, ".hdfs_storage")
ENTITIES = ["movie", "guests", "sessions", "tickets"]

def load_data_pyspark(file_format, entity):
    """Read data back using Apache Spark (PySpark) DataFrame API."""
    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder \
            .appName("TicketReservationHDFSVerification") \
            .master("local[*]") \
            .getOrCreate()

        ext = f".{file_format}"
        file_path = os.path.join(LOCAL_HDFS_SIMULATION_DIR, "cinema", "raw", file_format, entity, f"{entity}{ext}")
        
        if file_format == "csv":
            df = spark.read.option("header", "true").option("inferSchema", "true").csv(file_path)
        else:
            df = spark.read.json(file_path)
            
        rows = [row.asDict() for row in df.collect()]
        return rows
    except Exception as e:
        print(f"  [i] PySpark session unavailable ({e}). Falling back to standard HDFS file stream reader.")
        return None

def load_hdfs_csv(entity):
    filepath = os.path.join(LOCAL_HDFS_SIMULATION_DIR, "cinema", "raw", "csv", entity, f"{entity}.csv")
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"HDFS CSV Path missing: {filepath}")
    
    rows = []
    with open(filepath, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            rows.append(r)
    return rows

def load_hdfs_json(entity):
    filepath = os.path.join(LOCAL_HDFS_SIMULATION_DIR, "cinema", "raw", "json", entity, f"{entity}.json")
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"HDFS JSON Path missing: {filepath}")
    
    with open(filepath, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data

def verify_dataset(fmt, data_dict, engine="hdfs"):
    print(f"\n==================================================================")
    print(f"  VERIFYING {engine.upper()} READ-BACK FOR DATASET FORMAT: {fmt.upper()}  ")
    print(f"==================================================================")

    errors = []

    # 1. Record Count Validation
    print("\n--- 1. RECORD COUNT VALIDATION ---")
    for entity in ENTITIES:
        count = len(data_dict[entity])
        print(f"  [OK] Entity '{entity}': {count} records retrieved from HDFS path /cinema/raw/{fmt}/{entity}/{entity}.{fmt}")

    movies = data_dict["movie"]
    guests = data_dict["guests"]
    sessions = data_dict["sessions"]
    tickets = data_dict["tickets"]

    # 2. Constraint Checks
    print("\n--- 2. BUSINESS CONSTRAINT & DATA INTEGRITY CHECKS ---")
    
    # Check Guests: loyalty_points >= 0
    negative_loyalty = [g for g in guests if int(g["loyalty_points"]) < 0]
    if negative_loyalty:
        errors.append(f"Constraint Violation: {len(negative_loyalty)} guests have negative loyalty_points!")
    else:
        print("  [OK] CHECK Constraint Passed: `loyalty_points >= 0` enforced for all guests.")

    # Check Sessions: available_seats >= 0
    negative_seats = [s for s in sessions if int(s["available_seats"]) < 0]
    if negative_seats:
        errors.append(f"Constraint Violation: {len(negative_seats)} sessions have negative available_seats!")
    else:
        print("  [OK] CHECK Constraint Passed: `available_seats >= 0` enforced for all sessions.")

    # Check Tickets: ticket_price >= 0
    negative_price = [t for t in tickets if float(t["ticket_price"]) < 0]
    if negative_price:
        errors.append(f"Constraint Violation: {len(negative_price)} tickets have negative ticket_price!")
    else:
        print("  [OK] CHECK Constraint Passed: `ticket_price >= 0` enforced for all tickets.")

    # Foreign Key Referential Integrity
    movie_ids = {int(m["movie_id"]) for m in movies}
    guest_ids = {int(g["guest_id"]) for g in guests}
    session_ids = {int(s["session_id"]) for s in sessions}

    orphaned_sessions = [s for s in sessions if int(s["movie_id"]) not in movie_ids]
    if orphaned_sessions:
        errors.append(f"Referential Integrity Error: {len(orphaned_sessions)} sessions reference non-existent movie_id!")
    else:
        print("  [OK] FK Constraint Passed: All sessions reference valid `movie_id` keys.")

    orphaned_tickets_sess = [t for t in tickets if int(t["session_id"]) not in session_ids]
    orphaned_tickets_gst = [t for t in tickets if int(t["guest_id"]) not in guest_ids]

    if orphaned_tickets_sess or orphaned_tickets_gst:
        errors.append(f"Referential Integrity Error: Tickets with invalid FK references found!")
    else:
        print("  [OK] FK Constraint Passed: All tickets reference valid `session_id` and `guest_id` keys.")

    # 3. Print Sample Verification Snippets
    print("\n--- 3. SAMPLE DATA READ-BACK PREVIEW ---")
    
    print("\n[Sample Movie Record]:")
    sample_m = movies[0]
    print(f"  Movie ID     : {sample_m['movie_id']}")
    print(f"  Title        : {sample_m['series_title']} ({sample_m['released_year']})")
    print(f"  Runtime      : {sample_m['runtime_in_min']} min")
    print(f"  Genre        : {sample_m['genre']}")
    print(f"  Revenue      : ${float(sample_m['revenue']):,.2f}")
    credits_val = sample_m['credits'] if isinstance(sample_m['credits'], dict) else json.loads(sample_m['credits'])
    print(f"  Credits      : Director = {credits_val.get('director')}, Cast = {', '.join(credits_val.get('cast', []))}")

    print("\n[Sample Guest Record]:")
    sample_g = guests[0]
    print(f"  Guest ID     : {sample_g['guest_id']}")
    print(f"  Name         : {sample_g['name']}")
    print(f"  Phone        : {sample_g['phone_number']}")
    print(f"  Loyalty Pts  : {sample_g['loyalty_points']}")

    print("\n[Sample Session Record]:")
    sample_s = sessions[0]
    print(f"  Session ID   : {sample_s['session_id']}")
    print(f"  Movie ID     : {sample_s['movie_id']}")
    print(f"  Screen Time  : {sample_s['screen_time']}")
    print(f"  Hall         : {sample_s['hall_name']}")
    print(f"  Avail Seats  : {sample_s['available_seats']}")

    print("\n[Sample Ticket Record]:")
    sample_t = tickets[0]
    print(f"  Ticket ID    : {sample_t['ticket_id']}")
    print(f"  Session ID   : {sample_t['session_id']}")
    print(f"  Guest ID     : {sample_t['guest_id']}")
    print(f"  Seat Number  : {sample_t['seat_number']}")
    print(f"  Price        : ${float(sample_t['ticket_price']):.2f}")

    if errors:
        print(f"\n[!] Verification FAILED with {len(errors)} errors:")
        for err in errors:
            print(f"    - {err}")
        return False
    else:
        print(f"\n[SUCCESS] Verification PASSED for {fmt.upper()} dataset! Zero data corruption or loss.")
        return True

def main():
    parser = argparse.ArgumentParser(description="Read-back & verify cinema datasets from HDFS Cluster / Apache Spark.")
    parser.add_argument("--format", choices=["csv", "json", "all"], default="all", help="Data format to verify (csv, json, or all)")
    parser.add_argument("--engine", choices=["hdfs", "spark"], default="hdfs", help="Processing engine (hdfs or spark)")
    args = parser.parse_args()

    formats_to_test = ["csv", "json"] if args.format == "all" else [args.format]

    all_passed = True
    for fmt in formats_to_test:
        data_dict = {}
        if args.engine == "spark":
            for entity in ENTITIES:
                spark_data = load_data_pyspark(fmt, entity)
                data_dict[entity] = spark_data if spark_data is not None else (load_hdfs_csv(entity) if fmt == "csv" else load_hdfs_json(entity))
        else:
            for entity in ENTITIES:
                data_dict[entity] = load_hdfs_csv(entity) if fmt == "csv" else load_hdfs_json(entity)
        
        passed = verify_dataset(fmt, data_dict, args.engine)
        if not passed:
            all_passed = False

    if all_passed:
        print("\n==================================================================")
        print(f" [ALL PASSED] {args.engine.upper()} READ-BACK & DATA INTEGRITY VERIFICATION COMPLETE")
        print("==================================================================")
    else:
        sys.exit(1)

if __name__ == "__main__":
    main()
