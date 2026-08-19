import os
import sys
import json
import csv
from datetime import datetime
from collections import defaultdict

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
STAGING_DIR = os.path.join(BASE_DIR, "data", "staging")
CSV_DIR = os.path.join(BASE_DIR, "data", "raw", "csv")
JSON_DIR = os.path.join(BASE_DIR, "data", "raw", "json")
LOCAL_HDFS_SIMULATION_DIR = os.path.join(BASE_DIR, ".hdfs_storage")

ANALYTICS_HDFS_BASE = "/cinema/analytics"

def get_spark_session():
    """Create or retrieve active PySpark session with 10-core parallel configuration."""
    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder \
            .appName("DistributedTicketAnalytics") \
            .master("local[*]") \
            .config("spark.sql.shuffle.partitions", "10") \
            .config("spark.default.parallelism", "10") \
            .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        return spark
    except Exception as e:
        print(f"  [i] Note: PySpark initialization note ({e}). Falling back to multi-core batch engine.")
        return None

def load_dataset(entity_name):
    """Load dataset from staging or raw directories, preferring CSV."""
    candidates = [
        os.path.join(STAGING_DIR, f"{entity_name}.csv"),
        os.path.join(CSV_DIR, f"{entity_name}.csv"),
        os.path.join(LOCAL_HDFS_SIMULATION_DIR, "cinema", "csv", entity_name, f"{entity_name}.csv"),
        os.path.join(LOCAL_HDFS_SIMULATION_DIR, "cinema", "raw", "csv", entity_name, f"{entity_name}.csv"),
    ]
    for path in candidates:
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                return list(reader)
    
    json_candidates = [
        os.path.join(STAGING_DIR, f"{entity_name}.json"),
        os.path.join(JSON_DIR, f"{entity_name}.json"),
        os.path.join(LOCAL_HDFS_SIMULATION_DIR, "cinema", "json", entity_name, f"{entity_name}.json"),
    ]
    for path in json_candidates:
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
                
    raise FileNotFoundError(f"Could not locate dataset for entity '{entity_name}' in any storage paths.")

def save_analytics_output(job_name, fieldnames, records):
    """Save batch job output to HDFS storage structure in both CSV and JSON formats (idempotent)."""
    out_dir_csv = os.path.join(LOCAL_HDFS_SIMULATION_DIR, "cinema", "analytics", job_name, "csv")
    out_dir_json = os.path.join(LOCAL_HDFS_SIMULATION_DIR, "cinema", "analytics", job_name, "json")
    os.makedirs(out_dir_csv, exist_ok=True)
    os.makedirs(out_dir_json, exist_ok=True)

    csv_file = os.path.join(out_dir_csv, f"{job_name}.csv")
    json_file = os.path.join(out_dir_json, f"{job_name}.json")

    # Overwrite mode for idempotency
    with open(csv_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)

    with open(json_file, "w", encoding="utf-8") as f:
        json.dump(records, f, indent=2)

    return {
        "csv_path": csv_file,
        "json_path": json_file,
        "hdfs_path": f"{ANALYTICS_HDFS_BASE}/{job_name}",
        "count": len(records)
    }

# ==============================================================================
# BATCH JOB 1: Seat Occupancy Percentage per Event (Multi-dataset: events + seats)
# ==============================================================================
def job_1_seat_occupancy_per_event(use_spark=False):
    """Job 1: Calculates seat occupancy percentage and capacity utilization for each event."""
    if use_spark:
        spark = get_spark_session()
        if spark:
            try:
                events_p = os.path.join(STAGING_DIR, "events.csv")
                seats_p = os.path.join(STAGING_DIR, "seats.csv")
                events_df = spark.read.option("header", "true").option("inferSchema", "true").csv(events_p)
                seats_df = spark.read.option("header", "true").option("inferSchema", "true").csv(seats_p)

                events_df.createOrReplaceTempView("events")
                seats_df.createOrReplaceTempView("seats")

                sql = """
                SELECT 
                    e.event_id,
                    e.movie_title,
                    e.hall_name,
                    e.screen_time,
                    COUNT(s.seat_id) AS booked_seats,
                    e.available_seats,
                    (COUNT(s.seat_id) + e.available_seats) AS total_capacity,
                    ROUND(CAST(COUNT(s.seat_id) AS DOUBLE) / (COUNT(s.seat_id) + e.available_seats) * 100, 2) AS occupancy_percentage
                FROM events e
                LEFT JOIN seats s ON e.event_id = s.event_id
                GROUP BY e.event_id, e.movie_title, e.hall_name, e.screen_time, e.available_seats
                ORDER BY e.event_id
                """
                df = spark.sql(sql)
                records = [r.asDict() for r in df.collect()]
                fields = ["event_id", "movie_title", "hall_name", "screen_time", "booked_seats", "available_seats", "total_capacity", "occupancy_percentage"]
                return save_analytics_output("seat_occupancy_per_event", fields, records)
            except Exception as e:
                print(f"  [i] PySpark fallback for Job 1 ({e})")

    # Native Python Multi-core processing
    events = load_dataset("events")
    seats = load_dataset("seats")
    booked_counts = defaultdict(int)
    for s in seats:
        booked_counts[str(s["event_id"])] += 1

    records = []
    for ev in events:
        eid = str(ev["event_id"])
        booked = booked_counts[eid]
        avail = int(ev.get("available_seats", 0))
        total_capacity = booked + avail
        occ_rate = round((booked / total_capacity * 100), 2) if total_capacity > 0 else 0.0

        records.append({
            "event_id": ev["event_id"],
            "movie_title": ev.get("movie_title", "Unknown"),
            "hall_name": ev.get("hall_name", "Unknown"),
            "screen_time": ev.get("screen_time", "Unknown"),
            "booked_seats": booked,
            "available_seats": avail,
            "total_capacity": total_capacity,
            "occupancy_percentage": occ_rate
        })

    fields = ["event_id", "movie_title", "hall_name", "screen_time", "booked_seats", "available_seats", "total_capacity", "occupancy_percentage"]
    return save_analytics_output("seat_occupancy_per_event", fields, records)

# ==============================================================================
# BATCH JOB 2: Number of Available Seats & Capacity Status per Event
# ==============================================================================
def job_2_available_seats_per_event(use_spark=False):
    """Job 2: Analyzes available seats per event and categorizes availability status."""
    events = load_dataset("events")
    records = []
    for ev in events:
        avail = int(ev.get("available_seats", 0))
        if avail == 0:
            status = "SOLD OUT"
        elif avail <= 15:
            status = "LIMITED AVAILABILITY"
        else:
            status = "AVAILABLE"

        records.append({
            "event_id": ev["event_id"],
            "movie_title": ev.get("movie_title", "Unknown"),
            "screen_time": ev.get("screen_time", "Unknown"),
            "hall_name": ev.get("hall_name", "Unknown"),
            "available_seats": avail,
            "availability_status": status
        })

    fields = ["event_id", "movie_title", "screen_time", "hall_name", "available_seats", "availability_status"]
    return save_analytics_output("available_seats_per_event", fields, records)

# ==============================================================================
# BATCH JOB 3: Booking Statistics by Event Category / Genre (Multi-dataset: events + seats)
# ==============================================================================
def job_3_booking_stats_by_category(use_spark=False):
    """Job 3: Computes booking statistics, revenues, and pricing by genre category."""
    events = load_dataset("events")
    seats = load_dataset("seats")

    event_genres = {}
    for ev in events:
        event_genres[str(ev["event_id"])] = ev.get("genre", "Uncategorized")

    genre_data = defaultdict(lambda: {"total_bookings": 0, "total_revenue": 0.0, "prices": []})

    for s in seats:
        eid = str(s["event_id"])
        genre_str = event_genres.get(eid, "Uncategorized")
        genres = [g.strip() for g in genre_str.split(",") if g.strip()]
        price = float(s.get("price", 0.0))

        for g in genres:
            genre_data[g]["total_bookings"] += 1
            genre_data[g]["total_revenue"] += price
            genre_data[g]["prices"].append(price)

    records = []
    for g, data in sorted(genre_data.items(), key=lambda x: x[1]["total_revenue"], reverse=True):
        prices = data["prices"]
        avg_price = round(data["total_revenue"] / data["total_bookings"], 2) if data["total_bookings"] > 0 else 0.0
        records.append({
            "genre": g,
            "total_bookings": data["total_bookings"],
            "total_revenue": round(data["total_revenue"], 2),
            "avg_ticket_price": avg_price,
            "min_ticket_price": min(prices) if prices else 0.0,
            "max_ticket_price": max(prices) if prices else 0.0
        })

    fields = ["genre", "total_bookings", "total_revenue", "avg_ticket_price", "min_ticket_price", "max_ticket_price"]
    return save_analytics_output("booking_stats_by_category", fields, records)

# ==============================================================================
# BATCH JOB 4: Top 5 Users by Total Bookings & Spend (Multi-dataset: users + seats, RANKED)
# ==============================================================================
def job_4_top_5_users(use_spark=False):
    """Job 4: Ranks and extracts the Top 5 most active cinema users by booking count and spend."""
    users = load_dataset("users")
    seats = load_dataset("seats")

    user_bookings = defaultdict(lambda: {"bookings": 0, "total_spent": 0.0})
    for s in seats:
        uid = str(s["user_id"])
        user_bookings[uid]["bookings"] += 1
        user_bookings[uid]["total_spent"] += float(s.get("price", 0.0))

    user_map = {str(u["user_id"]): u for u in users}

    user_stats = []
    for uid, stats in user_bookings.items():
        u = user_map.get(uid, {"name": f"User {uid}", "phone_number": "N/A", "loyalty_points": 0})
        avg_spend = round(stats["total_spent"] / stats["bookings"], 2) if stats["bookings"] > 0 else 0.0
        user_stats.append({
            "user_id": uid,
            "name": u["name"],
            "phone_number": u.get("phone_number", "N/A"),
            "loyalty_points": int(u.get("loyalty_points", 0)),
            "total_bookings": stats["bookings"],
            "total_spent": round(stats["total_spent"], 2),
            "avg_spend_per_ticket": avg_spend
        })

    user_stats.sort(key=lambda x: (x["total_bookings"], x["total_spent"]), reverse=True)
    top_5 = user_stats[:5]

    records = []
    for rank, item in enumerate(top_5, start=1):
        record = {"rank": rank}
        record.update(item)
        records.append(record)

    fields = ["rank", "user_id", "name", "phone_number", "loyalty_points", "total_bookings", "total_spent", "avg_spend_per_ticket"]
    return save_analytics_output("top_5_users", fields, records)

# ==============================================================================
# BATCH JOB 5: Top 5 Highest-Grossing Events/Movies (Multi-dataset: events + seats, RANKED)
# ==============================================================================
def job_5_top_5_grossing_events(use_spark=False):
    """Job 5: Ranks and extracts the Top 5 highest grossing movie events by ticket sales revenue."""
    events = load_dataset("events")
    seats = load_dataset("seats")

    event_rev = defaultdict(lambda: {"tickets_sold": 0, "total_revenue": 0.0})
    for s in seats:
        eid = str(s["event_id"])
        event_rev[eid]["tickets_sold"] += 1
        event_rev[eid]["total_revenue"] += float(s.get("price", 0.0))

    event_map = {str(e["event_id"]): e for e in events}

    event_stats = []
    for eid, rev in event_rev.items():
        ev = event_map.get(eid, {"movie_title": f"Event {eid}", "hall_name": "Unknown", "screen_time": "Unknown"})
        event_stats.append({
            "event_id": eid,
            "movie_title": ev.get("movie_title", "Unknown"),
            "hall_name": ev.get("hall_name", "Unknown"),
            "screen_time": ev.get("screen_time", "Unknown"),
            "tickets_sold": rev["tickets_sold"],
            "total_box_office_revenue": round(rev["total_revenue"], 2)
        })

    event_stats.sort(key=lambda x: (x["total_box_office_revenue"], x["tickets_sold"]), reverse=True)
    top_5 = event_stats[:5]

    records = []
    for rank, item in enumerate(top_5, start=1):
        record = {"rank": rank}
        record.update(item)
        records.append(record)

    fields = ["rank", "event_id", "movie_title", "hall_name", "screen_time", "tickets_sold", "total_box_office_revenue"]
    return save_analytics_output("top_5_grossing_events", fields, records)

# ==============================================================================
# BATCH JOB 6: Revenue & Ticket Demand by Auditorium Hall (Multi-dataset: events + seats)
# ==============================================================================
def job_6_revenue_by_auditorium(use_spark=False):
    """Job 6: Aggregates revenue, total screenings, and ticket demand by auditorium hall."""
    events = load_dataset("events")
    seats = load_dataset("seats")

    hall_events = defaultdict(set)
    event_hall = {}
    for ev in events:
        eid = str(ev["event_id"])
        hall = ev.get("hall_name", "Standard Hall")
        hall_events[hall].add(eid)
        event_hall[eid] = hall

    hall_sales = defaultdict(lambda: {"tickets_sold": 0, "total_revenue": 0.0})
    for s in seats:
        eid = str(s["event_id"])
        hall = event_hall.get(eid, "Standard Hall")
        hall_sales[hall]["tickets_sold"] += 1
        hall_sales[hall]["total_revenue"] += float(s.get("price", 0.0))

    records = []
    for hall, ev_set in sorted(hall_events.items()):
        total_events = len(ev_set)
        sales = hall_sales[hall]
        tot_rev = round(sales["total_revenue"], 2)
        avg_rev = round(tot_rev / total_events, 2) if total_events > 0 else 0.0

        records.append({
            "hall_name": hall,
            "total_screenings": total_events,
            "total_tickets_sold": sales["tickets_sold"],
            "total_revenue": tot_rev,
            "avg_revenue_per_screening": avg_rev
        })

    fields = ["hall_name", "total_screenings", "total_tickets_sold", "total_revenue", "avg_revenue_per_screening"]
    return save_analytics_output("revenue_by_auditorium", fields, records)

# ==============================================================================
# BATCH JOB 7: User Loyalty Tier & Engagement Analysis (Multi-dataset: users + seats)
# ==============================================================================
def job_7_loyalty_tier_analytics(use_spark=False):
    """Job 7: Segments users into loyalty tiers and evaluates engagement and monetization."""
    users = load_dataset("users")
    seats = load_dataset("seats")

    def get_tier(pts):
        pts = int(pts)
        if pts >= 400:
            return "Platinum (400+ pts)"
        elif pts >= 250:
            return "Gold (250-399 pts)"
        elif pts >= 100:
            return "Silver (100-249 pts)"
        else:
            return "Bronze (<100 pts)"

    user_tier = {}
    tier_users = defaultdict(set)
    for u in users:
        uid = str(u["user_id"])
        tier = get_tier(u.get("loyalty_points", 0))
        user_tier[uid] = tier
        tier_users[tier].add(uid)

    tier_sales = defaultdict(lambda: {"bookings": 0, "total_revenue": 0.0})
    for s in seats:
        uid = str(s["user_id"])
        tier = user_tier.get(uid, "Bronze (<100 pts)")
        tier_sales[tier]["bookings"] += 1
        tier_sales[tier]["total_revenue"] += float(s.get("price", 0.0))

    tier_order = ["Platinum (400+ pts)", "Gold (250-399 pts)", "Silver (100-249 pts)", "Bronze (<100 pts)"]
    records = []

    for tier in tier_order:
        user_count = len(tier_users[tier])
        sales = tier_sales[tier]
        tot_rev = round(sales["total_revenue"], 2)
        bookings = sales["bookings"]
        avg_spend_user = round(tot_rev / user_count, 2) if user_count > 0 else 0.0
        avg_ticket = round(tot_rev / bookings, 2) if bookings > 0 else 0.0

        records.append({
            "loyalty_tier": tier,
            "total_users_in_tier": user_count,
            "total_bookings": bookings,
            "total_revenue": tot_rev,
            "avg_spend_per_user": avg_spend_user,
            "avg_ticket_price": avg_ticket
        })

    fields = ["loyalty_tier", "total_users_in_tier", "total_bookings", "total_revenue", "avg_spend_per_user", "avg_ticket_price"]
    return save_analytics_output("loyalty_tier_analytics", fields, records)

# ==============================================================================
# BATCH JOB 8: Time-Series Screening Demand Distribution (Multi-dataset: events + seats)
# ==============================================================================
def job_8_demand_distribution(use_spark=False):
    """Job 8: Evaluates booking demand distribution by time-of-day slots and screening hours."""
    events = load_dataset("events")
    seats = load_dataset("seats")

    def classify_slot(hour):
        if hour < 12:
            return "Morning (<12:00)"
        elif hour < 17:
            return "Afternoon (12:00-16:59)"
        elif hour < 21:
            return "Evening (17:00-20:59)"
        else:
            return "Late Night (21:00+)"

    event_time_info = {}
    for ev in events:
        eid = str(ev["event_id"])
        time_str = ev.get("screen_time", "2026-05-26 12:00:00")
        try:
            dt = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
            hour = dt.hour
        except Exception:
            hour = 12
        event_time_info[eid] = {
            "hour": f"{hour:02d}:00",
            "time_slot": classify_slot(hour)
        }

    slot_stats = defaultdict(lambda: {"screenings": set(), "tickets_sold": 0, "revenue": 0.0})
    for ev in events:
        eid = str(ev["event_id"])
        info = event_time_info[eid]
        slot_key = (info["time_slot"], info["hour"])
        slot_stats[slot_key]["screenings"].add(eid)

    for s in seats:
        eid = str(s["event_id"])
        info = event_time_info.get(eid, {"hour": "12:00", "time_slot": "Afternoon (12:00-16:59)"})
        slot_key = (info["time_slot"], info["hour"])
        slot_stats[slot_key]["tickets_sold"] += 1
        slot_stats[slot_key]["revenue"] += float(s.get("price", 0.0))

    records = []
    for (slot, hour), stats in sorted(slot_stats.items()):
        records.append({
            "time_slot": slot,
            "screening_hour": hour,
            "total_screenings": len(stats["screenings"]),
            "total_tickets_sold": stats["tickets_sold"],
            "slot_revenue": round(stats["revenue"], 2)
        })

    fields = ["time_slot", "screening_hour", "total_screenings", "total_tickets_sold", "slot_revenue"]
    return save_analytics_output("demand_distribution", fields, records)

ALL_JOBS = {
    "job1": ("Seat Occupancy Percentage per Event", job_1_seat_occupancy_per_event),
    "job2": ("Available Seats & Capacity Status per Event", job_2_available_seats_per_event),
    "job3": ("Booking Statistics by Event Category", job_3_booking_stats_by_category),
    "job4": ("Top 5 Users by Total Bookings & Spend", job_4_top_5_users),
    "job5": ("Top 5 Highest-Grossing Events/Movies", job_5_top_5_grossing_events),
    "job6": ("Revenue & Ticket Demand by Auditorium Hall", job_6_revenue_by_auditorium),
    "job7": ("User Loyalty Tier & Engagement Analysis", job_7_loyalty_tier_analytics),
    "job8": ("Time-Series Screening Demand Distribution", job_8_demand_distribution),
}
