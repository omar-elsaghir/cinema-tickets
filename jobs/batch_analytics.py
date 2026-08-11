"""
===============================================================================
🎟️ Cinema Ticket Reservation System - Batch Processing & Analytics (Part 2)
===============================================================================
Description:
    PySpark job to perform 8 distributed analytical queries on cinema ticket
    data stored in HDFS. Results are saved back to HDFS in Parquet format.

Cluster Execution:
    spark-submit --master yarn --deploy-mode client jobs/batch_analytics.py

Task Assignments:
    - User (Odd Jobs) : Jobs 1, 3, 5, 7
    - Omar (Even Jobs): Jobs 2, 4, 6, 8
===============================================================================
"""


"""
Recommended directory structure for your cinema-tickets project repository

cinema-tickets/
├── .gitignore                      # Excludes staging data, logs, and python caches from Git
├── README.md                       # Project documentation (Cluster topology, schemas, quickstart)
├── pyproject.toml                  # Python dependencies & environment config (managed by uv)
├── docker-compose.yml              # Cluster orchestration (11-Node Hadoop + YARN setup)
│
├── data/                           # Local data directory
│   └── staging/                    # Staging area for files before uploading to HDFS
│       ├── events/                 # (csv/ & json/)
│       ├── seats/                  # (csv/ & json/)
│       └── users/                  # (csv/ & json/)
│
├── docker/                         # Custom Dockerfiles and Hadoop configuration files
│   ├── base/                       # Base Hadoop image configs (core-site.xml, hdfs-site.xml)
│   ├── datanode/
│   ├── namenode/
│   └── resourcemanager/
│
├── jobs/                           # PySpark Batch Processing Jobs (Part 2)
│   └── batch_analytics.py          # PySpark script executing all 8 required analytical jobs
│
└── scripts/                        # Automation and management scripts
    ├── fetch_and_prepare_data.py   # Downloads/generates raw sample datasets
    ├── ingest_data.py              # Uploads local staging files into HDFS (/ticket_system/raw/)
    ├── verify_data.py              # Checks line count & schema parity between local and HDFS
    └── run_part2.sh                # Shell script to submit PySpark job to YARN via spark-submit


"""



from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as _sum, round as _round, to_date, desc

# =============================================================================
# 1. SPARK SESSION INITIALIZATION
# =============================================================================
def init_spark():
    """Initializes and returns a PySpark session configured for YARN."""
    return SparkSession.builder \
        .appName("TicketSystemBatchAnalytics") \
        .master("yarn") \
        .getOrCreate()


# =============================================================================
# 2. HDFS PATH CONFIGURATIONS
# =============================================================================
HDFS_BASE = "hdfs://namenode:9000/ticket_system"
HDFS_RAW = f"{HDFS_BASE}/raw"
HDFS_PROCESSED = f"{HDFS_BASE}/processed"


def main():
    spark = init_spark()
    spark.sparkContext.setLogLevel("WARN")
    
    print("\n🚀 Starting PySpark Batch Analytics Pipeline on YARN...\n")

    # =========================================================================
    # 3. LOAD RAW DATASETS FROM HDFS
    # =========================================================================
    print("📥 Loading datasets from HDFS...")
    
    events_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(f"{HDFS_RAW}/events/csv/events.csv")

    seats_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(f"{HDFS_RAW}/seats/csv/seats.csv")

    users_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(f"{HDFS_RAW}/users/csv/users.csv")

    print("✅ Datasets successfully loaded into DataFrames!\n")

    # =========================================================================
    # 4. ANALYTICAL JOBS EXECUTION
    # =========================================================================

    # -------------------------------------------------------------------------
    # JOB 1 (User - Odd): Total Bookings per Event
    # Multi-dataset: Yes (events + seats)
    # -------------------------------------------------------------------------
    print("▶ Running Job 1: Total Bookings per Event...")
    # TODO (User):
    # 1. Group seats_df by 'event_id' and count 'seat_id' as 'total_bookings'.
    # 2. Join with events_df to include 'movie_title'.
    # 3. Select columns: event_id, movie_title, total_bookings.
    # 4. Write output to HDFS under f"{HDFS_PROCESSED}/total_bookings_per_event".
    pass

    # -------------------------------------------------------------------------
    # JOB 2 (Omar - Even): Seat Occupancy Percentage per Event
    # Multi-dataset: Yes (events + seats)
    # -------------------------------------------------------------------------
    print("▶ Running Job 2: Seat Occupancy Percentage per Event...")
    # TODO (Omar):
    # 1. Count booked seats per event from seats_df.
    # 2. Join with events_df to get available_seats and movie_title.
    # 3. Calculate occupancy_percentage = (booked_seats / (booked_seats + available_seats)) * 100.
    # 4. Write output to HDFS under f"{HDFS_PROCESSED}/occupancy_percentage_per_event".
    pass

    # -------------------------------------------------------------------------
    # JOB 3 (User - Odd): Total Revenue per Event
    # Multi-dataset: Yes (events + seats)
    # -------------------------------------------------------------------------
    print("▶ Running Job 3: Total Revenue per Event...")
    # TODO (User):
    # 1. Group seats_df by 'event_id' and sum 'price' as 'total_revenue'.
    # 2. Join with events_df to include 'movie_title'.
    # 3. Round total_revenue to 2 decimal places.
    # 4. Write output to HDFS under f"{HDFS_PROCESSED}/total_revenue_per_event".
    pass

    # -------------------------------------------------------------------------
    # JOB 4 (Omar - Even): Number of Available Seats per Event
    # Multi-dataset: No (events only)
    # -------------------------------------------------------------------------
    print("▶ Running Job 4: Number of Available Seats per Event...")
    # TODO (Omar):
    # 1. Select event_id, movie_title, hall_name, and available_seats from events_df.
    # 2. Write output to HDFS under f"{HDFS_PROCESSED}/available_seats_per_event".
    pass

    # -------------------------------------------------------------------------
    # JOB 5 (User - Odd): Top 5 Most-Booked Events
    # Multi-dataset: Yes (events + seats) | Ranked: Yes (#1)
    # -------------------------------------------------------------------------
    print("▶ Running Job 5: Top 5 Most-Booked Events...")
    # TODO (User):
    # 1. Reuse Job 1 results (or re-aggregate bookings per event).
    # 2. Sort by 'total_bookings' in descending order.
    # 3. Limit result to Top 5 rows.
    # 4. Write output to HDFS under f"{HDFS_PROCESSED}/top5_events".
    pass

    # -------------------------------------------------------------------------
    # JOB 6 (Omar - Even): Booking Statistics by Event Category
    # Multi-dataset: Yes (events + seats)
    # -------------------------------------------------------------------------
    print("▶ Running Job 6: Booking Statistics by Event Category...")
    # TODO (Omar):
    # 1. Join seats_df and events_df on 'event_id'.
    # 2. Group by movie 'genre'.
    # 3. Aggregate: total_bookings (count) and total_revenue (sum of price).
    # 4. Write output to HDFS under f"{HDFS_PROCESSED}/bookings_by_category".
    pass

    # -------------------------------------------------------------------------
    # JOB 7 (User - Odd): Booking Statistics by Date
    # Multi-dataset: Yes (events + seats)
    # -------------------------------------------------------------------------
    print("▶ Running Job 7: Booking Statistics by Date...")
    # TODO (User):
    # 1. Convert 'screen_time' in events_df to date format ('YYYY-MM-DD').
    # 2. Join with seats_df on 'event_id'.
    # 3. Group by 'event_date' and aggregate total_bookings and daily_revenue.
    # 4. Write output to HDFS under f"{HDFS_PROCESSED}/bookings_by_date".
    pass

    # -------------------------------------------------------------------------
    # JOB 8 (Omar - Even): Top 5 Users by Number of Bookings
    # Multi-dataset: Yes (users + seats) | Ranked: Yes (#2)
    # -------------------------------------------------------------------------
    print("▶ Running Job 8: Top 5 Users by Number of Bookings...")
    # TODO (Omar):
    # 1. Group seats_df by 'user_id' and count bookings.
    # 2. Join with users_df to get user details (name, phone_number, loyalty_points).
    # 3. Order by total_bookings descending and limit to Top 5.
    # 4. Write output to HDFS under f"{HDFS_PROCESSED}/top5_users".
    pass

    # =========================================================================
    # 5. PIPELINE COMPLETION
    # =========================================================================
    print("\n🎉 All 8 Analytical Batch Jobs executed successfully!")
    spark.stop()


if __name__ == "__main__":
    main()



"""


💡 Remember for Write Operations
When writing the output DataFrames in your respective TODOs, use the .mode("overwrite") pattern so the job can be re-run safely without crashing:

Python
# Example write template to use in TODOs:
df.write.mode("overwrite").parquet(f"{HDFS_PROCESSED}/output_folder_name")



"""