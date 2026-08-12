"""
===============================================================================
🎟️ Cinema Ticket Reservation System - Batch Processing & Analytics (Part 2)
===============================================================================
Description:
    PySpark job to perform 8 distributed analytical queries on cinema ticket
    data stored in HDFS. Results are saved back to HDFS in Parquet format.

Cluster Execution:
    spark-submit --master yarn --deploy-mode client jobs/batch_analytics.py


===============================================================================
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
# 2. HDFS PATH CONFIGURATIONS (Python 3.5 Compatible)
# =============================================================================
HDFS_BASE = "hdfs://namenode:9000/ticket_system"
HDFS_RAW = HDFS_BASE + "/raw"
HDFS_PROCESSED = HDFS_BASE + "/processed"


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
        .csv(HDFS_RAW + "/events/csv/events.csv")

    seats_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(HDFS_RAW + "/seats/csv/seats.csv")

    users_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(HDFS_RAW + "/users/csv/users.csv")

    print("✅ Datasets successfully loaded into DataFrames!\n")

    # =========================================================================
    # 4. ANALYTICAL JOBS EXECUTION
    # =========================================================================

    # -------------------------------------------------------------------------
    # JOB 1 (User - Odd): Total Bookings per Event
    # Multi-dataset: Yes (events + seats)
    # -------------------------------------------------------------------------
    print("▶ Running Job 1: Total Bookings per Event...")
    
    # Aggregate booked seats per event
    bookings_per_event = seats_df.groupBy("event_id") \
        .agg(count("seat_id").alias("total_bookings"))

    # Join with events metadata to include movie titles
    job1_df = events_df.join(bookings_per_event, "event_id", "left") \
        .select("event_id", "movie_title", "total_bookings") \
        .na.fill({"total_bookings": 0})

    # Display evidence snippet in cluster logs
    print("📊 [Job 1 Sample Output]:")
    job1_df.show(5, truncate=False)

    # Idempotent write to HDFS in Parquet format
    job1_output_path = HDFS_PROCESSED + "/total_bookings_per_event"
    job1_df.write.mode("overwrite").parquet(job1_output_path)
    print("✅ Job 1 completed! Written to HDFS: " + job1_output_path + "\n")

    # -------------------------------------------------------------------------
    # JOB 2 (Omar - Even): Seat Occupancy Percentage per Event
    # -------------------------------------------------------------------------
    print("▶ Running Job 2: Seat Occupancy Percentage per Event...")
    pass

    # -------------------------------------------------------------------------
    # JOB 3 (User - Odd): Total Revenue per Event
    # -------------------------------------------------------------------------
    print("▶ Running Job 3: Total Revenue per Event...")
    
    # Aggregate total revenue per event
    revenue_per_event = seats_df.groupBy("event_id") \
        .agg(_round(_sum("price"), 2).alias("total_revenue"))

    # Join with events metadata
    job3_df = events_df.join(revenue_per_event, "event_id", "left") \
        .select("event_id", "movie_title", "total_revenue") \
        .na.fill({"total_revenue": 0.0})

    # Display evidence snippet in cluster logs
    print("📊 [Job 3 Sample Output]:")
    job3_df.show(5, truncate=False)

    # Write output to HDFS
    job3_output_path = HDFS_PROCESSED + "/total_revenue_per_event"
    job3_df.write.mode("overwrite").parquet(job3_output_path)
    print("✅ Job 3 completed! Written to HDFS: " + job3_output_path + "\n")

    # -------------------------------------------------------------------------
    # JOB 4 (Omar - Even): Number of Available Seats per Event
    # -------------------------------------------------------------------------
    print("▶ Running Job 4: Number of Available Seats per Event...")
    pass

    # -------------------------------------------------------------------------
    # JOB 5 (User - Odd): Top 5 Most-Booked Events
    # -------------------------------------------------------------------------
    print("▶ Running Job 5: Top 5 Most-Booked Events...")
    
    # Reuse job1_df, sort descending by total_bookings, and take top 5
    job5_df = job1_df.orderBy(desc("total_bookings")).limit(5)

    # Display evidence snippet in cluster logs
    print("📊 [Job 5 Sample Output]:")
    job5_df.show(truncate=False)

    # Write output to HDFS
    job5_output_path = HDFS_PROCESSED + "/top5_events"
    job5_df.write.mode("overwrite").parquet(job5_output_path)
    print("✅ Job 5 completed! Written to HDFS: " + job5_output_path + "\n")

    # -------------------------------------------------------------------------
    # JOB 6 (Omar - Even): Booking Statistics by Event Category
    # -------------------------------------------------------------------------
    print("▶ Running Job 6: Booking Statistics by Event Category...")
    pass

    # -------------------------------------------------------------------------
    # JOB 7 (User - Odd): Booking Statistics by Date
    # -------------------------------------------------------------------------
    print("▶ Running Job 7: Booking Statistics by Date...")
    pass

    # -------------------------------------------------------------------------
    # JOB 8 (Omar - Even): Top 5 Users by Number of Bookings
    # -------------------------------------------------------------------------
    print("▶ Running Job 8: Top 5 Users by Number of Bookings...")
    pass

    # =========================================================================
    # 5. PIPELINE COMPLETION
    # =========================================================================
    print("\n🎉 All active batch jobs executed successfully!")
    spark.stop()


if __name__ == "__main__":
    main()