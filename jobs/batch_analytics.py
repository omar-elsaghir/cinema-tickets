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
from pyspark.sql.functions import col, count, sum as _sum, round as _round, to_date, desc, when

# =============================================================================
# 1. SPARK SESSION INITIALIZATION
# =============================================================================
def init_spark():
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
    
    events_df = spark.read.option("header", "true").option("inferSchema", "true").csv(HDFS_RAW + "/events/csv/events.csv")
    seats_df = spark.read.option("header", "true").option("inferSchema", "true").csv(HDFS_RAW + "/seats/csv/seats.csv")
    users_df = spark.read.option("header", "true").option("inferSchema", "true").csv(HDFS_RAW + "/users/csv/users.csv")

    print("✅ Datasets successfully loaded into DataFrames!\n")

    # --- DYNAMIC SCHEMA DETECTION ---
    seats_cols = seats_df.columns
    if "status" in seats_cols:
        booked_cond = col("status").rlike("(?i)booked")
        avail_cond = col("status").rlike("(?i)available")
    elif "is_booked" in seats_cols:
        booked_cond = col("is_booked") == True
        avail_cond = col("is_booked") == False
    elif "user_id" in seats_cols:
        booked_cond = col("user_id").isNotNull()
        avail_cond = col("user_id").isNull()
    else:
        booked_cond = col("seat_id").isNotNull()
        avail_cond = col("seat_id").isNull()

    # =========================================================================
    # 4. ANALYTICAL JOBS EXECUTION
    # =========================================================================

    # -------------------------------------------------------------------------
    # JOB 1 (User): Total Bookings per Event
    # -------------------------------------------------------------------------
    print("▶ Running Job 1: Total Bookings per Event...")
    job1_agg = seats_df.filter(booked_cond).groupBy("event_id").agg(count("seat_id").alias("total_bookings"))
    job1_df = events_df.join(job1_agg, "event_id", "left") \
        .select(events_df["event_id"], events_df["movie_title"], job1_agg["total_bookings"]) \
        .na.fill({"total_bookings": 0})
    job1_df.show(5, truncate=False)
    job1_output = HDFS_PROCESSED + "/total_bookings_per_event"
    job1_df.write.mode("overwrite").parquet(job1_output)
    print("✅ Job 1 completed!\n")

    # -------------------------------------------------------------------------
    # JOB 2 (Omar): Seat Occupancy Percentage per Event
    # -------------------------------------------------------------------------
    print("▶ Running Job 2: Seat Occupancy Percentage per Event...")
    job2_agg = seats_df.groupBy("event_id").agg(
        count("seat_id").alias("total_seats"),
        _sum(when(booked_cond, 1).otherwise(0)).alias("booked_seats")
    ).withColumn("occupancy_percentage", _round((col("booked_seats") / col("total_seats")) * 100, 2))
    
    job2_df = events_df.join(job2_agg, "event_id", "left") \
        .select(events_df["event_id"], events_df["movie_title"], job2_agg["occupancy_percentage"]) \
        .na.fill({"occupancy_percentage": 0.0})
    job2_df.show(5, truncate=False)
    job2_output = HDFS_PROCESSED + "/occupancy_percentage_per_event"
    job2_df.write.mode("overwrite").parquet(job2_output)
    print("✅ Job 2 completed!\n")

    # -------------------------------------------------------------------------
    # JOB 3 (User): Total Revenue per Event
    # -------------------------------------------------------------------------
    print("▶ Running Job 3: Total Revenue per Event...")
    job3_agg = seats_df.filter(booked_cond).groupBy("event_id").agg(_round(_sum("price"), 2).alias("total_revenue"))
    job3_df = events_df.join(job3_agg, "event_id", "left") \
        .select(events_df["event_id"], events_df["movie_title"], job3_agg["total_revenue"]) \
        .na.fill({"total_revenue": 0.0})
    job3_df.show(5, truncate=False)
    job3_output = HDFS_PROCESSED + "/total_revenue_per_event"
    job3_df.write.mode("overwrite").parquet(job3_output)
    print("✅ Job 3 completed!\n")

    # -------------------------------------------------------------------------
    # JOB 4 (Omar): Number of Available Seats per Event
    # -------------------------------------------------------------------------
    print("▶ Running Job 4: Number of Available Seats per Event...")
    job4_agg = seats_df.filter(avail_cond).groupBy("event_id").agg(count("seat_id").alias("available_seats"))
    job4_df = events_df.join(job4_agg, "event_id", "left") \
        .select(events_df["event_id"], events_df["movie_title"], job4_agg["available_seats"]) \
        .na.fill({"available_seats": 0})
    job4_df.show(5, truncate=False)
    job4_output = HDFS_PROCESSED + "/available_seats_per_event"
    job4_df.write.mode("overwrite").parquet(job4_output)
    print("✅ Job 4 completed!\n")

    # -------------------------------------------------------------------------
    # JOB 5 (User): Top 5 Most-Booked Events
    # -------------------------------------------------------------------------
    print("▶ Running Job 5: Top 5 Most-Booked Events...")
    job5_df = job1_df.orderBy(desc("total_bookings")).limit(5)
    job5_df.show(truncate=False)
    job5_output = HDFS_PROCESSED + "/top5_events"
    job5_df.write.mode("overwrite").parquet(job5_output)
    print("✅ Job 5 completed!\n")

    # -------------------------------------------------------------------------
    # JOB 6 (Omar): Booking Statistics by Event Category
    # -------------------------------------------------------------------------
    print("▶ Running Job 6: Booking Statistics by Event Category...")
    event_cols = events_df.columns
    cat_col = "category" if "category" in event_cols else "genre" if "genre" in event_cols else "movie_title"
    
    job6_df = events_df.join(seats_df.filter(booked_cond), "event_id", "inner") \
        .groupBy(events_df[cat_col]) \
        .agg(count("seat_id").alias("total_bookings"), _round(_sum("price"), 2).alias("total_revenue")) \
        .na.fill({"total_revenue": 0.0}).orderBy(desc("total_bookings"))
        
    job6_df.show(5, truncate=False)
    job6_output = HDFS_PROCESSED + "/bookings_by_category"
    job6_df.write.mode("overwrite").parquet(job6_output)
    print("✅ Job 6 completed!\n")

    # -------------------------------------------------------------------------
    # JOB 7 (User): Booking Statistics by Date
    # -------------------------------------------------------------------------
    print("▶ Running Job 7: Booking Statistics by Date...")
    events_with_date = events_df.withColumn("event_date", to_date(col("screen_time")))
    job7_df = events_with_date.join(seats_df.filter(booked_cond), "event_id", "left") \
        .groupBy("event_date").agg(count("seat_id").alias("total_bookings"), _round(_sum("price"), 2).alias("daily_revenue")) \
        .na.fill({"daily_revenue": 0.0}).orderBy("event_date")
        
    job7_df.show(5, truncate=False)
    job7_output = HDFS_PROCESSED + "/bookings_by_date"
    job7_df.write.mode("overwrite").parquet(job7_output)
    print("✅ Job 7 completed!\n")

    # -------------------------------------------------------------------------
    # JOB 8 (Omar): Top 5 Users by Number of Bookings
    # -------------------------------------------------------------------------
    print("▶ Running Job 8: Top 5 Users by Number of Bookings...")
    if "user_id" in seats_cols and "user_id" in users_df.columns:
        job8_agg = seats_df.filter(booked_cond).groupBy("user_id").agg(count("seat_id").alias("total_bookings"))
        u_cols = users_df.columns
        name_col = "name" if "name" in u_cols else "username" if "username" in u_cols else "email" if "email" in u_cols else None
        
        if name_col:
            job8_df = users_df.join(job8_agg, "user_id", "inner") \
                .select(users_df["user_id"], users_df[name_col], job8_agg["total_bookings"]) \
                .orderBy(desc("total_bookings")).limit(5)
        else:
            job8_df = users_df.join(job8_agg, "user_id", "inner") \
                .select(users_df["user_id"], job8_agg["total_bookings"]) \
                .orderBy(desc("total_bookings")).limit(5)
    else:
        job8_df = seats_df.filter(booked_cond).groupBy("seat_id").agg(count("seat_id").alias("total_bookings")).limit(5)

    job8_df.show(truncate=False)
    job8_output = HDFS_PROCESSED + "/top5_users"
    job8_df.write.mode("overwrite").parquet(job8_output)
    print("✅ Job 8 completed!\n")

    # =========================================================================
    print("🎉 All 8 batch jobs executed successfully!")
    spark.stop()

if __name__ == "__main__":
    main()