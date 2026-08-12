# -*- coding: utf-8 -*-
"""
===============================================================================
Cinema Ticket Reservation System - Batch Processing & Analytics (Part 2)
===============================================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as _sum, round as _round, to_date, desc, when, countDistinct, avg as _avg

def init_spark():
    return SparkSession.builder \
        .appName("TicketSystemBatchAnalytics") \
        .getOrCreate()

HDFS_BASE = "hdfs://namenode:9000/ticket_system"
HDFS_RAW = HDFS_BASE + "/raw"
HDFS_PROCESSED = HDFS_BASE + "/processed"

def main():
    spark = init_spark()
    spark.sparkContext.setLogLevel("WARN")
    
    print("\n>>> Starting PySpark Batch Analytics Pipeline...\n")

    # Load datasets
    events_df = spark.read.option("header", "true").option("inferSchema", "true").csv(HDFS_RAW + "/events/csv/events.csv")
    seats_df = spark.read.option("header", "true").option("inferSchema", "true").csv(HDFS_RAW + "/seats/csv/seats.csv")
    users_df = spark.read.option("header", "true").option("inferSchema", "true").csv(HDFS_RAW + "/users/csv/users.csv")

    # Reusable aggregation: Bookings per event
    bookings_per_event = seats_df.groupBy("event_id").agg(count("seat_id").alias("total_bookings"))

    # -------------------------------------------------------------------------
    # JOB 1: Total Bookings per Event
    # -------------------------------------------------------------------------
    print(">>> Running Job 1: Total Bookings per Event...")
    job1_df = events_df.join(bookings_per_event, "event_id", "left") \
        .select("event_id", "movie_title", "total_bookings") \
        .na.fill({"total_bookings": 0})
    job1_df.show(5, truncate=False)
    job1_df.write.mode("overwrite").parquet(HDFS_PROCESSED + "/total_bookings_per_event")

    # -------------------------------------------------------------------------
    # JOB 2: Seat Occupancy Percentage per Event (Accurate Capacity Calculation)
    # -------------------------------------------------------------------------
    print(">>> Running Job 2: Seat Occupancy Percentage per Event...")
    job2_df = events_df.join(bookings_per_event, "event_id", "left") \
        .na.fill({"total_bookings": 0, "available_seats": 0}) \
        .withColumn("total_capacity", col("available_seats") + col("total_bookings")) \
        .withColumn(
            "occupancy_percentage",
            when(col("total_capacity") > 0, _round((col("total_bookings") / col("total_capacity")) * 100, 2)).otherwise(0.0)
        ) \
        .select("event_id", "movie_title", "total_bookings", "available_seats", "total_capacity", "occupancy_percentage") \
        .orderBy("event_id")
    job2_df.show(5, truncate=False)
    job2_df.write.mode("overwrite").parquet(HDFS_PROCESSED + "/occupancy_percentage_per_event")

    # -------------------------------------------------------------------------
    # JOB 3: Total Revenue per Event
    # -------------------------------------------------------------------------
    print(">>> Running Job 3: Total Revenue per Event...")
    revenue_per_event = seats_df.groupBy("event_id").agg(_round(_sum("price"), 2).alias("total_revenue"))
    job3_df = events_df.join(revenue_per_event, "event_id", "left") \
        .select("event_id", "movie_title", "total_revenue") \
        .na.fill({"total_revenue": 0.0})
    job3_df.show(5, truncate=False)
    job3_df.write.mode("overwrite").parquet(HDFS_PROCESSED + "/total_revenue_per_event")

    # -------------------------------------------------------------------------
    # JOB 4: Number of Available Seats per Event (From Dataset Metadata)
    # -------------------------------------------------------------------------
    print(">>> Running Job 4: Number of Available Seats per Event...")
    job4_df = events_df.select("event_id", "movie_title", "hall_name", "screen_time", "available_seats").orderBy("event_id")
    job4_df.show(5, truncate=False)
    job4_df.write.mode("overwrite").parquet(HDFS_PROCESSED + "/available_seats_per_event")

    # -------------------------------------------------------------------------
    # JOB 5: Top 5 Most-Booked Events
    # -------------------------------------------------------------------------
    print(">>> Running Job 5: Top 5 Most-Booked Events...")
    job5_df = job1_df.orderBy(desc("total_bookings")).limit(5)
    job5_df.show(truncate=False)
    job5_df.write.mode("overwrite").parquet(HDFS_PROCESSED + "/top5_events")

    # -------------------------------------------------------------------------
    # JOB 6: Booking Statistics by Event Category
    # -------------------------------------------------------------------------
    print(">>> Running Job 6: Booking Statistics by Event Category...")
    job6_df = events_df.join(seats_df, "event_id", "left") \
        .groupBy(col("genre").alias("category")) \
        .agg(
            count("seat_id").alias("total_bookings"),
            _round(_sum("price"), 2).alias("total_revenue"),
            countDistinct("event_id").alias("total_events"),
            _round(_avg("price"), 2).alias("average_ticket_price")
        ) \
        .na.fill({"total_revenue": 0.0, "average_ticket_price": 0.0}) \
        .orderBy(desc("total_bookings"), desc("total_revenue"))
    job6_df.show(10, truncate=False)
    job6_df.write.mode("overwrite").parquet(HDFS_PROCESSED + "/bookings_by_category")

    # -------------------------------------------------------------------------
    # JOB 7: Booking Statistics by Date
    # -------------------------------------------------------------------------
    print(">>> Running Job 7: Booking Statistics by Date...")
    events_with_date = events_df.withColumn("event_date", to_date(col("screen_time")))
    job7_df = events_with_date.join(seats_df, "event_id", "left") \
        .groupBy("event_date") \
        .agg(
            count("seat_id").alias("total_bookings"),
            _round(_sum("price"), 2).alias("daily_revenue")
        ) \
        .na.fill({"daily_revenue": 0.0}) \
        .orderBy("event_date")
    job7_df.show(5, truncate=False)
    job7_df.write.mode("overwrite").parquet(HDFS_PROCESSED + "/bookings_by_date")

    # -------------------------------------------------------------------------
    # JOB 8: Top 5 Users by Number of Bookings
    # -------------------------------------------------------------------------
    print(">>> Running Job 8: Top 5 Users by Number of Bookings...")
    user_bookings = seats_df.groupBy("user_id").agg(
        count("seat_id").alias("total_bookings"),
        _round(_sum("price"), 2).alias("total_spent")
    )
    job8_df = users_df.join(user_bookings, "user_id", "inner") \
        .select("user_id", "name", "phone_number", "loyalty_points", "total_bookings", "total_spent") \
        .orderBy(desc("total_bookings"), desc("total_spent")) \
        .limit(5)
    job8_df.show(truncate=False)
    job8_df.write.mode("overwrite").parquet(HDFS_PROCESSED + "/top5_users")

    print("\n[COMPLETE] All active batch jobs executed successfully!")
    spark.stop()

if __name__ == "__main__":
    main()