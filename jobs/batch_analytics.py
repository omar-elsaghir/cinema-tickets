# -*- coding: utf-8 -*-
import os
import sys
import subprocess

HAS_SPARK = False

def check_java():
    try:
        with open(os.devnull, 'w') as devnull:
            return subprocess.call(["java", "-version"], stdout=devnull, stderr=devnull) == 0
    except Exception:
        return False

if check_java():
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql.functions import col, count, sum as _sum, round as _round, to_date, desc, when, countDistinct, avg as _avg
        HAS_SPARK = True
    except Exception:
        HAS_SPARK = False


def run_pyspark_pipeline():
    hdfs_base = "hdfs://namenode:9000/ticket_system"
    hdfs_raw = hdfs_base + "/raw"
    hdfs_processed = hdfs_base + "/processed"

    spark = SparkSession.builder.appName("TicketSystemBatchAnalytics").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    print("\n>>> Starting PySpark Batch Analytics Pipeline on Cluster...\n")
    print("[INFO] Loading datasets from HDFS...")

    events_df = spark.read.option("header", "true").option("inferSchema", "true").csv(hdfs_raw + "/events/csv/events.csv")
    seats_df = spark.read.option("header", "true").option("inferSchema", "true").csv(hdfs_raw + "/seats/csv/seats.csv")
    users_df = spark.read.option("header", "true").option("inferSchema", "true").csv(hdfs_raw + "/users/csv/users.csv")
    print("[SUCCESS] Datasets successfully loaded into DataFrames!\n")

    bookings_per_event = seats_df.groupBy("event_id").agg(count("seat_id").alias("total_bookings"))

    # Job 1
    print(">>> Running Job 1: Total Bookings per Event...")
    job1_df = events_df.join(bookings_per_event, "event_id", "left").select("event_id", "movie_title", "total_bookings").na.fill({"total_bookings": 0})
    print("[OUTPUT] [Job 1 Sample Output]:")
    job1_df.show(5, truncate=False)
    job1_df.write.mode("overwrite").parquet(hdfs_processed + "/total_bookings_per_event")
    print("[SUCCESS] Job 1 completed!\n")

    # Job 2
    print(">>> Running Job 2: Seat Occupancy Percentage per Event...")
    job2_df = events_df.join(bookings_per_event, "event_id", "left").na.fill({"total_bookings": 0, "available_seats": 0}) \
        .withColumn("total_capacity", col("available_seats") + col("total_bookings")) \
        .withColumn("occupancy_percentage", when(col("total_capacity") > 0, _round((col("total_bookings") / col("total_capacity")) * 100, 2)).otherwise(0.0)) \
        .select("event_id", "movie_title", "total_bookings", "available_seats", "total_capacity", "occupancy_percentage") \
        .orderBy("event_id")
    print("[OUTPUT] [Job 2 Sample Output]:")
    job2_df.show(5, truncate=False)
    job2_df.write.mode("overwrite").parquet(hdfs_processed + "/occupancy_percentage_per_event")
    print("[SUCCESS] Job 2 completed!\n")

    # Job 3
    print(">>> Running Job 3: Total Revenue per Event...")
    revenue_per_event = seats_df.groupBy("event_id").agg(_round(_sum("price"), 2).alias("total_revenue"))
    job3_df = events_df.join(revenue_per_event, "event_id", "left").select("event_id", "movie_title", "total_revenue").na.fill({"total_revenue": 0.0})
    print("[OUTPUT] [Job 3 Sample Output]:")
    job3_df.show(5, truncate=False)
    job3_df.write.mode("overwrite").parquet(hdfs_processed + "/total_revenue_per_event")
    print("[SUCCESS] Job 3 completed!\n")

    # Job 4
    print(">>> Running Job 4: Number of Available Seats per Event...")
    job4_df = events_df.select("event_id", "movie_title", "hall_name", "screen_time", "available_seats").orderBy("event_id")
    print("[OUTPUT] [Job 4 Sample Output]:")
    job4_df.show(5, truncate=False)
    job4_df.write.mode("overwrite").parquet(hdfs_processed + "/available_seats_per_event")
    print("[SUCCESS] Job 4 completed!\n")

    # Job 5
    print(">>> Running Job 5: Top 5 Most-Booked Events...")
    job5_df = job1_df.orderBy(desc("total_bookings")).limit(5)
    print("[OUTPUT] [Job 5 Sample Output]:")
    job5_df.show(truncate=False)
    job5_df.write.mode("overwrite").parquet(hdfs_processed + "/top5_events")
    print("[SUCCESS] Job 5 completed!\n")

    # Job 6
    print(">>> Running Job 6: Booking Statistics by Event Category...")
    job6_df = events_df.join(seats_df, "event_id", "left").groupBy(col("genre").alias("category")) \
        .agg(
            count("seat_id").alias("total_bookings"),
            _round(_sum("price"), 2).alias("total_revenue"),
            countDistinct("event_id").alias("total_events"),
            _round(_avg("price"), 2).alias("average_ticket_price")
        ).na.fill({"total_revenue": 0.0, "average_ticket_price": 0.0}).orderBy(desc("total_bookings"), desc("total_revenue"))
    print("[OUTPUT] [Job 6 Sample Output]:")
    job6_df.show(10, truncate=False)
    job6_df.write.mode("overwrite").parquet(hdfs_processed + "/bookings_by_category")
    print("[SUCCESS] Job 6 completed!\n")

    # Job 7
    print(">>> Running Job 7: Booking Statistics by Date...")
    events_with_date = events_df.withColumn("event_date", to_date(col("screen_time")))
    job7_df = events_with_date.join(seats_df, "event_id", "left").groupBy("event_date") \
        .agg(count("seat_id").alias("total_bookings"), _round(_sum("price"), 2).alias("daily_revenue")) \
        .na.fill({"daily_revenue": 0.0}).orderBy("event_date")
    print("[OUTPUT] [Job 7 Sample Output]:")
    job7_df.show(5, truncate=False)
    job7_df.write.mode("overwrite").parquet(hdfs_processed + "/bookings_by_date")
    print("[SUCCESS] Job 7 completed!\n")

    # Job 8
    print(">>> Running Job 8: Top 5 Users by Number of Bookings...")
    user_bookings = seats_df.groupBy("user_id").agg(count("seat_id").alias("total_bookings"), _round(_sum("price"), 2).alias("total_spent"))
    job8_df = users_df.join(user_bookings, "user_id", "inner") \
        .select("user_id", "name", "phone_number", "loyalty_points", "total_bookings", "total_spent") \
        .orderBy(desc("total_bookings"), desc("total_spent")).limit(5)
    print("[OUTPUT] [Job 8 Sample Output]:")
    job8_df.show(truncate=False)
    job8_df.write.mode("overwrite").parquet(hdfs_processed + "/top5_users")
    print("[SUCCESS] Job 8 completed!\n")

    print("[COMPLETE] All 8 active batch jobs executed successfully!")
    spark.stop()


def run_local_pipeline():
    import pandas as pd
    import numpy as np

    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    staging_candidates = [
        os.path.join(base_dir, "data", "staging"),
        os.path.join(base_dir, "scripts", "data", "staging"),
        os.path.join(os.getcwd(), "data", "staging"),
        os.path.join(os.getcwd(), "scripts", "data", "staging"),
        "data/staging",
        "scripts/data/staging"
    ]
    staging_dir = next((p for p in staging_candidates if os.path.exists(os.path.join(p, "events.csv"))), "data/staging")
    local_processed_dir = os.path.join(base_dir, "my_analytics_results")
    hdfs_processed_base = "/ticket_system/processed"
    os.makedirs(local_processed_dir, exist_ok=True)

    events_path = os.path.join(staging_dir, "events.csv")
    seats_path = os.path.join(staging_dir, "seats.csv")
    users_path = os.path.join(staging_dir, "users.csv")

    if not (os.path.exists(events_path) and os.path.exists(seats_path) and os.path.exists(users_path)):
        print("Error: Staging files not found in " + str(staging_dir) + ". Run scripts/fetch_and_prepare_data.py first.")
        return

    print("\n>>> Starting Cinema Ticket Batch Analytics Pipeline (Local Engine)...\n")
    print("[INFO] Loading datasets from: " + str(staging_dir))

    events_df = pd.read_csv(events_path)
    seats_df = pd.read_csv(seats_path)
    users_df = pd.read_csv(users_path)
    print("[SUCCESS] Datasets loaded successfully!\n")

    def write_parquet_dir(df, path):
        import shutil
        if os.path.exists(path):
            if os.path.isdir(path):
                shutil.rmtree(path)
            else:
                os.remove(path)
        os.makedirs(path, exist_ok=True)
        df.to_parquet(os.path.join(path, "part-00000.snappy.parquet"), compression="snappy", index=False)

    bookings_per_event = seats_df.groupby("event_id").agg(total_bookings=("seat_id", "count")).reset_index()

    # Job 1
    print(">>> Running Job 1: Total Bookings per Event...")
    job1_df = events_df.merge(bookings_per_event, on="event_id", how="left")
    job1_df["total_bookings"] = job1_df["total_bookings"].fillna(0).astype(int)
    job1_df = job1_df[["event_id", "movie_title", "total_bookings"]]
    print("[OUTPUT] [Job 1 Sample Output]:")
    print(job1_df.head(5).to_string(index=False))
    write_parquet_dir(job1_df, os.path.join(local_processed_dir, "total_bookings_per_event"))
    print("[SUCCESS] Job 1 completed!\n")

    # Job 2
    print(">>> Running Job 2: Seat Occupancy Percentage per Event...")
    job2_df = events_df.merge(bookings_per_event, on="event_id", how="left")
    job2_df["total_bookings"] = job2_df["total_bookings"].fillna(0).astype(int)
    job2_df["available_seats"] = job2_df["available_seats"].fillna(0).astype(int)
    job2_df["total_capacity"] = job2_df["available_seats"] + job2_df["total_bookings"]
    job2_df["occupancy_percentage"] = np.where(
        job2_df["total_capacity"] > 0,
        np.round((job2_df["total_bookings"] / job2_df["total_capacity"]) * 100.0, 2),
        0.0
    )
    job2_df = job2_df.sort_values("event_id")[["event_id", "movie_title", "total_bookings", "available_seats", "total_capacity", "occupancy_percentage"]]
    print("[OUTPUT] [Job 2 Sample Output]:")
    print(job2_df.head(5).to_string(index=False))
    write_parquet_dir(job2_df, os.path.join(local_processed_dir, "occupancy_percentage_per_event"))
    print("[SUCCESS] Job 2 completed!\n")

    # Job 3
    print(">>> Running Job 3: Total Revenue per Event...")
    rev = seats_df.groupby("event_id").agg(total_revenue=("price", lambda x: round(x.sum(), 2))).reset_index()
    job3_df = events_df.merge(rev, on="event_id", how="left")
    job3_df["total_revenue"] = job3_df["total_revenue"].fillna(0.0)
    job3_df = job3_df[["event_id", "movie_title", "total_revenue"]]
    print("[OUTPUT] [Job 3 Sample Output]:")
    print(job3_df.head(5).to_string(index=False))
    write_parquet_dir(job3_df, os.path.join(local_processed_dir, "total_revenue_per_event"))
    print("[SUCCESS] Job 3 completed!\n")

    # Job 4
    print(">>> Running Job 4: Number of Available Seats per Event...")
    job4_df = events_df[["event_id", "movie_title", "hall_name", "screen_time", "available_seats"]].sort_values("event_id")
    print("[OUTPUT] [Job 4 Sample Output]:")
    print(job4_df.head(5).to_string(index=False))
    write_parquet_dir(job4_df, os.path.join(local_processed_dir, "available_seats_per_event"))
    print("[SUCCESS] Job 4 completed!\n")

    # Job 5
    print(">>> Running Job 5: Top 5 Most-Booked Events...")
    job5_df = job1_df.sort_values("total_bookings", ascending=False).head(5)
    print("[OUTPUT] [Job 5 Sample Output]:")
    print(job5_df.to_string(index=False))
    write_parquet_dir(job5_df, os.path.join(local_processed_dir, "top5_events"))
    print("[SUCCESS] Job 5 completed!\n")

    # Job 6
    print(">>> Running Job 6: Booking Statistics by Event Category...")
    cat_df = events_df.merge(seats_df, on="event_id", how="left")
    job6_df = cat_df.groupby("genre").agg(
        total_bookings=("seat_id", "count"),
        total_revenue=("price", lambda x: round(x.sum(), 2)),
        total_events=("event_id", "nunique"),
        average_ticket_price=("price", lambda x: round(x.mean() if len(x) > 0 and not x.isna().all() else 0.0, 2))
    ).reset_index().rename(columns={"genre": "category"})
    job6_df["total_revenue"] = job6_df["total_revenue"].fillna(0.0)
    job6_df["average_ticket_price"] = job6_df["average_ticket_price"].fillna(0.0)
    job6_df = job6_df.sort_values(["total_bookings", "total_revenue"], ascending=[False, False])
    print("[OUTPUT] [Job 6 Sample Output]:")
    print(job6_df.head(10).to_string(index=False))
    write_parquet_dir(job6_df, os.path.join(local_processed_dir, "bookings_by_category"))
    print("[SUCCESS] Job 6 completed!\n")

    # Job 7
    print(">>> Running Job 7: Booking Statistics by Date...")
    events_with_date = events_df.copy()
    events_with_date["event_date"] = pd.to_datetime(events_with_date["screen_time"]).dt.date.astype(str)
    date_df = events_with_date.merge(seats_df, on="event_id", how="left")
    job7_df = date_df.groupby("event_date").agg(
        total_bookings=("seat_id", "count"),
        daily_revenue=("price", lambda x: round(x.sum(), 2))
    ).reset_index().sort_values("event_date")
    job7_df["daily_revenue"] = job7_df["daily_revenue"].fillna(0.0)
    print("[OUTPUT] [Job 7 Sample Output]:")
    print(job7_df.head(5).to_string(index=False))
    write_parquet_dir(job7_df, os.path.join(local_processed_dir, "bookings_by_date"))
    print("[SUCCESS] Job 7 completed!\n")

    # Job 8
    print(">>> Running Job 8: Top 5 Users by Number of Bookings...")
    user_b = seats_df.groupby("user_id").agg(
        total_bookings=("seat_id", "count"),
        total_spent=("price", lambda x: round(x.sum(), 2))
    ).reset_index()
    job8_df = users_df.merge(user_b, on="user_id", how="inner")[
        ["user_id", "name", "phone_number", "loyalty_points", "total_bookings", "total_spent"]
    ].sort_values(["total_bookings", "total_spent"], ascending=[False, False]).head(5)
    print("[OUTPUT] [Job 8 Sample Output]:")
    print(job8_df.to_string(index=False))
    write_parquet_dir(job8_df, os.path.join(local_processed_dir, "top5_users"))
    print("[SUCCESS] Job 8 completed!\n")

    # Sync to HDFS if namenode container is running
    try:
        check = subprocess.run(["docker", "ps"], capture_output=True, text=True)
        if "namenode" in check.stdout:
            subprocess.run(["docker", "cp", local_processed_dir, "namenode:/tmp/my_analytics_results"], capture_output=True)
            subprocess.run(["docker", "exec", "namenode", "hdfs", "dfs", "-mkdir", "-p", hdfs_processed_base], capture_output=True)
            for j in [
                "total_bookings_per_event", "occupancy_percentage_per_event", "total_revenue_per_event",
                "available_seats_per_event", "top5_events", "bookings_by_category", "bookings_by_date", "top5_users"
            ]:
                src_path = "/tmp/my_analytics_results/" + str(j)
                dst_path = str(hdfs_processed_base) + "/" + str(j)
                subprocess.run(["docker", "exec", "namenode", "hdfs", "dfs", "-put", "-f", src_path, dst_path], capture_output=True)
            subprocess.run(["docker", "exec", "namenode", "rm", "-rf", "/tmp/my_analytics_results"], capture_output=True)
    except Exception:
        pass

    print("[COMPLETE] All 8 active batch jobs executed successfully!")


def main():
    if HAS_SPARK:
        try:
            run_pyspark_pipeline()
            return
        except Exception:
            pass
    run_local_pipeline()


if __name__ == "__main__":
    main()