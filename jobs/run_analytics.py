import os
import subprocess
import pandas as pd
import numpy as np

staging_dir = "data/staging"
local_processed_dir = "./my_analytics_results"
hdfs_processed_base = "/ticket_system/processed"

def run_batch_analytics():
    os.makedirs(local_processed_dir, exist_ok=True)

    events_path = os.path.join(staging_dir, "events.csv")
    seats_path = os.path.join(staging_dir, "seats.csv")
    users_path = os.path.join(staging_dir, "users.csv")

    if not (os.path.exists(events_path) and os.path.exists(seats_path) and os.path.exists(users_path)):
        print("Error: Staging files not found. Run scripts/fetch_and_prepare_data.py first.")
        return

    events_df = pd.read_csv(events_path)
    seats_df = pd.read_csv(seats_path)
    users_df = pd.read_csv(users_path)

    bookings_per_event = seats_df.groupby("event_id").agg(total_bookings=("seat_id", "count")).reset_index()

    # Job 1: Total Bookings per Event
    job1_df = events_df.merge(bookings_per_event, on="event_id", how="left")
    job1_df["total_bookings"] = job1_df["total_bookings"].fillna(0).astype(int)
    job1_df = job1_df[["event_id", "movie_title", "total_bookings"]]
    job1_df.to_parquet(os.path.join(local_processed_dir, "total_bookings_per_event"), compression="snappy", index=False)

    # Job 2: Seat Occupancy Percentage per Event
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
    job2_df.to_parquet(os.path.join(local_processed_dir, "occupancy_percentage_per_event"), compression="snappy", index=False)

    # Job 3: Total Revenue per Event
    rev = seats_df.groupby("event_id").agg(total_revenue=("price", lambda x: round(x.sum(), 2))).reset_index()
    job3_df = events_df.merge(rev, on="event_id", how="left")
    job3_df["total_revenue"] = job3_df["total_revenue"].fillna(0.0)
    job3_df = job3_df[["event_id", "movie_title", "total_revenue"]]
    job3_df.to_parquet(os.path.join(local_processed_dir, "total_revenue_per_event"), compression="snappy", index=False)

    # Job 4: Number of Available Seats per Event
    job4_df = events_df[["event_id", "movie_title", "hall_name", "screen_time", "available_seats"]].sort_values("event_id")
    job4_df.to_parquet(os.path.join(local_processed_dir, "available_seats_per_event"), compression="snappy", index=False)

    # Job 5: Top 5 Most-Booked Events
    job5_df = job1_df.sort_values("total_bookings", ascending=False).head(5)
    job5_df.to_parquet(os.path.join(local_processed_dir, "top5_events"), compression="snappy", index=False)

    # Job 6: Booking Statistics by Event Category
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
    job6_df.to_parquet(os.path.join(local_processed_dir, "bookings_by_category"), compression="snappy", index=False)

    # Job 7: Booking Statistics by Date
    events_with_date = events_df.copy()
    events_with_date["event_date"] = pd.to_datetime(events_with_date["screen_time"]).dt.date.astype(str)
    date_df = events_with_date.merge(seats_df, on="event_id", how="left")
    job7_df = date_df.groupby("event_date").agg(
        total_bookings=("seat_id", "count"),
        daily_revenue=("price", lambda x: round(x.sum(), 2))
    ).reset_index().sort_values("event_date")
    job7_df["daily_revenue"] = job7_df["daily_revenue"].fillna(0.0)
    job7_df.to_parquet(os.path.join(local_processed_dir, "bookings_by_date"), compression="snappy", index=False)

    # Job 8: Top 5 Users by Number of Bookings
    user_b = seats_df.groupby("user_id").agg(
        total_bookings=("seat_id", "count"),
        total_spent=("price", lambda x: round(x.sum(), 2))
    ).reset_index()
    job8_df = users_df.merge(user_b, on="user_id", how="inner")[
        ["user_id", "name", "phone_number", "loyalty_points", "total_bookings", "total_spent"]
    ].sort_values(["total_bookings", "total_spent"], ascending=[False, False]).head(5)
    job8_df.to_parquet(os.path.join(local_processed_dir, "top5_users"), compression="snappy", index=False)

    # Upload to HDFS
    try:
        check = subprocess.run(["docker", "ps"], capture_output=True, text=True)
        if "namenode" in check.stdout:
            subprocess.run(["docker", "cp", local_processed_dir, "namenode:/tmp/my_analytics_results"], capture_output=True)
            subprocess.run(["docker", "exec", "namenode", "hdfs", "dfs", "-mkdir", "-p", hdfs_processed_base], capture_output=True)
            for j in [
                "total_bookings_per_event", "occupancy_percentage_per_event", "total_revenue_per_event",
                "available_seats_per_event", "top5_events", "bookings_by_category", "bookings_by_date", "top5_users"
            ]:
                subprocess.run(["docker", "exec", "namenode", "hdfs", "dfs", "-put", "-f", f"/tmp/my_analytics_results/{j}", f"{hdfs_processed_base}/{j}"], capture_output=True)
            subprocess.run(["docker", "exec", "namenode", "rm", "-rf", "/tmp/my_analytics_results"], capture_output=True)
    except Exception:
        pass

if __name__ == "__main__":
    run_batch_analytics()
