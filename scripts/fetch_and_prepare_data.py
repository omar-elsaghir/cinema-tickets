import os
import pandas as pd
import kagglehub

# Download dataset from Kaggle
path = kagglehub.dataset_download("anechytailenko/cinema-dataset-practice-06-07")
print("Path to dataset files:", path)

# Read raw CSV files
movie_df = pd.read_csv(os.path.join(path, "movie.csv"))
guests_df = pd.read_csv(os.path.join(path, "guests.csv"))
sessions_df = pd.read_csv(os.path.join(path, "sessions.csv"))
tickets_df = pd.read_csv(os.path.join(path, "tickets.csv"))

# Make output folder
output_dir = "data/staging"
os.makedirs(output_dir, exist_ok=True)

# 1. Events (combine sessions and movies)
events = sessions_df.merge(movie_df, on="movie_id", how="left")
events = events.rename(columns={"session_id": "event_id", "series_title": "movie_title"})[
    ["event_id", "movie_title", "screen_time", "hall_name", "available_seats", "genre", "runtime_in_min"]
]

# 2. Users
users = guests_df.rename(columns={"guest_id": "user_id"})

# 3. Seats
seats = tickets_df.rename(columns={
    "ticket_id": "seat_id",
    "guest_id": "user_id",
    "session_id": "event_id",
    "ticket_price": "price"
})[["seat_id", "event_id", "user_id", "seat_number", "price"]]

# Save CSV and JSON versions
events.to_csv(f"{output_dir}/events.csv", index=False)
events.to_json(f"{output_dir}/events.json", orient="records", lines=True)

seats.to_csv(f"{output_dir}/seats.csv", index=False)
seats.to_json(f"{output_dir}/seats.json", orient="records", lines=True)

users.to_csv(f"{output_dir}/users.csv", index=False)
users.to_json(f"{output_dir}/users.json", orient="records", lines=True)