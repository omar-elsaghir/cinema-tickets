import os
import random
import pandas as pd
import kagglehub

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTPUT_DIR = os.path.join(BASE_DIR, "data", "staging")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# 1. Download dataset from Kaggle
print("[*] Downloading Kaggle cinema dataset...")
path = kagglehub.dataset_download("anechytailenko/cinema-dataset-practice-06-07")
print("Path to dataset files:", path)

# 2. Read raw CSV files
movie_df = pd.read_csv(os.path.join(path, "movie.csv"))
guests_df = pd.read_csv(os.path.join(path, "guests.csv"))
sessions_df = pd.read_csv(os.path.join(path, "sessions.csv"))
tickets_df = pd.read_csv(os.path.join(path, "tickets.csv"))

# 3. Events (combine sessions and movies)
events = sessions_df.merge(movie_df, on="movie_id", how="left")
events = events.rename(columns={"session_id": "event_id", "series_title": "movie_title"})[
    ["event_id", "movie_title", "screen_time", "hall_name", "available_seats", "genre", "runtime_in_min"]
]

# 4. Users (Original Kaggle Users + Generated to reach 400 users total)
users = guests_df.rename(columns={"guest_id": "user_id"}).copy()
current_user_count = len(users)
target_user_count = 400

print(f"[*] Pulled {current_user_count} original users from Kaggle.")
if target_user_count > current_user_count:
    print(f"[*] Generating {target_user_count - current_user_count} additional realistic users to reach {target_user_count} total users...")
    
    random.seed(42)  # Deterministic seed for reproducible results
    first_names = [
        "Liam", "Noah", "Oliver", "James", "Elijah", "William", "Henry", "Lucas", "Benjamin", "Theodore",
        "Mateo", "Levi", "Sebastian", "Daniel", "Jack", "Michael", "Alexander", "Owen", "Asher", "Samuel",
        "Emma", "Olivia", "Ava", "Sophia", "Isabella", "Mia", "Amelia", "Harper", "Evelyn", "Abigail",
        "Emily", "Ella", "Elizabeth", "Camila", "Luna", "Sofia", "Avery", "Mila", "Aria", "Scarlett"
    ]
    last_names = [
        "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez",
        "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin",
        "Lee", "Perez", "Thompson", "White", "Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson"
    ]
    
    generated_users = []
    start_id = users["user_id"].max() + 1
    for new_id in range(start_id, start_id + (target_user_count - current_user_count)):
        name = f"{random.choice(first_names)} {random.choice(last_names)}"
        phone = f"+1-555-{random.randint(100, 999):03d}-{random.randint(1000, 9999):04d}"
        loyalty_pts = random.randint(0, 500)
        generated_users.append({
            "user_id": new_id,
            "name": name,
            "phone_number": phone,
            "loyalty_points": loyalty_pts
        })
    
    gen_df = pd.DataFrame(generated_users)
    users = pd.concat([users, gen_df], ignore_index=True)

print(f"[OK] Total Users Finalized: {len(users)} users (IDs: {users['user_id'].min()} to {users['user_id'].max()})")

# 5. Seats (Tickets)
seats = tickets_df.rename(columns={
    "ticket_id": "seat_id",
    "guest_id": "user_id",
    "session_id": "event_id",
    "ticket_price": "price"
})[["seat_id", "event_id", "user_id", "seat_number", "price"]]

# 6. Save Staging Datasets in CSV and JSON formats
events.to_csv(os.path.join(OUTPUT_DIR, "events.csv"), index=False)
events.to_json(os.path.join(OUTPUT_DIR, "events.json"), orient="records", lines=True)

seats.to_csv(os.path.join(OUTPUT_DIR, "seats.csv"), index=False)
seats.to_json(os.path.join(OUTPUT_DIR, "seats.json"), orient="records", lines=True)

users.to_csv(os.path.join(OUTPUT_DIR, "users.csv"), index=False)
users.to_json(os.path.join(OUTPUT_DIR, "users.json"), orient="records", lines=True)

print(f"[SUCCESS] Staged datasets saved to: {OUTPUT_DIR}")
print(f"          - events.csv / events.json ({len(events)} events)")
print(f"          - users.csv / users.json   ({len(users)} users)")
print(f"          - seats.csv / seats.json   ({len(seats)} seats)")