# ⚙️ Cinema Tickets - Batch Analytics Jobs

This directory contains the PySpark application responsible for transforming raw cinema system data into actionable business intelligence across a distributed Hadoop cluster.

## 📄 File Structure

- `batch_analytics.py`: The core PySpark script submitted to the YARN cluster.

## 🛠️ Technical Implementation Details

### 1. Spark Session Configuration

The application is designed to run on a distributed Hadoop YARN cluster. The `SparkSession` is initialized with `--master yarn` and deployed in `client` mode. Environment variables (`PYSPARK_PYTHON`) must be configured to point to the Python 3.8 Miniconda environment shared across all NodeManagers.

### 2. Data Ingestion

Data is ingested directly from HDFS (`hdfs://namenode:9000/ticket_system/raw/`).
The raw data consists of three relational entities stored as CSVs:

1. **`events.csv`** — Contains `event_id`, `movie_title`, `screen_time`, and category metadata.
2. **`seats.csv`** — Contains `seat_id`, `event_id`, booking records, and ticket `price`.
3. **`users.csv`** — Contains `user_id` and demographic data.

### 3. Output Format

To optimize for downstream querying and visualization, all DataFrames are written back to HDFS (`/ticket_system/processed/`) in **Snappy-compressed Parquet format**. Writes are strictly idempotent (`mode("overwrite")`) to allow the pipeline to be rerun safely without duplicating data or causing collision errors.

---

## 📋 Task Assignments & Job Descriptions

The analytics pipeline executes 8 comprehensive business intelligence queries in parallel.

### 🟢 Odd Jobs (Completed by Ahmed&Youssef)

- **Job 1: Total Bookings per Event**
  - _Logic:_ Joins `events` and `seats`, groups by `event_id`, and counts total valid seat IDs. Includes events with 0 bookings using a `left` join and `na.fill()`.
- **Job 3: Total Revenue per Event**
  - _Logic:_ Groups `seats` by event and calculates the sum of the `price` column, rounded to 2 decimal places.
- **Job 5: Top 5 Most-Booked Events**
  - _Logic:_ Reuses the DataFrame from Job 1, applies an `orderBy` descending sort on `total_bookings`, and applies a `.limit(5)` transformation.
- **Job 7: Booking Statistics by Date**
  - _Logic:_ Parses the `screen_time` timestamp into a standard Date type. Groups total ticket counts and revenue sums by this distinct daily date.

### 🟡 Even Jobs (Completed by Omar&Adnan)

- **Job 2: Seat Occupancy Percentage per Event**
  - _Logic:_ Calculates `(Booked Seats / Total Venue Capacity) * 100` for each event, handling zero-occupancy events gracefully.
- **Job 4: Number of Available Seats per Event**
  - _Logic:_ Computes remaining capacity based on standard venue limits minus total bookings per screening.
- **Job 6: Booking Statistics by Event Category**
  - _Logic:_ Joins `events` and `seats`, grouping by the event genre/category to aggregate total bookings and revenue by genre.
- **Job 8: Top 5 Users by Number of Bookings**
  - _Logic:_ Joins `users` and `seats`, groups by `user_id`, and ranks the top 5 most active customers by ticket volume.

---

## ⚠️ Known Cluster Quirks & Troubleshooting

- **Missing NodeManagers:** The default `bde2020/hadoop-datanode` images do _not_ start the YARN NodeManager by default. If the job hangs in the `ACCEPTED` state due to 0 cluster resources, start the NodeManagers manually: 
  ```bash
  for i in {1..3}; do docker exec -d cinema-tickets-datanode-$i yarn --daemon start nodemanager; done
