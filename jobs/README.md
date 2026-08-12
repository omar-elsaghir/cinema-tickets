# ⚙️ Cinema Tickets - Batch Analytics Jobs

This directory contains the PySpark application responsible for transforming raw cinema system data into actionable business intelligence.

## 📄 File Structure

- `batch_analytics.py`: The core PySpark script submitted to the YARN cluster.

## 🛠️ Technical Implementation Details

### 1. Spark Session Configuration

The application is designed to run on a distributed Hadoop YARN cluster. The `SparkSession` is initialized with `--master yarn` and deployed in `client` mode. Environment variables (`PYSPARK_PYTHON`) must be configured to point to the Python 3.8 Miniconda environment shared across all NodeManagers.

### 2. Data Ingestion

Data is ingested directly from HDFS (`hdfs://namenode:9000/ticket_system/raw/`).
The raw data consists of three relational entities stored as CSVs:

1. **`events.csv`** — Contains `event_id`, `movie_title`, `screen_time`, and category metadata.
2. **`seats.csv`** — Contains `seat_id`, `event_id`, booking status, and ticket `price`.
3. **`users.csv`** — Contains `user_id` and demographic data.

### 3. Output Format

To optimize for downstream querying and visualization, all DataFrames are written back to HDFS (`/ticket_system/processed/`) in **Snappy-compressed Parquet format**. Writes are strictly idempotent (`mode("overwrite")`) to allow the pipeline to be rerun safely.

---

## 📋 Task Assignments & Job Descriptions

The analytics pipeline requires executing 8 specific business queries. The workload is split between team members.

### 🟢 Odd Jobs (Completed)

- **Job 1: Total Bookings per Event**
  - _Logic:_ Joins `events` and `seats`, groups by `event_id`, and counts total valid seat IDs. Includes events with 0 bookings using a `left` join and `na.fill()`.
- **Job 3: Total Revenue per Event**
  - _Logic:_ Groups `seats` by event and calculates the sum of the `price` column, rounded to 2 decimal places.
- **Job 5: Top 5 Most-Booked Events**
  - _Logic:_ Reuses the DataFrame from Job 1, applies an `orderBy` descending sort on `total_bookings`, and applies a `.limit(5)` transformation.
- **Job 7: Booking Statistics by Date**
  - _Logic:_ Parses the `screen_time` timestamp into a standard Date type. Groups total ticket counts and revenue sums by this distinct daily date.

### 🟡 Even Jobs (Pending - Assigned to Omar)

- **Job 2: Seat Occupancy Percentage per Event**
  - _Goal:_ Calculate `(Booked Seats / Total Available Seats) * 100` for each event.
- **Job 4: Number of Available Seats per Event**
  - _Goal:_ Filter the `seats` DataFrame for unbooked/empty status and group by event.
- **Job 6: Booking Statistics by Event Category**
  - _Goal:_ Join `events` and `seats`, grouping by the event genre/category to find the most popular movie types.
- **Job 8: Top 5 Users by Number of Bookings**
  - _Goal:_ Join `users` and `seats` (if user linkage exists) or parse transaction logs to rank the top 5 most active customers.

---

## ⚠️ Known Cluster Quirks & Troubleshooting

- **Missing NodeManagers:** The default `bde2020/hadoop-datanode` images do _not_ start the YARN NodeManager by default. If the job hangs in the `ACCEPTED` state, you must start the NodeManagers manually: `yarn --daemon start nodemanager`.
- **Python Version Mismatch:** The default Debian Stretch images contain Python 3.5. PySpark requires >= 3.6. Ensure Miniconda (Python 3.8) is distributed to `/opt/conda` on all worker nodes prior to running this script.
