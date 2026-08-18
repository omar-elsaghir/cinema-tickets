## ✅ Requirements Checklist

### 1. At least 4 jobs must process data from two different datasets

- **Status:** Met (All 8 jobs do this)
- **Evidence:** Almost every job in `jobs/batch_analytics.py` joins at least two distinct relational datasets:
  - Jobs 1, 2, 3, 4, 5: Join `events.csv` + `seats.csv` (Events metadata + Booking details).
  - Job 6: Joins `events.csv` + `seats.csv` (Category classification + Bookings).
  - Job 7: Joins `events.csv` + `seats.csv` (Screening dates + Bookings).
  - Job 8: Joins `users.csv` + `seats.csv` (User profiles + Bookings).

### 2. At least 2 jobs must produce a ranked/sorted result

- **Status:** Met
- **Evidence:**
  - Job 5 (`top5_events`): Uses `.orderBy(desc("total_bookings")).limit(5)` to extract the Top 5 most-booked events.
  - Job 8 (`top5_users`): Uses `.orderBy(desc("total_bookings")).limit(5)` to extract the Top 5 most active users.
  - _(Additionally, Jobs 3, 6, and 7 also utilize sorting/ordering.)_

### 3. Jobs must read input from distributed storage and write output back to it

- **Status:** Met
- **Evidence:** The PySpark job explicitly reads inputs from HDFS (`hdfs://namenode:9000/ticket_system/raw/`) and writes all processed results back to HDFS (`hdfs://namenode:9000/ticket_system/processed/`).

### 4. Jobs should be able to run again without breaking or duplicating results if re-run

- **Status:** Met
- **Evidence:** Every dataframe write operation in your script uses `.mode("overwrite").parquet(...)`. If you re-run the script multiple times, it safely overwrites the target directories in HDFS without appending duplicate partition files or causing collision errors.

### 5. The jobs must demonstrate parallel processing across the cluster

- **Status:** Met
- **Evidence:** By submitting the job via `--master yarn` against your active multi-node YARN cluster (ResourceManager and active NodeManager containers), PySpark successfully distributed tasks across multiple executors running in parallel containers.

### 6. Provide a simple way to trigger all jobs, such as a script or command

- **Status:** Met
- **Evidence:** You have an automated bash shell script (`./scripts/run_part2.sh`) that checks the NameNode container, copies the analytics script, configures the environment, and submits the job to the cluster with a single command.

### 7. Save the output in a clear folder structure so Part 4 can access the results

- **Status:** Met
- **Evidence:** All results are written under a clean, unified root directory in HDFS (`/ticket_system/processed/`) with explicit, descriptive subfolder names — `total_bookings_per_event`, `total_revenue_per_event`, `top5_events`, `bookings_by_date`, `occupancy_percentage_per_event`, `available_seats_per_event`, `bookings_by_category`, `top5_users` — that any downstream consumer (like Part 4) can easily query.
