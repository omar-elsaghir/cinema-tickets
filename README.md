# 🎟️ Cinema Ticket Reservation System - HDFS Data Storage & Processing

This repository manages the distributed storage and processing layer for a cinema ticket reservation platform deployed on an **11-Node Hadoop Cluster** (1 Master Node, 10 Worker DataNodes).

---

## 📌 Storage & Cluster Specification

- **Topology:** 1 NameNode (Master) + 1 YARN ResourceManager + 10 DataNodes (Workers)
- **Storage Layer:** Hadoop Distributed File System (HDFS)
- **Replication Factor:** `3` (Data blocks are mirrored across 3 DataNodes for fault tolerance)
- **HDFS Root Directory:** `/ticket_system/`

---

## 🛠️ Tech Stack Overview

- **Storage Engine:** Apache Hadoop HDFS (v3.2.1)
- **Resource Management:** Apache Hadoop YARN
- **Containerization:** Docker & Docker Compose
- **Ingestion & Verification:** Python 3 (`subprocess`, `json`, `os`)
- **Analytics Engine (Upcoming):** Apache Spark / PySpark

---

## 📂 HDFS Folder & Directory Structure

The system categorizes raw incoming data under `/ticket_system/raw/`, structured by entity type and storage format:

```text
/ticket_system/
└── raw/
    ├── events/
    │   ├── csv/
    │   │   └── events.csv
    │   └── json/
    │       └── events.json
    ├── seats/
    │   ├── csv/
    │   │   └── seats.csv
    │   └── json/
    │       └── seats.json
    └── users/
        ├── csv/
        │   └── users.csv
        └── json/
            └── users.json
```

---

## 📊 Dataset Descriptions & Schemas

### 1. 🎬 Events Dataset

Contains screening schedules, movie information, hall assignments, and capacity.

- **CSV Location:** `/ticket_system/raw/events/csv/events.csv`
- **JSON Location:** `/ticket_system/raw/events/json/events.json`
- **Schema:**
  | Field Name | Data Type | Description |
  | :--- | :--- | :--- |
  | `event_id` | Integer | Unique identifier for the screening event |
  | `movie_title` | String | Title of the featured movie |
  | `screen_time` | Timestamp | Scheduled movie date and time (`YYYY-MM-DD HH:MM:SS`) |
  | `hall_name` | String | Designated cinema auditorium (e.g., "Hall 2") |
  | `available_seats` | Integer | Remaining available capacity |
  | `genre` | String | Movie genre classification |
  | `runtime_in_min` | Integer | Movie runtime in minutes |

---

### 2. 💺 Seats Dataset

Tracks individual seat allocations, ticket pricing, and user booking reservations per event.

- **CSV Location:** `/ticket_system/raw/seats/csv/seats.csv`
- **JSON Location:** `/ticket_system/raw/seats/json/seats.json`
- **Schema:**
  | Field Name | Data Type | Description |
  | :--- | :--- | :--- |
  | `seat_id` | Integer | Primary transaction/seat record identifier |
  | `event_id` | Integer | Foreign key referencing `events.event_id` |
  | `user_id` | Integer | Foreign key referencing `users.user_id` |
  | `seat_number` | String | Alphanumeric seat code (e.g., "J20") |
  | `price` | Float | Ticket purchase price |

---

### 3. 👤 Users Dataset

Stores customer profiles and loyalty reward point tracking.

- **CSV Location:** `/ticket_system/raw/users/csv/users.csv`
- **JSON Location:** `/ticket_system/raw/users/json/users.json`
- **Schema:**
  | Field Name | Data Type | Description |
  | :--- | :--- | :--- |
  | `user_id` | Integer | Unique identifier for registered customer |
  | `name` | String | Full customer name |
  | `phone_number` | String | Contact phone number |
  | `loyalty_points` | Integer | Accumulated customer loyalty points |

---

## 🌐 Cluster Web Dashboards

- **HDFS NameNode UI:** `http://localhost:9870` (Cluster Health, DataNodes, HDFS File Explorer)
- **YARN ResourceManager UI:** `http://localhost:8088` (Node Managers, Memory/vCore Allocation, Running Jobs)

---

## 🚀 Deployment & Operations Quickstart

1. **Spin up the 11-node cluster:**

   ```bash
   docker compose up -d
   ```

2. **Fetch and ingest dataset files into HDFS:**

   ```bash
   python3 scripts/fetch_and_prepare_data.py
   python3 scripts/ingest_data.py
   ```

3. **Verify dataset integrity across HDFS:**
   ```bash
   python3 scripts/verify_data.py
   ```

---

## 🔍 Data Verification & Integrity Checks

Data integrity between local staging (`data/staging/`) and HDFS paths is validated using `scripts/verify_data.py`. The verification step enforces:

1. **Line Count Matching:** 100% parity between local source files and HDFS files.
2. **Schema & Syntax Validation:** Proper header alignment for CSV files and valid JSON object parsing.

---

## 🔮 Next Phase: Distributed Processing (Part 2)

- Read raw CSV and JSON datasets directly from HDFS into PySpark DataFrames.
- Execute distributed SQL joins, aggregations, and data cleaning.
- Output analytical reports back to HDFS under `/ticket_system/processed/`.

## 🚀 Part 2: Distributed Batch Analytics (PySpark & YARN)

The second phase of this project involves processing the raw HDFS data using **Apache Spark** running on a **10-Node YARN Cluster**.

### 🏗️ Architecture

- **Storage:** Hadoop Distributed File System (HDFS)
- **Compute:** YARN (1 ResourceManager, 10 NodeManagers)
- **Engine:** PySpark 3.2.4 (Python 3.8 via Miniconda)
- **Execution:** Client mode submission from the NameNode

### 📊 Analytical Jobs

The batch analytics pipeline is defined in `jobs/batch_analytics.py` and is divided into 8 distinct analytical jobs.

**Completed Jobs (Odd):**

- **Job 1:** Total Bookings per Event (Aggregating seat counts).
- **Job 3:** Total Revenue per Event (Summing ticket prices).
- **Job 5:** Top 5 Most-Booked Events (Sorting and limiting Job 1).
- **Job 7:** Booking Statistics by Date (Time-series aggregation of revenue and tickets).

**Pending Jobs (Even - Assigned to Omar):**

- **Job 2:** Seat Occupancy Percentage per Event.
- **Job 4:** Number of Available Seats per Event.
- **Job 6:** Booking Statistics by Event Category.
- **Job 8:** Top 5 Users by Number of Bookings.

### ⚙️ How to Run the Pipeline

Ensure your Docker cluster is running, and the DataNodes have been initialized as YARN NodeManagers.

```bash
# Submit the PySpark job to the YARN cluster
./scripts/run_part2.sh
```

The results are saved immutably back into HDFS at `/ticket_system/processed/` in compressed Parquet format.

## 📈 Data Visualization

To make the batch analytics accessible to stakeholders, the processed Parquet files are extracted from HDFS to the local machine and visualized using Pandas, Matplotlib, and Seaborn.

### Generating Charts Locally

**1. Extract the processed data from HDFS:**

```bash
docker exec namenode hdfs dfs -get /ticket_system/processed /tmp/processed_data
docker cp namenode:/tmp/processed_data ./my_analytics_results
```

**2. Run the visualization script:**

```bash
python3 visualize.py
```

This generates high-resolution `.png` charts in the project root, including:

- `chart_job1_booking_distribution.png` — Histogram of booking frequencies.
- `chart_job3_top10_revenue.png` — Horizontal bar chart of highest-grossing movies.
- `chart_job5_top_events.png` — Bar chart of the top 5 most-booked movies.
- `chart_job7_revenue_by_date.png` — Line graph of daily revenue over time.

## 📋 Job Descriptions & Technical Logic

The analytics pipeline executes 8 comprehensive business intelligence queries in parallel across the cluster:

* **Job 1: Total Bookings per Event**
  * *Logic:* Joins `events` and `seats`, groups by `event_id`, and counts total valid seat IDs. Includes events with 0 bookings using a `left` join and `na.fill()`.
* **Job 2: Seat Occupancy Percentage per Event**
  * *Logic:* Calculates `(Booked Seats / Total Venue Capacity) * 100` for each event, handling zero-occupancy events gracefully.
* **Job 3: Total Revenue per Event**
  * *Logic:* Groups `seats` by event and calculates the sum of the `price` column, rounded to 2 decimal places.
* **Job 4: Number of Available Seats per Event**
  * *Logic:* Computes remaining capacity based on standard venue limits minus total bookings per screening.
* **Job 5: Top 5 Most-Booked Events**
  * *Logic:* Reuses the DataFrame from Job 1, applies an `orderBy` descending sort on `total_bookings`, and applies a `.limit(5)` transformation.
* **Job 6: Booking Statistics by Event Category**
  * *Logic:* Joins `events` and `seats`, grouping by the event genre/category to aggregate total bookings and revenue by genre.
* **Job 7: Booking Statistics by Date**
  * *Logic:* Parses the `screen_time` timestamp into a standard Date type. Groups total ticket counts and revenue sums by this distinct daily date.
* **Job 8: Top 5 Users by Number of Bookings**
  * *Logic:* Joins `users` and `seats`, groups by `user_id`, and ranks the top 5 most active customers by ticket volume.
