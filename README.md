# 🎟️ Cinema Ticket Reservation System - HDFS Data Storage & Processing

This repository manages the distributed storage and processing layer for a cinema ticket reservation platform deployed on an **11-Node Hadoop Cluster** (1 Master Node, 10 Worker DataNodes).

---

## 📌 Storage & Cluster Specification

* **Topology:** 1 NameNode (Master) + 1 YARN ResourceManager + 10 DataNodes (Workers)
* **Storage Layer:** Hadoop Distributed File System (HDFS)
* **Replication Factor:** `3` (Data blocks are mirrored across 3 DataNodes for fault tolerance)
* **HDFS Root Directory:** `/ticket_system/`

---

## 🛠️ Tech Stack Overview

* **Storage Engine:** Apache Hadoop HDFS (v3.2.1)
* **Resource Management:** Apache Hadoop YARN
* **Containerization:** Docker & Docker Compose
* **Ingestion & Verification:** Python 3 (`subprocess`, `json`, `os`)
* **Analytics Engine (Upcoming):** Apache Spark / PySpark

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

* **CSV Location:** `/ticket_system/raw/events/csv/events.csv`
* **JSON Location:** `/ticket_system/raw/events/json/events.json`
* **Schema:**
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

* **CSV Location:** `/ticket_system/raw/seats/csv/seats.csv`
* **JSON Location:** `/ticket_system/raw/seats/json/seats.json`
* **Schema:**
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

* **CSV Location:** `/ticket_system/raw/users/csv/users.csv`
* **JSON Location:** `/ticket_system/raw/users/json/users.json`
* **Schema:**
  | Field Name | Data Type | Description |
  | :--- | :--- | :--- |
  | `user_id` | Integer | Unique identifier for registered customer |
  | `name` | String | Full customer name |
  | `phone_number` | String | Contact phone number |
  | `loyalty_points` | Integer | Accumulated customer loyalty points |

---

## 🌐 Cluster Web Dashboards

* **HDFS NameNode UI:** `http://localhost:9870` (Cluster Health, DataNodes, HDFS File Explorer)
* **YARN ResourceManager UI:** `http://localhost:8088` (Node Managers, Memory/vCore Allocation, Running Jobs)

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

* Read raw CSV and JSON datasets directly from HDFS into PySpark DataFrames.
* Execute distributed SQL joins, aggregations, and data cleaning.
* Output analytical reports back to HDFS under `/ticket_system/processed/`.