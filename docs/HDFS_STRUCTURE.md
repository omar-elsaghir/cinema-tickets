# Distributed Ticket Reservation System - HDFS Architecture & Folder Structure Documentation

## 1. Cluster Architecture Overview

The system runs on a **Distributed Hadoop (HDFS) / Spark Cluster Topology** designed to scale across multiple physical or virtual nodes to ensure fault tolerance, high availability, and parallel throughput:

- **1 Master Node (`namenode` / `ticket-master-node`)**:
  - Coordinates cluster metadata, namespace management, and block placement.
  - Exposes RPC endpoint at `hdfs://namenode:9000` and WebHDFS REST interface at `http://namenode:9870`.
- **10 Worker Nodes (`datanode-1` to `datanode-10`)**:
  - Store data blocks across independent persistent volumes.
  - Process distributed batch and streaming computations in parallel without bottlenecking the Master Node.
- **Replication Policy (`dfs.replication` = 3)**:
  - Every block written to HDFS is replicated across 3 separate worker nodes.
  - Guarantees zero data loss even if up to 2 worker nodes fail simultaneously.
- **Block Size (`dfs.blocksize` = 128MB)**:
  - Standard block allocation optimized for parallel Spark/MapReduce reader splits.

---

## 2. HDFS Directory Hierarchy

Data is organized into format-partitioned raw ingestion directories under `/cinema/raw/`:

```
/cinema/
└── raw/
    ├── csv/
    │   ├── movie/
    │   │   └── movie.csv
    │   ├── guests/
    │   │   └── guests.csv
    │   ├── sessions/
    │   │   └── sessions.csv
    │   └── tickets/
    │       └── tickets.csv
    └── json/
        ├── movie/
        │   └── movie.json
        ├── guests/
        │   └── guests.json
        ├── sessions/
        │   └── sessions.json
        └── tickets/
            └── tickets.json
```

---

## 3. Data Schema Specifications & Domain Models

### 3.1 `movie` (Core Catalog / Events)
- **HDFS Path**: `/cinema/raw/<format>/movie/movie.<format>`
- **Description**: Stores metadata and box-office financials for the movie catalog.

| Field Name | Type | Description | Constraints / Notes |
| :--- | :--- | :--- | :--- |
| `movie_id` | INT | Unique identifier for movie (Primary Key) | Sequential ranking (1..100) |
| `poster_link` | TEXT | Absolute URL to official movie poster asset | |
| `series_title` | VARCHAR(255) | Official public title of the film | NOT NULL |
| `released_year` | INT | Theatrical release year | |
| `runtime_in_min` | INT | Duration in minutes | |
| `genre` | VARCHAR(150) | Categorical genres | Comma-separated |
| `overview` | TEXT | Detailed narrative summary | |
| `revenue` | NUMERIC(15,2)| Worldwide gross revenue | |
| `credits` | JSONB / TEXT | Multi-tiered object containing director & cast array | e.g. `{"director": "Nolan", "cast": ["DiCaprio"]}` |

---

### 3.2 `guests` (Customer / User Base)
- **HDFS Path**: `/cinema/raw/<format>/guests/guests.<format>`
- **Description**: Models registered cinema users and loyalty program account balances.

| Field Name | Type | Description | Constraints / Notes |
| :--- | :--- | :--- | :--- |
| `guest_id` | INT | Unique guest account number (Primary Key) | Sequential ranking |
| `name` | VARCHAR(100) | Full name of cinema client | NOT NULL |
| `phone_number` | VARCHAR(30) | Standardized E.164 phone string | UNIQUE, NOT NULL |
| `loyalty_points`| INT | Accumulated unspent loyalty points balance | **CHECK (`loyalty_points >= 0`)** |

---

### 3.3 `sessions` (Infrastructure & Screening Schedules)
- **HDFS Path**: `/cinema/raw/<format>/sessions/sessions.<format>`
- **Description**: Connects movies to physical auditorium halls and screen time blocks.

| Field Name | Type | Description | Constraints / Notes |
| :--- | :--- | :--- | :--- |
| `session_id` | INT | Unique tracking key for scheduled screening event (PK) | |
| `movie_id` | INT | Reference pointing to `movie.movie_id` | Foreign Key (REFERENCES `movie`) |
| `screen_time` | TIMESTAMP | Scheduled screening time (`YYYY-MM-DD HH:MM:SS`) | NOT NULL |
| `hall_name` | VARCHAR(100) | Physical auditorium (e.g. IMAX, VIP Hall 4) | NOT NULL |
| `available_seats`| INT | Current balance of remaining unreserved seats | **CHECK (`available_seats >= 0`)** |

---

### 3.4 `tickets` (Transaction Ledger / Reservations)
- **HDFS Path**: `/cinema/raw/<format>/tickets/tickets.<format>`
- **Description**: Audit fact table establishing proof-of-purchase allocations.

| Field Name | Type | Description | Constraints / Notes |
| :--- | :--- | :--- | :--- |
| `ticket_id` | INT | Sequential ticket receipt audit number (Primary Key) | |
| `session_id` | INT | Reference pointing to `sessions.session_id` | Foreign Key (REFERENCES `sessions`) |
| `guest_id` | INT | Reference pointing to `guests.guest_id` | Foreign Key (REFERENCES `guests`) |
| `seat_number` | VARCHAR(10) | Seat coordinates in auditorium grid (e.g. G12, A4) | NOT NULL |
| `ticket_price` | DECIMAL(10,2)| Monetary cost processed during checkout transaction | **CHECK (`ticket_price >= 0`)** |

---

## 4. Downstream Pipeline Integration

The HDFS storage layer established in Part 1 serves as the single source of truth for:
- **Part 2 (Distributed Data Processing)**: PySpark / MapReduce ETL workflows reading `/cinema/raw/` to calculate revenue aggregations, seat occupancy rates, and user engagement metrics.
- **Part 3 (Data Analytics & Concurrency Engine)**: ACID transaction management engines handling concurrent ticket reservations against seat availability limits.
- **Part 4 (Presentation & Dashboard Layer)**: Real-time UI rendering active sessions, reserved seating charts, and guest loyalty point balances.
