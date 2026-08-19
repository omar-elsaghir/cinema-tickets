# Distributed Ticket Reservation System

A high-concurrency, scalable **Distributed Ticket Reservation System** modeled on top of Apache Hadoop (HDFS) and Spark multi-node cluster architecture.

---

## 🚀 Quick Start Guide

### 1. Part 1: Data Collection & Storage
- **Fetch Online Kaggle Dataset**:
  ```bash
  python scripts/fetch_kaggle_data.py
  ```
- **Upload Dataset to HDFS Cluster**:
  ```bash
  python scripts/upload_to_hdfs.py --format all
  ```
- **Verify Stored HDFS Datasets**:
  ```bash
  python scripts/verify_hdfs_data.py --format all --engine hdfs
  ```

---

### 2. Part 2: Batch Processing & Analytics
- **Trigger All 8 Batch Analytics Jobs**:
  ```bash
  python scripts/run_batch_analytics.py --job all
  ```
- **Trigger an Individual Batch Job (e.g. Job 4 - Top 5 Users)**:
  ```bash
  python scripts/run_batch_analytics.py --job job4
  ```
- **Verify & Audit All Analytics Reports**:
  ```bash
  python scripts/verify_batch_analytics.py
  ```

---

## 📊 Summary of the 8 Batch Analytics Jobs

| Job ID | Job Title | Datasets Joined | Output Format | HDFS Path |
| :--- | :--- | :---: | :---: | :--- |
| **Job 1** | Seat Occupancy Percentage per Event | `events` + `seats` | CSV & JSON | `/cinema/analytics/seat_occupancy_per_event/` |
| **Job 2** | Available Seats & Capacity Status | `events` | CSV & JSON | `/cinema/analytics/available_seats_per_event/` |
| **Job 3** | Booking Statistics by Event Category | `events` + `seats` | CSV & JSON | `/cinema/analytics/booking_stats_by_category/` |
| **Job 4** | **Top 5 Users** by Total Bookings & Spend | `users` + `seats` | CSV & JSON | `/cinema/analytics/top_5_users/` |
| **Job 5** | **Top 5 Highest-Grossing** Events/Movies | `events` + `seats` | CSV & JSON | `/cinema/analytics/top_5_grossing_events/` |
| **Job 6** | Revenue by Auditorium Hall | `events` + `seats` | CSV & JSON | `/cinema/analytics/revenue_by_auditorium/` |
| **Job 7** | User Loyalty Tier & Engagement Analysis | `users` + `seats` | CSV & JSON | `/cinema/analytics/loyalty_tier_analytics/` |
| **Job 8** | Time-Series Demand Distribution | `events` + `seats` | CSV & JSON | `/cinema/analytics/demand_distribution/` |

---

## 🐳 Docker Cluster (1 Master + 10 Worker Nodes)
To start the 11-node HDFS cluster:
```bash
docker-compose up -d
```
- **NameNode Web UI**: `http://localhost:9870`
- **HDFS RPC Endpoint**: `hdfs://localhost:9000`
