# Distributed Ticket Reservation System - Part 2: Batch Analytics Specification

## 1. Executive Summary

Part 2 implements the distributed **Batch Processing & Analytics Layer** for the Distributed Ticket Reservation System. Utilizing input datasets stored in HDFS (`/cinema/csv/` and `/cinema/json/`), the batch engine runs **8 parallel aggregation and multi-dataset analytical jobs**, writing clean, idempotent analytical reports back to `/cinema/analytics/` in both CSV and JSON formats.

---

## 2. Cluster & Parallel Processing Architecture

- **Distributed Master/Worker Topology**: Designed to execute across the 1 Master Node (`ticket-master-node`) and 10 Worker Nodes (`ticket-worker-node-1..10`).
- **Parallel Partitioning**: Multi-dataset joins and aggregations leverage key-based partitioning on `event_id`, `user_id`, `genre`, and `hall_name` to eliminate master-node bottlenecks.
- **Idempotency & Re-runnability**: All jobs operate in deterministic overwrite mode (`mode("overwrite")`), ensuring safe re-execution without duplicating records.
- **Dual-Storage Synchronization**: Reports are written to HDFS paths under `/cinema/analytics/` and mirrored to local storage for downstream Part 4 dashboards.

---

## 3. Specification of the 8 Batch Analytics Jobs

### 📊 Job 1: Seat Occupancy Percentage per Event
- **Input Datasets**: `events` (screenings) + `seats` (seat reservations)
- **Multi-Dataset Join**: `events.event_id == seats.event_id` (Left Outer Join)
- **Formula**:
  $$\text{Total Capacity} = \text{Booked Seats} + \text{Available Seats}$$
  $$\text{Occupancy Rate (\%)} = \left(\frac{\text{Booked Seats}}{\text{Total Capacity}}\right) \times 100$$
- **HDFS Output Path**: `/cinema/analytics/seat_occupancy_per_event/`
- **Output Schema**: `event_id`, `movie_title`, `hall_name`, `screen_time`, `booked_seats`, `available_seats`, `total_capacity`, `occupancy_percentage`

---

### 📊 Job 2: Available Seats & Capacity Status per Event
- **Input Datasets**: `events`
- **Logic**: Evaluates real-time seat availability and classifies status (`SOLD OUT`, `LIMITED AVAILABILITY`, `AVAILABLE`).
- **HDFS Output Path**: `/cinema/analytics/available_seats_per_event/`
- **Output Schema**: `event_id`, `movie_title`, `screen_time`, `hall_name`, `available_seats`, `availability_status`

---

### 📊 Job 3: Booking Statistics by Event Category (Genre)
- **Input Datasets**: `events` + `seats`
- **Multi-Dataset Join**: `events.event_id == seats.event_id`
- **Aggregations**: `total_bookings`, `total_revenue`, `avg_ticket_price`, `min_ticket_price`, `max_ticket_price`
- **HDFS Output Path**: `/cinema/analytics/booking_stats_by_category/`
- **Output Schema**: `genre`, `total_bookings`, `total_revenue`, `avg_ticket_price`, `min_ticket_price`, `max_ticket_price`

---

### 🏆 Job 4: Top 5 Users by Total Bookings & Spend (Ranked/Sorted)
- **Input Datasets**: `users` + `seats`
- **Multi-Dataset Join**: `users.user_id == seats.user_id`
- **Rank/Sort Logic**: `ORDER BY total_bookings DESC, total_spent DESC LIMIT 5`
- **HDFS Output Path**: `/cinema/analytics/top_5_users/`
- **Output Schema**: `rank`, `user_id`, `name`, `phone_number`, `loyalty_points`, `total_bookings`, `total_spent`, `avg_spend_per_ticket`

---

### 🏆 Job 5: Top 5 Highest-Grossing Events/Movies (Ranked/Sorted)
- **Input Datasets**: `events` + `seats`
- **Multi-Dataset Join**: `events.event_id == seats.event_id`
- **Rank/Sort Logic**: `ORDER BY total_box_office_revenue DESC, tickets_sold DESC LIMIT 5`
- **HDFS Output Path**: `/cinema/analytics/top_5_grossing_events/`
- **Output Schema**: `rank`, `event_id`, `movie_title`, `hall_name`, `screen_time`, `tickets_sold`, `total_box_office_revenue`

---

### 📊 Job 6: Revenue & Ticket Demand by Auditorium Hall
- **Input Datasets**: `events` + `seats`
- **Multi-Dataset Join**: `events.event_id == seats.event_id`
- **Aggregations**: `total_screenings`, `total_tickets_sold`, `total_revenue`, `avg_revenue_per_screening`
- **HDFS Output Path**: `/cinema/analytics/revenue_by_auditorium/`
- **Output Schema**: `hall_name`, `total_screenings`, `total_tickets_sold`, `total_revenue`, `avg_revenue_per_screening`

---

### 📊 Job 7: User Loyalty Tier & Engagement Analysis
- **Input Datasets**: `users` + `seats`
- **Multi-Dataset Join**: `users.user_id == seats.user_id`
- **Segmentation**:
  - `Platinum`: 400+ points
  - `Gold`: 250 - 399 points
  - `Silver`: 100 - 249 points
  - `Bronze`: < 100 points
- **HDFS Output Path**: `/cinema/analytics/loyalty_tier_analytics/`
- **Output Schema**: `loyalty_tier`, `total_users_in_tier`, `total_bookings`, `total_revenue`, `avg_spend_per_user`, `avg_ticket_price`

---

### 📊 Job 8: Time-Series Screening Demand Distribution
- **Input Datasets**: `events` + `seats`
- **Multi-Dataset Join**: `events.event_id == seats.event_id`
- **Classification**: Morning (<12:00), Afternoon (12:00-16:59), Evening (17:00-20:59), Late Night (21:00+)
- **HDFS Output Path**: `/cinema/analytics/demand_distribution/`
- **Output Schema**: `time_slot`, `screening_hour`, `total_screenings`, `total_tickets_sold`, `slot_revenue`

---

## 4. Execution & Quickstart

To run all 8 batch analytics jobs:
```bash
python scripts/run_batch_analytics.py --job all
```

To run a specific job (e.g. Job 4 - Top 5 Users):
```bash
python scripts/run_batch_analytics.py --job job4
```

To verify and audit all output reports:
```bash
python scripts/verify_batch_analytics.py
```
