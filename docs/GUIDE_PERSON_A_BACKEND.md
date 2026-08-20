# 📘 Developer Guide: Person A (Backend & Concurrency Engine - Part 3)

---

## 🎯 Role Overview
As **Person A**, you are responsible for **Part 3: Real-Time Booking Processing & Distributed Concurrency**. Your code handles seat reservation requests, enforces concurrency locking to prevent double-booking, maintains seat availability, syncs transactions to HDFS, and provides multi-threaded stress tests.

---

## 📁 Files Owned by Person A
You work **strictly** inside these paths to avoid merge conflicts with Person B:
```text
distributed_ticket_reservation_system/
├── src/
│   └── backend/
│       ├── app.py                 # Flask REST API server exposing endpoints
│       ├── booking_service.py     # Core business logic (Book, Cancel, Status)
│       ├── concurrency.py         # Thread-safe locking & race condition prevention
│       └── hdfs_sync.py           # Syncs bookings/cancellations to HDFS
└── tests/
    └── test_concurrency.py        # Multi-threaded concurrent stress test suite
```

---

## 🛠️ Step-by-Step Implementation Plan

### Step 1: Concurrency Lock Manager (`src/backend/concurrency.py`)
To prevent two users from booking the same seat at the exact same millisecond:
- Implement fine-grained per-seat locks using `threading.Lock()` or an event-level mutex.
- Acquire lock $\rightarrow$ check if seat is free $\rightarrow$ update status $\rightarrow$ release lock.

### Step 2: Core Booking Service (`src/backend/booking_service.py`)
Implement the four core methods:
1. `get_events()`: Returns list of active movie events with remaining seat counts.
2. `get_event_seats(event_id)`: Returns the current state of all seats in the auditorium (Available vs Booked).
3. `book_seat(user_id, event_id, seat_number)`:
   - Acquires the seat lock.
   - If seat is already booked $\rightarrow$ return `{"status": "error", "message": "Seat already booked."}` (HTTP 409).
   - If `available_seats <= 0` $\rightarrow$ return `{"status": "error", "message": "Seat unavailable."}` (HTTP 400).
   - If free $\rightarrow$ Mark seat booked, decrement event `available_seats`, generate `ticket_id`, trigger HDFS log, and return `{"status": "success", "message": "Booking successful.", "ticket_id": <id>}`.
4. `cancel_booking(ticket_id, user_id, event_id, seat_number)`:
   - Acquires the seat lock.
   - Verifies ownership and active booking status.
   - Frees seat, increments event `available_seats`, triggers HDFS cancellation log, and returns `{"status": "success", "message": "Booking cancelled."}`.

### Step 3: HDFS Storage Synchronization (`src/backend/hdfs_sync.py`)
- Automatically logs transactions into HDFS `/cinema/transactions/audit_log.csv` and `/cinema/raw/tickets/tickets.csv`.
- Supports live Docker cluster (`ticket-master-node`) and fallback to local `.hdfs_storage/`.

### Step 4: REST API Server (`src/backend/app.py`)
Expose standard REST endpoints and enable CORS so Person B's frontend can communicate:
- `GET /api/events`
- `GET /api/events/<event_id>/seats`
- `POST /api/book` (JSON: `{ user_id, event_id, seat_number }`)
- `POST /api/cancel` (JSON: `{ ticket_id, user_id, event_id, seat_number }`)
- `GET /api/users` (Provides list of users for frontend selector)

### Step 5: Automated Concurrency Test Suite (`tests/test_concurrency.py`)
Implement multi-threaded test cases:
- **Test 1 (Same Seat Race Condition)**: Spawn 50 concurrent threads attempting to book `event_id=53, seat='G12'`. Assert **exactly 1 thread succeeds (HTTP 200)** and **49 threads fail with "Seat already booked." (HTTP 409)**.
- **Test 2 (Different Seats Parallel Bookings)**: Spawn 20 concurrent threads booking seats `A1` to `A20`. Assert **all 20 threads succeed**.
- **Test 3 (Cancel & Re-book Cycle)**: Book seat $\rightarrow$ Cancel seat $\rightarrow$ Assert seat becomes available $\rightarrow$ Re-book successfully.

---

## 🚀 How to Run and Test (Person A Commands)

```bash
# 1. Start the Backend API Server
python src/backend/app.py

# 2. Run the Multi-Threaded Concurrency Test Suite
python tests/test_concurrency.py
```
