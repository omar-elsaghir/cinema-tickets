# 🎨 Developer Guide: Person B (Frontend Web UI & Visualization - Part 4)

---

## 🎯 Role Overview
As **Person B**, you are responsible for **Part 4: Web UI (Local Host Website)**. Your code provides an interactive, responsive web interface that connects to Person A's booking service, displays movie screening schedules, renders an interactive auditorium seating chart, and handles seat reservations and cancellations in real time.

---

## 📁 Files Owned by Person B
You work **strictly** inside these paths to avoid merge conflicts with Person A:
```text
distributed_ticket_reservation_system/
└── src/
    └── frontend/
        ├── index.html         # Main semantic HTML structure & layout
        ├── style.css          # Modern dark/light theme styling & seating chart CSS
        └── app.js             # API Client, event handlers, & dynamic DOM rendering
```

---

## 🛠️ Step-by-Step Implementation Plan

### Step 1: UI Layout & Components (`src/frontend/index.html`)
Build a modern single-page dashboard containing:
1. **Header & System Status Bar**: Displays backend connection status (`Connected to localhost:5000`) and live cluster metrics.
2. **Event & Customer Selection Bar**:
   - Movie Event Dropdown (shows title, screening time, hall, remaining seats).
   - User Dropdown (select from registered cinema guests like *Alice Martinez*, *Bob Davis*).
3. **Auditorium Screen & Seating Map**:
   - Cinema screen banner ("🎬 THEATER SCREEN 🎬").
   - Responsive seat grid with Row labels (A–K) and Seat columns (1–20).
4. **Action & Booking Summary Panel**:
   - Selected seat indicator and price display.
   - **"Book Selected Seat"** button.
   - **"Cancel My Booking"** button.
5. **Toast Notification System**:
   - Top-right floating banner for instant feedback.

### Step 2: Seating Chart & Color-Coded Styling (`src/frontend/style.css`)
Style seat grid items with distinct, accessible color indicators:
- 🟢 **Available Seat** (`.seat.available`): Green gradient, hover scale effect, cursor pointer.
- 🔴 **Booked Seat** (`.seat.booked`): Red/crimson gradient, disabled cursor.
- 🟡 **Selected Seat** (`.seat.selected`): Vibrant amber/yellow with glowing border.
- 🔵 **My Booked Seat** (`.seat.my-booking`): Blue/purple gradient indicating seat booked by currently selected user (clickable to cancel).

### Step 3: API Integration & Client Logic (`src/frontend/app.js`)
Connect to Person A's REST API endpoints:
1. `loadEvents()`: Fetches `GET /api/events` and populates the movie event dropdown.
2. `loadUsers()`: Fetches `GET /api/users` and populates the user selection dropdown.
3. `loadSeats(eventId)`: Fetches `GET /api/events/${eventId}/seats` and dynamically renders the seat grid with appropriate `.available` or `.booked` CSS classes.
4. `handleBookSeat()`:
   - Collects `user_id`, `event_id`, and `seat_number`.
   - Sends `POST /api/book`.
   - On Success $\rightarrow$ Displays toast `"Booking successful."` and reloads seat grid.
   - On Conflict/Failure $\rightarrow$ Displays toast `"Seat already booked."` or `"Seat unavailable."`.
5. `handleCancelSeat()`:
   - Sends `POST /api/cancel`.
   - On Success $\rightarrow$ Displays toast `"Booking cancelled."` and updates seat grid.
6. **Auto-Refresh**: Optional 5-second polling timer to automatically display bookings made by concurrent users in real time.

---

## 🚀 How to Run and Test (Person B Commands)

```bash
# Option 1: Serve directly via Python's built-in HTTP server
python -m http.server 8000 --directory src/frontend

# Option 2: Access via Flask backend once Person A starts the server
# (Person A's server hosts frontend automatically at http://localhost:5000)
```
Open your browser at `http://localhost:5000` or `http://localhost:8000`.
