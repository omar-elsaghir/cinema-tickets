// ==============================================================================
// PERSON B FRONTEND LOGIC (Part 4: Real-Time Booking Web UI)
// Integrates seamlessly with Person A's REST API contract
// ==============================================================================

// Resolve API base URL dynamically
const API_BASE = (window.location.protocol === "http:" || window.location.protocol === "https:") && window.location.port === "5000"
  ? ""
  : "http://127.0.0.1:5000";

let currentEventId = null;
let currentUserId = 1;
let selectedSeat = null;
let selectedSeatPrice = 0.0;
let currentEventSeats = [];
let allEventsCache = [];

// DOM Elements
const eventSelect = document.getElementById("event-select");
const userSelect = document.getElementById("user-select");
const movieTitle = document.getElementById("movie-title");
const movieGenre = document.getElementById("movie-genre");
const eventHall = document.getElementById("event-hall");
const eventTime = document.getElementById("event-time");
const eventAvail = document.getElementById("event-avail");
const eventBooked = document.getElementById("event-booked");
const occupancyPercent = document.getElementById("occupancy-percent");
const seatGrid = document.getElementById("seat-grid");
const selectedSeatLabel = document.getElementById("selected-seat-label");
const selectedSeatPriceLabel = document.getElementById("selected-seat-price");
const actionModeLabel = document.getElementById("action-mode-label");
const bookBtn = document.getElementById("book-btn");
const cancelBtn = document.getElementById("cancel-btn");
const refreshBtn = document.getElementById("refresh-btn");
const toastContainer = document.getElementById("toast-container");
const clusterStatusBadge = document.getElementById("cluster-status-badge");
const statusText = document.getElementById("status-text");

// Toast Notification Manager
function showToast(message, type = "success") {
  const toast = document.createElement("div");
  toast.className = `toast ${type}`;
  const icon = type === "success" ? "✅" : (type === "error" ? "❌" : "⚠️");
  toast.innerHTML = `<span>${icon}</span> <span>${message}</span>`;
  toastContainer.appendChild(toast);

  setTimeout(() => {
    toast.style.opacity = "0";
    toast.style.transform = "translateX(100%)";
    toast.style.transition = "all 0.3s ease";
    setTimeout(() => toast.remove(), 300);
  }, 4500);
}

function updateConnectionStatus(online = true) {
  if (online) {
    clusterStatusBadge.classList.remove("offline");
    statusText.textContent = "HDFS Multi-Node Cluster Connected";
  } else {
    clusterStatusBadge.classList.add("offline");
    statusText.textContent = "Backend Offline (Retrying...)";
  }
}

// 1. Load All Events
async function loadEvents() {
  try {
    const res = await fetch(`${API_BASE}/api/events`);
    if (!res.ok) throw new Error("API responded with error");
    const events = await res.json();
    allEventsCache = events;
    updateConnectionStatus(true);

    const prevSelected = eventSelect.value;
    eventSelect.innerHTML = "";
    events.forEach(ev => {
      const opt = document.createElement("option");
      opt.value = ev.event_id;
      opt.textContent = `#${ev.event_id} - ${ev.movie_title} (${ev.hall_name}) | ${ev.available_seats} seats remaining`;
      eventSelect.appendChild(opt);
    });

    if (prevSelected && events.some(e => String(e.event_id) === String(prevSelected))) {
      eventSelect.value = prevSelected;
      currentEventId = prevSelected;
    } else if (events.length > 0) {
      currentEventId = events[0].event_id;
      eventSelect.value = currentEventId;
    }

    if (currentEventId) {
      loadEventSeats(currentEventId);
    }
  } catch (err) {
    updateConnectionStatus(false);
    showToast("Connecting to backend booking service...", "warning");
  }
}

// 2. Load Cinema Users
async function loadUsers() {
  try {
    const res = await fetch(`${API_BASE}/api/users`);
    if (!res.ok) throw new Error("API error");
    const users = await res.json();

    const prevUser = userSelect.value;
    userSelect.innerHTML = "";
    users.forEach(u => {
      const opt = document.createElement("option");
      opt.value = u.user_id;
      opt.textContent = `User #${u.user_id}: ${u.name} (Loyalty: ${u.loyalty_points} pts)`;
      userSelect.appendChild(opt);
    });

    if (prevUser && users.some(u => String(u.user_id) === String(prevUser))) {
      userSelect.value = prevUser;
      currentUserId = prevUser;
    } else if (users.length > 0) {
      currentUserId = users[0].user_id;
      userSelect.value = currentUserId;
    }
  } catch (err) {
    console.warn("User list fetch delayed until backend responds.");
  }
}

// 3. Load Seating Grid & Real-time Metrics for Selected Event
async function loadEventSeats(eventId) {
  if (!eventId) return;
  try {
    const res = await fetch(`${API_BASE}/api/events/${eventId}/seats`);
    if (!res.ok) throw new Error("Could not load seats");
    const data = await res.json();
    updateConnectionStatus(true);

    movieTitle.textContent = data.movie_title;
    eventHall.textContent = `🏛️ ${data.hall_name}`;
    eventTime.textContent = `🕒 ${data.screen_time}`;
    eventAvail.textContent = `🎟️ Available: ${data.available_seats} seats`;

    // Find genre & runtime from cache
    const evMeta = allEventsCache.find(e => String(e.event_id) === String(eventId));
    if (evMeta) {
      movieGenre.textContent = `Genre: ${evMeta.genre || "Drama"} | Runtime: ${evMeta.runtime_in_min || 120} min`;
    }

    currentEventSeats = data.seats || [];
    
    // Calculate live occupancy stats
    const totalSeats = currentEventSeats.length || 200;
    const bookedCount = currentEventSeats.filter(s => s.status === "BOOKED").length;
    const availCount = data.available_seats !== undefined ? data.available_seats : (totalSeats - bookedCount);
    const occRate = totalSeats > 0 ? ((bookedCount / totalSeats) * 100).toFixed(1) : 0.0;

    eventBooked.textContent = `🔒 Booked: ${bookedCount} seats`;
    occupancyPercent.textContent = `${occRate}%`;

    renderSeats(currentEventSeats);
  } catch (err) {
    updateConnectionStatus(false);
  }
}

// 4. Render Interactive Seat Grid
function renderSeats(seats) {
  seatGrid.innerHTML = "";
  
  // Retain selection if seat still valid
  let activeSelectedSeatFound = false;

  seats.forEach(s => {
    const btn = document.createElement("button");
    btn.className = "seat-btn";
    btn.textContent = s.seat_number;
    btn.dataset.seat = s.seat_number;
    btn.dataset.price = s.price;
    btn.dataset.ticketId = s.ticket_id || "";
    btn.dataset.bookedBy = s.booked_by_user_id || "";

    const isBooked = s.status === "BOOKED";
    const isMyBooking = isBooked && String(s.booked_by_user_id) === String(currentUserId);
    const isCurrentlySelected = selectedSeat === s.seat_number;

    if (isCurrentlySelected) {
      activeSelectedSeatFound = true;
      btn.classList.add("selected");
    } else if (isMyBooking) {
      btn.classList.add("my-booking");
      btn.title = `Booked by you (Ticket #${s.ticket_id}). Click to Cancel.`;
    } else if (isBooked) {
      btn.classList.add("booked");
      btn.disabled = true;
      btn.title = `Booked by User #${s.booked_by_user_id}`;
    } else {
      btn.classList.add("available");
      btn.title = `Available ($${Number(s.price).toFixed(2)})`;
    }

    btn.addEventListener("click", () => handleSeatClick(s, btn));
    seatGrid.appendChild(btn);
  });

  if (!activeSelectedSeatFound) {
    selectedSeat = null;
    updateActionPanel(false);
  }
}

// 5. Seat Click Handling
function handleSeatClick(seatData, btnElement) {
  const isBooked = seatData.status === "BOOKED";
  const isMyBooking = isBooked && String(seatData.booked_by_user_id) === String(currentUserId);

  // If already selected, toggle off
  if (selectedSeat === seatData.seat_number) {
    selectedSeat = null;
    selectedSeatPrice = 0.0;
    renderSeats(currentEventSeats);
    updateActionPanel(false);
    return;
  }

  selectedSeat = seatData.seat_number;
  selectedSeatPrice = Number(seatData.price || 15.0);

  // Re-render to clear other selections and highlight current
  renderSeats(currentEventSeats);

  if (isMyBooking) {
    updateActionPanel(true, true); // (hasSelection, isMyBooking)
  } else if (!isBooked) {
    updateActionPanel(true, false);
  }
}

function updateActionPanel(hasSelection = false, isMyBooking = false) {
  if (hasSelection && selectedSeat) {
    selectedSeatLabel.textContent = selectedSeat;
    selectedSeatPriceLabel.textContent = `$${selectedSeatPrice.toFixed(2)}`;
    if (isMyBooking) {
      actionModeLabel.textContent = "Ready to Cancel Booking";
      actionModeLabel.style.color = "#f87171";
      bookBtn.disabled = true;
      cancelBtn.disabled = false;
    } else {
      actionModeLabel.textContent = "Ready to Reserve Seat";
      actionModeLabel.style.color = "#34d399";
      bookBtn.disabled = false;
      cancelBtn.disabled = true;
    }
  } else {
    selectedSeatLabel.textContent = "None";
    selectedSeatPriceLabel.textContent = "$0.00";
    actionModeLabel.textContent = "Select a seat";
    actionModeLabel.style.color = "#fbbf24";
    bookBtn.disabled = true;
    cancelBtn.disabled = true;
  }
}

// 6. Book Seat Action (POST /api/book)
async function bookSeat() {
  if (!currentEventId || !currentUserId || !selectedSeat) return;

  bookBtn.disabled = true;
  bookBtn.textContent = "Processing...";

  try {
    const res = await fetch(`${API_BASE}/api/book`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        event_id: parseInt(currentEventId),
        user_id: parseInt(currentUserId),
        seat_number: selectedSeat
      })
    });

    const data = await res.json();

    if (res.status === 200) {
      showToast(`${data.message} (Ticket #${data.ticket_id})`, "success");
      selectedSeat = null;
      await loadEventSeats(currentEventId);
      await loadEvents();
    } else if (res.status === 409) {
      showToast(data.message || "Seat already booked.", "error");
      selectedSeat = null;
      await loadEventSeats(currentEventId);
    } else {
      showToast(data.message || "Seat unavailable.", "error");
      selectedSeat = null;
      await loadEventSeats(currentEventId);
    }
  } catch (err) {
    showToast("Network error connecting to booking service.", "error");
  } finally {
    bookBtn.textContent = "✨ Book Seat";
  }
}

// 7. Cancel Booking Action (POST /api/cancel)
async function cancelBooking() {
  if (!currentEventId || !selectedSeat) return;

  const seatObj = currentEventSeats.find(s => s.seat_number === selectedSeat);
  const ticketId = seatObj ? seatObj.ticket_id : null;

  cancelBtn.disabled = true;
  cancelBtn.textContent = "Cancelling...";

  try {
    const res = await fetch(`${API_BASE}/api/cancel`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        ticket_id: ticketId,
        user_id: parseInt(currentUserId),
        event_id: parseInt(currentEventId),
        seat_number: selectedSeat
      })
    });

    const data = await res.json();

    if (res.status === 200) {
      showToast(data.message || "Booking cancelled.", "success");
      selectedSeat = null;
      await loadEventSeats(currentEventId);
      await loadEvents();
    } else {
      showToast(data.message || "Failed to cancel booking.", "error");
    }
  } catch (err) {
    showToast("Network error during cancellation.", "error");
  } finally {
    cancelBtn.textContent = "❌ Cancel Booking";
  }
}

// Event Listeners
eventSelect.addEventListener("change", (e) => {
  currentEventId = e.target.value;
  selectedSeat = null;
  loadEventSeats(currentEventId);
});

userSelect.addEventListener("change", (e) => {
  currentUserId = e.target.value;
  renderSeats(currentEventSeats);
});

bookBtn.addEventListener("click", bookSeat);
cancelBtn.addEventListener("click", cancelBooking);
refreshBtn.addEventListener("click", () => {
  showToast("Refreshing seating layout...", "warning");
  loadEventSeats(currentEventId);
});

// Periodic background auto-refresh every 5s to sync peer bookings
setInterval(() => {
  if (currentEventId && !selectedSeat) {
    loadEventSeats(currentEventId);
  }
}, 5000);

// Initialize on page load
window.addEventListener("DOMContentLoaded", () => {
  loadUsers();
  loadEvents();
});
