// ==============================================================================
// PERSON B FRONTEND LOGIC (Part 4: Real-Time Booking Web UI)
// Integrates seamlessly with Person A's REST API contract & Authentication
// ==============================================================================

// Resolve API base URL dynamically
const API_BASE = (window.location.protocol === "http:" || window.location.protocol === "https:") && window.location.port === "5000"
  ? ""
  : "http://127.0.0.1:5000";

let currentEventId = null;
let currentUserId = null;
let currentUsername = null;
let currentUserName = null;
let currentUserLoyalty = 0;

let selectedSeat = null;
let selectedSeatTicketId = null;
let selectedSeatPrice = 0.0;
let isSelectedSeatMyBooking = false;

let currentEventSeats = [];
let allEventsCache = [];
let allUsersCache = [];

// DOM Elements
const loginModalOverlay = document.getElementById("login-modal-overlay");
const loginForm = document.getElementById("login-form");
const loginUsernameInput = document.getElementById("login-username");
const loginPasswordInput = document.getElementById("login-password");

const headerUsername = document.getElementById("header-username");
const headerUserLoyalty = document.getElementById("header-user-loyalty");
const displayUserName = document.getElementById("display-user-name");
const logoutBtn = document.getElementById("logout-btn");
const switchUserBtn = document.getElementById("switch-user-btn");

const eventSelect = document.getElementById("event-select");
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

// ==============================================================================
// TOAST NOTIFICATIONS & STATUS
// ==============================================================================
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
    statusText.textContent = "HDFS Cluster Connected";
  } else {
    clusterStatusBadge.classList.add("offline");
    statusText.textContent = "Backend Offline (Retrying...)";
  }
}

// ==============================================================================
// AUTHENTICATION & LOGIN MANAGEMENT
// ==============================================================================
function checkLoginState() {
  const savedUser = sessionStorage.getItem("cinemapass_user");
  if (savedUser) {
    try {
      const userObj = JSON.parse(savedUser);
      setLoggedInUser(userObj.user_id, userObj.username, userObj.name, userObj.loyalty_points);
      loginModalOverlay.classList.add("hidden");
      return true;
    } catch (e) {
      sessionStorage.removeItem("cinemapass_user");
    }
  }

  // Not logged in -> Show login modal
  loginModalOverlay.classList.remove("hidden");
  if (loginUsernameInput) loginUsernameInput.focus();
  return false;
}

function setLoggedInUser(userId, username, name, loyaltyPoints = 0) {
  currentUserId = parseInt(userId);
  currentUsername = username;
  currentUserName = name || `User #${userId}`;
  currentUserLoyalty = loyaltyPoints;

  headerUsername.textContent = username;
  headerUserLoyalty.textContent = `Loyalty: ${loyaltyPoints} pts`;
  displayUserName.textContent = `${username} (${currentUserName})`;

  sessionStorage.setItem("cinemapass_user", JSON.stringify({
    user_id: currentUserId,
    username: currentUsername,
    name: currentUserName,
    loyalty_points: currentUserLoyalty
  }));

  // Re-render seats if already loaded to update 'My Bookings' highlights
  if (currentEventSeats.length > 0) {
    renderSeats(currentEventSeats);
  }
}

function handleLoginSubmit(e) {
  e.preventDefault();
  const inputUser = (loginUsernameInput.value || "").trim().toLowerCase();
  const inputPass = (loginPasswordInput.value || "").trim();

  // Validate format user<id> (e.g. user1, user2, user42, user400)
  const match = inputUser.match(/^user([1-9][0-9]*)$/);
  if (!match) {
    showToast("Invalid username! Must be formatted as user1, user2, ... user400.", "error");
    return;
  }

  // Universal password check
  if (inputPass !== "admin") {
    showToast("Invalid password! Universal password is 'admin'.", "error");
    return;
  }

  const parsedId = parseInt(match[1]);
  if (parsedId < 1 || parsedId > 400) {
    showToast("User ID must be between 1 and 400.", "error");
    return;
  }

  const userMeta = allUsersCache.find(u => parseInt(u.user_id) === parsedId);
  const realName = userMeta ? userMeta.name : `Customer ${parsedId}`;
  const points = userMeta ? (userMeta.loyalty_points || 0) : 100;

  setLoggedInUser(parsedId, inputUser, realName, points);
  loginModalOverlay.classList.add("hidden");
  showToast(`Welcome back, ${inputUser} (${realName})!`, "success");

  // Load latest events and seats
  loadEvents();
}

function handleLogout() {
  sessionStorage.removeItem("cinemapass_user");
  currentUserId = null;
  currentUsername = null;
  selectedSeat = null;
  selectedSeatTicketId = null;
  isSelectedSeatMyBooking = false;
  updateActionPanel(false);

  loginModalOverlay.classList.remove("hidden");
  loginUsernameInput.value = "";
  loginPasswordInput.value = "";
  loginUsernameInput.focus();
  showToast("Logged out successfully.", "warning");
}

// ==============================================================================
// DATA LOADING & SEAT RENDERING
// ==============================================================================

// 1. Load All Cinema Users
async function loadUsers() {
  try {
    const res = await fetch(`${API_BASE}/api/users`);
    if (!res.ok) throw new Error("API error");
    const data = await res.json();
    allUsersCache = Array.isArray(data) ? data : (data.users || data.data || []);
    
    // Update active user metadata if logged in
    if (currentUserId) {
      const u = allUsersCache.find(x => parseInt(x.user_id) === parseInt(currentUserId));
      if (u) {
        setLoggedInUser(u.user_id, currentUsername || `user${u.user_id}`, u.name, u.loyalty_points);
      }
    }
  } catch (err) {
    console.warn("User list fetch delayed until backend responds.");
  }
}

// 2. Load All Events
async function loadEvents() {
  try {
    const res = await fetch(`${API_BASE}/api/events`);
    if (!res.ok) throw new Error("API responded with error: " + res.status);
    const data = await res.json();
    
    const events = Array.isArray(data) ? data : (data.events || data.data || []);
    allEventsCache = events;
    updateConnectionStatus(true);

    if (events.length === 0) {
      eventSelect.innerHTML = `<option value="">No events available</option>`;
      return;
    }

    const prevSelected = eventSelect.value;
    eventSelect.innerHTML = "";
    events.forEach(ev => {
      const opt = document.createElement("option");
      opt.value = ev.event_id;
      const title = ev.movie_title || ev.title || "Movie";
      const hall = ev.hall_name || "Hall";
      const avail = ev.available_seats !== undefined ? ev.available_seats : (ev.total_seats ? ev.total_seats - (ev.booked_seats || 0) : 150);
      opt.textContent = `#${ev.event_id} - ${title} (${hall}) | ${avail} seats remaining`;
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
      await loadEventSeats(currentEventId);
    }
  } catch (err) {
    console.warn("Could not connect to backend at", API_BASE, err);
    updateConnectionStatus(false);
  }
}

// 3. Load Seating Grid for an Event
async function loadEventSeats(eventId) {
  if (!eventId) return;
  try {
    const res = await fetch(`${API_BASE}/api/events/${eventId}/seats`);
    if (!res.ok) throw new Error("Could not load seats: " + res.status);
    const data = await res.json();
    updateConnectionStatus(true);

    movieTitle.textContent = data.movie_title || "Cinema Screening";
    eventHall.textContent = `🏛️ ${data.hall_name || "Auditorium"}`;
    eventTime.textContent = `🕒 ${data.screen_time || "Scheduled"}`;
    
    const evMeta = allEventsCache.find(e => String(e.event_id) === String(eventId));
    if (evMeta) {
      movieGenre.textContent = `Genre: ${evMeta.genre || "Cinema"} | Runtime: ${evMeta.runtime_in_min || 120} min`;
    }

    currentEventSeats = data.seats || [];
    
    // Calculate TRUE live counts directly from the seat map
    const totalSeats = currentEventSeats.length || 150;
    const bookedSeatsCount = currentEventSeats.filter(s => {
      const st = String(s.status || "").toLowerCase();
      return st === "booked" || st === "unavailable";
    }).length;
    
    const availableSeatsCount = Math.max(0, totalSeats - bookedSeatsCount);
    const occRate = totalSeats > 0 ? ((bookedSeatsCount / totalSeats) * 100).toFixed(1) : 0.0;

    // Update real counts on UI
    eventAvail.textContent = `🎟️ Available: ${availableSeatsCount} seats`;
    eventBooked.textContent = `🔒 Booked: ${bookedSeatsCount} seats`;
    occupancyPercent.textContent = `${occRate}%`;

    renderSeats(currentEventSeats);
  } catch (err) {
    console.warn("Failed to load seats for event", eventId, err);
    updateConnectionStatus(false);
  }
}

// 4. Render Auditorium Seats (Handles Green Available, Purple My Booking, Red Booked)
function renderSeats(seats) {
  seatGrid.innerHTML = "";
  
  let activeSelectedFound = false;

  seats.forEach(s => {
    const btn = document.createElement("button");
    btn.className = "seat-btn";
    btn.textContent = s.seat_number;
    btn.dataset.seat = s.seat_number;
    btn.dataset.price = s.price || 15.0;
    btn.dataset.ticketId = s.ticket_id || "";

    const statusStr = String(s.status || "").toLowerCase();
    const isBooked = (statusStr === "booked" || statusStr === "unavailable");
    
    const bookedUserId = (s.user_id !== undefined && s.user_id !== null) 
      ? s.user_id 
      : s.booked_by_user_id;

    const isMyBooking = isBooked && currentUserId !== null && String(bookedUserId) === String(currentUserId);
    const isCurrentlySelected = selectedSeat === s.seat_number;

    if (isCurrentlySelected) {
      activeSelectedFound = true;
      isSelectedSeatMyBooking = isMyBooking;
      btn.classList.add("selected");
    } else if (isMyBooking) {
      btn.classList.add("my-booking");
      btn.title = `Your active reservation (Ticket #${s.ticket_id || s.seat_number}). Click to cancel.`;
    } else if (isBooked) {
      btn.classList.add("booked");
      btn.disabled = true;
      btn.title = `Booked by User #${bookedUserId || "N/A"}`;
    } else {
      btn.classList.add("available");
      btn.title = `Available ($${Number(s.price || 15).toFixed(2)})`;
    }

    btn.addEventListener("click", () => handleSeatClick(s, isMyBooking, isBooked));
    seatGrid.appendChild(btn);
  });

  if (!activeSelectedFound) {
    selectedSeat = null;
    selectedSeatTicketId = null;
    isSelectedSeatMyBooking = false;
    updateActionPanel(false);
  } else {
    updateActionPanel(true, isSelectedSeatMyBooking);
  }
}

// 5. Seat Click Handling
function handleSeatClick(seatData, isMyBooking, isBooked) {
  // If clicking same seat, deselect
  if (selectedSeat === seatData.seat_number) {
    selectedSeat = null;
    selectedSeatTicketId = null;
    selectedSeatPrice = 0.0;
    isSelectedSeatMyBooking = false;
    renderSeats(currentEventSeats);
    updateActionPanel(false);
    return;
  }

  // Select new seat
  selectedSeat = seatData.seat_number;
  selectedSeatTicketId = seatData.ticket_id || null;
  selectedSeatPrice = Number(seatData.price || 15.0);
  isSelectedSeatMyBooking = isMyBooking;

  renderSeats(currentEventSeats);
  updateActionPanel(true, isMyBooking);
}

function updateActionPanel(hasSelection = false, isMyBooking = false) {
  if (hasSelection && selectedSeat) {
    selectedSeatLabel.textContent = selectedSeat;
    selectedSeatPriceLabel.textContent = `$${selectedSeatPrice.toFixed(2)}`;
    
    if (isMyBooking) {
      actionModeLabel.textContent = `Ready to Cancel Booking (${selectedSeat})`;
      actionModeLabel.style.color = "#f87171";
      bookBtn.disabled = true;
      cancelBtn.disabled = false;
    } else {
      actionModeLabel.textContent = `Ready to Reserve Seat (${selectedSeat})`;
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

// ==============================================================================
// ACTIONS: BOOK SEAT & CANCEL BOOKING
// ==============================================================================

// 6. Book Seat Action (POST /api/book)
async function bookSeat() {
  if (!currentUserId) {
    showToast("Please sign in before booking.", "error");
    loginModalOverlay.classList.remove("hidden");
    return;
  }
  if (!currentEventId || !selectedSeat) return;

  bookBtn.disabled = true;
  bookBtn.textContent = "Booking...";

  try {
    const res = await fetch(`${API_BASE}/api/book`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        event_id: parseInt(currentEventId),
        user_id: parseInt(currentUserId),
        seat_number: selectedSeat,
        price: selectedSeatPrice
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
  if (!currentUserId) {
    showToast("Please sign in to manage bookings.", "error");
    loginModalOverlay.classList.remove("hidden");
    return;
  }
  if (!currentEventId || !selectedSeat) return;

  cancelBtn.disabled = true;
  cancelBtn.textContent = "Cancelling...";

  try {
    const res = await fetch(`${API_BASE}/api/cancel`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        ticket_id: selectedSeatTicketId,
        user_id: parseInt(currentUserId),
        event_id: parseInt(currentEventId),
        seat_number: selectedSeat
      })
    });

    const data = await res.json();

    if (res.status === 200) {
      showToast(data.message || "Booking cancelled.", "success");
      selectedSeat = null;
      selectedSeatTicketId = null;
      isSelectedSeatMyBooking = false;
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

// ==============================================================================
// EVENT LISTENERS & INITIALIZATION
// ==============================================================================
loginForm.addEventListener("submit", handleLoginSubmit);
logoutBtn.addEventListener("click", handleLogout);
switchUserBtn.addEventListener("click", handleLogout);

eventSelect.addEventListener("change", (e) => {
  currentEventId = e.target.value;
  selectedSeat = null;
  selectedSeatTicketId = null;
  isSelectedSeatMyBooking = false;
  loadEventSeats(currentEventId);
});

bookBtn.addEventListener("click", bookSeat);
cancelBtn.addEventListener("click", cancelBooking);

refreshBtn.addEventListener("click", () => {
  showToast("Refreshing seating status from cluster...", "warning");
  loadEventSeats(currentEventId);
  loadEvents();
});

// Periodic background auto-refresh every 5s
setInterval(() => {
  if (currentEventId && !selectedSeat) {
    loadEventSeats(currentEventId);
  }
}, 5000);

// Initialize on page load
window.addEventListener("DOMContentLoaded", async () => {
  await loadUsers();
  checkLoginState();
  await loadEvents();
});
