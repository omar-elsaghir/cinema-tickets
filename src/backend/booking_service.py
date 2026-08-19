"""
Core Booking Domain Service for Cinema Ticket Reservation System (Part 3 & 4).
Handles movie events, seat reservations, cancellations, real-time capacity tracking,
and coordinates with the Concurrency Lock Manager and HDFS Sync Manager.
"""

import os
import csv
import json
import uuid
import logging
import threading
from typing import Dict, List, Any, Optional, Tuple

from src.backend.concurrency import seat_lock_manager
from src.backend.hdfs_sync import hdfs_sync_manager

logger = logging.getLogger("booking_service")

AUDITORIUM_ROWS = ["A", "B", "C", "D", "E", "F", "G", "H", "J", "K"]  # 10 rows (A-K without I)
SEATS_PER_ROW = 20  # 20 columns (1-20 matching Kaggle cinema layout)
TOTAL_SEATS_PER_EVENT = len(AUDITORIUM_ROWS) * SEATS_PER_ROW  # 200 seats total

class BookingService:
    """
    Core Domain Service managing booking transactions and seat inventory.
    Enforces thread-safety, prevents double bookings, and maintains accurate real-time seat counts.
    """

    def __init__(self, data_dir: Optional[str] = None):
        if data_dir is None:
            project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
            data_dir = os.path.join(project_root, "data", "staging")
            if not os.path.exists(data_dir):
                data_dir = os.path.join(project_root, "scripts", "data", "staging")

        self.data_dir = data_dir
        self._state_lock = threading.RLock()

        # In-memory domain stores
        self._events: Dict[int, Dict[str, Any]] = {}
        # Key: (event_id, seat_number) -> Dict of seat info
        self._booked_seats: Dict[Tuple[int, str], Dict[str, Any]] = {}
        # Key: ticket_id -> Dict of ticket info
        self._tickets: Dict[str, Dict[str, Any]] = {}
        # Key: user_id -> Dict of user info
        self._users: Dict[int, Dict[str, Any]] = {}

        self._load_initial_data()

    def _load_initial_data(self):
        """Loads events, users, and initial booked seats from JSON/CSV files."""
        with self._state_lock:
            self._events.clear()
            self._booked_seats.clear()
            self._tickets.clear()
            self._users.clear()

            # 1. Load Events
            events_file_json = os.path.join(self.data_dir, "events.json")
            events_file_csv = os.path.join(self.data_dir, "events.csv")
            if os.path.exists(events_file_json):
                with open(events_file_json, "r", encoding="utf-8") as f:
                    for line in f:
                        if line.strip():
                            ev = json.loads(line.strip())
                            ev["event_id"] = int(ev["event_id"])
                            ev["runtime_in_min"] = int(ev.get("runtime_in_min", 120))
                            ev["total_seats"] = TOTAL_SEATS_PER_EVENT
                            ev["available_seats"] = TOTAL_SEATS_PER_EVENT
                            self._events[ev["event_id"]] = ev
            elif os.path.exists(events_file_csv):
                with open(events_file_csv, "r", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        eid = int(row["event_id"])
                        self._events[eid] = {
                            "event_id": eid,
                            "movie_title": row.get("movie_title", ""),
                            "screen_time": row.get("screen_time", ""),
                            "hall_name": row.get("hall_name", "Hall 1"),
                            "genre": row.get("genre", ""),
                            "runtime_in_min": int(row.get("runtime_in_min", 120)),
                            "total_seats": TOTAL_SEATS_PER_EVENT,
                            "available_seats": TOTAL_SEATS_PER_EVENT
                        }

            # 2. Load Users
            users_file_json = os.path.join(self.data_dir, "users.json")
            users_file_csv = os.path.join(self.data_dir, "users.csv")
            if os.path.exists(users_file_json):
                with open(users_file_json, "r", encoding="utf-8") as f:
                    for line in f:
                        if line.strip():
                            u = json.loads(line.strip())
                            uid = int(u["user_id"])
                            self._users[uid] = u
            elif os.path.exists(users_file_csv):
                with open(users_file_csv, "r", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        uid = int(row["user_id"])
                        self._users[uid] = {
                            "user_id": uid,
                            "name": row.get("name", f"User {uid}"),
                            "phone_number": row.get("phone_number", ""),
                            "loyalty_points": int(row.get("loyalty_points", 0))
                        }

            # 3. Load Existing Booked Seats
            seats_file_json = os.path.join(self.data_dir, "seats.json")
            seats_file_csv = os.path.join(self.data_dir, "seats.csv")
            if os.path.exists(seats_file_json):
                with open(seats_file_json, "r", encoding="utf-8") as f:
                    for line in f:
                        if line.strip():
                            s = json.loads(line.strip())
                            eid = int(s["event_id"])
                            seat_num = str(s["seat_number"]).strip().upper()
                            uid = int(s["user_id"]) if "user_id" in s and s["user_id"] is not None else None
                            price = float(s.get("price", 15.0))
                            ticket_id = f"TKT-{eid}-{seat_num}-{s.get('seat_id', uuid.uuid4().hex[:4])}"
                            
                            seat_record = {
                                "seat_id": s.get("seat_id"),
                                "event_id": eid,
                                "seat_number": seat_num,
                                "user_id": uid,
                                "booked_by_user_id": uid,
                                "price": price,
                                "status": "booked",
                                "ticket_id": ticket_id
                            }
                            self._booked_seats[(eid, seat_num)] = seat_record
                            self._tickets[ticket_id] = {
                                "ticket_id": ticket_id,
                                "event_id": eid,
                                "user_id": uid,
                                "seat_number": seat_num,
                                "price": price,
                                "status": "ACTIVE"
                            }
            elif os.path.exists(seats_file_csv):
                with open(seats_file_csv, "r", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        eid = int(row["event_id"])
                        seat_num = str(row["seat_number"]).strip().upper()
                        uid = int(row["user_id"]) if row.get("user_id") else None
                        price = float(row.get("price", 15.0))
                        ticket_id = f"TKT-{eid}-{seat_num}-{row.get('seat_id', uuid.uuid4().hex[:4])}"

                        seat_record = {
                            "seat_id": row.get("seat_id"),
                            "event_id": eid,
                            "seat_number": seat_num,
                            "user_id": uid,
                            "booked_by_user_id": uid,
                            "price": price,
                            "status": "booked",
                            "ticket_id": ticket_id
                        }
                        self._booked_seats[(eid, seat_num)] = seat_record
                        self._tickets[ticket_id] = {
                            "ticket_id": ticket_id,
                            "event_id": eid,
                            "user_id": uid,
                            "seat_number": seat_num,
                            "price": price,
                            "status": "ACTIVE"
                        }

    def reset_state(self):
        """Resets all booking state to the initial dataset fixtures."""
        seat_lock_manager.clear()
        self._load_initial_data()

    def get_events(self) -> List[Dict[str, Any]]:
        """Returns list of active movie events with TRUE calculated remaining seats."""
        with self._state_lock:
            # Calculate real booked count per event
            booked_counts = {}
            for (eid, _), _ in self._booked_seats.items():
                booked_counts[eid] = booked_counts.get(eid, 0) + 1

            events_list = []
            for ev in self._events.values():
                eid = ev["event_id"]
                booked = booked_counts.get(eid, 0)
                real_avail = max(0, TOTAL_SEATS_PER_EVENT - booked)
                
                ev_copy = dict(ev)
                ev_copy["total_seats"] = TOTAL_SEATS_PER_EVENT
                ev_copy["booked_seats"] = booked
                ev_copy["available_seats"] = real_avail
                ev_copy["occupancy_rate"] = round((booked / TOTAL_SEATS_PER_EVENT * 100), 1)
                events_list.append(ev_copy)

            return sorted(events_list, key=lambda x: x["event_id"])

    def get_event(self, event_id: int) -> Optional[Dict[str, Any]]:
        """Returns details for a single event."""
        with self._state_lock:
            event = self._events.get(int(event_id))
            if not event:
                return None
            
            booked = sum(1 for (eid, _) in self._booked_seats if eid == int(event_id))
            ev_copy = dict(event)
            ev_copy["total_seats"] = TOTAL_SEATS_PER_EVENT
            ev_copy["booked_seats"] = booked
            ev_copy["available_seats"] = max(0, TOTAL_SEATS_PER_EVENT - booked)
            ev_copy["occupancy_rate"] = round((booked / TOTAL_SEATS_PER_EVENT * 100), 1)
            return ev_copy

    def get_users(self) -> List[Dict[str, Any]]:
        """Returns list of all registered users."""
        with self._state_lock:
            return sorted(list(self._users.values()), key=lambda u: u["user_id"])

    def get_event_seats(self, event_id: int) -> Dict[str, Any]:
        """
        Returns full auditorium seat matrix (Rows A-K, Seats 1-20 = 200 seats)
        with TRUE calculated Available and Booked counts matching every seat on the grid.
        """
        event_id = int(event_id)
        with self._state_lock:
            event = self._events.get(event_id)
            if not event:
                return {"status": "error", "message": "Event not found.", "seats": []}

            seat_map = []
            booked_count = 0

            # Gather all known booked seats for this event
            event_booked = {
                seat_num: data
                for (eid, seat_num), data in self._booked_seats.items()
                if eid == event_id
            }

            # Generate standard 10 x 20 grid (200 seats)
            for row in AUDITORIUM_ROWS:
                for col in range(1, SEATS_PER_ROW + 1):
                    seat_num = f"{row}{col}"
                    if seat_num in event_booked:
                        booked_info = event_booked[seat_num]
                        uid = booked_info.get("user_id")
                        seat_map.append({
                            "seat_number": seat_num,
                            "row": row,
                            "col": col,
                            "status": "booked",
                            "price": booked_info.get("price", 15.0),
                            "ticket_id": booked_info.get("ticket_id"),
                            "user_id": uid,
                            "booked_by_user_id": uid
                        })
                        booked_count += 1
                    else:
                        seat_map.append({
                            "seat_number": seat_num,
                            "row": row,
                            "col": col,
                            "status": "available",
                            "price": 15.0,
                            "ticket_id": None,
                            "user_id": None,
                            "booked_by_user_id": None
                        })

            total_seats = len(seat_map)
            real_available_seats = max(0, total_seats - booked_count)
            occ_rate = round((booked_count / total_seats * 100), 1) if total_seats > 0 else 0.0

            return {
                "status": "success",
                "event_id": event_id,
                "movie_title": event.get("movie_title"),
                "screen_time": event.get("screen_time"),
                "hall_name": event.get("hall_name"),
                "total_seats": total_seats,
                "booked_seats_count": booked_count,
                "available_seats": real_available_seats,
                "occupancy_rate": occ_rate,
                "seats": seat_map
            }

    def book_seat(self, user_id: Any, event_id: Any, seat_number: str, price: float = 15.0) -> Tuple[Dict[str, Any], int]:
        """
        Thread-safe seat booking function with dynamic inventory tracking.
        """
        try:
            event_id = int(event_id)
        except (ValueError, TypeError):
            return {"status": "error", "message": "Invalid event ID."}, 400

        try:
            user_id = int(user_id) if str(user_id).isdigit() else str(user_id)
        except Exception:
            return {"status": "error", "message": "Invalid user ID."}, 400

        seat_number = str(seat_number).strip().upper()
        if not seat_number:
            return {"status": "error", "message": "Invalid seat number."}, 400

        # Fine-grained lock per (event_id, seat_number)
        with seat_lock_manager.lock_seat(event_id, seat_number):
            with self._state_lock:
                event = self._events.get(event_id)
                if not event:
                    return {"status": "error", "message": "Event not found."}, 404

                # Check if seat is already booked (Double booking check)
                seat_key = (event_id, seat_number)
                if seat_key in self._booked_seats:
                    return {"status": "error", "message": "Seat already booked."}, 409

                # Check total capacity
                current_booked = sum(1 for (eid, _) in self._booked_seats if eid == event_id)
                if current_booked >= TOTAL_SEATS_PER_EVENT:
                    return {"status": "error", "message": "Seat unavailable."}, 400

                # Generate unique ticket ID
                unique_suffix = uuid.uuid4().hex[:6].upper()
                ticket_id = f"TKT-{event_id}-{seat_number}-{unique_suffix}"

                # Update in-memory state
                seat_record = {
                    "event_id": event_id,
                    "seat_number": seat_number,
                    "user_id": user_id,
                    "booked_by_user_id": user_id,
                    "price": price,
                    "status": "booked",
                    "ticket_id": ticket_id
                }
                self._booked_seats[seat_key] = seat_record
                self._tickets[ticket_id] = {
                    "ticket_id": ticket_id,
                    "event_id": event_id,
                    "user_id": user_id,
                    "seat_number": seat_number,
                    "price": price,
                    "status": "ACTIVE"
                }

                remaining_seats = max(0, TOTAL_SEATS_PER_EVENT - (current_booked + 1))

            # Log transaction to HDFS / audit store
            try:
                hdfs_sync_manager.log_booking(
                    ticket_id=ticket_id,
                    event_id=event_id,
                    user_id=user_id,
                    seat_number=seat_number,
                    price=price
                )
            except Exception as e:
                logger.error(f"Failed to log booking to HDFS: {e}")

            return {
                "status": "success",
                "message": "Booking successful.",
                "ticket_id": ticket_id,
                "event_id": event_id,
                "seat_number": seat_number,
                "user_id": user_id,
                "price": price,
                "remaining_seats": remaining_seats
            }, 200

    def cancel_booking(
        self,
        ticket_id: Optional[str] = None,
        user_id: Optional[Any] = None,
        event_id: Optional[Any] = None,
        seat_number: Optional[str] = None
    ) -> Tuple[Dict[str, Any], int]:
        """
        Thread-safe booking cancellation function.
        """
        target_ticket_id = ticket_id
        target_event_id = int(event_id) if event_id is not None else None
        target_seat_num = str(seat_number).strip().upper() if seat_number else None
        target_user_id = int(user_id) if (user_id is not None and str(user_id).isdigit()) else user_id

        with self._state_lock:
            if target_ticket_id and target_ticket_id in self._tickets:
                t_info = self._tickets[target_ticket_id]
                target_event_id = t_info["event_id"]
                target_seat_num = t_info["seat_number"]
            elif target_event_id and target_seat_num:
                seat_key = (target_event_id, target_seat_num)
                if seat_key in self._booked_seats:
                    s_info = self._booked_seats[seat_key]
                    target_ticket_id = s_info.get("ticket_id")

        if not target_event_id or not target_seat_num:
            return {"status": "error", "message": "Booking not found or already cancelled."}, 404

        # Acquire per-seat lock for cancellation
        with seat_lock_manager.lock_seat(target_event_id, target_seat_num):
            with self._state_lock:
                seat_key = (target_event_id, target_seat_num)
                if seat_key not in self._booked_seats:
                    return {"status": "error", "message": "Booking not found or already cancelled."}, 404

                seat_info = self._booked_seats[seat_key]

                # Authorization check
                if target_user_id is not None and seat_info.get("user_id") is not None:
                    if str(seat_info.get("user_id")) != str(target_user_id):
                        return {"status": "error", "message": "Unauthorized: user does not own this ticket."}, 403

                # Free the seat
                del self._booked_seats[seat_key]

                # Update ticket record if exists
                if target_ticket_id and target_ticket_id in self._tickets:
                    self._tickets[target_ticket_id]["status"] = "CANCELLED"

                price = seat_info.get("price", 15.0)
                active_ticket_id = target_ticket_id or seat_info.get("ticket_id") or "UNKNOWN"
                current_booked = sum(1 for (eid, _) in self._booked_seats if eid == target_event_id)
                remaining = max(0, TOTAL_SEATS_PER_EVENT - current_booked)

            # Log cancellation to HDFS / audit store
            try:
                hdfs_sync_manager.log_cancellation(
                    ticket_id=active_ticket_id,
                    event_id=target_event_id,
                    user_id=target_user_id or seat_info.get("user_id") or 0,
                    seat_number=target_seat_num,
                    price=price
                )
            except Exception as e:
                logger.error(f"Failed to log cancellation to HDFS: {e}")

            return {
                "status": "success",
                "message": "Booking cancelled.",
                "ticket_id": active_ticket_id,
                "event_id": target_event_id,
                "seat_number": target_seat_num,
                "released_seat": target_seat_num,
                "remaining_seats": remaining
            }, 200

    def get_tickets(self, user_id: Optional[Any] = None) -> List[Dict[str, Any]]:
        """Returns list of active/cancelled tickets."""
        with self._state_lock:
            if user_id is not None:
                uid = int(user_id) if str(user_id).isdigit() else user_id
                return [t for t in self._tickets.values() if str(t.get("user_id")) == str(uid)]
            return list(self._tickets.values())

booking_service = BookingService()
