"""
Flask REST API Server for Cinema Ticket Reservation System (Part 3 & 4).
Exposes real-time endpoints for event queries, seat map availability,
concurrent seat booking, and cancellation with CORS enabled.
"""

import os
import sys
import logging

# Ensure project root is in sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from flask import Flask, request, jsonify
from flask_cors import CORS

from src.backend.booking_service import booking_service
from src.backend.hdfs_sync import hdfs_sync_manager

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("api_server")

app = Flask(__name__)
# Enable CORS for all routes to allow seamless communication with Person B's frontend
CORS(app, resources={r"/api/*": {"origins": "*"}}, supports_credentials=True)


@app.route("/", methods=["GET"])
def index():
    """Health check and API overview."""
    return jsonify({
        "status": "online",
        "service": "Cinema Ticket Reservation System API (Part 3)",
        "version": "1.0.0",
        "endpoints": {
            "GET /api/events": "List all active movie events",
            "GET /api/events/<event_id>/seats": "Get seat layout and live availability",
            "POST /api/book": "Book a specific seat (JSON: user_id, event_id, seat_number)",
            "POST /api/cancel": "Cancel a booking (JSON: ticket_id, user_id, event_id, seat_number)",
            "GET /api/users": "List all registered users",
            "GET /api/tickets": "List tickets (optionally filter by ?user_id=)",
            "GET /api/audit-logs": "List recent HDFS audit transactions",
            "POST /api/reset": "Reset in-memory state to baseline fixtures"
        }
    }), 200


@app.route("/api/health", methods=["GET"])
def health_check():
    """API health status endpoint."""
    return jsonify({
        "status": "healthy",
        "events_count": len(booking_service.get_events()),
        "users_count": len(booking_service.get_users())
    }), 200


@app.route("/api/events", methods=["GET"])
def get_events():
    """Returns list of active movie events with current remaining seat counts."""
    events = booking_service.get_events()
    return jsonify({
        "status": "success",
        "count": len(events),
        "events": events
    }), 200


@app.route("/api/events/<int:event_id>/seats", methods=["GET"])
def get_event_seats(event_id):
    """Returns the current state of all seats in the auditorium for an event."""
    result = booking_service.get_event_seats(event_id)
    if result.get("status") == "error":
        return jsonify(result), 404
    return jsonify(result), 200


@app.route("/api/book", methods=["POST"])
def book_seat():
    """
    Handles real-time seat reservation.
    JSON payload:
    {
        "user_id": int or str (required),
        "event_id": int (required),
        "seat_number": str (required),
        "price": float (optional, default 15.0)
    }
    """
    data = request.get_json(silent=True) or {}

    user_id = data.get("user_id")
    event_id = data.get("event_id")
    seat_number = data.get("seat_number")
    price = data.get("price", 15.0)

    if user_id is None or event_id is None or not seat_number:
        return jsonify({
            "status": "error",
            "message": "Missing required fields: user_id, event_id, and seat_number are required."
        }), 400

    result, status_code = booking_service.book_seat(
        user_id=user_id,
        event_id=event_id,
        seat_number=seat_number,
        price=price
    )
    return jsonify(result), status_code


@app.route("/api/cancel", methods=["POST"])
def cancel_booking():
    """
    Handles booking cancellation.
    JSON payload:
    {
        "ticket_id": str (optional if event_id & seat_number provided),
        "user_id": int or str (optional),
        "event_id": int (optional if ticket_id provided),
        "seat_number": str (optional if ticket_id provided)
    }
    """
    data = request.get_json(silent=True) or {}

    ticket_id = data.get("ticket_id")
    user_id = data.get("user_id")
    event_id = data.get("event_id")
    seat_number = data.get("seat_number")

    if not ticket_id and (event_id is None or not seat_number):
        return jsonify({
            "status": "error",
            "message": "Must provide either ticket_id or (event_id and seat_number)."
        }), 400

    result, status_code = booking_service.cancel_booking(
        ticket_id=ticket_id,
        user_id=user_id,
        event_id=event_id,
        seat_number=seat_number
    )
    return jsonify(result), status_code


@app.route("/api/users", methods=["GET"])
def get_users():
    """Returns list of registered users for frontend selector."""
    users = booking_service.get_users()
    return jsonify({
        "status": "success",
        "count": len(users),
        "users": users
    }), 200


@app.route("/api/tickets", methods=["GET"])
def get_tickets():
    """Returns list of tickets, optionally filtered by ?user_id="""
    user_id = request.args.get("user_id")
    tickets = booking_service.get_tickets(user_id=user_id)
    return jsonify({
        "status": "success",
        "count": len(tickets),
        "tickets": tickets
    }), 200


@app.route("/api/audit-logs", methods=["GET"])
def get_audit_logs():
    """Returns recent HDFS transaction audit logs."""
    limit = request.args.get("limit", default=100, type=int)
    logs = hdfs_sync_manager.get_audit_logs(limit=limit)
    return jsonify({
        "status": "success",
        "count": len(logs),
        "audit_logs": logs
    }), 200


@app.route("/api/reset", methods=["POST"])
def reset_state():
    """Resets system state back to initial baseline fixtures."""
    booking_service.reset_state()
    return jsonify({
        "status": "success",
        "message": "System state reset successfully."
    }), 200


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    host = os.environ.get("HOST", "0.0.0.0")
    print(f"[SERVER] Starting Cinema Ticket Reservation API Server on http://{host}:{port}")
    app.run(host=host, port=port, debug=False, threaded=True)
