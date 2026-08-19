"""
Flask REST API Server for Cinema Ticket Reservation System (Part 3 & 4).
Exposes real-time endpoints for event queries, seat map availability,
concurrent seat booking, and cancellation with CORS enabled, and serves the Web UI.
"""

import os
import sys
import logging

# Ensure project root is in sys.path
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, BASE_DIR)

from flask import Flask, request, jsonify, send_from_directory
from src.backend.booking_service import booking_service
from src.backend.hdfs_sync import hdfs_sync_manager

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("api_server")

FRONTEND_DIR = os.path.join(BASE_DIR, "src", "frontend")
app = Flask(__name__, static_folder=FRONTEND_DIR)

# Optional flask_cors fallback to manual headers
try:
    from flask_cors import CORS
    CORS(app, resources={r"/*": {"origins": "*"}}, supports_credentials=True)
except ImportError:
    pass

@app.after_request
def add_cors_headers(response):
    """Ensure CORS is present on every single response."""
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type,Authorization"
    response.headers["Access-Control-Allow-Methods"] = "GET,POST,OPTIONS"
    return response

# ==============================================================================
# FRONTEND STATIC UI ROUTES (Part 4)
# ==============================================================================
@app.route("/", methods=["GET"])
def serve_index():
    """Serve the local host website UI."""
    return send_from_directory(FRONTEND_DIR, "index.html")

@app.route("/<path:path>", methods=["GET"])
def serve_static_files(path):
    """Serve static files (style.css, app.js, etc.)."""
    if os.path.exists(os.path.join(FRONTEND_DIR, path)):
        return send_from_directory(FRONTEND_DIR, path)
    return jsonify({"status": "error", "message": "File not found"}), 404

# ==============================================================================
# REST API ENDPOINTS (Part 3)
# ==============================================================================
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
    return jsonify(events), 200

@app.route("/api/events/<int:event_id>/seats", methods=["GET"])
def get_event_seats(event_id):
    """Returns the current state of all seats in the auditorium for an event."""
    result = booking_service.get_event_seats(event_id)
    if isinstance(result, dict) and result.get("status") == "error":
        return jsonify(result), 404
    return jsonify(result), 200

@app.route("/api/book", methods=["POST", "OPTIONS"])
def book_seat():
    """
    Handles real-time seat reservation.
    JSON payload: { "user_id": int, "event_id": int, "seat_number": str }
    """
    if request.method == "OPTIONS":
        return jsonify({"status": "ok"}), 200

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

@app.route("/api/cancel", methods=["POST", "OPTIONS"])
def cancel_booking():
    """
    Handles booking cancellation.
    JSON payload: { "ticket_id": str, "user_id": int, "event_id": int, "seat_number": str }
    """
    if request.method == "OPTIONS":
        return jsonify({"status": "ok"}), 200

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
    return jsonify(users), 200

@app.route("/api/tickets", methods=["GET"])
def get_tickets():
    """Returns list of tickets, optionally filtered by ?user_id="""
    user_id = request.args.get("user_id")
    tickets = booking_service.get_tickets(user_id=user_id) if hasattr(booking_service, 'get_tickets') else []
    return jsonify(tickets), 200

@app.route("/api/audit-logs", methods=["GET"])
def get_audit_logs():
    """Returns recent HDFS transaction audit logs."""
    limit = request.args.get("limit", default=100, type=int)
    logs = hdfs_sync_manager.get_audit_logs(limit=limit) if hasattr(hdfs_sync_manager, 'get_audit_logs') else []
    return jsonify(logs), 200

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    host = os.environ.get("HOST", "0.0.0.0")
    print(f"[SERVER] Starting Cinema Ticket Reservation API Server on http://localhost:{port}")
    app.run(host=host, port=port, debug=False, threaded=True)
