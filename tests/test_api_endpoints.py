"""
REST API Endpoint & Service Integration Test Suite
Validates all Flask API endpoints, JSON responses, error handling,
booking/cancellation workflows, and audit logging.
"""

import os
import sys
import json
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.backend.app import app
from src.backend.booking_service import booking_service


class TestApiEndpoints(unittest.TestCase):

    def setUp(self):
        booking_service.reset_state()
        self.client = app.test_client()

    def test_01_health_check(self):
        """Test GET /api/health endpoint."""
        res = self.client.get("/api/health")
        self.assertEqual(res.status_code, 200)
        data = res.get_json()
        self.assertEqual(data["status"], "healthy")
        self.assertGreater(data["events_count"], 0)
        self.assertGreater(data["users_count"], 0)

    def test_02_get_events_list(self):
        """Test GET /api/events returns formatted events."""
        res = self.client.get("/api/events")
        self.assertEqual(res.status_code, 200)
        data = res.get_json()
        self.assertEqual(data["status"], "success")
        self.assertGreaterEqual(len(data["events"]), 100)
        first = data["events"][0]
        self.assertIn("event_id", first)
        self.assertIn("movie_title", first)
        self.assertIn("available_seats", first)

    def test_03_get_event_seats(self):
        """Test GET /api/events/<event_id>/seats returns layout and availability."""
        res = self.client.get("/api/events/53/seats")
        self.assertEqual(res.status_code, 200)
        data = res.get_json()
        self.assertEqual(data["status"], "success")
        self.assertIn("seats", data)
        self.assertIn("total_seats", data)
        self.assertIn("available_seats", data)
        self.assertIn("booked_seats_count", data)
        self.assertEqual(data["event_id"], 53)

        # Test non-existent event 404
        res_404 = self.client.get("/api/events/999999/seats")
        self.assertEqual(res_404.status_code, 404)

    def test_04_get_users(self):
        """Test GET /api/users returns registered users."""
        res = self.client.get("/api/users")
        self.assertEqual(res.status_code, 200)
        data = res.get_json()
        self.assertEqual(data["status"], "success")
        self.assertGreaterEqual(data["count"], 100)
        user = data["users"][0]
        self.assertIn("user_id", user)
        self.assertIn("name", user)

    def test_05_booking_workflow(self):
        """Test complete booking workflow via POST /api/book."""
        # 1. Book a seat
        payload = {
            "user_id": 1,
            "event_id": 53,
            "seat_number": "D10",
            "price": 18.00
        }
        res = self.client.post("/api/book", json=payload)
        self.assertEqual(res.status_code, 200)
        data = res.get_json()
        self.assertEqual(data["status"], "success")
        self.assertEqual(data["message"], "Booking successful.")
        self.assertIn("ticket_id", data)
        ticket_id = data["ticket_id"]

        # 2. Duplicate booking attempt on the same seat
        res_dup = self.client.post("/api/book", json=payload)
        self.assertEqual(res_dup.status_code, 409)
        data_dup = res_dup.get_json()
        self.assertEqual(data_dup["status"], "error")
        self.assertEqual(data_dup["message"], "Seat already booked.")

        # 3. Query user tickets
        res_tkt = self.client.get("/api/tickets?user_id=1")
        self.assertEqual(res_tkt.status_code, 200)
        tkts = res_tkt.get_json()
        user_ticket_ids = [t["ticket_id"] for t in tkts["tickets"]]
        self.assertIn(ticket_id, user_ticket_ids)

        # 4. Cancel the booking
        cancel_payload = {
            "ticket_id": ticket_id,
            "user_id": 1,
            "event_id": 53,
            "seat_number": "D10"
        }
        res_cancel = self.client.post("/api/cancel", json=cancel_payload)
        self.assertEqual(res_cancel.status_code, 200)
        data_cancel = res_cancel.get_json()
        self.assertEqual(data_cancel["status"], "success")
        self.assertEqual(data_cancel["message"], "Booking cancelled.")

        # 5. Re-book seat by different user
        rebook_payload = {
            "user_id": 2,
            "event_id": 53,
            "seat_number": "D10",
            "price": 18.00
        }
        res_rebook = self.client.post("/api/book", json=rebook_payload)
        self.assertEqual(res_rebook.status_code, 200)

    def test_06_validation_and_bad_requests(self):
        """Test API error handling on invalid requests."""
        # Missing seat_number
        res = self.client.post("/api/book", json={"user_id": 1, "event_id": 53})
        self.assertEqual(res.status_code, 400)

        # Missing cancel fields
        res = self.client.post("/api/cancel", json={})
        self.assertEqual(res.status_code, 400)

        # Non-existent ticket cancellation
        res = self.client.post("/api/cancel", json={"ticket_id": "NON_EXISTENT_ID"})
        self.assertEqual(res.status_code, 404)

    def test_07_audit_logs_and_reset(self):
        """Test GET /api/audit-logs and POST /api/reset."""
        # Book a seat to produce an audit record
        self.client.post("/api/book", json={"user_id": 5, "event_id": 70, "seat_number": "F8"})

        res_logs = self.client.get("/api/audit-logs?limit=50")
        self.assertEqual(res_logs.status_code, 200)
        logs = res_logs.get_json()
        self.assertGreater(logs["count"], 0)

        res_reset = self.client.post("/api/reset")
        self.assertEqual(res_reset.status_code, 200)
        self.assertEqual(res_reset.get_json()["status"], "success")


if __name__ == "__main__":
    unittest.main(verbosity=2)
