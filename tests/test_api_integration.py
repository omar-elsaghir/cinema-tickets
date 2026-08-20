import os
import sys
import threading
import time
import json
import urllib.request
import urllib.error

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src", "backend"))
from app import app

def run_integration_test():
    print("==================================================================")
    print("       END-TO-END REST API & UI INTEGRATION TEST                  ")
    print("==================================================================")

    # Start Flask API in a test thread on port 5088
    server_thread = threading.Thread(target=lambda: app.run(host="127.0.0.1", port=5088, debug=False))
    server_thread.daemon = True
    server_thread.start()
    time.sleep(1.2)

    base_url = "http://127.0.0.1:5088"

    # 1. Test GET /
    with urllib.request.urlopen(f"{base_url}/") as resp:
        html = resp.read().decode("utf-8")
        assert "<title>CinemaPass" in html, "Failed to serve UI index.html"
        print("[OK] 1. GET / -> Served Frontend HTML Website successfully.")

    # 2. Test GET /api/events
    with urllib.request.urlopen(f"{base_url}/api/events") as resp:
        events = json.loads(resp.read().decode("utf-8"))
        assert len(events) > 0, "No events returned"
        print(f"[OK] 2. GET /api/events -> Retrieved {len(events)} active movie screening events.")

    # 3. Test GET /api/events/53/seats (or event 1)
    with urllib.request.urlopen(f"{base_url}/api/events/53/seats") as resp:
        seats_data = json.loads(resp.read().decode("utf-8"))
        title = seats_data.get("movie_title", "Movie")
        num_seats = len(seats_data["seats"])
        print(f"[OK] 3. GET /api/events/53/seats -> Retrieved {num_seats} auditorium seats for '{title}'.")

    # 4. Test POST /api/book
    book_payload = json.dumps({"user_id": 1, "event_id": 53, "seat_number": "J10"}).encode("utf-8")
    req = urllib.request.Request(f"{base_url}/api/book", data=book_payload, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req) as resp:
        book_res = json.loads(resp.read().decode("utf-8"))
        ticket_id = book_res["ticket_id"]
        assert book_res["message"] == "Booking successful.", "Unexpected booking message"
        print(f"[OK] 4. POST /api/book -> Seat J10 booked (Ticket #{ticket_id}, Message: '{book_res['message']}').")

    # 5. Test Double-Booking Rejection (409 Conflict)
    try:
        req_dup = urllib.request.Request(f"{base_url}/api/book", data=book_payload, headers={"Content-Type": "application/json"})
        urllib.request.urlopen(req_dup)
        assert False, "Double-booking should have thrown HTTP 409 Conflict"
    except urllib.error.HTTPError as err:
        assert err.code == 409, f"Expected HTTP 409, got {err.code}"
        err_data = json.loads(err.read().decode("utf-8"))
        print(f"[OK] 5. Double-Booking Prevention -> Successfully rejected duplicate booking with HTTP 409: '{err_data['message']}'.")

    # 6. Test POST /api/cancel
    cancel_payload = json.dumps({"ticket_id": ticket_id, "user_id": 1, "event_id": 53, "seat_number": "J10"}).encode("utf-8")
    req_cancel = urllib.request.Request(f"{base_url}/api/cancel", data=cancel_payload, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req_cancel) as resp:
        cancel_res = json.loads(resp.read().decode("utf-8"))
        assert cancel_res["message"] == "Booking cancelled.", "Unexpected cancellation message"
        print(f"[OK] 6. POST /api/cancel -> Booking #{ticket_id} cancelled (Message: '{cancel_res['message']}').")

    print("\n==================================================================")
    print(" [INTEGRATION TEST PASSED] Full REST API & UI Pipeline Verified! ")
    print("==================================================================")
    return True

if __name__ == "__main__":
    run_integration_test()
