"""
Multi-Threaded Concurrency Test Suite for Cinema Ticket Reservation System (Part 3).
Validates race condition prevention, fine-grained locking, high parallel throughput,
capacity constraints, and cancel/re-book lifecycles under extreme concurrent load.
"""

import os
import sys
import time
import json
import unittest
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

# Add project root to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.backend.booking_service import booking_service
from src.backend.app import app


class TestConcurrencySuite(unittest.TestCase):

    def setUp(self):
        """Reset state before each test case."""
        booking_service.reset_state()
        self.app = app.test_client()

    def test_01_same_seat_race_condition(self):
        """
        Test 1: Same Seat Race Condition (50 concurrent threads)
        Spawn 50 concurrent threads attempting to book the exact same seat (event_id=53, seat='G12').
        Assert that EXACTLY 1 thread succeeds (HTTP 200) and 49 threads fail with 'Seat already booked.' (HTTP 409).
        """
        print("\n" + "=" * 70)
        print("[TEST] Running Test 1: Same Seat Race Condition (50 Threads)")
        print("=" * 70)

        target_event_id = 53
        target_seat = "G12"
        num_threads = 50

        # Capture initial available seat count
        event_before = booking_service.get_event(target_event_id)
        initial_available = event_before["available_seats"]
        print(f"Initial available seats for Event {target_event_id}: {initial_available}")

        results = []
        start_barrier = threading.Barrier(num_threads)

        def worker_attempt(user_id: int):
            # Synchronize thread launch so all 50 threads hit the service simultaneously
            start_barrier.wait()
            res, status_code = booking_service.book_seat(
                user_id=user_id,
                event_id=target_event_id,
                seat_number=target_seat
            )
            return (user_id, res, status_code)

        start_time = time.time()
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [executor.submit(worker_attempt, i + 1) for i in range(num_threads)]
            for future in as_completed(futures):
                results.append(future.result())
        elapsed = time.time() - start_time

        successful_bookings = [r for r in results if r[2] == 200]
        already_booked_errors = [r for r in results if r[2] == 409]

        print(f"Execution time: {elapsed * 1000:.2f} ms")
        print(f"Successful bookings (HTTP 200): {len(successful_bookings)}")
        print(f"Rejected bookings (HTTP 409):   {len(already_booked_errors)}")

        # Verification & Assertions
        self.assertEqual(len(successful_bookings), 1, "Exactly ONE booking request must succeed!")
        self.assertEqual(len(already_booked_errors), 49, "Exactly 49 requests must be rejected with 409 Conflict!")

        winner_user, winner_res, _ = successful_bookings[0]
        self.assertEqual(winner_res["status"], "success")
        self.assertEqual(winner_res["message"], "Booking successful.")
        self.assertIn("ticket_id", winner_res)
        print(f"[WINNER] User {winner_user} reserved seat {target_seat} with Ticket ID: {winner_res['ticket_id']}")

        # Verify failure message format
        for _, err_res, _ in already_booked_errors:
            self.assertEqual(err_res["status"], "error")
            self.assertEqual(err_res["message"], "Seat already booked.")

        # Verify capacity decremented by exactly 1
        event_after = booking_service.get_event(target_event_id)
        self.assertEqual(
            event_after["available_seats"],
            initial_available - 1,
            "Remaining available seats must be decremented by exactly 1!"
        )
        print("[PASS] Test 1 PASSED: Zero double bookings under 50-thread concurrent race!")

    def test_02_different_seats_parallel_bookings(self):
        """
        Test 2: Different Seats Parallel Bookings (20 concurrent threads)
        Spawn 20 concurrent threads simultaneously booking distinct seats (A1 to A20).
        Assert that ALL 20 threads succeed (HTTP 200).
        """
        print("\n" + "=" * 70)
        print("[TEST] Running Test 2: Different Seats Parallel Bookings (20 Threads)")
        print("=" * 70)

        target_event_id = 91
        num_threads = 20
        seats_to_book = [f"A{i}" for i in range(1, num_threads + 1)]

        event_before = booking_service.get_event(target_event_id)
        initial_available = event_before["available_seats"]
        print(f"Initial available seats for Event {target_event_id}: {initial_available}")

        results = []
        start_barrier = threading.Barrier(num_threads)

        def worker_attempt(idx: int, seat_num: str):
            start_barrier.wait()
            res, status_code = booking_service.book_seat(
                user_id=100 + idx,
                event_id=target_event_id,
                seat_number=seat_num
            )
            return (seat_num, res, status_code)

        start_time = time.time()
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [
                executor.submit(worker_attempt, i, seats_to_book[i])
                for i in range(num_threads)
            ]
            for future in as_completed(futures):
                results.append(future.result())
        elapsed = time.time() - start_time

        successful = [r for r in results if r[2] == 200]
        failed = [r for r in results if r[2] != 200]

        print(f"Execution time: {elapsed * 1000:.2f} ms")
        print(f"Successful bookings: {len(successful)} / {num_threads}")
        print(f"Failed bookings:     {len(failed)} / {num_threads}")

        self.assertEqual(len(successful), num_threads, "All 20 distinct seat bookings must succeed!")
        self.assertEqual(len(failed), 0, "No bookings should fail for independent seats!")

        # Verify capacity decremented by exactly 20
        event_after = booking_service.get_event(target_event_id)
        self.assertEqual(
            event_after["available_seats"],
            initial_available - num_threads,
            f"Available capacity must decrease by exactly {num_threads}!"
        )
        print("[PASS] Test 2 PASSED: 100% parallel throughput on independent seats!")

    def test_03_cancel_and_rebook_cycle(self):
        """
        Test 3: Cancel and Re-book Lifecycle
        Book seat -> Verify Booked -> Cancel seat -> Verify Available -> Re-book seat -> Verify Booked.
        """
        print("\n" + "=" * 70)
        print("[TEST] Running Test 3: Cancel & Re-Book Lifecycle")
        print("=" * 70)

        event_id = 70
        seat = "B5"
        user_id = 42

        # 1. Initial Booking
        res1, code1 = booking_service.book_seat(user_id=user_id, event_id=event_id, seat_number=seat)
        self.assertEqual(code1, 200)
        self.assertEqual(res1["message"], "Booking successful.")
        ticket_id = res1["ticket_id"]
        print(f"Step 1: Booked seat {seat} (Ticket: {ticket_id})")

        # 2. Attempting to book again before cancel must fail
        res_dup, code_dup = booking_service.book_seat(user_id=99, event_id=event_id, seat_number=seat)
        self.assertEqual(code_dup, 409)
        print("Step 2: Verified duplicate booking blocked with HTTP 409")

        # 3. Cancel Booking
        res_cancel, code_cancel = booking_service.cancel_booking(
            ticket_id=ticket_id,
            user_id=user_id,
            event_id=event_id,
            seat_number=seat
        )
        self.assertEqual(code_cancel, 200)
        self.assertEqual(res_cancel["message"], "Booking cancelled.")
        print("Step 3: Booking cancelled successfully with HTTP 200")

        # 4. Re-book the same seat by another user
        res_rebook, code_rebook = booking_service.book_seat(user_id=88, event_id=event_id, seat_number=seat)
        self.assertEqual(code_rebook, 200)
        self.assertEqual(res_rebook["message"], "Booking successful.")
        print(f"Step 4: Seat {seat} successfully re-booked by User 88 (Ticket: {res_rebook['ticket_id']})")
        print("[PASS] Test 3 PASSED: Full lifecycle cancel & re-book cycle validated!")

    def test_04_zero_capacity_rejection(self):
        """
        Test 4: Rejection when Available Seats = 0
        Verify that when event capacity is exhausted, requests are rejected with 'Seat unavailable.' (HTTP 400).
        """
        print("\n" + "=" * 70)
        print("[TEST] Running Test 4: Capacity Limit Rejection (Available Seats = 0)")
        print("=" * 70)

        # Event 33 in sample data has 0 available seats
        event_id = 33
        event = booking_service.get_event(event_id)
        if not event:
            # Force an event to have 0 seats for test
            event_id = 53
            booking_service._events[event_id]["available_seats"] = 0

        print(f"Testing with Event {event_id} (Available seats: 0)")
        res, code = booking_service.book_seat(user_id=1, event_id=event_id, seat_number="K14")

        self.assertEqual(code, 400)
        self.assertEqual(res["status"], "error")
        self.assertEqual(res["message"], "Seat unavailable.")
        print("[PASS] Test 4 PASSED: Correctly returned HTTP 400 'Seat unavailable.' when capacity is 0!")

    def test_05_api_endpoints_concurrent_http_stress(self):
        """
        Test 5: REST API HTTP Endpoints Multi-Threaded Stress Test
        Tests concurrent requests directly against Flask test client endpoints (/api/book).
        """
        print("\n" + "=" * 70)
        print("[TEST] Running Test 5: Flask REST API Concurrent HTTP Stress Test (30 Threads)")
        print("=" * 70)

        target_event = 56
        target_seat = "C10"
        num_threads = 30
        results = []
        barrier = threading.Barrier(num_threads)

        def client_worker(uid: int):
            barrier.wait()
            response = self.app.post(
                "/api/book",
                data=json.dumps({
                    "user_id": uid,
                    "event_id": target_event,
                    "seat_number": target_seat
                }),
                content_type="application/json"
            )
            return response.status_code, response.get_json()

        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [executor.submit(client_worker, i + 10) for i in range(num_threads)]
            for future in as_completed(futures):
                results.append(future.result())

        status_200 = [r for r in results if r[0] == 200]
        status_409 = [r for r in results if r[0] == 409]

        print(f"API HTTP 200 Successes: {len(status_200)}")
        print(f"API HTTP 409 Conflicts: {len(status_409)}")

        self.assertEqual(len(status_200), 1, "Exactly 1 API HTTP POST /api/book must return 200 OK!")
        self.assertEqual(len(status_409), 29, "29 API HTTP POST /api/book must return 409 Conflict!")
        print("[PASS] Test 5 PASSED: Flask REST API endpoints enforce perfect concurrency!")


if __name__ == "__main__":
    print("\n" + "=" * 80)
    print(" [CINEMA TICKET RESERVATION SYSTEM] CONCURRENCY ENGINE TEST SUITE")
    print("=" * 80)
    unittest.main(verbosity=2)
