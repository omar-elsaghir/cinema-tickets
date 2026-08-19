"""
Concurrency Lock Manager for Cinema Ticket Reservation System (Part 3).
Provides fine-grained, thread-safe per-seat locks to prevent race conditions
and double-booking while maximizing parallel throughput across independent seats.
"""

import threading
from contextlib import contextmanager
from typing import Dict, Tuple, Any


class SeatLockManager:
    """
    Thread-safe manager for fine-grained per-seat locks.
    Each seat in an event has a dedicated threading.Lock, preventing race conditions
    when multiple concurrent threads or requests attempt to book or cancel the exact
    same seat simultaneously.
    """

    def __init__(self):
        self._seat_locks: Dict[Tuple[Any, str], threading.Lock] = {}
        self._event_locks: Dict[Any, threading.Lock] = {}
        self._meta_lock = threading.Lock()

    def get_seat_lock(self, event_id: Any, seat_number: str) -> threading.Lock:
        """
        Retrieves or initializes a lock for a specific (event_id, seat_number) pair.
        Uses a meta-lock to ensure atomic lock creation without race conditions.
        """
        key = (str(event_id), str(seat_number).strip().upper())
        with self._meta_lock:
            if key not in self._seat_locks:
                self._seat_locks[key] = threading.Lock()
            return self._seat_locks[key]

    def get_event_lock(self, event_id: Any) -> threading.Lock:
        """
        Retrieves or initializes an event-level lock for aggregate capacity updates.
        """
        key = str(event_id)
        with self._meta_lock:
            if key not in self._event_locks:
                self._event_locks[key] = threading.Lock()
            return self._event_locks[key]

    @contextmanager
    def lock_seat(self, event_id: Any, seat_number: str):
        """
        Context manager for acquiring and releasing a seat-specific lock.

        Example:
            with lock_manager.lock_seat(event_id, seat_number):
                # Critical section: check availability and reserve seat
                ...
        """
        lock = self.get_seat_lock(event_id, seat_number)
        lock.acquire()
        try:
            yield lock
        finally:
            lock.release()

    @contextmanager
    def lock_event(self, event_id: Any):
        """
        Context manager for acquiring and releasing an event-level lock.
        """
        lock = self.get_event_lock(event_id)
        lock.acquire()
        try:
            yield lock
        finally:
            lock.release()

    def clear(self):
        """Resets all stored locks (used primarily during test setup/teardown)."""
        with self._meta_lock:
            self._seat_locks.clear()
            self._event_locks.clear()


# Global shared singleton instance
seat_lock_manager = SeatLockManager()
