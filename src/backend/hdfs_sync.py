"""
HDFS Storage Synchronization Engine (Part 3).
Handles distributed transaction logging (audit trail) and ticket persistence.
Supports live Hadoop Docker clusters (namenode / ticket-master-node) with an
automatic fallback to local persistent storage (.hdfs_storage/) for fault-tolerance.
"""

import os
import csv
import json
import logging
import datetime
import threading
import subprocess
from typing import Optional, Dict, Any, List

logger = logging.getLogger("hdfs_sync")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


class HDFSSyncManager:
    """
    Manages synchronization of booking and cancellation transactions to HDFS
    and maintains a local synchronized persistent storage layer.
    """

    def __init__(self, local_base_dir: Optional[str] = None):
        if local_base_dir is None:
            # Default to .hdfs_storage in the project root
            project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
            local_base_dir = os.path.join(project_root, ".hdfs_storage")

        self.local_base_dir = local_base_dir
        self.transactions_dir = os.path.join(self.local_base_dir, "cinema", "transactions")
        self.tickets_dir = os.path.join(self.local_base_dir, "cinema", "raw", "tickets")
        
        self.audit_log_path = os.path.join(self.transactions_dir, "audit_log.csv")
        self.tickets_csv_path = os.path.join(self.tickets_dir, "tickets.csv")

        self._lock = threading.Lock()
        self._init_local_storage()

        # Check Docker / HDFS connectivity
        self.docker_container = self._detect_docker_namenode()

    def _init_local_storage(self):
        """Ensures local storage directories and CSV headers exist."""
        os.makedirs(self.transactions_dir, exist_ok=True)
        os.makedirs(self.tickets_dir, exist_ok=True)

        with self._lock:
            # Initialize Audit Log CSV header if not exists
            if not os.path.exists(self.audit_log_path) or os.path.getsize(self.audit_log_path) == 0:
                with open(self.audit_log_path, "w", newline="", encoding="utf-8") as f:
                    writer = csv.writer(f)
                    writer.writerow([
                        "timestamp",
                        "transaction_type",
                        "ticket_id",
                        "event_id",
                        "user_id",
                        "seat_number",
                        "price",
                        "status"
                    ])

            # Initialize Tickets CSV header if not exists
            if not os.path.exists(self.tickets_csv_path) or os.path.getsize(self.tickets_csv_path) == 0:
                with open(self.tickets_csv_path, "w", newline="", encoding="utf-8") as f:
                    writer = csv.writer(f)
                    writer.writerow([
                        "ticket_id",
                        "event_id",
                        "user_id",
                        "seat_number",
                        "price",
                        "status",
                        "updated_at"
                    ])

    def _detect_docker_namenode(self) -> Optional[str]:
        """Detects if a running Hadoop NameNode Docker container is reachable."""
        candidate_containers = ["namenode", "ticket-master-node", "cinema-namenode-1"]
        try:
            result = subprocess.run(
                ["docker", "ps", "--format", "{{.Names}}"],
                capture_output=True,
                text=True,
                timeout=3,
                check=False
            )
            if result.returncode == 0:
                running_names = [line.strip() for line in result.stdout.strip().splitlines() if line.strip()]
                for cand in candidate_containers:
                    if cand in running_names:
                        logger.info(f"Connected to Hadoop NameNode container: {cand}")
                        return cand
        except Exception:
            pass
        logger.info("Live Hadoop Docker container not detected; using local persistent storage (.hdfs_storage/).")
        return None

    def log_transaction(
        self,
        transaction_type: str,
        ticket_id: str,
        event_id: Any,
        user_id: Any,
        seat_number: str,
        price: float = 0.0,
        status: str = "SUCCESS"
    ) -> Dict[str, Any]:
        """
        Thread-safely writes an audit record and updates ticket status in local storage
        and syncs to HDFS if available.
        """
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        record = {
            "timestamp": timestamp,
            "transaction_type": transaction_type.upper(),
            "ticket_id": str(ticket_id),
            "event_id": int(event_id),
            "user_id": int(user_id) if str(user_id).isdigit() else str(user_id),
            "seat_number": str(seat_number).strip().upper(),
            "price": float(price),
            "status": status.upper()
        }

        with self._lock:
            # 1. Append to local audit log CSV
            try:
                with open(self.audit_log_path, "a", newline="", encoding="utf-8") as f:
                    writer = csv.writer(f)
                    writer.writerow([
                        record["timestamp"],
                        record["transaction_type"],
                        record["ticket_id"],
                        record["event_id"],
                        record["user_id"],
                        record["seat_number"],
                        f"{record['price']:.2f}",
                        record["status"]
                    ])
            except Exception as e:
                logger.error(f"Error appending to local audit log: {e}")

            # 2. Append to tickets log CSV
            try:
                with open(self.tickets_csv_path, "a", newline="", encoding="utf-8") as f:
                    writer = csv.writer(f)
                    ticket_status = "ACTIVE" if record["transaction_type"] == "BOOKING" else "CANCELLED"
                    writer.writerow([
                        record["ticket_id"],
                        record["event_id"],
                        record["user_id"],
                        record["seat_number"],
                        f"{record['price']:.2f}",
                        ticket_status,
                        record["timestamp"]
                    ])
            except Exception as e:
                logger.error(f"Error appending to local tickets log: {e}")

        # 3. Sync to Docker HDFS asynchronously/safely if available
        if self.docker_container:
            self._sync_to_hdfs(record)

        return record

    def log_booking(
        self,
        ticket_id: str,
        event_id: Any,
        user_id: Any,
        seat_number: str,
        price: float = 0.0
    ) -> Dict[str, Any]:
        """Convenience method to log a successful booking transaction."""
        return self.log_transaction(
            transaction_type="BOOKING",
            ticket_id=ticket_id,
            event_id=event_id,
            user_id=user_id,
            seat_number=seat_number,
            price=price,
            status="SUCCESS"
        )

    def log_cancellation(
        self,
        ticket_id: str,
        event_id: Any,
        user_id: Any,
        seat_number: str,
        price: float = 0.0
    ) -> Dict[str, Any]:
        """Convenience method to log a booking cancellation transaction."""
        return self.log_transaction(
            transaction_type="CANCELLATION",
            ticket_id=ticket_id,
            event_id=event_id,
            user_id=user_id,
            seat_number=seat_number,
            price=price,
            status="CANCELLED"
        )

    def _sync_to_hdfs(self, record: Dict[str, Any]):
        """Uploads/appends the record to HDFS within the Docker container."""
        try:
            line = f"{record['timestamp']},{record['transaction_type']},{record['ticket_id']},{record['event_id']},{record['user_id']},{record['seat_number']},{record['price']},{record['status']}\n"
            hdfs_dir = "/cinema/transactions"
            hdfs_file = f"{hdfs_dir}/audit_log.csv"

            # Create HDFS directory if needed
            subprocess.run(
                ["docker", "exec", self.docker_container, "hdfs", "dfs", "-mkdir", "-p", hdfs_dir],
                capture_output=True,
                check=False,
                timeout=3
            )
            # Append record via bash echo inside container
            cmd = f"echo '{line.strip()}' | docker exec -i {self.docker_container} hdfs dfs -appendToFile - {hdfs_file}"
            subprocess.run(cmd, shell=True, capture_output=True, check=False, timeout=3)
        except Exception as e:
            logger.debug(f"HDFS sync skipped or encountered non-critical error: {e}")

    def get_audit_logs(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Reads recent transactions from the local audit log."""
        logs = []
        if not os.path.exists(self.audit_log_path):
            return logs

        with self._lock:
            with open(self.audit_log_path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    logs.append(row)

        return logs[-limit:]


# Global shared instance
hdfs_sync_manager = HDFSSyncManager()
