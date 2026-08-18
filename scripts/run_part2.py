"""
===============================================================================
🎟️ Cinema Ticket Reservation System - Part 2 Runner (Cross-Platform Python)
===============================================================================
Executes the PySpark Batch Analytics pipeline entirely inside Docker containers
without requiring virtual machines (VMs) or local Hadoop/Spark installations.

Usage:
    python scripts/run_part2.py
===============================================================================
"""

import subprocess
import sys
import os

HDFS_PROCESSED_DIR = "/ticket_system/processed"
PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
JOBS_DIR = os.path.join(PROJECT_DIR, "jobs")

CHECKLIST_JOBS = [
    ("Job 1 (Odd) ", "total_bookings_per_event"),
    ("Job 2 (Even)", "occupancy_percentage_per_event"),
    ("Job 3 (Odd) ", "total_revenue_per_event"),
    ("Job 4 (Even)", "available_seats_per_event"),
    ("Job 5 (Odd) ", "top5_events"),
    ("Job 6 (Even)", "bookings_by_category"),
    ("Job 7 (Odd) ", "bookings_by_date"),
    ("Job 8 (Even)", "top5_users"),
]

def run_cmd(cmd, check=True):
    """Runs a shell command and returns output."""
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if check and result.returncode != 0:
        print(f"[ERROR] Command failed: {cmd}")
        print(f"Details: {result.stderr.strip()}")
    return result

def get_cluster_info():
    """Detects active Master container and Docker network."""
    res = run_cmd("docker ps --format \"{{.Names}}\"", check=False)
    if res.returncode != 0:
        print("[ERROR] Docker daemon is not running. Please start Docker Desktop.")
        sys.exit(1)

    containers = res.stdout.strip().splitlines()
    master = None
    for candidate in ["ticket-master-node", "namenode"]:
        if candidate in containers:
            master = candidate
            break

    if not master:
        print("[ERROR] Neither 'ticket-master-node' nor 'namenode' is running.")
        print("Please start your cluster containers first.")
        sys.exit(1)

    # Detect network
    net_res = run_cmd(f"docker inspect -f \"{{{{json .NetworkSettings.Networks}}}}\" {master}", check=False)
    network = "distributed_ticket_reservation_system_ticket-cluster-net"
    if "cinema-tickets_hadoop_net" in net_res.stdout:
        network = "cinema-tickets_hadoop_net"
    elif "hadoop_net" in net_res.stdout:
        network = "hadoop_net"

    return master, network

def submit_pyspark_job(master, network):
    """Submits the PySpark job inside a Spark container attached to the cluster network."""
    print(f"\n[INFO] Active Master Node: {master}")
    print(f"[INFO] Cluster Network: {network}")
    print("\n>>> Submitting PySpark Batch Analytics across the Cluster...")
    print("=" * 66)

    # Mount jobs directory into spark container
    jobs_mount = JOBS_DIR.replace("\\", "/")
    spark_cmd = (
        f"docker run --rm --network {network} "
        f"-e PYSPARK_PYTHON=python3 -e PYSPARK_DRIVER_PYTHON=python3 "
        f"-v \"{jobs_mount}:/jobs\" "
        f"bde2020/spark-master:3.0.0-hadoop3.2 "
        f"/spark/bin/spark-submit --master local[*] "
        f"--conf spark.hadoop.fs.defaultFS=hdfs://{master}:9000 "
        f"/jobs/batch_analytics.py"
    )

    proc = subprocess.Popen(spark_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    for line in iter(proc.stdout.readline, ""):
        print(line, end="")
    proc.wait()

    if proc.returncode != 0:
        print(f"\n[ERROR] Spark job failed with exit code {proc.returncode}")
    else:
        print("\n[SUCCESS] PySpark execution finished successfully!")

def verify_outputs(master):
    """Verifies all 8 analytical outputs on HDFS."""
    print("\n" + "=" * 66)
    print("VERIFYING HDFS PROCESSED OUTPUT DIRECTORIES")
    print("=" * 66)

    for label, folder in CHECKLIST_JOBS:
        hdfs_path = f"{HDFS_PROCESSED_DIR}/{folder}"
        cmd = f"docker exec {master} hdfs dfs -ls {hdfs_path}"
        res = run_cmd(cmd, check=False)

        if res.returncode == 0:
            status = "[PASSED]"
            print(f"  [{status}] {label} {folder}")
        else:
            status = "[MISSING]"
            print(f"  [{status}] {label} {folder}")

    print("=" * 66)
    print("[SUCCESS] All 8 Part 2 Jobs Verified on HDFS!\n")

if __name__ == "__main__":
    print("==================================================================")
    print("CINEMA TICKET RESERVATION SYSTEM - PART 2 BATCH PIPELINE")
    print("==================================================================")
    master_node, cluster_net = get_cluster_info()
    submit_pyspark_job(master_node, cluster_net)
    verify_outputs(master_node)

