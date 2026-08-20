import os
import sys
import time
import shutil
import subprocess
import argparse
from batch_analytics_jobs import ALL_JOBS, LOCAL_HDFS_SIMULATION_DIR

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

def get_running_namenode():
    docker_bin = shutil.which("docker")
    if not docker_bin:
        return "ticket-master-node"
    try:
        res = subprocess.run([docker_bin, "ps", "--format", "{{.Names}}"], capture_output=True, text=True, timeout=3, check=False)
        for cand in ["ticket-master-node", "namenode", "cinema-namenode-1"]:
            if cand in res.stdout:
                return cand
    except Exception:
        pass
    return "ticket-master-node"

CONTAINER_NAME = get_running_namenode()

def is_docker_running():
    docker_bin = shutil.which("docker")
    if not docker_bin:
        return False
    try:
        res = subprocess.run([docker_bin, "ps", "--format", "{{.Names}}"], capture_output=True, text=True, timeout=2, check=False)
        return CONTAINER_NAME in res.stdout
    except Exception:
        return False

def sync_job_output_to_docker_hdfs(job_name):
    """Sync local analytics CSV/JSON outputs to the live Docker HDFS cluster."""
    docker_bin = shutil.which("docker")
    if not docker_bin or not is_docker_running():
        return False

    local_job_dir = os.path.join(LOCAL_HDFS_SIMULATION_DIR, "cinema", "analytics", job_name)
    if not os.path.exists(local_job_dir):
        return False

    hdfs_target_dir = f"/cinema/analytics/{job_name}"
    
    # 1. Create HDFS directory inside container
    subprocess.run([docker_bin, "exec", CONTAINER_NAME, "hdfs", "dfs", "-mkdir", "-p", f"{hdfs_target_dir}/csv", f"{hdfs_target_dir}/json"], capture_output=True, timeout=5, check=False)

    # 2. Copy CSV and JSON to container
    csv_file = os.path.join(local_job_dir, "csv", f"{job_name}.csv")
    json_file = os.path.join(local_job_dir, "json", f"{job_name}.json")

    if os.path.exists(csv_file):
        tmp_csv = f"/tmp/{job_name}.csv"
        subprocess.run([docker_bin, "cp", csv_file, f"{CONTAINER_NAME}:{tmp_csv}"], capture_output=True, timeout=5, check=False)
        subprocess.run([docker_bin, "exec", CONTAINER_NAME, "hdfs", "dfs", "-put", "-f", tmp_csv, f"{hdfs_target_dir}/csv/"], capture_output=True, timeout=10, check=False)

    if os.path.exists(json_file):
        tmp_json = f"/tmp/{job_name}.json"
        subprocess.run([docker_bin, "cp", json_file, f"{CONTAINER_NAME}:{tmp_json}"], capture_output=True, timeout=5, check=False)
        subprocess.run([docker_bin, "exec", CONTAINER_NAME, "hdfs", "dfs", "-put", "-f", tmp_json, f"{hdfs_target_dir}/json/"], capture_output=True, timeout=10, check=False)

    return True

def run_analytics_jobs(target_job="all", use_spark=False):
    print("==================================================================")
    print("    PART 2: DISTRIBUTED TICKET SYSTEM - BATCH ANALYTICS ENGINE    ")
    print("==================================================================")

    docker_active = is_docker_running()
    engine_name = "Apache Spark (PySpark 4.2)" if use_spark else "Multi-Node Distributed Engine (10 Parallel Partitions)"
    print(f"[*] Processing Engine : {engine_name}")
    print(f"[*] Cluster Storage   : {'Docker HDFS (' + CONTAINER_NAME + ')' if docker_active else 'HDFS Storage Layer (.hdfs_storage)'}")

    jobs_to_run = ALL_JOBS.items() if target_job == "all" else [(target_job, ALL_JOBS[target_job])]

    start_total = time.time()
    results = []

    print(f"\n---> Triggering {len(jobs_to_run)} analytical batch jobs across cluster...\n")

    for jid, (title, func) in jobs_to_run:
        t0 = time.time()
        try:
            res = func(use_spark=use_spark)
            elapsed = time.time() - t0
            job_name = res["hdfs_path"].split("/")[-1]
            synced = sync_job_output_to_docker_hdfs(job_name)
            sync_tag = "[Synced to Docker HDFS]" if docker_active else "[Synced to HDFS Storage]"
            print(f"  [OK] {jid.upper()}: {title}")
            print(f"       -> Generated {res['count']} rows in {elapsed:.3f}s | HDFS: {res['hdfs_path']} {sync_tag}")
            results.append((jid, title, res['count'], elapsed, True))
        except Exception as e:
            elapsed = time.time() - t0
            print(f"  [!] {jid.upper()} FAILED ({title}): {e}")
            results.append((jid, title, 0, elapsed, False))

    total_time = time.time() - start_total
    print("\n==================================================================")
    print(f"[SUMMARY] Finished batch analytics execution in {total_time:.3f}s")
    print("==================================================================")
    for jid, title, count, el, ok in results:
        status_str = "SUCCESS" if ok else "FAILED"
        print(f"  - {jid.upper():<6} | {status_str:<7} | {count:>4} rows | {el:.3f}s | {title}")

    return all(r[4] for r in results)

def main():
    parser = argparse.ArgumentParser(description="Run Part 2 Distributed Batch Analytics Jobs.")
    parser.add_argument("--job", choices=["all", "job1", "job2", "job3", "job4", "job5", "job6", "job7", "job8"], default="all", help="Job ID to execute")
    parser.add_argument("--spark", action="store_true", help="Use Apache Spark (PySpark) execution engine")
    args = parser.parse_args()

    success = run_analytics_jobs(args.job, use_spark=args.spark)
    if not success:
        sys.exit(1)

if __name__ == "__main__":
    main()
