import os
import json
import shutil
import subprocess

staging_dir = "data/staging"
hdfs_base = "/ticket_system/raw"

files = {
    "events.csv": f"{hdfs_base}/events/csv/events.csv",
    "events.json": f"{hdfs_base}/events/json/events.json",
    "seats.csv": f"{hdfs_base}/seats/csv/seats.csv",
    "seats.json": f"{hdfs_base}/seats/json/seats.json",
    "users.csv": f"{hdfs_base}/users/csv/users.csv",
    "users.json": f"{hdfs_base}/users/json/users.json"
}

def count_local_lines(filepath):
    if not os.path.exists(filepath):
        return -1
    with open(filepath, "r", encoding="utf-8") as f:
        return sum(1 for _ in f)

def read_hdfs_file(hdfs_path):
    # Use local hdfs if available, otherwise execute via Docker namenode container
    if shutil.which("hdfs"):
        cmd = f"hdfs dfs -cat {hdfs_path}"
    else:
        cmd = f"docker exec namenode hdfs dfs -cat {hdfs_path}"

    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if result.returncode != 0:
        return None
    lines = [line for line in result.stdout.strip().split("\n") if line]
    return lines

def verify():
    print("Starting verification of HDFS dataset files...\n")

    for filename, hdfs_path in files.items():
        print(f"--- Verifying {filename} ---")
        local_path = os.path.join(staging_dir, filename)

        local_count = count_local_lines(local_path)
        hdfs_lines = read_hdfs_file(hdfs_path)

        if hdfs_lines is None:
            print(f"Error: Could not read {hdfs_path} from HDFS\n")
            continue

        hdfs_count = len(hdfs_lines)
        print(f"Local line count : {local_count}")
        print(f"HDFS line count  : {hdfs_count}")

        if local_count == hdfs_count:
            print("Status: PASSED (Line counts match)")
        else:
            print("Status: FAILED (Count mismatch)")

        if filename.endswith(".csv"):
            print(f"Columns: {hdfs_lines[0]}")
        elif filename.endswith(".json"):
            try:
                first_record = json.loads(hdfs_lines[0])
                print(f"Fields: {list(first_record.keys())}")
            except Exception as e:
                print(f"JSON parsing error: {e}")

        print(f"Sample row: {hdfs_lines[0][:120]}\n")

    print("Verification finished.")

if __name__ == "__main__":
    verify()