import os
import shutil
import subprocess

staging_dir = "data/staging"
hdfs_base = "/ticket_system/raw"

file_mapping = {
    "events.csv": f"{hdfs_base}/events/csv/events.csv",
    "events.json": f"{hdfs_base}/events/json/events.json",
    "seats.csv": f"{hdfs_base}/seats/csv/seats.csv",
    "seats.json": f"{hdfs_base}/seats/json/seats.json",
    "users.csv": f"{hdfs_base}/users/csv/users.csv",
    "users.json": f"{hdfs_base}/users/json/users.json"
}

def run_hdfs_cmd(cmd_args):
    if shutil.which("hdfs"):
        full_cmd = ["hdfs", "dfs"] + cmd_args
    else:
        full_cmd = ["docker", "exec", "namenode", "hdfs", "dfs"] + cmd_args
    return subprocess.run(full_cmd, capture_output=True, text=True)

def upload_files():
    has_local_hdfs = bool(shutil.which("hdfs"))

    for filename, hdfs_target in file_mapping.items():
        local_path = os.path.join(staging_dir, filename)

        if not os.path.exists(local_path):
            print(f"File not found: {local_path}")
            continue

        parent_dir = os.path.dirname(hdfs_target)
        run_hdfs_cmd(["-mkdir", "-p", parent_dir])

        if has_local_hdfs:
            result = run_hdfs_cmd(["-put", "-f", local_path, hdfs_target])
        else:
            container_path = f"/ticket_system_local/staging/{filename}"
            result = run_hdfs_cmd(["-put", "-f", container_path, hdfs_target])

        if result.returncode == 0:
            print(f"Uploaded {filename} to {hdfs_target}")
        else:
            print(f"Failed to upload {filename}: {result.stderr.strip()}")

if __name__ == "__main__":
    upload_files()
