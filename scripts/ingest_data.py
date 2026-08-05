import os
import subprocess

staging_dir = "data/staging"
hdfs_base = "/cinema"

# Map local staging files to HDFS directories
file_mapping = {
    "events.csv": f"{hdfs_base}/csv/events",
    "events.json": f"{hdfs_base}/json/events",
    "seats.csv": f"{hdfs_base}/csv/seats",
    "seats.json": f"{hdfs_base}/json/seats",
    "users.csv": f"{hdfs_base}/csv/users",
    "users.json": f"{hdfs_base}/json/users"
}

def upload_files():

    for filename, hdfs_dir in file_mapping.items():
        local_path = os.path.join(staging_dir, filename)

        if not os.path.exists(local_path):
            print(f"File not found: {local_path}")
            continue

        # Create HDFS target directory if it doesn't exist
        subprocess.run(["hdfs", "dfs", "-mkdir", "-p", hdfs_dir], check=False)

        # Copy file from local staging to HDFS
        result = subprocess.run(["hdfs", "dfs", "-put", "-f", local_path, hdfs_dir], capture_output=True, text=True)

        if result.returncode == 0:
            print(f"Uploaded {filename} to {hdfs_dir}")
        else:
            print(f"Failed to upload {filename}: {result.stderr.strip()}")


if __name__ == "__main__":
    upload_files()
