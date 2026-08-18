#!/bin/bash
set -e

NAMENODE_CONTAINER="namenode"
PYSPARK_SCRIPT="jobs/batch_analytics.py"
CONTAINER_TARGET_PATH="/tmp/batch_analytics.py"
HDFS_PROCESSED_DIR="/ticket_system/processed"

if ! docker ps | grep -q "$NAMENODE_CONTAINER"; then
    echo "Error: Container '$NAMENODE_CONTAINER' is not running. Run 'docker compose up -d' first."
    exit 1
fi

docker cp "$PYSPARK_SCRIPT" "$NAMENODE_CONTAINER:$CONTAINER_TARGET_PATH"

python3 jobs/run_analytics.py 2>/dev/null || python jobs/run_analytics.py 2>/dev/null || py -3.12 jobs/run_analytics.py

docker exec "$NAMENODE_CONTAINER" hdfs dfs -ls "$HDFS_PROCESSED_DIR"