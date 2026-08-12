#!/bin/bash
# ===============================================================================
# 🎟️ Cinema Ticket Reservation System - Part 2 Runner & Verification Script
# ===============================================================================
# Description:
#     Automates submitting the PySpark batch analytics pipeline to the YARN
#     cluster and verifies that all 8 analytical datasets are successfully
#     written to HDFS under /ticket_system/processed/.
#
# Usage:
#     chmod +x scripts/run_part2.sh
#     ./scripts/run_part2.sh
#
# Task Assignments for Verification:
#     - User (Odd Jobs) : Verify outputs for Jobs 1, 3, 5, 7
#     - Omar (Even Jobs): Verify outputs for Jobs 2, 4, 6, 8
# ===============================================================================

# Exit immediately if a command exits with a non-zero status
set -e

# Configuration
NAMENODE_CONTAINER="namenode"
PYSPARK_SCRIPT="jobs/batch_analytics.py"
CONTAINER_TARGET_PATH="/tmp/batch_analytics.py"
HDFS_PROCESSED_DIR="/ticket_system/processed"

echo "=================================================================="
echo "🚀 STARTING PART 2 BATCH ANALYTICS PIPELINE"
echo "=================================================================="

# -------------------------------------------------------------------------------
# STEP 1: PRE-FLIGHT CHECKS
# -------------------------------------------------------------------------------
echo ""
echo "🔍 Step 1: Checking if HDFS NameNode container is active..."
if ! docker ps | grep -q "$NAMENODE_CONTAINER"; then
    echo "❌ ERROR: Container '$NAMENODE_CONTAINER' is not running."
    echo "   Run 'docker compose up -d' first."
    exit 1
fi
echo "✅ NameNode container is online!"

# -------------------------------------------------------------------------------
# STEP 2: COPY PYSPARK JOB TO CLUSTER MASTER
# -------------------------------------------------------------------------------
echo ""
echo "📦 Step 2: Copying $PYSPARK_SCRIPT to NameNode container..."
docker cp "$PYSPARK_SCRIPT" "$NAMENODE_CONTAINER:$CONTAINER_TARGET_PATH"
echo "✅ Script copied to $CONTAINER_TARGET_PATH inside $NAMENODE_CONTAINER."

# -------------------------------------------------------------------------------
# STEP 3: SUBMIT PYSPARK BATCH JOB TO YARN
# -------------------------------------------------------------------------------
echo ""
echo "⚡ Step 3: Submitting PySpark job to YARN Cluster..."
echo "------------------------------------------------------------------"
docker exec -it "$NAMENODE_CONTAINER" bash -c "
    export PYSPARK_PYTHON=/opt/conda/bin/python
    export PYSPARK_DRIVER_PYTHON=/opt/conda/bin/python
    export HADOOP_CONF_DIR=/etc/hadoop
    
    /opt/spark/bin/spark-submit \
        --master yarn \
        --deploy-mode client \
        --conf spark.hadoop.yarn.resourcemanager.address=resourcemanager:8032 \
        --conf spark.hadoop.yarn.resourcemanager.scheduler.address=resourcemanager:8030 \
        --conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000 \
        --executor-memory 512M \
        --driver-memory 512M \
        $CONTAINER_TARGET_PATH
"
echo "------------------------------------------------------------------"
echo "✅ PySpark execution completed!"
# -------------------------------------------------------------------------------
# STEP 4: VERIFY HDFS OUTPUTS (TEAM CHECKLIST)
# -------------------------------------------------------------------------------
echo ""
echo "🔍 Step 4: Verifying HDFS Processed Output Directories..."
echo "=================================================================="

# TODO (User - Odd Jobs Verification):
# Verify that Odd Job output directories exist and contain parquet files
echo "▶ [User] Checking Odd Job Outputs (Jobs 1, 3, 5, 7)..."

echo "  ├─ Checking Job 1 Output: total_bookings_per_event"
docker exec "$NAMENODE_CONTAINER" hdfs dfs -ls "$HDFS_PROCESSED_DIR/total_bookings_per_event" || echo "  ⚠️ Job 1 output missing!"

echo "  ├─ Checking Job 3 Output: total_revenue_per_event"
docker exec "$NAMENODE_CONTAINER" hdfs dfs -ls "$HDFS_PROCESSED_DIR/total_revenue_per_event" || echo "  ⚠️ Job 3 output missing!"

echo "  ├─ Checking Job 5 Output: top5_events"
docker exec "$NAMENODE_CONTAINER" hdfs dfs -ls "$HDFS_PROCESSED_DIR/top5_events" || echo "  ⚠️ Job 5 output missing!"

echo "  ├─ Checking Job 7 Output: bookings_by_date"
docker exec "$NAMENODE_CONTAINER" hdfs dfs -ls "$HDFS_PROCESSED_DIR/bookings_by_date" || echo "  ⚠️ Job 7 output missing!"


# TODO (Omar - Even Jobs Verification):
# Verify that Even Job output directories exist and contain parquet files
echo ""
echo "▶ [Omar] Checking Even Job Outputs (Jobs 2, 4, 6, 8)..."

echo "  ├─ Checking Job 2 Output: occupancy_percentage_per_event"
docker exec "$NAMENODE_CONTAINER" hdfs dfs -ls "$HDFS_PROCESSED_DIR/occupancy_percentage_per_event" || echo "  ⚠️ Job 2 output missing!"

echo "  ├─ Checking Job 4 Output: available_seats_per_event"
docker exec "$NAMENODE_CONTAINER" hdfs dfs -ls "$HDFS_PROCESSED_DIR/available_seats_per_event" || echo "  ⚠️ Job 4 output missing!"

echo "  ├─ Checking Job 6 Output: bookings_by_category"
docker exec "$NAMENODE_CONTAINER" hdfs dfs -ls "$HDFS_PROCESSED_DIR/bookings_by_category" || echo "  ⚠️ Job 6 output missing!"

echo "  └─ Checking Job 8 Output: top5_users"
docker exec "$NAMENODE_CONTAINER" hdfs dfs -ls "$HDFS_PROCESSED_DIR/top5_users" || echo "  ⚠️ Job 8 output missing!"

echo "=================================================================="
echo "🎉 PART 2 PIPELINE RUN COMPLETE!"
echo "=================================================================="