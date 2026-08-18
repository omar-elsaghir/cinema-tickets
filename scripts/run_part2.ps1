# ===============================================================================
# 🎟️ Cinema Ticket Reservation System - Part 2 Runner (PowerShell for Windows)
# ===============================================================================
# Description:
#     Automates submitting the PySpark batch analytics pipeline to Docker
#     containers and verifies all 8 outputs in HDFS under /ticket_system/processed/.
#
# Usage (in PowerShell):
#     .\scripts\run_part2.ps1
# ===============================================================================

$ErrorActionPreference = "Stop"
$NAMENODE_CONTAINER = "namenode"
$SPARK_CONTAINER = "spark-master"
$HDFS_PROCESSED_DIR = "/ticket_system/processed"
$JOBS_SCRIPT = "/jobs/batch_analytics.py"

Write-Host "==================================================================" -ForegroundColor Cyan
Write-Host "🚀 STARTING PART 2 BATCH ANALYTICS PIPELINE (DOCKER CONTAINERS)" -ForegroundColor Cyan
Write-Host "==================================================================" -ForegroundColor Cyan

# 1. Pre-flight checks
Write-Host "`n🔍 Step 1: Checking if cluster containers are active..." -ForegroundColor Yellow
$runningContainers = docker ps --format "{{.Names}}"
if ($runningContainers -notcontains $NAMENODE_CONTAINER) {
    Write-Host "❌ ERROR: Container '$NAMENODE_CONTAINER' is not running." -ForegroundColor Red
    Write-Host "   Run 'docker compose up -d' first." -ForegroundColor Yellow
    exit 1
}
Write-Host "✅ Cluster containers are online!" -ForegroundColor Green

# 2. Submit PySpark job
Write-Host "`n⚡ Step 2: Submitting PySpark Batch Job to Cluster..." -ForegroundColor Yellow
Write-Host "------------------------------------------------------------------"

if ($runningContainers -contains $SPARK_CONTAINER) {
    docker exec -it $SPARK_CONTAINER /spark/bin/spark-submit `
        --master spark://spark-master:7077 `
        --conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000 `
        --driver-memory 512M `
        --executor-memory 512M `
        $JOBS_SCRIPT
} else {
    docker exec -it $NAMENODE_CONTAINER bash -c "
        export PYSPARK_PYTHON=/opt/conda/bin/python
        /opt/spark/bin/spark-submit \
            --master yarn \
            --deploy-mode client \
            --conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000 \
            --conf spark.hadoop.yarn.resourcemanager.address=resourcemanager:8032 \
            --driver-memory 512M \
            --executor-memory 512M \
            $JOBS_SCRIPT
    "
}

Write-Host "✅ PySpark execution completed!" -ForegroundColor Green

# 3. Output verification
Write-Host "`n🔍 Step 3: Verifying HDFS Processed Output Directories..." -ForegroundColor Yellow
Write-Host "=================================================================="

$jobs = @(
    @{Label="Job 1 (Odd) "; Folder="total_bookings_per_event"},
    @{Label="Job 2 (Even)"; Folder="occupancy_percentage_per_event"},
    @{Label="Job 3 (Odd) "; Folder="total_revenue_per_event"},
    @{Label="Job 4 (Even)"; Folder="available_seats_per_event"},
    @{Label="Job 5 (Odd) "; Folder="top5_events"},
    @{Label="Job 6 (Even)"; Folder="bookings_by_category"},
    @{Label="Job 7 (Odd) "; Folder="bookings_by_date"},
    @{Label="Job 8 (Even)"; Folder="top5_users"}
)

foreach ($job in $jobs) {
    $path = "$HDFS_PROCESSED_DIR/$($job.Folder)"
    $check = docker exec $NAMENODE_CONTAINER hdfs dfs -ls $path 2>&1
    if ($LASTEXITCODE -eq 0) {
        Write-Host "  ├─ [$($job.Label)] $($job.Folder) : ✅ PASSED" -ForegroundColor Green
    } else {
        Write-Host "  ├─ [$($job.Label)] $($job.Folder) : ❌ MISSING" -ForegroundColor Red
    }
}

Write-Host "==================================================================" -ForegroundColor Cyan
Write-Host "🎉 PART 2 PIPELINE RUN COMPLETE!" -ForegroundColor Cyan
Write-Host "==================================================================" -ForegroundColor Cyan
