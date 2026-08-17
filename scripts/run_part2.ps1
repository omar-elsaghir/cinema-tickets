$containerCheck = docker ps | Select-String "namenode"
if (-not $containerCheck) {
    Write-Host "Error: Container 'namenode' is not running. Run 'docker compose up -d' first."
    exit 1
}

python jobs/run_analytics.py
if ($LASTEXITCODE -ne 0) {
    py -3.12 jobs/run_analytics.py
}

docker exec namenode hdfs dfs -ls /ticket_system/processed
