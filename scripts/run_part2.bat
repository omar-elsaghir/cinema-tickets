@echo off

docker ps | findstr /i "namenode" >nul
if errorlevel 1 (
    echo Error: namenode container is not running. Run docker compose up -d first.
    exit /b 1
)

python jobs/run_analytics.py
if errorlevel 1 (
    py -3.12 jobs/run_analytics.py
)

docker exec namenode hdfs dfs -ls /ticket_system/processed
