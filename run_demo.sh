#!/bin/bash

# ==============================================================================
# Big Data Pipeline Initialization Script
# Orchestrates the startup sequence for the entire Medallion architecture.
# ==============================================================================

echo "[INFO] Commencing Pipeline Initialization Sequence..."
echo "==================================================="

echo "[INFO] Step 0: Purging legacy data artifacts..."
rm -rf data/

echo "[INFO] Step 1: Initializing infrastructure (Kafka & PostgreSQL)..."
sudo docker compose up -d
echo "[INFO] Awaiting infrastructure stabilization (10s)..."
sleep 10

echo "[INFO] Step 2: Bootstrapping Data Simulator (Background Process)..."
python data_simulator.py > simulator.log 2>&1 &
SIMULATOR_PID=$!
echo "[INFO] Simulator PID: $SIMULATOR_PID"
sleep 3

echo "[INFO] Step 3: Initializing Spark Structured Streaming (Bronze Layer)..."
python spark_streaming_delta.py > streaming.log 2>&1 &
SPARK_STREAM_PID=$!
echo "[INFO] Streaming PID: $SPARK_STREAM_PID"

echo "[INFO] Polling for Bronze layer initialization (Timeout: 45s)..."
for i in {1..9}; do
  if [ -d "data/bronze/_delta_log" ]; then
    echo "[INFO] Bronze Delta table detected. Buffering initial data stream (10s)..."
    sleep 10
    break
  fi
  sleep 5
done

echo "[INFO] Step 4: Executing Silver Layer Batch ETL..."
python batch_silver.py

echo "[INFO] Step 5: Executing Gold Layer ETL (PostgreSQL Data Warehouse)..."
python etl_gold_warehouse.py

echo "[INFO] Step 6: Triggering initial Machine Learning (ALS) Model Training..."
python train_als.py

echo "[INFO] Step 7: Verifying Delta Lake Time Travel compliance..."
python demo_time_travel.py

echo "==================================================="
echo "[INFO] Terminating background processes to release resources..."
kill $SIMULATOR_PID
kill $SPARK_STREAM_PID
echo "[INFO] Initialization sequence completed successfully."
