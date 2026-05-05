"""
Continuous ML Orchestrator
A daemon script that periodically triggers the ETL pipeline (Bronze to Silver to Gold)
and retrains the Machine Learning model to ensure near real-time recommendation updates.
"""
import time
import os

def main():
    print("==================================================")
    print("[INFO] STARTING CONTINUOUS ML PIPELINE DAEMON")
    print("[INFO] Polling interval: 20 seconds")
    print("==================================================\n")

    cycle = 1
    poll_interval = 20

    while True:
        print(f"--- [Cycle {cycle}] Execution Started ---")
        
        print(f"[Cycle {cycle}] 1/3: Executing Silver Layer ETL...")
        os.system("python batch_silver.py 2>/dev/null") 
        
        print(f"[Cycle {cycle}] 2/3: Executing Gold Layer ETL (Data Warehouse)...")
        os.system("python etl_gold_warehouse.py 2>/dev/null")
        
        print(f"[Cycle {cycle}] 3/3: Retraining ALS Recommendation Model...")
        os.system("python train_als.py 2>/dev/null")
        
        print(f"--- [Cycle {cycle}] Execution Completed. Sleeping for {poll_interval}s ---\n")
        time.sleep(poll_interval)
        cycle += 1

if __name__ == "__main__":
    main()
