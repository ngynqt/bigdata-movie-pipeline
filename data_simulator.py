"""
Data Simulator
Simulates a real-time event stream of user movie ratings and publishes to Apache Kafka.
"""
from kafka import KafkaProducer
import json
import time
import random

def main():
    try:
        producer = KafkaProducer(
            bootstrap_servers='localhost:9092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
    except Exception as e:
        print(f"[ERROR] Failed to connect to Kafka Broker: {e}")
        return

    print("[INFO] Starting data simulation stream...")
    while True:
        # Batch simulation: generate 50 records per iteration for higher throughput
        for _ in range(50):
            # Simulate historical data distribution over the past year (365 days)
            current_time = int(time.time())
            one_year_ago = current_time - (365 * 24 * 60 * 60)
            
            payload = {
                "user_id": random.randint(1, 100),
                "movie_id": random.randint(1, 30),
                "rating": round(random.uniform(1, 5), 1),
                "timestamp": random.randint(one_year_ago, current_time)
            }
            producer.send("movie_ratings", payload)
        
        producer.flush()
        print("[INFO] Successfully published 50 records to Kafka topic 'movie_ratings'.")
        time.sleep(0.5)

if __name__ == "__main__":
    main()
