"""
Flask Web Application
Serves the Netflix-clone UI, handles user rating submissions to Kafka,
and exposes the ALS recommendation payload via REST API.
"""
from flask import Flask, render_template, request, jsonify
from kafka import KafkaProducer
import json
import time
import os

app = Flask(__name__)

# Initialize Kafka Producer globally
try:
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    print("[INFO] Successfully connected to Kafka Broker.")
except Exception as e:
    producer = None
    print(f"[ERROR] Failed to connect to Kafka Broker: {e}")

@app.route('/')
def index():
    """Serves the main frontend application."""
    return render_template('index.html')

@app.route('/api/rate', methods=['POST'])
def rate_movie():
    """Endpoint to ingest user ratings and publish to the Kafka event stream."""
    if not producer:
        return jsonify({"status": "error", "message": "Kafka producer is not initialized."}), 500

    data = request.json
    try:
        payload = {
            "user_id": int(data['user_id']),
            "movie_id": int(data['movie_id']),
            "rating": float(data['rating']),
            "timestamp": int(time.time())
        }
        producer.send("movie_ratings", payload)
        producer.flush()
        return jsonify({"status": "success", "message": "Event published successfully."})
    except (KeyError, ValueError) as e:
        return jsonify({"status": "error", "message": f"Invalid payload structure: {e}"}), 400

@app.route('/api/recommendations/<int:user_id>', methods=['GET'])
def get_recommendations(user_id):
    """Endpoint to retrieve personalized movie recommendations for a specific user."""
    rec_file = "data/recommendations.json"
    
    if not os.path.exists(rec_file):
        return jsonify({"status": "error", "message": "Recommendation model artifact not found."}), 404
    
    try:
        with open(rec_file, 'r') as f:
            recs = json.load(f)
        
        # Parse recommendation structures
        for row in recs:
            if row.get('user_id') == user_id:
                formatted_recs = [{"movie_id": m[0], "rating": m[1]} for m in row.get('recommendations', [])]
                return jsonify({"status": "success", "data": formatted_recs})
                
        return jsonify({"status": "error", "message": "No recommendations found for the specified user."}), 404
    except Exception as e:
        return jsonify({"status": "error", "message": f"Internal processing error: {e}"}), 500

if __name__ == '__main__':
    app.run(debug=True, port=5000)
