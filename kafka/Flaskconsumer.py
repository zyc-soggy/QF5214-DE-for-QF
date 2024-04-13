from kafka import KafkaConsumer
from flask import Flask, jsonify
import threading
import json


app = Flask(__name__)

# List to hold the data for our dashboard.
dashboard_data = []

consumer = KafkaConsumer(
    'Stock',
    bootstrap_servers='3.27.226.3:9092',  # Replace with your EC2 IP address.
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

def kafka_consumer():
    global dashboard_data
    for message in consumer:
        dashboard_data.append(message.value)
        if len(dashboard_data) > 10:  # Keeping the last 10 data points.
            dashboard_data.pop(0)

threading.Thread(target=kafka_consumer).start()

@app.route('/data', methods=['GET'])
def get_data():
    return jsonify(dashboard_data)

@app.route('/')
def index():
    return "Real-time Analytics Dashboard"

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)  # This will make the Flask app accessible via the EC2's public IP.