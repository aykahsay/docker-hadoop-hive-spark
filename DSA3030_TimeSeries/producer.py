import time
import json
import yfinance as yf
from kafka import KafkaProducer
from datetime import datetime

# Initialize Kafka Producer
# Uses localhost:9092 because we are running this on the host machine,
# and docker-compose exposes port 9092 to the host.
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

TOPIC_NAME = 'forex_stream'

print(f"[{datetime.now()}] Starting Producer...")
print(f"Fetching USD/KES historical and live data from Yahoo Finance...")

# We fetch a chunk of historical data so the Spark processor has something substantial to process
# Normally, a producer might just fetch real-time ticks, but yfinance only provides daily OHLC for this pair
data = yf.Ticker("KES=X").history(period="5y")
data.reset_index(inplace=True)

# Simulate streaming the historical data line by line
for index, row in data.iterrows():
    message = {
        "date": row['Date'].strftime('%Y-%m-%d'),
        "open": float(row['Open']),
        "high": float(row['High']),
        "low": float(row['Low']),
        "close": float(row['Close']),
        "volume": float(row['Volume'])
    }
    
    # Send to Kafka
    producer.send(TOPIC_NAME, value=message)
    print(f"Sent: {message}")
    
    # Small delay to simulate streaming rather than a massive dump
    time.sleep(0.01)

# Ensure all messages are sent
producer.flush()
print(f"[{datetime.now()}] Finished streaming dataset to Kafka topic '{TOPIC_NAME}'.")
