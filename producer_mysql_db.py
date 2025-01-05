from confluent_kafka import Producer
import json
import mysql.connector
from datetime import datetime
from decimal import Decimal
import time

# Kafka Configuration
KAFKA_TOPIC = 'mysql_data'
BROKER = 'localhost:9092'

# MySQL Configuration
MYSQL_HOST = "127.0.0.1"
MYSQL_USER = ""
MYSQL_PASSWORD = ""
MYSQL_DATABASE = "kafka"
MYSQL_TABLE = "transactions"

# Initialize Kafka producer
producer = Producer({'bootstrap.servers': BROKER})

# MySQL connection setup
db_connection = mysql.connector.connect(
    host=MYSQL_HOST,
    user=MYSQL_USER,
    password=MYSQL_PASSWORD,
    database=MYSQL_DATABASE
)

# Variable to track the last fetched timestamp
last_timestamp = None

# Function to fetch data from MySQL
def fetch_data_from_mysql():
    global last_timestamp
    cursor = db_connection.cursor(dictionary=True)
    
    # If last_timestamp is None, fetch all data; otherwise, fetch new data
    if last_timestamp is None:
        query = f"SELECT * FROM {MYSQL_TABLE}"
    else:
        query = f"SELECT * FROM {MYSQL_TABLE} WHERE timestamp > %s"
    
    cursor.execute(query, (last_timestamp,) if last_timestamp else ())
    rows = cursor.fetchall()
    cursor.close()

    if rows:
        # Update the last timestamp based on the most recent record
        last_timestamp = max([row['timestamp'] for row in rows])
    
    return rows

# Function to convert Decimal to float
def convert_decimal_to_float(data):
    if isinstance(data, Decimal):
        return float(data)
    elif isinstance(data, dict):
        return {key: convert_decimal_to_float(value) for key, value in data.items()}
    elif isinstance(data, list):
        return [convert_decimal_to_float(item) for item in data]
    else:
        return data

# Function to send data to Kafka
def send_to_kafka(data):
    for record in data:
        # Convert Decimal objects to float
        record = convert_decimal_to_float(record)
        
        # Convert datetime to string for serialization
        record = {key: (value.isoformat() if isinstance(value, datetime) else value)
                  for key, value in record.items()}
        
        producer.produce(KAFKA_TOPIC, value=json.dumps(record))
        producer.flush()
        print(f"Sent: {record}")

# Main producer loop
if __name__ == "__main__":
    while True:
        data = fetch_data_from_mysql()
        if data:
            send_to_kafka(data)
        else:
            print("No new data found in MySQL.")
        
        # Sleep for a short interval before fetching again (real-time behavior)
        time.sleep(30)  # Fetch every second or adjust as needed
