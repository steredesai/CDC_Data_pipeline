from confluent_kafka import Consumer
import json
import csv

# Kafka Configuration
KAFKA_TOPIC = 'mysql_data'
BROKER = ''
CSV_FILE = 'mysql_data.csv'

# Initialize Kafka consumer
consumer = Consumer({
    'bootstrap.servers': BROKER,
    'group.id': 'csv_writer_group',
    'auto.offset.reset': 'earliest'
})

# Subscribe to Kafka topic
consumer.subscribe([KAFKA_TOPIC])

# Open CSV file in append mode and set up a CSV writer
with open(CSV_FILE, mode='a', newline='') as file:
    writer = csv.writer(file)
    
    # Write CSV headers only if the file is empty
    file.seek(0, 2)  # Move to end of file to check if it's empty
    if file.tell() == 0:
        writer.writerow(["id", "name", "amount", "timestamp"])  # Write headers if file is empty

    print(f"Listening for data on topic {KAFKA_TOPIC}...")

    try:
        # Consume messages from Kafka and write to CSV
        while True:
            message = consumer.poll(timeout=1.0)

            if message is None:
                continue

            if message.error():
                print(f"Error: {message.error()}")
                continue

            # Deserialize the message value (JSON format)
            data = json.loads(message.value().decode('utf-8'))
            print(f"Received: {data}")

            # Extract and handle missing values
            id_value = data.get("id", "N/A")
            name_value = data.get("name", "N/A")
            amount_value = data.get("amount", "N/A")
            timestamp_value = data.get("timestamp", "N/A")

            # Write the data to the CSV file
            writer.writerow([id_value, name_value, amount_value, timestamp_value])

    except KeyboardInterrupt:
        print("Consumer interrupted")

    finally:
        # Close the consumer
        consumer.close()
