# MySQL to Kafka Data Pipeline

This repository contains two Python scripts to implement a data pipeline for fetching MySQL data, producing it to Kafka, and then consuming it to write into a CSV file.

## Features
- **Producer**: Reads data from a MySQL table and streams it into a Kafka topic in real-time.
- **Consumer**: Consumes data from a Kafka topic and appends it to a CSV file.

## Prerequisites

Ensure you have the following before running the scripts:
- Python 3.7 or higher
- Kafka installed and running locally or remotely
- MySQL database with a valid schema and data
- CSV file writable on the specified path

## Installation

### Clone the Repository
```bash
git clone https://github.com/your-username/mysql-to-kafka-pipeline.git
cd mysql-to-kafka-pipeline
```

### Install Dependencies
```bash
pip install confluent-kafka mysql-connector-python
```

### Set Up MySQL
Ensure your MySQL database is accessible and populated with the required table and data:
- Host: `127.0.0.1`
- Table: `transactions`

Modify the script if your configuration differs.

### Configure Kafka
1. Start your Kafka server and ensure the broker is running (e.g., `localhost:9092`).
2. Create a topic named `mysql_data` (or modify the topic name in the scripts).
   ```bash
   kafka-topics --create --topic mysql_data --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
   ```

## Usage

### Producer Script

#### Configuration
Update the following variables in the `producer.py` file:
- `BROKER`: Address of your Kafka broker
- `MYSQL_HOST`, `MYSQL_USER`, `MYSQL_PASSWORD`, `MYSQL_DATABASE`, `MYSQL_TABLE`: MySQL credentials

#### Run Producer
```bash
python producer.py
```

The producer fetches data from MySQL at 30-second intervals and streams it to the Kafka topic `mysql_data`.

### Consumer Script

#### Configuration
Update the following variables in the `consumer.py` file:
- `KAFKA_TOPIC`: Kafka topic to consume from
- `BROKER`: Address of your Kafka broker
- `CSV_FILE`: Path to the output CSV file

#### Run Consumer
```bash
python consumer.py
```

The consumer listens to the Kafka topic and appends received messages to the specified CSV file.

## Customization
- **Fetching Interval**: Modify the sleep duration in the producer (`time.sleep(30)`) to change how frequently data is fetched from MySQL.
- **Kafka Topic**: Change the `KAFKA_TOPIC` variable in both scripts to use a different topic.
- **Output CSV**: Adjust the `CSV_FILE` variable in `consumer.py` to specify a custom output file.

## Troubleshooting

1. **Kafka Not Running**:
   - Verify Kafka is installed and running.
   - Check the broker address in the scripts.

2. **Database Connection Issues**:
   - Confirm your MySQL database is accessible using the configured host and credentials.

3. **Missing Kafka Messages**:
   - Ensure the topic exists.
   - Verify the producer and consumer group settings.


