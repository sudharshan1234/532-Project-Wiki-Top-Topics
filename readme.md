# Kafka-based Wiki Changes Application

This repository contains code for a Kafka-based application to track and analyze changes in a Wiki dataset.

## Installation

1. Install dependencies by running:
   pip install -r requirements.txt

2. Set up Kafka and Zookeeper using Docker Compose (If running in local. If AWS MSK is being used, make sure to update the bootstrap servers in producer and consumer):
   docker-compose up

3. Create a Kafka topic named `wiki-changes` (replace the bootstrap servers here if required):
   kafka-topics.sh --create --topic wiki-changes --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

## Usage

1. Run the Kafka producer to simulate Wiki changes:
   python wiki-producer.py

2. Run the Kafka consumer to process and analyze Wiki changes:
   python wiki-consumer.py

3. Run the wiki-display-table.py to boot up a flask server in port 5000. Use the public ip to access the webpage.

## Description

The wiki producer (`wiki-producer.py`) reads streaming events from the Wikimedia recent changes live stream API and sends relevant data to a Kafka topic named `wiki-changes`. The script sets up a Kafka producer with a specific list of bootstrap servers and configures JSON serialization for the message values. It filters events to include only those from the English Wikipedia domain (en.wikipedia.org). For each valid event, it sends the data to the Kafka topic and handles callbacks for successful sends and error back handling for failed sends. The script runs indefinitely until interrupted, ensuring that all messages are sent and the producer resources are properly closed upon termination. Logging is configured to report errors, enhancing the debugging process.

The wiki consumer (`wiki-consumer.py`) sets up a Spark streaming application using the PySpark library to consume and process events from a Kafka topic named `wiki-changes`. The script initializes a Spark session, specifies Kafka bootstrap servers, and configures the stream to read from Kafka. It defines a complex schema for the incoming JSON-formatted data, extracts and transforms the data into a more usable form, and converts timestamps into appropriate formats. The processed data is then filtered to include only entries from the last 30 minutes, partitioned by date, and written to a Parquet file. The script also configures a checkpoint directory for fault tolerance and uses the append output mode to continuously update the output as new data arrives.

The Flask application (`wiki-display-table.py`) initializes a Spark session and sets up a web server. The application reads recent changes from parquet files, aggregates them to count the occurrences of each title, and displays the top 20 most frequently changed titles in a table format on a web page. This page auto-refreshes every 2 seconds to show updated results.
