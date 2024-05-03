# Kafka-based Wiki Changes Application

This repository contains code for a Kafka-based application to track and analyze changes in a Wiki dataset.

## Installation

1. Install dependencies by running:
   pip install -r requirements.txt

2. Set up Kafka and Zookeeper using Docker Compose:
   docker-compose up

3. Create a Kafka topic named `wiki-changes`:
   kafka-topics.sh --create --topic wiki-changes --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

## Usage

1. Run the Kafka producer to simulate Wiki changes:
   python kafka_producer.py

2. Run the Kafka consumer to process and analyze Wiki changes:
   python kafka_consumer.py

## Description

The Kafka producer (`kafka_producer.py`) simulates changes in a Wiki dataset by continuously producing messages to the `wiki-changes` Kafka topic. These messages represent changes in the form of JSON objects.

The Kafka consumer (`kafka_consumer.py`) consumes messages from the `wiki-changes` topic, processes them, and performs analysis such as counting the number of changes per title. It uses Spark Structured Streaming for real-time processing and Matplotlib for visualization.
