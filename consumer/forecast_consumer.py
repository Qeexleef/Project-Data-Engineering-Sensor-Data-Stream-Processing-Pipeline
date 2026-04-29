#!/usr/bin/env python
import json
from kafka import KafkaConsumer
import logging
import os
from pymongo import MongoClient
import sys

# Enable logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

# Connect to MongoDB
mongo_url = os.environ["MONGODB_URL"]
db_name = os.environ["MONGO_DATABASE"]
collection_name = os.environ["MONGO_COLLECTION"]
client = MongoClient(mongo_url)
db = client[db_name]
collection = db[collection_name]

# Setup Kafka consumer
bootstrap_servers = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
topic = os.environ["KAFKA_TOPIC"]
consumer = KafkaConsumer(
    topic,
    bootstrap_servers=bootstrap_servers,
)
logger.info(f"Listening to topic: {topic}")

# Read weather forecasts
for msg in consumer:
    document = json.loads(msg.value.decode())
    result = collection.replace_one(
        {"sensor_id": document["sensor_id"]},
        document,
        upsert=True,
    )
    logger.info(f"Received: {document}")
    sys.stdout.flush()
