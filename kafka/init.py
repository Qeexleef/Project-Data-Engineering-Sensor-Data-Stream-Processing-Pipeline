#!/usr/bin/env python
import logging
import os
import time
from kafka import KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import NoBrokersAvailable, TopicAlreadyExistsError

# Enable logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

# Try to connect to Kafka service
bootstrap_servers = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
topics = os.environ["KAFKA_TOPICS"]
try:
    client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
except NoBrokersAvailable:
    logger.error("Kafka not ready", exc_info=True)
    exit(1)

# Create topics
new_topics = [
    NewTopic(topic, num_partitions=3, replication_factor=1)
    for topic in topics.split(",")
]
try:
    client.create_topics(new_topics)
    logger.info(f"Created topics: {topics}")
except TopicAlreadyExistsError:
    logger.error("Topics already exist", exc_info=True)
