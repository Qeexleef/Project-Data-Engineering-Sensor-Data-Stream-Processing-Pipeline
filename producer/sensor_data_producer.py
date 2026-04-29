#!/usr/bin/env python
from fastapi import FastAPI
import inspect
import json
from kafka import KafkaProducer
import logging
import os
import pandas as pd
from pydantic import BaseModel
import schemas
import sys


# Helper function to handle different sensor types and send data to Kafka
def make_handler(topic, schema_objects):
    schema_class = schema_objects[topic]

    async def sensor_data_handler(
        item: schema_class,
    ):  # validated JSON automatically parsed
        # Send weather forecast to Kafka
        message = json.dumps(item.model_dump(), indent=2)
        producer.send(topic, value=bytes(message, "utf-8"))
        producer.flush()  # TODO do not use in production
        logger.info(f"Sent: {message}")
        sys.stdout.flush()

    return sensor_data_handler


# Helper function to map topics to pydantic schema classes (imported as schemas)
def map_pydantic_schemas(topics):
    lowercase_topics = {topic.replace("_", "").lower(): topic for topic in topics}
    pydantic_schemas = {}
    for name, obj in inspect.getmembers(schemas, inspect.isclass):
        # skip anything that is not a Pydantic model
        if not issubclass(obj, BaseModel):
            continue
        # skip BaseModel itself
        if obj is BaseModel:
            continue
        # map topic to schema (Pydantic object)
        if name.lower() in lowercase_topics:
            topic = lowercase_topics[name.lower()]
            pydantic_schemas[topic] = obj
        else:
            logger.error(f"No pydantic model class found to topic {topic}")
            exit(1)
    return pydantic_schemas



# Enable logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

# Setup Kafka producer
topics = os.environ["KAFKA_TOPICS"].split(",")
bootstrap_servers = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
producer = KafkaProducer(bootstrap_servers=bootstrap_servers, retries=10)

# For each topic:
# Specify incoming JSON format via pydantic schema classes (imported as schemas)
schema_objects = map_pydantic_schemas(topics)

# Listen to incoming sensor data
app = FastAPI()
for topic in topics:
    handler = make_handler(topic, schema_objects)
    handler.__name__ = f"receive_{topic}"
    app.post(f"/{topic}")(handler)
