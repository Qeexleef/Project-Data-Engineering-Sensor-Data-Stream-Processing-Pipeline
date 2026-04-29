#!/usr/bin/env python
import json
from kafka import KafkaConsumer
import logging
import os
import psycopg2
from psycopg2.extras import Json
import sys


# Enable logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

# Connect to Postgres DB
postgres_url = os.environ["POSTGRES_URL"]
conn = psycopg2.connect(postgres_url)
cur = conn.cursor()

# Setup Kafka consumer
bootstrap_servers = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
topic = os.environ["KAFKA_TOPIC"]
consumer = KafkaConsumer(
    topic,
    bootstrap_servers=bootstrap_servers,
)
logger.info(f"Listening to topic: {topic}")


# Read sensor data and save them into a Postgres DBS
for msg in consumer:
    data = json.loads(msg.value.decode())

    cur.execute(
        """
        INSERT INTO locations (sensor_id, latitude, longitude, elevation)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (sensor_id)
        DO UPDATE SET
            latitude = EXCLUDED.latitude,
            longitude = EXCLUDED.longitude,
            elevation = EXCLUDED.elevation
        ;
        """,
        (data["sensor_id"], data["latitude"], data["longitude"], data["elevation"]),
    )
    # reformatting data
    columns = []
    values = []
    for key, value in data.items():
        if key not in ("sensor_id", "latitude", "longitude", "elevation"):
            columns.append(key)
            values.append(value)

    cur.execute(
        f"""
        INSERT INTO {topic} (sensor_id, {", ".join(columns)})
        VALUES (%s, {", ".join(["%s"] * len(columns))})
        ON CONFLICT (sensor_id, date_time) DO NOTHING;
        """,
        (data["sensor_id"], *values),
    )
    conn.commit()  # TODO in production: use once at the end of the loop
    logger.info(f"Received: {data}")
    sys.stdout.flush()


# Close connection
cur.close()
conn.close()
