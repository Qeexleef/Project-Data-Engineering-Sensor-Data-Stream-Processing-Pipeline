#!/usr/bin/env python
import logging
import os
from pymongo import MongoClient

# Enable logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

# Connect to Mongo DB
mongo_url = os.environ["MONGODB_URL"]
try:
    client = MongoClient(mongo_url)
    client.admin.command("ping")
except:
    logger.error("MongoDB not ready", exc_info=True)
    exit(1)

# Initialize collection
db_name = os.environ["MONGO_DATABASE"]
collection_name = os.environ["MONGO_COLLECTION"]
db = client[db_name]
collection = db[collection_name]
# create sensor_id index
collection.create_index([("sensor_id", 1)])
logger.info(f"Created collection {collection_name} in database {db_name}")

# Close connection
client.close()
