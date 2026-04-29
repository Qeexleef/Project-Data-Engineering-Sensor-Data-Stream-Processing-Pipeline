#!/usr/bin/env python
import logging
import os
import psycopg2
import sqlparse

# Enable logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

# Connect to PostgreSQL DB
postgres_url = os.environ["POSTGRES_URL"]
try:
    conn = psycopg2.connect(postgres_url)
except:
    logger.error("Postgres not ready", exc_info=True)
    exit(1)
cur = conn.cursor()

# Create tables
with open("create_tables.sql", "r") as file:
    sql = file.read()

for statement in sqlparse.split(sql):
    cur.execute(statement)
logger.info("Created tables")
conn.commit()

# Close connection
cur.close()
conn.close()
