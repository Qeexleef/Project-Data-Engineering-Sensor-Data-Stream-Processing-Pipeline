#!/usr/bin/env python
from apscheduler.schedulers.blocking import BlockingScheduler
import dwd_warnings
import json
from kafka import KafkaProducer
import logging
import openmeteo_requests
import os
import pandas as pd
import requests_cache
from retry_requests import retry
import sys


def pull_daily_forecast(sensor_ids, latitudes, longitudes):
    # Setup the Open-Meteo API client with cache and retry on error
    cache_session = requests_cache.CachedSession(".cache", expire_after=600)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)
    # Specify Open-Meteo API with parameters
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": latitudes,
        "longitude": longitudes,
        "daily": [
            "temperature_2m_max",
            "temperature_2m_min",
            "apparent_temperature_max",
            "apparent_temperature_min",
            "rain_sum",
            "snowfall_sum",
            "precipitation_sum",
            "precipitation_hours",
            "wind_speed_10m_max",
            "wind_gusts_10m_max",
        ],
        "timezone": "Europe/Berlin",
        "forecast_days": 1,
    }
    # Pull data
    responses = openmeteo.weather_api(url, params=params)
    # Process multiple locations
    forecast_data = []
    for sid, lat, long, response in zip(sensor_ids, latitudes, longitudes, responses):
        # Process daily data
        daily = response.Daily()
        date_time = pd.to_datetime(daily.Time(), unit="s", utc=True)

        daily_data = {
            "sensor_id": sid,
            "date_time": date_time.strftime("%Y-%m-%d %H:%M:%S"),
            "latitude": lat,
            "longitude": long,
            "elevation": response.Elevation(),
            "temperature_2m_max": daily.Variables(0).ValuesAsNumpy()[0].item(),
            "temperature_2m_min": daily.Variables(1).ValuesAsNumpy()[0].item(),
            "apparent_temperature_max": daily.Variables(2).ValuesAsNumpy()[0].item(),
            "apparent_temperature_min": daily.Variables(3).ValuesAsNumpy()[0].item(),
            "rain_sum": daily.Variables(4).ValuesAsNumpy()[0].item(),
            "snowfall_sum": daily.Variables(5).ValuesAsNumpy()[0].item(),
            "precipitation_sum": daily.Variables(6).ValuesAsNumpy()[0].item(),
            "precipitation_hours": daily.Variables(7).ValuesAsNumpy()[0].item(),
            "wind_speed_10m_max": daily.Variables(8).ValuesAsNumpy()[0].item(),
            "wind_gusts_10m_max": daily.Variables(9).ValuesAsNumpy()[0].item(),
            "warnings": [],
        }

        # Generate exemplary heat warnings (depending on daily weather condition)
        heat_level = dwd_warnings.heat_level(
            daily_data["apparent_temperature_max"], daily_data["temperature_2m_min"]
        )
        if heat_level:
            daily_data["warnings"].append({"heat": heat_level})

        forecast_data.append(daily_data)
    return forecast_data


def send_daily_forecast_to_kafka(
    sensor_ids, latitudes, longitudes, kafka_producer, topic
):
    forecast_data = pull_daily_forecast(sensor_ids, latitudes, longitudes)
    for data in forecast_data:
        # Send weather forecast to Kafka
        message = json.dumps(data, indent=2)
        kafka_producer.send(topic, value=bytes(message, "utf-8"))
        logger.info(f"Sent: {message}")
        sys.stdout.flush()
    kafka_producer.flush()



# Enable logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

# Read locations from csv file
df = pd.read_csv("locations.csv")
sensor_ids = df["sensor_id"].tolist()
latitudes = df["latitude"].tolist()
longitudes = df["longitude"].tolist()

# Setup Kafka producer
bootstrap_servers = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
topic = os.environ["KAFKA_TOPIC"]
producer = KafkaProducer(bootstrap_servers=bootstrap_servers, retries=10)

# Schedule weather model simulation and
# Send weather forecasts to Kafka
send_daily_forecast_to_kafka(sensor_ids, latitudes, longitudes, producer, topic)
scheduler = BlockingScheduler()
scheduler.add_job(
    send_daily_forecast_to_kafka,
    "cron",
    hour=0,
    minute=0,
    args=[sensor_ids, latitudes, longitudes, producer, topic],
)
scheduler.start()
