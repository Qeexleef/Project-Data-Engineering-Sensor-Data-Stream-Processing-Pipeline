#!/usr/bin/env python
from apscheduler.schedulers.blocking import BlockingScheduler
from datetime import datetime
import openmeteo_requests
import os
import pandas as pd
from requests import post
import requests_cache
from retry_requests import retry
from time import sleep


def simulate_sensor_data_stream(sensor_ids, latitudes, longitudes, producer_url):
    # Setup the Open-Meteo API client with cache and retry on error
    cache_session = requests_cache.CachedSession(".cache", expire_after=300)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)
    # Pull current Open-Meteo data
    weather_data = pull_exemplary_weather_data(
        openmeteo, sensor_ids, latitudes, longitudes
    )
    air_quality_data = pull_exemplary_air_quality_data(
        openmeteo, sensor_ids, latitudes, longitudes
    )
    # Simulate sensor data stream
    for weather, air_quality in zip(weather_data, air_quality_data):
        post(f"{producer_url}/weather", json=weather)
        sleep(10)
        post(f"{producer_url}/air_quality", json=air_quality)
        sleep(10)


def pull_exemplary_weather_data(openmeteo_client, sensor_ids, latitudes, longitudes):
    # Specify Open-Meteo API with parameters
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": latitudes,
        "longitude": longitudes,
        "current": [
            "temperature_2m",
            "relative_humidity_2m",
            "apparent_temperature",
            "is_day",
            "precipitation",
            "rain",
            "snowfall",
            "surface_pressure",
            "wind_speed_10m",
            "wind_direction_10m",
            "wind_gusts_10m",
        ],
        "timezone": "Europe/Berlin",
    }
    # Pull data
    responses = openmeteo_client.weather_api(url, params=params)
    # Process multiple locations
    weather_data = []
    for sid, lat, long, response in zip(sensor_ids, latitudes, longitudes, responses):
        # Process current weather data
        current = response.Current()
        date_time = pd.to_datetime(current.Time(), unit="s", utc=True)

        current_weather = {
            "sensor_id": sid,
            "date_time": date_time.strftime("%Y-%m-%d %H:%M:%S"),
            "latitude": lat,
            "longitude": long,
            "elevation": response.Elevation(),
            "temperature_2m": current.Variables(0).Value(),
            "relative_humidity_2m": current.Variables(1).Value(),
            "apparent_temperature": current.Variables(2).Value(),
            "is_day": current.Variables(3).Value(),
            "precipitation": current.Variables(4).Value(),
            "rain": current.Variables(5).Value(),
            "snowfall": current.Variables(6).Value(),
            "surface_pressure": current.Variables(7).Value(),
            "wind_speed_10m": current.Variables(8).Value(),
            "wind_direction_10m": current.Variables(9).Value(),
            "wind_gusts_10m": current.Variables(10).Value(),
        }
        weather_data.append(current_weather)
    return weather_data


def pull_exemplary_air_quality_data(
    openmeteo_client, sensor_ids, latitudes, longitudes
):
    url = "https://air-quality-api.open-meteo.com/v1/air-quality"
    params = {
        "latitude": latitudes,
        "longitude": longitudes,
        "current": [
            "pm10",
            "pm2_5",
            "carbon_monoxide",
            "nitrogen_dioxide",
            "sulphur_dioxide",
            "ozone",
            "dust",
            "uv_index",
        ],
        "timezone": "Europe/Berlin",
    }
    # Pull data
    responses = openmeteo_client.weather_api(url, params=params)
    # Process multiple locations
    air_quality_data = []
    for sid, lat, long, response in zip(sensor_ids, latitudes, longitudes, responses):
        # Process current air quality data
        current = response.Current()
        date_time = pd.to_datetime(current.Time(), unit="s", utc=True)

        current_air_quality = {
            "sensor_id": sid,
            "date_time": date_time.strftime("%Y-%m-%d %H:%M:%S"),
            "latitude": lat,
            "longitude": long,
            "elevation": response.Elevation(),
            "pm10": current.Variables(0).Value(),
            "pm2_5": current.Variables(1).Value(),
            "carbon_monoxide": current.Variables(2).Value(),
            "nitrogen_dioxide": current.Variables(3).Value(),
            "sulphur_dioxide": current.Variables(4).Value(),
            "ozone": current.Variables(5).Value(),
            "dust": current.Variables(6).Value(),
            "uv_index": current.Variables(7).Value(),
        }
        air_quality_data.append(current_air_quality)
    return air_quality_data



# Read locations from csv file
df = pd.read_csv("locations.csv")
sensor_ids = df["sensor_id"].tolist()
latitudes = df["latitude"].tolist()
longitudes = df["longitude"].tolist()

producer_url = os.environ["PRODUCER_URL"]

# Schedule sensor data stream simulation
scheduler = BlockingScheduler()
scheduler.add_job(
    simulate_sensor_data_stream,
    trigger="interval",
    minutes=15,
    args=[sensor_ids, latitudes, longitudes, producer_url],
    max_instances=3,  # allows overlap
    next_run_time=datetime.now(),
)
scheduler.start()
