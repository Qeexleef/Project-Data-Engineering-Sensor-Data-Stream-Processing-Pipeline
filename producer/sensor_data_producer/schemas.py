#!/usr/bin/env python
from pydantic import BaseModel


class Weather(BaseModel):
    sensor_id: int
    date_time: str
    latitude: float
    longitude: float
    elevation: float
    temperature_2m: float
    relative_humidity_2m: float
    apparent_temperature: float
    is_day: bool
    precipitation: float
    rain: float
    snowfall: float
    surface_pressure: float
    wind_speed_10m: float
    wind_direction_10m: float
    wind_gusts_10m: float


class AirQuality(BaseModel):
    sensor_id: int
    date_time: str
    latitude: float
    longitude: float
    elevation: float
    pm10: float
    pm2_5: float
    carbon_monoxide: float
    nitrogen_dioxide: float
    sulphur_dioxide: float
    ozone: float
    dust: float
    uv_index: float
