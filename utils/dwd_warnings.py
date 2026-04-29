#!/usr/bin/env python

# define some exemplary weather warning levels following the criteria of the DWD:
# https://www.dwd.de/DE/wetter/warnungen_aktuell/kriterien/warnkriterien.html


def wind_gust_level(wind_gust_max):
    if wind_gust_max > 140:
        return 4
    if wind_gust_max > 105:
        return 3
    if wind_gust_max > 65:
        return 2
    if wind_gust_max > 50:
        return 1
    return 0


def rainfall_level(rain_sum, precipitation_hours):
    if (
        rain_sum > 80
        or (rain_sum > 70 and precipitation_hours <= 12)
        or (rain_sum > 60 and precipitation_hours <= 6)
        or (rain_sum > 40 and precipitation_hours <= 1)
    ):
        return 4
    if (
        rain_sum > 50
        or (rain_sum > 40 and precipitation_hours <= 12)
        or (rain_sum > 35 and precipitation_hours <= 6)
        or (rain_sum > 25 and precipitation_hours <= 1)
    ):
        return 3
    if (
        rain_sum > 30
        or (rain_sum > 25 and precipitation_hours <= 12)
        or (rain_sum > 20 and precipitation_hours <= 6)
        or (rain_sum > 15 and precipitation_hours <= 1)
    ):
        return 2
    return 0


def frost_level(temperature_min, elevation):
    if elevation <= 800:
        if temperature_min < -10:
            return 2
        if temperature_min < 0:
            return 1
    return 0


def heat_level(apparent_temperature_max, temperature_min):
    if apparent_temperature_max > 38:
        return 3
    if apparent_temperature_max > 32 and temperature_min >= 20:
        return 1
    return 0
