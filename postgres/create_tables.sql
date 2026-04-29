CREATE TABLE IF NOT EXISTS locations (
    sensor_id SERIAL PRIMARY KEY,
    latitude REAL NOT NULL,
    longitude REAL NOT NULL,
    elevation REAL,
    UNIQUE (sensor_id)
);

CREATE TABLE IF NOT EXISTS weather (
    sensor_id INTEGER NOT NULL,
    date_time TIMESTAMP NOT NULL,
    temperature_2m REAL,
    relative_humidity_2m REAL,
    apparent_temperature REAL,
    is_day BOOLEAN,
    precipitation REAL,
    rain REAL,
    snowfall REAL,
    surface_pressure REAL,
    wind_speed_10m REAL,
    wind_direction_10m REAL,
    wind_gusts_10m REAL,
    PRIMARY KEY (sensor_id, date_time),
    FOREIGN KEY (sensor_id) REFERENCES locations(sensor_id),
    UNIQUE (sensor_id, date_time)
);

CREATE TABLE IF NOT EXISTS air_quality (
    sensor_id INTEGER NOT NULL,
    date_time TIMESTAMP NOT NULL,
    pm10 REAL,
    pm2_5 REAL,
    carbon_monoxide REAL,
    nitrogen_dioxide REAL,
    sulphur_dioxide REAL,
    ozone REAL,
    dust REAL,
    uv_index REAL,
    PRIMARY KEY (sensor_id, date_time),
    FOREIGN KEY (sensor_id) REFERENCES locations(sensor_id),
    UNIQUE (sensor_id, date_time)
);