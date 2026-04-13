CREATE TABLE rooms (
    room_number       SERIAL PRIMARY KEY,
    name     TEXT NOT NULL,
    capacity INTEGER
);

CREATE TABLE classes (
    class_id          SERIAL PRIMARY KEY,
    room_id     INTEGER REFERENCES rooms(room_number),
    name        TEXT NOT NULL,
    day_of_week INTEGER,
    start_time  TIME,
    end_time    TIME,
    rating      REAL
);

CREATE TABLE weather (
    timestamp     TIMESTAMPTZ PRIMARY KEY,
    temperature   REAL,
    precipitation REAL,
    snowfall      REAL,
    windspeed     REAL,
    condition     INT
);


CREATE TABLE sensor_readings (
    id              SERIAL PRIMARY KEY,
    room_number         INTEGER REFERENCES rooms(room_number),
    timestamp       TIMESTAMPTZ DEFAULT NOW(),
    occupancy_count INTEGER
);
