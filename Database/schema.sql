CREATE TABLE weather (
    timestamp     TIMESTAMPTZ PRIMARY KEY,
    temperature   REAL,
    precipitation REAL,
    snowfall      REAL,
    windspeed     REAL,
    condition     INT
);

CREATE TABLE rooms (
    room         TEXT PRIMARY KEY,
    name         TEXT NOT NULL,
    capacity     INTEGER
);

CREATE TABLE classes (
    class_id     SERIAL PRIMARY KEY,
    room         TEXT REFERENCES rooms(room),
    name         TEXT NOT NULL,
    day_of_week  INTEGER,
    start_time   TIME,
    end_time     TIME,
    weekly_hours REAL
);

CREATE TABLE events (
    id              SERIAL PRIMARY KEY,
    room            TEXT REFERENCES rooms(room),
    timestamp       TIMESTAMPTZ DEFAULT NOW(),
    direction       TEXT,
    total_in        INTEGER DEFAULT 0,
    total_out       INTEGER DEFAULT 0,
    occupancy       INTEGER
);

CREATE TABLE room_state (
    room         TEXT PRIMARY KEY REFERENCES rooms(room),
    occupancy    INTEGER NOT NULL DEFAULT 0,
    total_in     INTEGER NOT NULL DEFAULT 0,
    total_out    INTEGER NOT NULL DEFAULT 0,
    last_update  TIMESTAMPTZ NOT NULL
);
