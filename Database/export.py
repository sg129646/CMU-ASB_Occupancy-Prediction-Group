// Make sure to have .env configured

import requests
import psycopg2
import os
from datetime import datetime
from dotenv import load_dotenv
import pandas as pd

load_dotenv()
# ==============================
# 2. YOUR SQL QUERY
# ==============================
QUERY = """
WITH room_list AS (
    SELECT DISTINCT rs.room
    FROM room_state rs
),

cutoff AS (
    SELECT
        date_trunc(
            'hour',
            (now() AT TIME ZONE 'America/New_York') - interval '24 hours'
        ) AS cutoff_hour
),

bounds AS (
    SELECT
        rs.room,
        date_trunc('hour', MIN(rs.last_update AT TIME ZONE 'America/New_York')) AS min_hour
    FROM room_state rs
    GROUP BY rs.room
),

room_bounds AS (
    SELECT
        b.room,
        b.min_hour,
        c.cutoff_hour AS max_hour
    FROM bounds b
    CROSS JOIN cutoff c
    WHERE b.min_hour <= c.cutoff_hour
),

hour_series AS (
    SELECT
        rb.room,
        gs.hour_start
    FROM room_bounds rb
    CROSS JOIN LATERAL generate_series(
        rb.min_hour,
        rb.max_hour,
        interval '1 hour'
    ) AS gs(hour_start)
),

hourly_base AS (
    SELECT
        hs.room,
        hs.hour_start,
        last_rs.occupancy AS occupancy_now,
        last_rs.last_update AS source_last_update_utc,
        last_rs.last_update AT TIME ZONE 'America/New_York' AS source_last_update_local
    FROM hour_series hs
    LEFT JOIN LATERAL (
        SELECT
            rs.occupancy,
            rs.last_update
        FROM room_state rs
        WHERE rs.room = hs.room
          AND (rs.last_update AT TIME ZONE 'America/New_York')
              <= hs.hour_start + interval '59 minutes 59 seconds'
        ORDER BY rs.last_update DESC
        LIMIT 1
    ) AS last_rs ON TRUE
),

hourly_enriched AS (
    SELECT
        b.*,

        r.room AS room_name,
        r.capacity,

        w.timestamp AS weather_timestamp,
        w.temperature,
        w.precipitation,
        w.snowfall,
        w.windspeed,
        w.condition,

        h.date AS holiday_date,
        h.in_session,
        h.description AS holiday_description,

        c.course_id,
        c.name AS class_name,
        c.day_of_week,
        c.start_time,
        c.end_time,
        c.fce_score,
        c.extra_hours

    FROM hourly_base b
    LEFT JOIN rooms r
        ON b.room = r.room
    LEFT JOIN weather w
        ON b.hour_start = date_trunc('hour', w.timestamp)
    LEFT JOIN holidays h
        ON b.hour_start::date = h.date
    LEFT JOIN classes c
        ON b.room = c.room
       AND c.day_of_week = CASE EXTRACT(DOW FROM b.hour_start)
            WHEN 0 THEN 'U'
            WHEN 1 THEN 'M'
            WHEN 2 THEN 'T'
            WHEN 3 THEN 'W'
            WHEN 4 THEN 'R'
            WHEN 5 THEN 'F'
            WHEN 6 THEN 'S'
       END
       AND b.hour_start::time >= c.start_time
       AND b.hour_start::time < c.end_time
),

future_hours AS (
    SELECT
        b.room,
        b.hour_start AS base_hour,
        h.hour_num,
        f.occupancy_now AS future_occupancy
    FROM hourly_enriched b
    CROSS JOIN generate_series(1, 24) AS h(hour_num)
    LEFT JOIN hourly_enriched f
        ON f.room = b.room
       AND f.hour_start = b.hour_start + (h.hour_num || ' hour')::interval
)

SELECT
    b.room,
    b.hour_start,
    b.occupancy_now,
    b.source_last_update_utc,
    b.source_last_update_local,

    b.room_name,
    b.capacity,

    b.weather_timestamp,
    b.temperature,
    b.precipitation,
    b.snowfall,
    b.windspeed,
    b.condition,

    b.holiday_date,
    b.in_session,
    b.holiday_description,

    b.course_id,
    b.class_name,
    b.day_of_week,
    b.start_time,
    b.end_time,
    b.fce_score,
    b.extra_hours,

    MAX(CASE WHEN fh.hour_num = 1  THEN fh.future_occupancy END) AS occupancy_h1,
    MAX(CASE WHEN fh.hour_num = 2  THEN fh.future_occupancy END) AS occupancy_h2,
    MAX(CASE WHEN fh.hour_num = 3  THEN fh.future_occupancy END) AS occupancy_h3,
    MAX(CASE WHEN fh.hour_num = 4  THEN fh.future_occupancy END) AS occupancy_h4,
    MAX(CASE WHEN fh.hour_num = 5  THEN fh.future_occupancy END) AS occupancy_h5,
    MAX(CASE WHEN fh.hour_num = 6  THEN fh.future_occupancy END) AS occupancy_h6,
    MAX(CASE WHEN fh.hour_num = 7  THEN fh.future_occupancy END) AS occupancy_h7,
    MAX(CASE WHEN fh.hour_num = 8  THEN fh.future_occupancy END) AS occupancy_h8,
    MAX(CASE WHEN fh.hour_num = 9  THEN fh.future_occupancy END) AS occupancy_h9,
    MAX(CASE WHEN fh.hour_num = 10 THEN fh.future_occupancy END) AS occupancy_h10,
    MAX(CASE WHEN fh.hour_num = 11 THEN fh.future_occupancy END) AS occupancy_h11,
    MAX(CASE WHEN fh.hour_num = 12 THEN fh.future_occupancy END) AS occupancy_h12,
    MAX(CASE WHEN fh.hour_num = 13 THEN fh.future_occupancy END) AS occupancy_h13,
    MAX(CASE WHEN fh.hour_num = 14 THEN fh.future_occupancy END) AS occupancy_h14,
    MAX(CASE WHEN fh.hour_num = 15 THEN fh.future_occupancy END) AS occupancy_h15,
    MAX(CASE WHEN fh.hour_num = 16 THEN fh.future_occupancy END) AS occupancy_h16,
    MAX(CASE WHEN fh.hour_num = 17 THEN fh.future_occupancy END) AS occupancy_h17,
    MAX(CASE WHEN fh.hour_num = 18 THEN fh.future_occupancy END) AS occupancy_h18,
    MAX(CASE WHEN fh.hour_num = 19 THEN fh.future_occupancy END) AS occupancy_h19,
    MAX(CASE WHEN fh.hour_num = 20 THEN fh.future_occupancy END) AS occupancy_h20,
    MAX(CASE WHEN fh.hour_num = 21 THEN fh.future_occupancy END) AS occupancy_h21,
    MAX(CASE WHEN fh.hour_num = 22 THEN fh.future_occupancy END) AS occupancy_h22,
    MAX(CASE WHEN fh.hour_num = 23 THEN fh.future_occupancy END) AS occupancy_h23,
    MAX(CASE WHEN fh.hour_num = 24 THEN fh.future_occupancy END) AS occupancy_h24

FROM hourly_enriched b
LEFT JOIN future_hours fh
    ON b.room = fh.room
   AND b.hour_start = fh.base_hour
GROUP BY
    b.room,
    b.hour_start,
    b.occupancy_now,
    b.source_last_update_utc,
    b.source_last_update_local,
    b.room_name,
    b.capacity,
    b.weather_timestamp,
    b.temperature,
    b.precipitation,
    b.snowfall,
    b.windspeed,
    b.condition,
    b.holiday_date,
    b.in_session,
    b.holiday_description,
    b.course_id,
    b.class_name,
    b.day_of_week,
    b.start_time,
    b.end_time,
    b.fce_score,
    b.extra_hours
ORDER BY b.room, b.hour_start;
"""

# ==============================
# 3. RUN QUERY + EXPORT
# ==============================
def main():
    print("Connecting to database...")
    conn = psycopg2.connect(os.getenv("DATABASE_URL"))

    print("Running query...")
    df = pd.read_sql_query(QUERY, conn)

    conn.close()
    print("Query complete!")

    output_file = "all_hourly_output.csv"

    print(f"Writing to {output_file}...")
    df.to_csv("all_hourly_output.csv", index=False)
    print("Done!")

if __name__ == "__main__":
    main()
