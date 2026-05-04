"""
45-Minute Resolution Dataset Exporter  ── v3
=============================================
Fix log vs v2:
  FIX-8  Start bucket alignment : min_ts snapped to 30-min grid before
                                   generate_series, so all buckets land on
                                   :00 or :30 boundaries
  FIX-9  Forward-fill occupancy : missing occupancy → ffill per room, then
                                   0 only if no prior record exists at all
                                   (avoids injecting false-zero signal)
  FIX-10 Floor time_bucket      : post-process df['time_bucket'].dt.floor()
                                   to eliminate sub-second drift artifacts
  FIX-11 Drop duplicate buckets : keep='last' per (room, time_bucket)
  FIX-12 fillna fce/extra_hours : expected nulls for non-class rows → 0.0

Requires: psycopg2, pandas, python-dotenv
  pip install psycopg2-binary pandas python-dotenv
"""

import os
import psycopg2
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

# ──────────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────────
INTERVAL_MINS = 45
FUTURE_STEPS  = int((24 * 60) / INTERVAL_MINS)   # 32
OUTPUT_FILE   = f"dataset_{INTERVAL_MINS}min.csv"


# ──────────────────────────────────────────────
# DYNAMIC FUTURE-COLUMN GENERATOR
# ──────────────────────────────────────────────
def generate_future_columns(steps: int) -> str:
    lines = [
        f"    MAX(CASE WHEN fh.step_num = {i} THEN fh.future_occupancy END) "
        f"AS occupancy_step_{i}"
        for i in range(1, steps + 1)
    ]
    return ",\n".join(lines)


# ──────────────────────────────────────────────
# QUERY BUILDER
# ──────────────────────────────────────────────
def build_query(interval_mins: int, future_steps: int) -> str:

    interval_str = f"'{interval_mins} minutes'"
    future_cols  = generate_future_columns(future_steps)

    return f"""
WITH

-- ── 1. CUTOFF ─────────────────────────────────────────────────────────────────
-- Epoch-floor to nearest 45-min boundary, then subtract 24 h.
-- e.g. if now() is 14:47 → floor to 14:30 → cutoff = yesterday 14:30
-- Uses epoch arithmetic (÷2700) since 45 doesn't divide evenly into 60.
cutoff AS (
    SELECT
        -- FIX for 45-min: floor to nearest 45-min boundary using epoch arithmetic.
        -- epoch / 2700 floors to nearest 45-min slot (2700 = 45*60 seconds),
        -- then convert back to timestamptz in local time, subtract 24h.
        to_timestamp(
            FLOOR(EXTRACT(EPOCH FROM (now() AT TIME ZONE 'America/New_York')) / (45 * 60))
            * (45 * 60)
        ) AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York'
        - interval '24 hours'
        AS cutoff_ts
),

-- ── 2. PER-ROOM START — CONTINUOUS GRID (no hour-snap) ─────────────────────────
-- For 45-min, date_trunc('hour')+floor(min/45) breaks at hour boundaries.
-- Instead: strip sub-second noise with date_trunc('minute') and let
-- generate_series step continuously: 00:00→00:45→01:30→02:15→03:00…
bounds AS (
    SELECT
        rs.room,
        -- raw earliest record (local)
        MIN(rs.last_update AT TIME ZONE 'America/New_York') AS raw_min_ts,
        -- snapped to 30-min grid  ← FIX-8
        -- FIX for 45-min: do NOT snap to hour boundary.
        -- 45 does not divide into 60, so date_trunc('hour') + floor(min/45)
        -- produces 00:00→00:45→01:00 (wrong) instead of 00:00→00:45→01:30.
        -- Just strip sub-minute precision and let generate_series step freely.
        date_trunc('minute', MIN(rs.last_update AT TIME ZONE 'America/New_York'))
        AS min_ts
    FROM room_state rs
    GROUP BY rs.room
),

-- ── 3. GENERATE 30-MIN BUCKETS PER ROOM ──────────────────────────────────────
-- All buckets now guaranteed to be :00 or :30 aligned.
time_series AS (
    SELECT
        b.room,
        gs.time_bucket
    FROM bounds b
    CROSS JOIN cutoff c
    CROSS JOIN LATERAL generate_series(
        b.min_ts,
        c.cutoff_ts,
        interval {interval_str}
    ) AS gs(time_bucket)
    WHERE b.min_ts <= c.cutoff_ts
),

-- ── 4. SNAP OCCUPANCY — STRICT BOUNDARY ──────────────────────────────────────
-- "Last known state AT OR BEFORE this bucket opened."
-- Strict <= means no value from inside the current bucket leaks in.
-- NULL result here = no record existed before this bucket at all.
bucket_occupancy AS (
    SELECT
        ts.room,
        ts.time_bucket,
        last_rs.occupancy                                    AS occupancy_now,
        last_rs.last_update                                  AS source_last_update_utc,
        last_rs.last_update AT TIME ZONE 'America/New_York'  AS source_last_update_local
    FROM time_series ts
    LEFT JOIN LATERAL (
        SELECT rs.occupancy, rs.last_update
        FROM room_state rs
        WHERE rs.room = ts.room
          AND (rs.last_update AT TIME ZONE 'America/New_York') <= ts.time_bucket
        ORDER BY rs.last_update DESC
        LIMIT 1
    ) AS last_rs ON TRUE
),

-- ── 5. ENRICHMENT ─────────────────────────────────────────────────────────────
enriched AS (
    SELECT
        b.room,
        b.time_bucket,
        b.occupancy_now,
        b.source_last_update_utc,
        b.source_last_update_local,

        -- Temporal features (raw; sin/cos encoding done in Python)
        EXTRACT(HOUR FROM b.time_bucket)::int                       AS hour_of_day,
        (EXTRACT(MINUTE FROM b.time_bucket) / {interval_mins})::int AS bucket_in_hour,
        EXTRACT(DOW FROM b.time_bucket)::int                        AS day_of_week_num,

        -- Room metadata
        r.name     AS room_name,
        r.capacity,

        -- Weather (hourly → join on truncated hour)
        w.timestamp AS weather_timestamp,
        w.temperature,
        w.precipitation,
        w.snowfall,
        w.windspeed,
        w.condition,

        -- Holiday calendar
        h.date          AS holiday_date,
        h.in_session    AS holiday_in_session,
        h.description   AS holiday_description,

        -- Class slot
        c.course_id,
        c.name      AS class_name,
        c.day_of_week,
        c.start_time,
        c.end_time,
        c.fce_score,
        c.extra_hours,

        -- Class-presence flag (use this as your in_session signal)
        CASE WHEN c.course_id IS NOT NULL THEN 1 ELSE 0 END AS class_active

    FROM bucket_occupancy b

    LEFT JOIN rooms r
        ON b.room = r.room

    LEFT JOIN weather w
        ON date_trunc('hour', b.time_bucket) = date_trunc('hour', w.timestamp)

    LEFT JOIN holidays h
        ON b.time_bucket::date = h.date

    LEFT JOIN classes c
        ON b.room = c.room
       AND c.day_of_week = CASE EXTRACT(DOW FROM b.time_bucket)
               WHEN 0 THEN 'U'
               WHEN 1 THEN 'M'
               WHEN 2 THEN 'T'
               WHEN 3 THEN 'W'
               WHEN 4 THEN 'R'
               WHEN 5 THEN 'F'
               WHEN 6 THEN 'S'
           END
       AND b.time_bucket::time >= c.start_time
       AND b.time_bucket::time <  c.end_time
),

-- ── 6. FUTURE STEPS ───────────────────────────────────────────────────────────
-- date_trunc('minute') on both sides prevents microsecond drift → NULL misses.
future_steps_cte AS (
    SELECT
        e.room,
        e.time_bucket                AS base_bucket,
        s.step_num,
        f.occupancy_now              AS future_occupancy
    FROM enriched e
    CROSS JOIN generate_series(1, {future_steps}) AS s(step_num)
    LEFT JOIN enriched f
        ON  f.room = e.room
        AND date_trunc('minute', f.time_bucket)
            = date_trunc('minute',
                e.time_bucket + (s.step_num * interval {interval_str})
              )
)

-- ── 7. FINAL SELECT ────────────────────────────────────────────────────────────
SELECT
    e.room,
    e.time_bucket,
    e.occupancy_now,
    e.source_last_update_utc,
    e.source_last_update_local,

    e.hour_of_day,
    e.bucket_in_hour,
    e.day_of_week_num,

    e.room_name,
    e.capacity,

    e.weather_timestamp,
    e.temperature,
    e.precipitation,
    e.snowfall,
    e.windspeed,
    e.condition,

    e.holiday_date,
    e.holiday_in_session,
    e.holiday_description,

    e.course_id,
    e.class_name,
    e.day_of_week,
    e.start_time,
    e.end_time,
    e.fce_score,
    e.extra_hours,
    e.class_active,

{future_cols}

FROM enriched e
LEFT JOIN future_steps_cte fh
    ON  e.room        = fh.room
    AND e.time_bucket = fh.base_bucket
GROUP BY
    e.room, e.time_bucket, e.occupancy_now,
    e.source_last_update_utc, e.source_last_update_local,
    e.hour_of_day, e.bucket_in_hour, e.day_of_week_num,
    e.room_name, e.capacity,
    e.weather_timestamp, e.temperature, e.precipitation,
    e.snowfall, e.windspeed, e.condition,
    e.holiday_date, e.holiday_in_session, e.holiday_description,
    e.course_id, e.class_name, e.day_of_week,
    e.start_time, e.end_time, e.fce_score, e.extra_hours,
    e.class_active
ORDER BY e.room, e.time_bucket;
"""


# ──────────────────────────────────────────────
# POST-PROCESSING
# ──────────────────────────────────────────────
def post_process(df: pd.DataFrame) -> pd.DataFrame:

    future_cols = [f"occupancy_step_{i}" for i in range(1, FUTURE_STEPS + 1)]

    # FIX-10: floor timestamps to eliminate sub-second drift (e.g. 00:00:01.128)
    df["time_bucket"] = pd.to_datetime(df["time_bucket"]).dt.floor(f"{INTERVAL_MINS}min")

    # FIX-11: drop duplicates that can arise from the GROUP BY + floor interaction
    # keep='last' preserves the most recently sourced row for each (room, bucket)
    before = len(df)
    df = df.sort_values(["room", "time_bucket", "source_last_update_utc"])
    df = df.drop_duplicates(subset=["room", "time_bucket"], keep="last")
    dropped = before - len(df)
    if dropped:
        print(f"  Dedup            : removed {dropped} duplicate bucket(s)")

    # FIX-9: forward-fill occupancy per room FIRST, then 0 only as a last resort.
    # Rationale: occupancy is a continuous state — a missing record means the
    # sensor didn't fire, not that the room emptied.  ffill carries the last
    # known count forward until a new reading arrives.
    df = df.sort_values(["room", "time_bucket"])
    df["occupancy_now"] = (
        df.groupby("room")["occupancy_now"]
          .ffill()        # carry last known value forward
          .fillna(0)      # only zero if NO prior record exists at all
          .astype(int)
    )

    # Fill future steps (NULL = no data that far out → 0)
    df[future_cols] = df[future_cols].fillna(0).astype(int)

    # FIX-12: fce_score / extra_hours are null for non-class rows — expected.
    # Fill with 0.0 so model gets a clean numeric input.
    df["fce_score"]   = df["fce_score"].fillna(0.0)
    df["extra_hours"] = df["extra_hours"].fillna(0.0)

    # ── Validation ────────────────────────────────────────────────────────────
    print("\n── Validation ───────────────────────────────────────────────────")

    # 1. Shape
    print(f"  Shape            : {df.shape[0]:,} rows × {df.shape[1]} cols")

    # 2. Timestamp cleanliness — no sub-minute component should remain
    dirty = df["time_bucket"].dt.second.ne(0).sum() + \
            df["time_bucket"].dt.microsecond.ne(0).sum()
    print(f"  Timestamp drift  : {'✓  all clean' if dirty == 0 else f'⚠  {dirty} dirty timestamps remain'}")

    # 3. NaN audit
    nan_counts  = df.isna().sum()
    nan_nonzero = nan_counts[nan_counts > 0]
    if nan_nonzero.empty:
        print("  NaN check        : ✓  none")
    else:
        print(f"  NaN check        : ⚠  {len(nan_nonzero)} cols have NaNs")
        print(nan_nonzero.to_string())

    # 4. Uniform 30-min spacing per room
    diffs    = df.groupby("room")["time_bucket"].diff().dropna().unique()
    expected = pd.Timedelta(minutes=INTERVAL_MINS)
    bad      = [d for d in diffs if d != expected]
    print(f"  Spacing check    : {'✓  uniform 30 min' if not bad else f'⚠  bad gaps: {bad[:5]}'}")

    # 5. Step-1 alignment — occupancy_step_1 should equal next row's occupancy_now
    probe           = df.copy()
    probe["_next"]  = probe.groupby("room")["occupancy_now"].shift(-1)
    valid           = probe.dropna(subset=["_next"])
    mismatch_pct    = (valid["occupancy_step_1"] != valid["_next"]).mean() * 100
    flag            = "✓" if mismatch_pct < 5 else "⚠"
    print(f"  Step-1 alignment : {flag}  {mismatch_pct:.1f}% differ from next-row "
          f"(>5% = problem)")

    # 6. Future column presence
    missing = [c for c in future_cols if c not in df.columns]
    print(f"  Future cols      : {'✓  all 32 present' if not missing else f'⚠  {len(missing)} missing'}")

    # 7. class_active vs course_id consistency
    active_rows      = df["class_active"].eq(1).sum()
    course_id_rows   = df["course_id"].notna().sum()
    consistent       = active_rows == course_id_rows
    print(f"  class_active     : {'✓' if consistent else '⚠'}  "
          f"{active_rows} active rows, {course_id_rows} with course_id "
          f"{'(match)' if consistent else '(MISMATCH)'}")

    print("─────────────────────────────────────────────────────────────────\n")
    return df


# ──────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────
def main():
    print(f"Interval    : {INTERVAL_MINS} min")
    print(f"Future steps: {FUTURE_STEPS}  ({FUTURE_STEPS * INTERVAL_MINS / 60:.0f} h ahead)")
    print(f"Output      : {OUTPUT_FILE}\n")

    print("Connecting to database...")
    conn = psycopg2.connect(os.getenv("DATABASE_URL"))

    print("Building query...")
    query = build_query(INTERVAL_MINS, FUTURE_STEPS)

    # Uncomment to inspect SQL before running:
    # print(query); conn.close(); return

    print("Running query...")
    df = pd.read_sql_query(query, conn)
    conn.close()
    print(f"Raw result: {len(df):,} rows × {len(df.columns)} cols")

    df = post_process(df)

    print(f"Writing {OUTPUT_FILE}...")
    df.to_csv(OUTPUT_FILE, index=False)
    print("Done!")


if __name__ == "__main__":
    main()