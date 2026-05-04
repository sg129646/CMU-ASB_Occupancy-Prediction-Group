"""
realtime_predict_clean.py
Fixes applied vs original:
  1. Ridge/XGB: add hour_of_day, day_of_week, is_weekend to feature row
  2. Ridge/XGB: flatten future schedule covariates (in_session_h1..h24, etc.)
         from df_future into feature row — models now see upcoming classes
  3. Ridge/XGB: load saved feature_cols list → guaranteed same column order
  4. TFT: drop_after(hist_end) not drop_after(hist_end + 1h) — no fake zero
  5. TFT: use saved room order (tft_room_order.pkl) — no room mismatch
  6. TFT: inverse_transform predictions with saved scaler
  7. TimeLLM: fix dead code + use forecasts['ds'] for hour mapping
  8. All models: reconstruct in_session from DB course_id join (not null col)
"""

import os
import argparse
import numpy as np
import pandas as pd
import psycopg2
from datetime import datetime
from zoneinfo import ZoneInfo
from dotenv import load_dotenv

load_dotenv()
TZ = ZoneInfo("America/New_York")

# ── SQL (unchanged from original — they were correct) ─────────────────────────
HISTORY_SQL = """
WITH
  now_et AS (
    SELECT date_trunc('hour', now() AT TIME ZONE 'America/New_York') AS cur
  ),
  hours AS (
    SELECT generate_series(
      (SELECT cur - interval '48 hours' FROM now_et),
      (SELECT cur                        FROM now_et),
      interval '1 hour'
    ) AS hour_start
  ),
  grid AS (
    SELECT r.room, h.hour_start
    FROM   rooms r CROSS JOIN hours h
  ),
  hourly AS (
    SELECT
      g.room,
      g.hour_start,
      COALESCE(last_rs.occupancy, 0) AS occupancy_now
    FROM grid g
    LEFT JOIN LATERAL (
      SELECT rs.occupancy
      FROM   room_state rs
      WHERE  rs.room = g.room
        AND  (rs.last_update AT TIME ZONE 'America/New_York')
               <= g.hour_start + interval '59 minutes 59 seconds'
      ORDER  BY rs.last_update DESC
      LIMIT  1
    ) last_rs ON TRUE
  )
SELECT
  h.room,
  h.hour_start,
  h.occupancy_now,
  r.capacity,
  COALESCE(c.fce_score, 0.0)                         AS fce_score,
  CASE WHEN c.course_id IS NOT NULL THEN 1 ELSE 0 END AS in_session
FROM hourly h
LEFT JOIN rooms r ON h.room = r.room
LEFT JOIN classes c
  ON  h.room = c.room
  AND c.day_of_week = CASE EXTRACT(DOW FROM h.hour_start)
        WHEN 0 THEN 'U' WHEN 1 THEN 'M' WHEN 2 THEN 'T'
        WHEN 3 THEN 'W' WHEN 4 THEN 'R' WHEN 5 THEN 'F' WHEN 6 THEN 'S'
      END
  AND h.hour_start::time >= c.start_time
  AND h.hour_start::time <  c.end_time
ORDER BY h.room, h.hour_start;
"""

FUTURE_SQL = """
WITH
  now_et AS (
    SELECT date_trunc('hour', now() AT TIME ZONE 'America/New_York') AS cur
  ),
  future AS (
    SELECT generate_series(
      (SELECT cur + interval '1 hour'  FROM now_et),
      (SELECT cur + interval '24 hours' FROM now_et),
      interval '1 hour'
    ) AS hour_start
  ),
  grid AS (SELECT r.room, f.hour_start FROM rooms r CROSS JOIN future f)
SELECT
  g.room,
  g.hour_start,
  r.capacity,
  COALESCE(c.fce_score, 0.0)                         AS fce_score,
  CASE WHEN c.course_id IS NOT NULL THEN 1 ELSE 0 END AS in_session
FROM grid g
LEFT JOIN rooms r ON g.room = r.room
LEFT JOIN classes c
  ON  g.room = c.room
  AND c.day_of_week = CASE EXTRACT(DOW FROM g.hour_start)
        WHEN 0 THEN 'U' WHEN 1 THEN 'M' WHEN 2 THEN 'T'
        WHEN 3 THEN 'W' WHEN 4 THEN 'R' WHEN 5 THEN 'F' WHEN 6 THEN 'S'
      END
  AND g.hour_start::time >= c.start_time
  AND g.hour_start::time <  c.end_time
ORDER BY g.room, g.hour_start;
"""


def fetch_data() -> tuple[pd.DataFrame, pd.DataFrame]:
    print("Connecting to database …")
    conn = psycopg2.connect(os.getenv("DATABASE_URL"))
    df_hist   = pd.read_sql_query(HISTORY_SQL, conn)
    df_future = pd.read_sql_query(FUTURE_SQL,  conn)
    conn.close()
    df_hist['hour_start']   = pd.to_datetime(df_hist['hour_start'])
    df_future['hour_start'] = pd.to_datetime(df_future['hour_start'])
    # ADD THESE TWO LINES TO DROP SQL DUPLICATES:
    df_hist = df_hist.drop_duplicates(subset=['room', 'hour_start'], keep='last').reset_index(drop=True)
    df_future = df_future.drop_duplicates(subset=['room', 'hour_start'], keep='last').reset_index(drop=True)
    
    print(f"  History rows: {len(df_hist)}  |  Future cov rows: {len(df_future)}")
    return df_hist, df_future


# Model 1 & 2 — Ridge / XGBoost

def predict_ridge_xgb(df_hist: pd.DataFrame, df_future: pd.DataFrame,
                      model_name: str = "xgb") -> pd.DataFrame:
    import joblib

    print(f"\n── {model_name.upper()} prediction ──")
    model       = joblib.load(f"occupancy_{model_name}.pkl")
    room_codes  = joblib.load("room_codes.pkl")
    feature_cols = joblib.load("feature_cols.pkl")   # FIX 3: exact column order

    lag_cols = [f"occ_lag_{i}" for i in range(1, 49)]

    feature_rows = []
    for room, grp in df_hist.groupby("room"):
        grp = grp.sort_values("hour_start")
        occ = grp["occupancy_now"].values

        if len(occ) < 49:
            occ = np.pad(occ, (49 - len(occ), 0), constant_values=0)

        last = grp.iloc[-1]

        # FIX 1: time-of-day features
        row = {
            "room":          room,
            "room_code":     room_codes.get(room, -1),
            "hour_of_day":   int(last["hour_start"].hour),
            "day_of_week": int(last["hour_start"].dayofweek),
            "is_weekend":  int(last["hour_start"].dayofweek >= 5),
            "in_session":    int(last["in_session"]),
            "fce_score":     float(last["fce_score"]),
            "capacity":      float(last["capacity"]),
        }
        for lag in range(1, 49):
            row[f"occ_lag_{lag}"] = float(occ[-(lag + 1)])

        # FIX 2: flatten future 24-hour schedule into feature row
        future_room = df_future[df_future["room"] == room].sort_values("hour_start")
        for h in range(1, 25):
            if h <= len(future_room):
                fr = future_room.iloc[h - 1]
                row[f"in_session_h{h}"] = int(fr["in_session"])
                row[f"fce_h{h}"]        = float(fr["fce_score"])
                row[f"hour_h{h}"]       = int(fr["hour_start"].hour)
            else:
                row[f"in_session_h{h}"] = 0
                row[f"fce_h{h}"]        = 0.0
                row[f"hour_h{h}"]       = 0

        feature_rows.append(row)

    df_rows  = pd.DataFrame(feature_rows)
    rooms    = df_rows["room"]
    X_pred   = df_rows[feature_cols]   # FIX 3: enforces exact training column order

    raw_pred = np.clip(model.predict(X_pred), 0, None).round().astype(int)

    now_hour = df_hist["hour_start"].max()
    records  = []
    for i, room in enumerate(rooms):
        for h in range(24):
            records.append({
                "room":       room,
                "hour":       now_hour + pd.Timedelta(hours=h + 1),
                "model":      model_name.upper(),
                "prediction": int(raw_pred[i, h]),
            })

    df_out = pd.DataFrame(records)
    print(f"  Done. Rooms: {rooms.nunique()}  |  Rows: {len(df_out)}")
    return df_out


# Model 3 — TFT

def predict_tft(df_hist: pd.DataFrame, df_future: pd.DataFrame) -> pd.DataFrame:
    import joblib
    import torch as _torch
    from darts import TimeSeries
    from darts.models import TFTModel
    from darts.dataprocessing.transformers import Scaler

    print("\n── TFT prediction ──")

    # FIX 5: load saved room order — prevents room mismatch
    rooms_sorted   = joblib.load("tft_room_order.pkl")
    target_scaler  = joblib.load("occupancy_tft_scaler.pkl")   # FIX 6

    # Add time features to history
    df_hist = df_hist.copy()
    df_hist['hour_of_day'] = df_hist['hour_start'].dt.hour
    df_hist['day_of_week'] = df_hist['hour_start'].dt.dayofweek

    # Combine history + future for covariate series only
    df_future_ext = df_future.copy()
    df_future_ext['occupancy_now'] = 0
    df_future_ext['hour_of_day']   = df_future_ext['hour_start'].dt.hour
    df_future_ext['day_of_week']   = df_future_ext['hour_start'].dt.dayofweek

    df_cov_all = pd.concat([
        df_hist[["room", "hour_start", "in_session", "fce_score",
                 "capacity", "hour_of_day", "day_of_week"]],
        df_future_ext[["room", "hour_start", "in_session", "fce_score",
                       "capacity", "hour_of_day", "day_of_week"]],
    ]).drop_duplicates(subset=["room", "hour_start"]).sort_values(["room", "hour_start"])

    hist_end = df_hist["hour_start"].max()

    # FIX 5: build series in known sorted order
    input_series, cov_series = [], []
    for room in rooms_sorted:
        h_grp = df_hist[df_hist["room"] == room].sort_values("hour_start")
        c_grp = df_cov_all[df_cov_all["room"] == room].sort_values("hour_start")

        ts_target = TimeSeries.from_dataframe(
            h_grp, time_col="hour_start", value_cols="occupancy_now",
            fill_missing_dates=True, freq="h"
        )
        ts_cov = TimeSeries.from_dataframe(
            c_grp, time_col="hour_start",
            value_cols=["in_session", "fce_score", "hour_of_day", "day_of_week"],
            fill_missing_dates=True, freq="h"
        )

        # FIX 4: drop_after(hist_end) — NOT hist_end + 1h (was injecting fake 0)
        input_series.append(ts_target.drop_after(hist_end))
        cov_series.append(ts_cov)

    # Scale input series using the training scaler
    input_scaled = target_scaler.transform(input_series)

    # Load model (PyTorch 2.6+ weights_only patch)
    _orig = _torch.load
    _torch.load = lambda *a, **kw: _orig(*a, **{**kw, "weights_only": False})
    try:
        model = TFTModel.load("occupancy_tft.pt")
    finally:
        _torch.load = _orig

    preds_scaled = model.predict(n=24, series=input_scaled,
                                 future_covariates=cov_series)

    # FIX 6: inverse transform predictions
    preds = target_scaler.inverse_transform(preds_scaled)

    now_hour = hist_end
    records  = []
    for room, pred_ts in zip(rooms_sorted, preds):   # FIX 5: aligned by sorted order
        raw = pred_ts.values()
        if raw.ndim == 3:
            raw = raw[:, :, raw.shape[2] // 2]
        raw  = np.nan_to_num(raw.flatten(), nan=0.0)
        vals = np.clip(raw, 0, None).round().astype(int)
        for h, val in enumerate(vals):
            records.append({
                "room":       room,
                "hour":       now_hour + pd.Timedelta(hours=h + 1),
                "model":      "TFT",
                "prediction": int(val),
            })

    df_out = pd.DataFrame(records)
    print(f"  Done. Rooms: {len(rooms_sorted)}  |  Rows: {len(df_out)}")
    return df_out


# Model 4 — TimeLLM

def predict_timellm(df_hist: pd.DataFrame) -> pd.DataFrame:
    from neuralforecast import NeuralForecast

    print("\n── TimeLLM prediction ──")

    # FIX 7: build proper per-room format (same as training)
    nf_df = (
        df_hist[["room", "hour_start", "occupancy_now"]]
        .rename(columns={"room": "unique_id", "hour_start": "ds", "occupancy_now": "y"})
        .sort_values(["unique_id", "ds"])
        .reset_index(drop=True)
    )

    nf = NeuralForecast.load("occupancy_timellm/")
    forecasts = nf.predict(df=nf_df)

    # FIX 7: fix dead code — use forecasts directly with correct 'ds' as hour
    # (original built a 'records' list then ignored it and used wrong hours)
    df_out = (
        forecasts
        .rename(columns={"unique_id": "room", "ds": "hour", "TimeLLM": "raw"})
        .assign(
            model      = "TimeLLM",
            prediction = lambda x: x["raw"].clip(lower=0).round().astype(int)
        )
        [["room", "hour", "model", "prediction"]]
    )

    print(f"  Done. Rooms: {df_out['room'].nunique()}  |  Rows: {len(df_out)}")
    return df_out


# ═══════════════════════════════════════════════════════════════════════════════
# Display helpers
# ═══════════════════════════════════════════════════════════════════════════════

def pivot_for_display(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["hour_label"] = df["hour"].dt.strftime("%a %H:00")
    return df.pivot_table(
        index="room", columns="hour_label", values="prediction", aggfunc="first"
    )


def print_forecast(df_long: pd.DataFrame, model_name: str):
    print(f"\n{'═'*70}")
    print(f"  {model_name} — 24-hour occupancy forecast")
    print(f"  Generated: {datetime.now(TZ).strftime('%Y-%m-%d %H:%M %Z')}")
    print(f"{'═'*70}")
    wide = pivot_for_display(df_long[df_long["model"] == model_name])
    print(wide.to_string())


# ═══════════════════════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--model",
                        choices=["ridge", "xgb", "tft", "timellm", "all"],
                        default="all")
    parser.add_argument("--output", default="forecast.csv")
    args = parser.parse_args()

    df_hist, df_future = fetch_data()
    results = []

    run_all   = args.model == "all"

    if run_all or args.model == "ridge":
        try:    results.append(predict_ridge_xgb(df_hist, df_future, "ridge"))
        except Exception as e: print(f"  Ridge failed: {e}")

    if run_all or args.model == "xgb":
        try:    results.append(predict_ridge_xgb(df_hist, df_future, "xgb"))
        except Exception as e: print(f"  XGB failed: {e}")

    if run_all or args.model == "tft":
        try:    results.append(predict_tft(df_hist, df_future))
        except Exception as e: print(f"  TFT failed: {e}")

    if run_all or args.model == "timellm":
        try:    results.append(predict_timellm(df_hist))
        except Exception as e: print(f"  TimeLLM failed: {e}")

    if not results:
        print("No models ran successfully.")
        return

    combined = pd.concat(results, ignore_index=True)

    for model_name in combined["model"].unique():
        print_forecast(combined, model_name)

    if combined["model"].nunique() > 1:
        ensemble = (
            combined.groupby(["room", "hour"])["prediction"]
            .mean().round().astype(int)
            .reset_index().assign(model="ENSEMBLE")
        )
        print_forecast(ensemble, "ENSEMBLE")
        combined = pd.concat([combined, ensemble], ignore_index=True)

    combined.to_csv(args.output, index=False)
    print(f"\nForecast saved to {args.output}")


if __name__ == "__main__":
    main()