import numpy as np
import pandas as pd
import joblib
import psycopg2
import os
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from dotenv import load_dotenv
from darts import TimeSeries
from darts.models import TFTModel
from neuralforecast import NeuralForecast

load_dotenv()

ROOMS = ['125b', 'tung', 'a7f', '125d']  # ← update to your actual room IDs

# ============================================================
# 1. FETCH LIVE DATA FROM DB
# ============================================================
HIST_QUERY = """
WITH hour_series AS (
    SELECT gs.hour_start
    FROM generate_series(
        date_trunc('hour', now() AT TIME ZONE 'America/New_York') - interval '47 hours',
        date_trunc('hour', now() AT TIME ZONE 'America/New_York'),
        interval '1 hour'
    ) AS gs(hour_start)
)
SELECT
    hs.hour_start,
    %(room)s                   AS room,
    r.capacity,
    COALESCE(last_rs.occupancy, 0) AS occupancy_now,
    COALESCE(h.in_session, 1)      AS in_session,
    COALESCE(c.fce_score, 0.0)     AS fce_score
FROM hour_series hs
CROSS JOIN rooms r
LEFT JOIN LATERAL (
    SELECT rs.occupancy FROM room_state rs
    WHERE rs.room = %(room)s
      AND (rs.last_update AT TIME ZONE 'America/New_York')
          <= hs.hour_start + interval '59 minutes 59 seconds'
    ORDER BY rs.last_update DESC LIMIT 1
) last_rs ON TRUE
LEFT JOIN holidays h ON hs.hour_start::date = h.date
LEFT JOIN classes c
    ON c.room = %(room)s
   AND c.day_of_week = CASE EXTRACT(DOW FROM hs.hour_start)
        WHEN 0 THEN 'U' WHEN 1 THEN 'M' WHEN 2 THEN 'T'
        WHEN 3 THEN 'W' WHEN 4 THEN 'R' WHEN 5 THEN 'F' WHEN 6 THEN 'S'
       END
   AND hs.hour_start::time >= c.start_time
   AND hs.hour_start::time <  c.end_time
WHERE r.room = %(room)s
ORDER BY hs.hour_start;
"""

FUTURE_QUERY = """
WITH future_hours AS (
    SELECT gs.hour_start FROM generate_series(
        date_trunc('hour', now() AT TIME ZONE 'America/New_York') + interval '1 hour',
        date_trunc('hour', now() AT TIME ZONE 'America/New_York') + interval '24 hours',
        interval '1 hour'
    ) AS gs(hour_start)
)
SELECT
    fh.hour_start,
    %(room)s                   AS room,
    r.capacity,
    COALESCE(h.in_session, 1)  AS in_session,
    COALESCE(c.fce_score, 0.0) AS fce_score
FROM future_hours fh
CROSS JOIN rooms r
LEFT JOIN holidays h ON fh.hour_start::date = h.date
LEFT JOIN classes c
    ON c.room = %(room)s
   AND c.day_of_week = CASE EXTRACT(DOW FROM fh.hour_start)
        WHEN 0 THEN 'U' WHEN 1 THEN 'M' WHEN 2 THEN 'T'
        WHEN 3 THEN 'W' WHEN 4 THEN 'R' WHEN 5 THEN 'F' WHEN 6 THEN 'S'
       END
   AND fh.hour_start::time >= c.start_time
   AND fh.hour_start::time <  c.end_time
WHERE r.room = %(room)s
ORDER BY fh.hour_start;
"""

def fetch_all_rooms():
    conn = psycopg2.connect(os.getenv("DATABASE_URL"))
    hist_frames, future_frames = [], []
    for room in ROOMS:
        h = pd.read_sql_query(HIST_QUERY,   conn, params={"room": room})
        f = pd.read_sql_query(FUTURE_QUERY, conn, params={"room": room})
        hist_frames.append(h)
        future_frames.append(f)
    conn.close()
    hist_all   = pd.concat(hist_frames,   ignore_index=True)
    future_all = pd.concat(future_frames, ignore_index=True)
    return hist_all, future_all

# ============================================================
# 2. PREDICT
# ============================================================
def predict():
    print("Loading models...")
    ridge        = joblib.load('occupancy_ridge.pkl')
    xgb          = joblib.load('occupancy_xgb.pkl')
    tft          = TFTModel.load('occupancy_tft.pt', map_location='cpu')
    nf           = NeuralForecast.load('occupancy_timellm/')

    print("Fetching live data from DB...")
    hist_all, future_all = fetch_all_rooms()
    hist_all['hour_start']   = pd.to_datetime(hist_all['hour_start'])
    future_all['hour_start'] = pd.to_datetime(future_all['hour_start'])

    future_timestamps = sorted(future_all['hour_start'].unique())

    # ── Ridge & XGBoost: build lag features per room ─────────────────────────
    hist_all = hist_all.sort_values(['room', 'hour_start'])
    for lag in range(1, 49):
        hist_all[f'occ_lag_{lag}'] = hist_all.groupby('room')['occupancy_now'].shift(lag)

    room_codes = joblib.load('room_codes.pkl')
    hist_all['room_code'] = hist_all['room'].map(room_codes)

    feature_cols = (
        ['room_code', 'in_session', 'fce_score', 'capacity'] +
        [f'occ_lag_{i}' for i in range(1, 49)] +
        ['occupancy_now']
    )

    # Most recent row per room = the prediction input
    latest_per_room = (
        hist_all.dropna(subset=feature_cols)
                .sort_values('hour_start')
                .groupby('room')
                .tail(1)
    )
    X_pred = latest_per_room[feature_cols]

    ridge_preds_raw = np.clip(ridge.predict(X_pred), 0, None).round().astype(int)
    xgb_preds_raw   = np.clip(xgb.predict(X_pred),   0, None).round().astype(int)

    # ── TFT: per-room series ──────────────────────────────────────────────────
    tft_series_list, tft_cov_list = [], []
    for room in ROOMS:
        h = hist_all[hist_all['room'] == room].set_index('hour_start').sort_index()
        f = future_all[future_all['room'] == room].sort_values('hour_start')

        ts = TimeSeries.from_dataframe(
            h.reset_index(), time_col='hour_start',
            value_cols='occupancy_now', fill_missing_dates=True, freq='h'
        )
        cov_hist = TimeSeries.from_dataframe(
            h.reset_index(), time_col='hour_start',
            value_cols=['in_session', 'fce_score', 'capacity'],
            fill_missing_dates=True, freq='h'
        )
        cov_fut = TimeSeries.from_dataframe(
            f, time_col='hour_start',
            value_cols=['in_session', 'fce_score', 'capacity']
        )
        full_cov = cov_hist.append(cov_fut)
        tft_series_list.append(ts)
        tft_cov_list.append(full_cov)

    tft_raw   = tft.predict(n=24, series=tft_series_list, future_covariates=tft_cov_list)
    tft_preds_raw = [np.clip(p.values().flatten(), 0, None).round().astype(int) for p in tft_raw]

    # ── TimeLLM: all rooms as unique_id ──────────────────────────────────────
    nf_hist = hist_all[['room', 'hour_start', 'occupancy_now']].rename(columns={
        'room': 'unique_id', 'hour_start': 'ds', 'occupancy_now': 'y'
    })
    llm_forecasts  = nf.predict(df=nf_hist)
    llm_preds_raw  = (
        llm_forecasts.reset_index()
                     .sort_values(['unique_id', 'ds'])
    )

    # ── Assemble per-room results ─────────────────────────────────────────────
    all_results = []
    for i, room in enumerate(ROOMS):
        room_future = future_all[future_all['room'] == room].sort_values('hour_start')
        hours       = room_future['hour_start'].values

        ridge_r = ridge_preds_raw[i] if len(ridge_preds_raw) > i else np.zeros(24)
        xgb_r   = xgb_preds_raw[i]   if len(xgb_preds_raw) > i   else np.zeros(24)
        tft_r   = tft_preds_raw[i]
        llm_r   = np.clip(
            llm_preds_raw[llm_preds_raw['unique_id'] == room]['TimeLLM'].values,
            0, None
        ).round().astype(int)

        n = min(len(hours), 24)
        df_room = pd.DataFrame({
            'hour':    hours[:n],
            'room':    room,
            'ridge':   ridge_r[:n],
            'xgboost': xgb_r[:n],
            'tft':     tft_r[:n],
            'timellm': llm_r[:n],
        })
        all_results.append(df_room)

    results = pd.concat(all_results, ignore_index=True)
    results['hour'] = pd.to_datetime(results['hour'])

    print("\n=== 24-Hour Forecast ===")
    print(results.to_string(index=False))
    results.to_csv("forecast_output.csv", index=False)

    # ── Plot ──────────────────────────────────────────────────────────────────
    fig, axes = plt.subplots(
        len(ROOMS), 1,
        figsize=(14, 5 * len(ROOMS)),
        sharex=False
    )
    if len(ROOMS) == 1:
        axes = [axes]

    colors = {
        'ridge':   '#4C72B0',
        'xgboost': '#DD8452',
        'tft':     '#55A868',
        'timellm': '#C44E52',
    }

    for ax, room in zip(axes, ROOMS):
        df_room = results[results['room'] == room]

        for model, color in colors.items():
            ax.plot(
                df_room['hour'], df_room[model],
                label=model.upper(), color=color,
                linewidth=2, marker='o', markersize=4
            )

        ax.set_title(f'Room {room.upper()} — 24-Hour Occupancy Forecast', fontsize=13, fontweight='bold')
        ax.set_ylabel('Predicted Occupancy (people)')
        ax.set_xlabel('Hour')
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%a %H:%M'))
        ax.xaxis.set_major_locator(mdates.HourLocator(interval=2))
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right')
        ax.legend(loc='upper right')
        ax.grid(True, alpha=0.3)
        ax.set_ylim(bottom=0)

    plt.suptitle(
        f'Occupancy Forecast — Generated {pd.Timestamp.now().strftime("%Y-%m-%d %H:%M")}',
        fontsize=15, fontweight='bold', y=1.01
    )
    plt.tight_layout()
    plt.savefig('forecast_plot.png', dpi=150, bbox_inches='tight')
    plt.show()
    print("Plot saved to forecast_plot.png")

    return results

if __name__ == "__main__":
    predict()