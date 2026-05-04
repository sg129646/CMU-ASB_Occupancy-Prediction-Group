"""
professor_plot_all_models.py
One script — fetches DB, runs all models, plots past 24h vs ground truth + future 24h.
"""
import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import joblib
import psycopg2
import torch as _torch
from dotenv import load_dotenv

load_dotenv()

HISTORY_SQL_72 = """
WITH now_et AS (SELECT '2026-04-27 21:00:00'::timestamp AS cur),
     hours AS (SELECT generate_series((SELECT cur - interval '72 hours' FROM now_et),
                                      (SELECT cur FROM now_et), interval '1 hour') AS hour_start),
     grid AS (SELECT r.room, h.hour_start FROM rooms r CROSS JOIN hours h),
     hourly AS (
       SELECT g.room, g.hour_start, COALESCE(last_rs.occupancy, 0) AS occupancy_now
       FROM grid g LEFT JOIN LATERAL (
         SELECT rs.occupancy FROM room_state rs WHERE rs.room = g.room
         AND (rs.last_update AT TIME ZONE 'America/New_York') <= g.hour_start + interval '59 minutes 59 seconds'
         ORDER BY rs.last_update DESC LIMIT 1
       ) last_rs ON TRUE
     )
SELECT h.room, h.hour_start, h.occupancy_now, r.capacity,
       COALESCE(c.fce_score, 0.0) AS fce_score,
       CASE WHEN c.course_id IS NOT NULL THEN 1 ELSE 0 END AS in_session
FROM hourly h LEFT JOIN rooms r ON h.room = r.room LEFT JOIN classes c ON h.room = c.room
  AND c.day_of_week = CASE EXTRACT(DOW FROM h.hour_start)
        WHEN 0 THEN 'U' WHEN 1 THEN 'M' WHEN 2 THEN 'T' WHEN 3 THEN 'W'
        WHEN 4 THEN 'R' WHEN 5 THEN 'F' WHEN 6 THEN 'S' END
  AND h.hour_start::time >= c.start_time AND h.hour_start::time < c.end_time
ORDER BY h.room, h.hour_start;
"""

FUTURE_SQL_24 = """
WITH now_et AS (SELECT '2026-04-27 21:00:00'::timestamp AS cur),
     future AS (SELECT generate_series((SELECT cur + interval '1 hour' FROM now_et),
                                       (SELECT cur + interval '24 hours' FROM now_et), interval '1 hour') AS hour_start),
     grid AS (SELECT r.room, f.hour_start FROM rooms r CROSS JOIN future f)
SELECT g.room, g.hour_start, r.capacity, COALESCE(c.fce_score, 0.0) AS fce_score,
       CASE WHEN c.course_id IS NOT NULL THEN 1 ELSE 0 END AS in_session
FROM grid g LEFT JOIN rooms r ON g.room = r.room LEFT JOIN classes c ON g.room = c.room
  AND c.day_of_week = CASE EXTRACT(DOW FROM g.hour_start)
        WHEN 0 THEN 'U' WHEN 1 THEN 'M' WHEN 2 THEN 'T' WHEN 3 THEN 'W'
        WHEN 4 THEN 'R' WHEN 5 THEN 'F' WHEN 6 THEN 'S' END
  AND g.hour_start::time >= c.start_time AND g.hour_start::time < c.end_time
ORDER BY g.room, g.hour_start;
"""

# ── 1. Fetch DB data ──────────────────────────────────────────────────────────
print("Fetching data from DB...")
conn = psycopg2.connect(os.getenv("DATABASE_URL"))
df_hist   = pd.read_sql_query(HISTORY_SQL_72, conn)
df_future = pd.read_sql_query(FUTURE_SQL_24, conn)
conn.close()

for df in [df_hist, df_future]:
    df['hour_start'] = pd.to_datetime(df['hour_start'])
NOW      = pd.Timestamp('2026-04-27 21:00:00')
past_anchor = NOW - pd.Timedelta(hours=24)
df_hist   = df_hist[df_hist['hour_start'] <= NOW].drop_duplicates(subset=['room','hour_start'], keep='last').reset_index(drop=True)
df_future = df_future.drop_duplicates(subset=['room','hour_start'], keep='last').reset_index(drop=True)
rooms    = sorted(df_hist['room'].unique())

# ── 2. Load sklearn artifacts ─────────────────────────────────────────────────
room_codes   = joblib.load("room_codes.pkl")
feature_cols = joblib.load("feature_cols.pkl")
ridge_model  = joblib.load("occupancy_ridge.pkl")
xgb_model    = joblib.load("occupancy_xgb.pkl")
xgb_model.n_jobs = 1

def get_sklearn_features(hist_slice, future_slice, room):
    hist_slice  = hist_slice.sort_values('hour_start').tail(49)
    occ = hist_slice['occupancy_now'].values
    if len(occ) < 49:
        occ = np.pad(occ, (49 - len(occ), 0))
    last = hist_slice.iloc[-1]
    row = {
        "room_code":   room_codes.get(room, -1),
        "hour_of_day": int(last["hour_start"].hour),
        "day_of_week": int(last["hour_start"].dayofweek),
        "is_weekend":  int(last["hour_start"].dayofweek >= 5),
        "in_session":  int(last["in_session"]),
        "fce_score":   float(last["fce_score"]),
        "capacity":    float(last["capacity"]),
    }
    for lag in range(1, 49):
        row[f"occ_lag_{lag}"] = float(occ[-(lag + 1)])
    future_slice = future_slice.sort_values('hour_start').head(24)
    for h in range(1, 25):
        if h <= len(future_slice):
            fr = future_slice.iloc[h - 1]
            row[f"in_session_h{h}"] = int(fr["in_session"])
            row[f"fce_h{h}"]        = float(fr["fce_score"])
            row[f"hour_h{h}"]       = int(fr["hour_start"].hour)
        else:
            row[f"in_session_h{h}"] = 0
            row[f"fce_h{h}"]        = 0.0
            row[f"hour_h{h}"]       = 0
    return pd.DataFrame([row])[feature_cols].values

# ── 3. Load TFT ───────────────────────────────────────────────────────────────
print("Loading TFT...")
from darts import TimeSeries
from darts.models import TFTModel
from darts.dataprocessing.transformers import Scaler

tft_rooms_sorted = joblib.load("tft_room_order.pkl")
tft_scaler       = joblib.load("occupancy_tft_scaler.pkl")

_orig = _torch.load
_torch.load = lambda *a, **kw: _orig(*a, **{**kw, "weights_only": False})
try:
    tft_model = TFTModel.load("occupancy_tft.pt")
finally:
    _torch.load = _orig

def get_tft_preds(hist_df, future_df, hist_end):
    hist_df = hist_df.copy()
    hist_df['hour_of_day'] = hist_df['hour_start'].dt.hour
    hist_df['day_of_week'] = hist_df['hour_start'].dt.dayofweek

    fut = future_df.copy()
    fut['occupancy_now'] = 0
    fut['hour_of_day']   = fut['hour_start'].dt.hour
    fut['day_of_week']   = fut['hour_start'].dt.dayofweek

    df_cov = pd.concat([
        hist_df[["room","hour_start","in_session","fce_score","capacity","hour_of_day","day_of_week"]],
        fut[["room","hour_start","in_session","fce_score","capacity","hour_of_day","day_of_week"]],
    ]).drop_duplicates(subset=["room","hour_start"]).sort_values(["room","hour_start"])

    input_series, cov_series = [], []
    for room in tft_rooms_sorted:
        h = hist_df[hist_df["room"]==room].drop_duplicates("hour_start").sort_values("hour_start")
        c = df_cov[df_cov["room"]==room].sort_values("hour_start")
        ts = TimeSeries.from_dataframe(h, time_col="hour_start", value_cols="occupancy_now",
                                       fill_missing_dates=True, freq="h")
        cv = TimeSeries.from_dataframe(c, time_col="hour_start",
                                       value_cols=["in_session","fce_score","hour_of_day","day_of_week"],
                                       fill_missing_dates=True, freq="h")
        input_series.append(ts.drop_after(hist_end))
        cov_series.append(cv)

    scaled  = tft_scaler.transform(input_series)
    preds_s = tft_model.predict(n=24, series=scaled, future_covariates=cov_series)
    preds   = tft_scaler.inverse_transform(preds_s)

    out = {}
    for room, pred_ts in zip(tft_rooms_sorted, preds):
        vals = np.clip(np.nan_to_num(pred_ts.values().flatten()), 0, None).round().astype(int)
        out[room] = vals
    return out

# ── 4. Load TimeLLM ───────────────────────────────────────────────────────────
print("Loading TimeLLM...")
from neuralforecast import NeuralForecast
nf = NeuralForecast.load("occupancy_timellm/")

def get_timellm_preds(hist_df):
    nf_df = (hist_df[["room","hour_start","occupancy_now"]]
             .rename(columns={"room":"unique_id","hour_start":"ds","occupancy_now":"y"})
             .sort_values(["unique_id","ds"])
             .drop_duplicates(subset=["unique_id","ds"], keep="last")
             .reset_index(drop=True))
    forecasts = nf.predict(df=nf_df)
    out = {}
    for room in rooms:
        r = forecasts[forecasts["unique_id"]==room].sort_values("ds")
        out[room] = np.clip(r["TimeLLM"].values, 0, None).round().astype(int)
    return out

# ── 5. Run all predictions (past backcast + future forecast) ──────────────────
print("Running predictions...")

# For each room: predictions at anchor = past_anchor (backcast) and NOW (forecast)
results = {room: {} for room in rooms}

for room in rooms:
    r_hist = df_hist[df_hist['room']==room]
    r_fut  = df_future[df_future['room']==room]

    # --- Backcast (anchor = past_anchor) ---
    back_hist   = r_hist[r_hist['hour_start'] <= past_anchor]
    back_future = r_hist[(r_hist['hour_start'] > past_anchor) & (r_hist['hour_start'] <= NOW)]

    X_back_ridge = get_sklearn_features(back_hist, back_future, room)
    X_back_xgb   = get_sklearn_features(back_hist, back_future, room)

    results[room]['ridge_past'] = np.clip(ridge_model.predict(X_back_ridge)[0], 0, None).round().astype(int)
    results[room]['xgb_past']   = np.clip(xgb_model.predict(X_back_xgb)[0],   0, None).round().astype(int)

    # --- Forecast (anchor = NOW) ---
    X_fut_ridge = get_sklearn_features(r_hist, r_fut, room)
    X_fut_xgb   = get_sklearn_features(r_hist, r_fut, room)

    results[room]['ridge_fut'] = np.clip(ridge_model.predict(X_fut_ridge)[0], 0, None).round().astype(int)
    results[room]['xgb_fut']   = np.clip(xgb_model.predict(X_fut_xgb)[0],   0, None).round().astype(int)

# TFT — backcast
tft_past = get_tft_preds(df_hist[df_hist['hour_start'] <= past_anchor].copy(),
                          df_hist[(df_hist['hour_start'] > past_anchor) & (df_hist['hour_start'] <= NOW)].copy(),
                          past_anchor)
# TFT — forecast
tft_fut  = get_tft_preds(df_hist.copy(), df_future.copy(), NOW)
for room in rooms:
    results[room]['tft_past'] = tft_past.get(room, np.zeros(24, dtype=int))
    results[room]['tft_fut']  = tft_fut.get(room,  np.zeros(24, dtype=int))

# TimeLLM — backcast (feed only history up to past_anchor)
timellm_past = get_timellm_preds(df_hist[df_hist['hour_start'] <= past_anchor].copy())
# TimeLLM — forecast (feed full history)
timellm_fut  = get_timellm_preds(df_hist.copy())
for room in rooms:
    results[room]['timellm_past'] = timellm_past.get(room, np.zeros(24, dtype=int))
    results[room]['timellm_fut']  = timellm_fut.get(room,  np.zeros(24, dtype=int))

# Ensemble = mean of all 4 models per horizon
for room in rooms:
    results[room]['ensemble_past'] = np.round(
        np.mean([results[room]['ridge_past'], results[room]['xgb_past'],
                 results[room]['tft_past'],   results[room]['timellm_past']], axis=0)
    ).astype(int)
    results[room]['ensemble_fut'] = np.round(
        np.mean([results[room]['ridge_fut'], results[room]['xgb_fut'],
                 results[room]['tft_fut'],   results[room]['timellm_fut']], axis=0)
    ).astype(int)

# ── 6. Plot ───────────────────────────────────────────────────────────────────
past_times = [past_anchor + pd.Timedelta(hours=h+1) for h in range(24)]
fut_times  = [NOW         + pd.Timedelta(hours=h+1) for h in range(24)]

MODEL_STYLES = {
    'RIDGE':   dict(color='tab:blue',   linestyle='--', linewidth=1.5, alpha=0.75, label='Ridge'),
    'XGB':     dict(color='tab:orange', linestyle='--', linewidth=1.5, alpha=0.75, label='XGB'),
    'TFT':     dict(color='tab:green',  linestyle='-.', linewidth=1.5, alpha=0.75, label='TFT'),
    'TimeLLM': dict(color='tab:red',    linestyle=':',  linewidth=1.5, alpha=0.75, label='TimeLLM'),
    'ENSEMBLE':dict(color='black',      linestyle='-',  linewidth=3.0, alpha=1.0,  label='Ensemble'),
}

fig, axes = plt.subplots(2, 2, figsize=(18, 11))
axes = axes.flatten()

for i, room in enumerate(rooms):
    ax  = axes[i]
    r   = results[room]
    res = df_hist[(df_hist['room']==room) &
                  (df_hist['hour_start'] > past_anchor) &
                  (df_hist['hour_start'] <= NOW)]

    # Ground truth (thick solid blue, past window only)
    ax.plot(res['hour_start'], res['occupancy_now'],
            color='tab:blue', linewidth=3, linestyle='-',
            label='Ground Truth', zorder=5)

    # Each model: dashed in past, same style into future (connected)
    for key, style in [('RIDGE','RIDGE'),('XGB','XGB'),('TFT','TFT'),
                       ('TimeLLM','TimeLLM'),('ENSEMBLE','ENSEMBLE')]:
        s = MODEL_STYLES[style].copy()
        label = s.pop('label')
        past_v = r[f'{key.lower()}_past']
        fut_v  = r[f'{key.lower()}_fut']
        all_t  = past_times + fut_times
        all_v  = list(past_v) + list(fut_v)
        lw = s['linewidth']
        # Past segment: thinner/more transparent
        ax.plot(past_times, past_v, **s, zorder=3)
        # Future segment: same style, label only here for legend
        ax.plot(fut_times, fut_v, label=label, zorder=4, **s)
        # Connect the two segments
        ax.plot([past_times[-1], fut_times[0]], [past_v[-1], fut_v[0]], zorder=3, **s)

    # Error bars past only
    gt_interp = np.interp([t.timestamp() for t in past_times],
                          [t.timestamp() for t in res['hour_start']],
                          res['occupancy_now'].values)
    for t, pred_v, act_v in zip(past_times, r['ensemble_past'], gt_interp):
        ax.vlines(t, min(pred_v, act_v), max(pred_v, act_v),
                  color='gray', linewidth=0.8, alpha=0.5)

    # NOW line
    ax.axvline(NOW, color='red', linewidth=2, zorder=6)
    ax.text(NOW, 0.97, '  NOW', transform=ax.get_xaxis_transform(),
            color='red', fontweight='bold', fontsize=12, va='top')

    # Shading
    ax.axvspan(past_times[0], NOW,          facecolor='gray',  alpha=0.08)
    ax.axvspan(NOW,           fut_times[-1], facecolor='green', alpha=0.05)

    ax.set_title(f"Room: {room.upper()}", fontsize=14, fontweight='bold')
    ax.set_ylabel("Occupancy")
    ax.grid(True, linestyle=':', alpha=0.6)
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%a %H:%M'))
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right')

# Single shared legend
handles, labels = axes[0].get_legend_handles_labels()
# Deduplicate (each model appears twice due to past+future plot calls)
seen, h2, l2 = set(), [], []
for h, l in zip(handles, labels):
    if l not in seen:
        seen.add(l); h2.append(h); l2.append(l)
fig.legend(h2, l2, loc='upper right', bbox_to_anchor=(0.99, 0.99),
           fontsize=11, framealpha=0.9)

plt.suptitle("Model Validation: Past 24h Fit vs. Future 24h Forecast (All Models)",
             fontsize=16, fontweight='bold', y=1.01)
plt.tight_layout()
plt.savefig("professor_validation_all_models.png", dpi=300, bbox_inches='tight')
print("Saved to professor_validation_all_models.png")