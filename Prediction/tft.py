"""
retrain_tft_clean.py
────────────────────────────────────────────────────────────────────────────────
Fixes applied vs original:
  1. ✅ Reconstruct in_session from course_id (was 100% null)
  2. ✅ Add hour_of_day + day_of_week as past covariates
  3. ✅ Scale target series with Darts Scaler before training,
         inverse_transform after prediction
  4. ✅ Future padding rows are ONLY added to covariate series, NOT to target
         series (original was fine here, but made explicit and safe)
  5. ✅ Use sorted room order so train ↔ predict room alignment is guaranteed
  6. ✅ Increase hidden_size + n_epochs for better capacity on small dataset
  7. ✅ Save scaler alongside model for consistent inference
"""

import numpy as np
import pandas as pd
from darts import TimeSeries
from darts.models import TFTModel
from darts.dataprocessing.transformers import Scaler
from sklearn.metrics import mean_absolute_error
import joblib

# ── 1. Load & clean ──────────────────────────────────────────────────────────
df = pd.read_csv('training_data.csv')
df['hour_start'] = pd.to_datetime(df['hour_start'])
df = df.drop_duplicates(subset=['room', 'hour_start']).sort_values(['room', 'hour_start'])

# FIX 1: Reconstruct in_session properly
df['in_session']  = df['course_id'].notna().astype(int)
df['fce_score']   = df['fce_score'].fillna(0.0)
df['occupancy_now'] = df['occupancy_now'].fillna(0)

# FIX 2: Add time features
df['hour_of_day'] = df['hour_start'].dt.hour
df['day_of_week'] = df['hour_start'].dt.dayofweek

df[['in_session', 'fce_score', 'capacity', 'hour_of_day', 'day_of_week']] = (
    df[['in_session', 'fce_score', 'capacity', 'hour_of_day', 'day_of_week']]
    .ffill().bfill()
)

# ── 2. Extend covariates only (NOT targets) into future ──────────────────────
#    This ensures the model has schedule info during inference without
#    leaking any fake occupancy values into training targets.
pad_until = pd.Timestamp('2026-04-23 01:00:00')
real_data_end = pd.Timestamp('2026-04-22 07:00:00')

extended_rows = []
for room_id, grp in df.groupby('room'):
    last_row = grp.sort_values('hour_start').iloc[-1]
    future_times = pd.date_range(
        start=last_row['hour_start'] + pd.Timedelta(hours=1),
        end=pad_until, freq='h'
    )
    if len(future_times) == 0:
        continue
    for ft in future_times:
        extended_rows.append({
            'room':          room_id,
            'hour_start':    ft,
            'occupancy_now': np.nan,          # NaN so we can slice it out
            'in_session':    0,               # assume no class in padding
            'fce_score':     last_row['fce_score'],
            'capacity':      last_row['capacity'],
            'hour_of_day':   ft.hour,
            'day_of_week':   ft.dayofweek,
        })

df_ext = pd.concat([df, pd.DataFrame(extended_rows)], ignore_index=True)
df_ext = df_ext.sort_values(['room', 'hour_start'])

# Separate: historical data for targets, full data for covariates
df_hist = df_ext[df_ext['hour_start'] <= real_data_end].copy()
df_covs = df_ext.copy()
df_covs['occupancy_now'] = df_covs['occupancy_now'].fillna(0)  # safe for cov series

# FIX 5: Sort rooms alphabetically for consistent ordering everywhere
rooms_sorted = sorted(df['room'].unique())

# ── 3. Build TimeSeries lists (alphabetically ordered) ───────────────────────
def build_series(data, value_cols, is_target=False):
    """Build per-room TimeSeries in alphabetical room order."""
    series_list = []
    for room in rooms_sorted:
        grp = data[data['room'] == room].sort_values('hour_start')
        if is_target:
            # Only up to real_data_end, drop NaN occupancy
            grp = grp.dropna(subset=['occupancy_now'])
        ts = TimeSeries.from_dataframe(
            grp, time_col='hour_start', value_cols=value_cols,
            fill_missing_dates=True, freq='h'
        )
        series_list.append(ts)
    return series_list

target_series = build_series(df_hist, 'occupancy_now', is_target=True)
cov_series    = build_series(df_covs,
                             ['in_session', 'fce_score', 'hour_of_day', 'day_of_week'])

# FIX 3: Scale targets → neural nets train much better on [0,1] range
target_scaler = Scaler()
target_series_scaled = target_scaler.fit_transform(target_series)

# ── 4. Walk-forward evaluation ────────────────────────────────────────────────
unique_anchors = [
    pd.Timestamp('2026-04-21 08:00:00'),
    pd.Timestamp('2026-04-21 10:00:00'),
    pd.Timestamp('2026-04-21 12:00:00'),
    pd.Timestamp('2026-04-21 14:00:00'),
    pd.Timestamp('2026-04-21 16:00:00'),
]

tft_maes = []
print("Running TFT walk-forward evaluation …")

for anchor in unique_anchors:
    train_end = anchor - pd.Timedelta(hours=1)
    test_end  = anchor + pd.Timedelta(hours=23)

    train_scaled, train_covs, val_scaled = [], [], []
    for ts_sc, cov in zip(target_series_scaled, cov_series):
        if ts_sc.start_time() >= anchor or ts_sc.end_time() < anchor:
            continue
        train_scaled.append(ts_sc.slice(ts_sc.start_time(), train_end))
        train_covs.append(cov)
        actual_end = min(test_end, real_data_end)
        val_scaled.append(ts_sc.slice(anchor, actual_end))

    if not train_scaled:
        print(f"  Skipping {anchor}: no rooms with sufficient data")
        continue

    tft = TFTModel(
        input_chunk_length=48,
        output_chunk_length=24,
        hidden_size=64,           # FIX 6: was 16 — too small
        lstm_layers=2,
        num_attention_heads=4,
        dropout=0.1,
        batch_size=16,
        n_epochs=30,              # FIX 6: was 15 — too few
        random_state=42,
        use_static_covariates=False,
        add_encoders={            # FIX 2: let Darts inject time encodings too
            'cyclic': {'future': ['hour', 'weekday']},
            'position': {'past': ['relative'], 'future': ['relative']},
        }
    )
    tft.fit(train_scaled, future_covariates=train_covs, verbose=False)
    preds_scaled = tft.predict(n=24, series=train_scaled, future_covariates=train_covs)

    # Inverse transform predictions and actuals
    preds_inv = target_scaler.inverse_transform(preds_scaled)
    vals_inv  = target_scaler.inverse_transform(val_scaled)

    clean_preds = np.concatenate([
        np.clip(p.values().flatten(), 0, None).round()[:len(v.values().flatten())]
        for p, v in zip(preds_inv, vals_inv)
    ])
    actuals = np.concatenate([v.values().flatten() for v in vals_inv])

    clean_preds = np.nan_to_num(clean_preds, nan=0.0)
    actuals     = np.nan_to_num(actuals,     nan=0.0)

    mae = mean_absolute_error(actuals, clean_preds)
    tft_maes.append(mae)
    print(f"  Anchor {anchor}: MAE = {mae:.2f}  ({len(actuals)} hours)")

print(f"\nFair TFT MAE: {np.mean(tft_maes):.2f} people")

# ── 5. Train final model on ALL data ─────────────────────────────────────────
print("\nTraining final TFT on full dataset …")

final_tft = TFTModel(
    input_chunk_length=48,
    output_chunk_length=24,
    hidden_size=64,
    lstm_layers=2,
    num_attention_heads=4,
    dropout=0.1,
    batch_size=16,
    n_epochs=30,
    random_state=42,
    use_static_covariates=False,
    add_encoders={
        'cyclic': {'future': ['hour', 'weekday']},
        'position': {'past': ['relative'], 'future': ['relative']},
    }
)
final_tft.fit(target_series_scaled, future_covariates=cov_series, verbose=False)

# FIX 7: Save scaler alongside model so inference can inverse_transform
final_tft.save("occupancy_tft.pt")
joblib.dump(target_scaler, "occupancy_tft_scaler.pkl")
joblib.dump(rooms_sorted,  "tft_room_order.pkl")   # FIX 5: save room order

print("Saved: occupancy_tft.pt, occupancy_tft_scaler.pkl, tft_room_order.pkl")