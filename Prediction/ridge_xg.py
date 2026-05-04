"""
Fixes done:
  1. Reconstruct in_session from course_id (was 100% null in CSV → always 0)
  2. Add hour_of_day + day_of_week (models had no time awareness)
  3. Flatten future covariates into X (in_session_h1..h24, fce_h1..h24)
         so Ridge/XGB know about upcoming classes, not just the current hour
  4. Use MultiOutputRegressor(XGBRegressor) for true independent per-horizon trees
  5. Use pre-computed occupancy_h1..h23 targets already in CSV instead of
         recomputing via shift (avoids accidental NaN boundary issues)
  6. Save feature column list so inference uses identical column order
"""

import pandas as pd
import numpy as np
import joblib
from sklearn.linear_model import Ridge
from sklearn.multioutput import MultiOutputRegressor
from xgboost import XGBRegressor

#  1. Load & clean 
df = pd.read_csv('training_data.csv')
df['hour_start'] = pd.to_datetime(df['hour_start'])
df = df.sort_values(['room', 'hour_start']).reset_index(drop=True)

# FIX 1: Reconstruct in_session from course_id (was 100% null → always 0)
df['in_session'] = df['course_id'].notna().astype(int)

# FIX 2: Add time-of-day features so models know night vs day
df['hour_of_day'] = df['hour_start'].dt.hour
df['day_of_week']  = df['hour_start'].dt.dayofweek   # 0=Mon … 6=Sun
df['is_weekend']   = (df['day_of_week'] >= 5).astype(int)

df['fce_score'] = df['fce_score'].fillna(0.0)
df['capacity']  = df['capacity'].fillna(df.groupby('room')['capacity'].transform('median'))

# ── 2. Build lag features (per room) ─────────────────────────────────────────
def add_lags(grp):
    grp = grp.copy()
    for lag in range(1, 49):
        grp[f'occ_lag_{lag}'] = grp['occupancy_now'].shift(lag)
    return grp

df = df.groupby('room', group_keys=False).apply(add_lags)
df['room_code'] = pd.Categorical(df['room']).codes

# Build future covariate features (h+1 … h+24)
# For each row at time T, we need in_session and fce_score at T+1 … T+24.
# We create these by shifting the covariate columns *backwards* within each room.
def add_future_covs(grp):
    grp = grp.copy()
    for h in range(1, 25):
        grp[f'in_session_h{h}'] = grp['in_session'].shift(-h)
        grp[f'fce_h{h}']        = grp['fce_score'].shift(-h)
        grp[f'hour_h{h}']       = grp['hour_of_day'].shift(-h)
    return grp

df = df.groupby('room', group_keys=False).apply(add_future_covs)

# 4. Define feature and target columns
lag_cols         = [f'occ_lag_{i}'     for i in range(1, 49)]
future_in_cols   = [f'in_session_h{h}' for h in range(1, 25)]
future_fce_cols  = [f'fce_h{h}'        for h in range(1, 25)]
future_hour_cols = [f'hour_h{h}'       for h in range(1, 25)]

# Use pre-computed targets from CSV (h1..h23); derive h24 from shift for consistency
target_cols_csv = [f'occupancy_h{i}' for i in range(1, 24)]   # h1..h23 already in CSV

# h24 is not in CSV → compute via shift
df['occupancy_h24'] = df.groupby('room')['occupancy_now'].shift(-24)
target_cols = target_cols_csv + ['occupancy_h24']              # h1..h24

feature_cols = (
    ['room_code', 'hour_of_day', 'day_of_week', 'is_weekend',
     'in_session', 'fce_score', 'capacity']
    + lag_cols
    + future_in_cols
    + future_fce_cols
    + future_hour_cols
)

# 5. Drop rows with NaNs in features or targets 
df_clean = df.dropna(subset=feature_cols + target_cols).copy()
print(f"Clean rows for training: {len(df_clean)} (from {len(df)} total)")

X = df_clean[feature_cols].values
y = df_clean[target_cols].values
print(f"X shape: {X.shape}  |  y shape: {y.shape}")

# 6. Train Ridge
print("Training Ridge …")
ridge = Ridge(alpha=1.0)
ridge.fit(X, y)
print("  Done.")

# FIX 4: Use MultiOutputRegressor so XGB trains a proper model per horizon
print("Training XGBoost (MultiOutputRegressor — this takes a while) …")
xgb = MultiOutputRegressor(
    XGBRegressor(n_estimators=300, max_depth=5, learning_rate=0.05,
                 subsample=0.8, colsample_bytree=0.8,
                 random_state=42, n_jobs=-1),
    n_jobs=-1
)
xgb.fit(X, y)
print("  Done.")

# 7. Save models + metadata
room_codes = dict(zip(
    df['room'],
    pd.Categorical(df['room']).codes
))

joblib.dump(ridge,        'occupancy_ridge.pkl')
joblib.dump(xgb,          'occupancy_xgb.pkl')
joblib.dump(room_codes,   'room_codes.pkl')
joblib.dump(feature_cols, 'feature_cols.pkl')     # save column order for inference
joblib.dump(target_cols,  'target_cols.pkl')

print(f"\nSaved: occupancy_ridge.pkl, occupancy_xgb.pkl, room_codes.pkl, feature_cols.pkl")
print(f"Feature count: {len(feature_cols)}  |  Target horizons: {len(target_cols)}")