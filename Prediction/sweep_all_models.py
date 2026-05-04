"""
Full sweep: Resolution × Lag → Ridge, XGB, TFT, TimeLLM
Saves best model per model type + overall best config.

Datasets:
  60min → training_data.csv
  45min → dataset_45min.csv
  30min → dataset_30min.csv

Usage:
  python sweep_all_models.py                        # all models
  python sweep_all_models.py --models ridge xgb     # sklearn only (fast)
  python sweep_all_models.py --models tft timellm   # deep only
  python sweep_all_models.py --metric rmse          # rank by RMSE instead

Output:
  sweep_results.csv          — full results table (all models × all combos)
  best_<model>.pkl / .pt     — best model per type
  best_config.json           — overall best config
"""

import argparse
import json
import gc
import warnings
import numpy as np
import pandas as pd
import joblib
from sklearn.linear_model import Ridge
from sklearn.multioutput import MultiOutputRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error
from xgboost import XGBRegressor

warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=UserWarning)

# Config 
DATASETS = {
    60: "training_data.csv",
    45: "dataset_45min.csv",
    30: "dataset_30min.csv",
}
LAG_HOURS  = [0, 12, 24, 48, 72, 96]
EVAL_FRAC  = 0.2   # last 20% per room for eval


# ═══════════════════════════════════════════════════════════════════════════════
# Data loading + feature building (shared by all models)
# ═══════════════════════════════════════════════════════════════════════════════

def load_dataset(resolution_min: int) -> pd.DataFrame:
    path = DATASETS[resolution_min]
    df   = pd.read_csv(path)

    time_col = "hour_start" if resolution_min == 60 else "time_bucket"
    df[time_col] = pd.to_datetime(df[time_col])
    df = df.rename(columns={time_col: "time_bucket"})
    df = df.sort_values(["room", "time_bucket"]).reset_index(drop=True)
    df = df.drop_duplicates(subset=["room", "time_bucket"], keep="last")

    # Targets — normalise to occupancy_step_1..N
    if resolution_min == 60:
        for i in range(1, 24):
            df[f"occupancy_step_{i}"] = df[f"occupancy_h{i}"]
        df["occupancy_step_24"] = df.groupby("room")["occupancy_now"].shift(-24)

    # in_session
    if "class_active" in df.columns:
        df["in_session"] = df["class_active"].fillna(0).astype(int)
    elif "course_id" in df.columns:
        df["in_session"] = df["course_id"].notna().astype(int)
    else:
        df["in_session"] = 0

    # Time features
    if "day_of_week_num" in df.columns:
        df["day_of_week"] = df["day_of_week_num"]
    else:
        df["day_of_week"] = df["time_bucket"].dt.dayofweek
    df["hour_of_day"] = df["time_bucket"].dt.hour
    df["is_weekend"]  = (df["day_of_week"] >= 5).astype(int)
    if "bucket_in_hour" not in df.columns:
        df["bucket_in_hour"] = 0

    df["fce_score"]   = df["fce_score"].fillna(0.0)
    df["extra_hours"] = df["extra_hours"].fillna(0.0) if "extra_hours" in df.columns else 0.0
    df["capacity"]    = df["capacity"].fillna(
        df.groupby("room")["capacity"].transform("median"))

    drop_cols = ["holiday_date", "holiday_in_session", "holiday_description",
                 "source_last_update_utc", "source_last_update_local",
                 "weather_timestamp", "condition", "room_name",
                 "class_name", "day_of_week_num", "course_id"]
    df = df.drop(columns=[c for c in drop_cols if c in df.columns])
    return df


def build_features(df: pd.DataFrame, resolution_min: int,
                   lag_hours: int) -> tuple:
    steps_per_hour = 60 / resolution_min
    lag_steps      = int(lag_hours * steps_per_hour)
    n_targets      = int(60 * 24 / resolution_min)  # 24hr horizon
    target_cols    = [f"occupancy_step_{i}" for i in range(1, n_targets + 1)]

    df = df.copy()
    df["room_code"] = pd.Categorical(df["room"]).codes

    # Lag features
    lag_cols = []
    if lag_steps > 0:
        def add_lags(grp):
            grp = grp.copy()
            for lag in range(1, lag_steps + 1):
                grp[f"occ_lag_{lag}"] = grp["occupancy_now"].shift(lag)
            return grp
        df = df.groupby("room", group_keys=False).apply(add_lags)
        lag_cols = [f"occ_lag_{i}" for i in range(1, lag_steps + 1)]

    # Future covariates
    def add_future_covs(grp):
        grp = grp.copy()
        for h in range(1, n_targets + 1):
            grp[f"in_session_f{h}"] = grp["in_session"].shift(-h)
            grp[f"fce_f{h}"]        = grp["fce_score"].shift(-h)
            grp[f"hour_f{h}"]       = grp["hour_of_day"].shift(-h)
        return grp
    df = df.groupby("room", group_keys=False).apply(add_future_covs)

    future_cols = (
        [f"in_session_f{h}" for h in range(1, n_targets + 1)] +
        [f"fce_f{h}"        for h in range(1, n_targets + 1)] +
        [f"hour_f{h}"       for h in range(1, n_targets + 1)]
    )
    feature_cols = (
        ["room_code", "hour_of_day", "bucket_in_hour",
         "day_of_week", "is_weekend", "in_session", "fce_score", "capacity"]
        + lag_cols + future_cols
    )
    return df, feature_cols, target_cols, lag_steps, n_targets


# ═══════════════════════════════════════════════════════════════════════════════
# Ridge + XGB
# ═══════════════════════════════════════════════════════════════════════════════

def eval_sklearn(df, feature_cols, target_cols, model_name):
    all_maes, all_rmses = [], []
    for room, grp in df.groupby("room"):
        grp = grp.dropna(subset=feature_cols + target_cols).reset_index(drop=True)
        if len(grp) < 20:
            continue
        split   = int(len(grp) * (1 - EVAL_FRAC))
        X_tr, y_tr = grp.iloc[:split][feature_cols].values, grp.iloc[:split][target_cols].values
        X_te, y_te = grp.iloc[split:][feature_cols].values, grp.iloc[split:][target_cols].values
        if model_name == "ridge":
            m = Ridge(alpha=1.0)
        else:
            m = MultiOutputRegressor(
                XGBRegressor(n_estimators=200, max_depth=5, learning_rate=0.05,
                             subsample=0.8, colsample_bytree=0.8,
                             random_state=42, n_jobs=1), n_jobs=1)
        m.fit(X_tr, y_tr)
        preds = np.clip(m.predict(X_te), 0, None)
        all_maes.append(mean_absolute_error(y_te, preds))
        all_rmses.append(np.sqrt(mean_squared_error(y_te, preds)))
    return float(np.mean(all_maes)), float(np.mean(all_rmses))


def train_full_sklearn(df, feature_cols, target_cols, model_name):
    clean = df.dropna(subset=feature_cols + target_cols)
    X, y  = clean[feature_cols].values, clean[target_cols].values
    if model_name == "ridge":
        m = Ridge(alpha=1.0)
    else:
        m = MultiOutputRegressor(
            XGBRegressor(n_estimators=300, max_depth=5, learning_rate=0.05,
                         subsample=0.8, colsample_bytree=0.8,
                         random_state=42, n_jobs=1), n_jobs=1)
    m.fit(X, y)
    return m


# ═══════════════════════════════════════════════════════════════════════════════
# TFT
# ═══════════════════════════════════════════════════════════════════════════════

def eval_tft(df_raw, resolution_min, lag_steps, n_targets):
    from darts import TimeSeries
    from darts.models import TFTModel
    from darts.dataprocessing.transformers import Scaler

    freq = f"{resolution_min}min" if resolution_min < 60 else "h"
    timedelta = pd.Timedelta(minutes=resolution_min)

    df = df_raw.copy()
    df["occupancy_now"] = df["occupancy_now"].fillna(0)
    rooms_sorted = sorted(df["room"].unique())

    # Pad covariates 24hr beyond last data point
    data_end  = df["time_bucket"].max()
    pad_until = data_end + pd.Timedelta(hours=24)
    ext = []
    for room, grp in df.groupby("room"):
        last = grp.sort_values("time_bucket").iloc[-1]
        times = pd.date_range(last["time_bucket"] + timedelta, pad_until, freq=freq)
        for t in times:
            ext.append({"room": room, "time_bucket": t, "occupancy_now": np.nan,
                        "in_session": 0, "fce_score": last["fce_score"],
                        "capacity": last["capacity"],
                        "hour_of_day": t.hour, "day_of_week": t.dayofweek})
    df_ext = pd.concat([df, pd.DataFrame(ext)], ignore_index=True).sort_values(["room","time_bucket"])

    real_end = data_end
    df_hist  = df_ext[df_ext["time_bucket"] <= real_end].copy()
    df_covs  = df_ext.copy()
    df_covs["occupancy_now"] = df_covs["occupancy_now"].fillna(0)

    def build_ts(data, vcols, target=False):
        out = []
        for room in rooms_sorted:
            grp = data[data["room"]==room].sort_values("time_bucket")
            if target:
                grp = grp.dropna(subset=["occupancy_now"])
            ts = TimeSeries.from_dataframe(grp, time_col="time_bucket",
                                           value_cols=vcols,
                                           fill_missing_dates=True, freq=freq)
            out.append(ts)
        return out

    targets  = build_ts(df_hist, "occupancy_now", target=True)
    covs     = build_ts(df_covs, ["in_session","fce_score","hour_of_day","day_of_week"])
    scaler   = Scaler()
    targets_sc = scaler.fit_transform(targets)

    # Walk-forward: split at 80%
    split_time = df_hist["time_bucket"].quantile(0.8)  # rough 80% cut
    if isinstance(split_time, float):
        split_time = pd.Timestamp(split_time)
    split_time = pd.Timestamp(split_time)

    train_sc = [ts.drop_after(split_time) for ts in targets_sc]
    val_sc   = [ts.drop_before(split_time) for ts in targets_sc]

    # Need at least input_chunk_length + output_chunk_length
    input_len = max(lag_steps, 2)
    if input_len < 2: input_len = 2

    tft = TFTModel(
        input_chunk_length=input_len,
        output_chunk_length=n_targets,
        hidden_size=64, lstm_layers=2, num_attention_heads=4,
        dropout=0.1, batch_size=16, n_epochs=30,
        random_state=42, use_static_covariates=False,
        add_encoders={
            "cyclic":   {"future": ["hour", "weekday"]},
            "position": {"past":   ["relative"], "future": ["relative"]},
        }
    )
    tft.fit(train_sc, future_covariates=covs, verbose=False)
    preds_sc = tft.predict(n=n_targets, series=train_sc, future_covariates=covs)

    preds_inv = scaler.inverse_transform(preds_sc)
    vals_inv  = scaler.inverse_transform(val_sc)

    all_preds, all_actuals = [], []
    for p, v in zip(preds_inv, vals_inv):
        pv = np.clip(np.nan_to_num(p.values().flatten()), 0, None)
        av = np.nan_to_num(v.values().flatten())
        mn = min(len(pv), len(av))
        all_preds.extend(pv[:mn]); all_actuals.extend(av[:mn])

    mae  = mean_absolute_error(all_actuals, all_preds)
    rmse = np.sqrt(mean_squared_error(all_actuals, all_preds))

    # Clean up GPU memory
    import torch; torch.cuda.empty_cache(); gc.collect()
    return mae, rmse, tft, scaler, rooms_sorted


def train_full_tft(df_raw, resolution_min, lag_steps, n_targets):
    from darts import TimeSeries
    from darts.models import TFTModel
    from darts.dataprocessing.transformers import Scaler

    freq     = f"{resolution_min}min" if resolution_min < 60 else "h"
    timedelta = pd.Timedelta(minutes=resolution_min)

    df = df_raw.copy()
    df["occupancy_now"] = df["occupancy_now"].fillna(0)
    rooms_sorted = sorted(df["room"].unique())

    data_end  = df["time_bucket"].max()
    pad_until = data_end + pd.Timedelta(hours=24)
    ext = []
    for room, grp in df.groupby("room"):
        last = grp.sort_values("time_bucket").iloc[-1]
        times = pd.date_range(last["time_bucket"] + timedelta, pad_until, freq=freq)
        for t in times:
            ext.append({"room": room, "time_bucket": t, "occupancy_now": np.nan,
                        "in_session": 0, "fce_score": last["fce_score"],
                        "capacity": last["capacity"],
                        "hour_of_day": t.hour, "day_of_week": t.dayofweek})
    df_ext = pd.concat([df, pd.DataFrame(ext)], ignore_index=True).sort_values(["room","time_bucket"])
    df_hist = df_ext[df_ext["time_bucket"] <= data_end].copy()
    df_covs = df_ext.copy()
    df_covs["occupancy_now"] = df_covs["occupancy_now"].fillna(0)

    def build_ts(data, vcols, target=False):
        out = []
        for room in rooms_sorted:
            grp = data[data["room"]==room].sort_values("time_bucket")
            if target: grp = grp.dropna(subset=["occupancy_now"])
            out.append(TimeSeries.from_dataframe(grp, time_col="time_bucket",
                                                 value_cols=vcols,
                                                 fill_missing_dates=True, freq=freq))
        return out

    targets  = build_ts(df_hist, "occupancy_now", target=True)
    covs     = build_ts(df_covs, ["in_session","fce_score","hour_of_day","day_of_week"])
    scaler   = Scaler()
    targets_sc = scaler.fit_transform(targets)

    input_len = max(lag_steps, 2)
    tft = TFTModel(
        input_chunk_length=input_len, output_chunk_length=n_targets,
        hidden_size=64, lstm_layers=2, num_attention_heads=4,
        dropout=0.1, batch_size=16, n_epochs=30,
        random_state=42, use_static_covariates=False,
        add_encoders={
            "cyclic":   {"future": ["hour", "weekday"]},
            "position": {"past":   ["relative"], "future": ["relative"]},
        }
    )
    tft.fit(targets_sc, future_covariates=covs, verbose=False)
    return tft, scaler, rooms_sorted


# ═══════════════════════════════════════════════════════════════════════════════
# TimeLLM
# ═══════════════════════════════════════════════════════════════════════════════

def eval_timellm(df_raw, resolution_min, lag_steps, n_targets):
    import torch
    from neuralforecast import NeuralForecast
    from neuralforecast.models import TimeLLM

    freq = f"{resolution_min}min" if resolution_min < 60 else "h"

    nf_df = (df_raw[["room","time_bucket","occupancy_now"]]
             .rename(columns={"room":"unique_id","time_bucket":"ds","occupancy_now":"y"})
             .drop_duplicates(subset=["unique_id","ds"], keep="last")
             .sort_values(["unique_id","ds"]).reset_index(drop=True))

    # 80/20 split per room
    train_rows, test_rows = [], []
    for uid, grp in nf_df.groupby("unique_id"):
        grp = grp.sort_values("ds").reset_index(drop=True)
        split = int(len(grp) * (1 - EVAL_FRAC))
        train_rows.append(grp.iloc[:split])
        test_rows.append(grp.iloc[split:])
    train_df = pd.concat(train_rows).reset_index(drop=True)
    test_df  = pd.concat(test_rows).reset_index(drop=True)

    input_size = max(lag_steps, 2)
    prompt_prefix = (
        f"Hourly occupancy data for a university building at {resolution_min}-min resolution. "
        "Rooms hold 25-75 people. Classes run weekdays 8am-8pm."
    )

    timellm = TimeLLM(
        h=n_targets, input_size=input_size,
        llm="openai-community/gpt2",
        prompt_prefix=prompt_prefix,
        batch_size=4, valid_batch_size=4,
        windows_batch_size=4, max_steps=100,
    )
    nf = NeuralForecast(models=[timellm], freq=freq)
    nf.fit(df=train_df)
    forecasts = nf.predict(df=train_df)

    merged = forecasts.merge(
        test_df.rename(columns={"y":"actual"}),
        on=["unique_id","ds"], how="inner"
    )
    if merged.empty:
        return float("inf"), float("inf"), None

    preds   = merged["TimeLLM"].clip(lower=0).round().values
    actuals = merged["actual"].values
    mae     = mean_absolute_error(actuals, preds)
    rmse    = np.sqrt(mean_squared_error(actuals, preds))

    del timellm, nf, forecasts, merged
    gc.collect(); torch.cuda.empty_cache()

    return mae, rmse, None


def train_full_timellm(df_raw, resolution_min, lag_steps, n_targets):
    from neuralforecast import NeuralForecast
    from neuralforecast.models import TimeLLM

    freq = f"{resolution_min}min" if resolution_min < 60 else "h"
    nf_df = (df_raw[["room","time_bucket","occupancy_now"]]
             .rename(columns={"room":"unique_id","time_bucket":"ds","occupancy_now":"y"})
             .drop_duplicates(subset=["unique_id","ds"], keep="last")
             .sort_values(["unique_id","ds"]).reset_index(drop=True))

    input_size = max(lag_steps, 2)
    prompt_prefix = (
        f"University building occupancy at {resolution_min}-min resolution. "
        "Classes weekdays 8am-8pm. Rooms hold 25-75 people."
    )
    timellm = TimeLLM(
        h=n_targets, input_size=input_size,
        llm="openai-community/gpt2",
        prompt_prefix=prompt_prefix,
        batch_size=4, valid_batch_size=4,
        windows_batch_size=4, max_steps=100,
    )
    nf = NeuralForecast(models=[timellm], freq=freq)
    nf.fit(df=nf_df)
    return nf


# ═══════════════════════════════════════════════════════════════════════════════
# Main sweep
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--models", nargs="+",
                        choices=["ridge","xgb","tft","timellm"],
                        default=["ridge","xgb","tft","timellm"])
    parser.add_argument("--metric", choices=["mae","rmse"], default="mae")
    args = parser.parse_args()

    results  = []
    # Track best per model
    best = {m: {"score": float("inf"), "config": None, "model_obj": None}
            for m in args.models}

    print("=" * 65)
    print(f"  SWEEP: All Models × Resolution × Lag")
    print(f"  Models     : {args.models}")
    print(f"  Resolutions: {list(DATASETS.keys())} min")
    print(f"  Lag windows: {LAG_HOURS} hrs")
    print(f"  Rank by    : {args.metric.upper()}")
    print("=" * 65)

    for res in sorted(DATASETS.keys(), reverse=True):
        print(f"\n{'─'*65}")
        print(f"  Resolution: {res}min  |  {DATASETS[res]}")
        print(f"{'─'*65}")
        df_raw = load_dataset(res)

        for lag_hrs in LAG_HOURS:
            steps_per_hour = 60 / res
            lag_steps = int(lag_hrs * steps_per_hour)
            n_targets = int(60 * 24 / res)
            print(f"\n  ▶ lag={lag_hrs}h ({lag_steps} steps)  targets={n_targets} steps")

            row = {"resolution_min": res, "lag_hrs": lag_hrs,
                   "lag_steps": lag_steps, "n_targets": n_targets}

            df_feat, feature_cols, target_cols, _, _ = build_features(df_raw, res, lag_hrs)

            # ── Ridge ──
            if "ridge" in args.models:
                mae, rmse = eval_sklearn(df_feat, feature_cols, target_cols, "ridge")
                row["mae_ridge"] = round(mae, 3); row["rmse_ridge"] = round(rmse, 3)
                score = mae if args.metric == "mae" else rmse
                print(f"      Ridge    MAE={mae:.3f}  RMSE={rmse:.3f}", end="")
                if score < best["ridge"]["score"]:
                    best["ridge"]["score"]  = score
                    best["ridge"]["config"] = {"res": res, "lag_hrs": lag_hrs,
                                               "lag_steps": lag_steps,
                                               "feature_cols": feature_cols,
                                               "target_cols": target_cols}
                    print("  ★ best", end="")
                print()

            # ── XGB ──
            if "xgb" in args.models:
                mae, rmse = eval_sklearn(df_feat, feature_cols, target_cols, "xgb")
                row["mae_xgb"] = round(mae, 3); row["rmse_xgb"] = round(rmse, 3)
                score = mae if args.metric == "mae" else rmse
                print(f"      XGB      MAE={mae:.3f}  RMSE={rmse:.3f}", end="")
                if score < best["xgb"]["score"]:
                    best["xgb"]["score"]  = score
                    best["xgb"]["config"] = {"res": res, "lag_hrs": lag_hrs,
                                             "lag_steps": lag_steps,
                                             "feature_cols": feature_cols,
                                             "target_cols": target_cols}
                    print("  ★ best", end="")
                print()

            # ── TFT ──
            if "tft" in args.models:
                try:
                    mae, rmse, tft_obj, scaler, rooms = eval_tft(df_raw, res, lag_steps, n_targets)
                    row["mae_tft"] = round(mae, 3); row["rmse_tft"] = round(rmse, 3)
                    score = mae if args.metric == "mae" else rmse
                    print(f"      TFT      MAE={mae:.3f}  RMSE={rmse:.3f}", end="")
                    if score < best["tft"]["score"]:
                        best["tft"]["score"]     = score
                        best["tft"]["config"]    = {"res": res, "lag_hrs": lag_hrs,
                                                    "lag_steps": lag_steps,
                                                    "n_targets": n_targets}
                        best["tft"]["model_obj"] = (tft_obj, scaler, rooms)
                        print("  ★ best", end="")
                    print()
                except Exception as e:
                    print(f"      TFT      FAILED: {e}")
                    row["mae_tft"] = None; row["rmse_tft"] = None

            # ── TimeLLM ──
            if "timellm" in args.models:
                try:
                    mae, rmse, _ = eval_timellm(df_raw, res, lag_steps, n_targets)
                    row["mae_timellm"] = round(mae, 3); row["rmse_timellm"] = round(rmse, 3)
                    score = mae if args.metric == "mae" else rmse
                    print(f"      TimeLLM  MAE={mae:.3f}  RMSE={rmse:.3f}", end="")
                    if score < best["timellm"]["score"]:
                        best["timellm"]["score"]  = score
                        best["timellm"]["config"] = {"res": res, "lag_hrs": lag_hrs,
                                                     "lag_steps": lag_steps,
                                                     "n_targets": n_targets}
                    print("  ★ best" if score < best["timellm"]["score"] + 0.001 else "", end="")
                    print()
                except Exception as e:
                    print(f"      TimeLLM  FAILED: {e}")
                    row["mae_timellm"] = None; row["rmse_timellm"] = None

            results.append(row)

    # ── Save results ──────────────────────────────────────────────────────────
    df_res = pd.DataFrame(results)
    df_res.to_csv("sweep_results.csv", index=False)

    print(f"\n{'='*65}")
    print("  SWEEP COMPLETE — Results")
    print(f"{'='*65}")
    print(df_res.to_string(index=False))

    # ── Retrain best of each model on full data and save ─────────────────────
    print(f"\n{'─'*65}")
    print("  Retraining best configs on full data …")
    print(f"{'─'*65}")

    best_configs_out = {}

    for model_name in args.models:
        cfg = best["ridge" if model_name == "ridge" else model_name]["config"]
        if cfg is None:
            continue
        print(f"\n  {model_name.upper()}  →  res={cfg['res']}min  lag={cfg['lag_hrs']}h  "
              f"score={best[model_name]['score']:.3f}")

        df_raw = load_dataset(cfg["res"])
        df_feat, feature_cols, target_cols, lag_steps, n_targets = build_features(
            df_raw, cfg["res"], cfg["lag_hrs"])

        if model_name in ("ridge", "xgb"):
            m = train_full_sklearn(df_feat, feature_cols, target_cols, model_name)
            joblib.dump(m,            f"best_{model_name}.pkl")
            joblib.dump(feature_cols, f"best_{model_name}_feature_cols.pkl")
            joblib.dump(target_cols,  f"best_{model_name}_target_cols.pkl")
            room_codes = dict(zip(df_raw["room"], pd.Categorical(df_raw["room"]).codes))
            joblib.dump(room_codes,   f"best_{model_name}_room_codes.pkl")
            print(f"    Saved: best_{model_name}.pkl")

        elif model_name == "tft":
            tft, scaler, rooms = train_full_tft(df_raw, cfg["res"],
                                                cfg["lag_steps"], cfg["n_targets"])
            tft.save(f"best_tft.pt")
            joblib.dump(scaler, "best_tft_scaler.pkl")
            joblib.dump(rooms,  "best_tft_room_order.pkl")
            print(f"    Saved: best_tft.pt, best_tft_scaler.pkl, best_tft_room_order.pkl")

        elif model_name == "timellm":
            nf = train_full_timellm(df_raw, cfg["res"],
                                    cfg["lag_steps"], cfg["n_targets"])
            nf.save("best_timellm/", overwrite=True)
            print(f"    Saved: best_timellm/")

        best_configs_out[model_name] = {
            "resolution_min": cfg["res"],
            "lag_hrs":        cfg["lag_hrs"],
            "lag_steps":      cfg.get("lag_steps", 0),
            "n_targets":      cfg.get("n_targets", 0),
            f"{args.metric}": best[model_name]["score"],
        }

    with open("best_config.json", "w") as f:
        json.dump(best_configs_out, f, indent=2)

    print(f"\nSaved: sweep_results.csv, best_config.json")
    print("\nBest config per model:")
    print(json.dumps(best_configs_out, indent=2))


if __name__ == "__main__":
    main()
