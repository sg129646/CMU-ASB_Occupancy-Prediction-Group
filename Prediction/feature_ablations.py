"""
feature_ablation.py
────────────────────────────────────────────────────────────────────────────────
Phase 3 — Feature Ablation
For each model at its best config:
  1. Train FULL model (all features) → baseline MAE
  2. Remove one feature group at a time → measure ΔMAE
  3. Rank feature importance by impact

Feature Groups:
  G1 — Temporal        : hour_of_day, day_of_week, is_weekend, bucket_in_hour
  G2 — Schedule        : in_session, fce_score
  G3 — Capacity        : capacity, extra_hours
  G4 — Weather         : temperature, precipitation, snowfall, windspeed
  G5 — History (lags)  : occ_lag_*
  G6 — Future covs     : in_session_f*, fce_f*, hour_f*

Usage:
  python feature_ablation.py                        # all 4 models
  python feature_ablation.py --models ridge xgb     # sklearn only
  python feature_ablation.py --models tft timellm   # deep only

Output:
  ablation_results.csv   — full ΔMAE table
  ablation_summary.csv   — ranked importance per model
"""

import argparse
import gc
import warnings
import numpy as np
import pandas as pd
import joblib
from sklearn.linear_model import Ridge
from sklearn.multioutput import MultiOutputRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error
from xgboost import XGBRegressor

warnings.filterwarnings("ignore")

# ── Best configs from sweep ───────────────────────────────────────────────────
BEST_CONFIGS = {
    "ridge":   {"resolution_min": 60, "lag_hrs": 72},
    "xgb":     {"resolution_min": 60, "lag_hrs": 96},
    "tft":     {"resolution_min": 30, "lag_hrs": 48},
    "timellm": {"resolution_min": 30, "lag_hrs": 24},
}

DATASETS = {
    60: "training_data.csv",
    45: "dataset_45min.csv",
    30: "dataset_30min.csv",
}

EVAL_FRAC = 0.2

# ── Feature group definitions ─────────────────────────────────────────────────
FEATURE_GROUPS = {
    "G1_temporal":  ["hour_of_day", "day_of_week", "is_weekend", "bucket_in_hour"],
    "G2_schedule":  ["in_session", "fce_score"],
    "G3_capacity":  ["capacity", "extra_hours"],
    "G4_weather":   ["temperature", "precipitation", "snowfall", "windspeed"],
    "G5_history":   "prefix:occ_lag_",
    "G6_future":    "prefix:in_session_f,fce_f,hour_f",
}


def resolve_group(group_def, all_feature_cols):
    """Expand prefix patterns to actual column names."""
    if isinstance(group_def, list):
        return [c for c in group_def if c in all_feature_cols]
    prefixes = [p.strip() for p in group_def.replace("prefix:", "").split(",")]
    return [c for c in all_feature_cols if any(c.startswith(p) for p in prefixes)]


# ═══════════════════════════════════════════════════════════════════════════════
# Data loading
# ═══════════════════════════════════════════════════════════════════════════════

def load_dataset(resolution_min):
    path = DATASETS[resolution_min]
    df   = pd.read_csv(path)
    time_col = "hour_start" if resolution_min == 60 else "time_bucket"
    df[time_col] = pd.to_datetime(df[time_col])
    df = df.rename(columns={time_col: "time_bucket"})
    df = df.sort_values(["room", "time_bucket"]).reset_index(drop=True)
    df = df.drop_duplicates(subset=["room", "time_bucket"], keep="last")

    if resolution_min == 60:
        for i in range(1, 24):
            df[f"occupancy_step_{i}"] = df[f"occupancy_h{i}"]
        df["occupancy_step_24"] = df.groupby("room")["occupancy_now"].shift(-24)

    # ── FIX: in_session construction ──────────────────────────────────────────
    # Priority: use in_session if populated, else class_active, else course_id.
    # The original code blindly filled in_session with 0 when the column existed
    # but was entirely NaN — producing all-zero schedule features.
    if "in_session" in df.columns and df["in_session"].notna().sum() > 0:
        df["in_session"] = df["in_session"].fillna(0).astype(int)
    elif "class_active" in df.columns and df["class_active"].notna().sum() > 0:
        df["in_session"] = df["class_active"].fillna(0).astype(int)
    elif "course_id" in df.columns:
        df["in_session"] = df["course_id"].notna().astype(int)
    else:
        df["in_session"] = 0
        print("  ⚠️  WARNING: No class/session source found — in_session set to 0 everywhere")

    # Sanity check — warn loudly if still all-zero after fix
    n_active = df["in_session"].sum()
    n_total  = len(df)
    if n_active == 0:
        print("  ⚠️  WARNING: in_session is still all 0 after fix — check source columns")
    else:
        print(f"  ✅  in_session: {n_active}/{n_total} rows active "
              f"({n_active/n_total:.1%})")
    # ─────────────────────────────────────────────────────────────────────────

    if "day_of_week_num" in df.columns:
        df["day_of_week"] = df["day_of_week_num"]
    else:
        df["day_of_week"] = df["time_bucket"].dt.dayofweek

    df["hour_of_day"] = df["time_bucket"].dt.hour
    df["is_weekend"]  = (df["day_of_week"] >= 5).astype(int)
    if "bucket_in_hour" not in df.columns:
        df["bucket_in_hour"] = 0

    # ── FIX: fce_score / extra_hours smart fill ───────────────────────────────
    # BUG: naive fillna(0.0) made ~61% of class rows in 60min dataset look
    # identical to non-class rows (fce_score=0) because fce_score was null
    # for those class rows.  The correct approach:
    #   - Non-class rows (in_session==0): fill with 0.0 (no class = no score)
    #   - Class rows with missing score:  fill with per-room median of known scores
    #     so they still signal "a class is happening" to the model
    for col in ["fce_score", "extra_hours"]:
        if col not in df.columns:
            df[col] = 0.0
            continue
        # Per-room median of non-null class-row values
        room_medians = (df.loc[df["in_session"] == 1]
                        .groupby("room")[col].median())
        global_median = df.loc[df["in_session"] == 1, col].median()
        if pd.isna(global_median):
            global_median = 0.0

        def fill_col(grp, col=col, room_medians=room_medians,
                     global_median=global_median):
            grp = grp.copy()
            room = grp["room"].iloc[0]
            med  = room_medians.get(room, global_median)
            if pd.isna(med):
                med = global_median
            # Non-class rows → 0; class rows with null → room median
            mask_class_null = (grp["in_session"] == 1) & grp[col].isna()
            mask_noclass    = grp["in_session"] == 0
            grp.loc[mask_noclass, col]    = grp.loc[mask_noclass, col].fillna(0.0)
            grp.loc[mask_class_null, col] = med
            return grp

        df = df.groupby("room", group_keys=False).apply(fill_col)

    # Sanity check
    for col in ["fce_score", "extra_hours"]:
        remaining = df[col].isna().sum()
        if remaining > 0:
            print(f"  ⚠️  {col}: {remaining} nulls remain after smart fill — "
                  f"falling back to 0.0")
            df[col] = df[col].fillna(0.0)
    # ─────────────────────────────────────────────────────────────────────────
    df["capacity"]    = df["capacity"].fillna(
        df.groupby("room")["capacity"].transform("median"))

    for w in ["temperature", "precipitation", "snowfall", "windspeed"]:
        if w not in df.columns:
            df[w] = 0.0
        else:
            df[w] = df.groupby("room")[w].ffill().bfill().fillna(0.0)

    drop_cols = ["holiday_date", "holiday_in_session", "holiday_description",
                 "source_last_update_utc", "source_last_update_local",
                 "weather_timestamp", "condition", "room_name",
                 "class_name", "day_of_week_num", "course_id",
                 "start_time", "end_time",
                 # Ghost column: unnamed two-space column in training_data.csv;
                 # appears to be a shifted occupancy artifact — correlated with
                 # occupancy_now (r=0.63) but not a valid feature.
                 "  "]
    df = df.drop(columns=[c for c in drop_cols if c in df.columns])
    return df


def build_features(df, resolution_min, lag_hours):
    steps_per_hour = 60 / resolution_min
    lag_steps      = int(lag_hours * steps_per_hour)
    n_targets      = int(60 * 24 / resolution_min)
    target_cols    = [f"occupancy_step_{i}" for i in range(1, n_targets + 1)]

    df = df.copy()
    df["room_code"] = pd.Categorical(df["room"]).codes

    lag_cols = []
    if lag_steps > 0:
        def add_lags(grp):
            grp = grp.copy()
            for lag in range(1, lag_steps + 1):
                grp[f"occ_lag_{lag}"] = grp["occupancy_now"].shift(lag)
            return grp
        df = df.groupby("room", group_keys=False).apply(add_lags)
        lag_cols = [f"occ_lag_{i}" for i in range(1, lag_steps + 1)]

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

    base_cols = ["room_code", "hour_of_day", "bucket_in_hour", "day_of_week",
                 "is_weekend", "in_session", "fce_score", "capacity", "extra_hours",
                 "temperature", "precipitation", "snowfall", "windspeed"]
    feature_cols = base_cols + lag_cols + future_cols

    return df, feature_cols, target_cols, lag_steps, n_targets


# ═══════════════════════════════════════════════════════════════════════════════
# Evaluators
# ═══════════════════════════════════════════════════════════════════════════════

def eval_sklearn(df, feature_cols, target_cols, model_name):
    all_maes = []
    for room, grp in df.groupby("room"):
        grp = grp.dropna(subset=feature_cols + target_cols).reset_index(drop=True)
        if len(grp) < 20:
            continue
        split = int(len(grp) * (1 - EVAL_FRAC))
        X_tr = grp.iloc[:split][feature_cols].values
        y_tr = grp.iloc[:split][target_cols].values
        X_te = grp.iloc[split:][feature_cols].values
        y_te = grp.iloc[split:][target_cols].values
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
    return float(np.mean(all_maes))


def eval_tft(df_raw, resolution_min, lag_steps, n_targets, feature_cols_used=None):
    import torch
    from darts import TimeSeries
    from darts.models import TFTModel
    from darts.dataprocessing.transformers import Scaler

    freq      = f"{resolution_min}min" if resolution_min < 60 else "h"
    timedelta = pd.Timedelta(minutes=resolution_min)
    df        = df_raw.copy()
    df["occupancy_now"] = df["occupancy_now"].fillna(0)
    rooms_sorted = sorted(df["room"].unique())

    data_end  = df["time_bucket"].max()
    pad_until = data_end + pd.Timedelta(hours=24)
    ext = []
    for room, grp in df.groupby("room"):
        last  = grp.sort_values("time_bucket").iloc[-1]
        times = pd.date_range(last["time_bucket"] + timedelta, pad_until, freq=freq)
        for t in times:
            ext.append({"room": room, "time_bucket": t, "occupancy_now": np.nan,
                        "in_session": 0, "fce_score": last["fce_score"],
                        "capacity": last["capacity"], "extra_hours": 0.0,
                        "temperature": last.get("temperature", 0.0),
                        "precipitation": 0.0, "snowfall": 0.0, "windspeed": 0.0,
                        "hour_of_day": t.hour, "day_of_week": t.dayofweek})
    df_ext  = pd.concat([df, pd.DataFrame(ext)], ignore_index=True).sort_values(["room","time_bucket"])
    df_hist = df_ext[df_ext["time_bucket"] <= data_end].copy()
    df_covs = df_ext.copy()
    df_covs["occupancy_now"] = df_covs["occupancy_now"].fillna(0)

    active_covs = ["in_session", "fce_score", "hour_of_day", "day_of_week",
                   "capacity", "extra_hours", "temperature", "precipitation",
                   "snowfall", "windspeed"]
    if feature_cols_used is not None:
        active_covs = [c for c in active_covs if c in feature_cols_used or
                       c in ["in_session","fce_score","hour_of_day","day_of_week"]]
        if "temperature" not in feature_cols_used:
            active_covs = [c for c in active_covs if c not in
                           ["temperature","precipitation","snowfall","windspeed"]]
        if "capacity" not in feature_cols_used:
            active_covs = [c for c in active_covs if c not in ["capacity","extra_hours"]]
        if "in_session" not in feature_cols_used:
            active_covs = [c for c in active_covs if c not in ["in_session","fce_score"]]
        if "hour_of_day" not in feature_cols_used:
            active_covs = [c for c in active_covs if c not in ["hour_of_day","day_of_week"]]

    if not active_covs:
        active_covs = ["hour_of_day"]

    def build_ts(data, vcols, target=False):
        out = []
        for room in rooms_sorted:
            grp = data[data["room"]==room].sort_values("time_bucket")
            if target: grp = grp.dropna(subset=["occupancy_now"])
            out.append(TimeSeries.from_dataframe(
                grp, time_col="time_bucket", value_cols=vcols,
                fill_missing_dates=True, freq=freq))
        return out

    targets    = build_ts(df_hist, "occupancy_now", target=True)
    covs       = build_ts(df_covs, active_covs)
    scaler     = Scaler()
    targets_sc = scaler.fit_transform(targets)

    split_time = df_hist["time_bucket"].quantile(0.8)
    split_time = pd.Timestamp(split_time)
    train_sc   = [ts.drop_after(split_time) for ts in targets_sc]
    val_sc     = [ts.drop_before(split_time) for ts in targets_sc]

    input_len = max(lag_steps, 2)
    tft = TFTModel(
        input_chunk_length=input_len, output_chunk_length=n_targets,
        hidden_size=64, lstm_layers=2, num_attention_heads=4,
        dropout=0.1, batch_size=16, n_epochs=30,
        random_state=42, use_static_covariates=False,
        add_encoders={"cyclic":   {"future": ["hour","weekday"]},
                      "position": {"past": ["relative"],"future": ["relative"]}}
    )
    tft.fit(train_sc, future_covariates=covs, verbose=False)
    preds_sc  = tft.predict(n=n_targets, series=train_sc, future_covariates=covs)
    preds_inv = scaler.inverse_transform(preds_sc)
    vals_inv  = scaler.inverse_transform(val_sc)

    all_p, all_a = [], []
    for p, v in zip(preds_inv, vals_inv):
        pv = np.clip(np.nan_to_num(p.values().flatten()), 0, None)
        av = np.nan_to_num(v.values().flatten())
        mn = min(len(pv), len(av))
        all_p.extend(pv[:mn]); all_a.extend(av[:mn])

    mae = mean_absolute_error(all_a, all_p)
    torch.cuda.empty_cache(); gc.collect()
    return mae


def eval_timellm(df_raw, resolution_min, lag_steps, n_targets):
    import torch
    from neuralforecast import NeuralForecast
    from neuralforecast.models import TimeLLM

    freq  = f"{resolution_min}min" if resolution_min < 60 else "h"
    nf_df = (df_raw[["room","time_bucket","occupancy_now"]]
             .rename(columns={"room":"unique_id","time_bucket":"ds","occupancy_now":"y"})
             .drop_duplicates(subset=["unique_id","ds"], keep="last")
             .sort_values(["unique_id","ds"]).reset_index(drop=True))

    train_rows, test_rows = [], []
    for uid, grp in nf_df.groupby("unique_id"):
        grp   = grp.sort_values("ds").reset_index(drop=True)
        split = int(len(grp) * (1 - EVAL_FRAC))
        train_rows.append(grp.iloc[:split])
        test_rows.append(grp.iloc[split:])
    train_df = pd.concat(train_rows).reset_index(drop=True)
    test_df  = pd.concat(test_rows).reset_index(drop=True)

    input_size = max(lag_steps, 2)
    timellm = TimeLLM(
        h=n_targets, input_size=input_size,
        llm="openai-community/gpt2",
        prompt_prefix="University building occupancy. Classes weekdays 8am-8pm.",
        batch_size=4, valid_batch_size=4,
        windows_batch_size=4, max_steps=100,
    )
    nf = NeuralForecast(models=[timellm], freq=freq)
    nf.fit(df=train_df)
    forecasts = nf.predict(df=train_df)
    merged    = forecasts.merge(test_df.rename(columns={"y":"actual"}),
                                on=["unique_id","ds"], how="inner")
    if merged.empty:
        return float("inf")

    mae = mean_absolute_error(merged["actual"].values,
                              merged["TimeLLM"].clip(lower=0).round().values)
    del timellm, nf, forecasts, merged
    gc.collect(); torch.cuda.empty_cache()
    return mae


# ═══════════════════════════════════════════════════════════════════════════════
# Main ablation loop
# ═══════════════════════════════════════════════════════════════════════════════

def run_ablation_for_model(model_name, run_eval_fn):
    cfg        = BEST_CONFIGS[model_name]
    res        = cfg["resolution_min"]
    lag_hrs    = cfg["lag_hrs"]

    print(f"\n{'═'*65}")
    print(f"  {model_name.upper()}  |  res={res}min  lag={lag_hrs}h")
    print(f"{'═'*65}")

    df_raw = load_dataset(res)
    df_feat, feature_cols, target_cols, lag_steps, n_targets = build_features(
        df_raw, res, lag_hrs)

    groups = {name: resolve_group(defn, feature_cols)
              for name, defn in FEATURE_GROUPS.items()}

    print(f"  Feature groups resolved:")
    for g, cols in groups.items():
        present = [c for c in cols if c in feature_cols]
        print(f"    {g}: {len(present)} cols  {present[:3]}{'...' if len(present)>3 else ''}")

    results = []

    print(f"\n  [FULL] all features ({len(feature_cols)} cols)")
    baseline_mae = run_eval_fn(df_feat, df_raw, feature_cols, target_cols,
                               lag_steps, n_targets, res)
    print(f"    MAE = {baseline_mae:.4f}  (baseline)")
    results.append({"model": model_name, "experiment": "FULL",
                    "removed_group": "none", "n_features": len(feature_cols),
                    "mae": round(baseline_mae, 4), "delta_mae": 0.0})

    for group_name, group_cols in groups.items():
        present = [c for c in group_cols if c in feature_cols]
        if not present:
            print(f"\n  [-{group_name}] SKIP — no columns present in feature set")
            continue

        reduced_cols = [c for c in feature_cols if c not in present]
        print(f"\n  [-{group_name}]  removing {len(present)} cols → {len(reduced_cols)} remain")

        mae = run_eval_fn(df_feat, df_raw, reduced_cols, target_cols,
                          lag_steps, n_targets, res)
        delta    = mae - baseline_mae
        rel_delta = delta / baseline_mae if baseline_mae > 0 else 0
        impact   = "🔴 HIGH" if rel_delta > 0.15 else ("🟠 MED" if rel_delta > 0.05 else "🟢 LOW")
        print(f"    MAE = {mae:.4f}  ΔMAE = {delta:+.4f}  rel={rel_delta:+.1%}  {impact}")

        results.append({"model": model_name, "experiment": f"-{group_name}",
                        "removed_group": group_name, "n_features": len(reduced_cols),
                        "mae": round(mae, 4), "delta_mae": round(delta, 4),
                        "rel_delta_mae": round(rel_delta, 4)})

    return results


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--models", nargs="+",
                        choices=["ridge","xgb","tft","timellm"],
                        default=["ridge","xgb","tft","timellm"])
    args = parser.parse_args()

    all_results = []

    for model_name in args.models:

        if model_name == "timellm":
            print(f"\n{'═'*65}")
            print(f"  TIMELLM — feature ablation NOT applicable")
            print(f"  TimeLLM only uses occupancy history (no exogenous inputs).")
            print(f"  Reporting single baseline MAE only.")
            print(f"{'═'*65}")
            cfg     = BEST_CONFIGS["timellm"]
            df_raw  = load_dataset(cfg["resolution_min"])
            _, _, _, lag_steps, n_targets = build_features(
                df_raw, cfg["resolution_min"], cfg["lag_hrs"])
            baseline = eval_timellm(df_raw, cfg["resolution_min"], lag_steps, n_targets)
            print(f"  Baseline MAE = {baseline:.4f}")
            all_results.append({
                "model": "timellm", "experiment": "FULL",
                "removed_group": "none", "n_features": "N/A",
                "mae": round(baseline, 4), "delta_mae": 0.0, "rel_delta_mae": 0.0,
                "note": "TimeLLM does not use exogenous features; ablation not applicable"
            })
            continue

        if model_name == "ridge":
            def run_eval(df_feat, df_raw, fcols, tcols, lag_steps, n_targets, res):
                return eval_sklearn(df_feat, fcols, tcols, "ridge")

        elif model_name == "xgb":
            def run_eval(df_feat, df_raw, fcols, tcols, lag_steps, n_targets, res):
                return eval_sklearn(df_feat, fcols, tcols, "xgb")

        elif model_name == "tft":
            def run_eval(df_feat, df_raw, fcols, tcols, lag_steps, n_targets, res):
                return eval_tft(df_raw, res, lag_steps, n_targets, fcols)

        results = run_ablation_for_model(model_name, run_eval)
        all_results.extend(results)

    df_out = pd.DataFrame(all_results)
    df_out.to_csv("ablation_results.csv", index=False)

    df_rank = df_out[(df_out["experiment"] != "FULL") & (df_out["model"] != "timellm")]
    summary = (df_rank.groupby("removed_group")["rel_delta_mae"]
               .mean().sort_values(ascending=False)
               .reset_index()
               .rename(columns={"rel_delta_mae": "avg_rel_delta_mae"}))
    summary["importance"] = summary["avg_rel_delta_mae"].apply(
        lambda x: "🔴 HIGH" if x > 0.15 else ("🟠 MEDIUM" if x > 0.05 else "🟢 LOW"))
    summary.to_csv("ablation_summary.csv", index=False)

    print(f"\n{'═'*65}")
    print("  ABLATION COMPLETE")
    print(f"{'═'*65}")
    print("\n  Feature Importance (avg relative ΔMAE — Ridge + XGB + TFT only):")
    print("  Note: TimeLLM excluded — does not use exogenous features")
    print(summary.to_string(index=False))
    print(f"\n  Saved: ablation_results.csv, ablation_summary.csv")


if __name__ == "__main__":
    main()