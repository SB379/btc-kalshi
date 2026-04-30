"""
Fit a logistic regression probability model from historical BRTI + opportunity logs.

Outcome: P(YES resolves) — whether BRTI >= strike at market close.

Features:
  distance          = brti_est - strike  (signed; positive = above strike = favors YES)
  seconds_to_close  = (closes_at - timestamp_ms) / 1000
  realized_vol_30s  = stddev of BRTI values in the 30-second window ending at timestamp_ms

Usage:
    python3 data/backtest/fit_probability_model.py --dates 2026-04-08
    python3 data/backtest/fit_probability_model.py --dates 2026-04-08 2026-04-09 --strike-filter 5
"""

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score
from sklearn.model_selection import cross_val_score


# ── Data loading ───────────────────────────────────────────────────────────────

def load_log(log_dir: Path, log_type: str, date: str) -> pd.DataFrame:
    jsonl = log_dir / f"{log_type}_{date}.jsonl"
    parquet = log_dir / f"{log_type}_{date}.parquet"

    if jsonl.exists():
        df = pd.read_json(jsonl, lines=True, convert_dates=False)
    elif parquet.exists():
        df = pd.read_parquet(parquet)
    else:
        return pd.DataFrame()

    for col in ("timestamp_ms", "closes_at", "exchange_ts", "local_ts"):
        if col in df.columns and pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].astype("int64") // 1_000_000

    return df


def to_ms(val) -> int:
    if isinstance(val, pd.Timestamp):
        return int(val.timestamp() * 1000)
    return int(val)


# ── Feature engineering ────────────────────────────────────────────────────────

def get_brti_at_close(brti_df: pd.DataFrame, closes_at_ms: int, window_ms: int = 60_000) -> float | None:
    window = brti_df[
        (brti_df["timestamp_ms"] >= closes_at_ms - window_ms) &
        (brti_df["timestamp_ms"] <= closes_at_ms)
    ]
    if len(window) == 0:
        return None
    return float(window["value"].mean())


def realized_vol_30s(brti_df: pd.DataFrame, timestamp_ms: int, min_samples: int = 5) -> float | None:
    """Std-dev of BRTI values in the 30-second window ending at timestamp_ms."""
    window = brti_df[
        (brti_df["timestamp_ms"] >= timestamp_ms - 30_000) &
        (brti_df["timestamp_ms"] <= timestamp_ms)
    ]
    if len(window) < min_samples:
        return None
    return float(window["value"].std())


def build_training_set(
    opps: pd.DataFrame,
    brti: pd.DataFrame,
    strike_filter: float | None,
) -> pd.DataFrame:
    """
    Build a DataFrame with features and outcome for each opportunity.
    Rows are skipped when:
      - BRTI didn't cover the market close (incomplete session)
      - < 5 BRTI samples in the 30-second vol window
    """
    rows = []
    skipped_no_close_brti = 0
    skipped_no_vol = 0

    for _, opp in opps.iterrows():
        closes_at = to_ms(opp["closes_at"])
        ts = to_ms(opp["timestamp_ms"])

        # Outcome: did YES resolve?
        brti_close = get_brti_at_close(brti, closes_at)
        if brti_close is None:
            skipped_no_close_brti += 1
            continue

        yes_resolved = 1 if brti_close >= opp["strike"] else 0

        # Features
        distance = float(opp["brti_est"]) - float(opp["strike"])
        seconds_to_close = (closes_at - ts) / 1000.0

        vol = realized_vol_30s(brti, ts)
        if vol is None:
            skipped_no_vol += 1
            continue

        if strike_filter is not None and abs(distance) >= strike_filter:
            continue

        rows.append({
            "distance":         distance,
            "seconds_to_close": seconds_to_close,
            "realized_vol_30s": vol,
            "yes_resolved":     yes_resolved,
            "ticker":           opp["ticker"],
            "strike":           opp["strike"],
            "brti_est":         opp["brti_est"],
            "timestamp_ms":     ts,
        })

    if skipped_no_close_brti:
        print(f"  [note] {skipped_no_close_brti} rows skipped — market closed after BRTI logging ended")
    if skipped_no_vol:
        print(f"  [note] {skipped_no_vol} rows skipped — < 5 BRTI samples in 30s vol window")

    return pd.DataFrame(rows)


# ── Calibration table ──────────────────────────────────────────────────────────

def print_calibration(df: pd.DataFrame, model: LogisticRegression):
    """Print predicted vs. actual probability per decile."""
    X = df[["distance", "seconds_to_close", "realized_vol_30s"]].values
    probs = model.predict_proba(X)[:, 1]
    df = df.copy()
    df["pred_prob"] = probs

    df["decile"] = pd.qcut(df["pred_prob"], q=10, duplicates="drop")
    table = df.groupby("decile", observed=True).agg(
        n=("yes_resolved", "count"),
        mean_pred=("pred_prob", "mean"),
        actual=("yes_resolved", "mean"),
    ).round(3)
    table["diff"] = (table["mean_pred"] - table["actual"]).abs().round(3)

    print("\n── CALIBRATION TABLE (predicted vs. actual per decile) ──────")
    print(f"  {'Bucket':<22} | {'n':>4} | {'predicted':>9} | {'actual':>7} | {'|diff|':>6}")
    print(f"  {'-'*22}-+-{'-'*4}-+-{'-'*9}-+-{'-'*7}-+-{'-'*6}")
    for idx, row in table.iterrows():
        print(f"  {str(idx):<22} | {int(row['n']):>4} | {row['mean_pred']:>9.3f} | {row['actual']:>7.3f} | {row['diff']:>6.3f}")

    max_diff = table["diff"].max()
    if max_diff <= 0.10:
        print(f"  ✓ All deciles within 10pp (max diff: {max_diff:.3f})")
    else:
        print(f"  ⚠ Some deciles exceed 10pp threshold (max diff: {max_diff:.3f})")


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Fit logistic regression probability model from historical logs."
    )
    parser.add_argument(
        "--dates",
        nargs="+",
        metavar="DATE",
        required=True,
        help="One or more dates in YYYY-MM-DD format",
    )
    parser.add_argument(
        "--log-dir",
        default="data/logs",
        help="Path to the JSONL/Parquet log directory (default: data/logs)",
    )
    parser.add_argument(
        "--strike-filter",
        type=float,
        default=5.0,
        metavar="DOLLARS",
        help="Only train on rows where |distance| < DOLLARS (default: 5.0)",
    )
    parser.add_argument(
        "--output",
        default=None,
        metavar="FILE",
        help="Save coefficients JSON to this file (default: data/logs/probability_model_<date>.json)",
    )
    args = parser.parse_args()

    log_dir = Path(args.log_dir)
    if not log_dir.exists():
        print(f"Error: log directory '{log_dir}' not found.", file=sys.stderr)
        sys.exit(1)

    all_opps = []
    all_brti = []

    for date in args.dates:
        print(f"\nLoading {date}...")
        opps = load_log(log_dir, "opportunities", date)
        brti = load_log(log_dir, "brti", date)

        if opps.empty:
            print(f"  No opportunities log for {date} — skipping.")
            continue
        if brti.empty:
            print(f"  No BRTI log for {date} — skipping.")
            continue

        print(f"  Opportunities: {len(opps)} rows | BRTI: {len(brti)} rows")
        all_opps.append(opps)
        all_brti.append(brti)

    if not all_opps:
        print("No data loaded. Exiting.")
        sys.exit(1)

    # Combine across dates; each date's brti is self-contained so we concatenate.
    combined_opps = pd.concat(all_opps, ignore_index=True)
    combined_brti = pd.concat(all_brti, ignore_index=True).sort_values("timestamp_ms")

    print(f"\nBuilding training set (strike_filter=±${args.strike_filter})...")
    df = build_training_set(combined_opps, combined_brti, strike_filter=args.strike_filter)

    if df.empty:
        print("No training rows after filtering. Exiting.")
        sys.exit(1)

    print(f"  Training rows: {len(df)}")
    print(f"  YES resolved : {int(df['yes_resolved'].sum())} ({df['yes_resolved'].mean():.1%})")
    print(f"  Feature stats:")
    for col in ["distance", "seconds_to_close", "realized_vol_30s"]:
        print(f"    {col:<22}: mean={df[col].mean():+.2f}  std={df[col].std():.2f}  "
              f"min={df[col].min():.2f}  max={df[col].max():.2f}")

    X = df[["distance", "seconds_to_close", "realized_vol_30s"]].values
    y = df["yes_resolved"].values

    model = LogisticRegression(max_iter=1000, random_state=42)
    model.fit(X, y)

    auc = roc_auc_score(y, model.predict_proba(X)[:, 1])
    cv_auc = cross_val_score(model, X, y, cv=min(5, len(df)), scoring="roc_auc").mean()

    intercept = float(model.intercept_[0])
    coef_distance, coef_seconds, coef_vol = [float(c) for c in model.coef_[0]]

    print(f"\n── FITTED COEFFICIENTS ──────────────────────────────────")
    print(f"  intercept           : {intercept:+.6f}")
    print(f"  coef_distance       : {coef_distance:+.6f}")
    print(f"  coef_seconds_to_close: {coef_seconds:+.6f}")
    print(f"  coef_realized_vol   : {coef_vol:+.6f}")
    print(f"  AUC (train)         : {auc:.4f}")
    print(f"  AUC (5-fold CV)     : {cv_auc:.4f}")
    print(f"  n_samples           : {len(df)}")

    print_calibration(df, model)

    # Sanity check: P(YES | BRTI $5 above strike, 60s to close, low vol)
    test_cases = [
        ("BRTI +$5 above strike, 60s, vol=5",  [+5.0,  60.0, 5.0]),
        ("BRTI -$5 below strike, 60s, vol=5",  [-5.0,  60.0, 5.0]),
        ("BRTI +$2 above strike, 300s, vol=10", [+2.0, 300.0, 10.0]),
        ("BRTI  $0 at strike,    60s, vol=5",   [ 0.0,  60.0, 5.0]),
    ]
    print(f"\n── SANITY CHECKS ────────────────────────────────────────")
    for label, x in test_cases:
        z = intercept + coef_distance * x[0] + coef_seconds * x[1] + coef_vol * x[2]
        p = 1.0 / (1.0 + np.exp(-z))
        print(f"  P(YES | {label}): {p:.3f}")

    # Save coefficients
    result = {
        "intercept":              intercept,
        "coef_distance":          coef_distance,
        "coef_seconds_to_close":  coef_seconds,
        "coef_realized_vol_30s":  coef_vol,
        "n_samples":              len(df),
        "auc_train":              round(auc, 4),
        "auc_cv":                 round(cv_auc, 4),
        "strike_filter_dollars":  args.strike_filter,
        "dates":                  args.dates,
    }

    if args.output:
        out_path = Path(args.output)
    else:
        date_tag = "_".join(args.dates)
        out_path = log_dir / f"probability_model_{date_tag}.json"

    out_path.write_text(json.dumps(result, indent=2))
    print(f"\n  Coefficients saved to: {out_path}")
    print(f"\n── RUST CONSTANTS (paste into probability.rs) ───────────")
    print(f"  const INTERCEPT: f64       = {intercept:+.6f};")
    print(f"  const COEF_DISTANCE: f64   = {coef_distance:+.6f};")
    print(f"  const COEF_SECONDS: f64    = {coef_seconds:+.6f};")
    print(f"  const COEF_VOL: f64        = {coef_vol:+.6f};")


if __name__ == "__main__":
    main()
