"""
Backtest: replays historical opportunity and BRTI data to measure
whether signals predicted market direction correctly.

The only metric that matters: when the system generated an opportunity,
did the market resolve in the predicted direction?

Usage:
    python3 data/backtest/backtest.py --date 2026-04-08
    python3 data/backtest/backtest.py --date 2026-04-08 --bankroll 2500
    python3 data/backtest/backtest.py --date 2026-04-08 2026-04-09   # multi-day
"""

import argparse
import sys
from pathlib import Path

import pandas as pd


# ── Data loading ──────────────────────────────────────────────────────────────

def load_log(log_dir: Path, log_type: str, date: str) -> pd.DataFrame:
    """
    Load a daily log file. Tries JSONL first, then Parquet.
    Returns an empty DataFrame (not an error) if the file doesn't exist —
    callers decide whether absence is fatal.

    Epoch-millisecond columns are forced to int64 so they can be compared
    as plain integers — pandas would otherwise auto-parse them as datetime64.
    """
    jsonl = log_dir / f"{log_type}_{date}.jsonl"
    parquet = log_dir / f"{log_type}_{date}.parquet"

    if jsonl.exists():
        df = pd.read_json(jsonl, lines=True, convert_dates=False)
    elif parquet.exists():
        df = pd.read_parquet(parquet)
    else:
        return pd.DataFrame()

    # Coerce any epoch-ms columns that pandas mis-parsed as datetime64 → int64
    for col in ("timestamp_ms", "closes_at", "exchange_ts", "local_ts"):
        if col in df.columns and pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].astype("int64") // 1_000_000  # ns → ms

    return df


def load_logs(date: str, log_dir: Path) -> dict[str, pd.DataFrame]:
    return {
        "brti": load_log(log_dir, "brti", date),
        "opps": load_log(log_dir, "opportunities", date),
        "fills": load_log(log_dir, "fills", date),
        "realized_pnl": load_log(log_dir, "realized_pnl", date),
    }


# ── Market resolution ─────────────────────────────────────────────────────────

def to_ms(val) -> int:
    """Coerce a closes_at value to integer milliseconds regardless of pandas dtype.
    pandas may parse large epoch-ms integers as Timestamps when reading JSONL."""
    if isinstance(val, pd.Timestamp):
        return int(val.timestamp() * 1000)
    return int(val)


def get_brti_at_close(brti_df: pd.DataFrame, closes_at_ms: int, window_ms: int = 60_000) -> float | None:
    """
    CF Benchmarks BRTI is computed as the mean over the 60-second window ending at close.
    Returns None when there are no BRTI records in that window (e.g. logging started late).
    """
    window = brti_df[
        (brti_df["timestamp_ms"] >= closes_at_ms - window_ms) &
        (brti_df["timestamp_ms"] <= closes_at_ms)
    ]
    if len(window) == 0:
        return None
    return float(window["value"].mean())


def resolve_market(brti_at_close: float, strike: float) -> str:
    """YES if BRTI >= strike at expiry, NO otherwise."""
    return "Yes" if brti_at_close >= strike else "No"


# ── Scoring ───────────────────────────────────────────────────────────────────

def score_opportunities(opps: pd.DataFrame, brti: pd.DataFrame) -> pd.DataFrame:
    """
    For each opportunity, determine whether the prediction was correct.
    Skips rows where BRTI coverage doesn't reach the market close time
    (the market closed after logging ended — not an error, just incomplete data).
    Skips rows where synthetic=True (price was synthesised — no real orderbook,
    so edge scores are meaningless and would corrupt accuracy stats).
    """
    if opps.empty:
        return pd.DataFrame()

    # Filter out synthetic-price opportunities before scoring
    if "synthetic" in opps.columns:
        n_synthetic = int(opps["synthetic"].sum())
        if n_synthetic:
            opps = opps[~opps["synthetic"]].copy()
            print(f"  [note] {n_synthetic} synthetic-price opportunities excluded from scoring "
                  f"(no real orderbook — edge was meaningless)")

    results = []
    skipped_no_brti = 0

    for _, opp in opps.iterrows():
        brti_at_close = get_brti_at_close(brti, to_ms(opp["closes_at"]))
        if brti_at_close is None:
            skipped_no_brti += 1
            continue

        actual_outcome = resolve_market(brti_at_close, opp["strike"])
        predicted_correctly = opp["side"] == actual_outcome

        results.append({
            "ticker":              opp["ticker"],
            "side":                opp["side"],
            "edge":                opp["edge"],
            "kelly_fraction":      opp["kelly_fraction"],
            "market_yes_price":    opp["market_yes_price"],
            "market_no_price":     opp["market_no_price"],
            "strike":              opp["strike"],
            "closes_at":           opp["closes_at"],
            "brti_at_signal":      opp["brti_est"],
            "brti_at_close":       brti_at_close,
            "actual_outcome":      actual_outcome,
            "predicted_correctly": predicted_correctly,
            "signal_confidence":   opp["signal_confidence"],
        })

    if skipped_no_brti:
        print(f"  [note] {skipped_no_brti} opportunities skipped — "
              f"market closed after BRTI logging ended (expected for end-of-session markets)")

    return pd.DataFrame(results)


# ── P&L simulation ────────────────────────────────────────────────────────────

def simulate_pnl(
    scored: pd.DataFrame,
    bankroll_cents: int,
    recompute_kelly: bool = False,
    fee_per_contract_cents: float = 0.0,
    fill_rate: float = 1.0,
    slippage_cents: float = 0.0,
) -> pd.DataFrame:
    """
    Simulate what would have happened if every opportunity had been executed.
    Uses the recorded kelly_fraction and market price to compute contract count
    and P&L. Rows with zero market price are skipped (price not captured at
    signal time — known data quality gap from early logging).

    If recompute_kelly=True, ignores the stored kelly_fraction (which was computed
    with a placeholder odds=1.0) and recomputes quarter-Kelly from the logged price
    and edge: kelly = max(0, (edge * odds - (1-edge)) / odds) * 0.25.

    Payout: 100¢ per contract on a win, 0¢ on a loss.

    fee_per_contract_cents: Kalshi taker fee each way (round-trip = 2x this).
    fill_rate: fraction of desired contracts assumed to fill (0.43 = observed Apr 8 rate).
    slippage_cents: executor places at market_price + this premium per contract.
    """
    if scored.empty:
        return pd.DataFrame()

    pnl_rows = []
    running_bankroll = bankroll_cents
    skipped_zero_price = 0
    total_fee_cents = 0.0

    for _, row in scored.iterrows():
        price = row["market_yes_price"] if row["side"] == "Yes" else row["market_no_price"]
        if price <= 0:
            skipped_zero_price += 1
            continue

        if recompute_kelly:
            odds = (100.0 - price) / price if price < 100.0 else 0.0
            edge = row["edge"]
            kelly_frac = max(0.0, (edge * odds - (1.0 - edge)) / odds) * 0.25 if odds > 0 else 0.0
        else:
            kelly_frac = row["kelly_fraction"]

        bet_cents = kelly_frac * running_bankroll
        desired_contracts = int(bet_cents / price)
        if desired_contracts == 0:
            continue

        contracts_filled = int(desired_contracts * fill_rate)
        if contracts_filled == 0:
            continue

        effective_price = price + slippage_cents
        cost_cents = contracts_filled * effective_price
        fee_cents = contracts_filled * fee_per_contract_cents * 2  # round-trip
        payout_cents = contracts_filled * 100 if row["predicted_correctly"] else 0
        pnl_cents = payout_cents - cost_cents - fee_cents
        running_bankroll += pnl_cents
        total_fee_cents += fee_cents

        pnl_rows.append({
            **row.to_dict(),
            "contracts":              contracts_filled,
            "cost_cents":             cost_cents,
            "fee_cents":              fee_cents,
            "payout_cents":           payout_cents,
            "pnl_cents":              pnl_cents,
            "running_bankroll_cents": running_bankroll,
        })

    if skipped_zero_price:
        print(f"  [note] {skipped_zero_price} opportunities skipped in P&L sim — "
              f"market price was 0 at signal time (price capture bug, now fixed)")

    df = pd.DataFrame(pnl_rows)
    if not df.empty:
        df.attrs["total_fee_cents"] = total_fee_cents
        df.attrs["fee_per_contract_cents"] = fee_per_contract_cents
        df.attrs["fill_rate"] = fill_rate
        df.attrs["slippage_cents"] = slippage_cents
    return df


# ── Reporting ─────────────────────────────────────────────────────────────────

def print_strike_proximity_breakdown(scored: pd.DataFrame):
    """
    Break down accuracy by distance from strike.
    ±$5 band is the critical band — should show 65–75% if edge is real.
    Always shown regardless of --strike-filter.
    """
    if "brti_at_signal" not in scored.columns or "strike" not in scored.columns:
        return

    dist = (scored["brti_at_signal"] - scored["strike"]).abs()
    bands = [
        ("±$5",   dist < 5),
        ("$5–$15", (dist >= 5) & (dist < 15)),
        ("$15–$30", (dist >= 15) & (dist < 30)),
        (">$30",  dist >= 30),
    ]

    print(f"\n── ACCURACY BY DISTANCE FROM STRIKE ─────────────────────")
    print(f"  (critical band: ±$5 should show 65–75% if edge is real)")
    print(f"  {'Bucket':<12} | {'count':>5} | {'accuracy':>8}")
    print(f"  {'-'*12}-+-{'-'*5}-+-{'-'*8}")
    for label, mask in bands:
        subset = scored[mask]
        count = len(subset)
        if count == 0:
            print(f"  {label:<12} | {count:>5} | {'—':>8}")
        else:
            acc = subset["predicted_correctly"].mean()
            key = " ← KEY RESULT" if label == "±$5" else ""
            print(f"  {label:<12} | {count:>5} | {acc:>7.1%}{key}")


def print_report(scored: pd.DataFrame, simulated: pd.DataFrame, date_label: str):
    print(f"\n{'=' * 60}")
    print(f"  BACKTEST REPORT — {date_label}")
    print(f"{'=' * 60}")

    if scored.empty:
        print("  No scoreable opportunities found for this date.")
        print("  (Either no opportunities were logged, or BRTI didn't cover any closes.)")
        return

    acc = scored["predicted_correctly"].mean()
    n = len(scored)

    print(f"\n── SIGNAL ACCURACY ──────────────────────────────────────")
    print(f"  Opportunities scored : {n}")
    print(f"  Correct predictions  : {int(scored['predicted_correctly'].sum())}")
    print(f"  Accuracy             : {acc:.1%}")
    print(f"  vs random (50%)      : {(acc - 0.5):+.1%}")

    verdict = (
        "REAL EDGE — signal is statistically meaningful"    if acc > 0.55 else
        "WEAK EDGE — borderline, need more data"            if acc > 0.50 else
        "COIN FLIP — signal is noise at this sample size"   if acc == 0.50 else
        "INVERTED — signal is systematically wrong"
    )
    print(f"  Verdict              : {verdict}")

    # Accuracy by edge bucket
    print(f"\n── ACCURACY BY EDGE BUCKET ──────────────────────────────")
    print(f"  (higher edge should predict higher accuracy if model is calibrated)")
    scored["edge_bucket"] = pd.cut(
        scored["edge"],
        bins=[0, 0.10, 0.20, 0.30, 0.50, 1.0],
        labels=["0–10%", "10–20%", "20–30%", "30–50%", "50%+"],
    )
    edge_table = (
        scored.groupby("edge_bucket", observed=True)["predicted_correctly"]
        .agg(accuracy="mean", count="count")
        .round(3)
    )
    edge_table["accuracy"] = edge_table["accuracy"].map("{:.1%}".format)
    print(edge_table.to_string())

    # Accuracy by confidence
    print(f"\n── ACCURACY BY SIGNAL CONFIDENCE ────────────────────────")
    scored["conf_bucket"] = pd.cut(
        scored["signal_confidence"],
        bins=[0, 0.45, 0.60, 0.75, 1.0],
        labels=["low (<0.45)", "med (0.45–0.60)", "high (0.60–0.75)", "very high (>0.75)"],
    )
    conf_table = (
        scored.groupby("conf_bucket", observed=True)["predicted_correctly"]
        .agg(accuracy="mean", count="count")
        .round(3)
    )
    conf_table["accuracy"] = conf_table["accuracy"].map("{:.1%}".format)
    print(conf_table.to_string())

    # Accuracy by direction
    print(f"\n── ACCURACY BY SIDE ─────────────────────────────────────")
    side_table = (
        scored.groupby("side")["predicted_correctly"]
        .agg(accuracy="mean", count="count")
        .round(3)
    )
    side_table["accuracy"] = side_table["accuracy"].map("{:.1%}".format)
    print(side_table.to_string())
    print(f"  (large Yes/No asymmetry = direction bias in model_probability)")

    print_strike_proximity_breakdown(scored)

    # P&L simulation
    print(f"\n── SIMULATED P&L ────────────────────────────────────────")
    if simulated.empty:
        print("  No trades simulatable (all opportunities had zero price captured).")
        print("  This is a data quality gap — prices are now recorded correctly.")
    else:
        total_fee = simulated.attrs.get("total_fee_cents", 0.0)
        fee_per = simulated.attrs.get("fee_per_contract_cents", 0.0)
        fill_rate = simulated.attrs.get("fill_rate", 1.0)
        slippage = simulated.attrs.get("slippage_cents", 0.0)

        gross_pnl = simulated["pnl_cents"].sum() + total_fee
        net_pnl = simulated["pnl_cents"].sum()
        win_rate = (simulated["pnl_cents"] > 0).mean()
        final_bankroll = simulated["running_bankroll_cents"].iloc[-1]
        starting = simulated["running_bankroll_cents"].iloc[0] - simulated["pnl_cents"].iloc[0]

        print(f"  Trades simulated     : {len(simulated)}")
        print(f"  Win rate             : {win_rate:.1%}")
        print(f"  Fill rate assumed    : {fill_rate:.0%}")
        print(f"  Fee per contract     : {fee_per:.1f}¢ each way")
        print(f"  Slippage assumed     : {slippage:.1f}¢/contract")
        print(f"  Fee drag (total)     : -{total_fee/100:.2f}")
        print(f"  Gross P&L            : {gross_pnl:+.0f}¢  (${gross_pnl/100:+.2f})")
        print(f"  Net P&L (after fees) : {net_pnl:+.0f}¢  (${net_pnl/100:+.2f})")
        print(f"  Starting bankroll    : ${starting/100:.2f}")
        print(f"  Final bankroll       : ${final_bankroll/100:.2f}")
        roi = (final_bankroll - starting) / starting * 100
        print(f"  ROI                  : {roi:+.1f}%")

    # Key diagnostic questions
    print(f"\n── KEY QUESTIONS ────────────────────────────────────────")
    print(f"  Accuracy > 55%?          {'YES' if acc > 0.55 else 'NO'}")
    edge_corr = scored.groupby("edge_bucket", observed=True)["predicted_correctly"].mean()
    monotone = all(
        edge_corr.iloc[i] <= edge_corr.iloc[i + 1]
        for i in range(len(edge_corr) - 1)
        if edge_corr.iloc[i] > 0 and edge_corr.iloc[i + 1] > 0
    )
    print(f"  Higher edge → higher accuracy?  {'YES — model is calibrated' if monotone else 'NO — edge scores are not predictive'}")
    yes_acc = scored[scored["side"] == "Yes"]["predicted_correctly"].mean() if "Yes" in scored["side"].values else 0
    no_acc = scored[scored["side"] == "No"]["predicted_correctly"].mean() if "No" in scored["side"].values else 0
    print(f"  Yes accuracy: {yes_acc:.1%}  |  No accuracy: {no_acc:.1%}")
    if abs(yes_acc - no_acc) > 0.15:
        print(f"  ⚠  Large Yes/No gap ({abs(yes_acc-no_acc):.1%}) — check model_probability symmetry")

    print()


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Score historical opportunities against actual BRTI outcomes."
    )
    parser.add_argument(
        "dates",
        nargs="*",
        metavar="DATE",
        help="One or more dates in YYYY-MM-DD format (default: today)",
    )
    parser.add_argument(
        "--date",
        dest="date_flag",
        metavar="DATE",
        help="Single date (alternative to positional args)",
    )
    parser.add_argument(
        "--bankroll",
        type=int,
        default=2500,
        help="Starting bankroll in cents for P&L simulation (default: 2500 = $25)",
    )
    parser.add_argument(
        "--log-dir",
        default="data/logs",
        help="Path to the JSONL/Parquet log directory (default: data/logs)",
    )
    parser.add_argument(
        "--strike-filter",
        type=float,
        default=None,
        metavar="DOLLARS",
        help="Only score opportunities where |brti_est - strike| < DOLLARS (e.g. 30)",
    )
    parser.add_argument(
        "--recompute-kelly",
        action="store_true",
        help="Recompute Kelly fraction from logged prices instead of using stored kelly_fraction",
    )
    parser.add_argument(
        "--side",
        choices=["Yes", "No", "both"],
        default="both",
        help="Filter opportunities by side before scoring (default: both)",
    )
    parser.add_argument(
        "--side-filter",
        choices=["Yes", "No"],
        default=None,
        help="Only score opportunities for the given side (Yes or No)",
    )
    parser.add_argument(
        "--fee-per-contract",
        type=float,
        default=0.0,
        metavar="CENTS",
        help="Kalshi taker fee per contract per side in cents (round-trip = 2x). Default: 0",
    )
    parser.add_argument(
        "--fill-rate",
        type=float,
        default=1.0,
        metavar="FRACTION",
        help="Fraction of desired contracts assumed to fill (0.43 = observed Apr 8). Default: 1.0",
    )
    parser.add_argument(
        "--slippage",
        type=float,
        default=0.0,
        metavar="CENTS",
        help="Limit price premium per contract in cents. Default: 0",
    )
    parser.add_argument(
        "--realistic",
        action="store_true",
        help="Shortcut: fee=1.0¢, fill-rate=0.43, slippage=1.0¢ (observed Apr 8 parameters)",
    )
    args = parser.parse_args()

    if args.realistic:
        args.fee_per_contract = 1.0
        args.fill_rate = 0.43
        args.slippage = 1.0

    # Resolve date list
    dates = args.dates or ([args.date_flag] if args.date_flag else [])
    if not dates:
        import datetime
        dates = [datetime.date.today().isoformat()]

    log_dir = Path(args.log_dir)
    if not log_dir.exists():
        print(f"Error: log directory '{log_dir}' not found.", file=sys.stderr)
        print("Run from the repo root, or pass --log-dir path/to/logs", file=sys.stderr)
        sys.exit(1)

    all_scored = []
    all_simulated = []
    running_bankroll = args.bankroll

    for date in dates:
        print(f"\nLoading logs for {date}...")
        logs = load_logs(date, log_dir)

        if logs["opps"].empty:
            print(f"  No opportunities log found for {date} — skipping.")
            continue
        if logs["brti"].empty:
            print(f"  No BRTI log found for {date} — cannot score, skipping.")
            continue

        opps = logs["opps"]
        if args.side != "both":
            before = len(opps)
            opps = opps[opps["side"] == args.side].copy()
            print(f"  Side          : kept {len(opps)}/{before} opportunities (side == {args.side})")

        if args.side_filter is not None:
            before = len(opps)
            opps = opps[opps["side"] == args.side_filter].copy()
            print(f"  Side filter   : kept {len(opps)}/{before} opportunities (side == {args.side_filter})")

        if args.strike_filter is not None:
            before = len(opps)
            opps = opps[
                (opps["brti_est"] - opps["strike"]).abs() < args.strike_filter
            ].copy()
            print(f"  Strike filter: kept {len(opps)}/{before} opportunities "
                  f"(|brti_est - strike| < {args.strike_filter})")

        print(f"  Opportunities: {len(opps)} rows")
        print(f"  BRTI records : {len(logs['brti'])} rows")

        scored = score_opportunities(opps, logs["brti"])
        simulated = simulate_pnl(
            scored,
            running_bankroll,
            recompute_kelly=args.recompute_kelly,
            fee_per_contract_cents=args.fee_per_contract,
            fill_rate=args.fill_rate,
            slippage_cents=args.slippage,
        )

        if not simulated.empty:
            running_bankroll = int(simulated["running_bankroll_cents"].iloc[-1])

        print_report(scored, simulated, date)

        all_scored.append(scored)
        all_simulated.append(simulated)

    # Aggregate report across multiple dates
    if len(dates) > 1:
        combined_scored = pd.concat([s for s in all_scored if not s.empty], ignore_index=True)
        combined_sim = pd.concat([s for s in all_simulated if not s.empty], ignore_index=True)
        print_report(combined_scored, combined_sim, f"COMBINED ({', '.join(dates)})")


if __name__ == "__main__":
    main()
