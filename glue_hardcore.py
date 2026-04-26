"""
GLUE-Style Parameter Calibration for Hardcore NEPSE Strategy
=============================================================
Concept borrowed from DSSAT GLUEP:
  1. Define MINIMA / MAXIMA for each parameter
  2. Draw N random parameter sets (Latin Hypercube-style uniform sampling)
  3. Run backtest for each set
  4. Score each set — compute likelihood (P&L × profit factor)
  5. Keep only "behavioral" sets (top 20% by likelihood)
  6. Report posterior: mean, stdev, best-fit value per parameter

Parameters sampled:
  Entry side:
    adx_min          : minimum ADX to enter          [15 – 35]
    rsi_lo           : RSI lower bound for entry      [45 – 58]
    rsi_hi           : RSI upper bound for entry      [62 – 78]
    vol_mult         : volume ≥ N × 20d avg           [1.0 – 2.0]
    drop_3d_limit    : max allowed 3-day drop %       [-3 – -10]
    hh_lookback      : higher-high lookback days      [5 – 20]

  Exit side:
    weakness_min_hold: min days held before WEAKNESS  [3 – 12]
    weakness_rsi     : RSI threshold for WEAKNESS     [40 – 50]
    trail_stop_pct   : trailing stop % from peak      [10 – 20]
    hard_stop_pct    : hard stop % from entry         [7 – 13]

Usage:
    python glue_hardcore.py              # 500 runs (fast)
    python glue_hardcore.py --runs 2000  # more robust posteriors
    python glue_hardcore.py --runs 2000 --cores 4
"""

import os
import sys
import argparse
import numpy as np
import pandas as pd
from concurrent.futures import ProcessPoolExecutor, as_completed

# ── import backtest helpers ──────────────────────────────────────────────────
from backtest import load_data, calc_qty, RISK_PCT_DEFAULT, MIN_HISTORY, NEPSE_MIN_LOT

# ── Parameter space (MINIMA / MAXIMA) ───────────────────────────────────────
PARAM_SPACE = {
    # name           : (min,   max,   type)
    "adx_min"        : (15,    35,    "int"),
    "rsi_lo"         : (45,    58,    "int"),
    "rsi_hi"         : (62,    78,    "int"),
    "vol_mult"       : (1.0,   2.0,   "float"),
    "drop_3d_limit"  : (-10.0, -3.0,  "float"),
    "hh_lookback"    : (5,     20,    "int"),
    "weakness_hold"  : (3,     12,    "int"),
    "weakness_rsi"   : (40,    50,    "int"),
    "trail_stop_pct" : (10.0,  20.0,  "float"),
    "hard_stop_pct"  : (7.0,   13.0,  "float"),
}

# ── Baseline (v1 defaults) for behavioral threshold ─────────────────────────
BASELINE_PNL = 65_000    # NPR — must beat this
BASELINE_PF  = 1.10      # profit factor — must beat this
BEHAVIORAL_TOP_PCT = 0.20  # keep top 20% by likelihood


# ─────────────────────────────────────────────────────────────────────────────
def sample_parameters(n_runs, seed=42):
    """Latin Hypercube-style uniform sampling across all parameter dimensions."""
    rng = np.random.default_rng(seed)
    sets = []
    for _ in range(n_runs):
        ps = {}
        for name, (lo, hi, ptype) in PARAM_SPACE.items():
            val = rng.uniform(lo, hi)
            ps[name] = int(round(val)) if ptype == "int" else round(float(val), 3)
        # Ensure rsi_lo < rsi_hi
        if ps["rsi_lo"] >= ps["rsi_hi"]:
            ps["rsi_hi"] = ps["rsi_lo"] + 5
        sets.append(ps)
    return sets


# ─────────────────────────────────────────────────────────────────────────────
def run_single(sym_df, symbol, sector_series, nepse_close, ps, risk_pct):
    """Run backtest for one symbol with given parameter set. Returns list of trades."""
    sym_df = sym_df.sort_values("date").reset_index(drop=True)
    if len(sym_df) < MIN_HISTORY:
        return []

    trades         = []
    position       = None
    last_exit_date = None

    adx_min       = ps["adx_min"]
    rsi_lo        = ps["rsi_lo"]
    rsi_hi        = ps["rsi_hi"]
    vol_mult      = ps["vol_mult"]
    drop_3d_lim   = ps["drop_3d_limit"]
    hh_lookback   = ps["hh_lookback"]
    weak_hold     = ps["weakness_hold"]
    weak_rsi      = ps["weakness_rsi"]
    trail_pct     = ps["trail_stop_pct"] / 100
    hard_pct      = ps["hard_stop_pct"]  / 100

    for i in range(1, len(sym_df)):
        today     = sym_df.iloc[i]
        yesterday = sym_df.iloc[i - 1]

        close   = float(today["close"])
        date    = today["date"]

        ema9    = float(yesterday["ema9"])   if not pd.isna(yesterday["ema9"])   else float("nan")
        ema21   = float(yesterday["ema21"])  if not pd.isna(yesterday["ema21"])  else float("nan")
        ema50   = float(yesterday["ema50"])  if not pd.isna(yesterday["ema50"])  else float("nan")
        rsi     = float(yesterday["rsi"])    if not pd.isna(yesterday["rsi"])    else 50.0
        adx     = float(yesterday["adx"])    if not pd.isna(yesterday["adx"])    else 0.0
        vol     = float(today["volume"])     if not pd.isna(today["volume"])     else 0.0
        vol_avg = float(yesterday["vol_avg20"]) if not pd.isna(yesterday["vol_avg20"]) else 0.0

        if pd.isna(ema9) or pd.isna(ema21) or pd.isna(ema50) or close < 100:
            continue

        # Volume increasing vs last 2 days
        vol_inc = False
        if i >= 3:
            v2 = float(sym_df.iloc[i-2]["volume"]) if not pd.isna(sym_df.iloc[i-2]["volume"]) else 0.0
            v1 = float(sym_df.iloc[i-1]["volume"]) if not pd.isna(sym_df.iloc[i-1]["volume"]) else 0.0
            vol_inc = (vol >= v1) and (v1 >= v2)

        # 3-day drop
        drop_3d = 0.0
        if i >= 3:
            p3 = float(sym_df.iloc[i-3]["close"])
            drop_3d = (close - p3) / p3 * 100 if p3 > 0 else 0.0

        # Higher high
        lb = min(i, hh_lookback)
        recent_hi = sym_df.iloc[i-lb: i]["high"].max() if lb > 0 else close
        higher_high = float(today["high"]) > float(recent_hi)

        # RS vs NEPSE
        outperforms = True
        if nepse_close is not None and i >= 20:
            try:
                sr = (close / float(sym_df.iloc[i-20]["close"]) - 1) * 100
                np_past = nepse_close[nepse_close.index <= date]
                if len(np_past) >= 20:
                    nr = (float(np_past.iloc[-1]) / float(np_past.iloc[-20]) - 1) * 100
                    outperforms = sr > nr
            except Exception:
                pass

        if position is None:
            if last_exit_date is not None and (date - last_exit_date).days <= 10:
                continue
            if sector_series is not None:
                past = sector_series[sector_series.index <= date]
                if not past.empty and past.iloc[-1] == "BEAR":
                    continue

            buy = (
                ema9 > ema21 > ema50            and
                close > ema21                    and
                close > ema50                    and
                rsi_lo <= rsi <= rsi_hi          and
                adx > adx_min                    and
                vol_avg > 0 and vol >= vol_mult * vol_avg and
                vol_inc                          and
                drop_3d > drop_3d_lim            and
                higher_high                      and
                outperforms
            )
            if buy:
                position = {
                    "entry_date":    date,
                    "entry_price":   close,
                    "peak_price":    close,
                    "qty":           calc_qty(close, risk_pct=risk_pct),
                    "ema_cross_days": 0,
                }
        else:
            days_held  = (date - position["entry_date"]).days
            profit_pct = (close - position["entry_price"]) / position["entry_price"]

            if close > position["peak_price"]:
                position["peak_price"] = close
            dfp = (close - position["peak_price"]) / position["peak_price"]

            if ema9 < ema21:
                position["ema_cross_days"] = position.get("ema_cross_days", 0) + 1
            else:
                position["ema_cross_days"] = 0

            lb_tp = max(min(i, 60), 30)
            res_hi = sym_df.iloc[max(0, i - lb_tp): i]["high"].max()

            exit_reason = None
            if profit_pct <= -hard_pct:
                exit_reason = f"HARD_STOP"
            elif position.get("ema_cross_days", 0) >= 3:
                exit_reason = f"TREND_FAIL"
            elif dfp <= -trail_pct:
                exit_reason = f"TRAIL_STOP"
            elif close >= res_hi and days_held >= 5:
                exit_reason = f"TAKE_PROFIT"
            elif rsi < weak_rsi and days_held >= weak_hold:
                exit_reason = f"WEAKNESS"

            if exit_reason:
                pnl_pct = profit_pct * 100
                pnl_npr = (close - position["entry_price"]) * position["qty"]
                trades.append({
                    "pnl_pct": round(pnl_pct, 2),
                    "pnl_npr": round(pnl_npr, 2),
                    "exit":    exit_reason,
                })
                position       = None
                last_exit_date = date

    # Open position
    if position is not None:
        last  = sym_df.iloc[-1]
        close = float(last["close"])
        pnl_pct = (close - position["entry_price"]) / position["entry_price"] * 100
        pnl_npr = (close - position["entry_price"]) * position["qty"]
        trades.append({"pnl_pct": round(pnl_pct, 2), "pnl_npr": round(pnl_npr, 2), "exit": "OPEN"})

    return trades


def score_param_set(args_tuple):
    """Worker function: run all symbols for one parameter set, return metrics."""
    sym_data, sector_regime_series, sector_map, nepse_close, ps, risk_pct = args_tuple

    all_trades = []
    for sym, sym_df in sym_data:
        sid = sector_map.get(sym)
        sec = (sector_regime_series or {}).get(sid) if sid is not None else None
        trades = run_single(sym_df, sym, sec, nepse_close, ps, risk_pct)
        all_trades.extend(trades)

    closed = [t for t in all_trades if t["exit"] != "OPEN"]
    if len(closed) < 30:  # too few trades — not enough sample
        return None

    pnls   = [t["pnl_pct"] for t in closed]
    nprs   = [t["pnl_npr"] for t in closed]
    wins   = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p <= 0]

    if not wins or not losses:
        return None

    win_rate = len(wins) / len(closed) * 100
    avg_win  = np.mean(wins)
    avg_loss = abs(np.mean(losses))
    pf       = (len(wins) * avg_win) / (len(losses) * avg_loss)
    total_pnl= sum(nprs)

    # Likelihood: combined score of P&L and profit factor (both normalized)
    # Using multiplicative likelihood — same concept as GLUE behavioral threshold
    likelihood = total_pnl * pf

    return {
        **ps,
        "trades":    len(closed),
        "win_rate":  round(win_rate, 1),
        "avg_win":   round(avg_win, 2),
        "avg_loss":  round(-avg_loss, 2),
        "pf":        round(pf, 3),
        "total_pnl": round(total_pnl),
        "likelihood":round(likelihood, 1),
    }


# ─────────────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="GLUE-style parameter calibration for Hardcore strategy")
    parser.add_argument("--runs",    type=int, default=500, help="Number of random parameter sets (default: 500)")
    parser.add_argument("--cores",   type=int, default=4,   help="Parallel workers (default: 4)")
    parser.add_argument("--seed",    type=int, default=42,  help="Random seed")
    parser.add_argument("--top",     type=float, default=BEHAVIORAL_TOP_PCT, help="Top fraction to keep as behavioral (default: 0.20)")
    args = parser.parse_args()

    print("Loading data...")
    df, sector_regime_series = load_data("merged_data.csv")

    sector_map = {}
    if os.path.exists("chukul_fundamental.csv"):
        fund = pd.read_csv("chukul_fundamental.csv")[["symbol", "sector_id"]].dropna()
        sector_map = {r["symbol"]: int(r["sector_id"]) for _, r in fund.iterrows()}

    nepse_close = None
    if "NEPSE" in df["symbol"].values:
        nepse_df    = df[df["symbol"] == "NEPSE"].sort_values("date")
        nepse_close = nepse_df.set_index("date")["close"]
        print(f"NEPSE index loaded ({len(nepse_close)} rows) for RS filter.")

    # Pre-group symbol data once
    sym_data = [(sym, df[df["symbol"] == sym].copy()) for sym in sorted(df["symbol"].unique())]

    print(f"\nSampling {args.runs} parameter sets...")
    param_sets = sample_parameters(args.runs, seed=args.seed)

    # Build worker args
    worker_args = [
        (sym_data, sector_regime_series, sector_map, nepse_close, ps, RISK_PCT_DEFAULT)
        for ps in param_sets
    ]

    print(f"Running {args.runs} backtests on {args.cores} cores...")
    results = []
    done = 0
    with ProcessPoolExecutor(max_workers=args.cores) as pool:
        futures = {pool.submit(score_param_set, a): i for i, a in enumerate(worker_args)}
        for fut in as_completed(futures):
            done += 1
            if done % 50 == 0 or done == args.runs:
                print(f"  {done}/{args.runs} complete...", flush=True)
            res = fut.result()
            if res:
                results.append(res)

    if not results:
        print("No valid results — all param sets had too few trades.")
        return

    res_df = pd.DataFrame(results)

    # ── Behavioral threshold (GLUE concept) ─────────────────────────────────
    # Keep top N% by likelihood — these are "behavioral" parameter sets
    n_behavioral = max(1, int(len(res_df) * args.top))
    behavioral   = res_df.nlargest(n_behavioral, "likelihood")

    print(f"\n{len(res_df)} valid sets from {args.runs} runs.")
    print(f"Behavioral threshold (top {args.top*100:.0f}%): {n_behavioral} sets")
    print(f"Behavioral P&L range: NPR {behavioral['total_pnl'].min():+,.0f} → {behavioral['total_pnl'].max():+,.0f}")

    # ── Posterior distribution (GLUE output) ────────────────────────────────
    param_names = list(PARAM_SPACE.keys())
    print("\n" + "═" * 80)
    print("  POSTERIOR DISTRIBUTION  (behavioral parameter sets)")
    print("═" * 80)
    print(f"  {'Parameter':<20} {'Min':>8} {'Mean':>8} {'Max':>8} {'StdDev':>8}  {'BestFit':>8}")
    print("  " + "-" * 68)
    best = behavioral.nlargest(1, "likelihood").iloc[0]
    for p in param_names:
        lo, hi, _ = PARAM_SPACE[p]
        col = behavioral[p]
        print(f"  {p:<20} {col.min():>8.2f} {col.mean():>8.2f} {col.max():>8.2f} {col.std():>8.2f}  {best[p]:>8.2f}")
    print("═" * 80)

    # ── Top 20 sets ──────────────────────────────────────────────────────────
    top20 = behavioral.nlargest(20, "likelihood")
    print("\n  TOP 20 BEHAVIORAL SETS (by likelihood = P&L × profit factor)")
    print(f"  {'ADX':>5} {'RSI_lo':>7} {'RSI_hi':>7} {'VolMult':>8} {'Drop3d':>7} {'HH_lb':>6} "
          f"{'WkHold':>7} {'WkRSI':>6} {'Trail':>6} {'HStop':>6} "
          f"{'Trades':>7} {'WR%':>6} {'PF':>5} {'P&L':>12}")
    print("  " + "-" * 110)
    for _, r in top20.iterrows():
        print(f"  {r.adx_min:>5} {r.rsi_lo:>7} {r.rsi_hi:>7} {r.vol_mult:>8.2f} {r.drop_3d_limit:>7.1f} "
              f"{r.hh_lookback:>6} {r.weakness_hold:>7} {r.weakness_rsi:>6} "
              f"{r.trail_stop_pct:>6.1f} {r.hard_stop_pct:>6.1f} "
              f"{r.trades:>7} {r.win_rate:>5.1f}% {r.pf:>5.2f}  NPR {r.total_pnl:>+10,.0f}")

    # ── Best single set ──────────────────────────────────────────────────────
    print(f"\n  BEST PARAMETER SET (highest likelihood):")
    for p in param_names:
        lo, hi, _ = PARAM_SPACE[p]
        print(f"    {p:<22} = {best[p]}   (range: {lo} – {hi})")
    print(f"\n    → Trades={best['trades']}  WinRate={best['win_rate']}%  "
          f"PF={best['pf']}  Total P&L=NPR {best['total_pnl']:+,.0f}")

    # ── Save results ─────────────────────────────────────────────────────────
    out_file = "glue_hardcore_results.csv"
    res_df.sort_values("likelihood", ascending=False).to_csv(out_file, index=False)
    print(f"\n  Full results saved → {out_file}")

    # ── Sensitivity: which parameters vary most in behavioral sets? ──────────
    print("\n  PARAMETER SENSITIVITY (coefficient of variation in behavioral sets)")
    print("  Higher CV = parameter matters more — wider range still performs well")
    print(f"  {'Parameter':<22} {'CV%':>8}  Interpretation")
    print("  " + "-" * 60)
    sensitivity = []
    for p in param_names:
        col = behavioral[p]
        cv = (col.std() / abs(col.mean()) * 100) if col.mean() != 0 else 0
        sensitivity.append((p, cv))
    for p, cv in sorted(sensitivity, key=lambda x: x[1]):
        interp = "SENSITIVE (narrow range works)" if cv < 10 else "ROBUST (wide range works)" if cv > 25 else "MODERATE"
        print(f"  {p:<22} {cv:>7.1f}%  {interp}")


if __name__ == "__main__":
    main()
