"""
Fortress Parameter Tuner  (vectorized — fast)
=============================================
Pre-computes all daily signals as boolean arrays, then for each parameter
combination just filters + counts wins/losses — no per-row Python loop.

Run:  python tune.py
"""

import os
import sys
import itertools
import numpy as np
import pandas as pd

from signals_mr import _adjust_prices, _calc_rsi, _calc_ema, _calc_adx

MIN_HISTORY     = 50
DEFAULT_BUY_QTY = 20
MIN_TRADES      = 30


# ── 1. Load & indicator calc (same as live bot) ──────────────────────────────

def load_data(ohlcv_file="chukul_data.csv"):
    df = pd.read_csv(ohlcv_file)
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], format="mixed").dt.normalize()
    if "symbol" not in df.columns and "stock" in df.columns:
        df.rename(columns={"stock": "symbol"}, inplace=True)
    df.sort_values(["symbol", "date"], inplace=True)
    df = _adjust_prices(df.copy())

    if os.path.exists("chukul_fundamental.csv"):
        fund = pd.read_csv("chukul_fundamental.csv")
        eps_ok = fund[fund["eps"].notna() & (fund["eps"] > 0)]["symbol"]
        roe_ok = fund[fund["roe"].notna() & (fund["roe"] > 5)]["symbol"]
        npl_ok = fund[fund["npl"].isna() | (fund["npl"] < 10)]["symbol"]
        good   = set(eps_ok) & set(roe_ok) & set(npl_ok)
        df = df[df["symbol"].isin(good)]

    print("Calculating indicators...")
    df["ema9"]       = df.groupby("symbol")["close"].transform(lambda x: _calc_ema(x, 9))
    df["ema21"]      = df.groupby("symbol")["close"].transform(lambda x: _calc_ema(x, 21))
    df["rsi"]        = df.groupby("symbol")["close"].transform(lambda x: _calc_rsi(x, 14))
    df["vol_avg20"]  = df.groupby("symbol")["volume"].transform(
                           lambda x: x.rolling(20, min_periods=5).mean())

    adx_parts = []
    for sym, grp in df.groupby("symbol"):
        adx_vals = _calc_adx(grp["high"], grp["low"], grp["close"])
        adx_parts.append(pd.Series(adx_vals.values, index=grp.index, name="adx"))
    df["adx"] = pd.concat(adx_parts).reindex(df.index)

    # Shifted versions (yesterday's confirmed candle — same as live bot)
    df["ema9_y"]      = df.groupby("symbol")["ema9"].shift(1)
    df["ema21_y"]     = df.groupby("symbol")["ema21"].shift(1)
    df["rsi_y"]       = df.groupby("symbol")["rsi"].shift(1)
    df["adx_y"]       = df.groupby("symbol")["adx"].shift(1)
    df["vol_avg20_y"] = df.groupby("symbol")["vol_avg20"].shift(1)
    df["prev_vol"]    = df.groupby("symbol")["volume"].shift(1)
    df["prev_close"]  = df.groupby("symbol")["close"].shift(1)
    df["ema9_2d"]     = df.groupby("symbol")["ema9"].shift(2)
    df["ema21_2d"]    = df.groupby("symbol")["ema21"].shift(2)

    df = df.dropna(subset=["ema9_y", "ema21_y", "rsi_y", "adx_y"])
    df = df[df["close"] >= 100]
    return df


# ── 2. Per-symbol sequential simulation (needed for position state) ──────────

def simulate_symbol(sym_df, params):
    """Returns list of closed trade pnl_pct values."""
    ADX_MIN     = params["adx_min"]
    RSI_MIN     = params["rsi_min"]
    RSI_MAX     = params["rsi_max"]
    VOL_FACTOR  = params["vol_factor"]
    TP_PCT      = params["tp_pct"]
    SL_PCT      = params["sl_pct"]
    RSI_OB      = params["rsi_ob"]
    MIN_HOLD    = params["min_hold"]
    EMA_CONFIRM = params["ema_confirm"]

    rows = sym_df.to_numpy()
    # col indices
    COL = {c: i for i, c in enumerate(sym_df.columns)}
    ci_close   = COL["close"]
    ci_ema9y   = COL["ema9_y"]
    ci_ema21y  = COL["ema21_y"]
    ci_rsi     = COL["rsi_y"]
    ci_adx     = COL["adx_y"]
    ci_va20    = COL["vol_avg20_y"]
    ci_pvol    = COL["prev_vol"]
    ci_pcls    = COL["prev_close"]
    ci_e9_2d   = COL["ema9_2d"]
    ci_e21_2d  = COL["ema21_2d"]
    ci_date    = COL["date"]

    trades         = []
    position       = None
    last_exit_date = None

    for row in rows:
        close     = float(row[ci_close])
        ema9      = float(row[ci_ema9y])
        ema21     = float(row[ci_ema21y])
        rsi       = float(row[ci_rsi]) if not np.isnan(float(row[ci_rsi])) else 50.0
        adx       = float(row[ci_adx]) if not np.isnan(float(row[ci_adx])) else 0.0
        vol_avg20 = float(row[ci_va20]) if not np.isnan(float(row[ci_va20])) else 0.0
        prev_vol  = float(row[ci_pvol])
        prev_close= float(row[ci_pcls]) if not np.isnan(float(row[ci_pcls])) else close
        date      = row[ci_date]
        e9_2d     = float(row[ci_e9_2d]) if not np.isnan(float(row[ci_e9_2d])) else ema9
        e21_2d    = float(row[ci_e21_2d]) if not np.isnan(float(row[ci_e21_2d])) else ema21

        if np.isnan(ema9) or np.isnan(ema21):
            continue

        ema_bull       = ema9 > ema21
        ema_below_now  = ema9 < ema21
        ema_below_prev = e9_2d < e21_2d
        vol_surge      = vol_avg20 > 0 and prev_vol >= vol_avg20 * VOL_FACTOR
        daily_ret      = (close - prev_close) / prev_close if prev_close > 0 else 0

        if position is None:
            if last_exit_date is not None and (date - last_exit_date).days <= 10:
                continue
            if (ema_bull and adx > ADX_MIN and RSI_MIN <= rsi <= RSI_MAX
                    and vol_surge and close > ema21 and daily_ret > -0.05):
                position = {"entry_date": date, "entry_price": close,
                            "initial_entry": close, "ema_cross_days": 0}
        else:
            days_held       = (date - position["entry_date"]).days
            profit_pct      = (close - position["entry_price"]) / position["entry_price"] * 100
            drop_from_start = (close - position["initial_entry"]) / position["initial_entry"] * 100

            if ema_below_now and ema_below_prev:
                position["ema_cross_days"] += 1
            else:
                position["ema_cross_days"] = 0

            if days_held < 3:
                continue

            exit_pnl = None
            if drop_from_start <= -25 and days_held >= 20:
                exit_pnl = profit_pct
            elif profit_pct >= TP_PCT:
                exit_pnl = profit_pct
            elif profit_pct <= SL_PCT:
                exit_pnl = profit_pct
            elif rsi > RSI_OB:
                exit_pnl = profit_pct
            elif days_held >= MIN_HOLD and position["ema_cross_days"] >= EMA_CONFIRM:
                exit_pnl = profit_pct

            if exit_pnl is not None:
                trades.append(exit_pnl)
                position = None
                last_exit_date = date

    return trades


def run_backtest(df, params):
    all_pnl = []
    for sym, sym_df in df.groupby("symbol"):
        if len(sym_df) < MIN_HISTORY:
            continue
        all_pnl.extend(simulate_symbol(sym_df.reset_index(drop=True), params))

    if len(all_pnl) < MIN_TRADES:
        return None

    arr      = np.array(all_pnl)
    wins     = arr[arr > 0]
    losses   = arr[arr <= 0]
    n        = len(arr)
    win_rate = len(wins) / n
    avg_win  = wins.mean()  if len(wins)   else 0.0
    avg_loss = losses.mean() if len(losses) else 0.0
    exp      = win_rate * avg_win + (1 - win_rate) * avg_loss

    return {
        "n":          n,
        "win_rate":   round(win_rate * 100, 1),
        "avg_win":    round(avg_win, 2),
        "avg_loss":   round(avg_loss, 2),
        "expectancy": round(exp, 3),
    }


def main():
    if not os.path.exists("chukul_data.csv"):
        print("chukul_data.csv not found.")
        sys.exit(1)

    df = load_data()

    # Parameter grid — 3 values each = 3^9 = 19683 combos but runs fast now
    grid = {
        "adx_min":     [20, 25, 30],
        "rsi_min":     [40, 45, 50],
        "rsi_max":     [60, 65, 70],
        "vol_factor":  [1.2, 1.5, 2.0],
        "tp_pct":      [15.0, 20.0, 25.0],
        "sl_pct":      [-8.0, -10.0, -12.0],
        "rsi_ob":      [70, 75, 80],
        "min_hold":    [3, 5, 7],
        "ema_confirm": [2, 3, 4],
    }

    keys   = list(grid.keys())
    combos = list(itertools.product(*grid.values()))
    total  = len(combos)
    print(f"Testing {total} parameter combinations...")

    results = []
    for i, combo in enumerate(combos):
        if i % 1000 == 0:
            print(f"  {i}/{total}...", end="\r", flush=True)
        params = dict(zip(keys, combo))
        res    = run_backtest(df, params)
        if res:
            results.append({**params, **res})

    if not results:
        print("No valid results.")
        return

    res_df = pd.DataFrame(results).sort_values("expectancy", ascending=False)

    print(f"\n\nTop 20 configurations by expectancy (avg P&L per trade):\n")
    hdr = f"{'ADX':>4} {'RSI':>8} {'Vol':>5} {'TP':>5} {'SL':>5} {'OB':>4} {'Hold':>5} {'EMA':>4} {'N':>4} {'WR%':>6} {'AvgW':>6} {'AvgL':>6} {'Exp%':>6}"
    print(hdr)
    print("-" * len(hdr))
    for _, r in res_df.head(20).iterrows():
        print(f"{r['adx_min']:>4} {r['rsi_min']:>4}-{r['rsi_max']:<3} {r['vol_factor']:>5.1f} "
              f"{r['tp_pct']:>5.0f} {r['sl_pct']:>5.0f} {r['rsi_ob']:>4} "
              f"{r['min_hold']:>5} {r['ema_confirm']:>4} "
              f"{r['n']:>4} {r['win_rate']:>5.1f}% {r['avg_win']:>+5.1f}% {r['avg_loss']:>+5.1f}% "
              f"{r['expectancy']:>+5.2f}%")

    best = res_df.iloc[0]
    print(f"\n\nBest config (expectancy = {best['expectancy']:+.2f}% per trade):")
    for k in keys:
        print(f"  {k:<15} = {best[k]}")
    print(f"\n  Trades     : {int(best['n'])}")
    print(f"  Win rate   : {best['win_rate']}%")
    print(f"  Avg win    : {best['avg_win']:+.2f}%")
    print(f"  Avg loss   : {best['avg_loss']:+.2f}%")
    print(f"  Expectancy : {best['expectancy']:+.3f}% per trade")

    # Also show best by win rate (min 40 trades)
    by_wr = res_df[res_df["n"] >= 40].sort_values("win_rate", ascending=False)
    if len(by_wr):
        bw = by_wr.iloc[0]
        print(f"\nBest win rate ({bw['win_rate']}% @ {int(bw['n'])} trades, exp={bw['expectancy']:+.2f}%):")
        for k in keys:
            print(f"  {k:<15} = {bw[k]}")


if __name__ == "__main__":
    main()
