"""
Parameter sweep for backtest_hardcore entry/exit improvements.
Tests combinations of:
  - adx_min       : 20, 23, 25, 28, 30
  - weakness_min_hold : 3, 5, 7, 10
Prints a ranked table sorted by Total P&L.
"""

import sys
import pandas as pd
from backtest import (
    load_data, backtest_hardcore,
    build_sector_regime_series, RISK_PCT_DEFAULT
)

ADX_VALUES        = [20, 23, 25, 28, 30]
WEAKNESS_HOLDS    = [3, 5, 7, 10]

def run_combo(df, sector_regime_series, sector_map, nepse_close, adx_min, wh):
    all_trades = []
    for sym in sorted(df["symbol"].unique()):
        sym_df = df[df["symbol"] == sym].copy()
        sid = sector_map.get(sym)
        trades = backtest_hardcore(
            sym_df, sym,
            sector_id=sid,
            sector_regime_series=sector_regime_series,
            nepse_close=nepse_close,
            risk_pct=RISK_PCT_DEFAULT,
            adx_min=adx_min,
            weakness_min_hold=wh,
        )
        all_trades.extend(trades)

    if not all_trades:
        return None

    df_t   = pd.DataFrame(all_trades)
    closed = df_t[df_t["exit_date"] != "OPEN"]
    if closed.empty:
        return None

    wins     = closed[closed["pnl_pct"] > 0]
    losses   = closed[closed["pnl_pct"] <= 0]
    n        = len(closed)
    win_rate = len(wins) / n * 100
    avg_win  = wins["pnl_pct"].mean()  if len(wins)   else 0
    avg_loss = losses["pnl_pct"].mean() if len(losses) else 0
    total_pnl = closed["pnl_npr"].sum()
    pf = (len(wins) * avg_win) / (len(losses) * abs(avg_loss)) if len(losses) and avg_loss != 0 else 0

    # Exit breakdown
    reasons = closed["exit_reason"].str.split("(").str[0].str.strip().value_counts().to_dict()

    return {
        "adx_min":    adx_min,
        "weak_hold":  wh,
        "trades":     n,
        "win_rate":   round(win_rate, 1),
        "avg_win":    round(avg_win, 2),
        "avg_loss":   round(avg_loss, 2),
        "pf":         round(pf, 2),
        "total_pnl":  round(total_pnl),
        "hard_stops": reasons.get("HARD_STOP", 0),
        "weakness":   reasons.get("WEAKNESS", 0),
        "take_profit":reasons.get("TAKE_PROFIT", 0),
    }


def main():
    print("Loading data...")
    df, sector_regime_series = load_data("merged_data.csv")

    sector_map = {}
    import os
    if os.path.exists("chukul_fundamental.csv"):
        fund = pd.read_csv("chukul_fundamental.csv")[["symbol", "sector_id"]].dropna()
        sector_map = {r["symbol"]: int(r["sector_id"]) for _, r in fund.iterrows()}

    nepse_close = None
    if "NEPSE" in df["symbol"].values:
        nepse_df    = df[df["symbol"] == "NEPSE"].sort_values("date")
        nepse_close = nepse_df.set_index("date")["close"]

    results = []
    total   = len(ADX_VALUES) * len(WEAKNESS_HOLDS)
    done    = 0
    for adx_min in ADX_VALUES:
        for wh in WEAKNESS_HOLDS:
            done += 1
            print(f"  [{done}/{total}] ADX>{adx_min}  weakness_hold={wh}d ...", end=" ", flush=True)
            row = run_combo(df, sector_regime_series, sector_map, nepse_close, adx_min, wh)
            if row:
                results.append(row)
                print(f"P&L={row['total_pnl']:+,.0f}  WR={row['win_rate']}%  PF={row['pf']}")
            else:
                print("no trades")

    if not results:
        print("No results.")
        return

    res_df = pd.DataFrame(results).sort_values("total_pnl", ascending=False)

    print("\n" + "═" * 90)
    print("  PARAMETER SWEEP RESULTS — sorted by Total P&L")
    print("═" * 90)
    print(f"  {'ADX>':>5} {'WeakHold':>9} {'Trades':>7} {'WinRate':>8} {'AvgWin':>7} {'AvgLoss':>8} "
          f"{'PF':>5} {'TotalPNL':>12} {'HardStop':>9} {'Weakness':>9} {'TP':>6}")
    print("  " + "-" * 88)
    for _, r in res_df.iterrows():
        marker = " ◄ BEST" if _ == res_df.index[0] else ""
        print(f"  {r['adx_min']:>5} {r['weak_hold']:>9}d {r['trades']:>7} {r['win_rate']:>7.1f}% "
              f"{r['avg_win']:>+7.1f}% {r['avg_loss']:>+7.1f}%  {r['pf']:>4.2f}  "
              f"{r['total_pnl']:>+11,.0f}  {r['hard_stops']:>8}  {r['weakness']:>8}  {r['take_profit']:>5}{marker}")
    print("═" * 90)

    best = res_df.iloc[0]
    print(f"\n  BEST: ADX > {best['adx_min']}  |  weakness_min_hold = {best['weak_hold']}d")
    print(f"        Total P&L = NPR {best['total_pnl']:+,.0f}  |  Win rate = {best['win_rate']}%  |  PF = {best['pf']}")


if __name__ == "__main__":
    main()
