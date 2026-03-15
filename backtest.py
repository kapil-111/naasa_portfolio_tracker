"""
backtest.py — Backtest the NAASA signal strategy on historical data.

Usage:
    python backtest.py                          # all symbols, default settings
    python backtest.py --symbols NABIL,ADBL     # specific symbols
    python backtest.py --buy-threshold 4 --sell-threshold -3 --capital 200000

Requires: chukul_data.csv  (run fetch_chukul_history.py first)
Optional: chukul_fundamental.csv  (adds fundamental scoring)
"""

import argparse
import os
import sys
import numpy as np
import pandas as pd


# ── Default config ─────────────────────────────────────────────────────────────
DEFAULT_CAPITAL        = 100_000
DEFAULT_BUY_THRESHOLD  = 4
DEFAULT_SELL_THRESHOLD = -3
DEFAULT_BUY_QTY        = 10
MAX_DAILY_BUYS         = 2


# ── Indicators ─────────────────────────────────────────────────────────────────

def calc_ema(series, span):
    return series.ewm(span=span, adjust=False).mean()


def calc_macd(series, fast=12, slow=26, signal=9):
    ema_fast    = series.ewm(span=fast,   adjust=False).mean()
    ema_slow    = series.ewm(span=slow,   adjust=False).mean()
    macd_line   = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=signal, adjust=False).mean()
    return macd_line, signal_line


def calc_rsi(series, period=14):
    delta    = series.diff()
    gain     = delta.clip(lower=0)
    loss     = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1 / period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))


# ── Scoring (mirrors signals.py, RSI computed from history) ───────────────────

def score_symbol(symbol, sym_df, fund_map):
    if len(sym_df) < 26:
        return 0, {}

    close    = sym_df["close"]
    volume   = sym_df["volume"] if "volume" in sym_df.columns else None
    bd       = {"ema": 0, "macd": 0, "volume": 0, "rsi": 0, "fundamental": 0}

    # 1. EMA Crossover
    ema9  = calc_ema(close, 9)
    ema21 = calc_ema(close, 21)
    prev  = ema9.iloc[-2] - ema21.iloc[-2]
    curr  = ema9.iloc[-1] - ema21.iloc[-1]
    if   prev <= 0 and curr > 0: bd["ema"] = +2
    elif prev >= 0 and curr < 0: bd["ema"] = -2
    elif curr > 0:               bd["ema"] = +1
    elif curr < 0:               bd["ema"] = -1

    # 2. MACD
    ml, sl = calc_macd(close)
    pd_    = ml.iloc[-2] - sl.iloc[-2]
    cd_    = ml.iloc[-1] - sl.iloc[-1]
    if   pd_ <= 0 and cd_ > 0: bd["macd"] = +1
    elif pd_ >= 0 and cd_ < 0: bd["macd"] = -1

    # 3. Volume
    if volume is not None and len(volume) >= 21:
        avg_vol = volume.iloc[-21:-1].mean()
        if float(volume.iloc[-1]) > avg_vol:
            if bd["ema"] > 0:  bd["volume"] = +1
            elif bd["ema"] < 0: bd["volume"] = -1

    # 4. RSI (computed from OHLCV history)
    if len(close) >= 15:
        rsi = float(calc_rsi(close).iloc[-1])
        if not np.isnan(rsi):
            if   rsi > 70:        bd["rsi"] = -2
            elif rsi < 30:        bd["rsi"] = +2
            elif 60 <= rsi <= 70: bd["rsi"] = +1

    # 5. Fundamentals
    fund = fund_map.get(symbol, {})
    eps  = fund.get("eps")
    pe   = fund.get("pe")
    if eps is not None and not np.isnan(eps):
        if eps <= 0: bd["fundamental"] -= 1
    if pe is not None and not np.isnan(pe):
        if pe <= 50:   bd["fundamental"] += 1
        elif pe > 500: bd["fundamental"] -= 1

    return sum(bd.values()), bd


# ── Backtest Engine ────────────────────────────────────────────────────────────

def run_backtest(df, fund_map, buy_threshold, sell_threshold, initial_capital, buy_qty):
    dates    = sorted(df["date"].unique())
    cash     = float(initial_capital)
    holdings = {}  # symbol -> {qty, avg_price, buy_date}
    trades   = []

    for i, date in enumerate(dates):
        if i < 26:
            continue

        window     = df[df["date"] <= date]
        buys_today = 0

        for symbol in window["symbol"].unique():
            sym_df = window[window["symbol"] == symbol].copy().reset_index(drop=True)
            score, _ = score_symbol(symbol, sym_df, fund_map)
            last_close = float(sym_df["close"].iloc[-1])

            # SELL
            if score <= sell_threshold and symbol in holdings:
                pos      = holdings.pop(symbol)
                proceeds = pos["qty"] * last_close
                cash    += proceeds
                pnl      = proceeds - pos["qty"] * pos["avg_price"]
                trades.append({
                    "symbol":    symbol,
                    "side":      "SELL",
                    "date":      date,
                    "price":     last_close,
                    "qty":       pos["qty"],
                    "pnl":       pnl,
                    "pnl_pct":   pnl / (pos["qty"] * pos["avg_price"]) * 100,
                    "hold_days": (date - pos["buy_date"]).days,
                })

            # BUY
            elif score >= buy_threshold and symbol not in holdings and buys_today < MAX_DAILY_BUYS:
                cost = buy_qty * last_close
                if cash >= cost:
                    cash -= cost
                    holdings[symbol] = {"qty": buy_qty, "avg_price": last_close, "buy_date": date}
                    trades.append({
                        "symbol": symbol, "side": "BUY", "date": date,
                        "price": last_close, "qty": buy_qty,
                        "pnl": 0, "pnl_pct": 0, "hold_days": 0,
                    })
                    buys_today += 1

    # Close all open positions at last available price
    last_date = dates[-1]
    for symbol, pos in holdings.items():
        last_price = float(df[df["symbol"] == symbol]["close"].iloc[-1])
        proceeds   = pos["qty"] * last_price
        cash      += proceeds
        pnl        = proceeds - pos["qty"] * pos["avg_price"]
        trades.append({
            "symbol":    symbol,
            "side":      "SELL(end)",
            "date":      last_date,
            "price":     last_price,
            "qty":       pos["qty"],
            "pnl":       pnl,
            "pnl_pct":   pnl / (pos["qty"] * pos["avg_price"]) * 100,
            "hold_days": (last_date - pos["buy_date"]).days,
        })

    return cash, trades


# ── Report ─────────────────────────────────────────────────────────────────────

def print_report(initial_capital, final_capital, trades):
    sell_trades = [t for t in trades if "SELL" in t["side"]]
    buy_trades  = [t for t in trades if t["side"] == "BUY"]
    total_return     = final_capital - initial_capital
    total_return_pct = total_return / initial_capital * 100

    print("\n" + "=" * 55)
    print("  BACKTEST RESULTS")
    print("=" * 55)
    print(f"  Initial Capital  : NPR {initial_capital:>12,.2f}")
    print(f"  Final Capital    : NPR {final_capital:>12,.2f}")
    print(f"  Total Return     : NPR {total_return:>+12,.2f}  ({total_return_pct:+.1f}%)")
    print("-" * 55)
    print(f"  Total BUY trades : {len(buy_trades)}")
    print(f"  Total SELL trades: {len(sell_trades)}")

    if sell_trades:
        pnls       = [t["pnl"] for t in sell_trades]
        winners    = [p for p in pnls if p > 0]
        losers     = [p for p in pnls if p <= 0]
        win_rate   = len(winners) / len(sell_trades) * 100
        avg_hold   = np.mean([t["hold_days"] for t in sell_trades])
        best       = max(sell_trades, key=lambda t: t["pnl"])
        worst      = min(sell_trades, key=lambda t: t["pnl"])

        print(f"  Win Rate         : {win_rate:.1f}%")
        print(f"  Avg Win          : NPR {np.mean(winners):>+,.2f}" if winners else "  Avg Win         : —")
        print(f"  Avg Loss         : NPR {np.mean(losers):>+,.2f}"  if losers  else "  Avg Loss        : —")
        print(f"  Avg Hold Period  : {avg_hold:.1f} days")
        print(f"  Best Trade       : {best['symbol']}  NPR {best['pnl']:>+,.2f}  ({best['pnl_pct']:+.1f}%)")
        print(f"  Worst Trade      : {worst['symbol']} NPR {worst['pnl']:>+,.2f}  ({worst['pnl_pct']:+.1f}%)")
    print("=" * 55)

    if sell_trades:
        print(f"\n  {'Date':<12} {'Symbol':<8} {'Side':<10} {'Qty':>4} {'Price':>8} {'P&L':>10} {'%':>7} {'Days':>5}")
        print("  " + "-" * 65)
        for t in sorted(sell_trades, key=lambda x: x["date"]):
            print(f"  {str(t['date'])[:10]:<12} {t['symbol']:<8} {t['side']:<10} "
                  f"{t['qty']:>4} {t['price']:>8.2f} {t['pnl']:>+10.2f} "
                  f"{t['pnl_pct']:>+6.1f}% {t['hold_days']:>5}d")


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Backtest NAASA signal strategy")
    parser.add_argument("--capital",        type=float, default=DEFAULT_CAPITAL)
    parser.add_argument("--buy-threshold",  type=int,   default=DEFAULT_BUY_THRESHOLD)
    parser.add_argument("--sell-threshold", type=int,   default=DEFAULT_SELL_THRESHOLD)
    parser.add_argument("--qty",            type=int,   default=DEFAULT_BUY_QTY)
    parser.add_argument("--symbols",        type=str,   default=None,
                        help="Comma-separated symbols to test (default: all)")
    args = parser.parse_args()

    if not os.path.exists("chukul_data.csv"):
        print("Error: chukul_data.csv not found. Fetch historical data first.")
        sys.exit(1)

    print("Loading OHLCV data...")
    df = pd.read_csv("chukul_data.csv")
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])
    if "symbol" not in df.columns and "stock" in df.columns:
        df.rename(columns={"stock": "symbol"}, inplace=True)
    df.sort_values(["symbol", "date"], inplace=True)

    if args.symbols:
        syms = [s.strip().upper() for s in args.symbols.split(",")]
        df   = df[df["symbol"].isin(syms)]
        print(f"Symbols : {syms}")

    # Load fundamentals
    fund_map = {}
    if os.path.exists("chukul_fundamental.csv"):
        fdf = pd.read_csv("chukul_fundamental.csv")
        fdf.columns = [c.lower() for c in fdf.columns]
        for _, row in fdf.iterrows():
            sym = str(row.get("symbol", "")).strip()
            if not sym:
                continue
            eps = pd.to_numeric(row.get("eps") or row.get("basic_eps"), errors="coerce")
            pe  = pd.to_numeric(row.get("pe")  or row.get("pe_ratio"),  errors="coerce")
            fund_map[sym] = {
                "eps": float(eps) if not pd.isna(eps) else float("nan"),
                "pe":  float(pe)  if not pd.isna(pe)  else float("nan"),
            }

    date_min = df["date"].min().date()
    date_max = df["date"].max().date()
    print(f"Period  : {date_min} → {date_max}")
    print(f"Symbols : {df['symbol'].nunique()}")
    print(f"Config  : BUY>={args.buy_threshold}  SELL<={args.sell_threshold}  "
          f"Qty={args.qty}  Capital=NPR {args.capital:,.0f}")
    print("Running backtest (this may take a minute for all symbols)...")

    final_capital, trades = run_backtest(
        df, fund_map,
        buy_threshold   = args.buy_threshold,
        sell_threshold  = args.sell_threshold,
        initial_capital = args.capital,
        buy_qty         = args.qty,
    )

    print_report(args.capital, final_capital, trades)


if __name__ == "__main__":
    main()
