"""
backtest.py — Backtest the NAASA signal strategy on historical data.

Usage:
    python backtest.py --strategy mean_reversion --capital 100000

Requires: chukul_data.csv  (run fetch_chukul_history.py first)
"""

import argparse
import os
import sys
import numpy as np
import pandas as pd
import requests # Need this for _load_nepse_index


# ── Default config ─────────────────────────────────────────────────────────────
DEFAULT_CAPITAL        = 100_000
DEFAULT_BUY_QTY        = 10 # This will be used as a default for buy_qty in mean_reversion_backtest


# ── Indicators (General Purpose) ─────────────────────────────────────────────────
# Keep these as they are general purpose and might be useful for mean_reversion in the future
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

def calc_atr(high, low, close, period):
    prev_close = close.shift(1)
    tr = pd.concat([high - low,
                    (high - prev_close).abs(),
                    (low  - prev_close).abs()], axis=1).max(axis=1)
    return tr.ewm(span=period, adjust=False).mean()


# ── Price Adjustment for Corporate Actions ─────────────────────────────────────
# Keep this utility
def _adjust_prices(df, actions_file="chukul_corporate_actions.csv"):
    """
    Backward-adjust OHLC prices for bonus shares and right issues.
    """
    if not os.path.exists(actions_file):
        return df

    ca = pd.read_csv(actions_file, parse_dates=["book_close_date"])
    date_min = df["date"].min()
    ca = ca[ca["book_close_date"] >= date_min - pd.Timedelta(days=5)]

    if ca.empty:
        return df

    all_dates = sorted(df["date"].unique())

    def next_trading_day(bcd):
        for d in all_dates:
            if d > bcd:
                return d
        return None

    adjustments = {}
    for _, row in ca.iterrows():
        sym     = row["symbol"]
        bcd     = row["book_close_date"]
        atype   = row["action_type"]
        pct     = float(row["pct"])
        ex_date = next_trading_day(bcd)
        if ex_date is None:
            continue

        if atype == "bonus":
            factor = 1.0 / (1.0 + pct / 100.0)
        else: # right share — use actual price ratio
            sym_df   = df[df["symbol"] == sym].sort_values("date")
            ex_row   = sym_df[sym_df["date"] == ex_date]
            prev_row = sym_df[sym_df["date"] < ex_date].tail(1)
            if ex_row.empty or prev_row.empty:
                continue
            ratio = float(ex_row["open"].iloc[0]) / float(prev_row["close"].iloc[0])
            if ratio > 0.92: # less than 8% drop — ambiguous, skip
                continue
            factor = ratio

        if sym not in adjustments:
            adjustments[sym] = []
        adjustments[sym].append((ex_date, factor))

    if not adjustments:
        return df

    ohlc_cols = [c for c in ["open", "high", "low", "close", "ltp"] if c in df.columns]
    parts = []
    for sym, sym_df in df.groupby("symbol", sort=False):
        sym_df = sym_df.copy()
        if sym in adjustments:
            for ex_date, factor in sorted(adjustments[sym], key=lambda x: x[0]):
                mask = sym_df["date"] < ex_date
                sym_df.loc[mask, ohlc_cols] = sym_df.loc[mask, ohlc_cols] * factor
        parts.append(sym_df)

    adjusted = pd.concat(parts, ignore_index=True)
    total = sum(len(v) for v in adjustments.values())
    print(f"Price-adjusted {total} corporate action events across {len(adjustments)} symbols.")
    return adjusted


# ── Backtest Engine Helpers ────────────────────────────────────────────────────
def _load_nepse_index():
    """Fetch NEPSE index history. Bullish = triple EMA alignment (EMA9 > EMA21 > EMA50)."""
    try:
        r = requests.get("https://chukul.com/api/data/historydata/?symbol=NEPSE",
                         headers={"User-Agent": "Mozilla/5.0"}, timeout=15)
        data = r.json()
        df = pd.DataFrame(data)
        df["date"]  = pd.to_datetime(df["date"])
        df["close"] = pd.to_numeric(df["close"], errors="coerce")
        df = df[["date", "close"]].sort_values("date").reset_index(drop=True)
        ema9  = calc_ema(df["close"], 9)
        ema21 = calc_ema(df["close"], 21)
        # Bullish: EMA9 > EMA21 AND EMA21 is rising (not just a whipsaw cross)
        ema21_rising = ema21 > ema21.shift(5)
        df["bullish"] = (ema9 > ema21) & ema21_rising
        return df.set_index("date")["bullish"].to_dict()
    except Exception as e:
        print(f"Warning: Could not fetch NEPSE index — market filter disabled: {e}")
        return {}


# ── Mean Reversion Strategy (52-week high/low) ───────────────────────────────

def run_mean_reversion_backtest(df, initial_capital, buy_qty,
                                stop_loss_pct, take_profit_pct, max_hold_days,
                                use_market_filter):
    """
    Mean Reversion Strategy
    -----------------------
    BUY : Price is within 5% of the 52-week low.
    SELL: Price is within 5% of the 52-week high.
    Position sizing: 10% of capital per trade. Min hold: T+3.
    """
    dates = sorted(df["date"].unique())
    cash = float(initial_capital)
    holdings = {}
    trades = []
    nepse_trend = _load_nepse_index() if use_market_filter else {}
    trading_days_in_year = 252  # Approximate trading days in a year

    # Pre-calculate all 52-week highs and lows for efficiency
    print("Pre-calculating 52-week highs and lows...")
    # Using min_periods=1 to align with original user request, making it less strict
    df['low_52wk'] = df.groupby('symbol')['low'].transform(lambda x: x.rolling(trading_days_in_year, min_periods=1).min())
    df['high_52wk'] = df.groupby('symbol')['high'].transform(lambda x: x.rolling(trading_days_in_year, min_periods=1).max())
    print("Pre-calculation complete.")

    for i, date in enumerate(dates):
        # The rolling window already handles the min_periods, so no need for 'if i < trading_days_in_year:'
        # However, we still need sufficient data for the strategy to make sense
        if i < 20: # arbitrary minimum for some price action
            continue

        market_bullish = nepse_trend.get(date, True) if use_market_filter else True
        
        # Use pre-calculated values for the current date
        daily_df = df[df["date"] == date].copy() # Ensure copy to avoid SettingWithCopyWarning
        buys_today = 0

        for _, row in daily_df.iterrows():
            symbol = row['symbol']
            last_close = float(row['close'])
            low_52_week = float(row['low_52wk'])
            high_52_week = float(row['high_52wk'])

            if pd.isna(low_52_week) or pd.isna(high_52_week):
                continue
            
            if last_close < 100: # Filter out very low price stocks
                continue

            buy_trigger_price = low_52_week * 1.05
            sell_trigger_price = high_52_week * 0.95

            # Exit Conditions
            if symbol in holdings:
                pos = holdings[symbol]
                hold_days = (date - pos["buy_date"]).days
                if hold_days < 3: # Min T+3 hold
                    continue

                change = (last_close - pos["avg_price"]) / pos["avg_price"] * 100
                exit_reason = None
                
                # Primary exit: 52-week high target
                if last_close >= sell_trigger_price:
                    exit_reason = f"52WK-HIGH({change:+.1f}%)"
                # Secondary exits: Max hold, Stop-loss, Take-profit
                elif hold_days >= max_hold_days:
                    exit_reason = f"MAX-HOLD({hold_days}d)"
                elif change <= -stop_loss_pct:
                    exit_reason = f"STOP-LOSS({change:+.1f}%)"
                elif change >= take_profit_pct:
                    exit_reason = f"TAKE-PROFIT({change:+.1f}%)"
                
                if exit_reason:
                    holdings.pop(symbol)
                    proceeds = pos["qty"] * last_close
                    cash += proceeds
                    pnl = proceeds - pos["qty"] * pos["avg_price"]
                    trades.append({
                        "symbol": symbol, "side": exit_reason, "date": date,
                        "price": last_close, "qty": pos["qty"], "pnl": pnl,
                        "pnl_pct": pnl / (pos["qty"] * pos["avg_price"]) * 100,
                        "hold_days": hold_days
                    })
                    continue

            # BUY Signal
            # Only buy if not already holding, within daily limit, and market is bullish
            if last_close <= buy_trigger_price and symbol not in holdings and buys_today < 2 and market_bullish: # Using a hardcoded max daily buys as the config var is gone
                qty = max(1, int((cash * 0.10) / last_close)) # Size trade at 10% of current capital
                cost = qty * last_close
                if cash >= cost:
                    cash -= cost
                    holdings[symbol] = {"qty": qty, "avg_price": last_close, "buy_date": date}
                    trades.append({
                        "symbol": symbol, "side": "BUY", "date": date,
                        "price": last_close, "qty": qty,
                        "pnl": 0, "pnl_pct": 0, "hold_days": 0
                    })
                    buys_today += 1 # Increment daily buys for this date

    # Close remaining positions at the last available price in the dataset
    last_date = dates[-1]
    for symbol, pos in holdings.items():
        last_price_series = df[df["symbol"] == symbol]["close"]
        if not last_price_series.empty:
            last_price = float(last_price_series.iloc[-1])
            proceeds = pos["qty"] * last_price
            cash += proceeds
            pnl = proceeds - pos["qty"] * pos["avg_price"]
            trades.append({
                "symbol": symbol, "side": "SELL(end)", "date": last_date,
                "price": last_price, "qty": pos["qty"], "pnl": pnl,
                "pnl_pct": pnl / (pos["qty"] * pos["avg_price"]) * 100,
                "hold_days": (last_date - pos["buy_date"]).days
            })

    return cash, trades


# ── Report ─────────────────────────────────────────────────────────────────────

def print_backtest_report(initial_capital, final_capital, trades):
    """Generates a comprehensive backtest report including per-symbol summary."""
    sell_trades = [t for t in trades if t["side"] != "BUY"]
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
        
        # Per-symbol summary
        # Filter for all exit types, including the new 52WK-HIGH
        exit_trades = [t for t in trades if t["side"] != "BUY"] # Changed to include all non-buy as exits
        if exit_trades:
            print("\n  PER-SYMBOL SUMMARY")
            print("  " + "-" * 45)
            sym_groups = {}
            for t in exit_trades:
                sym_groups.setdefault(t["symbol"], []).append(t["pnl_pct"])
            # Filter out symbols with no valid pnl_pct (e.g., from 'SELL(end)' if no buy)
            rows = [(s, np.mean([p for p in pcts if not pd.isna(p)]), len(pcts)) for s, pcts in sym_groups.items() if [p for p in pcts if not pd.isna(p)]]
            rows.sort(key=lambda x: -x[1])
            for sym, avg_pct, count in rows:
                bar = "+" if avg_pct >= 0 else ""
                print(f"  {sym:<8}  avg={bar}{avg_pct:.1f}%  trades={count}")
            overall_avg = np.mean([r[1] for r in rows]) if rows else 0
            print(f"\n  Overall avg profit/symbol: {overall_avg:+.1f}%")


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Backtest NAASA signal strategy")
    parser.add_argument("--strategy",       type=str,   default="mean_reversion",
                        help="Strategy: 'mean_reversion' (52wk H/L)") # Only mean_reversion
    parser.add_argument("--capital",        type=float, default=DEFAULT_CAPITAL)
    parser.add_argument("--qty",            type=int,   default=DEFAULT_BUY_QTY)
    parser.add_argument("--stop-loss",      type=float, default=7.0,  help="Stop-loss %%  (default: 7)")
    parser.add_argument("--take-profit",    type=float, default=15.0, help="Take-profit %% (default: 15)")
    parser.add_argument("--max-hold",       type=int,   default=30,   help="Max hold days (default: 30)")
    parser.add_argument("--market-filter",  action="store_true",      help="Only BUY when NEPSE index EMA9 > EMA21")
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
    df = _adjust_prices(df)

    if args.symbols:
        syms = [s.strip().upper() for s in args.symbols.split(",")]
        df   = df[df["symbol"].isin(syms)]
        print(f"Symbols : {syms}")

    date_min = df["date"].min().date()
    date_max = df["date"].max().date()
    mf = "ON" if args.market_filter else "OFF"
    print(f"Period  : {date_min} → {date_max}")
    print(f"Symbols : {df['symbol'].nunique()}")
    print(f"Strategy: {args.strategy.upper()}  SL=-{args.stop_loss}%  TP=+{args.take_profit}%  "
          f"MaxHold={args.max_hold}d  MarketFilter={mf}  Qty={args.qty}  Capital=NPR {args.capital:,.0f}")
    print("Running backtest (this may take a minute for all symbols)...")

    # Only the mean_reversion strategy remains
    if args.strategy.lower() == "mean_reversion":
        print(f"Mean Reversion params: buy<=52wk_low*1.05, sell>=52wk_high*0.95")
        final_capital, trades = run_mean_reversion_backtest(
            df,
            initial_capital   = args.capital,
            buy_qty           = args.qty,
            stop_loss_pct     = args.stop_loss,
            take_profit_pct   = args.take_profit,
            max_hold_days     = args.max_hold,
            use_market_filter = args.market_filter,
        )
    else: # Fallback for incorrect strategy argument - should not be hit if default is mean_reversion
        print("Error: Only 'mean_reversion' strategy is supported now. Exiting.")
        sys.exit(1)

    print_backtest_report(args.capital, final_capital, trades)


if __name__ == "__main__":
    main()
