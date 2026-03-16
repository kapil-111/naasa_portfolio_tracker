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
try:
    import plotly.graph_objects as go
    PLOTLY_AVAILABLE = True
except ImportError:
    PLOTLY_AVAILABLE = False


# ── Default config ─────────────────────────────────────────────────────────────
DEFAULT_CAPITAL        = 100_000
DEFAULT_BUY_QTY        = 20


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
                                take_profit_pct,
                                use_market_filter):
    """
    Mean Reversion Strategy
    -----------------------
    BUY      : Price within 5% of 52-week low. Max 2 stocks/day.
               Re-entry allowed only if price is 20% below last sell price.
    DOUBLE   : If price drops 10% from entry, buy 2x original qty (once per cycle).
    HALF-SELL: Sell 50% of position at 10% profit (T+3 min).
    FINAL-SELL: Sell remaining 50% at 20% profit OR price within 5% of 52-week high.
    """
    dates = sorted(df["date"].unique())
    cash = float(initial_capital)
    holdings = {}       # symbol -> position dict
    last_sell_prices = {}  # symbol -> last final sell price (for re-entry gate)
    trades = []
    nepse_trend = _load_nepse_index() if use_market_filter else {}

    print("Pre-calculating 52-week highs and lows...")
    df['low_52wk']  = df.groupby('symbol')['low'].transform(
        lambda x: x.rolling(252, min_periods=126).min())
    df['high_52wk'] = df.groupby('symbol')['high'].transform(
        lambda x: x.rolling(252, min_periods=126).max())
    df['vol_avg20'] = df.groupby('symbol')['volume'].transform(
        lambda x: x.rolling(20, min_periods=5).mean())
    df['rsi']        = df.groupby('symbol')['close'].transform(lambda x: calc_rsi(x, 14))
    df['prev_close'] = df.groupby('symbol')['close'].shift(1)
    print("Pre-calculation complete.")

    for i, date in enumerate(dates):
        if i < 126:
            continue

        market_bullish = nepse_trend.get(date, True) if use_market_filter else True
        daily_df = df[df["date"] == date].copy()
        for _, row in daily_df.iterrows():
            symbol      = row['symbol']
            last_close  = float(row['close'])
            low_52wk    = float(row['low_52wk'])
            high_52wk   = float(row['high_52wk'])
            volume      = float(row['volume'])
            vol_avg20   = float(row['vol_avg20']) if not pd.isna(row['vol_avg20']) else 0

            if pd.isna(low_52wk) or pd.isna(high_52wk):
                continue
            if last_close < 100:
                continue

            volume_spike = vol_avg20 > 0 and volume >= vol_avg20 * 1.5
            buy_trigger  = low_52wk  * 1.05
            near_52wk_hi = last_close >= high_52wk * 0.95
            rsi          = float(row['rsi'])        if not pd.isna(row['rsi'])        else 50
            prev_close   = float(row['prev_close']) if not pd.isna(row['prev_close']) else last_close

            # ── Manage existing position ──────────────────────────────────────
            if symbol in holdings:
                pos       = holdings[symbol]
                hold_days = (date - pos["buy_date"]).days
                change    = (last_close - pos["avg_price"]) / pos["avg_price"] * 100
                entry_chg = (last_close - pos["entry_price"]) / pos["entry_price"] * 100

                # Double-buy: price dropped 10% from entry, only once, no T+3 needed
                if not pos["doubled"] and entry_chg <= -10:
                    double_qty = 2 * pos["initial_qty"]
                    cost = double_qty * last_close
                    if cash >= cost:
                        cash -= cost
                        new_qty     = pos["qty"] + double_qty
                        pos["avg_price"] = (pos["qty"] * pos["avg_price"] + double_qty * last_close) / new_qty
                        pos["qty"]    = new_qty
                        pos["doubled"] = True
                        trades.append({
                            "symbol": symbol, "side": "DOUBLE-BUY", "date": date,
                            "price": last_close, "qty": double_qty,
                            "pnl": 0, "pnl_pct": 0, "hold_days": hold_days
                        })
                    continue  # no exits on double-buy day

                # T+3 gate for all exits
                if hold_days < 3:
                    continue

                # RSI overbought + price not rising → exit all
                if rsi > 80 and last_close <= prev_close:
                    proceeds = pos["qty"] * last_close
                    cash    += proceeds
                    pnl      = proceeds - pos["qty"] * pos["avg_price"]
                    last_sell_prices[symbol] = last_close
                    holdings.pop(symbol)
                    trades.append({
                        "symbol": symbol, "side": "RSI-OB(>80)", "date": date,
                        "price": last_close, "qty": pos["qty"], "pnl": pnl,
                        "pnl_pct": pnl / (pos["qty"] * pos["avg_price"]) * 100,
                        "hold_days": hold_days
                    })
                    continue

                # Half-sell at 10% profit
                if not pos["half_sold"] and change >= 10:
                    half_qty = pos["qty"] // 2
                    if half_qty > 0:
                        proceeds = half_qty * last_close
                        cash    += proceeds
                        pnl      = proceeds - half_qty * pos["avg_price"]
                        pos["qty"]       -= half_qty
                        pos["half_sold"]  = True
                        trades.append({
                            "symbol": symbol, "side": "HALF-SELL(10%)", "date": date,
                            "price": last_close, "qty": half_qty, "pnl": pnl,
                            "pnl_pct": pnl / (half_qty * pos["avg_price"]) * 100,
                            "hold_days": hold_days
                        })
                    # fall through to check final sell on same day

                # Final sell: 20% profit OR near 52-week high (after half sold)
                if pos["half_sold"] and (change >= take_profit_pct or near_52wk_hi):
                    reason   = "52WK-HIGH" if near_52wk_hi else f"FINAL-SELL({change:+.1f}%)"
                    proceeds = pos["qty"] * last_close
                    cash    += proceeds
                    pnl      = proceeds - pos["qty"] * pos["avg_price"]
                    last_sell_prices[symbol] = last_close
                    holdings.pop(symbol)
                    trades.append({
                        "symbol": symbol, "side": reason, "date": date,
                        "price": last_close, "qty": pos["qty"], "pnl": pnl,
                        "pnl_pct": pnl / (pos["qty"] * pos["avg_price"]) * 100,
                        "hold_days": hold_days
                    })

                continue  # done with this symbol for today

            # ── BUY signal ────────────────────────────────────────────────────
            if last_close <= buy_trigger and market_bullish and volume_spike:
                # Re-entry gate: price must be 20% below last sell price
                if symbol in last_sell_prices and last_close > last_sell_prices[symbol] * 0.80:
                    continue

                qty  = buy_qty
                cost = qty * last_close
                if cash >= cost:
                    cash -= cost
                    holdings[symbol] = {
                        "qty": qty, "initial_qty": qty,
                        "avg_price": last_close, "entry_price": last_close,
                        "buy_date": date, "doubled": False, "half_sold": False
                    }
                    trades.append({
                        "symbol": symbol, "side": "BUY", "date": date,
                        "price": last_close, "qty": qty,
                        "pnl": 0, "pnl_pct": 0, "hold_days": 0
                    })

    # Close remaining open positions at last available price
    last_date = dates[-1]
    for symbol, pos in holdings.items():
        last_price_series = df[df["symbol"] == symbol]["close"]
        if not last_price_series.empty:
            last_price = float(last_price_series.iloc[-1])
            proceeds   = pos["qty"] * last_price
            cash      += proceeds
            pnl        = proceeds - pos["qty"] * pos["avg_price"]
            trades.append({
                "symbol": symbol, "side": "SELL(end)", "date": last_date,
                "price": last_price, "qty": pos["qty"], "pnl": pnl,
                "pnl_pct": pnl / (pos["qty"] * pos["avg_price"]) * 100,
                "hold_days": (last_date - pos["buy_date"]).days
            })

    return cash, trades


# ── Interactive Plot ───────────────────────────────────────────────────────────

def plot_trades(df, trades):
    if not PLOTLY_AVAILABLE:
        print("plotly not installed. Run: pip install plotly")
        return

    SIDE_CONFIG = {
        "BUY":            ("green",  "triangle-up",   "Buy"),
        "DOUBLE-BUY":     ("blue",   "triangle-up",   "Double Buy"),
        "HALF-SELL(10%)": ("orange", "triangle-down", "Half Sell"),
        "52WK-HIGH":      ("red",    "triangle-down", "Final Sell"),
        "SELL(end)":      ("gray",   "x",             "End Close"),
    }
    # Any FINAL-SELL(...) variant
    def side_key(side):
        if side.startswith("FINAL-SELL"):
            return "52WK-HIGH"
        return side

    traded_symbols = sorted({t["symbol"] for t in trades})
    trades_by_sym  = {s: [t for t in trades if t["symbol"] == s] for s in traded_symbols}

    TRACES_PER_SYM = 1 + 2 + len(SIDE_CONFIG)  # price + low_band + high_band + events
    fig = go.Figure()

    for sym in traded_symbols:
        sym_df  = df[df["symbol"] == sym].sort_values("date")
        visible = (sym == traded_symbols[0])

        fig.add_trace(go.Scatter(
            x=sym_df["date"], y=sym_df["close"],
            name=f"{sym} Price", line=dict(color="lightgray", width=1),
            visible=visible
        ))

        # 52-week low band (buy zone)
        fig.add_trace(go.Scatter(
            x=sym_df["date"], y=sym_df["low_52wk"] * 1.05,
            name="Buy Zone (52wk low +5%)", line=dict(color="green", width=1, dash="dot"),
            visible=visible
        ))

        # 52-week high band (sell zone)
        fig.add_trace(go.Scatter(
            x=sym_df["date"], y=sym_df["high_52wk"] * 0.95,
            name="Sell Zone (52wk high -5%)", line=dict(color="red", width=1, dash="dot"),
            visible=visible
        ))

        sym_trades = trades_by_sym[sym]
        for key, (color, marker_sym, label) in SIDE_CONFIG.items():
            events = [(t["date"], t["price"]) for t in sym_trades if side_key(t["side"]) == key]
            if events:
                dates, prices = zip(*events)
            else:
                dates, prices = [], []
            fig.add_trace(go.Scatter(
                x=list(dates), y=list(prices),
                mode="markers", name=label,
                marker=dict(color=color, size=12, symbol=marker_sym),
                visible=visible
            ))

    buttons = []
    for i, sym in enumerate(traded_symbols):
        vis = [False] * (len(traded_symbols) * TRACES_PER_SYM)
        for j in range(TRACES_PER_SYM):
            idx = i * TRACES_PER_SYM + j
            if idx < len(fig.data):
                vis[idx] = True
        buttons.append(dict(label=sym, method="update",
                            args=[{"visible": vis}, {"title": f"Trades — {sym}"}]))

    fig.update_layout(
        updatemenus=[dict(active=0, buttons=buttons, x=0.01, y=1.12, xanchor="left")],
        title=f"Trades — {traded_symbols[0]}",
        xaxis_title="Date", yaxis_title="Price (NPR)",
        template="plotly_white", height=600
    )
    fig.show()


# ── Report ─────────────────────────────────────────────────────────────────────

def print_backtest_report(initial_capital, final_capital, trades):
    BUY_SIDES      = {"BUY", "DOUBLE-BUY"}
    STRATEGY_SIDES = {"HALF-SELL(10%)", "52WK-HIGH", "FINAL-SELL"}  # completed exits
    buy_trades     = [t for t in trades if t["side"] in BUY_SIDES]
    double_buys    = [t for t in trades if t["side"] == "DOUBLE-BUY"]
    strategy_exits = [t for t in trades if t["side"] not in BUY_SIDES and t["side"] != "SELL(end)"]
    open_ends      = [t for t in trades if t["side"] == "SELL(end)"]
    total_return     = final_capital - initial_capital
    total_return_pct = total_return / initial_capital * 100

    print("\n" + "=" * 60)
    print("  BACKTEST RESULTS")
    print("=" * 60)
    print(f"  Initial Capital      : NPR {initial_capital:>12,.2f}")
    print(f"  Final Capital        : NPR {final_capital:>12,.2f}")
    print(f"  Total Return         : NPR {total_return:>+12,.2f}  ({total_return_pct:+.1f}%)")
    print("-" * 60)
    print(f"  BUY signals          : {len(buy_trades)} ({len(double_buys)} double-buys)")
    print(f"  Strategy exits       : {len(strategy_exits)}  (completed cycles)")
    print(f"  Open at end (SELL(end)): {len(open_ends)}  (position not yet closed by strategy)")

    # ── Completed cycles only ──────────────────────────────────────
    if strategy_exits:
        pnls     = [t["pnl"] for t in strategy_exits]
        winners  = [p for p in pnls if p > 0]
        losers   = [p for p in pnls if p <= 0]
        win_rate = len(winners) / len(strategy_exits) * 100
        avg_hold = np.mean([t["hold_days"] for t in strategy_exits])
        best     = max(strategy_exits, key=lambda t: t["pnl_pct"])
        worst    = min(strategy_exits, key=lambda t: t["pnl_pct"])
        completed_pnl = sum(pnls)

        print(f"\n  ── COMPLETED CYCLES ──────────────────────────────────")
        print(f"  Total P&L            : NPR {completed_pnl:>+,.2f}")
        print(f"  Win Rate             : {win_rate:.1f}%")
        print(f"  Avg Win              : NPR {np.mean(winners):>+,.2f}" if winners else "  Avg Win             : —")
        print(f"  Avg Loss             : NPR {np.mean(losers):>+,.2f}"  if losers  else "  Avg Loss            : —")
        print(f"  Avg Hold Period      : {avg_hold:.1f} days")
        print(f"  Best Exit            : {best['symbol']} {best['pnl_pct']:+.1f}%")
        print(f"  Worst Exit           : {worst['symbol']} {worst['pnl_pct']:+.1f}%")

    # ── Open positions summary ─────────────────────────────────────
    if open_ends:
        open_pnl     = sum(t["pnl"] for t in open_ends)
        open_winners = [t for t in open_ends if t["pnl"] > 0]
        open_losers  = [t for t in open_ends if t["pnl"] <= 0]
        print(f"\n  ── OPEN POSITIONS (still holding, not strategy exits) ──")
        print(f"  Count                : {len(open_ends)}")
        print(f"  Unrealized P&L       : NPR {open_pnl:>+,.2f}")
        print(f"  In profit            : {len(open_winners)}  |  In loss: {len(open_losers)}")

    print("=" * 60)

    # ── Completed exits trade log ──────────────────────────────────
    if strategy_exits:
        print(f"\n  COMPLETED EXITS")
        print(f"  {'Date':<12} {'Symbol':<8} {'Side':<18} {'Qty':>4} {'Price':>8} {'P&L':>10} {'%':>7} {'Days':>5}")
        print("  " + "-" * 75)
        for t in sorted(strategy_exits, key=lambda x: x["date"]):
            print(f"  {str(t['date'])[:10]:<12} {t['symbol']:<8} {t['side']:<18} "
                  f"{t['qty']:>4} {t['price']:>8.2f} {t['pnl']:>+10.2f} "
                  f"{t['pnl_pct']:>+6.1f}% {t['hold_days']:>5}d")

    # ── Per-symbol summary (completed only) ───────────────────────
    if strategy_exits:
        sym_groups = {}
        for t in strategy_exits:
            sym_groups.setdefault(t["symbol"], []).append(t["pnl_pct"])
        rows = [(s, np.mean(pcts), len(pcts)) for s, pcts in sym_groups.items()]
        rows.sort(key=lambda x: -x[1])
        print(f"\n  PER-SYMBOL (completed cycles only)")
        print("  " + "-" * 45)
        for sym, avg_pct, count in rows:
            print(f"  {sym:<8}  avg={avg_pct:+.1f}%  trades={count}")
        print(f"\n  Avg return per completed symbol: {np.mean([r[1] for r in rows]):+.1f}%")


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Backtest NAASA signal strategy")
    parser.add_argument("--strategy",       type=str,   default="mean_reversion",
                        help="Strategy: 'mean_reversion' (52wk H/L)") # Only mean_reversion
    parser.add_argument("--capital",        type=float, default=DEFAULT_CAPITAL)
    parser.add_argument("--qty",            type=int,   default=DEFAULT_BUY_QTY)
    parser.add_argument("--take-profit",    type=float, default=20.0, help="Final sell profit %% (default: 20)")
    parser.add_argument("--market-filter",  action="store_true",      help="Only BUY when NEPSE index EMA9 > EMA21")
    parser.add_argument("--symbols",        type=str,   default=None,
                        help="Comma-separated symbols to test (default: all)")
    parser.add_argument("--plot",           action="store_true",
                        help="Show interactive trade chart after backtest")

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

    if os.path.exists("chukul_fundamental.csv"):
        fund = pd.read_csv("chukul_fundamental.csv")
        before = df["symbol"].nunique()
        eps_ok  = fund[fund["eps"].notna()  & (fund["eps"]  > 0)]["symbol"]
        roe_ok  = fund[fund["roe"].notna()  & (fund["roe"]  > 5)]["symbol"]
        npl_ok  = fund[fund["npl"].isna()   | (fund["npl"]  < 10)]["symbol"]
        good = set(eps_ok) & set(roe_ok) & set(npl_ok)
        df = df[df["symbol"].isin(good)]
        print(f"Fundamental filter: {before} → {df['symbol'].nunique()} symbols "
              f"(EPS>0, ROE>5%, NPL<10%)")

    if args.symbols:
        syms = [s.strip().upper() for s in args.symbols.split(",")]
        df   = df[df["symbol"].isin(syms)]
        print(f"Symbols : {syms}")

    date_min = df["date"].min().date()
    date_max = df["date"].max().date()
    mf = "ON" if args.market_filter else "OFF"
    print(f"Period  : {date_min} → {date_max}")
    print(f"Symbols : {df['symbol'].nunique()}")
    print(f"Strategy: {args.strategy.upper()}  TP=+{args.take_profit}%  "
          f"MarketFilter={mf}  Qty={args.qty}  Capital=NPR {args.capital:,.0f}")
    print("Running backtest (this may take a minute for all symbols)...")

    # Only the mean_reversion strategy remains
    if args.strategy.lower() == "mean_reversion":
        print(f"Mean Reversion params: buy<=52wk_low*1.05, sell>=52wk_high*0.95")
        final_capital, trades = run_mean_reversion_backtest(
            df,
            initial_capital   = args.capital,
            buy_qty           = args.qty,
            take_profit_pct   = args.take_profit,
            use_market_filter = args.market_filter,
        )
    else: # Fallback for incorrect strategy argument - should not be hit if default is mean_reversion
        print("Error: Only 'mean_reversion' strategy is supported now. Exiting.")
        sys.exit(1)

    print_backtest_report(args.capital, final_capital, trades)

    if args.plot:
        plot_trades(df, trades)


if __name__ == "__main__":
    main()
