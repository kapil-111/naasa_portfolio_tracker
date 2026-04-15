"""
Fortress Signal Backtest
========================
Uses the same indicator logic as signals_mr.py to simulate buy/sell decisions
historically. Runs on chukul_data.csv (same file the live bot uses).

Usage:
    python backtest.py                    # all symbols
    python backtest.py NABIL HDL DDBL    # specific symbols
    python backtest.py --from 2024-01-01 # from a specific date
"""

import os
import sys
import json
import argparse
import pandas as pd

# ── Import the exact same helpers used in live trading ──────────────────────
from signals_mr import (
    _adjust_prices, _calc_rsi, _calc_ema, _calc_adx, _load_swing_targets,
    FORTRESS_ADX_MIN,
    FORTRESS_TP_PCT, FORTRESS_SL_PCT, FORTRESS_RSI_OB,
    FORTRESS_MIN_HOLD, FORTRESS_EMA_CONFIRM, MIN_SELL_QTY,
)


def build_sector_regime_series(ohlcv_df, fundamental_file="chukul_fundamental.csv"):
    """
    Builds a per-(sector_id, date) regime lookup for backtesting.
    Returns dict: {sector_id: pd.Series(index=date, values='BULL'|'BEAR'|'UNKNOWN')}
    Uses EMA50 of median close across sector members — same logic as get_sector_regimes().
    """
    if not os.path.exists(fundamental_file):
        return {}
    try:
        fund = pd.read_csv(fundamental_file)[["symbol", "sector_id"]].dropna()
        fund["sector_id"] = fund["sector_id"].astype(int)
        df = ohlcv_df.merge(fund, on="symbol", how="inner")

        sector_series = {}
        for sid, grp in df.groupby("sector_id"):
            daily = grp.groupby("date")["close"].median().reset_index().sort_values("date")
            if len(daily) < 50:
                continue
            daily["ema50"] = _calc_ema(daily["close"], 50)
            daily["regime"] = daily.apply(
                lambda r: "BULL" if r["close"] > r["ema50"] else "BEAR", axis=1
            )
            sector_series[sid] = daily.set_index("date")["regime"]
        return sector_series
    except Exception as e:
        print(f"[SECTOR REGIME] Could not build sector regime series: {e}")
        return {}

DEFAULT_BUY_QTY   = 20
INITIAL_CAPITAL   = 100_000   # NPR — per symbol, for P&L sizing
MIN_HISTORY       = 50        # need enough bars for warm-up


def load_data(ohlcv_file="merged_data.csv", symbols=None, from_date=None):
    if not os.path.exists(ohlcv_file):
        print(f"Error: {ohlcv_file} not found.")
        sys.exit(1)

    df = pd.read_csv(ohlcv_file)
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], format="mixed").dt.normalize()
    if "symbol" not in df.columns and "stock" in df.columns:
        df.rename(columns={"stock": "symbol"}, inplace=True)
    df.sort_values(["symbol", "date"], inplace=True)

    df = _adjust_prices(df.copy())

    # Fundamental filter: only trade quality stocks
    BLACKLIST = {"NIBSF2", "NEPSE"}
    swing_target_syms = set(_load_swing_targets().keys()) - BLACKLIST
    if os.path.exists("chukul_fundamental.csv"):
        fund = pd.read_csv("chukul_fundamental.csv")
        eps_ok = fund[fund["eps"].notna() & (fund["eps"] > 0)]["symbol"]
        roe_ok = fund[fund["roe"].notna() & (fund["roe"] > 5)]["symbol"]
        npl_ok = fund[fund["npl"].isna() | (fund["npl"] < 10)]["symbol"]
        good = set(eps_ok) & set(roe_ok) & set(npl_ok) - BLACKLIST
        before = df["symbol"].nunique()
        df = df[df["symbol"].isin(good | swing_target_syms)]
        print(f"Fundamental filter: {before} → {df['symbol'].nunique()} symbols (incl. {len(swing_target_syms)} swing-target exits)")

    # Build sector regime series BEFORE filtering to from_date (needs full history for EMA50)
    sector_regime_series = build_sector_regime_series(df)
    if sector_regime_series:
        print(f"[SECTOR REGIME] Built regime series for {len(sector_regime_series)} sectors.")

    if symbols:
        df = df[df["symbol"].isin(symbols)]

    # Calculate indicators per symbol (no shift here — we iterate day-by-day below)
    print("Calculating indicators...")
    df["ema9"]      = df.groupby("symbol")["close"].transform(lambda x: _calc_ema(x, 9))
    df["ema21"]     = df.groupby("symbol")["close"].transform(lambda x: _calc_ema(x, 21))
    df["ema50"]     = df.groupby("symbol")["close"].transform(lambda x: _calc_ema(x, 50))
    df["rsi"]       = df.groupby("symbol")["close"].transform(lambda x: _calc_rsi(x, 14))
    df["vol_avg20"] = df.groupby("symbol")["volume"].transform(
                          lambda x: x.rolling(20, min_periods=5).mean())

    adx_parts = []
    for sym, grp in df.groupby("symbol"):
        adx_vals = _calc_adx(grp["high"], grp["low"], grp["close"])
        adx_parts.append(pd.Series(adx_vals.values, index=grp.index, name="adx"))
    df["adx"] = pd.concat(adx_parts).reindex(df.index)

    # ── Big-movers indicators (from sensitivity analysis) ──
    # body_pct: candle body size as % of open (large = strong directional push)
    df["body_pct"]  = (df["close"] - df["open"]).abs() / df["open"] * 100
    # bb_width: Bollinger Band width % (wide = volatile, explosive)
    for sym, grp in df.groupby("symbol"):
        mid   = grp["close"].rolling(20, min_periods=5).mean()
        sd    = grp["close"].rolling(20, min_periods=5).std()
        df.loc[grp.index, "bb_width"] = (4 * sd / mid * 100)  # (upper-lower)/mid * 100
    # range_10d: 10-day high-low range as % of close (stock already in motion)
    df["range_10d"] = (
        df.groupby("symbol")["high"].transform(lambda x: x.rolling(10, min_periods=5).max()) -
        df.groupby("symbol")["low"].transform(lambda x: x.rolling(10, min_periods=5).min())
    ) / df["close"] * 100
    # consec_up: consecutive up-close days (stock in uptrend)
    def _consec_up(series):
        result, count, prev = [], 0, None
        for v in series:
            count = (count + 1) if (prev is not None and v > prev) else 0
            result.append(count)
            prev = v
        return result
    for sym, grp in df.groupby("symbol"):
        df.loc[grp.index, "consec_up"] = _consec_up(grp["close"].values)

    if from_date:
        df = df[df["date"] >= pd.to_datetime(from_date)]

    return df, sector_regime_series


def backtest_symbol(sym_df, symbol, sector_id=None, sector_regime_series=None):
    """
    Simulate Fortress signals on a single symbol's daily OHLCV+indicators.
    Uses yesterday's confirmed candle (shift-by-1) — same as live bot.
    sector_id — int sector_id for this symbol (None = no sector filter)
    sector_regime_series — dict {sector_id: pd.Series(date → 'BULL'|'BEAR')}
    Returns list of trade dicts.
    """
    sym_df = sym_df.sort_values("date").reset_index(drop=True)
    if len(sym_df) < MIN_HISTORY:
        return []

    sector_series = (sector_regime_series or {}).get(sector_id) if sector_id is not None else None

    trades         = []
    position       = None   # None or dict with entry info
    ema_cross_days = 0
    last_exit_date = None   # Track T+3 + 10-day cooldown (same as live bot)
    pending_signal = None   # {signal_date, signal_bar_idx} — 2-day confirmation wait

    BUY_DELAY_DAYS = 3      # wait N trading days after signal before entering

    for i in range(1, len(sym_df)):   # start at 1 so we always have a previous row
        today     = sym_df.iloc[i]
        yesterday = sym_df.iloc[i - 1]

        # Use yesterday's confirmed candle — same as live bot's shift(1)
        close      = float(today["close"])
        ema9       = float(yesterday["ema9"])
        ema21      = float(yesterday["ema21"])
        adx        = float(yesterday["adx"])      if not pd.isna(yesterday["adx"])      else 0.0
        rsi        = float(yesterday["rsi"])      if not pd.isna(yesterday["rsi"])      else 50.0
        prev_close = float(yesterday["close"])
        body_pct   = float(yesterday["body_pct"])  if not pd.isna(yesterday["body_pct"])  else 0.0
        bb_width   = float(yesterday["bb_width"])  if not pd.isna(yesterday["bb_width"])  else 0.0
        range_10d  = float(yesterday["range_10d"]) if not pd.isna(yesterday["range_10d"]) else 0.0
        consec_up  = int(yesterday["consec_up"])   if not pd.isna(yesterday["consec_up"])  else 0
        date       = today["date"]

        # Need two consecutive days of EMA data
        if i >= 2:
            day_before = sym_df.iloc[i - 2]
            prev_ema9  = float(day_before["ema9"])
            prev_ema21 = float(day_before["ema21"])
        else:
            prev_ema9 = prev_ema21 = float("nan")

        if pd.isna(ema9) or pd.isna(ema21) or close < 100:
            pending_signal = None  # cancel pending if data is bad
            continue

        ema_below_now  = ema9 < ema21
        ema_below_prev = (not pd.isna(prev_ema9)) and prev_ema9 < prev_ema21
        daily_return   = (close - prev_close) / prev_close if prev_close > 0 else 0

        if position is None:
            # ── 10-day re-entry cooldown after exit (same as live bot) ──
            if last_exit_date is not None and (date - last_exit_date).days <= 10:
                pending_signal = None
                continue

            # ── Sector regime check ──
            if sector_series is not None:
                past = sector_series[sector_series.index <= date]
                sector_regime = past.iloc[-1] if not past.empty else "UNKNOWN"
                if sector_regime == "BEAR":
                    pending_signal = None
                    continue

            # ── Pending signal: check if N days have passed and setup still holds ──
            if pending_signal is not None:
                days_waited = i - pending_signal["bar_idx"]
                if days_waited >= BUY_DELAY_DAYS:
                    # Re-validate: ADX still strong, stock still in uptrend
                    if adx > FORTRESS_ADX_MIN and consec_up >= 1 and close > ema21:
                        position = {
                            "entry_date":    date,
                            "entry_price":   close,
                            "initial_entry": close,
                            "qty":           DEFAULT_BUY_QTY,
                            "ema_cross_days": 0,
                        }
                        ema_cross_days = 0
                    pending_signal = None
                continue

            # ── BUY check (big-movers derived) ──
            # Top features from sensitivity analysis:
            #   body_pct (large candle body), adx (strong trend),
            #   range_10d (already in motion), bb_width (volatile/explosive),
            #   consec_up (uptrend continuity)
            fortress_buy = (
                adx > FORTRESS_ADX_MIN           and  # strong trend
                body_pct >= 1.5                  and  # meaningful candle body (not doji)
                bb_width >= 8.0                  and  # bands wide — stock is moving
                range_10d >= 5.0                 and  # already in motion over 10 days
                consec_up >= 2                   and  # at least 2 consecutive up days
                ema9 > ema21                     and  # short-term trend up
                close > ema21                    and  # price above trend
                daily_return > -0.05
            )
            if fortress_buy:
                pending_signal = {"signal_date": date, "bar_idx": i}

        else:
            # ── SELL check ──
            days_held      = (date - position["entry_date"]).days
            profit_pct     = (close - position["entry_price"]) / position["entry_price"] * 100
            drop_from_start= (close - position["initial_entry"]) / position["initial_entry"] * 100

            # Update EMA cross counter
            if ema_below_now and ema_below_prev:
                position["ema_cross_days"] = position.get("ema_cross_days", 0) + 1
            else:
                position["ema_cross_days"] = 0

            if days_held < 3:
                continue

            exit_reason = None

            if drop_from_start <= -25 and days_held >= 20:
                exit_reason = f"CUT_LOSS ({drop_from_start:.1f}%)"
            elif profit_pct >= FORTRESS_TP_PCT:
                exit_reason = f"TAKE_PROFIT (+{profit_pct:.1f}%)"
            elif profit_pct <= FORTRESS_SL_PCT:
                exit_reason = f"STOP_LOSS ({profit_pct:.1f}%)"
            elif rsi > FORTRESS_RSI_OB:
                exit_reason = f"RSI_OB ({rsi:.1f})"
            elif (days_held >= FORTRESS_MIN_HOLD and
                  position.get("ema_cross_days", 0) >= FORTRESS_EMA_CONFIRM):
                exit_reason = f"EMA_CROSS ({position['ema_cross_days']}d)"

            if exit_reason:
                pnl_pct = profit_pct
                pnl_npr = (close - position["entry_price"]) * position["qty"]
                trades.append({
                    "symbol":      symbol,
                    "entry_date":  position["entry_date"].strftime("%Y-%m-%d"),
                    "exit_date":   date.strftime("%Y-%m-%d"),
                    "days_held":   days_held,
                    "entry_price": round(position["entry_price"], 2),
                    "exit_price":  round(close, 2),
                    "qty":         position["qty"],
                    "pnl_pct":     round(pnl_pct, 2),
                    "pnl_npr":     round(pnl_npr, 2),
                    "exit_reason": exit_reason,
                })
                position = None
                last_exit_date = date
                ema_cross_days = 0

    # Open position at end of data
    if position is not None:
        last  = sym_df.iloc[-1]
        close = float(last["close"])
        pnl_pct = (close - position["entry_price"]) / position["entry_price"] * 100
        pnl_npr = (close - position["entry_price"]) * position["qty"]
        trades.append({
            "symbol":      symbol,
            "entry_date":  position["entry_date"].strftime("%Y-%m-%d"),
            "exit_date":   "OPEN",
            "days_held":   (last["date"] - position["entry_date"]).days,
            "entry_price": round(position["entry_price"], 2),
            "exit_price":  round(close, 2),
            "qty":         position["qty"],
            "pnl_pct":     round(pnl_pct, 2),
            "pnl_npr":     round(pnl_npr, 2),
            "exit_reason": "STILL_OPEN",
        })

    return trades


def backtest_52w_range(full_df, symbol,
                       proximity_pct=5.0,
                       tp_pct=8.0,
                       sl_pct=-8.0,
                       max_hold_days=30):
    """
    52-Week Range Strategy
    ─────────────────────
    Split: first half of data → compute 52-week high & low anchor levels.
            second half        → trade using those levels.

    BUY  : price comes within proximity_pct% ABOVE the 52w low  (support bounce)
    SELL : price reaches within proximity_pct% BELOW the 52w high (resistance)
           OR stop-loss at sl_pct%
           OR max hold period exceeded (time stop)

    The 52w levels are fixed from the training half — they don't update.
    """
    sym_df = full_df.sort_values("date").reset_index(drop=True)
    if len(sym_df) < MIN_HISTORY * 2:
        return []

    # ── Split ──
    mid = len(sym_df) // 2
    train = sym_df.iloc[:mid]
    test  = sym_df.iloc[mid:].reset_index(drop=True)

    high_52w = float(train["high"].max())
    low_52w  = float(train["low"].min())

    if high_52w <= 0 or low_52w <= 0 or high_52w == low_52w:
        return []

    # Proximity thresholds
    buy_zone_upper  = low_52w  * (1 + proximity_pct / 100)   # within 5% above low  → BUY zone
    sell_zone_lower = high_52w * (1 - proximity_pct / 100)   # within 5% below high → SELL zone

    print(f"  [{symbol}] 52w low={low_52w:.1f} buy≤{buy_zone_upper:.1f} | "
          f"52w high={high_52w:.1f} sell≥{sell_zone_lower:.1f}  "
          f"(test rows={len(test)})")

    trades         = []
    position       = None
    last_exit_date = None

    for i in range(len(test)):
        row   = test.iloc[i]
        close = float(row["close"])
        date  = row["date"]

        if close < 100:
            continue

        if position is None:
            # 10-day cooldown
            if last_exit_date is not None and (date - last_exit_date).days <= 10:
                continue

            # BUY: price in support zone (within proximity% above 52w low)
            if close <= buy_zone_upper:
                position = {
                    "entry_date":  date,
                    "entry_price": close,
                    "qty":         DEFAULT_BUY_QTY,
                }

        else:
            days_held  = (date - position["entry_date"]).days
            profit_pct = (close - position["entry_price"]) / position["entry_price"] * 100

            exit_reason = None

            # SELL: price reached resistance zone (within proximity% below 52w high)
            if close >= sell_zone_lower:
                exit_reason = f"RESISTANCE (52w_high={high_52w:.0f})"
            elif profit_pct >= tp_pct:
                exit_reason = f"TAKE_PROFIT (+{profit_pct:.1f}%)"
            elif profit_pct <= sl_pct:
                exit_reason = f"STOP_LOSS ({profit_pct:.1f}%)"
            elif days_held >= max_hold_days:
                exit_reason = f"TIME_STOP ({days_held}d)"

            if exit_reason:
                pnl_npr = (close - position["entry_price"]) * position["qty"]
                trades.append({
                    "symbol":      symbol,
                    "entry_date":  position["entry_date"].strftime("%Y-%m-%d"),
                    "exit_date":   date.strftime("%Y-%m-%d"),
                    "days_held":   days_held,
                    "entry_price": round(position["entry_price"], 2),
                    "exit_price":  round(close, 2),
                    "qty":         position["qty"],
                    "pnl_pct":     round(profit_pct, 2),
                    "pnl_npr":     round(pnl_npr, 2),
                    "exit_reason": exit_reason,
                })
                position      = None
                last_exit_date = date

    # Open position at end
    if position is not None:
        last  = test.iloc[-1]
        close = float(last["close"])
        profit_pct = (close - position["entry_price"]) / position["entry_price"] * 100
        trades.append({
            "symbol":      symbol,
            "entry_date":  position["entry_date"].strftime("%Y-%m-%d"),
            "exit_date":   "OPEN",
            "days_held":   (last["date"] - position["entry_date"]).days,
            "entry_price": round(position["entry_price"], 2),
            "exit_price":  round(close, 2),
            "qty":         position["qty"],
            "pnl_pct":     round(profit_pct, 2),
            "pnl_npr":     round((close - position["entry_price"]) * position["qty"], 2),
            "exit_reason": "STILL_OPEN",
        })

    return trades


def plot_equity_curve(all_trades, out_file="backtest_equity.html", strategy="fortress", ohlcv_df=None):
    """
    Builds an interactive per-symbol trade chart with a dropdown selector.
    Shows the actual price line plus entry/exit markers for each stock.
    Opens the HTML file in the default browser automatically.
    """
    import plotly.graph_objects as go
    import webbrowser

    trades_df = pd.DataFrame(all_trades)
    closed = trades_df[trades_df["exit_date"] != "OPEN"].copy()
    if closed.empty:
        print("No closed trades to plot.")
        return

    closed["exit_date"] = pd.to_datetime(closed["exit_date"])
    closed["entry_date"] = pd.to_datetime(closed["entry_date"])
    closed = closed.sort_values(["symbol", "entry_date"])

    # Prepare OHLCV lookup
    price_lookup = {}
    if ohlcv_df is not None:
        ohlcv = ohlcv_df.copy()
        ohlcv["date"] = pd.to_datetime(ohlcv["date"])
        for sym, grp in ohlcv.groupby("symbol"):
            price_lookup[sym] = grp.sort_values("date")[["date", "close"]].reset_index(drop=True)

    symbols = sorted(closed["symbol"].unique())

    # 4 traces per symbol: price line, entry markers, exit markers, trade bars
    TRACES_PER_SYM = 4
    traces = []
    buttons = []

    for i, sym in enumerate(symbols):
        sym_trades = closed[closed["symbol"] == sym].reset_index(drop=True)
        total_pnl = sym_trades["pnl_npr"].sum()
        wins = (sym_trades["pnl_pct"] > 0).sum()
        wr = wins / len(sym_trades) * 100

        visible = (i == 0)

        # ── Price line ──────────────────────────────────────────────
        if sym in price_lookup:
            px = price_lookup[sym]
            trace_price = go.Scatter(
                x=px["date"],
                y=px["close"],
                mode="lines",
                name="Close price",
                line=dict(color="royalblue", width=1.5),
                hovertemplate="<b>%{x|%Y-%m-%d}</b><br>Close: NPR %{y:,.2f}<extra></extra>",
                visible=visible,
            )
        else:
            # Fallback: connect entry/exit points
            pts = pd.concat([
                sym_trades[["entry_date", "entry_price"]].rename(columns={"entry_date": "date", "entry_price": "close"}),
                sym_trades[["exit_date", "exit_price"]].rename(columns={"exit_date": "date", "exit_price": "close"}),
            ]).sort_values("date")
            trace_price = go.Scatter(
                x=pts["date"], y=pts["close"],
                mode="lines", name="Price",
                line=dict(color="royalblue", width=1.5),
                visible=visible,
            )

        # ── Entry markers ────────────────────────────────────────────
        trace_entry = go.Scatter(
            x=sym_trades["entry_date"],
            y=sym_trades["entry_price"],
            mode="markers",
            name="Buy",
            marker=dict(symbol="triangle-up", color="blue", size=12, line=dict(width=1, color="darkblue")),
            hovertext=[
                f"<b>BUY {row.entry_date.date()}</b><br>@ NPR {row.entry_price:,.2f}"
                for row in sym_trades.itertuples()
            ],
            hoverinfo="text",
            visible=visible,
        )

        # ── Exit markers ─────────────────────────────────────────────
        exit_colors = ["green" if p > 0 else "red" for p in sym_trades["pnl_pct"]]
        trace_exit = go.Scatter(
            x=sym_trades["exit_date"],
            y=sym_trades["exit_price"],
            mode="markers",
            name="Sell",
            marker=dict(symbol="triangle-down", color=exit_colors, size=12, line=dict(width=1, color="black")),
            hovertext=[
                f"<b>SELL {row.exit_date.date()}</b><br>"
                f"@ NPR {row.exit_price:,.2f}<br>"
                f"P&L: {row.pnl_pct:+.1f}%  (NPR {row.pnl_npr:+,.0f})<br>"
                f"Held: {row.days_held}d  |  {row.exit_reason}"
                for row in sym_trades.itertuples()
            ],
            hoverinfo="text",
            visible=visible,
        )

        # ── Trade range bars (entry→exit shaded region) ──────────────
        bar_colors = ["rgba(0,180,0,0.2)" if p > 0 else "rgba(220,0,0,0.2)" for p in sym_trades["pnl_pct"]]
        trace_bars = go.Bar(
            x=sym_trades["entry_date"],
            y=sym_trades["exit_price"] - sym_trades["entry_price"],
            base=sym_trades["entry_price"],
            name="Trade range",
            marker_color=bar_colors,
            marker_line_width=0,
            width=[max((row.exit_date - row.entry_date).days, 1) * 86400000 * 0.8
                   for row in sym_trades.itertuples()],
            hoverinfo="skip",
            visible=visible,
        )

        traces.extend([trace_price, trace_entry, trace_exit, trace_bars])

        # Visibility mask
        n_traces = len(symbols) * TRACES_PER_SYM
        vis = [False] * n_traces
        base = i * TRACES_PER_SYM
        for k in range(TRACES_PER_SYM):
            vis[base + k] = True

        buttons.append(dict(
            label=sym,
            method="update",
            args=[
                {"visible": vis},
                {"title": f"{sym} — {len(sym_trades)} trades  |  Win rate {wr:.0f}%  |  Total P&L NPR {total_pnl:+,.0f}"},
            ],
        ))

    fig = go.Figure(data=traces)

    first_sym = symbols[0]
    first_trades = closed[closed["symbol"] == first_sym]
    total_pnl0 = first_trades["pnl_npr"].sum()
    wins0 = (first_trades["pnl_pct"] > 0).sum()
    wr0 = wins0 / len(first_trades) * 100

    fig.update_layout(
        title=dict(
            text=f"{first_sym} — {len(first_trades)} trades  |  Win rate {wr0:.0f}%  |  Total P&L NPR {total_pnl0:+,.0f}",
            font=dict(size=15),
        ),
        updatemenus=[dict(
            buttons=buttons,
            direction="down",
            x=0.0,
            xanchor="left",
            y=1.13,
            yanchor="top",
            showactive=True,
            bgcolor="white",
            bordercolor="#aaa",
            font=dict(size=13),
        )],
        xaxis=dict(title="Date", showgrid=True, gridcolor="rgba(200,200,200,0.3)"),
        yaxis=dict(title="Price (NPR)", showgrid=True, gridcolor="rgba(200,200,200,0.3)"),
        barmode="overlay",
        hovermode="x unified",
        plot_bgcolor="white",
        paper_bgcolor="white",
        height=600,
        legend=dict(x=0.01, y=0.01),
        margin=dict(t=100),
    )

    fig.write_html(out_file)
    print(f"\n[CHART] Per-symbol trade chart saved → {out_file}")
    webbrowser.open(f"file://{os.path.abspath(out_file)}")


def print_results(all_trades):
    if not all_trades:
        print("\nNo trades generated.")
        return

    df = pd.DataFrame(all_trades)
    closed = df[df["exit_date"] != "OPEN"]
    open_  = df[df["exit_date"] == "OPEN"]

    total_trades = len(closed)
    wins  = closed[closed["pnl_pct"] > 0]
    losses= closed[closed["pnl_pct"] <= 0]
    win_rate = len(wins) / total_trades * 100 if total_trades else 0
    avg_win  = wins["pnl_pct"].mean()   if len(wins)   else 0
    avg_loss = losses["pnl_pct"].mean() if len(losses) else 0
    total_pnl= closed["pnl_npr"].sum()

    print("\n" + "═" * 70)
    print("  FORTRESS BACKTEST RESULTS")
    print("═" * 70)
    print(f"  Closed trades : {total_trades}")
    print(f"  Win rate      : {win_rate:.1f}%  ({len(wins)}W / {len(losses)}L)")
    print(f"  Avg win       : {avg_win:+.1f}%")
    print(f"  Avg loss      : {avg_loss:+.1f}%")
    print(f"  Total P&L     : NPR {total_pnl:+,.0f}  (at {DEFAULT_BUY_QTY} shares/trade)")
    print(f"  Open positions: {len(open_)}")
    print("═" * 70)

    # Exit reason breakdown
    if total_trades:
        reason_counts = closed["exit_reason"].str.split("(").str[0].str.strip().value_counts()
        print("\n  Exit reasons:")
        for reason, cnt in reason_counts.items():
            sub = closed[closed["exit_reason"].str.startswith(reason)]
            print(f"    {reason:<20} {cnt:>3} trades   avg {sub['pnl_pct'].mean():+.1f}%")

    # Per-symbol summary
    print("\n  Per-symbol closed trades:")
    print(f"  {'Symbol':<10} {'Trades':>6} {'WinRate':>8} {'AvgPnL':>8} {'TotalPnL NPR':>14}")
    print("  " + "-" * 52)
    for sym, grp in closed.groupby("symbol"):
        w = (grp["pnl_pct"] > 0).sum()
        wr = w / len(grp) * 100
        print(f"  {sym:<10} {len(grp):>6} {wr:>7.0f}%  {grp['pnl_pct'].mean():>+7.1f}%  {grp['pnl_npr'].sum():>12,.0f}")

    # All trades detail
    print("\n  All trades:")
    print(f"  {'Symbol':<10} {'Entry':>11} {'Exit':>11} {'Days':>5} {'Entry NPR':>10} {'Exit NPR':>9} {'P&L%':>7}  Reason")
    print("  " + "-" * 80)
    for _, r in df.sort_values(["symbol", "entry_date"]).iterrows():
        tag = "→" if r["exit_date"] == "OPEN" else " "
        print(f"  {r['symbol']:<10} {r['entry_date']:>11} {r['exit_date']:>11} {r['days_held']:>5}"
              f"  {r['entry_price']:>9,.2f}  {r['exit_price']:>8,.2f}  {r['pnl_pct']:>+6.1f}%{tag}  {r['exit_reason']}")

    print("═" * 70)


def main():
    parser = argparse.ArgumentParser(description="Fortress Strategy Backtest")
    parser.add_argument("symbols", nargs="*", help="Symbols to backtest (default: all)")
    parser.add_argument("--from",     dest="from_date", default=None, help="Start date YYYY-MM-DD")
    parser.add_argument("--file",     default="merged_data.csv", help="OHLCV CSV file")
    parser.add_argument("--strategy", default="fortress", choices=["fortress", "52w"],
                        help="Strategy: fortress (default) or 52w (52-week range)")
    parser.add_argument("--proximity", type=float, default=5.0,
                        help="[52w] %% proximity to 52w high/low to trigger (default: 5)")
    parser.add_argument("--tp",        type=float, default=8.0,
                        help="[52w] Take-profit %% (default: 8)")
    parser.add_argument("--sl",        type=float, default=-8.0,
                        help="[52w] Stop-loss %% (default: -8)")
    parser.add_argument("--max-hold",  type=int,   default=30,
                        help="[52w] Max days to hold before time-stop (default: 30)")
    args = parser.parse_args()

    symbols = [s.upper() for s in args.symbols] if args.symbols else None

    print(f"Loading data from {args.file}...")
    df, sector_regime_series = load_data(args.file, symbols=symbols, from_date=args.from_date)

    # Build symbol → sector_id map
    sector_map = {}
    if os.path.exists("chukul_fundamental.csv"):
        try:
            fund = pd.read_csv("chukul_fundamental.csv")[["symbol", "sector_id"]].dropna()
            sector_map = {row["symbol"]: int(row["sector_id"]) for _, row in fund.iterrows()}
        except Exception:
            pass

    syms = df["symbol"].unique()
    print(f"Backtesting {len(syms)} symbol(s) with strategy={args.strategy}...")

    # Load raw data for 52w strategy (needs unadjusted split, no indicator filtering)
    if args.strategy == "52w":
        raw_df = pd.read_csv(args.file)
        raw_df["date"] = pd.to_datetime(raw_df["date"], format="mixed").dt.normalize()
        if "symbol" not in raw_df.columns and "stock" in raw_df.columns:
            raw_df.rename(columns={"stock": "symbol"}, inplace=True)
        raw_df.sort_values(["symbol", "date"], inplace=True)
        if symbols:
            raw_df = raw_df[raw_df["symbol"].isin(symbols)]

    all_trades = []
    for sym in sorted(syms):
        if args.strategy == "52w":
            sym_df = raw_df[raw_df["symbol"] == sym].copy()
            trades = backtest_52w_range(sym_df, sym,
                                        proximity_pct=args.proximity,
                                        tp_pct=args.tp,
                                        sl_pct=args.sl,
                                        max_hold_days=args.max_hold)
        else:
            sym_df = df[df["symbol"] == sym].copy()
            sid = sector_map.get(sym)
            trades = backtest_symbol(sym_df, sym, sector_id=sid, sector_regime_series=sector_regime_series)
        all_trades.extend(trades)
        if trades:
            closed = [t for t in trades if t["exit_date"] != "OPEN"]
            open_  = [t for t in trades if t["exit_date"] == "OPEN"]
            wins   = sum(1 for t in closed if t["pnl_pct"] > 0)
            print(f"  {sym}: {len(closed)} closed trades, {wins} wins"
                  + (f", {len(open_)} open" if open_ else ""))

    print_results(all_trades)
    plot_equity_curve(all_trades, ohlcv_df=df)


if __name__ == "__main__":
    main()
