import os
import json
import pandas as pd

def _load_swing_targets(path="swing_targets.json"):
    if os.path.exists(path):
        with open(path) as f:
            return json.load(f)
    return {}

def remove_swing_target(symbol, path="swing_targets.json"):
    targets = _load_swing_targets(path)
    if symbol in targets:
        del targets[symbol]
        with open(path, 'w') as f:
            json.dump(targets, f, indent=4)
        print(f"[{symbol}] Removed from swing_targets.json — now eligible for fundamental re-evaluation.")

# --- Data Loading and Preparation Helpers ---

def _adjust_prices(df, actions_file="chukul_corporate_actions.csv"):
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
            if d > bcd: return d
        return None

    adjustments = {}
    for _, row in ca.iterrows():
        sym, bcd, atype, pct = row["symbol"], row["book_close_date"], row["action_type"], float(row["pct"])
        ex_date = next_trading_day(bcd)
        if ex_date is None: continue
        
        factor = 0
        if atype == "bonus":
            factor = 1.0 / (1.0 + pct / 100.0)
        else:
            sym_df = df[df["symbol"] == sym].sort_values("date")
            ex_row = sym_df[sym_df["date"] == ex_date]
            prev_row = sym_df[sym_df["date"] < ex_date].tail(1)
            if ex_row.empty or prev_row.empty: continue
            ratio = float(ex_row["open"].iloc[0]) / float(prev_row["close"].iloc[0])
            if ratio <= 0.92: factor = ratio
        
        if factor > 0:
            if sym not in adjustments: adjustments[sym] = []
            adjustments[sym].append((ex_date, factor))

    if not adjustments: return df
    
    ohlc_cols = [c for c in ["open", "high", "low", "close", "ltp"] if c in df.columns]
    parts = []
    for sym, sym_df in df.groupby("symbol", sort=False):
        sym_df = sym_df.copy()
        if sym in adjustments:
            for ex_date, factor in sorted(adjustments[sym], key=lambda x: x[0]):
                sym_df.loc[sym_df["date"] < ex_date, ohlc_cols] *= factor
        parts.append(sym_df)
    return pd.concat(parts, ignore_index=True)

def _calc_rsi(series, period=14):
    delta    = series.diff()
    gain     = delta.clip(lower=0)
    loss     = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1/period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1/period, adjust=False).mean()
    rs       = avg_gain / avg_loss.where(avg_loss != 0, other=float('nan'))
    return 100 - (100 / (1 + rs))


def _calc_ema(series, period):
    return series.ewm(span=period, adjust=False).mean()


def _calc_adx(high, low, close, period=14):
    """Returns ADX series."""
    prev_high  = high.shift(1)
    prev_low   = low.shift(1)
    prev_close = close.shift(1)
    plus_dm  = (high - prev_high).clip(lower=0).where(
                    (high - prev_high) > (prev_low - low), other=0)
    minus_dm = (prev_low - low).clip(lower=0).where(
                    (prev_low - low) > (high - prev_high), other=0)
    tr = pd.concat([
        high - low,
        (high - prev_close).abs(),
        (low  - prev_close).abs()
    ], axis=1).max(axis=1)
    atr      = tr.ewm(alpha=1/period, adjust=False).mean()
    plus_di  = 100 * plus_dm.ewm(alpha=1/period, adjust=False).mean() / atr.replace(0, float('nan'))
    minus_di = 100 * minus_dm.ewm(alpha=1/period, adjust=False).mean() / atr.replace(0, float('nan'))
    dx       = (100 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, float('nan')))
    adx      = dx.ewm(alpha=1/period, adjust=False).mean()
    return adx


def load_and_prepare_data(ohlcv_file="chukul_data.csv"):
    """Loads OHLCV data, adjusts for corporate actions, and calculates Fortress indicators."""
    print("Loading and preparing data for Fortress strategy...")
    if not os.path.exists(ohlcv_file):
        print(f"Error: {ohlcv_file} not found.")
        return None

    df = pd.read_csv(ohlcv_file)
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])
    if "symbol" not in df.columns and "stock" in df.columns:
        df.rename(columns={"stock": "symbol"}, inplace=True)
    df.sort_values(["symbol", "date"], inplace=True)

    df_adjusted = _adjust_prices(df.copy())

    # Fundamental filter: only trade quality stocks
    # Exception: always include symbols with a swing target set (for exit-only tracking)
    swing_target_syms = set(_load_swing_targets().keys())
    if os.path.exists("chukul_fundamental.csv"):
        fund = pd.read_csv("chukul_fundamental.csv")
        eps_ok = fund[fund["eps"].notna() & (fund["eps"] > 0)]["symbol"]
        roe_ok = fund[fund["roe"].notna() & (fund["roe"] > 5)]["symbol"]
        npl_ok = fund[fund["npl"].isna() | (fund["npl"] < 10)]["symbol"]
        good = set(eps_ok) & set(roe_ok) & set(npl_ok)
        before = df_adjusted["symbol"].nunique()
        df_adjusted = df_adjusted[df_adjusted["symbol"].isin(good | swing_target_syms)]
        print(f"Fundamental filter: {before} → {df_adjusted['symbol'].nunique()} symbols (incl. {len(swing_target_syms)} swing-target exits)")

    print("Calculating Fortress indicators (EMA9, EMA21, ADX, RSI, volume)...")
    # All indicators are shifted by 1 so they are based on the last CLOSED candle only.
    # This means during live market hours, today's partial candle does NOT affect signals —
    # the bot always acts on yesterday's confirmed data.
    df_adjusted['ema9']        = df_adjusted.groupby('symbol')['close'].transform(lambda x: _calc_ema(x, 9).shift(1))
    df_adjusted['ema21']       = df_adjusted.groupby('symbol')['close'].transform(lambda x: _calc_ema(x, 21).shift(1))
    df_adjusted['prev_ema9']   = df_adjusted.groupby('symbol')['ema9'].shift(1)
    df_adjusted['prev_ema21']  = df_adjusted.groupby('symbol')['ema21'].shift(1)
    df_adjusted['rsi']         = df_adjusted.groupby('symbol')['close'].transform(lambda x: _calc_rsi(x, 14).shift(1))
    df_adjusted['vol_avg20']   = df_adjusted.groupby('symbol')['volume'].transform(
                                     lambda x: x.rolling(20, min_periods=5).mean().shift(1))
    df_adjusted['prev_volume'] = df_adjusted.groupby('symbol')['volume'].shift(1)
    df_adjusted['prev_close']  = df_adjusted.groupby('symbol')['close'].shift(1)

    # ADX per symbol — also shifted by 1
    adx_parts = []
    for sym, grp in df_adjusted.groupby('symbol'):
        adx_vals = _calc_adx(grp['high'], grp['low'], grp['close']).shift(1)
        adx_parts.append(pd.Series(adx_vals.values, index=grp.index, name='adx'))
    df_adjusted['adx'] = pd.concat(adx_parts).reindex(df_adjusted.index)

    print("Data preparation complete.")

    # Return last 2 rows per symbol — needed to detect 2-day EMA cross confirmation on exits
    # Keep symbol as a column so generate_signals can group by it
    parts = []
    for sym, grp in df_adjusted.groupby('symbol'):
        top2 = grp.nlargest(2, 'date').copy()
        top2['symbol'] = sym
        parts.append(top2)
    latest2 = pd.concat(parts, ignore_index=True)
    return latest2

# --- Portfolio column helpers ---

_SYMBOL_KEYS = ['Symbol', 'symbol', 'Stock Symbol', 'Script', 'Scrip']
_QTY_KEYS    = ['CDS Total\nBalance', 'NAASA\nBalance', 'Quantity', 'Total Qty', 'Qty', 'Balance Quantity', 'Units', 'Current Balance']
_RATE_KEYS   = ['Average Rate', 'Avg Rate', 'Average Cost', 'Cost Price', 'Close Price\nPrice', 'LTP']

def _get_holding_symbol(h):
    for k in _SYMBOL_KEYS:
        v = h.get(k)
        if v:
            return str(v).strip()
    return None

def _get_holding_qty(h):
    for k in _QTY_KEYS:
        v = h.get(k)
        if v is not None and str(v).strip():
            try:
                return int(float(str(v).replace(',', '')))
            except (ValueError, TypeError):
                pass
    return 0

def _get_holding_rate(h):
    for k in _RATE_KEYS:
        v = h.get(k)
        if v is not None and str(v).strip():
            try:
                return float(str(v).replace(',', ''))
            except (ValueError, TypeError):
                pass
    return None


# Fortress Signal constants (match backtest defaults)
FORTRESS_ADX_MIN      = 25
FORTRESS_RSI_MIN      = 45
FORTRESS_RSI_MAX      = 65
FORTRESS_VOL_FACTOR   = 1.5
FORTRESS_TP_PCT       = 20.0
FORTRESS_SL_PCT       = -10.0
FORTRESS_RSI_OB       = 75
FORTRESS_MIN_HOLD     = 5    # min calendar days before EMA-cross exit allowed
FORTRESS_EMA_CONFIRM  = 2    # consecutive days EMA9 < EMA21 needed to exit
MIN_SELL_QTY          = 10


# --- Core Signal Generation Logic ---

def generate_signals(latest_data, states, portfolio, daily_buy_count, daily_buy_limit,
                     _daily_double_down_count=0, _daily_double_down_limit=1):
    """
    Generates trading signals based on the Fortress Signal strategy.
    BUY  : EMA9 > EMA21 AND ADX > 25 AND RSI in [45,65] AND volume >= 1.5x avg
           AND price > EMA21 AND not a panic day (daily return > -5%)
    SELL : TP at +20% OR SL at -10% OR RSI > 75 OR EMA9 < EMA21 for 2 consecutive days
           (EMA-cross exit only allowed after min 5 days held)
           Special: cut-loss, swing target always apply regardless of hold period.

    Does NOT modify state; state changes are handled by the main loop after successful trades.
    daily_buy_count / daily_buy_limit  — controls INITIAL buys only (double-down removed)
    """
    signals = []
    swing_targets = _load_swing_targets()
    held_symbols = {}
    for h in portfolio.get('holdings', []):
        sym = _get_holding_symbol(h)
        if sym:
            held_symbols[sym] = h

    # Build per-symbol dict: {symbol: (row_today, row_yesterday)} sorted descending by date
    symbol_rows = {}
    for sym, grp in latest_data.groupby('symbol'):
        rows = grp.sort_values('date', ascending=False)
        symbol_rows[sym] = (rows.iloc[0], rows.iloc[1] if len(rows) > 1 else rows.iloc[0])

    for symbol, (row, prev_row) in symbol_rows.items():
        state = states.get(symbol, {})
        is_in_live_portfolio = symbol in held_symbols

        # --- State Reconciliation ---
        if state.get('in_position') and not is_in_live_portfolio and held_symbols:
            print(f"[{symbol}] State conflict: in position by state but not in live portfolio. Resetting.")
            states[symbol] = {}
            state = {}

        if float(row['close']) < 100:
            continue

        # --- Orphan Position: held in portfolio but no bot state ---
        if is_in_live_portfolio and not state.get('in_position') and symbol not in swing_targets:
            holding = held_symbols[symbol]
            avg_rate = _get_holding_rate(holding)
            if avg_rate is None:
                avg_rate = float(row['close'])
                print(f"[{symbol}] Orphan: no average rate, using current price as entry.")
            state = {
                'in_position':    True,
                'entry_date':     (pd.to_datetime('today') - pd.Timedelta(days=10)).strftime('%Y-%m-%d'),
                'entry_price':    avg_rate,
                'initial_entry':  avg_rate,
                'ema_cross_days': 0,
            }
            states[symbol] = state
            print(f"[{symbol}] Orphan position seeded: avg_rate={avg_rate}")

        # --- Read indicators ---
        def _f(r, col, default=0.0):
            v = r.get(col, float('nan')) if hasattr(r, 'get') else getattr(r, col, float('nan'))
            return float(v) if not pd.isna(v) else default

        close      = _f(row, 'close')
        ema9       = _f(row, 'ema9')
        ema21      = _f(row, 'ema21')
        adx        = _f(row, 'adx')
        rsi        = _f(row, 'rsi', 50.0)
        vol_avg20  = _f(row, 'vol_avg20')
        prev_vol   = _f(row, 'prev_volume')
        prev_close = _f(row, 'prev_close', close)

        prev_ema9  = _f(prev_row, 'ema9')
        prev_ema21 = _f(prev_row, 'ema21')

        ema_bullish   = ema9 > ema21
        ema_below_now = ema9 < ema21
        ema_below_prev = prev_ema9 < prev_ema21
        volume_surge  = vol_avg20 > 0 and prev_vol >= vol_avg20 * FORTRESS_VOL_FACTOR
        daily_return  = (close - prev_close) / prev_close if prev_close > 0 else 0

        # --- Generate BUY Signal ---
        if not state.get('in_position'):
            if symbol in swing_targets:
                continue
            if daily_buy_count >= daily_buy_limit:
                continue

            fortress_buy = (
                ema_bullish                              and
                adx > FORTRESS_ADX_MIN                  and
                FORTRESS_RSI_MIN <= rsi <= FORTRESS_RSI_MAX and
                volume_surge                             and
                close > ema21                            and
                daily_return > -0.05
            )

            if fortress_buy:
                print(f"[{symbol}] *** FORTRESS BUY *** price={close:.2f} EMA9={ema9:.2f} EMA21={ema21:.2f} ADX={adx:.1f} RSI={rsi:.1f}")
                signals.append({
                    "side": "BUY", "symbol": symbol, "price": close, "type": "INITIAL",
                    "quantity": int(os.getenv("DEFAULT_BUY_QTY", 20)),
                })
                daily_buy_count += 1

        # --- Generate SELL Signals (existing positions) ---
        else:
            days_held      = (pd.to_datetime('today') - pd.to_datetime(state['entry_date'])).days
            profit_pct     = (close - state['entry_price']) / state['entry_price'] * 100
            drop_from_start = (close - state['initial_entry']) / state['initial_entry'] * 100
            current_qty    = _get_holding_qty(held_symbols.get(symbol, {}))

            # Update EMA cross day counter in state (for confirmation logic)
            if ema_below_now and ema_below_prev:
                state['ema_cross_days'] = state.get('ema_cross_days', 0) + 1
            else:
                state['ema_cross_days'] = 0

            # Exit signals only after T+3
            if days_held < 3:
                continue

            _ctx = {"profit_pct": round(profit_pct, 1), "days_held": days_held, "entry_price": state['entry_price']}

            print(f"[{symbol}] Exit check: qty={current_qty}, days={days_held}, profit={profit_pct:.1f}%, "
                  f"ADX={adx:.1f}, RSI={rsi:.1f}, EMA9={ema9:.2f} EMA21={ema21:.2f}")

            # 1. Cut-loss: >25% drop from initial entry after 20+ days (hard override)
            if drop_from_start <= -25 and days_held >= 20:
                if current_qty >= MIN_SELL_QTY:
                    print(f"[{symbol}] *** CUT LOSS *** drop={drop_from_start:.1f}% days={days_held}")
                    signals.append({
                        "side": "SELL", "symbol": symbol, "price": close, "type": "CUT_LOSS",
                        "quantity": current_qty, **_ctx,
                    })
                    continue

            # 2. Swing target (manual resistance level, always applies)
            if symbol in swing_targets and close >= swing_targets[symbol]:
                print(f"[{symbol}] *** SWING TARGET HIT *** price={close} >= target={swing_targets[symbol]}")
                if current_qty >= MIN_SELL_QTY:
                    signals.append({
                        "side": "SELL", "symbol": symbol, "price": close, "type": "SWING_TARGET",
                        "quantity": current_qty, **_ctx,
                    })
                    continue

            # 3. Take profit at +20%
            if profit_pct >= FORTRESS_TP_PCT:
                if current_qty >= MIN_SELL_QTY:
                    print(f"[{symbol}] *** FORTRESS TP *** profit={profit_pct:.1f}%")
                    signals.append({
                        "side": "SELL", "symbol": symbol, "price": close, "type": "FULL_EXIT",
                        "quantity": current_qty, **_ctx,
                    })
                    continue

            # 4. Stop loss at -10%
            if profit_pct <= FORTRESS_SL_PCT:
                if current_qty >= MIN_SELL_QTY:
                    print(f"[{symbol}] *** FORTRESS SL *** profit={profit_pct:.1f}%")
                    signals.append({
                        "side": "SELL", "symbol": symbol, "price": close, "type": "FULL_EXIT",
                        "quantity": current_qty, **_ctx,
                    })
                    continue

            # 5. RSI overbought
            if rsi > FORTRESS_RSI_OB:
                if current_qty >= MIN_SELL_QTY:
                    print(f"[{symbol}] *** FORTRESS RSI OB *** rsi={rsi:.1f}")
                    signals.append({
                        "side": "SELL", "symbol": symbol, "price": close, "type": "RSI_OB",
                        "quantity": current_qty, **_ctx,
                    })
                    continue

            # 6. EMA cross exit — only after min_hold_days, confirmed for 2 consecutive days
            if (days_held >= FORTRESS_MIN_HOLD and
                    state.get('ema_cross_days', 0) >= FORTRESS_EMA_CONFIRM):
                if current_qty >= MIN_SELL_QTY:
                    print(f"[{symbol}] *** FORTRESS EMA CROSS EXIT *** EMA9={ema9:.2f} < EMA21={ema21:.2f} for {state['ema_cross_days']}d")
                    signals.append({
                        "side": "SELL", "symbol": symbol, "price": close, "type": "FULL_EXIT",
                        "quantity": current_qty, **_ctx,
                    })

    return signals

