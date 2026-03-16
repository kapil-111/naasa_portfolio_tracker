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

def load_and_prepare_data(ohlcv_file="chukul_data.csv"):
    """Loads OHLCV data, adjusts for corporate actions, and calculates 52-week boundaries."""
    print("Loading and preparing data for MR strategy...")
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

    print("Calculating 52-week highs/lows and 20-day avg volume...")
    df_adjusted['low52']     = df_adjusted.groupby('symbol')['low'].transform(lambda x: x.rolling(252, min_periods=126).min())
    df_adjusted['high52']    = df_adjusted.groupby('symbol')['high'].transform(lambda x: x.rolling(252, min_periods=126).max())
    df_adjusted['vol_avg20'] = df_adjusted.groupby('symbol')['volume'].transform(lambda x: x.rolling(20, min_periods=5).mean())
    df_adjusted.dropna(subset=['low52', 'high52'], inplace=True)
    print("Data preparation complete.")

    return df_adjusted.loc[df_adjusted.groupby('symbol')['date'].idxmax()].set_index('symbol')

# --- Core Signal Generation Logic ---

def generate_signals(latest_data, states, portfolio, daily_buy_count, daily_buy_limit):
    """
    Generates trading signals based on the Mean Reversion strategy.
    Does NOT modify state; state changes are handled by the main loop after successful trades.
    """
    signals = []
    swing_targets = _load_swing_targets()
    held_symbols = {h.get('Symbol') or h.get('symbol'): h for h in portfolio.get('holdings', [])}

    for symbol, row in latest_data.iterrows():
        state = states.get(symbol, {})
        is_in_live_portfolio = symbol in held_symbols
        
        # --- State Reconciliation ---
        # If state says we are in a position but we don't hold the stock, reset the state.
        if state.get('in_position') and not is_in_live_portfolio:
            print(f"[{symbol}] State conflict: In position by state, but not in live portfolio. Resetting state.")
            states[symbol] = {}
            state = {}

        if row['close'] < 100:
            continue

        # --- Orphan Position: held in portfolio but no bot state ---
        # Auto-seed state so exit logic applies. Entry price from NAASA "Average Rate" column.
        if is_in_live_portfolio and not state.get('in_position') and symbol not in swing_targets:
            holding = held_symbols[symbol]
            avg_rate_str = holding.get('Average Rate', '')
            try:
                avg_rate = float(str(avg_rate_str).replace(',', ''))
            except (ValueError, TypeError):
                avg_rate = float(row['close'])
                print(f"[{symbol}] Orphan position: could not read 'Average Rate', using current price as entry.")
            state = {
                'in_position': True,
                'entry_date':  (pd.to_datetime('today') - pd.Timedelta(days=10)).strftime('%Y-%m-%d'),
                'entry_price': avg_rate,
                'initial_entry': avg_rate,
                'half_sold':   False,
                'position_count': 1,
            }
            states[symbol] = state
            print(f"[{symbol}] Orphan position seeded: avg_rate={avg_rate}")

        # --- Generate BUY Signals ---
        if not state.get('in_position'):
            if symbol in swing_targets:
                continue  # Swing-target stocks are exit-only — never buy
            if daily_buy_count >= daily_buy_limit:
                continue # Skip new buys if daily limit is reached

            vol_avg20     = float(row['vol_avg20']) if 'vol_avg20' in row.index and not pd.isna(row['vol_avg20']) else 0
            volume_spike  = vol_avg20 > 0 and float(row.get('volume', 0)) >= vol_avg20 * 1.5

            buy_signal = row['close'] <= (row['low52'] * 1.05) and volume_spike
            if state.get('last_exit_price', 0) > 0 and row['close'] <= (state['last_exit_price'] * 0.80) and volume_spike:
                buy_signal = True

            if buy_signal:
                print(f"[{symbol}] *** MR BUY (Initial) *** price={row['close']}")
                signals.append({
                    "side": "BUY", "symbol": symbol, "price": row['close'], "type": "INITIAL",
                    "quantity": int(os.getenv("DEFAULT_BUY_QTY"))
                })
        
        # --- Generate Position Management Signals (for existing positions) ---
        else:
            days_held = (pd.to_datetime('today') - pd.to_datetime(state['entry_date'])).days
            profit_pct = (row['close'] - state['entry_price']) / state['entry_price'] * 100
            drop_from_start = (row['close'] - state['initial_entry']) / state['initial_entry'] * 100

            # Double-down BUY signal
            if not state.get('half_sold') and drop_from_start <= -10 and state.get('position_count') == 1:
                if daily_buy_count < daily_buy_limit:
                    print(f"[{symbol}] *** MR BUY (Double Down) *** price={row['close']}")
                    current_qty = int(held_symbols.get(symbol, {}).get('Quantity', 0))
                    signals.append({
                        "side": "BUY", "symbol": symbol, "price": row['close'], "type": "DOUBLE_DOWN",
                        "quantity": current_qty * 2 # Buy 2x the current holding
                    })
                else:
                    print(f"[{symbol}] Skipping double-down buy due to daily limit.")

            # Exit signals (only after T+3)
            if days_held >= 3:
                current_qty = int(held_symbols.get(symbol, {}).get('Quantity', 0))

                # Swing target exit — sell all when price hits manual resistance
                if symbol in swing_targets and row['close'] >= swing_targets[symbol]:
                    print(f"[{symbol}] *** SWING TARGET HIT *** price={row['close']} >= target={swing_targets[symbol]}")
                    signals.append({
                        "side": "SELL", "symbol": symbol, "price": row['close'], "type": "SWING_TARGET",
                        "quantity": current_qty
                    })
                    continue

                # Half-sell signal
                if not state.get('half_sold') and profit_pct >= 10:
                    sell_qty = max(1, current_qty // 2)
                    print(f"[{symbol}] *** MR SELL (Half) *** price={row['close']}")
                    signals.append({
                        "side": "SELL", "symbol": symbol, "price": row['close'], "type": "HALF_SELL",
                        "quantity": sell_qty
                    })

                # Full-sell signal
                if (state.get('half_sold') and profit_pct >= 20) or (row['close'] >= (row['high52'] * 0.95)):
                    print(f"[{symbol}] *** MR SELL (Full) *** price={row['close']}")
                    signals.append({
                        "side": "SELL", "symbol": symbol, "price": row['close'], "type": "FULL_EXIT",
                        "quantity": current_qty
                    })

    return signals

