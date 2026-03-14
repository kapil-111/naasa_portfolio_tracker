"""
signals.py — Multi-Factor Signal Scoring Engine

Generates BUY/SELL signals using a weighted scoring system across:
  1. EMA Crossover    (primary technical trigger)
  2. MACD             (secondary trend confirmation)
  3. Volume           (trade confirmation)
  4. RSI              (momentum zone + hard caps)
  5. Broker Flow      (institutional accumulation/distribution)
  6. Fundamentals     (EPS/P/E quality gate)

Signal threshold: score >= BUY_THRESHOLD → BUY, score <= SELL_THRESHOLD → SELL

Data sources:
  - chukul_data.csv            → OHLCV history
  - chukul_indicators.csv      → RSI-14
  - chukul_broker_buy.csv      → top buyers per symbol
  - chukul_broker_sell.csv     → top sellers per symbol
  - chukul_fundamental.csv     → EPS, P/E
"""

import os
import pandas as pd
import numpy as np


# ─────────────────────────────────────────
# Data Loaders
# ─────────────────────────────────────────

def load_ohlcv(filepath="chukul_data.csv"):
    """Load historical OHLCV data. Returns DataFrame sorted by symbol+date."""
    if not os.path.exists(filepath):
        print(f"Error: {filepath} not found.")
        return None
    try:
        df = pd.read_csv(filepath)
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"])
        if "symbol" not in df.columns and "stock" in df.columns:
            df.rename(columns={"stock": "symbol"}, inplace=True)
        df.sort_values(["symbol", "date"], inplace=True)
        return df
    except Exception as e:
        print(f"Error loading OHLCV: {e}")
        return None


def load_rsi_map(filepath="chukul_indicators.csv"):
    """Returns {symbol: rsi14} from indicators CSV."""
    if not os.path.exists(filepath):
        print(f"Note: {filepath} not found. RSI factor skipped.")
        return {}
    try:
        df = pd.read_csv(filepath)
        if "symbol" in df.columns and "rsi14" in df.columns:
            return dict(zip(df["symbol"], pd.to_numeric(df["rsi14"], errors="coerce")))
    except Exception as e:
        print(f"Warning: Could not load RSI map: {e}")
    return {}


def load_broker_flow(concentration_threshold,
                     buy_file="chukul_broker_buy.csv",
                     sell_file="chukul_broker_sell.csv"):
    """
    Compute broker concentration per symbol.
    Returns {symbol: net_score} where:
      +1 = top 3 buyers control > concentration_threshold of volume  → bullish
      -1 = top 3 sellers control > concentration_threshold of volume → bearish
       0 = balanced / no data
    """
    flow: dict[str, dict[str, float]] = {}
    try:
        if os.path.exists(buy_file):
            df_buy = pd.read_csv(buy_file)
            if "symbol" in df_buy.columns and "quantity" in df_buy.columns:
                for sym, grp in df_buy.groupby("symbol"):
                    grp_sorted = grp.sort_values("quantity", ascending=False)
                    total = grp_sorted["quantity"].sum()
                    top3  = grp_sorted["quantity"].head(3).sum()
                    if total > 0:
                        if sym not in flow:
                            flow[sym] = {}
                        flow[sym]["buy_conc"] = top3 / total

        if os.path.exists(sell_file):
            df_sell = pd.read_csv(sell_file)
            if "symbol" in df_sell.columns and "quantity" in df_sell.columns:
                for sym, grp in df_sell.groupby("symbol"):
                    grp_sorted = grp.sort_values("quantity", ascending=False)
                    total = grp_sorted["quantity"].sum()
                    top3  = grp_sorted["quantity"].head(3).sum()
                    if total > 0:
                        if sym not in flow:
                            flow[sym] = {}
                        flow[sym]["sell_conc"] = top3 / total
    except Exception as e:
        print(f"Warning: Could not load broker flow: {e}")

    result = {}
    for sym, d in flow.items():
        buy_conc  = d.get("buy_conc",  0)
        sell_conc = d.get("sell_conc", 0)
        score = 0
        if buy_conc > concentration_threshold:
            score += 1
        if sell_conc > concentration_threshold:
            score -= 1
        result[sym] = score
    return result


def load_fundamental_map(filepath="chukul_fundamental.csv"):
    """
    Returns {symbol: {"eps": float, "pe": float}} from fundamentals CSV.
    Handles various column name casing.
    """
    if not os.path.exists(filepath):
        print(f"Note: {filepath} not found. Fundamental factor skipped.")
        return {}
    try:
        df = pd.read_csv(filepath)
        df.columns = [c.lower() for c in df.columns]
        result = {}
        for _, row in df.iterrows():
            sym = str(row.get("symbol", "")).strip()
            if not sym:
                continue
            eps = pd.to_numeric(row.get("eps") or row.get("basic_eps"), errors="coerce")
            pe  = pd.to_numeric(row.get("pe")  or row.get("pe_ratio"),  errors="coerce")
            result[sym] = {"eps": eps, "pe": pe}
        return result
    except Exception as e:
        print(f"Warning: Could not load fundamentals: {e}")
    return {}


# ─────────────────────────────────────────
# Technical Calculators
# ─────────────────────────────────────────

def calc_ema(series, span):
    return series.ewm(span=span, adjust=False).mean()


def calc_macd(series, fast=12, slow=26, signal=9):
    """
    Returns (macd_line, signal_line) as Series.
    MACD = EMA(fast) - EMA(slow)
    Signal = EMA(MACD, signal)
    """
    ema_fast    = series.ewm(span=fast,   adjust=False).mean()
    ema_slow    = series.ewm(span=slow,   adjust=False).mean()
    macd_line   = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=signal, adjust=False).mean()
    return macd_line, signal_line


# ─────────────────────────────────────────
# Scoring Engine
# ─────────────────────────────────────────

def score_symbol(symbol, symbol_df, rsi_map, broker_flow, fund_map):
    """
    Compute a signal score for one symbol.

    Returns:
        (score: int, breakdown: dict, last_close: float)
        score > 0 → bullish, < 0 → bearish
    """
    breakdown = {
        "ema": 0, "macd": 0, "volume": 0,
        "rsi": 0, "broker": 0, "fundamental": 0
    }

    if len(symbol_df) < 26:   # need 26 bars for MACD slow EMA
        return 0, breakdown, None

    close  = symbol_df["close"]
    volume = symbol_df["volume"] if "volume" in symbol_df.columns else None
    last_close = float(close.iloc[-1])

    # ── 1. EMA Crossover ──────────────────────────────────────────
    ema9  = calc_ema(close, 9)
    ema21 = calc_ema(close, 21)

    prev_cross = ema9.iloc[-2] - ema21.iloc[-2]
    curr_cross = ema9.iloc[-1] - ema21.iloc[-1]

    if prev_cross <= 0 and curr_cross > 0:
        breakdown["ema"] = +2   # Golden Cross
    elif prev_cross >= 0 and curr_cross < 0:
        breakdown["ema"] = -2   # Death Cross
    elif curr_cross > 0:
        breakdown["ema"] = +1   # Already bullish trend
    elif curr_cross < 0:
        breakdown["ema"] = -1   # Already bearish trend

    # ── 2. MACD ───────────────────────────────────────────────────
    macd_line, signal_line = calc_macd(close)
    prev_macd_diff = macd_line.iloc[-2] - signal_line.iloc[-2]
    curr_macd_diff = macd_line.iloc[-1] - signal_line.iloc[-1]

    if prev_macd_diff <= 0 and curr_macd_diff > 0:
        breakdown["macd"] = +1
    elif prev_macd_diff >= 0 and curr_macd_diff < 0:
        breakdown["macd"] = -1

    # ── 3. Volume Confirmation ────────────────────────────────────
    if volume is not None and len(volume) >= 20:
        avg_vol_20 = volume.iloc[-21:-1].mean()   # previous 20 days
        today_vol  = float(volume.iloc[-1])
        raw_ema_score = breakdown["ema"]
        if today_vol > avg_vol_20:
            if raw_ema_score > 0:
                breakdown["volume"] = +1
            elif raw_ema_score < 0:
                breakdown["volume"] = -1

    # ── 4. RSI ────────────────────────────────────────────────────
    rsi = rsi_map.get(symbol)
    if rsi is not None and not np.isnan(rsi):
        if rsi > 70:
            breakdown["rsi"] = -2   # Overbought: kill any buy signal
        elif rsi < 30:
            breakdown["rsi"] = +2   # Oversold: kill any sell signal
        elif 60 <= rsi <= 70:
            breakdown["rsi"] = +1   # Strong momentum, confirms BUY

    # ── 5. Broker Flow ────────────────────────────────────────────
    breakdown["broker"] = broker_flow.get(symbol, 0)

    # ── 6. Fundamental Gate ───────────────────────────────────────
    fund = fund_map.get(symbol, {})
    eps = fund.get("eps")
    pe  = fund.get("pe")

    if eps is not None and not np.isnan(eps):
        if eps <= 0:
            breakdown["fundamental"] = -1   # Unprofitable: penalise buy signals

    if pe is not None and not np.isnan(pe):
        if pe <= 50:
            breakdown["fundamental"] += 1   # Value stock bonus
        elif pe > 500:
            breakdown["fundamental"] -= 1   # Extreme overvaluation

    score = sum(breakdown.values())
    return score, breakdown, last_close


# ─────────────────────────────────────────
# Main Entry Point
# ─────────────────────────────────────────

def generate_signals(portfolio):
    """
    Main signal generation function.
    Called from main.py each trading cycle.

    Args:
        portfolio: dict with "holdings" list from NAASA scraper

    Returns:
        List of signal dicts ready for the Trader.
    """
    buy_threshold  = int(os.getenv("BUY_THRESHOLD") or "")
    sell_threshold = int(os.getenv("SELL_THRESHOLD") or "")
    default_buy_qty = int(os.getenv("DEFAULT_BUY_QTY") or "")
    broker_conc_threshold = float(os.getenv("BROKER_CONCENTRATION_THRESHOLD") or "")

    signals = []

    df = load_ohlcv()
    if df is None:
        return signals

    rsi_map     = load_rsi_map()
    broker_flow = load_broker_flow(broker_conc_threshold)
    fund_map    = load_fundamental_map()

    print(f"\n--- Multi-Factor Signal Engine ---")
    print(f"  OHLCV symbols : {df['symbol'].nunique()}")
    print(f"  RSI coverage  : {len(rsi_map)} symbols")
    print(f"  Broker flow   : {len(broker_flow)} symbols")
    print(f"  Fundamentals  : {len(fund_map)} symbols")
    print()

    for symbol in df["symbol"].unique():
        symbol_df = df[df["symbol"] == symbol].copy().reset_index(drop=True)

        score, breakdown, last_close = score_symbol(
            symbol, symbol_df, rsi_map, broker_flow, fund_map
        )

        if last_close is None:
            continue

        bd_str = "  ".join(f"{k}={v:+d}" for k, v in breakdown.items() if v != 0)
        if score >= buy_threshold:
            signal_side = "BUY"
        elif score <= sell_threshold:
            signal_side = "SELL"
        else:
            if abs(score) >= 2:
                print(f"  [{symbol}] NEUTRAL score={score:+d}  {bd_str}")
            continue

        print(f"  [{symbol}] *** {signal_side} ***  score={score:+d}  price={last_close}  {bd_str}")

        qty = default_buy_qty

        if signal_side == "SELL":
            owned_qty = 0
            if portfolio and "holdings" in portfolio:
                for h in portfolio["holdings"]:
                    h_sym = (h.get("Symbol") or h.get("symbol")
                             or h.get("Script") or h.get("Scrip"))
                    if h_sym == symbol:
                        try:
                            raw = str(h.get("Quantity",
                                      h.get("Balance", "0"))).replace(",", "")
                            owned_qty = int(float(raw))
                        except Exception:
                            owned_qty = 0
                        break
            if owned_qty <= 0:
                continue
            qty = owned_qty

        signals.append({
            "side":      signal_side,
            "symbol":    symbol,
            "quantity":  qty,
            "price":     last_close,
            "score":     score,
            "breakdown": breakdown,
        })

    print(f"\nTotal signals generated: {len(signals)}")
    return signals


# ─────────────────────────────────────────
# Standalone Test
# ─────────────────────────────────────────

if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    dummy_portfolio = {
        "holdings": [
            {"Symbol": "NICA",   "Quantity": "50"},
            {"Symbol": "AKPL",   "Quantity": "100"},
            {"Symbol": "NABIL",  "Quantity": "20"},
        ]
    }
    sigs = generate_signals(dummy_portfolio)
    print("\nFinal Signals:")
    for s in sigs:
        bd = "  ".join(f"{k}={v:+d}" for k, v in s["breakdown"].items())
        print(f"  {s['side']:4s} {s['symbol']:10s} @ {s['price']}  score={s['score']:+d}  [{bd}]")
