import os
import sys
import time
import json
import pandas as pd
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright
from auth import login
from scraper import scrape_portfolio
from storage import save_to_csv, save_to_json
from trader import Trader
from signals import generate_signals
from fetch_live_data import fetch_live_data
from fetch_chukul_history import update_chukul_data
from fetch_chukul_indicators import update_indicators_data
from fetch_chukul_fundamental import update_fundamental_data
from fetch_chukul_broker import update_broker_data
from fetch_chukul_floorsheet import update_floorsheet_data
from chukul_client import fetch_all_symbols, BASE_URL, _get
from notifications import (
    notify_market_open,
    notify_signals,
    notify_order,
    notify_error,
    notify_cycle_summary,
    notify_market_close,
)

from datetime import datetime, time as dt_time
import pytz


def is_market_open():
    """
    Checks NEPSE market status via Chukul API (handles weekends + holidays).
    Falls back to time-based check (11 AM–3 PM, Sun–Thu) if API is unavailable.
    """
    data = _get(f"{BASE_URL}/tools/market/status/")
    if data and "is_open" in data:
        as_of = data.get("as_of_live", "")
        if data["is_open"]:
            return True, f"Market Open (as of {as_of})"
        else:
            return False, f"Market Closed (as of {as_of})"

    # Fallback: time-based check
    tz = pytz.timezone('Asia/Kathmandu')
    now = datetime.now(tz)
    if now.weekday() in [4, 5]:
        return False, "Market Closed (Weekend)"
    market_open  = dt_time(11, 0)
    market_close = dt_time(15, 0)
    if market_open <= now.time() <= market_close:
        return True, "Market Open"
    return False, f"Market Closed (Time: {now.strftime('%H:%M')})"


def load_placed_orders():
    """Loads today's placed orders to prevent duplicates and limit buys."""
    filename  = "placed_orders_today.json"
    today_str = datetime.now().strftime("%Y-%m-%d")

    if os.path.exists(filename):
        try:
            with open(filename, 'r') as f:
                data = json.load(f)
                if data.get("date") != today_str:
                    return {"date": today_str, "orders": []}
                if "symbols" in data and "orders" not in data:
                    migrated = [{"symbol": s, "side": "BUY"} for s in data["symbols"]]
                    return {"date": today_str, "orders": migrated}
                return data
        except json.JSONDecodeError:
            pass

    return {"date": today_str, "orders": []}


def save_placed_order(symbol, side):
    """Saves a symbol and its side to the placed orders list."""
    filename = "placed_orders_today.json"
    data     = load_placed_orders()

    new_order = {"symbol": symbol, "side": side}
    if new_order not in data["orders"]:
        data["orders"].append(new_order)

    with open(filename, 'w') as f:
        json.dump(data, f)
    print(f"Recorded order for {side} {symbol} in state file.")


def is_nepse_bullish():
    """
    Returns True if NEPSE index EMA9 > EMA21 (bullish trend), False if bearish.
    Defaults to True on API failure so the bot doesn't block all buys on connectivity issues.
    """
    data = _get(f"{BASE_URL}/data/historydata/?symbol=NEPSE")
    if not data or not isinstance(data, list) or len(data) < 22:
        print("Warning: Could not fetch NEPSE index — market filter defaulting to bullish.")
        return True
    try:
        df = pd.DataFrame(data)
        df["close"] = pd.to_numeric(df["close"], errors="coerce")
        df = df.dropna(subset=["close"]).sort_values("date").reset_index(drop=True)
        ema9  = df["close"].ewm(span=9,  adjust=False).mean()
        ema21 = df["close"].ewm(span=21, adjust=False).mean()
        bullish = bool(ema9.iloc[-1] > ema21.iloc[-1])
        trend = "BULLISH" if bullish else "BEARISH"
        print(f"NEPSE trend: {trend} (EMA9={ema9.iloc[-1]:.1f}, EMA21={ema21.iloc[-1]:.1f})")
        return bullish
    except Exception as e:
        print(f"Warning: NEPSE filter error — defaulting to bullish: {e}")
        return True


def _load_cached_portfolio():
    """Load last known portfolio holdings from CSV for offline analysis."""
    if os.path.exists("portfolio_data.csv"):
        try:
            df = pd.read_csv("portfolio_data.csv")
            return {"holdings": df.to_dict("records"), "summary": {}}
        except Exception as e:
            print(f"Warning: Could not load cached portfolio: {e}")
    return {"holdings": [], "summary": {}}


def _ensure_market_ohlcv():
    """
    Ensure all market data CSVs (OHLCV, indicators, broker, fundamentals) are populated.
    If cache has < 100 symbols, fetches all data for full market scan.
    """
    existing_count = 0
    if os.path.exists("chukul_data.csv"):
        try:
            df_existing = pd.read_csv("chukul_data.csv", usecols=lambda c: c in ("symbol", "stock"))
            col = "symbol" if "symbol" in df_existing.columns else "stock"
            existing_count = df_existing[col].nunique()
        except Exception:
            pass

    if existing_count >= 100:
        print(f"OHLCV cache has {existing_count} symbols — using cache.")
        return

    print(f"OHLCV cache has only {existing_count} symbols. Fetching all market data for full scan...")
    symbols = fetch_all_symbols()
    if not symbols:
        print("Warning: Could not fetch symbol list for market scan.")
        return

    update_chukul_data(symbols=symbols, verbose=False)
    print(f"OHLCV updated with {len(symbols)} symbols.")

    try:
        update_indicators_data(symbols=symbols, verbose=False)
        print("Indicators updated.")
    except Exception as e:
        print(f"Warning: Indicators fetch failed: {e}")

    try:
        update_broker_data(symbols=symbols, verbose=False)
        print("Broker data updated.")
    except Exception as e:
        print(f"Warning: Broker data fetch failed: {e}")

    try:
        update_fundamental_data(symbols=symbols, verbose=False)
        print("Fundamentals updated.")
    except Exception as e:
        print(f"Warning: Fundamentals fetch failed: {e}")


def _fetch_chukul_data(today_str, last_fundamental_date):
    """Fetch all Chukul data for all NEPSE symbols. Returns updated last_fundamental_date."""
    print("Fetching full NEPSE symbol list from Chukul...")
    symbols = fetch_all_symbols()
    if not symbols:
        print("Warning: Could not fetch symbol list. Falling back to portfolio symbols.")
        symbols = None  # each update_* will fall back to live_market_data.csv

    print(f"Updating historical data for {len(symbols) if symbols else '?'} symbols...")
    update_chukul_data(symbols=symbols, verbose=False)

    print("Fetching technical indicators from Chukul...")
    try:
        update_indicators_data(symbols=symbols, verbose=False)
    except Exception as e:
        print(f"Warning: Indicators fetch failed: {e}")

    print("Fetching broker analysis from Chukul...")
    try:
        update_broker_data(symbols=symbols, verbose=False)
    except Exception as e:
        print(f"Warning: Broker data fetch failed: {e}")

    print(f"Fetching floorsheet for {today_str}...")
    try:
        update_floorsheet_data(date=today_str, verbose=False)
    except Exception as e:
        print(f"Warning: Floorsheet fetch failed: {e}")

    if last_fundamental_date != today_str:
        print("Fetching fundamental data from Chukul (daily run)...")
        try:
            update_fundamental_data(symbols=symbols, verbose=False)
            last_fundamental_date = today_str
        except Exception as e:
            print(f"Warning: Fundamental data fetch failed: {e}")
    else:
        print("Skipping fundamental fetch (already done today).")

    return last_fundamental_date


def main():
    load_dotenv()

    username = os.getenv("NAASA_USERNAME")
    password = os.getenv("NAASA_PASSWORD")

    if not username or not password:
        print("Error: NAASA_USERNAME or NAASA_PASSWORD not found.")
        sys.exit(1)

    _required = {
        "DRY_RUN":               os.getenv("DRY_RUN"),
        "POLL_INTERVAL":         os.getenv("POLL_INTERVAL"),
        "SLEEP_INTERVAL_CLOSED": os.getenv("SLEEP_INTERVAL_CLOSED"),
        "RUN_ONCE":              os.getenv("RUN_ONCE"),
        "MAX_DAILY_BUYS":        os.getenv("MAX_DAILY_BUYS"),
        "MAX_PORTFOLIO_STOCKS":  os.getenv("MAX_PORTFOLIO_STOCKS"),
    }
    missing = [k for k, v in _required.items() if not v]
    if missing:
        print(f"Error: Missing required env vars: {', '.join(missing)}")
        sys.exit(1)

    DRY_RUN               = _required["DRY_RUN"].lower() == "true"               # type: ignore[union-attr]
    POLL_INTERVAL         = int(_required["POLL_INTERVAL"])                       # type: ignore[arg-type]
    SLEEP_INTERVAL_CLOSED = int(_required["SLEEP_INTERVAL_CLOSED"])               # type: ignore[arg-type]
    RUN_ONCE              = _required["RUN_ONCE"].lower() == "true"               # type: ignore[union-attr]
    MAX_DAILY_BUYS        = int(_required["MAX_DAILY_BUYS"])                      # type: ignore[arg-type]
    MAX_PORTFOLIO_STOCKS  = int(_required["MAX_PORTFOLIO_STOCKS"])                # type: ignore[arg-type]

    if RUN_ONCE:
        print("--- Starting Portfolio Tracker Bot (Single Run Mode) ---")
    else:
        print("--- Starting Portfolio Tracker Bot (Scheduler Mode) ---")

    last_fundamental_date = None
    market_open_notified_date = None
    last_was_open = False

    while True:
        open_status, message = is_market_open()
        today_str = datetime.now().strftime("%Y-%m-%d")

        # ── Market just closed: send close summary ───────────────────────────
        if last_was_open and not open_status:
            placed = load_placed_orders().get("orders", [])
            notify_market_close(placed)
        last_was_open = open_status

        signals = []
        orders_placed_this_cycle = 0

        # ── Market CLOSED: analysis-only cycle (no login, no trading) ────────
        if not open_status:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] {message}. Running analysis cycle...")

            try:
                # Only refresh Chukul data during the pre-market window (9–11 AM NPT).
                # All other closed cycles (post-market, weekends, manual runs) use cache.
                tz  = pytz.timezone('Asia/Kathmandu')
                npt = datetime.now(tz)
                is_market_day    = npt.weekday() not in [4, 5]   # Mon–Thu, Sun = 0-3,6
                in_premarket_win = dt_time(10, 30) <= npt.time() < dt_time(10, 45)
                if is_market_day and in_premarket_win:
                    last_fundamental_date = _fetch_chukul_data(today_str, last_fundamental_date)
                else:
                    print("Outside pre-market window — using cached Chukul data.")

                data    = _load_cached_portfolio()
                _ensure_market_ohlcv()
                signals = generate_signals(data)
                print(f"Generated {len(signals)} signals from cached portfolio.")

                actionable = [s for s in signals if s["side"] in ("BUY", "SELL")]
                if actionable:
                    notify_signals(actionable)

            except Exception as e:
                print(f"Analysis cycle error: {e}")
                notify_error(e)

            notify_cycle_summary(signals, 0, SLEEP_INTERVAL_CLOSED)

            if RUN_ONCE:
                print("Analysis complete. Exiting (RUN_ONCE mode).")
                break

            time.sleep(SLEEP_INTERVAL_CLOSED)
            continue

        # ── Market OPEN: full trading cycle ───────────────────────────────────
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Market is OPEN. Starting cycle...")

        if market_open_notified_date != today_str:
            notify_market_open(DRY_RUN)
            market_open_notified_date = today_str

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context()
            page    = context.new_page()

            try:
                # 1. Login
                login(page, username, password)

                # 2. Fetch Live Market Data
                print("Fetching live market data...")
                fetch_live_data(page)

                # 3. Scrape Portfolio
                data = scrape_portfolio(page)

                if data:
                    print("Saving portfolio data...")
                    if "summary" in data:
                        save_to_json(data["summary"], "portfolio_summary.json")
                    if "holdings" in data and data["holdings"]:
                        save_to_csv(data["holdings"], "portfolio_data.csv")
                    else:
                        print("No holdings data to save.")

                # 4. Fetch fresh Chukul data every market cycle (hourly run)
                print("Fetching fresh market data from Chukul...")
                last_fundamental_date = _fetch_chukul_data(today_str, last_fundamental_date)

                # 5. Generate Signals
                signals = generate_signals(data)
                print(f"Generated {len(signals)} signals.")

                actionable = [s for s in signals if s["side"] in ("BUY", "SELL")]
                if actionable:
                    notify_signals(actionable)

                # 10. Execute Trades (sorted by score: strongest first)
                if signals:
                    trader        = Trader(page, dry_run=DRY_RUN)
                    placed_orders = load_placed_orders()
                    nepse_bullish = is_nepse_bullish()

                    for signal in sorted(signals, key=lambda s: s["score"], reverse=True):
                        symbol = signal['symbol']
                        side   = signal['side'].upper()

                        already_placed = any(
                            o['symbol'] == symbol and o['side'] == side
                            for o in placed_orders.get('orders', [])
                        )
                        if already_placed:
                            print(f"[STATE CHECK] Order for {side} {symbol} already placed today. Skipping.")
                            continue

                        if side == "BUY":
                            if not nepse_bullish:
                                print(f"[MARKET FILTER] NEPSE bearish — skipping BUY {symbol}.")
                                continue
                            buy_count = sum(1 for o in placed_orders.get('orders', []) if o['side'] == 'BUY')
                            if buy_count >= MAX_DAILY_BUYS:
                                print(f"[LIMIT REACHED] Max daily buys ({MAX_DAILY_BUYS}) reached. Skipping BUY {symbol}.")
                                continue
                            portfolio_size = len(data.get("holdings", []))
                            if portfolio_size >= MAX_PORTFOLIO_STOCKS:
                                print(f"[LIMIT REACHED] Portfolio has {portfolio_size} stocks (max {MAX_PORTFOLIO_STOCKS}). Skipping BUY {symbol}.")
                                continue

                        success = trader.place_order(signal)
                        if success:
                            save_placed_order(symbol, side)
                            placed_orders = load_placed_orders()
                            orders_placed_this_cycle += 1
                            notify_order(signal, is_dry_run=DRY_RUN)
                else:
                    print("No trading signals generated.")

            except Exception as e:
                print(f"An error occurred: {e}")
                notify_error(e)
            finally:
                print("Closing browser...")
                browser.close()

        notify_cycle_summary(signals, orders_placed_this_cycle, POLL_INTERVAL)

        if RUN_ONCE:
            print("Single run complete. Exiting...")
            break

        print(f"Cycle complete. Waiting {POLL_INTERVAL} seconds...")
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
