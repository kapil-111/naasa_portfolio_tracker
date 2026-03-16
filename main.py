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
from signals_mr import load_and_prepare_data, generate_signals as generate_mr_signals, remove_swing_target
from state_manager import load_states, save_states, update_state_for_trade
from fetch_live_data import fetch_live_data
from fetch_chukul_history import update_chukul_data
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


def save_placed_order(symbol, side, signal_type):
    """Saves a symbol and its side to the placed orders list."""
    filename = "placed_orders_today.json"
    data     = load_placed_orders()

    # With the new strategy, we can have multiple actions for one symbol (e.g. half-sell, full-sell)
    # The check for duplicates should be more specific, including the type.
    new_order = {"symbol": symbol, "side": side, "type": signal_type}
    if new_order not in data["orders"]:
        data["orders"].append(new_order)

    with open(filename, 'w') as f:
        json.dump(data, f, indent=4)
    print(f"Recorded order for {side} {symbol} ({signal_type}) in state file.")


def _load_cached_portfolio():
    """Load last known portfolio holdings from CSV for offline analysis."""
    if os.path.exists("portfolio_data.csv"):
        try:
            df = pd.read_csv("portfolio_data.csv")
            return {"holdings": df.to_dict("records"), "summary": {}}
        except Exception as e:
            print(f"Warning: Could not load cached portfolio: {e}")
    return {"holdings": [], "summary": {}}


def _fetch_chukul_data():
    """Fetch historical OHLCV data for all NEPSE symbols."""
    print("Fetching full NEPSE symbol list from Chukul...")
    symbols = fetch_all_symbols()
    if not symbols:
        print("Warning: Could not fetch symbol list. Falling back to portfolio symbols.")
        symbols = None
    print(f"Updating historical data for {len(symbols) if symbols else '?'} symbols...")
    update_chukul_data(symbols=symbols, verbose=False)


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
        "DEFAULT_BUY_QTY":       os.getenv("DEFAULT_BUY_QTY"),
    }
    missing = [k for k, v in _required.items() if not v]
    if missing:
        print(f"Error: Missing required env vars: {', '.join(missing)}")
        sys.exit(1)

    DRY_RUN               = _required["DRY_RUN"].lower() == "true"
    POLL_INTERVAL         = int(_required["POLL_INTERVAL"])
    SLEEP_INTERVAL_CLOSED = int(_required["SLEEP_INTERVAL_CLOSED"])
    RUN_ONCE              = _required["RUN_ONCE"].lower() == "true"
    MAX_DAILY_BUYS        = int(_required["MAX_DAILY_BUYS"])
    MAX_PORTFOLIO_STOCKS  = int(_required["MAX_PORTFOLIO_STOCKS"])

    print("--- Starting Portfolio Tracker Bot (Mean Reversion Strategy) ---")

    market_open_notified_date = None
    last_was_open = False

    while True:
        open_status, message = is_market_open()
        today_str = datetime.now().strftime("%Y-%m-%d")

        if last_was_open and not open_status:
            placed = load_placed_orders().get("orders", [])
            notify_market_close(placed)
        last_was_open = open_status

        signals = []
        orders_placed_this_cycle = 0

        _fetch_chukul_data()

        # Run analysis cycle when market is closed
        if not open_status:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] {message}. Running analysis-only cycle...")
            try:
                latest_data = load_and_prepare_data()
                if latest_data is not None:
                    states = load_states()
                    portfolio = _load_cached_portfolio()
                    # In analysis mode, no daily buy limit is applied for notification purposes
                    signals = generate_mr_signals(latest_data, states, portfolio, 0, 99) 
                    print(f"Generated {len(signals)} potential signals for next open.")
                    if signals:
                        notify_signals(signals)
            except Exception as e:
                print(f"Analysis cycle error: {e}")
                notify_error(e)
            
            if RUN_ONCE: break
            time.sleep(SLEEP_INTERVAL_CLOSED)
            continue

        # --- Market OPEN: full trading cycle ---
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Market is OPEN. Starting trading cycle...")

        if market_open_notified_date != today_str:
            notify_market_open(DRY_RUN)
            market_open_notified_date = today_str

        # Load strategy states at the start of the cycle
        states = load_states()

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context()
            page = context.new_page()

            try:
                login(page, username, password)
                fetch_live_data(page)
                portfolio_data = scrape_portfolio(page)
                
                if portfolio_data:
                    save_to_json(portfolio_data.get("summary", {}), "portfolio_summary.json")
                    save_to_csv(portfolio_data.get("holdings", []), "portfolio_data.csv")

                # --- NEW SIGNAL GENERATION ---
                latest_data = load_and_prepare_data()
                if latest_data is not None:
                    placed_orders = load_placed_orders()
                    buy_count = sum(1 for o in placed_orders.get('orders', []) if o['side'] == 'BUY')
                    
                    signals = generate_mr_signals(latest_data, states, portfolio_data, buy_count, MAX_DAILY_BUYS)
                    print(f"Generated {len(signals)} signals.")
                    if signals:
                        notify_signals(signals)

                # --- NEW TRADE EXECUTION LOOP ---
                if signals:
                    trader = Trader(page, dry_run=DRY_RUN)
                    for signal in signals:
                        symbol = signal['symbol']
                        side = signal['side'].upper()
                        signal_type = signal.get('type', 'FULL')

                        # Prevent re-placing the same specific action (e.g. half-sell)
                        already_placed = any(
                            o['symbol'] == symbol and o['side'] == side and o.get('type') == signal_type
                            for o in placed_orders.get('orders', [])
                        )
                        if already_placed:
                            print(f"[STATE CHECK] Order for {side} {symbol} ({signal_type}) already placed today. Skipping.")
                            continue
                        
                        # Portfolio size limits only for INITIAL buys
                        if signal_type == "INITIAL":
                            portfolio_size = len(portfolio_data.get("holdings", []))
                            if portfolio_size >= MAX_PORTFOLIO_STOCKS:
                                print(f"[LIMIT REACHED] Portfolio has {portfolio_size} stocks (max {MAX_PORTFOLIO_STOCKS}). Skipping BUY {symbol}.")
                                continue

                        success = trader.place_order(signal)
                        if success:
                            save_placed_order(symbol, side, signal_type)

                            if signal_type == "SWING_TARGET":
                                remove_swing_target(symbol)

                            # --- UPDATE STATE on successful trade ---
                            symbol_state = states.get(symbol, {})
                            new_symbol_state = update_state_for_trade(symbol_state, signal, signal['price'], signal['quantity'])
                            states[symbol] = new_symbol_state

                            placed_orders = load_placed_orders() # Refresh placed orders
                            orders_placed_this_cycle += 1
                            notify_order(signal, is_dry_run=DRY_RUN)
                else:
                    print("No trading signals generated.")

            except Exception as e:
                print(f"An error occurred: {e}")
                notify_error(e)
            finally:
                # Save the final state at the end of the trading cycle
                save_states(states)
                print("Closing browser...")
                browser.close()

        notify_cycle_summary(signals, orders_placed_this_cycle, POLL_INTERVAL)
        if RUN_ONCE: break
        print(f"Cycle complete. Waiting {POLL_INTERVAL} seconds...")
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
