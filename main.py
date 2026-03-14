import os
import sys
import time
import json
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
from notifications import (
    send_email_notification,
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
    Checks if the NEPSE market is open (11 AM - 3 PM, Sun-Thu).
    Friday (4) and Saturday (5) are closed.
    Timezone: Asia/Kathmandu
    """
    tz = pytz.timezone('Asia/Kathmandu')
    now = datetime.now(tz)

    if now.weekday() in [4, 5]:
        return False, "Market Closed (Weekend)"

    market_open  = dt_time(11, 0)
    market_close = dt_time(15, 0)
    current_time = now.time()

    if market_open <= current_time <= market_close:
        return True, "Market Open"
    else:
        return False, f"Market Closed (Time: {current_time.strftime('%H:%M')})"


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

    if RUN_ONCE:
        print("--- Starting Portfolio Tracker Bot (Single Run Mode) ---")
    else:
        print("--- Starting Portfolio Tracker Bot (Scheduler Mode) ---")

    last_fundamental_date = None
    market_open_notified_date = None   # send market-open Telegram once per day
    last_was_open = False              # track transition → closed for market-close alert

    while True:
        open_status, message = is_market_open()
        today_str = datetime.now().strftime("%Y-%m-%d")

        # ── Market just closed: send close summary ──────────────────────────
        if last_was_open and not open_status:
            placed = load_placed_orders().get("orders", [])
            notify_market_close(placed)
        last_was_open = open_status

        if not open_status:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] {message}. Waiting...")
            time.sleep(SLEEP_INTERVAL_CLOSED)
            continue

        print(f"[{datetime.now().strftime('%H:%M:%S')}] Market is OPEN. Starting cycle...")

        # ── Market open: notify once per day ────────────────────────────────
        if market_open_notified_date != today_str:
            notify_market_open(DRY_RUN)
            market_open_notified_date = today_str

        orders_placed_this_cycle = 0

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

                # 3. Update Historical Data from Chukul
                print("Updating historical data from Chukul...")
                update_chukul_data(verbose=False)

                # 4. Fetch Technical Indicators
                print("Fetching technical indicators from Chukul...")
                try:
                    update_indicators_data(verbose=False)
                except Exception as e:
                    print(f"Warning: Indicators fetch failed: {e}")

                # 5. Fetch Broker Buy/Sell Data
                print("Fetching broker analysis from Chukul...")
                try:
                    update_broker_data(verbose=False)
                except Exception as e:
                    print(f"Warning: Broker data fetch failed: {e}")

                # 6. Fetch Floorsheet for today
                print(f"Fetching floorsheet for {today_str}...")
                try:
                    update_floorsheet_data(date=today_str, verbose=False)
                except Exception as e:
                    print(f"Warning: Floorsheet fetch failed: {e}")

                # 7. Fetch Fundamental Data (once per day)
                if last_fundamental_date != today_str:
                    print("Fetching fundamental data from Chukul (daily run)...")
                    try:
                        update_fundamental_data(verbose=False)
                        last_fundamental_date = today_str
                    except Exception as e:
                        print(f"Warning: Fundamental data fetch failed: {e}")
                else:
                    print("Skipping fundamental fetch (already done today).")

                # 8. Scrape Portfolio
                data = scrape_portfolio(page)

                if data:
                    print("Saving portfolio data...")
                    if "summary" in data:
                        save_to_json(data["summary"], "portfolio_summary.json")
                    if "holdings" in data and data["holdings"]:
                        save_to_csv(data["holdings"], "portfolio_data.csv")
                    else:
                        print("No holdings data to save.")

                # 9. Generate Signals
                signals = generate_signals(data)
                print(f"Generated {len(signals)} signals.")

                # Telegram: notify all BUY/SELL signals at once
                actionable = [s for s in signals if s["side"] in ("BUY", "SELL")]
                if actionable:
                    notify_signals(actionable)

                # 10. Execute Trades
                if signals:
                    trader        = Trader(page, dry_run=DRY_RUN)
                    placed_orders = load_placed_orders()

                    for signal in signals:
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
                            buy_count = sum(1 for o in placed_orders.get('orders', []) if o['side'] == 'BUY')
                            if buy_count >= MAX_DAILY_BUYS:
                                print(f"[LIMIT REACHED] Max daily buys ({MAX_DAILY_BUYS}) reached. Skipping BUY {symbol}.")
                                continue

                        success = trader.place_order(signal)
                        if success:
                            save_placed_order(symbol, side)
                            placed_orders = load_placed_orders()
                            orders_placed_this_cycle += 1
                            notify_order(signal, is_dry_run=DRY_RUN)
                            send_email_notification(signal, is_dry_run=DRY_RUN)
                else:
                    print("No trading signals generated.")

            except Exception as e:
                print(f"An error occurred: {e}")
                notify_error(e)
            finally:
                print("Closing browser...")
                browser.close()

        # Telegram: cycle summary
        notify_cycle_summary(signals if 'signals' in dir() else [], orders_placed_this_cycle, POLL_INTERVAL)

        if RUN_ONCE:
            print("Single run complete. Exiting...")
            break

        print(f"Cycle complete. Waiting {POLL_INTERVAL} seconds...")
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
