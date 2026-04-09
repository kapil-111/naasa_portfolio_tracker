import os
import sys
import time
import json
import pandas as pd
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright
from auth import login
from scraper import scrape_portfolio, scrape_available_fund
from storage import save_to_csv, save_to_json
from trader import Trader
from signals_mr import load_and_prepare_data, generate_signals as generate_mr_signals, remove_swing_target, save_avg_price
from state_manager import load_states, save_states, update_state_for_trade
from fetch_live_data import fetch_live_data
from fetch_chukul_history import update_chukul_data
from fetch_chukul_fundamental import update_fundamental_data
from chukul_client import fetch_all_symbols, BASE_URL, _get
from notifications import (
    notify_bot_started,
    notify_market_open,
    notify_signals,
    notify_order,
    notify_error,
    notify_cycle_summary,
    notify_market_close,
    notify_premarket_report,
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


def _get_live_ltp(symbol):
    """Return live LTP for symbol from live_market_data.csv, or None if unavailable."""
    try:
        if os.path.exists("live_market_data.csv"):
            ldf = pd.read_csv("live_market_data.csv")
            row = ldf[ldf["Symbol"] == symbol]
            if not row.empty:
                return float(str(row.iloc[0]["LTP"]).replace(",", ""))
    except Exception:
        pass
    return None


def _adjust_order_price(signal):
    """
    Adjust order price using live LTP before placement:
    - BUY : live price falling (negative day) → place 1% below LTP for better fill
    - SELL: live price rising (positive day)  → place 1% above LTP for better fill
    Returns a copy of signal with adjusted price (original unchanged).
    """
    symbol    = signal['symbol']
    side      = signal['side'].upper()
    base_price = float(signal['price'])

    ltp = _get_live_ltp(symbol)
    if ltp is None or ltp <= 0:
        return signal  # no live data — use signal price as-is

    daily_chg = (ltp - base_price) / base_price if base_price > 0 else 0

    if side == 'BUY' and daily_chg < 0:
        # Price falling today — place order 1% below LTP to get a better entry
        adjusted = round(ltp * 0.99, 2)
        print(f"[PRICE ADJ] BUY {symbol}: LTP={ltp:.2f} (day {daily_chg:+.1%}) → order @ {adjusted:.2f}")
    elif side == 'SELL' and daily_chg > 0:
        # Price rising today — place order 1% above LTP to get a better exit
        adjusted = round(ltp * 1.01, 2)
        print(f"[PRICE ADJ] SELL {symbol}: LTP={ltp:.2f} (day {daily_chg:+.1%}) → order @ {adjusted:.2f}")
    else:
        return signal  # no favourable condition — keep original price

    adjusted_signal = dict(signal)
    adjusted_signal['price'] = adjusted
    return adjusted_signal


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
    """Fetch historical OHLCV and (if stale) fundamental data for all NEPSE symbols."""
    print("Fetching full NEPSE symbol list from Chukul...")
    symbols = fetch_all_symbols()
    if not symbols:
        print("Warning: Could not fetch symbol list from Chukul API.")
        if os.path.exists("chukul_data.csv"):
            try:
                existing = pd.read_csv("chukul_data.csv")
                if "stock" in existing.columns:
                    symbols = existing["stock"].unique().tolist()
                    print(f"Falling back to {len(symbols)} symbols from existing chukul_data.csv.")
            except Exception:
                pass
        if not symbols:
            print("Warning: No symbol source available, skipping historical data update.")
            return
    print(f"Updating historical data for {len(symbols)} symbols...")
    update_chukul_data(symbols=symbols, verbose=False)

    fund_file = "chukul_fundamental.csv"
    stale = True
    if os.path.exists(fund_file):
        age_days = (datetime.now().timestamp() - os.path.getmtime(fund_file)) / 86400
        stale = age_days > 7
    if stale:
        print("Fetching fundamental data (file missing or >7 days old)...")
        update_fundamental_data(symbols=symbols, verbose=False)


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
    open_status, message = is_market_open()
    notify_bot_started(DRY_RUN, message)

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
                with sync_playwright() as p:
                    browser = p.chromium.launch(
                        headless=True,
                        args=["--disable-blink-features=AutomationControlled", "--no-sandbox"]
                    )
                    context = browser.new_context(
                        user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
                        viewport={"width": 1280, "height": 800},
                    )
                    context.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
                    page = context.new_page()
                    login(page, username, password)
                    portfolio_data = scrape_portfolio(page)
                    available_fund = scrape_available_fund(page)
                    if portfolio_data and portfolio_data.get("holdings"):
                        save_to_csv(portfolio_data.get("holdings", []), "portfolio_data.csv")
                    else:
                        portfolio_data = _load_cached_portfolio()
                    browser.close()

                holdings_count = len(portfolio_data.get("holdings", []))
                fund_str = f"NPR {available_fund:,.2f}" if available_fund is not None else "N/A"
                print(f"Portfolio: {holdings_count} holdings | Fund: {fund_str}")

                latest_data = load_and_prepare_data()
                if latest_data is not None:
                    states = load_states()
                    signals = generate_mr_signals(latest_data, states, portfolio_data, 0, 99)
                    print(f"Generated {len(signals)} potential signals for next open.")
                    notify_premarket_report(portfolio_data, available_fund, signals)
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
            browser = p.chromium.launch(
                headless=True,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--no-sandbox",
                ]
            )
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
                viewport={"width": 1280, "height": 800},
            )
            context.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            page = context.new_page()

            try:
                login(page, username, password)
                fetch_live_data(page)
                portfolio_data = scrape_portfolio(page)
                available_fund = scrape_available_fund(page)
                if available_fund is not None:
                    save_to_json({"available_fund": available_fund}, "available_fund.json")
                    print(f"Available fund: NPR {available_fund:,.2f}")

                if portfolio_data and portfolio_data.get("holdings"):
                    save_to_json(portfolio_data.get("summary", {}), "portfolio_summary.json")
                    save_to_csv(portfolio_data.get("holdings", []), "portfolio_data.csv")
                else:
                    print("Warning: Portfolio scraping returned empty holdings. Falling back to cached portfolio.")
                    portfolio_data = _load_cached_portfolio()

                # --- NEW SIGNAL GENERATION ---
                latest_data = load_and_prepare_data()
                if latest_data is not None:
                    placed_orders = load_placed_orders()
                    buy_count = sum(1 for o in placed_orders.get('orders', []) if o['side'] == 'BUY' and o.get('type') == 'INITIAL')

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

                        qty = signal.get('quantity', 0)
                        if qty <= 0:
                            print(f"[SKIP] {side} {symbol} qty={qty} — skipping zero-quantity order.")
                            continue
                        if side == 'SELL' and qty < 10:
                            print(f"[SKIP] SELL {symbol} qty={qty} — below minimum sell threshold (10).")
                            continue
                        if side == 'BUY' and available_fund is not None:
                            ltp_check = _get_live_ltp(symbol) or signal['price']
                            order_cost = ltp_check * qty
                            if order_cost > available_fund:
                                print(f"[SKIP] BUY {symbol} qty={qty} cost={order_cost:,.0f} > fund={available_fund:,.0f}.")
                                continue

                        order_signal = _adjust_order_price(signal)
                        # Record BEFORE submitting — prevents retry on any failure/crash
                        # (MKT orders: once submitted, broker executes regardless of our state)
                        save_placed_order(symbol, side, signal_type)
                        success = trader.place_order(order_signal)
                        if not success:
                            notify_error(f"place_order failed: {side} {symbol} ({signal_type})\n{trader.last_error}")
                        else:
                            if signal_type == "SWING_TARGET":
                                remove_swing_target(symbol)

                            # --- UPDATE STATE on successful trade ---
                            symbol_state = states.get(symbol, {})
                            new_symbol_state = update_state_for_trade(symbol_state, signal, order_signal['price'], signal['quantity'])
                            states[symbol] = new_symbol_state

                            # --- UPDATE avg_prices.json on BUY ---
                            if side == 'BUY':
                                from signals_mr import _get_holding_qty, _get_holding_symbol
                                existing_holding = next(
                                    (h for h in portfolio_data.get('holdings', []) if _get_holding_symbol(h) == symbol),
                                    {}
                                )
                                existing_qty = _get_holding_qty(existing_holding)
                                save_avg_price(symbol, order_signal['price'], qty, existing_qty)

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

        notify_cycle_summary(signals, orders_placed_this_cycle, POLL_INTERVAL, load_placed_orders().get("orders", []))
        if RUN_ONCE: break
        print(f"Cycle complete. Waiting {POLL_INTERVAL} seconds...")
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
