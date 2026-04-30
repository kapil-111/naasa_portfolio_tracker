import os
import sys
import time
import json
import pandas as pd
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright
from auth import login
from session import SessionExpiredError, raise_if_login_page
from scraper import scrape_portfolio, scrape_available_fund
from storage import save_to_csv, save_to_json
from trader import Trader
from signals_mr import load_and_prepare_data, generate_signals as generate_mr_signals, remove_swing_target, save_avg_price, get_nepse_regime
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
from telegram_commands import poll_and_handle

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


def save_placed_order(symbol, side, signal_type, quantity=0):
    """Saves a symbol, side, type, and quantity to the placed orders list."""
    filename = "placed_orders_today.json"
    data     = load_placed_orders()

    # Dedup check excludes quantity so the same order isn't placed twice
    new_order = {"symbol": symbol, "side": side, "type": signal_type, "quantity": quantity}
    if not any(
        o.get("symbol") == symbol and o.get("side") == side and o.get("type") == signal_type
        for o in data["orders"]
    ):
        data["orders"].append(new_order)

    with open(filename, 'w') as f:
        json.dump(data, f, indent=4)
        f.flush()
        os.fsync(f.fileno())
    print(f"Recorded order for {side} {symbol} ({signal_type}) qty={quantity} in state file.")


def remove_placed_order(symbol, side, signal_type):
    """Removes a previously recorded order — used when order definitively failed (not unconfirmed)."""
    filename = "placed_orders_today.json"
    data = load_placed_orders()
    before = len(data["orders"])
    data["orders"] = [
        o for o in data["orders"]
        if not (o.get("symbol") == symbol and o.get("side") == side and o.get("type") == signal_type)
    ]
    if len(data["orders"]) < before:
        with open(filename, 'w') as f:
            json.dump(data, f, indent=4)
            f.flush()
            os.fsync(f.fileno())
        print(f"Removed failed order for {side} {symbol} ({signal_type}) — will retry next cycle.")


def _clear_avg_price(symbol, path="avg_prices.json"):
    """Remove a symbol's entry from avg_prices.json after a full exit."""
    if not os.path.exists(path):
        return
    try:
        with open(path) as f:
            prices = json.load(f)
        if symbol in prices:
            del prices[symbol]
            with open(path, 'w') as f:
                json.dump(prices, f, indent=4, sort_keys=True)
            print(f"[{symbol}] Cleared avg price from avg_prices.json after full exit.")
    except Exception as e:
        print(f"Warning: could not clear avg price for {symbol}: {e}")


def _load_cached_portfolio():
    """Load last known portfolio holdings from CSV for offline analysis."""
    if os.path.exists("portfolio_data.csv"):
        try:
            df = pd.read_csv("portfolio_data.csv")
            return {"holdings": df.to_dict("records"), "summary": {}}
        except Exception as e:
            print(f"Warning: Could not load cached portfolio: {e}")
    return {"holdings": [], "summary": {}}


def _clean_portfolio(portfolio_data, states, placed_orders):
    """
    Remove or adjust holdings that are already settled/sold but still show in the
    scraped portfolio due to T+3 settlement lag.

    Two corrections:
    1. Full exits: remove the holding entirely if state says in_position=False
       and last_exit_date is within the last 3 days (T+3 pending).
    2. Quantity adjustment: if a SELL order was placed today for a symbol that is
       still in the portfolio, subtract the sold quantity so signals use the
       correct remaining qty instead of the pre-sell total.
    """
    from signals_mr import _get_holding_symbol, _get_holding_qty

    # Build a map of today's sell orders: symbol -> total qty sold today
    sold_qty_today = {}
    for o in placed_orders.get("orders", []):
        if o.get("side") == "SELL":
            sym = o.get("symbol")
            qty = o.get("quantity", 0)
            if sym:
                sold_qty_today[sym] = sold_qty_today.get(sym, 0) + qty

    cleaned = []
    for h in portfolio_data.get("holdings", []):
        sym = _get_holding_symbol(h)
        if not sym:
            cleaned.append(h)
            continue
        if sym.lower().startswith("total"):
            continue

        state = states.get(sym, {})
        last_exit = state.get("last_exit_date")
        days_since_exit = (
            (pd.to_datetime("today") - pd.to_datetime(last_exit)).days
            if last_exit else 999
        )

        # 1. Full exit — state says gone, T+3 still showing it
        # Skip removal if it was a partial sell (remaining shares still held)
        if not state.get("in_position") and days_since_exit <= 3 and not state.get("sideways_half_sold"):
            print(f"[PORTFOLIO CLEAN] Removing {sym} — sold {last_exit} ({days_since_exit}d ago), T+3 pending.")
            continue

        # 2. Partial sell — adjust qty for same-day sells
        if sym in sold_qty_today:
            original_qty = _get_holding_qty(h)
            adjusted_qty = max(0, original_qty - sold_qty_today[sym])
            if adjusted_qty != original_qty:
                h = dict(h)  # don't mutate the original
                # Update whichever qty key the scraper used
                from signals_mr import _QTY_KEYS
                for k in _QTY_KEYS:
                    if k in h and h[k] is not None and str(h[k]).strip():
                        h[k] = adjusted_qty
                        break
                print(f"[PORTFOLIO CLEAN] {sym} qty adjusted {original_qty} → {adjusted_qty} (sold {sold_qty_today[sym]} today).")
            if adjusted_qty == 0:
                continue  # fully sold today, don't include

        cleaned.append(h)

    removed = len(portfolio_data.get("holdings", [])) - len(cleaned)
    if removed:
        print(f"[PORTFOLIO CLEAN] {removed} holding(s) removed/adjusted for T+3 lag.")
    return {**portfolio_data, "holdings": cleaned}


def _fetch_chukul_data(max_retries=3, retry_delay=30):
    """Fetch historical OHLCV and (if stale) fundamental data for all NEPSE symbols.
    Retries up to max_retries times with retry_delay seconds between attempts
    so transient chukul.com outages at market open don't leave signals stale.
    """
    _CHUKUL_DATA_FILE = "chukul_data.csv"

    for attempt in range(1, max_retries + 1):
        print(f"Fetching full NEPSE symbol list from Chukul (attempt {attempt}/{max_retries})...")
        symbols = fetch_all_symbols()

        if not symbols:
            print("Warning: Could not fetch symbol list from Chukul API.")
            if attempt < max_retries:
                print(f"Retrying in {retry_delay}s...")
                time.sleep(retry_delay)
                continue
            # All retries exhausted — fall back to symbols from existing file
            if os.path.exists(_CHUKUL_DATA_FILE):
                try:
                    existing = pd.read_csv(_CHUKUL_DATA_FILE)
                    col = "stock" if "stock" in existing.columns else (
                          "symbol" if "symbol" in existing.columns else None)
                    if col:
                        symbols = existing[col].unique().tolist()
                        print(f"Falling back to {len(symbols)} symbols from existing {_CHUKUL_DATA_FILE}.")
                except Exception:
                    pass
            if not symbols:
                print("Warning: No symbol source available, skipping historical data update.")
                return
            break  # use fallback symbols — no point retrying fetch

        # Always include NEPSE index for regime detection
        if "NEPSE" not in symbols:
            symbols = list(symbols) + ["NEPSE"]
        print(f"Updating historical data for {len(symbols)} symbols (incl. NEPSE index)...")
        update_chukul_data(symbols=symbols, verbose=False)

        # Check whether new data was actually written (latest date advanced)
        try:
            df_check = pd.read_csv(_CHUKUL_DATA_FILE)
            date_col = "date" if "date" in df_check.columns else None
            if date_col:
                df_check[date_col] = pd.to_datetime(df_check[date_col], errors="coerce")
                latest = df_check[date_col].max()
                today  = pd.Timestamp.today().normalize()
                age_days = (today - latest).days
                if age_days > 1 and attempt < max_retries:
                    print(f"[DATA CHECK] Latest bar is {age_days} day(s) old ({latest.date()}). "
                          f"Retrying in {retry_delay}s to get fresher data...")
                    time.sleep(retry_delay)
                    continue
                print(f"[DATA CHECK] chukul_data.csv latest bar: {latest.date()} ({age_days}d old). OK.")
        except Exception:
            pass
        break  # success

    fund_file = "chukul_fundamental.csv"
    stale = True
    if os.path.exists(fund_file):
        age_days_fund = (datetime.now().timestamp() - os.path.getmtime(fund_file)) / 86400
        stale = age_days_fund > 7
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

    print("--- Starting Portfolio Tracker Bot (Fortress Signal Strategy) ---")
    open_status, message = is_market_open()
    notify_bot_started(DRY_RUN, message)

    market_open_notified_date = None
    last_was_open = False

    while True:
        cycle_start = time.monotonic()
        open_status, message = is_market_open()
        today_str = datetime.now().strftime("%Y-%m-%d")

        if last_was_open and not open_status:
            placed = load_placed_orders().get("orders", [])
            notify_market_close(placed)
        last_was_open = open_status

        signals = []
        orders_placed_this_cycle = 0

        if not os.getenv("SKIP_DATA_FETCH"):
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
                    raise_if_login_page(page, "closed market: after holding report")
                    available_fund = scrape_available_fund(page)
                    raise_if_login_page(page, "closed market: after wallet")
                    if portfolio_data and portfolio_data.get("holdings"):
                        save_to_csv(portfolio_data.get("holdings", []), "portfolio_data.csv")
                    else:
                        portfolio_data = _load_cached_portfolio()

                    # TEST MODE: force an order while browser is still open
                    test_order = os.getenv("TEST_ORDER")
                    if test_order:
                        parts = test_order.split(":")
                        if len(parts) == 3:
                            t_side, t_sym, t_qty = parts[0].upper(), parts[1].upper(), int(parts[2])
                            ltp = _get_live_ltp(t_sym) or 100.0
                            test_signal = {"symbol": t_sym, "side": t_side, "quantity": t_qty, "price": ltp, "type": "TEST"}
                            print(f"[TEST MODE] Forcing order: {t_side} {t_sym} x{t_qty} @ {ltp}")
                            trader_test = Trader(page, dry_run=False)
                            trader_test.place_order(test_signal)

                    # Poll Telegram for manual commands before closing browser
                    try:
                        _states_for_poll = load_states()
                        trader_closed = Trader(page, dry_run=DRY_RUN)
                        poll_and_handle(page, trader_closed, _states_for_poll, portfolio_data, available_fund, DRY_RUN)
                    except Exception as e:
                        print(f"Telegram command poll error: {e}")

                    browser.close()

                holdings_count = len(portfolio_data.get("holdings", []))
                fund_str = f"NPR {available_fund:,.2f}" if available_fund is not None else "N/A"
                print(f"Portfolio: {holdings_count} holdings | Fund: {fund_str}")

                from signals_mr import _get_holding_symbol
                _held = {_get_holding_symbol(h) for h in portfolio_data.get('holdings', [])} - {None, ''}
                latest_data = load_and_prepare_data(held_symbols=_held)
                if latest_data is not None:
                    states = load_states()
                    placed_orders = load_placed_orders()
                    portfolio_data = _clean_portfolio(portfolio_data, states, placed_orders)
                    regime = get_nepse_regime()
                    signals = generate_mr_signals(latest_data, states, portfolio_data, 0, 99, regime=regime, available_fund=available_fund)
                    print(f"Generated {len(signals)} potential signals for next open.")
                    notify_premarket_report(portfolio_data, available_fund, signals, regime=regime)
            except SessionExpiredError as e:
                print(f"Session / auth error (analysis cycle): {e}")
                notify_error(e)
            except Exception as e:
                print(f"Analysis cycle error: {e}")
                notify_error(e)

            if RUN_ONCE: break
            elapsed = time.monotonic() - cycle_start
            wait = max(0, SLEEP_INTERVAL_CLOSED - elapsed)
            time.sleep(wait)
            continue

        # --- Market OPEN: full trading cycle ---
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Market is OPEN. Starting trading cycle...")

        if market_open_notified_date != today_str:
            notify_market_open(DRY_RUN)
            market_open_notified_date = today_str

        # Load strategy states at the start of the cycle
        states = load_states()

        portfolio_data = _load_cached_portfolio()
        available_fund = None
        signals = []

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
                raise_if_login_page(page, "open market: after market watch")
                portfolio_data = scrape_portfolio(page)
                raise_if_login_page(page, "open market: after holding report")
                available_fund = scrape_available_fund(page)
                raise_if_login_page(page, "open market: after wallet")
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
                from signals_mr import _get_holding_symbol
                _held = {_get_holding_symbol(h) for h in portfolio_data.get('holdings', [])} - {None, ''}
                latest_data = load_and_prepare_data(held_symbols=_held)
                if latest_data is not None:
                    placed_orders = load_placed_orders()
                    portfolio_data = _clean_portfolio(portfolio_data, states, placed_orders)
                    buy_count = sum(1 for o in placed_orders.get('orders', []) if o['side'] == 'BUY' and o.get('type') == 'INITIAL')
                    regime = get_nepse_regime()

                    signals = generate_mr_signals(latest_data, states, portfolio_data, buy_count, MAX_DAILY_BUYS, regime=regime, available_fund=available_fund)
                    print(f"Generated {len(signals)} signals.")

                    # TEST MODE: inject a forced signal to verify order placement end-to-end
                    test_order = os.getenv("TEST_ORDER")  # e.g. "SELL:SNLI:1" or "BUY:SNLI:1"
                    if test_order:
                        parts = test_order.split(":")
                        if len(parts) == 3:
                            t_side, t_sym, t_qty = parts[0].upper(), parts[1].upper(), int(parts[2])
                            ltp = _get_live_ltp(t_sym) or 100.0
                            signals = [{"symbol": t_sym, "side": t_side, "quantity": t_qty, "price": ltp, "type": "TEST"}]
                            print(f"[TEST MODE] Injected signal: {t_side} {t_sym} x{t_qty} @ {ltp}")

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
                            order_cost = ltp_check * qty * 1.005  # include ~0.5% broker commission
                            if order_cost > available_fund:
                                print(f"[SKIP] BUY {symbol} qty={qty} cost={order_cost:,.0f} (incl. commission) > fund={available_fund:,.0f}.")
                                continue

                        order_signal = _adjust_order_price(signal)
                        raise_if_login_page(page, f"open market: before order {side} {symbol}")
                        # Record BEFORE submitting — prevents retry on any failure/crash
                        # (MKT orders: once submitted, broker executes regardless of our state)
                        save_placed_order(symbol, side, signal_type, quantity=qty)
                        success = trader.place_order(order_signal)
                        if not success:
                            if trader.last_outcome == "unconfirmed":
                                # Order was submitted but UI gave no confirmation — could have executed.
                                # Keep in placed_orders to avoid double-execution. Manual check required.
                                notify_error(f"place_order failed: {side} {symbol} ({signal_type})\n{trader.last_error}")
                            else:
                                # Order definitively failed before reaching broker — safe to retry next cycle.
                                remove_placed_order(symbol, side, signal_type)
                                notify_error(f"place_order failed: {side} {symbol} ({signal_type})\n{trader.last_error}")
                        else:
                            if signal_type == "SWING_TARGET":
                                remove_swing_target(symbol)

                            # --- UPDATE STATE on successful trade ---
                            symbol_state = states.get(symbol, {})
                            new_symbol_state = update_state_for_trade(symbol_state, signal, order_signal['price'], signal['quantity'])
                            # Persist sideways half-sell flag so BEAR can finish the exit
                            if signal.get('sideways_half_sold'):
                                new_symbol_state['sideways_half_sold'] = True
                                new_symbol_state['sideways_sold_qty'] = signal.get('sideways_sold_qty', 0)
                            # Clear sideways flag on full exit or successful re-buy
                            if signal_type in ('FULL_EXIT', 'CUT_LOSS', 'RSI_OB', 'SWING_TARGET', 'TRAIL_SL'):
                                new_symbol_state.pop('sideways_half_sold', None)
                                new_symbol_state.pop('sideways_sold_qty', None)
                            states[symbol] = new_symbol_state

                            # --- UPDATE avg_prices.json on BUY / clear on full SELL ---
                            if side == 'BUY':
                                from signals_mr import _get_holding_qty, _get_holding_symbol
                                existing_holding = next(
                                    (h for h in portfolio_data.get('holdings', []) if _get_holding_symbol(h) == symbol),
                                    {}
                                )
                                existing_qty = _get_holding_qty(existing_holding)
                                save_avg_price(symbol, order_signal['price'], qty, existing_qty)
                            elif side == 'SELL' and signal_type in ('FULL_EXIT', 'CUT_LOSS', 'RSI_OB', 'SWING_TARGET'):
                                _clear_avg_price(symbol)

                            placed_orders = load_placed_orders() # Refresh placed orders
                            orders_placed_this_cycle += 1
                            notify_order(signal, is_dry_run=DRY_RUN)
                else:
                    print("No trading signals generated.")

            except SessionExpiredError as e:
                print(f"Session / auth error (trading cycle): {e}")
                notify_error(e)
            except Exception as e:
                print(f"An error occurred: {e}")
                notify_error(e)
            finally:
                # Poll Telegram for manual commands before closing browser
                try:
                    trader_cmd = Trader(page, dry_run=DRY_RUN)
                    poll_and_handle(page, trader_cmd, states, portfolio_data, available_fund, DRY_RUN)
                except Exception as e:
                    print(f"Telegram command poll error: {e}")
                # Save the final state at the end of the trading cycle
                save_states(states)
                print("Closing browser...")
                browser.close()

        notify_cycle_summary(signals, orders_placed_this_cycle, POLL_INTERVAL, load_placed_orders().get("orders", []))
        if RUN_ONCE: break
        elapsed = time.monotonic() - cycle_start
        wait = max(0, POLL_INTERVAL - elapsed)
        print(f"Cycle complete in {elapsed:.0f}s. Waiting {wait:.0f}s...")
        time.sleep(wait)


if __name__ == "__main__":
    main()
