import os
import sys
import shutil
import time # Keep standard time for sleep
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

from datetime import datetime, time as dt_time, timedelta # Rename datetime.time to prevent conflict
import pytz

# ... existing imports ...

def is_market_open():
    """
    Checks if the NEPSE market is open (11 AM - 3 PM, Sun-Thu).
    Friday (4) and Saturday (5) are closed.
    Timezone: Asia/Kathmandu
    """
    tz = pytz.timezone('Asia/Kathmandu')
    now = datetime.now(tz)
    
    # 1. Check Day (Friday=4, Saturday=5 are closed)
    if now.weekday() in [4, 5]: # Friday, Saturday
        return False, "Market Closed (Weekend)"
    
    # 2. Check Time (11:00 - 15:00)
    market_open = dt_time(11, 0)
    market_close = dt_time(15, 0)
    current_time = now.time()
    
    if market_open <= current_time <= market_close:
        return True, "Market Open"
    else:
        return False, f"Market Closed (Time: {current_time.strftime('%H:%M')})"

def load_placed_orders():
    """Loads today's placed orders to prevent duplicates."""
    filename = "placed_orders_today.json"
    today_str = datetime.now().strftime("%Y-%m-%d")
    
    if os.path.exists(filename):
        try:
            with open(filename, 'r') as f:
                data = json.load(f)
                # If the file is from a previous day, clear it
                if data.get("date") != today_str:
                    return {"date": today_str, "symbols": []}
                return data
        except json.JSONDecodeError:
            pass
            
    return {"date": today_str, "symbols": []}

def save_placed_order(symbol):
    """Saves a symbol to the placed orders list."""
    filename = "placed_orders_today.json"
    data = load_placed_orders()
    if symbol not in data["symbols"]:
        data["symbols"].append(symbol)
        
    with open(filename, 'w') as f:
        json.dump(data, f)
    print(f"Recorded order for {symbol} in state file.")

def main():
    load_dotenv()
    
    username = os.getenv("NAASA_USERNAME")
    password = os.getenv("NAASA_PASSWORD")
    
    if not username or not password:
        print("Error: NAASA_USERNAME or NAASA_PASSWORD not found.")
        sys.exit(1)

    # Trading Configuration
    DRY_RUN = True 
    POLL_INTERVAL = 60 # Seconds to wait between polls if market is open
    SLEEP_INTERVAL_CLOSED = 60 # Seconds to wait if market is closed (checking periodically)

    print("--- Starting Portfolio Tracker Bot (Scheduler Mode) ---")

    while True:
        open_status, message = True, "Simulating Open Market"
        
        if not open_status:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] {message}. Waiting...")
            time.sleep(SLEEP_INTERVAL_CLOSED) # Sleep and check again
            continue
            
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Market is OPEN. Starting cycle...")

        with sync_playwright() as p:
            # RUN HEADLESS FOR AUTOMATION
            browser = p.chromium.launch(headless=True)
            context = browser.new_context()
            page = context.new_page()
            
            try:
                # 1. Login
                login(page, username, password)
                
                # 2. Fetch Live Market Data
                print("Fetching live market data...")
                fetch_live_data(page)
                
                # 3. Update Historical Data from Chukul
                print("Updating historical data from Chukul...")
                update_chukul_data(verbose=False)
                
                # 2. Scrape Portfolio
                data = scrape_portfolio(page)
                
                if data:
                    print("Saving portfolio data...")
                    # Save summary
                    if "summary" in data:
                        save_to_json(data["summary"], "portfolio_summary.json")
                    
                    # Save holdings
                    if "holdings" in data and data["holdings"]:
                        save_to_csv(data["holdings"], "portfolio_data.csv")
                    else:
                        print("No holdings data to save.")
                
                # 5. Generate Signals
                signals = generate_signals(data)
                print(f"Generated {len(signals)} signals.")
                
                # 6. Execute Trades
                if signals:
                    trader = Trader(page, dry_run=DRY_RUN)
                    placed_orders = load_placed_orders()
                    
                    for signal in signals:
                        symbol = signal['symbol']
                        
                        # Check state to prevent infinite loops
                        if symbol in placed_orders["symbols"]:
                            print(f"[STATE CHECK] Order for {symbol} already placed today. Skipping.")
                            continue
                            
                        success = trader.place_order(signal)
                        if success:
                            save_placed_order(symbol)
                else:
                    print("No trading signals generated.")
                    
            except Exception as e:
                print(f"An error occurred: {e}")
                # Optional: page.screenshot(path=f"error_{datetime.now().strftime('%H%M%S')}.png")
            finally:
                print("Closing browser...")
                browser.close()
        
        print(f"Cycle complete. Waiting {POLL_INTERVAL} seconds...")
        time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    main()
