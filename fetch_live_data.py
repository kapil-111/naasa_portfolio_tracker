import os
import sys
import csv
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright
from auth import login

def fetch_live_data(page=None):
    load_dotenv()
    username = os.getenv("NAASA_USERNAME")
    password = os.getenv("NAASA_PASSWORD")
    
    if not username or not password:
        print("Error: NAASA_USERNAME or NAASA_PASSWORD not found.")
        sys.exit(1)

    def scrape(p):
        try:
            # Login only if we aren't already on the dashboard/marketwatch (assuming if page is provided, we are logged in or login was handled)
            # A more robust check might be needed, but for now we assume if page is provided by main.py, it handles login.
            if page is None:
                login(p, username, password)
            
            print("Navigating to Market Watch...")
            p.goto("https://x.naasasecurities.com.np/MarketWatch")
            
            # Wait for table rows
            print("Waiting for market data table...")
            p.wait_for_selector("#LiveMarketWatchTable tr.outr_row", timeout=30000)
            
            # Allow some time for data to populate if it's dynamic
            p.wait_for_timeout(3000)
            
            # Locate all rows
            rows = p.locator("#LiveMarketWatchTable tr.outr_row")
            count = rows.count()
            print(f"Found {count} rows.")
            
            market_data = []
            
            # Iterate through rows
            # Note: locator.all() returns a list of locators
            for i in range(count):
                row = rows.nth(i)
                try:
                    symbol = row.locator("td[colname='ticker']").inner_text().strip()
                    ltp = row.locator("td[colname='LTP']").inner_text().strip()
                    
                    if symbol:
                        # Clean LTP (remove commas if any)
                        ltp_clean = ltp.replace(",", "")
                        market_data.append({"Symbol": symbol, "LTP": ltp_clean})
                except Exception as e:
                    print(f"Error parsing row {i}: {e}")
            
            # Deduplicate (if needed) and sort by Symbol
            # Use a dict to dedup by symbol, keeping the last seen (or first?)
            # Let's simple dedup based on symbol
            unique_data = {d["Symbol"]: d for d in market_data}
            sorted_data = sorted(unique_data.values(), key=lambda x: x["Symbol"])
            
            print(f"Extracted {len(sorted_data)} unique records.")
            
            if sorted_data:
                with open("live_market_data.csv", "w", newline="") as csvfile:
                    fieldnames = ["Symbol", "LTP"]
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    writer.writeheader()
                    for data in sorted_data:
                        writer.writerow(data)
                print("Saved data to live_market_data.csv")
                return sorted_data
            else:
                print("No data extracted.")
                return None
            
        except Exception as e:
            print(f"Error fetching live data: {e}")
            p.screenshot(path="fetch_live_data_error.png")
            print("Saved fetch_live_data_error.png")
            return None

    if page:
        return scrape(page)
    else:
        with sync_playwright() as play:
            browser = play.chromium.launch(headless=True)
            context = browser.new_context()
            new_page = context.new_page()
            try:
                return scrape(new_page)
            finally:
                browser.close()

if __name__ == "__main__":
    fetch_live_data()
