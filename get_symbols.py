import os
import sys
import csv
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright
from auth import login

def get_symbols():
    load_dotenv()
    username = os.getenv("NAASA_USERNAME")
    password = os.getenv("NAASA_PASSWORD")
    
    if not username or not password:
        print("Error: NAASA_USERNAME or NAASA_PASSWORD not found.")
        sys.exit(1)

    with sync_playwright() as p:
        # Launching headless=True for automation
        browser = p.chromium.launch(headless=True)
        context = browser.new_context()
        page = context.new_page()
        
        try:
            login(page, username, password)
            
            print("Navigating to Market Watch...")
            page.goto("https://x.naasasecurities.com.np/MarketWatch")
            
            # Wait for any table row in the live market table
            print("Waiting for symbol table...")
            # Try a broader selector first, just the table
            page.wait_for_selector("#LiveMarketWatchTable", timeout=30000)
            
            # Now wait for at least one row
            # Use 'tr.outr_row' based on the HTML dump
            page.wait_for_selector("#LiveMarketWatchTable tr.outr_row", timeout=10000)
            
            # Extract symbols
            # Selector: td element with attribute colname="ticker"
            ticker_locators = page.locator("#LiveMarketWatchTable td[colname='ticker']")
            
            # Wait for content to likely be populated
            page.wait_for_timeout(2000) 
            
            count = ticker_locators.count()
            print(f"Found {count} ticker cells visible.")
            
            if count == 0:
                print("No symbols found. Dumping HTML...")
                with open("debug_symbols_failed.html", "w") as f:
                    f.write(page.content())
            
            symbols = ticker_locators.all_inner_texts()
            
            # Deduplicate and sort
            unique_symbols = sorted(list(set([s.strip() for s in symbols if s.strip()])))
            print(f"Extracted {len(unique_symbols)} unique symbols.")
            
            if unique_symbols:
                with open("all_symbols.csv", "w", newline="") as csvfile:
                    writer = csv.writer(csvfile)
                    writer.writerow(["Symbol"])
                    for symbol in unique_symbols:
                        writer.writerow([symbol])
                print("Saved symbols to all_symbols.csv")
            else:
                print("No unique symbols extracted.")
            
        except Exception as e:
            print(f"Error: {e}")
            page.screenshot(path="get_symbols_error.png")
            print("Saved get_symbols_error.png")
        finally:
            browser.close()

if __name__ == "__main__":
    get_symbols()
