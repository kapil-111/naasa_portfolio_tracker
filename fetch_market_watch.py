import os
import sys
import time
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright
from auth import login

def fetch_market_watch():
    # Load environment variables
    load_dotenv()
    
    username = os.getenv("NAASA_USERNAME")
    password = os.getenv("NAASA_PASSWORD")
    
    if not username or not password:
        print("Error: NAASA_USERNAME or NAASA_PASSWORD not found.")
        sys.exit(1)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context()
        page = context.new_page()
        
        try:
            login(page, username, password)
            
            print("Navigating to Market Watch...")
            page.goto("https://x.naasasecurities.com.np/MarketWatch")
            page.wait_for_load_state("networkidle")
            
            # Save HTML
            with open("market_watch.html", "w", encoding="utf-8") as f:
                f.write(page.content())
            print("Saved market_watch.html")
            
            # Take a screenshot
            page.screenshot(path="market_watch.png")
            print("Saved market_watch.png")
            
        except Exception as e:
            print(f"Error: {e}")
            page.screenshot(path="mw_error.png")
        finally:
            browser.close()

if __name__ == "__main__":
    fetch_market_watch()
