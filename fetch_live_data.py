import os
import sys
import csv
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright
from auth import login
from naasa_locators import (
    market_row_ltp_cell,
    market_row_ticker_cell,
    market_watch_rows,
    naasa_market_watch,
    wait_market_watch_rows_ready,
)


def fetch_live_data(page=None):
    load_dotenv()
    username = os.getenv("NAASA_USERNAME")
    password = os.getenv("NAASA_PASSWORD")

    if not username or not password:
        print("Error: NAASA_USERNAME or NAASA_PASSWORD not found.")
        sys.exit(1)

    def scrape(p):
        try:
            if page is None:
                login(p, username, password)

            print("Navigating to Market Watch...")
            p.goto(naasa_market_watch())

            print("Waiting for market data table...")
            wait_market_watch_rows_ready(p, timeout=30000)

            rows = market_watch_rows(p)
            count = rows.count()
            print(f"Found {count} rows.")

            market_data = []

            for i in range(count):
                row = rows.nth(i)
                try:
                    symbol = market_row_ticker_cell(row).inner_text().strip()
                    ltp = market_row_ltp_cell(row).inner_text().strip()

                    if symbol:
                        ltp_clean = ltp.replace(",", "")
                        market_data.append({"Symbol": symbol, "LTP": ltp_clean})
                except Exception as e:
                    print(f"Error parsing row {i}: {e}")

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
            print("No data extracted.")
            return None

        except Exception as e:
            print(f"Error fetching live data: {e}")
            p.screenshot(path="fetch_live_data_error.png")
            print("Saved fetch_live_data_error.png")
            return None

    if page:
        return scrape(page)
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
