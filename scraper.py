from playwright.sync_api import Page


def scrape_available_fund(page: Page):
    """
    Scrapes Total Collateral from NAASA wallet.
    Returns float or None if not found.
    """
    print("Scraping available fund...")
    page.goto("https://wallet.naasasecurities.com.np/")
    try:
        page.wait_for_selector("text=Total Collateral", timeout=10000)
    except Exception:
        print("[FUND] Timed out waiting for wallet page.")
        return None

    try:
        # "Total Collateral" label → next sibling div → first span (the value)
        value_text = page.locator("xpath=//span[normalize-space()='Total Collateral']/following-sibling::div[1]/span[1]").inner_text(timeout=3000)
        value = float(value_text.replace(",", "").replace("Rs.", "").strip())
        print(f"[FUND] Total Collateral: {value:,.2f}")
        return value
    except Exception as e:
        print(f"[FUND] Could not parse Total Collateral: {e}")
        return None


def scrape_portfolio(page: Page):
    """
    Scrapes portfolio data from the dashboard.
    Returns a dictionary containing summary and holdings data.
    """
    print("Scraping portfolio...")
    portfolio_data = {
        "summary": {},
        "holdings": []
    }

    # Scrape Dashboard Summary — short timeout since we may not be on dashboard
    print("--- Dashboard Summary ---")
    try:
        total_value    = page.locator(".TotalAmountasofCP").inner_text(timeout=5000)
        total_holdings = page.locator(".TotalNoOfHoldings").inner_text(timeout=5000)
        todays_gain    = page.locator("#TodaysGain").inner_text(timeout=5000)
        portfolio_data["summary"] = {
            "total_value":    total_value,
            "total_holdings": total_holdings,
            "todays_gain":    todays_gain
        }
    except Exception as e:
        print(f"Warning: Could not scrape dashboard summary (not on dashboard page): {e}")
        portfolio_data["summary"] = {}

    # Navigate to Holding Report
    print("Navigating to Holding Report...")
    page.goto("https://x.naasasecurities.com.np/TradeBook?Report=HOLDINGDATA")

    try:
        print("Waiting for data to load...")
        try:
            page.wait_for_selector("#GridDiv table", state="visible", timeout=15000)
            print("Holdings table detected.")
        except Exception:
            print("Holdings table not detected within timeout.")

        # Extra wait for Syncfusion grid to populate rows asynchronously
        page.wait_for_timeout(3000)

        # Check for "No data" message
        if page.locator("#GridDiv:has-text('No data to display')").is_visible():
            print("Status: No holdings data available (Server returned 'No data').")
            return portfolio_data

        grid_div = page.locator("#GridDiv")

        # Strategy 1: Syncfusion specific classes
        header_cells = grid_div.locator(".e-gridheader table thead th")
        data_rows    = grid_div.locator(".e-gridcontent table tbody tr")

        # Strategy 2: Generic fallback
        if header_cells.count() == 0:
            print("Syncfusion classes not found, trying generic tables...")
            header_cells = grid_div.locator("table thead th")
            data_rows    = grid_div.locator("table tbody tr")

        if header_cells.count() > 0:
            headers = [h.strip() for h in header_cells.all_inner_texts()]
            print(f"Found headers: {headers}")

            rows = data_rows.all()
            print(f"Found {len(rows)} rows.")

            for i, row in enumerate(rows):
                cells      = row.locator("td").all_inner_texts()
                clean_cells = [c.strip() for c in cells]
                print(f"Row {i} cells: {clean_cells}")

                if len(cells) == len(headers):
                    if any(clean_cells):
                        holding = dict(zip(headers, clean_cells))
                        portfolio_data["holdings"].append(holding)
                    else:
                        print(f"Skipping empty row {i}")
        else:
            print("Status: No table elements found in #GridDiv.")
            page.screenshot(path="holdings_no_table.png")

    except Exception as e:
        print(f"Error scraping holding report: {e}")
        page.screenshot(path="holdings_error.png")

    print(f"Scraped {len(portfolio_data['holdings'])} holdings.")
    return portfolio_data
