from playwright.sync_api import Page

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
    
    # Scrape Dashboard Summary (DP Holdings)
    print("--- Dashboard Summary ---")
    try:
        total_value = page.locator(".TotalAmountasofCP").inner_text()
        total_holdings = page.locator(".TotalNoOfHoldings").inner_text()
        todays_gain = page.locator("#TodaysGain").inner_text()
        
        portfolio_data["summary"] = {
            "total_value": total_value,
            "total_holdings": total_holdings,
            "todays_gain": todays_gain
        }
        
    except Exception as e:
        print(f"Error scraping dashboard summary: {e}")
        portfolio_data["summary"] = {"error": str(e)}

    # Navigate to Holding Report
    print("Navigating to Holding Report...")
    page.goto("https://x.naasasecurities.com.np/TradeBook?Report=HOLDINGDATA")
    
    try:
        print("Waiting for data to load...")
        # Increase timeout and explicitly wait for the grid table
        try:
            # Wait for the table to appear inside the GridDiv
            page.wait_for_selector("#GridDiv table", state="visible", timeout=10000)
            print("Holdings table detected.")
        except:
            print("Holdings table not detected within timeout.")

        # Check for specific "No data" message
        if page.locator("#GridDiv:has-text('No data to display')").is_visible():
             print("Status: No holdings data available (Server returned 'No data').")
        
        # Scrape table if it exists
        # Handle Syncfusion grid (headers and content often in separate tables)
        grid_div = page.locator("#GridDiv")
        
        # Strategy 1: Syncfusion specific classes
        header_cells = grid_div.locator(".e-gridheader table thead th")
        data_rows = grid_div.locator(".e-gridcontent table tbody tr")
        
        # Strategy 2: Generic fallback (all tables)
        if header_cells.count() == 0:
             print("Syncfusion classes not found, trying generic tables...")
             header_cells = grid_div.locator("table thead th")
             # If headers are in one table and rows in another, this Generic locator might pick up headers twice if we are not careful
             # But 'table tbody tr' should get all rows
             data_rows = grid_div.locator("table tbody tr")

        if header_cells.count() > 0:
            headers = header_cells.all_inner_texts()
            headers = [h.strip() for h in headers]
            print(f"Found headers: {headers}")
            
            rows = data_rows.all()
            print(f"Found {len(rows)} rows.")
            
            for i, row in enumerate(rows):
                    cells = row.locator("td").all_inner_texts()
                    # Debug: print cell content
                    clean_cells = [c.strip() for c in cells]
                    print(f"Row {i} cells: {clean_cells}")
                    
                    # Only add if cell count matches header count AND not all empty
                    if len(cells) == len(headers):
                        if any(clean_cells): # Check if at least one cell has data
                            holding = dict(zip(headers, clean_cells))
                            portfolio_data["holdings"].append(holding)
                        else:
                            print(f"Skipping empty row {i}")
                            # Debug: print HTML to see if it's a filter row or something
                            print(f"Row {i} HTML: {row.inner_html()}")
                    elif len(cells) > 0:
                         pass # Skip non-matching rows
            else:
                 print("Status: Table found but no headers detected.")
        else:
             print("Status: No table elements found in #GridDiv.")

    except Exception as e:
        print(f"Error scraping holding report: {e}")
        page.screenshot(path="holdings_error.png")
        
    return portfolio_data


