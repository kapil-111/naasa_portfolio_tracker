from playwright.sync_api import Page

from naasa_locators import (
    holding_data_rows,
    holding_grid_root,
    holding_header_cells,
    holding_next_page,
    holding_no_data,
    naasa_holding_report,
    wallet_home,
    wallet_total_collateral_label,
    wallet_total_collateral_value,
    wait_holding_grid_ready,
)


def scrape_available_fund(page: Page):
    """
    Scrapes Total Collateral from NAASA wallet.
    Returns float or None if not found.
    """
    print("Scraping available fund...")
    page.goto(wallet_home())
    try:
        wallet_total_collateral_label(page).wait_for(state="visible", timeout=10000)
    except Exception:
        print("[FUND] Timed out waiting for wallet page.")
        return None

    try:
        value_text = wallet_total_collateral_value(page).inner_text(timeout=3000)
        value = float(value_text.replace(",", "").replace("Rs.", "").strip())
        print(f"[FUND] Total Collateral: {value:,.2f}")
        return value
    except Exception as e:
        print(f"[FUND] Could not parse Total Collateral: {e}")
        return None


def parse_holding_grid(page: Page) -> list:
    """
    Parse holdings from the current Holding Report page (#GridDiv).
    Assumes navigation to the holding report is already done.
    """
    holdings: list = []

    try:
        wait_holding_grid_ready(page, timeout=15000)

        if holding_no_data(page).is_visible():
            print("Status: No holdings data available (Server returned 'No data').")
            return holdings

        grid_div = holding_grid_root(page)
        header_cells = holding_header_cells(grid_div)
        data_rows = holding_data_rows(grid_div)

        if header_cells.count() == 0:
            print("Syncfusion classes not found, trying generic tables...")
            header_cells = grid_div.locator("table thead th")
            data_rows = grid_div.locator("table tbody tr")

        if header_cells.count() > 0:
            headers = [h.strip() for h in header_cells.all_inner_texts()]
            print(f"Found headers: {headers}")

            page_num = 1
            while True:
                print(f"Scraping page {page_num}...")
                page.wait_for_timeout(500)
                rows = data_rows.all()
                print(f"Found {len(rows)} rows on page {page_num}.")

                for i, row in enumerate(rows):
                    cells = row.locator("td").all_inner_texts()
                    clean_cells = [c.strip() for c in cells]
                    print(f"Row {i} cells: {clean_cells}")
                    if len(cells) == len(headers):
                        if any(clean_cells):
                            holding = dict(zip(headers, clean_cells))
                            holdings.append(holding)
                        else:
                            print(f"Skipping empty row {i}")

                next_btn = holding_next_page(grid_div)
                if next_btn.count() > 0:
                    print(f"Going to page {page_num + 1}...")
                    next_btn.first.click()
                    page.wait_for_timeout(1500)
                    page_num += 1
                else:
                    print("No more pages.")
                    break
        else:
            print("Status: No table elements found in #GridDiv.")
            page.screenshot(path="holdings_no_table.png")

    except Exception as e:
        print(f"Error scraping holding report: {e}")
        page.screenshot(path="holdings_error.png")

    print(f"Scraped {len(holdings)} holdings.")
    return holdings


def scrape_portfolio(page: Page):
    """
    Scrapes portfolio data from the dashboard.
    Returns a dictionary containing summary and holdings data.
    """
    print("Scraping portfolio...")
    portfolio_data = {
        "summary": {},
        "holdings": [],
    }

    print("--- Dashboard Summary ---")
    try:
        total_value = page.locator(".TotalAmountasofCP").inner_text(timeout=5000)
        total_holdings = page.locator(".TotalNoOfHoldings").inner_text(timeout=5000)
        todays_gain = page.locator("#TodaysGain").inner_text(timeout=5000)
        portfolio_data["summary"] = {
            "total_value": total_value,
            "total_holdings": total_holdings,
            "todays_gain": todays_gain,
        }
    except Exception as e:
        print(f"Warning: Could not scrape dashboard summary (not on dashboard page): {e}")
        portfolio_data["summary"] = {}

    print("Navigating to Holding Report...")
    page.goto(naasa_holding_report())

    portfolio_data["holdings"] = parse_holding_grid(page)
    return portfolio_data
