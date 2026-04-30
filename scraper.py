import re

from playwright.sync_api import Page

from naasa_locators import (
    goto_broker_page,
    holding_data_rows,
    holding_grid_root,
    holding_header_cells,
    holding_next_page,
    holding_no_data,
    naasa_holding_report,
    naasa_order,
    order_available_collateral,
    wallet_home,
    wallet_total_collateral_label,
    wallet_total_collateral_value,
    wait_holding_grid_ready,
)

# Column names NAASA / Syncfusion may use for the scrip column
_SYMBOL_HEADER_KEYS = (
    "Symbol",
    "symbol",
    "Stock Symbol",
    "Script",
    "Scrip",
)


def _holding_row_symbol(headers: list, clean_cells: list) -> str:
    row = dict(zip(headers, clean_cells))
    for k in _SYMBOL_HEADER_KEYS:
        v = row.get(k)
        if v is not None and str(v).strip():
            return str(v).strip()
    if clean_cells:
        return str(clean_cells[0]).strip()
    return ""


def _is_holding_page_total_row(headers: list, clean_cells: list) -> bool:
    """
    NAASA holding grid ends each page with a blue 'Total :' row (page subtotal), not a stock.
    """
    sym = _holding_row_symbol(headers, clean_cells).lower()
    if not sym:
        return False
    if sym.startswith("total"):
        return True
    return re.match(r"^total\s*:?\s*$", sym) is not None


def scrape_available_fund(page: Page):
    """
    Scrapes Available Collateral from the order page (already loaded during trading cycle).
    Falls back to wallet page if the order page value is missing.
    Returns float or None if not found.
    """
    def _parse(text):
        return float(text.replace(",", "").replace("Rs.", "").strip())

    # Try order page first — no extra navigation needed
    print("Scraping available fund from order page...")
    try:
        goto_broker_page(page, naasa_order())
        loc = order_available_collateral(page)
        loc.wait_for(state="visible", timeout=8000)
        value = _parse(loc.inner_text(timeout=3000))
        print(f"[FUND] Available Collateral (order page): {value:,.2f}")
        return value
    except Exception as e:
        print(f"[FUND] Order page collateral failed ({e}), falling back to wallet...")

    # Fallback: wallet page
    goto_broker_page(page, wallet_home())
    try:
        wallet_total_collateral_label(page).wait_for(state="visible", timeout=10000)
    except Exception:
        print("[FUND] Timed out waiting for wallet page.")
        return None
    try:
        value_text = wallet_total_collateral_value(page).inner_text(timeout=3000)
        value = _parse(value_text)
        print(f"[FUND] Total Collateral (wallet): {value:,.2f}")
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
                            if _is_holding_page_total_row(headers, clean_cells):
                                print(f"Skipping page total/footer row {i}: {clean_cells[:3]}...")
                                continue
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
    goto_broker_page(page, naasa_holding_report())

    portfolio_data["holdings"] = parse_holding_grid(page)
    return portfolio_data
