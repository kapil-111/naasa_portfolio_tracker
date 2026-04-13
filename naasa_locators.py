"""
Centralized NAASA X / wallet URLs and Playwright locators with fallback chains.

Update fallbacks here when the broker changes markup. UI last verified in-repo: 2026-04.
"""
from __future__ import annotations

import time

from playwright.sync_api import Locator, Page, TimeoutError as PlaywrightTimeoutError

# --- URLs ---
NAASA_BASE = "https://x.naasasecurities.com.np"
WALLET_BASE = "https://wallet.naasasecurities.com.np"


def naasa_home() -> str:
    return f"{NAASA_BASE}/"


def naasa_order() -> str:
    return f"{NAASA_BASE}/MarketOrder/Order"


def naasa_holding_report() -> str:
    return f"{NAASA_BASE}/TradeBook?Report=HOLDINGDATA"


def naasa_market_watch() -> str:
    return f"{NAASA_BASE}/MarketWatch"


def wallet_home() -> str:
    return f"{WALLET_BASE}/"


def login_username(page: Page) -> Locator:
    return page.locator("#username")


def login_password(page: Page) -> Locator:
    return page.locator("#login-password").or_(page.locator("input[type='password'][name='password']"))


def login_submit(page: Page) -> Locator:
    return page.locator("#kc-login").or_(page.locator("input[type='submit'][value*='Log']"))


def dashboard_url_glob() -> str:
    return "**/Home/Dashboard"


# --- Order form ---
def order_side_buy(page: Page) -> Locator:
    return page.get_by_role("link", name="BUY").or_(page.locator(".sl_by a:has-text('BUY')"))


def order_side_sell(page: Page) -> Locator:
    return page.get_by_role("link", name="SELL").or_(page.locator(".sl_by a:has-text('SELL')"))


def order_symbol_input(page: Page) -> Locator:
    return page.locator("#searchStock").or_(page.locator("input#searchStock"))


def order_type_mkt(page: Page) -> Locator:
    return page.locator("label:has-text('MKT')").or_(page.get_by_text("MKT", exact=True))


def order_quantity_input(page: Page) -> Locator:
    return page.locator("#OrdertxtQty")


def order_submit_button(page: Page) -> Locator:
    return page.locator("#btnBuy")


def order_success_indicators(page: Page) -> Locator:
    return page.locator(".alert-success, .toast-success, [class*='success']")


# --- Wallet / collateral ---
def wallet_total_collateral_label(page: Page) -> Locator:
    return page.get_by_text("Total Collateral", exact=True).or_(page.locator("text=Total Collateral"))


def wallet_total_collateral_value(page: Page) -> Locator:
    """Label 'Total Collateral' → sibling div → span (value)."""
    return page.locator(
        "xpath=//span[normalize-space()='Total Collateral']/following-sibling::div[1]/span[1]"
    )


# --- Holding report grid ---
def holding_grid_root(page: Page) -> Locator:
    return page.locator("#GridDiv")


def holding_grid_table_wait(page: Page) -> Locator:
    # #GridDiv often has separate header/content tables — avoid strict-mode violation
    return page.locator("#GridDiv table").first


def holding_no_data(page: Page) -> Locator:
    return page.locator("#GridDiv:has-text('No data to display')")


def holding_header_cells(grid: Locator) -> Locator:
    return grid.locator(".e-gridheader table thead th").or_(grid.locator("table thead th"))


def holding_data_rows(grid: Locator) -> Locator:
    return grid.locator(".e-gridcontent table tbody tr").or_(grid.locator("table tbody tr"))


def holding_next_page(grid: Locator) -> Locator:
    return grid.locator(".e-nextpage:not(.e-disable)")


# --- Market watch ---
def market_watch_table(page: Page) -> Locator:
    return page.locator("#LiveMarketWatchTable")


def market_watch_rows(page: Page) -> Locator:
    return page.locator("#LiveMarketWatchTable tr.outr_row").or_(
        page.locator("#LiveMarketWatchTable tbody tr")
    )


def market_row_ticker_cell(row: Locator) -> Locator:
    return row.locator("td[colname='ticker']").or_(row.locator("td").first)


def market_row_ltp_cell(row: Locator) -> Locator:
    return row.locator("td[colname='LTP']").or_(row.locator("td").nth(1))


# --- Wait helpers ---
def wait_for_login_form(page: Page, timeout: float = 15_000) -> None:
    login_username(page).wait_for(state="visible", timeout=timeout)


def wait_for_order_page(page: Page, timeout: float = 30_000) -> None:
    page.wait_for_load_state("networkidle", timeout=timeout)
    order_symbol_input(page).wait_for(state="visible", timeout=10_000)


def wait_after_side_select(page: Page) -> None:
    """Prefer waiting on symbol field over fixed sleep."""
    try:
        order_symbol_input(page).wait_for(state="visible", timeout=5_000)
    except PlaywrightTimeoutError:
        pass


def wait_after_symbol_entry(page: Page) -> None:
    try:
        order_quantity_input(page).wait_for(state="visible", timeout=5_000)
    except PlaywrightTimeoutError:
        pass


def wait_holding_grid_ready(page: Page, timeout: float = 15_000) -> None:
    """Wait for grid shell, then either 'no data' or at least one body row."""
    holding_grid_table_wait(page).wait_for(state="visible", timeout=timeout)
    no_data = holding_no_data(page)
    if no_data.is_visible():
        return
    grid = holding_grid_root(page)
    rows = holding_data_rows(grid)
    try:
        rows.first.wait_for(state="visible", timeout=10_000)
    except PlaywrightTimeoutError:
        # Syncfusion sometimes paints after network idle — short buffer
        page.wait_for_timeout(1500)


def wait_market_watch_rows_ready(page: Page, timeout: float = 30_000) -> None:
    market_watch_table(page).wait_for(state="visible", timeout=timeout)
    rows = market_watch_rows(page)
    rows.first.wait_for(state="visible", timeout=timeout)
    deadline = time.time() + timeout / 1000.0
    while time.time() < deadline:
        if rows.count() == 0:
            page.wait_for_timeout(200)
            continue
        try:
            txt = market_row_ticker_cell(rows.first).inner_text(timeout=2_000).strip()
            if txt:
                return
        except Exception:
            pass
        page.wait_for_timeout(200)
