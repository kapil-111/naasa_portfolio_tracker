"""
Centralized NAASA X / wallet URLs and Playwright locators with fallback chains.

Update fallbacks here when the broker changes markup. UI last verified in-repo: 2026-04.
"""
from __future__ import annotations

import time
from typing import Literal, Optional, Tuple

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
# Prefer .sl_by (original NAASA order form) first so .first never grabs another "BUY"/"SELL" on the page.
def order_side_buy(page: Page) -> Locator:
    return page.locator(".sl_by a:has-text('BUY')").or_(page.get_by_role("link", name="BUY"))


def order_side_sell(page: Page) -> Locator:
    return page.locator(".sl_by a:has-text('SELL')").or_(page.get_by_role("link", name="SELL"))


def order_symbol_input(page: Page) -> Locator:
    return page.locator("#searchStock").or_(page.locator("input#searchStock"))


def order_type_mkt(page: Page) -> Locator:
    # Keep label first (original behavior); generic "MKT" text is fallback only.
    return page.locator("label:has-text('MKT')").or_(page.get_by_text("MKT", exact=True))


def order_quantity_input(page: Page) -> Locator:
    return page.locator("#OrdertxtQty")


def order_submit_button(page: Page) -> Locator:
    return page.locator("#btnBuy")


def dismiss_any_confirmation(page: Page, timeout_ms: int = 3_000) -> bool:
    """
    Generic confirmation handler — works regardless of NAASA UI changes.
    Looks for any visible modal/dialog overlay and clicks the most affirmative button.
    Returns True if a confirmation was clicked, False if none found.
    """
    # Broad selector: any visible modal/dialog/overlay element
    dialog_roots = [
        "div[role='dialog']",
        "div[role='alertdialog']",
        ".modal",
        "[class*='modal']",
        "[class*='dialog']",
        "[class*='confirm']",
        "[class*='popup']",
        "[class*='overlay']",
    ]
    # Priority order for confirm button text
    confirm_texts = ["Yes", "Confirm", "OK", "Proceed", "Accept", "Place Order", "Submit"]

    deadline = time.time() + timeout_ms / 1000.0
    while time.time() < deadline:
        for root_sel in dialog_roots:
            try:
                roots = page.locator(root_sel)
                if roots.count() == 0:
                    continue
                for i in range(roots.count()):
                    root = roots.nth(i)
                    if not root.is_visible():
                        continue
                    # Try each confirm text in priority order
                    for text in confirm_texts:
                        try:
                            btn = root.get_by_role("button", name=text, exact=False)
                            if btn.count() > 0 and btn.first.is_visible():
                                print(f"[CONFIRM] Detected dialog '{root_sel}' — clicking '{text}' button")
                                btn.first.click()
                                return True
                        except Exception:
                            continue
            except Exception:
                continue
        page.wait_for_timeout(150)
    return False


def order_error_indicators(page: Page) -> Locator:
    """Visible broker error / rejection UI after submit (toasts, alerts, validation)."""
    return page.locator(
        ".alert-danger, .toast-error, .toast-danger, .invalid-feedback, .text-danger"
    )


def poll_order_submission_outcome(
    page: Page, timeout_ms: float = 8_000
) -> Tuple[Literal["success", "failure", "timeout"], Optional[str]]:
    """
    NAASA X shows no UI on success — the qty field silently resets to empty.
    Strategy:
      1. Watch for a visible error indicator → failure.
      2. Watch for qty field to clear (value becomes empty) → success.
      3. If neither happens within timeout → unconfirmed (caller treats as unknown).
    """
    deadline = time.time() + timeout_ms / 1000.0
    error_loc = order_error_indicators(page)
    qty_loc = order_quantity_input(page)

    def _safe_visible_first(loc: Locator) -> bool:
        try:
            if loc.count() == 0:
                return False
            return loc.first.is_visible()
        except Exception:
            return False

    def _safe_inner(loc: Locator) -> str:
        try:
            if loc.count() == 0:
                return ""
            t = loc.first.inner_text(timeout=800).strip()
            return t
        except Exception:
            return ""

    price_loc = page.locator("#OrdertxtPrice")

    def _form_reset() -> bool:
        """Both qty and price fields clear simultaneously on ErrorCode == 0."""
        try:
            qty_val = qty_loc.first.input_value(timeout=500).strip() if qty_loc.count() > 0 else None
            price_val = price_loc.first.input_value(timeout=500).strip() if price_loc.count() > 0 else None
            # Either field clearing alone is enough — MKT orders may not use price
            return qty_val == "" or price_val == ""
        except Exception:
            return False

    while time.time() < deadline:
        if _safe_visible_first(error_loc):
            return ("failure", _safe_inner(error_loc) or "Broker reported an error.")
        if _form_reset():
            return ("success", None)
        page.wait_for_timeout(150)

    return ("timeout", None)


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


def goto_broker_page(page: Page, url: str, timeout: float = 30_000) -> None:
    """
    Navigate without waiting for the full window `load` event.

    The broker pages are often interactive before every asset finishes loading,
    and some sessions appear to keep requests open long enough for Playwright's
    default `wait_until="load"` to timeout. Callers should wait for the specific
    element they need after navigation.
    """
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=timeout)
    except PlaywrightTimeoutError:
        # Last-resort fallback for pages that never reach DOMContentLoaded cleanly.
        page.goto(url, wait_until="commit", timeout=10_000)


def wait_for_order_page(page: Page, timeout: float = 30_000) -> None:
    order_symbol_input(page).wait_for(state="visible", timeout=timeout)


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
