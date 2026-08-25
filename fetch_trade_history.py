"""
Scrape NAASA trade history and compute weighted-average buy prices.

Tries known TradeBook report variants to find the one with historical
filled BUY trades, then calculates weighted avg for each symbol and
merges into avg_prices.json (only for symbols NOT already present).

Run standalone:
    python fetch_trade_history.py [--overwrite] [--dry-run]

  --overwrite  Replace existing avg_prices.json entries (default: skip)
  --dry-run    Print computed prices but don't write to avg_prices.json
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from collections import defaultdict
from datetime import date, timedelta

from dotenv import load_dotenv
from playwright.sync_api import sync_playwright

from auth import login
from naasa_locators import NAASA_BASE, goto_broker_page
from scraper import parse_holding_grid

_AVG_PRICES_FILE = "avg_prices.json"

# 2026-08 site redesign moved these off /TradeBook?Report=X to dedicated pages, and
# folded CONTRACTNOTE/TRADEBOOK's old distinction into one "Trade Book" page.
_REPORT_CANDIDATES = [
    "reports/trade-book",   # was CONTRACTNOTE/TRADEBOOK -- full historical filled trades
    "reports/order-book",   # was ORDERBOOK -- today-only
]

# Column name aliases — cover legacy CONTRACTNOTE headers, the new Trade Book page's
# headers (Symbol/Type/Quantity/Price/Amount), and generic variants.
_COL_SYMBOL  = ("SYMBOL", "SYM", "Scrip", "Symbol", "Stock Symbol", "Script", "scrip", "symbol")
_COL_SIDE    = ("TYPE", "Type", "BuySellText", "Buy/Sell", "Side", "OrderType", "Order Type")
_COL_PRICE   = ("PRICE", "Price", "Rate", "Trade Price", "Traded Price", "TradePrice")
_COL_QTY     = ("QTY", "TRADED QTY", "TradedQuantity", "Traded Qty", "Traded Quantity", "Quantity", "Qty", "Executed Qty")


def _get(row: dict, keys: tuple) -> str:
    for k in keys:
        v = row.get(k)
        if v is not None and str(v).strip():
            return str(v).strip()
    return ""


def _float(val: str) -> float:
    try:
        return float(str(val).replace(",", ""))
    except (ValueError, TypeError):
        return 0.0


def _int(val: str) -> int:
    try:
        return int(float(str(val).replace(",", "")))
    except (ValueError, TypeError):
        return 0


def _load_avg_prices() -> dict:
    try:
        with open(_AVG_PRICES_FILE) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def _save_avg_prices(prices: dict) -> None:
    with open(_AVG_PRICES_FILE, "w") as f:
        json.dump(prices, f, indent=4, sort_keys=True)


def _fill_date_and_generate(page, from_date: str, to_date: str) -> None:
    """Fill #frmDate / #todate (DD-MM-YYYY) and trigger search. Legacy Syncfusion
    report pages only -- a harmless no-op on the redesigned pages below."""
    try:
        frm = page.locator("#frmDate")
        tod = page.locator("#todate")
        if frm.count() > 0 and frm.first.is_visible():
            frm.first.fill(from_date)
            page.wait_for_timeout(300)
        if tod.count() > 0 and tod.first.is_visible():
            tod.first.fill(to_date)
            page.wait_for_timeout(300)
        # Press Enter on the to-date field to trigger search
        if tod.count() > 0:
            tod.first.press("Enter")
            page.wait_for_timeout(2000)
    except Exception as e:
        print(f"  [date form] {e}")


_MONTH_NAMES = ["January", "February", "March", "April", "May", "June",
                "July", "August", "September", "October", "November", "December"]
_MONTH_HEADER_RE = re.compile(r'^(' + '|'.join(_MONTH_NAMES) + r') (\d{4})$')


def _ordinal(n: int) -> str:
    if 11 <= n % 100 <= 13:
        return f"{n}th"
    suffix = {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th")
    return f"{n}{suffix}"


def _day_aria_label(d: date) -> str:
    # Matches react-day-picker's default aria-label format, e.g.
    # "Tuesday, August 25th, 2026" -- confirmed against the live site 2026-08.
    return f"{d.strftime('%A')}, {_MONTH_NAMES[d.month - 1]} {_ordinal(d.day)}, {d.year}"


def _visible_calendar_months(page) -> list[tuple[int, int]]:
    """(month, year) pairs for the month headers currently shown in an open
    date-range popover, in left-to-right order."""
    texts = page.evaluate(
        "() => Array.from(document.querySelectorAll('*'))"
        ".filter(el => el.children.length === 0)"
        ".map(el => el.textContent.trim())"
    )
    months = []
    for t in texts:
        m = _MONTH_HEADER_RE.match(t)
        if m:
            months.append((_MONTH_NAMES.index(m.group(1)) + 1, int(m.group(2))))
    return months


def _select_new_calendar_range(page, from_date: date, to_date: date) -> bool:
    """
    Select a date range in the redesigned (2026-08) shadcn/react-day-picker range
    picker. Returns False on any structural mismatch rather than raising, so a
    future markup change degrades to "no rows found" (same as before this fix)
    instead of crashing the scrape.
    """
    try:
        trigger = page.get_by_text("Pick a date range", exact=False)
        if trigger.count() == 0:
            return False
        trigger.first.click()
        page.wait_for_timeout(600)

        prev_btn = page.get_by_label("Go to the Previous Month")
        next_btn = page.get_by_label("Go to the Next Month")

        def _navigate_to(target: date) -> bool:
            months = _visible_calendar_months(page)
            if not months:
                return False
            m0, y0 = months[0]
            delta = (target.year - y0) * 12 + (target.month - m0)
            btn = prev_btn if delta < 0 else next_btn
            for _ in range(abs(delta)):
                btn.click()
                page.wait_for_timeout(150)
            return True

        def _click_day(d: date) -> bool:
            # Scoped to the specific month's named grid, not the whole page --
            # react-day-picker renders leading/trailing overflow days from the
            # adjacent month at each grid's edges, so the same date can appear
            # (with the identical aria-label) in two visible grids at once.
            grid = page.get_by_role("grid", name=_MONTH_NAMES[d.month - 1])
            if grid.count() == 0:
                return False
            label = grid.first.get_by_label(_day_aria_label(d), exact=True)
            if label.count() == 0:
                return False
            label.first.click()
            return True

        if not _navigate_to(from_date) or not _click_day(from_date):
            return False
        page.wait_for_timeout(300)

        if not _navigate_to(to_date) or not _click_day(to_date):
            return False
        page.wait_for_timeout(300)

        apply_btn = page.get_by_text("Apply", exact=True)
        if apply_btn.count() == 0:
            return False
        apply_btn.first.click()
        page.wait_for_timeout(2500)
        return True
    except Exception as e:
        print(f"  [new date range] {e}")
        return False


def _try_report(page, report: str, from_date: str, to_date: str,
                 from_date_d: date | None = None, to_date_d: date | None = None) -> list[dict]:
    url = f"{NAASA_BASE}/{report}"
    print(f"  Trying {url} ...")
    try:
        goto_broker_page(page, url)
        page.wait_for_timeout(3000)
        _fill_date_and_generate(page, from_date, to_date)  # legacy pages; no-op on the new ones
        if from_date_d and to_date_d:
            _select_new_calendar_range(page, from_date_d, to_date_d)
        rows = parse_holding_grid(page, grid_timeout=45000)
        if not rows:
            return []
        first = rows[0]
        has_price = any(first.get(k) for k in _COL_PRICE)
        has_qty   = any(first.get(k) for k in _COL_QTY)
        if has_price and has_qty:
            print(f"  => Found {len(rows)} rows with price+qty columns.")
            return rows
        print(f"  => Rows found but missing price/qty columns: {list(first.keys())}")
        return []
    except Exception as e:
        print(f"  => Error: {e}")
        return []


_COL_STATUS = ("STATUS", "Status", "StatusText")


def compute_avg_prices(rows: list[dict]) -> dict[str, float]:
    """
    Compute weighted average BUY price per symbol from trade rows.
    Only counts COMPLETE (fully or partially filled) BUY orders.
    Returns {symbol: avg_price}.
    """
    totals: dict[str, list] = defaultdict(lambda: [0.0, 0])  # [cost_sum, qty_sum]

    for row in rows:
        side = _get(row, _COL_SIDE).upper()
        if "BUY" not in side:
            continue
        traded_qty = _int(_get(row, ("TRADED QTY", "TradedQuantity", "Traded Qty", "Traded Quantity")))
        status = _get(row, _COL_STATUS).upper()
        # Allow CANCELLED rows if shares were actually traded (partial fill then cancelled at close)
        if status and "COMPLETE" not in status and "PARTIAL" not in status and traded_qty == 0:
            continue
        symbol = _get(row, _COL_SYMBOL)
        if not symbol or symbol.lower().startswith("total"):
            continue
        price = _float(_get(row, _COL_PRICE))
        qty = traded_qty if traded_qty > 0 else _int(_get(row, _COL_QTY))
        if price <= 0 or qty <= 0:
            continue
        totals[symbol][0] += price * qty
        totals[symbol][1] += qty

    return {
        sym: round(cost / qty, 2)
        for sym, (cost, qty) in totals.items()
        if qty > 0
    }


def scrape_trade_history_avg_prices(page, days: int = 180) -> dict[str, float]:
    """
    Use an already-authenticated Playwright page to scrape ORDERBOOK trade history
    and return {symbol: weighted_avg_buy_price}. Does NOT write to avg_prices.json.
    Called by main.py to auto-backfill missing avg prices.
    """
    today       = date.today()
    from_date_d = today - timedelta(days=days)
    to_date     = today.strftime("%d-%m-%Y")
    from_date   = from_date_d.strftime("%d-%m-%Y")
    for report in _REPORT_CANDIDATES:
        rows = _try_report(page, report, from_date, to_date, from_date_d, today)
        if rows:
            return compute_avg_prices(rows)
    return {}


def main():
    parser = argparse.ArgumentParser(description="Scrape NAASA trade history → avg_prices.json")
    parser.add_argument("--overwrite",  action="store_true", help="Overwrite existing entries")
    parser.add_argument("--dry-run",    action="store_true", help="Print only, don't save")
    parser.add_argument("--from-date",  default=None, help="From date MM/DD/YYYY (default: 180 days ago)")
    parser.add_argument("--to-date",    default=None, help="To date MM/DD/YYYY (default: today)")
    args = parser.parse_args()

    today     = date.today()
    to_date   = args.to_date   or today.strftime("%d-%m-%Y")
    from_date = args.from_date or (today - timedelta(days=180)).strftime("%d-%m-%Y")
    # Date objects for the new calendar picker -- only when using the defaults, since
    # --from-date/--to-date accept arbitrary CLI string formats we don't parse back.
    to_date_d   = None if args.to_date   else today
    from_date_d = None if args.from_date else today - timedelta(days=180)

    load_dotenv()
    username = os.getenv("NAASA_USERNAME")
    password = os.getenv("NAASA_PASSWORD")
    if not username or not password:
        print("Error: NAASA_USERNAME or NAASA_PASSWORD not set in .env")
        sys.exit(1)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        page = browser.new_page()

        print("Logging in...")
        login(page, username, password)
        print("Login OK.\n")

        print(f"Date range: {from_date} → {to_date}\n")
        rows: list[dict] = []
        matched_report = None
        for report in _REPORT_CANDIDATES:
            rows = _try_report(page, report, from_date, to_date, from_date_d, to_date_d)
            if rows:
                matched_report = report
                break

        browser.close()

    if not rows:
        print("\nCould not find trade history in any known report URL.")
        print("Try opening the NAASA portal manually and check what Report= param")
        print("the trade history page uses, then add it to _REPORT_CANDIDATES.")
        sys.exit(1)

    print(f"\nUsing report: {matched_report} ({len(rows)} rows)")
    computed = compute_avg_prices(rows)

    if not computed:
        print("No BUY trades found in scraped rows.")
        sys.exit(0)

    print(f"\nComputed avg buy prices ({len(computed)} symbols):")
    for sym, price in sorted(computed.items()):
        print(f"  {sym}: {price:.2f}")

    if args.dry_run:
        print("\n[dry-run] Not writing to avg_prices.json")
        return

    existing = _load_avg_prices()
    updated = 0
    skipped = 0
    for sym, price in computed.items():
        if sym in existing and not args.overwrite:
            print(f"  SKIP {sym} (already in avg_prices.json = {existing[sym]})")
            skipped += 1
        else:
            action = "UPDATE" if sym in existing else "ADD"
            print(f"  {action} {sym}: {price:.2f}")
            existing[sym] = price
            updated += 1

    _save_avg_prices(existing)
    print(f"\nDone. {updated} updated, {skipped} skipped. avg_prices.json saved.")


if __name__ == "__main__":
    main()
