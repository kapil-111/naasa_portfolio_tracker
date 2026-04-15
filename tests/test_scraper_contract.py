"""Contract tests: local HTML fixtures mimic NAASA DOM; no live broker."""
from pathlib import Path
from unittest.mock import Mock

import pytest
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError, sync_playwright

from scraper import parse_holding_grid
from naasa_locators import (
    goto_broker_page,
    market_row_ltp_cell,
    market_row_ticker_cell,
    market_watch_rows,
    wait_market_watch_rows_ready,
)

_FIXTURES = Path(__file__).resolve().parent / "fixtures"


def _file_url(name: str) -> str:
    return (_FIXTURES / name).as_uri()


@pytest.fixture(scope="module")
def chromium_page():
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page()
        yield page
        browser.close()


def test_parse_holding_grid_syncfusion_fixture(chromium_page):
    chromium_page.goto(_file_url("holding_report.html"))
    rows = parse_holding_grid(chromium_page)
    assert len(rows) == 2  # excludes per-page "Total :" footer row
    assert rows[0]["Symbol"] == "ABC"
    assert rows[0]["Qty"] == "10"
    assert rows[1]["Symbol"] == "XYZ"


def test_parse_holding_grid_no_data(chromium_page):
    chromium_page.goto(_file_url("holding_report_no_data.html"))
    rows = parse_holding_grid(chromium_page)
    assert rows == []


def test_market_watch_rows_fixture(chromium_page):
    chromium_page.goto(_file_url("market_watch.html"))
    wait_market_watch_rows_ready(chromium_page, timeout=5000)
    rows = market_watch_rows(chromium_page)
    assert rows.count() == 2
    assert market_row_ticker_cell(rows.first).inner_text().strip() == "TEST"
    assert market_row_ltp_cell(rows.first).inner_text().strip() == "1,234.50"


def test_goto_broker_page_falls_back_from_domcontentloaded_timeout():
    page = Mock()
    page.goto.side_effect = [
        PlaywrightTimeoutError("domcontentloaded timeout"),
        None,
    ]

    goto_broker_page(page, "https://example.test/path", timeout=12345)

    assert page.goto.call_args_list[0].kwargs == {
        "wait_until": "domcontentloaded",
        "timeout": 12345,
    }
    assert page.goto.call_args_list[0].args == ("https://example.test/path",)
    assert page.goto.call_args_list[1].kwargs == {
        "wait_until": "commit",
        "timeout": 10_000,
    }
    assert page.goto.call_args_list[1].args == ("https://example.test/path",)
