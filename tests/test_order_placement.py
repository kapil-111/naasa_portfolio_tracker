"""
Order-placement contract tests. A synthetic order form is served at the real order URL via
request interception; every other request is aborted, so nothing reaches the broker.
"""
import json
import re
from pathlib import Path

import pytest
from playwright.sync_api import sync_playwright

import telegram_commands
import trader as trader_mod
from auth import _is_logged_in_url
from naasa_locators import new_order_errors
from session import is_login_url

_ROOT = Path(__file__).resolve().parent.parent
_FORM_HTML = (Path(__file__).resolve().parent / "fixtures" / "order_form.html").read_text()
_ORDER_URL = "https://x.naasasecurities.com.np/MarketOrder/Order"


@pytest.fixture(scope="module")
def browser():
    with sync_playwright() as p:
        b = p.chromium.launch()
        yield b
        b.close()


@pytest.fixture
def order_page(browser, monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)  # Trader writes order_*.png relative to cwd
    monkeypatch.setattr(trader_mod, "notify_order_screenshot", lambda *a, **k: None)
    # The fixture never shows a confirmation dialog; skip its 3s polling window.
    monkeypatch.setattr(trader_mod, "dismiss_any_confirmation", lambda *a, **k: False)
    page = browser.new_page()
    page.route("**/*", lambda route: route.abort())  # no network, ever
    page.route(re.compile(r".*/MarketOrder/Order.*"),
               lambda route: route.fulfill(body=_FORM_HTML, content_type="text/html"))

    def open_form(**params):
        page.goto(_ORDER_URL + "?" + "&".join(f"{k}={v}" for k, v in params.items()))
        return page

    yield open_form
    page.close()


def _place(page, dry_run, side="BUY"):
    t = trader_mod.Trader(page, dry_run=dry_run)
    ok = t.place_order({"symbol": "NABIL", "side": side, "quantity": 50})
    state = page.evaluate("({submits: __submits, side: __side, sym: __sym, mkt: __mkt})")
    return ok, t, state


def test_dry_run_fills_form_but_never_submits(order_page):
    ok, t, st = _place(order_page(mode="success"), dry_run=True)
    assert ok is True and t.last_outcome is None
    assert st == {"submits": 0, "side": "BUY", "sym": "NABIL", "mkt": True}


@pytest.mark.parametrize("side", ["BUY", "SELL"])
def test_live_order_accepted_when_quantity_resets(order_page, side):
    ok, t, st = _place(order_page(mode="success"), dry_run=False, side=side)
    assert ok is True and t.last_outcome == "success"
    assert st["submits"] == 1 and st["side"] == side and st["mkt"] is True


def test_live_order_rejection_is_failure(order_page):
    ok, t, _ = _place(order_page(mode="failure"), dry_run=False)
    assert ok is False and t.last_outcome == "failure"
    assert t.last_error == "Insufficient collateral"


def test_rejection_detected_behind_hidden_error_placeholders(order_page):
    # Regression: a hidden .invalid-feedback/.text-danger earlier in the DOM used to make
    # err_loc.first invisible, so a real rejection was reported as "unconfirmed".
    ok, t, _ = _place(order_page(mode="failure", hidden=1), dry_run=False)
    assert ok is False and t.last_outcome == "failure"
    assert t.last_error == "Insufficient collateral"


def test_visible_red_label_is_not_a_rejection(order_page):
    # Regression: an accepted order on a falling stock (red % label) used to be reported as a
    # failure, which made the bot drop its record and re-send a duplicate real order.
    ok, t, st = _place(order_page(mode="success", label="red"), dry_run=False)
    assert ok is True and t.last_outcome == "success" and st["submits"] == 1


def test_no_feedback_is_unconfirmed_not_failure(order_page):
    ok, t, _ = _place(order_page(mode="unconfirmed"), dry_run=False)
    assert ok is False and t.last_outcome == "unconfirmed"


def test_preexisting_identical_error_stays_conservative(order_page):
    # Documented limitation: an identical stale error can't be told apart from a new one, so
    # the outcome is "unconfirmed" (keeps the order record, asks for a manual check) — never "success".
    ok, t, _ = _place(order_page(mode="failure", stale=1), dry_run=False)
    assert ok is False and t.last_outcome == "unconfirmed"


def test_new_order_errors_is_a_multiset_diff():
    assert new_order_errors([], ["x"]) == ["x"]
    assert new_order_errors(["-1.2%"], ["-1.2%"]) == []
    assert new_order_errors(["-1.2%"], ["-1.2%", "Rejected"]) == ["Rejected"]
    assert new_order_errors(["a"], ["a", "a"]) == ["a"]


@pytest.mark.parametrize("url,expected", [
    ("https://x.naasasecurities.com.np/", True),
    ("https://x.naasasecurities.com.np/dashboard", True),
    ("https://x.naasasecurities.com.np/Home/Dashboard", True),
    ("https://x.naasasecurities.com.np/login", False),
    ("https://id.example.com/realms/x/protocol/openid-connect/auth", False),
    ("about:blank", False),
])
def test_logged_in_url_predicate(url, expected):
    assert _is_logged_in_url(url) is expected


def test_is_login_url_markers():
    assert is_login_url("https://h/auth/realms/naasa/protocol/openid-connect/auth")
    assert not is_login_url("https://x.naasasecurities.com.np/MarketOrder/Order")


def test_main_never_hardcodes_a_live_trader():
    # Regression: the TEST_ORDER branch built Trader(page, dry_run=False) and ignored DRY_RUN.
    src = (_ROOT / "main.py").read_text()
    assert "dry_run=False" not in src


# --- Telegram manual orders -------------------------------------------------------------

class _FakeTrader:
    def __init__(self, ok=True, outcome=None, error=""):
        self.ok, self.last_outcome, self.last_error = ok, outcome, error
        self.orders = []

    def place_order(self, signal):
        self.orders.append(signal)
        return self.ok


@pytest.fixture
def tg(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    replies = []
    monkeypatch.setattr(telegram_commands, "_reply", replies.append)
    return replies


def _recorded():
    from state_manager import load_placed_orders
    return load_placed_orders()["orders"]


def test_manual_buy_is_recorded_and_labelled_market(tg):
    t = _FakeTrader()
    telegram_commands._handle_buy(["/buy", "NABIL", "10", "500"], None, t, {}, {"holdings": []}, 100_000, False)
    assert _recorded() == [{"symbol": "NABIL", "side": "BUY", "type": "MANUAL", "quantity": 10}]
    assert "MARKET" in tg[0] and "not a limit" in tg[0]
    assert "MARKET order placed" in tg[-1]


def test_manual_buy_rejected_when_over_available_fund(tg):
    t = _FakeTrader()
    telegram_commands._handle_buy(["/buy", "NABIL", "10", "500"], None, t, {}, {"holdings": []}, 4_000, False)
    assert t.orders == [] and _recorded() == []
    assert "available fund" in tg[-1]


def test_manual_buy_definite_failure_drops_the_record(tg):
    t = _FakeTrader(ok=False, outcome="failure", error="Insufficient collateral")
    telegram_commands._handle_buy(["/buy", "NABIL", "10", "500"], None, t, {}, {"holdings": []}, None, False)
    assert _recorded() == []
    assert "failed" in tg[-1] and "Insufficient collateral" in tg[-1]


def test_manual_sell_unconfirmed_keeps_record_and_warns(tg):
    t = _FakeTrader(ok=False, outcome="unconfirmed")
    states = {"HFIN": {"in_position": True}}
    telegram_commands._handle_sell(["/sell", "HFIN", "10", "750"], None, t, states, {"holdings": []}, False)
    assert _recorded() == [{"symbol": "HFIN", "side": "SELL", "type": "MANUAL", "quantity": 10}]
    assert "UNCONFIRMED" in tg[-1]
    assert states["HFIN"]["in_position"] is True  # not marked exited on an unconfirmed order


def test_failed_retry_does_not_erase_an_earlier_real_manual_order(tg):
    ok = _FakeTrader()
    telegram_commands._handle_buy(["/buy", "NABIL", "10", "500"], None, ok, {}, {"holdings": []}, None, False)
    bad = _FakeTrader(ok=False, outcome="failure")
    telegram_commands._handle_buy(["/buy", "NABIL", "10", "500"], None, bad, {}, {"holdings": []}, None, False)
    assert len(_recorded()) == 1


def test_help_text_says_orders_are_market():
    assert "MARKET" in telegram_commands.HELP_TEXT and "NOT a limit" in telegram_commands.HELP_TEXT
