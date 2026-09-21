from playwright.sync_api import Page

from naasa_locators import (
    goto_broker_page,
    login_password,
    login_submit,
    login_username,
    NAASA_BASE,
    naasa_home,
    wait_for_login_form,
)
from session import is_login_url, raise_if_login_page


def _is_logged_in_url(url: str) -> bool:
    return url.startswith(NAASA_BASE) and not is_login_url(url)


def login(page: Page, username, password):
    """
    Logs into Naasa Securities using the provided credentials.
    """
    print("Navigating to login page...")
    goto_broker_page(page, naasa_home())
    print("Waiting for login form...")
    wait_for_login_form(page)
    print("Entering credentials...")
    login_username(page).fill(username)
    login_password(page).fill(password)
    print("Submitting login...")
    login_submit(page).click()
    print("Waiting for dashboard...")
    try:
        # Not a fixed /Home/Dashboard glob: the 2026-08 redesign moved the landing URL.
        # Logged in == back on the broker host and off any login / SSO endpoint.
        page.wait_for_url(_is_logged_in_url, timeout=15000)
        print(f"Login successful! Reached {page.url}")
    except Exception as e:
        print(f"Warning: Did not detect a post-login URL. Current URL: {page.url}")
        page.screenshot(path="login_debug.png")
    raise_if_login_page(page, "login")
