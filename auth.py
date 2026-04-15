from playwright.sync_api import Page

from naasa_locators import (
    dashboard_url_glob,
    goto_broker_page,
    login_password,
    login_submit,
    login_username,
    naasa_home,
    wait_for_login_form,
)
from session import raise_if_login_page


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
        page.wait_for_url(dashboard_url_glob(), timeout=15000)
        print("Login successful! Reached Dashboard.")
    except Exception as e:
        print(f"Warning: Did not detect Dashboard URL immediately. Current URL: {page.url}")
        page.screenshot(path="login_debug.png")
    raise_if_login_page(page, "login")
