from playwright.sync_api import Page

def login(page: Page, username, password):
    """
    Logs into Naasa Securities using the provided credentials.
    """
    print("Navigating to login page...")
    page.goto("https://x.naasasecurities.com.np/")
    print("Waiting for login form...")
    page.wait_for_selector("#username")
    print("Entering credentials...")
    page.fill("#username", username)
    page.fill("#login-password", password)
    print("Submitting login...")
    page.click("#kc-login")
    print("Waiting for dashboard...")
    try:
        page.wait_for_url("**/Home/Dashboard", timeout=15000)
        print("Login successful! Reached Dashboard.")
    except Exception as e:
        print(f"Warning: Did not detect Dashboard URL immediately. Current URL: {page.url}")
        page.screenshot(path="login_debug.png")
