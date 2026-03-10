from playwright.sync_api import Page
import time

def login(page: Page, username, password):
    """
    Logs into Naasa Securities using the provided credentials.
    """
    print("Navigating to login page...")
    # The main URL redirects to the auth page
    page.goto("https://x.naasasecurities.com.np/")
    
    # Wait for the login form elements to appear
    # Based on exploration: #username, #login-password, #kc-login
    print("Waiting for login form...")
    page.wait_for_selector("#username")
    
    print("Entering credentials...")
    page.fill("#username", username)
    page.fill("#login-password", password)
    
    # Check "Remember me" if desired
    # page.click("#rememberMe") 
    
    print("Submitting login...")
    page.click("#kc-login")
    
    # Wait for navigation to dashboard or some indication of success
    # We expect to be redirected back to x.naasasecurities.com.np
    print("Waiting for dashboard...")
    try:
        page.wait_for_url("**/Home/Dashboard", timeout=15000)
        print("Login successful! Reached Dashboard.")
    except Exception as e:
        print(f"Warning: Did not detect Dashboard URL immediately. Current URL: {page.url}")
        # Take a screenshot for debugging if login fails
        page.screenshot(path="login_debug.png")
