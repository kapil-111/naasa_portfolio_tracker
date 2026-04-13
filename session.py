"""
Detect NAASA / Keycloak login page so we fail closed instead of trading on bad data.
"""
from playwright.sync_api import Page


class SessionExpiredError(RuntimeError):
    """Broker session lost: login form visible or auth URL, not an authenticated app view."""


def is_login_page(page: Page) -> bool:
    """
    True if the current page looks like a login / SSO screen (session expired or never logged in).
    """
    url = (page.url or "").lower()
    if not url or url == "about:blank":
        return False

    url_markers = (
        "/login",
        "openid",
        "keycloak",
        "signin",
        "/protocol/",
        "/auth/realms",
        "/oauth/",
        "identity.",
    )
    if any(m in url for m in url_markers):
        return True

    try:
        user = page.locator("#username")
        if user.count() == 0:
            return False
        if not user.first.is_visible(timeout=1500):
            return False
        pw = page.locator("#login-password").or_(page.locator("input[type='password'][name='password']"))
        if pw.count() > 0 and pw.first.is_visible(timeout=800):
            return True
    except Exception:
        pass

    return False


def raise_if_login_page(page: Page, context: str) -> None:
    """Raise SessionExpiredError if the login page is shown; save a screenshot for debugging."""
    if is_login_page(page):
        path = "session_expired.png"
        try:
            page.screenshot(path=path)
        except Exception:
            path = "(screenshot failed)"
        raise SessionExpiredError(
            f"Not authenticated or session expired ({context}). "
            f"URL={page.url!r} — see {path}"
        )
