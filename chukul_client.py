"""
chukul_client.py

Shared HTTP client for Chukul API.
Provides unauthenticated _get() and authenticated _session_get() with lazy login.
All fetch modules import from here — no duplicate HEADERS/BASE_URL/_get() definitions.
"""

import os
import requests
from dotenv import load_dotenv

load_dotenv()

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}
BASE_URL = "https://chukul.com/api"

# Authenticated session — populated after _chukul_login()
_session = requests.Session()
_session.headers.update(HEADERS)
_logged_in = False


def _chukul_login():
    """
    Log in to Chukul and attach session cookies for authenticated endpoints.
    Called automatically before any authenticated request.
    """
    global _logged_in
    if _logged_in:
        return True
    username = os.getenv("CHUKUL_USERNAME")
    password = os.getenv("CHUKUL_PASSWORD")
    if not username or not password:
        print("Warning: CHUKUL_USERNAME/CHUKUL_PASSWORD not set. Authenticated endpoints will be skipped.")
        return False
    try:
        resp = _session.post(
            f"{BASE_URL}/auth/login/",
            json={"username": username, "password": password},
            timeout=15
        )
        if resp.status_code in (200, 201):
            _logged_in = True
            print("Chukul login successful.")
            return True
        else:
            print(f"Chukul login failed: HTTP {resp.status_code}")
            return False
    except Exception as e:
        print(f"Chukul login error: {e}")
        return False


def _get(url, params=None):
    """Unauthenticated GET with error handling."""
    try:
        resp = requests.get(url, headers=HEADERS, params=params, timeout=15)
        if resp.status_code == 200:
            return resp.json()
    except Exception:
        pass
    return None


def _session_get(url, params=None):
    """Authenticated GET using the logged-in session."""
    if not _chukul_login():
        return None
    try:
        resp = _session.get(url, params=params, timeout=15)
        if resp.status_code == 200:
            return resp.json()
    except Exception:
        pass
    return None
