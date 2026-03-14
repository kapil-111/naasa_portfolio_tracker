"""
chukul_client.py

Shared HTTP client for Chukul API.
Provides unauthenticated _get() and authenticated _session_get() with lazy login.
All fetch modules import from here — no duplicate HEADERS/BASE_URL/_get() definitions.
"""

import os
import re
import threading
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
_login_failed = False
_login_lock = threading.Lock()


def _chukul_login():
    """
    Log in to Chukul and attach session cookies for authenticated endpoints.
    Thread-safe: only one login attempt per process; failure is cached.
    """
    global _logged_in, _login_failed
    if _logged_in:
        return True
    if _login_failed:
        return False
    with _login_lock:
        # Re-check inside lock — another thread may have just finished
        if _logged_in:
            return True
        if _login_failed:
            return False
        username = os.getenv("CHUKUL_USERNAME")
        password = os.getenv("CHUKUL_PASSWORD")
        if not username or not password:
            print("Warning: CHUKUL_USERNAME/CHUKUL_PASSWORD not set. Broker endpoints skipped.")
            _login_failed = True
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
                print(f"Chukul login failed: HTTP {resp.status_code}. Broker endpoints skipped.")
                _login_failed = True
                return False
        except Exception as e:
            print(f"Chukul login error: {e}. Broker endpoints skipped.")
            _login_failed = True
            return False


_EXCLUDE_EXACT = {
    'BANKING', 'BANKINGIND', 'DEBENTURE', 'DEVBANK', 'DEVBANKIND',
    'FINANCE', 'FINANCEIND', 'FLOATIND', 'HOTELIND', 'HOTELS',
    'HYDRO', 'HYDROPOWIND', 'INVESTMENT', 'INVIDX', 'LIFEINSU',
    'LIFEINSUIND', 'MANUFACTURE', 'MANUFACTUREIND', 'MICROFINANCE',
    'MICROFININD', 'MUTUAL', 'MUTUALIND', 'NEPSE', 'NONLIFEIND',
    'NONLIFEINSU', 'OTHERS', 'OTHERSIND', 'SENSFLTIND', 'SENSIND',
    'TRADINGIND', 'TRDIND', 'PROMOTER',
}


def _is_common_share(sym):
    """Return True only for regular tradeable common shares."""
    if sym in _EXCLUDE_EXACT:
        return False
    # Special characters → debentures or junk
    if '/' in sym or '%' in sym or ' ' in sym:
        return False
    # Starts with digit → debenture (e.g. 10CBD90, 9%CSB82)
    if sym[0].isdigit():
        return False
    # Promoter shares → end with P (ALICLP, NABILP, etc.)
    if sym.endswith('P') and not sym.endswith('PO'):
        return False
    # Rights / public offerings → end with PO (APEXPO, BFCLPO, etc.)
    if sym.endswith('PO'):
        return False
    # Debentures → end with D + 2–4 digits (EBLD86, BOKD2085, etc.)
    if re.search(r'D\d{2,4}$', sym):
        return False
    # B-series bonds → end with B + 2–4 digits (ADBLB86, HBLB86, EBLEB89, etc.)
    if re.search(r'B\d{2,4}$', sym):
        return False
    # Year-based debentures → end with 4-digit year 20XX (CSB2084, etc.)
    if re.search(r'20\d{2}$', sym):
        return False
    # Mutual fund units → end with F + digit(s) (CMF1, LVF2, NMBHF1, etc.)
    if re.search(r'F\d+$', sym):
        return False
    # Mutual fund management/units with MF/BF/GF/SF/STF suffix
    if re.search(r'(MF|GF|STF|NMBSBF|NMB50)$', sym):
        return False
    # Fund units with SY/S digit suffix (GBIMESY2, GIMES1, etc.)
    if re.search(r'(SY|GES)\d+$', sym):
        return False
    # NIBL/NIC mutual fund series (NIBLGF, NIBLPF, NIBLSF, NIBLSTF, NICGF, NICSF, NICBF)
    if re.search(r'(NIBL|NIC)(GF|PF|SF|BF|STF)$', sym):
        return False
    return True


def fetch_all_symbols():
    """
    Fetch common-share NEPSE symbols from Chukul API.
    Filters out indices, debentures, promoter shares, rights, and mutual fund units.
    Returns a list of symbol strings, e.g. ["NABIL", "NICA", ...]
    """
    data = _get(f"{BASE_URL}/data/symbol/")
    raw = []
    if data and isinstance(data, list):
        raw = data
    elif data and isinstance(data, dict):
        raw = data.get("results") or data.get("data") or []

    symbols = []
    for item in raw:
        sym = item.get("symbol") or item.get("ticker")
        if sym and _is_common_share(sym):
            symbols.append(sym)
    return symbols


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
