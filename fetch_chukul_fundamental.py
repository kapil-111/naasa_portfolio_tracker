"""
fetch_chukul_fundamental.py

Fetches fundamental analysis data from Chukul for all NEPSE symbols:
  - Symbol → Internal ID mapping
  - Fundamental report (EPS, P/E, Paid-up capital, Net Interest Income, etc.)
  - Bonus share & dividend history

Outputs:
  - chukul_fundamental.csv
  - chukul_bonus.csv
"""

import pandas as pd
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

from chukul_client import BASE_URL, _get

# Cache of symbol -> stock_id
_symbol_id_cache = {}


def fetch_symbol_map():
    """
    Fetch all stock metadata from /api/data/symbol/ and build a symbol->id dict.
    Returns: { "NABIL": 123, "NICA": 456, ... }
    """
    data = _get(f"{BASE_URL}/data/symbol/")
    mapping = {}
    if data and isinstance(data, list):
        for item in data:
            sym = item.get("symbol") or item.get("ticker")
            sid = item.get("id") or item.get("stock_id") or item.get("security_id")
            if sym and sid:
                mapping[sym] = sid
    elif data and isinstance(data, dict):
        results = data.get("results") or data.get("data") or []
        for item in results:
            sym = item.get("symbol") or item.get("ticker")
            sid = item.get("id") or item.get("stock_id")
            if sym and sid:
                mapping[sym] = sid
    return mapping


def get_stock_id(symbol, symbol_map=None):
    """Resolve symbol to internal Chukul stock ID."""
    global _symbol_id_cache
    if symbol in _symbol_id_cache:
        return _symbol_id_cache[symbol]
    if symbol_map and symbol in symbol_map:
        _symbol_id_cache[symbol] = symbol_map[symbol]
        return symbol_map[symbol]
    data = _get(f"{BASE_URL}/stock/", params={"symbol": symbol})
    if data:
        if isinstance(data, list) and data:
            sid = data[0].get("id")
        elif isinstance(data, dict):
            sid = data.get("id")
        else:
            sid = None
        if sid:
            _symbol_id_cache[symbol] = sid
            return sid
    return None


def fetch_fundamental_report(stock_id):
    """
    Fetch full fundamental report for a stock_id.
    Returns raw dict with all available fundamental fields.
    """
    if not stock_id:
        return {}
    data = _get(f"{BASE_URL}/stock/{stock_id}/report/")
    if data:
        if isinstance(data, list) and data:
            return data[0]
        elif isinstance(data, dict):
            return data
    return {}


def fetch_bonus_history(symbol):
    """
    Fetch historical bonus shares & cash dividend data for a symbol.
    Returns a list of dicts.
    """
    data = _get(f"{BASE_URL}/bonus/", params={"symbol": symbol})
    if data and isinstance(data, list):
        return data
    elif data and isinstance(data, dict):
        return data.get("results") or data.get("data") or []
    return []


def _fetch_fundamental_for_symbol(symbol, symbol_map=None):
    """Fetch all fundamental data for a single symbol."""
    stock_id = get_stock_id(symbol, symbol_map)
    report = fetch_fundamental_report(stock_id)

    row = {"symbol": symbol, "stock_id": stock_id}
    for key, val in report.items():
        row[key] = val

    row["fetched_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return row


def update_fundamental_data(symbols=None, input_file="live_market_data.csv",
                            fundamental_output="chukul_fundamental.csv",
                            bonus_output="chukul_bonus.csv",
                            verbose=True):
    """
    Fetch fundamental + bonus data for all symbols and save to CSV.
    """
    if symbols is None:
        if not os.path.exists(input_file):
            print(f"Error: {input_file} not found. Run fetch_live_data.py first.")
            sys.exit(1)
        try:
            live_data = pd.read_csv(input_file)
            symbols = live_data["Symbol"].tolist()
        except Exception as e:
            print(f"Error reading {input_file}: {e}")
            sys.exit(1)

    print("Building symbol → ID map from chukul.com...")
    symbol_map = fetch_symbol_map()
    print(f"  Mapped {len(symbol_map)} symbols.")

    print(f"\nFetching fundamental reports for {len(symbols)} symbols...")
    fundamental_rows = []
    max_workers = int(os.getenv("FETCH_MAX_WORKERS") or "")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_sym = {
            executor.submit(_fetch_fundamental_for_symbol, sym, symbol_map): sym
            for sym in symbols
        }
        for future in as_completed(future_to_sym):
            sym = future_to_sym[future]
            try:
                row = future.result()
                fundamental_rows.append(row)
                if verbose:
                    sid = row.get("stock_id")
                    eps = row.get("eps") or row.get("EPS") or row.get("basic_eps") or "N/A"
                    pe  = row.get("pe")  or row.get("PE")  or row.get("pe_ratio")  or "N/A"
                    print(f"  [{sym}] ID={sid} EPS={eps} P/E={pe}")
            except Exception as exc:
                print(f"  [{sym}] Error: {exc}")

    if fundamental_rows:
        df_fund = pd.DataFrame(fundamental_rows)
        df_fund.sort_values("symbol", inplace=True)
        df_fund.to_csv(fundamental_output, index=False)
        print(f"\nSaved {len(df_fund)} rows to {fundamental_output}")
    else:
        print("No fundamental data fetched.")

    print(f"\nFetching bonus & dividend history for {len(symbols)} symbols...")
    bonus_rows = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_sym = {executor.submit(fetch_bonus_history, sym): sym for sym in symbols}
        for future in as_completed(future_to_sym):
            sym = future_to_sym[future]
            try:
                records = future.result()
                for r in records:
                    r["symbol"] = sym
                    bonus_rows.append(r)
                if verbose and records:
                    print(f"  [{sym}] {len(records)} bonus/dividend records")
            except Exception as exc:
                print(f"  [{sym}] Bonus error: {exc}")

    if bonus_rows:
        df_bonus = pd.DataFrame(bonus_rows)
        df_bonus.to_csv(bonus_output, index=False)
        print(f"\nSaved {len(df_bonus)} rows to {bonus_output}")
        print(df_bonus.head().to_string(index=False))
    else:
        print("No bonus/dividend data fetched.")

    return fundamental_rows, bonus_rows


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    update_fundamental_data(verbose=True)
