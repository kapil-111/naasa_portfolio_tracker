"""
fetch_chukul_indicators.py

Fetches technical analysis data from Chukul for all NEPSE symbols:
  - RSI-14
  - Support levels
  - Resistance levels
  - Level alerts (pre-computed SMA crossover signals)

Output: chukul_indicators.csv
"""

import pandas as pd
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

from chukul_client import BASE_URL, _get


def fetch_indicators(symbol):
    """Fetch RSI-14 for a symbol."""
    data = _get(f"{BASE_URL}/data/indicators/", params={"symbol": symbol})
    if data and isinstance(data, list) and data:
        return data[0]
    return {}


def fetch_support(symbol):
    """Fetch support price levels for a symbol."""
    data = _get(f"{BASE_URL}/data/support/", params={"symbol": symbol})
    if data and isinstance(data, list) and data:
        return data
    return []


def fetch_resistance(symbol):
    """Fetch resistance price levels for a symbol."""
    data = _get(f"{BASE_URL}/data/resistance/", params={"symbol": symbol})
    if data and isinstance(data, list) and data:
        return data
    return []


def fetch_level_alerts(symbol):
    """
    Fetch pre-computed technical signals (e.g., 'Price above 200-day SMA').
    Returns a list of alert strings/objects.
    """
    data = _get(f"{BASE_URL}/data/level-alert/", params={"symbol": symbol})
    if data and isinstance(data, list):
        return data
    return []


def _fetch_all_for_symbol(symbol):
    """Fetch all indicator data for a single symbol and return a combined dict."""
    result = {"symbol": symbol, "fetched_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

    indicators = fetch_indicators(symbol)
    result["rsi14"] = indicators.get("rsi14")

    support_levels = fetch_support(symbol)
    for i, lvl in enumerate(support_levels[:3], 1):
        if isinstance(lvl, dict):
            result[f"support_{i}"] = lvl.get("price") or lvl.get("value") or (list(lvl.values())[0] if lvl else None)
        else:
            result[f"support_{i}"] = lvl

    resistance_levels = fetch_resistance(symbol)
    for i, lvl in enumerate(resistance_levels[:3], 1):
        if isinstance(lvl, dict):
            result[f"resistance_{i}"] = lvl.get("price") or lvl.get("value") or (list(lvl.values())[0] if lvl else None)
        else:
            result[f"resistance_{i}"] = lvl

    alerts = fetch_level_alerts(symbol)
    if alerts:
        alert_strs = []
        for a in alerts:
            if isinstance(a, dict):
                msg = a.get("message") or a.get("alert") or a.get("description") or str(a)
            else:
                msg = str(a)
            alert_strs.append(msg)
        result["level_alerts"] = " | ".join(alert_strs)
    else:
        result["level_alerts"] = None

    return result


def update_indicators_data(symbols=None, input_file="live_market_data.csv",
                           output_file="chukul_indicators.csv", verbose=True):
    """
    Fetch indicator data for all symbols and save to CSV.
    If symbols is None, reads from input_file.
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

    print(f"Fetching technical indicators for {len(symbols)} symbols from chukul.com...")

    all_results = []
    max_workers = int(os.getenv("FETCH_MAX_WORKERS") or "")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_symbol = {executor.submit(_fetch_all_for_symbol, sym): sym for sym in symbols}
        for future in as_completed(future_to_symbol):
            sym = future_to_symbol[future]
            try:
                row = future.result()
                all_results.append(row)
                if verbose:
                    print(f"  [{sym}] RSI14={row.get('rsi14')}")
            except Exception as exc:
                print(f"  [{sym}] Error: {exc}")

    if all_results:
        df = pd.DataFrame(all_results)
        df.sort_values("symbol", inplace=True)
        df.to_csv(output_file, index=False)
        print(f"\nSaved {len(df)} rows to {output_file}")
        print(df[["symbol", "rsi14", "support_1", "resistance_1"]].head(10).to_string(index=False))
    else:
        print("No indicator data fetched.")

    return all_results


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    update_indicators_data(verbose=True)
