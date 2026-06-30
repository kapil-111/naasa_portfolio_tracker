"""
market_snapshot.py

Generates nepse_market.json with:
  - nepse        : {close, change, change_pct, date}
  - market_status: {is_open, as_of}
  - sectors      : [{name, symbol, close, change, change_pct}]
  - breadth      : {increased, decreased, unchanged, date}

Called from main.py once per cycle so the dashboard always has fresh data.
"""

import json
import os
from datetime import datetime

import pandas as pd

from chukul_client import BASE_URL, _get

SECTOR_INDICES = [
    ("Banking",       "BANKINGIND"),
    ("Dev. Bank",     "DEVBANKIND"),
    ("Finance",       "FINANCEIND"),
    ("Life Ins.",     "LIFEINSUIND"),
    ("Non-Life Ins.", "NONLIFEIND"),
    ("Hydropower",    "HYDROPOWIND"),
    ("Microfinance",  "MICROFININD"),
    ("Trading",       "TRADINGIND"),
    ("Hotels",        "HOTELIND"),
    ("Mfg & Proc.",   "MANUFACTUREIND"),
    ("Mutual Fund",   "MUTUALIND"),
    ("Investment",    "INVIDX"),
]

_INDEX_SYMS = {
    "NEPSE", "NEPSEI",
    "BANKINGIND", "DEVBANKIND", "FINANCEIND", "LIFEINSUIND", "NONLIFEIND",
    "HYDROPOWIND", "MICROFININD", "TRADINGIND", "HOTELIND", "MANUFACTUREIND",
    "MUTUALIND", "FLOATIND", "INVIDX", "SENSIND",
    "BANKING", "DEVBANK", "FINANCE", "LIFEINSU", "NONLIFEINSU", "HYDRO",
    "MICROFINANCE", "HOTELS", "MANUFACTURE", "MUTUAL", "INVESTMENT",
}


def _fetch_two(symbol):
    """Return (today_row, prev_row) from historydata, or None on failure."""
    data = _get(f"{BASE_URL}/data/historydata/?symbol={symbol}")
    if not data or not isinstance(data, list) or len(data) == 0:
        return None
    rows = sorted(data, key=lambda x: x.get("date", ""), reverse=True)
    today = rows[0]
    prev  = rows[1] if len(rows) > 1 else today
    return today, prev


def generate_market_snapshot(ohlcv_file="chukul_data.csv", output="nepse_market.json"):
    snapshot = {
        "generated_at":  datetime.now().strftime("%Y-%m-%d %H:%M"),
        "nepse":         None,
        "market_status": {"is_open": False, "as_of": ""},
        "sectors":       [],
        "breadth":       {"increased": 0, "decreased": 0, "unchanged": 0, "date": ""},
    }

    # 1. Market status
    status = _get(f"{BASE_URL}/tools/market/status/")
    if status:
        snapshot["market_status"] = {
            "is_open": bool(status.get("is_open", False)),
            "as_of":   status.get("as_of_live", "") or status.get("as_of", ""),
        }

    # 2. NEPSE index
    result = _fetch_two("NEPSE")
    if result:
        today, prev = result
        change     = float(today["close"]) - float(prev["close"])
        change_pct = change / float(prev["close"]) * 100 if float(prev["close"]) else 0
        snapshot["nepse"] = {
            "close":      round(float(today["close"]), 2),
            "change":     round(change, 2),
            "change_pct": round(change_pct, 2),
            "date":       today.get("date", ""),
        }

    # 3. Sector indices
    for name, sym in SECTOR_INDICES:
        result = _fetch_two(sym)
        if not result:
            continue
        today, prev = result
        change     = float(today["close"]) - float(prev["close"])
        change_pct = change / float(prev["close"]) * 100 if float(prev["close"]) else 0
        snapshot["sectors"].append({
            "name":       name,
            "symbol":     sym,
            "close":      round(float(today["close"]), 2),
            "change":     round(change, 2),
            "change_pct": round(change_pct, 2),
        })

    # 4. Breadth from chukul_data.csv (common shares only)
    if os.path.exists(ohlcv_file):
        try:
            df      = pd.read_csv(ohlcv_file)
            sym_col = "stock" if "stock" in df.columns else "symbol"
            df["date"] = pd.to_datetime(df["date"], errors="coerce")
            df = df[~df[sym_col].isin(_INDEX_SYMS)]

            latest = df["date"].max()
            today_df = df[df["date"] == latest]

            inc = dec = unc = 0
            for sym, grp in today_df.groupby(sym_col):
                prev_rows = df[(df[sym_col] == sym) & (df["date"] < latest)].sort_values("date")
                if prev_rows.empty:
                    continue
                tc = float(grp.iloc[-1]["close"])
                pc = float(prev_rows.iloc[-1]["close"])
                if tc > pc:
                    inc += 1
                elif tc < pc:
                    dec += 1
                else:
                    unc += 1

            snapshot["breadth"] = {
                "increased": inc,
                "decreased": dec,
                "unchanged": unc,
                "date": latest.strftime("%Y-%m-%d") if not pd.isna(latest) else "",
            }
        except Exception as e:
            print(f"[MARKET SNAPSHOT] Breadth calculation failed: {e}")

    with open(output, "w") as f:
        json.dump(snapshot, f, indent=2)

    nepse_val = snapshot["nepse"]["close"] if snapshot["nepse"] else "N/A"
    b = snapshot["breadth"]
    print(f"[MARKET SNAPSHOT] {output}: NEPSE={nepse_val} "
          f"sectors={len(snapshot['sectors'])} "
          f"breadth=+{b['increased']}/-{b['decreased']}/={b['unchanged']}")
    return snapshot
