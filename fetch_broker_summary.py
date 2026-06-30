"""
Fetch daily broker summary from Chukul and append to chukul_broker_summary.csv.

Endpoint: https://chukul.com/api/data/broker-summary/
Returns 90 rows (one per registered NEPSE broker) with:
  buyer, buy_amount, sell_amount, net_amount, net_matching

Called once per trading day from main.py. Skips if today's date already exists.
"""

import os
import csv
from datetime import date

from chukul_client import BASE_URL, _get

_OUTPUT_FILE = "chukul_broker_summary.csv"
_FIELDNAMES  = ["date", "broker", "buy_amount", "sell_amount", "net_amount", "net_matching"]


def _today_already_saved(today_str: str) -> bool:
    if not os.path.exists(_OUTPUT_FILE):
        return False
    with open(_OUTPUT_FILE, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get("date") == today_str:
                return True
    return False


def fetch_broker_summary(output_file: str = _OUTPUT_FILE) -> bool:
    today_str = date.today().isoformat()

    if _today_already_saved(today_str):
        print(f"[BROKER SUMMARY] Already saved for {today_str}. Skipping.")
        return True

    data = _get(f"{BASE_URL}/data/broker-summary/")
    if not data or not isinstance(data, list):
        print("[BROKER SUMMARY] No data returned from API.")
        return False

    write_header = not os.path.exists(output_file)
    with open(output_file, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=_FIELDNAMES)
        if write_header:
            writer.writeheader()
        for row in data:
            writer.writerow({
                "date":         today_str,
                "broker":       row.get("buyer", ""),
                "buy_amount":   row.get("buy_amount", 0),
                "sell_amount":  row.get("sell_amount", 0),
                "net_amount":   row.get("net_amount", 0),
                "net_matching": row.get("net_matching", 0),
            })

    print(f"[BROKER SUMMARY] Saved {len(data)} broker rows for {today_str} → {output_file}")
    return True


if __name__ == "__main__":
    fetch_broker_summary()
