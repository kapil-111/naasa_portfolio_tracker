"""
Fetch daily broker stock accumulation/release data from Chukul and append to
chukul_broker_stocks.csv.

For each of the top-10 net-buying brokers:  records which stocks they accumulated.
For each of the top-10 net-selling brokers: records which stocks they released.

Endpoints:
  /api/data/broker-top-holding/top-10/?days=0
  /api/data/broker-top-holding/{broker}/holding/?days=0
  /api/data/broker-top-released/top-10/?days=0
  /api/data/broker-top-released/{broker}/released/?days=0

Called once per trading day from main.py. Skips if today's date already exists.
"""

import os
import csv
from datetime import date

from chukul_client import BASE_URL, _get

_OUTPUT_FILE = "chukul_broker_stocks.csv"
_FIELDNAMES  = ["date", "type", "broker", "symbol", "quantity", "rate", "amount", "turnover", "rank"]


def _today_already_saved(today_str: str) -> bool:
    if not os.path.exists(_OUTPUT_FILE):
        return False
    with open(_OUTPUT_FILE, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get("date") == today_str:
                return True
    return False


def _fetch_holding(broker: str) -> list[dict]:
    return _get(f"{BASE_URL}/data/broker-top-holding/{broker}/holding/?days=0") or []


def _fetch_released(broker: str) -> list[dict]:
    return _get(f"{BASE_URL}/data/broker-top-released/{broker}/released/?days=0") or []


def fetch_broker_stocks(output_file: str = _OUTPUT_FILE) -> bool:
    today_str = date.today().isoformat()

    if _today_already_saved(today_str):
        print(f"[BROKER STOCKS] Already saved for {today_str}. Skipping.")
        return True

    top_holding  = _get(f"{BASE_URL}/data/broker-top-holding/top-10/?days=0") or []
    top_released = _get(f"{BASE_URL}/data/broker-top-released/top-10/?days=0") or []

    if not top_holding and not top_released:
        print("[BROKER STOCKS] No data returned from API.")
        return False

    rows = []

    for entry in top_holding:
        broker = str(entry.get("buyer", ""))
        for stock in _fetch_holding(broker):
            rows.append({
                "date":     today_str,
                "type":     "holding",
                "broker":   broker,
                "symbol":   stock.get("symbol", ""),
                "quantity": stock.get("quantity", 0),
                "rate":     stock.get("rate", 0),
                "amount":   stock.get("amount", 0),
                "turnover": stock.get("turnover", 0),
                "rank":     stock.get("rn", ""),
            })

    for entry in top_released:
        broker = str(entry.get("seller", ""))
        for stock in _fetch_released(broker):
            rows.append({
                "date":     today_str,
                "type":     "released",
                "broker":   broker,
                "symbol":   stock.get("symbol", ""),
                "quantity": stock.get("quantity", 0),
                "rate":     stock.get("rate", 0),
                "amount":   stock.get("amount", 0),
                "turnover": stock.get("turnover", 0),
                "rank":     stock.get("rn", ""),
            })

    if not rows:
        print("[BROKER STOCKS] No stock rows fetched.")
        return False

    write_header = not os.path.exists(output_file)
    with open(output_file, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=_FIELDNAMES)
        if write_header:
            writer.writeheader()
        writer.writerows(rows)

    print(f"[BROKER STOCKS] Saved {len(rows)} rows for {today_str} → {output_file}")
    return True


def generate_broker_insights(stocks_file: str = _OUTPUT_FILE, output: str = "broker_insights.json") -> bool:
    import json
    from collections import defaultdict

    today_str = date.today().isoformat()
    if not os.path.exists(stocks_file):
        return False

    acc: dict[str, dict] = defaultdict(lambda: {"total_amount": 0.0, "total_quantity": 0, "brokers": set()})
    rel: dict[str, dict] = defaultdict(lambda: {"total_amount": 0.0, "total_quantity": 0, "brokers": set()})

    with open(stocks_file, newline="") as f:
        for row in csv.DictReader(f):
            if row.get("date") != today_str:
                continue
            sym = row.get("symbol", "")
            amt = float(row.get("amount", 0) or 0)
            qty = float(row.get("quantity", 0) or 0)
            broker = row.get("broker", "")
            if row.get("type") == "holding":
                acc[sym]["total_amount"]   += amt
                acc[sym]["total_quantity"] += qty
                acc[sym]["brokers"].add(broker)
            elif row.get("type") == "released":
                rel[sym]["total_amount"]   += amt
                rel[sym]["total_quantity"] += qty
                rel[sym]["brokers"].add(broker)

    def top5(d):
        return [
            {"symbol": sym, "total_amount": round(v["total_amount"], 2),
             "total_quantity": int(v["total_quantity"]), "broker_count": len(v["brokers"])}
            for sym, v in sorted(d.items(), key=lambda x: -x[1]["total_amount"])[:5]
        ]

    insights = {
        "date": today_str,
        "top_accumulating": top5(acc),
        "top_releasing":    top5(rel),
    }

    with open(output, "w") as f:
        json.dump(insights, f, indent=2)

    print(f"[BROKER INSIGHTS] Saved → {output}")
    return True


if __name__ == "__main__":
    fetch_broker_stocks()
    generate_broker_insights()
