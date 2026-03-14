import pandas as pd
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

from chukul_client import BASE_URL, _get


def fetch_broker_list(output_file="chukul_brokers.csv"):
    """
    Fetch the full list of registered NEPSE brokers.
    Saves to chukul_brokers.csv and returns a dict {broker_id: broker_name}.
    """
    data = _get(f"{BASE_URL}/broker/")
    brokers = {}

    if data:
        if isinstance(data, list):
            records = data
        elif isinstance(data, dict):
            records = data.get("results") or data.get("data") or []
        else:
            records = []

        for item in records:
            bid = str(item.get("id") or item.get("broker_id") or item.get("member_id") or "")
            bname = item.get("name") or item.get("broker_name") or item.get("member_name") or ""
            if bid:
                brokers[bid] = bname

        if brokers:
            df = pd.DataFrame([{"broker_id": k, "broker_name": v} for k, v in brokers.items()])
            df.to_csv(output_file, index=False)
            print(f"Saved {len(df)} brokers to {output_file}")
        else:
            print("Warning: No broker data returned. Saving raw response for inspection.")
            if data:
                import json
                with open("broker_debug.json", "w") as f:
                    json.dump(data if isinstance(data, list) else [data], f, indent=2)

    return brokers


def fetch_top_buyers(symbol, from_date, to_date):
    """
    Fetch top buying brokers for a symbol over a date range.
    Returns a list of dicts with broker number, quantity, rate, amount.
    """
    data = _get(f"{BASE_URL}/data/top-buy/", params={
        "symbol": symbol,
        "from_date": from_date,
        "to_date": to_date
    })
    if data and isinstance(data, list):
        for row in data:
            row["symbol"] = symbol
            row["from_date"] = from_date
            row["to_date"] = to_date
            row["side"] = "BUY"
        return data
    return []


def fetch_top_sellers(symbol, from_date, to_date):
    """
    Fetch top selling brokers for a symbol over a date range.
    Returns a list of dicts with broker number, quantity, rate, amount.
    """
    data = _get(f"{BASE_URL}/data/top-sell/", params={
        "symbol": symbol,
        "from_date": from_date,
        "to_date": to_date
    })
    if data and isinstance(data, list):
        for row in data:
            row["symbol"] = symbol
            row["from_date"] = from_date
            row["to_date"] = to_date
            row["side"] = "SELL"
        return data
    return []


def _fetch_broker_data_for_symbol(symbol, from_date, to_date):
    """Fetch both buy and sell broker data for a single symbol."""
    buyers = fetch_top_buyers(symbol, from_date, to_date)
    sellers = fetch_top_sellers(symbol, from_date, to_date)
    return buyers, sellers


def update_broker_data(symbols=None, input_file="live_market_data.csv",
                       buy_output="chukul_broker_buy.csv",
                       sell_output="chukul_broker_sell.csv",
                       broker_output="chukul_brokers.csv",
                       date_range_days=5, verbose=True):
    """
    Fetch top buyer/seller broker data for all symbols over the last N trading days.
    Also refreshes the broker list.
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

    to_date = datetime.now().strftime("%Y-%m-%d")
    from_date = (datetime.now() - timedelta(days=date_range_days)).strftime("%Y-%m-%d")
    print(f"Date range: {from_date} → {to_date}")

    print("\nFetching broker list...")
    fetch_broker_list(output_file=broker_output)

    print(f"\nFetching broker buy/sell data for {len(symbols)} symbols...")
    all_buy_rows = []
    all_sell_rows = []
    max_workers = int(os.getenv("FETCH_MAX_WORKERS", "8"))

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_sym = {
            executor.submit(_fetch_broker_data_for_symbol, sym, from_date, to_date): sym
            for sym in symbols
        }
        for future in as_completed(future_to_sym):
            sym = future_to_sym[future]
            try:
                buyers, sellers = future.result()
                all_buy_rows.extend(buyers)
                all_sell_rows.extend(sellers)
                if verbose:
                    top_buyer = buyers[0].get("buyer") if buyers else "N/A"
                    top_seller = sellers[0].get("seller") if sellers else "N/A"
                    buy_qty = buyers[0].get("quantity") if buyers else 0
                    sell_qty = sellers[0].get("quantity") if sellers else 0
                    print(f"  [{sym}] Top Buyer=#{top_buyer}({buy_qty} shares)  Top Seller=#{top_seller}({sell_qty} shares)")
            except Exception as exc:
                print(f"  [{sym}] Error: {exc}")

    if all_buy_rows:
        df_buy = pd.DataFrame(all_buy_rows)
        df_buy.to_csv(buy_output, index=False)
        print(f"\nSaved {len(df_buy)} buy broker rows to {buy_output}")
    else:
        print("No broker buy data fetched.")

    if all_sell_rows:
        df_sell = pd.DataFrame(all_sell_rows)
        df_sell.to_csv(sell_output, index=False)
        print(f"Saved {len(df_sell)} sell broker rows to {sell_output}")
    else:
        print("No broker sell data fetched.")

    return all_buy_rows, all_sell_rows


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    update_broker_data(verbose=True)
