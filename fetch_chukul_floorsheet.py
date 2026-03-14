"""
fetch_chukul_floorsheet.py

Fetches raw transaction-level floorsheet data from Chukul.
Each row = one transaction: buyer broker, seller broker, quantity, rate, amount.

This is the most granular data available — useful for:
  - Detecting institutional block trades
  - Identifying broker accumulation/distribution patterns
  - Building custom broker-level signals

Output: chukul_floorsheet_<YYYY-MM-DD>.csv
"""

import pandas as pd
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

from chukul_client import BASE_URL, _get


def fetch_floorsheet(symbol, date):
    """
    Fetch all transactions for a symbol on a specific date.

    Args:
        symbol: NEPSE ticker (e.g., "NABIL")
        date:   Date string "YYYY-MM-DD"

    Returns:
        List of transaction dicts with keys:
          transaction, symbol, buyer, seller, quantity, rate, amount
    """
    data = _get(f"{BASE_URL}/data/floorsheet/", params={"symbol": symbol, "date": date})
    if data and isinstance(data, list):
        return data
    return []


def summarize_floorsheet(transactions):
    """
    Aggregate raw transactions by broker side.
    Returns (buyer_summary_df, seller_summary_df).
    """
    if not transactions:
        return pd.DataFrame(), pd.DataFrame()

    df = pd.DataFrame(transactions)

    buyer_summary = (
        df.groupby("buyer")
        .agg(total_quantity=("quantity", "sum"),
             total_amount=("amount", "sum"),
             avg_rate=("rate", "mean"),
             transactions=("transaction", "count"))
        .reset_index()
        .rename(columns={"buyer": "broker"})
        .sort_values("total_quantity", ascending=False)
    )
    buyer_summary["side"] = "BUY"

    seller_summary = (
        df.groupby("seller")
        .agg(total_quantity=("quantity", "sum"),
             total_amount=("amount", "sum"),
             avg_rate=("rate", "mean"),
             transactions=("transaction", "count"))
        .reset_index()
        .rename(columns={"seller": "broker"})
        .sort_values("total_quantity", ascending=False)
    )
    seller_summary["side"] = "SELL"

    return buyer_summary, seller_summary


def _fetch_floorsheet_for_symbol(symbol, date):
    """Fetch floorsheet for a single symbol on a given date."""
    transactions = fetch_floorsheet(symbol, date)
    for t in transactions:
        t.setdefault("symbol", symbol)
        t.setdefault("date", date)
    return transactions


def update_floorsheet_data(symbols=None, date=None, input_file="live_market_data.csv",
                           output_dir=".", verbose=True):
    """
    Fetch floorsheet data for all symbols on a given date.

    Args:
        symbols:    List of tickers. If None, reads from input_file.
        date:       Date string "YYYY-MM-DD". Defaults to today.
        output_dir: Directory to save output CSV files.
        verbose:    Print progress.

    Outputs:
        chukul_floorsheet_<date>.csv         — all raw transactions
        chukul_floorsheet_<date>_summary.csv — broker-level aggregation
    """
    if date is None:
        date = datetime.now().strftime("%Y-%m-%d")

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

    raw_output     = os.path.join(output_dir, f"chukul_floorsheet_{date}.csv")
    summary_output = os.path.join(output_dir, f"chukul_floorsheet_{date}_summary.csv")

    print(f"Fetching floorsheet for {len(symbols)} symbols on {date}...")

    all_transactions = []
    max_workers = int(os.getenv("FETCH_MAX_WORKERS") or "")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_sym = {
            executor.submit(_fetch_floorsheet_for_symbol, sym, date): sym
            for sym in symbols
        }
        for future in as_completed(future_to_sym):
            sym = future_to_sym[future]
            try:
                transactions = future.result()
                all_transactions.extend(transactions)
                if verbose and transactions:
                    total_qty = sum(t.get("quantity", 0) for t in transactions)
                    print(f"  [{sym}] {len(transactions)} transactions, {total_qty:.0f} shares traded")
                elif verbose:
                    print(f"  [{sym}] No floorsheet data (non-trading day or no activity)")
            except Exception as exc:
                print(f"  [{sym}] Error: {exc}")

    if all_transactions:
        df_raw = pd.DataFrame(all_transactions)
        df_raw.to_csv(raw_output, index=False)
        print(f"\nSaved {len(df_raw)} transactions to {raw_output}")

        buyer_parts = []
        seller_parts = []
        for sym in df_raw["symbol"].unique():
            sym_df = df_raw[df_raw["symbol"] == sym]
            b_sum, s_sum = summarize_floorsheet(sym_df.to_dict("records"))
            if not b_sum.empty:
                b_sum["symbol"] = sym
                buyer_parts.append(b_sum)
            if not s_sum.empty:
                s_sum["symbol"] = sym
                seller_parts.append(s_sum)

        if buyer_parts or seller_parts:
            df_summary = pd.concat(buyer_parts + seller_parts, ignore_index=True)
            df_summary.to_csv(summary_output, index=False)
            print(f"Saved broker summary to {summary_output}")

        print("\n--- Top Buyers (all symbols) ---")
        if buyer_parts:
            top_buyers = pd.concat(buyer_parts).sort_values("total_quantity", ascending=False).head(10)
            print(top_buyers[["symbol", "broker", "total_quantity", "total_amount"]].to_string(index=False))

        print("\n--- Top Sellers (all symbols) ---")
        if seller_parts:
            top_sellers = pd.concat(seller_parts).sort_values("total_quantity", ascending=False).head(10)
            print(top_sellers[["symbol", "broker", "total_quantity", "total_amount"]].to_string(index=False))
    else:
        print(f"No floorsheet data fetched for {date} (market may have been closed).")

    return all_transactions


if __name__ == "__main__":
    import argparse
    from dotenv import load_dotenv
    load_dotenv()
    parser = argparse.ArgumentParser(description="Fetch NEPSE floorsheet data from Chukul")
    parser.add_argument("--date", type=str, default=None,
                        help="Date to fetch (YYYY-MM-DD). Defaults to today.")
    args = parser.parse_args()
    update_floorsheet_data(date=args.date, verbose=True)
