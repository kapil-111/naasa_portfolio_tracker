import pandas as pd
import os
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

from chukul_client import BASE_URL, _get


def fetch_chukul_history(symbol, since_date=None):
    """
    Fetch historical OHLCV data for a symbol from Chukul API.
    If since_date (pd.Timestamp) is provided, only rows after that date are returned.
    """
    url = f"{BASE_URL}/data/historydata/"
    data = _get(url, params={"symbol": symbol})
    if not data or not isinstance(data, list):
        return None

    df = pd.DataFrame(data)
    df['stock'] = symbol

    if since_date is not None and 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        df = df[df['date'] > since_date]

    return df if not df.empty else None


def update_chukul_data(symbols=None, input_file="live_market_data.csv",
                       output_file="chukul_data.csv", verbose=False):
    if symbols is None:
        if not os.path.exists(input_file):
            print(f"Warning: {input_file} not found, skipping historical data update.")
            return
        print(f"Reading symbols from {input_file}...")
        try:
            live_data = pd.read_csv(input_file)
            if "Symbol" not in live_data.columns:
                print("Error: 'Symbol' column not found in input CSV.")
                return
            symbols = live_data["Symbol"].tolist()
        except Exception as e:
            print(f"Error reading input CSV: {e}")
            return

    # Load existing data and find latest date per symbol for incremental fetch
    existing_df = None
    latest_dates = {}
    if os.path.exists(output_file):
        try:
            existing_df = pd.read_csv(output_file)
            date_col = 'date' if 'date' in existing_df.columns else None
            sym_col  = 'stock' if 'stock' in existing_df.columns else (
                       'symbol' if 'symbol' in existing_df.columns else None)
            if date_col and sym_col:
                existing_df[date_col] = pd.to_datetime(existing_df[date_col], errors='coerce')
                latest_dates = (
                    existing_df.groupby(sym_col)[date_col].max().to_dict()
                )
                print(f"Incremental update: loaded {len(existing_df)} existing rows, "
                      f"{len(latest_dates)} symbols with history.")
        except Exception as e:
            print(f"Warning: could not read existing {output_file}: {e}. Full fetch will run.")
            existing_df = None

    print(f"Fetching new data for {len(symbols)} symbols (incremental where available)...")

    new_data = []
    max_workers = int(os.getenv("FETCH_MAX_WORKERS", "8"))
    # Early-abort: if first PROBE_SIZE symbols all fail, chukul.com is unreachable
    PROBE_SIZE = 5
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_symbol = {
            executor.submit(fetch_chukul_history, sym, latest_dates.get(sym)): sym
            for sym in symbols
        }
        iterable = as_completed(future_to_symbol)
        if verbose:
            iterable = tqdm(iterable, total=len(symbols))

        consecutive_errors = 0   # only counts hard failures (exceptions / HTTP errors)
        completed = 0
        for future in iterable:
            symbol = future_to_symbol[future]
            completed += 1
            try:
                df = future.result()
                if df is not None:
                    new_data.append(df)
                    consecutive_errors = 0
                # df is None → symbol up-to-date, not a failure — don't count it
            except Exception as exc:
                print(f'{symbol} generated an exception: {exc}')
                consecutive_errors += 1

            # Abort only if the first PROBE_SIZE completed futures ALL raised exceptions
            # (None result = up-to-date, not an error)
            if completed <= PROBE_SIZE and len(new_data) == 0 and consecutive_errors >= PROBE_SIZE:
                print(f"Early abort: first {PROBE_SIZE} symbols all errored — chukul.com appears unreachable.")
                for f in future_to_symbol:
                    f.cancel()
                break

    if not new_data and existing_df is not None:
        print("No new rows fetched — existing data is up to date.")
        return

    if new_data:
        new_df = pd.concat(new_data, ignore_index=True)
        if existing_df is not None:
            combined_df = pd.concat([existing_df, new_df], ignore_index=True)
            # Deduplicate on (stock/symbol, date) keeping the latest
            sym_col  = 'stock' if 'stock' in combined_df.columns else 'symbol'
            date_col = 'date'
            combined_df.drop_duplicates(subset=[sym_col, date_col], keep='last', inplace=True)
            combined_df.sort_values([sym_col, date_col], inplace=True)
        else:
            combined_df = new_df

        combined_df.to_csv(output_file, index=False)
        print(f"Saved {len(combined_df)} rows to {output_file} "
              f"(+{len(new_df)} new rows)")
    else:
        print("No data was fetched for any symbol.")


if __name__ == "__main__":
    update_chukul_data(verbose=True)
