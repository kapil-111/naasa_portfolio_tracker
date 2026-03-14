import pandas as pd
import requests
import time
import os
import sys
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

def fetch_chukul_history(symbol):
    """
    Fetch historical stock data for a symbol from chukul.com
    """
    url = f"https://chukul.com/api/data/historydata/?symbol={symbol}"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    try:
        response = requests.get(url, headers=headers, timeout=15)
        if response.status_code != 200:
            # print(f"Failed to retrieve {symbol}, status code: {response.status_code}")
            return None

        data = response.json()
        if not isinstance(data, list) or not data:
            # print(f"No data found for {symbol}")
            return None

        df = pd.DataFrame(data)
        df['stock'] = symbol
        return df

    except Exception as e:
        # print(f"Error processing {symbol}: {str(e)}")
        return None

def update_chukul_data(symbols=None, input_file="live_market_data.csv",
                       output_file="chukul_data.csv", verbose=False):
    if symbols is None:
        if not os.path.exists(input_file):
            print(f"Error: {input_file} not found.")
            sys.exit(1)
        print(f"Reading symbols from {input_file}...")
        try:
            live_data = pd.read_csv(input_file)
            if "Symbol" not in live_data.columns:
                print("Error: 'Symbol' column not found in input CSV.")
                sys.exit(1)
            symbols = live_data["Symbol"].tolist()
        except Exception as e:
            print(f"Error reading input CSV: {e}")
            sys.exit(1)

    all_data = []

    print(f"Fetching data for {len(symbols)} symbols from chukul.com (Concurrent)...")

    # Use ThreadPoolExecutor for parallel processing
    # Adjust max_workers as needed (too high might get flagged by server)
    max_workers = 10 
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_symbol = {executor.submit(fetch_chukul_history, symbol): symbol for symbol in symbols}
        
        # Disable tqdm if not verbose to reduce log spam in automated environments
        iterable = as_completed(future_to_symbol)
        if verbose:
            iterable = tqdm(iterable, total=len(symbols))
            
        for future in iterable:
            symbol = future_to_symbol[future]
            try:
                df = future.result()
                if df is not None:
                    all_data.append(df)
            except Exception as exc:
                print(f'{symbol} generated an exception: {exc}')

    if all_data:
        combined_df = pd.concat(all_data, ignore_index=True)
        combined_df.to_csv(output_file, index=False)
        print(f"\nSaved {len(combined_df)} rows to {output_file}")
        print(combined_df.head())
    else:
        print("No data was fetched for any symbol.")

if __name__ == "__main__":
    update_chukul_data(verbose=True)
