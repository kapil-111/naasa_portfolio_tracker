import os
import pandas as pd

# This function is copied directly from backtest.py to ensure consistency.
def _adjust_prices(df, actions_file="chukul_corporate_actions.csv"):
    """
    Backward-adjust OHLC prices for bonus shares and right issues.
    """
    print(f"Attempting to adjust prices using corporate actions file: {actions_file}")
    if not os.path.exists(actions_file):
        print(f"Warning: Corporate actions file '{actions_file}' not found. Prices will not be adjusted.")
        return df

    ca = pd.read_csv(actions_file, parse_dates=["book_close_date"])
    date_min = df["date"].min()
    ca = ca[ca["book_close_date"] >= date_min - pd.Timedelta(days=5)]

    if ca.empty:
        print("No relevant corporate actions found in the specified date range.")
        return df

    all_dates = sorted(df["date"].unique())

    def next_trading_day(bcd):
        for d in all_dates:
            if d > bcd:
                return d
        return None

    adjustments = {}
    for _, row in ca.iterrows():
        sym     = row["symbol"]
        bcd     = row["book_close_date"]
        atype   = row["action_type"]
        pct     = float(row["pct"])
        ex_date = next_trading_day(bcd)
        if ex_date is None:
            continue

        if atype == "bonus":
            factor = 1.0 / (1.0 + pct / 100.0)
        else:
            sym_df   = df[df["symbol"] == sym].sort_values("date")
            ex_row   = sym_df[sym_df["date"] == ex_date]
            prev_row = sym_df[sym_df["date"] < ex_date].tail(1)
            if ex_row.empty or prev_row.empty:
                continue
            ratio = float(ex_row["open"].iloc[0]) / float(prev_row["close"].iloc[0])
            if ratio > 0.92:
                continue
            factor = ratio

        if sym not in adjustments:
            adjustments[sym] = []
        adjustments[sym].append((ex_date, factor))

    if not adjustments:
        print("No valid price adjustment factors could be calculated.")
        return df

    ohlc_cols = [c for c in ["open", "high", "low", "close", "ltp"] if c in df.columns]
    parts = []
    for sym, sym_df in df.groupby("symbol", sort=False):
        sym_df = sym_df.copy()
        if sym in adjustments:
            for ex_date, factor in sorted(adjustments[sym], key=lambda x: x[0]):
                mask = sym_df["date"] < ex_date
                sym_df.loc[mask, ohlc_cols] = sym_df.loc[mask, ohlc_cols] * factor
        parts.append(sym_df)

    adjusted = pd.concat(parts, ignore_index=True)
    total = sum(len(v) for v in adjustments.values())
    print(f"Price-adjusted {total} corporate action events across {len(adjustments)} symbols.")
    return adjusted


def main():
    input_file = "chukul_data.csv"
    output_file = "chukul_data_adjusted.csv"

    if not os.path.exists(input_file):
        print(f"Error: Input file '{input_file}' not found.")
        return

    print(f"Loading data from '{input_file}'...")
    df = pd.read_csv(input_file)

    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])
    if "symbol" not in df.columns and "stock" in df.columns:
        df.rename(columns={"stock": "symbol"}, inplace=True)
    df.sort_values(["symbol", "date"], inplace=True)

    adjusted_df = _adjust_prices(df)

    print(f"Saving adjusted data to '{output_file}'...")
    adjusted_df.to_csv(output_file, index=False)
    print(f"Successfully created '{output_file}'.")

if __name__ == "__main__":
    main()
