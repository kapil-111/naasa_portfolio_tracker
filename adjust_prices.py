import os
import pandas as pd
from signals_mr import _adjust_prices


def main():
    input_file  = "chukul_data.csv"
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
