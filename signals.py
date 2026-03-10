import pandas as pd
import os
import datetime

def load_data(filepath="chukul_data.csv"):
    """
    Loads historical data from CSV.
    """
    if not os.path.exists(filepath):
        print(f"Error: {filepath} not found. Cannot generate EMA signals.")
        return None
    
    try:
        df = pd.read_csv(filepath)
        # Ensure date is datetime
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'])
        elif 'epochVal' in df.columns:
             # Handle epoch if necessary, but sample showed 'date'
             pass
             
        # Normalize symbol column name
        if 'symbol' not in df.columns and 'stock' in df.columns:
            df.rename(columns={'stock': 'symbol'}, inplace=True)
            
        # Sort by symbol and date
        df.sort_values(by=['symbol', 'date'], inplace=True)
        return df
    except Exception as e:
        print(f"Error loading data: {e}")
        return None

def calculate_ema(df, window):
    """
    Calculates EMA for a given window.
    """
    return df['close'].ewm(span=window, adjust=False).mean()

def generate_signals(portfolio):
    """
    Generates BUY/SELL signals based on EMA Crossover (9 vs 21).
    Strategy:
    - golden_cross (Buy): EMA 9 crosses above EMA 21
    - death_cross (Sell): EMA 9 crosses below EMA 21
    """
    signals = []
    
    df = load_data()
    if df is None:
        return signals

    print("\n--- Generating EMA Signals ---")
    
    # Get unique symbols
    symbols = df['symbol'].unique()
    
    for symbol in symbols:
        # Filter data for this symbol
        symbol_df = df[df['symbol'] == symbol].copy()
        
        # Need at least 21 days for EMA 21
        if len(symbol_df) < 21:
            continue
            
        # Calculate EMAs
        symbol_df['EMA_9'] = calculate_ema(symbol_df, 9)
        symbol_df['EMA_21'] = calculate_ema(symbol_df, 21)
        
        # Check for crossover on the LATEST day
        # We look at the last row and the second to last row
        last_row = symbol_df.iloc[-1]
        prev_row = symbol_df.iloc[-2]
        
        # Signal variables
        signal_side = None
        price = last_row['close'] # Use closing price as reference
        
        # Golden Cross: Prev 9 <= Prev 21 AND Curr 9 > Curr 21
        if prev_row['EMA_9'] <= prev_row['EMA_21'] and last_row['EMA_9'] > last_row['EMA_21']:
            signal_side = "BUY"
            
        # Death Cross: Prev 9 >= Prev 21 AND Curr 9 < Curr 21
        elif prev_row['EMA_9'] >= prev_row['EMA_21'] and last_row['EMA_9'] < last_row['EMA_21']:
            signal_side = "SELL"
            
        if signal_side:
            print(f"Signal Found: {signal_side} {symbol} @ {price} (EMA9: {last_row['EMA_9']:.2f}, EMA21: {last_row['EMA_21']:.2f})")
            
            qty = 10 # Default quantity
            
            # Logic to filter/adjust based on portfolio
            if signal_side == "SELL":
                # Only sell if we own it
                owned_qty = 0
                if portfolio and "holdings" in portfolio:
                    for h in portfolio["holdings"]:
                        # Loose matching on keys incase of case sensitivity
                        h_symbol = h.get("Symbol") or h.get("symbol") or h.get("Script") or h.get("Scrip")
                        if h_symbol == symbol:
                            # Try to parse quantity
                            try:
                                q_str = str(h.get("Quantity", h.get("Balance", "0"))).replace(",", "")
                                owned_qty = int(float(q_str))
                            except:
                                owned_qty = 0
                            break
                            
                if owned_qty > 0:
                    qty = owned_qty # Sell all?
                    signals.append({
                        "side": "SELL",
                        "symbol": symbol,
                        "quantity": qty,
                        "price": price
                    })
            
            elif signal_side == "BUY":
                # For BUY, we might want to filter if we already own it, 
                # or just generate the signal and let the trader decide/user limits.
                # For now, generate the signal.
                signals.append({
                    "side": "BUY",
                    "symbol": symbol,
                    "quantity": 10, # Fixed buy size for now
                    "price": price
                })

    print(f"Total Signals Generated: {len(signals)}")
    return signals

if __name__ == "__main__":
    # Test run
    dummy_portfolio = {
        "holdings": [
            {"Symbol": "NICA", "Quantity": "50"},
            {"Symbol": "AKPL", "Quantity": "100"}
        ]
    }
    sigs = generate_signals(dummy_portfolio)
    print("\nFinal Signals List:")
    for s in sigs:
        print(s)
