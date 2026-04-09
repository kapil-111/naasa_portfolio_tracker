import json
import os
from datetime import datetime

STATE_FILE = "fortress_state.json"

def load_states():
    """Loads the strategy state from a JSON file."""
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, 'r') as f:
                return json.load(f)
        except json.JSONDecodeError:
            print(f"Warning: Could not decode {STATE_FILE}. Starting with a fresh state.")
            return {}
    return {}

def save_states(states):
    """Saves the strategy state to a JSON file."""
    with open(STATE_FILE, 'w') as f:
        json.dump(states, f, indent=4)
    print(f"Strategy state saved to {STATE_FILE}.")

def update_state_for_trade(state, signal, current_price, quantity=None):
    """
    Calculates the new state for a symbol after a successful trade.
    
    Args:
        state (dict): The current state for the symbol.
        signal (dict): The signal that was just executed.
        current_price (float): The price at which the trade was executed.
        quantity (int): The quantity of the trade.

    Returns:
        dict: The new, updated state for the symbol.
    """
    new_state = state.copy()
    signal_type = signal.get("type", "FULL") # Default to FULL BUY/SELL

    if signal['side'] == 'BUY':
        # This is a double-down buy
        if new_state.get('in_position') and new_state.get('position_count') == 1:
            print(f"[{signal['symbol']}] STATE: Doubling down.")
            # Calculate new average entry price. Assume the new buy is for 2 units.
            # (initial_price * 1 + new_price * 2) / 3 total units
            new_state['entry_price'] = (new_state['entry_price'] + 2 * current_price) / 3
            new_state['position_count'] = 3
        # This is a fresh, initial buy
        else:
            print(f"[{signal['symbol']}] STATE: Initial entry.")
            new_state['in_position'] = True
            new_state['half_sold'] = False
            new_state['initial_entry'] = current_price
            new_state['entry_price'] = current_price
            new_state['entry_date'] = datetime.now().strftime('%Y-%m-%d')
            new_state['position_count'] = 1
            new_state['trades'] = new_state.get('trades', 0)

    elif signal['side'] == 'SELL':
        # This is a partial sell (half of the position)
        if signal_type == 'HALF_SELL':
            print(f"[{signal['symbol']}] STATE: Half-selling position.")
            new_state['half_sold'] = True
            # Note: P&L tracking is not part of state management for live trading.
            # We just record that the partial sale happened.
        # This is a full sell (including cut-loss)
        else:
            print(f"[{signal['symbol']}] STATE: Exiting full position.")
            new_state['in_position'] = False
            new_state['half_sold'] = False
            new_state['last_exit_price'] = current_price
            new_state['last_exit_date'] = datetime.now().strftime('%Y-%m-%d')
            new_state['trades'] = new_state.get('trades', 0) + 1
            new_state['entry_date'] = None
            new_state['initial_entry'] = 0
            new_state['entry_price'] = 0
            new_state['position_count'] = 0
            
    return new_state
