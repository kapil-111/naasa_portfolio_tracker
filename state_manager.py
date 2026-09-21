import copy
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
        f.flush()
        os.fsync(f.fileno())
    print(f"Strategy state saved to {STATE_FILE}.")

PLACED_ORDERS_FILE = "placed_orders_today.json"


def load_placed_orders():
    """Loads today's placed orders to prevent duplicates and limit buys."""
    filename  = PLACED_ORDERS_FILE
    today_str = datetime.now().strftime("%Y-%m-%d")

    if os.path.exists(filename):
        try:
            with open(filename, 'r') as f:
                data = json.load(f)
                if data.get("date") != today_str:
                    return {"date": today_str, "orders": []}
                if "symbols" in data and "orders" not in data:
                    migrated = [{"symbol": s, "side": "BUY"} for s in data["symbols"]]
                    return {"date": today_str, "orders": migrated}
                return data
        except json.JSONDecodeError:
            pass

    return {"date": today_str, "orders": []}


def _write_placed_orders(data):
    with open(PLACED_ORDERS_FILE, 'w') as f:
        json.dump(data, f, indent=4)
        f.flush()
        os.fsync(f.fileno())


def save_placed_order(symbol, side, signal_type, quantity=0):
    """Saves a symbol, side, type, and quantity to the placed orders list."""
    data = load_placed_orders()

    # Dedup check excludes quantity so the same order isn't placed twice
    new_order = {"symbol": symbol, "side": side, "type": signal_type, "quantity": quantity}
    if not any(
        o.get("symbol") == symbol and o.get("side") == side and o.get("type") == signal_type
        for o in data["orders"]
    ):
        data["orders"].append(new_order)

    _write_placed_orders(data)
    print(f"Recorded order for {side} {symbol} ({signal_type}) qty={quantity} in state file.")


def remove_placed_order(symbol, side, signal_type):
    """Removes a previously recorded order — used when order definitively failed (not unconfirmed)."""
    data = load_placed_orders()
    before = len(data["orders"])
    data["orders"] = [
        o for o in data["orders"]
        if not (o.get("symbol") == symbol and o.get("side") == side and o.get("type") == signal_type)
    ]
    if len(data["orders"]) < before:
        _write_placed_orders(data)
        print(f"Removed failed order for {side} {symbol} ({signal_type}) — will retry next cycle.")


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
    new_state = copy.deepcopy(state)
    signal_type = signal.get("type", "FULL") # Default to FULL BUY/SELL

    if signal['side'] == 'BUY':
        # This is a double-down buy
        if new_state.get('in_position') and new_state.get('position_count') == 1:
            print(f"[{signal['symbol']}] STATE: Doubling down.")
            # Calculate new average entry price using actual quantity.
            # Initial position counts as 1 unit; new buy is weighted by its quantity.
            new_qty = quantity if quantity and quantity > 0 else 2
            new_state['entry_price'] = (new_state['entry_price'] + new_qty * current_price) / (1 + new_qty)
            new_state['position_count'] = 1 + new_qty
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
        # This is a partial sell (half of the position or trailing SL partial)
        if signal_type in ('HALF_SELL', 'TRAIL_SL'):
            print(f"[{signal['symbol']}] STATE: Partial sell ({signal_type}).")
            new_state['half_sold'] = True
            # Reset peak_price so trail restarts from current price after partial exit
            new_state['peak_price'] = current_price
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
