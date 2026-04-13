"""
telegram_commands.py

Polls Telegram for incoming messages and handles manual trading commands.
Called once per main loop cycle — processes any new messages since last check.

Supported commands (send to your bot on Telegram):
  /sell SYMBOL QTY PRICE   — place a manual sell order
  /buy  SYMBOL QTY PRICE   — place a manual buy order
  /status                  — portfolio + fund summary
  /help                    — list available commands
"""

import os
import json
import requests
from datetime import datetime

_OFFSET_FILE = "telegram_offset.json"


def _token():
    return os.getenv("TELEGRAM_BOT_TOKEN", "")


def _chat_id():
    return os.getenv("TELEGRAM_CHAT_ID", "")


def _reply(text):
    token = _token()
    chat  = _chat_id()
    if not token or not chat:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat, "text": text, "parse_mode": "HTML"},
            timeout=10,
        )
    except Exception as e:
        print(f"[TG CMD] Reply failed: {e}")


def _load_offset():
    if os.path.exists(_OFFSET_FILE):
        try:
            return json.load(open(_OFFSET_FILE)).get("offset", 0)
        except Exception:
            pass
    return 0


def _save_offset(offset):
    with open(_OFFSET_FILE, "w") as f:
        json.dump({"offset": offset}, f)


def _get_updates(offset):
    token = _token()
    if not token:
        return []
    try:
        resp = requests.get(
            f"https://api.telegram.org/bot{token}/getUpdates",
            params={"offset": offset, "timeout": 2, "limit": 20},
            timeout=10,
        )
        if resp.status_code == 200:
            return resp.json().get("result", [])
    except Exception as e:
        print(f"[TG CMD] getUpdates error: {e}")
    return []


def _handle_sell(parts, page, trader, states, portfolio_data, dry_run):
    """Handle /sell SYMBOL QTY PRICE"""
    if len(parts) < 4:
        _reply("Usage: /sell SYMBOL QTY PRICE\nExample: /sell HFIN 10 750")
        return

    symbol = parts[1].upper()
    try:
        qty   = int(parts[2])
        price = float(parts[3])
    except ValueError:
        _reply("Invalid QTY or PRICE. Example: /sell HFIN 10 750")
        return

    if qty <= 0 or price <= 0:
        _reply("QTY and PRICE must be positive.")
        return

    signal = {
        "side":     "SELL",
        "symbol":   symbol,
        "price":    price,
        "quantity": qty,
        "type":     "MANUAL",
        "reason":   f"Manual sell via Telegram @ {price}",
    }

    _reply(f"⏳ Placing SELL {symbol} x{qty} @ {price:.2f}...")
    success = trader.place_order(signal)

    if success:
        # Update state
        from state_manager import save_states
        from signals_mr import save_avg_price
        sym_state = states.get(symbol, {})
        sym_state["last_exit_date"]  = datetime.now().strftime("%Y-%m-%d")
        sym_state["last_exit_price"] = price
        sym_state["in_position"]     = False
        sym_state["entry_price"]     = 0
        sym_state["entry_date"]      = None
        states[symbol] = sym_state
        save_states(states)

        # Clear avg price
        _clear_avg_price_local(symbol)

        _reply(
            f"✅ SELL order placed\n"
            f"{symbol} x{qty} @ {price:.2f}\n"
            f"{'[DRY RUN]' if dry_run else '[LIVE]'}"
        )
    else:
        err = getattr(trader, "last_error", "unknown error")
        _reply(f"❌ SELL failed for {symbol}: {err}")


def _handle_buy(parts, page, trader, states, portfolio_data, available_fund, dry_run):
    """Handle /buy SYMBOL QTY PRICE"""
    if len(parts) < 4:
        _reply("Usage: /buy SYMBOL QTY PRICE\nExample: /buy NABIL 10 500")
        return

    symbol = parts[1].upper()
    try:
        qty   = int(parts[2])
        price = float(parts[3])
    except ValueError:
        _reply("Invalid QTY or PRICE. Example: /buy NABIL 10 500")
        return

    if qty <= 0 or price <= 0:
        _reply("QTY and PRICE must be positive.")
        return

    signal = {
        "side":     "BUY",
        "symbol":   symbol,
        "price":    price,
        "quantity": qty,
        "type":     "MANUAL",
        "reason":   f"Manual buy via Telegram @ {price}",
    }

    _reply(f"⏳ Placing BUY {symbol} x{qty} @ {price:.2f}...")
    success = trader.place_order(signal)

    if success:
        from state_manager import save_states
        from signals_mr import save_avg_price
        sym_state = states.get(symbol, {})
        existing_qty = 0
        for h in portfolio_data.get("holdings", []):
            from signals_mr import _get_holding_symbol, _get_holding_qty
            if _get_holding_symbol(h) == symbol:
                existing_qty = _get_holding_qty(h)
                break
        save_avg_price(symbol, price, qty, existing_qty)

        if not sym_state.get("in_position"):
            states[symbol] = {
                "in_position":   True,
                "entry_date":    datetime.now().strftime("%Y-%m-%d"),
                "entry_price":   price,
                "initial_entry": price,
                "ema_cross_days": 0,
            }
        save_states(states)

        _reply(
            f"✅ BUY order placed\n"
            f"{symbol} x{qty} @ {price:.2f}\n"
            f"{'[DRY RUN]' if dry_run else '[LIVE]'}"
        )
    else:
        err = getattr(trader, "last_error", "unknown error")
        _reply(f"❌ BUY failed for {symbol}: {err}")


def _handle_status(portfolio_data, available_fund):
    """Handle /status"""
    import json as _json
    avg_prices = {}
    if os.path.exists("avg_prices.json"):
        try:
            avg_prices = _json.load(open("avg_prices.json"))
        except Exception:
            pass

    holdings = portfolio_data.get("holdings", []) if portfolio_data else []
    lines = [f"📊 Status — {_now_npt()}\n"]

    if holdings:
        from signals_mr import _get_holding_symbol, _get_holding_qty
        total_cost = 0.0
        total_val  = 0.0
        for h in holdings:
            sym = _get_holding_symbol(h)
            if sym and sym.lower().startswith("total"):
                continue
            qty = _get_holding_qty(h)
            avg = avg_prices.get(sym)
            ltp = None
            for k in ["LTP", "Close Price", "Last Traded Price"]:
                v = h.get(k)
                if v is not None and str(v).strip():
                    try:
                        ltp = float(str(v).replace(",", ""))
                        break
                    except ValueError:
                        pass
            if sym and qty and avg and ltp:
                pnl_pct = (ltp - avg) / avg * 100
                pnl_amt = (ltp - avg) * qty
                total_cost += avg * qty
                total_val  += ltp * qty
                lines.append(f"• {sym} x{qty}  avg={avg:.0f}  ltp={ltp:.0f}  {pnl_pct:+.1f}%  NPR {pnl_amt:+,.0f}")
            elif sym:
                lines.append(f"• {sym} x{qty or '?'}")

        if total_cost > 0:
            total_pnl = total_val - total_cost
            lines.append(f"\nTotal P&L: NPR {total_pnl:+,.0f} ({(total_pnl/total_cost)*100:+.1f}%)")
    else:
        lines.append("No holdings found.")

    fund_str = f"NPR {available_fund:,.2f}" if available_fund is not None else "N/A"
    lines.append(f"Available Fund: {fund_str}")

    _reply("\n".join(lines))


def _clear_avg_price_local(symbol, path="avg_prices.json"):
    if not os.path.exists(path):
        return
    try:
        with open(path) as f:
            prices = json.load(f)
        if symbol in prices:
            del prices[symbol]
            with open(path, "w") as f:
                json.dump(prices, f, indent=4, sort_keys=True)
    except Exception as e:
        print(f"[TG CMD] Could not clear avg price for {symbol}: {e}")


def _now_npt():
    from datetime import datetime
    import pytz
    return datetime.now(pytz.timezone("Asia/Kathmandu")).strftime("%H:%M NPT")


HELP_TEXT = (
    "🤖 <b>NAASA Bot Commands</b>\n\n"
    "/sell SYMBOL QTY PRICE\n"
    "  → Place a sell order\n"
    "  Example: /sell HFIN 10 750\n\n"
    "/buy SYMBOL QTY PRICE\n"
    "  → Place a buy order\n"
    "  Example: /buy NABIL 10 500\n\n"
    "/status\n"
    "  → Show portfolio & fund\n\n"
    "/help\n"
    "  → Show this message"
)


def poll_and_handle(page, trader, states, portfolio_data, available_fund, dry_run):
    """
    Poll Telegram for new messages and process any commands found.
    Call this once per main loop cycle.

    page            — active Playwright page (already logged in)
    trader          — Trader instance
    states          — current fortress_state dict (mutated in place on trades)
    portfolio_data  — current portfolio dict
    available_fund  — float or None
    dry_run         — bool
    """
    offset = _load_offset()
    updates = _get_updates(offset)

    if not updates:
        return

    # Only accept messages from our own chat_id (security: ignore strangers)
    allowed_chat = str(_chat_id())

    for update in updates:
        offset = update["update_id"] + 1
        msg = update.get("message") or update.get("edited_message")
        if not msg:
            continue

        # Security: only respond to the configured chat
        sender_chat = str(msg.get("chat", {}).get("id", ""))
        if sender_chat != allowed_chat:
            print(f"[TG CMD] Ignoring message from unknown chat {sender_chat}")
            continue

        text = (msg.get("text") or "").strip()
        if not text.startswith("/"):
            continue

        parts = text.split()
        cmd   = parts[0].lower().split("@")[0]  # strip @botname suffix if present
        print(f"[TG CMD] Received command: {text}")

        if cmd == "/help":
            _reply(HELP_TEXT)

        elif cmd == "/status":
            _handle_status(portfolio_data, available_fund)

        elif cmd == "/sell":
            _handle_sell(parts, page, trader, states, portfolio_data, dry_run)

        elif cmd == "/buy":
            _handle_buy(parts, page, trader, states, portfolio_data, available_fund, dry_run)

        else:
            _reply(f"Unknown command: {cmd}\nSend /help for available commands.")

    _save_offset(offset)
