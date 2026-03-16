import os
import requests


# ─────────────────────────────────────────
# Telegram
# ─────────────────────────────────────────

def _tg_send(text):
    """Send a Telegram message. Returns True on success."""
    token   = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        return False
    try:
        resp = requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": text, "parse_mode": "Markdown"},
            timeout=10
        )
        return resp.status_code == 200
    except Exception as e:
        print(f"Telegram send error: {e}")
        return False


def notify_market_open(dry_run):
    mode = "🟡 DRY RUN" if dry_run else "🔴 LIVE TRADING"
    _tg_send(
        f"🟢 *NEPSE Market Open*\n"
        f"Bot started — {_now_npt()}\n"
        f"Mode: {mode}"
    )


def notify_signals(signals):
    """Send a summary of all BUY/SELL signals generated this cycle."""
    buys  = [s for s in signals if s["side"] == "BUY"]
    sells = [s for s in signals if s["side"] == "SELL"]

    if not buys and not sells:
        return

    lines = [f"📊 *Signals — {_now_npt()}*\n"]

    for s in buys:
        lines.append(f"🟢 *BUY {s['symbol']}* ({s.get('type','?')})  @{s['price']:.2f}  qty={s.get('quantity','?')}")

    for s in sells:
        lines.append(f"🔴 *SELL {s['symbol']}* ({s.get('type','?')})  @{s['price']:.2f}  qty={s.get('quantity','?')}")

    _tg_send("\n".join(lines))


def notify_order(signal, is_dry_run):
    """Send an order placed / simulated alert."""
    side   = signal.get("side", "?")
    symbol = signal.get("symbol", "?")
    qty    = signal.get("quantity", 0)
    price  = signal.get("price", 0)
    score  = signal.get("score", 0)

    icon = "✅" if not is_dry_run else "🔔"
    tag  = "[DRY RUN]" if is_dry_run else "[LIVE]"

    _tg_send(
        f"{icon} *{tag} Order {'Simulated' if is_dry_run else 'Placed'}*\n"
        f"`{side}` *{symbol}* × {qty} @ {price:.2f}\n"
        f"Score: `{score:+d}`"
    )


def notify_error(error_msg):
    _tg_send(
        f"⚠️ *Bot Error — {_now_npt()}*\n"
        f"`{str(error_msg)[:300]}`"
    )


def notify_cycle_summary(signals, orders_placed, next_in_seconds):
    buys  = sum(1 for s in signals if s["side"] == "BUY")
    sells = sum(1 for s in signals if s["side"] == "SELL")
    _tg_send(
        f"🔄 *Cycle Done — {_now_npt()}*\n"
        f"Signals: {buys} BUY · {sells} SELL\n"
        f"Orders placed: {orders_placed}\n"
        f"Next check in: {next_in_seconds}s"
    )


def notify_market_close(daily_orders):
    lines = [f"🔴 *Market Closed — {_now_npt()}*\n"]
    if daily_orders:
        lines.append(f"Orders today: {len(daily_orders)}")
        for o in daily_orders:
            lines.append(f"  • {o.get('side')} {o.get('symbol')}")
    else:
        lines.append("No orders placed today.")
    _tg_send("\n".join(lines))


# ─────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────

def _now_npt():
    from datetime import datetime
    import pytz
    return datetime.now(pytz.timezone("Asia/Kathmandu")).strftime("%H:%M NPT")


def _breakdown_str(signal):
    bd = signal.get("breakdown", {})
    parts = [f"{k}={v:+d}" for k, v in bd.items() if v != 0]
    return "  ".join(parts) if parts else "—"


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    _tg_send("✅ Telegram test from NAASA bot")
    test_signal = {'side': 'BUY', 'symbol': 'TEST', 'quantity': 100, 'price': 1000,
                   'score': 4, 'breakdown': {'ema': 2, 'macd': 1, 'volume': 1}}
    notify_signals([{**test_signal, 'side': 'BUY'}])
    notify_order(test_signal, is_dry_run=True)
