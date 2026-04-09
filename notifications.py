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
        print("Telegram: token or chat_id missing")
        return False
    try:
        resp = requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": text},
            timeout=10
        )
        if resp.status_code != 200:
            print(f"Telegram send failed: {resp.status_code} {resp.text}")
            return False
        return True
    except Exception as e:
        print(f"Telegram send error: {type(e).__name__}: {e}")
        return False


def notify_bot_started(dry_run, market_status):
    mode = "DRY RUN" if dry_run else "LIVE"
    _tg_send(
        f"🤖 Bot Started — {_now_npt()}\n"
        f"Mode: {mode} | {market_status}"
    )


def notify_market_open(dry_run):
    mode = "DRY RUN" if dry_run else "LIVE TRADING"
    _tg_send(
        f"🟢 NEPSE Market Open\n"
        f"Bot started — {_now_npt()}\n"
        f"Mode: {mode}"
    )


def notify_signals(signals):
    """Send a summary of all BUY/SELL signals generated this cycle."""
    buys  = [s for s in signals if s["side"] == "BUY"]
    sells = [s for s in signals if s["side"] == "SELL"]

    if not buys and not sells:
        return

    lines = [f"📊 Signals — {_now_npt()}\n"]

    for s in buys:
        line = f"🟢 BUY {s['symbol']} ({s.get('type','?')})  @{s['price']:.2f}  qty={s.get('quantity','?')}"
        if s.get('reason'):
            line += f"\n    Reason: {s['reason']}"
        lines.append(line)

    for s in sells:
        line = f"🔴 SELL {s['symbol']} ({s.get('type','?')})  @{s['price']:.2f}  qty={s.get('quantity','?')}"
        if s.get('profit_pct') is not None:
            line += f"\n    P&L={s['profit_pct']:+.1f}%  entry={s.get('entry_price','?')}  held={s.get('days_held','?')}d"
        if s.get('reason'):
            line += f"\n    Reason: {s['reason']}"
        lines.append(line)

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
        f"{icon} {tag} Order {'Simulated' if is_dry_run else 'Placed'}\n"
        f"{side} {symbol} x{qty} @ {price:.2f}\n"
        f"Score: {score:+d}"
    )


def notify_error(error_msg):
    _tg_send(
        f"⚠️ Bot Error — {_now_npt()}\n"
        f"{str(error_msg)[:300]}"
    )


def notify_cycle_summary(signals, orders_placed, next_in_seconds):
    buys  = sum(1 for s in signals if s["side"] == "BUY")
    sells = sum(1 for s in signals if s["side"] == "SELL")
    _tg_send(
        f"🔄 Cycle Done — {_now_npt()}\n"
        f"Signals: {buys} BUY · {sells} SELL\n"
        f"Orders placed: {orders_placed}\n"
        f"Next check in: {next_in_seconds}s"
    )


def notify_premarket_report(portfolio_data, available_fund, signals):
    """Send morning report: holdings, available fund, and today's buy/sell signals."""
    print("Sending morning report via Telegram...")
    lines = [f"📋 Morning Report — {_now_npt()}\n"]

    # Portfolio holdings
    avg_prices = {}
    if os.path.exists("avg_prices.json"):
        import json as _json
        with open("avg_prices.json") as _f:
            avg_prices = _json.load(_f)

    BLACKLIST = {"NIBSF2"}
    holdings = [h for h in (portfolio_data.get("holdings", []) if portfolio_data else [])
                if str(h.get("Symbol") or h.get("symbol") or h.get("Script") or h.get("Scrip") or "").strip() not in BLACKLIST]

    total_market_value = 0.0
    total_cost = 0.0

    if holdings:
        lines.append(f"Holdings ({len(holdings)} stocks):")
        for h in holdings:
            sym = None
            qty = None
            ltp = None
            for k in ['Symbol', 'symbol', 'Stock Symbol', 'Script', 'Scrip']:
                if h.get(k):
                    sym = str(h[k]).strip()
                    break
            for k in ['CDS Total\nBalance', 'NAASA\nBalance', 'Quantity', 'Total Qty', 'Qty', 'Balance Quantity', 'Units', 'Current Balance']:
                if h.get(k) is not None and str(h.get(k)).strip():
                    try:
                        qty = int(float(str(h[k]).replace(',', '')))
                        break
                    except (ValueError, TypeError):
                        pass
            for k in ['LTP', 'Close Price', 'Last Traded Price']:
                if h.get(k) is not None and str(h.get(k)).strip():
                    try:
                        ltp = float(str(h[k]).replace(',', ''))
                        break
                    except (ValueError, TypeError):
                        pass
            if sym:
                avg = avg_prices.get(sym)
                if avg and qty and ltp:
                    market_val = ltp * qty
                    cost_val   = avg * qty
                    pnl_amt    = market_val - cost_val
                    pnl_pct    = (pnl_amt / cost_val) * 100
                    total_market_value += market_val
                    total_cost         += cost_val
                    pnl_str = f"  {pnl_pct:+.1f}%  NPR {pnl_amt:+,.0f}"
                    lines.append(f"  • {sym} x{qty}  avg={avg:,.2f}  ltp={ltp:,.2f}{pnl_str}")
                elif avg and qty:
                    total_cost += avg * qty
                    lines.append(f"  • {sym} x{qty}  avg={avg:,.2f}")
                else:
                    lines.append(f"  • {sym} x{qty or '?'}")

        # Portfolio summary
        if total_cost > 0:
            total_pnl     = total_market_value - total_cost
            total_pnl_pct = (total_pnl / total_cost) * 100
            lines.append(f"\nPortfolio Value:  NPR {total_market_value:,.0f}")
            lines.append(f"Total Invested:   NPR {total_cost:,.0f}")
            lines.append(f"Total P&L:        NPR {total_pnl:+,.0f}  ({total_pnl_pct:+.1f}%)")
    else:
        lines.append("Holdings: None")

    # Available fund
    fund_str = f"NPR {available_fund:,.2f}" if available_fund is not None else "N/A"
    lines.append(f"Available Fund:   {fund_str}")

    # Signals
    buys  = [s for s in signals if s["side"] == "BUY"]
    sells = [s for s in signals if s["side"] == "SELL"]
    if buys or sells:
        lines.append("\nSignals for today:")
        for s in buys:
            line = f"  🟢 BUY {s['symbol']} qty={s.get('quantity','?')} @{s['price']:.2f}"
            if s.get('reason'):
                line += f"\n    Reason: {s['reason']}"
            lines.append(line)
        for s in sells:
            line = f"  🔴 SELL {s['symbol']} ({s.get('type','?')}) qty={s.get('quantity','?')}  P&L={s.get('profit_pct',0):+.1f}%"
            if s.get('reason'):
                line += f"\n    Reason: {s['reason']}"
            lines.append(line)
    else:
        lines.append("\nSignals: None for today")

    ok = _tg_send("\n".join(lines))
    print(f"Morning report Telegram send: {'OK' if ok else 'FAILED'}")


def notify_market_close(daily_orders):
    lines = [f"🔴 Market Closed — {_now_npt()}\n"]
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


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    _tg_send("✅ Telegram test from NAASA bot")
    test_signal = {'side': 'BUY', 'symbol': 'TEST', 'quantity': 100, 'price': 1000,
                   'score': 4, 'breakdown': {'ema': 2, 'macd': 1, 'volume': 1}}
    notify_signals([{**test_signal, 'side': 'BUY'}])
    notify_order(test_signal, is_dry_run=True)
