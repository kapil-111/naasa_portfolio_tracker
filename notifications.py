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


def _tg_send_photo(path: str, caption: str = "") -> bool:
    """Send a photo file via Telegram. Returns True on success."""
    token   = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        return False
    if not os.path.exists(path):
        print(f"Telegram photo: file not found: {path}")
        return False
    try:
        with open(path, "rb") as f:
            resp = requests.post(
                f"https://api.telegram.org/bot{token}/sendPhoto",
                data={"chat_id": chat_id, "caption": caption},
                files={"photo": f},
                timeout=20,
            )
        if resp.status_code != 200:
            print(f"Telegram photo send failed: {resp.status_code} {resp.text}")
            return False
        return True
    except Exception as e:
        print(f"Telegram photo send error: {type(e).__name__}: {e}")
        return False




# ─────────────────────────────────────────
# Facebook Messenger
# ─────────────────────────────────────────

_fb_psid_cache = None
_fb_cache_time = 0

def _fb_get_subscribers():
    global _fb_psid_cache, _fb_cache_time
    import time
    
    token = os.getenv("FB_PAGE_ACCESS_TOKEN")
    if not token:
        return []
        
    # Cache PSIDs for 1 hour to avoid excessive API calls
    if _fb_psid_cache is not None and time.time() - _fb_cache_time < 3600:
        return _fb_psid_cache

    try:
        url = f"https://graph.facebook.com/v19.0/me/conversations?fields=participants&access_token={token}"
        resp = requests.get(url, timeout=10)
        
        if resp.status_code != 200:
            print(f"Facebook conversations fetch failed: {resp.status_code} {resp.text}")
            return []
            
        data = resp.json()
        psids = set()
        
        me_resp = requests.get(f"https://graph.facebook.com/v19.0/me?access_token={token}", timeout=10)
        page_id = me_resp.json().get("id") if me_resp.status_code == 200 else None

        conversations = data.get("data", [])
        for conv in conversations:
            participants = conv.get("participants", {}).get("data", [])
            for p in participants:
                pid = p.get("id")
                if pid and pid != page_id:
                    psids.add(pid)
                    
        _fb_psid_cache = list(psids)
        _fb_cache_time = time.time()
        return _fb_psid_cache
    except Exception as e:
        print(f"Facebook get subscribers error: {type(e).__name__}: {e}")
        return []

def _fb_send(text):
    token = os.getenv("FB_PAGE_ACCESS_TOKEN")
    if not token:
        return False
        
    psids = _fb_get_subscribers()
    if not psids:
        return False

    success = True
    for psid in psids:
        try:
            url = f"https://graph.facebook.com/v19.0/me/messages?access_token={token}"
            payload = {
                "recipient": {"id": psid},
                "message": {"text": text},
                "messaging_type": "MESSAGE_TAG",
                "tag": "ACCOUNT_UPDATE"
            }
            resp = requests.post(url, json=payload, timeout=10)
            if resp.status_code != 200:
                print(f"Facebook send failed to {psid}: {resp.status_code} {resp.text}")
                success = False
        except Exception as e:
            print(f"Facebook send error to {psid}: {type(e).__name__}: {e}")
            success = False
            
    return success

def _fb_send_photo(path: str, caption: str = "") -> bool:
    token = os.getenv("FB_PAGE_ACCESS_TOKEN")
    if not token:
        return False
    if not os.path.exists(path):
        return False
        
    psids = _fb_get_subscribers()
    if not psids:
        return False

    success = True
    for psid in psids:
        try:
            with open(path, "rb") as f:
                url = f"https://graph.facebook.com/v19.0/me/messages?access_token={token}"
                payload = {
                    "recipient": f'{{"id":"{psid}"}}',
                    "message": f'{{"attachment":{{"type":"image", "payload":{{"is_reusable":true}}}}}}',
                    "messaging_type": "MESSAGE_TAG",
                    "tag": "ACCOUNT_UPDATE"
                }
                resp = requests.post(
                    url,
                    data=payload,
                    files={"filedata": f},
                    timeout=20
                )
                if resp.status_code != 200:
                    print(f"Facebook photo send failed to {psid}: {resp.status_code} {resp.text}")
                    success = False
        except Exception as e:
            print(f"Facebook photo send error to {psid}: {type(e).__name__}: {e}")
            success = False
            
    return success

# ─────────────────────────────────────────
# Universal Notification Wrappers
# ─────────────────────────────────────────

def _send_text(text):
    tg_ok = _tg_send(text)
    fb_ok = _fb_send(text)
    return tg_ok or fb_ok

def _send_photo(path: str, caption: str = "") -> bool:
    tg_ok = _tg_send_photo(path, caption)
    fb_ok = _fb_send_photo(path, caption)
    return tg_ok or fb_ok

def notify_order_screenshot(path: str, label: str, symbol: str, side: str) -> None:
    """Send an order form screenshot to Telegram with a descriptive caption."""
    caption = f"{label}\n{side} {symbol} — {_now_npt()}"
    if len(caption) > 1024:
        caption = caption[:1021] + "..."
    ok = _send_photo(path, caption=caption)
    print(f"Order screenshot ({label}) Alerts send: {'OK' if ok else 'FAILED'}")


def notify_bot_started(dry_run, market_status):
    mode = "DRY RUN" if dry_run else "LIVE"
    _send_text(
        f"🤖 Bot Started — {_now_npt()}\n"
        f"Mode: {mode} | {market_status}"
    )


def notify_market_open(dry_run):
    mode = "DRY RUN" if dry_run else "LIVE TRADING"
    _send_text(
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

    _send_text("\n".join(lines))


def notify_order(signal, is_dry_run):
    """Send an order placed / simulated alert."""
    side   = signal.get("side", "?")
    symbol = signal.get("symbol", "?")
    qty    = signal.get("quantity", 0)
    price  = signal.get("price", 0)

    icon = "✅" if not is_dry_run else "🔔"
    tag  = "[DRY RUN]" if is_dry_run else "[LIVE]"

    _send_text(
        f"{icon} {tag} Order {'Simulated' if is_dry_run else 'Placed'}\n"
        f"{side} {symbol} x{qty} @ MKT (ref: {price:.2f})\n"
        f"Reason: {signal.get('reason', '?')}"
    )


def notify_error(error_msg):
    msg = str(error_msg)
    if len(msg) > 300:
        msg = msg[:297] + "..."
    _send_text(f"⚠️ Bot Error — {_now_npt()}\n{msg}")


def notify_cycle_summary(signals, orders_placed, next_in_seconds, daily_orders=None):
    buys  = sum(1 for s in signals if s["side"] == "BUY")
    sells = sum(1 for s in signals if s["side"] == "SELL")
    mins  = next_in_seconds // 60
    next_str = f"{mins}m" if mins >= 2 else f"{next_in_seconds}s"
    lines = [
        f"🔄 Cycle Done — {_now_npt()}",
        f"Signals: {buys} BUY · {sells} SELL",
        f"Orders placed: {orders_placed}",
        f"Next check in: {next_str}",
    ]
    if daily_orders:
        lines.append(f"\n📋 Today's orders ({len(daily_orders)}):")
        for o in daily_orders:
            lines.append(f"  • {o.get('side')} {o.get('symbol')} [{o.get('type', '?')}]")
    _send_text("\n".join(lines))


def notify_premarket_report(portfolio_data, available_fund, signals, regime="UNKNOWN"):
    """Send morning report: holdings, available fund, and today's buy/sell signals."""
    print("Sending morning report via Telegram...")
    regime_emoji = {"BULL": "🟢", "BEAR": "🔴", "SIDEWAYS": "🟡"}.get(regime, "⚪")
    lines = [f"📋 Morning Report — {_now_npt()}\n",
             f"Market Trend: {regime_emoji} {regime}\n"]

    # Portfolio holdings
    import json as _json
    avg_prices = {}
    if os.path.exists("avg_prices.json"):
        with open("avg_prices.json") as _f:
            avg_prices = _json.load(_f)
    states = {}
    if os.path.exists("fortress_state.json"):
        with open("fortress_state.json") as _f:
            states = _json.load(_f)

    BLACKLIST = {"NIBSF2", "NEPSE"}

    def _sym_key(h):
        return str(h.get("Symbol") or h.get("symbol") or h.get("Script") or h.get("Scrip") or "").strip()

    holdings = [
        h
        for h in (portfolio_data.get("holdings", []) if portfolio_data else [])
        if _sym_key(h) not in BLACKLIST and not _sym_key(h).lower().startswith("total")
    ]

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
            for k in ['CDS Free\nBalance', 'NAASA\nBalance', 'CDS Total\nBalance', 'Quantity', 'Total Qty', 'Qty', 'Balance Quantity', 'Units', 'Current Balance']:
                if h.get(k) is not None and str(h.get(k)).strip():
                    try:
                        qty = int(float(str(h[k]).replace(',', '')))
                        break
                    except (ValueError, TypeError):
                        pass
            # IPO lock-in: CDS Free Balance = 0 for months; use CDS Total Balance (actual shares owned)
            if not qty and sym and states.get(sym, {}).get('is_ipo'):
                try:
                    qty = int(float(str(h.get('CDS Total\nBalance', 0)).replace(',', ''))) or None
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

    ok = _send_text("\n".join(lines))
    print(f"Morning report Alerts send: {'OK' if ok else 'FAILED'}")


def notify_eod_fill_report(fill_results):
    """
    Send end-of-day fill reconciliation report.
    fill_results: list of dicts with keys:
      symbol, side, signal_qty, traded_qty, fill_status, price
    """
    if not fill_results:
        _send_text(f"📋 EOD Fill Report — {_now_npt()}\nNo orders to reconcile.")
        return

    lines = [f"📋 EOD Fill Report — {_now_npt()}\n"]
    for r in fill_results:
        sym    = r["symbol"]
        side   = r["side"]
        status = r["fill_status"]
        tqty   = r["traded_qty"]
        sqty   = r["signal_qty"]
        price  = r.get("price", 0)

        if status == "COMPLETE":
            icon = "✅"
            detail = f"qty={tqty} @{price:.2f}"
        elif status == "PARTIAL":
            icon = "⚠️"
            detail = f"filled={tqty}/{sqty} @{price:.2f}"
        elif status == "CANCELLED":
            icon = "❌"
            detail = f"qty=0/{sqty} — fully cancelled"
        else:
            icon = "❓"
            detail = f"traded={tqty} status={status}"

        lines.append(f"{icon} {side} {sym} [{status}]  {detail}")

    _send_text("\n".join(lines))


def notify_market_close(daily_orders):
    lines = [f"🔴 Market Closed — {_now_npt()}\n"]
    if daily_orders:
        lines.append(f"Orders today: {len(daily_orders)}")
        for o in daily_orders:
            lines.append(f"  • {o.get('side')} {o.get('symbol')}")
    else:
        lines.append("No orders placed today.")
    _send_text("\n".join(lines))


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
    _send_text("✅ Telegram test from NAASA bot")
    test_signal = {'side': 'BUY', 'symbol': 'TEST', 'quantity': 100, 'price': 1000,
                   'score': 4, 'breakdown': {'ema': 2, 'macd': 1, 'volume': 1}}
    notify_signals([{**test_signal, 'side': 'BUY'}])
    notify_order(test_signal, is_dry_run=True)
