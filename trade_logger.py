"""
Trade logger — appends every confirmed BUY/SELL to trade_log.csv.
"""
import csv
import os
from datetime import datetime

TRADE_LOG = "trade_log.csv"
FIELDNAMES = [
    "date", "symbol", "side", "type", "qty", "price",
    "avg_cost", "pnl_pct", "pnl_npr", "notes"
]


def _now_npt():
    from datetime import timezone, timedelta
    return datetime.now(timezone(timedelta(hours=5, minutes=45))).strftime("%Y-%m-%d %H:%M")


def log_trade(symbol, side, signal_type, qty, price, avg_cost=None, notes=""):
    """
    Append one trade to trade_log.csv.
    P&L is calculated only for SELL trades where avg_cost is known.
    """
    pnl_pct = ""
    pnl_npr = ""
    if side == "SELL" and avg_cost and avg_cost > 0:
        pnl_pct = round((price - avg_cost) / avg_cost * 100, 2)
        pnl_npr = round((price - avg_cost) * qty, 2)

    row = {
        "date":      _now_npt(),
        "symbol":    symbol,
        "side":      side,
        "type":      signal_type,
        "qty":       qty,
        "price":     price,
        "avg_cost":  avg_cost if avg_cost else "",
        "pnl_pct":   pnl_pct,
        "pnl_npr":   pnl_npr,
        "notes":     notes,
    }

    write_header = not os.path.exists(TRADE_LOG)
    with open(TRADE_LOG, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        if write_header:
            writer.writeheader()
        writer.writerow(row)

    print(f"[TRADE LOG] {side} {symbol} qty={qty} @{price} pnl={pnl_pct}% NPR {pnl_npr}")


def get_summary():
    """Return a formatted summary string of all trades."""
    if not os.path.exists(TRADE_LOG):
        return "No trades recorded yet."

    with open(TRADE_LOG, newline="") as f:
        trades = list(csv.DictReader(f))

    if not trades:
        return "No trades recorded yet."

    buys  = [t for t in trades if t["side"] == "BUY"]
    sells = [t for t in trades if t["side"] == "SELL"]

    # Only sells with P&L data
    closed = [t for t in sells if t["pnl_npr"]]
    total_pnl   = sum(float(t["pnl_npr"]) for t in closed)
    wins        = [t for t in closed if float(t["pnl_npr"]) > 0]
    losses      = [t for t in closed if float(t["pnl_npr"]) <= 0]
    win_rate    = len(wins) / len(closed) * 100 if closed else 0
    avg_win_pct = sum(float(t["pnl_pct"]) for t in wins)   / len(wins)   if wins   else 0
    avg_los_pct = sum(float(t["pnl_pct"]) for t in losses) / len(losses) if losses else 0

    lines = [
        "📊 Trade Record",
        f"  Total trades : {len(trades)} ({len(buys)} buys, {len(sells)} sells)",
        f"  Closed P&L   : {len(closed)} with P&L data",
        f"  Win rate     : {win_rate:.1f}%  ({len(wins)}W / {len(losses)}L)",
        f"  Avg win      : {avg_win_pct:+.1f}%",
        f"  Avg loss     : {avg_los_pct:+.1f}%",
        f"  Total P&L    : NPR {total_pnl:+,.0f}",
        "",
        "Recent trades (last 10):",
    ]
    for t in trades[-10:]:
        pnl_str = f"  P&L={t['pnl_pct']}%  NPR {t['pnl_npr']}" if t["pnl_npr"] else ""
        lines.append(f"  {t['date']}  {t['side']:4s} {t['symbol']:8s} x{t['qty']} @{t['price']}{pnl_str}")

    return "\n".join(lines)
