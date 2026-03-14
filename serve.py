"""
serve.py — Local analysis dashboard server.

Run:  python serve.py
Opens http://localhost:8765/analysis.html in your browser automatically.
Press Ctrl+C to stop.
"""

import os
import json
import glob
import threading
import webbrowser
import pandas as pd
from http.server import HTTPServer, SimpleHTTPRequestHandler
from dotenv import load_dotenv

load_dotenv()

from signals import (
    load_ohlcv, load_rsi_map, load_broker_flow,
    load_fundamental_map, score_symbol
)

HOST = "localhost"
PORT = 8765


class DashboardHandler(SimpleHTTPRequestHandler):

    def do_GET(self):
        path = self.path.split("?")[0]
        if path.startswith("/api/"):
            self._handle_api(path)
        else:
            super().do_GET()

    def _handle_api(self, path):
        handlers = {
            "/api/portfolio":    self._get_portfolio,
            "/api/signals":      self._get_signals,
            "/api/indicators":   self._get_indicators,
            "/api/fundamentals": self._get_fundamentals,
            "/api/brokers":      self._get_brokers,
            "/api/floorsheet":   self._get_floorsheet,
            "/api/market":       self._get_market,
        }
        fn = handlers.get(path)
        if fn is None:
            self._json_response({"error": "Unknown endpoint"}, 404)
            return
        try:
            self._json_response(fn())
        except Exception as e:
            self._json_response({"error": str(e)}, 500)

    def _json_response(self, data, status=200):
        body = json.dumps(data, default=str).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", len(body))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

    def _get_portfolio(self):
        result = {"summary": {}, "holdings": []}
        if os.path.exists("portfolio_summary.json"):
            with open("portfolio_summary.json") as f:
                result["summary"] = json.load(f)
        if os.path.exists("portfolio_data.csv"):
            result["holdings"] = pd.read_csv("portfolio_data.csv").to_dict("records")
        return result

    def _get_signals(self):
        buy_threshold  = int(os.getenv("BUY_THRESHOLD") or "")
        sell_threshold = int(os.getenv("SELL_THRESHOLD") or "")
        broker_conc    = float(os.getenv("BROKER_CONCENTRATION_THRESHOLD") or "")

        df = load_ohlcv()
        if df is None:
            return []

        rsi_map     = load_rsi_map()
        broker_flow = load_broker_flow(broker_conc)
        fund_map    = load_fundamental_map()

        signals = []
        for symbol in df["symbol"].unique():
            symbol_df = df[df["symbol"] == symbol].copy().reset_index(drop=True)
            score, breakdown, last_close = score_symbol(
                symbol, symbol_df, rsi_map, broker_flow, fund_map
            )
            if last_close is None:
                continue
            if score >= buy_threshold:
                side = "BUY"
            elif score <= sell_threshold:
                side = "SELL"
            else:
                side = "NEUTRAL"

            signals.append({
                "symbol":   symbol,
                "side":     side,
                "score":    score,
                "price":    last_close,
                "ema":      breakdown["ema"],
                "macd":     breakdown["macd"],
                "volume":   breakdown["volume"],
                "rsi":      breakdown["rsi"],
                "broker":   breakdown["broker"],
                "fundamental": breakdown["fundamental"],
            })

        signals.sort(key=lambda x: abs(x["score"]), reverse=True)
        return signals

    def _get_indicators(self):
        if not os.path.exists("chukul_indicators.csv"):
            return []
        df = pd.read_csv("chukul_indicators.csv")
        cols = ["symbol", "rsi14", "support_1", "support_2", "support_3",
                "resistance_1", "resistance_2", "resistance_3", "level_alerts", "fetched_at"]
        cols = [c for c in cols if c in df.columns]
        return df[cols].to_dict("records")

    def _get_fundamentals(self):
        if not os.path.exists("chukul_fundamental.csv"):
            return []
        df = pd.read_csv("chukul_fundamental.csv")
        cols = ["symbol", "eps", "net_worth", "roe", "roa", "paidup_capital",
                "quarter", "fiscal_year", "fetched_at"]
        cols = [c for c in cols if c in df.columns]
        return df[cols].fillna("").to_dict("records")

    def _get_brokers(self):
        result = {"buy": [], "sell": []}
        if os.path.exists("chukul_broker_buy.csv"):
            result["buy"] = pd.read_csv("chukul_broker_buy.csv").to_dict("records")
        if os.path.exists("chukul_broker_sell.csv"):
            result["sell"] = pd.read_csv("chukul_broker_sell.csv").to_dict("records")
        return result

    def _get_floorsheet(self):
        files = sorted(glob.glob("chukul_floorsheet_*_summary.csv"), reverse=True)
        if not files:
            return {"date": None, "data": []}
        latest = files[0]
        date = latest.replace("chukul_floorsheet_", "").replace("_summary.csv", "")
        df = pd.read_csv(latest)
        return {"date": date, "data": df.to_dict("records")}

    def _get_market(self):
        if not os.path.exists("live_market_data.csv"):
            return []
        return pd.read_csv("live_market_data.csv").to_dict("records")

    def log_message(self, format, *args):
        pass  # suppress per-request logging


def main():
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    server = HTTPServer((HOST, PORT), DashboardHandler)
    url = f"http://{HOST}:{PORT}/analysis.html"
    print(f"Dashboard → {url}")
    print("Press Ctrl+C to stop.\n")
    threading.Timer(0.8, lambda: webbrowser.open(url)).start()
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nServer stopped.")


if __name__ == "__main__":
    main()
