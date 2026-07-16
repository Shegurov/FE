"""
Embedded BI server — zero external dependencies (Python stdlib http only).

Serves a single-page dashboard plus a small JSON API over the marts. It opens
the warehouse read-only, so the BI layer can never corrupt the store and can run
concurrently with ingestion.

    python -m bi.server --db dwh.duckdb --port 8080
"""
from __future__ import annotations

import argparse
import json
from datetime import date, datetime
from decimal import Decimal
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

from warehouse.engine import Warehouse
from bi.metrics import MARTS

DASHBOARD = Path(__file__).with_name("dashboard.html")


def _json_default(o):
    if isinstance(o, (datetime, date)):
        return o.isoformat()
    if isinstance(o, Decimal):
        return float(o)
    raise TypeError(f"not serializable: {type(o)}")


class Handler(BaseHTTPRequestHandler):
    db_path = "dwh.duckdb"

    def log_message(self, *a):  # keep the console quiet
        pass

    def _send(self, code, body, ctype="application/json"):
        payload = body if isinstance(body, bytes) else body.encode()
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def do_GET(self):
        path = urlparse(self.path).path
        try:
            if path in ("/", "/index.html"):
                return self._send(200, DASHBOARD.read_bytes(), "text/html; charset=utf-8")
            if path == "/api/summary":
                return self._send(200, json.dumps(self._summary(), default=_json_default))
            if path.startswith("/api/mart/"):
                name = path.rsplit("/", 1)[-1]
                if name not in MARTS:
                    return self._send(404, json.dumps({"error": "unknown mart"}))
                return self._send(200, json.dumps(self._mart(name), default=_json_default))
            return self._send(404, json.dumps({"error": "not found"}))
        except Exception as exc:  # never 500 silently
            return self._send(500, json.dumps({"error": str(exc)}))

    # -- data access (read-only) --------------------------------------------

    def _wh(self) -> Warehouse:
        return Warehouse(self.db_path, read_only=True)

    def _summary(self) -> dict:
        wh = self._wh()
        try:
            row = wh.records(
                """
                SELECT
                    count(*)                                    AS tx_count,
                    count(DISTINCT customer_key)                AS customers,
                    sum(amount_base)                            AS gross_amount,
                    sum(CASE WHEN status='settled' THEN amount_base ELSE 0 END) AS revenue,
                    sum(fee_base)                               AS fees,
                    avg(amount_base)                            AS avg_ticket
                FROM core.fact_transaction
                """
            )[0]
            row["daily"] = wh.records(
                "SELECT event_date, revenue, tx_count, active_customers "
                "FROM mart.mart_daily_revenue ORDER BY event_date"
            )
            row["top_merchants"] = wh.records(
                "SELECT merchant_name, revenue, tx_count, decline_rate_pct "
                "FROM mart.mart_merchant_performance ORDER BY revenue DESC LIMIT 8"
            )
            row["currency_mix"] = wh.records(
                "SELECT currency_code, gross_base, tx_count "
                "FROM mart.mart_currency_mix ORDER BY gross_base DESC"
            )
            row["channels"] = wh.records(
                "SELECT channel, sum(tx_count) AS tx_count, sum(gross_amount) AS gross "
                "FROM mart.mart_channel_status GROUP BY channel ORDER BY gross DESC"
            )
            return row
        finally:
            wh.close()

    def _mart(self, name: str) -> list[dict]:
        wh = self._wh()
        try:
            return wh.records(f"SELECT * FROM mart.{name}")
        finally:
            wh.close()


def serve(db_path: str, host: str = "127.0.0.1", port: int = 8080) -> None:
    Handler.db_path = db_path
    httpd = ThreadingHTTPServer((host, port), Handler)
    print(f"BI dashboard  ->  http://{host}:{port}   (db: {db_path})")
    print("Ctrl-C to stop")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nstopped")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Embedded BI dashboard server")
    ap.add_argument("--db", default="dwh.duckdb")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=8080)
    args = ap.parse_args()
    serve(args.db, args.host, args.port)
