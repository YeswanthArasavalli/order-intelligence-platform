"""CONTROLLED TEST HTTP receiver with durable SQLite idempotency receipts.

Not Slack/email delivery. Bound to loopback, no credentials. Tests program response
sequences; receipt/attempt evidence survives receiver process restart with its file.
"""

from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import re
import sqlite3
import threading
import time
from observability import telemetry as m9


class Receiver(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, ledger, port=0):
        self.ledger = str(ledger)
        with self.db() as conn:
            conn.executescript('''CREATE TABLE IF NOT EXISTS receipts (
                notification_id TEXT PRIMARY KEY, request_digest TEXT NOT NULL,
                reference TEXT NOT NULL, accepted_at TEXT NOT NULL, payload TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS calls (sequence INTEGER PRIMARY KEY, notification_id TEXT,
                destination TEXT, mode TEXT, status INTEGER, received_at TEXT);
                CREATE TABLE IF NOT EXISTS plans (destination TEXT PRIMARY KEY,modes TEXT,position INTEGER);''')
        super().__init__(("127.0.0.1", port), Handler)

    def db(self):
        conn = sqlite3.connect(self.ledger, timeout=10)
        conn.row_factory = sqlite3.Row
        return conn

    @property
    def endpoint(self):
        return f"http://127.0.0.1:{self.server_port}/notify"

    def plan(self, destination, modes):
        with self.db() as conn:
            conn.execute("INSERT OR REPLACE INTO plans VALUES (?,?,0)", (destination, json.dumps(modes)))

    def records(self):
        with self.db() as conn:
            return {table: [dict(r) for r in conn.execute(f"SELECT * FROM {table} ORDER BY 1")]
                    for table in ("calls", "receipts")}


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *_):
        pass  # No URLs, bodies or access logs.

    def respond(self, status, value):
        body = json.dumps(value).encode()
        try:
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            pass  # Deliberately timed-out client; receipt remains durable.

    def do_POST(self):
        try:
            length = int(self.headers.get("Content-Length", "0"))
            if self.path != "/notify" or not 0 < length <= 32768:
                return self.respond(400, {"error": "invalid request"})
            data = json.loads(self.rfile.read(length))
            key, destination = data["notification"]["notification_id"], data["destination"]
            if not re.fullmatch(r"[0-9a-f]{64}", key) or self.headers.get("Idempotency-Key") != key:
                return self.respond(400, {"error": "invalid identity"})
            encoded = json.dumps(data, sort_keys=True, separators=(",", ":"))
            request_digest = hashlib.sha256(encoded.encode()).hexdigest()
            now = datetime.now(timezone.utc).isoformat()
            with self.server.db() as conn:
                conn.execute("BEGIN IMMEDIATE")
                receipt = conn.execute("SELECT * FROM receipts WHERE notification_id=?", (key,)).fetchone()
                plan = conn.execute("SELECT * FROM plans WHERE destination=?", (destination,)).fetchone()
                modes = json.loads(plan["modes"]) if plan else ["success"]
                position = plan["position"] if plan else 0
                mode = modes[min(position, len(modes) - 1)]
                if plan:
                    conn.execute("UPDATE plans SET position=position+1 WHERE destination=?", (destination,))
                if receipt:
                    status = 200 if receipt["request_digest"] == request_digest else 409
                    mode = "deduplicated" if status == 200 else "conflict"
                else:
                    status = int(mode) if mode.isdigit() else 200
                    if mode in {"success", "accepted_timeout"}:
                        conn.execute("INSERT INTO receipts VALUES (?,?,?,?,?)",
                                     (key, request_digest, "controlled-" + key, now, encoded))
                conn.execute("INSERT INTO calls(notification_id,destination,mode,status,received_at) VALUES (?,?,?,?,?)",
                             (key, destination, mode, status, now))
            # Receipt and call records commit before response; acceptance ambiguity is real.
            if not receipt and mode in {"success", "accepted_timeout"}:
                m9.record("oip_provider_acceptances_total", component="provider", provider="controlled_http", channel="http")
            if mode in {"timeout", "accepted_timeout"}:
                time.sleep(0.4)
            if status != 200:
                return self.respond(status, {"error": "CONTROLLED_SECRET_CANARY_DO_NOT_PERSIST"})
            if mode in {"malformed", "timeout"}:
                return self.respond(200, {"invalid": True})
            return self.respond(200, {"notification_id": key, "reference": "controlled-" + key, "status": "accepted"})
        except (ValueError, KeyError, TypeError):
            self.respond(400, {"error": "invalid controlled request"})


@contextmanager
def running_receiver(ledger, port=0):
    receiver = Receiver(ledger, port)
    thread = threading.Thread(target=receiver.serve_forever, daemon=True)
    thread.start()
    try:
        yield receiver
    finally:
        receiver.shutdown()
        receiver.server_close()
        thread.join(timeout=5)


def main():
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ledger", required=True)
    parser.add_argument("--port", type=int, required=True)
    args = parser.parse_args()
    server = Receiver(args.ledger, args.port)
    try:
        server.serve_forever()
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
