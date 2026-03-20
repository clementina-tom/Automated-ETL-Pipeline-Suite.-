"""Integration-style tests using real local HTTP/SQLite sources."""

import json
import sqlite3
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pandas as pd

from etl_pipeline.extractors import APIExtractor, DatabaseExtractor, WebExtractor


class _TestHandler(BaseHTTPRequestHandler):
    def do_GET(self):  # noqa: N802
        if self.path.startswith("/table"):
            html = """
            <html><body>
              <table>
                <tr><th>id</th><th>value</th></tr>
                <tr><td>1</td><td>A</td></tr>
                <tr><td>2</td><td>B</td></tr>
              </table>
            </body></html>
            """
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.end_headers()
            self.wfile.write(html.encode("utf-8"))
            return

        if self.path.startswith("/api"):
            # Supports ?page=x&size=y
            _, _, query = self.path.partition("?")
            params = dict(part.split("=") for part in query.split("&") if "=" in part)
            page = int(params.get("page", 1))
            size = int(params.get("size", 2))

            all_data = [
                {"beneficiary_id": "B001", "id": "G001", "amount": 100},
                {"beneficiary_id": "B002", "id": "G002", "amount": 200},
                {"beneficiary_id": "B003", "id": "G003", "amount": 300},
            ]
            start = (page - 1) * size
            end = start + size
            payload = {"data": all_data[start:end]}

            body = json.dumps(payload).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        self.send_response(404)
        self.end_headers()

    def log_message(self, format, *args):  # noqa: A003
        # Silence server logs in tests
        return


def _start_test_server():
    server = HTTPServer(("127.0.0.1", 0), _TestHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server


def test_web_extractor_reads_real_html_table():
    server = _start_test_server()
    try:
        base_url = f"http://127.0.0.1:{server.server_port}"
        df = WebExtractor(source=f"{base_url}/table").run()
        assert len(df) == 2
        assert list(df.columns) == ["id", "value"]
    finally:
        server.shutdown()


def test_api_extractor_reads_paginated_endpoint():
    server = _start_test_server()
    try:
        base_url = f"http://127.0.0.1:{server.server_port}"
        df = APIExtractor(endpoint_url=f"{base_url}/api", page_size=2).run()
        assert len(df) == 3
        assert {"beneficiary_id", "id", "amount"}.issubset(df.columns)
    finally:
        server.shutdown()


def test_database_extractor_reads_real_sqlite(tmp_path):
    db_path = tmp_path / "source.db"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE gifts (id TEXT, amount REAL)")
    conn.execute("INSERT INTO gifts VALUES ('G001', 100.0)")
    conn.execute("INSERT INTO gifts VALUES ('G002', 200.0)")
    conn.commit()
    conn.close()

    extractor = DatabaseExtractor(
        connection_string=f"sqlite:///{db_path}",
        query="SELECT * FROM gifts ORDER BY id",
    )
    df = extractor.run()

    assert len(df) == 2
    assert df.iloc[0]["id"] == "G001"
