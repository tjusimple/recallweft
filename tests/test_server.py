import contextlib
import json
from pathlib import Path
import socket
import subprocess
import sys
from tempfile import TemporaryDirectory
import time
import unittest
from urllib.error import HTTPError, URLError
from urllib.request import ProxyHandler, Request, build_opener


ROOT = Path(__file__).resolve().parents[1]
OPENER = build_opener(ProxyHandler({}))


class ServerTests(unittest.TestCase):
    def setUp(self):
        self.temporary = TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.home = Path(self.temporary.name) / "cold-state"
        with socket.socket() as host:
            host.bind(("127.0.0.1", 0))
            self.port = host.getsockname()[1]
        self.base = f"http://127.0.0.1:{self.port}"
        self.command = [sys.executable, str(ROOT / "serve.py"), "--state-dir", str(self.home), "--port", str(self.port)]

    @contextlib.contextmanager
    def running(self):
        process = subprocess.Popen(self.command + ["start"], cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        try:
            deadline = time.monotonic() + 8
            while time.monotonic() < deadline and process.poll() is None:
                try:
                    with OPENER.open(self.base + "/health", timeout=.3) as response:
                        self.assertEqual(json.load(response)["storage"], "sqlite-fts5")
                    break
                except (URLError, TimeoutError):
                    time.sleep(.05)
            else:
                self.fail("local service did not become ready")
            yield
        finally:
            if process.poll() is None:
                process.terminate()
            process.communicate(timeout=5)

    def cli(self, *args):
        command = subprocess.run(self.command + list(args), cwd=ROOT, capture_output=True, timeout=8, check=True)
        return json.loads(command.stdout)

    def post(self, path, data, auth=True):
        headers = {"Content-Type": "application/json"}
        if auth:
            headers["Authorization"] = "Bearer " + (self.home / "api-key").read_text()
        op = Request(self.base + path, method="POST", data=json.dumps(data).encode(), headers=headers)
        with OPENER.open(op, timeout=3) as response:
            return json.load(response)["data"]

    def test_cold_start_real_write_search_and_restart(self):
        phrase = "The redwood deployment starts after the schema check."
        with self.running():
            self.assertEqual(self.cli("search", "redwood")["evidence"], [])
            self.assertEqual(self.cli("remember", "--request-id", "r1", phrase), {"stored": 1, "replayed": False})
            self.assertEqual(self.cli("remember", "--request-id", "r1", phrase), {"stored": 1, "replayed": True})
            self.cli("remember", "--kind", "atomic", phrase)
            self.assertEqual(self.cli("search", "--user", "other", "redwood")["evidence"], [])
            self.assertEqual(self.cli("search", "--team", "other", "redwood")["evidence"], [])
            self.assertEqual(self.cli("search", "--agent", "other", "redwood")["evidence"], [])
            self.cli("remember", "金丝雀部署完成")
            self.assertEqual(self.cli("search", "金丝雀")["evidence"][0]["text"], "金丝雀部署完成")
        with self.running():
            result = self.cli("search", "redwood deployment")
            self.assertEqual(len(result["evidence"]), 1)
            item = result["evidence"][0]
            self.assertEqual(item["text"], phrase)
            self.assertEqual({citation["source"] for citation in item["citations"]}, {"atomic", "conversation"})
            self.assertTrue((self.home / "memory.sqlite3").is_file())

    def test_http_auth_validation_and_idempotency(self):
        body = {"user_id": "http-user", "session_id": "s", "request_id": "r",
                "messages": [{"role": "user", "content": "Record the rollout checkpoint"}]}
        with self.running():
            with self.assertRaises(HTTPError) as rejected:
                self.post("/v1/memories/add", body, auth=False)
            self.assertEqual(rejected.exception.code, 401)
            self.assertEqual(self.post("/v1/memories/add", body)["stored"], 1)
            changed = {**body, "messages": [{"content": "Conflict"}]}
            with self.assertRaises(HTTPError) as rejected:
                self.post("/v1/memories/add", changed)
            self.assertEqual(rejected.exception.code, 409)
            query = {"user_id": "http-user", "query": "rollout", "top_k": 1}
            self.assertEqual(len(self.post("/v1/memories/search", query)["items"]), 1)
            self.assertFalse(self.post("/v1/memories/search", dict(query, user_id="other"))["items"])
            with self.assertRaises(HTTPError) as rejected:
                self.post("/v1/memories/search", dict(query, top_k=True))
            self.assertEqual(rejected.exception.code, 400)


if __name__ == "__main__":
    unittest.main()
