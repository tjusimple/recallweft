from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import threading
import unittest

from recall import Gateway, RecallError, Scope, recall, select_evidence


SCOPE = Scope("test-team", "test-user", "test-agent")


class RankingTests(unittest.TestCase):
    def test_exact_dedup_and_citations(self):
        result = select_evidence({
            "atomic": [{"id": "a", "content": "Build"}, {"id": "dup", "content": "Build"}],
            "conversation": [{"id": "c", "content": "Build"}, {"id": "d", "content": "build"}],
        }, 50, 8)
        items = result["evidence"]
        self.assertEqual([v["text"] for v in items], ["Build", "build"])
        self.assertEqual([c["id"] for c in items[0]["citations"]], ["a", "c"])

    def test_budget_is_utf8_bytes_and_skips_oversize(self):
        result = select_evidence({"atomic": [
            {"id": "large", "content": "甲乙"}, {"id": "fit", "content": "甲"},
            {"id": "small", "content": "z"},
        ]}, 4, 8)
        self.assertEqual([v["text"] for v in result["evidence"]], ["甲", "z"])
        self.assertEqual(result["evidence_bytes"], 4)

    def test_disables_partial_results_by_default(self):
        def search(source, scope, query, limit):
            if source == "atomic":
                raise RecallError("HTTP 503")
            return [{"id": "x", "content": "history"}]
        with self.assertRaisesRegex(RecallError, "atomic: HTTP 503"):
            recall(search, SCOPE, "test")
        result = recall(search, SCOPE, "test", partial=True)
        self.assertEqual(result["failed_sources"], {"atomic": "HTTP 503"})
        self.assertEqual(result["evidence"][0]["text"], "history")

    def test_invalid_inputs_do_not_call_gateway(self):
        with self.assertRaises(ValueError):
            recall(lambda *unused: self.fail("must not fetch"), SCOPE, " ")
        with self.assertRaises(ValueError):
            Gateway("http://example.com", "default", "test")


class GatewayTests(unittest.TestCase):
    def test_two_scoped_reads_use_documented_routes(self):
        seen = []
        sync = threading.Barrier(2, timeout=2)
        routes = {"/v3/atomic/search": "items", "/v3/conversation/search": "messages"}

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_POST(self):
                fields = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                seen.append((self.path, self.headers["X-TDAI-Service-Id"], fields))
                try:
                    sync.wait()
                except threading.BrokenBarrierError:
                    self.send_error(503)
                    return
                data = json.dumps({"code": 0, "data": {
                    routes[self.path]: [{"id": self.path, "content": "test evidence"}],
                }}).encode()
                self.send_response(200)
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

        with ThreadingHTTPServer(("127.0.0.1", 0), Handler) as server:
            worker = threading.Thread(target=server.serve_forever, daemon=True)
            worker.start()
            try:
                client = Gateway(f"http://127.0.0.1:{server.server_port}", "test-service", "fixture")
                result = recall(client.search, SCOPE, "index", limit=3)
            finally:
                server.shutdown()
                worker.join()
        self.assertEqual({v[0] for v in seen}, set(routes))
        self.assertTrue(all(v[1] == "test-service" for v in seen))
        self.assertTrue(all(v[2] == dict(team_id="test-team", user_id="test-user",
                                        agent_id="test-agent", query="index", limit=3)
                            for v in seen))
        self.assertEqual(len(result["evidence"]), 1)
        self.assertEqual(len(result["evidence"][0]["citations"]), 2)


if __name__ == "__main__":
    unittest.main()
