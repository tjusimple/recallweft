"""Local, persistent RecallWeft memory gateway and CLI (Python 3.10+)."""

from __future__ import annotations

import argparse
from contextlib import closing
from datetime import datetime, timezone
import hashlib
import hmac
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import re
import secrets
import sqlite3
import sys
import unicodedata
from urllib.error import HTTPError, URLError
from urllib.request import ProxyHandler, Request, build_opener

from recall import Gateway, RecallError, Scope, _NoRedirect, recall


DEFAULT_HOME = Path(__file__).resolve().parent / ".recallweft-runtime" / "local"
MAX_BODY = 1024 * 1024
MAX_TEXT = 8192
MAX_RESULTS = 25
WORD = re.compile(r"[\u3400-\u9fff]|[^\W\u3400-\u9fff_]+", re.UNICODE)


class InputError(ValueError):
    pass


class Conflict(InputError):
    pass


def text(obj, key, maximum, default=None):
    value = obj.get(key, default)
    if not isinstance(value, str) or not value.strip() or len(value) > maximum:
        raise InputError(f"invalid {key}")
    try:
        value.encode("utf-8")
    except UnicodeError:
        raise InputError(f"invalid {key}") from None
    return value


def words(value):
    return WORD.findall(unicodedata.normalize("NFKC", value).casefold())


def digest(value):
    data = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(data.encode()).hexdigest()


class Store:
    def __init__(self, path):
        self.path = path
        with closing(self.connect()) as db:
            db.executescript('''
                PRAGMA journal_mode=WAL;
                CREATE TABLE IF NOT EXISTS requests (
                    service TEXT, team TEXT, person TEXT, agent TEXT, request TEXT,
                    signature TEXT NOT NULL, item_count INTEGER NOT NULL,
                    PRIMARY KEY(service, team, person, agent, request));
                CREATE TABLE IF NOT EXISTS memories (
                    id INTEGER PRIMARY KEY, service TEXT, team TEXT, person TEXT, agent TEXT,
                    kind TEXT NOT NULL, session TEXT NOT NULL, content TEXT NOT NULL, created TEXT NOT NULL);
                CREATE INDEX IF NOT EXISTS memories_scope ON memories(service, team, person, agent, kind);
                CREATE VIRTUAL TABLE IF NOT EXISTS memory_fts USING fts5(tokens);
            ''')

    def connect(self):
        db = sqlite3.connect(self.path, timeout=10)
        db.row_factory = sqlite3.Row
        return db

    def add(self, scope, payload):
        request = text(payload, "request_id", 160)
        session = text(payload, "session_id", 160)
        messages = payload.get("messages")
        if not isinstance(messages, list) or not 1 <= len(messages) <= 64:
            raise InputError("messages must contain 1 to 64 items")
        items = []
        for item in messages:
            if not isinstance(item, dict):
                raise InputError("each message must be an object")
            kind = item.get("kind", "conversation")
            if kind not in ("atomic", "conversation"):
                raise InputError("kind must be atomic or conversation")
            items.append((kind, text(item, "content", MAX_TEXT)))
        signature = digest([session, items])
        created = datetime.now(timezone.utc).isoformat()
        with closing(self.connect()) as db, db:
            db.execute("BEGIN IMMEDIATE")
            previous = db.execute('''SELECT signature, item_count FROM requests
                WHERE service=? AND team=? AND person=? AND agent=? AND request=?''', (*scope, request)).fetchone()
            if previous:
                if previous["signature"] != signature:
                    raise Conflict("request_id already used for different content")
                return {"stored": previous["item_count"], "replayed": True}
            for kind, content in items:
                row = db.execute('''INSERT INTO memories(service,team,person,agent,kind,session,content,created)
                    VALUES (?,?,?,?,?,?,?,?)''', (*scope, kind, session, content, created))
                db.execute("INSERT INTO memory_fts(rowid,tokens) VALUES(?,?)", (row.lastrowid, " ".join(words(content))))
            db.execute("INSERT INTO requests VALUES(?,?,?,?,?,?,?)", (*scope, request, signature, len(items)))
        return {"stored": len(items), "replayed": False}

    def search(self, scope, query, limit, kind=None):
        tokens = list(dict.fromkeys(words(query)))[:64]
        if not tokens:
            return []
        match = " OR ".join('"' + token.replace('"', '""') + '"' for token in tokens)
        params = [match, *scope]
        sql = '''SELECT m.id,m.content,m.created,m.kind,m.session, bm25(memory_fts) AS relevance
                 FROM memory_fts JOIN memories m ON m.id=memory_fts.rowid
                 WHERE memory_fts MATCH ? AND m.service=? AND m.team=? AND m.person=? AND m.agent=?'''
        if kind:
            sql += " AND m.kind=?"
            params.append(kind)
        with closing(self.connect()) as db:
            return [{"id": str(r["id"]), "content": r["content"], "created_at": r["created"],
                     "kind": r["kind"], "session_id": r["session"], "score": max(0., -r["relevance"])}
                    for r in db.execute(sql + " ORDER BY relevance, m.id DESC LIMIT ?", (*params, limit))]


class Server(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, port, store, key):
        self.store, self.key = store, key.encode()
        super().__init__(("127.0.0.1", port), Handler)

    def get_request(self):
        connection, address = super().get_request()
        connection.settimeout(10)
        return connection, address


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass  # Do not log authorization, queries or submitted memories.

    def send_json(self, status, document):
        data = json.dumps(document, ensure_ascii=False, separators=(",", ":")).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.end_headers()
        self.wfile.write(data)

    def local_origin(self):
        port = self.server.server_port
        host = self.headers.get("Host", "").lower()
        return host in {f"127.0.0.1:{port}", f"localhost:{port}"} and not self.headers.get("Origin")

    def do_GET(self):
        if not self.local_origin():
            self.send_json(403, {"error": "local requests only"})
        elif self.path == "/health":
            self.send_json(200, {"status": "ok", "storage": "sqlite-fts5"})
        else:
            self.send_json(404, {"error": "unknown route"})

    def payload(self):
        if (self.headers.get_content_type() != "application/json" or self.headers.get("Transfer-Encoding")
                or len(self.headers.get_all("Content-Length", [])) != 1):
            raise InputError("a JSON request with Content-Length is required")
        length = int(self.headers["Content-Length"])
        if not 1 <= length <= MAX_BODY:
            raise InputError("invalid body size")
        obj = json.loads(self.rfile.read(length))
        if not isinstance(obj, dict):
            raise InputError("expected a JSON object")
        return obj

    def do_POST(self):
        if not self.local_origin():
            self.send_json(403, {"error": "local requests only"})
            return
        supplied = self.headers.get("Authorization", "").encode("utf-8")
        if not hmac.compare_digest(supplied, b"Bearer " + self.server.key):
            self.send_json(401, {"error": "invalid authorization"})
            return
        try:
            data = self.payload()
            service = text({"service": self.headers.get("X-TDAI-Service-Id", "default")}, "service", 120)
            scope = (service, text(data, "team_id", 160, "local"), text(data, "user_id", 160),
                     text(data, "agent_id", 160, "default"))
            if self.path == "/v1/memories/add":
                result = self.server.store.add(scope, data)
            elif self.path in {"/v1/memories/search", "/v3/atomic/search", "/v3/conversation/search"}:
                local = self.path.startswith("/v1/")
                kind = None if local else self.path.split("/")[2]
                limit = data.get("top_k" if local else "limit", 8)
                if type(limit) is not int or not 1 <= limit <= MAX_RESULTS:
                    raise InputError(f"limit must be between 1 and {MAX_RESULTS}")
                found = self.server.store.search(scope, text(data, "query", MAX_TEXT), limit, kind)
                result = {("messages" if kind == "conversation" else "items"): found}
            else:
                self.send_json(404, {"error": "unknown route"})
                return
        except Conflict:
            self.send_json(409, {"error": "request_id already used for different content"})
            return
        except InputError as error:
            self.send_json(400, {"error": str(error)})
            return
        except (ValueError, UnicodeError, TimeoutError, RecursionError):
            self.send_json(400, {"error": "invalid JSON request"})
            return
        except sqlite3.Error:
            self.send_json(503, {"error": "memory store unavailable"})
            return
        self.send_json(200, {"code": 0, "data": result})


def read_key(home, create=False):
    path = home / "api-key"
    if create and not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        try:
            with os.fdopen(os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), "w") as f:
                f.write(secrets.token_urlsafe(32))
        except FileExistsError:
            pass
    key = path.read_text(encoding="utf-8").strip()
    if not re.fullmatch(r"[A-Za-z0-9_-]{32,128}", key):
        raise InputError("invalid local key file")
    return key


def remember(url, key, scope, args):
    payload = {"team_id": scope.team_id, "user_id": scope.user_id, "agent_id": scope.agent_id,
               "request_id": args.request_id or secrets.token_hex(12), "session_id": args.session,
               "messages": [{"kind": args.kind, "content": args.content}]}
    request = Request(url + "/v1/memories/add", data=json.dumps(payload).encode(), method="POST",
                      headers={"Authorization": "Bearer " + key, "Content-Type": "application/json"})
    with build_opener(ProxyHandler({}), _NoRedirect()).open(request, timeout=10) as response:
        return json.load(response)["data"]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-dir", type=Path, default=DEFAULT_HOME)
    parser.add_argument("--port", type=int, default=8420)
    commands = parser.add_subparsers(dest="command")
    commands.add_parser("start")
    add = commands.add_parser("remember", help="write a real memory to the local service")
    ask = commands.add_parser("search", help="return stored evidence from the local service")
    for command in (add, ask):
        command.add_argument("--team", default="local")
        command.add_argument("--user", default="local-user")
        command.add_argument("--agent", default="default")
    add.add_argument("--kind", choices=("atomic", "conversation"), default="conversation")
    add.add_argument("--session", default="default")
    add.add_argument("--request-id")
    add.add_argument("content")
    ask.add_argument("query")
    args = parser.parse_args()
    try:
        if not 1 <= args.port <= 65535:
            raise InputError("port must be between 1 and 65535")
        if args.command in (None, "start"):
            key = read_key(args.state_dir, create=True)
            with Server(args.port, Store(args.state_dir / "memory.sqlite3"), key) as server:
                print(f"RecallWeft ready at http://127.0.0.1:{args.port}", flush=True)
                try:
                    server.serve_forever()
                except KeyboardInterrupt:
                    pass
            return 0
        key = read_key(args.state_dir)
        scope = Scope(args.team, args.user, args.agent)
        url = f"http://127.0.0.1:{args.port}"
        result = (remember(url, key, scope, args) if args.command == "remember"
                  else recall(Gateway(url, "default", key).search, scope, args.query))
        print(json.dumps(result, ensure_ascii=True, indent=2))
        return 0
    except HTTPError as error:
        print(f"recallweft: HTTP {error.code}", file=sys.stderr)
    except (URLError, TimeoutError, FileNotFoundError):
        print("recallweft: start the local server first and check the port and state directory", file=sys.stderr)
    except (InputError, RecallError, ValueError) as error:
        print(f"recallweft: {error}", file=sys.stderr)
    except sqlite3.Error:
        print("recallweft: SQLite with FTS5 is required", file=sys.stderr)
    except OSError:
        print("recallweft: could not open the state directory or bind the local port", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
