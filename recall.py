"""Read-only evidence selection for a TencentDB Agent Memory v3 gateway."""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass
import json
import math
import os
import sys
from typing import Any, Callable
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener


ROUTES = {
    "atomic": ("/v3/atomic/search", "items"),
    "conversation": ("/v3/conversation/search", "messages"),
}
RESPONSE_BYTES = 1024 * 1024


class RecallError(Exception):
    """A gateway error report that does not include query text or response bodies."""


@dataclass(frozen=True)
class Scope:
    team_id: str
    user_id: str
    agent_id: str

    def __post_init__(self) -> None:
        if any(not value.strip() for value in (self.team_id, self.user_id, self.agent_id)):
            raise ValueError("team, user and agent are all required")


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class Gateway:
    def __init__(self, url: str, service: str, token: str, timeout: float = 5.0):
        parsed = urlsplit(url)
        if (
            not parsed.hostname
            or parsed.username is not None
            or parsed.password is not None
            or parsed.query
            or parsed.fragment
            or parsed.scheme not in {"http", "https"}
            or (parsed.scheme == "http" and parsed.hostname not in {"127.0.0.1", "::1", "localhost"})
        ):
            raise ValueError("use an HTTPS gateway or an HTTP loopback address, without credentials in the URL")
        if not service.strip() or not token.strip() or any(c in service+token for c in "\r\n"):
            raise ValueError("provide a service ID and a gateway token")
        if not math.isfinite(timeout) or not 0 < timeout <= 60:
            raise ValueError("timeout must be between 0 and 60 seconds")
        self.url, self.timeout = url.rstrip("/"), timeout
        self.headers = {
            "Content-Type": "application/json",
            "Authorization": "Bearer " + token,
            "x-tdai-service-id": service,
        }

    def search(self, source: str, scope: Scope, query: str, limit: int) -> list[dict[str, Any]]:
        route, key = ROUTES[source]
        payload = json.dumps({**asdict(scope), "query": query, "limit": limit}).encode()
        request = Request(self.url + route, data=payload, headers=self.headers, method="POST")
        try:
            with build_opener(ProxyHandler({}), _NoRedirect()).open(request, timeout=self.timeout) as response:
                body = response.read(RESPONSE_BYTES + 1)
        except HTTPError as exc:
            raise RecallError(f"HTTP {exc.code}") from None
        except (URLError, TimeoutError, OSError):
            raise RecallError("connection or timeout error") from None
        if len(body) > RESPONSE_BYTES:
            raise RecallError("response exceeds size limit")
        try:
            envelope = json.loads(body)
        except (ValueError, UnicodeError):
            raise RecallError("invalid JSON response") from None
        data = envelope.get("data") if isinstance(envelope, dict) else None
        if not isinstance(data, dict) or type(envelope.get("code")) is not int or envelope["code"] != 0:
            raise RecallError("gateway did not return a success envelope")
        records = data.get(key)
        if not isinstance(records, list) or not all(isinstance(row, dict) for row in records):
            raise RecallError("invalid search records")
        return records[:limit]


Search = Callable[[str, Scope, str, int], list[dict[str, Any]]]


def select_evidence(results: dict[str, list[dict[str, Any]]], budget: int, limit: int) -> dict[str, Any]:
    candidates: dict[str, dict[str, Any]] = {}
    for source in ROUTES:
        for rank, row in enumerate(results.get(source, []), start=1):
            content, identifier = row.get("content"), row.get("id")
            if not isinstance(content, str) or not content.strip():
                continue
            if not isinstance(identifier, (str, int)) or type(identifier) is bool or not str(identifier).strip():
                continue
            try:
                size = len(content.encode("utf-8"))
            except UnicodeEncodeError:
                continue
            if size > budget:
                continue
            candidate = candidates.setdefault(content, {"text": content, "bytes": size, "citations": {}})
            candidate["citations"].setdefault(source, {"source": source, "id": str(identifier), "rank": rank})
    ranked = []
    for candidate in candidates.values():
        citations = list(candidate.pop("citations").values())
        score = math.fsum(1 / (20 + c["rank"]) for c in citations)
        ranked.append({**candidate, "citations": citations, "fusion_score": score})
    ranked.sort(key=lambda x: (-x["fusion_score"], x["bytes"], x["text"]))
    selected, used = [], 0
    for item in ranked:
        if len(selected) >= limit:
            break
        if used + item["bytes"] <= budget:
            selected.append(item)
            used += item["bytes"]
    return {"evidence": selected, "evidence_bytes": used}


def recall(search: Search, scope: Scope, query: str, *, limit: int = 8, budget: int = 4096,
           partial: bool = False) -> dict[str, Any]:
    if type(limit) is not int or not 1 <= limit <= 25:
        raise ValueError("limit must be between 1 and 25")
    if type(budget) is not int or not 1 <= budget <= 131072:
        raise ValueError("budget must be between 1 and 131072 bytes")
    if not query.strip() or len(query) > 8192:
        raise ValueError("query must contain between 1 and 8192 characters")
    results, failures = {}, {}
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = {source: pool.submit(search, source, scope, query, limit) for source in ROUTES}
        for source, future in futures.items():
            try:
                results[source] = future.result()
            except RecallError as exc:
                failures[source] = str(exc)
    if failures and (not partial or not results):
        raise RecallError("; ".join(f"{source}: {message}" for source, message in failures.items()))
    return {**select_evidence(results, budget, limit), "failed_sources": failures}


def demo_search(source: str, scope: Scope, query: str, limit: int) -> list[dict[str, Any]]:
    shared = "Rebuild the index from the event snapshot before enabling new writes."
    samples = {
        "atomic": [{"id": "a1", "content": shared}],
        "conversation": [{"id": "c8", "content": "Verify the event count after the rebuild."},
                         {"id": "c9", "content": shared}],
    }
    return samples[source][:limit]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("query", nargs="?")
    parser.add_argument("--demo", action="store_true")
    parser.add_argument("--url", default="http://127.0.0.1:8420")
    parser.add_argument("--service", default="default")
    parser.add_argument("--team", default="")
    parser.add_argument("--user", default="")
    parser.add_argument("--agent", default="")
    parser.add_argument("--budget", default=4096, type=int)
    parser.add_argument("--limit", default=8, type=int)
    parser.add_argument("--partial", action="store_true")
    args = parser.parse_args()
    try:
        if args.demo:
            scope, query, search = Scope("demo", "demo", "demo"), "index rebuild", demo_search
        else:
            scope = Scope(args.team, args.user, args.agent)
            query = args.query or ""
            search = Gateway(args.url, args.service, os.environ.get("TDAI_API_KEY", "")).search
        result = recall(search, scope, query, limit=args.limit, budget=args.budget, partial=args.partial)
    except (RecallError, ValueError) as exc:
        print(f"recall: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(result, ensure_ascii=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
