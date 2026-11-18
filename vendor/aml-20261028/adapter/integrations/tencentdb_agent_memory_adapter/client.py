from __future__ import annotations

import asyncio
import json
from typing import Any

import httpx

from .config import AdapterConfig


RETRYABLE_STATUSES = {408, 409, 425, 429, 500, 502, 503, 504}


class UpstreamError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        status_code: int = 502,
        retryable: bool = False,
        outcome_uncertain: bool = False,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.retryable = retryable
        self.outcome_uncertain = outcome_uncertain


def _safe_detail(body: Any) -> str:
    if isinstance(body, dict):
        for key in ("message", "error", "detail"):
            value = body.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()[:500]
        return json.dumps(body, ensure_ascii=False)[:500]
    return str(body).replace("\r", " ").replace("\n", " ")[:500]


class TencentMemoryClient:
    def __init__(
        self,
        config: AdapterConfig,
        *,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self.config = config
        headers = {
            "Content-Type": "application/json",
            "x-tdai-service-id": config.upstream_service_id,
        }
        if config.upstream_api_key:
            headers["Authorization"] = f"Bearer {config.upstream_api_key}"
        self._client = httpx.AsyncClient(
            base_url=config.upstream_base_url.rstrip("/") + "/",
            headers=headers,
            timeout=httpx.Timeout(config.upstream_timeout_seconds, connect=10.0),
            transport=transport,
        )

    async def close(self) -> None:
        await self._client.aclose()

    async def _request_json(
        self,
        method: str,
        path: str,
        *,
        payload: dict[str, Any] | None = None,
        attempts: int | None = None,
        outcome_uncertain_on_transport: bool = False,
    ) -> dict[str, Any]:
        max_attempts = max(1, attempts or self.config.upstream_retry_attempts)
        last_error: UpstreamError | None = None
        for attempt in range(1, max_attempts + 1):
            try:
                response = await self._client.request(
                    method,
                    path.lstrip("/"),
                    json=payload if payload is not None else None,
                )
            except (httpx.TimeoutException, httpx.TransportError) as exc:
                last_error = UpstreamError(
                    f"TencentDB transport failure: {type(exc).__name__}",
                    status_code=504 if isinstance(exc, httpx.TimeoutException) else 502,
                    retryable=True,
                    outcome_uncertain=outcome_uncertain_on_transport,
                )
                if attempt >= max_attempts:
                    raise last_error from exc
            else:
                try:
                    body = response.json()
                except ValueError as exc:
                    raise UpstreamError(
                        "TencentDB returned a non-JSON response",
                        status_code=502,
                        retryable=response.status_code >= 500,
                    ) from exc
                if not isinstance(body, dict):
                    raise UpstreamError("TencentDB response must be a JSON object")
                if response.status_code < 400:
                    return body
                retryable = response.status_code in RETRYABLE_STATUSES
                last_error = UpstreamError(
                    f"TencentDB HTTP {response.status_code}: {_safe_detail(body)}",
                    status_code=response.status_code,
                    retryable=retryable,
                    outcome_uncertain=outcome_uncertain_on_transport and retryable,
                )
                if not retryable or attempt >= max_attempts:
                    raise last_error

            delay = min(
                self.config.retry_max_seconds,
                self.config.retry_base_seconds * (2 ** (attempt - 1)),
            )
            if delay:
                await asyncio.sleep(delay)
        assert last_error is not None
        raise last_error

    async def _post_envelope(
        self,
        path: str,
        payload: dict[str, Any],
        *,
        attempts: int | None = None,
        outcome_uncertain_on_transport: bool = False,
    ) -> Any:
        body = await self._request_json(
            "POST",
            path,
            payload=payload,
            attempts=attempts,
            outcome_uncertain_on_transport=outcome_uncertain_on_transport,
        )
        code = body.get("code")
        if code != 0:
            numeric_code = int(code) if isinstance(code, (int, float)) else 502
            if numeric_code == 4291:
                status_code = 429
            else:
                status_code = numeric_code if 400 <= numeric_code < 600 else 502
            raise UpstreamError(
                f"TencentDB business error {code}: {_safe_detail(body)}",
                status_code=status_code,
                retryable=status_code in RETRYABLE_STATUSES,
            )
        return body.get("data")

    async def health(self) -> dict[str, Any]:
        return await self._request_json("GET", "/health", attempts=1)

    async def add_conversation(self, payload: dict[str, Any]) -> dict[str, Any]:
        # Add is not retried here: a timeout can occur after the write committed.
        # The adapter's persistent request_id reconciliation decides whether a retry is safe.
        data = await self._post_envelope(
            "/v3/conversation/add",
            payload,
            attempts=1,
            outcome_uncertain_on_transport=True,
        )
        if not isinstance(data, dict):
            raise UpstreamError("TencentDB Add response is missing its data object")
        return data

    async def conversation_query(
        self,
        *,
        isolation: dict[str, str],
        session_id: str,
    ) -> list[dict[str, Any]]:
        offset = 0
        messages: list[dict[str, Any]] = []
        while True:
            payload: dict[str, Any] = {
                **isolation,
                "session_id": session_id,
                "limit": 100,
                "offset": offset,
            }
            data = await self._post_envelope("/v3/conversation/query", payload)
            if not isinstance(data, dict):
                raise UpstreamError("TencentDB conversation query returned invalid data")
            batch = data.get("messages")
            if not isinstance(batch, list):
                raise UpstreamError("TencentDB conversation query is missing messages")
            messages.extend(item for item in batch if isinstance(item, dict))
            total = int(data.get("total") or len(messages))
            if not batch or len(messages) >= total:
                return messages
            offset += len(batch)

    async def flush_session(
        self,
        session_id: str,
        *,
        user_id: str,
        team_id: str,
        agent_id: str,
    ) -> None:
        body = await self._request_json(
            "POST",
            "/session/end",
            payload={
                "session_key": session_id,
                "user_id": user_id,
                "team_id": team_id,
                "agent_id": agent_id,
            },
        )
        if body.get("flushed") is not True:
            raise UpstreamError(
                f"TencentDB did not confirm session flush: {_safe_detail(body)}"
            )

    async def pipeline_status(self) -> dict[str, Any]:
        data = await self._post_envelope("/v2/pipeline/status", {})
        if not isinstance(data, dict):
            raise UpstreamError("TencentDB pipeline status returned invalid data")
        return data

    async def search_atomic(
        self, *, query: str, limit: int, isolation: dict[str, str]
    ) -> list[dict[str, Any]]:
        data = await self._post_envelope(
            "/v3/atomic/search", {**isolation, "query": query, "limit": limit}
        )
        items = data.get("items") if isinstance(data, dict) else None
        if not isinstance(items, list):
            raise UpstreamError("TencentDB atomic search is missing items")
        return [item for item in items if isinstance(item, dict)]

    async def search_conversation(
        self, *, query: str, limit: int, isolation: dict[str, str]
    ) -> list[dict[str, Any]]:
        data = await self._post_envelope(
            "/v3/conversation/search",
            {**isolation, "query": query, "limit": limit},
        )
        messages = data.get("messages") if isinstance(data, dict) else None
        if not isinstance(messages, list):
            raise UpstreamError("TencentDB conversation search is missing messages")
        return [item for item in messages if isinstance(item, dict)]

    async def list_scenarios(self, isolation: dict[str, str]) -> list[dict[str, Any]]:
        data = await self._post_envelope("/v3/scenario/ls", isolation)
        entries = data.get("entries") if isinstance(data, dict) else None
        if not isinstance(entries, list):
            raise UpstreamError("TencentDB scenario list is missing entries")
        return [item for item in entries if isinstance(item, dict)]

    async def read_scenario(
        self, *, isolation: dict[str, str], path: str
    ) -> dict[str, Any] | None:
        data = await self._post_envelope(
            "/v3/scenario/read", {**isolation, "path": path}
        )
        if not isinstance(data, dict) or not data.get("content"):
            return None
        return data

    async def read_core(self, isolation: dict[str, str]) -> dict[str, Any] | None:
        data = await self._post_envelope("/v3/core/read", isolation)
        if not isinstance(data, dict) or not data.get("content"):
            return None
        return data
