from __future__ import annotations

import asyncio
import hashlib
import json
import re
import time
from collections import Counter
from datetime import datetime, timedelta, timezone
from typing import Any

from .client import TencentMemoryClient, UpstreamError
from .config import AdapterConfig
from .models import AddRequest, SearchItem, SearchRequest
from .state import AdapterState, DirtySession, PayloadConflictError


_TOKEN_RE = re.compile(r"[A-Za-z0-9_]+(?:[-'][A-Za-z0-9_]+)*|[\u3400-\u9fff]")


class PipelineReadinessError(RuntimeError):
    pass


class AdapterService:
    def __init__(
        self,
        *,
        config: AdapterConfig,
        state: AdapterState,
        client: TencentMemoryClient,
    ) -> None:
        self.config = config
        self.state = state
        self.client = client
        self._add_slots = asyncio.Semaphore(config.add_concurrency)
        self._search_slots = asyncio.Semaphore(config.search_concurrency)
        self._flush_slots = asyncio.Semaphore(config.flush_concurrency)
        self._profile_read_slots = asyncio.Semaphore(min(8, config.search_concurrency))
        self._readiness_lock = asyncio.Lock()
        self._request_locks: dict[str, asyncio.Lock] = {}
        self._request_locks_guard = asyncio.Lock()

    async def close(self) -> None:
        await self.client.close()

    def agent_id_for_user(self, user_id: str) -> str:
        digest = hashlib.sha256(user_id.encode("utf-8")).hexdigest()[:24]
        return f"{self.config.agent_id_prefix}-{digest}"

    def isolation_for_user(self, user_id: str) -> dict[str, str]:
        # L2/L3 are scoped primarily by team+agent upstream. A per-user agent ID
        # prevents profile leakage between independent benchmark samples.
        return {
            "team_id": self.config.team_id,
            "agent_id": self.agent_id_for_user(user_id),
            "user_id": user_id,
        }

    async def add(self, request: AddRequest) -> dict[str, Any]:
        lock = await self._request_lock(request.request_id)
        async with lock:
            isolation = self.isolation_for_user(request.user_id)
            payload_sha256 = self._logical_payload_sha256(
                request=request,
                isolation=isolation,
            )
            messages = self._upstream_messages(request)
            upstream_payload: dict[str, Any] = {
                **isolation,
                "session_id": request.session_id,
                "messages": messages,
            }
            legacy_payload_sha256 = self._payload_sha256(upstream_payload)
            reservation = self.state.reserve(
                request_id=request.request_id,
                payload_sha256=payload_sha256,
                legacy_payload_sha256=legacy_payload_sha256,
                user_id=request.user_id,
                session_id=request.session_id,
                allow_committed_payload_rehash=any(
                    request.request_id.startswith(prefix)
                    for prefix in self.config.legacy_committed_rehash_request_prefixes
                ),
            )
            if reservation.status == "committed" and reservation.response is not None:
                return reservation.response

            response = {
                "success": True,
                "request_id": request.request_id,
                "user_id": request.user_id,
                "session_id": request.session_id,
            }

            if not reservation.created and await self._reconcile_pending_add(
                isolation=isolation,
                session_id=request.session_id,
                messages=messages,
            ):
                self.state.mark_committed(
                    request_id=request.request_id,
                    response=response,
                    user_id=request.user_id,
                    session_id=request.session_id,
                    agent_id=isolation["agent_id"],
                )
                return response

            try:
                async with self._add_slots:
                    await self.client.add_conversation(upstream_payload)
            except UpstreamError as exc:
                if not exc.outcome_uncertain:
                    self.state.mark_failed(request.request_id, str(exc))
                raise

            self.state.mark_committed(
                request_id=request.request_id,
                response=response,
                user_id=request.user_id,
                session_id=request.session_id,
                agent_id=isolation["agent_id"],
            )
            return response

    async def search(self, request: SearchRequest) -> dict[str, Any]:
        await self.ensure_pipeline_ready()
        isolation = self.isolation_for_user(request.user_id)
        query = self._compact_query(request.query)
        limit = min(100, request.top_k)

        async with self._search_slots:
            atomic_task = asyncio.create_task(
                self.client.search_atomic(query=query, limit=limit, isolation=isolation)
            )
            conversation_task = asyncio.create_task(
                self.client.search_conversation(
                    query=query, limit=limit, isolation=isolation
                )
            )
            scenario_task = asyncio.create_task(
                self._scenario_candidates(query=query, isolation=isolation)
            )
            core_task = asyncio.create_task(self._core_candidates(isolation=isolation))
            atomic, conversation, scenarios, core = await asyncio.gather(
                atomic_task, conversation_task, scenario_task, core_task
            )

        source_lists = {
            "atomic": self._normalize_atomic(atomic),
            "scenario": scenarios,
            "core": core,
            "conversation": self._normalize_conversation(conversation),
        }
        items = self._reciprocal_rank_fusion(source_lists, top_k=request.top_k)
        return {"data": [item.model_dump(exclude_none=True) for item in items]}

    async def ensure_pipeline_ready(self) -> None:
        if not self.state.dirty_sessions():
            return
        async with self._readiness_lock:
            while True:
                target_generation = self.state.current_generation()
                dirty = self.state.dirty_sessions(up_to_generation=target_generation)
                if not dirty:
                    return

                async def flush(item: DirtySession) -> None:
                    async with self._flush_slots:
                        await self.client.flush_session(
                            item.session_id,
                            user_id=item.user_id,
                            team_id=self.config.team_id,
                            agent_id=item.agent_id,
                        )

                await asyncio.gather(*(flush(item) for item in dirty))
                await self._wait_for_pipeline_idle()
                self.state.mark_sessions_ready(up_to_generation=target_generation)
                if self.state.current_generation() <= target_generation:
                    return

    async def _wait_for_pipeline_idle(self) -> None:
        deadline = time.monotonic() + self.config.pipeline_ready_timeout_seconds
        stable = 0
        idle_since: float | None = None
        while time.monotonic() < deadline:
            status = await self.client.pipeline_status()
            if self._all_pipeline_layers_idle(status):
                if idle_since is None:
                    idle_since = time.monotonic()
                stable += 1
                quiet_for = time.monotonic() - idle_since
                if (
                    stable >= self.config.pipeline_stable_polls
                    and quiet_for >= self.config.pipeline_quiet_period_seconds
                ):
                    return
            else:
                stable = 0
                idle_since = None
            await asyncio.sleep(self.config.pipeline_poll_interval_seconds)
        raise PipelineReadinessError(
            "TencentDB L1/L2/L3 pipeline did not become idle before the readiness timeout"
        )

    @staticmethod
    def _all_pipeline_layers_idle(status: dict[str, Any]) -> bool:
        for key in ("l1", "l2", "l3"):
            layer = status.get(key)
            if not isinstance(layer, dict):
                raise PipelineReadinessError(
                    f"TencentDB pipeline status is missing the {key.upper()} layer"
                )
            queued = int(layer.get("queued") or 0)
            running = int(layer.get("running") or 0)
            if queued != 0 or running != 0 or layer.get("idle") is not True:
                return False
        return True

    async def _reconcile_pending_add(
        self,
        *,
        isolation: dict[str, str],
        session_id: str,
        messages: list[dict[str, Any]],
    ) -> bool:
        deadline = time.monotonic() + self.config.pending_reconcile_seconds
        while True:
            existing = await self.client.conversation_query(
                isolation=isolation, session_id=session_id
            )
            if self._contains_messages(existing, messages):
                return True
            if time.monotonic() >= deadline:
                return False
            await asyncio.sleep(min(1.0, max(0.0, deadline - time.monotonic())))

    @staticmethod
    def _contains_messages(
        existing: list[dict[str, Any]], expected: list[dict[str, Any]]
    ) -> bool:
        def identity(item: dict[str, Any]) -> tuple[str, str, str]:
            return (
                str(item.get("role") or ""),
                str(item.get("content") or ""),
                AdapterService._normalize_timestamp_text(item.get("timestamp")),
            )

        existing_counts = Counter(identity(item) for item in existing)
        expected_counts = Counter(identity(item) for item in expected)
        if all(existing_counts[key] >= count for key, count in expected_counts.items()):
            return True

        # Older stores may not preserve timestamps in their query response.
        existing_without_time = Counter((key[0], key[1]) for key in map(identity, existing))
        expected_without_time = Counter((key[0], key[1]) for key in map(identity, expected))
        return all(
            existing_without_time[key] >= count
            for key, count in expected_without_time.items()
        )

    async def _request_lock(self, request_id: str) -> asyncio.Lock:
        async with self._request_locks_guard:
            return self._request_locks.setdefault(request_id, asyncio.Lock())

    def _logical_payload_sha256(
        self,
        *,
        request: AddRequest,
        isolation: dict[str, str],
    ) -> str:
        messages: list[dict[str, Any]] = []
        for message in request.messages:
            item: dict[str, Any] = {
                "role": message.role,
                "content": message.content,
            }
            timestamp = self._timestamp_as_datetime(message.timestamp)
            if timestamp is not None:
                item["timestamp"] = self._format_datetime(timestamp)
            messages.append(item)
        logical_payload = {
            **isolation,
            "session_id": request.session_id,
            "messages": messages,
        }
        return self._payload_sha256(logical_payload)

    @staticmethod
    def _payload_sha256(payload: dict[str, Any]) -> str:
        return hashlib.sha256(
            json.dumps(
                payload,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()

    def _upstream_messages(self, request: AddRequest) -> list[dict[str, Any]]:
        result: list[dict[str, Any]] = []
        for message in request.messages:
            pieces = self._split_text(
                message.content,
                max_chars=self.config.max_message_chars,
                label="message",
            )
            base_timestamp = self._timestamp_as_datetime(message.timestamp)
            for index, content in enumerate(pieces):
                item: dict[str, Any] = {"role": message.role, "content": content}
                if base_timestamp is not None:
                    item["timestamp"] = self._format_datetime(
                        base_timestamp + timedelta(milliseconds=index)
                    )
                result.append(item)
        if len(result) > self.config.max_upstream_messages:
            raise ValueError(
                "Add payload expands beyond TencentDB's maximum message count; "
                "reduce the LDBD chunk size"
            )
        return result

    @staticmethod
    def _timestamp_as_datetime(value: int | float | str | None) -> datetime | None:
        if value is None or value == "":
            return None
        if isinstance(value, (int, float)):
            seconds = float(value)
            if abs(seconds) >= 10_000_000_000:
                seconds /= 1000.0
            return datetime.fromtimestamp(seconds, tz=timezone.utc)
        text = str(value).strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        parsed = datetime.fromisoformat(text)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)

    @staticmethod
    def _format_datetime(value: datetime) -> str:
        return value.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace(
            "+00:00", "Z"
        )

    @staticmethod
    def _normalize_timestamp_text(value: Any) -> str:
        if value in (None, ""):
            return ""
        try:
            parsed = AdapterService._timestamp_as_datetime(value)
        except (TypeError, ValueError, OverflowError):
            return str(value)
        return AdapterService._format_datetime(parsed) if parsed else ""

    def _compact_query(self, query: str) -> str:
        query = query.strip()
        limit = self.config.max_query_chars
        if len(query) <= limit:
            return query
        marker = "\n...[middle omitted by LDBD adapter]...\n"
        remaining = limit - len(marker)
        head = remaining // 2
        return query[:head] + marker + query[-(remaining - head) :]

    async def _scenario_candidates(
        self, *, query: str, isolation: dict[str, str]
    ) -> list[dict[str, Any]]:
        entries = [
            item
            for item in await self.client.list_scenarios(isolation)
            if item.get("path") and not str(item["path"]).endswith("/")
        ]
        entries.sort(
            key=lambda item: (
                self._lexical_score(
                    query,
                    f"{item.get('path', '')}\n{item.get('summary', '')}",
                ),
                str(item.get("updated_at") or ""),
            ),
            reverse=True,
        )
        selected = entries[: self.config.scenario_read_limit]

        async def read(entry: dict[str, Any]) -> dict[str, Any] | None:
            async with self._profile_read_slots:
                data = await self.client.read_scenario(
                    isolation=isolation, path=str(entry["path"])
                )
            if data is None:
                return None
            return {**data, "_path": str(entry["path"])}

        files = await asyncio.gather(*(read(entry) for entry in selected))
        candidates: list[dict[str, Any]] = []
        for item in files:
            if item is None:
                continue
            path = str(item["_path"])
            for index, content in enumerate(
                self._split_text(
                    str(item["content"]),
                    max_chars=self.config.profile_chunk_chars,
                    label=f"scenario {path}",
                ),
                start=1,
            ):
                candidates.append(
                    {
                        "id": f"l2:{hashlib.sha256(path.encode()).hexdigest()[:20]}:{index}",
                        "content": content,
                        "created_at": item.get("created_at"),
                        "_lexical": self._lexical_score(query, content),
                    }
                )
        candidates.sort(key=lambda item: float(item["_lexical"]), reverse=True)
        return candidates

    async def _core_candidates(
        self, *, isolation: dict[str, str]
    ) -> list[dict[str, Any]]:
        async with self._profile_read_slots:
            item = await self.client.read_core(isolation)
        if item is None:
            return []
        content = str(item["content"])
        return [
            {
                "id": f"l3:{hashlib.sha256(content.encode()).hexdigest()[:20]}:{index}",
                "content": part,
                "created_at": item.get("created_at"),
            }
            for index, part in enumerate(
                self._split_text(
                    content,
                    max_chars=self.config.profile_chunk_chars,
                    label="core profile",
                ),
                start=1,
            )
        ]

    @staticmethod
    def _normalize_atomic(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [
            {
                "id": f"l1:{item.get('id')}",
                "content": str(item.get("content") or ""),
                "created_at": item.get("created_at"),
            }
            for item in items
            if item.get("id") and str(item.get("content") or "").strip()
        ]

    @staticmethod
    def _normalize_conversation(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [
            {
                "id": f"l0:{item.get('id')}",
                "content": str(item.get("content") or ""),
                "created_at": item.get("timestamp"),
            }
            for item in items
            if item.get("id") and str(item.get("content") or "").strip()
        ]

    def _reciprocal_rank_fusion(
        self,
        source_lists: dict[str, list[dict[str, Any]]],
        *,
        top_k: int,
    ) -> list[SearchItem]:
        weights = {
            "atomic": self.config.atomic_weight,
            "scenario": self.config.scenario_weight,
            "core": self.config.core_weight,
            "conversation": self.config.conversation_weight,
        }
        fused: dict[str, dict[str, Any]] = {}
        for source, items in source_lists.items():
            for rank, item in enumerate(items, start=1):
                content = str(item.get("content") or "").strip()
                if not content:
                    continue
                content_key = hashlib.sha256(
                    " ".join(content.lower().split()).encode("utf-8")
                ).hexdigest()
                score = weights[source] / (60.0 + rank)
                if content_key not in fused:
                    fused[content_key] = {
                        "id": str(item["id"]),
                        "content": content,
                        "score": score,
                        "created_at": item.get("created_at"),
                    }
                else:
                    fused[content_key]["score"] += score

        ordered = sorted(
            fused.values(),
            key=lambda item: (float(item["score"]), str(item["id"])),
            reverse=True,
        )
        return [SearchItem.model_validate(item) for item in ordered[:top_k]]

    @staticmethod
    def _lexical_score(query: str, text: str) -> float:
        query_tokens = Counter(token.lower() for token in _TOKEN_RE.findall(query))
        text_tokens = Counter(token.lower() for token in _TOKEN_RE.findall(text))
        if not query_tokens or not text_tokens:
            return 0.0
        overlap = sum(min(count, text_tokens[token]) for token, count in query_tokens.items())
        return overlap / max(1, sum(query_tokens.values()))

    @staticmethod
    def _split_text(text: str, *, max_chars: int, label: str) -> list[str]:
        if len(text) <= max_chars:
            return [text]
        marker_reserve = 48
        chunk_size = max_chars - marker_reserve
        raw_parts = [text[index : index + chunk_size] for index in range(0, len(text), chunk_size)]
        total = len(raw_parts)
        return [
            f"[{label} part {index}/{total}] {part}"
            for index, part in enumerate(raw_parts, start=1)
        ]

    async def health(self) -> dict[str, Any]:
        upstream = await self.client.health()
        return {
            "ok": True,
            "adapter": "ldbd-tencentdb-agent-memory",
            "retrieval_layers": ["L1", "L2", "L3", "L0-fallback"],
            "upstream": upstream,
            "state": self.state.summary(),
        }


__all__ = [
    "AdapterService",
    "PayloadConflictError",
    "PipelineReadinessError",
    "UpstreamError",
]
