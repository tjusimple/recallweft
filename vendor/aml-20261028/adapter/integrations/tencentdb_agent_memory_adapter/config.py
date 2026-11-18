from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def _env_int(name: str, default: int, *, minimum: int = 1) -> int:
    value = int(os.environ.get(name, default))
    if value < minimum:
        raise ValueError(f"{name} must be >= {minimum}")
    return value


def _env_float(name: str, default: float, *, minimum: float = 0.0) -> float:
    value = float(os.environ.get(name, default))
    if value < minimum:
        raise ValueError(f"{name} must be >= {minimum}")
    return value


def _env_csv(name: str) -> tuple[str, ...]:
    return tuple(
        item.strip()
        for item in os.environ.get(name, "").split(",")
        if item.strip()
    )


@dataclass(frozen=True)
class AdapterConfig:
    upstream_base_url: str = "http://127.0.0.1:8420"
    upstream_api_key: str = ""
    upstream_service_id: str = "default"
    inbound_api_key: str = ""
    team_id: str = "ldbd-evaluation"
    agent_id_prefix: str = "ldbd-sample"
    state_db_path: Path = Path("var/tencentdb-adapter/state.db")
    upstream_timeout_seconds: float = 900.0
    upstream_retry_attempts: int = 6
    retry_base_seconds: float = 1.0
    retry_max_seconds: float = 20.0
    add_concurrency: int = 6
    search_concurrency: int = 16
    flush_concurrency: int = 8
    pipeline_poll_interval_seconds: float = 2.0
    pipeline_ready_timeout_seconds: float = 3600.0
    pipeline_stable_polls: int = 2
    pipeline_quiet_period_seconds: float = 15.0
    pending_reconcile_seconds: float = 15.0
    max_message_chars: int = 20000
    max_upstream_messages: int = 100
    max_query_chars: int = 2048
    scenario_read_limit: int = 32
    profile_chunk_chars: int = 6000
    atomic_weight: float = 1.0
    scenario_weight: float = 0.85
    core_weight: float = 0.75
    conversation_weight: float = 0.65
    legacy_committed_rehash_request_prefixes: tuple[str, ...] = ()

    @classmethod
    def from_env(cls) -> "AdapterConfig":
        return cls(
            upstream_base_url=os.environ.get(
                "TENCENTDB_MEMORY_BASE_URL", "http://127.0.0.1:8420"
            ).rstrip("/"),
            upstream_api_key=os.environ.get("TENCENTDB_MEMORY_API_KEY", ""),
            upstream_service_id=os.environ.get("TENCENTDB_MEMORY_SERVICE_ID", "default"),
            inbound_api_key=os.environ.get("LDBD_ADAPTER_API_KEY", ""),
            team_id=os.environ.get("TENCENTDB_MEMORY_TEAM_ID", "ldbd-evaluation"),
            agent_id_prefix=os.environ.get("TENCENTDB_MEMORY_AGENT_PREFIX", "ldbd-sample"),
            state_db_path=Path(
                os.environ.get("TENCENTDB_ADAPTER_STATE_DB", "var/tencentdb-adapter/state.db")
            ),
            upstream_timeout_seconds=_env_float(
                "TENCENTDB_MEMORY_TIMEOUT_SECONDS", 900.0, minimum=1.0
            ),
            upstream_retry_attempts=_env_int("TENCENTDB_MEMORY_RETRY_ATTEMPTS", 6),
            retry_base_seconds=_env_float("TENCENTDB_MEMORY_RETRY_BASE_SECONDS", 1.0),
            retry_max_seconds=_env_float("TENCENTDB_MEMORY_RETRY_MAX_SECONDS", 20.0),
            add_concurrency=_env_int("TENCENTDB_ADAPTER_ADD_CONCURRENCY", 6),
            search_concurrency=_env_int("TENCENTDB_ADAPTER_SEARCH_CONCURRENCY", 16),
            flush_concurrency=_env_int("TENCENTDB_ADAPTER_FLUSH_CONCURRENCY", 8),
            pipeline_poll_interval_seconds=_env_float(
                "TENCENTDB_PIPELINE_POLL_SECONDS", 2.0, minimum=0.1
            ),
            pipeline_ready_timeout_seconds=_env_float(
                "TENCENTDB_PIPELINE_READY_TIMEOUT_SECONDS", 3600.0, minimum=1.0
            ),
            pipeline_stable_polls=_env_int("TENCENTDB_PIPELINE_STABLE_POLLS", 2),
            pipeline_quiet_period_seconds=_env_float(
                "TENCENTDB_PIPELINE_QUIET_SECONDS", 15.0
            ),
            pending_reconcile_seconds=_env_float(
                "TENCENTDB_PENDING_RECONCILE_SECONDS", 15.0
            ),
            max_message_chars=_env_int("TENCENTDB_MAX_MESSAGE_CHARS", 20000, minimum=256),
            max_upstream_messages=_env_int("TENCENTDB_MAX_MESSAGES_PER_ADD", 100),
            max_query_chars=_env_int("TENCENTDB_MAX_QUERY_CHARS", 2048, minimum=128),
            scenario_read_limit=_env_int("TENCENTDB_SCENARIO_READ_LIMIT", 32),
            profile_chunk_chars=_env_int("TENCENTDB_PROFILE_CHUNK_CHARS", 6000, minimum=512),
            legacy_committed_rehash_request_prefixes=_env_csv(
                "TENCENTDB_LEGACY_COMMITTED_REHASH_REQUEST_PREFIXES"
            ),
        )
