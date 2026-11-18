from __future__ import annotations

import json
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any


LEGACY_PAYLOAD_HASH_VERSION = "transport-v1"
LOGICAL_PAYLOAD_HASH_VERSION = "logical-v2"


@dataclass(frozen=True)
class Reservation:
    created: bool
    status: str
    response: dict[str, Any] | None
    updated_at: float


@dataclass(frozen=True)
class DirtySession:
    session_id: str
    user_id: str
    agent_id: str
    generation: int


class PayloadConflictError(ValueError):
    pass


class AdapterState:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=30.0)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA busy_timeout=30000")
        return connection

    def initialize(self) -> None:
        with self._connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS add_requests (
                    request_id TEXT PRIMARY KEY,
                    payload_sha256 TEXT NOT NULL,
                    payload_hash_version TEXT NOT NULL DEFAULT 'logical-v2',
                    user_id TEXT NOT NULL,
                    session_id TEXT NOT NULL,
                    status TEXT NOT NULL,
                    response_json TEXT,
                    last_error TEXT,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL
                );

                CREATE TABLE IF NOT EXISTS dirty_sessions (
                    session_id TEXT PRIMARY KEY,
                    user_id TEXT NOT NULL,
                    agent_id TEXT NOT NULL,
                    generation INTEGER NOT NULL,
                    updated_at REAL NOT NULL
                );

                CREATE TABLE IF NOT EXISTS adapter_meta (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );

                INSERT OR IGNORE INTO adapter_meta(key, value)
                VALUES ('generation', '0');
                """
            )
            columns = {
                str(row["name"])
                for row in connection.execute("PRAGMA table_info(add_requests)")
            }
            if "payload_hash_version" not in columns:
                connection.execute(
                    """
                    ALTER TABLE add_requests
                    ADD COLUMN payload_hash_version TEXT NOT NULL DEFAULT 'transport-v1'
                    """
                )

    def reserve(
        self,
        *,
        request_id: str,
        payload_sha256: str,
        legacy_payload_sha256: str,
        user_id: str,
        session_id: str,
        allow_committed_payload_rehash: bool = False,
    ) -> Reservation:
        now = time.time()
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT * FROM add_requests WHERE request_id=?", (request_id,)
            ).fetchone()
            if row is None:
                connection.execute(
                    """
                    INSERT INTO add_requests(
                        request_id, payload_sha256, payload_hash_version,
                        user_id, session_id, status, response_json, last_error,
                        created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, 'pending', NULL, NULL, ?, ?)
                    """,
                    (
                        request_id,
                        payload_sha256,
                        LOGICAL_PAYLOAD_HASH_VERSION,
                        user_id,
                        session_id,
                        now,
                        now,
                    ),
                )
                connection.commit()
                return Reservation(True, "pending", None, now)

            response = (
                json.loads(str(row["response_json"])) if row["response_json"] else None
            )
            payload_hash_version = str(row["payload_hash_version"])
            if payload_hash_version not in {
                LEGACY_PAYLOAD_HASH_VERSION,
                LOGICAL_PAYLOAD_HASH_VERSION,
            }:
                raise RuntimeError(
                    f"unsupported Add payload hash version: {payload_hash_version}"
                )
            is_legacy_hash = payload_hash_version == LEGACY_PAYLOAD_HASH_VERSION
            legacy_payload_matches = (
                is_legacy_hash
                and str(row["payload_sha256"]) == legacy_payload_sha256
            )
            can_migrate_legacy_committed = (
                allow_committed_payload_rehash
                and is_legacy_hash
                and str(row["status"]) == "committed"
                and str(row["user_id"]) == user_id
                and str(row["session_id"]) == session_id
                and response is not None
            )
            logical_payload_matches = (
                not is_legacy_hash
                and str(row["payload_sha256"]) == payload_sha256
            )
            if not logical_payload_matches and not legacy_payload_matches:
                can_rehash_committed = can_migrate_legacy_committed
                if can_rehash_committed:
                    connection.execute(
                        """
                        UPDATE add_requests
                        SET payload_sha256=?, payload_hash_version=?, updated_at=?
                        WHERE request_id=? AND status='committed'
                        """,
                        (
                            payload_sha256,
                            LOGICAL_PAYLOAD_HASH_VERSION,
                            now,
                            request_id,
                        ),
                    )
                    connection.commit()
                    return Reservation(False, "committed", response, now)
                raise PayloadConflictError(
                    "request_id was already used with a different Add payload"
                )
            if is_legacy_hash:
                connection.execute(
                    """
                    UPDATE add_requests
                    SET payload_sha256=?, payload_hash_version=?, updated_at=?
                    WHERE request_id=?
                    """,
                    (
                        payload_sha256,
                        LOGICAL_PAYLOAD_HASH_VERSION,
                        now,
                        request_id,
                    ),
                )
            if str(row["status"]) == "failed":
                connection.execute(
                    """
                    UPDATE add_requests
                    SET status='pending', last_error=NULL, updated_at=?
                    WHERE request_id=?
                    """,
                    (now, request_id),
                )
                connection.commit()
                return Reservation(True, "pending", None, now)
            connection.commit()
            return Reservation(
                False,
                str(row["status"]),
                response,
                float(row["updated_at"]),
            )

    def mark_committed(
        self,
        *,
        request_id: str,
        response: dict[str, Any],
        user_id: str,
        session_id: str,
        agent_id: str,
    ) -> int:
        now = time.time()
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT status FROM add_requests WHERE request_id=?", (request_id,)
            ).fetchone()
            if row is None:
                raise RuntimeError(f"missing Add reservation: {request_id}")

            generation = int(
                connection.execute(
                    "SELECT value FROM adapter_meta WHERE key='generation'"
                ).fetchone()[0]
            )
            if str(row["status"]) != "committed":
                generation += 1
                connection.execute(
                    "UPDATE adapter_meta SET value=? WHERE key='generation'",
                    (str(generation),),
                )
                connection.execute(
                    """
                    INSERT INTO dirty_sessions(
                        session_id, user_id, agent_id, generation, updated_at
                    ) VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(session_id) DO UPDATE SET
                        user_id=excluded.user_id,
                        agent_id=excluded.agent_id,
                        generation=excluded.generation,
                        updated_at=excluded.updated_at
                    """,
                    (session_id, user_id, agent_id, generation, now),
                )

            connection.execute(
                """
                UPDATE add_requests
                SET status='committed', response_json=?, last_error=NULL, updated_at=?
                WHERE request_id=?
                """,
                (json.dumps(response, ensure_ascii=False), now, request_id),
            )
            connection.commit()
            return generation

    def mark_failed(self, request_id: str, error: str) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                UPDATE add_requests
                SET status='failed', last_error=?, updated_at=?
                WHERE request_id=?
                """,
                (error[:2000], time.time(), request_id),
            )

    def current_generation(self) -> int:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT value FROM adapter_meta WHERE key='generation'"
            ).fetchone()
            return int(row[0])

    def dirty_sessions(self, *, up_to_generation: int | None = None) -> list[DirtySession]:
        sql = "SELECT * FROM dirty_sessions"
        parameters: tuple[Any, ...] = ()
        if up_to_generation is not None:
            sql += " WHERE generation <= ?"
            parameters = (up_to_generation,)
        sql += " ORDER BY generation, session_id"
        with self._connect() as connection:
            rows = connection.execute(sql, parameters).fetchall()
        return [
            DirtySession(
                session_id=str(row["session_id"]),
                user_id=str(row["user_id"]),
                agent_id=str(row["agent_id"]),
                generation=int(row["generation"]),
            )
            for row in rows
        ]

    def mark_sessions_ready(self, *, up_to_generation: int) -> None:
        with self._connect() as connection:
            connection.execute(
                "DELETE FROM dirty_sessions WHERE generation <= ?", (up_to_generation,)
            )

    def summary(self) -> dict[str, int]:
        with self._connect() as connection:
            dirty = int(
                connection.execute("SELECT COUNT(*) FROM dirty_sessions").fetchone()[0]
            )
            pending = int(
                connection.execute(
                    "SELECT COUNT(*) FROM add_requests WHERE status='pending'"
                ).fetchone()[0]
            )
            committed = int(
                connection.execute(
                    "SELECT COUNT(*) FROM add_requests WHERE status='committed'"
                ).fetchone()[0]
            )
        return {
            "generation": self.current_generation(),
            "dirty_sessions": dirty,
            "pending_adds": pending,
            "committed_adds": committed,
        }
