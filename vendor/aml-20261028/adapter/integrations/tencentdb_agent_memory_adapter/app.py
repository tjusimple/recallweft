from __future__ import annotations

import hmac
from contextlib import asynccontextmanager
from typing import Annotated

from fastapi import Depends, FastAPI, Header, HTTPException

from .client import TencentMemoryClient, UpstreamError
from .config import AdapterConfig
from .models import AddRequest, SearchRequest
from .service import AdapterService, PipelineReadinessError
from .state import AdapterState, PayloadConflictError


def create_app(
    *,
    config: AdapterConfig | None = None,
    service: AdapterService | None = None,
) -> FastAPI:
    config = config or AdapterConfig.from_env()
    owns_service = service is None
    if service is None:
        state = AdapterState(config.state_db_path)
        service = AdapterService(
            config=config,
            state=state,
            client=TencentMemoryClient(config),
        )

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        yield
        if owns_service:
            await service.close()

    app = FastAPI(
        title="LDBD TencentDB Agent Memory Adapter",
        version="1.0.0",
        lifespan=lifespan,
    )
    app.state.adapter_service = service

    async def authorize(
        authorization: Annotated[str | None, Header(alias="Authorization")] = None,
    ) -> None:
        if not config.inbound_api_key:
            return
        expected = f"Bearer {config.inbound_api_key}"
        if not authorization or not hmac.compare_digest(authorization, expected):
            raise HTTPException(
                status_code=401,
                detail={"stage": "authentication", "code": "invalid_adapter_key"},
            )

    @app.get("/")
    async def root() -> dict:
        return {
            "service": "ldbd-tencentdb-agent-memory-adapter",
            "version": "1.0.0",
            "layers": ["L1", "L2", "L3", "L0-fallback"],
        }

    @app.get("/health")
    async def health() -> dict:
        try:
            return await service.health()
        except UpstreamError as exc:
            raise HTTPException(
                status_code=503,
                detail={
                    "stage": "upstream_health",
                    "code": "tencentdb_unavailable",
                    "possible_problem": str(exc),
                },
            ) from exc

    @app.post("/v1/memories/add", dependencies=[Depends(authorize)])
    async def add(request: AddRequest) -> dict:
        try:
            return await service.add(request)
        except PayloadConflictError as exc:
            raise HTTPException(
                status_code=409,
                detail={
                    "stage": "add_idempotency",
                    "code": "request_id_payload_conflict",
                    "possible_problem": str(exc),
                },
            ) from exc
        except ValueError as exc:
            raise HTTPException(
                status_code=422,
                detail={
                    "stage": "add_validation",
                    "code": "unsupported_add_payload",
                    "possible_problem": str(exc),
                },
            ) from exc
        except UpstreamError as exc:
            raise _upstream_http_exception("add", exc) from exc

    @app.post("/v1/memories/search", dependencies=[Depends(authorize)])
    async def search(request: SearchRequest) -> dict:
        try:
            return await service.search(request)
        except PipelineReadinessError as exc:
            raise HTTPException(
                status_code=503,
                detail={
                    "stage": "memory_pipeline",
                    "code": "pipeline_not_ready",
                    "possible_problem": str(exc),
                },
            ) from exc
        except UpstreamError as exc:
            raise _upstream_http_exception("search", exc) from exc

    return app


def _upstream_http_exception(stage: str, exc: UpstreamError) -> HTTPException:
    status = exc.status_code if 400 <= exc.status_code < 600 else 502
    return HTTPException(
        status_code=status,
        detail={
            "stage": stage,
            "code": "tencentdb_upstream_error",
            "retriable": exc.retryable,
            "outcome_uncertain": exc.outcome_uncertain,
            "possible_problem": str(exc),
        },
    )
