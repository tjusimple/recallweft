from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


class MemoryMessage(BaseModel):
    model_config = ConfigDict(extra="forbid")

    role: Literal["user", "assistant"]
    content: str = Field(min_length=1)
    timestamp: int | float | str | None = None

    @field_validator("content")
    @classmethod
    def content_must_not_be_blank(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("content must not be blank")
        return value


class AddRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    request_id: str = Field(min_length=1, max_length=512)
    messages: list[MemoryMessage] = Field(min_length=1)
    session_id: str = Field(min_length=1, max_length=1024)
    user_id: str = Field(min_length=1, max_length=1024)


class SearchRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    query: str = Field(min_length=1)
    user_id: str = Field(min_length=1, max_length=1024)
    top_k: int = Field(default=100, ge=1, le=100)
    options: list[str] | None = None

    @field_validator("query")
    @classmethod
    def query_must_not_be_blank(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("query must not be blank")
        return value


class SearchItem(BaseModel):
    id: str
    content: str
    score: float
    created_at: Any | None = None
