from typing import Any

from pydantic import BaseModel, Field


class QueryConfig(BaseModel):
    path: str | None = ""
    fields: str | None = ""
    ids: str | None = ""
    limit: str | None = "25"
    since: str | None = ""
    until: str | None = ""
    parameters: str | None = None


class QueryRow(BaseModel):
    id: int
    type: str
    name: str
    run_by_id: bool = Field(alias="run-by-id", default=False)
    query: QueryConfig
    disabled: bool = Field(default=False)


class Account(BaseModel):
    id: str
    name: str
    account_id: str | None = None
    business_name: str | None = None
    currency: str | None = None
    category: str | None = None
    category_list: list[dict[str, Any]] | None = None
    tasks: list[str] | None = None
    fb_page_id: str | None = None


class Configuration(BaseModel):
    accounts: dict[str, Account] = Field(default_factory=dict)
    queries: list[QueryRow] = Field(default_factory=list)
    api_version: str = Field(alias="api-version", default="v23.0")
    bucket_id: str | None = Field(alias="bucket-id", default=None)
    # CFTL-630 / SUPPORT-16160: opt-in V1-parity output. When false (default) the
    # OutputParser behaves exactly like 0.0.17. When true it copies all scalar fields
    # onto per-action-breakdown rows and backfills declared-but-omitted fields.
    v1_compatibility: bool = Field(default=False)
