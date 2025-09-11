from typing import Any, Optional

from pydantic import BaseModel, Field


class QueryConfig(BaseModel):
    path: Optional[str] = ""
    fields: Optional[str] = ""
    ids: Optional[str] = ""
    limit: Optional[str] = "25"
    since: Optional[str] = ""
    until: Optional[str] = ""
    parameters: Optional[str] = None


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
    account_id: Optional[str] = None
    business_name: Optional[str] = None
    currency: Optional[str] = None
    category: Optional[str] = None
    category_list: Optional[list[dict[str, Any]]] = None
    tasks: Optional[list[str]] = None
    fb_page_id: Optional[str] = None


class Configuration(BaseModel):
    accounts: dict[str, Account] = Field(default_factory=dict)
    queries: list[QueryRow] = Field(default_factory=list)
    api_version: str = Field(alias="api-version", default="v23.0")
    bucket_id: Optional[str] = Field(alias="bucket-id", default=None)
