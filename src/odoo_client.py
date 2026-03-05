from __future__ import annotations

import xmlrpc.client
from typing import Any

from tenacity import retry, stop_after_attempt, wait_exponential

from src.config import OdooConfig


class OdooXmlRpcClient:
    def __init__(self, cfg: OdooConfig) -> None:
        self._cfg = cfg
        self._uid: int | None = None
        self._common = xmlrpc.client.ServerProxy(f"{cfg.url}/xmlrpc/2/common")
        self._models = xmlrpc.client.ServerProxy(f"{cfg.url}/xmlrpc/2/object")

    @retry(wait=wait_exponential(multiplier=1, min=1, max=30), stop=stop_after_attempt(5), reraise=True)
    def authenticate(self) -> int:
        uid = self._common.authenticate(self._cfg.db, self._cfg.username, self._cfg.api_key, {})
        if not uid:
            raise RuntimeError("Odoo authentication failed. Check URL/DB/username/API key.")
        self._uid = int(uid)
        return self._uid

    @property
    def uid(self) -> int:
        if self._uid is None:
            raise RuntimeError("Not authenticated. Call authenticate() first.")
        return self._uid

    @retry(wait=wait_exponential(multiplier=1, min=1, max=30), stop=stop_after_attempt(5), reraise=True)
    def execute_kw(
        self,
        model: str,
        method: str,
        args: list[Any] | None = None,
        kwargs: dict[str, Any] | None = None,
    ) -> Any:
        return self._models.execute_kw(
            self._cfg.db,
            self.uid,
            self._cfg.api_key,
            model,
            method,
            args or [],
            kwargs or {},
        )

    @retry(wait=wait_exponential(multiplier=1, min=1, max=30), stop=stop_after_attempt(5), reraise=True)
    def search_read(
        self,
        model: str,
        domain: list[Any],
        fields: list[str],
        offset: int,
        limit: int,
        order: str,
    ) -> list[dict[str, Any]]:
        return self.execute_kw(
            model=model,
            method="search_read",
            args=[domain],
            kwargs={"fields": fields, "offset": offset, "limit": limit, "order": order},
        )

    @retry(wait=wait_exponential(multiplier=1, min=1, max=30), stop=stop_after_attempt(5), reraise=True)
    def write(self, model: str, record_ids: list[int], values: dict[str, Any]) -> bool:
        return bool(self.execute_kw(model=model, method="write", args=[record_ids, values]))
