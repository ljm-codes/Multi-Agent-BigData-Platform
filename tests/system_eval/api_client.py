"""真实 HTTP API 客户端：登录、SSE、resume、历史、工具轨迹、验证/后台、运维接口。"""
from __future__ import annotations

import time
from typing import Any

import requests


class ApiError(RuntimeError):
    def __init__(self, message: str, *, status: int | None = None, body: Any = None):
        super().__init__(message)
        self.status = status
        self.body = body


class ApiClient:
    def __init__(self, base_url: str = "http://127.0.0.1:8000"):
        self.base_url = base_url.rstrip("/")
        self.s = requests.Session()
        self.token: str | None = None
        self.user: dict[str, Any] = {}

    def close(self) -> None:
        self.s.close()

    # ---------- auth ----------
    def login(self, email: str, password: str) -> dict[str, Any]:
        resp = self.s.post(
            f"{self.base_url}/auth/admin/login",
            json={"email": email, "password": password},
            timeout=30,
        )
        try:
            body = resp.json()
        except Exception as exc:  # noqa: BLE001
            raise ApiError(f"登录返回非 JSON: {resp.status_code} {resp.text[:300]}", status=resp.status_code) from exc
        if resp.status_code != 200 or body.get("code") != 0:
            raise ApiError(f"登录失败: {body.get('msg') or body}", status=resp.status_code, body=body)
        data = body.get("data") or {}
        self.token = data.get("token")
        self.user = data.get("payload") or {}
        if not self.token:
            raise ApiError("登录响应没有 token", body=body)
        return data

    @property
    def headers(self) -> dict[str, str]:
        if not self.token:
            raise ApiError("尚未登录")
        return {"Authorization": f"Bearer token {self.token}"}

    # ---------- generic ----------
    def _json(self, method: str, path: str, *, timeout: float = 60, **kwargs) -> dict[str, Any]:
        resp = self.s.request(
            method, f"{self.base_url}{path}", headers=self.headers,
            timeout=timeout, **kwargs,
        )
        try:
            body = resp.json()
        except Exception as exc:  # noqa: BLE001
            raise ApiError(
                f"{method} {path} 返回非 JSON: HTTP {resp.status_code} {resp.text[:300]}",
                status=resp.status_code,
            ) from exc
        if resp.status_code != 200:
            raise ApiError(
                f"{method} {path} HTTP {resp.status_code}: {body}",
                status=resp.status_code, body=body,
            )
        if isinstance(body, dict) and body.get("code") not in (None, 0):
            raise ApiError(
                f"{method} {path} 业务错误 {body.get('code')}: {body.get('msg')}",
                status=resp.status_code, body=body,
            )
        return body

    def get(self, path: str, *, timeout: float = 60, **kwargs) -> dict[str, Any]:
        return self._json("GET", path, timeout=timeout, **kwargs)

    def post(self, path: str, *, timeout: float = 60, **kwargs) -> dict[str, Any]:
        return self._json("POST", path, timeout=timeout, **kwargs)

    # ---------- chat ----------
    def chat_stream(
        self, message: str, thread_id: str | None = None,
        *, connect_timeout: float = 20, read_timeout: float = 1800,
    ):
        body: dict[str, Any] = {"message": message}
        if thread_id:
            body["thread_id"] = thread_id
        resp = self.s.post(
            f"{self.base_url}/ai/chat/stream",
            headers={**self.headers, "Accept": "text/event-stream"},
            json=body, stream=True, timeout=(connect_timeout, read_timeout),
        )
        if resp.status_code != 200:
            raise ApiError(
                f"/ai/chat/stream HTTP {resp.status_code}: {resp.text[:500]}",
                status=resp.status_code,
            )
        ctype = (resp.headers.get("content-type") or "").lower()
        if "text/event-stream" not in ctype:
            text = resp.text[:1000]
            raise ApiError(f"/ai/chat/stream 不是 SSE: {ctype} {text}", status=resp.status_code)
        resp.encoding = "utf-8"
        return resp

    def resume(self, thread_id: str, answer: Any, *, timeout: float = 2400, retries: int = 5) -> dict[str, Any]:
        last: Exception | None = None
        for i in range(retries):
            try:
                body = self.post(
                    "/ai/resume",
                    json={"thread_id": thread_id, "answer": answer},
                    timeout=timeout,
                )
                return body.get("data") or {}
            except ApiError as exc:
                last = exc
                # interrupt 后 paused 标志稍后才设置，409 要短重试
                if exc.status == 409 and i < retries - 1:
                    time.sleep(0.8 + i * 0.5)
                    continue
                raise
        raise last or ApiError("resume 失败")

    def history(self, thread_id: str) -> dict[str, Any]:
        return self.get("/ai/history", params={"thread_id": thread_id}).get("data") or {}

    def tool_traces(self, thread_id: str, *, limit: int = 500) -> list[dict[str, Any]]:
        body = self.get(
            "/ai/tool-traces",
            params={"thread_id": thread_id, "limit": limit},
        )
        return body.get("data") or []

    def verification(self, thread_id: str) -> dict[str, Any] | None:
        body = self.get("/ai/verification", params={"thread_id": thread_id})
        return body.get("data")

    def backgrounds(self, thread_id: str | None = None) -> list[dict[str, Any]]:
        params = {"thread_id": thread_id} if thread_id else None
        body = self.get("/ai/background", params=params)
        return body.get("data") or []

    def threads(self, *, limit: int = 50, offset: int = 0, is_archive: bool = False) -> dict[str, Any]:
        return self.get(
            "/ai/threads",
            params={"limit": limit, "offset": offset, "is_archive": is_archive},
        ).get("data") or {}

    def archive_thread(self, thread_id: str) -> dict[str, Any]:
        return self._json("PUT", "/ai/thread/archive", params={"thread_id": thread_id})

    # ---------- ops ----------
    def ops_checker_errors(self, **params) -> dict[str, Any]:
        return self.get("/ops/checker/app/error", params=params).get("data") or {}

    def ops_agent_info(self, **params) -> dict[str, Any]:
        return self.get("/ops/agent/info", params=params).get("data") or {}

    def ops_agent_history(self, session_id: str) -> dict[str, Any]:
        return self.get("/ops/agent/history", params={"session_id": session_id}).get("data") or {}

    def ops_handover_history(self, session_id: str) -> dict[str, Any]:
        return self.get(
            "/ops/agent/handover/history", params={"session_id": session_id}
        ).get("data") or {}

    def ops_resume(self, session_id: str, answer: Any, *, timeout: float = 1800) -> dict[str, Any]:
        return self.post(
            "/ops/agent/resume",
            json={"session_id": session_id, "answer": answer},
            timeout=timeout,
        ).get("data") or {}

    def ops_handover_resume(self, session_id: str, answer: Any, *, timeout: float = 2400) -> dict[str, Any]:
        return self.post(
            "/ops/agent/handover/resume",
            json={"session_id": session_id, "answer": answer},
            timeout=timeout,
        ).get("data") or {}
