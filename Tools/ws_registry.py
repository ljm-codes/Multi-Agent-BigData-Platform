"""
后台任务 WebSocket 注册表

放在 Tools 下而不是某一边的路由里：鉴权侧（踢下线）和对话侧（建立连接）都要改它，
放在任何一边都会形成反向依赖。

一个账号可以同时开多个窗口，所以用 set 存连接；每个连接自己订阅 queue，
推送时只发给当前连接，避免多窗口互相收到重复消息。
"""
from __future__ import annotations

import asyncio

from fastapi import WebSocket

# user_id(str) -> 该账号当前的所有连接
aid_to_websocket: dict[str, set[WebSocket]] = {}
# user_id(str) -> 保护上面那个集合的锁（asyncio.Lock，必须用 async with）
aid_websocket_locks: dict[str, asyncio.Lock] = {}


def _lock_for(user_id: str) -> asyncio.Lock:
    return aid_websocket_locks.setdefault(str(user_id), asyncio.Lock())


async def register_ws(user_id: str, websocket: WebSocket) -> None:
    async with _lock_for(user_id):
        aid_to_websocket.setdefault(str(user_id), set()).add(websocket)


async def unregister_ws(user_id: str, websocket: WebSocket) -> None:
    async with _lock_for(user_id):
        sockets = aid_to_websocket.get(str(user_id))
        if sockets is None:
            return
        sockets.discard(websocket)
        if not sockets:
            aid_to_websocket.pop(str(user_id), None)


async def close_user_sockets(user_id: str, code: int = 4008) -> int:
    """
    关闭某个账号的全部连接，返回关掉的条数

    先把集合从字典里摘掉（在锁内），再逐个 close（close 会 await，不放在锁里）
    """
    async with _lock_for(user_id):
        sockets = list(aid_to_websocket.pop(str(user_id), ()))

    for websocket in sockets:
        try:
            await websocket.close(code=code)
        except Exception:  # noqa: BLE001
            # 连接可能已经断了，忽略即可
            pass
    return len(sockets)
