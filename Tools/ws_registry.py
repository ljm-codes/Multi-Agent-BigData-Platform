"""
后台任务 / 运维感知 WebSocket 注册表

放在 Tools 下而不是某一边的路由里：鉴权侧（踢下线）和对话侧（建立连接）都要改它，
放在任何一边都会形成反向依赖。

一个账号可以同时开多个窗口，所以每个账号存一个 set；
每条连接注册时带上权限，广播时按 `min_permission` 过滤 ——
不能只靠“连接时校验过”就默认这条连接一直有权限。
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field

from fastapi import WebSocket
from fastapi.encoders import jsonable_encoder


@dataclass
class UserConnections:
    """一个账号的连接集合 + 当前的权限等级"""
    permission: int = 0
    sockets: set[WebSocket] = field(default_factory=set)


# user_id(str) -> 该账号当前的所有连接
aid_to_websocket: dict[str, UserConnections] = {}
# user_id(str) -> 保护上面那个集合的锁（asyncio.Lock，必须用 async with）
aid_websocket_locks: dict[str, asyncio.Lock] = {}


def _lock_for(user_id: str) -> asyncio.Lock:
    return aid_websocket_locks.setdefault(str(user_id), asyncio.Lock())


async def register_ws(user_id: str, websocket: WebSocket, permission: int = 0) -> None:
    async with _lock_for(user_id):
        entry = aid_to_websocket.get(str(user_id))
        if entry is None:
            entry = UserConnections(permission=int(permission or 0))
            aid_to_websocket[str(user_id)] = entry
        else:
            # 同一账号不同连接用同一权限；以最近一次连接/权限更新为准
            entry.permission = int(permission or 0)
        entry.sockets.add(websocket)


async def update_ws_permission(user_id: str, permission: int) -> None:
    """账号权限变更时同步注册表，下一次广播立即按新权限过滤"""
    async with _lock_for(user_id):
        entry = aid_to_websocket.get(str(user_id))
        if entry is not None:
            entry.permission = int(permission or 0)


async def send_json_to_user(user_id: str, payload: dict) -> None:
    """
    给某个账号的所有连接发送 JSON 消息
    -> 这里使用的是 aid_to_websocket 为了统一管理，及时踢人下线

    先快照再发（不要在锁里 await 网络）；单个死连接不能挡住其它连接。
    """
    encoded = jsonable_encoder(payload)
    async with _lock_for(user_id):
        entry = aid_to_websocket.get(str(user_id))
        sockets = list(entry.sockets) if entry is not None else []
    for websocket in sockets:
        try:
            await websocket.send_json(encoded)
        except Exception:  # noqa: BLE001 —— 连接已断，顺手注销
            await unregister_ws(user_id, websocket)


async def send_json_to_all(payload: dict, *, min_permission: int = 0) -> None:
    """
    广播给所有已注册连接；**按注册表里的权限过滤**，死连接顺手剔除

    运维感知用 min_permission=1；后台任务广播不要用这个（用 send_json_to_user）。
    """
    encoded = jsonable_encoder(payload)
    pairs = [
        (user_id, websocket)
        for user_id, entry in list(aid_to_websocket.items())
        if entry.permission >= min_permission
        for websocket in list(entry.sockets)
    ]
    for user_id, websocket in pairs:
        try:
            await websocket.send_json(encoded)
        except Exception:  # noqa: BLE001
            await unregister_ws(user_id, websocket)


async def unregister_ws(user_id: str, websocket: WebSocket) -> None:
    async with _lock_for(user_id):
        entry = aid_to_websocket.get(str(user_id))
        if entry is None:
            return
        entry.sockets.discard(websocket)
        if not entry.sockets:
            aid_to_websocket.pop(str(user_id), None)


async def close_user_sockets(user_id: str, code: int = 4008) -> int:
    """
    关闭某个账号的全部连接，返回关掉的条数

    先把集合从字典里摘掉（在锁内），再逐个 close（close 会 await，不放在锁里）
    """
    async with _lock_for(user_id):
        entry = aid_to_websocket.pop(str(user_id), None)
        sockets = list(entry.sockets) if entry is not None else []

    for websocket in sockets:
        try:
            await websocket.close(code=code)
        except Exception:  # noqa: BLE001
            # 连接可能已经断了，忽略即可
            pass
    return len(sockets)
