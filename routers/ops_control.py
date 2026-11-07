import asyncio
import shlex
from datetime import datetime
from typing import Any, Literal

import jwt
from fastapi import APIRouter, WebSocket, Query, Request
from fastapi.encoders import jsonable_encoder
from langchain_core.messages import AIMessage
from langgraph.types import Command
from pydantic import BaseModel
from sqlalchemy import select, func
from starlette.websockets import WebSocketDisconnect

from SPO.main_agent_state_model import content_text
from SPO.operations_agent_state_model import OperationsAndReconcileContext
from SPO.postgresql_model import AgentRun, CheckLog
from SPO.route_results import RouteResponse, ResultCode
from agent.operations_agent_run import (_finish_agent_run, _mark_run_running,
                                        _release_lease)
from Tools.alarm_sender import send_warning_msg
from Tools.db import AsyncSessionLocal
from Tools.job_command import normalize_answer, split as split_command, validate_command
from Tools.log_settings import LogSetting
from Tools.main_check_tool import APP_PERCEPTION_QUEUE
from Tools.share_check_tool import redis_con
from Tools.ws_registry import register_ws, send_json_to_all, unregister_ws

route = APIRouter()
logger = LogSetting.create(__name__)


async def perception_broadcast():
    """
    感知队列的**单消费者**：一条记录广播给所有 ops 连接

    不能在每个 WebSocket 里各自 get()：多个窗口/多个用户会瓜分队列，
    而且踢人之后那个连接还在偷偷消费。
    """
    while True:
        try:
            record = await APP_PERCEPTION_QUEUE.get()
            if record is None:
                continue
            await send_json_to_all(record, min_permission=1)
        except Exception as exc:  # noqa: BLE001 —— 广播任务不能因为一条坏数据退出
            logger.error(f"推送应用错误感知失败：{type(exc).__name__}: {exc}")


@route.websocket('/ws/perception/error')
async def perception_error(websocket: WebSocket, ticket: str = Query(default="")):
    # 这里的验证逻辑与 ai_chat.py 中的逻辑一致
    raw = redis_con.hget("temp_code", ticket)
    if not raw:
        await websocket.close(code=1008)
        return

    try:
        short_code, user_id, jwt_token = raw.decode("utf-8").split(":", 2)
    except ValueError:
        redis_con.hdel("temp_code", ticket)
        await websocket.close(code=1008)
        return

    # Ticket 一次性：不论校验成败都先消费掉，避免重放
    tmp_key = redis_con.hget("jwt_key", f"background:temp-{user_id}")
    redis_con.hdel("temp_code", short_code)
    redis_con.hdel("jwt_key", f"background:temp-{user_id}")

    try:
        payload = jwt.decode(jwt_token, tmp_key, algorithms=["HS256"])
    except Exception as exc:  # noqa: BLE001
        logger.warning(f"Ticket 校验失败：{exc}")
        await websocket.close(code=4008)
        return

    user_id = payload.get("user_id")
    disabled = payload.get("disabled")
    permission = int(payload.get("permission") or 0)
    if not user_id or disabled or permission < 1:
        await websocket.close(code=4008)
        return

    await websocket.accept()
    await register_ws(user_id, websocket, permission=permission)
    try:
        # 这里只等断开：消息由 perception_broadcast 统一广播。
        # 踢人时 close_user_sockets 会立即断开，receive() 抛 WebSocketDisconnect -> finally 注销。
        while True:
            await websocket.receive()
    except (WebSocketDisconnect, RuntimeError):
        pass
    finally:
        await unregister_ws(user_id, websocket)


@route.get("/checker/app/error")
async def get_checker_app(
        request: Request, app_id: str | None = Query(default=None),
        app_type: Literal["ALL", "SPARK", "FLINK", "CLUSTER"] = Query(default="ALL"),
        is_checker_error: bool = Query(default=False),
        status: Literal["PENDING", "PROCESSING", "DONE", "ESCALATED", "FAILED"] = Query(default="PENDING"),
        is_solved: bool = Query(default=False),
        repeat_count: int | None = Query(default=None),
        limit: int = Query(default=10), offset: int = Query(default=0),
        start_time: datetime | None = Query(default=None),
        end_time: datetime | None = Query(default=None),
        order_by: Literal["id", "app_id", "repeat_count", "create_time"] = Query(default="id"),
        desc_flag: bool = Query(default=False),
) -> RouteResponse:
    """
    查询检查到的应用错误 -> 包括 检查器自身发生的错误

    app_type=ALL 查全部；CLUSTER 查集群级（DB 里 app_type 为 NULL）。
    """
    try:
        permission = int(request.state.info.get("permission"))
    except (ValueError, TypeError):
        return RouteResponse.error(msg="token解析错误，请重新登录", code=ResultCode.INVALID_VALUE_ERROR)
    if permission < 1:
        return RouteResponse.error(msg="权限不足", code=ResultCode.PERMISSION_ERROR)

    payload = {
        "metadata": {
            "app_type": app_type,
            "is_checker_error": is_checker_error,
            "status": status,
            "is_solved": is_solved,
            "limit": limit,
            "offset": offset,
            "order_by": order_by,
            "desc_flag": desc_flag,
        },
    }
    filters = [
        CheckLog.status == status,
        CheckLog.is_solved == is_solved,
    ]
    if app_type == "CLUSTER":
        filters.append(CheckLog.app_type.is_(None))
    elif app_type != "ALL":
        filters.append(CheckLog.app_type == app_type)
    if app_id is not None:
        filters.append(CheckLog.app_id == app_id)
    if is_checker_error:
        filters.append(CheckLog.checker_error.is_not(None))
    if repeat_count is not None:
        filters.append(CheckLog.repeat_count == repeat_count)
        payload["metadata"]["repeat_count"] = repeat_count
    if start_time is not None:
        filters.append(CheckLog.create_time >= start_time)
        payload["metadata"]["start_time"] = start_time
    if end_time is not None:
        filters.append(CheckLog.create_time <= end_time)
        payload["metadata"]["end_time"] = end_time

    order_col = getattr(CheckLog, order_by)
    if desc_flag:
        order_col = order_col.desc()
    # 选“表列”而不是实体：select(CheckLog) 走 mappings() 时键是类名，
    # dict(row) 会得到 {'CheckLog': <ORM 对象>}，FastAPI 序列化会直接 500
    stmt = select(*CheckLog.__table__.columns).where(*filters).order_by(
        order_col).limit(limit).offset(offset)
    count_stmt = select(func.count()).select_from(CheckLog).where(*filters)

    try:
        async with AsyncSessionLocal() as session:
            records = (await session.execute(stmt)).mappings().all()
            total = (await session.execute(count_stmt)).scalar_one()
        # 保持既有约定：total = 当前页条数；total_count = 全量匹配数
        payload["total"] = len(records)
        payload["total_count"] = int(total or 0)
        payload["records"] = [dict(row) for row in records]
        return RouteResponse.ok(data=payload)
    except Exception as exc:
        logger.error(f"查询检查到的应用错误失败：{exc}")
        return RouteResponse.error(msg="查询检查到的应用错误失败", code=ResultCode.DB_ERROR)


@route.get("/agent/info")
async def get_agent_info(
        request: Request, app_id: str | None = Query(default=None),
        status: Literal["ALL", "RUNNING", "PAUSED", "DONE", "ERROR", "ABANDONED"] = Query(default="ALL"),
        start_time: datetime | None = Query(default=None),
        end_time: datetime | None = Query(default=None),
        limit: int = Query(default=10), offset: int = Query(default=0),
) -> RouteResponse:
    """
    获取运维 Agent 的运行记录（历史归档，不查 app_lease 锁表）
    start_time / end_time 针对 started_at
    """
    try:
        permission = int(request.state.info.get("permission"))
    except (ValueError, TypeError):
        return RouteResponse.error(msg="token解析错误，请重新登录", code=ResultCode.INVALID_VALUE_ERROR)
    if permission < 1:
        return RouteResponse.error(msg="权限不足", code=ResultCode.PERMISSION_ERROR)

    payload = {
        "metadata": {
            "app_id": app_id,
            "status": status,
            "limit": limit,
            "offset": offset,
        },
    }
    filters = []
    if app_id is not None:
        filters.append(AgentRun.app_id == app_id)
    if status != "ALL":
        filters.append(AgentRun.status == status)
    if start_time is not None:
        filters.append(AgentRun.started_at >= start_time)
        payload["metadata"]["start_time"] = start_time
    if end_time is not None:
        filters.append(AgentRun.started_at <= end_time)
        payload["metadata"]["end_time"] = end_time

    # 选表列，避免 mappings() 返回 {'AgentRun': <ORM 对象>}
    stmt = select(*AgentRun.__table__.columns)
    if filters:
        stmt = stmt.where(*filters)
    stmt = stmt.order_by(AgentRun.started_at.desc()).limit(limit).offset(offset)
    count_stmt = select(func.count()).select_from(AgentRun)
    if filters:
        count_stmt = count_stmt.where(*filters)

    try:
        async with AsyncSessionLocal() as session:
            records = (await session.execute(stmt)).mappings().all()
            total = (await session.execute(count_stmt)).scalar_one()
        # 保持既有约定：total = 当前页条数；total_count = 全量匹配数
        payload["total"] = len(records)
        payload["total_count"] = int(total or 0)
        payload["records"] = [dict(row) for row in records]
        return RouteResponse.ok(data=payload)
    except Exception as exc:
        logger.error(f"查询 Agent 运行记录失败：{exc}")
        return RouteResponse.error(msg="查询 Agent 运行记录失败", code=ResultCode.DB_ERROR)


@route.get("/agent/history")
async def get_agent_history(request: Request, session_id: str | None = Query(default=None)) -> RouteResponse:
    """
    获取 ops-Agent 解决错误应用的记录（messages + 工具调用）
    """
    try:
        permission = int(request.state.info.get("permission"))
    except (ValueError, TypeError):
        return RouteResponse.error(msg="token解析错误，请重新登录", code=ResultCode.INVALID_VALUE_ERROR)
    if permission < 1:
        return RouteResponse.error(msg="权限不足", code=ResultCode.PERMISSION_ERROR)
    if not session_id:
        return RouteResponse.error(msg="session_id不能为空", code=ResultCode.INVALID_VALUE_ERROR)

    ops_agent = getattr(request.app.state, "operations_agent", None)
    if ops_agent is None:
        return RouteResponse.error(msg="运维图尚未初始化", code=ResultCode.SERVER_ERROR)

    try:
        snapshot = await ops_agent.aget_state({'configurable': {'thread_id': session_id}})
        interrupts = list(snapshot.interrupts or ())
        values = snapshot.values or {}
        messages = []
        for message in values.get('messages', []):
            messages.append({
                'type': type(message).__name__,
                'content': message.content,
                'name': getattr(message, 'name', None),
                'tool_calls': getattr(message, 'tool_calls', None) or [],
                'additional_kwargs': getattr(message, 'additional_kwargs', None) or {},
            })
        data = {
            'thread_id': session_id,
            'app_id': values.get('app_id', ''),
            'app_type': values.get('app_type', ''),
            'out_msg': values.get('out_msg', ''),
            'type': values.get('type', ''),
            'interrupted': bool(interrupts),
            'interrupt': jsonable_encoder(interrupts),
            'error_desc_text': values.get('error_desc_text', ''),
            'error_id_list': values.get('error_id_list', []),
            'messages': messages,
            # 有转交时前端显示入口；信息来自运维图状态，不额外建表
            'handover': ({
                'dev_thread_id': values.get('handover_dev_thread_id'),
                'app_name': values.get('handover_app_name', ''),
                'project_name': values.get('handover_project_name', ''),
            } if values.get('handover_dev_thread_id') else None),
        }
        return RouteResponse.ok(data=jsonable_encoder(data))
    except Exception as exc:
        logger.error(f"获取会话历史记录失败：{exc}")
        return RouteResponse.error(msg=f"获取会话历史记录失败：{exc}", code=ResultCode.SERVER_ERROR)


class OpsResumeRequest(BaseModel):
    session_id: str
    answer: Any


class ResumeError(Exception):
    """人工审核恢复的业务错误（400）"""


_resume_locks: dict[str, asyncio.Lock] = {}


def _resume_lock(session_id: str) -> asyncio.Lock:
    lock = _resume_locks.get(session_id)
    if lock is None:
        lock = asyncio.Lock()
        _resume_locks[session_id] = lock
    return lock


async def _run_app_id(session_id: str) -> str:
    """从 AgentRun 反查 app_id（state 里没有时兜底）"""
    async with AsyncSessionLocal() as session:
        return (await session.execute(
            select(AgentRun.app_id).where(AgentRun.session_id == session_id)
        )).scalar_one_or_none() or ""


async def _resume_ops_session(ops_agent: Any, session_id: str, answer: Any) -> dict:
    """
    恢复一个停在人工审核的运维会话

    :raises ResumeError: 没中断 / 中断类型不支持 / 回复或命令结构不合法
    """
    snapshot = await ops_agent.aget_state({"configurable": {"thread_id": session_id}})
    interrupts = list(snapshot.interrupts or ())
    if not interrupts:
        raise ResumeError("该会话没有待确认的中断")

    first = interrupts[0]
    value = getattr(first, "value", first)
    if not isinstance(value, dict) or value.get("kind") != "job_restart":
        raise ResumeError("该中断类型暂不支持在运维看板处理")

    original_command = value.get("command") or ""
    try:
        original_tokens = value.get("command_tokens") or split_command(original_command)
    except ValueError as exc:
        raise ResumeError(str(exc)) from exc

    action, new_tokens, msg, error = normalize_answer(answer)
    if error:
        raise ResumeError(f"人工审核回复格式不正确: {error}")

    ignored_values: list[dict] = []
    if action == "yes":
        resume_answer = {"type": "yes", "base_command": original_command}
    elif msg:
        resume_answer = {"type": "no", "msg": msg, "base_command": original_command}
    else:
        effective, ignored_values, command_error = validate_command(
            original_tokens, new_tokens or [], value.get("app_type") or "")
        if command_error:
            raise ResumeError(f"人工修改的命令不可执行: {command_error}")
        resume_answer = {"type": "no", "new_command": shlex.join(effective),
                         "base_command": original_command}

    app_id = (snapshot.values or {}).get("app_id") or await _run_app_id(session_id)
    await asyncio.to_thread(_mark_run_running, session_id)

    if len(interrupts) > 1:
        interrupt_id = getattr(first, "id", "")
        if not interrupt_id:
            raise ResumeError("存在多个待确认中断，但缺少中断 id")
        resume_payload = Command(resume={interrupt_id: resume_answer})
    else:
        resume_payload = Command(resume=resume_answer)

    try:
        result = await ops_agent.ainvoke(
            resume_payload,
            config={"configurable": {"thread_id": session_id}},
            context=OperationsAndReconcileContext(app_id=app_id, session_id=session_id),
        )
    except Exception as exc:  # noqa: BLE001
        # 中断可能已被消费；再读一次，决定该记 PAUSED 还是 ERROR
        still_interrupted = False
        try:
            snap2 = await ops_agent.aget_state({"configurable": {"thread_id": session_id}})
            still_interrupted = bool(snap2.interrupts)
        except Exception:
            pass
        if still_interrupted:
            await asyncio.to_thread(
                _finish_agent_run, session_id, "PAUSED", "等待人工审核", "",
                finished=False)
        else:
            await asyncio.to_thread(
                _finish_agent_run, session_id, "ERROR", "",
                f"{type(exc).__name__}: {exc}")
            await asyncio.to_thread(_release_lease, app_id, session_id)
        raise ResumeError(f"恢复失败: {type(exc).__name__}: {exc}") from exc

    interrupted = isinstance(result, dict) and bool(result.get("__interrupt__"))
    out_msg = result.get("out_msg", "") if isinstance(result, dict) else ""
    if interrupted:
        await asyncio.to_thread(
            _finish_agent_run, session_id, "PAUSED", "等待人工审核", "",
            finished=False)
    elif result.get("type", "") == "error":
        await asyncio.to_thread(_finish_agent_run, session_id, "ERROR", "", out_msg)
        await asyncio.to_thread(_release_lease, app_id, session_id)
        await asyncio.to_thread(
            send_warning_msg, app_id=app_id,
            description=f"程序运行出错了：{out_msg}", error_id=None)
    else:
        await asyncio.to_thread(_finish_agent_run, session_id, "DONE", out_msg, "")
        await asyncio.to_thread(_release_lease, app_id, session_id)

    return {
        "thread_id": session_id,
        "interrupted": interrupted,
        "interrupt": jsonable_encoder(result.get("__interrupt__")) if interrupted else [],
        "output_msg": "" if interrupted else out_msg,
        "type": result.get("type", "") if isinstance(result, dict) else "",
        "ignored_values": ignored_values,
    }


@route.post("/agent/resume", response_model=RouteResponse)
async def resume_agent(req: OpsResumeRequest, request: Request) -> RouteResponse:
    """恢复停在人工审核的 ops-Agent 会话"""
    try:
        permission = int(request.state.info.get("permission"))
    except (ValueError, TypeError):
        return RouteResponse.error(msg="token解析错误，请重新登录",
                                   code=ResultCode.INVALID_VALUE_ERROR)
    if permission < 1:
        return RouteResponse.error(msg="权限不足", code=ResultCode.PERMISSION_ERROR)
    if not (req.session_id or "").strip():
        return RouteResponse.error(msg="session_id 不能为空",
                                   code=ResultCode.INVALID_VALUE_ERROR)

    ops_agent = getattr(request.app.state, "operations_agent", None)
    if ops_agent is None:
        return RouteResponse.error(msg="运维图尚未初始化", code=ResultCode.SERVER_ERROR)

    async with _resume_lock(req.session_id):
        try:
            data = await _resume_ops_session(ops_agent, req.session_id, req.answer)
            return RouteResponse.ok(data=data)
        except ResumeError as exc:
            return RouteResponse.error(msg=str(exc),
                                       code=ResultCode.INVALID_VALUE_ERROR)
        except Exception as exc:  # noqa: BLE001
            logger.error(f"恢复运维会话 {req.session_id} 失败：{type(exc).__name__}: {exc}")
            return RouteResponse.error(msg=f"恢复失败：{exc}",
                                       code=ResultCode.SERVER_ERROR)


@route.get("/agent/handover/history")
async def get_handover_history(request: Request, session_id: str | None = Query(default=None)) -> RouteResponse:
    """开发图历史（线程 ops:{session_id}），含中断信息"""
    try:
        permission = int(request.state.info.get("permission"))
    except (ValueError, TypeError):
        return RouteResponse.error(msg="token解析错误，请重新登录", code=ResultCode.INVALID_VALUE_ERROR)
    if permission < 1:
        return RouteResponse.error(msg="权限不足", code=ResultCode.PERMISSION_ERROR)
    if not (session_id or "").strip():
        return RouteResponse.error(msg="session_id 不能为空", code=ResultCode.INVALID_VALUE_ERROR)

    dev_graph = getattr(request.app.state, "main_graph", None)
    if dev_graph is None:
        return RouteResponse.error(msg="开发图尚未初始化", code=ResultCode.SERVER_ERROR)

    from Tasks.dev_handover import dev_thread_id
    thread_id = dev_thread_id(session_id)
    try:
        snapshot = await dev_graph.aget_state({'configurable': {'thread_id': thread_id}})
        interrupts = list(snapshot.interrupts or ())
        values = snapshot.values or {}
        messages = []
        for message in values.get("messages", []):
            text = content_text(message.content)
            # 结构化输出只有 tool call、没有正文，前端不展示
            if isinstance(message, AIMessage) and not text.strip():
                continue
            messages.append({
                "type": type(message).__name__,
                "name": getattr(message, "name", None),
                "content": text,
            })
        data = {
            "session_id": session_id,
            "thread_id": thread_id,
            "app_type": values.get("app_type", ""),
            "project_name": values.get("project_name", ""),
            "date": values.get("date", ""),
            "is_ops": bool(values.get("is_ops")),
            "interrupted": bool(interrupts),
            "interrupt": jsonable_encoder(interrupts),
            "output_msg": "" if interrupts else values.get("output_msg", ""),
            "metadata": values.get("metadata") or {},
            "messages": messages,
        }
        return RouteResponse.ok(data=jsonable_encoder(data))
    except Exception as exc:  # noqa: BLE001
        logger.error(f"获取转交开发历史失败：{type(exc).__name__}: {exc}")
        return RouteResponse.error(msg=f"获取转交开发历史失败：{exc}",
                                   code=ResultCode.SERVER_ERROR)


@route.post("/agent/handover/resume")
async def resume_handover(request: Request, req: OpsResumeRequest) -> RouteResponse:
    """恢复转交开发会话停在的人工确认"""
    try:
        permission = int(request.state.info.get("permission"))
    except (ValueError, TypeError):
        return RouteResponse.error(msg="token解析错误，请重新登录", code=ResultCode.INVALID_VALUE_ERROR)
    if permission < 1:
        return RouteResponse.error(msg="权限不足", code=ResultCode.PERMISSION_ERROR)
    if not (req.session_id or "").strip():
        return RouteResponse.error(msg="session_id 不能为空", code=ResultCode.INVALID_VALUE_ERROR)

    if getattr(request.app.state, "main_graph", None) is None:
        return RouteResponse.error(msg="开发图尚未初始化", code=ResultCode.SERVER_ERROR)

    from Tasks.background_broker import background_broker
    from Tasks.dev_handover import dev_thread_id, resume_handover as resume_dev_handover
    thread_id = dev_thread_id(req.session_id)
    if background_broker.occupancy(thread_id) == "active":
        return RouteResponse.error(msg="开发会话正在运行，请稍后再试", code=ResultCode.SERVER_ERROR)

    try:
        result = await resume_dev_handover(req.session_id, req.answer)
    except Exception as exc:  # noqa: BLE001
        logger.error(f"恢复转交开发会话 {req.session_id} 失败：{type(exc).__name__}: {exc}")
        return RouteResponse.error(msg=f"恢复失败：{exc}", code=ResultCode.SERVER_ERROR)

    interrupted = isinstance(result, dict) and bool(result.get("__interrupt__"))
    return RouteResponse.ok(data={
        "session_id": req.session_id,
        "thread_id": thread_id,
        "interrupted": interrupted,
        "interrupt": jsonable_encoder(result.get("__interrupt__")) if interrupted else [],
        "output_msg": "" if interrupted or not isinstance(result, dict) else result.get("output_msg", ""),
        "type": (result.get("metadata") or {}).get("type", "") if isinstance(result, dict) else "",
    })
