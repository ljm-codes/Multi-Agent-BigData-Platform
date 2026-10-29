import asyncio
import json
from dataclasses import asdict
from datetime import datetime
from typing import Any, Literal
from uuid import uuid4

from fastapi import APIRouter, HTTPException, Query, Request, WebSocket, WebSocketDisconnect
from fastapi.encoders import jsonable_encoder
from fastapi.responses import StreamingResponse
from langgraph.types import Command
from pydantic import BaseModel, Field
from sqlalchemy import select, and_, false, true, delete, update, func
from sqlalchemy.dialects.postgresql import insert
from starlette.concurrency import run_in_threadpool

from SPO.main_agent_state_model import MainStateContext
from SPO.postgresql_model import AdminThread
from SPO.route_results import RouteResponse, ResultCode
from Tasks.background_broker import background_broker, background_registry
from Tools import LogSetting
from Tools.db import AsyncSessionLocal
from load_config.config import ROOT_BASE_DIR_PATH

route = APIRouter()
logger = LogSetting.create(__name__)


class ChatRequest(BaseModel):
    user_id: str = Field(default="default", description="用户 id")
    message: str = Field(min_length=1, description="用户原话")
    thread_id: str | None = Field(default=None, description="会话 id；不传则新建")


class ResumeRequest(BaseModel):
    user_id: str = Field(default="default", description="用户 id")
    thread_id: str = Field(description="停在 interrupt 的会话 id")
    # CVA 人工确认的格式：{"pass": true/false, "msg": "...", "force_terminate": true/false}
    answer: dict[str, Any] | Literal["yes", "y", "no", "n"] = Field(
        description="人工回复，必须与 interrupt 的 payload 对应")


class ChatResponse(BaseModel):
    thread_id: str
    output_msg: str = ""
    out_time: datetime | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    thread_action: Literal["continue", "new_task"] = "continue"
    thread_title: str | None = None
    # new_task 时旧会话 id；前端据此删除旧窗口里的乐观消息
    previous_thread_id: str | None = None
    interrupted: bool = False
    interrupt: Any = None
    # 本轮对应的项目目录（tmp_code/{app_type}/{date}/{project_name}），没有就是 None
    project_dir: str | None = None


_thread_locks: dict[str, asyncio.Lock] = {}


def _new_thread_id(user_id: str) -> str:
    return f"{user_id}:{uuid4().hex}"


def _lock_for(thread_id: str) -> asyncio.Lock:
    lock = _thread_locks.get(thread_id)
    if lock is None:
        lock = asyncio.Lock()
        _thread_locks[thread_id] = lock
    return lock


async def _wait_until_idle(thread_id: str) -> None:
    """同 thread 串行：active 时等待，paused 时交给 /resume"""
    while background_broker.is_occupied(thread_id):
        if background_broker.occupancy(thread_id) == "paused":
            raise HTTPException(
                status_code=409,
                detail="该会话停在 interrupt，请调用 /ai/resume 回复人工确认",
            )
        await background_broker.wait()


async def _invoke(graph: Any, thread_id: str, user_id: str, payload: Any):
    return await graph.ainvoke(
        payload,
        config={"configurable": {"thread_id": thread_id}},
        context=MainStateContext(thread_id=thread_id, user_id=user_id),
    )


async def _run_user_turn(graph: Any, thread_id: str, user_id: str, input_state: dict) -> dict:
    async with _lock_for(thread_id):
        await _wait_until_idle(thread_id)
        background_broker.set_active(thread_id)
        try:
            result = await _invoke(graph, thread_id, user_id, input_state)
        except Exception:
            background_broker.set_idle(thread_id)
            raise
        if isinstance(result, dict) and result.get("__interrupt__"):
            background_broker.set_paused(thread_id)
        else:
            background_broker.set_idle(thread_id)
        return result


def _is_interrupted(result: Any) -> bool:
    return isinstance(result, dict) and bool(result.get("__interrupt__"))


def _project_dir(metadata: dict) -> str | None:
    """由 metadata 拼出项目目录；任一字段缺失或路径越界都返回 None"""
    app_type = metadata.get("app_type")
    date = metadata.get("date")
    p_name = metadata.get("project_name")
    if not (app_type and date and p_name):
        return None
    root = (ROOT_BASE_DIR_PATH / "tmp_code").resolve()
    path = (root / str(app_type) / str(date) / str(p_name)).resolve()
    return str(path) if root in path.parents else None


def _response(result: dict, thread_id: str, **extra: Any) -> RouteResponse:
    metadata = result.get("metadata") or {}
    fields: dict[str, Any] = {
        "thread_id": thread_id,
        "output_msg": result.get("output_msg", ""),
        "out_time": result.get("out_time"),
        "metadata": metadata,
        "thread_action": metadata.get("thread_action", "continue"),
        "thread_title": metadata.get("thread_title"),
        "interrupted": _is_interrupted(result),
        "interrupt": jsonable_encoder(result.get("__interrupt__")),
        "project_dir": _project_dir(metadata),
    }
    fields.update(extra)
    out_data = ChatResponse(**fields).model_dump()
    return RouteResponse.ok(data=out_data)


def _sse(event: str, data: Any) -> str:
    return f"event: {event}\ndata: {json.dumps(jsonable_encoder(data), ensure_ascii=False)}\n\n"


def _done_payload(
        output: dict,
        thread_id: str,
        *,
        thread_action: str = "continue",
        thread_title: str | None = None,
        previous_thread_id: str | None = None,
) -> dict:
    metadata = output.get("metadata") or {}
    return {
        "thread_id": thread_id,
        "output_msg": output.get("output_msg", ""),
        "out_time": output.get("out_time"),
        "metadata": metadata,
        "thread_action": thread_action if thread_action != "continue"
        else metadata.get("thread_action", "continue"),
        "thread_title": thread_title or metadata.get("thread_title"),
        "previous_thread_id": previous_thread_id,
        "project_dir": _project_dir(metadata),
    }


async def _stream_user_turn(graph: Any, thread_id: str, user_id: str, payload: Any, ctx: dict):
    """一次流式图运行（event streaming v3）；负责该 thread 的 occupancy 释放"""
    async with _lock_for(thread_id):
        try:
            await _wait_until_idle(thread_id)
        except HTTPException as exc:
            yield "error", {"thread_id": thread_id, "detail": exc.detail, "status": exc.status_code}
            return
        background_broker.set_active(thread_id)
        try:
            stream = await graph.astream_events(
                payload,
                config={"configurable": {"thread_id": thread_id}},
                context=MainStateContext(thread_id=thread_id, user_id=user_id),
                version="v3",
            )
            async with stream:
                async for message in stream.messages:
                    node = getattr(message, "node", None)
                    ns = list(getattr(message, "namespace", []) or [])
                    yield "update", {
                        "thread_id": thread_id,
                        "node": node,
                        "ns": ns,
                        "phase": "llm_start",
                    }
                    async for delta in message.text:
                        if delta:
                            yield "token", {
                                "thread_id": thread_id,
                                "node": node,
                                "ns": ns,
                                "content": delta,
                            }
                interrupts = await stream.interrupts()
                output = await stream.output() or {}
            ctx["output"] = output
            ctx["interrupted"] = bool(interrupts)
            if interrupts:
                ctx["interrupts"] = interrupts
                yield "interrupt", {"thread_id": thread_id, "interrupt": interrupts}
        except asyncio.CancelledError:
            if ctx["interrupted"]:
                background_broker.set_paused(thread_id)
            else:
                background_broker.set_idle(thread_id)
            raise
        except Exception as exc:  # noqa: BLE001
            background_broker.set_idle(thread_id)
            yield "error", {"thread_id": thread_id, "detail": str(exc)}
            return
        if ctx["interrupted"]:
            background_broker.set_paused(thread_id)
        else:
            background_broker.set_idle(thread_id)


async def _thread_owner(thread_id: str) -> int | None:
    """会话属于谁；不存在返回 None"""
    async with AsyncSessionLocal() as session:
        return await session.scalar(
            select(AdminThread.admin_id).where(AdminThread.thread_id == thread_id)
        )


@route.post("/chat/stream")
async def chat_stream(req: ChatRequest, request: Request) -> StreamingResponse | RouteResponse:
    graph = getattr(request.app.state, "main_graph", None)
    if graph is None:
        raise HTTPException(status_code=503, detail="主图尚未初始化")

    admin_id = int(request.state.info.get("id"))
    req.user_id = str(admin_id)  # 一律以 token 为准，请求体里的 user_id 不再采信
    thread_id = req.thread_id or _new_thread_id(req.user_id)
    human_msg = req.message

    owner = await _thread_owner(thread_id)
    if owner is not None and owner != admin_id:
        return RouteResponse.error(msg="只能操作自己的会话", code=ResultCode.UNAUTHORIZED)
    if owner is None:
        try:
            async with AsyncSessionLocal() as session:
                await session.execute(insert(AdminThread).values(
                    thread_id=thread_id,
                    thread_name=human_msg[:10],
                    admin_id=admin_id,
                ))
                await session.commit()
        except Exception as exc:  # noqa: BLE001
            logger.error(f"添加线程到 admin_thread 失败: {exc}")
            return RouteResponse.error(msg=f"添加线程到 admin_thread 失败: {exc}", code=ResultCode.DB_ERROR)

    async def event_stream():
        yield _sse("meta", {"thread_id": thread_id})

        ctx: dict[str, Any] = {"output": {}, "interrupted": False}
        async for event, data in _stream_user_turn(
                graph, thread_id, req.user_id, {"input_msg": human_msg}, ctx):
            yield _sse(event, data)
            if event == "error":
                return
        if ctx["interrupted"]:
            return

        output = ctx["output"] or {}
        metadata = output.get("metadata") or {}
        if metadata.get("thread_action") == "new_task":
            new_thread_id = _new_thread_id(req.user_id)
            request_text = metadata.get("request_text") or req.message
            yield _sse("thread_switch", {
                "previous_thread_id": thread_id,
                "thread_id": new_thread_id,
                "thread_title": metadata.get("thread_title"),
            })
            ctx2: dict[str, Any] = {"output": {}, "interrupted": False}
            async for event, data in _stream_user_turn(
                    graph, new_thread_id, req.user_id, {"input_msg": request_text}, ctx2):
                yield _sse(event, data)
                if event == "error":
                    return
            if ctx2["interrupted"]:
                return
            yield _sse("done", _done_payload(
                ctx2["output"], new_thread_id,
                thread_action="new_task",
                thread_title=metadata.get("thread_title"),
                previous_thread_id=thread_id,
            ))
            return

        yield _sse("done", _done_payload(output, thread_id))

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@route.post("/chat", response_model=RouteResponse)
async def chat(req: ChatRequest, request: Request) -> RouteResponse:
    graph = getattr(request.app.state, "main_graph", None)
    if graph is None:
        raise HTTPException(status_code=503, detail="主图尚未初始化")

    admin_id = int(request.state.info.get("id"))
    req.user_id = str(admin_id)  # 一律以 token 为准
    thread_id = req.thread_id or _new_thread_id(req.user_id)
    human_msg = req.message

    owner = await _thread_owner(thread_id)
    if owner is not None and owner != admin_id:
        return RouteResponse.error(msg="只能操作自己的会话", code=ResultCode.UNAUTHORIZED)
    if owner is None:
        try:
            async with AsyncSessionLocal() as session:
                await session.execute(insert(AdminThread).values(
                    thread_id=thread_id,
                    thread_name=human_msg[:10],
                    admin_id=admin_id,
                ))
                await session.commit()
        except Exception as exc:  # noqa: BLE001
            logger.error(f"添加线程到 admin_thread 失败: {exc}")
            return RouteResponse.error(msg=f"添加线程到 admin_thread 失败: {exc}", code=ResultCode.DB_ERROR)

    result = await _run_user_turn(graph, thread_id, req.user_id, {"input_msg": human_msg})
    if _is_interrupted(result):
        return _response(result, thread_id)

    metadata = result.get("metadata") or {}
    if metadata.get("thread_action") == "new_task":
        # 旧 thread 有未消费的后台任务：换新 thread，用用户原话重跑
        new_thread_id = _new_thread_id(req.user_id)
        request_text = metadata.get("request_text") or req.message
        new_result = await _run_user_turn(graph, new_thread_id, req.user_id, {"input_msg": request_text})
        return _response(
            new_result,
            new_thread_id,
            thread_action="new_task",
            thread_title=metadata.get("thread_title"),
            previous_thread_id=thread_id,
        )

    return _response(result, thread_id)


@route.post("/resume", response_model=RouteResponse)
async def resume(req: ResumeRequest, request: Request) -> RouteResponse:
    graph = getattr(request.app.state, "main_graph", None)
    if graph is None:
        raise HTTPException(status_code=503, detail="主图尚未初始化")

    admin_id = int(request.state.info.get("id"))
    req.user_id = str(admin_id)
    thread_id = req.thread_id

    owner = await _thread_owner(thread_id)
    if owner is None:
        return RouteResponse.error(msg="会话不存在", code=ResultCode.NOT_FOUND)
    if owner != admin_id:
        return RouteResponse.error(msg="只能操作自己的会话", code=ResultCode.UNAUTHORIZED)

    async with _lock_for(thread_id):
        state = background_broker.occupancy(thread_id)
        if state == "active":
            raise HTTPException(status_code=409, detail="该会话正在运行")
        background_broker.set_active(thread_id)
        try:
            result = await _invoke(graph, thread_id, req.user_id, Command(resume=req.answer))
        except Exception as exc:  # noqa: BLE001
            background_broker.set_idle(thread_id)
            raise HTTPException(status_code=400, detail=f"resume 失败：{exc}") from exc
        if _is_interrupted(result):
            background_broker.set_paused(thread_id)
            # 又停在新的 interrupt：把最新的人工确认问题写回记录
            for record in background_registry.locked_records(thread_id):
                if record.interrupt:
                    background_registry.update(record.task_id, interrupt=result["__interrupt__"])
        else:
            background_broker.set_idle(thread_id)
            # 后台续跑停在 interrupt 时记录还挂在 running；resume 跑完由入口收尾
            output_msg = result.get("output_msg", "") if isinstance(result, dict) else ""
            for record in background_registry.locked_records(thread_id):
                # 只收尾真正停在 interrupt 的那条；同线程其它 pending 结果留给 perceptron
                if record.interrupt:
                    background_registry.update(
                        record.task_id, status="completed", result=output_msg, interrupt=None
                    )
        return _response(result, thread_id)


@route.get("/history")
async def history(request: Request, thread_id: str = Query(..., description="只查某个会话")) -> RouteResponse:
    """
    获取用户会话历史记录
    """
    graph = getattr(request.app.state, "main_graph", None)
    if graph is None:
        raise HTTPException(status_code=503, detail="主图尚未初始化")

    try:
        admin_id = int(request.state.info.get("id"))
    except ValueError:
        return RouteResponse.error(msg="token 解析错误", code=ResultCode.INVALID_VALUE_ERROR)
    thread_id = thread_id.strip()
    if not thread_id:
        return RouteResponse.error(msg="thread_id 不能为空", code=ResultCode.INVALID_VALUE_ERROR)
    # 归属 + 归档状态一次查出来；查不到说明"不存在或不是你的"，不要继续读 checkpoint
    stmt = select(AdminThread.is_archive).where(
        and_(
            AdminThread.thread_id == thread_id,
            AdminThread.admin_id == admin_id,
        )
    )
    try:
        async with AsyncSessionLocal() as session:
            result = await session.execute(stmt)
            is_archive = result.scalar()
    except Exception as exc:
        logger.error(f"查询会话归属失败：{exc}")
        return RouteResponse.error(msg=f"查询会话归属失败：{exc}", code=ResultCode.DB_ERROR)
    if is_archive is None:
        return RouteResponse.error(msg="会话不存在", code=ResultCode.NOT_FOUND)
    # 归档只是收起，不是封存：照样能查，前端按 is_archive 显示成只读即可
    # 获取会话历史记录
    try:
        snapshot = await graph.aget_state({'configurable': {'thread_id': thread_id}})
        messages = [
            {"type": type(m).__name__, "content": str(m.content)}
            for m in snapshot.values.get("messages", [])
        ]
        return RouteResponse.ok(data={
            'thread_id': thread_id,
            'is_archive': is_archive,
            'manual_intervention': snapshot.values.get('manual_intervention', False),
            'project_dir': _project_dir(snapshot.values.get('metadata') or {}),
            'messages': messages,
        })
    except Exception as exc:
        logger.error(f"获取会话历史记录失败：{exc}")
        return RouteResponse.error(msg=f"获取会话历史记录失败：{exc}", code=ResultCode.SERVER_ERROR)


@route.get("/threads")
async def get_thread(
        request: Request, limit: int = Query(default=10, description="最多返回 limit 条记录"),
        offset: int = Query(default=0, description="偏移量"),
        is_archive: bool = Query(default=False, description="是否查询归档会话"),
        start_time: datetime = Query(default=None, description="查询时间范围开始"),
        end_time: datetime = Query(default=None, description="查询时间范围结束"),
) -> RouteResponse:
    try:
        admin_id = int(request.state.info.get("id"))
    except ValueError:
        return RouteResponse.error(msg="token 解析错误", code=ResultCode.INVALID_VALUE_ERROR)
    if not admin_id:
        return RouteResponse.error(msg="token 解析错误", code=ResultCode.INVALID_VALUE_ERROR)
    stmt = select(AdminThread.id, AdminThread.thread_id, AdminThread.is_archive, AdminThread.create_time,
                  AdminThread.thread_name).where(
        and_(
            AdminThread.admin_id == admin_id,
            AdminThread.is_archive == (false() if not is_archive else true()),
        )
    ).limit(limit).offset(offset).order_by(AdminThread.create_time.desc())
    payload = {
        'limit': limit,
        'offset': offset,
    }
    if start_time and end_time:
        stmt = stmt.where(AdminThread.create_time.between(start_time, end_time))
    try:
        async with AsyncSessionLocal() as session:
            result = await session.execute(stmt).mappings().all()
        if not result:
            payload['total'] = 0
            payload['records'] = []
            return RouteResponse.ok(data=payload)
        payload['total'] = len(result)
        payload['records'] = result
        return RouteResponse.ok(data=payload)
    except Exception as exc:
        logger.error(f"查询会话失败：{exc}")
        return RouteResponse.error(msg=f"查询会话失败：{exc}", code=ResultCode.DB_ERROR)


@route.put("/thread/archive")
async def archive_thread(request: Request, thread_id: str = Query(..., description="要归档的会话ID")) -> RouteResponse:
    """
    归档会话：只把 admin_thread.is_archive 置为 true，checkpoint 一条都不动（还能查、能恢复）

    -> 只能归档自己的
    -> 幂等：已经是归档状态时直接返回成功
    """
    try:
        admin_id = int(request.state.info.get("id"))
    except (TypeError, ValueError):
        return RouteResponse.error(msg="token 解析错误", code=ResultCode.INVALID_VALUE_ERROR)
    thread_id = thread_id.strip()
    if not thread_id:
        return RouteResponse.error(msg="thread_id 不能为空", code=ResultCode.INVALID_VALUE_ERROR)

    try:
        async with AsyncSessionLocal() as session:
            row = (await session.execute(
                select(AdminThread.admin_id, AdminThread.is_archive)
                .where(AdminThread.thread_id == thread_id)
            )).first()
    except Exception as exc:  # noqa: BLE001
        logger.error(f"查询会话 {thread_id} 归属失败：{exc}")
        return RouteResponse.error(msg=f"查询会话归属失败：{exc}", code=ResultCode.DB_ERROR)
    if row is None:
        return RouteResponse.error(msg="会话不存在", code=ResultCode.NOT_FOUND)
    owner, is_archive = row
    if owner != admin_id:
        return RouteResponse.error(msg="只能归档自己的会话", code=ResultCode.UNAUTHORIZED)
    if is_archive:
        return RouteResponse.ok(msg="该会话已归档")

    try:
        async with AsyncSessionLocal() as session:
            await session.execute(
                update(AdminThread)
                .where(AdminThread.thread_id == thread_id)
                .values(is_archive=True, update_time=func.now())
            )
            await session.commit()
    except Exception as exc:  # noqa: BLE001
        logger.error(f"归档会话 {thread_id} 失败：{exc}")
        return RouteResponse.error(msg=f"归档会话失败：{exc}", code=ResultCode.DB_ERROR)

    logger.info(f"会话已归档：thread_id={thread_id}, operator={admin_id}")
    return RouteResponse.ok(msg="会话已归档")


@route.put("/thread/archives")
async def archive_thread(
        request: Request,
        thread_ids: list[str] = Query(..., description="批量归档的会话ID")
) -> RouteResponse:
    try:
        admin_id = int(request.state.info.get("id"))
    except (TypeError, ValueError):
        return RouteResponse.error(msg="token 解析错误", code=ResultCode.INVALID_VALUE_ERROR)
    if not thread_ids:
        return RouteResponse.error(msg="thread_ids 不能为空", code=ResultCode.INVALID_VALUE_ERROR)

    try:
        async with AsyncSessionLocal() as session:
            rows = (await session.execute(
                select(AdminThread.admin_id, AdminThread.is_archive, AdminThread.thread_id)
                .where(AdminThread.thread_id.in_(thread_ids))
            )).mappings().all()
    except Exception as exc:  # noqa: BLE001
        logger.error(f"查询会话 {thread_ids} 归属失败：{exc}")
        return RouteResponse.error(msg=f"查询会话归属失败：{exc}", code=ResultCode.DB_ERROR)
    if not rows:
        return RouteResponse.error(msg="会话不存在", code=ResultCode.NOT_FOUND)
    filtered_threads = []  # 过滤出可以归档的会话
    for row in rows:
        owner, is_archive, thread_id = row['admin_id'], row['is_archive'], row['thread_id']
        if owner != admin_id:
            continue
        if is_archive:
            continue
        filtered_threads.append(thread_id)  # 可以归档的会话

    try:
        async with AsyncSessionLocal() as session:
            await session.execute(
                update(AdminThread)
                .where(AdminThread.thread_id.in_(filtered_threads))
                .values(is_archive=True, update_time=func.now())
            )
            await session.commit()
    except Exception as exc:  # noqa: BLE001
        logger.error(f"归档会话 {filtered_threads} 失败：{exc}")
        return RouteResponse.error(msg=f"归档会话失败：{exc}", code=ResultCode.DB_ERROR)

    logger.info(f"会话已归档：thread_ids={filtered_threads}, operator={admin_id}")
    return RouteResponse.ok(msg=f"批量归档 {len(filtered_threads)} 个会话")


@route.delete("/thread/delete")
async def delete_thread(request: Request, thread_id: str = Query(..., description="要删除的会话ID")) -> RouteResponse:
    """
    删除会话：清掉 LangGraph 的 checkpoint，再删 admin_thread 记录

    -> 只能删自己的（按 admin_thread.admin_id 校验，不看 thread_id 前缀）
    -> 必须先归档：没归档不允许直接删（归档是第一步，删除是第二步）
    -> 正在运行 / 停在人工确认的会话不允许删：图还在跑，删完会立刻写回新的 checkpoint
    -> store 没用到（0 行）、cache 的键不含 thread_id，两者都不用动
    """
    graph = getattr(request.app.state, "main_graph", None)
    if graph is None:
        raise HTTPException(status_code=503, detail="主图尚未初始化")
    try:
        admin_id = int(request.state.info.get("id"))
    except (TypeError, ValueError):
        return RouteResponse.error(msg="token 解析错误", code=ResultCode.INVALID_VALUE_ERROR)
    thread_id = thread_id.strip()
    if not thread_id:
        return RouteResponse.error(msg="thread_id 不能为空", code=ResultCode.INVALID_VALUE_ERROR)

    # 先查归属和归档状态：不是自己的会话，连"存不存在"都不告诉他
    try:
        async with AsyncSessionLocal() as session:
            row = (await session.execute(
                select(AdminThread.admin_id, AdminThread.is_archive)
                .where(AdminThread.thread_id == thread_id)
            )).first()
    except Exception as exc:  # noqa: BLE001
        logger.error(f"查询会话 {thread_id} 归属失败：{exc}")
        return RouteResponse.error(msg=f"查询会话归属失败：{exc}", code=ResultCode.DB_ERROR)
    if row is None:
        return RouteResponse.error(msg="会话不存在", code=ResultCode.NOT_FOUND)
    owner, is_archive = row
    if owner != admin_id:
        return RouteResponse.error(msg="只能删除自己的会话", code=ResultCode.UNAUTHORIZED)
    # 删除是归档之后的第二步：没归档不允许直接删
    if not is_archive:
        return RouteResponse.error(msg="请先归档该会话，再执行删除", code=ResultCode.INVALID_VALUE_ERROR)

    # 占用中不能删：图还在写，删完 checkpoint 会被重新创建
    if background_broker.is_occupied(thread_id):
        return RouteResponse.error(msg="该会话正在运行或停在人工确认，无法删除",
                                   code=ResultCode.INVALID_VALUE_ERROR)

    # 1. 清 checkpoint：内部 DELETE checkpoints / checkpoint_blobs / checkpoint_writes
    checkpointer = getattr(graph, "checkpointer", None)
    if checkpointer is None:
        return RouteResponse.error(msg="checkpointer 未初始化", code=ResultCode.SERVER_ERROR)
    try:
        await checkpointer.adelete_thread(thread_id)
    except Exception as exc:  # noqa: BLE001
        logger.error(f"删除会话 {thread_id} 的 checkpoint 失败：{exc}")
        return RouteResponse.error(msg=f"删除会话状态失败：{exc}", code=ResultCode.SERVER_ERROR)

    # 2. 清内存里的后台任务记录，否则已删会话还会继续往 WebSocket 推
    background_registry.remove_thread(thread_id)

    # 3. 删 admin_thread 记录（放最后：checkpoint 删失败时记录还在，可以重试）
    try:
        async with AsyncSessionLocal() as session:
            await session.execute(delete(AdminThread).where(AdminThread.thread_id == thread_id))
            await session.commit()
    except Exception as exc:  # noqa: BLE001
        logger.error(f"删除会话 {thread_id} 的 admin_thread 记录失败：{exc}")
        return RouteResponse.error(msg=f"会话状态已删除，但记录删除失败：{exc}", code=ResultCode.DB_ERROR)

    _thread_locks.pop(thread_id, None)
    logger.info(f"会话已删除：thread_id={thread_id}, operator={admin_id}")
    return RouteResponse.ok(msg="会话已删除")


@route.delete("/thread/deletes")
async def delete_threads(
        request: Request,
        thread_ids: list[str] = Query(..., description="批量删除的会话ID")
) -> RouteResponse:
    graph = getattr(request.app.state, "main_graph", None)
    if graph is None:
        raise HTTPException(status_code=503, detail="主图尚未初始化")
    try:
        admin_id = int(request.state.info.get("id"))
    except (TypeError, ValueError):
        return RouteResponse.error(msg="token 解析错误", code=ResultCode.INVALID_VALUE_ERROR)
    if not thread_ids:
        return RouteResponse.error(msg="thread_ids 不能为空", code=ResultCode.INVALID_VALUE_ERROR)

    # 先查归属和归档状态：不是自己的会话，连"存不存在"都不告诉他
    try:
        async with AsyncSessionLocal() as session:
            rows = (await session.execute(
                select(AdminThread.admin_id, AdminThread.is_archive, AdminThread.thread_id)
                .where(AdminThread.thread_id.in_(thread_ids))
            )).mappings().all()
    except Exception as exc:  # noqa: BLE001
        logger.error(f"查询会话 {thread_ids} 归属失败：{exc}")
        return RouteResponse.error(msg=f"查询会话归属失败：{exc}", code=ResultCode.DB_ERROR)
    if not rows:
        return RouteResponse.error(msg="会话不存在", code=ResultCode.NOT_FOUND)
    filtered_threads = []
    for row in rows:
        owner, is_archive, thread_id = row['admin_id'], row['is_archive'], row['thread_id']
        if owner != admin_id:
            continue
        # 删除是归档之后的第二步：没归档不允许直接删
        if not is_archive:
            continue
        # 占用中不能删：图还在写，删完 checkpoint 会被重新创建
        if background_broker.is_occupied(thread_id):
            continue
        filtered_threads.append(thread_id)

    # 1. 清 checkpoint：内部 DELETE checkpoints / checkpoint_blobs / checkpoint_writes
    checkpointer = getattr(graph, "checkpointer", None)
    if checkpointer is None:
        return RouteResponse.error(msg="checkpointer 未初始化", code=ResultCode.SERVER_ERROR)
    try:
        for thread_id in filtered_threads:
            await checkpointer.adelete_thread(thread_id)
    except Exception as exc:  # noqa: BLE001
        logger.error(f"删除会话 {filtered_threads} 的 checkpoint 失败：{exc}")
        return RouteResponse.error(msg=f"删除会话状态失败：{exc}", code=ResultCode.SERVER_ERROR)

    # 2. 清内存里的后台任务记录，否则已删会话还会继续往 WebSocket 推
    for thread_id in filtered_threads:
        background_registry.remove_thread(thread_id)

    # 3. 删 admin_thread 记录（放最后：checkpoint 删失败时记录还在，可以重试）
    try:
        async with AsyncSessionLocal() as session:
            await session.execute(delete(AdminThread).where(AdminThread.thread_id.in_(filtered_threads)))
            await session.commit()
    except Exception as exc:  # noqa: BLE001
        logger.error(f"删除会话 {filtered_threads} 的 admin_thread 记录失败：{exc}")
        return RouteResponse.error(msg=f"会话状态已删除，但记录删除失败：{exc}", code=ResultCode.DB_ERROR)
    for thread_id in filtered_threads:
        _thread_locks.pop(thread_id, None)
    logger.info(f"会话已删除：thread_id={filtered_threads}, operator={admin_id}")
    return RouteResponse.ok(msg="会话已删除")


@route.get("/background")
async def list_background(
        request: Request,
        thread_id: str | None = Query(default=None, description="只查某个会话"),
) -> RouteResponse:
    """前端轮询后台任务状态/结果；只返回自己的任务"""
    user_id = str(request.state.info.get("id"))
    records = background_registry.list_for_user(user_id)
    if thread_id:
        records = [record for record in records if record.main_thread_id == thread_id]
    return RouteResponse.ok(data=jsonable_encoder([asdict(record) for record in records]))


@route.websocket("/ws/background")
async def background_ws(websocket: WebSocket, token: str = Query(default="")):
    """后台任务实时推送；WS 不经过 HTTP 中间件，所以这里自己校验 token"""
    verify = getattr(websocket.app.state, "verify_token", None)
    prefix = getattr(websocket.app.state, "auth_prefix", "Bearer token ")
    if verify is None or not token:
        await websocket.close(code=1008)
        return
    token_data, _ = await run_in_threadpool(verify, prefix + token)
    if token_data is None:
        await websocket.close(code=1008)
        return
    user_id = str(token_data.id)

    await websocket.accept()
    queue = background_registry.subscribe()
    try:
        snapshot = [asdict(record) for record in background_registry.list_for_user(user_id)]
        await websocket.send_json({"type": "snapshot", "records": jsonable_encoder(snapshot)})
        while True:
            record = await queue.get()
            if record.user_id != user_id:
                continue
            await websocket.send_json({"type": "update", "record": jsonable_encoder(asdict(record))})
    except (WebSocketDisconnect, RuntimeError):
        pass
    finally:
        background_registry.unsubscribe(queue)
