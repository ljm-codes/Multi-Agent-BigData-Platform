import asyncio
import base64
import json
import os
import secrets
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from typing import Any, Literal
from uuid import uuid4

import jwt
from fastapi import APIRouter, HTTPException, Query, Request, WebSocket, WebSocketDisconnect, Body
from fastapi.encoders import jsonable_encoder
from fastapi.responses import StreamingResponse
from langchain_core.messages import AIMessage, AIMessageChunk
from langgraph.types import Command
from pydantic import BaseModel, Field
from sqlalchemy import select, and_, false, true, delete, update, func
from sqlalchemy.dialects.postgresql import insert

from SPO.main_agent_state_model import MainStateContext, content_text
from SPO.postgresql_model import AdminThread
from SPO.route_results import RouteResponse, ResultCode
from Tasks.background_broker import background_broker, background_registry
from Tasks.verification_registry import verification_registry
from Tools.log_settings import LogSetting
from Tools.tool_trace_store import tool_trace_store
from Tools.db import AsyncSessionLocal
from Tools.share_check_tool import redis_con
from Tools.ws_registry import register_ws, unregister_ws
from load_config.config import ROOT_BASE_DIR_PATH, config

route = APIRouter()
logger = LogSetting.create(__name__)
_is_conn_cluster = bool(config.get("app", {}).get("connect_to_cluster", False))


class ChatRequest(BaseModel):
    user_id: str = Field(default="default", description="用户 id")
    message: str = Field(min_length=1, description="用户原话")
    thread_id: str | None = Field(default=None, description="会话 id；不传则新建")


class ThreadRequest(BaseModel):
    thread_id: str = Field(description="会话 id")
    thread_name: str = Field(description="新会话名称")
    is_top: bool = Field(default=False, description="是否置顶")


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
    interrupted = _is_interrupted(result)
    fields: dict[str, Any] = {
        "thread_id": thread_id,
        # 停在 interrupt 时 output_msg 还是上一轮的标量值，不能回给前端
        "output_msg": "" if interrupted else result.get("output_msg", ""),
        "out_time": result.get("out_time"),
        "metadata": metadata,
        "thread_action": metadata.get("thread_action", "continue"),
        "thread_title": metadata.get("thread_title"),
        "interrupted": interrupted,
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


async def _final_frames(text: str, thread_id: str, *, chars: int = 6, delay: float = 0.03):
    """把最终答案按 chars 字/帧、间隔 delay 秒推送，前端收到即追加"""
    text = text or ""
    for index in range(0, len(text), chars):
        yield "token", {
            "thread_id": thread_id,
            "node": "final",
            "agent": "",
            "ns": [],
            "content": text[index:index + chars],
        }
        await asyncio.sleep(delay)


def _stream_agent_key(message: Any, subgraph: Any = None) -> str | None:
    """从 v3 message 的 node/namespace 或 subgraph path 识别 Agent。"""
    parts: list[str] = []
    if subgraph is not None:
        parts.extend([str(getattr(subgraph, "graph_name", "") or "")])
        parts.extend(str(x) for x in (getattr(subgraph, "path", ()) or ()))
    parts.extend(str(x) for x in (getattr(message, "namespace", ()) or ()))
    node = getattr(message, "node", None)
    if node:
        parts.append(str(node))
    scope = " ".join(parts)
    if "code_verify" in scope or "verify_agent" in scope or "ver_" in scope:
        return "code_verify"
    if "data_analysis" in scope:
        return "data_analysis"
    if "data_develop" in scope:
        return "data_develop"
    # 非数据 Agent 的子图（CVA 自定义子图）默认按质检处理
    if subgraph is not None:
        return "code_verify"
    return None


def _agent_from_dispatch(output: Any) -> str | None:
    """从 dis_chat 的结构化输出里读出本轮下发给了谁；v3 的 model 消息本身没有 agent tag。"""
    for call in (getattr(output, "tool_calls", None) or []):
        args = call.get("args") if isinstance(call, dict) else getattr(call, "args", None)
        if not isinstance(args, dict):
            continue
        for info in (args.get("send_info_list") or []):
            if not isinstance(info, dict):
                continue
            send_to = info.get("send_to")
            if send_to in ("data_analysis", "data_develop"):
                return send_to
            app_type = info.get("app_type")
            if app_type == "spark":
                return "data_analysis"
            if app_type == "flink":
                return "data_develop"
    return None


async def _stream_user_turn(graph: Any, thread_id: str, user_id: str, payload: Any, ctx: dict):
    """v3 event streaming：自动消费子图消息，CVA 也能出现在 SSE 里。"""
    async with _lock_for(thread_id):
        try:
            await _wait_until_idle(thread_id)
        except HTTPException as exc:
            yield "error", {"thread_id": thread_id, "detail": exc.detail, "status": exc.status_code}
            return
        background_broker.set_active(thread_id)
        event_queue: asyncio.Queue = asyncio.Queue()
        verification_queue = verification_registry.subscribe()
        last_tool_by_agent: dict[str, str] = {}
        seen_messages: set[str] = set()
        current_agent: str | None = None

        async def emit_message(message: Any, subgraph: Any = None) -> None:
            nonlocal current_agent
            node = str(getattr(message, "node", "") or "")
            if subgraph is not None and node in {"ver_handler", "__start__", "start"}:
                # CVA 子图的输入消息（原任务 + 开发者回复）不重复塞进 code_verify 气泡
                return
            agent = _stream_agent_key(message, subgraph)
            # 先把 text 消费掉，避免 v3 投影背压；Human/Tool 消息随后丢掉
            text_parts: list[str] = []
            try:
                async for delta in message.text:
                    if delta:
                        text_parts.append(delta)
            except asyncio.CancelledError:
                raise
            except Exception:
                pass
            try:
                output = await message.output
            except asyncio.CancelledError:
                raise
            except Exception:
                output = None
            if getattr(message, "node", None) == "dis_chat":
                dispatched = _agent_from_dispatch(output)
                if dispatched:
                    current_agent = dispatched
            if agent is None and subgraph is None and getattr(message, "node", None) == "model":
                # create_agent 内部的模型消息 node 是 "model"，没有 agent tag；
                # 用最近一次 dis_chat 的结构化输出补上归属。
                agent = current_agent
            if not agent or not isinstance(output, AIMessage):
                return
            msg_id = str(getattr(message, "message_id", "") or id(message))
            if msg_id in seen_messages:
                return
            seen_messages.add(msg_id)
            if text_parts:
                await event_queue.put(("update", {
                    "thread_id": thread_id, "node": agent, "agent": agent,
                    "ns": [], "phase": "llm_start",
                }, None))
                for delta in text_parts:
                    await event_queue.put(("token", {
                        "thread_id": thread_id, "node": agent, "agent": agent,
                        "ns": [], "content": delta,
                    }, None))
            # tool_calls 在 output 里是完整的；在工具真正执行前推一次状态即可
            for call in (getattr(output, "tool_calls", None) or []):
                name = call.get("name") if isinstance(call, dict) else getattr(call, "name", None)
                if not name or name in {"VerifyAgentOutClass", "DevelopAgentOutState"}:
                    continue
                if last_tool_by_agent.get(agent) == name:
                    continue
                last_tool_by_agent[agent] = name
                await event_queue.put(("status", {
                    "thread_id": thread_id, "node": agent, "agent": agent,
                    "text": f"正在调用工具：{name}",
                }, None))

        async def root_producer() -> None:
            try:
                async for message in stream.messages:
                    await emit_message(message)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                await event_queue.put(("error", exc, None))

        async def subgraph_producer() -> None:
            try:
                async for subgraph in stream.subgraphs:
                    async for message in subgraph.messages:
                        await emit_message(message, subgraph)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                await event_queue.put(("error", exc, None))

        async def values_producer() -> None:
            # v3 的 values 是原生 projection；不消费会背压住整条 stream
            try:
                async for _ in stream.values:
                    pass
            except asyncio.CancelledError:
                raise
            except Exception:
                pass

        async def lifecycle_producer() -> None:
            try:
                async for _ in stream.lifecycle:
                    pass
            except asyncio.CancelledError:
                raise
            except Exception:
                pass

        async def verification_producer() -> None:
            try:
                while True:
                    record = await verification_queue.get()
                    await event_queue.put(("verification", record, None))
            except asyncio.CancelledError:
                raise

        stream = await graph.astream_events(
            payload,
            config={"configurable": {"thread_id": thread_id}},
            context=MainStateContext(thread_id=thread_id, user_id=user_id),
            version="v3",
        )

        async def driver() -> None:
            try:
                async with stream:
                    await asyncio.gather(
                        root_producer(), subgraph_producer(),
                        values_producer(), lifecycle_producer(),
                    )
                    output = await stream.output() or {}
                    interrupts = list(await stream.interrupts() or ())
                    await event_queue.put(("done", (output, interrupts), None))
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                await event_queue.put(("error", exc, None))

        driver_task = asyncio.create_task(driver())
        verification_task = asyncio.create_task(verification_producer())
        try:
            while True:
                kind, item, _meta = await event_queue.get()
                if kind == "done":
                    output, interrupts = item
                    ctx["output"] = output or {}
                    ctx["interrupted"] = bool(interrupts)
                    if interrupts:
                        ctx["interrupts"] = interrupts
                        yield "interrupt", {"thread_id": thread_id, "interrupt": interrupts}
                    break
                if kind == "error":
                    raise item
                if kind == "verification":
                    record = item.get("record") if isinstance(item, dict) and "record" in item else item
                    if isinstance(record, dict) and record.get("thread_id") == thread_id:
                        yield "verification", {"thread_id": thread_id, "record": record}
                    continue
                if kind in ("update", "token", "status"):
                    yield kind, item
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
        finally:
            driver_task.cancel()
            verification_task.cancel()
            verification_registry.unsubscribe(verification_queue)
            await asyncio.gather(driver_task, verification_task, return_exceptions=True)
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


async def _ensure_thread(
        thread_id: str, admin_id: int, thread_name: str,
) -> tuple[ResultCode, str] | None:
    """确保 admin_thread 有记录；失败返回 (错误码, 消息)，成功返回 None。"""
    owner = await _thread_owner(thread_id)
    if owner is not None:
        if owner != admin_id:
            return ResultCode.UNAUTHORIZED, "只能操作自己的会话"
        return None
    try:
        async with AsyncSessionLocal() as session:
            await session.execute(insert(AdminThread).values(
                thread_id=thread_id,
                thread_name=(thread_name or "新会话")[:10],
                admin_id=admin_id,
            ))
            await session.commit()
    except Exception as exc:  # noqa: BLE001
        logger.error(f"添加线程到 admin_thread 失败: {exc}")
        return ResultCode.DB_ERROR, f"添加线程到 admin_thread 失败: {exc}"
    return None


@route.post("/chat/stream")
async def chat_stream(req: ChatRequest, request: Request) -> StreamingResponse:
    graph = getattr(request.app.state, "main_graph", None)
    if graph is None:
        raise HTTPException(status_code=503, detail="主图尚未初始化")

    admin_id = int(request.state.info.get("id"))
    req.user_id = str(admin_id)  # 一律以 token 为准，请求体里的 user_id 不再采信
    thread_id = req.thread_id or _new_thread_id(req.user_id)
    human_msg = req.message

    failure = await _ensure_thread(thread_id, admin_id, human_msg)
    if failure:
        code, msg = failure
        return RouteResponse.error(msg=msg, code=code)

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
            failure = await _ensure_thread(
                new_thread_id, admin_id, metadata.get("thread_title") or request_text)
            if failure:
                code, msg = failure
                yield _sse("error", {
                    "thread_id": new_thread_id, "detail": msg, "status": code,
                })
                return
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
            async for event, data in _final_frames(
                    (ctx2["output"] or {}).get("output_msg", ""), new_thread_id):
                yield _sse(event, data)
            yield _sse("done", _done_payload(
                ctx2["output"], new_thread_id,
                thread_action="new_task",
                thread_title=metadata.get("thread_title"),
                previous_thread_id=thread_id,
            ))
            return

        async for event, data in _final_frames(output.get("output_msg", ""), thread_id):
            yield _sse(event, data)
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

    failure = await _ensure_thread(thread_id, admin_id, human_msg)
    if failure:
        code, msg = failure
        return RouteResponse.error(msg=msg, code=code)

    result = await _run_user_turn(graph, thread_id, req.user_id, {"input_msg": human_msg})
    if _is_interrupted(result):
        return _response(result, thread_id)

    metadata = result.get("metadata") or {}
    if metadata.get("thread_action") == "new_task":
        # 旧 thread 有未消费的后台任务：换新 thread，用用户原话重跑
        new_thread_id = _new_thread_id(req.user_id)
        request_text = metadata.get("request_text") or req.message
        failure = await _ensure_thread(
            new_thread_id, admin_id, metadata.get("thread_title") or request_text)
        if failure:
            code, msg = failure
            return RouteResponse.error(msg=msg, code=code)
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
        interrupts = list(snapshot.interrupts or ())
        values = snapshot.values or {}
        messages = []
        for m in values.get("messages", []):
            text = content_text(m.content)
            # 调度的结构化 AIMessage 只有 tool call、没有正文，前端不展示
            if isinstance(m, AIMessage) and not text.strip():
                continue
            messages.append({
                "type": type(m).__name__,
                "name": getattr(m, "name", None),
                "content": text,
            })
        metadata = values.get('metadata') or {}
        return RouteResponse.ok(data={
            'thread_id': thread_id,
            'is_archive': is_archive,
            'manual_intervention': bool(interrupts),
            'interrupted': bool(interrupts),
            'interrupt': jsonable_encoder(interrupts),
            'project_dir': _project_dir(metadata),
            # 旧会话兜底；新会话最终正文已经在 messages 里，前端会去重
            'output_msg': '' if interrupts else values.get('output_msg', ''),
            'metadata': metadata,
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
    filters = [
        AdminThread.admin_id == admin_id,
        AdminThread.is_archive == (false() if not is_archive else true()),
    ]
    if start_time and end_time:
        filters.append(AdminThread.create_time.between(start_time, end_time))
    stmt = select(AdminThread.id, AdminThread.thread_id, AdminThread.is_archive, AdminThread.create_time,
                  AdminThread.thread_name, AdminThread.is_top).where(*filters).order_by(
        AdminThread.create_time.desc()).limit(limit).offset(offset)
    count_stmt = select(func.count()).select_from(AdminThread).where(*filters)
    payload = {
        'limit': limit,
        'offset': offset,
    }
    try:
        async with AsyncSessionLocal() as session:
            result = (await session.execute(stmt)).mappings().all()
            total = (await session.execute(count_stmt)).scalar_one()
        # total = 当前页条数；total_count = 全量匹配数
        payload['total'] = len(result)
        payload['total_count'] = int(total or 0)
        payload['records'] = [dict(row) for row in result]
        return RouteResponse.ok(data=payload)
    except Exception as exc:
        logger.error(f"查询会话失败：{exc}")
        return RouteResponse.error(msg=f"查询会话失败：{exc}", code=ResultCode.DB_ERROR)


@route.put("/thread/change")
async def change_thread_name(request: Request, thread_info: ThreadRequest) -> RouteResponse:
    thread_id = thread_info.thread_id.strip()
    thread_name = thread_info.thread_name.strip()[:10]
    if not thread_id or not thread_name:
        return RouteResponse.error(msg="thread_id 或 thread_name 不能为空", code=ResultCode.INVALID_VALUE_ERROR)
    try:
        admin_id = int(request.state.info.get("id"))
    except (TypeError, ValueError):
        return RouteResponse.error(msg="token 解析错误", code=ResultCode.INVALID_VALUE_ERROR)
    # 先看看还在不在
    try:
        async with AsyncSessionLocal() as session:
            row = (await session.execute(
                select(AdminThread.admin_id)
                .where(and_(
                    AdminThread.thread_id == thread_id,
                    AdminThread.admin_id == admin_id,
                ))
            )).first()
    except Exception as exc:  # noqa: BLE001
        logger.error(f"查询会话 {thread_id} 归属失败：{exc}")
        return RouteResponse.error(msg=f"查询会话归属失败：{exc}", code=ResultCode.DB_ERROR)
    if row is None:
        return RouteResponse.error(msg="会话不存在", code=ResultCode.NOT_FOUND)
    # 正式修改
    try:
        async with AsyncSessionLocal() as session:
            await session.execute(
                update(AdminThread)
                .where(and_(
                    AdminThread.thread_id == thread_id,
                    AdminThread.admin_id == admin_id,
                ))
                .values(thread_name=thread_name, update_time=func.now())
            )
            await session.commit()
    except Exception as exc:  # noqa: BLE001
        logger.error(f"修改会话 {thread_id} 名称失败：{exc}")
        return RouteResponse.error(msg=f"修改会话 {thread_id} 名称失败：{exc}", code=ResultCode.DB_ERROR)
    return RouteResponse.ok(msg="会话名称修改成功")


@route.put("/thread/top")
async def top_thread(request: Request, thread_info: ThreadRequest) -> RouteResponse:
    thread_id = thread_info.thread_id.strip()
    thread_id_top = thread_info.is_top
    if not thread_id:
        return RouteResponse.error(msg="thread_id 或 is_top 不能为空", code=ResultCode.INVALID_VALUE_ERROR)
    try:
        admin_id = int(request.state.info.get("id"))
    except (TypeError, ValueError):
        return RouteResponse.error(msg="token 解析错误", code=ResultCode.INVALID_VALUE_ERROR)
        # 先看看还在不在
    try:
        async with AsyncSessionLocal() as session:
            row = (await session.execute(
                select(AdminThread.admin_id, AdminThread.is_top)
                .where(and_(
                    AdminThread.thread_id == thread_id,
                    AdminThread.admin_id == admin_id,
                ))
            )).first()
    except Exception as exc:  # noqa: BLE001
        logger.error(f"查询会话 {thread_id} 归属失败：{exc}")
        return RouteResponse.error(msg=f"查询会话归属失败：{exc}", code=ResultCode.DB_ERROR)
    if row is None:
        return RouteResponse.error(msg="会话不存在", code=ResultCode.NOT_FOUND)
    owner, is_top = row
    if owner != admin_id:
        return RouteResponse.error(msg="只能置顶自己的会话", code=ResultCode.UNAUTHORIZED)
    if is_top == thread_id_top:
        return RouteResponse.ok(msg=f"会话已经处于{'置顶' if thread_id_top else '未置顶'}状态，无需重复操作")
    # 正式修改
    try:
        async with AsyncSessionLocal() as session:
            await session.execute(
                update(AdminThread)
                .where(and_(
                    AdminThread.thread_id == thread_id,
                    AdminThread.admin_id == admin_id,
                ))
                .values(is_top=thread_id_top, update_time=func.now())
            )
            await session.commit()
    except Exception as exc:  # noqa: BLE001
        logger.error(f"修改会话 {thread_id} 名称失败：{exc}")
        return RouteResponse.error(msg=f"修改会话 {thread_id} 状态失败：{exc}", code=ResultCode.DB_ERROR)
    return RouteResponse.ok(msg=f"会话状态修改成功，当前状态为{'置顶' if thread_id_top else '未置顶'}")


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


@route.get("/tool-traces")
async def list_tool_traces(
        request: Request,
        thread_id: str = Query(..., description="会话 id"),
        limit: int = Query(default=100, ge=1, le=500),
        offset: int = Query(default=0, ge=0),
) -> RouteResponse:
    """工具调用轨迹；历史直接查 PostgreSQL log.tool_trace。"""
    admin_id = int(request.state.info.get("id"))
    owner = await _thread_owner(thread_id)
    if owner != admin_id:
        return RouteResponse.error(msg="只能查看自己的会话", code=ResultCode.UNAUTHORIZED)
    records = await tool_trace_store.list(thread_id, limit=limit, offset=offset)
    return RouteResponse.ok(data=records)


@route.get("/verification")
async def get_verification(
        request: Request,
        thread_id: str = Query(..., description="会话 id"),
) -> RouteResponse:
    """当前 thread 的最新验证进度事件（刷新/WS 重连时的兜底）。"""
    admin_id = int(request.state.info.get("id"))
    owner = await _thread_owner(thread_id)
    if owner != admin_id:
        return RouteResponse.error(msg="只能查看自己的会话", code=ResultCode.UNAUTHORIZED)
    return RouteResponse.ok(data=verification_registry.get(thread_id))


def _generate_temp_key() -> str:
    raw_key = os.urandom(32)
    base64_key = base64.b64encode(raw_key).decode('utf-8')
    return base64_key


@route.post("/background/verify")
async def background_verify(
        request: Request,
        source: Literal["main", "ops"] = Body(
            default="main",
            description="信号来源，避免当未开启 ops 功能时，ops 信号被错误触发",
            embed=True
        )
) -> RouteResponse:
    """将token放于这里进行校验，返回一个短暂的 Ticket 用于后续的 WebSocket 连接"""
    if source == "ops" and not _is_conn_cluster:
        return RouteResponse.error(msg="ops 功能未开启", code=ResultCode.INVALID_VALUE_ERROR)
    token_info = request.state.info
    if not token_info or not isinstance(token_info, dict):
        return RouteResponse.error(msg="token 校验失败, 请重新登录", code=ResultCode.INVALID_VALUE_ERROR)
    user_id = str(token_info.get("id"))
    disabled = token_info.get("disabled", False)
    try:
        payload = {
            "user_id": user_id,
            "disabled": disabled,
            # 权限随票带过去；WS 注册和 ops 广播都靠它过滤
            "permission": int(token_info.get("permission") or 0),
            # 只允许 20秒的时限（必须带时区，否则 PyJWT 按 UTC 解释 naive 时间）
            "exp": datetime.now(timezone.utc) + timedelta(seconds=20),
            "iat": datetime.now(timezone.utc),
        }
        tmp_key = _generate_temp_key()
        # 存储临时key到Redis
        redis_con.hset("jwt_key", f"background:temp-{user_id}", tmp_key)
        ticket = jwt.encode(payload, tmp_key, algorithm="HS256")
        code = secrets.token_urlsafe(16)
        redis_con.hset("temp_code", code, f"background:{user_id}:{ticket}")
        return RouteResponse.ok(data={"ticket": code})
    except Exception as exc:  # noqa: BLE001
        logger.error(f"生成 Ticket 失败：{exc}")
        return RouteResponse.error(msg=f"生成 Ticket 失败：{exc}", code=ResultCode.SERVER_ERROR)


@route.websocket("/ws/background")
async def background_ws(
        websocket: WebSocket,
        ticket: str = Query(default=""),
        thread_id: str | None = Query(default=None),
):
    """后台任务/工具轨迹/验证进度实时推送；WS 不经过 HTTP 中间件，用 Ticket 校验。"""
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
    if not user_id or disabled:
        await websocket.close(code=4008)
        return

    # 带 thread_id 时只允许订阅自己的会话，避免工具参数/输出串用户
    if thread_id:
        owner = await _thread_owner(thread_id)
        if owner != int(user_id):
            await websocket.close(code=4008)
            return

    await websocket.accept()
    await register_ws(user_id, websocket, permission=permission)
    bg_queue = background_registry.subscribe()
    trace_queue = tool_trace_store.subscribe() if thread_id else None
    ver_queue = verification_registry.subscribe() if thread_id else None

    merged: asyncio.Queue = asyncio.Queue()

    async def pump(queue: Any, kind: str) -> None:
        while True:
            item = await queue.get()
            await merged.put((kind, item))

    pumps: list[asyncio.Task] = [asyncio.create_task(pump(bg_queue, "background"))]
    if trace_queue is not None:
        pumps.append(asyncio.create_task(pump(trace_queue, "tool_trace")))
    if ver_queue is not None:
        pumps.append(asyncio.create_task(pump(ver_queue, "verification")))

    try:
        records = [asdict(record) for record in background_registry.list_for_user(user_id)]
        # 快照只发给刚连上的这一个连接
        await websocket.send_json({"type": "snapshot", "records": jsonable_encoder(records)})

        if thread_id:
            traces = await tool_trace_store.list(thread_id, limit=200)
            await websocket.send_json({
                "type": "tool_trace_snapshot",
                "records": jsonable_encoder(traces),
            })
            await websocket.send_json({
                "type": "verification_snapshot",
                "record": jsonable_encoder(verification_registry.get(thread_id)),
            })

        while True:
            kind, item = await merged.get()
            if kind == "background":
                record = item
                if record.user_id != user_id:
                    continue
                await websocket.send_json({
                    "type": "update",
                    "record": jsonable_encoder(asdict(record)),
                })
            elif kind == "tool_trace":
                record = (item or {}).get("record") or {}
                if record.get("thread_id") != thread_id:
                    continue
                await websocket.send_json({
                    "type": "tool_trace",
                    "record": jsonable_encoder(record),
                })
            elif kind == "verification":
                record = (item or {}).get("record") or {}
                if record.get("thread_id") != thread_id:
                    continue
                await websocket.send_json({
                    "type": "verification",
                    "record": jsonable_encoder(record),
                })
    except (WebSocketDisconnect, RuntimeError):
        pass
    finally:
        for task in pumps:
            task.cancel()
        for task in pumps:
            try:
                await task
            except asyncio.CancelledError:
                pass
        background_registry.unsubscribe(bg_queue)
        if trace_queue is not None:
            tool_trace_store.unsubscribe(trace_queue)
        if ver_queue is not None:
            verification_registry.unsubscribe(ver_queue)
        await unregister_ws(user_id, websocket)
