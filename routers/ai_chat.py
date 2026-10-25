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

from SPO.main_agent_state_model import MainStateContext
from Tasks.background_broker import background_broker, background_registry

route = APIRouter()


class ChatRequest(BaseModel):
    user_id: str = Field(default="default", description="用户 id")
    message: str = Field(min_length=1, description="用户原话")
    thread_id: str | None = Field(default=None, description="会话 id；不传则新建")


class ResumeRequest(BaseModel):
    user_id: str = Field(default="default", description="用户 id")
    thread_id: str = Field(description="停在 interrupt 的会话 id")
    # CVA 人工确认的格式：{"pass": true/false, "msg": "...", "force_terminate": true/false}
    answer: dict[str, Any] = Field(description="人工回复，必须与 interrupt 的 payload 对应")


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


def _response(result: dict, thread_id: str, **extra: Any) -> ChatResponse:
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
    }
    fields.update(extra)
    return ChatResponse(**fields)


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


@route.post("/chat/stream")
async def chat_stream(req: ChatRequest, request: Request) -> StreamingResponse:
    graph = getattr(request.app.state, "main_graph", None)
    if graph is None:
        raise HTTPException(status_code=503, detail="主图尚未初始化")

    async def event_stream():
        thread_id = req.thread_id or _new_thread_id(req.user_id)
        yield _sse("meta", {"thread_id": thread_id})

        ctx: dict[str, Any] = {"output": {}, "interrupted": False}
        async for event, data in _stream_user_turn(
                graph, thread_id, req.user_id, {"input_msg": req.message}, ctx):
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


@route.post("/chat", response_model=ChatResponse)
async def chat(req: ChatRequest, request: Request) -> ChatResponse:
    graph = getattr(request.app.state, "main_graph", None)
    if graph is None:
        raise HTTPException(status_code=503, detail="主图尚未初始化")

    thread_id = req.thread_id or _new_thread_id(req.user_id)
    result = await _run_user_turn(graph, thread_id, req.user_id, {"input_msg": req.message})
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


@route.post("/resume", response_model=ChatResponse)
async def resume(req: ResumeRequest, request: Request) -> ChatResponse:
    graph = getattr(request.app.state, "main_graph", None)
    if graph is None:
        raise HTTPException(status_code=503, detail="主图尚未初始化")

    thread_id = req.thread_id
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


@route.get("/background")
async def list_background(
        user_id: str = Query(..., description="用户 id"),
        thread_id: str | None = Query(default=None, description="只查某个会话"),
) -> list[dict[str, Any]]:
    """前端轮询后台任务状态/结果；后台完成的消息从这里回到原会话窗口"""
    records = background_registry.list_for_user(user_id)
    if thread_id:
        records = [record for record in records if record.main_thread_id == thread_id]
    return jsonable_encoder([asdict(record) for record in records])


@route.websocket("/ws/background")
async def background_ws(websocket: WebSocket, user_id: str):
    """后台任务实时推送：连上先发快照，之后 register/update 有变化就推"""
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
