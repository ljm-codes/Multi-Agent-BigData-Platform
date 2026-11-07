"""运维转交开发：搬源码目录、后台跑开发图、维护运行记录/租约、广播 WS

开发图的 runner 由 main.py 在 lifespan 里注入（避免 agent -> main 的循环导入）。
同一个 thread 的调用串行执行，占用状态仍用 background_broker 维护：
    started -> active -> paused（停在 interrupt）/ idle（跑完）
"""
from __future__ import annotations

import asyncio
import shutil
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Awaitable, Callable
from uuid import uuid4

from langgraph.types import Command
from sqlalchemy import select

from SPO.main_agent_state_model import DevelopAgentInputState, OpsHandoverState
from SPO.postgresql_model import AgentRun
from Tasks.background_broker import background_broker
from Tools.db import SessionLocal
from Tools.log_settings import LogSetting
from Tools.ws_registry import send_json_to_all
from agent.operations_agent_run import _finish_agent_run, _release_lease
from load_config.config import ROOT_BASE_DIR_PATH

logger = LogSetting.create(__name__)

DevRunner = Callable[[str, Any], Awaitable[Any]]
DevStateGetter = Callable[[str], Awaitable[dict]]
_dev_runner: DevRunner | None = None
_state_getter: DevStateGetter | None = None
_locks: dict[str, asyncio.Lock] = {}
_tasks: set[asyncio.Task] = set()
OPS_USER_ID = "ops"


def set_dev_runner(runner: DevRunner | None) -> None:
    global _dev_runner
    _dev_runner = runner


def set_dev_state_getter(getter: DevStateGetter | None) -> None:
    global _state_getter
    _state_getter = getter


def dev_thread_id(session_id: str) -> str:
    return f"ops:{session_id}"


def ops_session_id(thread_id: str) -> str:
    return thread_id[4:] if thread_id.startswith("ops:") else thread_id


def _lock_for(thread_id: str) -> asyncio.Lock:
    lock = _locks.get(thread_id)
    if lock is None:
        lock = asyncio.Lock()
        _locks[thread_id] = lock
    return lock


async def _wait_idle(thread_id: str) -> None:
    while background_broker.occupancy(thread_id) == "active":
        await background_broker.wait()


def prepare_project(source_path: str, app_type: str) -> tuple[str, str]:
    """tmp/target/... -> tmp_code/{app_type}/{today}/{app_name}，返回 (project_name, date)

    目标已存在时按 initialize_project 的规则加前缀；阻塞操作，调用方用 to_thread。
    """
    src = Path(source_path)
    app_type = (app_type or "").lower()
    date = datetime.now().strftime("%Y-%m-%d")
    dest_dir = ROOT_BASE_DIR_PATH / "tmp_code" / app_type / date
    dest_dir.mkdir(parents=True, exist_ok=True)
    project_name = src.name
    dest = dest_dir / project_name
    if dest.exists():
        project_name = f"{app_type}-{time.time()}-{uuid4()}-{project_name}"
        dest = dest_dir / project_name
    shutil.move(str(src), str(dest))
    return project_name, date


def _handover_message(handover: OpsHandoverState) -> str:
    tasks = "\n".join(f"- {item}" for item in handover.get("task_list") or []) or "- 按错误描述修复"
    return (
        f"运维 Agent 转交的现网应用代码修复任务（项目已存在，不要重新初始化）：\n"
        f"应用 ID：{handover['app_id']}；应用名：{handover['app_name']}；类型：{handover['app_type']}\n"
        f"错误描述：{handover['error_describe']}\n"
        f"建议：{handover['suggest']}\n"
        f"任务清单：\n{tasks}"
    )


async def _broadcast(session_id: str, thread_id: str, status: str, **extra: Any) -> None:
    try:
        await send_json_to_all({
            "kind": "handover",
            "session_id": session_id,
            "dev_thread_id": thread_id,
            "status": status,
            **extra,
        }, min_permission=1)
    except Exception as exc:  # noqa: BLE001
        logger.warning(f"广播转交事件失败 {session_id}: {type(exc).__name__}: {exc}")


def _run_app_id(session_id: str) -> str:
    with SessionLocal() as session:
        return session.scalar(select(AgentRun.app_id).where(AgentRun.session_id == session_id)) or ""


def _hold_ops_run(session_id: str, project_name: str) -> None:
    """转交后保持 PAUSED、finished_at 为空：对账不会清锁，新任务也抢不走"""
    _finish_agent_run(session_id, "PAUSED", f"已转交开发 Agent（{project_name}）", "", finished=False)


def _finish_ops_run(session_id: str, status: str, out_msg: str, error: str) -> None:
    _finish_agent_run(session_id, status, out_msg, error)
    aid = _run_app_id(session_id)
    if aid:
        _release_lease(aid, session_id)


def _result_meta(result: Any) -> tuple[bool, str, dict]:
    if not isinstance(result, dict):
        return False, "", {}
    return bool(result.get("__interrupt__")), result.get("output_msg", "") or "", result.get("metadata") or {}


async def _flow_finished(thread_id: str) -> bool:
    """metadata 只有 work_end 才会写；没有说明还在后台跑（深层测试转后台）"""
    if _state_getter is None:
        return True
    try:
        values = await _state_getter(thread_id)
    except Exception as exc:  # noqa: BLE001
        logger.warning(f"读取开发图状态失败 {thread_id}: {type(exc).__name__}: {exc}")
        return True
    return bool((values or {}).get("metadata"))


async def _after_turn(thread_id: str, result: Any) -> None:
    """一轮开发图结束后的收尾：中断保持 PAUSED；转后台保持占用；真正结束才写 AgentRun/解锁"""
    session_id = ops_session_id(thread_id)
    interrupted, out_msg, metadata = _result_meta(result)
    if interrupted:
        await _broadcast(session_id, thread_id, "paused", interrupt=result.get("__interrupt__"))
        return
    if not await _flow_finished(thread_id):
        # 深层测试还在后台跑：本轮结束但流程没完，记录保持 PAUSED、租约继续持有
        await _broadcast(session_id, thread_id, "running", output_msg=out_msg)
        return
    failed = metadata.get("type") == "error"
    await asyncio.to_thread(
        _finish_ops_run, session_id,
        "ERROR" if failed else "DONE", out_msg, out_msg if failed else "",
    )
    await _broadcast(session_id, thread_id, "error" if failed else "done",
                     output_msg=out_msg, message=out_msg if failed else "")


async def _invoke(thread_id: str, payload: Any) -> Any:
    if _dev_runner is None:
        raise RuntimeError("开发图 runner 尚未注入（应在 lifespan 里 set_dev_runner）")
    try:
        result = await _dev_runner(thread_id, payload)
    except Exception as exc:  # noqa: BLE001
        session_id = ops_session_id(thread_id)
        message = f"{type(exc).__name__}: {exc}"
        await asyncio.to_thread(_finish_ops_run, session_id, "ERROR", "", message)
        await _broadcast(session_id, thread_id, "error", message=message)
        raise
    await _after_turn(thread_id, result)
    return result


async def run_dev_turn(thread_id: str, payload: Any) -> Any:
    """开发图的一次调用（首发 / resume）：串行 + 占用标记 + 收尾写回 AgentRun"""
    async with _lock_for(thread_id):
        await _wait_idle(thread_id)
        background_broker.set_active(thread_id)
        try:
            result = await _invoke(thread_id, payload)
        except Exception:
            background_broker.set_idle(thread_id)
            raise
        interrupted = isinstance(result, dict) and bool(result.get("__interrupt__"))
        if interrupted:
            background_broker.set_paused(thread_id)
        else:
            background_broker.set_idle(thread_id)
        return result


async def run_ops_reflow(thread_id: str, payload: Any) -> Any:
    """感知层拉起后台续跑（深层测试结果回流）：占用由 perceptron 管理，这里只收尾/广播"""
    return await _invoke(thread_id, payload)


def _on_task_done(task: asyncio.Task) -> None:
    _tasks.discard(task)
    if not task.cancelled() and task.exception() is not None:
        logger.error(f"转交开发任务异常: {task.exception()}")


async def start_handover(*, session_id: str, handover: OpsHandoverState) -> str:
    """app_handover 调用：先挂住运行记录/租约，再后台起开发图"""
    thread_id = dev_thread_id(session_id)
    await asyncio.to_thread(_hold_ops_run, session_id, handover["project_name"])
    input_state = DevelopAgentInputState(
        input_msg=_handover_message(handover),
        is_ops=True,
        ops_handover=handover,
    )
    task = asyncio.create_task(run_dev_turn(thread_id, input_state), name=f"Handover-{session_id}")
    _tasks.add(task)
    task.add_done_callback(_on_task_done)
    await _broadcast(session_id, thread_id, "started",
                     app_name=handover["app_name"], project_name=handover["project_name"])
    return thread_id


async def resume_handover(session_id: str, answer: Any) -> Any:
    thread_id = dev_thread_id(session_id)
    return await run_dev_turn(thread_id, Command(resume=answer))
