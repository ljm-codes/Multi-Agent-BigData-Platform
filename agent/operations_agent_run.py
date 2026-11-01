"""
运维 Agent 代码

假设有n个运维Agent(单个Agent并发处理 -> 伪多运维Agent),
初始时，每个Agent只能获取一个应用的ID，并将 记录对应的 “占用” 信息
然后，会从postgresql中获取该应用的全部错误信息（按check_time升序），对Agent进行串行invoke
当Agent处理完成一个应用的ID后，“占用信息” 会自动释放

⚠️ 图必须在主事件循环上跑：
    ops-Agent 的 checkpointer 是 AsyncPostgresSaver，连接绑定创建它的事件循环。
    旧实现用线程池 + asyncio.run（每个 worker 一个新 loop），会撞
    "attached to a different loop"。所以这里用 asyncio.create_task 调度，
    阻塞的 DB / Redis / 邮件部分丢给 asyncio.to_thread。
"""
import asyncio
import time
from datetime import datetime, timedelta, timezone
from uuid import uuid4

from sqlalchemy import func, select, and_, delete, update
from sqlalchemy.dialects.postgresql import insert

from SPO.operations_agent_state_model import OperationsAndReconcileInputState, OperationsAndReconcileContext, \
    OperationsAndReconcileCheckInfo
from SPO.postgresql_model import AgentRun, app_lease, CheckLog
from Tools.log_settings import LogSetting
from Tools.alarm_sender import send_warning_msg
from Tools.db import SessionLocal
from Tools.share_check_tool import redis_con
from agent._operations_agent import get_ops_agent
from load_config.config import config

n_workers = config.get("agent", {}).get("OM", {}).get("n_workers", 3)
key = config.get("hadoop", {}).get("checker", {}).get("error_queue", {}).get("error_app_redis_key", "error_app_queue")
RUNNING = True

logger = LogSetting.create(__name__)

# 本机上redis < 5.0版本，不支持 zpopmin 命令
# 这里使用Lua脚本时限类似 zpopmin 的功能
# Lua
_ZPOPMIN_LUA = """
-- KEYS[1] 是有序集合的键名
local key = KEYS[1]
-- 获取分数最小的第一个成员
local result = redis.call('ZRANGE', key, 0, 0)
local member = result[1]
if member then
    -- 如果存在，则删除该成员
    redis.call('ZREM', key, member)
    return member
else
    return nil
end
"""
_zpopmin = redis_con.register_script(_ZPOPMIN_LUA)


def get_error_app_id():
    """
    判断 Redis 的 指定集合 是否有元素
    """
    try:
        value = _zpopmin(keys=[key])
        if value is None:
            return None
        return value.decode("utf-8") if isinstance(value, bytes) else str(value)
    except Exception as e:
        logger.error(f"在尝试从Redis中获取错误应用ID时出错 {type(e).__name__}: {e}")
        return None


def try_lock(aid: str, sid: str):
    """
    尝试获取应用的占用锁
    """
    with SessionLocal() as session:
        try:
            stmt = insert(app_lease).values(
                app_id=aid, session_id=sid,
                acquired_at=func.now(), heartbeat_at=func.now()
            )
            stmt = stmt.on_conflict_do_update(
                index_elements=[app_lease.c.app_id],
                set_={
                    "session_id": stmt.excluded.session_id,
                    "acquired_at": func.now(),
                    "heartbeat_at": func.now(),
                },
            ).returning(app_lease.c.app_id)
            result = session.execute(stmt)
            row = result.first()
            session.commit()
            return row is not None
        except Exception as e:
            logger.error(f"在尝试添加应用占用锁时出错 {type(e).__name__}: {e}")
            return False


def _release_lease(aid: str, sid: str):
    """worker 兜底解锁：图没走到 chat_end（异常/中断）也不能把 app 永久占住"""
    try:
        with SessionLocal() as session:
            session.execute(
                delete(app_lease).where(
                    and_(
                        app_lease.c.app_id == aid,
                        app_lease.c.session_id == sid,
                    )
                )
            )
            session.commit()
    except Exception as e:  # noqa: BLE001
        logger.error(f"兜底释放应用占用锁失败 {aid}: {type(e).__name__}: {e}")


def _start_agent_run(aid: str, sid: str, app_type: str, error_count: int):
    try:
        with SessionLocal() as session:
            session.execute(
                insert(AgentRun).values(
                    app_id=aid,
                    session_id=sid,
                    app_type=app_type or None,
                    status="RUNNING",
                    error_count=error_count,
                    started_at=func.now(),
                ).on_conflict_do_update(
                    index_elements=[AgentRun.__table__.c.session_id],
                    set_={
                        "app_id": aid,
                        "app_type": app_type or None,
                        "status": "RUNNING",
                        "error_count": error_count,
                        "started_at": func.now(),
                        "finished_at": None,
                        "out_msg": None,
                        "error": None,
                    },
                )
            )
            session.commit()
    except Exception as e:  # noqa: BLE001
        logger.error(f"写入 Agent 运行记录失败 {aid}: {type(e).__name__}: {e}")


def _finish_agent_run(sid: str, status: str, out_msg: str = "", error: str = ""):
    try:
        with SessionLocal() as session:
            session.execute(
                update(AgentRun).where(AgentRun.session_id == sid).values(
                    status=status,
                    finished_at=func.now(),
                    out_msg=out_msg or "",
                    error=error or "",
                )
            )
            session.commit()
    except Exception as e:  # noqa: BLE001
        logger.error(f"更新 Agent 运行记录失败 {sid}: {type(e).__name__}: {e}")


def _load_error_info(aid: str):
    """同步读取 PENDING 错误（阻塞 DB，调用方用 asyncio.to_thread 包起来）"""
    app_error_info = []
    app_type = None
    with SessionLocal() as session:
        stmt = select(
            CheckLog.id,
            CheckLog.app_type, CheckLog.checker_error,
            CheckLog.check_type, CheckLog.error_type,
            CheckLog.check_describe, CheckLog.repeat_count
        ).where(
            and_(
                CheckLog.app_id == aid,
                CheckLog.status == "PENDING",
                CheckLog.is_solved.is_(False)
            )
        ).order_by(CheckLog.check_time, CheckLog.repeat_count.desc(), CheckLog.id)
        result = session.execute(stmt)
        rows = result.fetchall()
        for row in rows or []:
            app_error_info.append({
                "id": row[0],
                "app_type": row[1],

                "checker_error": row[2],

                "check_type": row[3],
                "error_type": row[4],
                "check_describe": row[5],

                "repeat_count": row[6],
            })

        stmt = select(CheckLog.app_type).where(
            and_(
                CheckLog.app_id == aid,
                CheckLog.status == "PENDING",
                CheckLog.is_solved.is_(False)
            )
        ).group_by(CheckLog.app_type)
        result = session.execute(stmt).first()
        if result:
            app_type = result[0]
    return app_error_info, app_type


async def worker_handle(aid: str, sid: str):
    """
    运维Agent 处理函数 -> 也是图的调用方
    :param aid: 需要处理的应用ID
    :param sid: 会话ID
    :return:
    """
    run_finished = False
    try:
        app_error_info, app_type = await asyncio.to_thread(_load_error_info, aid)
        app_type = app_type or "CLUSTER"
        error_list = []
        is_cluster = True if app_type == "CLUSTER" else False
        for error_info in app_error_info:
            if error_info["checker_error"]:
                # 出现了检查者本身错误，跳过
                continue
            # 其他错误，继续处理
            error_list.append(OperationsAndReconcileCheckInfo(
                id=error_info["id"],
                error_describe=error_info["check_describe"],
                check_type=error_info["check_type"],
                error_type=error_info["error_type"],
                repeat_count=error_info["repeat_count"],
            ))

        await asyncio.to_thread(_start_agent_run, aid, sid, app_type, len(error_list))

        # 调用运维Agent（必须在主事件循环里 await）
        operations_agent = get_ops_agent()
        result = await operations_agent.ainvoke(
            OperationsAndReconcileInputState(error_list=error_list, is_cluster=is_cluster, app_id=aid,
                                             app_type=app_type, ), config={'configurable': {'thread_id': sid}},
            context=OperationsAndReconcileContext(app_id=aid, session_id=sid))
        out_msg = result.get("out_msg", "") if isinstance(result, dict) else ""
        if result.get("type", "") == "error":
            await asyncio.to_thread(_finish_agent_run, sid, "ERROR", "", out_msg)
            run_finished = True
            await asyncio.to_thread(
                send_warning_msg, app_id=aid,
                description=f"程序运行出错了：{out_msg}",
                error_id=None
            )
        else:
            await asyncio.to_thread(_finish_agent_run, sid, "DONE", out_msg, "")
            run_finished = True

    except Exception as e:
        if not run_finished:
            await asyncio.to_thread(_finish_agent_run, sid, "ERROR", "", f"{type(e).__name__}: {e}")
            run_finished = True
        logger.error(f"在尝试解决应用 {aid} 中的错误信息时出错 {type(e).__name__}: {e}")
        return None
    finally:
        # 取消/强杀路径也要留一条可查记录；chat_end 正常跑过时会先删锁
        if not run_finished:
            _finish_agent_run(sid, "ABANDONED", "", "worker cancelled or terminated")
        _release_lease(aid, sid)


def reconcile_from_db():
    """清理过期租约；把 worker 崩溃留下的 RUNNING run 标成 ABANDONED"""
    try:
        cutoff = datetime.now(timezone.utc) - timedelta(minutes=10)
        with SessionLocal() as session:
            result = session.execute(
                delete(app_lease).where(app_lease.c.heartbeat_at < cutoff)
            )
            abandoned = session.execute(
                update(AgentRun).where(
                    AgentRun.status == "RUNNING",
                    AgentRun.started_at < cutoff,
                ).values(
                    status="ABANDONED",
                    finished_at=func.now(),
                    error="worker timeout/abandoned",
                )
            )
            session.commit()
            if result.rowcount:
                logger.warning(f"清理了 {result.rowcount} 条过期应用占用锁")
            if abandoned.rowcount:
                logger.warning(f"标记了 {abandoned.rowcount} 条超时 Agent 运行为 ABANDONED")
    except Exception as e:  # noqa: BLE001
        logger.error(f"对账清理过期占用锁失败: {type(e).__name__}: {e}")


async def operations_main():
    get_ops_agent()  # 启动时显式检查，避免每个任务都白跑
    last_reconcile = time.time()
    tasks: set[asyncio.Task] = set()
    try:
        while RUNNING:
            # 当有空闲的Agent时，从redis中获取一个应用中的错误应用ID
            while len(tasks) < n_workers:
                app_id = await asyncio.to_thread(get_error_app_id)
                # 防止出现app_id为0的情况（虽然不可能）
                if app_id is None:
                    break
                session_id = str(uuid4())
                if not await asyncio.to_thread(try_lock, app_id, session_id):
                    continue

                # 开始分配任务：同一事件循环内的 task，图调用不会跨 loop
                tasks.add(asyncio.create_task(
                    worker_handle(app_id, session_id), name=f"Ops-{app_id}"))

            if tasks:
                done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                for task in done:
                    if task.cancelled():
                        continue
                    exc = task.exception()
                    if exc is not None:
                        logger.error(f"运维任务异常: {type(exc).__name__}: {exc}")
                tasks = pending
            else:
                # 没有任务可处理，等待3秒
                await asyncio.sleep(3)

            # 周期对账
            if time.time() - last_reconcile > 45:
                await asyncio.to_thread(reconcile_from_db)
                last_reconcile = time.time()
    finally:
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)


if __name__ == '__main__':
    # test 专用
    asyncio.run(operations_main())
