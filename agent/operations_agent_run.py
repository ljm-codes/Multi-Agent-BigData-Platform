"""
运维 Agent 代码

假设有n个运维Agent(单个Agent并发处理 -> 伪多运维Agent),
初始时，每个Agent只能获取一个应用的ID，并将 记录对应的 “占用” 信息
然后，会从postgresql中获取该应用的全部错误信息（按check_time升序），对Agent进行串行invoke
当Agent处理完成一个应用的ID后，“占用信息” 会自动释放
"""
import asyncio
import time
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
from uuid import uuid4

from sqlalchemy import func, select, and_
from sqlalchemy.dialects.postgresql import insert

from SPO.operations_agent_state_model import OperationsAndReconcileInputState, OperationsAndReconcileContext, \
    OperationsAndReconcileCheckInfo
from SPO.postgresql_model import app_lease, CheckLog
from Tools.log_settings import LogSetting
from Tools.alarm_sender import send_warning_msg
from Tools.db import SessionLocal
from Tools.share_check_tool import redis_con
from agent._operations_agent import operations_agent
from load_config.config import config

n_workers = config.get("agent", {}).get("OM", {}).get("n_workers", 3)
key = config.get("hadoop", {}).get("checker", {}).get("error_queue", {}).get("error_app_redis_key", "error_app_queue")
executor = ThreadPoolExecutor(max_workers=n_workers)
app_to_future = {}  # 应用ID -> Future对象
future_to_app = {}  # Future对象 -> 应用ID
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
        return _zpopmin(keys=[key])
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


async def worker_handle(aid: str, sid: str):
    """
    运维Agent 处理函数 -> 也是图的调用方
    :param aid: 需要处理的应用ID
    :param sid: 会话ID
    :return:
    """
    try:
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

        # 调用运维Agent
        result = await operations_agent.ainvoke(
            OperationsAndReconcileInputState(error_list=error_list, is_cluster=is_cluster, app_id=aid,
                                             app_type=app_type, ), config={'configurable': {'thread_id': sid}},
            context=OperationsAndReconcileContext(app_id=aid, session_id=sid))
        if result.get("type", "") == "error":
            send_warning_msg(app_id=aid, description=f"程序运行出错了：{result.get('out_msg'), ''}")

    except Exception as e:
        logger.error(f"在尝试解决应用 {aid} 中的错误信息时出错 {type(e).__name__}: {e}")
        return None


def reconcile_from_db():
    pass


def run_async(coro_func, *args, **kwargs):
    return asyncio.run(coro_func(*args, **kwargs))


def operations_main():
    last_reconcile = time.time()
    while RUNNING:
        # 当有空闲的Agent时，从redis中获取一个应用中的错误应用ID
        while len(app_to_future) < n_workers:
            app_id = get_error_app_id()
            # 防止出现app_id为0的情况（虽然不可能）
            if app_id is None:
                break
            session_id = str(uuid4())
            if not try_lock(app_id, session_id):
                continue

            # 开始分配任务：串行invoke
            future = executor.submit(run_async, worker_handle, app_id, session_id)
            app_to_future[app_id] = future
            future_to_app[future] = app_id

        if app_to_future:
            done, _ = wait(app_to_future.values(), return_when=FIRST_COMPLETED)
            for f in done:
                app_id = future_to_app.pop(f, None)
                if app_id is not None:
                    app_to_future.pop(app_id, None)
        else:
            # 没有任务可处理，等待3秒
            time.sleep(3)

        # 周期对账
        if time.time() - last_reconcile > 45:
            reconcile_from_db()
            last_reconcile = time.time()


if __name__ == '__main__':
    # test 专用
    operations_main()
