"""
汇总所有检测工具的消息 将消息进行处理放入一个专门的队列中
首先：
将所有关于spark/flink的checker消息汇总、处理后的结果：
并将下面的结果暂时存入 “队列” 中（这里的队列是postgresql（存放真实数据 -> 一行一个队列数据） + Redis（通知）的伪队列）
[{
    "app_id": "app_id",
    "app_type": "SPARK/FLINK",
    "describe": "表述",
    "is_solved": "是否解决",

    "check_type": "check_type",
    "error_type": "error/warning",
    "check_data": object,

    "checker_error": "checker本身的错误信息",
    "check_time": "check_time",
},
{...},
...
]

> 考虑到Agent并发获取消息，因此采用一个Agent获取一个应用一种消息后，
> 将锁住该应用该类型的其他消息，直到该消息处理完成

redis -> 不做存储，而是只是作为“通知”，只放(set) app_id (去查)
之后的运维Agent以 min(len(redis), n) 个进行并发分发给 Agent
n -> 运维Agent的最大数
len(redis) -> 放入Redis中的应用错误数量

阈值与报警(按postgreSql库中的数据为准)：
在yaml中按数据量(不分类型)设置 低告警 中告警 高告警
"""
import hashlib
import time
from datetime import datetime, timezone, timedelta
from typing import List

from sqlalchemy import select, func

from SPO.check_result import CheckOutcome
from SPO.postgresql_model import CheckLog
from Tools import LogSetting
from Tools.db import SessionLocal
from Tools.alarm_sender import send_error_email
from Tools.scheduler import run_cycle
from Tools.share_check_tool import redis_con
from load_config.config import config

logger = LogSetting.create(__name__)

queue_config = config.get("hadoop", {}).get("checker", {}).get("error_queue", {})
yarn_config = config.get("hadoop", {}).get("yarn", {})
mode = queue_config.get("mode", "number")
POLL_INTERVAL = yarn_config.get("poll_interval", 30)
key = queue_config.get("error_app_redis_key", "error_app_queue")
if mode not in ["number", "time"]:
    raise ValueError(f"error_queue mode 必须为 number 或 time，当前为 {mode}")
_value = queue_config.get("value", [])
if not _value or len(_value) != 3:
    raise ValueError(f"error_queue value 必须为3个阈值，当前为 {_value}")
mode_value_list = []
try:
    mode_value_list = [int(v) for v in _value]
except ValueError:
    raise ValueError(f"value 必须为3个整数，当前为 {_value}")

# Lua
_DEDUPLICATE_AND_INSERT = """
-- KEYS[1] = zadd key
-- KEYS[2] = 自增序号 key
-- ARGV    = 成员列表
local added = 0
for i = 1, #ARGV do
    local member = ARGV[i]
    if not redis.call('ZSCORE', KEYS[1], member) then
        local seq = redis.call('INCR', KEYS[2])
        redis.call('ZADD', KEYS[1], seq, member)
        added = added + 1
    end
end
return added
"""

_add_ordered = redis_con.register_script(_DEDUPLICATE_AND_INSERT)


def make_error_key(app_id: str, app_type: str,
                   check_type: str, *, error_type: str = "", describe: str = ""):
    parts = app_id.strip() + app_type.strip() + check_type.strip() + error_type.strip() + describe.strip()
    return hashlib.md5(parts.encode("utf-8")).hexdigest()


def add_error_app_infos(error_app_infos: List[str]):
    if not error_app_infos:
        return 0
    return _add_ordered(keys=[key, f"{key}:seq"], args=error_app_infos)


def check_unique_key(data, session: SessionLocal, error_key: str, checker_error: bool) -> bool:
    """
    检查 error_key 是否已经在 postgresql 中不存在
    若存在，则对 repeat_count 字段进行 +1 操作，并返回False，反之返回True
    :param data: 检查数据
    :param session: 数据库会话
    :param error_key: 错误键
    :param checker_error: 检查者本身是否出错
    :return: bool
    """
    # 只看 PENDING: 处理完(DONE/...)的老行不该挡住复发重新入队
    # (表上的唯一索引 uk_checker_log_dedup_pending 也是 where status='PENDING')
    log = session.query(CheckLog).filter(CheckLog.dedup_key == error_key,
                                         CheckLog.status == "PENDING").first()
    if not log:
        return True
    # 存在，更新 repeat_count 字段
    update_data = {
        "repeat_count": log.repeat_count + 1,
        "update_time": datetime.now(timezone.utc),
    }
    if checker_error:
        update_data['checker_error'] = data
    else:
        update_data['check_data'] = data
    session.query(CheckLog).filter(CheckLog.dedup_key == error_key,
                                   CheckLog.status == "PENDING").update(update_data)
    # 不统一提交事务，由调用方负责提交
    return False


def _enqueue_one(session: SessionLocal, outcome: CheckOutcome):
    """
    把一条 outcome 写进队列（clean 不会走到这里）

    只管写，不提交也不回滚 —— 交给调用方按 savepoint 兜
    """
    check_result = outcome.result
    # 集群级检查没有应用类型：落库要是 NULL 不能是空串（表上有 check 约束）
    app_type = outcome.app_type or None
    # 出现 检查者 本身出错时
    if outcome.status == "invalid" and outcome.error:
        error_key = make_error_key(outcome.app_id, outcome.app_type, outcome.label, describe=outcome.error)
        if not check_unique_key(outcome.error, session, error_key, True):
            return
        session.add(CheckLog(
            app_id=outcome.app_id,
            app_type=app_type,
            checker_error=outcome.error,
            check_type=outcome.label,
            dedup_key=error_key,
            status="PENDING",
            is_solved=False,
            repeat_count=1,
            create_time=datetime.now(timezone.utc),
            update_time=datetime.now(timezone.utc),
            check_time=outcome.error_time,
        ))
    # 当 检查者 检查到错误时
    if outcome.status == "found" and check_result:
        check_data = check_result.data
        check_desc = check_result.describe
        check_type = check_result.type

        check_key = make_error_key(outcome.app_id, outcome.app_type,
                                   outcome.label, error_type=check_type,
                                   describe=check_desc)
        if not check_unique_key(check_data, session, check_key, False):
            return

        check_time = check_result.check_time or datetime.now(timezone.utc)
        session.add(CheckLog(
            app_id=outcome.app_id,
            app_type=app_type,
            check_type=outcome.label,
            dedup_key=check_key,
            error_type=check_type,
            check_describe=check_desc,
            check_data=check_data,
            status="PENDING",
            is_solved=False,
            repeat_count=1,
            create_time=datetime.now(timezone.utc),
            update_time=datetime.now(timezone.utc),
            check_time=check_time,
        ))


def check_message_handle(outcomes: List[CheckOutcome]):
    if not outcomes:
        return None
    with SessionLocal() as session:
        for outcome in outcomes:
            if outcome.status == "clean":
                continue
            if not outcome.result and not outcome.error:
                logger.warning(f"不允许出现 check_result 为空，且 error 也为空")
                continue
            if outcome.result and outcome.error:
                logger.warning(f"不允许check_result 与 error 同时存在")
                continue
            try:
                # 一条消息一个 savepoint：写不进去的只丢它自己，同批其它消息照常入队
                with session.begin_nested():
                    _enqueue_one(session, outcome)

                    session.flush()
            except Exception as e:
                logger.error(f"丢弃一条消息 app={outcome.app_id!r} {outcome.label}: "
                             f"{type(e).__name__}: {e}")
        try:
            # 成功时提交事务
            session.commit()
        except Exception as e:
            # 只剩"提交本身"失败（连接断了等），这时整批确实保不住
            logger.error(f"提交失败，本批 {len(outcomes)} 条消息全部丢弃: {type(e).__name__}: {e}")
            session.rollback()


def release_notice_to_redis():
    # 由于在 postgreSQL 中的数据存在 一个应用 多个错误的情况，那么此时采用 分组查询+id 排序，确保每个应用只发送一次通知
    try:
        stmt = (select(
            CheckLog.app_id
        ).where(
            CheckLog.status == "PENDING"
        ).distinct(CheckLog.app_id).order_by(CheckLog.app_id, CheckLog.check_time.asc()))
        error_app_infos = []
        with SessionLocal() as session:
            results = session.execute(stmt).all()

            if not results:
                return None

            for result in results:
                app_id = result[0]
                # 发送通知到 redis
                error_app_infos.append(str(app_id))

        # 减少反复操作 redis，提高效率
        add_error_app_infos(error_app_infos)
    except Exception as e:
        logger.error(f"在 发送错误应用通知 时，出现了异常{type(e).__name__}: {e}")


def get_db_size():
    try:
        with SessionLocal() as session:
            stmt = select(func.count()).select_from(CheckLog).where(CheckLog.status == "PENDING")
            return session.execute(stmt).scalar()
    except Exception as e:
        logger.error(f"在 获取数据库大小 时，出现了异常{type(e).__name__}: {e}")
        return None


def get_db_earliest_time():
    try:
        with SessionLocal() as session:
            stmt = select(func.min(CheckLog.check_time).label("check_time")).where(CheckLog.status == "PENDING")
            return session.execute(stmt).scalar()
    except Exception as e:
        logger.error(f"在 获取数据库最早时间 时，出现了异常{type(e).__name__}: {e}")
        return None


_MAIL_DAILY_LIMIT = 3  # 邮件一天最多 3 封
_last_level = 0  # 上一轮的档位，0=没到阈值 1/2/3=低/中/高；用来判断"这轮是不是刚涨上来"


def _send_alarm_mail(subject: str, content: str):
    """发告警邮件，受当日上限约束（只计发成功的那些）"""
    counter = f"{key}:mail:{datetime.now(timezone.utc):%Y%m%d}"
    if int(redis_con.get(counter) or 0) >= _MAIL_DAILY_LIMIT:
        logger.warning(f"今日告警邮件已达上限 {_MAIL_DAILY_LIMIT} 封，不再发送: {subject}")
        return
    if send_error_email(subject=subject, content=content):
        redis_con.incr(counter)
        redis_con.expire(counter, 2 * 24 * 3600)  # 过期交给 redis，不用自己清


def check_queue_situation():
    global _last_level
    level, alarm = 0, ""  # 本轮档位 + 该档位的告警文案
    if mode == "number":
        db_size = get_db_size()
        if db_size is None:  # 取不到 ≠ 恢复正常，这轮不判断
            return
        if not db_size:
            _last_level = 0  # 队列空了 = 恢复
            return
        if mode_value_list[0] <= db_size < mode_value_list[1]:
            # 这里是低阈值
            level = 1
            logger.warning(
                f"当前 redis 队列的 应用错误数量已经超过 低阈值 {mode_value_list[0]}，当前数量为 {db_size}")
        elif mode_value_list[1] <= db_size < mode_value_list[2]:
            # 这里是中阈值
            level = 2
            logger.error(f"当前 redis 队列的 应用错误数量已经超过 中阈值 {mode_value_list[1]}，当前数量为 {db_size}")
        elif db_size >= mode_value_list[2]:
            # 这里是高阈值
            level = 3
            alarm = f"当前 redis 队列的 应用错误数量已经超过 高阈值 {mode_value_list[2]}，当前数量为 {db_size}"
            logger.critical(alarm)
    if mode == "time":
        now_time = datetime.now(timezone.utc)
        earliest_time = get_db_earliest_time()
        if not earliest_time:
            # 队列空 -> min() 也是 None（取不到数的故障也走这里，有日上限兜着）
            _last_level = 0
            return
        if timedelta(seconds=mode_value_list[0]) <= now_time - earliest_time < timedelta(seconds=mode_value_list[1]):
            # 这里是低阈值
            level = 1
            logger.warning(
                f"当前 redis 队列的 最早的应用错误时间已经超过 低阈值 {mode_value_list[0]}，当前时间为 {earliest_time}")
        elif timedelta(seconds=mode_value_list[1]) <= now_time - earliest_time < timedelta(seconds=mode_value_list[2]):
            # 这里是中阈值
            level = 2
            logger.error(
                f"当前 redis 队列的 最早的应用错误时间已经超过 中阈值 {mode_value_list[1]}，当前时间为 {earliest_time}")
        elif now_time - earliest_time >= timedelta(seconds=mode_value_list[2]):
            # 这里是高阈值
            level = 3
            alarm = f"当前 redis 队列的 最早的应用错误时间已经超过 高阈值 {mode_value_list[2]}，当前时间为 {earliest_time}"
            logger.critical(alarm)

    # 只在"刚涨进高阈值"那一轮发信：一直待在高阈值不重复发；掉下来再涨上去算新的一次，
    # 但当日上限兜着（阈值线上来回抖也不会连发）
    if level == 3 and _last_level != 3:
        _send_alarm_mail(subject="高阈值触发", content=alarm)
    _last_level = level


def main_check():
    while True:
        try:
            # 进行检测
            outcomes = run_cycle()
            # 处理检测结果
            check_message_handle(outcomes)
            # 将 postgreSQL 中已经存入的错误信息的应用放入 redis 通知中
            release_notice_to_redis()
            # 检查队列情况
            check_queue_situation()
        except Exception as e:
            logger.error(f"本轮调度异常: {type(e).__name__}: {e}")

        time.sleep(POLL_INTERVAL)
