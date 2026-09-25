"""
验证 Tools/main_check_tool.py（入队层）—— 离线可跑：不需要 VM / YARN，需要本机 PostgreSQL + Redis

覆盖:
  [1] 配置解析（mode / 三档阈值 / 轮询间隔 / redis key）
  [2] clean 不入库
  [3] found / invalid 落库的形状
  [4] 同一个错误重复出现 -> repeat_count+1, 不新增行
  [5] 集群级检查 (app_id="" / app_type="") 落库
  [6] 一批里混进一条坏 outcome, 好的会不会被连累
  [7] describe 里不能带会漂移的测量值（直接跑真 checker 验）
  [8] redis 通知：按应用去重 / 重复调用 / 行处理完后通知还在不在
  [9] number / time 两种阈值告警（含邮件分支, 打桩不发真邮件）
  [10] main_check 主循环：run_cycle 抛异常后还能不能跑下一轮

打的一个桩（不打则一行都跑不起来）:
  * yarn_api_client.ResourceManager —— share_check_tool 在模块级构造它并主动探测 RM,
    VM 关着 => "No active RMs found" => 任何 Tools.* 都 import 不了。

落库走的是**真的 db.py**（连接方式/驱动和生产一致），只是把 config 里的库名换成临时库
`mabp_verify`（schema 从开发库 pg_dump 过来），跑完删掉，不碰开发库。

用法: python tests/verify_main_check_tool.py [--keep]
"""
import argparse
import logging
import pathlib
import sys
import types
from datetime import datetime, timezone, timedelta

# ---- 桩 1: 必须在任何 Tools.* 之前 ----
import yarn_api_client
import yarn_api_client.resource_manager as _rm_mod


class _OfflineRM:
    def __init__(self, *args, **kwargs):
        pass


yarn_api_client.ResourceManager = _OfflineRM
_rm_mod.ResourceManager = _OfflineRM

PROJECT_ROOT = pathlib.Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

import psycopg
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from load_config.config import config

PG = config["databases"]["postgres"]
SRC_DB = PG["db"].lower()  # PostgreSQL 把未加引号的标识符折叠成小写, 建库时就是这么建的
TEST_DB = "mabp_verify"

PASS, FAIL = "✅", "❌"
_results = []
_defects = []
_notes = []


def check(name: str, ok: bool, detail: str = "", defect: str = ""):
    _results.append((name, ok))
    print(f"  {PASS if ok else FAIL} {name}")
    if detail:
        print(f"      {detail}")
    if not ok and defect:
        _defects.append(f"{name} —— {defect}")


def note(name: str, detail: str = ""):
    """留给用户定夺的观察, 不计入通过率"""
    _notes.append(f"{name} —— {detail}" if detail else name)
    print(f"  ⚠️  {name}")
    if detail:
        print(f"      {detail}")


def section(title: str):
    print(f"\n{title}")


# ---------------------------------------------------------------- 环境准备
def pg_conn(dbname: str):
    return psycopg.connect(host=PG["host"], port=PG["port"], user=PG["user"],
                           password=PG["password"], dbname=dbname, connect_timeout=5)


def _find_pg_dump() -> str | None:
    import shutil
    found = shutil.which("pg_dump")
    if found:
        return found
    for root in (r"D:\PostgreSQL", r"C:\Program Files\PostgreSQL"):
        for path in pathlib.Path(root).glob("*/bin/pg_dump.exe"):
            return str(path)
    return None


def setup_test_db():
    """
    建一个空的临时库, 再把开发库的 **schema** 克隆过去

    不用 `CREATE DATABASE ... TEMPLATE`: 开发库上有别的连接(实测 2 个), TEMPLATE 会
    报 "source database is being accessed by other users" —— 那是别人的会话, 不去踢。
    """
    with pg_conn("postgres") as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(f"DROP DATABASE IF EXISTS {TEST_DB} WITH (FORCE)")
            cur.execute(f"CREATE DATABASE {TEST_DB}")

    import os
    import subprocess
    pg_dump = _find_pg_dump()
    env = {**os.environ, "PGPASSWORD": PG["password"]}
    dump = subprocess.run(
        [pg_dump, "-h", PG["host"], "-p", str(PG["port"]), "-U", PG["user"],
         "--schema-only", "--no-owner", "--no-privileges", SRC_DB],
        capture_output=True, env=env,
    )
    if dump.returncode != 0:
        raise RuntimeError(f"pg_dump 失败: {dump.stderr.decode('utf-8', 'replace')}")
    # pg_dump 17+ 会在头尾插 \restrict / \unrestrict 这类 psql 元命令, 直接丢给
    # psycopg 会 "syntax error at or near \\", 这里按行滤掉
    sql = "\n".join(line for line in dump.stdout.decode("utf-8").splitlines()
                    if not line.startswith("\\"))
    with pg_conn(TEST_DB) as conn:
        conn.execute(sql)


setup_test_db()
# Tools.db 在 import 时就按 config 建 engine —— 先把库名换成临时库, 再 import,
# 这样走的是**真的** db.py(连接方式/驱动都按生产的来), 只是数据不落在开发库上
config["databases"]["postgres"]["db"] = TEST_DB

import Tools.main_check_tool as mct  # noqa: E402  (RM 桩就位后才能 import)
from SPO.check_result import CheckOutcome, CheckResult  # noqa: E402
from Tools.db import SessionLocal as SESSION, engine as ENGINE  # noqa: E402
from Tools.share_check_tool import redis_con  # noqa: E402


def reset_state():
    with SESSION() as s:
        s.execute(text("truncate table log.checker_log"))
        s.commit()
    redis_con.delete(mct.key, f"{mct.key}:seq")
    for k in redis_con.keys(f"{mct.key}:mail:*"):  # 告警邮件的当日计数
        redis_con.delete(k)
    mct._last_level = 0


def rows():
    """(app_id, app_type, check_type, error_type, check_describe, repeat_count, status)"""
    with SESSION() as s:
        return s.execute(text(
            "select app_id, app_type, check_type, error_type, check_describe, repeat_count, status "
            "from log.checker_log order by id"
        )).all()


def seed_pending(n: int, app_prefix: str = "app", age_seconds: int = 0):
    """灌 n 条 PENDING, check_time 递减(第 1 条最老)"""
    with SESSION() as s:
        s.execute(text(
            "insert into log.checker_log "
            "(app_id, app_type, dedup_key, status, check_type, error_type, check_describe, "
            " check_time, create_time, update_time) "
            "select :p || g, 'SPARK', md5(g::text || :p), 'PENDING', '造数', 'error', '', "
            "       now() - ((g + :age) || ' seconds')::interval, now(), now() "
            "from generate_series(1, :n) g"
        ), {"n": n, "p": app_prefix, "age": age_seconds})
        s.commit()


class _Capture(logging.Handler):
    def __init__(self):
        super().__init__()
        self.records = []

    def emit(self, record):
        self.records.append(record)


def capture_logs(fn):
    handler = _Capture()
    mct.logger.addHandler(handler)
    try:
        fn()
    finally:
        mct.logger.removeHandler(handler)
    return handler.records


# ---------------------------------------------------------------- outcome 构造
def found(app_id: str, label: str, describe: str, app_type: str = "SPARK",
          error_type: str = "error", data=None) -> CheckOutcome:
    return CheckOutcome(
        label=label, app_id=app_id, app_type=app_type, status="found",
        result=CheckResult(type=error_type, describe=describe,
                           check_time=datetime.now(timezone.utc),
                           data=data if data is not None else {"k": "v"}),
    )


def invalid(app_id: str, label: str, error: str, app_type: str = "SPARK") -> CheckOutcome:
    return CheckOutcome(label=label, app_id=app_id, app_type=app_type, status="invalid",
                        error=error, error_time=datetime.now(timezone.utc))


def clean(app_id: str, label: str, app_type: str = "SPARK") -> CheckOutcome:
    return CheckOutcome(label=label, app_id=app_id, app_type=app_type, status="clean")


def brief(e) -> str:
    """SQLAlchemy 的异常信息带整条 SQL + 全部参数, 打印时只留第一行"""
    return str(e).splitlines()[0][:200] if e else ""


# ---------------------------------------------------------------- 用例
def t1_config():
    section("[1] 配置解析")
    check("mode 解析为 number", mct.mode == "number", f"mode={mct.mode!r}")
    check("三档阈值解析为整数列表", mct.mode_value_list == [50, 100, 150], f"{mct.mode_value_list}")
    check("轮询间隔取自 hadoop.yarn.poll_interval", mct.POLL_INTERVAL == 30, f"{mct.POLL_INTERVAL}")
    check("redis key 取自 error_queue.error_app_redis_key", mct.key == "error_app_queue", f"{mct.key!r}")

    check("make_error_key 对字段变化敏感",
          len({mct.make_error_key("a", "SPARK", "倾斜", error_type="error", describe="d"),
               mct.make_error_key("b", "SPARK", "倾斜", error_type="error", describe="d"),
               mct.make_error_key("a", "FLINK", "倾斜", error_type="error", describe="d"),
               mct.make_error_key("a", "SPARK", "OOM", error_type="error", describe="d"),
               mct.make_error_key("a", "SPARK", "倾斜", error_type="warning", describe="d"),
               mct.make_error_key("a", "SPARK", "倾斜", error_type="error", describe="d2")}) == 6,
          "6 个字段任一不同都应得到不同的 key")


def t2_clean():
    section("[2] clean 不入库")
    reset_state()
    records = capture_logs(lambda: mct.check_message_handle([clean("app_1", "数据倾斜")]))
    check("clean 不写库", rows() == [], f"实际 {rows()}")
    warnings = [r for r in records if r.levelno >= logging.WARNING]
    check("clean 不该被当成「畸形 outcome」告警", not warnings,
          f"实际打了 {len(warnings)} 条: {[r.getMessage() for r in warnings]}",
          defect="每轮每条 clean 都会打一条「check_result 为空且 error 也为空」的 WARNING, "
                 "把「正常」报成了「畸形输入」")


def t3_found_and_invalid():
    section("[3] found / invalid 落库形状")
    reset_state()
    mct.check_message_handle([found("app_1", "数据倾斜", "出现数据倾斜", data={"stage": 1})])
    r = rows()
    check("found -> 1 行", len(r) == 1, f"实际 {r}")
    if r:
        app_id, app_type, c_type, e_type, desc, repeat, status = r[0]
        check("found 落库字段正确",
              (app_id, app_type, c_type, e_type, desc, repeat, status)
              == ("app_1", "SPARK", "数据倾斜", "error", "出现数据倾斜", 1, "PENDING"),
              f"{r[0]}")

    reset_state()
    mct.check_message_handle([invalid("app_1", "数据倾斜", "ConnectTimeout: 取数失败")])
    r = rows()
    check("invalid -> 1 行", len(r) == 1, f"实际 {r}")
    if r:
        check("invalid 的 checker_error 落库", r[0][2] == "数据倾斜" and r[0][4] == "" and r[0][3] is None,
              f"check_type/error_type/describe = {r[0][2], r[0][3], r[0][4]}")
    with SESSION() as s:
        err = s.execute(text("select checker_error from log.checker_log")).scalar()
    check("checker_error 存了原始报错", err == "ConnectTimeout: 取数失败", f"{err!r}")


def t4_repeat():
    section("[4] 同一错误重复出现")
    reset_state()
    mct.check_message_handle([found("app_1", "数据倾斜", "出现数据倾斜")])
    mct.check_message_handle([found("app_1", "数据倾斜", "出现数据倾斜")])
    mct.check_message_handle([found("app_1", "数据倾斜", "出现数据倾斜")])
    r = rows()
    check("3 轮同一错误 -> 仍 1 行", len(r) == 1, f"实际 {len(r)} 行")
    check("repeat_count 累加到 3", r and r[0][5] == 3, f"repeat_count={r[0][5] if r else None}")

    # 处理完(DONE)之后又复发: 该不该重新入队?
    with SESSION() as s:
        s.execute(text("update log.checker_log set status='DONE'"))
        s.commit()
    redis_con.delete(mct.key, f"{mct.key}:seq")
    mct.check_message_handle([found("app_1", "数据倾斜", "出现数据倾斜")])
    r = rows()
    pending = [x for x in r if x[6] == "PENDING"]
    mct.release_notice_to_redis()
    check("已处理完的错误再次复发 -> 重新入队", len(pending) == 1 and redis_con.zcard(mct.key) == 1,
          f"PENDING 行数={len(pending)}, 行状态={[x[6] for x in r]}, redis 通知数={redis_con.zcard(mct.key)}",
          defect="check_unique_key 按 dedup_key 查全表(没带 status='PENDING'), 命中 DONE 的老行就只 +repeat_count "
                 "—— 处理过的错误再复发时既不重新入队也不发通知, 永远不再被看见; "
                 "而表上那条唯一索引本身是 where status='PENDING' 的, 说明允许复发重排")


def t5_cluster_level():
    section("[5] 集群级检查落库（app_id="" / app_type=""）")
    reset_state()
    outcome = CheckOutcome(
        label="磁盘", app_id="", app_type="", status="found",
        result=CheckResult(type="error", describe="出现磁盘空间不足",
                           check_time=datetime.now(timezone.utc), data={"node": "node2"}),
    )
    try:
        mct.check_message_handle([outcome])
        err = None
    except Exception as e:
        err = e
    r = rows()
    check("集群级检出能落库", err is None and len(r) == 1,
          f"异常={type(err).__name__}: {brief(err)}" if err else f"实际 {r}",
          defect="CLUSTER_CHECKERS 的 outcome 带 app_type=\"\", 而表上有 "
                 "check (app_type in ('SPARK','FLINK')) —— 集群级一检出(磁盘满/句柄/队列资源) "
                 "就抛 CheckViolation, 且异常从 commit 抛出后整轮入队全废")
    check("集群级的 app_type 落成 NULL(不是空串)", r and r[0][1] is None,
          f"app_type={r[0][1]!r}")

    # 集群级消息进的是 app_id="" 的通知, Agent 侧租约要能接住
    redis_con.delete(mct.key, f"{mct.key}:seq")
    mct.release_notice_to_redis()
    note("集群级消息的通知里 app_id 是空串",
         f"zset 成员: {redis_con.zrange(mct.key, 0, -1)} —— 租约/分发要能处理『没有应用』的活")


def t6_batch_poison():
    section("[6] 一批里混进一条坏 outcome")
    reset_state()
    def bad(app_id: str):
        """写不进去的一条: data 里塞了 datetime, JSON 序列化会失败"""
        return found(app_id, "OOM", "data 里有不可序列化的对象",
                     data={"when": datetime.now(timezone.utc)})

    good1, good2 = found("app_1", "数据倾斜", "出现数据倾斜"), found("app_3", "数据倾斜", "出现数据倾斜")
    try:
        # 坏行放最前、放中间各一次 —— 顺带验失败之后 session 还能继续用
        mct.check_message_handle([bad("app_0"), good1, bad("app_2"), good2])
        err = None
    except Exception as e:
        err = e
    r = rows()
    check("坏行只丢它自己, 同批的好行照常入队",
          err is None and {x[0] for x in r} == {"app_1", "app_3"},
          f"异常={type(err).__name__}: {brief(err)}" if err else f"实际入库 {[x[0] for x in r]}",
          defect="整批一个事务时, 一条坏行会把同批全部消息一起回滚丢失")
    check("check_message_handle 不往外抛异常", err is None, f"{type(err).__name__}: {brief(err)}")


def t7_describe_contract():
    """
    入参约定: describe 是 dedup_key 的一部分, 所以里面**不能有每轮都在动的测量值**

    这一条只靠队列层是测不出来的(喂什么 describe 就存什么), 得直接跑真 checker:
    同一个 stage / 同一个 executor, 只把测量值改掉, 断言 describe 一个字都不变。
    """
    section("[7] describe 里不能带会漂移的测量值")
    import contextlib
    import io

    # ---- Spark 倾斜: 同一个 stage, 比值从 10.21 变到 25.00 ----
    import Tools.spark.check_spark_data_skew as skew

    def _stage(records):
        return {"stageId": 3, "attemptId": 0, "status": "COMPLETE",
                "tasks": {str(i): {"taskId": i, "status": "SUCCESS",
                                   "taskMetrics": {"shuffleReadMetrics": {"recordsRead": r},
                                                   "jvmGcTime": 100}}
                          for i, r in enumerate(records)}}

    def run_skew(records):
        skew.get_latest_stage_info = lambda sid, aid: _stage(records)
        with contextlib.redirect_stdout(io.StringIO()):  # checker 里有一串 print
            return skew.check_stage_summary([3], "application_fake")

    mild, severe = run_skew([10210] + [1000] * 9), run_skew([25000] + [1000] * 9)
    check("倾斜: 比值 10.21 -> 25.00, describe 不变",
          mild is not None and severe is not None and mild.describe == severe.describe,
          f"{mild.describe if mild else None!r} vs {severe.describe if severe else None!r}",
          defect="describe 里带倾斜比/GC 比 -> dedup_key 每轮都变一条新 PENDING, 去重形同虚设")
    check("倾斜: 比值落在 data 里",
          severe is not None and isinstance(severe.data, dict)
          and round(severe.data.get("skew_ratio", 0), 2) == 25.0,
          f"data.skew_ratio={severe.data.get('skew_ratio') if severe and isinstance(severe.data, dict) else None}")

    # ---- Spark OOM: 溢写量变 / 未知死因的 executor 个数变 ----
    import Tools.spark.check_spark_OOM as oom
    oom.ensure_node_mem_sampler = lambda: None  # 别真起采样线程

    def _active(on_heap_exec: int, max_memory: int = 434031820):
        return [{"id": "1", "hostPort": "node2:44705", "isActive": True, "totalTasks": 1134,
                 "maxMemory": max_memory,
                 "peakMemoryMetrics": {"OnHeapExecutionMemory": on_heap_exec}}]

    def run_spill(spill_bytes: int):
        oom.request_get = lambda uri, params=None, timeout=10: (
            [{"jobId": 1, "stageIds": [3]}] if uri.endswith("/jobs")
            else _active(400_000_000))  # 水位 400MB/434MB = 0.92, 过 0.9 线
        oom.get_latest_stage_info = lambda sid, aid: {"diskBytesSpilled": spill_bytes}
        return oom.check_OOM("application_fake")

    small, big = run_spill(320 * 1024 * 1024), run_spill(990 * 1024 * 1024)
    check("OOM 溢写: 320MB -> 990MB, describe 不变",
          small is not None and big is not None and small.describe == big.describe,
          f"{small.describe if small else None!r} vs {big.describe if big else None!r}",
          defect="describe 里带溢写 MB 数 -> 溢写每涨一点就是一条新 PENDING")
    check("OOM 溢写: 字节数落在 data 里",
          small is not None and isinstance(small.data, dict)
          and small.data.get("disk_bytes_spilled") == 320 * 1024 * 1024,
          f"data={small.data if small else None}")

    def run_uncertain(dead_count: int):
        oom.request_get = lambda uri, params=None, timeout=10: [
            {"id": str(i), "hostPort": "node2:44705", "isActive": False,
             "removeReason": "Container exited with a non-zero exit code 137",
             "executorLogs": {}} for i in range(dead_count)]
        return oom.check_OOM("application_fake")

    one, two = run_uncertain(1), run_uncertain(2)
    check("OOM 死因不明: 1 个 -> 2 个 Executor, describe 不变",
          one is not None and two is not None and one.describe == two.describe,
          f"{one.describe if one else None!r} vs {two.describe if two else None!r}",
          defect="describe 里带 Executor 个数 -> 每死一个就是一条新 PENDING")

    # ---- 队列层: 同一处倾斜连跑 3 轮 ----
    reset_state()
    for ratio in (10.21, 10.24, 10.19):
        mct.check_message_handle([found("app_1", "数据倾斜", "出现数据倾斜, 阶段 3",
                                        data={"stage_id": 3, "skew_ratio": ratio})])
    r = rows()
    check("同一处倾斜连跑 3 轮 -> 1 行", len(r) == 1,
          f"实际 {len(r)} 行: {[x[4] for x in r]}")
    with SESSION() as s:
        data = s.execute(text("select check_data from log.checker_log")).scalar()
    check("check_data 更新到最新一轮的比值", (data or {}).get("skew_ratio") == 10.19, f"{data}")


def t8_redis_notice():
    section("[8] redis 通知")
    reset_state()
    mct.check_message_handle([found("app_1", "数据倾斜", "出现数据倾斜"),
                              found("app_1", "OOM", "executor OOM"),
                              found("app_2", "数据倾斜", "出现数据倾斜")])
    mct.release_notice_to_redis()
    members = redis_con.zrange(mct.key, 0, -1)
    check("一个应用一条通知(3 条错误 -> 2 条通知)", len(members) == 2, f"实际 {members}")
    check("通知按 zset 分值有序", list(redis_con.zrange(mct.key, 0, -1, withscores=True))[0][1] == 1,
          f"{redis_con.zrange(mct.key, 0, -1, withscores=True)}")

    mct.release_notice_to_redis()
    check("重复调用不会重复发通知", len(redis_con.zrange(mct.key, 0, -1)) == 2,
          f"实际 {redis_con.zrange(mct.key, 0, -1)}")

    # 应用处理完后: 行不再 PENDING
    with SESSION() as s:
        s.execute(text("update log.checker_log set status='DONE' where app_id='app_1'"))
        s.commit()
    mct.release_notice_to_redis()
    stale = [m for m in redis_con.zrange(mct.key, 0, -1) if b"app_1" in m]
    note("redis 通知只增不减（已知，暂不处理）",
         f"行变 DONE 后仍留在 zset 里: {stale}；这一侧只负责 ZADD, 减的逻辑归 Agent 租约侧")


def t9_thresholds():
    section("[9] 阈值告警")
    reset_state()
    original = (mct.mode, mct.mode_value_list)
    mct.mode, mct.mode_value_list = "number", [3, 5, 7]
    sent = []
    original_email = mct.send_error_email
    mct.send_error_email = lambda subject, content: sent.append((subject, content)) or True
    try:
        for n, (expect_level, tag) in ((2, (None, "低阈值以下不报")),
                                       (3, (logging.WARNING, "低阈值")),
                                       (5, (logging.ERROR, "中阈值")),
                                       (7, (logging.CRITICAL, "高阈值"))):
            reset_state()
            seed_pending(n)
            records = capture_logs(mct.check_queue_situation)
            level = max((r.levelno for r in records), default=None)
            if expect_level is None:
                check(tag, level is None, f"实际 level={level}")
            else:
                check(f"{tag}: {n} 条", level == expect_level,
                      f"期望 {logging.getLevelName(expect_level)}, 实际 {logging.getLevelName(level) if level else None}")
        # ---- 邮件：只在刚涨进高阈值那一轮发 ----
        reset_state()
        seed_pending(7)
        sent.clear()
        capture_logs(mct.check_queue_situation)
        check("刚涨进高阈值 -> 发 1 封", len(sent) == 1, f"实际 {len(sent)} 封")
        capture_logs(mct.check_queue_situation)
        check("一直待在高阈值 -> 不再重复发", len(sent) == 1, f"实际 {len(sent)} 封")
        mct._last_level = 1  # 模拟掉到低阈值又涨上来
        capture_logs(mct.check_queue_situation)
        check("掉下来再涨回去 -> 再发一封", len(sent) == 2, f"实际 {len(sent)} 封")

        # ---- 当日上限 ----
        reset_state()
        seed_pending(7)
        sent.clear()
        records = []
        for _ in range(4):
            mct._last_level = 0  # 每轮都当成"刚涨上来" —— 阈值线上抖动就是这个效果
            records = capture_logs(mct.check_queue_situation)
        check("抖动 4 次 -> 只发 3 封（当日上限）", len(sent) == 3, f"实际 {len(sent)} 封")
        check("被上限挡下要说出来",
              any(r.levelno == logging.WARNING and "已达上限" in r.getMessage() for r in records),
              f"{[r.getMessage() for r in records if r.levelno == logging.WARNING]}")

        # ---- 恢复：队列空了档位归零 ----
        reset_state()
        sent.clear()
        capture_logs(mct.check_queue_situation)
        check("队列空了 -> 档位归 0（下次涨上来能再发）", mct._last_level == 0, f"_last_level={mct._last_level}")
        seed_pending(7)
        capture_logs(mct.check_queue_situation)
        check("恢复后再涨上来 -> 又能发一封", len(sent) == 1, f"实际 {len(sent)} 封")
        sent.clear()

        # 7 条里 4 条处理完 -> 只剩 3 条, 应回落到低阈值
        reset_state()
        seed_pending(7)
        with SESSION() as s:
            s.execute(text("update log.checker_log set status='DONE' where app_id in ('app1','app2','app3','app4')"))
            s.commit()
        records = capture_logs(mct.check_queue_situation)
        check("只统计 PENDING: 7 条里 4 条 DONE -> 回落到低阈值 WARNING",
              max((r.levelno for r in records), default=None) == logging.WARNING,
              f"实际 {[logging.getLevelName(r.levelno) for r in records]}")

        reset_state()
        seed_pending(7)
        with SESSION() as s:
            s.execute(text("update log.checker_log set status='DONE'"))
            s.commit()
        records = capture_logs(mct.check_queue_situation)
        check("全部 DONE 后不再告警", not records, f"实际 {[r.getMessage() for r in records]}")

        mct.mode = "time"
        mct.mode_value_list = [60, 120, 300]
        mail_before = len(sent)
        reset_state()
        seed_pending(1, age_seconds=30)
        records = capture_logs(mct.check_queue_situation)
        check("time 模式: 30 秒(低于低阈值)不报", not records, f"实际 {[r.getMessage() for r in records]}")

        reset_state()
        seed_pending(1, age_seconds=90)
        records = capture_logs(mct.check_queue_situation)
        check("time 模式: 90 秒 -> 低阈值 WARNING",
              max((r.levelno for r in records), default=None) == logging.WARNING,
              f"实际 {[logging.getLevelName(r.levelno) for r in records]}")

        reset_state()
        seed_pending(1, age_seconds=400)
        records = capture_logs(mct.check_queue_situation)
        check("time 模式: 400 秒 -> 高阈值 CRITICAL + 邮件",
              max((r.levelno for r in records), default=None) == logging.CRITICAL
              and len(sent) - mail_before == 1,
              f"level={[logging.getLevelName(r.levelno) for r in records]}, "
              f"新增邮件={len(sent) - mail_before}")
    finally:
        mct.mode, mct.mode_value_list = original
        mct.send_error_email = original_email


def t10_main_loop():
    section("[10] main_check 主循环")
    calls = {"cycle": 0, "handle": 0, "notice": 0, "queue": 0}
    stub = type(sys)("stub")

    def fake_sleep(_):
        calls["sleep"] = calls.get("sleep", 0) + 1
        if calls["sleep"] >= 3:  # 跑满 3 轮就退出(build 出循环靠的就是 sleep)
            raise SystemExit("stop")

    stub.sleep = fake_sleep

    def fake_cycle():
        calls["cycle"] += 1
        if calls["cycle"] == 1:
            raise RuntimeError("取应用列表炸了")
        return []

    saved = (mct.time, mct.run_cycle, mct.check_message_handle, mct.release_notice_to_redis,
             mct.check_queue_situation)
    mct.time = stub
    mct.run_cycle = fake_cycle
    mct.check_message_handle = lambda outs: calls.__setitem__("handle", calls["handle"] + 1)
    mct.release_notice_to_redis = lambda: calls.__setitem__("notice", calls["notice"] + 1)
    mct.check_queue_situation = lambda: calls.__setitem__("queue", calls["queue"] + 1)
    try:
        try:
            mct.main_check()
        except SystemExit:
            pass
    finally:
        (mct.time, mct.run_cycle, mct.check_message_handle, mct.release_notice_to_redis,
         mct.check_queue_situation) = saved

    check("run_cycle 抛异常后主循环没有退出", calls["cycle"] == 3, f"实际只跑了 {calls['cycle']} 轮")
    check("第一轮炸了之后, 后续轮次照常入队", calls["handle"] == 2, f"handle 调用 {calls['handle']} 次")
    check("第一轮炸了之后, 后续轮次照常发通知/查阈值",
          calls["notice"] == 2 and calls["queue"] == 2, f"{calls}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--keep", action="store_true", help="保留临时库 mabp_verify 供排查")
    args = parser.parse_args()

    print("=" * 72)
    print(f"main_check_tool 入队层 验证  (临时库 {TEST_DB}, 从 {SRC_DB} 复制)")
    print("=" * 72)

    for fn in (t1_config, t2_clean, t3_found_and_invalid, t4_repeat, t5_cluster_level,
               t6_batch_poison, t7_describe_contract, t8_redis_notice, t9_thresholds, t10_main_loop):
        try:
            fn()
        except Exception as e:
            import traceback
            check(f"{fn.__name__} 执行失败", False, f"{type(e).__name__}: {e}")
            traceback.print_exc()

    ok = sum(1 for _, v in _results if v)
    total = len(_results)
    print("\n" + "=" * 72)
    print(f"结果: {ok}/{total} 通过")
    if _defects:
        print(f"\n发现 {len(_defects)} 处缺陷:")
        for i, d in enumerate(_defects, 1):
            print(f"  {i}. {d}")
    if _notes:
        print(f"\n{len(_notes)} 处供定夺（不算失败）:")
        for i, n in enumerate(_notes, 1):
            print(f"  {i}. {n}")
    print("=" * 72)

    redis_con.delete(mct.key, f"{mct.key}:seq")
    if not args.keep:
        ENGINE.dispose()
        with pg_conn("postgres") as conn:
            conn.autocommit = True
            conn.cursor().execute(f"DROP DATABASE IF EXISTS {TEST_DB} WITH (FORCE)")
    sys.exit(0 if ok == total else 1)
