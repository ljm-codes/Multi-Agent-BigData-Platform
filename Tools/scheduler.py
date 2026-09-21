"""
checker 调度 —— 每轮把该跑的检查跑一遍, 产出 CheckOutcome

调用模型(依据是实测耗时, 不是感觉):
  * 应用之间**并行**(线程数 = 应用数), 应用内**串行**;
  * 集群级 3 个串行, 跑在主循环, 不按应用重复;
  * checker 内部的扇出统一压到 SMALL_N_JOBS。

为什么应用内不并行: 它们打的是同一个应用的同一批 REST(`/jobs`、`/stages` 被几个
checker 重复取), 并行只省钱不省事 —— 单应用整轮实测约 2.5 秒, 而轮询周期 30 秒,
余量 12 倍; 反过来并行会把同一台 RM 压出超时, 而**超时 = 取不到数 = 和"健康"同形**。
"""
import importlib
import inspect
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import List, Optional

from SPO.check_result import CheckOutcome
from Tools.checker_registry import CLUSTER_CHECKERS, checkers_for
from Tools.log_settings import LogSetting
from Tools.share_check_tool import get_all_running_apps

logger = LogSetting.create(__name__)

# checker 内部扇出(取 stage / subtask 级数据)的并发。默认的 10 是给"单独跑一个
# checker"用的; 调度器里多个应用是并行的, 相乘之后会打爆小集群的 RM。
SMALL_N_JOBS = 4

# 应用之间并行的上限(应用再多也排队, 别把 RM 压垮)
MAX_APP_CONCURRENCY = 4

_N_JOBS_SUPPORT = {}


def _accepts_n_jobs(fn) -> bool:
    """该 checker 接不接受 n_jobs(判一次记住) —— 不去改那些已经验过的 checker"""
    key = f"{fn.__module__}.{fn.__qualname__}"  # 模块名.限定名
    if key not in _N_JOBS_SUPPORT:
        _N_JOBS_SUPPORT[key] = "n_jobs" in inspect.signature(fn).parameters
    return _N_JOBS_SUPPORT[key]


def _ms(t0: float) -> int:
    return int((time.time() - t0) * 1000)


def _call(label: str,
          mod_name: str,
          fn_name: str,
          app_id: Optional[str] = None,
          app_type: Optional[str] = ""
          ) -> CheckOutcome:
    """
    调一次 checker, 把它的返回**翻译**成 CheckOutcome

    checker 本身不改(仍返回 CheckResult | None), 翻译只在调度层做:
        None -> clean(真的没异常); CheckResult -> found; 抛异常/超时 -> invalid
    """
    t0 = time.time()
    try:
        fn = getattr(importlib.import_module(mod_name), fn_name)
    except Exception as e:
        return CheckOutcome(label=label, app_id=app_id or "",
                            status="invalid", app_type=app_type,
                            error=f"导入失败 {mod_name}.{fn_name}: {type(e).__name__}: {e}",
                            error_time=datetime.now(timezone.utc), cost_ms=_ms(t0))
    try:
        # 签名有两种: 应用级 fn(app_id[, n_jobs]), 集群级 fn()
        if app_id is None:
            result = fn()
        elif _accepts_n_jobs(fn):
            result = fn(app_id, n_jobs=SMALL_N_JOBS)
        else:
            result = fn(app_id)
        return CheckOutcome(label=label, app_id=app_id or "", app_type=app_type,
                            status="found" if result else "clean", result=result,
                            cost_ms=_ms(t0))
    except Exception as e:
        logger.error(f"[{label}] 检查失败(测量无效) app={app_id or '-'}: {type(e).__name__}: {e}")
        return CheckOutcome(label=label, app_id=app_id or "",
                            status="invalid", app_type=app_type,
                            error=f"{type(e).__name__}: {e}",
                            error_time=datetime.now(timezone.utc), cost_ms=_ms(t0))


def run_app(app: dict) -> List[CheckOutcome]:
    """跑一个应用的该类型全部检查(串行, 见模块开头的说明)"""
    app_id = app.get("id", "")
    checkers, app_type = checkers_for(app)
    if checkers is None:
        # 未知类型 = 这个应用一条检查都没跑。必须说出来 —— 否则它和"全部正常"同形
        logger.warning(f"未知应用类型, 跳过全部检查: app={app_id} "
                       f"applicationType={app.get('applicationType')!r}")
        return []
    return [_call(label, mod, fn, app_id, app_type) for label, mod, fn in checkers]


def run_cycle(apps: Optional[List[dict]] = None) -> List[CheckOutcome]:
    """
    跑一轮全部检查

    :param apps: 要检查的应用(YARN /ws/v1/cluster/apps 的条目);
                 不给就现场取一次正在运行的
    """
    # 集群检查的类型恒为 ""
    outcomes = [_call(label, mod, fn, app_type="") for label, mod, fn in CLUSTER_CHECKERS]

    if apps is None:
        apps = get_all_running_apps()
    if apps:
        with ThreadPoolExecutor(max_workers=min(len(apps), MAX_APP_CONCURRENCY)) as pool:
            for one_app_outcomes in pool.map(run_app, apps):
                outcomes.extend(one_app_outcomes)

    _log_summary(outcomes)
    return outcomes


def _log_summary(outcomes: List[CheckOutcome]) -> None:
    """每轮一行汇总。invalid 单独喊 —— 它是"没测到", 不能混进"没异常"里"""
    found = [o for o in outcomes if o.status == "found"]
    invalid = [o for o in outcomes if o.status == "invalid"]
    logger.info(f"本轮完成 {len(outcomes)} 项, 耗时合计 {sum(o.cost_ms for o in outcomes)}ms: "
                + ", ".join(f"{o.app_id or '集群'}/{o.label}={o.status}" for o in outcomes))
    if found:
        logger.warning("检出异常: " + ", ".join(
            f"{o.app_id or '集群'}/{o.label} -> {o.result.type}: {o.result.describe}" for o in found))
    if invalid:
        logger.error(f"测量无效 {len(invalid)} 项(不要当成没异常): " + ", ".join(
            f"{o.app_id or '集群'}/{o.label}({o.error})" for o in invalid))


if __name__ == "__main__":
    # test
    import json

    print(json.dumps([o.model_dump() for o in run_cycle()], ensure_ascii=False, indent=2))
