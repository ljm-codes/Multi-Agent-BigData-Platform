"""
验证 Tools/scheduler.py + Tools/checker_registry.py（离线，不需要集群）

覆盖:
  1. 注册表里每条登记都能解析（模块能 import、函数存在）
  2. 注册表和两份验证脚本的清单**不重不漏**（新增 checker 忘记登记会被抓出来）
  3. _call 的三态翻译: None -> clean / CheckResult -> found / 抛异常 -> invalid
  4. _call 的两种签名: 接 n_jobs 的传 n_jobs, 不接的（check_job_timeout）不传
  5. 未知应用类型 -> 一条都不跑, 且显式告警（不能默认当 spark）

用法: python tests/verify_scheduler.py

⚠️ 本文件**离线可跑**, 靠的是在导入 Tools 之前给 yarn_api_client 打个桩 ——
`Tools/share_check_tool.py` 在**模块级**构造 `ResourceManager`, 而那个类会主动探测 RM、
探不到就抛 `No active RMs found`(连接超时 30 秒)。也就是说导入这个包就等于要求 RM 在线。
桩只挡掉"构造时的探测", 不影响本文件覆盖的调度/注册表逻辑。
"""
import importlib
import importlib.util
import pathlib
import sys

# 必须在任何 Tools.* 之前打桩(见文件开头的说明)
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

PASS, FAIL = "✅", "❌"
_results = []


def check(name: str, ok: bool, detail: str = ""):
    _results.append((name, ok))
    print(f"  {PASS if ok else FAIL} {name}")
    if detail:
        print(f"      {detail}")


def _load_entries(rel_path: str) -> list:
    """把验证脚本按路径加载进来, 读它的 ENTRIES —— 不执行 __main__"""
    path = PROJECT_ROOT / rel_path
    spec = importlib.util.spec_from_file_location(f"_probe_{path.stem}", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod.ENTRIES


def t1_registry_resolvable():
    print("\n[1] 注册表里的登记都能解析")
    from Tools.checker_registry import CLUSTER_CHECKERS, SPARK_CHECKERS, FLINK_CHECKERS

    for group, entries in (("集群级", CLUSTER_CHECKERS), ("Spark", SPARK_CHECKERS),
                           ("Flink", FLINK_CHECKERS)):
        for label, mod_name, fn_name in entries:
            try:
                fn = getattr(importlib.import_module(mod_name), fn_name)
                check(f"{group}/{label} -> {mod_name}.{fn_name}", callable(fn))
            except Exception as e:
                check(f"{group}/{label} -> {mod_name}.{fn_name}", False, f"{type(e).__name__}: {e}")


def t2_registry_matches_harness():
    """注册表要和两份验证脚本的清单对得上 —— 漏登记 = 那条检查从来不跑"""
    print("\n[2] 注册表和验证脚本的清单不重不漏")
    from Tools.checker_registry import SPARK_CHECKERS, FLINK_CHECKERS

    for name, rel, registry in (("Spark", "tests/check_spark_live.py", SPARK_CHECKERS),
                                ("Flink", "tests/check_flink_live.py", FLINK_CHECKERS)):
        entries = _load_entries(rel)
        # ENTRIES 是 (label, mod, fn), 注册表是 (label, mod, fn) —— 只比 (mod, fn)
        harness = {(m, f) for _, m, f in entries}
        registered = {(m, f) for _, m, f in registry}
        missing = harness - registered
        extra = registered - harness
        check(f"{name}: 验证脚本里的每条都登记了", not missing, f"漏登记: {sorted(missing)}")
        check(f"{name}: 注册表里没有多余的", not extra, f"多出来: {sorted(extra)}")


def t3_call_three_states():
    print("\n[3] _call 的三态翻译")
    import Tools.scheduler as sch
    from SPO.check_result import CheckResult

    # 造三个假 checker(用真实模块路径不好控, 直接塞进 sys.modules 更干净)
    import types
    fake = types.ModuleType("_fake_checkers")

    def returns_none(app_id=None, n_jobs=10):
        return None

    def returns_result(app_id=None, n_jobs=10):
        return CheckResult(type="error", describe="假异常")

    def raises(app_id=None, n_jobs=10):
        raise RuntimeError("取不到数")

    def no_n_jobs(app_id=None):
        return CheckResult(type="warning", describe=f"app={app_id}")

    def records_n_jobs(app_id=None, n_jobs=10):
        return CheckResult(type="warning", describe=f"收到 n_jobs={n_jobs}")

    fake.returns_none, fake.returns_result, fake.raises = returns_none, returns_result, raises
    fake.no_n_jobs, fake.records_n_jobs = no_n_jobs, records_n_jobs
    sys.modules["_fake_checkers"] = fake

    check("返回 None -> clean",
          sch._call("x", "_fake_checkers", "returns_none", "app_1").status == "clean")
    r = sch._call("x", "_fake_checkers", "returns_result", "app_1")
    check("返回 CheckResult -> found 且带上结果",
          r.status == "found" and r.result is not None and r.result.type == "error")
    r = sch._call("x", "_fake_checkers", "raises", "app_1")
    check("抛异常 -> invalid(不能变成 clean)",
          r.status == "invalid" and "RuntimeError" in r.error, f"error={r.error!r}")
    check("耗时被记下来", r.cost_ms >= 0)

    print("\n[4] 两种签名都要能调")
    r = sch._call("x", "_fake_checkers", "no_n_jobs", "app_1")
    check("不接 n_jobs 的 checker(如 check_job_timeout) 不传 n_jobs",
          r.status == "found" and "app_1" in r.result.describe, f"{r.result.describe if r.result else r.error}")
    r = sch._call("x", "_fake_checkers", "records_n_jobs", "app_1")
    check(f"接 n_jobs 的 checker 收到 SMALL_N_JOBS={sch.SMALL_N_JOBS}",
          r.status == "found" and f"n_jobs={sch.SMALL_N_JOBS}" in r.result.describe,
          f"{r.result.describe if r.result else r.error}")


def t5_unknown_app_type():
    print("\n[5] 未知应用类型")
    import Tools.scheduler as sch

    outcomes = sch.run_app({"id": "application_x", "applicationType": "MapReduce"})
    check("未知类型 -> 一条检查都不跑(返回空)", outcomes == [], f"实际 {outcomes}")


if __name__ == "__main__":
    print("=" * 72)
    print("调度器 / 注册表 验证")
    print("=" * 72)
    for fn in (t1_registry_resolvable, t2_registry_matches_harness,
               t3_call_three_states, t5_unknown_app_type):
        try:
            fn()
        except Exception as e:
            check(f"{fn.__name__} 执行失败", False, f"{type(e).__name__}: {e}")

    ok = sum(1 for _, v in _results if v)
    total = len(_results)
    print("\n" + "=" * 72)
    print(f"结果: {ok}/{total} 通过")
    print("=" * 72)
    sys.exit(0 if ok == total else 1)
