"""
临时冒烟脚本 —— 逐个调用全部 checker, 打印结果与异常

用途:
    1. VM 起来后确认联通性(端点是通的、字段名是对的)
    2. 健康集群下确认零误报(所有 checker 应返回 None)
    3. 注入故障后确认检出能力(对应 checker 应返回 CheckResult)

用法:
    cd 项目根目录
    python tests/smoke_check.py              # 全量跑
    python tests/smoke_check.py --preflight  # 只做联通性检查
    python tests/smoke_check.py --only flink # 只跑 Flink 侧
"""
import argparse
import importlib
import pathlib
import socket
import sys
import time
import traceback
from urllib.parse import urlparse

# 直接 python tests/smoke_check.py 时, sys.path 里只有 tests/, 需要把项目根补上
PROJECT_ROOT = pathlib.Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Windows 控制台默认 GBK, 中文输出会炸
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# 注意: 这里刻意不 import Tools.* —— share_check_tool 在模块级会构造
# yarn_api_client.ResourceManager, RM 不可达时直接抛 "No active RMs found",
# 导致预检还没跑就崩在 import 上。所以下面的端点值是与 share_check_tool 保持
# 一致的副本, 改那边时记得同步改这里。
MANAGER = "http://node1:8088"
PROXY = "http://node1:8089"
FLINK_MANAGER = "http://node1:7788"
REDIS_ADDR = "localhost:6379"

# ---------------- 检查项登记表 ----------------
# (显示名, 模块路径, 入口函数名)
GLOBAL_CHECKS = [
    ("磁盘异常", "Tools.check_disk_abnormal", "check_disk"),
    ("文件句柄耗尽", "Tools.check_file_handle_abnormal", "check_file_handle"),
    ("资源不足/无法分配", "Tools.check_resource_abnormal", "check_resource_insufficient"),
]

FLINK_CHECKS = [
    ("检查点异常", "Tools.flink.check_flink_checkpoint", "check_checkpoint"),
    ("数据倾斜", "Tools.flink.check_flink_data_skew", "check_flink_data_skew"),
    ("任务挂起", "Tools.flink.check_flink_job_hang", "check_flink_job_hang"),
    ("TaskManager OOM 水位", "Tools.flink.check_flink_oom", "check_flink_oom"),
    ("水位线异常", "Tools.flink.check_flink_watermarks", "check_watermarks"),
    ("背压与网络", "Tools.flink.check_flink_backpressure", "check_flink_backpressure"),
    ("其他异常(类冲突/数据源/权限/格式)", "Tools.flink.check_flink_other_abnormal",
     "check_flink_other_abnormal"),
]

SPARK_CHECKS = [
    ("数据倾斜", "Tools.spark.check_spark_data_skew", "check_spark_data_skew"),
    ("OOM", "Tools.spark.check_spark_OOM", "check_OOM"),
    ("网络超时", "Tools.spark.check_spark_network_timeout", "network_timeout_check"),
    ("其他异常(类冲突/数据源/权限/格式)", "Tools.spark.check_other_spark_abnormal",
     "check_other"),
    ("任务超时", "Tools.check_job_timeout", "check_spark_job_timeout"),
]

# 联通性预检目标: (显示名, URL)
PREFLIGHT_TARGETS = [
    ("YARN ResourceManager", f"{MANAGER}/cluster/apps"),
    ("YARN 代理(Spark UI)", f"{PROXY}/proxy"),
    ("Flink JobManager", f"{FLINK_MANAGER}/jobs/overview"),
    ("Flink /applications 端点(存疑)", f"{FLINK_MANAGER}/applications"),
]


def _tcp_alive(url: str, timeout: float = 3.0) -> tuple:
    """
    只测 TCP 能否建连, 不关心 HTTP 状态码
    :return: (是否连通, 说明)
    """
    parsed = urlparse(url)
    host, port = parsed.hostname, parsed.port or 80
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True, f"{host}:{port} 可建连"
    except Exception as e:
        return False, f"{host}:{port} 不可达 ({type(e).__name__}: {e})"


def _http_probe(url: str, timeout: float = 5.0) -> tuple:
    """
    发一个真实请求, 报告状态码
    :return: (是否 2xx, 说明)
    """
    import requests
    try:
        resp = requests.get(url, timeout=timeout)
        return resp.status_code // 100 == 2, f"HTTP {resp.status_code}"
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"


def preflight() -> bool:
    """
    联通性预检, 不调用任何 checker
    """
    print("=" * 72)
    print("联通性预检")
    print("=" * 72)

    global_flags = []

    # Redis 是多个 checker 的硬依赖(挂起/水位线/文件句柄/超时/资源 都用到)
    redis_ok, redis_msg = _tcp_alive(f"http://{REDIS_ADDR}")
    global_flags.append(redis_ok)
    print(f"  [{'OK ' if redis_ok else 'FAIL'}] {'Redis':<22} {redis_msg}")

    for name, url in PREFLIGHT_TARGETS:
        ok, msg = _http_probe(url)
        global_flags.append(ok)
        print(f"  [{'OK ' if ok else 'FAIL'}] {name:<22} {url}")
        print(f"         └─ {msg}")

    print()
    if not all(global_flags):
        print("  ⚠️  有目标不可达 —— 先确认 VM 已开机、集群已启动、hosts 解析正常")
    return all(global_flags)


def _call(module_path: str, func_name: str, app_id: str = None):
    """
    动态取模块函数并调用, 返回 (结果, 异常, 耗时秒)

    import 也放在 try 内: 所有 checker 都会经 share_check_tool 构造
    yarn_api_client.ResourceManager, 集群不可达时 import 本身就会抛异常
    """
    start = time.perf_counter()
    try:
        module = importlib.import_module(module_path)
        func = getattr(module, func_name)
        result = func(app_id) if app_id else func()
        return result, None, time.perf_counter() - start
    except Exception as e:
        return None, e, time.perf_counter() - start


def _report(label: str, module_path: str, func_name: str, app_id: str = None) -> dict:
    """
    跑单个 checker 并打印结果
    :return: {"label":, "status": "clean"/"found"/"error", ...}
    """
    result, exc, elapsed = _call(module_path, func_name, app_id)
    tag = f"{label}" + (f" @ {app_id}" if app_id else "")

    if exc is not None:
        print(f"  [ERROR] {tag}  ({elapsed:.2f}s)")
        print(f"          {type(exc).__name__}: {exc}")
        for line in traceback.format_exc().splitlines()[-4:-1]:
            print(f"          {line.strip()}")
        return {"label": tag, "status": "error", "exc": exc}

    # 注意: 有的 checker 正常时返回 None, 有的返回 [] —— 两者都算"干净"
    if result is None or result == []:
        print(f"  [  --  ] {tag}  无异常  ({elapsed:.2f}s)")
        return {"label": tag, "status": "clean"}

    print(f"  [FOUND] {tag}  检出异常!  ({elapsed:.2f}s)")
    data = getattr(result, "data", None)
    describe = getattr(result, "describe", None)
    rtype = getattr(result, "type", None)
    print(f"          type={rtype!r} describe={describe!r}")
    if data is not None:
        rendered = repr(data)
        print(f"          data={rendered[:600]}{'...(截断)' if len(rendered) > 600 else ''}")
    return {"label": tag, "status": "found", "result": result}


def run_group(title: str, checks: list, app_ids: list = None) -> list:
    """
    跑一组 checker; app_ids 为 None 时表示全局检查(不传 app_id)
    :return: 结果字典列表
    """
    print()
    print("-" * 72)
    print(title)
    print("-" * 72)
    records = []
    for label, module_path, func_name in checks:
        if app_ids is None:
            records.append(_report(label, module_path, func_name))
        elif not app_ids:
            print(f"  [  --  ] {label}  无应用可检查, 跳过")
        else:
            for app_id in app_ids:
                records.append(_report(label, module_path, func_name, app_id))
    return records


def main():
    parser = argparse.ArgumentParser(description="checker 冒烟脚本")
    parser.add_argument("--preflight", action="store_true", help="只做联通性预检")
    parser.add_argument("--only", choices=["flink", "spark", "global"],
                        help="只跑指定分组")
    args = parser.parse_args()

    if args.preflight:
        sys.exit(0 if preflight() else 1)

    preflight()

    # 发现正在运行的应用
    print()
    print("=" * 72)
    print("正在运行的 YARN 应用")
    print("=" * 72)
    try:
        # 到这里才 import: 上面的预检已经确认过联通性了
        from Tools.share_check_tool import get_all_running_apps, is_flink_app
        running_apps = get_all_running_apps()
    except Exception as e:
        print(f"  获取失败: {type(e).__name__}: {e}")
        print()
        print("  ⚠️  注意: share_check_tool 在模块级构造 ResourceManager, "
              "RM 不可达时整个 Tools 包都无法 import。")
        print("     这不是网络抖动, 是设计问题 —— 集群没起来时 checker 会崩在 import 阶段。")
        running_apps = []

    flink_apps, spark_apps, other_apps = [], [], []
    for app in running_apps:
        app_id = app.get("id", "")
        app_type = app.get("applicationType", "")
        if not app_id:
            continue
        print(f"  {app_id}  type={app_type!r}  name={app.get('name', '')!r}  "
              f"state={app.get('state', '')!r}")
        # 用 is_flink_app 而不是 == "FLINK": 实测 YARN 报的 applicationType 是
        # "Apache Flink", 直接比较会把 Flink 应用全归进「其他」而不跑它的 checker
        if is_flink_app(app):
            flink_apps.append(app_id)
        elif app_type == "SPARK":
            spark_apps.append(app_id)
        else:
            other_apps.append(app_id)

    if not running_apps:
        print("  (无) —— 注意: 没有运行中的应用时, 所有按 app_id 检查的 checker "
              "都会直接返回 None, 这不代表它们工作正常")
    if other_apps:
        print(f"  ⚠️  {len(other_apps)} 个应用类型既非 FLINK 也非 SPARK, "
              f"没有任何 checker 会覆盖它们: {other_apps}")

    print()
    print(f"  合计: FLINK={len(flink_apps)} SPARK={len(spark_apps)} 其他={len(other_apps)}")

    records = []
    if args.only in (None, "global"):
        records += run_group("全局检查(与具体应用无关)", GLOBAL_CHECKS)
    if args.only in (None, "flink"):
        records += run_group(f"Flink 检查({len(flink_apps)} 个应用)", FLINK_CHECKS, flink_apps)
    if args.only in (None, "spark"):
        records += run_group(f"Spark 检查({len(spark_apps)} 个应用)", SPARK_CHECKS, spark_apps)

    # ---------------- 汇总 ----------------
    print()
    print("=" * 72)
    print("汇总")
    print("=" * 72)
    errored = [r for r in records if r["status"] == "error"]
    found = [r for r in records if r["status"] == "found"]
    clean = [r for r in records if r["status"] == "clean"]

    print(f"  无异常: {len(clean)}    检出异常: {len(found)}    抛异常: {len(errored)}")

    if errored:
        print()
        print("  ⚠️  以下 checker 自身抛异常(代码 bug 或环境不通), 需要逐个排查:")
        for r in errored:
            print(f"      - {r['label']}: {type(r['exc']).__name__}: {r['exc']}")

    if found:
        print()
        print("  检出异常的 checker(若是健康集群, 这里本该是空的 → 疑似误报):")
        for r in found:
            print(f"      - {r['label']}: {getattr(r['result'], 'describe', '')}")

    # 有 checker 抛异常 → 非 0 退出, 方便接 CI
    sys.exit(1 if errored else 0)


if __name__ == "__main__":
    main()
