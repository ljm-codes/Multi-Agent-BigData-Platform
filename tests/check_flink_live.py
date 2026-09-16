"""
对真实 Flink 应用跑一遍全部 7 个 Flink checker —— 端到端验证

走 YARN 的 WebAppProxy (node1:8089/proxy/{app_id}), 和 checker 现在的取数路径完全一致。
健康作业上 7 个 checker 应当**全部返回 None**(零误报); 有故障时对应 checker 应检出。

用法:
    python tests/check_flink_live.py                          # 自动发现 YARN 上 RUNNING 的 Flink 应用
    python tests/check_flink_live.py --app application_xxx_0003
    python tests/check_flink_live.py --proxy http://node1:8089
"""
import argparse
import importlib
import pathlib
import sys
import time

PROJECT_ROOT = pathlib.Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

import requests

ENTRIES = [
    ("检查点异常", "Tools.flink.check_flink_checkpoint", "check_checkpoint"),
    ("数据倾斜", "Tools.flink.check_flink_data_skew", "check_flink_data_skew"),
    ("任务挂起", "Tools.flink.check_flink_job_hang", "check_flink_job_hang"),
    ("TaskManager OOM 水位", "Tools.flink.check_flink_oom", "check_flink_oom"),
    ("水位线异常", "Tools.flink.check_flink_watermarks", "check_watermarks"),
    ("背压与网络", "Tools.flink.check_flink_backpressure", "check_flink_backpressure"),
    ("其他异常", "Tools.flink.check_flink_other_abnormal", "check_flink_other_abnormal"),
]


def discover_flink_apps(manager: str) -> list:
    """从 YARN 发现正在运行的 Flink 应用 (用 checker 自己的判定函数, 顺便验它)"""
    from Tools.share_check_tool import get_all_running_apps, is_flink_app
    apps = get_all_running_apps()
    out = []
    for app in apps:
        app_id = app.get("id", "")
        if not app_id:
            continue
        print(f"  id={app_id}  name={app.get('name')!r}  "
              f"applicationType={app.get('applicationType')!r}  "
              f"is_flink_app={is_flink_app(app)}")
        if is_flink_app(app):
            out.append(app_id)
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--proxy", default="http://node1:8089", help="YARN WebAppProxy 地址")
    ap.add_argument("--app", help="指定 YARN application id, 不填则自动发现")
    args = ap.parse_args()

    # checker 的基址由 share_check_tool.proxy 决定, 改它一处即可
    import Tools.share_check_tool as sct
    sct.proxy = args.proxy

    print("=" * 74)
    print(f"对真实 Flink 应用跑 checker   代理: {args.proxy}")
    print("=" * 74)

    if args.app:
        app_ids = [args.app]
    else:
        print("\n从 YARN 发现 RUNNING 的 Flink 应用:")
        app_ids = discover_flink_apps(args.proxy)

    if not app_ids:
        print("\n⚠️  没有发现 RUNNING 的 Flink 应用 —— 提交一条再来")
        sys.exit(0)

    # 先确认代理通道通
    app_id = app_ids[0]
    base = f"{args.proxy}/proxy/{app_id}"
    print(f"\n应用: {app_id}")
    print(f"基址: {base}")
    try:
        r = requests.get(f"{base}/jobs/overview", timeout=10)
        print(f"  GET /jobs/overview -> HTTP {r.status_code}")
        if r.status_code == 200:
            jobs = r.json().get("jobs", [])
            for j in jobs:
                print(f"      jid={j.get('jid')}  name={j.get('name')}  state={j.get('state')}")
    except Exception as e:
        print(f"  ❌ 代理不通: {type(e).__name__}: {e}")
        sys.exit(1)

    print()
    print("-" * 74)
    print("跑 checker (健康作业应当全部返回 None)")
    print("-" * 74)

    results = []
    for label, mod_name, func_name in ENTRIES:
        mod = importlib.import_module(mod_name)
        func = getattr(mod, func_name)
        start = time.perf_counter()
        try:
            result = func(app_id)
            elapsed = time.perf_counter() - start
            if result is None or result == []:
                print(f"  [  --  ] {label:24} 无异常   ({elapsed:.2f}s)")
                results.append((label, "clean", None))
            else:
                data = getattr(result, "data", None)
                n = len(data) if isinstance(data, (list, dict)) else "-"
                print(f"  [FOUND] {label:24} 检出!   ({elapsed:.2f}s)")
                print(f"           describe={getattr(result, 'describe', '')!r}")
                print(f"           data {n} 条: {repr(data)[:400]}")
                results.append((label, "found", result))
        except Exception as e:
            elapsed = time.perf_counter() - start
            print(f"  [ERROR] {label:24} {type(e).__name__}: {e}   ({elapsed:.2f}s)")
            import traceback
            for line in traceback.format_exc().splitlines()[-4:-1]:
                print(f"           {line.strip()}")
            results.append((label, "error", e))

    clean = [r for r in results if r[1] == "clean"]
    found = [r for r in results if r[1] == "found"]
    errored = [r for r in results if r[1] == "error"]

    print()
    print("=" * 74)
    print(f"无异常: {len(clean)}    检出异常: {len(found)}    抛异常: {len(errored)}")
    print("=" * 74)
    if errored:
        print("\n⚠️  以下 checker 抛异常(需要排查):")
        for label, _, e in errored:
            print(f"    - {label}: {type(e).__name__}: {e}")
    if found:
        print("\n检出异常的 checker:")
        for label, _, r in found:
            print(f"    - {label}: {getattr(r, 'describe', '')}")
        print("  (对照本次作业的 --fault 判断是否命中预期; 跑 normal 基线时这里非空才是误报)")

    sys.exit(1 if errored else 0)


if __name__ == "__main__":
    main()
