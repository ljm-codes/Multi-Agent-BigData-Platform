"""
量 Spark 数据倾斜判定的**噪声地板**与**响应曲线**, 用来定阈值

背景: `check_spark_data_skew` 拿 shuffle 读记录数的 max/median 当倾斜比, 阈值写死
2.0 / 5.0 —— 但这两个数是拍出来的, 没有依据。而 PySpark 下 `recordsRead` 记的是
**序列化批次**不是条数(README 已知坑第 2 条), 所以「真实倾斜比 → 检出的比值」
这个映射噪声很大。不量出来就定阈值, 定多高都是猜。

跑法(Windows 侧):
    python tests/skew_threshold_sweep.py

每轮: 提交 -> 等 jobs[0] 跑完 -> 抓 stage 详情 -> 算每个 stage 的 max/median。
最后打出汇总表, 用来定 warning / error 两条线。

    normal ×3          量噪声地板(健康作业能跑到多少)
    skew 3:1 / 5:1 / 10:1 / 20:1   量响应曲线(真实倾斜比 → 检出比值)

skew 的真实比靠 --hot-records 调(cold 固定 40000, 热点分区独占 hot_records 条)。
submit.sh 末尾会把额外参数透传给作业, 重复的 argparse 参数取最后一个, 所以直接
追加 --hot-records 就能覆盖。
"""
import argparse
import json
import pathlib
import re
import shlex
import shutil
import statistics
import subprocess
import sys
import time

PROJECT_ROOT = pathlib.Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

import requests

N1 = "/tmp/n1.sh"     # ssh 到 node1 的助手脚本(见 tests/spark/README 或 /tmp/n1.sh)
PROXY = "http://node1:8089"
COLD = 40000          # submit.sh 里 skew 的默认冷分区记录数

# 从 Windows 的 Python 里调这两个东西都有坑, 必须显式走 git-bash:
#   1. subprocess 直接调 "timeout" 会解析到 **Windows 自带的 timeout.exe**(参数语法
#      完全不同, 报 "Default option is not allowed more than '1' time(s)"),
#      所以超时一律用 subprocess 自己的 timeout 参数, 不调外部 timeout。
#   2. "/tmp/n1.sh" 是 git-bash 的路径, Windows 的 CreateProcess 找不到, 必须让
#      bash 去解析。
BASH = shutil.which("bash") or r"D:\Git\Git\usr\bin\bash.EXE"


def n1(cmd: str, timeout: int = 300):
    """在 node1 上跑一条命令(经 /tmp/n1.sh 的 ssh 助手)"""
    return subprocess.run([BASH, "-c", f"/tmp/n1.sh {shlex.quote(cmd)}"],
                          capture_output=True, text=True, timeout=timeout)

RUNS = [
    ("normal", "健康基线", []),
    ("normal", "健康基线", []),
    ("normal", "健康基线", []),
    ("skew", "真实比 3:1", ["--hot-records", str(COLD * 3)]),
    ("skew", "真实比 5:1", ["--hot-records", str(COLD * 5)]),
    ("skew", "真实比 10:1", ["--hot-records", str(COLD * 10)]),
    ("skew", "真实比 20:1", ["--hot-records", str(COLD * 20)]),
]


def submit(fault: str, extra: list) -> str:
    cmd = "cd /home/hadoop/spark-fault && bash submit.sh " + fault + " " + " ".join(extra)
    try:
        r = n1(cmd, timeout=420)
    except subprocess.TimeoutExpired:
        print("      ⚠️  提交超时")
        return ""
    out = r.stdout
    if not out.strip():
        print(f"      ⚠️  提交无输出, stderr: {r.stderr.strip()[:200]}")
    ids = re.findall(r"application_\d+_\d+", out)
    return ids[-1] if ids else ""


def kill(app_id: str) -> None:
    if app_id:
        try:
            n1(f"yarn application -kill {app_id}", timeout=90)
        except subprocess.TimeoutExpired:
            pass


def wait_job_done(app_id: str, timeout: int = 900) -> bool:
    """等到 jobs[0] 不再是 RUNNING(或超时)"""
    base = f"{PROXY}/proxy/{app_id}/api/v1/applications/{app_id}"
    started = time.time()
    while time.time() - started < timeout:
        try:
            r = requests.get(f"{base}/jobs", timeout=10)
            jobs = r.json()
            if jobs:
                running = [j for j in jobs if j.get("status") == "RUNNING"]
                if not running:
                    print(f"      就绪: jobs[0]=job{jobs[0].get('jobId')} "
                          f"({jobs[0].get('status')}), 等了 {time.time() - started:.0f}s")
                    return True
        except Exception:
            pass          # 代理过渡期返回 HTML / 应用还没起来, 继续等
        time.sleep(10)
    print(f"      ⚠️  等作业完成超时({timeout}s)")
    return False


def stage_ratios(app_id: str) -> list:
    """算出该应用 jobs[0] 每个 stage 的 max/median, 以及健康时的样子"""
    base = f"{PROXY}/proxy/{app_id}/api/v1/applications/{app_id}"
    jobs = requests.get(f"{base}/jobs", timeout=15).json()
    if not jobs:
        return []
    stage_ids = jobs[0].get("stageIds", [])
    out = []
    for sid in stage_ids:
        try:
            attempts = requests.get(f"{base}/stages/{sid}/",
                                    params={"details": True}, timeout=20).json()
        except Exception:
            continue
        if not attempts:
            continue
        if not isinstance(attempts, list):
            attempts = [attempts]
        latest = max((a for a in attempts if isinstance(a, dict)),
                     key=lambda a: a.get("attemptId", 0), default=None)
        if not latest:
            continue
        tasks = latest.get("tasks", {})
        vals = []
        for tv in (tasks.values() if isinstance(tasks, dict) else tasks):
            rr = (tv.get("taskMetrics", {}) or {}).get("shuffleReadMetrics", {}) or {}
            vals.append(rr.get("recordsRead", 0))
        vals = [v for v in vals if v is not None]
        if not vals or max(vals) == 0 or statistics.median(vals) == 0:
            out.append((sid, latest.get("attemptId"), None, len(vals),
                        max(vals) if vals else 0, statistics.median(vals) if vals else 0))
            continue
        out.append((sid, latest.get("attemptId"), max(vals) / statistics.median(vals),
                    len(vals), max(vals), statistics.median(vals)))
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", help="只跑匹配这个子串的轮次(按标签过滤)")
    args = ap.parse_args()

    results = []
    prev = ""
    for fault, label, extra in RUNS:
        if args.only and args.only not in label:
            continue
        kill(prev)
        time.sleep(5)
        print(f"\n{'=' * 70}\n{fault} / {label}  {extra}\n{'=' * 70}")
        app = submit(fault, extra)
        if not app:
            print("  ❌ 提交失败")
            prev = ""
            continue
        print(f"  app_id = {app}")
        ok = wait_job_done(app)
        ratios = stage_ratios(app) if ok else []
        if not ratios:
            print("  ⚠️  没拿到 stage 数据")
        for sid, aid, ratio, n, mx, med in ratios:
            if ratio is None:
                print(f"    stage {sid}.{aid}: 无法计算({n} 个 task, max={mx}, median={med})")
            else:
                print(f"    stage {sid}.{aid}: 倾斜比 = {ratio:.2f}   "
                      f"({n} 个 task, max={mx}, median={med})")
        results.append((fault, label, app, ratios))
        prev = app
    kill(prev)

    print(f"\n\n{'=' * 70}\n汇总\n{'=' * 70}")
    print(f"{'轮次':22} {'stage':10} {'倾斜比':>8}")
    for fault, label, app, ratios in results:
        if not ratios:
            print(f"{label:22} {'-':10} {'无数据':>8}")
            continue
        for sid, aid, ratio, n, mx, med in ratios:
            r = f"{ratio:.2f}" if ratio is not None else "n/a"
            print(f"{label:22} {f'{sid}.{aid}':10} {r:>8}")

    baseline = [r for f, l, a, rs in results if f == "normal" for _, _, r, *_ in rs if r]
    skews = [(l, r) for f, l, a, rs in results if f == "skew" for _, _, r, *_ in rs if r]
    if baseline:
        print(f"\n健康基线的倾斜比: {[f'{x:.2f}' for x in baseline]}")
        print(f"  -> 噪声地板约 {min(baseline):.2f} ~ {max(baseline):.2f}")
    if skews:
        print("\n真实倾斜比 -> 检出比值:")
        for l, r in skews:
            print(f"  {l:14} -> {r:.2f}")


if __name__ == "__main__":
    main()
