"""
按固定间隔采样 executor 峰值内存与 stage 溢写指标 —— 专治「静默测不到」

为什么不用 check_spark_live.py 的 --dump: 那个只在跑 checker 前抓**一次快照**。
P2 的争议恰恰在于峰值可能落在某个没被测到的窗口里 —— 上次就是这么把「读取失败」
当成了「没有数据」, 于是得出了后来被推翻的结论。所以这个探针要:

  * **密**: 默认每 5 秒一次(而不是一个作业一次);
  * **诚实**: 读失败 / 响应不是 JSON 一律**显式记成 `valid=false` 并带上原因**,
    绝不跳过 —— 每条记录都带 HTTP 状态码和 Content-Type, 事后能分清
    「真的全 0」和「那一次根本没读到」;
  * **全程**: 从应用起来一直探到它结束之后(`--duration`), 不预设峰值什么时候出现。

用法:
    python -u tests/probe_spark_peak.py --app <app_id> [--interval 5] [--duration 1500]
输出:
    tmp/peak-probe-<app_id>.jsonl   一行一个采样点
    stdout                          每个采样点一行摘要 + 结束时的汇总(便于边跑边看)
"""
import argparse
import datetime
import json
import pathlib
import sys
import time

PROJECT_ROOT = pathlib.Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

import requests


def ts() -> str:
    return datetime.datetime.now().strftime("%H:%M:%S")


def fetch(session, url: str) -> dict:
    """
    取一次并**如实记录**: 成功/失败/不是 JSON 三种情况都要落到记录里

    这里绝不 return None 了事 —— 探测失败必须是「测量无效」这个结论本身,
    而不是被下游当成「没有数据」。
    """
    rec = {"url": url}
    try:
        r = session.get(url, timeout=10)
        rec["http"] = r.status_code
        rec["content_type"] = r.headers.get("Content-Type", "")
        try:
            rec["data"] = r.json()
            rec["valid"] = True
        except ValueError as e:
            # 应用结束后 YARN 代理返回 200 + "Moved" 重定向 HTML, 状态码仍是 200 ——
            # 只看状态码会以为读到了
            rec["valid"] = False
            rec["reason"] = f"响应不是 JSON: {type(e).__name__}"
    except Exception as e:
        rec["valid"] = False
        rec["reason"] = f"{type(e).__name__}: {e}"
    return rec


def summarize(rec: dict) -> dict:
    """把原始响应压成关心的几个量(仍然保留 valid 标志)"""
    out = {"valid": rec.get("valid", False)}
    if not out["valid"]:
        out["reason"] = rec.get("reason")
        out["http"] = rec.get("http")
        out["content_type"] = rec.get("content_type")
        return out
    data = rec["data"]
    tag = "executors" if "allexecutors" in rec["url"] else "stages"
    if tag == "executors":
        out["executors"] = [
            {
                "id": e.get("id"),
                "isActive": e.get("isActive"),
                "maxMemory": e.get("maxMemory"),
                # 就是 P2 条件一的分子
                "onHeapExecution": (e.get("peakMemoryMetrics") or {}).get("OnHeapExecutionMemory"),
                "jvmHeap": (e.get("peakMemoryMetrics") or {}).get("JVMHeapMemory"),
                # 非 0 的键单独列出来 —— 二十个键全 0 还是有值, 一眼能看出来
                "nonzeroPeaks": {k: v for k, v in (e.get("peakMemoryMetrics") or {}).items() if v},
            }
            for e in data
            if isinstance(e, dict)
        ]
    else:
        out["stages"] = [
            {
                "stageId": s.get("stageId"),
                "attemptId": s.get("attemptId"),
                "status": s.get("status"),
                "numTasks": s.get("numTasks"),
                # P2 条件二
                "diskBytesSpilled": s.get("diskBytesSpilled"),
                # 条件一的另一个来源: stage 级的执行内存峰值
                "peakExecutionMemory": s.get("peakExecutionMemory"),
                "shuffleWriteBytes": s.get("shuffleWriteBytes"),
                "executorRunTime": s.get("executorRunTime"),
            }
            for s in data
            if isinstance(s, dict)
        ]
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--app", required=True)
    ap.add_argument("--proxy", default="http://node1:8089")
    ap.add_argument("--interval", type=float, default=5.0, help="采样间隔秒数")
    ap.add_argument("--duration", type=float, default=1500.0, help="总共探多少秒")
    ap.add_argument("--out", default=None, help="输出 jsonl 路径, 默认 tmp/peak-probe-<app>.jsonl")
    args = ap.parse_args()

    rest = f"{args.proxy}/proxy/{args.app}/api/v1/applications/{args.app}"
    out_path = pathlib.Path(args.out) if args.out else PROJECT_ROOT / "tmp" / f"peak-probe-{args.app}.jsonl"

    print(f"探针: {args.app}  每 {args.interval}s 一次, 共 {args.duration}s")
    print(f"输出: {out_path}")
    print("-" * 74)

    session = requests.Session()
    started = time.time()
    n_valid = n_invalid = 0
    best_onheap = 0
    best_spill = 0
    last_alive = None

    with open(out_path, "w", encoding="utf-8") as fh:
        while time.time() - started < args.duration:
            sample = {"t": ts(), "elapsed": round(time.time() - started, 1)}
            ex = fetch(session, f"{rest}/allexecutors")
            st = fetch(session, f"{rest}/stages")
            sample["allexecutors"] = summarize(ex)
            sample["stages"] = summarize(st)
            # 应用是否还活着: 两个都读不成就基本是代理回落成 HTML 了
            sample["app_responsive"] = bool(ex.get("valid") or st.get("valid"))
            fh.write(json.dumps(sample, ensure_ascii=False) + "\n")
            fh.flush()

            if sample["app_responsive"]:
                n_valid += 1
                last_alive = sample["elapsed"]
            else:
                n_invalid += 1

            # 边跑边算关心的两个峰值
            if ex.get("valid"):
                for e in sample["allexecutors"].get("executors", []):
                    best_onheap = max(best_onheap, e.get("onHeapExecution") or 0)
            if st.get("valid"):
                for s in sample["stages"].get("stages", []):
                    best_spill = max(best_spill, s.get("diskBytesSpilled") or 0)

            onheap_now = "-"
            if ex.get("valid"):
                vals = [(e.get("id"), e.get("onHeapExecution") or 0) for e in sample["allexecutors"]["executors"]]
                onheap_now = ",".join(f"{i}:{v / 1024 / 1024:.1f}M" for i, v in vals)
            spill_now = "-"
            if st.get("valid"):
                spill_now = ",".join(
                    f"{s.get('stageId')}:{(s.get('diskBytesSpilled') or 0) / 1024 / 1024:.2f}M"
                    for s in sample["stages"]["stages"]
                ) or "无 stage"

            print(f"[{sample['t']:<8} {sample['elapsed']:>6.1f}s] "
                  f"{'响应' if sample['app_responsive'] else '**测量无效(应用已结束/代理回落HTML)**'} "
                  f"exec峰值内存 {onheap_now} | spill {spill_now}")

            time.sleep(args.interval)

    print("-" * 74)
    print(f"采样结束: 有效 {n_valid} 次, 无效 {n_invalid} 次, 应用最后响应于 {last_alive}s")
    print(f"全程见过的最大 OnHeapExecutionMemory = {best_onheap / 1024 / 1024:.2f} MB"
          f"   (P2 条件一要它 / maxMemory >= 0.9)")
    print(f"全程见过的最大 diskBytesSpilled      = {best_spill / 1024 / 1024:.2f} MB"
          f"   (P2 条件二要它 > 0)")
    if n_invalid:
        print(f"⚠️  有 {n_invalid} 次采样是**测量无效** —— 结论里必须把这部分排除, 不能当成「没有数据」")
    print(f"明细: {out_path}")


if __name__ == "__main__":
    main()
