"""
采样 YARN 节点的**真实物理内存占用** —— 用来验证"内核 OOM"能不能被 REST 检出

背景: 内核 OOM 杀掉的容器, 在 removeReason 里和外部 `kill -9` **完全同形**
(`Killed by external signal`, 没有任何内存文本)。要把这类"YARN 看不见的 OOM"检出来,
线索不在容器上, 而在**节点**上:

  /ws/v1/cluster/nodes  ->  resourceUtilization.nodePhysicalMemoryMB
                             ↑ NM 实测的整机物理内存占用(不是按容器分配量算的账)
  /ws/v1/cluster/nodes  ->  usedMemoryMB
                             ↑ 按容器分配量算的 YARN 账 —— 内存是在容器账外被吃掉的,
                               所以这个值在内核 OOM 时**照样很低**

命题: 内核 OOM 发生的那一刻, 前者顶到接近节点真实内存, 后者仍然低。

为什么单独写一个: 这个字段是个**瞬时值**, 没有历史 —— 不在那一刻采就永远拿不到。
所以本脚本密采(默认 2 秒)并**如实记录失败**(读不到就标 valid=false, 绝不跳过)。

用法:
    python -u tests/probe_node_memory.py [--interval 2] [--duration 600]
输出: tmp/node-memory-<时间戳>.jsonl + stdout 每个采样点一行
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

from Tools.share_check_tool import rm      # noqa: E402  (RM 的 REST 客户端)


def snapshot() -> dict:
    """采一次; 失败要如实记成 valid=false 并带上原因"""
    rec = {"t": datetime.datetime.now().strftime("%H:%M:%S")}
    try:
        data = rm.cluster_nodes().data
    except Exception as e:
        rec["valid"] = False
        rec["reason"] = f"{type(e).__name__}: {e}"
        return rec
    nodes = (data or {}).get("nodes", {}).get("node", [])
    if not nodes:
        rec["valid"] = False
        rec["reason"] = "响应里没有 nodes 字段"
        return rec
    rec["valid"] = True
    rec["nodes"] = [
        {
            "id": n.get("id"),
            "state": n.get("state"),
            "healthReport": (n.get("healthReport") or "")[:120],
            # ★ 要验的那个: NM 实测的整机物理内存占用(MB)
            "nodePhysicalMemoryMB": (n.get("resourceUtilization") or {}).get("nodePhysicalMemoryMB"),
            # 对照: 按容器分配量算的 YARN 账 —— 预期它在内核 OOM 时仍然很低
            "usedMemoryMB": n.get("usedMemoryMB"),
            "availMemoryMB": n.get("availMemoryMB"),
            "memUtilization": n.get("memUtilization"),
            "numContainers": n.get("numContainers"),
        }
        for n in nodes
    ]
    return rec


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--interval", type=float, default=2.0)
    ap.add_argument("--duration", type=float, default=600.0)
    ap.add_argument("--out", default=None)
    args = ap.parse_args()

    out_path = pathlib.Path(args.out) if args.out else (
        PROJECT_ROOT / "tmp" / f"node-memory-{datetime.datetime.now():%H%M%S}.jsonl")
    print(f"节点内存采样: 每 {args.interval}s 一次, 共 {args.duration}s -> {out_path}")
    print(f"{'时刻':<10} {'节点':<18} {'物理占用MB':>10} {'YARN账usedMB':>13} {'容器数':>6} {'状态'}")
    print("-" * 74)

    started = time.time()
    n_valid = n_invalid = 0
    peak = {}          # node -> 见过的最大 nodePhysicalMemoryMB

    with open(out_path, "w", encoding="utf-8") as fh:
        while time.time() - started < args.duration:
            rec = snapshot()
            rec["elapsed"] = round(time.time() - started, 1)
            fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
            fh.flush()
            if not rec["valid"]:
                n_invalid += 1
                print(f"{rec['t']:<10} **测量无效**: {rec['reason']}")
            else:
                n_valid += 1
                for n in rec["nodes"]:
                    phys = n["nodePhysicalMemoryMB"]
                    peak[n["id"]] = max(peak.get(n["id"], 0), phys or 0)
                    print(f"{rec['t']:<10} {n['id']:<18} {phys!s:>10} {n['usedMemoryMB']!s:>13} "
                          f"{n['numContainers']!s:>6} {n['state']}")
            time.sleep(args.interval)

    print("-" * 74)
    print(f"采样结束: 有效 {n_valid} 次, 无效 {n_invalid} 次")
    print("各节点见过的最大物理内存占用(MB):")
    for k, v in sorted(peak.items()):
        print(f"  {k:20} {v}")
    print("对照节点真实内存总量(MemTotal, 需另取): node1≈4779MB, node2/node3≈3267MB")
    if n_invalid:
        print(f"⚠️  有 {n_invalid} 次采样是**测量无效** —— 结论里必须排除, 不能当成'没有数据'")
    print(f"明细: {out_path}")


if __name__ == "__main__":
    main()
