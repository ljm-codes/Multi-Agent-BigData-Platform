"""
把一个 Spark 应用的 REST 表面**整个**抓下来存盘 —— 调查用

和 `check_spark_live.py --dump` 的区别: 那个是给验证 checker 用的, 只抓 jobs/stages/
allexecutors 和**一个** stage 的详情。调查"为什么这个指标是 0"时需要的是全量:

  * `/sql` 的 `planDescription` —— **算子是哪种、常量有没有被折叠**, 只能从这里看。
    (例: `collect_list` 是 TypedImperativeAggregate, 走 ObjectHashAggregateExec,
     缓冲区是普通 Java 对象、不经 TaskMemoryManager, 于是 peakExecutionMemory 恒为 0。
     这类结论必须看计划, 不能靠推理。)
  * **每个** stage 的每一次 attempt 的 task 级指标, 而不是只抓一个。

用法:
    python -u tests/capture_spark_app.py --app <app_id> [--out-dir tmp/capture-<app_id>]
"""
import argparse
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


def get(session, url: str):
    """取 JSON; 失败返回 (None, 原因) —— 调用方要把它记下来, 不能当成空数据"""
    try:
        r = session.get(url, timeout=15)
        if r.status_code != 200:
            return None, f"HTTP {r.status_code}"
        ct = r.headers.get("Content-Type", "")
        if "json" not in ct.lower():
            return None, f"Content-Type={ct!r}(多半是应用已结束、代理回落的 HTML)"
        return r.json(), None
    except Exception as e:
        return None, f"{type(e).__name__}: {e}"


def save(out_dir: pathlib.Path, name: str, data) -> None:
    (out_dir / name).write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--app", required=True)
    ap.add_argument("--proxy", default="http://node1:8089")
    ap.add_argument("--out-dir", default=None)
    args = ap.parse_args()

    rest = f"{args.proxy}/proxy/{args.app}/api/v1/applications/{args.app}"
    out_dir = pathlib.Path(args.out_dir) if args.out_dir else PROJECT_ROOT / "tmp" / f"capture-{args.app}"
    out_dir.mkdir(parents=True, exist_ok=True)
    session = requests.Session()

    print(f"抓取 {args.app} -> {out_dir}")
    problems = []

    for name in ("jobs", "stages", "allexecutors", "executors"):
        data, why = get(session, f"{rest}/{name}")
        if data is None:
            problems.append(f"{name}: {why}")
            print(f"  {name}: 抓取失败 —— {why}")
            continue
        save(out_dir, f"{name}.json", data)
        print(f"  {name}: {len(data) if isinstance(data, list) else '-'} 条")

    # SQL 执行计划 —— 调查指标为 0 的原因时这是关键证据
    sql_list, why = get(session, f"{rest}/sql")
    if sql_list is None:
        problems.append(f"sql: {why}")
        print(f"  sql: 抓取失败 —— {why}")
    else:
        save(out_dir, "sql-list.json", sql_list)
        print(f"  sql: {len(sql_list)} 条执行记录")
        for item in sql_list if isinstance(sql_list, list) else []:
            eid = item.get("id")
            plan = item.get("planDescription") or ""
            save(out_dir, f"sql-{eid}.json", item)
            # 计划里最该看的两件事: 聚合/排序算子是什么、有没有 ShuffleExchange
            ops = sorted({line.strip().split(" ")[0] for line in plan.splitlines() if line.strip().startswith(("ObjectHashAggregate", "HashAggregate", "Sort", "SortMergeJoin", "ShuffledHashJoin", "ShuffleExchange", "AdaptiveSparkPlan", "CollectLimit", "Generate", "Expand"))})
            print(f"    sql#{eid} 关键算子: {ops if ops else '(没识别到, 见 sql-{}.json)'.format(eid)}")

    # 每个 stage 的每一次 attempt 的 task 详情
    stages, why = get(session, f"{rest}/stages")
    if stages is not None:
        n = 0
        for s in stages:
            sid, aid = s.get("stageId"), s.get("attemptId")
            detail, why2 = get(session, f"{rest}/stages/{sid}/{aid}?details=true")
            if detail is None:
                problems.append(f"stage {sid}/{aid}: {why2}")
                continue
            save(out_dir, f"stage-{sid}-{aid}.json", detail)
            n += 1
        print(f"  stage 详情: {n} 个 attempt 抓到了")

    if problems:
        print("\n⚠️  以下抓取**失败**(不是「没有数据」):")
        for p in problems:
            print(f"    - {p}")
    print(f"\n完成: {out_dir}")


if __name__ == "__main__":
    main()
