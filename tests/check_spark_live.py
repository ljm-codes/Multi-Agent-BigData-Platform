"""
对真实 Spark 应用跑一遍全部 Spark checker —— 端到端验证

走 YARN 的 WebAppProxy (node1:8089/proxy/{app_id}), 和 checker 现在的取数路径完全一致。
健康作业上各 checker 应当**全部返回 None**(零误报); 有故障时对应 checker 应检出。

用法:
    python tests/check_spark_live.py                       # 自动发现 YARN 上 RUNNING 的 SPARK 应用
    python tests/check_spark_live.py --app application_xxx_0003
    python tests/check_spark_live.py --app <id> --job-timeout-sec 60
    python tests/check_spark_live.py --app <id> --dump     # 顺带把原始 JSON 存到 tmp/

关于 --job-timeout-sec:
    check_spark_job_timeout 要求作业已运行超过 max_job_timeout(代码里写死 30 分钟)才报,
    验证时把它调小, 否则要干等半小时。它还要连着调两次(第一次只写快照, 返回 False),
    所以这个 checker 会自动跑两遍。
"""
import argparse
import importlib
import json
import pathlib
import sys
import time
import traceback

PROJECT_ROOT = pathlib.Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

import requests

ENTRIES = [
    ("数据倾斜", "Tools.spark.check_spark_data_skew", "check_spark_data_skew"),
    ("OOM", "Tools.spark.check_spark_OOM", "check_OOM"),
    ("网络超时", "Tools.spark.check_spark_network_timeout", "network_timeout_check"),
    ("其他异常", "Tools.spark.check_other_spark_abnormal", "check_other"),
    ("任务超时", "Tools.check_job_timeout", "check_spark_job_timeout"),
]

# 需要连着调两次的 checker(靠 redis 存前后两次的任务数快照)
NEEDS_TWO_PASSES = {"check_spark_job_timeout"}


def discover_spark_apps() -> list:
    """从 YARN 发现正在运行的 Spark 应用"""
    from Tools.share_check_tool import get_all_running_apps
    apps = get_all_running_apps()
    out = []
    for app in apps:
        app_id = app.get("id", "")
        if not app_id:
            continue
        app_type = app.get("applicationType", "")
        print(f"  id={app_id}  name={app.get('name')!r}  applicationType={app_type!r}")
        if app_type == "SPARK":
            out.append(app_id)
    return out


def dump_raw(app_id: str, proxy: str, out_dir: pathlib.Path) -> None:
    """
    抓原始 JSON 存盘 —— 排查「字段名/路径对不对」时看这个, 比读 checker 代码快

    重点看两处:
      /stages/{id}/?details=true  —— checker 用的就是这个路径(注意没有 attemptId),
                                     要看返回里到底有没有 tasks 字段
      /allexecutors               —— removeReason 的实际文本、OnHeapExecutionMemory
                                     与 maxMemory 的实际比例
    """
    base = f"{proxy}/proxy/{app_id}/api/v1/applications/{app_id}"
    targets = [
        ("jobs", f"{base}/jobs"),
        ("allexecutors", f"{base}/allexecutors"),
        ("executors", f"{base}/executors"),
        ("stages", f"{base}/stages"),
    ]
    # 这里必须自己兜住异常: 应用结束后 YARN 代理不再转发, 而是返回一个 200 +
    # "Moved" 重定向 HTML(指向 RM 页面)。.json() 会抛 JSONDecodeError, 而 --dump
    # 在跑 checker 之前执行 —— 不兜住的话整个脚本挂掉, 一个 checker 结果都拿不到。
    app_info = requests.get(base, timeout=10)
    if app_info.status_code == 200:
        try:
            info = app_info.json()
            print(f"  应用: {info.get('name')!r} attempts={len(info.get('attempts', []))}")
        except ValueError:
            print("  ⚠️  应用根路径返回的不是 JSON(多半是应用已结束, 代理回落到 YARN 的"
                  "重定向 HTML, 状态码仍是 200) —— 后续 checker 会全部返回 None, 结果无意义")

    for name, url in targets:
        try:
            r = requests.get(url, timeout=15)
            if r.status_code != 200:
                print(f"  {name}: HTTP {r.status_code}")
                continue
            data = r.json()
            path = out_dir / f"spark-{app_id}-{name}.json"
            path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
            n = len(data) if isinstance(data, list) else "-"
            print(f"  {name}: {n} 条 -> {path.name}")
            if name == "jobs" and isinstance(data, list) and data:
                print(f"      jobId 顺序: {[j.get('jobId') for j in data]}")
                print(f"      jobs[0] = jobId {data[0].get('jobId')} status={data[0].get('status')}")
        except Exception as e:
            print(f"  {name}: {type(e).__name__}: {e}")

    # 阶段详情: 两种路径都抓, 对比有无 tasks 字段
    try:
        stages = requests.get(f"{base}/stages", timeout=15).json()
        active = [s for s in stages if s.get("status") in ("ACTIVE", "RUNNING")] or stages
        if active:
            s0 = active[0]
            sid, aid = s0.get("stageId"), s0.get("attemptId")
            for tag, url in [
                (f"stage-{sid}-{aid}-withAttempt", f"{base}/stages/{sid}/{aid}?details=true"),
                (f"stage-{sid}-noAttempt", f"{base}/stages/{sid}/?details=true"),
            ]:
                r = requests.get(url, timeout=15)
                if r.status_code != 200:
                    print(f"  {tag}: HTTP {r.status_code}")
                    continue
                data = r.json()
                # 接口对不同路径返回的包装不一样: 有时是单个对象, 有时是 [{...}]
                obj = data[0] if isinstance(data, list) else data
                obj = obj[0] if isinstance(obj, list) and obj else obj
                path = out_dir / f"spark-{app_id}-{tag}.json"
                path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
                has_tasks = isinstance(obj, dict) and "tasks" in obj
                print(f"  {tag}: type={type(data).__name__} tasks={has_tasks} -> {path.name}")
                if has_tasks:
                    tasks = obj["tasks"]
                    tk = list(tasks)[0] if isinstance(tasks, dict) else 0
                    tv = tasks[tk] if isinstance(tasks, dict) else tasks[0]
                    print(f"      task[{tk}] 字段: {sorted(tv.keys())}")
                    print(f"      status={tv.get('status')!r} errorMessage={'有' if tv.get('errorMessage') else '无'}")
    except Exception as e:
        print(f"  阶段详情: {type(e).__name__}: {e}")


def app_is_live(app_id: str, proxy: str) -> bool:
    """
    应用是否还查得到 —— 跑 checker 之前必须确认

    应用结束后 YARN 代理不再转发到 AM, 而是返回 **200 + 一个 "Moved" 重定向 HTML**
    (指向 RM 页面)。此时每个 checker 的 request_get 都拿到 HTML、解析失败、返回 None,
    跑出来的「没检出」看着和「真的没异常」一模一样。这个坑已经让 spill 白跑过一轮,
    所以宁可多一次探测。
    """
    url = f"{proxy}/proxy/{app_id}/api/v1/applications/{app_id}"
    try:
        r = requests.get(url, timeout=10)
        if r.status_code != 200:
            return False
        r.json()          # 返回 HTML 时这里抛 ValueError
        return True
    except Exception:
        return False


def failed_stage_attempts(rest: str, app_id: str) -> list:
    """
    从原始 REST 里找 FAILED 的 stage attempt —— **只用来卡时机, 不参与判定**

    netfail 这类故障的证据是「失败过」而不是「跑完了」: 提交后要等它真的失败一次,
    太早跑 checker 会全部返回 None, 和「真的没异常」分不开。

    之所以不直接调 checker 的判定函数来等, 是免得变成循环论证 ——
    「等到 checker 自己说命中, 再宣布 checker 命中」。这里只看原始 status 字段。

    :return: [(stage_id, attempt_id, failureReason), ...]
    """
    from Tools.share_check_tool import get_stage_info
    jobs = requests.get(f"{rest}/jobs", timeout=10).json()
    stage_ids = sorted({sid for j in jobs for sid in j.get("stageIds", [])})
    out = []
    for stage_id in stage_ids:
        infos = get_stage_info(stage_id, app_id)
        if not infos:
            continue
        if not isinstance(infos, list):
            infos = [infos]
        for info in infos:
            if isinstance(info, dict) and info.get("status") == "FAILED":
                reason = (info.get("failureReason") or "").replace("\n", " ")
                out.append((stage_id, info.get("attemptId"), reason))
    return out


def removed_executors(rest: str, app_id: str) -> list:
    """
    从原始 REST 里找**已经被移除的 executor** —— 只用来卡时机, 不参与判定

    oom_limit 这类容器层故障的证据是「有容器被 YARN 杀掉过」, 而容器越界要等堆真的涨起来,
    太早跑 checker 会全部返回 None, 和「真的没异常」分不开。

    同样**不调 checker 的判定函数**(和 failed_stage_attempts 一个道理), 只看原始
    `isActive` 字段 —— 免得变成「等到 checker 自己说命中, 再宣布 checker 命中」的循环论证。

    注意**不能按内存文本筛**: 那正是 checker 要判的东西。这里只问「有没有死的」,
    死因留给 checker 去分档 —— 否则又成了拿结论等结论。

    :return: [(executor_id, hostPort, removeReason), ...]
    """
    data = requests.get(f"{rest}/allexecutors", timeout=10).json()
    out = []
    for e in data:
        if e.get("isActive", True):
            continue
        out.append((e.get("id"), e.get("hostPort"),
                    (e.get("removeReason") or "").replace("\n", " ")))
    return out


def run_one(func_name: str, label: str, func, app_id: str):
    start = time.perf_counter()
    try:
        result = func(app_id)
        elapsed = time.perf_counter() - start
        if result is None or result == []:
            print(f"  [  --  ] {label:24} 无异常   ({elapsed:.2f}s)")
            return (label, "clean", None)
        data = getattr(result, "data", None)
        n = len(data) if isinstance(data, (list, dict)) else "-"
        print(f"  [FOUND] {label:24} 检出!   ({elapsed:.2f}s)")
        print(f"           type={getattr(result, 'type', '')!r}")
        print(f"           describe={getattr(result, 'describe', '')!r}")
        print(f"           data {n} 条: {repr(data)[:600]}")
        return (label, "found", result)
    except Exception as e:
        elapsed = time.perf_counter() - start
        print(f"  [ERROR] {label:24} {type(e).__name__}: {e}   ({elapsed:.2f}s)")
        for line in traceback.format_exc().splitlines()[-4:-1]:
            print(f"           {line.strip()}")
        return (label, "error", e)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--proxy", default="http://node1:8089", help="YARN WebAppProxy 地址")
    ap.add_argument("--app", help="指定 YARN application id, 不填则自动发现")
    ap.add_argument("--wait", type=int, default=0,
                    help="先等应用就绪(Spark UI 可经代理访问)的秒数, 刚提交完的作业用得上")
    ap.add_argument("--job-timeout-sec", type=int, default=None,
                    help="覆盖 check_spark_job_timeout 的 max_job_timeout(默认 1800 秒)")
    ap.add_argument("--timeout-gap", type=float, default=20.0,
                    help="任务超时 checker 两次调用之间的间隔秒数")
    ap.add_argument("--dump", action="store_true", help="抓原始 JSON 到 tmp/")
    ap.add_argument("--wait-job-done", action="store_true",
                    help="等到 jobs[0] 不再是 RUNNING 再跑 checker(长调度作业用, 如 spill)。"
                         "此时 --wait 要设得足够大(900 起步)")
    ap.add_argument("--wait-failed-stage", action="store_true",
                    help="等到出现 FAILED 的 stage attempt 再跑 checker —— 失败类故障用"
                         "(如 netfail: 它要等 shuffle 数据丢了之后的那次失败), 不然跑早了"
                         "全是 None。和 --wait-job-done 一样要配大 --wait")
    ap.add_argument("--wait-removed-executor", action="store_true",
                    help="等到出现 inactive 的 executor 再跑 checker —— 容器类故障用"
                         "(如 oom_limit: 容器越界要等堆涨起来)。只看原始 isActive 字段, "
                         "不看死因, 死因留给 checker 判")
    args = ap.parse_args()

    # checker 的基址由 share_check_tool.proxy 决定, 必须在 import checker 之前改
    import Tools.share_check_tool as sct
    sct.proxy = args.proxy

    print("=" * 74)
    print(f"对真实 Spark 应用跑 checker   代理: {args.proxy}")
    print("=" * 74)

    if args.app:
        app_ids = [args.app]
    else:
        print("\n从 YARN 发现 RUNNING 的 SPARK 应用:")
        app_ids = discover_spark_apps()

    if not app_ids:
        print("\n⚠️  没有发现 RUNNING 的 SPARK 应用 —— 提交一条再来")
        sys.exit(0)

    app_id = app_ids[0]
    base = f"{args.proxy}/proxy/{app_id}"
    # REST 基址必须带 /api/v1/applications/{app_id} —— 只写到 /proxy/{app_id} 的话
    # {base}/jobs 是 **Spark UI 的 HTML 页面**(也是 200), .json() 永远解析失败,
    # 就绪判断于是永远不成立、每次都干等到 --wait 用完才硬跑 checker。
    # 这个 bug 原先是有的, 表现就是每轮都打印「⚠️ 等超时, 继续跑(结果可能不准)」。
    rest = f"{base}/api/v1/applications/{app_id}"
    print(f"\n应用: {app_id}")
    print(f"基址: {base}")

    # 等应用就绪 —— 只等「UI 能通」是不够的: 故障作业还没跑起来时 checker 会全部
    # 返回 None, 看起来像「没检出」, 其实只是跑早了。所以还要等 job 列表非空。
    # 有意挂住的故障(stuck/oom_kill9)会一直 RUNNING, 超过 60 秒就不再等。
    deadline = time.time() + args.wait
    started = time.time()
    while True:
        try:
            r = requests.get(rest, timeout=10)
            if r.status_code == 200:
                rj = requests.get(f"{rest}/jobs", timeout=10)
                ctype = rj.headers.get("Content-Type", "").lower()
                try:
                    jobs = rj.json()
                except Exception:
                    jobs = None      # 代理过渡期会返回 200 + HTML, 这里当没就绪
                if jobs:
                    running = [j for j in jobs if j.get("status") == "RUNNING"]
                    print(f"  就绪: {len(jobs)} 个 job, RUNNING {len(running)} 个, "
                          f"jobs[0]=job{jobs[0].get('jobId')}({jobs[0].get('status')})")
                    if args.wait_job_done:
                        # 长调度作业用: 等到最新 job 跑完再跑 checker。spill 这类大聚合
                        # 从提交到作业真正开始要 ~9 分钟, 只等「job 列表非空」会在作业
                        # 还没起来时就跑 checker, 全返回 None —— 和「真的没异常」分不开。
                        if not running:
                            break
                    elif args.wait_removed_executor:
                        # 容器类故障用: oom_limit 要等容器**真的被 YARN 杀掉**(堆涨起来
                        # 需要时间)。同样**不设 60 秒提前退出** —— 还没被杀就该继续等,
                        # 提前跑等于把「还没被杀」当成「没检出」。
                        dead = removed_executors(rest, app_id)
                        if dead:
                            for eid, host, reason in dead:
                                print(f"  移除证据已出现: executor {eid} @ {host}: {reason[:140]}")
                            break
                        print(f"  等容器被移除... ({time.time() - started:.0f}s) "
                              f"还没有 inactive 的 executor")
                    elif args.wait_failed_stage:
                        # 失败类故障用: netfail 要等 shuffle 数据丢了之后那次失败。
                        # 这里**不设 60 秒提前退出** —— 失败还没发生就该继续等,
                        # 提前跑等于把「还没失败」当成「没检出」。
                        fails = failed_stage_attempts(rest, app_id)
                        if fails:
                            for sid, aid, reason in fails:
                                print(f"  失败证据已出现: stage {sid} attempt {aid}: {reason[:140]}")
                            break
                        print(f"  等失败证据... ({time.time() - started:.0f}s) "
                              f"还没有 FAILED 的 stage attempt")
                    elif not running or time.time() - started > 60:
                        break
                else:
                    # 应用根路径已通但 /jobs 还没内容。必须打印 —— 否则「等超时」和
                    # 「没检出」在日志里长得一样, 分不清是 checker 漏检还是作业没跑起来。
                    # 还要区分两种「没就绪」: 代理过渡期返回的是 HTML 页面(应用还没起来),
                    # 和真·空数组(应用起来了、还没跑 job)。实测从提交到 UI 可用要 3 分钟
                    # 以上, 别把这个当成 checker 的问题。
                    why = "代理过渡期, 返回的是 HTML" if "html" in ctype else "job 列表为空"
                    print(f"  等作业出现... ({time.time() - started:.0f}s) {why}")
            else:
                print(f"  等应用启动... ({time.time() - started:.0f}s) "
                      f"应用根路径 HTTP {r.status_code}")
        except Exception as e:
            print(f"  ❌ 代理不通(继续等): {type(e).__name__}: {e}")
        if time.time() >= deadline:
            print("  ⚠️  等超时, 继续跑(结果可能不准)")
            break
        time.sleep(5)

    if not app_is_live(app_id, args.proxy):
        print("\n" + "!" * 74)
        print("⚠️  应用已经不在运行了 —— 代理回落成 YARN 的重定向 HTML。")
        print("    下面所有 checker 都会静默返回 None, 那不代表「没异常」, 结果一律不可信。")
        print(f"    重跑: bash tests/spark/submit.sh <fault>  然后 --app <新app_id>")
        print("!" * 74)

    if args.dump:
        print("\n--- 原始 JSON ---")
        dump_raw(app_id, args.proxy, PROJECT_ROOT / "tmp")

    if args.job_timeout_sec is not None:
        import Tools.check_job_timeout as cjt
        cjt.max_job_timeout = args.job_timeout_sec
        print(f"\n(check_spark_job_timeout 的 max_job_timeout 临时改为 {args.job_timeout_sec}s)")

    print()
    print("-" * 74)
    print("跑 checker (健康作业应当全部返回 None)")
    print("-" * 74)

    results = []
    for label, mod_name, func_name in ENTRIES:
        mod = importlib.import_module(mod_name)
        func = getattr(mod, func_name)
        if func_name in NEEDS_TWO_PASSES:
            # 第一次只写快照, 必然返回 False; 隔一会儿再调一次才是判定
            run_one(func_name, f"{label}(第1次/写快照)", func, app_id)
            print(f"           ...等 {args.timeout_gap}s 再调一次")
            time.sleep(args.timeout_gap)
            results.append(run_one(func_name, f"{label}(第2次/判定)", func, app_id))
        else:
            results.append(run_one(func_name, label, func, app_id))

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
