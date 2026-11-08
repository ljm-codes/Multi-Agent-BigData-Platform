"""Rubric 评分：完成度/正确性/验证完整性/工具效率/体验/健壮性。"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from tests.system_eval.runner import CaseSpec


def _load(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _clamp(value: float) -> float:
    return max(0.0, min(5.0, round(value, 2)))


def _verification_reports(case_dir: Path) -> list[dict[str, Any]]:
    path = case_dir / "verification.jsonl"
    out = []
    if not path.exists():
        return out
    for line in path.read_text(encoding="utf-8").splitlines():
        try:
            rec = json.loads(line).get("record") or {}
        except Exception:
            continue
        if rec.get("report"):
            out.append(rec["report"])
    return out


def _latest_reports(reports: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """同一 stage 可能因为 shallow 失败重跑多次；评分只认最后一次报告。"""
    latest: dict[str, dict[str, Any]] = {}
    for report in reports:
        stage = str(report.get("stage") or "?")
        latest[stage] = report
    return latest


def _tool_stats(traces: list[dict[str, Any]]) -> dict[str, Any]:
    by_name: dict[str, int] = {}
    errors = 0
    max_ms = 0
    for trace in traces or []:
        name = str(trace.get("tool") or "?")
        by_name[name] = by_name.get(name, 0) + 1
        if str(trace.get("status")) == "error":
            errors += 1
        try:
            max_ms = max(max_ms, int(trace.get("duration_ms") or 0))
        except Exception:
            pass
    return {
        "count": len(traces or []),
        "errors": errors,
        "by_name": by_name,
        "max_duration_ms": max_ms,
    }


def _final_text(result: dict[str, Any]) -> str:
    done = result.get("done") or {}
    if isinstance(done, dict) and done.get("output_msg"):
        return str(done.get("output_msg"))
    histories = result.get("histories") or {}
    texts = []
    for hist in histories.values():
        if isinstance(hist, dict) and hist.get("output_msg"):
            texts.append(str(hist["output_msg"]))
    return texts[-1] if texts else ""


def _verdict(total: float, correctness: float) -> str:
    if total >= 3.5 and correctness >= 3:
        return "PASS"
    if total >= 2.5:
        return "PARTIAL"
    return "FAIL"


def _base(case_id: str, stage: str) -> dict[str, Any]:
    return {
        "case": case_id, "stage": stage,
        "dimensions": {}, "evidence": {}, "manual_review": {
            "notes": "", "observations": [],
        },
    }


def score_chat_case(spec: CaseSpec, case_dir: Path) -> dict[str, Any]:
    result = _load(case_dir / "result.json") or {}
    timeline = _load(case_dir / "timeline.json") or {}
    cluster = _load(case_dir / "cluster.json") or {}
    traces = _load(case_dir / "tool_traces.json") or []
    producer = _load(case_dir / "producer.json") or {}
    reports = _verification_reports(case_dir)
    latest_reports = _latest_reports(reports)
    tools = _tool_stats(traces)
    final = _final_text(result)
    status = result.get("status") or "unknown"
    error = result.get("error")
    handler_error = "无法处理结果返回的消息" in final or "无法处理结果返回的消息" in str(error or "")

    # 完成度
    completion = 5.0
    if status != "completed" or error:
        completion = 1.0 if error else 2.0
    if not final and not result.get("done") and not result.get("resumes"):
        completion = min(completion, 2.0)

    # 正确性
    verify = spec.meta.get("verify")
    correct = 3.0
    if verify in ("yarn", "running"):
        if cluster.get("pass"):
            correct = 5.0
        elif cluster.get("error"):
            correct = 1.0
        elif cluster.get("checks"):
            ok = sum(1 for c in cluster["checks"] if c.get("ok"))
            correct = _clamp(5.0 * ok / max(1, len(cluster["checks"])))
        else:
            correct = 2.0
    else:
        # 不提交的任务：验证报告 + 代码可读性；明确说“未提交”再加分
        if latest_reports and all((r.get("failed") or 0) == 0 for r in latest_reports.values()):
            correct = 4.0
        honest = any(k in final for k in ("未提交", "没有提交", "先不提交", "未运行", "未在集群"))
        if honest:
            correct = min(5.0, correct + 1.0)

    # 验证完整性：只看每个 stage 的最后一次报告，避免把重试失败重复扣分。
    # Maven 编译失败时 CVA 可能给出 total>0/passed=0/failed=0 的“空报告”，按失败算。
    verification = 3.0
    if latest_reports:
        failed = 0
        passed = 0
        for report in latest_reports.values():
            total = int(report.get("total") or 0)
            p = int(report.get("passed") or 0)
            f = int(report.get("failed") or 0)
            if total > 0 and p == 0 and f == 0:
                f = total
            passed += p
            failed += f
        if failed == 0 and passed > 0:
            verification = 5.0
        elif passed == 0 and failed > 0:
            verification = 1.5
        else:
            verification = _clamp(5.0 - failed)
    if result.get("resumes"):
        verification = min(5.0, verification + 0.5)

    # 工具效率
    tool_score = 5.0
    tool_score -= min(3, tools["errors"])
    if tools["count"] > 80:
        tool_score -= 1
    if tools["count"] > 150:
        tool_score -= 1
    if any(v > 20 for v in tools["by_name"].values()):
        tool_score -= 1
    if tools["max_duration_ms"] > 300_000:
        tool_score -= 1
    tool_score = _clamp(tool_score)

    # 体验/延迟
    ux = 5.0
    ttfb = timeline.get("ttfb_ms")
    total_ms = timeline.get("total_ms") or 0
    first_final = timeline.get("first_final_token_ms")
    replay_ms = (total_ms - first_final) if first_final is not None else 0
    if ttfb is not None and ttfb > 30_000:
        ux -= 1
    if total_ms > 1_800_000:
        ux -= 1
    if not timeline.get("tool_status"):
        ux -= 1
    if len(final) < 80:
        ux -= 1
    if replay_ms > 60_000:
        ux -= 1
    ux = _clamp(ux)

    # 健壮性
    robustness = 5.0
    if error:
        robustness = 1.0
    elif status != "completed":
        robustness = 2.0
    elif latest_reports and any((r.get("failed") or 0) > 0 for r in latest_reports.values()):
        robustness = 4.0
    elif result.get("resumes"):
        robustness = 4.5
    robustness = _clamp(robustness)

    if handler_error:
        # CVA 自己把命令结果当“无法处理的消息”结束：这是一次失败，不是完成
        completion = min(completion, 2.0)
        correct = min(correct, 1.5)
        verification = min(verification, 2.0)
        robustness = min(robustness, 1.5)

    dims = {
        "completion": completion, "correctness": correct,
        "verification": verification, "tool_efficiency": tool_score,
        "ux": ux, "robustness": robustness,
    }
    total = _clamp(
        completion * 0.2 + correct * 0.3 + verification * 0.2
        + tool_score * 0.1 + ux * 0.1 + robustness * 0.1
    )
    out = _base(spec.id, spec.stage)
    out.update({
        "dimensions": dims,
        "total": total,
        "verdict": _verdict(total, correct),
        "evidence": {
            "status": status,
            "error": error,
            "final_len": len(final),
            "ttfb_ms": ttfb,
            "total_ms": total_ms,
            "first_final_token_ms": first_final,
            "replay_ms": replay_ms,
            "tool_stats": tools,
            "verification_reports": reports,
            "latest_verification": latest_reports,
            "cluster_pass": cluster.get("pass"),
            "cluster_checks": cluster.get("checks"),
            "producer_expected": producer.get("expected"),
            "resumes": len(result.get("resumes") or []),
            "threads": result.get("threads"),
        },
    })
    return out


def score_ops_case(spec: CaseSpec, case_dir: Path) -> dict[str, Any]:
    result = _load(case_dir / "result.json") or {}
    timeline = _load(case_dir / "timeline.json") or {}
    ops = _load(case_dir / "ops.json") or {}
    cluster = _load(case_dir / "cluster.json") or {}
    snapshot = ops.get("snapshot") or {}
    runs = snapshot.get("runs") or []
    checker = snapshot.get("checker") or []
    leases = snapshot.get("leases") or []
    handover_hist = ops.get("handover_history") or {}
    dev_traces = snapshot.get("tool_traces") or []
    status = result.get("status") or "unknown"
    error = result.get("error")
    # 以“第一次有实际结果的终态”为主，重复 lease 只作为缺陷扣分；
    # C1 的空 out_msg / C2 的重复 handover 都能在 evidence 里看到。
    terminal_runs = [
        r for r in runs
        if str(r.get("status")) in ("DONE", "ERROR", "ABANDONED")
        and str(r.get("out_msg") or "").strip()
    ]
    primary = terminal_runs[0] if terminal_runs else (runs[-1] if runs else {})
    run_status = str(primary.get("status")) if primary else None
    out_msg = str((primary.get("out_msg") if primary else "") or "")
    if terminal_runs:
        # 只要有一轮真的完成，就算完成了；重复 run 由 run_count 扣分
        run_status = str(terminal_runs[0].get("status"))
    handover = bool(ops.get("handover"))
    expected = spec.meta.get("expected") or {}
    expected_handover = bool(expected.get("handover"))
    keywords = [str(k).lower() for k in (expected.get("error_keywords") or [])]

    completion = 5.0
    if error or not runs:
        completion = 1.0
    elif run_status in ("ERROR", "ABANDONED"):
        completion = 2.0
    elif run_status == "PAUSED" and not handover:
        completion = 3.0
    if len(runs) > 1:
        # 同一个 app 被反复 lease：Agent 结束但没有把错误处理掉
        completion = min(completion, 2.5)
    if len(runs) > 2:
        completion = min(completion, 1.5)
    if not out_msg.strip():
        completion = min(completion, 2.0)

    # 正确性：预期决策 + 关键证据
    correct = 3.0
    if not out_msg.strip():
        correct = 1.5
    if handover == expected_handover:
        correct += 1.0
    if keywords:
        hay = (out_msg + " " + json.dumps(handover_hist, ensure_ascii=False)).lower()
        hits = sum(1 for k in keywords if k in hay)
        correct += 1.5 * hits / len(keywords)
    if spec.id == "C2":
        if cluster.get("pass"):
            correct += 1.5
        elif cluster.get("checks"):
            ok = sum(1 for c in cluster["checks"] if c.get("ok"))
            correct += 1.5 * ok / max(1, len(cluster["checks"]))
    if spec.id == "C3":
        # 提交确认 no：dev 历史里应有 submit 确认的动作/文本
        if handover_hist.get("metadata"):
            correct += 0.5
    correct = _clamp(correct)

    # 验证：handover 里的 verification / CVA 消息
    verification = 3.0
    meta = handover_hist.get("metadata") or {}
    ver = meta.get("verification") or {}
    if ver:
        verification = 5.0
    elif handover and handover_hist.get("messages"):
        verification = 4.0
    if spec.id == "C2" and cluster.get("pass"):
        verification = min(5.0, verification + 0.5)

    # 工具效率：ops 工具调用在消息里；dev 工具在 log.tool_trace
    ops_msgs = (ops.get("history") or {}).get("messages") or []
    ops_tool_calls = sum(len(m.get("tool_calls") or []) for m in ops_msgs if isinstance(m, dict))
    dev_errors = sum(1 for t in dev_traces if str(t.get("status")) == "error")
    tool_score = 5.0 - min(3, dev_errors)
    if ops_tool_calls > 40:
        tool_score -= 1
    if len(dev_traces) > 60:
        tool_score -= 1
    tool_score = _clamp(tool_score)

    # 体验：检测/处理时延
    ux = 5.0
    total_ms = timeline.get("total_ms") or 0
    if total_ms > 1_800_000:
        ux -= 1
    if total_ms > 3_600_000:
        ux -= 1
    if not out_msg:
        ux -= 1
    if not checker:
        ux -= 1
    ux = _clamp(ux)

    # 健壮性：租约释放/终态
    robustness = 5.0
    if error:
        robustness = 1.0
    elif run_status in ("ERROR", "ABANDONED"):
        robustness = 2.0
    elif leases:
        robustness = 3.0
    elif len(runs) > 2:
        robustness = 1.0
    elif len(runs) > 1:
        robustness = 2.0
    elif run_status == "DONE":
        robustness = 5.0
    else:
        robustness = 3.5
    pending_after = sum(1 for row in checker if str(row.get("status")) == "PENDING")
    if pending_after:
        robustness = min(robustness, 2.0)
    robustness = _clamp(robustness)

    dims = {
        "completion": completion, "correctness": correct,
        "verification": verification, "tool_efficiency": tool_score,
        "ux": ux, "robustness": robustness,
    }
    total = _clamp(
        completion * 0.2 + correct * 0.3 + verification * 0.2
        + tool_score * 0.1 + ux * 0.1 + robustness * 0.1
    )
    out = _base(spec.id, spec.stage)
    out.update({
        "dimensions": dims,
        "total": total,
        "verdict": _verdict(total, correct),
        "evidence": {
            "status": status,
            "error": error,
            "run_status": run_status,
            "out_msg": out_msg[-4000:],
            "handover": handover,
            "expected_handover": expected_handover,
            "checker_rows": len(checker),
            "checker_pending": sum(1 for row in checker if str(row.get("status")) == "PENDING"),
            "run_count": len(runs),
            "out_msg_empty": not out_msg.strip(),
            "leases": leases,
            "ops_tool_calls": ops_tool_calls,
            "dev_tool_traces": len(dev_traces),
            "dev_tool_errors": dev_errors,
            "verification": ver,
            "cluster_pass": cluster.get("pass"),
            "cluster_checks": cluster.get("checks"),
            "total_ms": total_ms,
        },
    })
    return out
