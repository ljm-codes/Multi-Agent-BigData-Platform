"""汇总报告：9 个用例总表、分环节统计、好用性发现。"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from tests.system_eval.runner import CaseSpec, load_specs


def _load(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _case_rows(run_dir: Path) -> list[dict[str, Any]]:
    rows = []
    for spec in load_specs():
        case_dir = run_dir / "cases" / spec.id
        if not case_dir.exists():
            continue
        result = _load(case_dir / "result.json") or {}
        timeline = _load(case_dir / "timeline.json") or {}
        rubric = _load(case_dir / "rubric.json") or {}
        cluster = _load(case_dir / "cluster.json") or {}
        ops = _load(case_dir / "ops.json") or {}
        prompt = (case_dir / "prompt.md").read_text(encoding="utf-8") if (case_dir / "prompt.md").exists() else ""
        rows.append({
            "id": spec.id, "stage": spec.stage, "difficulty": spec.meta.get("difficulty"),
            "case_dir": case_dir, "result": result, "timeline": timeline,
            "rubric": rubric, "cluster": cluster, "ops": ops,
            "prompt": prompt, "spec": spec,
        })
    return rows


def _task_short(prompt: str) -> str:
    for line in prompt.splitlines():
        line = line.strip().lstrip("#").strip()
        if line:
            return line[:70]
    return ""


def _fmt_s(ms: Any) -> str:
    try:
        return f"{float(ms) / 1000:.1f}s"
    except Exception:
        return "-"


def _app_state(cluster: dict[str, Any]) -> str:
    if not cluster or cluster.get("skipped"):
        return "-"
    if cluster.get("app_id"):
        return f"{cluster.get('final_status') or cluster.get('state') or '?'} ({cluster.get('app_id')})"
    return cluster.get("error") or "-"


def build_summary(run_dir: Path, run_info: dict[str, Any] | None = None) -> dict[str, Any]:
    rows = _case_rows(run_dir)
    summary = {
        "run_dir": str(run_dir),
        "run_info": run_info or _load(run_dir / "run.json") or {},
        "cases": [],
        "stages": {},
    }
    for row in rows:
        timeline = row["timeline"] or {}
        rubric = row["rubric"] or {}
        ev = rubric.get("evidence") or {}
        tools = ev.get("tool_stats") or {}
        is_ops = row["stage"] == "ops"
        case_summary = {
            "id": row["id"], "stage": row["stage"], "difficulty": row["difficulty"],
            "verdict": rubric.get("verdict") or "N/A",
            "score": rubric.get("total"),
            "duration_ms": timeline.get("total_ms") or (row["result"] or {}).get("duration_ms"),
            "ttfb_ms": timeline.get("ttfb_ms"),
            "first_token_ms": timeline.get("first_token_ms"),
            "tool_calls": ev.get("ops_tool_calls") if is_ops else tools.get("count"),
            "tool_errors": ev.get("dev_tool_errors") if is_ops else tools.get("errors"),
            "app": _app_state(row["cluster"] or {}),
            "run_status": ev.get("run_status"),
            "handover": ev.get("handover"),
            "error": row["result"].get("error") if row["result"] else None,
            "case_dir": str(row["case_dir"]),
        }
        summary["cases"].append(case_summary)

    for stage in ("analysis", "develop", "ops"):
        stage_rows = [c for c in summary["cases"] if c["stage"] == stage]
        if not stage_rows:
            continue
        verdicts = [c["verdict"] for c in stage_rows]
        durations = [c["duration_ms"] or 0 for c in stage_rows]
        scores = [c["score"] or 0 for c in stage_rows]
        summary["stages"][stage] = {
            "count": len(stage_rows),
            "pass": verdicts.count("PASS"),
            "partial": verdicts.count("PARTIAL"),
            "fail": verdicts.count("FAIL"),
            "avg_score": round(sum(scores) / len(scores), 2) if scores else None,
            "total_duration_ms": sum(durations),
        }

    (run_dir / "summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    return summary


def write_summary_md(run_dir: Path, summary: dict[str, Any]) -> Path:
    lines = [
        "# 系统化 Agent 评测摘要",
        "",
        f"- 运行目录：`{run_dir}`",
        f"- 开始：{(summary.get('run_info') or {}).get('started_at', '-')}",
        f"- 结束：{(summary.get('run_info') or {}).get('finished_at', '-')}",
        "",
        "## 总表",
        "",
        "| 用例 | 环节 | 难度 | 结论 | 分数 | 总耗时 | 首 token | 工具/运维调用 | 工具失败 | 集群/运维 |",
        "|---|---|---|---|---:|---:|---:|---:|---:|---|",
    ]
    for c in summary["cases"]:
        lines.append(
            f"| {c['id']} | {c['stage']} | {c.get('difficulty') or '-'} | {c['verdict']} "
            f"| {c['score'] if c['score'] is not None else '-'} | {_fmt_s(c['duration_ms'])} "
            f"| {_fmt_s(c.get('first_token_ms') or c.get('ttfb_ms'))} "
            f"| {c['tool_calls'] if c['tool_calls'] is not None else '-'} "
            f"| {c['tool_errors'] if c['tool_errors'] is not None else '-'} | {c['app'] or '-'} |"
        )
    lines.extend(["", "## 分环节", ""])
    for stage, data in summary["stages"].items():
        lines.append(
            f"- **{stage}**：{data['count']} 例，PASS {data['pass']} / PARTIAL {data['partial']} "
            f"/ FAIL {data['fail']}，平均分 {data['avg_score']}，总耗时 {_fmt_s(data['total_duration_ms'])}"
        )
    lines.extend(["", "## 用例明细", ""])
    for c in summary["cases"]:
        lines.append(f"### {c['id']} — {c['verdict']}（{c['score']}）")
        lines.append("")
        lines.append(f"- 数据目录：`{c['case_dir']}`")
        lines.append(f"- 总耗时：{_fmt_s(c['duration_ms'])}；首 token：{_fmt_s(c.get('first_token_ms') or c.get('ttfb_ms'))}；工具/运维调用：{c['tool_calls']}（失败 {c['tool_errors']}）")
        if c.get("app"):
            lines.append(f"- 集群/运维：{c['app']}")
        if c.get("error"):
            lines.append(f"- 错误：`{c['error']}`")
        lines.append("- 证据：`events.jsonl`、`result.json`、`tool_traces.json`、`verification.jsonl`、`cluster.json`、`ops.json`、`rubric.json`")
        lines.append("")
    path = run_dir / "summary.md"
    path.write_text("\n".join(lines), encoding="utf-8")
    return path


def write_usability_findings(run_dir: Path, summary: dict[str, Any]) -> Path:
    rows = _case_rows(run_dir)
    lines = [
        "# 好用性观察（Dogfooding）",
        "",
        "> 本文件由自动证据 + 人工复核组成；主观结论必须能指到具体用例文件。",
        "",
        "## 自动观察",
        "",
    ]
    for row in rows:
        tid = row["timeline"] or {}
        rubric = row["rubric"] or {}
        ev = rubric.get("evidence") or {}
        result = row["result"] or {}
        final = ""
        done = result.get("done") or {}
        if isinstance(done, dict):
            final = str(done.get("output_msg") or "")
        if not final:
            for hist in (result.get("histories") or {}).values():
                if isinstance(hist, dict) and hist.get("output_msg"):
                    final = str(hist["output_msg"])
        total = tid.get("total_ms") or 0
        first_final = tid.get("first_final_token_ms")
        replay = (total - first_final) if first_final is not None else None
        notes = []
        if replay and replay > 30_000:
            notes.append(f"最终答案回放耗时 {replay/1000:.1f}s（done 被 6字/30ms 回放拖慢）")
        if not (tid.get("tool_status") or {}):
            notes.append("SSE 没有任何工具状态事件（CVA 阶段不可见）")
        if result.get("error"):
            notes.append(f"用例错误：{result['error']}")
        if row["stage"] != "ops" and not final:
            notes.append("没有拿到最终正文")
        if notes:
            lines.append(f"### {row['id']}")
            for note in notes:
                lines.append(f"- {note}")
            lines.append("")
    lines.extend([
        "## 已知结构性观察（需人工复核）",
        "",
        "- `/ai/chat/stream` 的 `done` 在最终答案回放之后才到；前端观感是“答案已经生成但还要慢慢打字”，"
        "评测时间线要区分图完成和流关闭。",
        "- CVA（质检 Agent）的 LLM 输出不在 SSE 里，只有 `/ai/verification` 和 `log.tool_trace` 能看；"
        "前端深链路的进度依赖 WS/轮询。",
        "- 正常成功路径的 `done.metadata.project_dir` 可能为 null（work_end 的 Send 没带 app_type/date/project_name），"
        "需要从工具轨迹/verification 文本定位项目。",
        "- `verification_registry` 只保留最新事件；刷新/重连只能拿快照，没有历史。",
        "- 运维 `log.agent_runs.PAUSED` 同时表示“等待人工审核”和“已转交开发”，只能靠 out_msg 区分。",
        "- 服务没有 `/health` 端点，评测只能靠登录/静态页探测就绪。",
        "",
        "## 人工复核",
        "",
        "- 待填：答案是否可执行、是否解释假设、是否清楚说明未验证风险、interrupt 文案是否足够决策。",
        "",
    ])
    path = run_dir / "usability_findings.md"
    path.write_text("\n".join(lines), encoding="utf-8")
    return path
