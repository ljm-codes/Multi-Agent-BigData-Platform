"""系统化评测编排：预检 → 阶段 A/B → 运维服务 → 阶段 C → 清理/恢复 → 汇总。"""
from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tests.system_eval import config_env, preflight as preflight_mod  # noqa: E402
from tests.system_eval.api_client import ApiClient  # noqa: E402
from tests.system_eval.cluster import Cluster  # noqa: E402
from tests.system_eval.recorder import CaseRecorder, now_iso  # noqa: E402
from tests.system_eval.runner import (  # noqa: E402
    _handle_stream, _wait_ready, load_specs, run_chat_case, run_ops_case,
)
from tests.system_eval import report  # noqa: E402


def _log(run_dir: Path, message: str) -> None:
    line = f"[{datetime.now().strftime('%H:%M:%S')}] {message}"
    print(line, flush=True)
    with (run_dir / "run.log").open("a", encoding="utf-8") as f:
        f.write(line + "\n")


def _write_json(path: Path, data: Any) -> None:
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2, default=str), encoding="utf-8")


def _start_server(role: str, run_dir: Path, port: int = 8000) -> tuple[subprocess.Popen, ApiClient]:
    log_path = run_dir / f"server_{role}.log"
    log_file = log_path.open("a", encoding="utf-8")
    env = dict(os.environ)
    env["PYTHONUTF8"] = "1"
    env["PYTHONIOENCODING"] = "utf-8"
    env["PYTHONPATH"] = str(ROOT)
    cmd = [
        sys.executable, "-m", "tests.system_eval.server",
        "--role", role, "--host", "127.0.0.1", "--port", str(port),
        "--data-dir", str(run_dir),
        "--marker", str(run_dir / f"server_{role}.json"),
    ]
    flags = 0
    if sys.platform.startswith("win"):
        flags = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
    proc = subprocess.Popen(
        cmd, cwd=str(ROOT), env=env, stdout=log_file, stderr=subprocess.STDOUT,
        creationflags=flags,
    )
    api = ApiClient(f"http://127.0.0.1:{port}")
    deadline = time.monotonic() + 240
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            tail = log_path.read_text(encoding="utf-8", errors="replace")[-4000:]
            raise RuntimeError(f"服务 {role} 启动失败 rc={proc.returncode}\n{tail}")
        try:
            _wait_ready(api, timeout=10)
            return proc, api
        except Exception:
            time.sleep(3)
    raise RuntimeError(f"服务 {role} 在 240s 内未就绪，见 {log_path}")


def _stop_server(proc: subprocess.Popen | None, api: ApiClient | None) -> None:
    if api is not None:
        try:
            api.close()
        except Exception:
            pass
    if proc is None or proc.poll() is not None:
        return
    if sys.platform.startswith("win"):
        try:
            proc.send_signal(signal.CTRL_BREAK_EVENT)
        except Exception:
            pass
        try:
            proc.wait(timeout=25)
            return
        except subprocess.TimeoutExpired:
            subprocess.run(["taskkill", "/PID", str(proc.pid), "/T", "/F"],
                           capture_output=True)
    else:
        proc.terminate()
        try:
            proc.wait(timeout=20)
        except subprocess.TimeoutExpired:
            proc.kill()


def _smoke(api: ApiClient, run_dir: Path) -> dict[str, Any]:
    smoke_dir = run_dir / "_smoke"
    rec = CaseRecorder(smoke_dir)
    holder: dict[str, Any] = {"thread_id": None}
    result: dict[str, Any] = {"status": "completed"}
    try:
        resp = api.chat_stream("你好", read_timeout=180)
        ctx = _handle_stream(rec, resp, holder)
        result["done"] = ctx.get("done")
        result["error"] = ctx.get("error")
        if ctx.get("error") or not ctx.get("done"):
            result["status"] = "failed"
    except Exception as exc:  # noqa: BLE001
        result["status"] = "failed"
        result["error"] = f"{type(exc).__name__}: {exc}"
    rec.final(result)
    _write_json(run_dir / "_smoke" / "result.json", result)
    if result.get("status") != "completed":
        raise RuntimeError(f"smoke 对话失败: {result.get('error')}")
    return result


def _case_ctx(api: ApiClient, cluster: Cluster, run_dir: Path, run_id: str,
              known: set[str]) -> dict[str, Any]:
    return {
        "api": api, "cluster": cluster, "run_dir": run_dir, "run_id": run_id,
        "known_app_ids": known,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="系统化 Agent 评测")
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--stages", default="analysis,develop,ops",
                        help="逗号分隔：analysis,develop,ops")
    parser.add_argument("--ids", default=None, help="逗号分隔的用例 id")
    parser.add_argument("--resume", action="store_true", help="跳过已完成的用例")
    parser.add_argument("--skip-preflight", action="store_true")
    parser.add_argument("--quick-preflight", action="store_true", help="跳过 Spark→Hive 检查")
    parser.add_argument("--ignore-preflight", action="store_true", help="预检失败仍继续")
    parser.add_argument("--keep-server", action="store_true")
    args = parser.parse_args()

    run_id = args.run_id or ("eval_" + datetime.now().strftime("%Y%m%d_%H%M%S"))
    run_dir = config_env.DATA_ROOT / run_id
    (run_dir / "cases").mkdir(parents=True, exist_ok=True)
    config_env.PRIVATE.mkdir(parents=True, exist_ok=True)

    stages = {s.strip() for s in args.stages.split(",") if s.strip()}
    ids = {s.strip() for s in args.ids.split(",")} if args.ids else None
    specs = [s for s in load_specs(stages=stages, ids=ids)]
    if not specs:
        print("没有匹配的用例", file=sys.stderr)
        return 2

    run_info: dict[str, Any] = {
        "run_id": run_id, "started_at": now_iso(), "stages": sorted(stages),
        "ids": [s.id for s in specs], "root": str(ROOT),
        "python": sys.version, "platform": sys.platform,
    }
    _write_json(run_dir / "run.json", run_info)
    _log(run_dir, f"run_id={run_id} cases={[s.id for s in specs]}")

    server_proc: subprocess.Popen | None = None
    api: ApiClient | None = None
    cluster = Cluster()
    known: set[str] = set()
    exit_code = 0

    try:
        config_env.prepare_eval_db()
        config_env.backup_and_patch_config()
        config_env.backup_and_clear_cache()
        _log(run_dir, "评测库与临时配置已准备")

        if not args.skip_preflight:
            _log(run_dir, "开始预检…")
            pf = preflight_mod.run_preflight(
                full_spark_check=not args.quick_preflight, run_id=run_id,
            )
            _write_json(run_dir / "preflight.json", pf)
            _log(run_dir, f"预检完成 ok={pf.get('ok')} blocking={pf.get('blocking')}")
            if not pf.get("ok") and not args.ignore_preflight:
                _write_json(run_dir / "run.json", {**run_info, "status": "preflight_failed",
                                                   "finished_at": now_iso()})
                report.build_summary(run_dir, {**run_info, "status": "preflight_failed"})
                report.write_summary_md(run_dir, report.build_summary(run_dir))
                return 3

        # 阶段 A/B：noops 服务（真实提交，但不起巡检/运维）
        chat_specs = [s for s in specs if s.stage in ("analysis", "develop")]
        ops_specs = [s for s in specs if s.stage == "ops"]
        if chat_specs:
            _log(run_dir, "启动 no-ops 服务（true 提交、不跑巡检）")
            server_proc, api = _start_server("noops", run_dir)
            _log(run_dir, f"no-ops 服务就绪 pid={server_proc.pid}")
            _smoke(api, run_dir)
            _log(run_dir, "smoke 对话通过")
            for spec in chat_specs:
                outcome_path = run_dir / "cases" / spec.id / "outcome.json"
                if args.resume and outcome_path.exists():
                    try:
                        if json.loads(outcome_path.read_text(encoding="utf-8")).get("status") == "completed":
                            _log(run_dir, f"跳过已完成 {spec.id}")
                            continue
                    except Exception:
                        pass
                _log(run_dir, f"开始 {spec.id} ({spec.stage})")
                started = time.perf_counter()
                try:
                    res = run_chat_case(spec, _case_ctx(api, cluster, run_dir, run_id, known))
                    _log(run_dir, f"结束 {spec.id} status={res.get('status')} "
                                  f"耗时={time.perf_counter()-started:.1f}s")
                except Exception as exc:  # noqa: BLE001
                    _log(run_dir, f"{spec.id} 异常: {type(exc).__name__}: {exc}")
                    _write_json(run_dir / "cases" / spec.id / "outcome.json",
                                {"id": spec.id, "status": "failed",
                                 "error": f"{type(exc).__name__}: {exc}"})
            if not args.keep_server:
                _stop_server(server_proc, api)
                server_proc, api = None, None
                _log(run_dir, "no-ops 服务已停止")

        if ops_specs:
            if server_proc is not None:
                _stop_server(server_proc, api)
                server_proc, api = None, None
            _log(run_dir, "清理 A/B 残留应用与队列…")
            try:
                _write_json(run_dir / "pre_ops_clean.json", cluster.ensure_ops_clean())
                cleanup = config_env.cleanup_eval_ops_state()
                _write_json(run_dir / "pre_ops_db_clean.json", cleanup)
            except Exception as exc:  # noqa: BLE001
                _log(run_dir, f"预清理失败（继续）: {exc}")
            _log(run_dir, "启动 ops 服务（真实巡检 + 运维 Worker）")
            server_proc, api = _start_server("ops", run_dir)
            _log(run_dir, f"ops 服务就绪 pid={server_proc.pid}")
            _smoke(api, run_dir)
            _log(run_dir, "smoke 对话通过")
            for spec in ops_specs:
                outcome_path = run_dir / "cases" / spec.id / "outcome.json"
                if args.resume and outcome_path.exists():
                    try:
                        if json.loads(outcome_path.read_text(encoding="utf-8")).get("status") == "completed":
                            _log(run_dir, f"跳过已完成 {spec.id}")
                            continue
                    except Exception:
                        pass
                _log(run_dir, f"开始 {spec.id} (ops)")
                started = time.perf_counter()
                try:
                    res = run_ops_case(spec, _case_ctx(api, cluster, run_dir, run_id, known))
                    _log(run_dir, f"结束 {spec.id} status={res.get('status')} "
                                  f"耗时={time.perf_counter()-started:.1f}s")
                except Exception as exc:  # noqa: BLE001
                    _log(run_dir, f"{spec.id} 异常: {type(exc).__name__}: {exc}")
                    _write_json(run_dir / "cases" / spec.id / "outcome.json",
                                {"id": spec.id, "status": "failed",
                                 "error": f"{type(exc).__name__}: {exc}"})

        # 汇总 + 清理
        try:
            _write_json(run_dir / "post_ops_clean.json", cluster.ensure_ops_clean())
        except Exception:
            pass
        try:
            _write_json(run_dir / "db_snapshot.json", config_env.eval_db_snapshot())
        except Exception as exc:  # noqa: BLE001
            _log(run_dir, f"DB 快照失败: {exc}")
        try:
            _write_json(run_dir / "post_db_clean.json", config_env.cleanup_eval_ops_state())
        except Exception:
            pass

        run_info.update({"finished_at": now_iso(), "status": "completed"})
        _write_json(run_dir / "run.json", run_info)
        summary = report.build_summary(run_dir, run_info)
        report.write_summary_md(run_dir, summary)
        report.write_usability_findings(run_dir, summary)
        _log(run_dir, f"完成：{run_dir}")
        return 0

    except Exception as exc:  # noqa: BLE001
        import traceback
        traceback.print_exc()
        run_info.update({"finished_at": now_iso(), "status": "failed",
                         "error": f"{type(exc).__name__}: {exc}"})
        _write_json(run_dir / "run.json", run_info)
        try:
            summary = report.build_summary(run_dir, run_info)
            report.write_summary_md(run_dir, summary)
            report.write_usability_findings(run_dir, summary)
        except Exception:
            pass
        exit_code = 1
        return exit_code
    finally:
        if server_proc is not None:
            _stop_server(server_proc, api)
        cluster.close()
        try:
            config_env.restore_all()
        except Exception as exc:  # noqa: BLE001
            print(f"[恢复配置失败] {exc}", file=sys.stderr)


if __name__ == "__main__":
    raise SystemExit(main())
