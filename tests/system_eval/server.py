"""
评测专用 FastAPI 启动器。

与 run.py 相同的 Windows Selector 事件循环；不修改业务代码，只在进程内：
  * 把所有对外告警（飞书/邮件）替换成写 JSONL 的 no-op；
  * role=noops 时把巡检和运维 Worker 换成空转，保证 A/B 阶段提交作业时不触发运维 Agent。

用法：
  python -m tests.system_eval.server --role noops --port 8000 --data-dir tests/system_eval_data/<run>
  python -m tests.system_eval.server --role ops   --port 8000 --data-dir tests/system_eval_data/<run>
"""
from __future__ import annotations

import argparse
import asyncio
import importlib
import json
import os
import sys
import threading
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

if sys.platform.startswith("win"):
    try:
        asyncio.get_event_loop().close()
    except RuntimeError:
        pass
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

_lock = threading.Lock()
_alerts_path: Path | None = None


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _record_alert(kind: str, **payload) -> None:
    if _alerts_path is None:
        return
    try:
        with _lock:
            _alerts_path.parent.mkdir(parents=True, exist_ok=True)
            with _alerts_path.open("a", encoding="utf-8") as f:
                f.write(json.dumps(
                    {"ts": _now(), "kind": kind, **payload},
                    ensure_ascii=False, default=str,
                ) + "\n")
    except Exception:
        pass


def _silent_feishu(text: str):
    _record_alert("feishu", text=str(text)[:4000])
    return True, "silenced-by-system-eval"


def _silent_warning(*args, **kwargs):
    _record_alert("warning", args=[str(a)[:1000] for a in args],
                  kwargs={k: str(v)[:1000] for k, v in kwargs.items()})
    return True, "silenced-by-system-eval"


def _silent_email(subject: str = "", content: str = "", *args, **kwargs):
    _record_alert("email", subject=str(subject)[:500], content=str(content)[:4000])
    return True


def _patch_alerts_before_import() -> None:
    # 先 patch 源头；之后业务模块的 `from Tools.alarm_sender import ...` 会绑定 no-op
    import Tools.alarm_sender as A
    A.send_feishu_text = _silent_feishu
    A.send_warning_msg = _silent_warning
    A.send_error_email = _silent_email


def _patch_alerts_after_import() -> None:
    # 已经直接 import 过函数的模块，再补一次属性覆盖，避免漏网
    for name in (
        "agent._operations_agent",
        "agent.operations_agent_run",
        "middleware.fail_tool_middleware",
        "routers.ops_control",
        "Tools.main_check_tool",
    ):
        try:
            mod = importlib.import_module(name)
        except Exception:
            continue
        if hasattr(mod, "send_feishu_text"):
            mod.send_feishu_text = _silent_feishu
        if hasattr(mod, "send_warning_msg"):
            mod.send_warning_msg = _silent_warning
        if hasattr(mod, "send_error_email"):
            mod.send_error_email = _silent_email


async def _idle_loop(*args, **kwargs):
    while True:
        await asyncio.sleep(3600)


def _disable_ops_workers() -> None:
    import Tools.main_check_tool as M
    M.main_check = _idle_loop
    import agent.operations_agent_run as R
    R.operations_main = _idle_loop


def _patch_redis_submit_bug() -> None:
    """
    评测期 shim：agent/code_verify_agent.py 里 `redis_con.hdel(key)` 少传了 field
    （注册信息是 hash，正确动作应是 delete 整个 key）。这是被测平台的真实缺陷；
    为了让提交/运维链路能继续评测，只在服务进程内把一参数 hdel 解释成 delete，
    并写入 alerts.jsonl 作为证据。业务代码不改。
    """
    import agent.code_verify_agent as CVA

    class _RedisSubmitShim:
        def __init__(self, redis):
            self._redis = redis

        def __getattr__(self, name):
            return getattr(self._redis, name)

        def hdel(self, *args, **kwargs):
            if len(args) == 1 and not kwargs:
                _record_alert("redis_hdel_shim", key=str(args[0]),
                              note="one-arg hdel -> delete(key)")
                return self._redis.delete(args[0])
            return self._redis.hdel(*args, **kwargs)

    if not isinstance(CVA.redis_con, _RedisSubmitShim):
        CVA.redis_con = _RedisSubmitShim(CVA.redis_con)


def main() -> int:
    global _alerts_path
    parser = argparse.ArgumentParser()
    parser.add_argument("--role", choices=("noops", "ops"), required=True)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--data-dir", required=True)
    parser.add_argument("--marker", default=None)
    args = parser.parse_args()

    data_dir = Path(args.data_dir)
    data_dir.mkdir(parents=True, exist_ok=True)
    _alerts_path = data_dir / "alerts.jsonl"

    _patch_alerts_before_import()
    import run  # noqa: E402  —— 必须在事件循环策略与告警 patch 之后
    _patch_alerts_after_import()
    _patch_redis_submit_bug()
    if args.role == "noops":
        _disable_ops_workers()

    marker = Path(args.marker) if args.marker else data_dir / f"server_{args.role}.json"
    marker.write_text(json.dumps({
        "pid": os.getpid(), "role": args.role, "port": args.port,
        "started_at": _now(), "alerts_file": str(_alerts_path),
    }, ensure_ascii=False, indent=2), encoding="utf-8")

    import uvicorn
    uvicorn.run(
        run.app,
        host=args.host,
        port=args.port,
        loop="none",
        reload=False,
        log_level="info",
        timeout_graceful_shutdown=10,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
