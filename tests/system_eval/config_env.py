"""
评测环境的配置隔离：备份/覆盖/恢复 config.yaml 与 SQLite cache，创建独立 Postgres 评测库。

不修改业务代码；所有临时改动都记录在 tests/system_eval_data/_private/env_state.json，
即使进程崩溃，也能用 `python -m tests.system_eval.config_env restore` 恢复。
"""
from __future__ import annotations

import json
import os
import re
import shutil
import sys
from pathlib import Path
from typing import Any

import bcrypt
import psycopg
import yaml

ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT = ROOT / "tests" / "system_eval_data"
PRIVATE = DATA_ROOT / "_private"
STATE_FILE = PRIVATE / "env_state.json"
CONFIG_FILE = ROOT / "config.yaml"
CONFIG_BAK = PRIVATE / "config.yaml.bak"
CACHE_FILE = ROOT / "cache" / "agent_cache.db"
CACHE_BAK = PRIVATE / "agent_cache.db.bak"

EVAL_DB = "multiagentbigdataplatform_eval"
EVAL_EMAIL = "system-eval@example.com"
EVAL_USERNAME = "system-eval"


def _cfg() -> dict[str, Any]:
    return yaml.safe_load(CONFIG_FILE.read_text(encoding="utf-8"))


def _pg_conf(cfg: dict[str, Any] | None = None) -> dict[str, Any]:
    cfg = cfg or _cfg()
    return cfg["databases"]["postgres"]


def _connect(dbname: str, *, autocommit: bool = True, cfg: dict[str, Any] | None = None):
    pg = _pg_conf(cfg)
    return psycopg.connect(
        host=pg["host"], port=pg["port"], user=pg["user"],
        password=pg["password"], dbname=dbname, autocommit=autocommit,
        connect_timeout=10,
    )


def _split_sql(text: str) -> list[str]:
    """按行尾分号切 DDL；文件里没有函数体/美元引号，够用。"""
    lines = []
    for line in text.splitlines():
        if line.strip().startswith("--"):
            continue
        lines.append(line)
    statements = []
    buf: list[str] = []
    for line in lines:
        buf.append(line)
        if line.rstrip().endswith(";"):
            stmt = "\n".join(buf).strip().rstrip(";").strip()
            if stmt:
                statements.append(stmt)
            buf = []
    tail = "\n".join(buf).strip()
    if tail:
        statements.append(tail)
    return statements


def _ensure_database() -> None:
    with _connect("postgres") as conn:
        exists = conn.execute(
            "select 1 from pg_database where datname = %s", (EVAL_DB,)
        ).fetchone()
        if not exists:
            conn.execute(f'create database "{EVAL_DB}"')


def _reset_eval_schema() -> None:
    schema_sql = (ROOT / "sql" / "postgresql.sql").read_text(encoding="utf-8")
    # 去掉首行 create database，改为连到 eval 库再建表
    schema_sql = re.sub(r"^\s*create\s+database\s+[^;]+;", "", schema_sql, count=1, flags=re.I)
    with _connect(EVAL_DB) as conn:
        for stmt in [
            "drop schema if exists log cascade",
            "drop schema if exists code cascade",
            "drop table if exists public.admin_info cascade",
            "drop table if exists public.admin_thread cascade",
            "drop table if exists public.app_leases cascade",
            "drop table if exists public.checkpoint_migrations cascade",
            "drop table if exists public.checkpoints cascade",
            "drop table if exists public.checkpoint_blobs cascade",
            "drop table if exists public.checkpoint_writes cascade",
            "drop table if exists public.store_migrations cascade",
            "drop table if exists public.store cascade",
            "drop table if exists public.store_vectors cascade",
        ]:
            conn.execute(stmt)
        for stmt in _split_sql(schema_sql):
            conn.execute(stmt)


def ensure_eval_admin(password: str | None = None) -> dict[str, Any]:
    password = password or os.getenv("SYSTEM_EVAL_PASSWORD") or "Eval@2026!"
    pwd_hash = bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()
    with _connect(EVAL_DB) as conn:
        row = conn.execute(
            "select id, username, email, permission from admin_info where email = %s",
            (EVAL_EMAIL,),
        ).fetchone()
        if row is None:
            row = conn.execute(
                "insert into admin_info (username, password, email, permission, disabled) "
                "values (%s, %s, %s, 2, false) returning id, username, email, permission",
                (EVAL_USERNAME, pwd_hash, EVAL_EMAIL),
            ).fetchone()
        else:
            conn.execute(
                "update admin_info set password = %s, permission = 2, disabled = false "
                "where email = %s",
                (pwd_hash, EVAL_EMAIL),
            )
    return {
        "id": row[0], "username": row[1], "email": row[2],
        "password": password, "permission": row[3],
    }


def prepare_eval_db() -> dict[str, Any]:
    _ensure_database()
    _reset_eval_schema()
    creds = ensure_eval_admin()
    PRIVATE.mkdir(parents=True, exist_ok=True)
    (PRIVATE / "credentials.json").write_text(
        json.dumps(creds, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    return creds


def read_eval_creds() -> dict[str, Any]:
    data = json.loads((PRIVATE / "credentials.json").read_text(encoding="utf-8"))
    return data


def backup_and_patch_config() -> dict[str, Any]:
    """备份原配置，写入评测配置；返回原配置中的 db 名给恢复/记录用。"""
    PRIVATE.mkdir(parents=True, exist_ok=True)
    DATA_ROOT.mkdir(parents=True, exist_ok=True)
    cfg = _cfg()
    original_db = cfg["databases"]["postgres"]["db"]
    if not CONFIG_BAK.exists():
        CONFIG_BAK.write_bytes(CONFIG_FILE.read_bytes())

    cfg["app"]["connect_to_cluster"] = True
    cfg["databases"]["postgres"]["db"] = EVAL_DB
    # 双保险：错误队列阈值抬高，避免测试期触发邮件
    queue = cfg.setdefault("hadoop", {}).setdefault("checker", {}).setdefault("error_queue", {})
    queue["value"] = [1_000_000, 2_000_000, 3_000_000]

    CONFIG_FILE.write_text(
        yaml.safe_dump(cfg, allow_unicode=True, sort_keys=False), encoding="utf-8"
    )
    state = {
        "config_backup": str(CONFIG_BAK),
        "original_db": original_db,
        "eval_db": EVAL_DB,
        "cache_backup": None,
    }
    STATE_FILE.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    return state


def backup_and_clear_cache() -> None:
    PRIVATE.mkdir(parents=True, exist_ok=True)
    if CACHE_FILE.exists():
        if not CACHE_BAK.exists():
            shutil.copy2(CACHE_FILE, CACHE_BAK)
        CACHE_FILE.unlink()
    state = json.loads(STATE_FILE.read_text(encoding="utf-8")) if STATE_FILE.exists() else {}
    state["cache_backup"] = str(CACHE_BAK) if CACHE_BAK.exists() else None
    STATE_FILE.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def restore_config() -> None:
    if CONFIG_BAK.exists():
        shutil.copy2(CONFIG_BAK, CONFIG_FILE)


def restore_cache() -> None:
    if CACHE_BAK.exists():
        shutil.copy2(CACHE_BAK, CACHE_FILE)


def restore_all() -> None:
    restore_config()
    restore_cache()


def eval_db_snapshot(tables: list[str] | None = None) -> dict[str, Any]:
    """把评测库里关键表的当前内容导出成可 JSON 化的快照。"""
    tables = tables or [
        "log.checker_log", "log.agent_runs", "public.app_leases",
        "code.app_file_idx", "log.tool_trace", "admin_thread",
    ]
    out: dict[str, Any] = {}
    with _connect(EVAL_DB, autocommit=False) as conn:
        for table in tables:
            try:
                cur = conn.execute(f"select * from {table}")
                cols = [d.name for d in cur.description]
                rows = []
                for row in cur.fetchall():
                    rows.append({c: _jsonable(v) for c, v in zip(cols, row)})
                out[table] = rows
            except Exception as exc:  # noqa: BLE001
                conn.rollback()
                out[table] = {"error": f"{type(exc).__name__}: {exc}"}
    return out


def _jsonable(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, (list, tuple)):
        return [_jsonable(v) for v in value]
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    return str(value)


def cleanup_eval_ops_state() -> dict[str, int]:
    """把评测残留的 PENDING 错误/租约清掉；证据行保留（改成 DONE）。"""
    result = {"pending": 0, "leases": 0}
    with _connect(EVAL_DB) as conn:
        cur = conn.execute(
            "update log.checker_log set status='DONE', is_solved=true "
            "where status in ('PENDING','PROCESSING') and app_id <> ''"
        )
        result["pending"] = int(cur.rowcount or 0)
        cur = conn.execute("delete from public.app_leases")
        result["leases"] = int(cur.rowcount or 0)
    try:
        import redis
        cfg = _cfg()
        r = redis.Redis(
            host=cfg["redis"]["host"], port=cfg["redis"]["port"],
            password=cfg["redis"]["password"], db=cfg["redis"]["db"],
        )
        key = cfg["hadoop"]["checker"]["error_queue"]["error_app_redis_key"]
        r.delete(key, f"{key}:seq")
    except Exception:
        pass
    return result


def main() -> int:
    action = sys.argv[1] if len(sys.argv) > 1 else "restore"
    if action == "restore":
        restore_all()
        print("已恢复 config.yaml 与 cache")
        return 0
    if action == "prepare":
        prepare_eval_db()
        backup_and_patch_config()
        print("已创建评测库并写入临时配置")
        return 0
    print(f"未知动作: {action}")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
