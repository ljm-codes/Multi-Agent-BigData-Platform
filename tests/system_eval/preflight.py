"""测试前环境预检：本地依赖、VM 服务、Hive/Spark、Kafka、YARN、沙箱、Maven。"""
from __future__ import annotations

import json
import os
import shutil
import socket
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import requests

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tests.system_eval import config_env, producer  # noqa: E402
from tests.system_eval.cluster import Cluster  # noqa: E402


def _run(cmd: list[str], *, timeout: float = 60, cwd: Path | None = None) -> dict[str, Any]:
    try:
        argv = list(cmd)
        resolved = shutil.which(argv[0]) or argv[0]
        argv[0] = resolved
        if resolved.lower().endswith((".cmd", ".bat")):
            argv = ["cmd", "/c", *argv]
        p = subprocess.run(
            argv, cwd=str(cwd or ROOT), capture_output=True, text=True,
            timeout=timeout, encoding="utf-8", errors="replace",
        )
        return {"ok": p.returncode == 0, "rc": p.returncode,
                "out": (p.stdout or "").strip()[-2000:],
                "err": (p.stderr or "").strip()[-2000:]}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}


def _port_open(host: str, port: int, timeout: float = 1.5) -> bool:
    s = socket.socket()
    s.settimeout(timeout)
    try:
        s.connect((host, port))
        return True
    except Exception:
        return False
    finally:
        s.close()


def _parse_first_number(text: str) -> int | None:
    import re
    # beeline csv2 输出首列就是数字；spark-sql 输出里找纯数字行
    for line in (text or "").splitlines():
        line = line.strip().strip("|").strip()
        if line.isdigit():
            return int(line)
    m = re.search(r"\b(\d{6,})\b", text or "")
    return int(m.group(1)) if m else None


def ensure_flink_kafka_classes(cluster: Cluster) -> dict[str, Any]:
    """检查 DataStream Kafka connector 类；缺了就从本地 ~/.m2 复制到三节点。"""
    # 只查 KafkaSource 不够：flink-sql-connector-kafka 也带 KafkaSource，
    # 但把 Kafka 客户端 shaded 掉了；DataStream 代码引用的是未 shade 的
    # org.apache.kafka.clients.consumer.*，必须确认这个类真的在 lib 里。
    check_cmd = (
        "for f in /export/server/flink/lib/*.jar; do "
        "unzip -Z1 \"$f\" 2>/dev/null | grep -qx 'org/apache/kafka/clients/consumer/OffsetResetStrategy.class' "
        "&& echo FOUND:$f && break; done"
    )
    result: dict[str, Any] = {"nodes": {}, "copied": []}
    local_connector = Path.home() / ".m2" / "repository" / "org" / "apache" / "flink" / "flink-connector-kafka" / "3.2.0-1.18" / "flink-connector-kafka-3.2.0-1.18.jar"
    # flink-connector-kafka 3.2.0-1.18 的 pom 依赖 kafka-clients 3.4.0；
    # 差一个版本可能在运行期缺类（B2g 的 OffsetResetStrategy 就是运行时才发现）。
    kafka_clients = sorted(
        (Path.home() / ".m2" / "repository" / "org" / "apache" / "kafka" / "kafka-clients").glob("3.4.0/kafka-clients-*.jar")
    )
    if not kafka_clients:
        kafka_clients = sorted(
            (Path.home() / ".m2" / "repository" / "org" / "apache" / "kafka" / "kafka-clients").glob("3.*/kafka-clients-*.jar")
        )
    for host in ("node1", "node2", "node3"):
        c = cluster if host == cluster.host else Cluster(host=host, yarn_url=cluster.yarn_url)
        rc, out = c.ssh(check_cmd, timeout=60)
        found = "FOUND:" in out
        result["nodes"][host] = {"found": found, "output": out.strip()[:500]}
        if not found and local_connector.exists():
            for jar in [local_connector] + ([kafka_clients[0]] if kafka_clients else []):
                c.sftp_put(jar, f"/export/server/flink/lib/{jar.name}")
                result["copied"].append({"host": host, "jar": jar.name})
        if host != cluster.host:
            c.close()
    return result


def run_preflight(*, full_spark_check: bool = True,
                  run_id: str = "preflight") -> dict[str, Any]:
    checks: dict[str, Any] = {}
    t0 = time.perf_counter()

    # 1. 本地文件/依赖
    checks["static_dist"] = {"ok": (ROOT / "static" / "dist" / "index.html").exists()}
    checks["config"] = {"ok": (ROOT / "config.yaml").exists()}
    sandbox_py = Path(r"D:\conda\envs\spark_sandbox\python.exe")
    checks["spark_sandbox"] = _run([
        str(sandbox_py), "-c",
        "import sys, pyspark, pytest; print(sys.version); print(pyspark.__version__); print(pytest.__version__)",
    ], timeout=60) if sandbox_py.exists() else {"ok": False, "error": "spark_sandbox python.exe 不存在"}
    checks["java"] = _run(["java", "-version"], timeout=30)
    checks["maven"] = _run([str(ROOT / "Maven" / "mvn-3.9.9" / "bin" / "mvn.cmd"), "-v"], timeout=60)
    checks["npx"] = _run(["npx", "--version"], timeout=30)
    checks["node"] = _run(["node", "--version"], timeout=30)

    # 2. 本地服务
    checks["local_postgres"] = {"ok": _port_open("127.0.0.1", 5432)}
    checks["local_redis"] = {"ok": _port_open("127.0.0.1", 6379)}
    checks["server_port"] = {"ok": not _port_open("127.0.0.1", 8000), "note": "8000 被占用" if _port_open("127.0.0.1", 8000) else ""}

    # 3. VM 端口/SSH/YARN
    ports = {22: "ssh", 8088: "yarn", 9870: "hdfs", 10000: "hive", 2181: "zk", 9092: "kafka"}
    checks["vm_ports"] = {name: {"ok": _port_open("node1", port)} for port, name in ports.items()}
    cluster = Cluster()
    try:
        rc, out = cluster.ssh("hostname; whoami", timeout=20)
        checks["ssh"] = {"ok": rc == 0 and "node1" in out, "out": out.strip()}
        try:
            sched = requests.get("http://node1:8088/ws/v1/cluster/scheduler", timeout=15).json()
            stype = (((sched.get("scheduler") or {}).get("schedulerInfo") or {}).get("type"))
            checks["yarn_scheduler"] = {"ok": stype == "fairScheduler", "type": stype}
        except Exception as exc:  # noqa: BLE001
            checks["yarn_scheduler"] = {"ok": False, "error": str(exc)}

        rc, out = cluster.beeline(
            "select count(1) from fireflymall.userbehaviordata where dt='2026-03-24'",
            timeout=300,
        )
        cnt = _parse_first_number(out)
        checks["hive_table"] = {"ok": rc == 0 and cnt is not None, "count": cnt, "out": out[-500:]}

        if full_spark_check:
            rc, out = cluster.spark_sql(
                "select count(1) from fireflymall.userbehaviordata where dt='2026-03-24'",
                timeout=600,
            )
            scnt = _parse_first_number(out)
            checks["spark_hive"] = {
                "ok": rc == 0 and scnt is not None,
                "count": scnt, "out": out[-1500:],
            }
        else:
            checks["spark_hive"] = {"ok": None, "skipped": True}

        try:
            checks["flink_kafka"] = ensure_flink_kafka_classes(cluster)
            checks["flink_kafka"]["ok"] = all(
                v.get("found") for v in checks["flink_kafka"]["nodes"].values()
            ) or bool(checks["flink_kafka"]["copied"])
        except Exception as exc:  # noqa: BLE001
            checks["flink_kafka"] = {"ok": False, "error": f"{type(exc).__name__}: {exc}"}

        try:
            checks["kafka_selftest"] = producer.selftest(run_id)
            checks["kafka_selftest"]["ok"] = bool(checks["kafka_selftest"].get("ok"))
        except Exception as exc:  # noqa: BLE001
            checks["kafka_selftest"] = {"ok": False, "error": f"{type(exc).__name__}: {exc}"}
    finally:
        cluster.close()

    # 4. eval DB（如果还没准备，这里只检查连接；prepare 由 run_all 先做）
    try:
        with config_env._connect(config_env.EVAL_DB) as conn:
            conn.execute("select 1")
        checks["eval_db"] = {"ok": True}
    except Exception as exc:  # noqa: BLE001
        checks["eval_db"] = {"ok": False, "error": f"{type(exc).__name__}: {exc}"}

    # 5. Redis
    try:
        import redis as redis_lib
        from load_config.config import config as app_cfg
        r = redis_lib.Redis(
            host=app_cfg["redis"]["host"], port=app_cfg["redis"]["port"],
            password=app_cfg["redis"]["password"], db=app_cfg["redis"]["db"],
        )
        checks["redis"] = {"ok": bool(r.ping())}
    except Exception as exc:  # noqa: BLE001
        checks["redis"] = {"ok": False, "error": f"{type(exc).__name__}: {exc}"}

    blocking = []
    for key, value in checks.items():
        if isinstance(value, dict) and value.get("ok") is False:
            blocking.append(key)
        elif isinstance(value, dict) and "ok" not in value:
            for sub, subval in value.items():
                if isinstance(subval, dict) and subval.get("ok") is False:
                    blocking.append(f"{key}.{sub}")
    result = {
        "ok": not blocking,
        "blocking": blocking,
        "checks": checks,
        "elapsed_ms": round((time.perf_counter() - t0) * 1000, 1),
    }
    return result


if __name__ == "__main__":
    full = "--quick" not in sys.argv
    print(json.dumps(run_preflight(full_spark_check=full, run_id="preflight"),
                     ensure_ascii=False, indent=2, default=str))
