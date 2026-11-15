"""get_cluster_env 的解析顺序：本地文件 -> 远程路径/关键值 -> 兜底。

远程 probe 不写死节点、安装目录或库表；节点地址来自 config，
配置目录靠节点上的 command -v / HOME 环境变量发现。
"""
from __future__ import annotations

import re
import shlex
import time
from pathlib import Path
from typing import Optional

from Tools import node_ssh
from Tools.log_settings import LogSetting
from load_config.config import config
from services.cluster_access import (
    DISABLED_NO_CONFIG,
    DISABLED_WITH_CONFIG,
    cluster_access_enabled,
)
from services.cluster_profile_store import (
    ProfileStoreError,
    profile_store,
    redact_config_text,
    sha256_bytes,
)
from services.cluster_materialize import local_project_resources
from services.cluster_snapshot_store import snapshot_store

logger = LogSetting.create(__name__)

_PROBE_TTL = 60
_probe_cache: dict[str, tuple[float, dict]] = {}

_PROBE_SCRIPT = r'''
paths=""
add_file() {
  f="$1"
  [ -f "$f" ] || return 0
  case "|$paths|" in *"|$f|"*) return 0;; esac
  paths="$paths|$f"
  printf 'PATH|%s\n' "$f"
}
add_dir() {
  d="$1"
  [ -n "$d" ] && [ -d "$d" ] || return 0
  for f in core-site.xml hdfs-site.xml yarn-site.xml mapred-site.xml hive-site.xml hbase-site.xml flink-conf.yaml server.properties; do
    add_file "$d/$f"
  done
}
root_of() {
  p=$(command -v "$1" 2>/dev/null || true)
  [ -n "$p" ] && dirname "$(dirname "$(readlink -f "$p")")"
}
add_dir "${HADOOP_CONF_DIR:-}"
add_dir "${HADOOP_HOME:-}/etc/hadoop"
r=$(root_of hadoop); [ -n "$r" ] && add_dir "$r/etc/hadoop"
add_dir "${HIVE_CONF_DIR:-}"
add_dir "${HIVE_HOME:-}/conf"
r=$(root_of hive); [ -n "$r" ] && add_dir "$r/conf"
r=$(root_of beeline); [ -n "$r" ] && add_dir "$r/conf"
add_dir "${HBASE_CONF_DIR:-}"
add_dir "${HBASE_HOME:-}/conf"
r=$(root_of hbase); [ -n "$r" ] && add_dir "$r/conf"
add_dir "${FLINK_CONF_DIR:-}"
add_dir "${FLINK_HOME:-}/conf"
r=$(root_of flink); [ -n "$r" ] && add_dir "$r/conf"
add_dir "${KAFKA_HOME:-}/config"
r=$(root_of kafka-topics.sh); [ -n "$r" ] && add_dir "$r/config"

kv_xml() {
  f="$1"; k="$2"
  v=$(awk -v key="$k" 'BEGIN{RS="</property>"}
    index($0,"<name>" key "</name>") {
      if (match($0,/<value>[^<]*<\/value>/)) print substr($0,RSTART+7,RLENGTH-15)
    }' "$f" | tail -1)
  [ -n "$v" ] && printf 'KV|%s|%s\n' "$k" "$v"
}
kv_prop() {
  f="$1"; k="$2"
  v=$(grep -E "^[[:space:]]*$k[[:space:]]*[=:]" "$f" | tail -1 | sed 's/^[^=:]*[=:]//')
  [ -n "$v" ] && printf 'KV|%s|%s\n' "$k" "$v"
}
printf '%s\n' "$paths" | tr '|' '\n' | while read -r f; do
  [ -n "$f" ] || continue
  case "$f" in
    */core-site.xml) kv_xml "$f" fs.defaultFS ;;
    */hive-site.xml) kv_xml "$f" hive.metastore.uris; kv_xml "$f" hive.server2.thrift.port ;;
    */yarn-site.xml) kv_xml "$f" yarn.resourcemanager.address ;;
    */hdfs-site.xml) kv_xml "$f" dfs.nameservices ;;
    */server.properties) kv_prop "$f" bootstrap.servers; kv_prop "$f" zookeeper.connect ;;
    */flink-conf.yaml)
      v=$(grep -E '^[[:space:]]*jobmanager\.rpc\.address:' "$f" | tail -1 | sed 's/^[^:]*:[[:space:]]*//')
      [ -n "$v" ] && printf 'KV|%s|%s\n' jobmanager.rpc.address "$v" ;;
  esac
done
'''


def configured_host() -> str:
    """从 config 的 YARN/HDFS 地址里取集群节点；不在代码里写 node1。"""
    hadoop = config.get("hadoop") or {}
    candidates = [
        (hadoop.get("yarn") or {}).get("manager"),
        ((hadoop.get("hdfs") or {}).get("webhdfs") or {}).get("url"),
        ((hadoop.get("yarn") or {}).get("proxy")),
    ]
    for value in candidates:
        text = str(value or "").strip()
        if not text:
            continue
        text = re.sub(r"^[A-Za-z][A-Za-z0-9+.-]*://", "", text)
        host = text.split("/")[0].split(":")[0].strip()
        if host and host not in ("localhost", "127.0.0.1"):
            return host
    return ""


def _parse_probe_output(stdout: str) -> tuple[dict[str, str], dict[str, str]]:
    paths: dict[str, str] = {}
    values: dict[str, str] = {}
    for line in stdout.splitlines():
        if line.startswith("PATH|"):
            path = line[5:].strip()
            if path:
                name = Path(path).name
                paths.setdefault(name, path)
        elif line.startswith("KV|"):
            parts = line.split("|", 2)
            if len(parts) == 3 and parts[1] and parts[2]:
                values[parts[1]] = parts[2].strip()
    return paths, values


def probe_cluster_env(force: bool = False) -> dict:
    """在集群节点上发现配置路径和少量安全键值；不传整份文件。"""
    host = configured_host()
    if not host:
        return {"ok": False, "host": "", "error": "config 未配置可连接的 YARN/HDFS 节点"}
    now = time.monotonic()
    cached = _probe_cache.get(host)
    if not force and cached and now - cached[0] < _PROBE_TTL:
        return cached[1]
    command = "bash -lc " + shlex.quote(_PROBE_SCRIPT)
    result = node_ssh.run(host, command)
    if not result.ok:
        payload = {"ok": False, "host": host,
                   "error": f"{result.error_kind or 'ssh'}: {result.message}"}
    else:
        paths, values = _parse_probe_output(result.stdout)
        payload = {
            "ok": bool(paths or values),
            "host": host,
            "paths": paths,
            "values": values,
            "stderr": result.stderr[-1000:] if result.stderr else "",
        }
        if not paths and not values:
            payload["error"] = "节点上没发现配置目录（PATH/HOME 里没有 hadoop/hive/flink/kafka）"
    _probe_cache[host] = (now, payload)
    return payload


def _decode_configs(files: dict[str, bytes]) -> dict[str, str]:
    configs: dict[str, str] = {}
    for name, data in files.items():
        if len(data) > profile_store.max_file_bytes:
            continue
        try:
            configs[name] = redact_config_text(data.decode("utf-8"))
        except UnicodeDecodeError:
            continue
    return configs


def _local_project(admin_id: int, project_id: str) -> tuple[str, dict[str, str]]:
    target, files = local_project_resources(admin_id, project_id)
    return target, _decode_configs(files)


def _local_public(admin_id: int) -> tuple[dict[str, str], bool]:
    try:
        snapshot_store.ensure_public(0, "get_cluster_env 读取 cluster_resources")
    except ProfileStoreError as exc:
        logger.warning("读取 cluster_resources 失败：%s", exc)
        return {}, False
    files = snapshot_store.read_public_files()
    drift = False
    current = snapshot_store.get_public(include_content=False)
    if current is not None:
        expected = {
            item.get("filename"): item.get("sha256")
            for item in (current.get("files") or [])
            if item.get("filename")
        }
        actual = {name: sha256_bytes(content) for name, content in files.items()}
        if expected and actual and actual != expected:
            drift = True
    return _decode_configs(files), drift


def _local_profile(admin_id: int, profile_id: Optional[int]) -> dict[str, str]:
    if profile_id is None:
        return {}
    try:
        configs: dict[str, str] = {}
        for row in profile_store.list_files(admin_id, int(profile_id)):
            data = profile_store.read_file(admin_id, int(profile_id), row["filename"])
            configs[row["filename"]] = data["content"]
        return configs
    except ProfileStoreError as exc:
        logger.warning("读取 profile %s 失败：%s", profile_id, exc)
        return {}


def resolve_cluster_env(admin_id: int, project_id: str = "",
                        profile_id: Optional[int] = None) -> dict:
    enabled = cluster_access_enabled()
    access = "enabled" if enabled else "disabled"

    if project_id:
        try:
            target, project_configs = _local_project(admin_id, project_id)
        except ProfileStoreError:
            project_configs = {}
            target = ""
        if project_configs:
            return {
                "cluster_access": access,
                "live_probe": "not_needed",
                "source": f"project:{target}",
                "configs": project_configs,
                "needs_config": False,
                "message": ("已返回项目 src/main/resources 的本地配置，未发起远程探测。"
                            if enabled else DISABLED_WITH_CONFIG),
            }

    public_configs, public_drift = _local_public(admin_id)
    profile_configs = _local_profile(admin_id, profile_id)
    merged = {**public_configs, **profile_configs}
    if public_drift and not merged:
        return {
            "cluster_access": access,
            "live_probe": "not_needed",
            "source": "cluster_resources",
            "configs": {},
            "public_drift": True,
            "needs_config": True,
            "message": "cluster_resources 与公有快照不一致，且当前目录没有可用配置；"
                       "请让管理员先处理 public drift（保留/回滚）。",
        }
    if merged:
        source = []
        if public_configs:
            source.append("cluster_resources")
        if profile_configs:
            source.append(f"profile:{profile_id}")
        message = "已返回本地配置，未发起远程探测。" if enabled else DISABLED_WITH_CONFIG
        if public_drift:
            message += "；注意：cluster_resources 与公有快照不一致，需管理员先处理 public drift"
        return {
            "cluster_access": access,
            "live_probe": "not_needed",
            "source": "+".join(source),
            "configs": merged,
            "public_drift": public_drift,
            "needs_config": bool(public_drift),
            "message": message,
        }

    if not enabled:
        return {
            "cluster_access": "disabled",
            "live_probe": "disabled",
            "source": "none",
            "configs": {},
            "needs_config": True,
            "message": DISABLED_NO_CONFIG,
        }

    probe = probe_cluster_env()
    if probe.get("ok"):
        return {
            "cluster_access": "enabled",
            "live_probe": "ok",
            "host": probe.get("host"),
            "paths": probe.get("paths") or {},
            "values": probe.get("values") or {},
            "configs": {},
            "needs_config": False,
            "message": ("本地无配置，已从集群节点读取配置文件路径和关键值；"
                        "默认不传整份文件（避免带出密码）。"),
        }
    return {
        "cluster_access": "enabled",
        "live_probe": "failed",
        "host": probe.get("host"),
        "error": probe.get("error"),
        "configs": {},
        "needs_config": True,
        "message": (f"本地无配置，远程探测失败：{probe.get('error')}；"
                    "请询问用户或使用兜底查询工具。"),
    }
