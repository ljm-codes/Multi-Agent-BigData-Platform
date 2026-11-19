"""集群/profile/project 资源感知工具。

给 DDA/CVA/MA 在生成、验证前读取环境与项目资源；只读，不改资源。
"""
import json
from datetime import datetime
from typing import Literal

from confluent_kafka import TopicPartition, ConsumerGroupTopicPartitions
from confluent_kafka.admin import AdminClient, OffsetSpec
from hdfs import InsecureClient
from langchain_core.tools import tool
from langgraph.prebuilt import ToolRuntime
from sqlalchemy import select, and_, text

from SPO.cluster_env_model import ProjectResource
from SPO.postgresql_model import AdminProject
from Tasks.project_registry import admin_id_of
from Tools.db import SessionLocal, HiveSession
from Tools.log_settings import LogSetting
from Tools.registry import register_tool
from Tools.share_check_tool import rm
from load_config.config import config
from services.cluster_env_resolver import resolve_cluster_env
from services.cluster_materialize import check_project

logger = LogSetting.create(__name__)
_hdfs_conf = (config.get("hadoop") or {}).get("hdfs") or {}
_hdfs_webhdfs_url = _hdfs_conf.get("webhdfs", {}).get("url", "node1:9870")
if "://" not in _hdfs_webhdfs_url:
    _hdfs_webhdfs_url = f"http://{_hdfs_webhdfs_url}"
_hdfs_webhdfs_user = _hdfs_conf.get("webhdfs", {}).get("user", "hadoop")
CONNECT_TO_CLUSTER = bool((config.get("app") or {}).get("connect_to_cluster", True))
_hdfs_client = None if not CONNECT_TO_CLUSTER else InsecureClient(_hdfs_webhdfs_url, user=_hdfs_webhdfs_user)
amqp = config.get("AMQP") or {}
kafka = amqp.get("kafka") or {}
bootstrap_servers = str(kafka.get("bootstrap-servers") or "").strip()

conf = {
    "bootstrap.servers": bootstrap_servers,
    "socket.timeout.ms": 10000,
    "request.timeout.ms": 10000,
    "api.version.request.timeout.ms": 10000,
}
kafka_admin_client = None if not CONNECT_TO_CLUSTER else AdminClient(conf)


def _json(data) -> str:
    return json.dumps(data, ensure_ascii=False, default=str)


@register_tool("DDA", priority=1)
@register_tool("DAA", priority=1)
@register_tool("CVA", priority=1)
@register_tool("MA", priority=1)
@tool
def get_project_resources(runtime: ToolRuntime, project_id: str = "") -> str:
    """
    作用：读取项目当前 materialize 后的环境资源（cluster_resources + profile 合并结果），并检查配置漂移。
          生成代码、写测试、验证前应先调用本工具；不要凭记忆猜项目资源。
    参数：
        project_id：项目登记表里的 project_id。
    返回：JSON 字符串。
          needs_initialize=true：项目坐标还没回填，必须先调用 initialize_project；
          needs_config=true：项目已创建，但没有任何环境配置，必须询问用户；
          needs_public_decision=true：cluster_resources 与项目记录的公有快照不一致，需管理员处理；
          needs_decision=true：项目 resources 被改动，必须询问保留还是回滚；
          clean=true：可继续，包含 files/contents/effective_hash/target_path/snapshots。
    """
    admin_id = admin_id_of(runtime.context.user_id)
    if not admin_id:
        return _json({"error": "无法识别当前用户"})
    project_id = (project_id or runtime.state.get("project_id") or "").strip()
    if not project_id:
        return _json({"error": "缺少 project_id"})
    with SessionLocal() as session:
        project = session.execute(
            select(AdminProject).where(
                and_(
                    AdminProject.project_id == project_id,
                    AdminProject.admin_id == admin_id,
                )
            )
        ).scalar_one_or_none()
        if project is None:
            return _json({"error": "项目不存在或无权访问", "project_id": project_id})
        if not project.project_date or not project.project_name:
            return _json({
                "project_id": project_id, "needs_initialize": True,
                "message": "项目坐标尚未回填。请先调用 initialize_project 创建项目目录，"
                           "成功后再读取环境资源；不要在未初始化时开始写代码。",
            })
        resource = session.get(ProjectResource, project_id)
    if resource is None:
        from services.cluster_materialize import materialize_project
        from services.cluster_profile_store import ProfileStoreError
        try:
            materialize_project(admin_id, project_id, reason="get_project_resources")
        except ProfileStoreError as exc:
            if exc.code == "invalid":
                return _json({
                    "project_id": project_id, "needs_config": True,
                    "message": "项目已创建，但 cluster_resources 和 profile 都没有可用配置。"
                               "请先用 info_inquiry 询问用户提供集群配置，"
                               "不要凭空猜 Kafka/Hive/HDFS 参数。",
                })
            if exc.code == "conflict":
                return _json({
                    "project_id": project_id, "needs_public_decision": True,
                    "message": "cluster_resources 与公有快照不一致，需要管理员先处理 public drift"
                               "（保留修改或回滚）；决策前不得继续。",
                })
            return _json({"error": str(exc), "project_id": project_id})
        except Exception as exc:  # noqa: BLE001
            logger.warning("读取项目资源时 materialize 失败：%s: %s", type(exc).__name__, exc)
            return _json({"error": f"{type(exc).__name__}: {exc}", "project_id": project_id})
        with SessionLocal() as session:
            resource = session.get(ProjectResource, project_id)
        if resource is None:
            return _json({
                "project_id": project_id, "needs_config": True,
                "message": "项目没有可 materialize 的配置；请询问用户提供环境配置。",
            })

    drift = check_project(admin_id, project_id)
    if not drift.get("clean"):
        public_drift = drift.get("public_drift")
        if drift.get("drift_id"):
            return _json({
                "project_id": project_id, "needs_decision": True, "drift": drift,
                "target_path": resource.target_path,
                "effective_hash": resource.effective_hash,
                "message": "检测到项目 resources 与记录不一致。请询问用户："
                           "保留修改（verify_status=stale，后续必须重新验证）"
                           "还是回滚到 materialized snapshot；决策完成前不得继续。",
            })
        if public_drift:
            return _json({
                "project_id": project_id, "needs_public_decision": True,
                "public_drift": public_drift,
                "message": "cluster_resources 与项目记录的公有快照不一致。"
                           "请让管理员处理 public drift（保留/回滚），决策前不得继续。",
            })
        return _json({"project_id": project_id, "needs_decision": True, "drift": drift,
                      "message": "项目配置不一致，请先处理 drift。"})

    from services.cluster_materialize import local_project_resources, project_resource_info
    from services.cluster_profile_store import redact_config_text
    target, files = local_project_resources(admin_id, project_id)
    info = project_resource_info(admin_id, project_id)
    contents = {
        name: redact_config_text(content.decode("utf-8", "replace"))
        for name, content in files.items()
    }
    return _json({
        "project_id": project_id, "clean": True,
        "target_path": target,
        "effective_hash": resource.effective_hash,
        "files": sorted(info.get("files") or []),
        "contents": contents,
        "content_omitted": info.get("content_omitted") or [],
        "snapshots": {
            "public_snapshot_id": resource.public_snapshot_id,
            "profile_snapshot_id": resource.profile_snapshot_id,
            "materialized_snapshot_id": resource.materialized_snapshot_id,
        },
    })


@register_tool("DDA", priority=1)
@register_tool("DAA", priority=1)
@register_tool("CVA", priority=1)
@register_tool("MA", priority=1)
@tool
def get_cluster_env(runtime: ToolRuntime, project_id: str = "",
                    profile_id: int | None = None) -> str:
    """
    作用：读取当前可用的集群环境配置。
          顺序固定：项目 src/main/resources -> cluster_resources -> profile；
          以上都没有且 connect_to_cluster=true 时才远程探测；
          远程只返回配置文件路径和安全键值，不传整份文件。
    参数：
        project_id：可选，传入后会从项目的 materialized 资源里取绑定 profile；
        profile_id：可选，直接指定环境配置 profile。
    返回：JSON 字符串，含 cluster_access（enabled/disabled）、live_probe
          （not_needed/ok/failed/disabled）、source、configs、paths、values、needs_config、message。
          needs_config=true 时必须询问用户或使用兜底查询工具。
    """
    admin_id = admin_id_of(runtime.context.user_id)
    if not admin_id:
        return _json({"error": "无法识别当前用户"})
    project_id = (project_id or runtime.state.get("project_id") or "").strip()
    if profile_id is None and project_id:
        with SessionLocal() as session:
            resource = session.get(ProjectResource, project_id)
            if resource is not None and resource.profile_id:
                profile_id = resource.profile_id
    return _json(resolve_cluster_env(admin_id, project_id, profile_id))


@register_tool("DDA", priority=1)
@register_tool("DAA", priority=1)
@register_tool("CVA", priority=1)
@register_tool("MA", priority=1)
@tool
def get_hive_db(limit: int = 10, offset: int = 0) -> str:
    """
    获取 当前的 Hive 中所有的数据库名
    参数：
        limit：可选，返回的最大数据库数量，默认 10。
        offset：可选，返回的数据库列表偏移量，默认 0。
    返回：
        错误信息或数据库名列表
    """
    if offset < 0:
        return "offset 必须大于等于 0"
    if not HiveSession:
        return "当前被禁止访问 Hive 数据库，请进行询问用户"
    with HiveSession() as session:
        result = session.execute(text(f"show databases"))
        db_names = [row[0] for row in result]
    max_db = len(db_names)
    filtered_db = db_names[min(offset, max_db):min(offset + limit, max_db)]
    return f"在 limit:{limit}，offset:{offset} 区间内的数据库为：{'\n'.join(filtered_db)}"


@register_tool("DDA", priority=1)
@register_tool("DAA", priority=1)
@register_tool("CVA", priority=1)
@register_tool("MA", priority=1)
@tool
def get_hive_table_by_db(db_name: str, limit: int = 10, offset: int = 0) -> str:
    """
    获取 当前的 Hive 中指定数据库的所有表名
    参数：
        db_name：指定的数据库名。
        limit：可选，返回的最大表数量，默认 10。
        offset：可选，返回的表列表偏移量，默认 0。
    返回：
        错误信息或表名列表
    """
    if offset < 0:
        return "offset 必须大于等于 0"
    if not HiveSession:
        return "当前被禁止访问 Hive 数据库，请进行询问用户"
    with HiveSession() as session:
        result = session.execute(text(f"show databases"))
        db_names = [row[0] for row in result]
        if db_name not in db_names:
            return f"数据库 {db_name} 不存在"
        session.execute(text(f"use {db_name}"))
        result = session.execute(text(f"show tables"))
    table_names = [row[0] for row in result]
    max_table = len(table_names)
    filtered_table = table_names[min(offset, max_table):min(offset + limit, max_table)]
    return f"在 limit:{limit}，offset:{offset} 区间内的表为：{'\n'.join(filtered_table)}"


filter_type = Literal[
    "columns", "partition_info",
    "detailed_table_info", "storage_info",
    "constraints", "default_constraints",
]
filter_re_dict = {
    "columns": "columns",
    "partition_info": "Partition Information",
    "detailed_table_info": "Detailed Table Information",
    "storage_info": "Storage Information",
    "constraints": "Constraints",
    "default_constraints": "Default Constraints",
}
filter_re_key_set = set(filter_re_dict.keys())


@register_tool("DDA", priority=1)
@register_tool("DAA", priority=1)
@register_tool("CVA", priority=1)
@register_tool("MA", priority=1)
@tool
def get_hive_table_desc(
        db_name: str, table_name: str,
        filter_types: list[filter_type] | Literal["all"] = "all",
) -> str:
    """
    获取 当前的 Hive 中指定数据库指定表的描述信息
    参数：
        db_name：指定的数据库名。
        table_name：指定的表名。
        filter_types：可选，返回的描述信息类型，默认 "all"。
            - 当为字符串时，只能选 "all" -> 表示一次性返回所有信息
            - 当为列表时（可选一个或多个）
                - columns -> 列信息（列名、数据类型、注释）
                - partition_info -> 分区信息（分区列名、分区的数据类型、注释）
                - detailed_table_info -> 表的相关元数据，如：Database、OwnerType、Table Parameters等
                - storage_info -> 存储信息，如：SerDe Library、InputFormat、Storage Desc Params等
                - constraints -> 约束信息
                - default_constraints -> 默认约束信息
    注意：
        - 关于`注释`，可能会显示 `?` 的情况，这可能因为系统编码或者是原本表就出现了这种情况，所以请忽略或者询问用户。
    返回：
        错误信息或对应的描述信息
    """
    if isinstance(filter_types, str) and filter_types != "all":
        return f"当前输入的 filter_types 为 {filter_types}，你是否需要的是 'all'?"
    filter_types_set = set(filter_types)
    if isinstance(filter_types, list) and not filter_types_set <= filter_re_key_set:
        return f"当前输入的 filter_types 为 {','.join(filter_types_set)}，你是否需要的是 {','.join(filter_re_key_set)}"
    if not HiveSession:
        return "当前被禁止访问 Hive 数据库，请进行询问用户"
    with HiveSession() as session:
        result = session.execute(text(f"show databases"))
        db_names = [row[0] for row in result]
        if db_name not in db_names:
            return f"数据库 {db_name} 不存在"
        session.execute(text(f"use {db_name}"))
        result = session.execute(text(f"show tables"))
        table_names = [row[0] for row in result]
        if table_name not in table_names:
            return f"表 {table_name} 不存在"
        # 返回的是三元元组，(列名, 数据类型, 注释)
        result = session.execute(text(f"desc formatted {table_name}"))
    lines = [
        "\t".join("" if c is None else str(c).strip() for c in row)
        for row in result
    ]
    table_desc = "\n".join(lines).strip()
    if isinstance(filter_types, str) and filter_types == "all":
        return f"表 {table_name} 的描述信息为：\n{table_desc}"

    want = filter_types_set
    keep = False
    prev_title = ""
    output = []
    for line in table_desc.splitlines():
        line = line.rstrip()
        if not line.strip():
            continue
        if line.startswith("# "):
            # `# col_name` 是一个三列表头，需取第一个 \t 前的一段才是真正的标题
            title = line[2:].strip().split("\t", 1)[0].strip()
            if title == "col_name":
                # # col_name 出现两次：紧跟 # Partition Information 的是分区字段段，
                # 否则是表字段段 —— 用前一个标题区分归属，命中 filter_re_dict 的 key
                key = "partition_info" if prev_title == filter_re_dict["partition_info"] else "columns"
            else:
                # 用 filter_re_dict 反查：值等于标题的项对应的 key 即当前段
                key = next((k for k, v in filter_re_dict.items() if v == title), None)
            keep = key is not None and key in want
            prev_title = title
            if keep:
                output.append(line)
            continue
        if keep:
            output.append(line)
    return f"表 {table_name} 的描述信息为：\n" + "\n".join(output)


@register_tool("DDA", priority=1)
@register_tool("DAA", priority=1)
@register_tool("CVA", priority=1)
@register_tool("MA", priority=1)
@tool
def get_hive_data_by_table(
        db_name: str, table_name: str,
        is_random_sample: bool = False, percentage: float = 0.1,
        fields: list[str] | None = None,
        limit: int = 10, offset: int = 0
) -> str:
    """
    向某个 Hive 表取样数据
    参数：
        db_name：指定的数据库名。
        table_name：指定的表名。
        is_random_sample：可选，是否随机取样，默认 False。
        percentage：可选，取样的数据量占比，默认 0.1（最大为1）。
        fields：可选，指定需要查询的字段名列表，默认 None（返回所有字段数据）。
        limit：可选，返回的最大数据量，默认 10（最大为100）。
        offset：可选，返回的数据列表偏移量，默认 0。
    注意：
        - limit 参数 必须在 1 到 100 之间。若小于1时，将会强制设为1；若大于100时，将会强制设为100。
        - offset 参数 必须大于等于 0。若小于0时，将会强制设为0。
        - fields 参数 必须是 table 里面有的字段名，否则将会报错。
    返回：
        错误信息或数据列表
    """
    if percentage < 0 or percentage > 1:
        return "percentage 必须在 0 到 1 之间"

    limit = max(1, min(100, limit))
    offset = max(0, offset)

    if not HiveSession:
        return "当前被禁止访问 Hive 数据库，请进行询问用户"
    try:
        with HiveSession() as session:
            result = session.execute(text(f"show databases"))
            db_names = [row[0] for row in result]
            if db_name not in db_names:
                return f"数据库 {db_name} 不存在"
            session.execute(text(f"use {db_name}"))
            result = session.execute(text(f"show tables"))
            table_names = [row[0] for row in result]
            if table_name not in table_names:
                return f"表 {table_name} 不存在"
            if fields:
                fields_str = ", ".join(fields)
                basic_sql = f"select {fields_str} from {table_name}"
            else:
                basic_sql = f"select * from {table_name}"
            if is_random_sample:
                basic_sql += f" tablesample ({percentage * 100} percent)"
            basic_sql += f" limit {limit} offset {offset}"

            result = session.execute(text(basic_sql)).mappings().all()
            if not result:
                return f"表 {table_name} 没有数据"
            return f"表 {table_name} 的数据为：{result}"
    except Exception as e:
        logger.error(f"查询表 {table_name} 引发了错误{type(e).__name__}: {e}")
        return f"查询表 {table_name} 数据失败, 具体的错误信息为：{e}"


@register_tool("DDA", priority=1)
@register_tool("DAA", priority=1)
@tool
def ls_hdfs(path: str = "/") -> str:
    """
    列出 HDFS 上指定路径下的文件和目录
    参数：
        path：可选，指定的 HDFS 路径，默认 "/"。
    返回：
        - 成功：
            权限\t复制数\t所有者\t组\t类型\t大小\t修改时间\t路径
        - 失败：返回错误信息，例如："查询路径失败: 错误信息"
    """
    if not _hdfs_client:
        return "当前被禁止访问 HDFS 文件系统，请进行询问用户"
    try:
        root = _hdfs_client.list(path, status=True)
    except Exception as e:
        return f"查询路径失败: {e}"
    lines = []
    for i in root:
        status_dict = i[1]
        file_name = i[0]
        mod_time_ms = status_dict.get('modificationTime')
        mod_time = (datetime.fromtimestamp(mod_time_ms / 1000).strftime('%Y-%m-%d %H:%M:%S')
                    if mod_time_ms is not None else 'UNKNOWN')
        lines.append(f"{status_dict.get('permission', 0)}\t{status_dict.get('replication', '-')}"
                     f"\t{status_dict.get('owner', 'UNKNOWN')}\t{status_dict.get('group', 'UNKNOWN')}"
                     f"\t{status_dict.get('type', 'DIRECTORY')}\t{status_dict.get('length', 0)}"
                     f"\t{mod_time}\t{'/' if path == '/' else path + '/'}{file_name}")
    return f"Found {len(lines)} items" + "\n".join(lines)


@register_tool("DDA", priority=1)
@register_tool("DAA", priority=1)
@tool
def read_hdfs_file(
        path: str = "/", encoding: str = "utf-8",
        mode: Literal["cat", "head", "tail"] = "cat",
        n: int = 10
) -> str:
    """
    读取 HDFS 上指定路径下的文件内容
    参数：
        path：可选，指定的 HDFS 路径，默认 "/"。
    返回：
        - 成功：返回文件内容
        - 失败：返回错误信息，例如："读取文件失败: 错误信息"
    """
    if not _hdfs_client:
        return "当前被禁止访问 HDFS 文件系统，请进行询问用户"
    try:
        with _hdfs_client.read(path, encoding=encoding) as f:
            lines = f.readlines()
            if mode == "cat":
                return "\n".join(lines)
            elif mode == "head":
                return "\n".join(lines[:n])
            elif mode == "tail":
                return "\n".join(lines[-n:])
    except Exception as e:
        return f"读取文件失败: {e}"


# ============================================================================
# Kafka 环境感知工具（confluent-kafka，只读）
# 不建 topic、不改配置、不 commit offset；bootstrap 来自配置，不写死、不猜参数。
# ============================================================================

# describe_configs 返回的 configs 只保留这些对写作业/发现坑有用的键；其余一律丢弃
_KAFKA_CONFIG_KEEP_KEYS = {
    "cleanup.policy", "retention.ms", "retention.bytes",
    "min.insync.replicas", "compression.type", "segment.ms",
    "max.message.bytes", "delete.retention.ms", "message.timestamp.type",
}


def _kafka_metadata_topic():
    """拉一次全量 metadata；topic 包在大写时也能命中。返回 ClusterMetadata 或 Raised。"""
    if kafka_admin_client is None:
        return None
    return kafka_admin_client.list_topics(timeout=10)


@register_tool("DDA", priority=1)
@register_tool("DAA", priority=1)
@register_tool("MA", priority=1)
@tool
def get_kafka_env() -> str:
    """
    作用：探测/返回当前可用的 Kafka 集群环境（连接谁的 Kafka、有哪些 broker）。
          是写消费/生产作业前必须读取的环境信息；bootstrap 来自配置，不凭空猜。
    返回：JSON 字符串，含 cluster_access（enabled/disabled）、bootstrap_servers、
          live_probe（ok/failed/disabled）、brokers、controller_id、needs_config、message。
    """
    if kafka_admin_client is None:
        return _json({"cluster_access": "disabled", "live_probe": "failed",
                      "needs_config": False,
                      "message": "当前 Kafka 环境不可用，请询问用户"})
    try:
        metadata = kafka_admin_client.list_topics(timeout=10)
    except Exception as exc:  # noqa: BLE001
        logger.warning("探测 Kafka 集群失败：%s: %s", type(exc).__name__, exc)
        return _json({"cluster_access": "enabled", "live_probe": "failed",
                      "bootstrap_servers": bootstrap_servers,
                      "needs_config": True,
                      "message": f"连接 {bootstrap_servers} 失败：{type(exc).__name__}: {exc}；"
                                 "请询问用户当前 Kafka 是否可用"})
    brokers = [
        {"id": b.id, "host": b.host, "port": b.port}
        for b in metadata.brokers.values()
    ]
    return _json({
        "cluster_access": "enabled",
        "live_probe": "ok",
        "bootstrap_servers": bootstrap_servers,
        "brokers": brokers,
        "controller_id": metadata.controller_id,
        "needs_config": False,
        "message": "Kafka 环境可用",
    })


@register_tool("DDA", priority=1)
@register_tool("DAA", priority=1)
@tool
def list_kafka_topics() -> str:
    """
    作用：列出 Kafka 集群中当前真实存在的所有 topic。
          写消费/生产作业前先看这里，避免凭记忆写不存在的 topic 名。
    参数：无。
    返回：JSON 字符串，含 topics（topic/partitions/replication_factor）、total、needs_config。
    """
    if kafka_admin_client is None:
        return _json({"cluster_access": "disabled", "live_probe": "failed",
                      "needs_config": False,
                      "message": "当前 Kafka 环境不可用，请询问用户"})
    try:
        metadata = _kafka_metadata_topic()
    except Exception as exc:  # noqa: BLE001
        logger.warning("列出 Kafka topics 失败：%s: %s", type(exc).__name__, exc)
        return _json({"error": f"列出 Kafka topics 失败：{type(exc).__name__}: {exc}",
                      "needs_config": True})
    topics = []
    for name, tmd in metadata.topics.items():
        parts = tmd.partitions
        replication = 0
        if parts:
            first = next(iter(parts.values()))
            replication = len(first.replicas) if first.replicas else 0
        topics.append({"topic": name, "partitions": len(parts), "replication_factor": replication})
    topics.sort(key=lambda t: t["topic"])
    return _json({"ok": True, "total": len(topics), "topics": topics,
                  "contains_topic": [t["topic"] for t in topics[:50]],
                  "needs_config": False})


@register_tool("DDA", priority=1)
@tool
def get_kafka_topic_desc(topic: str, include_configs: bool = False) -> str:
    """
    作用：查看单个 Kafka topic 的详细环境信息：分区布局（leader/replicas/ISR）、
          各分区 offset 水位线、以及（可选）精简过的 topic 配置。
    参数：
        topic：Kafka topic 名。
        include_configs：可选，默认 False。true 时才额外拉取 topic 配置
            （只保留 cleanup.policy/retention.ms/min.insync.replicas 等关键项，其余丢弃），
            该调用相对更贵，是否取由 Agent 自行决定。
    返回：JSON 字符串，含 exists、partitions、configs（include_configs=true 时）、message。
    """
    if kafka_admin_client is None:
        return _json({"cluster_access": "disabled", "live_probe": "failed",
                      "needs_config": False,
                      "message": "当前 Kafka 环境不可用，请询问用户"})
    if not topic or not topic.strip():
        return _json({"error": "topic 不能为空"})
    topic = topic.strip()
    try:
        metadata = _kafka_metadata_topic()
    except Exception as exc:  # noqa: BLE001
        logger.warning("读取 topic 元数据失败：%s: %s", type(exc).__name__, exc)
        return _json({"error": f"读取 topic 元数据失败：{type(exc).__name__}: {exc}",
                      "needs_config": True})
    tmd = metadata.topics.get(topic)
    if tmd is None:
        tmd = metadata.topics.get(f"___{topic}") or next(
            (v for k, v in metadata.topics.items() if k.upper() == topic.upper()), None)
    if tmd is None:
        return _json({"topic": topic, "exists": False, "message": f"topic {topic} 不存在"})
    real_name = next(k for k in metadata.topics.keys()
                     if k == topic or k.upper() == topic.upper())
    partitions = []
    try:
        tp_by_part = {p: TopicPartition(real_name, p) for p in tmd.partitions.keys()}
        low_res = kafka_admin_client.list_offsets(
            {tp: OffsetSpec.earliest() for tp in tp_by_part.values()}, request_timeout=10)
        high_res = kafka_admin_client.list_offsets(
            {tp: OffsetSpec.latest() for tp in tp_by_part.values()}, request_timeout=10)
        low_off = {tp.partition: fut.result(10).offset for tp, fut in low_res.items()}
        high_off = {tp.partition: fut.result(10).offset for tp, fut in high_res.items()}
        for p, ptd in tmd.partitions.items():
            partitions.append({
                "partition": p,
                "leader": ptd.leader,
                "replicas": list(ptd.replicas or []),
                "isr": list(ptd.isrs or []),
                "offset_low": low_off.get(p),
                "offset_high": high_off.get(p),
            })
    except Exception as exc:  # noqa: BLE001
        logger.warning("读取 topic 水位线失败（降级为仅元数据）：%s: %s", type(exc).__name__, exc)
        for p, ptd in tmd.partitions.items():
            partitions.append({
                "partition": p,
                "leader": ptd.leader,
                "replicas": list(ptd.replicas or []),
                "isr": list(ptd.isrs or []),
                "offset_low": None, "offset_high": None,
            })

    configs = {}
    if include_configs:
        try:
            from confluent_kafka.admin import ConfigResource
            res = kafka_admin_client.describe_configs(
                [ConfigResource(ConfigResource.Type.TOPIC, real_name)], request_timeout=10)
            future = res[ConfigResource(ConfigResource.Type.TOPIC, real_name)]
            for name, entry in (future.result(10) or {}).items():
                if name in _KAFKA_CONFIG_KEEP_KEYS and entry.value is not None and not entry.is_sensitive:
                    configs[name] = str(entry.value)
        except Exception as exc:  # noqa: BLE001
            logger.warning("读取 topic 配置失败（忽略）：%s: %s", type(exc).__name__, exc)
            configs = {"error": f"读取 topic 配置失败：{type(exc).__name__}"}
    return _json({"topic": real_name, "exists": True,
                  "partitions": partitions, "configs": configs,
                  "needs_config": False})


@register_tool("DDA", priority=1)
@tool
def get_kafka_consumer_groups() -> str:
    """
    作用：查看当前 Kafka 集群有多少消费组、各自订阅哪些分区、消费到哪个 offset、
          是否落后（lag）——用于判断作业是否挂了/消费积压。
    参数：无。
    返回：JSON 字符串，含 groups（group_id/state/partitions/current_offset/log_end_offset/lag）、
          total_groups、needs_config。
    """
    if kafka_admin_client is None:
        return _json({"cluster_access": "disabled", "live_probe": "failed",
                      "needs_config": False,
                      "message": "当前 Kafka 环境不可用，请询问用户"})
    try:
        fut = kafka_admin_client.list_consumer_groups(request_timeout=10)
        listing = fut.result(10)  # list_consumer_groups 返回 Future，需 result() 取 ListConsumerGroupsResult
    except Exception as exc:  # noqa: BLE001
        logger.warning("列出 consumer groups 失败：%s: %s", type(exc).__name__, exc)
        return _json({"error": f"列出 consumer groups 失败：{type(exc).__name__}: {exc}",
                      "needs_config": True})

    def _state(value):
        # ConsumerGroupState 是枚举；统一转成可读字符串（如 STABLE/EMPTY）
        if value is None:
            return ""
        name = getattr(value, "name", None)
        if name:
            return name
        return str(value)

    valid_groups = list(getattr(listing, "valid", None) or [])
    result = []
    for g in valid_groups:
        group_id = getattr(g, "group_id", None)
        state = _state(getattr(g, "state", None))
        if not group_id or state == "DEAD":
            continue
        group_detail = {"group_id": group_id, "state": state}
        try:
            # 1) 成员数：describe_consumer_groups
            dres = kafka_admin_client.describe_consumer_groups([group_id], request_timeout=10)
            desc = dres[group_id].result(10)
            members = list(getattr(desc, "members", None) or [])
            group_detail["members"] = len(members)
            # 2) 该组各分区已提交 offset（不给 partitions => 返回该组全部）
            off_req = [ConsumerGroupTopicPartitions(group_id)]
            ores = kafka_admin_client.list_consumer_group_offsets(off_req, request_timeout=10)
            cgtp = ores[group_id].result(10)
            group_tps = list(getattr(cgtp, "topic_partitions", None) or [])
            commits = {}
            for tp in group_tps:
                # tp.offset == -1 表示无提交 offset
                commits[(tp.topic, tp.partition)] = None if getattr(tp, "offset", -1) < 0 else tp.offset
            # 3) log_end offset（纯 admin，不消费）算 lag
            end_res = kafka_admin_client.list_offsets(
                {tp: OffsetSpec.latest() for tp in group_tps}, request_timeout=10)
            end_off = {(tp.topic, tp.partition): fut.result(10).offset for tp, fut in end_res.items()}

            rows = []
            seen = set()
            for tp in group_tps:
                key = (tp.topic, tp.partition)
                if key in seen:
                    continue
                seen.add(key)
                cur = commits.get(key)
                end = end_off.get(key)
                rows.append({
                    "topic": tp.topic,
                    "partition": tp.partition,
                    "current_offset": cur,
                    "log_end_offset": end,
                    "lag": (end - cur) if (end is not None and cur is not None) else None,
                })

            group_detail["partitions"] = rows
        except Exception as exc:  # noqa: BLE001
            logger.warning("读取消费组 %s 详情失败：%s: %s", group_id, type(exc).__name__, exc)
            group_detail["partitions"] = []
            group_detail["error"] = f"{type(exc).__name__}: {exc}"
        result.append(group_detail)
    return _json({"groups": result, "total_groups": len(result), "needs_config": False})


@register_tool("DDA", priority=1)
@register_tool("DAA", priority=1)
@tool
def get_cluster_status() -> str:
    """
    作用：获取当前 YARN 集群状态与版本信息
    参数：无。
    返回：{state, haState, version} 或 错误信息
    """
    if rm is None:
        return _json({"error": "当前 YARN 集群不可用，请询问用户"})
    try:
        cluster_info = rm.cluster_information().data
    except Exception as e:
        logger.error(f"获取 YARN 集群状态失败: {e}")
        return _json({"error": "获取 YARN 集群状态失败"})
    if cluster_info is None:
        return _json({"error": "获取 YARN 集群状态失败"})
    clusterInfo = cluster_info.get("clusterInfo", {})
    return _json({
        "state": clusterInfo.get("state", ""),
        "haState": clusterInfo.get("haState", ""),
        "version": clusterInfo.get("hadoopVersion", ""),
    })


node_status = Literal[
    "NEW", "RUNNING", "UNHEALTHY",
    "DECOMMISSIONING", "DECOMMISSIONED",
    "LOST", "REBOOTED", "SHUTDOWN"
]


@register_tool("DDA", priority=1)
@register_tool("DAA", priority=1)
@tool
def get_cluster_node_ids(status: node_status | None = None) -> str:
    """
    获取 YARN 集群节点 ID 列表
    参数：status - 节点状态(可选, 默认 None -> 表示筛选所有状态)
        - NEW -> 新节点
        - RUNNING -> 运行中节点
        - UNHEALTHY -> 不健康节点
        - DECOMMISSIONING -> 退出中节点
        - DECOMMISSIONED -> 退出节点
        - LOST -> 丢失节点
        - REBOOTED -> 重启中节点
        - SHUTDOWN -> 关闭节点
    返回：节点 ID 列表 或 错误信息
    """
    if rm is None:
        return _json({"error": "当前 YARN 集群不可用，请询问用户"})
    try:
        nodes_info = rm.cluster_nodes(states=status).data
    except Exception as e:
        logger.error(f"获取 YARN 集群节点信息失败: {e}")
        return _json({"error": "获取 YARN 集群节点信息失败"})
    if nodes_info is None:
        return _json({"error": "获取 YARN 集群节点信息失败"})
    nodes = nodes_info.get("nodes", {}).get("node", [])
    node_ids = [node.get("id", "") for node in nodes]
    return _json({"node_ids": node_ids})


@register_tool("DDA", priority=1)
@register_tool("DAA", priority=1)
@tool
def get_cluster_node_info(node_id: str) -> str:
    """
    获取 YARN 集群节点信息
    参数：node_id - 节点 ID
    返回：节点信息 或 错误信息
    """
    if rm is None:
        return _json({"error": "当前 YARN 集群不可用，请询问用户"})
    try:
        node_info = rm.cluster_node(node_id).data
    except Exception as e:
        logger.error(f"获取 YARN 集群节点信息失败: {e}")
        return _json({"error": f"获取 YARN 集群节点信息失败({type(e).__name__}): {e}"})
    if node_info is None:
        return _json({"error": "获取 YARN 集群节点信息失败"})
    node = node_info.get("node", {})
    return _json({"node": node})
