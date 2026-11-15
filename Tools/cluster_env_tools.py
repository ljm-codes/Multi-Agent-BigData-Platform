"""集群/profile/project 资源感知工具。

给 DDA/CVA/MA 在生成、验证前读取环境与项目资源；只读，不改资源。
"""
import json

from langchain_core.tools import tool
from langgraph.prebuilt import ToolRuntime
from sqlalchemy import select, and_

from SPO.cluster_env_model import ProjectResource
from SPO.postgresql_model import AdminProject
from Tasks.project_registry import admin_id_of
from Tools.db import SessionLocal
from Tools.log_settings import LogSetting
from Tools.registry import register_tool
from services.cluster_env_resolver import resolve_cluster_env
from services.cluster_materialize import check_project

logger = LogSetting.create(__name__)


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
