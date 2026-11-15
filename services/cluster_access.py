"""集群访问开关与无集群时的兜底返回。

connect_to_cluster=false 时：先返回 public/profile XML，再返回固定话术；不发起 SSH。
"""
from __future__ import annotations

from typing import Optional

from load_config.config import config
from services.cluster_profile_store import ProfileStoreError, profile_store
from services.cluster_snapshot_store import snapshot_store

DISABLED_WITH_CONFIG = (
    "当前环境强制禁止连接集群，已返回本地 XML 配置；"
    "如需实时信息请询问用户或使用兜底查询工具"
)
DISABLED_NO_CONFIG = "当前环境强制禁止连接集群，请进行询问用户或者走兜底查询工具"


def cluster_access_enabled() -> bool:
    return bool((config.get("app") or {}).get("connect_to_cluster", False))


def build_cluster_env_fallback(admin_id: int, profile_id: Optional[int] = None) -> dict:
    """connect_to_cluster=false / live probe 不可用时的结构化返回。"""
    contents: dict[str, str] = {}
    sources: list[str] = []
    public = snapshot_store.get_public(include_content=True)
    if public and public.get("contents"):
        contents.update({k: v for k, v in public["contents"].items() if v is not None})
        sources.append(f"public:{public['snapshot_id']}")
    if profile_id is not None:
        try:
            files = profile_store.list_files(admin_id, int(profile_id))
            for item in files:
                data = profile_store.read_file(admin_id, int(profile_id), item["filename"])
                contents[item["filename"]] = data["content"]
            sources.append(f"profile:{int(profile_id)}:{files[0]['revision'] if files else 0}")
        except ProfileStoreError:
            pass
    has_config = bool(contents)
    return {
        "cluster_access": "disabled",
        "live_probe": "disabled",
        "source": "+".join(sources) if sources else "none",
        "configs": contents,
        "message": DISABLED_WITH_CONFIG if has_config else DISABLED_NO_CONFIG,
    }
