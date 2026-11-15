"""项目 resources 拷贝/替换/回滚/守卫回归（本地 Postgres；测试后清理）。"""
import asyncio
import shutil
import tempfile
import unittest
import uuid
from datetime import datetime, timezone
from pathlib import Path

from langchain_core.messages import AIMessage
from sqlalchemy import delete, select
from unittest.mock import patch

import services.cluster_materialize as mm
import services.cluster_env_resolver as resolver
from SPO.cluster_env_model import (
    ClusterInfo,
    ClusterProfile,
    ConfigDrift,
    ConfigSnapshot,
    ConfigSnapshotArchive,
    ConfigSnapshotFile,
    EnvEvent,
    ProfileFile,
    ProjectResource,
    PublicConfig,
)
from SPO.postgresql_model import AdminProject, AdminProjectConversation
from Tasks.project_registry import project_registry
from Tools.db import SessionLocal
from Tools.node_ssh import SshResult
from load_config.config import ROOT_BASE_DIR_PATH
from middleware.tool_call_guard import ToolCallHistoryGuard, repair_missing_tool_messages
from services.cluster_materialize import (
    check_project,
    materialize_project,
    project_resource_info,
    replace_project_resource,
    resolve_project_drift,
)
from services.cluster_profile_store import ProfileStoreError, profile_store
from services.cluster_snapshot_store import ClusterSnapshotStore
from services.project_resource_guard import check_project_fast, invalidate_project_guard


class ClusterProjectCopyTest(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="cluster_copy_test_"))
        self.public = self.tmp / "public"
        self.archive = self.tmp / "archive"
        self.public.mkdir(parents=True)
        self.public_name = f"__test_pub_{uuid.uuid4().hex}"
        self.store = ClusterSnapshotStore(
            public_root=self.public, archive_root=self.archive,
            public_config_name=self.public_name,
        )
        self._old_store = mm.snapshot_store
        self._old_resolver_store = resolver.snapshot_store
        mm.snapshot_store = self.store
        resolver.snapshot_store = self.store
        self.start = datetime.now(timezone.utc)

        self.project_id = str(uuid.uuid4())
        self.project_name = f"__proj_{uuid.uuid4().hex}"
        self.target = (ROOT_BASE_DIR_PATH / "tmp_code" / "flink" /
                       "2026-11-15" / self.project_name / "src/main/resources")
        with SessionLocal() as session:
            session.add(AdminProject(
                project_id=self.project_id, admin_id=1, thread_id="test",
                title="test", description="", summary="", app_type="flink",
                project_date="2026-11-15", project_name=self.project_name,
                status="active", verify_status="unknown",
            ))
            session.commit()

        self.profile = profile_store.create_profile(
            1, f"__profile_{uuid.uuid4().hex}",
            cluster_name=f"__cluster_{uuid.uuid4().hex}",
        )
        self.profile_id = self.profile["id"]
        self.cluster_id = self.profile["cluster_id"]
        profile_store.upload_files(
            1, self.profile_id,
            [("core-site.xml",
              b"<configuration><property><name>fs.defaultFS</name>"
              b"<value>hdfs://profile:8020</value></property></configuration>")],
            reason="test",
        )

    def tearDown(self):
        mm.snapshot_store = self._old_store
        resolver.snapshot_store = self._old_resolver_store
        with SessionLocal() as session:
            snapshots = session.execute(
                select(ConfigSnapshot).where(
                    (ConfigSnapshot.project_id == self.project_id) |
                    (ConfigSnapshot.profile_id == self.profile_id) |
                    (ConfigSnapshot.public_config_id.in_(
                        select(PublicConfig.id).where(PublicConfig.name == self.public_name)
                    ))
                )
            ).scalars().all()
            ids = [row.id for row in snapshots]
            session.execute(delete(ConfigDrift).where(ConfigDrift.project_id == self.project_id))
            session.execute(delete(ProjectResource).where(
                ProjectResource.project_id == self.project_id
            ))
            if ids:
                session.execute(delete(ConfigSnapshotArchive).where(
                    ConfigSnapshotArchive.snapshot_id.in_(ids)
                ))
                session.execute(delete(ConfigSnapshotFile).where(
                    ConfigSnapshotFile.snapshot_id.in_(ids)
                ))
                session.execute(delete(ConfigSnapshot).where(ConfigSnapshot.id.in_(ids)))
            public = session.execute(
                select(PublicConfig).where(PublicConfig.name == self.public_name)
            ).scalar_one_or_none()
            if public is not None:
                session.execute(delete(PublicConfig).where(PublicConfig.id == public.id))
            session.execute(delete(ProfileFile).where(
                ProfileFile.profile_id == self.profile_id
            ))
            session.execute(delete(ClusterProfile).where(
                ClusterProfile.id == self.profile_id
            ))
            session.execute(delete(ClusterInfo).where(ClusterInfo.id == self.cluster_id))
            session.execute(delete(AdminProjectConversation).where(
                AdminProjectConversation.project_id == self.project_id
            ))
            session.execute(delete(AdminProject).where(
                AdminProject.project_id == self.project_id
            ))
            session.execute(delete(EnvEvent).where(EnvEvent.created_at >= self.start))
            session.commit()
        shutil.rmtree(self.target.parent.parent.parent, ignore_errors=True)
        shutil.rmtree(profile_store.storage_root / str(self.profile_id), ignore_errors=True)
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_public_autocopy_replace_and_keep(self):
        (self.public / "core-site.xml").write_bytes(
            b"<configuration><property><name>fs.defaultFS</name>"
            b"<value>hdfs://public:8020</value></property></configuration>"
        )
        result = materialize_project(1, self.project_id, reason="test")
        self.assertTrue((self.target / "core-site.xml").is_file())
        self.assertEqual(result["files"], ["core-site.xml"])
        self.assertTrue(check_project(1, self.project_id)["clean"])

        replaced = replace_project_resource(
            1, self.project_id, "custom/app.properties", content=b"a=b\n", reason="add"
        )
        self.assertIn("custom/app.properties", replaced["files"])
        self.assertTrue((self.target / "custom/app.properties").is_file())
        self.assertTrue(check_project(1, self.project_id)["clean"])

        (self.target / "core-site.xml").write_bytes(b"<configuration/>")
        drift = check_project(1, self.project_id)
        self.assertFalse(drift["clean"])
        resolve_project_drift(1, drift["drift_id"], "keep", "keep")
        self.assertTrue(check_project(1, self.project_id)["clean"])

    def test_rollback_prefers_profile_revision(self):
        result = materialize_project(
            1, self.project_id, profile_id=self.profile_id, reason="profile"
        )
        self.assertEqual(result["files"], ["core-site.xml"])
        self.assertTrue((self.target / "core-site.xml").is_file())
        (self.target / "core-site.xml").write_bytes(b"<configuration/>")
        drift = check_project(1, self.project_id)
        self.assertFalse(drift["clean"])
        rolled = resolve_project_drift(1, drift["drift_id"], "rollback", "rollback")
        self.assertEqual(rolled["unrestorable"], [])
        self.assertIn(b"hdfs://profile:8020", (self.target / "core-site.xml").read_bytes())
        self.assertTrue(check_project(1, self.project_id)["clean"])

    def test_large_file_only_hash(self):
        materialize_project(1, self.project_id, profile_id=self.profile_id, reason="profile")
        big = b"x" * (profile_store.max_file_bytes + 10)
        data = replace_project_resource(1, self.project_id, "data/blob.bin",
                                        content=big, reason="big")
        self.assertIn("data/blob.bin", data["content_omitted"])
        info = project_resource_info(1, self.project_id)
        self.assertIn("data/blob.bin", info["content_omitted"])
        self.assertTrue(check_project(1, self.project_id)["clean"])

    def test_resolver_local_first_does_not_probe(self):
        materialize_project(1, self.project_id, profile_id=self.profile_id, reason="profile")
        old_probe = resolver.probe_cluster_env
        called = {"value": False}

        def fake_probe():
            called["value"] = True
            return {"ok": False, "error": "should not be called"}

        resolver.probe_cluster_env = fake_probe
        try:
            payload = resolver.resolve_cluster_env(1, self.project_id)
        finally:
            resolver.probe_cluster_env = old_probe
        self.assertFalse(called["value"])
        self.assertEqual(payload["live_probe"], "not_needed")
        self.assertIn("hdfs://profile:8020", payload["configs"]["core-site.xml"])

    def test_profile_delete_refused_when_referenced(self):
        materialize_project(1, self.project_id, profile_id=self.profile_id, reason="profile")
        with self.assertRaises(ProfileStoreError):
            profile_store.delete_profile(1, self.profile_id)

    def test_delete_thread_archives_last_project(self):
        with SessionLocal() as session:
            session.add(AdminProjectConversation(
                project_id=self.project_id, thread_id="thread-x",
                start_message_id="m1",
            ))
            session.commit()
        result = asyncio.run(project_registry.handle_thread_delete("thread-x", 1))
        self.assertEqual(result["segments"], 1)
        self.assertEqual([item["project_id"] for item in result["archived"]], [self.project_id])
        with SessionLocal() as session:
            project = session.execute(select(AdminProject).where(
                AdminProject.project_id == self.project_id
            )).scalar_one()
            self.assertEqual(project.status, "archived")

    def test_guard_content_hash_ignores_delete_readd_same_content(self):
        materialize_project(1, self.project_id, profile_id=self.profile_id, reason="profile")
        invalidate_project_guard(self.project_id)
        self.assertTrue(check_project_fast(1, self.project_id)["clean"])
        content = (self.target / "core-site.xml").read_bytes()
        (self.target / "core-site.xml").unlink()
        (self.target / "core-site.xml").write_bytes(content)
        self.assertTrue(check_project_fast(1, self.project_id)["clean"])
        (self.target / "core-site.xml").write_bytes(b"<configuration/>")
        self.assertFalse(check_project_fast(1, self.project_id)["clean"])

    def test_resolver_disabled_fallback_message(self):
        with patch.object(resolver, "cluster_access_enabled", return_value=False):
            payload = resolver.resolve_cluster_env(1, "")
        self.assertEqual(payload["cluster_access"], "disabled")
        self.assertEqual(payload["live_probe"], "disabled")
        self.assertTrue(payload["needs_config"])
        self.assertNotIn("已返回本地配置", payload["message"])

    def test_resolver_remote_paths_when_enabled(self):
        fake = SshResult(ok=True, rc=0, stdout=(
            "PATH|/opt/hadoop/etc/hadoop/core-site.xml\n"
            "KV|fs.defaultFS|hdfs://node:8020\n"
        ))
        resolver._probe_cache.clear()
        with patch.object(resolver, "cluster_access_enabled", return_value=True), \
                patch.object(resolver.node_ssh, "run", return_value=fake):
            payload = resolver.resolve_cluster_env(1, "")
        self.assertEqual(payload["cluster_access"], "enabled")
        self.assertEqual(payload["live_probe"], "ok")
        self.assertEqual(payload["values"]["fs.defaultFS"], "hdfs://node:8020")
        self.assertNotIn("强制禁止连接集群", payload["message"])

    def test_profile_delete_allowed_when_unreferenced(self):
        result = profile_store.delete_profile(1, self.profile_id, "unused")
        self.assertTrue(result["deleted"])
        with self.assertRaises(ProfileStoreError):
            profile_store.get_profile(1, self.profile_id)

    def test_bootstrap_empty_does_not_create_snapshot(self):
        from services.cluster_materialize import bootstrap_cluster_env
        data = bootstrap_cluster_env(0)
        self.assertTrue(data["ok"])
        self.assertEqual(data["files"], 0)
        with SessionLocal() as session:
            public = session.execute(select(PublicConfig).where(
                PublicConfig.name == self.public_name
            )).scalar_one_or_none()
            self.assertIsNone(public)

    def test_bootstrap_creates_initial_snapshot_once(self):
        from services.cluster_materialize import bootstrap_cluster_env
        (self.public / "core-site.xml").write_bytes(
            b"<configuration><property><name>fs.defaultFS</name>"
            b"<value>hdfs://public:8020</value></property></configuration>"
        )
        first = bootstrap_cluster_env(0)
        self.assertTrue(first["ok"])
        self.assertIsNotNone(first["snapshot_id"])
        second = bootstrap_cluster_env(0)
        self.assertEqual(first["snapshot_id"], second["snapshot_id"])

    def test_repair_missing_tool_message(self):
        message = AIMessage(
            content="", tool_calls=[{"id": "call-1", "name": "info_inquiry", "args": {}}]
        )
        repairs = repair_missing_tool_messages([message])
        self.assertEqual(len(repairs), 1)
        self.assertEqual(repairs[0].tool_call_id, "call-1")

        runtime = type("R", (), {"context": type("C", (), {"thread_id": "t"})()})()
        update = ToolCallHistoryGuard("test").before_model(
            {"messages": [message]}, runtime
        )
        self.assertEqual(len(update["messages"]), 1)


if __name__ == "__main__":
    unittest.main()
