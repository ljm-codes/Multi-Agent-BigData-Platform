"""快照/公有资源/归档/GC 集成测试（本地 Postgres；测试后清理）。"""
import shutil
import tempfile
import unittest
import uuid
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from sqlalchemy import delete, select

from SPO.cluster_env_model import (
    ConfigSnapshot,
    ConfigSnapshotArchive,
    ConfigSnapshotFile,
    EnvEvent,
    PublicConfig,
)
from Tools.db import SessionLocal
from services.cluster_access import (
    DISABLED_NO_CONFIG,
    DISABLED_WITH_CONFIG,
    build_cluster_env_fallback,
    cluster_access_enabled,
)
from services.cluster_profile_store import ProfileStoreError
from services.cluster_snapshot_store import ClusterSnapshotStore


class ClusterSnapshotStoreTest(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="cluster_snapshot_test_"))
        self.public = self.tmp / "public"
        self.archive = self.tmp / "archive"
        self.public.mkdir(parents=True)
        self.name = f"__test_public_{uuid.uuid4().hex}"
        self.store = ClusterSnapshotStore(
            public_root=self.public,
            archive_root=self.archive,
            keep_versions=0,
            retention_days=0,
            public_config_name=self.name,
        )
        self.start = datetime.now(timezone.utc)

    def tearDown(self):
        with SessionLocal() as session:
            public = session.execute(
                select(PublicConfig).where(PublicConfig.name == self.name)
            ).scalar_one_or_none()
            if public is not None:
                snapshots = session.execute(
                    select(ConfigSnapshot).where(ConfigSnapshot.public_config_id == public.id)
                ).scalars().all()
                ids = [snapshot.id for snapshot in snapshots]
                if ids:
                    session.execute(delete(ConfigSnapshotArchive).where(
                        ConfigSnapshotArchive.snapshot_id.in_(ids)
                    ))
                    session.execute(delete(ConfigSnapshotFile).where(
                        ConfigSnapshotFile.snapshot_id.in_(ids)
                    ))
                    session.execute(delete(ConfigSnapshot).where(ConfigSnapshot.id.in_(ids)))
                session.execute(delete(PublicConfig).where(PublicConfig.id == public.id))
            session.execute(delete(EnvEvent).where(EnvEvent.created_at >= self.start))
            session.commit()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _write(self, name: str, content: bytes) -> None:
        (self.public / name).write_bytes(content)

    def test_scan_archive_gc_restore(self):
        self._write("core-site.xml", b"<configuration/>")
        first = self.store.scan_public(1, "scan1")
        self.assertEqual(first["version"], 1)
        self.assertEqual(first["contents"]["core-site.xml"], "<configuration/>")
        first_id = first["snapshot_id"]

        self._write("hive-site.xml",
                    b"<configuration><property><name>x</name><value>1</value></property></configuration>")
        second = self.store.scan_public(1, "scan2")
        self.assertEqual(second["version"], 2)
        with SessionLocal() as session:
            old = session.get(ConfigSnapshot, first_id)
            self.assertEqual(old.status, "superseded")
            public = session.execute(
                select(PublicConfig).where(PublicConfig.name == self.name)
            ).scalar_one()
            self.assertEqual(public.current_snapshot_id, second["snapshot_id"])

        dry = self.store.run_gc(1, dry_run=True)
        self.assertEqual(dry["candidates"][0]["snapshot_id"], first_id)
        result = self.store.run_gc(1, dry_run=False)
        self.assertEqual(result["candidates"][0]["action"], "archived_purged")
        with SessionLocal() as session:
            old = session.get(ConfigSnapshot, first_id)
            self.assertEqual(old.status, "purged")
            files = session.execute(
                select(ConfigSnapshotFile).where(ConfigSnapshotFile.snapshot_id == first_id)
            ).scalars().all()
            self.assertTrue(all(row.content is None for row in files))

        archives = self.store.list_archives()
        self.assertEqual(len(archives), 1)
        archive = archives[0]
        self.assertTrue(self.store.archive_path(archive["archive_id"]).is_file())

        restored = self.store.restore_archive(1, archive["archive_id"], "restore")
        self.assertEqual(restored["version"], 3)
        current = self.store.get_public(include_content=True)
        self.assertEqual(current["version"], 3)
        self.assertIn("core-site.xml", current["contents"])
        self.assertNotIn("hive-site.xml", current["contents"])

        self.store.delete_archive(1, archive["archive_id"], "done")
        with self.assertRaises(ProfileStoreError):
            self.store.archive_path(archive["archive_id"])
        with self.assertRaises(ProfileStoreError):
            self.store.restore_archive(1, archive["archive_id"], "again")

    def test_cluster_access_fallback(self):
        # 本机 config.yaml 现在是 connect_to_cluster=true；这个用例只测禁用分支
        with patch("tests.cluster_snapshot_store_test.cluster_access_enabled", return_value=False):
            self.assertFalse(cluster_access_enabled())
            payload = build_cluster_env_fallback(1)
            self.assertEqual(payload["cluster_access"], "disabled")
            self.assertEqual(payload["live_probe"], "disabled")
            self.assertIn(payload["message"], (DISABLED_WITH_CONFIG, DISABLED_NO_CONFIG))


if __name__ == "__main__":
    unittest.main()
