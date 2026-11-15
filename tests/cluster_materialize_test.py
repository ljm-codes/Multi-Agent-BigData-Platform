"""项目 materialize + project/public drift 集成测试（本地 Postgres；测试后清理）。"""
import shutil
import tempfile
import unittest
import uuid
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy import delete, or_, select

import services.cluster_materialize as materialize_module
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
from SPO.postgresql_model import AdminProject
from Tools.db import SessionLocal
from load_config.config import ROOT_BASE_DIR_PATH
from services.cluster_materialize import (
    check_project,
    check_public,
    materialize_project,
    resolve_project_drift,
    resolve_public_drift,
)
from services.cluster_profile_store import profile_store
from services.cluster_snapshot_store import ClusterSnapshotStore


class ClusterMaterializeTest(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="cluster_materialize_test_"))
        self.public = self.tmp / "public"
        self.archive = self.tmp / "archive"
        self.public.mkdir(parents=True)
        self.public_name = f"__test_pub_{uuid.uuid4().hex}"
        self.store = ClusterSnapshotStore(
            public_root=self.public, archive_root=self.archive,
            public_config_name=self.public_name,
        )
        self._old_store = materialize_module.snapshot_store
        materialize_module.snapshot_store = self.store
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

        (self.public / "core-site.xml").write_bytes(b"<configuration/>")
        self.store.scan_public(1, "public init")

        self.profile = profile_store.create_profile(
            1, f"__profile_{uuid.uuid4().hex}",
            cluster_name=f"__cluster_{uuid.uuid4().hex}",
            fs_default_fs="hdfs://node1:8020",
            hive_metastore_uris="thrift://node1:9083",
            kafka_bootstrap="node1:9092",
        )
        self.profile_id = self.profile["id"]
        self.cluster_id = self.profile["cluster_id"]
        profile_store.upload_files(
            1, self.profile_id,
            [("hive-site.xml",
              b"<configuration><property><name>a</name><value>1</value></property></configuration>")],
            reason="profile init",
        )

    def tearDown(self):
        materialize_module.snapshot_store = self._old_store
        with SessionLocal() as session:
            snapshots = session.execute(
                select(ConfigSnapshot).where(or_(
                    ConfigSnapshot.project_id == self.project_id,
                    ConfigSnapshot.profile_id == self.profile_id,
                    ConfigSnapshot.public_config_id.in_(
                        select(PublicConfig.id).where(PublicConfig.name == self.public_name)
                    ),
                ))
            ).scalars().all()
            ids = [snapshot.id for snapshot in snapshots]
            session.execute(delete(ConfigDrift).where(or_(
                ConfigDrift.project_id == self.project_id,
                ConfigDrift.public_config_id.in_(
                    select(PublicConfig.id).where(PublicConfig.name == self.public_name)
                ),
            )))
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
            session.execute(delete(ClusterInfo).where(
                ClusterInfo.id == self.cluster_id
            ))
            session.execute(delete(AdminProject).where(
                AdminProject.project_id == self.project_id
            ))
            session.execute(delete(EnvEvent).where(EnvEvent.created_at >= self.start))
            session.commit()
        shutil.rmtree(self.target.parent.parent.parent, ignore_errors=True)
        shutil.rmtree(profile_store.storage_root / "cluster_profiles" / str(self.profile_id),
                      ignore_errors=True)
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_materialize_and_drift(self):
        result = materialize_project(
            1, self.project_id, profile_id=self.profile_id, reason="mat",
        )
        self.assertTrue((self.target / "core-site.xml").is_file())
        self.assertTrue((self.target / "hive-site.xml").is_file())
        self.assertEqual(set(result["files"]), {"core-site.xml", "hive-site.xml"})

        # project drift: keep
        (self.target / "core-site.xml").write_bytes(
            b"<configuration><property><name>x</name><value>1</value></property></configuration>"
        )
        drift = check_project(1, self.project_id)
        self.assertFalse(drift["clean"])
        kept = resolve_project_drift(1, drift["drift_id"], "keep", "test keep")
        self.assertEqual(kept["verify_status"], "stale")
        with SessionLocal() as session:
            project = session.execute(
                select(AdminProject).where(AdminProject.project_id == self.project_id)
            ).scalar_one()
            self.assertEqual(project.verify_status, "stale")

        # project drift: rollback
        (self.target / "hive-site.xml").write_bytes(b"<configuration/>")
        drift = check_project(1, self.project_id)
        self.assertFalse(drift["clean"])
        rolled = resolve_project_drift(1, drift["drift_id"], "rollback", "test rollback")
        self.assertEqual(rolled["verify_status"], "unknown")
        content = (self.target / "hive-site.xml").read_text(encoding="utf-8")
        self.assertIn("<name>a</name>", content)

        # public drift: keep + rollback
        (self.public / "core-site.xml").write_bytes(
            b"<configuration><property><name>y</name><value>2</value></property></configuration>"
        )
        public_drift = check_public(1)
        self.assertFalse(public_drift["clean"])
        resolve_public_drift(1, public_drift["drift_id"], "keep", "test public keep")
        current = self.store.get_public(include_content=True)
        self.assertIn("<name>y</name>", current["contents"]["core-site.xml"])

        (self.public / "core-site.xml").write_bytes(b"<configuration/>")
        public_drift = check_public(1)
        self.assertFalse(public_drift["clean"])
        resolve_public_drift(1, public_drift["drift_id"], "rollback", "test public rollback")
        current = self.store.get_public(include_content=True)
        self.assertIn("<name>y</name>", current["contents"]["core-site.xml"])


if __name__ == "__main__":
    unittest.main()
