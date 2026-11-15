"""公有资源/归档接口权限 + 基本 HTTP 冒烟（本地 Postgres；测试后清理）。"""
import shutil
import tempfile
import unittest
import uuid
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.testclient import TestClient
from sqlalchemy import delete, select

import routers.cluster_archive as archive_router
import routers.cluster_public as public_router
from SPO.cluster_env_model import (
    ConfigSnapshot,
    ConfigSnapshotArchive,
    ConfigSnapshotFile,
    EnvEvent,
    PublicConfig,
)
from Tools.db import SessionLocal
from services.cluster_snapshot_store import ClusterSnapshotStore


class ClusterSnapshotApiTest(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="cluster_snapshot_api_test_"))
        self.public = self.tmp / "public"
        self.archive = self.tmp / "archive"
        self.public.mkdir(parents=True)
        self.name = f"__test_api_{uuid.uuid4().hex}"
        self.store = ClusterSnapshotStore(
            public_root=self.public,
            archive_root=self.archive,
            public_config_name=self.name,
        )
        self.start = datetime.now(timezone.utc)
        self._old_public_store = public_router.snapshot_store
        self._old_archive_store = archive_router.snapshot_store
        public_router.snapshot_store = self.store
        archive_router.snapshot_store = self.store

        self.app = FastAPI()

        @self.app.middleware("http")
        async def fake_auth(request: Request, call_next):
            request.state.info = {
                "id": 1,
                "username": "test",
                "permission": int(request.headers.get("X-Permission", "0")),
            }
            return await call_next(request)

        self.app.include_router(public_router.route, prefix="/cluster")
        self.app.include_router(archive_router.route, prefix="/cluster")
        self.client = TestClient(self.app)

    def tearDown(self):
        public_router.snapshot_store = self._old_public_store
        archive_router.snapshot_store = self._old_archive_store
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

    def test_permission_and_public_scan(self):
        (self.public / "core-site.xml").write_bytes(b"<configuration/>")

        denied = self.client.post("/cluster/public/scan", json={"reason": "x"},
                                  headers={"X-Permission": "1"}).json()
        self.assertEqual(denied["code"], 4005)

        allowed = self.client.post("/cluster/public/scan", json={"reason": "x"},
                                   headers={"X-Permission": "2"}).json()
        self.assertEqual(allowed["code"], 0)
        self.assertEqual(allowed["data"]["version"], 1)
        self.assertEqual(allowed["data"]["contents"]["core-site.xml"], "<configuration/>")

        current = self.client.get("/cluster/public", headers={"X-Permission": "1"}).json()
        self.assertEqual(current["code"], 0)
        self.assertEqual(current["data"]["version"], 1)

        denied_archives = self.client.get("/cluster/archives",
                                          headers={"X-Permission": "1"}).json()
        self.assertEqual(denied_archives["code"], 4005)
        allowed_archives = self.client.get("/cluster/archives",
                                           headers={"X-Permission": "2"}).json()
        self.assertEqual(allowed_archives["code"], 0)

        gc = self.client.post("/cluster/archives/gc", json={"dry_run": True},
                              headers={"X-Permission": "2"}).json()
        self.assertEqual(gc["code"], 0)
        self.assertTrue(gc["data"]["dry_run"])


if __name__ == "__main__":
    unittest.main()
