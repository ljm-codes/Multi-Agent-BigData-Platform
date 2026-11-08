"""集群操作：SSH、YARN REST/日志、Hive/Spark SQL、HDFS、故障/修复应用提交。"""
from __future__ import annotations

import json
import os
import re
import shlex
import time
from pathlib import Path
from typing import Any

import paramiko
import requests

ROOT = Path(__file__).resolve().parents[2]
ENV_FILE = ROOT / ".env"
HADOOP_HOME = "/export/server/hadoop"
SPARK_HOME = "/export/server/spark3"
FLINK_HOME = "/export/server/flink"
HIVE_HOME = "/export/server/hive"
SPARK_PYTHON = "/export/server/miniconda3/envs/spark_study/bin/python3"


def _ssh_password() -> str:
    for line in ENV_FILE.read_text(encoding="utf-8").splitlines():
        if line.startswith("SSH_PASSWORD="):
            return line.split("=", 1)[1].strip().strip('"').strip("'")
    return ""


class Cluster:
    def __init__(self, host: str = "node1", user: str = "hadoop",
                 yarn_url: str = "http://node1:8088",
                 hdfs_url: str = "hdfs://node1:8020"):
        self.host = host
        self.user = user
        self.yarn_url = yarn_url.rstrip("/")
        self.hdfs_url = hdfs_url.rstrip("/")
        self.password = _ssh_password()
        self._ssh: paramiko.SSHClient | None = None

    # ---------- ssh ----------
    def _client(self) -> paramiko.SSHClient:
        if self._ssh is None:
            cli = paramiko.SSHClient()
            cli.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            cli.connect(self.host, username=self.user, password=self.password, timeout=15)
            self._ssh = cli
        return self._ssh

    def close(self) -> None:
        if self._ssh is not None:
            self._ssh.close()
            self._ssh = None

    def ssh(self, cmd: str, *, timeout: float = 120, merge_stderr: bool = True) -> tuple[int, str]:
        client = self._client()
        full = f"bash -lc {shlex.quote(cmd)}"
        if merge_stderr:
            full = f"bash -lc {shlex.quote(cmd + ' 2>&1')}"
        _, stdout, _ = client.exec_command(full, timeout=timeout)
        out = stdout.read().decode("utf-8", "replace")
        rc = stdout.channel.recv_exit_status()
        return rc, out

    def sftp_put(self, local: Path, remote: str) -> None:
        client = self._client()
        sftp = client.open_sftp()
        try:
            self._mkdir_sftp(sftp, os.path.dirname(remote))
            sftp.put(str(local), remote)
        finally:
            sftp.close()

    def sftp_put_dir(self, local_dir: Path, remote_dir: str) -> None:
        client = self._client()
        sftp = client.open_sftp()
        try:
            self._mkdir_sftp(sftp, remote_dir)
            for path in sorted(Path(local_dir).rglob("*")):
                rel = path.relative_to(local_dir).as_posix()
                target = f"{remote_dir.rstrip('/')}/{rel}"
                if path.is_dir():
                    self._mkdir_sftp(sftp, target)
                else:
                    self._mkdir_sftp(sftp, os.path.dirname(target))
                    sftp.put(str(path), target)
        finally:
            sftp.close()

    @staticmethod
    def _mkdir_sftp(sftp, path: str) -> None:
        if not path or path == "/":
            return
        parts = path.strip("/").split("/")
        cur = ""
        for part in parts:
            cur += "/" + part
            try:
                sftp.stat(cur)
            except OSError:
                sftp.mkdir(cur)

    # ---------- yarn ----------
    def yarn_apps(self, *, states: list[str] | None = None,
                  started_after_ms: int | None = None, size: int = 100) -> list[dict[str, Any]]:
        params: dict[str, Any] = {"size": size}
        if states:
            params["states"] = ",".join(states)
        if started_after_ms:
            params["startedTimeBegin"] = int(started_after_ms)
        resp = requests.get(f"{self.yarn_url}/ws/v1/cluster/apps", params=params, timeout=20)
        resp.raise_for_status()
        return (((resp.json() or {}).get("apps") or {}).get("app") or [])

    def app_by_id(self, app_id: str) -> dict[str, Any] | None:
        resp = requests.get(f"{self.yarn_url}/ws/v1/cluster/apps/{app_id}", timeout=20)
        if resp.status_code != 200:
            return None
        return (resp.json() or {}).get("app")

    def app_by_name(self, name: str, *, started_after_ms: int | None = None) -> dict[str, Any] | None:
        apps = self.yarn_apps(started_after_ms=started_after_ms, size=200)
        matches = [a for a in apps if a.get("name") == name]
        if not matches:
            return None
        matches.sort(key=lambda a: a.get("startedTime") or 0, reverse=True)
        return matches[0]

    def wait_app(self, app_id: str, *, states: set[str] | None = None,
                 timeout: float = 1800, interval: float = 5) -> dict[str, Any] | None:
        deadline = time.monotonic() + timeout
        last = None
        while time.monotonic() < deadline:
            last = self.app_by_id(app_id)
            if last and (states is None or last.get("state") in states):
                return last
            time.sleep(interval)
        return last

    def kill_app(self, app_id: str) -> str:
        rc, out = self.ssh(
            f"export PATH={HADOOP_HOME}/bin:$PATH; yarn application -kill {shlex.quote(app_id)}",
            timeout=60,
        )
        return out

    def yarn_logs(self, app_id: str, *, timeout: float = 240, retries: int = 12) -> str:
        cmd = (
            f"export PATH={HADOOP_HOME}/bin:$PATH; "
            f"yarn logs -applicationId {shlex.quote(app_id)} 2>&1"
        )
        last = ""
        for _ in range(retries):
            rc, out = self.ssh(cmd, timeout=timeout)
            last = out
            if out and "Log aggregation has not completed" not in out and "No logs found" not in out:
                if len(out.strip()) > 200:
                    return out
            time.sleep(5)
        return last

    # ---------- hdfs ----------
    def hdfs(self, args: str, *, timeout: float = 180) -> tuple[int, str]:
        return self.ssh(
            f"export PATH={HADOOP_HOME}/bin:$PATH; hdfs dfs {args}",
            timeout=timeout,
        )

    def hdfs_mkdir(self, path: str) -> None:
        self.hdfs(f"-mkdir -p {shlex.quote(path)}")

    def hdfs_put_dir(self, local_dir: Path, remote_dir: str) -> None:
        stage = f"/tmp/system_eval_upload_{int(time.time() * 1000)}"
        self.ssh(f"rm -rf {shlex.quote(stage)}; mkdir -p {shlex.quote(stage)}")
        self.sftp_put_dir(local_dir, stage)
        self.hdfs_mkdir(remote_dir)
        # bash 先展开 stage/*，hdfs put 多源到目标目录；不能用 stage/.（会多套一层目录）
        rc, out = self.hdfs(f"-put -f {shlex.quote(stage)}/* {shlex.quote(remote_dir)}/")
        if rc != 0:
            raise RuntimeError(f"HDFS 上传失败: {out[-1000:]}")
        self.ssh(f"rm -rf {shlex.quote(stage)}")

    def hdfs_rm(self, path: str) -> None:
        self.hdfs(f"-rm -r -f {shlex.quote(path)}")

    def hdfs_put_file(self, local: Path, remote: str) -> None:
        stage = f"/tmp/system_eval_upload_{int(time.time() * 1000)}"
        self.sftp_put(local, stage)
        self.hdfs_mkdir(os.path.dirname(remote))
        rc, out = self.hdfs(f"-put -f {shlex.quote(stage)} {shlex.quote(remote)}")
        self.ssh(f"rm -f {shlex.quote(stage)}")
        if rc != 0:
            raise RuntimeError(f"HDFS 上传文件失败: {out[-1000:]}")

    # ---------- hive / spark ----------
    def beeline(self, sql: str, *, timeout: float = 600) -> tuple[int, str]:
        one_line = " ".join(sql.split())
        cmd = (
            f"export PATH={HIVE_HOME}/bin:$PATH; "
            f"beeline -u 'jdbc:hive2://node1:10000' -n hadoop --silent=true "
            f"--outputformat=csv2 -e {shlex.quote(one_line)} 2>/dev/null"
        )
        return self.ssh(cmd, timeout=timeout, merge_stderr=False)

    def spark_sql(self, sql: str, *, timeout: float = 300) -> tuple[int, str]:
        one_line = " ".join(sql.split())
        cmd = (
            f"{SPARK_HOME}/bin/spark-sql --master 'local[2]' --driver-memory 1g "
            f"--conf spark.sql.shuffle.partitions=8 -e {shlex.quote(one_line)}"
        )
        return self.ssh(cmd, timeout=timeout)

    # ---------- submissions ----------
    def spark_submit_hdfs(
        self, script_hdfs: str, app_name: str, *,
        conf: dict[str, str] | None = None,
        args: list[str] | None = None,
        executor_memory: str = "512m",
        driver_memory: str = "768m",
        num_executors: int = 1,
        timeout: float = 120,
    ) -> str:
        parts = [
            f"{SPARK_HOME}/bin/spark-submit",
            "--master", "yarn",
            "--deploy-mode", "cluster",
            "--name", shlex.quote(app_name),
            "--conf", "spark.yarn.submit.waitAppCompletion=false",
            "--conf", f"spark.yarn.appMasterEnv.PYSPARK_PYTHON={SPARK_PYTHON}",
            "--conf", f"spark.yarn.appMasterEnv.PYSPARK_DRIVER_PYTHON={SPARK_PYTHON}",
            "--conf", f"spark.executorEnv.PYSPARK_PYTHON={SPARK_PYTHON}",
            "--driver-memory", driver_memory,
            "--executor-memory", executor_memory,
            "--num-executors", str(num_executors),
        ]
        for k, v in (conf or {}).items():
            parts.extend(["--conf", f"{k}={v}"])
        parts.append(shlex.quote(script_hdfs))
        for item in args or []:
            parts.append(shlex.quote(str(item)))
        rc, out = self.ssh(" ".join(parts), timeout=timeout)
        if rc != 0:
            raise RuntimeError(f"spark-submit 失败 rc={rc}: {out[-2000:]}")
        return out

    def flink_submit_hdfs(
        self, jar_hdfs: str, app_name: str, main_class: str, *,
        parallelism: int = 1,
        jobmanager_memory: int = 768,
        taskmanager_memory: int = 1024,
        slots: int = 1,
        conf: dict[str, str] | None = None,
        args: list[str] | None = None,
        timeout: float = 180,
    ) -> str:
        parts = [
            f"{FLINK_HOME}/bin/flink", "run-application", "-t", "yarn-application",
            "-d", "-c", shlex.quote(main_class),
            "-p", str(parallelism),
            "-D", f"jobmanager.memory.process.size={jobmanager_memory}m",
            "-D", f"taskmanager.memory.process.size={taskmanager_memory}m",
            "-D", f"taskmanager.numberOfTaskSlots={slots}",
            "-D", f"yarn.application.name={app_name}",
        ]
        for k, v in (conf or {}).items():
            parts.extend(["-D", f"{k}={v}"])
        parts.append(shlex.quote(jar_hdfs))
        for item in args or []:
            parts.append(shlex.quote(str(item)))
        rc, out = self.ssh(" ".join(parts), timeout=timeout)
        if rc != 0:
            raise RuntimeError(f"flink run-application 失败 rc={rc}: {out[-2000:]}")
        return out

    def submit_c1_oom(self) -> str:
        local = ROOT / "tests" / "O&M" / "spark"
        remote = "/tmp/system_eval/spark-fault"
        self.sftp_put_dir(local, remote)
        rc, out = self.ssh(f"bash {remote}/submit.sh oom_limit", timeout=240)
        if rc != 0:
            raise RuntimeError(f"C1 故障提交失败: {out[-2000:]}")
        return out

    def ensure_ops_clean(self) -> dict[str, Any]:
        """清理上一次评测残留的 RUNNING 应用/队列/租约（只动名字带 eval/spark-fault 的）。"""
        killed = []
        for app in self.yarn_apps(states=["RUNNING", "ACCEPTED", "NEW"], size=200):
            name = app.get("name") or ""
            lower = name.lower()
            if name.startswith(("eval-", "spark-fault-", "flink-fault-")) or any(
                key in lower for key in ("eval", "pvuv", "topn", "wordcount", "fault")
            ):
                self.kill_app(app.get("id"))
                killed.append({"id": app.get("id"), "name": name})
        return {"killed": killed}
