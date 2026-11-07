"""
code.commit 的 git 来源：GitHub API 上传 / 下载

上传走 Git Data API，一次提交写入整个目录，仓库里已有的文件不会被删。
下载先按 source_dir 过滤 tree，再用 blob API 取原始内容。

配置：
    code.commit.git.git_repo = owner/repo
    .env GIT_API             = GitHub token

分支固定用仓库的默认分支（GitHub API 直接查），不额外配置。
"""
import base64
import os
import posixpath
from pathlib import Path
from urllib.parse import quote

import requests

from load_config.config import config

try:
    # Windows 下 certifi 不会自动带系统根证书；GitHub 走系统信任库
    import truststore
    truststore.inject_into_ssl()
except Exception:
    pass

_API = "https://api.github.com"


def _conf() -> tuple[str, str]:
    commit = ((config.get("code") or {}).get("commit") or {})
    git = commit.get("git") or {}
    repo = str(git.get("git_repo") or "").strip()
    if repo.count("/") != 1 or not all(repo.split("/")):
        raise ValueError(f"code.commit.git.git_repo 必须是 owner/repo，当前是 {repo!r}")
    owner, name = repo.split("/")
    return owner, name


def _token() -> str:
    token = os.getenv("GIT_API", "").strip()
    if not token:
        raise ValueError("GIT_API 未配置")
    return token


def _headers(token: str) -> dict:
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }


def ssh_endpoint() -> tuple[str, str, int, str]:
    """code.commit.ssh 的连接参数 (host, user, port, password)；上传和下载共用"""
    commit = ((config.get("code") or {}).get("commit") or {})
    ssh_cfg = commit.get("ssh") or {}
    host = str(ssh_cfg.get("ip") or "").strip()
    if not host:
        raise ValueError("code.commit.ssh.ip 未配置")
    base = config.get("ssh") or {}
    user = str(ssh_cfg.get("user") or base.get("user") or "hadoop")
    port = int(ssh_cfg.get("port") or base.get("port") or 22)
    password = os.getenv("COMMIT_SSH_PASSWORD") or os.getenv("SSH_PASSWORD") or ""
    return host, user, port, password


def _check(resp: requests.Response, what: str) -> requests.Response:
    if not resp.ok:
        raise RuntimeError(f"{what} 失败: HTTP {resp.status_code}: {resp.text[:300]}")
    return resp


def _branch(owner: str, name: str, headers: dict) -> str:
    resp = _check(requests.get(f"{_API}/repos/{owner}/{name}", headers=headers, timeout=15),
                  "读取仓库信息")
    return str((resp.json() or {}).get("default_branch") or "main")


def _head(owner: str, name: str, branch: str, headers: dict) -> tuple[str | None, str | None]:
    ref = f"heads/{quote(branch, safe='/')}"
    resp = requests.get(f"{_API}/repos/{owner}/{name}/git/ref/{ref}",
                        headers=headers, timeout=15)
    if resp.status_code == 409 and "empty" in resp.text.lower():
        # 空仓库还没有 ref；上传时需要建 root commit 和分支
        return None, None
    _check(resp, "读取分支 ref")
    commit_sha = str((((resp.json() or {}).get("object") or {}).get("sha")) or "")
    if not commit_sha:
        raise RuntimeError(f"分支 {branch!r} 没有可供提交的 head")
    commit = _check(requests.get(f"{_API}/repos/{owner}/{name}/git/commits/{commit_sha}",
                                 headers=headers, timeout=15), "读取 commit").json() or {}
    tree_sha = str(((commit.get("tree") or {}).get("sha")) or "")
    if not tree_sha:
        raise RuntimeError("无法解析分支的 base tree")
    return commit_sha, tree_sha


def upload(source_dir: str, files: list[tuple[Path, str]], message: str) -> str:
    """把 files（本地路径, 目录内相对路径）写入 GitHub 的 source_dir"""
    owner, name = _conf()
    headers = _headers(_token())
    branch = _branch(owner, name, headers)
    rel = source_dir.strip("/")
    if not rel:
        raise ValueError("source_dir 不能为空")

    for attempt in range(3):
        commit_sha, base_tree = _head(owner, name, branch, headers)
        if commit_sha is None:
            # 空仓库：Git Data API 的 blob 也会 409，先用 Contents API 建第一个文件/分支
            if not files:
                raise RuntimeError("空仓库初始化失败：没有可提交的文件")
            first_local, first_sub = files[0]
            path = posixpath.join(rel, first_sub).lstrip("/")
            content = base64.b64encode(Path(first_local).read_bytes()).decode("ascii")
            init = requests.put(
                f"{_API}/repos/{owner}/{name}/contents/{quote(path, safe='/')}",
                headers=headers,
                json={"message": message, "content": content, "branch": branch},
                timeout=60,
            )
            if init.ok:
                continue
            if attempt == 0 and init.status_code in (409, 422):
                continue
            _check(init, "初始化空仓库")
        entries = []
        for local, sub in files:
            content = base64.b64encode(Path(local).read_bytes()).decode("ascii")
            blob = _check(requests.post(
                f"{_API}/repos/{owner}/{name}/git/blobs", headers=headers,
                json={"content": content, "encoding": "base64"}, timeout=60,
            ), "创建 blob").json() or {}
            blob_sha = str(blob.get("sha") or "")
            if not blob_sha:
                raise RuntimeError("创建 blob 后没有返回 sha")
            entries.append({
                "path": posixpath.join(rel, sub).lstrip("/"),
                "mode": "100644",
                "type": "blob",
                "sha": blob_sha,
            })
        tree_payload = {"tree": entries}
        if base_tree:
            tree_payload["base_tree"] = base_tree
        tree = _check(requests.post(
            f"{_API}/repos/{owner}/{name}/git/trees", headers=headers,
            json=tree_payload, timeout=30,
        ), "创建 tree").json() or {}
        tree_sha = str(tree.get("sha") or "")
        if not tree_sha:
            raise RuntimeError("创建 tree 后没有返回 sha")
        commit_payload = {"message": message, "tree": tree_sha}
        if commit_sha:
            commit_payload["parents"] = [commit_sha]
        new_commit = _check(requests.post(
            f"{_API}/repos/{owner}/{name}/git/commits", headers=headers,
            json=commit_payload, timeout=30,
        ), "创建 commit").json() or {}
        new_sha = str(new_commit.get("sha") or "")
        if not new_sha:
            raise RuntimeError("创建 commit 后没有返回 sha")

        resp = requests.patch(
            f"{_API}/repos/{owner}/{name}/git/refs/heads/{quote(branch, safe='/')}",
            headers=headers, json={"sha": new_sha, "force": False}, timeout=15,
        )
        if resp.ok:
            return f"/{rel}"
        # 提交期间分支被推进：重读 head 再试一次，不做 force push
        if attempt == 0 and resp.status_code in (409, 422):
            continue
        _check(resp, "更新分支 ref")
    raise RuntimeError("更新分支 ref 失败（并发提交）")


def download(source_dir: str, target: Path) -> None:
    """把 GitHub 上 source_dir 目录下的文件下载到 target，保留相对目录"""
    owner, name = _conf()
    headers = _headers(_token())
    branch = _branch(owner, name, headers)
    rel = source_dir.strip("/")
    if not rel:
        raise ValueError("source_dir 不能为空")

    _, tree_sha = _head(owner, name, branch, headers)
    if not tree_sha:
        raise RuntimeError("仓库为空，没有可下载的代码")
    tree = _check(requests.get(
        f"{_API}/repos/{owner}/{name}/git/trees/{tree_sha}",
        headers=headers, params={"recursive": "1"}, timeout=30,
    ), "读取仓库 tree").json() or {}
    if tree.get("truncated"):
        raise RuntimeError("仓库 tree 过大被截断，无法完整下载")

    prefix = f"{rel}/"
    target.mkdir(parents=True, exist_ok=True)
    downloaded = 0
    for entry in tree.get("tree") or []:
        if entry.get("type") != "blob":
            continue
        path = str(entry.get("path") or "")
        if not path.startswith(prefix):
            continue
        sub = path[len(prefix):]
        if not sub:
            continue
        raw_headers = {**headers, "Accept": "application/vnd.github.raw"}
        resp = _check(requests.get(
            f"{_API}/repos/{owner}/{name}/git/blobs/{entry.get('sha')}",
            headers=raw_headers, timeout=60,
        ), f"下载 {path}")
        local = target / Path(sub)
        local.parent.mkdir(parents=True, exist_ok=True)
        local.write_bytes(resp.content)
        downloaded += 1
    if not downloaded:
        raise RuntimeError(f"GitHub 仓库里没有目录: {source_dir}")
