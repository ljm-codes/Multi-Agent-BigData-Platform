from fastapi import APIRouter, HTTPException
from starlette.responses import FileResponse

from load_config.config import ROOT_BASE_DIR_PATH

route = APIRouter()

DIST_DIR = ROOT_BASE_DIR_PATH / "static" / "dist"


@route.get('/{full_path:path}')
async def index(full_path: str):
    """
    前端静态托管 + SPA 兜底

    -> 命中真实文件（含 /assets/*.js）就直接返回，FileResponse 会按扩展名给 content-type
    -> 找不到就回 index.html，交给 vue-router（/admin、/recommend/register 刷新才不会是 404）
    """
    if not DIST_DIR.is_dir():
        raise HTTPException(status_code=503, detail="前端产物不存在，请先执行 pnpm build")

    root = DIST_DIR.resolve()
    candidate = (root / full_path).resolve()
    if full_path and candidate.is_file() and root in candidate.parents:
        return FileResponse(candidate)
    return FileResponse(root / "index.html")
