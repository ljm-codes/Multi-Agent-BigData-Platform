"""
本地预览用的假后端

不连 Postgres、不连 Redis、不跑 LangGraph，只按前端的接口契约返回假数据，
用来在没启动虚拟机的时候直接看界面。真实联调不要用它。

用法：
    python scripts/mock_backend.py
    然后打开 http://127.0.0.1:8010/
"""
import asyncio
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import uvicorn
from fastapi import FastAPI, Query, Request, WebSocket
from fastapi.responses import FileResponse, StreamingResponse

ROOT_DIR = Path(__file__).resolve().parent.parent
DIST_DIR = ROOT_DIR / "static" / "dist"
PORT = 8010

app = FastAPI(title="mock backend")

PROFILE = {"id": 1, "username": "admin", "permission": 2, "disabled": False}


def ok(data: Any = None, msg: str = "success"):
    return {"code": 0, "msg": msg, "data": data}


def now_iso(offset_minutes: int = 0):
    return (datetime.now() - timedelta(minutes=offset_minutes)).isoformat(timespec="seconds")


THREADS = [
    {"id": 3, "thread_id": "1:mock-spark-001", "thread_name": "统计1-100的奇偶数量",
     "is_archive": False, "create_time": now_iso(5)},
    {"id": 2, "thread_id": "1:mock-flink-002", "thread_name": "Flink 实时词频统计",
     "is_archive": False, "create_time": now_iso(60)},
    {"id": 1, "thread_id": "1:mock-archived-003", "thread_name": "已归档的历史会话",
     "is_archive": True, "create_time": now_iso(600)},
]

HISTORY = [
    {"type": "HumanMessage", "content": "帮我统计 1 到 100 的奇数和偶数各多少个，用 Spark 写"},
    {"type": "AIMessage", "content": "任务已拆解，正在进行中……"},
    {"type": "ToolMessage",
     "content": '{"tool": "write_analysis_app_code", "path": "src/main/python/count.py", "status": "ok"}'},
    {"type": "AIMessage", "content": "已完成 Spark 代码，浅层测试通过。"},
]

PROJECT_DIR = str(ROOT_DIR / "tmp_code" / "spark" / "2026-10-29" / "demo-project")


@app.post("/auth/admin/login")
async def login():
    return ok({"token": "mock-token", "payload": PROFILE, "time": now_iso()})


@app.get("/auth/admin/admin_info")
async def admin_info(limit: int = 20, offset: int = 0, disabled: bool = False):
    rows = [
        {"id": 1, "username": "admin", "email": "ad***@example.com", "permission": 2,
         "disabled": False, "create_time": now_iso(1000)},
        {"id": 2, "username": "zhangsan", "email": "zh***@example.com", "permission": 1,
         "disabled": False, "create_time": now_iso(800)},
        {"id": 3, "username": "lisi", "email": "li***@example.com", "permission": 0,
         "disabled": disabled, "create_time": now_iso(600)},
    ]
    return ok({"admin_data": rows, "total": len(rows), "limit": limit, "offset": offset})


@app.post("/auth/admin/build/link")
async def build_link():
    code = "mockcode123"
    return ok({"code": code, "link": f"/recommend/register?code={code}",
               "expire_in": 1200, "expires_at": now_iso(-20)})


@app.post("/auth/admin/account/status")
async def account_status():
    return ok(msg="已更新")


@app.post("/auth/admin/account/update")
async def account_update():
    return ok(msg="账号已更新", data=PROFILE)


@app.get("/auth/admin/recommend/register")
async def register_info(code: str = ""):
    if not code:
        return {"code": 4001, "msg": "缺少推荐码", "data": None}
    return ok({"recommended_level": 0, "expire_in": 900})


@app.post("/auth/admin/recommend/register")
async def register():
    return ok(msg="注册成功")


@app.get("/ai/threads")
async def threads(is_archive: bool = False, limit: int = 50, offset: int = 0):
    rows = [t for t in THREADS if t["is_archive"] == is_archive]
    return ok({"records": rows, "total": len(rows), "limit": limit, "offset": offset})


@app.get("/ai/history")
async def history(thread_id: str = Query(...)):
    return ok({
        "thread_id": thread_id,
        "is_archive": thread_id.endswith("003"),
        "manual_intervention": False,
        "project_dir": PROJECT_DIR,
        "messages": HISTORY,
    })


def sse(event: str, data: Any) -> str:
    return f"event: {event}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n"


@app.post("/ai/chat/stream")
async def chat_stream(request: Request):
    body = await request.json()
    text = body.get("message", "")
    thread_id = body.get("thread_id") or "1:mock-new-thread"

    async def gen():
        yield sse("meta", {"thread_id": thread_id})

        if "中断" in text:
            await asyncio.sleep(0.3)
            yield sse("interrupt", {
                "thread_id": thread_id,
                "interrupt": [{"value": {
                    "type": "info",
                    "app_type": "spark",
                    "msg": "## 数据结果\n奇偶各 50 个。\n\n## 核心代码片段\n"
                           "```python\nspark.range(1, 101).withColumn('odd', col('id') % 2)"
                           ".groupBy('odd').count()\n```\n"
                           "[在 IDE 中查看](IDE://src/main/python/count.py#L12-L18)",
                }}],
            })
            return

        for chunk in ["好的，", "我先看一下", "你的需求，", "然后拆成任务分给下游 Agent。"]:
            await asyncio.sleep(0.25)
            yield sse("token", {"thread_id": thread_id, "node": "dis_chat", "ns": [], "content": chunk})

        yield sse("done", {
            "thread_id": thread_id,
            "output_msg": "任务已经派给 data_analysis，正在生成 Spark 代码。",
            "out_time": now_iso(),
            "metadata": {"thread_action": "continue"},
            "thread_action": "continue",
            "project_dir": PROJECT_DIR,
        })

    return StreamingResponse(
        gen(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.post("/ai/resume")
async def resume():
    await asyncio.sleep(0.3)
    return ok({
        "thread_id": "1:mock-spark-001",
        "output_msg": "已确认，继续执行深层测试。",
        "out_time": now_iso(),
        "metadata": {},
        "thread_action": "continue",
        "interrupted": False,
        "project_dir": None,
    })


@app.put("/ai/thread/archive")
async def archive(thread_id: str = Query(...)):
    return ok(msg="会话已归档")


@app.put("/ai/thread/archives")
async def archives():
    return ok(msg="批量归档完成")


@app.delete("/ai/thread/delete")
async def delete(thread_id: str = Query(...)):
    return ok(msg="会话已删除")


@app.delete("/ai/thread/deletes")
async def deletes():
    return ok(msg="批量删除完成")


@app.get("/ai/background")
async def background(thread_id: str | None = None):
    main_thread = thread_id or "1:mock-spark-001"
    return [
        {"task_id": "deep-8f2a", "user_id": "1", "main_thread_id": main_thread,
         "bg_thread_id": f"{main_thread}:bg:deep-8f2a", "status": "running",
         "summary": "深度验证 spark/统计1-100的奇偶数量", "result": "", "interrupt": None,
         "created_at": now_iso(3), "updated_at": now_iso(1)},
        {"task_id": "deep-7c11", "user_id": "1", "main_thread_id": main_thread,
         "bg_thread_id": f"{main_thread}:bg:deep-7c11", "status": "completed",
         "summary": "深度验证 flink 词频", "result": "全部测试通过（12/12）", "interrupt": None,
         "created_at": now_iso(40), "updated_at": now_iso(30)},
    ]


@app.post("/ai/background/verify")
async def background_verify():
    """假 Ticket，WS 那边不校验，只为了跑通前端流程"""
    return ok({"ticket": "mock-ticket"})


@app.websocket("/ai/ws/background")
async def background_ws(websocket: WebSocket):
    await websocket.accept()
    snapshot = await background()
    await websocket.send_json({"type": "snapshot", "records": snapshot})
    try:
        # 每 10 秒推一条“运行中”的更新，方便看实时效果
        while True:
            await asyncio.sleep(10)
            await websocket.send_json({"type": "update", "record": {
                "task_id": "deep-8f2a", "user_id": "1",
                "main_thread_id": snapshot[0]["main_thread_id"],
                "bg_thread_id": snapshot[0]["bg_thread_id"], "status": "running",
                "summary": snapshot[0]["summary"], "result": "", "interrupt": None,
                "created_at": snapshot[0]["created_at"], "updated_at": now_iso(),
            }})
    except Exception:  # noqa: BLE001
        pass


@app.get("/open/app/get_ides")
async def get_ides():
    return ok({
        "notepad": {"path": "C:\\Windows\\notepad.exe", "display": "记事本"},
        "pycharm": {"path": "C:\\Program Files\\JetBrains\\PyCharm\\bin\\pycharm64.exe",
                    "display": "PyCharm"},
        "code": {"path": "C:\\Users\\me\\AppData\\Local\\Programs\\Microsoft VS Code\\Code.exe",
                 "display": "Visual Studio Code"},
    })


@app.post("/open/app/open_file")
async def open_file():
    return ok({"opened": "mock", "line": 1, "editor": "notepad"})


@app.get("/{full_path:path}")
async def static_files(full_path: str):
    if not DIST_DIR.is_dir():
        return {"code": 500, "msg": "static/dist 不存在，请先 pnpm build", "data": None}
    root = DIST_DIR.resolve()
    candidate = (root / full_path).resolve()
    if full_path and candidate.is_file() and root in candidate.parents:
        return FileResponse(candidate)
    return FileResponse(root / "index.html")


if __name__ == "__main__":
    if not DIST_DIR.is_dir():
        print(f"[提示] 还没有构建产物：{DIST_DIR}")
        print("       先执行 cd static/source && pnpm build")
    print(f"[启动] 假后端：http://127.0.0.1:{PORT}/")
    print("[登录] 任意邮箱 + 任意密码都可以进（比如 admin@example.com / 123456）")
    uvicorn.run(app, host="127.0.0.1", port=PORT, log_level="warning")
