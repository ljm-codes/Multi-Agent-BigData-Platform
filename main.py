"""
项目主文件
用于fastapi的路由定义、中间件配置以及LangGraph图的初始化等操作
"""
import os
from contextlib import asynccontextmanager
from typing import cast, Optional

import jwt
from fastapi import FastAPI, Request
from langchain_core.stores import BaseStore
from langgraph.cache.sqlite import SqliteCache
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
from langgraph.graph import StateGraph
from langgraph.store.memory import InMemoryStore
from langgraph.store.postgres import AsyncPostgresStore
from langgraph.types import Checkpointer
from pydantic import BaseModel, Field
from sqlalchemy import select, and_, false, true, func
from starlette.concurrency import run_in_threadpool
from starlette.middleware.cors import CORSMiddleware
from starlette.datastructures import State
from starlette.responses import JSONResponse
from starlette.staticfiles import StaticFiles

from SPO.main_agent_state_model import MainStateContext
from SPO.postgresql_model import AdminInfo
from Tasks.background_handler import start_perceptron
from Tasks.jwt_key_manage import KeyManage
from Tools import LogSetting
from Tools.db import SessionLocal
from Tools.share_check_tool import redis_con
from agent.develop_main_agent import builder
from load_config.config import ROOT_BASE_DIR_PATH, config
from routers.ai_chat import route as ai_chat_route
from routers.manager import route as login_route
from routers.open_app import route as open_app_route
from routers.start_file import route as start_file_route

main_graph = None
_postgres_conf = (config.get('databases') or {}).get('postgres') or {}
_postgres_url = f"postgresql://{_postgres_conf.get('user')}:{_postgres_conf.get('password')}@{_postgres_conf.get('host')}:{_postgres_conf.get('port')}/{_postgres_conf.get('db')}?sslmode=disable"
# enabled: false 时退回 InMemorySaver/InMemoryStore（仅适合本地调试）
_pg_enabled = bool(_postgres_conf.get('enabled', True)) if _postgres_conf else False

logger = LogSetting.create(__name__)
token_prefix = "Bearer token "
whitelist = {"/auth/admin/recommend/register", "/auth/admin/login", "/open/app/get_ides", "/",
             "/favicon.ico"}

if not os.path.isdir(ROOT_BASE_DIR_PATH / 'cache'):
    os.makedirs(ROOT_BASE_DIR_PATH / 'cache')


class ParsedTokenData(BaseModel):
    id: str = Field(default='', description="管理员ID")
    username: str = Field(default='', description="管理员用户名")
    permission: Optional[int] = Field(default=None, description="管理员权限")
    disabled: bool = Field(default=False, description="是否禁用")
    is_old: bool = Field(default=False, description="是否为旧密钥")


async def create_graph(
        graph_build: StateGraph,
        store: Optional[BaseStore] = None,
        check: Optional[Checkpointer] = None
):
    """
    当 store 与 check 均为None时，默认使用InMemoryStore和InMemorySaver
    """
    global main_graph
    if store is None:
        store = InMemoryStore()
    if check is None:
        check = InMemorySaver()
    if hasattr(check, 'setup'):
        await check.setup()
    if hasattr(store, 'setup'):
        await store.setup()
    main_graph = graph_build.compile(
        checkpointer=check,
        store=store,
        cache=SqliteCache(path=ROOT_BASE_DIR_PATH / 'cache/agent_cache.db')
    )


@asynccontextmanager
async def _graph_scope():
    """Postgres 持久化；配置 enabled=false 时退回内存"""
    if _pg_enabled:
        async with AsyncPostgresSaver.from_conn_string(_postgres_url) as check, \
                AsyncPostgresStore.from_conn_string(_postgres_url) as store:
            await create_graph(builder, store=store, check=check)
            yield
    else:
        await create_graph(builder)
        yield


async def _background_runner(thread_id: str, user_id: str, input_state: dict):
    """perceptron 拉起后台续跑：和用户入口共用同一张主图、同一个 thread_id"""
    if main_graph is None:
        raise RuntimeError("main_graph 尚未初始化")
    return await main_graph.ainvoke(
        input_state,
        config={"configurable": {"thread_id": thread_id}},
        context=MainStateContext(thread_id=thread_id, user_id=user_id),
    )


def _normalize_id(raw_id) -> str:
    """token 里的 id 可能是数字或字符串，统一转成字符串（ParsedTokenData.id 是 str 类型，
    pydantic v2 不会把 int 隐式转 str，数字 id 会抛 ValidationError 导致 401）"""
    return '' if raw_id is None else str(raw_id)


def _verify_token_paylod_db(payload: dict) -> tuple[bool, str]:
    admin_id = payload.get('id')
    username = payload.get('username')
    permission = payload.get('permission')
    disabled = payload.get('disabled')
    if not admin_id or not username or permission is None or disabled is None:
        return False, "token错误"
    try:
        stmt = select(func.count(AdminInfo.id)).where(
            and_(
                AdminInfo.id == int(admin_id),
                AdminInfo.username == username,
                AdminInfo.permission == permission,
                AdminInfo.disabled == (false() if not disabled else true()),
            )
        )
        with SessionLocal() as session:
            count = session.scalar(stmt) or 0
    except ValueError:
        return False, "token错误"
    except Exception as e:
        logger.error(f"查询数据库失败：{e}")
        return False, "token错误"
    if count <= 0:
        return False, "token错误"
    return True, "token验证通过"


def _verify_authorization_header(token: str) -> tuple[ParsedTokenData | None, str]:
    """
    验证token是否有效
    :param token: token字符串
    :return: 解析后的token数据或False
    """
    if not token.startswith(token_prefix):
        return None, "token错误"
    token = token[len(token_prefix):]
    if not token:
        return None, "token错误"
    old_key = redis_con.hget('jwt_key', 'old')
    new_key = redis_con.hget('jwt_key', 'new')
    # 捕获 Exception：PyJWT 的 ExpiredSignatureError 不是 DecodeError 的子类，
    # 只捕获 DecodeError 会让过期 token 抛未捕获异常导致 500；key 为 None 时还会抛 TypeError
    try:
        payload = jwt.decode(jwt=token, key=new_key, algorithms=['HS256'])
        # 顺便进行库查询
        is_valid, msg = _verify_token_paylod_db(payload)
        if not is_valid:
            return None, msg
        # 验证通过，返回解析后的token数据
        return ParsedTokenData(
            id=_normalize_id(payload.get('id')),
            username=payload.get('username', ''),
            permission=payload.get('permission', None),
            disabled=payload.get('disabled', False),
            is_old=False,
        ), msg
    except Exception:
        try:
            payload = jwt.decode(jwt=token, key=old_key, algorithms=['HS256'])
            # 顺便进行库查询
            is_valid, msg = _verify_token_paylod_db(payload)
            if not is_valid:
                return None, msg
            # 验证通过，返回解析后的token数据
            return ParsedTokenData(
                id=_normalize_id(payload.get('id')),
                is_old=True,
                permission=payload.get('permission', None),
                disabled=payload.get('disabled', False),
                username=payload.get('username', ''),
            ), msg
        except Exception:
            return None, "token错误"


@asynccontextmanager
async def lifespan(api: FastAPI):
    global main_graph
    async with _graph_scope():
        api.state.main_graph = main_graph
        api.state.perceptron = start_perceptron(_background_runner)
        key_manage = KeyManage()
        key_manage.start()
        logger.info("=" * 60)
        logger.info("FastAPI 应用已【完全启动成功】！")
        logger.info("主页: http://127.0.0.1:8000")
        logger.info("=" * 60)
        try:
            yield
        finally:
            perceptron = getattr(api.state, "perceptron", None)
            if perceptron is not None:
                await perceptron.stop()
            key_manage.shutdown()
            api.state.main_graph = None
            main_graph = None
            logger.info("应用已关闭")


def start_app():
    app = FastAPI(lifespan=lifespan)

    @app.middleware("http")
    async def add_process_time_header(request: Request, call_next):
        """
        请求中间件，用于进行验证身份等操作
        """
        url = request.url.path
        # 匹配前端静态资源
        if url in whitelist or url.startswith("/assets/"):
            return await call_next(request)
        authorization = request.headers.get("Authorization")
        if not authorization:
            return JSONResponse(
                {"type": "error", "msg": "Authorization 请求头缺失"},
                status_code=401
            )
        # 解析 Authorization 请求头
        token_data, msg = await run_in_threadpool(_verify_authorization_header, authorization)
        if not token_data:
            return JSONResponse(
                {"type": "error", "msg": msg},
                status_code=401
            )
        request.state.info = token_data.model_dump()
        response = await call_next(request)
        response.headers["replace_jwt"] = str(token_data.is_old)
        return response

    # test 用，允许所有来源的请求
    app.add_middleware(
        cast("_MiddlewareFactory", CORSMiddleware),
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
        expose_headers=["replace_jwt"],
    )
    app.state = State()
    # WebSocket 不走 HTTP 中间件，把校验函数和前缀挂到 state 上供它复用
    app.state.verify_token = _verify_authorization_header
    app.state.auth_prefix = token_prefix
    app.include_router(ai_chat_route, prefix="/ai", tags=["Agent对话聊天等操作"])
    app.include_router(login_route, prefix="/auth/admin", tags=["账号管理"])
    app.include_router(open_app_route, prefix="/open/app", tags=["打开应用操作"])
    app.include_router(start_file_route, tags=["启动文件"])
    app.mount("/", StaticFiles(directory=ROOT_BASE_DIR_PATH / "static" / "dist"), name="static")
    return app
