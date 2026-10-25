"""
项目主文件
用于fastapi的路由定义、中间件配置以及LangGraph图的初始化等操作
"""
import os
from contextlib import asynccontextmanager
from typing import cast, Optional

from fastapi import FastAPI, Request
from langchain_core.stores import BaseStore
from langgraph.cache.sqlite import SqliteCache
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
from langgraph.graph import StateGraph
from langgraph.store.memory import InMemoryStore
from langgraph.store.postgres import AsyncPostgresStore
from langgraph.types import Checkpointer
from starlette.middleware.cors import CORSMiddleware
from starlette.datastructures import State

from SPO.main_agent_state_model import MainStateContext
from Tasks.background_handler import start_perceptron
from Tools import LogSetting
from agent.develop_main_agent import builder
from load_config.config import ROOT_BASE_DIR_PATH, config
from routers.ai_chat import route as ai_chat_route

main_graph = None
_postgres_conf = (config.get('databases') or {}).get('postgres') or {}
_postgres_url = f"postgresql://{_postgres_conf.get('user')}:{_postgres_conf.get('password')}@{_postgres_conf.get('host')}:{_postgres_conf.get('port')}/{_postgres_conf.get('db')}?sslmode=disable"
# enabled: false 时退回 InMemorySaver/InMemoryStore（仅适合本地调试）
_pg_enabled = bool(_postgres_conf.get('enabled', True)) if _postgres_conf else False

logger = LogSetting.create(__name__)

if not os.path.isdir(ROOT_BASE_DIR_PATH / 'cache'):
    os.makedirs(ROOT_BASE_DIR_PATH / 'cache')


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


@asynccontextmanager
async def lifespan(api: FastAPI):
    global main_graph
    async with _graph_scope():
        api.state.main_graph = main_graph
        api.state.perceptron = start_perceptron(_background_runner)
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
        return await call_next(request)

    # test 用，允许所有来源的请求
    app.add_middleware(
        cast("_MiddlewareFactory", CORSMiddleware),
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    app.state = State()
    app.include_router(ai_chat_route, prefix="/ai", tags=["AI 对话聊天"])
    return app
