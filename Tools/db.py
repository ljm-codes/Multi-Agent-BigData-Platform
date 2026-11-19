from sqlalchemy import create_engine, QueuePool, AsyncAdaptedQueuePool
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy.orm import sessionmaker

from load_config.config import config, ROOT_BASE_DIR_PATH

postgres_config = config.get("databases", {}).get("postgres", {})
hive_config = config.get("databases", {}).get("hive", {})
connect_to_cluster = bool((config.get("app") or {}).get("connect_to_cluster", False))
if not postgres_config:
    raise ValueError("不允许未设定 PostgreSQL 数据库配置")

_POSTGRESQL_URL = (
    f"postgresql+psycopg://{postgres_config.get('user')}:{postgres_config.get('password')}"
    f"@{postgres_config.get('host')}:{postgres_config.get('port')}/{postgres_config.get('db')}"
)

_ASYNC_POSTGRESQL_URL = (
    f"postgresql+asyncpg://{postgres_config.get('user')}:{postgres_config.get('password')}"
    f"@{postgres_config.get('host')}:{postgres_config.get('port')}/{postgres_config.get('db')}"
)


def supplement_url(host, port, user, password) -> str | None:
    from lxml import etree
    hive_xml_path = ROOT_BASE_DIR_PATH / "cluster_resources/hive-site.xml"
    if not hive_xml_path.exists():
        # 兜底文件不存在，返回 None 表示无法补充 URL
        return None
    hive_tree = etree.parse(hive_xml_path)
    hive_root = hive_tree.getroot()
    properties = hive_root.findall("property")
    for p in properties or []:
        name = p.findtext("name")
        value = p.findtext("value")
        if not host and name == "hive.server2.thrift.bind.host":
            host = value
        elif not port and name == "hive.server2.thrift.port":
            port = value
        elif not user and name == "javax.jdo.option.ConnectionUserName":
            user = value
        elif not password and name == "javax.jdo.option.ConnectionPassword":
            password = value
    if not host or not port or not user:
        return None
    return f"hive://{user}{f':{password}' if password else ''}@{host}:{port}{'?auth=CUSTOM' if password else ''}"


def get_hive_url(conf: dict) -> str | None:
    host = conf.get("thrift_host")
    port = conf.get("thrift_port")
    password = conf.get("password")
    user = conf.get("username")
    if not host or not port or not user:
        return supplement_url(host, port, user, password)
    return f"hive://{user}{f':{password}' if password else ''}@{host}:{port}{'?auth=CUSTOM' if password else ''}"


engine = create_engine(
    _POSTGRESQL_URL,
    poolclass=QueuePool,
    pool_size=10,
    max_overflow=20,
    pool_recycle=3600,
    pool_pre_ping=True,
    echo=False,
)

async_engine = create_async_engine(
    _ASYNC_POSTGRESQL_URL,
    poolclass=AsyncAdaptedQueuePool,
    pool_size=10,
    max_overflow=20,
    pool_recycle=3600,
    pool_pre_ping=True,
    echo=False,
)

if connect_to_cluster:
    if hive_url := get_hive_url(hive_config):
        engine = create_engine(
            hive_url, poolclass=QueuePool,
            pool_size=10, max_overflow=20,
            pool_recycle=3600, pool_pre_ping=True,
            echo=False
        )
        HiveSession = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    else:
        HiveSession = None
else:
    HiveSession = None

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
AsyncSessionLocal = async_sessionmaker(autocommit=False, autoflush=False, bind=async_engine)
