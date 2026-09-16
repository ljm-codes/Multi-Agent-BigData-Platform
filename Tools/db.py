from sqlalchemy import create_engine, QueuePool
from sqlalchemy.orm import sessionmaker

from load_config.config import config

postgres_config = config.get("databases", {}).get("postgres", {})
if not postgres_config:
    raise ValueError("不允许未设定 PostgreSQL 数据库配置")

_POSTGRESQL_URL = (
    f"postgresql+psycopg2://{postgres_config.get('user')}:{postgres_config.get('password')}"
    f"@{postgres_config.get('host')}:{postgres_config.get('port')}/{postgres_config.get('db')}"
)

engine = create_engine(
    _POSTGRESQL_URL,
    poolclass=QueuePool,
    pool_size=10,
    max_overflow=20,
    pool_recycle=3600,
    pool_pre_ping=True,
    echo=False,
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
