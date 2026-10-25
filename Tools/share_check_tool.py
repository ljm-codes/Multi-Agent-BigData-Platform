from typing import List, Dict, Optional, Any

import redis
import requests
from yarn_api_client import ResourceManager
from yarn_api_client.constants import RUNNING

from Tools.log_settings import LogSetting

logger = LogSetting.create(__name__)

try:
    from load_config.config import config as _app_config

    _yarn_config = ((_app_config or {}).get("hadoop") or {}).get("yarn") or {}
    _redis_config = _app_config.get("redis") or {}
except Exception as _e:  # 缺文件 / yaml 格式错 / 必填项缺失, 一律回退
    logger.error(f"读取 config.yaml 的 hadoop.yarn 失败, 使用内置默认值: {type(_e).__name__}: {_e}")
    _yarn_config = {}
    _redis_config = {}


def yarn_conf(key: str, default):
    """取 hadoop.yarn 下的配置项, 缺失或为 None 时回退到 default"""
    value = _yarn_config.get(key)
    return default if value is None else value


def _with_scheme(value, default: str) -> str:
    """
    给 host:port 补上 http:// 再返回

    config.yaml 里的 manager/proxy 存的是 **不带 scheme** 的写法(实测是 "node1:8088"),
    而 requests 和 yarn_api_client 都要求完整 URL —— 直接拿来用会报
    "No connection adapters were found for 'node1:8088/cluster'"。
    """
    if not value:
        return default
    value = str(value).strip().rstrip("/")
    return value if "://" in value else "http://" + value


poll_interval = yarn_conf('poll_interval', 15)  # 时间间隔，单位秒
manager = _with_scheme(yarn_conf('manager', ''), "http://node1:8088")
proxy = _with_scheme(yarn_conf('proxy', ''), "http://node1:8089")
max_job_timeout = yarn_conf('max_job_timeout', 30 * 60)  # 最大运行时间，单位秒
if not manager:
    raise ValueError("YARN 配置中缺少 manager 项，请现在主目录的 config.yaml 中配置 hadoop yarn 配置")

redis_pool = redis.ConnectionPool(
    host=_redis_config.get("host", 'localhost'),
    port=_redis_config.get("port", 6379),
    db=_redis_config.get("db", 0),
    password=_redis_config.get("password", ''),
    max_connections=_redis_config.get("max_connections", 20),
)
redis_con = redis.Redis(connection_pool=redis_pool)

rm = ResourceManager(service_endpoints=[manager])


def get_all_running_apps() -> List[Dict]:
    """
    获取全部正在运行的应用信息
    """
    try:
        running_apps = rm.cluster_applications(states=[RUNNING])
        running_apps_data = running_apps.data
        # print(f"正在运行的应用信息: {running_apps_data.get("apps", {}).get("app", [])}")
        return running_apps_data.get("apps", {}).get("app", [])
    except Exception as e:
        logger.error(f"获取全部正在运行的应用信息失败: {e}")
        return []


def is_flink_app(app: Dict) -> bool:
    """
    判断 YARN 应用是否为 Flink 应用
    """
    return "flink" in app.get("applicationType", "").lower()


def is_spark_app(app: Dict) -> bool:
    """
    判断 YARN 应用是否为 Spark 应用
    """
    return "spark" in app.get("applicationType", "").lower()


def request_get(uri: str, params: Optional[Dict[str, Any]] = None, timeout: int = 10) -> Any:
    try:
        response = requests.get(url=uri, timeout=timeout, params=params)
        if response.status_code in (200, 202):
            return response.json()
        else:
            logger.warning(f"获取数据失败: 状态码为 {response.status_code}, 响应内容为 {response.text}")
            return None
    except Exception as e:
        logger.error(f"获取数据失败: {e}")
        return None


def get_flink_rest_base(app_id: str) -> str:
    """
    取 Flink 应用的 REST 基址

    走 YARN 的 WebAppProxy (node1:8089), 用 YARN 的 application id 寻址。
    代理地址编写, 自动转发到该应用的 ApplicationMaster。
    """
    return f"{proxy}/proxy/{app_id}"


def get_running_flink_job_ids(app_id: str) -> List[str]:
    """
    取该 Flink 应用中正在运行的工作 ID 列表, 并清理已结束工作的水位线记录

    原实现走 {flink_manager}/applications/{app_id} 再自行过滤, 现改用 /jobs/overview:
    它直接带 state 字段, 一个请求即可, 也省去了与 /jobs/overview 的二次比对。
    """
    overview_info = request_get(f"{get_flink_rest_base(app_id)}/jobs/overview")
    if not overview_info:
        return []
    jobs = overview_info.get("jobs", [])
    running_job_ids = [
        job.get("jid", "") for job in jobs
        if job.get("jid", "") and job.get("state", "") == "RUNNING"
    ]
    # 不论成功还是失败, 但凡不是正在运行的工作都清理, 避免水位线记录只增不减
    all_job_ids = {job.get("jid", "") for job in jobs if job.get("jid", "")}
    for finished_job_id in all_job_ids - set(running_job_ids):
        redis_con.delete(f"watermarks:{finished_job_id}")
    return running_job_ids


def pick_stage_info(stage_infos: Any) -> Any:
    """
    从阶段信息中取出最新一次尝试
    :param stage_infos: 阶段信息, 可能是单个字典, 也可能是同阶段多次尝试的列表
    :return: 最新一次尝试的阶段信息, 无数据时返回 None
    """
    if not stage_infos:
        return None
    if isinstance(stage_infos, list):
        if len(stage_infos) == 1:
            return stage_infos[0]
        # 根据 attemptId 字段进行降序, 取最新一次尝试
        stage_infos.sort(key=lambda stage_info: stage_info["attemptId"], reverse=True)
        return stage_infos[0]
    return stage_infos


def get_latest_stage_info(stage_id: int, app_id: str) -> Any:
    """
    获取 Spark 应用任务最新一次尝试的详细信息

    只适合**指标类**判定(倾斜比、拉取等待占比、溢写字节): 那些必须看最新一次
    attempt —— 失败那次的数据是残缺的, 拿它算比值没有意义。

    **不要用它做失败判定**: 它按 attemptId 降序取最新, 而 Spark 会重试 stage,
    「失败一次后重试成功」的 stage 最新 attempt 是 COMPLETE, 那次真实失败就永远
    看不见了。实测 application_..._0040 的 stage 1: attempt 0 = FAILED
    (FetchFailedException: remote executor(Id: 3) is dead), attempt 1 = COMPLETE,
    5 个 checker 全返回 None。失败判定请用 get_stage_summary 的 failed 侧。
    """
    return pick_stage_info(get_stage_info(stage_id, app_id))


def get_stage_summary(stage_id: int, app_id: str):
    """
    一次取回同一个 stage 的两个视角, 供不同类型的判定各取所需

    :return: (latest, latest_failed)
             latest        最近一次 attempt —— 指标类判定用
             latest_failed 最近一次 FAILED 的 attempt, 没失败过则为 None —— 失败判定用

    Spark 对失败的 stage 会重试, 而重试成功后最新 attempt
    就是 COMPLETE。只看 latest 的话, 那些**自愈的失败**(executor 死亡、取数失败、
    网络抖动 —— 往往是最早的劣化信号)对 checker 完全不可见。
    """
    stage_infos = get_stage_info(stage_id, app_id)
    if not stage_infos:
        return None, None
    if not isinstance(stage_infos, list):
        stage_infos = [stage_infos]
    stage_infos = [info for info in stage_infos if isinstance(info, dict)]
    if not stage_infos:
        return None, None

    latest = max(stage_infos, key=lambda info: info.get("attemptId", 0))
    failed_infos = [info for info in stage_infos if info.get("status") == "FAILED"]
    latest_failed = (
        max(failed_infos, key=lambda info: info.get("attemptId", 0))
        if failed_infos else None
    )
    return latest, latest_failed


def get_stage_info(stage_id: int, app_id: str) -> Dict[str, Any]:
    """
    获取 Spark 应用任务的详细信息
    :param stage_id: 任务 ID
    :param app_id: 应用 ID
    :return: 任务详细信息
    """
    uri = f"{proxy}/proxy/{app_id}/api/v1/applications/{app_id}/stages/{stage_id}/"
    try:
        response = requests.get(url=uri, timeout=10, params={"details": True})
        if response.status_code in (200, 202):
            # print(f"获取任务详细信息成功: {response.json()}")
            return response.json()
        else:
            logger.warning(f"获取任务摘要信息失败: 状态码为 {response.status_code}, 响应内容为 {response.text}")
            return {}
    except Exception as e:
        logger.error(f"获取任务摘要信息失败: {e}")
        return {}
