import json
import sys
import time
from datetime import datetime, timezone

from SPO.check_result import CheckResult
from Tools.log_settings import LogSetting
from Tools.share_check_tool import request_get, manager, redis_con

logger = LogSetting.create(__name__)


def check_times():
    file_redis_data = redis_con.get("file_handle")
    if not file_redis_data:
        # 首次检查时 Redis 里还没有记录, 返回与"未达阈值"一致的形状,
        # 否则调用方 result[0] 会因 None 而抛 TypeError
        return False, None, None, None
    file_json = json.loads(file_redis_data)
    file_ratio = float(file_json.get("file_ratio", 0.0))
    times = file_json.get("times", 0)
    sustain_time = float(file_json.get("time", 0.0))
    if times >= 3 or time.time() - sustain_time >= 60 * 3:
        return True, file_ratio, times, sustain_time
    return False, None, None, None


def upsert(file_ratio: float):
    file_redis_data = redis_con.get("file_handle")
    if not file_redis_data:
        file_json = json.dumps({
            "file_ratio": file_ratio,
            "times": 0,
            "time": time.time()
        })
        redis_con.set("file_handle", file_json)
        return
    file_json = json.loads(file_redis_data)
    if file_ratio - float(file_json.get("file_ratio")) > 0:
        # 正相关相加
        file_json["times"] += 1
        redis_con.set("file_handle", json.dumps(file_json))
    else:
        # 负相关，只更新时间戳
        file_json["time"] = time.time()
        redis_con.set("file_handle", json.dumps(file_json))


def clear_check():
    redis_con.delete("file_handle")


def check_file_handle():
    uri = f"{manager}/jmx"
    file_data = request_get(uri, params={"qry": "java.lang:type=OperatingSystem"})
    if not file_data:
        return None
    # YARN 的 /jmx?qry=... 返回 {"beans": [{"name": ..., "OpenFileDescriptorCount": ...}]}
    # name 在 bean 内部而非顶层, 未命中时 beans 为空列表
    beans = file_data.get("beans") or []
    if not isinstance(beans, list) or not beans:
        return None
    file_info = beans[0]
    if not isinstance(file_info, dict):
        return None
    if file_info.get("name", "") != "java.lang:type=OperatingSystem":
        return None
    open_file_count = file_info.get("OpenFileDescriptorCount", 0)
    max_file_count = file_info.get("MaxFileDescriptorCount", 0)
    if max_file_count == sys.maxsize:
        logger.warning("无法通过YARN来检测文件句柄数量")
        return None
    if max_file_count < 0:
        logger.warning("YARN权限不足，无法获取文件句柄数量")
        return None
    result = check_times()
    if result[0]:
        file_ratio, times, sustain_time = result[1:]
        return CheckResult(
            type="error",
            describe="文件句柄耗尽",
            check_time=datetime.now(timezone.utc),
            data={
                "file_ratio": file_ratio,
                "times": times,
                "sustain_time": sustain_time,
            }
        )
    if open_file_count == 0:
        file_ratio = 0.0
    else:
        file_ratio = open_file_count / max_file_count

    if file_ratio <= 0.8:
        # 正常
        clear_check()
        return None
    if file_ratio >= 0.9:
        upsert(file_ratio)
