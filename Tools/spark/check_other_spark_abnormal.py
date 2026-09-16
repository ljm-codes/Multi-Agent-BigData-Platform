"""
检查异常情况：
 - 类/依赖冲突
 - 外部数据源故障
 - 认证/权限问题
 - 数据格式/类型不匹配
"""
import re
from concurrent.futures import ThreadPoolExecutor, as_completed

from SPO.check_result import CheckResult, StageAbnormalInfo
from Tools.log_settings import LogSetting
from Tools.share_check_tool import proxy, request_get, get_stage_summary

logger = LogSetting.create(__name__)


def check_task_error(rule, rule_desc: str, task_dict: dict, stage_id: int, attempt_id: int = 0) -> list:
    error_json_list = []
    for _, value in task_dict.items():
        if not isinstance(value, dict):
            continue
        if value.get("status", "") != "FAILED":
            continue
        error_msg = value.get("errorMessage", "")
        if not error_msg:
            continue

        if rule.search(error_msg):
            error_json_list.append(StageAbnormalInfo(
                stage_id=stage_id,
                status=value.get("status", ""),
                describe=rule_desc,
                task_id=value.get("taskId", 0),
                error_msg=error_msg,
                attempt_id=attempt_id,
            ))

    return error_json_list


def check_other(app_id: str):
    jobs_json = request_get(f"{proxy}/proxy/{app_id}/api/v1/applications/{app_id}/jobs")
    if not jobs_json:
        return None
    stage_ids = jobs_json[0].get("stageIds", [])
    if not stage_ids:
        return None
    other_error_json_list = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {
            executor.submit(get_stage_summary, stage_id, app_id): stage_id
            for stage_id in stage_ids
        }
        for future in as_completed(futures):
            try:
                stage_id = futures[future]
                # 失败判定必须看「曾失败过的那次 attempt」而不是最新 attempt:
                # stage 会重试, 重试成功后最新 attempt 是 COMPLETE, 只看最新的话
                # 那些自愈的失败(executor 死亡 / 取数失败 / 网络抖动)就全丢了。
                _, stage_infos = future.result()
                if not stage_infos:
                    continue
                attempt_id = stage_infos.get("attemptId", 0)
                failure_reason = stage_infos.get("failureReason", "")
                if not failure_reason:
                    continue
                class_pattern = re.compile(r"NoClassDefFoundError|ClassNotFoundException|IncompatibleClassChangeError")
                # Timeout 太宽: 它会匹配 "Connection timed out" 这类**网络**超时,
                # 于是同一个超时故障被这里和 check_spark_network_timeout 同时上报。
                # 现在只留数据源侧特有的写法, 通用的网络超时归网络那个 checker ——
                # 两个正则不再有交集(逐条比对见 tests 里的注释)。
                # 注意 "Read timed out": 它落在 java.net.SocketTimeoutException
                # 的文本里, 加回来就和网络侧重新重叠了; JDBC 的读超时会被包成
                # SQLException/SQLTimeoutException, 已经覆盖到。
                # Connection refused 留在这里(不留在网络侧): 被拒绝是立即返回的,
                # 不是超时, 语义上属于「连不上数据源」。
                data_source_pattern = re.compile(
                    r"SQLException|SQLTimeoutException|FileNotFoundException|Connection refused"
                )
                auth_pattern = re.compile(r"AccessControlException|Permission denied|authentication")
                data_format_pattern = re.compile(r"NumberFormatException|DateTimeException|DataTypeMismatchException"
                                                 r"|data type mismatch|cannot cast|Schema mismatch")
                if class_pattern.search(failure_reason):
                    other_error_json_list.extend(
                        check_task_error(
                            class_pattern,
                            "出现 类/依赖冲突",
                            stage_infos.get("tasks", {}),
                            stage_id,
                            attempt_id
                        )
                    )
                if data_source_pattern.search(failure_reason):
                    other_error_json_list.extend(
                        check_task_error(
                            data_source_pattern,
                            "出现 外部数据源故障",
                            stage_infos.get("tasks", {}),
                            stage_id,
                            attempt_id
                        )
                    )
                if auth_pattern.search(failure_reason):
                    other_error_json_list.extend(
                        check_task_error(
                            auth_pattern,
                            "出现 认证/权限问题",
                            stage_infos.get("tasks", {}),
                            stage_id,
                            attempt_id
                        )
                    )
                if data_format_pattern.search(failure_reason):
                    other_error_json_list.extend(
                        check_task_error(
                            data_format_pattern,
                            "出现 数据格式/类型不匹配",
                            stage_infos.get("tasks", {}),
                            stage_id,
                            attempt_id
                        )
                    )
            except Exception as e:
                logger.error(f"在检查应用{app_id}的stage{stage_id}时出错：{e}")
                continue

    if not other_error_json_list:
        return None
    return CheckResult(
        type="error",
        describe="检测出出现了其他异常情况",
        data=other_error_json_list
    )
