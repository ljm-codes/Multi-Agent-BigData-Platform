"""
检查异常情况：
 - 类/依赖冲突
 - 外部数据源故障
 - 认证/权限问题
 - 数据格式/类型不匹配
"""
import re
import time

from SPO.check_result import CheckResult, FlinkOtherAbnormalInfo
from Tools.share_check_tool import (get_all_running_apps, get_flink_rest_base,
                                    get_running_flink_job_ids, is_flink_app,
                                    request_get, poll_interval)

# 每条规则包含: 描述、异常类名正则、堆栈文本正则
# 异常类名优先, 命中即不再扫堆栈, 减少误判
ABNORMAL_RULES = [
    {
        "describe": "出现 类/依赖冲突",
        "exception_pattern": re.compile(
            r'(NoClassDefFoundError|ClassNotFoundException|IncompatibleClassChangeError|'
            r'NoSuchMethodError|NoSuchFieldError)'
        ),
        "stacktrace_pattern": re.compile(
            r'(NoClassDefFoundError|ClassNotFoundException|IncompatibleClassChangeError|'
            r'NoSuchMethodError|NoSuchFieldError)'
        ),
    },
    {
        "describe": "出现 外部数据源故障",
        "exception_pattern": re.compile(
            r'(SQLException|SQLTransientConnectionException|FileNotFoundException|'
            r'RedisConnectionFailureException|KafkaException|NoBrokersAvailable)',
            re.IGNORECASE
        ),
        "stacktrace_pattern": re.compile(
            r'(SQLException|FileNotFoundException|Connection refused|Connection timed out|'
            r'NoBrokersAvailable|UnknownTopicOrPartition)',
            re.IGNORECASE
        ),
    },
    {
        "describe": "出现 认证/权限问题",
        "exception_pattern": re.compile(
            r'(AccessControlException|AccessDeniedException|AuthenticationException|'
            r'AuthorizationException|SaslAuthenticationException|LoginException)',
            re.IGNORECASE
        ),
        "stacktrace_pattern": re.compile(
            r'(AccessControlException|Permission denied|AccessDenied|authentication[ ]?failed|'
            r'SaslAuthentication|Invalid credentials)',
            re.IGNORECASE
        ),
    },
    {
        "describe": "出现 数据格式/类型不匹配",
        "exception_pattern": re.compile(
            r'(NumberFormatException|DateTimeException|DateTimeParseException|'
            r'DataTypeMismatchException|ClassCastException)',
            re.IGNORECASE
        ),
        "stacktrace_pattern": re.compile(
            r'(NumberFormatException|DateTimeParseException|cannot cast|Schema mismatch|'
            r'data type mismatch|Invalid format)',
            re.IGNORECASE
        ),
    },
]


def match_abnormal_rule(exception_name: str, stacktrace: str) -> str:
    """
    匹配异常所属的异常类型
    :param exception_name: 异常类名
    :param stacktrace: 异常堆栈
    :return: 命中的规则描述, 未命中时返回空字符串
    """
    for rule in ABNORMAL_RULES:
        # 先按异常类名精确判断, 再回退到堆栈文本
        if rule["exception_pattern"].search(exception_name):
            return rule["describe"]
        if rule["stacktrace_pattern"].search(stacktrace):
            return rule["describe"]
    return ""


def check_job_exception_abnormal(base: str, job_id: str, app_id: str) -> list:
    """
    检查工作异常历史中的其它异常情况
    :param base: 该应用的 Flink REST 基址
    :param job_id: 工作 ID
    :param app_id: 应用 ID
    :return: 异常信息列表
    """
    # /jobs/:jobid/exceptions -> JobExceptionsInfoWithHistory
    exceptions_info = request_get(f"{base}/jobs/{job_id}/exceptions")
    if not exceptions_info:
        return []
    entries = exceptions_info.get("exceptionHistory", {}).get("entries", [])
    if not entries:
        return []
    abnormal_error_list = []
    for entry in entries:
        # 根异常与并发异常都可能命中
        exception_infos = [entry] + entry.get("concurrentExceptions", [])
        for exception_info in exception_infos:
            exception_name = exception_info.get("exceptionName", "")
            stacktrace = exception_info.get("stacktrace", "")
            if not exception_name and not stacktrace:
                continue
            describe = match_abnormal_rule(exception_name, stacktrace)
            if not describe:
                continue
            abnormal_error_list.append(FlinkOtherAbnormalInfo(
                app_id=app_id,
                job_id=job_id,
                type="error",
                describe=describe,
                exception_name=exception_name,
                task_name=exception_info.get("taskName", ""),
                taskmanager_id=exception_info.get("taskManagerId", ""),
                timestamp=exception_info.get("timestamp", 0),
                stacktrace=stacktrace
            ))
    return abnormal_error_list


def check_flink_other_abnormal(app_id: str):
    """
    检查 Flink 应用的其它异常情况
    """
    base = get_flink_rest_base(app_id)
    jobs_list = get_running_flink_job_ids(app_id)
    if not jobs_list:
        return None
    other_error_list = []
    for job_id in jobs_list:
        other_error_list.extend(check_job_exception_abnormal(base, job_id, app_id))
    if not other_error_list:
        return None
    return CheckResult(
        type="error",
        describe="检测出出现了其他异常情况",
        data=other_error_list
    )


def check_flink_abnormal_situation():
    """
    检查运行中 Flink 应用的其它异常情况
    """
    running_apps = get_all_running_apps()
    if not running_apps:
        return None
    for running_app in running_apps:
        app_id = running_app.get("id", "")
        if not app_id:
            continue
        if not is_flink_app(running_app):
            continue
        # logger.info(f"正在检测ID为 {app_id} 的应用其它异常情况")
        error_result = check_flink_other_abnormal(app_id)
        if error_result:
            return error_result
    return None


if __name__ == "__main__":
    while True:
        error_result = check_flink_abnormal_situation()
        if error_result:
            print(f"!!!检测到其他异常!!!: {error_result}")
            break
        time.sleep(poll_interval)
