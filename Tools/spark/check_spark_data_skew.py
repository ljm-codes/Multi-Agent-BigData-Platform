import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List

import numpy as np

from SPO.check_result import TaskMetrics, StageMetrics, CheckResult
from Tools.log_settings import LogSetting
from Tools.share_check_tool import (get_all_running_apps, request_get, proxy,
                                    get_latest_stage_info, poll_interval, yarn_conf)

# 倾斜阈值从 config.yaml 的 hadoop.yarn.spark 段读, 缺失就回退到这两个默认值。
# 默认值的依据是实测(tests/skew_threshold_sweep.py):
#   健康作业的检出比稳定在 2.00(连跑 3 次一致), 而真实倾斜 5:1 -> 检出 5.27,
#   10:1 -> 9.29, 20:1 -> 25.50 —— 映射基本准确。
#   所以 5.0 对应真实约 5:1(有 2.5 倍余量), 10.0 对应约 10:1。
_skew_conf = yarn_conf('spark', {}) or {}
skew_warning_ratio = float(_skew_conf.get('skew_warning_ratio', 5.0))
skew_error_ratio = float(_skew_conf.get('skew_error_ratio', 10.0))

logger = LogSetting.create(__name__)


# 检查运行时的数据倾斜情况
def check_abnormal_situation():
    """
    检查运行时的异常情况
    """
    running_apps = get_all_running_apps()

    if not running_apps:
        return None

    for running_app in running_apps:
        app_type = running_app.get("applicationType", "")
        app_id = running_app.get("id", "")
        logger.info(f"正在检测ID为 {app_id} 的应用数据倾斜情况")
        if not app_id:
            continue
        if app_type == "SPARK":
            return check_spark_data_skew(app_id)


def check_stage_summary(stage_ids: List[int], app_id: str):
    """
    扫 stages, 报 最严重 的倾斜
    """
    worst = None  # (skew_ratio, jvm_gc_time_skew_ratio, stage_id, stage_metrics)
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {
            executor.submit(get_latest_stage_info, stage_id, app_id): stage_id
            for stage_id in stage_ids
        }
        for future in as_completed(futures):
            try:
                stage_id = futures[future]
                stage_infos = future.result()
                if not stage_infos:
                    continue
                tasks_info = stage_infos.get("tasks", {})
                task_metrics = []
                for stage, value in tasks_info.items():
                    task_info = TaskMetrics(
                        task_id=value.get("taskId", 0),
                        status=value.get("status", ""),
                        records_read=value.get("taskMetrics", {}).get("shuffleReadMetrics", {}).get("recordsRead", 0),
                        jvm_gc_time=value.get("taskMetrics", {}).get("jvmGcTime", 0),
                    )
                    task_metrics.append(task_info)
                # 排序 升序
                # task_metrics.sort(key=lambda x: x["records_read"], reverse=False)
                stage_metrics = StageMetrics(
                    stage_id=stage_id,
                    status=stage_infos.get("status", ""),
                    tasks=task_metrics,
                )
                read_records = [task["records_read"] for task in task_metrics]
                jvm_gc_times = [task.get("jvm_gc_time") for task in task_metrics]

                if not read_records:
                    continue

                median_read_record = np.median(read_records)
                max_read_record = max(read_records)

                median_jvm_gc_time = np.median(jvm_gc_times)
                max_jvm_gc_time = max(jvm_gc_times)

                if not read_records:
                    logger.warning(f"阶段 {stage_id} 无 shuffle 读取记录")
                    continue
                if max_read_record == 0.0 or median_read_record == 0.0:
                    continue
                skew_ratio = max_read_record / median_read_record
                # 中位 GC 时间为 0 时这个比值没有意义(0.0 是个哨兵值, 不是真实比值)。
                # 任务快、数据量小的时候中位 GC 为 0 很常见(jvmGcTime 是毫秒, 小任务就是 0)。
                if median_jvm_gc_time == 0:
                    jvm_gc_time_skew_ratio = 0.0
                else:
                    jvm_gc_time_skew_ratio = max_jvm_gc_time / median_jvm_gc_time
                print(f"阶段 {stage_id} 数据倾斜比为 {skew_ratio:.2f}, JVM GC 时间比为 {jvm_gc_time_skew_ratio:.2f}")

                if skew_ratio <= skew_warning_ratio:
                    print(f"阶段 {stage_id} 数据倾斜比为 {skew_ratio:.2f}，"
                          f"未超过阈值 {skew_warning_ratio}，不上报")
                    continue
                # 只看倾斜比, JVM GC 时间比只作为描述信息带出去。
                logger.info(f"阶段 {stage_id} 数据倾斜比为 {skew_ratio:.2f}, "
                            f"JVM GC 时间比为 {jvm_gc_time_skew_ratio:.2f}, "
                            f"超过阈值 {skew_warning_ratio}")
                # 只收集, 不返回 —— 等所有 stage 看完再定档(见函数开头)
                if worst is None or skew_ratio > worst[0]:
                    worst = (skew_ratio, jvm_gc_time_skew_ratio, stage_id, stage_metrics)
            except Exception as e:
                # logger.error(f"检查阶段 {stage_id} 数据倾斜情况失败: {e}")
                logger.error(f"检查阶段 {stage_id} 数据倾斜情况失败: {e}")
                # 显示完整错误栈
                import traceback
                traceback.print_exc()
                continue

    if worst is None:
        return None

    # 定档只看倾斜比(JVM GC 时间比只作为描述带出去), 由 最严重 的 stage 决定
    skew_ratio, jvm_gc_time_skew_ratio, stage_id, stage_metrics = worst
    severity = "error" if skew_ratio > skew_error_ratio else "warning"
    logger.info(f"最严重的 stage 是 {stage_id}(倾斜比 {skew_ratio:.2f}) -> {severity}"
                f"(error 线 {skew_error_ratio})")
    if severity == "error":
        return CheckResult(
            type="error",
            describe=f"出现数据倾斜, 阶段 {stage_id} 数据倾斜比为 {skew_ratio:.2f}, JVM GC 时间比为 {jvm_gc_time_skew_ratio:.2f}",
            data=stage_metrics
        )
    return CheckResult(
        type="warning",
        describe=f"疑似出现倾斜。阶段 {stage_id} 数据倾斜比为 {skew_ratio:.2f}, JVM GC 时间比为 {jvm_gc_time_skew_ratio:.2f}",
        data=stage_metrics
    )


def check_spark_data_skew(app_id: str):
    """
    检查 Spark 应用的数据倾斜情况
    """
    app_jobs = request_get(f"{proxy}/proxy/{app_id}/api/v1/applications/{app_id}/jobs")
    if not app_jobs:
        return None
    app_run_job = app_jobs[0]
    stage_ids = app_run_job.get("stageIds", [])
    # 获取对应的 taskSummary 信息
    result = check_stage_summary(stage_ids, app_id)
    if result:
        return result


if __name__ == "__main__":
    # test
    while True:
        error_result = check_abnormal_situation()
        if error_result:
            # logger.error("检测到数据倾斜问题")
            print(f"!!!检测到数据倾斜问题!!!: {error_result}")
            break
        time.sleep(poll_interval)
