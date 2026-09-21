import re
from datetime import datetime, timezone
from typing import Tuple, Optional

from SPO.check_result import CheckResult
from Tools.share_check_tool import rm


def parse_bad_disk_ratio(health_report: str) -> Tuple[Optional[int], Optional[int]]:
    if not health_report:
        return None, None
    pattern = re.compile(r'(\d+)/(\d+)\s+local[\s-]?.+\s+are\s+bad', re.IGNORECASE)
    match = pattern.search(health_report)
    if match:
        return int(match.group(1)), int(match.group(2))
    return None, None


def check_disk():
    nodes_data = rm.cluster_nodes().data
    if not nodes_data:
        return None
    nodes_info = nodes_data.get("nodes", {}).get("node", [])
    if not nodes_info:
        return None
    error_data = []
    for node in nodes_info:
        node_id = node.get("id", "")
        state = node.get("state", "")
        health_report = node.get("healthReport", "")

        bad_cnt, total_cnt = parse_bad_disk_ratio(health_report)

        report_lower = health_report.lower()
        has_disk_keyword = (
                'bad' in report_lower or
                'no space' in report_lower or
                'disk' in report_lower
        )
        has_bad_disk = bool(bad_cnt and total_cnt and bad_cnt > 0)
        if state == "UNHEALTHY" and (has_disk_keyword or has_bad_disk):
            # 关键词与坏盘两个条件由构造同时成立(能解析出坏盘比的正则必须含 "are bad",
            # 因而必然命中 has_disk_keyword), 原本会 append 两条, 这里合并为一条
            error_item = {
                "node_id": node_id,
                "state": state,
                "health_report": health_report,
            }
            if has_bad_disk:
                error_item["bad_disk_ratio"] = f"{bad_cnt}/{total_cnt}"
            error_data.append(error_item)
        elif state == "RUNNING" and has_bad_disk:
            error_data.append({
                "node_id": node_id,
                "state": state,
                "health_report": health_report,
                "bad_disk_ratio": f"{bad_cnt}/{total_cnt}",
            })
    if error_data:
        return CheckResult(
            type="error",
            describe="出现磁盘空间不足",
            check_time=datetime.now(timezone.utc),
            data=error_data,
        )
    return None
