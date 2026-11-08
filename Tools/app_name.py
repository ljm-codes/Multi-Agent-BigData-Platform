"""应用名校验：app_name 会作为 HDFS 目录名和 cmd 文件名，注册时就要拦住非法值。"""
import re

# 路径分隔符、Windows 文件名非法字符、控制字符
_INVALID_APP_NAME = re.compile(r'[\\/:*?"<>|\x00-\x1f]')


def validate_app_name(app_name: str) -> str | None:
    """合法返回 None；不合法返回给模型看的错误说明。"""
    raw = str(app_name if app_name is not None else "")
    if not raw.strip():
        return "应用名不能为空"
    if raw != raw.strip():
        return "应用名首尾不能有空白"
    if raw in (".", ".."):
        return "应用名不能是 . 或 .."
    if raw.endswith("."):
        return "应用名不能以 . 结尾（Windows 目录名会被截断）"
    if len(raw) > 128:
        return "应用名不能超过 128 个字符"
    bad = sorted(set(_INVALID_APP_NAME.findall(raw)))
    if bad:
        return (f"应用名不能包含 {' '.join(bad)} 等路径/文件名字符；"
                f"建议使用字母数字下划线，如 FlinkKafkaPVUVJob")
    return None
