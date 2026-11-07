"""
解析 jar 包中的类文件
这个文件将所有解析函数提取为一个公共包
"""
from pathlib import Path

import re
from typing import Literal

from Tools.log_settings import LogSetting
from Tools.run_command import run_java_command
from load_config.config import ROOT_BASE_DIR_PATH

logger = LogSetting.create(__name__)

_DEFAULT_OUT_PATH = str(ROOT_BASE_DIR_PATH / "tmp")


def check_jar_class(
        jar_path: Path, keyword: str | None = None,
        ignore_case: bool = False, inner_class: bool = False,
) -> tuple[bool, str]:
    """
    查看 jar 包 中所有类
    参数：
        jar_path: jar包路径
        keyword: 匹配的类名关键词（默认不进行匹配，即查看所有类名）
        ignore_case: 是否忽略大小写匹配（默认忽略大小写匹配）(True->开启, False->关闭)
        inner_class: 是否忽略内部类（默认忽略内部类）（True->不忽略内部类, False->忽略内部类）
    """
    if not jar_path.is_file():
        return False, f"jar包路径不存在: {jar_path}"
    return_code, jar_class_out_str = run_java_command(f"jar tf {jar_path}")
    if return_code != 0:
        return False, f"查看jar包类名失败，错误信息: {jar_class_out_str}"
    capture = r"(?<![\w$/])([\w/]+)" if not inner_class else r".*?"
    default_reg = capture + r"\.class"
    try:
        jar_class_str_list = re.findall(default_reg, jar_class_out_str, flags=re.IGNORECASE if ignore_case else 0)
        if keyword:
            if ignore_case:
                comp_fun = lambda name: keyword.lower() in name.lower()
            else:
                comp_fun = lambda name: keyword in name
            jar_class_str_list = [c.replace("/", ".") for c in jar_class_str_list if comp_fun(c.rsplit("/", 1)[-1])]
        if not jar_class_str_list:
            return False, f"指定jar包下未找到相关的任何类名"
        return True, f"指定jar包下的类名为: {','.join(jar_class_str_list)}"
    except Exception as e:
        logger.error(f"查看jar包类名失败{type(e).__name__}: {e}")
        return False, f"查看jar包类名失败{type(e).__name__}: {e}"


def check_java_api_roughly_info(
        jar_path: Path, class_name: str, *,
        method_name: str | None = None, ignore_case: bool = True,
        exclude_type: Literal["method", "variable"] | None = None,
) -> str:
    # 构建命令
    command = f"javap -classpath {jar_path.name} -p {class_name}"
    return_code, javap_out_str = run_java_command(command, path=str(jar_path.parent))
    if return_code != 0:
        return f"查看类大致信息失败，错误信息: {javap_out_str}"
    # 清洗
    raw_lines = [ln.rstrip() for ln in javap_out_str.splitlines()]
    lines = [ln.strip() for ln in raw_lines if ln.strip()]
    if lines and lines[0].startswith("Compiled from"):
        lines = lines[1:]
    if not lines:
        return "解析失败：javap 输出为空"
    # 类声明行（末尾可能带 '{'，去掉）
    class_decl = lines[0].rstrip("{").strip()
    member_lines = lines[1:]

    # 逐行提取
    members: list[str] = []
    brace_depth = 0
    for ln in member_lines:
        brace_depth += ln.count("{") - ln.count("}")
        # 跳过内部类声明 -> 虽然说实测没有
        if brace_depth > 0:
            continue
        if ln == "}" or ln.startswith("static {"):
            continue
        if ln.endswith(";"):
            members.append(ln[:-1].strip())

    # 提取成员名
    def _member_name(decl: str) -> str | None:
        if "(" in decl:  # 方法/构造器
            before = decl[: decl.index("(")]
            m = re.search(r"([A-Za-z_$][\w$]*)\s*$", before)
            return m.group(1) if m else None
        # 字段：取最后一个词（泛型里不会带空格，最后一个 token 就是字段名）
        parts = decl.split()
        return parts[-1] if parts else None

    # 按需过滤
    def _keep(decl: str) -> bool:
        is_method = "(" in decl
        if exclude_type == "method" and is_method:
            return False
        if exclude_type == "variable" and not is_method:
            return False
        if method_name:
            name = _member_name(decl)
            if name is None:
                return False
            if ignore_case:
                return name.lower() in method_name.lower()
            else:
                return name in method_name
        return True

    filtered = [m for m in members if _keep(m)]
    # 组装
    out_lines = [f"{class_decl}:"]
    for ln in filtered:
        out_lines.append(f"\t{ln};")
    return "\n".join(out_lines)


def check_java_code(
        jar_path: Path, class_name: str, class_absolute_path: Path,
        *,
        method_name: str | None = None, ignore_case: bool = True,
        out_path: str = _DEFAULT_OUT_PATH,
) -> str:
    def _filter_code(java_code: str) -> str:
        """用于处理传入的 method_name 与 ignore_case 参数，返回过滤后的代码"""
        if not method_name:
            return java_code

        def match(text: str, target: str) -> bool:
            return target.lower() in text.lower() if ignore_case else target in text

        class_stack: list[tuple[str, int]] = []  # (类名, 进入类体后的括号深度)
        brace_depth = 0  # 当前大括号嵌套深度
        in_method = False
        method_close_depth = 0  # 方法体闭合时要回落到的深度
        method_lines: list[str] = []
        blocks: list[str] = []

        class_pattern = re.compile(r'\b(?:class|interface|enum)\s+(\w+)')
        for raw_line in java_code.splitlines():
            line = raw_line.strip()
            open_count = line.count("{")
            close_count = line.count("}")
            new_depth = brace_depth + open_count - close_count

            if in_method:
                # 这一行会让深度回落到方法外层 -> 它是方法的结束花括号
                if new_depth <= method_close_depth:
                    method_lines.append(line)  # 结束花括号不加 tab
                    blocks.append("\n".join(method_lines))
                    method_lines = []
                    in_method = False
                else:
                    method_lines.append("\t" + line)
            else:
                # 检测类 / 接口 / 枚举声明
                cls_match = class_pattern.search(line)
                if cls_match and open_count > 0:
                    # 记录类名 + 进入类体后的深度
                    class_stack.append((cls_match.group(1), new_depth))
                elif "(" in line:
                    # 只取方法名之前的部分做匹配, 避免参数名误命中
                    sig = line.rsplit("(", 1)[0]
                    if match(sig, method_name):
                        qualifier = ".".join(name for name, _ in class_stack)
                        header = (
                            f"// {qualifier}.{method_name}"
                            if qualifier else f"// {method_name}"
                        )
                        method_lines = [header, line]
                        in_method = True
                        method_close_depth = brace_depth
                        # 注意: 这里不更新 brace_depth, 让后面统一处理

            # 统一更新括号深度
            brace_depth = new_depth

            # 弹出已闭合的类
            while class_stack and brace_depth < class_stack[-1][1]:
                class_stack.pop()

        if not blocks:
            return f"未在 {class_name} 中找到方法: {method_name}"
        return "\r\n".join(blocks)

    # CFR 输出的是 .java，调用方传进来的可能是 .class
    java_absolute_path = class_absolute_path.with_suffix(".java")
    if java_absolute_path.is_file():
        with open(java_absolute_path, "r", encoding="utf-8") as f:
            code = f.read()
        return _filter_code(code)
    # 构建命令
    class_name = class_name.replace(".", "\\.")
    # 让中文不再显示为 Unicode 编码的字符串
    command = (f'java -jar "{ROOT_BASE_DIR_PATH / "Tools" / "jar" / "cfr-0.152.jar"}" '
               f'{jar_path.name} --jarfilter ^{class_name}$ '
               f'--outputdir "{out_path}" --hideutf false')
    # 执行命令
    return_code, stderr = run_java_command(command, path=str(jar_path.parent))
    if return_code != 0:
        return f"查看类大致代码失败，错误信息: {stderr}"
    if not java_absolute_path.is_file():
        return f"反编译后没有找到 {java_absolute_path}（class 名或 jarfilter 可能不对）"
    # 清洗代码
    with open(java_absolute_path, "r", encoding="utf-8") as f:
        code = f.read()
    return _filter_code(code)
