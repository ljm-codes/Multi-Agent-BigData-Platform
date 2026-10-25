import re
import os
import subprocess
from pathlib import Path
# from typing import Literal
from uuid import uuid4

from lxml import etree

_POM_TEXT = """
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">

    <modelVersion>4.0.0</modelVersion>
    <groupId>com.fm</groupId>
    <artifactId>__PROJECT_NAME__</artifactId>
    <version>0.1.0</version>

    <properties>
        <maven.compiler.source>8</maven.compiler.source>
        <maven.compiler.target>8</maven.compiler.target>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
        <Flink.version>1.18.1</Flink.version>
        <lombok.version>1.18.34</lombok.version>
    </properties>

    <dependencies>
        <!-- Flink 依赖 -->
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-clients</artifactId>
            <version>${Flink.version}</version>
        </dependency>

        <!-- Flink JSON格式处理（用于POJO反序列化） -->
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-json</artifactId>
            <version>${Flink.version}</version>
        </dependency>
        <!-- Jackson（JSON反序列化核心依赖） -->
        <dependency>
            <groupId>com.fasterxml.jackson.core</groupId>
            <artifactId>jackson-databind</artifactId>
            <version>2.15.2</version>
        </dependency>

        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-streaming-java</artifactId>
            <version>${Flink.version}</version>
        </dependency>

        <!-- Flink Web UI 测试专用-->
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-runtime-web</artifactId>
            <version>${Flink.version}</version>
        </dependency>

        <dependency>
            <groupId>org.projectlombok</groupId>
            <artifactId>lombok</artifactId>
            <version>${lombok.version}</version>
        </dependency>

    </dependencies>

</project>
"""
ROOT_BASE_DIR_PATH = Path(r"D:\pycharm\pycharm study\Multi-Agent BigData Platform")


def _split_command(command: str) -> list[str]:
    tokens: list[str] = []
    current: list[str] = []
    quote_char: str | None = None

    for ch in command:
        if quote_char is not None:
            if ch == quote_char:
                quote_char = None
            else:
                current.append(ch)
        else:
            if ch in ('"', "'"):
                quote_char = ch
            elif ch.isspace():
                if current:
                    tokens.append("".join(current))
                    current = []
            else:
                current.append(ch)

    if current:
        tokens.append("".join(current))

    return tokens


def _run_mvn_command(command: str, path: str) -> tuple[int, str]:
    """
    执行 mvn 命令，会自动匹配 mvn
    """
    cmd_list = command.split(" ")
    cmd_list = [cmd for cmd in cmd_list if cmd != "mvn"]
    command_list = ["mvn", *cmd_list]
    print(command_list)
    res = subprocess.run(
        command_list,
        cwd=path,
        shell=True,
        capture_output=True,
        text=True
    )
    # print(f"stdout: {res.stdout}, stderr: {res.stderr}")
    # print(f"res: {res}")
    if res.returncode != 0:
        return res.returncode, res.stderr or res.stdout
    return res.returncode, res.stdout


def _run_java_command(command: str, path: str | None = None) -> tuple[int, str]:
    stripped = command.lstrip()
    if not (stripped.startswith("java") or stripped.startswith("jar")):
        return -1, f"必须是 java 命令或 jar 命令: {command}"
    print(command)
    try:
        cmd_list = _split_command(command)
    except Exception as e:
        return -1, f"命令解析失败: {type(e).__name__}: {e}"
    print(cmd_list)
    try:
        res = subprocess.run(
            cmd_list,
            cwd=path,
            shell=False,  # 传 list 时不要用 shell=True
            capture_output=True,
            text=True,
            encoding="utf-8",  # 显式指定编码, 避免 Windows 默认 cp936
            errors="replace",  # 遇到无法解码的字节不抛异常
        )

        if res.returncode != 0:
            # 有些 Java 程序把错误输出到 stdout, 返回更完整的信息
            return res.returncode, (res.stderr or res.stdout or "").strip()

        return res.returncode, (res.stdout or "").strip()
    except Exception as e:
        print(f"执行 java 命令 {command} 失败, 错误信息{type(e).__name__}: {e}")
        return -1, f"执行 java 命令 {command} 失败, 错误信息{type(e).__name__}: {e}"


def write_big_data_app_code(code: str, file_path: str) -> str:
    # 构建应用的主目录
    app_path = Path("../tmp/")
    code = code.strip()
    if not code:
        return "代码不能为空"
    try:
        # 先进行主存放目录的创建，再进行文件的写入
        if not os.path.exists(app_path):
            os.makedirs(app_path)
        Path(app_path / file_path).parent.mkdir(parents=True, exist_ok=True)
        Path(app_path / file_path).write_text(code, encoding='utf-8')
        return f"文件 {file_path} 写入成功"
    except Exception as e:
        print(f"创建目录或者写入文件失败{type(e).__name__}: {e}")
        return f"创建目录或者写入文件失败{type(e).__name__}: {e}"


def replace_lines(path, start, end, new_text, encoding='utf-8'):
    """
    把文件中第 start ~ end 行（从 1 开始计数，含两端）替换为 new_text。
    new_text: str（可含多行）或 行的列表
    """
    app_path = Path("../tmp/")
    path = app_path / path
    with open(path, 'r', encoding=encoding, newline='') as f:
        lines = f.readlines()

    # 统一整理成「行列表」
    if isinstance(new_text, str):
        new_lines = new_text.splitlines(keepends=True)
    else:
        new_lines = list(new_text)

    # 保证每行以换行符结尾，避免和后面的内容粘连
    for i, l in enumerate(new_lines):
        if not l.endswith(('\n', '\r')):
            new_lines[i] = l + '\n'

    lines[start - 1:end - 1] = new_lines  # 多行、少行、0 行都能自动伸缩

    with open(path, 'w', encoding=encoding, newline='') as f:
        f.writelines(lines)


def view_code(path: str, start_idx: int, end_idx: int | None = None) -> str:
    app_path = Path("../tmp/")
    try:
        print_list = []
        with open((app_path / path), "r", encoding='utf-8', newline='') as f:
            file_code = f.readlines()
        if end_idx >= len(file_code):
            end_idx = len(file_code) + 1
        for i, line in enumerate(file_code[start_idx - 1:end_idx - 1], start=1):
            print_list.append(f"{i}\t{line}")
        return f"区间[{start_idx}, {end_idx})的代码如下：\n{'\r'.join(print_list)}"
    except Exception as e:
        return f"查看文件 {path} 失败{type(e).__name__}: {e}"


def initialize_project(app_type: str):
    uid = str(uuid4())
    app_path = Path(f"../tmp/{uid}")
    try:
        if not app_path.exists():
            app_path.mkdir()
        if app_type == "flink":
            # 创建 pom
            (app_path / "pom.xml").write_text(_POM_TEXT.strip().replace("__PROJECT_NAME__", uid), encoding='utf-8')
            # 创建src/...
            # 先构建写代码的目录
            (app_path / "src" / "main" / "java" / "com" / "fm").mkdir(parents=True, exist_ok=True)
            # 构建对应的资源文件夹
            (app_path / "src" / "main" / "resources").mkdir(parents=True, exist_ok=True)
            # 构建对应的测试目录
            (app_path / "src" / "test" / "java").mkdir(parents=True, exist_ok=True)
            return_code, stderr = _run_mvn_command(f"mvn clean install", str(app_path))

            if return_code != 0:
                print(f"项目构建失败，错误信息: {stderr}")
                return
            print("项目构建完成，相对项目路径为：src/main/java/com/fm, 之后的代码请在这里进行编写")
        elif app_type == "spark":
            pass
    except Exception as e:
        # 打印完整的异常信息，方便调试
        import traceback
        traceback.print_exc()
        print(f"项目初始化失败{type(e).__name__}: {e}")


NS = "http://maven.apache.org/POM/4.0.0"


def _qn(tag: str) -> str:
    """封装 maven 命名空间"""
    return f"{{{NS}}}{tag}"


def add_maven_dependence(
        group_id: str,
        artifact_id: str, version: str,
        scope: str | None = None
):
    try:
        project_path = Path("../tmp/e3b3678d-ed09-4e28-aa7f-b76bad190c72")
        pom_file_path = project_path / "pom.xml"
        tree = etree.parse(pom_file_path)
        pom_root = tree.getroot()
        dependencies = pom_root.find(_qn("dependencies"))
        already_have_dependency = dependencies.findall(_qn("dependency"))
        for dependency in already_have_dependency or []:
            already_have_group = dependency.findtext(_qn("groupId"))
            already_have_artifact = dependency.findtext(_qn("artifactId"))
            corresponding_version = dependency.findtext(_qn("version"))
            if corresponding_version and re.match(r"\$\{.*?}", corresponding_version):
                # 版本号是占位符, 进行处理
                version_placeholder = re.match(r"\$\{(.*?)}", corresponding_version).group(1)
                properties = pom_root.find(_qn("properties"))
                version = properties.findtext(_qn(f"{version_placeholder}"))
                if not version:
                    print(f"版本号 {version_placeholder} 未定义")
                    corresponding_version = "undefined"
                else:
                    corresponding_version = version.strip()
            if already_have_group == group_id and already_have_artifact == artifact_id:
                print(f"依赖 {group_id}:{artifact_id} 已存在, 对应的版本为 {corresponding_version}")
                return
        # 添加依赖标签
        new_dependency = etree.SubElement(dependencies, _qn("dependency"))
        new_group = etree.SubElement(new_dependency, _qn("groupId"))
        new_artifact = etree.SubElement(new_dependency, _qn("artifactId"))
        new_version = etree.SubElement(new_dependency, _qn("version"))
        # 添加文本
        new_group.text = group_id
        new_artifact.text = artifact_id
        new_version.text = version
        if scope:
            new_scope = etree.SubElement(new_dependency, _qn("scope"))
            new_scope.text = scope
        # 保存 pom.xml
        etree.indent(tree, space="    ")
        tree.write(pom_file_path, encoding='utf-8', xml_declaration=True, pretty_print=True)
        print(f"依赖 {group_id}:{artifact_id}:{version} 添加成功")
    except Exception as e:
        print(f"错误消息{type(e).__name__}: {e}")
    # return_code, stderr = _run_mvn_command("mvn clean install", path=str(project_path))
    # if return_code != 0:
    #     return f"项目构建失败，错误信息: {stderr}"
    # return f"依赖 {group_id}:{artifact_id}:{version} 添加成功"


def get_jar_by_maven(group_ids: list[str] | None = None, artifact_ids: list[str] | None = None):
    project_path = Path("../tmp/e3b3678d-ed09-4e28-aa7f-b76bad190c72")
    group_ids = set(group_ids or [])
    artifact_ids = set(artifact_ids or [])
    group_ids_str = ",".join(group_ids)
    artifact_ids_str = ",".join(artifact_ids)
    # 构建命令
    command = "mvn dependency:build-classpath"
    if group_ids_str:
        command += f' -DincludeGroupIds={group_ids_str}'
    if artifact_ids_str:
        command += f' -DincludeArtifactIds={artifact_ids_str}'
    print(command)
    # 执行命令
    return_code, out = _run_mvn_command(command, path=str(project_path))
    if return_code != 0:
        print(f"获取依赖jar包路径失败，错误信息: {out}")
    # 解析输出
    print(out)
    if dependency := re.search(r"\[INFO] Dependencies classpath:[ \t]*\r?\n([\s\S]*?)(?=\r?\n\[INFO])", out):
        dependency_str = dependency.group(1)
        # print(dependency_str)
        dependency_list = dependency_str.split(os.pathsep)
        dependencies_paths = []
        for dependency in dependency_list:
            try:
                dependencies_paths.append(Path(dependency).relative_to(Path(Path.home() / ".m2/repository")).as_posix())
            except ValueError:
                # 不在路径内，直接添加
                dependencies_paths.append(Path(dependency).as_posix())

        print(f"依赖jar包路径列表: {'\n'.join(dependencies_paths)}")


def view_java_class_by_jar(
        jar_path: str, *, keyword: str | None = None,
        ignore_case: bool = True, inner_class: bool = False
):
    jar_absolute_path = Path(Path.home() / ".m2/repository") / jar_path
    if not (jar_absolute_path.exists() or jar_absolute_path.is_file()):
        print(f"jar包路径不存在: {jar_path}")
        return
    print(jar_absolute_path)
    return_code, jar_class_out_str = _run_java_command(f"jar tf {jar_absolute_path.name}",
                                                       path=str(jar_absolute_path.parent))
    if return_code != 0:
        print(f"查看jar包类名失败，错误信息: {jar_class_out_str}")
        return
    default_reg = rf"{'(?<![\w$/])[\w/]+' if not inner_class else '.*?'}\.class"
    print(default_reg)
    jar_class_str_list = re.findall(default_reg, jar_class_out_str, flags=re.IGNORECASE if ignore_case else 0)
    if ignore_case:
        comp_fun = lambda name: keyword.lower() in name.lower()
    else:
        comp_fun = lambda name: keyword in name
    if keyword:
        jar_class_str_list = [c.replace("/", ".") for c in jar_class_str_list if comp_fun(c.rsplit("/", 1)[-1])]
    if not jar_class_str_list:
        print(f"指定jar包下未找到相关的任何类名")
        return
    print(f"指定jar包下的类名为: \n{'\n'.join(jar_class_str_list)}")


# def view_java_api_roughly_info(
#         jar_path: str, class_name: str,
#         method_name: str | None = None, ignore_case: bool = True,
#         exclude_type: Literal["method", "variable"] | None = None,
# ):
#     jar_absolute_path = Path(Path.home() / ".m2/repository") / jar_path
#     if not (jar_absolute_path.exists() or jar_absolute_path.is_file()):
#         print(f"jar包路径不存在: {jar_path}")
#         return
#     # 构建命令
#     command = f"javap -classpath {jar_absolute_path.name} -p {class_name}"
#     return_code, javap_out_str = _run_java_command(command, path=str(jar_absolute_path.parent))
#     if return_code != 0:
#         print(f"查看类大致信息失败，错误信息: {javap_out_str}")
#         return
#     # 清洗
#     raw_lines = [ln.rstrip() for ln in javap_out_str.splitlines()]
#     lines = [ln.strip() for ln in raw_lines if ln.strip()]
#     if lines and lines[0].startswith("Compiled from"):
#         lines = lines[1:]
#     if not lines:
#         print("解析失败：javap 输出为空")
#         return
#     # 类声明行（末尾可能带 '{'，去掉）
#     class_decl = lines[0].rstrip("{").strip()
#     print(class_decl)
#     member_lines = lines[1:]
#     print(member_lines)
#
#     # 逐行提取
#     members: list[str] = []
#     brace_depth = 0
#     for ln in member_lines:
#         brace_depth += ln.count("{") - ln.count("}")
#         # 跳过内部类声明 -> 虽然说实测没有
#         if brace_depth > 0:
#             continue
#         if ln == "}" or ln.startswith("static {"):
#             continue
#         if ln.endswith(";"):
#             members.append(ln[:-1].strip())
#
#     # 提取成员名
#     def _member_name(decl: str) -> str | None:
#         if "(" in decl:  # 方法/构造器
#             before = decl[: decl.index("(")]
#             m = re.search(r"([A-Za-z_$][\w$]*)\s*$", before)
#             return m.group(1) if m else None
#         # 字段：取最后一个词（泛型里不会带空格，最后一个 token 就是字段名）
#         parts = decl.split()
#         return parts[-1] if parts else None
#
#     # 按需过滤
#     def _keep(decl: str) -> bool:
#         is_method = "(" in decl
#         if exclude_type == "method" and is_method:
#             return False
#         if exclude_type == "variable" and not is_method:
#             return False
#         if method_name:
#             name = _member_name(decl)
#             if name is None:
#                 return False
#             if ignore_case:
#                 return name.lower() in method_name.lower()
#             else:
#                 return name in method_name
#         return True
#
#     filtered = [m for m in members if _keep(m)]
#     # 组装
#     out_lines = [f"{class_decl}:"]
#     for ln in filtered:
#         out_lines.append(f"\t{ln};")
#     print("\n".join(out_lines))


def view_java_api_code(
        jar_path: str, class_name: str,
        method_name: str | None = None, ignore_case: bool = True
):
    # 先进行检查对应的 tmp 目录有没有反编译的类文件
    class_path = class_name.replace(".", "/") + ".java"
    class_absolute_path = ROOT_BASE_DIR_PATH / "tmp" / class_path

    def match(text: str, target: str) -> bool:
        return target.lower() in text.lower() if ignore_case else target in text

    class_stack: list[tuple[str, int]] = []  # (类名, 进入类体后的括号深度)
    brace_depth = 0  # 当前大括号嵌套深度
    in_method = False
    method_close_depth = 0  # 方法体闭合时要回落到的深度
    method_lines: list[str] = []
    blocks: list[str] = []

    class_pattern = re.compile(r'\b(?:class|interface|enum)\s+(\w+)')

    if class_absolute_path.exists() and class_absolute_path.is_file():
        with open(class_absolute_path, "r", encoding="utf-8") as f:
            code = f.read()
        # print(code)
        for raw_line in code.splitlines():
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

        if blocks:
            # for block in blocks:
            #     print(block)
            #     print()
            print("\r\n".join(blocks))
        else:
            print(f"# 未在 {class_name} 中找到方法: {method_name}")
        return
    # 没有的话，进行反编译
    jar_absolute_path = Path(Path.home() / ".m2/repository") / jar_path
    if not (jar_absolute_path.exists() or jar_absolute_path.is_file()):
        print(f"jar包路径不存在: {jar_path}")
        return
    # 构建命令
    jar_class_name = class_name.replace(".", "\\.")
    command = (f'java -jar "{ROOT_BASE_DIR_PATH / "Tools" / "jar" / "cfr-0.152.jar"}" '
               f'{jar_absolute_path.name} --jarfilter ^{jar_class_name}$ '
               f'--outputdir "{ROOT_BASE_DIR_PATH / "tmp"}"')
    # 执行命令
    return_code, stderr = _run_java_command(command, path=str(jar_absolute_path.parent))
    if return_code != 0:
        print(f"查看类大致代码失败，错误信息: {stderr}")
        return
    # 清洗代码
    with open(class_absolute_path, "r", encoding="utf-8") as f:
        code = f.read()
    print(code)


if __name__ == '__main__':
    code = """
public class MyApp {
    public static void main(String[] args) {
        System.out.println(\"Hello World!\");
    }
}
    """
    # file_path = "src/main/com/example/MyApp.java"
    # print(write_big_data_app_code(code, file_path))
    # replace_lines(file_path, 3, 4, new_text="\t\tSystem.out.println(\"7k7k\");")
    # print(view_code(file_path, 1, 10))
    initialize_project("flink")
    # add_maven_dependence("org.apache.flink", "flink-clients", "1.16.0")
    # get_jar_by_maven()
    # get_jar_by_maven(["org.apache.flink", "org.apache.flink"], ["flink-clients", "flink-json"])
    # view_java_class_by_jar(
    #     "org/apache/flink/flink-clients/1.18.1/flink-clients-1.18.1.jar",
    #     ignore_case=True,
    #     inner_class=False,
    #     keyword="cluster",
    # )
    # view_java_api_roughly_info(
    #     "org/apache/flink/flink-clients/1.18.1/flink-clients-1.18.1.jar",
    #     "org.apache.flink.client.deployment.ClusterSpecification",
    # )
    # view_java_api_code(
    #     "org/apache/flink/flink-clients/1.18.1/flink-clients-1.18.1.jar",
    #     "org.apache.flink.client.deployment.ClusterSpecification",
    #     method_name="MemoryMB",
    #     ignore_case=True,
    # )
    # with open("../tmp/jar.txt", "a", encoding="utf-8") as f:
    #     f.write("\n123456")
