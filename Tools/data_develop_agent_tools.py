"""
数据开发Agent工具
必须包含：
1. 代码编写
2. 修改代码
3. 查看源码
4. 简单的语法验证
...
"""
import json
import os
import re
import shutil
import time
from pathlib import Path
from typing import TypedDict, NotRequired, Literal
from uuid import uuid4

from langchain_core.messages import ToolMessage
from langchain_core.tools import tool
from langgraph.types import Command
from langgraph.prebuilt import ToolRuntime
from lxml import etree

from Tools import LogSetting
from Tools.registry import register_tool
from Tools.run_command import run_mvn_command, run_java_command
from Tools.share_check_tool import redis_con
from load_config.config import config, ROOT_BASE_DIR_PATH

AGENT_NAME = "DDA"
dda_config = config.get("agent", {}).get(AGENT_NAME, {})
java_maven_dir = dda_config.get("java_maven_lib_dir", "~/.m2/repository")
logger = LogSetting.create(__name__)

if java_maven_dir.startswith("~"):
    java_maven_dir = java_maven_dir[2:]
    java_maven_path = Path.home() / java_maven_dir
else:
    java_maven_path = Path(java_maven_dir)

if not (java_maven_path.exists() and java_maven_path.is_dir()):
    logger.error(f"Java Maven repository directory {java_maven_path} does not exist or is not a directory")
    raise FileNotFoundError(f"Java Maven repository directory {java_maven_path} does not exist or is not a directory")

if not os.path.exists(ROOT_BASE_DIR_PATH / "tmp_code"):
    os.makedirs(ROOT_BASE_DIR_PATH / "tmp_code")

if not os.path.exists(ROOT_BASE_DIR_PATH / "tmp"):
    os.makedirs(ROOT_BASE_DIR_PATH / "tmp")

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

        <!-- CVA 写 JUnit 4 测试所需 -->
        <dependency>
            <groupId>junit</groupId>
            <artifactId>junit</artifactId>
            <version>4.13.2</version>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-test-utils</artifactId>
            <version>${Flink.version}</version>
            <scope>test</scope>
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
    
    <build>
        <plugins>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-dependency-plugin</artifactId>
                <version>3.9.0</version>
            </plugin>
        </plugins>
    </build>
    
</project>
"""
NS = "http://maven.apache.org/POM/4.0.0"


class MavenInfo(TypedDict):
    group_id: str
    artifact_id: str
    version: str
    scope: NotRequired[str]


class MavenList(TypedDict):
    maven_infos: list[MavenInfo]


def _qn(tag: str) -> str:
    """封装 maven 命名空间"""
    return f"{{{NS}}}{tag}"


# 只允许往代码目录里写文件（flink -> src/main/java/com/fm，spark -> src/main/python）
_CODE_DIRS = {
    "flink": ("src", "main", "java", "com", "fm"),
    "spark": ("src", "main", "python"),
}


def _code_file_path(app_path: Path, file_path: str, app_type: str) -> Path | str:
    """
    拼出代码文件的绝对路径
    路径不是相对项目 / 落到代码目录外（resources、target、其它模块）时，返回错误说明
    """
    code_dir = _CODE_DIRS.get(app_type)
    if code_dir is None:
        return f"未知的应用类型: {app_type}"
    rel = Path(file_path.strip())
    if not file_path.strip() or rel.is_absolute() or rel.root or rel.drive:
        return f"file_path 只能是相对项目的路径，如：{'/'.join(code_dir)}/MyApp.java"
    target = (app_path / rel).resolve()
    if not target.is_relative_to((app_path / Path(*code_dir)).resolve()):
        return f"file_path 必须落在项目内的 {'/'.join(code_dir)} 目录下（收到的是 {file_path}）"
    return target


# javac 报错行长这样："MyApp.java:12: 错误: 找不到符号"（英文环境是 "error:"）
_JAVAC_ERROR_LINE = re.compile(r":\d+:\s*(?:错误|error)", re.IGNORECASE)


def _pick_javac_errors(out: str, keep_after: int = 4, max_chars: int = 2000) -> str:
    """
    只挑 javac 的报错段落（报错行 + 紧随其后的源码、^、符号/位置），
    把批注处理的「注:」和结尾统计这些噪音去掉 —— 只给模型看关键的地方
    """
    lines = out.splitlines()
    heads = [i for i, line in enumerate(lines) if _JAVAC_ERROR_LINE.search(line)]
    if not heads:  # 没匹配到（比如报的是选项错误）就不裁，宁可多给也别把真正的原因吞掉
        return out.strip()[:max_chars]
    kept, covered = [], -1
    for i in heads:
        if i <= covered:
            continue
        kept.extend(lines[i:i + keep_after])
        covered = i + keep_after - 1
    return "\n".join(kept).strip()[:max_chars]


def _javac_classpath(app_path: Path) -> tuple[str, str]:
    """
    取本项目的依赖 jar 清单（给 javac -cp 用）

    清单放 target/ 下：mvn clean 会连它一起删，所以依赖一变（add/change/remove_maven_dependence
    跑的都是 mvn clean install）下次进来就会自动重建，不用单独维护失效
    :return: (清单, 拿不到的原因)；清单为空表示没拿到，调用方要按"本次未加载依赖"处理
    """
    cp_file = app_path / "target" / "classpath.txt"
    if not cp_file.exists():
        cp_file.parent.mkdir(parents=True, exist_ok=True)
        return_code, out = run_mvn_command(
            "mvn -q dependency:build-classpath -Dmdep.outputFile=target/classpath.txt",
            path=str(app_path)
        )
        if return_code != 0 or not cp_file.exists():
            return "", (out or "mvn 没有跑起来").strip()[:200]
    return cp_file.read_text(encoding="utf-8").strip(), ""


def _remove_dir_error_handler(_, path, exc):
    logger.error(f"删除目录 {path} 失败: {exc}")


@register_tool(AGENT_NAME)
@tool
def initialize_project(runtime: ToolRuntime, project_name: str | None = None) -> Command | str:
    """
    初始化项目
    参数： project_name: 项目名称(可选, 若不填，则系统自动处理)
    返回： 对应的相对项目路径，未来的填写、修改都要依靠这个路径
    注意： 若初始化项目失败，系统会自动清除项目目录；
          同时，若项目已存在，系统会自动进行重构，但这并不会影响后续的代码编写、更改等操作，
          因为系统要的是相对项目路径，其中并不包括项目名称。
    """
    app_type = runtime.context.app_type
    # 默认情况下为 uuid
    uid = runtime.state.get("project_name")
    is_repair = runtime.state.get("is_repair", False)
    if is_repair:
        return "项目需要进行修改，不用进行初始化"
    date = runtime.context.date
    p_name = project_name or uid
    app_path = ROOT_BASE_DIR_PATH / "tmp_code" / app_type / date / p_name
    # 如果存在，那么系统将会进行重构
    if os.path.exists(app_path):
        p_name = f"{app_type}-{time.time()}-{uuid4()}-{p_name}"
        app_path = ROOT_BASE_DIR_PATH / "tmp_code" / app_type / date / p_name
    os.makedirs(app_path)
    try:
        if app_type == "flink":
            # 创建 pom
            (app_path / "pom.xml").write_text(
                _POM_TEXT.strip().replace("__PROJECT_NAME__", p_name),
                encoding='utf-8'
            )
            # 创建src/...
            # 先构建写代码的目录
            (app_path / "src" / "main" / "java" / "com" / "fm").mkdir(parents=True, exist_ok=True)
            # 构建对应的资源文件夹
            (app_path / "src" / "main" / "resources").mkdir(parents=True, exist_ok=True)
            # 构建对应的测试目录
            (app_path / "src" / "test" / "java").mkdir(parents=True, exist_ok=True)
            return_code, stderr = run_mvn_command("mvn clean install", path=str(app_path))
            if return_code != 0:
                logger.error(f"项目构建失败，错误信息: {stderr}")
                # 清空项目
                app_path.rmdir()
                return f"项目构建失败(已自动清除项目目录)，错误信息: {stderr}"
            return Command(
                update={
                    "messages": [
                        ToolMessage(
                            content="项目构建完成，相对项目路径为：src/main/java/com/fm, 之后的代码请在这里进行编写",
                            tool_call_id=runtime.tool_call_id,
                            id=f"tool-{uuid4()}"
                        )
                    ],
                    "project_name": p_name
                }
            )
        elif app_type == "spark":
            # 对于 spark 不用 java 与 pom ，只用 python 代码，所以直接创建对应的目录即可
            (app_path / "src" / "main" / "python").mkdir(parents=True, exist_ok=True)
            (app_path / "src" / "main" / "resources").mkdir(parents=True, exist_ok=True)
            (app_path / "src" / "tests").mkdir(parents=True, exist_ok=True)
            return Command(
                update={
                    "messages": [
                        ToolMessage(
                            content=f"项目构建完成，相对项目路径为：src/main/python，之后的代码请在这里进行编写",
                            tool_call_id=runtime.tool_call_id,
                            id=f"tool-{uuid4()}"
                        )
                    ],
                    "project_name": p_name,
                }
            )
    except Exception as e:
        logger.error(f"初始化项目失败{type(e).__name__}: {e}")
        # 强制清空项目
        shutil.rmtree(app_path, onexc=_remove_dir_error_handler)
        return f"初始化项目失败(已自动清除项目目录){type(e).__name__}: {e}"


@register_tool(AGENT_NAME)
@tool
def add_maven_dependence(runtime: ToolRuntime, maven_list: MavenList) -> str:
    """
    在原有的基础上一次性添加 一种或多种 maven 依赖
    参数：
        maven_list: maven 依赖列表
        其中:
            - maven_infos: maven 依赖信息列表
            - 每个依赖信息包含:
                - group_id: 依赖的 groupId
                - artifact_id: 依赖的 artifactId
                - version: 依赖的版本号
                - scope: maven 依赖的范围(可选, 若不填,则默认为 "compile")
    注意:
        - 若依赖已存在, 则跳过添加
        - 若添加失败, 则会自动回滚到上一次的状态
    """
    if not maven_list.get("maven_infos"):
        return "maven_infos 不能为空"
    maven_infos = maven_list.get("maven_infos")

    app_type = runtime.context.app_type
    p_name = runtime.state.get("project_name")
    date = runtime.context.date
    project_path = ROOT_BASE_DIR_PATH / "tmp_code" / app_type / date / p_name
    pom_file_path = project_path / "pom.xml"
    # 安全处理
    # 将原本的 pom.xml 文件改为 pom.xml.bak 备份文件，并且 将原本的 pom.xml 复制一份
    back_pom_file_path = project_path / "pom.xml.bak"
    shutil.copy2(pom_file_path, back_pom_file_path)
    try:
        tree = etree.parse(pom_file_path)
        pom_root = tree.getroot()
        dependencies = pom_root.find(_qn("dependencies"))
        already_have_dependency = dependencies.findall(_qn("dependency"))
        maven_filter_list = []
        # 为了降低时间复杂度, 这里将 already_have_dependency 转换为 dict 字典
        already_have_info_dict = {}  # -> groupId:artifactId -> version
        filtered_maven_infos = {}
        for dependency in already_have_dependency or []:
            version = dependency.findtext(_qn("version"))
            group_id = dependency.findtext(_qn("groupId"))
            artifact_id = dependency.findtext(_qn("artifactId"))
            # 处理版本号占位符
            if version:
                if version_placeholder := re.match(r"\$\{(.*?)}", version).group(1):
                    # 版本号是占位符, 进行处理
                    properties = pom_root.find(_qn("properties"))
                    version = properties.findtext(_qn(f"{version_placeholder}"))
                    if not version:
                        logger.warning(f"版本号 {version_placeholder} 未定义")
                        version = "undefined"
                    else:
                        version = version.strip()
                else:
                    version = "undefined"
            already_have_info_dict[f"{group_id}:{artifact_id}"] = version or "undefined"
        # 过滤已存在的依赖, 只添加不存在的依赖
        for maven_info in maven_infos:
            group_id = maven_info.get("group_id")
            artifact_id = maven_info.get("artifact_id")
            if corresponding_version := already_have_info_dict.get(f"{group_id}:{artifact_id}"):
                logger.warning(f"依赖 {group_id}:{artifact_id} 已存在, 对应的版本为 {corresponding_version}, 跳过")
                filtered_maven_infos[f"{group_id}:{artifact_id}"] = corresponding_version
                continue
            maven_filter_list.append(maven_info)
        # 添加依赖标签
        for maven_info in maven_filter_list:
            group_id = maven_info.get("group_id")
            artifact_id = maven_info.get("artifact_id")
            version = maven_info.get("version")
            scope = maven_info.get("scope")

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
    except Exception as e:
        logger.error(f"添加依赖失败{type(e).__name__}: {e}")
        # 进行回滚操作
        os.replace(back_pom_file_path, pom_file_path)
        return f"添加依赖失败(已经自动回滚){type(e).__name__}: {e}"
    return_code, stderr = run_mvn_command("mvn clean install", path=str(project_path))
    if return_code != 0:
        logger.error(f"项目构建失败，错误信息: {stderr}")
        # 进行回滚操作
        os.replace(back_pom_file_path, pom_file_path)
        return f"项目构建失败(已经自动回滚)，错误信息: {stderr}"
    # 恢复 pom.xml 文件 -> 直接删除 pom.xml.bak 文件
    os.remove(back_pom_file_path)
    filtered_maven_text = ""
    for filtered_maven_key, filtered_maven_version in filtered_maven_infos.items():
        filtered_maven_text += f"\n{filtered_maven_key} -> {filtered_maven_version}\n"
    return f"依赖添加并构建成功{', 被过滤的依赖如下：' if filtered_maven_text else ', 无被过滤依赖'}"


@register_tool(AGENT_NAME)
@tool
def change_maven_dependence_version(runtime: ToolRuntime, maven_list: MavenList) -> str:
    """
    一次性修改 一个或多个 已有的 Maven 依赖的版本号
    参数：
        maven_list: maven 依赖列表
        其中:
            - maven_infos: maven 依赖信息列表
            - 每个依赖信息包含:
                - group_id: 依赖的 groupId
                - artifact_id: 依赖的 artifactId
                - version: 即将要替换为的版本号
    注意：
        - version 不能为空，若为空则表示不修改版本号
        - scope 会在这里自动忽略，所以不用填这个参数，系统将会自动忽略
        - 当 group_id 和 artifact_id 同时满足时，才会进行版本号替换
        - 若这个group_id与artifact_id匹配了，但是version为空，那么将会替换为这个添加指定的版本号
        - 若更改失败, 则会自动回滚到上一次的状态
    """
    if not maven_list.get("maven_infos"):
        return "maven_infos 不能为空"
    maven_infos = maven_list.get("maven_infos")

    app_type = runtime.context.app_type
    p_name = runtime.state.get("project_name")
    date = runtime.context.date
    project_path = ROOT_BASE_DIR_PATH / "tmp_code" / app_type / date / p_name
    pom_file_path = project_path / "pom.xml"
    # 安全处理
    # 将原本的 pom.xml 文件改为 pom.xml.bak 备份文件，并且 将原本的 pom.xml 复制一份
    back_pom_file_path = project_path / "pom.xml.bak"
    shutil.copy2(pom_file_path, back_pom_file_path)
    try:
        maven_update_version_dict = {}
        for maven_info in maven_infos:
            group_id = maven_info.get("group_id")
            artifact_id = maven_info.get("artifact_id")
            version = maven_info.get("version")
            maven_update_version_dict[f"{group_id}:{artifact_id}"] = version
        # 读取 pom.xml 文件
        tree = etree.parse(pom_file_path)
        root = tree.getroot()
        dependencies = root.find(_qn("dependencies"))
        for dependency in dependencies or []:
            group_id = dependency.findtext(_qn("groupId"))
            artifact_id = dependency.findtext(_qn("artifactId"))
            if new_version := maven_update_version_dict.get(f"{group_id}:{artifact_id}"):
                old_version = dependency.find(_qn("version"))
                if not old_version:
                    old_version = etree.SubElement(dependency, _qn("version"))
                old_version.text = new_version
        # 保存 pom.xml
        etree.indent(tree, space="    ")
        tree.write(pom_file_path, encoding='utf-8', xml_declaration=True, pretty_print=True)
    except Exception as e:
        logger.error(f"修改依赖版本号失败{type(e).__name__}: {e}")
        # 进行回滚操作
        os.replace(back_pom_file_path, pom_file_path)
        return f"修改依赖版本号失败(已经自动回滚){type(e).__name__}: {e}"
    # 重新构建项目
    return_code, stderr = run_mvn_command("mvn clean install", path=str(project_path))
    if return_code != 0:
        logger.error(f"项目构建失败，错误信息: {stderr}")
        # 进行回滚操作
        os.replace(back_pom_file_path, pom_file_path)
        return f"项目构建失败(已经自动回滚)，错误信息: {stderr}"
    # 恢复 pom.xml 文件 -> 直接删除 pom.xml.bak 文件
    os.remove(back_pom_file_path)
    return f"依赖版本号修改成功"


@register_tool(AGENT_NAME)
@tool
def remove_maven_dependence(runtime: ToolRuntime, maven_list: MavenList) -> str:
    """
    一次性移除 一个或多个 已有的 Maven 依赖
    参数：
        maven_list: maven 依赖列表
        其中:
            - maven_infos: maven 依赖信息列表
            - 每个依赖信息包含:
                - group_id: 依赖的 groupId
                - artifact_id: 依赖的 artifactId
    注意：
        - 当 group_id 和 artifact_id 同时满足时，才会进行移除
        - 本次的 version 与 scope 会自动忽略
        - 若移除失败, 则会自动回滚到上一次的状态
    """
    if not maven_list.get("maven_infos"):
        return "maven_infos 不能为空"
    maven_infos = maven_list.get("maven_infos")

    app_type = runtime.context.app_type
    p_name = runtime.state.get("project_name")
    date = runtime.context.date
    project_path = ROOT_BASE_DIR_PATH / "tmp_code" / app_type / date / p_name
    pom_file_path = project_path / "pom.xml"
    # 安全处理
    # 将原本的 pom.xml 文件改为 pom.xml.bak 备份文件，并且 将原本的 pom.xml 复制一份
    back_pom_file_path = project_path / "pom.xml.bak"
    shutil.copy2(pom_file_path, back_pom_file_path)
    maven_delete_version_set = set()
    try:
        for maven_info in maven_infos:
            group_id = maven_info.get("group_id")
            artifact_id = maven_info.get("artifact_id")
            maven_delete_version_set.add(f"{group_id}:{artifact_id}")
        # 读取 pom.xml 文件
        tree = etree.parse(pom_file_path)
        root = tree.getroot()
        dependencies = root.find(_qn("dependencies"))
        for dependency in dependencies or []:
            group_id = dependency.findtext(_qn("groupId"))
            artifact_id = dependency.findtext(_qn("artifactId"))
            if f"{group_id}:{artifact_id}" in maven_delete_version_set:
                dependencies.remove(dependency)
        # 保存 pom.xml
        etree.indent(tree, space="    ")
        tree.write(pom_file_path, encoding='utf-8', xml_declaration=True, pretty_print=True)
    except Exception as e:
        logger.error(f"移除依赖失败{type(e).__name__}: {e}")
        # 进行回滚操作
        os.replace(back_pom_file_path, pom_file_path)
        return f"移除依赖失败(已经自动回滚){type(e).__name__}: {e}"
    # 重新构建项目
    return_code, stderr = run_mvn_command("mvn clean install", path=str(project_path))
    if return_code != 0:
        logger.error(f"项目构建失败，错误信息: {stderr}")
        # 进行回滚操作
        os.replace(back_pom_file_path, pom_file_path)
        return f"项目构建失败(已经自动回滚)，错误信息: {stderr}"
    # 恢复 pom.xml 文件 -> 直接删除 pom.xml.bak 文件
    os.remove(back_pom_file_path)
    return f"依赖移除成功"


@register_tool(AGENT_NAME)
@tool
def view_java_api_code(
        jar_path: str, class_name: str,
        method_name: str | None = None, ignore_case: bool = True
) -> str:
    """
    查看 jar包 中某个 java类的 大致代码
    参数：
        - jar_path: 相对 Maven仓库 的jar包路径
        - class_name: 类名（必须是来自 view_java_class_by_jar 工具的类名，即不带.class后缀的类名）
        - method_name: 方法名（默认不进行匹配，即查看所有方法）（支持模糊匹配）
        - ignore_case: 是否忽略大小写匹配（默认忽略大小写匹配）(True->开启, False->关闭)
    返回：该类反编译后的代码
    """
    # 先进行检查对应的 tmp 目录有没有反编译的类文件
    class_path = class_name.replace(".", "/") + ".class"
    class_absolute_path = ROOT_BASE_DIR_PATH / "tmp" / class_path

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

    if class_absolute_path.exists() and class_absolute_path.is_file():
        with open(class_absolute_path, "r", encoding="utf-8") as f:
            code = f.read()
        return _filter_code(code)
    # 没有的话，进行反编译
    jar_absolute_path = java_maven_path / jar_path
    if not (jar_absolute_path.exists() or jar_absolute_path.is_file()):
        return f"jar包路径不存在: {jar_path}"
    # 构建命令
    class_name = class_name.replace(".", "\\.")
    command = (f'java -jar "{ROOT_BASE_DIR_PATH / "Tools" / "jar" / "cfr-0.152.jar"}" '
               f'{jar_absolute_path.name} --jarfilter ^{class_name}$ '
               f'--outputdir "{ROOT_BASE_DIR_PATH / "tmp"}"')
    # 执行命令
    return_code, stderr = run_java_command(command, path=str(jar_absolute_path.parent))
    if return_code != 0:
        return f"查看类大致代码失败，错误信息: {stderr}"
    # 清洗代码
    with open(class_absolute_path, "r", encoding="utf-8") as f:
        code = f.read()
    return _filter_code(code)


@register_tool(AGENT_NAME)
@tool
def view_java_api_roughly_info(
        jar_path: str, class_name: str,
        method_name: str | None = None, ignore_case: bool = True,
        exclude_type: Literal["method", "variable"] | None = None,
) -> str:
    """
    查看 jar包 中某个 java类的 大致信息
    即 该类中的 类方法、类方法参数、类变量 等信息。
    参数：
        - jar_path: 相对 Maven仓库 的jar包路径
        - class_name: 类名（必须是来自 view_java_class_by_jar 工具的类名，即不带.class后缀的类名）
        - method_name: 方法名（默认不进行匹配，即查看所有方法）（支持模糊匹配）
        - ignore_case: 是否忽略大小写匹配（默认忽略大小写匹配）(True->开启, False->关闭)
        - exclude_type: 排除的类型（默认不进行任何排除）（method->排除方法, variable->排除变量）
    注意：
        - 若 method_name 为空，则 ignore_case 无效
    返回：类名信息，与对应的类方法、类方法参数、类变量 等信息
        如：
            public class com.fm.Main:
                public static void main(String[] args);
                public void sort(int[] arr);
        若 假设 method_name 为 "sort"，则返回：
            public class com.fm.Main:
                public void sort(int[] arr);
    """
    jar_absolute_path = java_maven_path / jar_path
    if not (jar_absolute_path.exists() or jar_absolute_path.is_file()):
        return f"jar包路径不存在: {jar_path}"
    # 构建命令
    command = f"javap -classpath {jar_absolute_path.name} -p {class_name}"
    return_code, javap_out_str = run_java_command(command, path=str(jar_absolute_path.parent))
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


@register_tool(AGENT_NAME)
@tool
def view_java_class_by_jar(
        jar_path: str, keyword: str | None = None,
        ignore_case: bool = True, inner_class: bool = False
) -> str:
    """
    查看某个 jar包 的class类名
    参数：
        jar_path: 相对 Maven仓库 的jar包路径
        keyword: 匹配的类名关键词（默认不进行匹配，即查看所有类名）
        ignore_case: 是否忽略大小写匹配（默认忽略大小写匹配）(True->开启, False->关闭)
        inner_class: 是否忽略内部类（默认忽略内部类）（True->不忽略内部类, False->忽略内部类）
    注意：
        - 若 match_str 为空，则 enable_cap 无效
        - 不用去猜绝对路径，只需要输入相对 Maven仓库 的jar包路径即可，系统会自动拼接
    返回：
        指定jar包下的所有类名列表
    """
    jar_absolute_path = java_maven_path / jar_path
    if not (jar_absolute_path.exists() or jar_absolute_path.is_file()):
        return f"jar包路径不存在: {jar_path}"
    return_code, jar_class_out_str = run_java_command(f"jar tf {jar_absolute_path}")
    if return_code != 0:
        return f"查看jar包类名失败，错误信息: {jar_class_out_str}"
    default_reg = rf"{'(?<![\w$/])([\w/]+)' if not inner_class else '.*?'}\.class"
    try:
        jar_class_str_list = re.findall(default_reg, jar_class_out_str, flags=re.IGNORECASE if ignore_case else 0)
        if keyword:
            if ignore_case:
                comp_fun = lambda name: keyword.lower() in name.lower()
            else:
                comp_fun = lambda name: keyword in name
            jar_class_str_list = [c.replace("/", ".") for c in jar_class_str_list if comp_fun(c.rsplit("/", 1)[-1])]
        if not jar_class_str_list:
            return f"指定jar包下未找到相关的任何类名"
        return f"指定jar包下的类名为: {','.join(jar_class_str_list)}"
    except Exception as e:
        logger.error(f"查看jar包类名失败{type(e).__name__}: {e}")
        return f"查看jar包类名失败{type(e).__name__}: {e}"


@register_tool(AGENT_NAME)
@tool
def get_jar_by_maven(runtime: ToolRuntime, maven_list: MavenList | None = None) -> str:
    """
    获取 maven项目 中指定或所有依赖 对应的jar包路径
    参数：
        maven_list: maven 依赖列表
        其中:
            - maven_infos: maven 依赖信息列表
            - 每个依赖信息包含:
                - group_id: 依赖的 groupId
                - artifact_id: 依赖的 artifactId
    注意：
        - maven_infos 中的 version 与 scope 会自动忽略
    返回：
        相对 Maven仓库 的jar包路径列表
    """
    maven_list = maven_list or {"maven_infos": []}
    maven_infos = maven_list.get("maven_infos", [])
    app_type = runtime.context.app_type
    p_name = runtime.state.get("project_name")
    date = runtime.context.date
    project_path = ROOT_BASE_DIR_PATH / "tmp_code" / app_type / date / p_name
    group_ids = set(maven_info.get("group_id") for maven_info in maven_infos)
    artifact_ids = set(maven_info.get("artifact_id") for maven_info in maven_infos)
    group_ids_str = ",".join(group_ids)
    artifact_ids_str = ",".join(artifact_ids)
    # 构建命令
    command = "mvn dependency:build-classpath"
    if group_ids_str:
        command += f' -DincludeGroupIds={group_ids_str}'
    if artifact_ids_str:
        command += f' -DincludeArtifactIds={artifact_ids_str}'
    # 执行命令
    return_code, out = run_mvn_command(command, path=str(project_path))
    if return_code != 0:
        logger.error(f"获取依赖jar包路径失败，错误信息: {out}")
        return f"获取依赖jar包路径失败，错误信息: {out}"
    # 解析输出
    if dependency := re.search(r"\[INFO] Dependencies classpath:[ \t]*\r?\n([\s\S]*?)(?=\r?\n\[INFO])", out):
        dependency_str = dependency.group(1)
        dependency_list = dependency_str.split(os.pathsep)
        dependencies_paths = []
        for dependency in dependency_list:
            try:
                dependencies_paths.append(Path(dependency).relative_to(java_maven_path).as_posix())
            except ValueError:
                # 不在路径内，直接添加
                dependencies_paths.append(Path(dependency).as_posix())

        return f"依赖jar包路径列表: {'\n'.join(dependencies_paths)}"
    return "没有找到依赖jar包路径"


@register_tool(AGENT_NAME)
@tool
def write_big_data_app_code(runtime: ToolRuntime, code: str, file_path: str) -> str:
    """
    书写大数据应用的代码 到指定文件
    参数:
        code: 代码内容
        file_path: 文件路径，如：src/main/com/example/MyApp.java
        注意：文件路径不能是绝对路径（因为系统会自动管理文件路径），只能是相对于 项目的路径，如：src/main/com/example/MyApp.java
    返回:
        写入结果
    """
    app_type = runtime.context.app_type
    p_name = runtime.state.get("project_name")
    date = runtime.context.date
    tmp_code = code.strip()
    if not tmp_code:
        return "代码不能为空"
    # 构建应用的主目录
    app_path = ROOT_BASE_DIR_PATH / "tmp_code" / app_type / date / p_name
    # 进行限制判断
    if app_type == "flink" and not file_path.endswith(".java"):
        return "flink应用只能用java代码来写"
    if app_type == "spark" and not file_path.endswith(".py"):
        return "spark应用只能用python代码来写"
    try:
        # 先进行主存放目录的创建，再进行文件的写入
        if not os.path.exists(app_path):
            return "项目未初始化，请先调用工具 initialize_project 进行初始化"
        app_file_path = _code_file_path(app_path, file_path, app_type)
        if isinstance(app_file_path, str):
            return app_file_path
        app_file_path.parent.mkdir(parents=True, exist_ok=True)
        app_file_path.write_text(code, encoding='utf-8')
        return f"文件 {file_path} 写入成功"
    except Exception as e:
        logger.error(f"创建目录或者写入文件失败{type(e).__name__}: {e}")
        return f"创建目录或者写入文件失败{type(e).__name__}: {e}"


@register_tool(AGENT_NAME)
@tool
def change_big_data_app_code(
        runtime: ToolRuntime, new_code: str,
        file_path: str, start_idx: int,
        end_idx: int | None = None
) -> str:
    """
    修改指定区间的大数据应用代码
    参数:
        new_code: 代码内容
        file_path: 文件路径，如：src/main/com/example/MyApp.java
        start_idx: 起始索引（从1开始计数，1 表示首行）
        end_idx: 结束索引（从1开始计数，1 表示首行）（默认值为None）
        注意：
            - 文件路径不能是绝对路径（因为系统会自动管理文件路径），只能是相对于 项目的路径，如：src/main/com/example/MyApp.java
            - 其中的起始索引与结束索引表示一个区间，起始索引是闭区间, 结束索引是开区间。即：[start_idx, end_idx)
            - 同时，允许出现 new_code 的行数不等于原始代码的行数的情况。即，可比原始区间更长或更短。
            - 若 end_idx 为 None，则默认只替换 start_idx 行的代码
            - 若你不知道对应的区间的代码是怎么样的，你可以先尝试使用 view_app_interval_code 查看对应的代码
    返回:
        修改结果
    """
    app_type = runtime.context.app_type
    p_name = runtime.state.get("project_name")
    date = runtime.context.date

    # 构建应用的主目录
    app_path = ROOT_BASE_DIR_PATH / "tmp_code" / app_type / date / p_name
    app_file_path = _code_file_path(app_path, file_path, app_type)
    if isinstance(app_file_path, str):
        return app_file_path
    if not app_file_path.exists():
        return f"文件 {file_path} 不存在, 请尝试相对项目的路径写法，如：src/main/com/example/MyApp.java"
    # 进行限制判断
    if app_type == "flink" and not file_path.endswith(".java"):
        return "flink应用只能用java代码来写"
    if app_type == "spark" and not file_path.endswith(".py"):
        return "spark应用只能用python代码来写"
    if start_idx <= 0:
        logger.warning(f"Agent填入了无效的起始索引 start_idx: {start_idx}，已经自动调整为1")
        start_idx = 1
    if (end_idx is None) or (start_idx == end_idx):
        end_idx = start_idx + 1
    if start_idx > end_idx:
        logger.error("不允许出现 start_idx 大于 end_idx 的情况")
        return f"不允许出现 start_idx 大于 end_idx 的情况. start_idx: {start_idx}, end_idx: {end_idx}"
    # 进行备份修改
    back_file_path = app_file_path.with_suffix(".bak")
    shutil.copy2(app_file_path, back_file_path)
    try:
        with open(app_file_path, "r", encoding='utf-8', newline='') as f:
            old_code = f.readlines()
        # 替换代码进行转换为列表
        if isinstance(new_code, str):
            new_lines = new_code.splitlines()
        else:
            new_lines = list(new_code)

        old_code[start_idx - 1:end_idx - 1] = new_lines
        for i, line in enumerate(old_code[:-1]):  # 只有文件最后一行可以不带换行
            if not line.endswith(("\n", "\r")):
                old_code[i] = line + "\n"

        with open(app_file_path, "w", encoding='utf-8', newline='') as f:
            f.writelines(old_code)
        # 清除备份文件
        os.remove(back_file_path)
        return f"文件 {file_path} 修改成功"
    except Exception as e:
        logger.error(f"修改文件 {file_path} 失败{type(e).__name__}: {e}")
        # 回滚
        os.replace(back_file_path, app_file_path)
        return f"修改文件 {file_path} 失败{type(e).__name__}: {e}"


@register_tool(AGENT_NAME)
@tool
def add_big_data_app_code(runtime: ToolRuntime, new_code: str, file_path: str, auto_wrap: bool = True) -> str:
    """
    追加 代码 到 指定文件的 末尾
    参数：
        - new_code： 需要添加的代码文本
        - file_path： 文件路径，如：src/main/com/example/MyApp.java
        - auto_wrap： 是否自动添加换行符（默认值为True）（这里是指在 new_code 前面加上 \n 换行符）
    注意：
        - 系统会自动处理首行的换行符(防止出现首行在尾行开始)，无需手动添加。即：会在 new_code 前面加上 \n 换行符 -> \n{new_code}
        - 若 auto_wrap 为 False，则不会在 new_code 前面加上 \n 换行符
    返回：
        追加是否成功的结果
    """
    app_type = runtime.context.app_type
    p_name = runtime.state.get("project_name")
    date = runtime.context.date

    # 构建应用的主目录
    app_path = ROOT_BASE_DIR_PATH / "tmp_code" / app_type / date / p_name
    app_file_path = _code_file_path(app_path, file_path, app_type)
    if isinstance(app_file_path, str):
        return app_file_path
    if not app_file_path.exists():
        return f"文件 {file_path} 不存在, 请尝试相对项目的路径写法，如：src/main/com/example/MyApp.java"
    # 进行限制判断
    if app_type == "flink" and not file_path.endswith(".java"):
        return "flink应用只能用java代码来写"
    if app_type == "spark" and not file_path.endswith(".py"):
        return "spark应用只能用python代码来写"
    # 进行备份
    back_file_path = app_file_path.with_suffix(".bak")
    shutil.copy2(app_file_path, back_file_path)
    try:
        with open(app_file_path, "a+", encoding='utf-8', newline='') as f:
            f.write(f"{'\n' if auto_wrap else ''}{new_code}")
        # 清除备份文件
        os.remove(back_file_path)
        return f"文件追加成功"
    except Exception as e:
        logger.error(f"追加文件 失败{type(e).__name__}: {e}")
        # 回滚
        os.replace(back_file_path, app_file_path)
        return f"追加文件 失败{type(e).__name__}: {e}"


@register_tool(AGENT_NAME)
@tool
def view_app_interval_code(runtime: ToolRuntime, file_path: str, start_idx: int, end_idx: int | None = None) -> str:
    """
    查看指定区间的大数据应用代码
    参数:
        file_path: 文件路径，如：src/main/com/example/MyApp.java
        start_idx: 起始索引（从1开始计数，1 表示首行）
        end_idx: 结束索引（从1开始计数，1 表示首行）（默认值为None）
    返回:
        查看结果
    """
    app_type = runtime.context.app_type
    p_name = runtime.state.get("project_name")
    date = runtime.context.date
    # 构建应用的主目录
    app_path = ROOT_BASE_DIR_PATH / "tmp_code" / app_type / date / p_name
    if not (app_path / file_path).exists():
        return f"文件 {file_path} 不存在, 请尝试相对项目的路径写法，如：src/main/com/example/MyApp.java"
    if start_idx <= 0:
        logger.warning(f"Agent填入了无效的起始索引 start_idx: {start_idx}，已经自动调整为1")
        start_idx = 1
    if start_idx > end_idx:
        logger.error("不允许出现 start_idx 大于 end_idx 的情况")
        return f"不允许出现 start_idx 大于 end_idx 的情况. start_idx: {start_idx}, end_idx: {end_idx}"
    try:
        print_list = []
        with open((app_path / file_path), "r", encoding='utf-8', newline='') as f:
            code = f.readlines()
        if end_idx is None:
            end_idx = len(code) + 1
        for i, line in enumerate(code, start=1):
            # 筛选出 [start_idx - 1:end_idx - 1] 区间的行
            if start_idx <= i < end_idx:
                print_list.append(f"{i}\t{line}")
        return f"区间[{start_idx}, {end_idx})的代码如下：\n{'\r'.join(print_list)}"
    except Exception as e:
        logger.error(f"查看文件 {file_path} 失败{type(e).__name__}: {e}")
        return f"查看文件 {file_path} 失败{type(e).__name__}: {e}"


@register_tool(AGENT_NAME)
@tool
def verify_big_data_app_code(runtime: ToolRuntime, file_path: str) -> str:
    """
    验证单个大数据应用代码能否编译（java 代码专用）
    参数:
        file_path: 文件路径，如：src/main/java/com/fm/MyApp.java
    返回:
        验证结果
    注意:
        - 依赖清单第一次验证时用 mvn 生成一次，存在 target/classpath.txt，之后直接复用
        - target/ 会被 mvn clean 删掉，所以改过依赖之后会自动重建
        - 拿不到清单时不带依赖编译，返回值会明确说明"程序包不存在"这类错误不算代码问题
    """
    # 拼接路径
    app_type = runtime.context.app_type
    p_name = runtime.state.get("project_name")
    date = runtime.context.date

    app_path = ROOT_BASE_DIR_PATH / "tmp_code" / app_type / date / p_name
    app_file_path = _code_file_path(app_path, file_path, app_type)
    if isinstance(app_file_path, str):
        return app_file_path
    if not app_file_path.exists():
        return f"文件 {file_path} 不存在, 请尝试相对项目的路径写法，如：src/main/com/example/MyApp.java"
    classpath, why = _javac_classpath(app_path)
    classes_path = app_path / "target" / "classes"
    cp = f"{classes_path}{os.pathsep}{classpath}" if classpath else f"{classes_path}"
    res_code, res_out = run_java_command(
        f'javac -encoding UTF-8 -d "{classes_path}" -sourcepath src/main/java -cp "{cp}" {app_file_path.name}',
        path=f"{app_file_path.parent}"
    )
    if res_code != 0:
        if not classpath:
            # 没有依赖清单时，"程序包 xxx 不存在"是工具的盲区，不是代码的问题，别让 Agent 去改 import
            return (f"编译未通过，但本次没有加载依赖（{why}），"
                    f"像'程序包 xxx 不存在'这类错误不算代码问题：\n{_pick_javac_errors(res_out)}")
        return f"语法错误: {_pick_javac_errors(res_out)}"
    if not classpath:
        return f"语法正确（本次未加载依赖，只确认了语法层面）"
    return f"语法正确"


@register_tool(AGENT_NAME)
@tool
def register_submit_command(
        runtime: ToolRuntime, app_name: str,
        main_class: str, parallelism: int = 1,
        yjm: int | None = None, ytm: int | None = None,
        yn: int | None = None, ynm: str | None = None,
        ys: int | None = None, app_params: dict[str, str] | None = None
) -> str:
    """
    注册应用提交命令（只针对于 java flink 应用） -> 当开发的最后必须调用该工具，注册对应的提交命令
    参数：
        app_name: 应用名称（即你在 env.execute 中填写的应用名称）
        main_class: 指定应用运行主类（即有main方法的类名） 如：com.fm.MyApp
        parallelism: 指定应用的并行度（默认值为1）（可选）
        yjm: 指定 flink jobmanager 的内存（可选）
        ytm: 指定 flink taskmanager 的内存（可选）
        yn: 定义 taskmanager 的数量（可选）
        ynm: 指定应用名称（可选）
        ys: 分配的 slot 数量（可选）
        app_params: 应用的自定义参数（可选）
    返回：
        注册结果
    注意：
        - 无需填写对应的jar包，系统会自动处理
        - 关于 app_params 中，对应的 key 为参数名，value 为参数值
            其中的 key 必须写上关于 -- 或者 - 的前缀，如：--jobmanager-memory 、-p 等，否则系统无法感知
    """
    # 采用Redis存储注册信息
    # 以 p_name 为 key, "submit_command" 为 field, json字符串存储命令信息
    # 同样，以 p_name 为 key, "app_type" 为 field, 存储应用类型，值为 flink 或 spark
    # 同样，以 p_name 为 key, "date" 为 field, 存储当前日期
    if parallelism <= 0:
        return f"并行度必须大于0，当前值为{parallelism}"
    app_type = runtime.context.app_type
    p_name = runtime.state.get("project_name")
    date = runtime.context.date
    register_dict = {"c": main_class, "p": parallelism}
    if yjm and yjm > 0:
        register_dict["yjm"] = yjm
    if ytm and ytm > 0:
        register_dict["ytm"] = ytm
    if yn and yn > 0:
        register_dict["yn"] = yn
    if ynm:
        register_dict["ynm"] = ynm
    if ys and ys > 0:
        register_dict["ys"] = ys
    if app_params:
        register_dict["app_params"] = app_params
    if app_params:
        for k in app_params:
            if not str(k).startswith("-"):
                return f"app_params 的 key 必须带 - 或 -- 前缀（如 --days），收到的是 {k!r}"

    # 存储到Redis
    try:
        redis_con.hset(f"DDA:{p_name}", "submit_command", json.dumps(register_dict))
        redis_con.hset(f"DDA:{p_name}", "app_name", app_name)
        redis_con.hset(f"DDA:{p_name}", "app_type", app_type)
        redis_con.hset(f"DDA:{p_name}", "date", date)
    except Exception as e:
        logger.error(f"注册提交命令失败{type(e).__name__}: {e}")
        return f"注册提交命令失败{type(e).__name__}: {e}"
    return f"注册提交命令成功"
