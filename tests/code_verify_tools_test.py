from pathlib import Path
from typing import Literal

from Tools.code_analysis import JavaAnalyzer, PyAnalyzer

_CODE_DIRS = {
    "flink": {
        "test_code": ("src", "test", "java"),
        "business_code": ("src", "main", "java", "com", "fm")
    },
    "spark": {
        "test_code": ("src", "tests"),
        "business_code": ("src", "main", "python")
    }
}


def get_project_list(app_type, p_name):
    tmp_dir = Path("../tmp")
    app_path = tmp_dir / p_name
    if app_type == "flink":
        business_dir = _CODE_DIRS.get(app_type, {}).get("business_code", None)
        if not business_dir:
            return f"未知的应用类型: {app_type}, 或者未记录对应的目录"
        business_path = app_path / Path(*business_dir)
        if not business_path.exists():
            return f"业务代码目录不存在: {business_dir}"
        # 返回相对项目目录下的文件，资源文件与测试文件不算业务代码
        business_files = [str(f.relative_to(app_path)) for f in business_path.iterdir() if f.is_file()]
        business_files.append("pom.xml")
        return business_files


def get_code_file_info(app_type, file_path: str, get_type: Literal["class_hierarchy", "vars"] = "class_hierarchy"):
    """
    获取指定项目代码文件中的 变量、类、函数、方法等信息
    """
    if app_type == "flink":
        analyzer = JavaAnalyzer(file_path)
        print(analyzer.class_hierarchy() if get_type == "class_hierarchy" else analyzer.vars())
    elif app_type == "spark":
        py_analyzer = PyAnalyzer(file_path)
        print(
            py_analyzer.class_hierarchy()
            if get_type == "class_hierarchy"
            # 模块、赋值 + 类、纯类内变量
            else py_analyzer.vars("类", "纯类内变量")
        )


def code_read(
        app_type: Literal["spark", "flink"],
        file_path: str,
        qualname: str,
        start_line: int | None = None,
        end_line: int | None = None
):
    if app_type == "spark":
        py_analyzer = PyAnalyzer(str(file_path))
        got = py_analyzer.code_by_name(qualname)
        # print(got)
        if not got:
            print(f"未找到类、方法或函数: {qualname}")
            return
        qualname_code = got.get("code")
        if start_line is not None:
            start_idx = got.get("lineno")
            got["lineno"] = start_line
            qualname_code_lines = qualname_code.splitlines()
            if end_line is None:
                end_line = start_line
            got["end_lineno"] = end_line
            end_idx = end_line - start_idx + 1
            start_idx = start_line - start_idx
            print(f"{start_idx}, {end_idx}")
            qualname_code_lines = qualname_code_lines[start_idx:end_idx]
            got["code"] = "\n".join(qualname_code_lines)

        print(f"{got}")


def write_test_code(
        file_path: str,
        test_code: str
):
    p_file_path = Path(file_path)
    if isinstance(p_file_path, str):
        print(p_file_path)
        return
    try:
        p_file_path.parent.mkdir(parents=True, exist_ok=True)
        p_file_path.write_text(test_code, encoding='utf-8')
        print(f"文件 {file_path} 写入成功")
    except Exception as e:
        print(f"创建目录或者写入文件失败{type(e).__name__}: {e}")


if __name__ == '__main__':
    # files = get_project_list("flink", "e3b3678d-ed09-4e28-aa7f-b76bad190c72")
    # print(files)
    # r"D:\pycharm\pycharm study\Multi-Agent BigData Platform\tmp\org\apache\flink\client\deployment\ClusterSpecification.java"
    # get_code_file_info(
    #     "spark",
    #     r"D:\pycharm\pycharm study\Multi-Agent BigData Platform\Tools\code_analysis.py",
    #     get_type="vars"
    # )
    # code_read(
    #     "spark",
    #     r"D:\pycharm\pycharm study\Multi-Agent BigData Platform\Tools\code_analysis.py",
    #     "PyAnalyzer",
    #     615,
    #     643
    # )
    write_test_code(
        "../tmp/test_code/abc.py",
        "print('hello world')"
    )
