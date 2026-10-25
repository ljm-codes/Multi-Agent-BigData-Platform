"""
专门的代码解析工具，只支持 python java 代码
"""
import ast
import logging
import os
import re
import sys
from typing import Literal, cast, Optional

# from Tools import LogSetting

FUNC_NODES = (ast.FunctionDef, ast.AsyncFunctionDef)
SCOPE_NODES = FUNC_NODES + (ast.ClassDef,)

# logger = LogSetting.create(__name__)


logger = logging.getLogger(__name__)  # 临时测试用，避免出现 需要其他配置的问题


class VarCollector(ast.NodeVisitor):
    """
    变量收集器，包括函数内部，类体，模块级等
    """

    def __init__(self):
        # 作用域栈，元素是 (kind, name)
        # kind: 'module' / 'class' / 'function'
        self.scope_stack: list[tuple[str, str | None]] = [('module', None)]
        self.vars = []  # (作用域描述, 变量名, 行号, 绑定方式)

    def current_scope(self):
        return self.scope_stack[-1]

    def add_var(self, name, lineno, how):
        kind, scope_name = self.current_scope()

        if kind == 'module':
            scope_desc = '模块级'
        elif kind == 'function':
            scope_desc = f'函数 {scope_name} 内'
        elif kind == 'class':
            scope_desc = f'类 {scope_name} 体'
        else:
            scope_desc = kind

        self.vars.append((scope_desc, name, lineno, how))

    def _add_class_member(self, name, lineno):
        """将变量记录为“纯类内变量”，作用域描述使用“类 xxx 内”"""
        kind, scope_name = self.current_scope()

        if kind == 'class':
            class_name = scope_name
        elif kind == 'function' and len(self.scope_stack) >= 2 and self.scope_stack[-2][0] == 'class':
            # 当前在类的方法中（方法直接定义在类里）
            class_name = self.scope_stack[-2][1]
        else:
            return

        self.vars.append((f'类 {class_name} 内', name, lineno, '纯类内变量'))

    def _collect_class_member_target(self, target):
        """
        根据当前作用域，收集类成员变量：
        - 类体直接定义：收集 Name（如 A、B）
        - 类方法内：收集 self.xxx
        """
        kind, _ = self.current_scope()

        if kind == 'class':
            for name_node in self._iter_store_names(target):
                self._add_class_member(name_node.id, name_node.lineno)
        elif kind == 'function' and len(self.scope_stack) >= 2 and self.scope_stack[-2][0] == 'class':
            for attr_node in self._iter_self_attrs(target):
                self._add_class_member(f'self.{attr_node.attr}', attr_node.lineno)

    def _iter_self_attrs(self, node):
        """从赋值目标中提取所有 self.xxx 的 Attribute 节点"""
        if isinstance(node, ast.Attribute):
            if isinstance(node.value, ast.Name) and node.value.id == 'self':
                if isinstance(node.ctx, ast.Store):
                    yield node
        elif isinstance(node, (ast.Tuple, ast.List)):
            for elt in node.elts:
                yield from self._iter_self_attrs(elt)
        elif isinstance(node, ast.Starred):
            yield from self._iter_self_attrs(node.value)

    # ---------- 作用域进入 / 退出 ----------
    def visit_ClassDef(self, node):
        # 类名绑定在当前作用域
        self.add_var(node.name, node.lineno, 'class定义')

        self.scope_stack.append(('class', node.name))
        self.generic_visit(node)
        self.scope_stack.pop()

    def visit_FunctionDef(self, node):
        # 函数名绑定在当前作用域
        self.add_var(node.name, node.lineno, 'def定义')

        # 进入函数作用域，函数体、参数都在这个作用域里
        self.scope_stack.append(('function', node.name))
        self.generic_visit(node)
        self.scope_stack.pop()

    visit_AsyncFunctionDef = visit_FunctionDef

    def visit_arg(self, node):
        # 函数参数
        self.add_var(node.arg, node.lineno, '参数')

    # ---------- 各种变量绑定节点 ----------
    def visit_Assign(self, node):
        for target in node.targets:
            self._handle_target(target, '赋值')
        self.generic_visit(node)

    def visit_AnnAssign(self, node):
        if node.value is not None:
            self._handle_target(node.target, '注解赋值')
        else:
            # 无值注解（如 A: int）：原有逻辑不记录到当前作用域，
            # 但在类体/方法中需要作为类成员收集。
            self._collect_class_member_target(node.target)
        self.generic_visit(node)

    def visit_AugAssign(self, node):
        self._handle_target(node.target, '增量赋值')
        self.generic_visit(node)

    def visit_For(self, node):
        self._handle_target(node.target, 'for循环变量')
        self.generic_visit(node)

    def visit_AsyncFor(self, node):
        self._handle_target(node.target, 'async for循环变量')
        self.generic_visit(node)

    def visit_With(self, node):
        for item in node.items:
            if item.optional_vars:
                self._handle_target(item.optional_vars, 'with as变量')
        self.generic_visit(node)

    def visit_AsyncWith(self, node):
        for item in node.items:
            if item.optional_vars:
                self._handle_target(item.optional_vars, 'async with as变量')
        self.generic_visit(node)

    def visit_ExceptHandler(self, node):
        if node.name:
            self.add_var(node.name, node.lineno, 'except as变量')
        self.generic_visit(node)

    def visit_comprehension(self, node):
        self._handle_target(node.target, '推导式变量')
        self.generic_visit(node)

    def visit_NamedExpr(self, node):
        self._handle_target(node.target, '海象运算符变量')
        self.generic_visit(node)

    def visit_Import(self, node):
        for alias in node.names:
            name = alias.asname or alias.name.split('.')[0]
            self.add_var(name, node.lineno, 'import')
        self.generic_visit(node)

    def visit_ImportFrom(self, node):
        for alias in node.names:
            name = alias.asname or alias.name
            self.add_var(name, node.lineno, 'from import')
        self.generic_visit(node)

    # ---------- 辅助函数 ----------
    def _handle_target(self, target, how, collect_class_member=True):
        for name_node in self._iter_store_names(target):
            self.add_var(name_node.id, name_node.lineno, how)

        if collect_class_member:
            self._collect_class_member_target(target)

    def _iter_store_names(self, node):
        """从赋值目标里提取出所有 Store 上下文的 Name 节点"""
        if isinstance(node, ast.Name):
            if isinstance(node.ctx, ast.Store):
                yield node
        elif isinstance(node, (ast.Tuple, ast.List)):
            for elt in node.elts:
                yield from self._iter_store_names(elt)
        elif isinstance(node, ast.Starred):
            yield from self._iter_store_names(node.value)


class CallVisitor(ast.NodeVisitor):
    """
    调用收集器，收集所有对 targets 的调用
    targets: 要收集的函数名集合，按调用表达式最后一段匹配
    parents: 父子关系表，用来确定调用者
    skip:    不参与收集的节点 id（装饰器里的调用）
    """

    def __init__(self, targets: set[str], parents: dict, skip: frozenset = frozenset()):
        self.targets = targets
        self.parents = parents
        self.skip = skip
        self.calls = []

    def visit_Call(self, node):
        if id(node) not in self.skip:
            func = node.func
            if isinstance(func, ast.Name):
                name = func.id
            elif isinstance(func, ast.Attribute):
                name = func.attr
            else:
                name = None

            if name in self.targets:
                self.calls.append({
                    "name": name,
                    "path": ast.unparse(func),
                    "lineno": node.lineno,
                    "col_offset": node.col_offset,
                    "caller": _qualname(_find_owner(node, self.parents), self.parents),
                })
        self.generic_visit(node)


def _filter_vars(vars_list, scope: str = None, how: str = None, name: str = None):
    """
    scope: '模块级' / '函数 xxx 内' / '类 xxx 体'，或只传 '函数'、'模块'、'类' 关键字
    how:   绑定方式，如 '赋值'、'参数'、'for循环变量'、'def定义'
    name:  变量名
    """
    result = []
    for scope_desc, var_name, lineno, how_desc in vars_list:
        if scope and scope not in scope_desc:
            continue
        if how and how != how_desc:
            continue
        if name and name != var_name:
            continue
        result.append((scope_desc, var_name, lineno, how_desc))
    return result


def _build_parent_map(tree):
    """
    构建一个以 子节点 为键 父节点 为值的字典
    """
    parents = {}
    for node in ast.walk(tree):
        for child in ast.iter_child_nodes(node):
            parents[child] = node
    return parents


def _find_owner(node, parents):
    """
    向上找最近的直接外层作用域节点（ClassDef / FunctionDef / AsyncFunctionDef）
    """
    cur = parents.get(node)
    while cur is not None:
        if isinstance(cur, SCOPE_NODES):
            return cur
        cur = parents.get(cur)
    return None


def _qualname(node, parents):
    """
    作用域的限定名，如 类A.函数A.函数B

    :return: 限定名(str)；**node 为 None 时返回 '模块级'**（调用者就是模块本身）
    """
    if node is None:
        return '模块级'

    parts = []
    cur = node
    while cur is not None:
        parts.append(cur.name)
        cur = _find_owner(cur, parents)
    return '.'.join(reversed(parts))


def scope_of_line(lineno: int, spans: list):
    """
    行号落在哪个作用域里 —— 给一组行区间，取包含该行的最内层

    :param lineno: 行号(1 起)
    :param spans:  [(起行, 止行, 限定名), ...]，各语言自己造
    :return: 限定名(str)；**没有区间包含该行时返回 None**
    """
    hit = None
    for start, end, name in spans:
        if start <= lineno <= end and (hit is None or start > hit[0]):
            hit = (start, name)
    return hit[1] if hit else None


def _build_scope_tree(tree, parents):
    """
    构建作用域的定义嵌套树：节点是作用域（ClassDef / FunctionDef / AsyncFunctionDef），
    子节点是语法上直接嵌套在其中的作用域 —— 是定义关系，不是调用关系
    """
    all_scopes = [n for n in ast.walk(tree) if isinstance(n, SCOPE_NODES)]

    children_of = {n: [] for n in all_scopes}
    roots = []

    for n in all_scopes:
        owner = _find_owner(n, parents)
        if owner is None:
            roots.append(n)
        else:
            children_of[owner].append(n)

    sort_key = lambda n: (n.lineno, n.col_offset)
    roots.sort(key=sort_key)
    for n in all_scopes:
        children_of[n].sort(key=sort_key)

    return roots, children_of


def _grep_text(source_code: str, pattern: str, start_line: int = None,
               end_line: int = None, is_regex: bool = False):
    """
    按行搜源码文本 —— 纯字符串操作，和语言无关

    :param source_code: 源码原文
    :param pattern: 要找的内容；默认按字面量匹配
    :param start_line: 起始行(1 起)；不给则从第 1 行开始
    :param end_line:   结束行；不给则到最后一行
    :param is_regex:   True 时 pattern 按正则解释
    :return: [(行号, 该行原文), ...]
    """
    lines = source_code.splitlines()
    lo = max((start_line or 1) - 1, 0)
    hi = min(end_line or len(lines), len(lines))

    rx = re.compile(pattern if is_regex else re.escape(pattern))
    return [(i + 1, lines[i]) for i in range(lo, hi) if rx.search(lines[i])]


def _grep(source_code: str, scope_spans: list, skip_spans: list, pattern: str,
          start_line: int = None, end_line: int = None, is_regex: bool = False,
          skip: bool = False):
    """
    两个分析器共用的 grep 主体 —— 搜文本 + 标作用域 + 按需剔除

    :param scope_spans: [(起行, 止行, 限定名), ...]，用来给命中行标作用域
    :param skip_spans:  [(起行, 止行), ...]，skip 为 True 时剔除落在里面的命中
    :param skip: 是否剔除（Python 剔除的是 docstring，Java 是注释）
    :return: [{"lineno", "text", "scope"}, ...]
    """
    if start_line is not None and end_line is None:
        # 单行模式：只给起始行 = 只看这一行，正则与剔除都不参与
        is_regex, skip = False, False
        end_line = start_line

    hits = _grep_text(source_code, pattern, start_line, end_line, is_regex)

    if skip:
        hits = [(ln, txt) for ln, txt in hits
                if not any(lo <= ln <= hi for lo, hi in skip_spans)]

    return [{"lineno": ln, "text": txt, "scope": scope_of_line(ln, scope_spans) or '模块级'}
            for ln, txt in hits]


# ---------- 模块定位 ----------
# 全程不 import、不执行被查的代码，只做文件系统探测 + 文本搜索
# 本地和远程是同一套逻辑，区别只在 roots 从哪来

def default_roots(*extra):
    """
    本地默认搜索根

    :param extra: 优先搜索的根目录（一般是项目根）
    :return: [根目录, ...]；后面接当前解释器的 sys.path，第三方包就在里面
    """
    return [p for p in extra if p] + [p for p in sys.path if p]


def find_module_path(module: str, roots: list):
    """
    把模块名定位成文件路径 —— 只探测，不 import、不列目录

    :param module: 点分模块名，如 "pyspark.sql.functions"
    :param roots:  搜索根目录列表
    :return: 文件路径(str)；**找不到返回 None**。
             包(__init__.py)优先于同名模块，与 Python 自己的查找规则一致
    """
    if not module:
        return None

    parts = module.split(".")
    for root in roots:
        base = os.path.join(root or ".", *parts)
        for cand in (os.path.join(base, "__init__.py"), base + ".py"):
            if os.path.isfile(cand):
                return cand
    return None


def _package_of(path):
    """
    文件所属的包（点分）—— 从所在目录往上，连续带 __init__.py 的那一段

    :param path: 文件路径
    :return: 包名(str)；**不在任何包里返回 None**
    """
    d = os.path.dirname(os.path.abspath(path))
    segs = []
    while os.path.isfile(os.path.join(d, "__init__.py")):
        segs.append(os.path.basename(d))
        parent = os.path.dirname(d)
        if parent == d:
            break
        d = parent
    return ".".join(reversed(segs)) if segs else None


def _resolve_relative(module, from_file):
    """
    相对模块名 -> 绝对模块名

    :param module: 带点的相对模块名，如 ".rel" / "..pkg.f"；点号个数就是 from 后面的层级
    :param from_file: 写这行 import 的文件
    :return: 绝对模块名(str)；**超出顶层包、或文件根本不在包里时返回 None**
    """
    level = len(module) - len(module.lstrip("."))
    rest = module[level:]

    pkg = _package_of(from_file)
    segs = pkg.split(".") if pkg else []

    drop = level - 1
    if drop >= len(segs):
        return None
    base = segs[:len(segs) - drop]
    if rest:
        base += rest.split(".")
    return ".".join(base)


def _search_define(name, path):
    """
    在文件文本里找名字的定义行 —— 只认模块级（列 0），因为 import * 只导出模块级名字

    :param name: 要找的名字
    :param path: 文件路径
    :return: {"file", "lineno", "text"}；**文件读不了、或没找到返回 None**
    """
    try:
        with open(path, encoding="utf-8", errors="replace") as f:
            text = f.read()
    except OSError:
        return None

    escaped = re.escape(name)
    rx = re.compile(
        rf"^(?:async\s+)?def\s+{escaped}\s*\(|"
        rf"^class\s+{escaped}\s*[(:]|"
        rf"^{escaped}\s*(?::[^=\n]*)?=(?!=)",
        re.M)
    m = rx.search(text)
    if m is None:
        return None

    lineno = text.count("\n", 0, m.start()) + 1
    return {"file": path, "lineno": lineno, "text": text.splitlines()[lineno - 1].strip()}


def _reexport_targets(name, path):
    """
    从一个文件里找出「可能定义了 name 的来源模块」—— 只看模块顶层的 import

    :param name: 要找的名字（它在本文件里被绑定的那个名字）
    :param path: 文件路径
    :return: [(模块名, 到了那边要找的名字), ...]；**相对导入的模块名原样带点返回**，由调用方再解析。
             别名要换名：from .deep import baz as renamed，找 renamed 时到了 deep 要找 baz。
             文件解析不了返回空列表
    """
    try:
        with open(path, encoding="utf-8", errors="replace") as f:
            tree = ast.parse(f.read())
    except (OSError, SyntaxError):
        return []

    targets = []
    for node in tree.body:
        if not isinstance(node, ast.ImportFrom):
            continue
        prefix = "." * node.level + (node.module or "")
        for alias in node.names:
            if alias.name == "*":
                targets.append((prefix, name))
                break
            # 具名导入认的是「绑定到本地的名字」；对上了就到那边找它的原名
            if (alias.asname or alias.name) == name:
                targets.append((prefix, alias.name))
                break
    return targets


def _find_in_module(name, module, roots, seen):
    """在模块里找 name，找不到就顺着转发行往下追 —— 递归主体"""
    path = find_module_path(module, roots)
    if path is None or path in seen:
        return None
    seen.add(path)

    # from X import name 里的 name 可能是个子模块，不是 X 里的名字
    sub = find_module_path(f"{module}.{name}", roots)
    if sub is not None:
        return {"module": f"{module}.{name}", "file": sub, "lineno": None, "text": None}

    hit = _search_define(name, path)
    if hit is not None:
        return {"module": module, **hit}

    for target, target_name in _reexport_targets(name, path):
        if target.startswith("."):
            target = _resolve_relative(target, path)
            if target is None:
                continue
        got = _find_in_module(target_name, target, roots, seen)
        if got is not None:
            return got
    return None


def find_definition(name: str, module: str, roots: list, from_file: str = None):
    """
    找一个名字在哪个文件、哪一行被定义

    顺着 from / import 转发行往下追；纯文件系统探测 + 文本搜索，**全程不 import、不执行被查的代码**

    :param name: 要找的名字，如 "col"
    :param module: 起点模块名（点分），如 "pyspark.sql.functions"
    :param roots: 搜索根目录列表 —— 本地用 default_roots(项目根)，远程由配置给
    :param from_file: 模块名是从哪个文件里写的；**只有 module 是相对形式**（".rel" / "..pkg.f"）才需要
    :return: {"module", "file", "lineno", "text"}；**命中子模块时 lineno/text 是 None**；找不到返回 None
    """
    if module.startswith("."):
        if from_file is None:
            return None
        module = _resolve_relative(module, from_file)
        if module is None:
            return None
    return _find_in_module(name, module, roots, set())


def _serialize(node, children_of, extract):
    """
    递归序列化作用域节点 —— Python / Java 共用这套渲染，字段由 extract 提供

    :param extract: 作用域节点 -> {"kind": "class"/"func", "name", "start", "end", "decorators"?}
    :return: dict；函数节点是 name / func_start_line / func_end_line，
             类节点是 classes / class_start_line / class_end_line，
             有装饰器才给 decorators，有子作用域才给 child
    """
    info = extract(node)

    if info["kind"] == "class":
        item = {
            "classes": info["name"],
            "class_start_line": info["start"],
            "class_end_line": info["end"],
        }
    else:
        item = {
            "name": info["name"],
            "func_start_line": info["start"],
            "func_end_line": info["end"],
        }
        if info.get("decorators"):
            item["decorators"] = info["decorators"]

    children = children_of.get(node, [])
    if children:
        item["child"] = [_serialize(c, children_of, extract) for c in children]

    return item


def _py_scope_info(node):
    """Python 作用域节点 -> _serialize 要的字段"""
    if isinstance(node, ast.ClassDef):
        return {"kind": "class", "name": node.name,
                "start": node.lineno, "end": node.end_lineno}

    info = {"kind": "func", "name": node.name,
            "start": node.lineno, "end": node.end_lineno}
    if node.decorator_list:
        info["decorators"] = [ast.unparse(d) for d in node.decorator_list]
    return info


class PyAnalyzer:
    """
    一个Python文件的解析入口

    tree 和源码只在构造时给一次；父子表、作用域索引、变量表都只建一次，后续查询直接复用
    """

    def __init__(self, file_url: Optional[str] = None, source_code: Optional[str] = None):
        if not file_url and not source_code:
            logger.error("PyAnalyzer 构造时必须给 file_url 或 source_code")
            raise ValueError("PyAnalyzer 构造时必须给 file_url 或 source_code")
        if file_url and source_code:
            logger.warning("不允许同时给 file_url 和 source_code，将会采用 source_code 的值")
        elif file_url:
            with open(file_url, "r", encoding="utf-8") as f:
                source_code = f.read()

        if not source_code:
            logger.error("PyAnalyzer 无法解析出 实际文本")
            raise ValueError("PyAnalyzer 无法解析出 实际文本")

        self.tree = ast.parse(source_code)
        self.source_code = source_code

        self.parents = _build_parent_map(self.tree)
        self.roots, self.children_of = _build_scope_tree(self.tree, self.parents)

        # 限定名 -> 作用域节点
        # 同一作用域里重名时，后定义的（真正生效的那个）覆盖前面的
        scopes = sorted((n for n in ast.walk(self.tree) if isinstance(n, SCOPE_NODES)),
                        key=lambda n: (n.lineno, n.col_offset))
        self.scopes = {_qualname(n, self.parents): n for n in scopes}

        self._var_cache = None
        self._span_cache = None
        self._docstring_cache = None

    # ---------- 结构 ----------
    def all_class(self):
        """
        获取文件中所有类

        :return: [{"name", "lineno", "end_lineno"}, ...]，嵌套类也在里面，但不体现嵌套关系
        """
        return [{"name": n.name, "lineno": n.lineno, "end_lineno": n.end_lineno}
                for n in ast.walk(self.tree) if isinstance(n, ast.ClassDef)]

    def class_hierarchy(self):
        """
        获取文件中函数/类的定义嵌套树

        :return: 嵌套的 list，函数节点是 name/func_start_line/func_end_line，
                 类节点是 classes/class_start_line/class_end_line
        """
        return [_serialize(r, self.children_of, _py_scope_info) for r in self.roots]

    def entry(self):
        """
        找 Python 的入口 —— `if __name__ == "__main__"` 那一行

        :return: 行号(int)；**没写这个式子时返回 None** —— None 不代表没有入口
        """
        for hit in self.grep("__main__"):
            if "__name__" in hit["text"]:
                return hit["lineno"]
        return None

    # ---------- 按限定名取 ----------
    def func_desc(self, qualname: str):
        """
        获取函数的文档字符串

        :param qualname: 函数的限定名，如 "类A.方法" / "函数A.内部函数"；模块级函数就是裸名
        :return: {"qualname", "docstring", "lineno", "end_lineno"}；
                 **没找到返回 None**；找到了但没写 docstring 时，docstring 字段是 None
        """
        node = self.scopes.get(qualname)
        if not isinstance(node, FUNC_NODES):
            return None
        return {"qualname": qualname, "docstring": ast.get_docstring(node),
                "lineno": node.lineno, "end_lineno": node.end_lineno}

    def class_desc(self, qualname: str):
        """
        获取类的文档字符串

        :param qualname: 类的限定名，如 "类A.内部类"；顶层类就是裸名
        :return: {"qualname", "docstring", "lineno", "end_lineno"}；
                 **没找到返回 None**；找到了但没写 docstring 时，docstring 字段是 None
        """
        node = self.scopes.get(qualname)
        if not isinstance(node, ast.ClassDef):
            return None
        return {"qualname": qualname, "docstring": ast.get_docstring(node),
                "lineno": node.lineno, "end_lineno": node.end_lineno}

    def code_by_name(self, qualname: str):
        """
        获取函数或类的代码段

        :param qualname: 限定名，如 "类A.方法"；模块级的就用裸名
        :return: {"qualname", "code", "lineno", "end_lineno"}；
                 **没找到、或没给源码返回 None**
        """
        node = self.scopes.get(qualname)
        if node is None or self.source_code is None:
            return None
        return {"qualname": qualname,
                "code": ast.get_source_segment(self.source_code, cast(ast.AST, node)),
                "lineno": node.lineno, "end_lineno": node.end_lineno}

    def lookup_hint(self, qualname: str):
        """
        查不到时给一句「错在哪一层、那一层有什么」—— 只报这一层的名字，不拿后缀去猜前缀

        :param qualname: 同 code_by_name
        :return: 提示文本(str)；**这个名字能解析出来时返回 None**
        """
        segments = [s for s in (qualname or "").strip().split(".") if s]
        prefix = ""
        for seg in segments:
            path = f"{prefix}.{seg}" if prefix else seg
            if path not in self.scopes:
                names = " / ".join(self._names_under(prefix)) or "（空）"
                place = "顶层" if not prefix else f"{prefix} 下面"
                return f"没有 {seg!r}；{place}只有 {names}，带上外层再查一次"
            prefix = path
        return None

    def _names_under(self, prefix):
        """某一层直接有哪些类/函数名（按定义顺序）"""
        head = f"{prefix}." if prefix else ""
        return [q[len(head):] for q in self.scopes
                if q.startswith(head) and "." not in q[len(head):]]

    # ---------- 变量 ----------
    @property
    def _var_items(self):
        """变量表只在第一次查询时收集一遍"""
        if self._var_cache is None:
            collector = VarCollector()
            collector.visit(self.tree)
            self._var_cache = collector.vars
        return self._var_cache

    def vars(self, scope: str, how: str, name: str = None):
        """
        获取文件中指定的变量信息

        :param scope: 作用域，子串匹配，如 '模块级' / '类 A 体'；传空串不过滤
        :param how:   绑定方式，精确匹配，如 '赋值' / '参数'；传空串不过滤
        :param name:  变量名，精确匹配；不给则不过滤
        :return: [(作用域描述, 变量名, 行号, 绑定方式), ...]
        """
        return _filter_vars(self._var_items, scope, how, name)

    def vars_indicator(self, indicator: Literal["scope", "how"]):
        """
        获取文件中变量有哪些可用的指标取值

        :param indicator: "scope" 取作用域取值，"how" 取绑定方式取值
        :return: set(str)
        """
        result = set()
        for scope, name, lineno, how in self._var_items:
            if indicator == "scope":
                result.add(scope)
            elif indicator == "how":
                result.add(how)
        return result

    # ---------- 调用 ----------
    def calls(self, targets: set[str]):
        """
        收集文件中所有对 targets 的调用

        :param targets: 函数名集合，按调用表达式最后一段匹配（self.helper() 也算命中 "helper"）
        :return: [{"name", "path", "lineno", "col_offset", "caller"}, ...]
                 path 是完整的调用表达式（self.helper），caller 是调用者限定名，
                 模块级调用时 caller 是 "模块级"；装饰器里的调用不算
        """
        visitor = CallVisitor(targets, self.parents, self._decorator_nodes())
        visitor.visit(self.tree)
        return visitor.calls

    def _decorator_nodes(self):
        """装饰器表达式里出现的所有节点 id —— 这些位置上的调用不收集"""
        ids = set()
        for n in ast.walk(self.tree):
            if isinstance(n, SCOPE_NODES):
                for deco in n.decorator_list:
                    for sub in ast.walk(deco):
                        ids.add(id(sub))
        return frozenset(ids)

    # ---------- 文本搜索 ----------
    def grep(self, pattern: str, start_line: int = None, end_line: int = None,
             is_regex: bool = False, exclude_docstring: bool = False):
        """
        在源码文本里按行搜索 —— 能命中注释和字符串，这是它区别于 vars() / calls() 的地方

        :param pattern: 要找的内容；默认按字面量匹配
        :param start_line: 起始行(1 起)；不给则从第 1 行开始
        :param end_line: 结束行；**不给就只看 start_line 那一行**，此时正则与 docstring 排除都不生效
        :param is_regex: True 时 pattern 按正则解释
        :param exclude_docstring: True 时剔除落在 docstring 里的命中（含模块级 docstring）
        :return: [{"lineno", "text", "scope"}, ...]；scope 是限定名，不在任何作用域里就是 "模块级"
        """
        return _grep(self.source_code, self._scope_spans, self._docstring_spans,
                     pattern, start_line, end_line, is_regex, exclude_docstring)

    @property
    def _scope_spans(self):
        """(起行, 止行, 限定名) 列表，只在第一次查询时建一次"""
        if self._span_cache is None:
            self._span_cache = [(n.lineno, n.end_lineno, q) for q, n in self.scopes.items()]
        return self._span_cache

    @property
    def _docstring_spans(self):
        """所有 docstring 占的行区间 —— 含模块级那一个（模块 docstring 不在 SCOPE_NODES 里）"""
        if self._docstring_cache is None:
            spans = []
            holders = [self.tree] + [n for n in ast.walk(self.tree) if isinstance(n, SCOPE_NODES)]
            for holder in holders:
                body = holder.body
                if not body:
                    continue
                first = body[0]
                if isinstance(first, ast.Expr) and isinstance(first.value, ast.Constant) \
                        and isinstance(first.value.value, str):
                    spans.append((first.lineno, first.end_lineno))
            self._docstring_cache = spans
        return self._docstring_cache

    # ---------- 导入 ----------
    def imports(self):
        """
        获取文件里 import 进来的名字及其来源

        :return: {绑定名: {"module": 来源全路径, "lineno", "kind"}}；
                 kind 是 "import" / "from import"；相对导入的来源以点开头，如 ".c"
        """
        result = {}
        for node in ast.walk(self.tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    bound = alias.asname or alias.name.split('.')[0]
                    result[bound] = {"module": alias.name, "lineno": node.lineno, "kind": "import"}
            elif isinstance(node, ast.ImportFrom):
                # 相对导入（from . import x）的 module 是 None，那一段没有名字，不能再补点
                prefix = '.' * node.level + (f"{node.module}." if node.module else '')
                for alias in node.names:
                    bound = alias.asname or alias.name
                    result[bound] = {"module": f"{prefix}{alias.name}",
                                     "lineno": node.lineno, "kind": "from import"}
        return result


# ---------------------------------------------------------------------------
# Java
# ---------------------------------------------------------------------------
# 用 tree_sitter_languages（惰性导入）：源码是从 HDFS 抓来的，可能是任意 Java 版本，
# 出错容忍的解析器撞上生语法也照样给得出类/方法的行区间，grep 更不受影响

JAVA_TYPE_NODES = ("class_declaration", "interface_declaration", "enum_declaration",
                   "record_declaration", "annotation_type_declaration")
JAVA_FUNC_NODES = ("method_declaration", "constructor_declaration")
JAVA_SCOPE_NODES = JAVA_TYPE_NODES + JAVA_FUNC_NODES

_JAVA_PARSER = None

# 类/接口/枚举/record 等类型声明的节点类型 -> 对外暴露的 attribute 值
_JAVA_CLASS_KINDS = {
    "class_declaration": "class",
    "interface_declaration": "interface",
    "enum_declaration": "enum",
    "record_declaration": "record",
    "annotation_type_declaration": "annotation",
}
# interface 的隐式常量是 constant_declaration，class 是 field_declaration。
_JAVA_FIELD_NODES = {"field_declaration", "constant_declaration"}


def _java_parser():
    """惰性取 Java 解析器 —— 不碰 Java 就不需要 tree_sitter"""
    global _JAVA_PARSER
    if _JAVA_PARSER is None:
        try:
            from tree_sitter_languages import get_parser
        except ImportError as exc:
            raise RuntimeError("解析 Java 需要 tree_sitter_languages，当前环境里没有") from exc
        _JAVA_PARSER = get_parser("java")
    return _JAVA_PARSER


def _walk_java(node):
    """深度优先遍历树上所有节点（对应 Python 侧的 ast.walk）"""
    stack = [node]
    while stack:
        cur = stack.pop()
        yield cur
        stack.extend(cur.named_children)


def _java_span(node):
    """节点占的行区间 (起行, 止行)，1 起"""
    start = node.start_point[0] + 1
    end_row, end_col = node.end_point
    return start, (end_row + 1 if end_col else end_row)


def _java_name(node):
    """节点的名字 —— 类名 / 方法名；构造器的名字就是类名"""
    name = node.child_by_field_name("name")
    return name.text.decode() if name is not None else None


def _java_signature(node):
    """声明处的名字 + 参数表原文，如 "process(String line, int retry)" """
    params = node.child_by_field_name("parameters")
    text = params.text.decode() if params is not None else "()"
    return f"{_java_name(node) or '?'}{' '.join(text.split())}"


def _java_modifiers(node):
    """声明上的修饰符 —— modifiers 是按类型挂的子节点，不是字段，取不出来就是没有"""
    for child in node.children:
        if child.type == "modifiers":
            return child.text.split()
    return []


def _java_scope_pairs(root):
    """
    按定义嵌套收集所有作用域 —— (外层作用域节点 或 None, 作用域节点, 限定名)

    只收命名的作用域；匿名类里的方法没有名字可索引，不算
    """
    pairs = []

    def walk(node, owner, prefix):
        for child in node.named_children:
            if child.type not in JAVA_SCOPE_NODES:
                walk(child, owner, prefix)
                continue
            name = _java_name(child)
            if name is None:
                walk(child, owner, prefix)
                continue
            qualname = f"{prefix}.{name}" if prefix else name
            pairs.append((owner, child, qualname))
            walk(child, child, qualname)

    walk(root, None, "")
    return pairs


def _java_scope_info(node):
    """Java 作用域节点 -> _serialize 要的字段（Java 没有装饰器这一类，不给 decorators）"""
    start, end = _java_span(node)
    return {"kind": "class" if node.type in JAVA_TYPE_NODES else "func",
            "name": _java_name(node), "start": start, "end": end}


def _collect_java_vars(class_node, path, result):
    """
    递归收集某个类型声明下的成员字段

    只遍历 body 的直接成员：field/constant 才收，子类型才递归；
    method_declaration / constructor_declaration / static_initializer / block
    都直接跳过 —— 因此天然不会收进方法体里的局部变量、局部类和匿名类。
    """
    name_node = class_node.child_by_field_name("name")
    class_name = name_node.text.decode("utf-8") if name_node is not None else "<anonymous>"
    kind = _JAVA_CLASS_KINDS[class_node.type]
    current_path = path + [class_name]

    body = class_node.child_by_field_name("body")
    if body is None:
        return

    for member in body.named_children:
        if member.type in _JAVA_FIELD_NODES:
            names = []
            for child in member.children:
                if child.type == "variable_declarator":
                    n = child.child_by_field_name("name")
                    if n is not None:
                        names.append(n.text.decode("utf-8"))
            if not names:
                continue
            result.append({
                "classes": ".".join(current_path),
                "attribute": kind,
                "variable": names[0] if len(names) == 1 else names,
            })
        elif member.type in _JAVA_CLASS_KINDS:
            _collect_java_vars(member, current_path, result)


class JavaAnalyzer:
    """
    一个 Java 文件的解析入口

    用法和 PyAnalyzer 一致：源码只在构造时给一次，作用域、导入表都只建一次，后续查询直接复用

    package 是本文件 package 声明的名字（如 "com.ops.fault"）；**没写 package 时是 None**
    """

    def __init__(self, file_url: Optional[str] = None, source_code: Optional[str] = None):
        if not file_url and not source_code:
            logger.error("JavaAnalyzer 构造时必须给 file_url 或 source_code")
            raise ValueError("JavaAnalyzer 构造时必须给 file_url 或 source_code")
        if file_url and source_code:
            logger.warning("不允许同时给 file_url 和 source_code，将会采用 source_code 的值")
        elif file_url:
            with open(file_url, "r", encoding="utf-8") as f:
                source_code = f.read()

        if not source_code:
            logger.error("JavaAnalyzer 无法解析出 实际文本")
            raise ValueError("JavaAnalyzer 无法解析出 实际文本")

        self.tree = _java_parser().parse(source_code.encode("utf-8"))
        self.root = self.tree.root_node
        self.source_code = source_code
        self.package = self._parse_package()

        # 限定名 -> 声明节点列表（按源码顺序）；**方法同名就是重载**，类永远只有一个
        self.decls: dict[str, list] = {}
        self.roots = []
        self.children_of = {}
        for owner, node, qualname in _java_scope_pairs(self.root):
            self.decls.setdefault(qualname, []).append(node)
            if owner is None:
                self.roots.append(node)
            else:
                self.children_of.setdefault(owner, []).append(node)

        self._span_cache = None
        self._comment_cache = None

    @property
    def has_error(self):
        """解析时有没有撞上不认识的东西 —— tree-sitter 出错也照样给树，这个标记只是提示"""
        return self.root.has_error

    def _parse_package(self):
        """本文件的 package 声明"""
        for node in self.root.named_children:
            if node.type == "package_declaration":
                m = re.match(r"package\s+([\w.]+)\s*;", node.text.decode())
                if m:
                    return m.group(1)
        return None

    # ---------- 结构 ----------
    def class_hierarchy(self):
        """
        获取文件中类/方法的定义嵌套树

        :return: 嵌套的 list，类节点是 classes/class_start_line/class_end_line，
                 方法节点是 name/func_start_line/func_end_line；
                 **匿名类里的方法没有名字，不进这棵树**
        """
        return [_serialize(r, self.children_of, _java_scope_info) for r in self.roots]

    def vars(self):
        """
        获取类变量，不包括方法内的变量

        支持嵌套类/接口/枚举，路径按定义层级用 "." 拼起来。
        同一 field_declaration 里声明的多个变量合并成一个列表；
        只有一个变量时直接给字符串。

        :return: list，每项形如
                 {"classes": "Main.SubA", "attribute": "class", "variable": "a"}
                 {"classes": "Main",      "attribute": "class", "variable": ["b", "c"]}
                 {"classes": "Main.SubB", "attribute": "interface", "variable": "CONST"}
                 方法内的局部变量、局部类、匿名类一律不会出现
        """
        result = []
        for node in self.root.named_children:
            if node.type in _JAVA_CLASS_KINDS:
                _collect_java_vars(node, [], result)
        return result

    def entry(self):
        """
        找 Java 的入口 —— `public static void main(String[])` 的声明行

        :return: 行号(int)；**没有 static main 时返回 None**；有多个时给最前面那个
        """
        first = None
        for node in _walk_java(self.root):
            if node.type != "method_declaration" or _java_name(node) != "main":
                continue
            if b"static" not in _java_modifiers(node):
                continue
            start = _java_span(node)[0]
            if first is None or start < first:
                first = start
        return first

    # ---------- 按限定名取 ----------
    def code_by_name(self, qualname: str, start_idx: int | None = None, end_idx: int | None = None):
        """
        获取类或方法的代码段

        :param qualname: 限定名，**从文件顶层往下写全**，如 "FaultJob.RateSource.run"；
                         也吃栈帧写法（"com.ops.fault.FaultJob$RateSource.run"）——
                         包名整段去掉、$ 当 .，这两条只是写法换算，不做任何补全
        :param start_idx: 开始行号，从 1 开始；默认 None，即从头开始
        :param end_idx: 结束行号，默认 None，即到尾结束(若填了start_idx，而没填此参数，则只返回start_idx这行)
        :return: {"qualname", "code", "lineno", "end_lineno"}，qualname 是解析出来的全名；
                 同名方法（重载）时多一个 "overloads"，每项是 {"signature", "lineno", "end_lineno"}，
                 code 给的是**源码顺序最靠前**的那一个；
                 **路径走不通返回 None** —— 这时候可以调 lookup_hint 问错在哪一层
        """
        segments = self._segments(qualname)
        if not segments:
            return None

        prefix = ""
        for seg in segments[:-1]:
            prefix = f"{prefix}.{seg}" if prefix else seg
            if len(self.decls.get(prefix) or []) != 1:
                return None

        full = f"{prefix}.{segments[-1]}" if prefix else segments[-1]
        hits = self.decls.get(full)
        if not hits:
            return None

        start, end = _java_span(hits[0])
        if start_idx is None and end_idx is None:
            start_idx = start
            end_idx = end
        elif start_idx is not None and end_idx is None:
            end_idx = start_idx
        got = {"qualname": full,
               "code": "\n".join(self.source_code.splitlines()[start_idx - 1:end_idx]),
               "lineno": start_idx, "end_lineno": end_idx}
        if len(hits) > 1:
            got["overloads"] = [{"signature": _java_signature(n),
                                 "lineno": _java_span(n)[0], "end_lineno": _java_span(n)[1]}
                                for n in hits]
        return got

    def lookup_hint(self, qualname: str):
        """
        查不到时给一句「错在哪一层、那一层有什么」—— 只报这一层的名字，不拿后缀去猜前缀

        :param qualname: 同 code_by_name
        :return: 提示文本(str)；**这个名字能解析出来时返回 None**
        """
        segments = self._segments(qualname)
        prefix = ""
        for i, seg in enumerate(segments):
            path = f"{prefix}.{seg}" if prefix else seg
            hits = self.decls.get(path)
            if hits is None:
                names = " / ".join(self._names_under(prefix)) or "（空）"
                place = "顶层" if not prefix else f"{prefix} 下面"
                return f"没有 {seg!r}；{place}只有 {names}，带上外层再查一次"
            if len(hits) > 1 and i < len(segments) - 1:
                return (f"{path} 有 {len(hits)} 个同名声明（重载），说不清往哪一个里面找，"
                        f"先单独取它")
            prefix = path
        return None

    def _names_under(self, prefix):
        """某一层直接有哪些类/方法名（按源码顺序）"""
        head = f"{prefix}." if prefix else ""
        return [q[len(head):] for q in self.decls
                if q.startswith(head) and "." not in q[len(head):]]

    def _segments(self, qualname):
        """限定名 -> 路径段；顺带做写法换算：$ 当 .，本文件的包名整段去掉"""
        q = (qualname or "").strip().replace("$", ".")
        if self.package and q.startswith(f"{self.package}."):
            q = q[len(self.package) + 1:]
        return [s for s in q.split(".") if s]

    # ---------- 导入 ----------
    def imports(self):
        """
        获取文件里 import 进来的名字及其来源

        :return: {绑定名: {"module": 来源全路径, "lineno", "kind"}}；
                 kind 是 "import" / "static import"；
                 **通配导入没有绑定单一名字，键直接用它的全路径**（如 "java.util.*"）；
                 这里只列写了 import 的，**没写不等于找不到**（同包、同类都不用 import）
        """
        result = {}
        for node in self.root.named_children:
            if node.type != "import_declaration":
                continue
            m = re.match(r"import\s+(static\s+)?([\w.*]+)\s*;", node.text.decode())
            if m is None:
                continue
            path = m.group(2)
            bound = path if path.endswith(".*") else path.rsplit(".", 1)[-1]
            result[bound] = {"module": path, "lineno": _java_span(node)[0],
                             "kind": "static import" if m.group(1) else "import"}
        return result

    # ---------- 文本搜索 ----------
    def grep(self, pattern: str, start_line: int = None, end_line: int = None,
             is_regex: bool = False, exclude_docstring: bool = False):
        """
        在源码文本里按行搜索 —— 能命中注释和字符串，这是它区别于 code_by_name 的地方

        :param pattern: 要找的内容；默认按字面量匹配
        :param start_line: 起始行(1 起)；不给则从第 1 行开始
        :param end_line: 结束行；**不给就只看 start_line 那一行**，此时正则与注释排除都不生效
        :param is_regex: True 时 pattern 按正则解释
        :param exclude_docstring: True 时剔除落在注释里的命中（// 和 /* */，含 javadoc）
        :return: [{"lineno", "text", "scope"}, ...]；scope 是限定名，不在任何类/方法里就是 "模块级"
        """
        return _grep(self.source_code, self._scope_spans, self._comment_spans,
                     pattern, start_line, end_line, is_regex, exclude_docstring)

    @property
    def _scope_spans(self):
        """(起行, 止行, 限定名) 列表，只在第一次查询时建一次"""
        if self._span_cache is None:
            self._span_cache = [(s, e, q) for q, nodes in self.decls.items()
                                for s, e in (_java_span(n) for n in nodes)]
        return self._span_cache

    @property
    def _comment_spans(self):
        """注释占的行区间 —— Java 里 exclude_docstring 剔除的就是它"""
        if self._comment_cache is None:
            self._comment_cache = [_java_span(n) for n in _walk_java(self.root)
                                   if n.type in ("line_comment", "block_comment")]
        return self._comment_cache


if __name__ == '__main__':
    # file_url = r"D:\python\Lib\site-packages\langchain\agents\factory.py"
    # file_url = r"D:\pycharm\pycharm study\intelligent_customer_service\Tools\middleware\memory\time_memory.py"
    file_url = r"./operations_agent_tools.py"
    with open(file_url, 'r', encoding='utf-8') as f:
        code = f.read()
    #     code = """
    # def outer():
    #     def inner():
    #         def deep():
    #             pass
    #
    # class A:
    #     class B:
    #         def deep_method(self):
    #             pass
    #
    # class C:
    #     def method(self):
    #         def inner_method():
    #             pass
    #
    # def standalone():
    #     pass
    #     """
    # analyzer = PyAnalyzer(file_url, code)
    # print(analyzer.class_hierarchy())
    # print(analyzer.func_desc("_load_user_profile"))
    # print(analyzer.class_desc("BalancedMultiDimensionMemory"))
    # for scope, name, lineno, how in analyzer.vars(scope="模块级", how="def定义"):
    #     print(f"{scope:12} {name:10} 行{lineno:3} 方式:{how}")
    # print(analyzer.code_by_name("BalancedMultiDimensionMemory"))
    # print(analyzer.imports())
    # for call in analyzer.calls({"_dump"}):
    #     print(f"调用 {call['path']} 在行 {call['lineno']}, 列 {call['col_offset']}, 调用者 {call['caller']}")

    # ---------------- Java ----------------
    # root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    # analyzer = JavaAnalyzer(os.path.join(
    #     root, r"tests/O&M/flink/src/main/java/com/ops/fault/FaultJob.java"))
    analyzer = JavaAnalyzer(
        r"D:\Java\JAVA study\Demo\BigData\BigDataFlink\src\main\java\com\fm\MainRealTimeRankingTopN.java")
    print(analyzer.package, "has_error =", analyzer.has_error, "entry =", analyzer.entry())
    print(analyzer.class_hierarchy())
    print(analyzer.imports())
    print(analyzer.code_by_name("com.fm.MainRealTimeRankingTopN.brandTopNHandle")["lineno"])
    print(analyzer.lookup_hint("brandTopNHandle"))
