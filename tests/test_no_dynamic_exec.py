
"""
禁止 src/ 内使用 eval / exec / compile 执行动态代码。

公式语言、策略配置等外部输入一旦进入动态求值，等于开放任意代码执行权限；
本测试用 AST 扫描兜底，Qt 的 ``obj.exec()`` 属于属性调用，不在拦截范围。
"""

import ast
from pathlib import Path

SRC_DIR = Path(__file__).resolve().parents[1] / "src"
FORBIDDEN_BUILTINS = {"eval", "exec", "compile"}


def test_src_has_no_dynamic_code_execution():
    offenders = []
    for path in SRC_DIR.rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Name)
                and node.func.id in FORBIDDEN_BUILTINS
            ):
                rel = path.relative_to(SRC_DIR.parent)
                offenders.append(f"{rel}:{node.lineno}: {node.func.id}()")

    assert not offenders, "发现动态代码执行: " + "; ".join(offenders)
