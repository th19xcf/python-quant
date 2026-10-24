#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
架构守卫：禁止 src/ui/** 新增对业务/数据层的 import

规则：
- 禁止 src/ui/ 下任何 .py 文件 import src.database.* 或 src.tech_analysis.*
- 现有违规列入 FROZEN_WHITELIST 冻结，只减不增：
  - 出现白名单之外的新违规 → 测试失败
  - 白名单中的违规被消除 → 测试失败并提示收紧白名单（防止回潮）

随 pytest 全量套件自动执行，无需额外工具。
"""

import ast
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
UI_ROOT = REPO_ROOT / 'src' / 'ui'

# 被禁止的 import 根模块
FORBIDDEN_ROOTS = ('src.database', 'src.tech_analysis')

# 冻结白名单：(相对 src/ui 的文件路径, 被禁止的模块)，仅允许减少
FROZEN_WHITELIST = frozenset({
    ('async_processing.py', 'src.tech_analysis.technical_analyzer'),
    ('stock_search_dialog.py', 'src.database.models.stock'),
    ('chart_components/chart_data_preparer.py', 'src.tech_analysis.indicators.trend'),
    ('chart_components/indicator_renderer.py', 'src.tech_analysis.indicator_calculator'),
})


def _is_forbidden(module: str) -> bool:
    return any(module == root or module.startswith(root + '.') for root in FORBIDDEN_ROOTS)


def find_forbidden_imports(source: str, rel_path: str) -> set:
    """
    解析源码 AST，找出其中对禁止根模块的全部 import（含函数内延迟 import）。

    Args:
        source: Python 源码文本
        rel_path: 相对 src/ui 的文件路径（正斜杠），用于相对 import 解析

    Returns:
        set: (rel_path, 被禁止的模块名) 二元组
    """
    tree = ast.parse(source)
    # 当前文件所在包路径，用于解析相对 import（from ... import 形式）
    pkg_parts = ('src', 'ui') + tuple(Path(rel_path).parent.parts)
    if pkg_parts[-1] == '.':
        pkg_parts = pkg_parts[:-1]

    found = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if _is_forbidden(alias.name):
                    found.add((rel_path, alias.name))
        elif isinstance(node, ast.ImportFrom):
            module = node.module or ''
            if node.level:
                # 相对 import：level=1 为当前包，逐级向上
                base = pkg_parts[:len(pkg_parts) - (node.level - 1)]
                module = '.'.join(list(base) + ([module] if module else []))
            if module and _is_forbidden(module):
                found.add((rel_path, module))
    return found


def _collect_ui_violations() -> set:
    violations = set()
    for py_file in sorted(UI_ROOT.rglob('*.py')):
        rel_path = py_file.relative_to(UI_ROOT).as_posix()
        violations |= find_forbidden_imports(py_file.read_text(encoding='utf-8-sig'), rel_path)
    return violations


def test_no_new_ui_business_imports():
    """src/ui/** 不得新增对 src.database / src.tech_analysis 的 import"""
    current = _collect_ui_violations()

    new_violations = current - FROZEN_WHITELIST
    assert not new_violations, (
        '检测到 UI 层新增对业务/数据层的 import（违反分层守卫规则）：\n'
        + '\n'.join(f'  src/ui/{f} -> {m}' for f, m in sorted(new_violations))
        + '\n请改由应用服务层（src/api）中转，或确属合理时与本团队确认后加入白名单。'
    )

    stale = FROZEN_WHITELIST - current
    assert not stale, (
        '以下白名单违规已被消除，请同步从 FROZEN_WHITELIST 中移除（白名单只减不增）：\n'
        + '\n'.join(f'  src/ui/{f} -> {m}' for f, m in sorted(stale))
    )


def test_scanner_detects_violations():
    """自测：扫描器能识别顶层/函数内/相对 import 三种违规形态"""
    src = (
        'import src.database.db_manager\n'
        'from src.tech_analysis.indicator_calculator import calculate_ma_polars\n'
        'def f():\n'
        '    from src.database.models.stock import StockBasic\n'
        'from ...tech_analysis.utils import x\n'
        'from src.api.business_api import ok\n'  # 合法，不应命中
        'from src.utils.logger import logger\n'  # 合法，不应命中
    )
    found = find_forbidden_imports(src, 'chart_components/dummy.py')
    modules = {m for _, m in found}
    assert 'src.database.db_manager' in modules
    assert 'src.tech_analysis.indicator_calculator' in modules
    assert 'src.database.models.stock' in modules
    assert 'src.tech_analysis.utils' in modules
    assert not any(m.startswith('src.api') or m.startswith('src.utils') for m in modules)


if __name__ == '__main__':
    raise SystemExit(pytest.main([__file__, '-q']))
