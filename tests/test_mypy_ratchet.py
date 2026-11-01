#!/usr/bin/env python3

"""
mypy 注解棘轮：未注解函数数量只允许下降，不允许增长。

P1-8：pyproject 已对 src.backtest / src.risk / src.api 强制
disallow_untyped_defs（这两处必须恒为 0），其余模块无法一次性补齐，
用「总数不超过基线」的棘轮防止债务反弹。

统计口径（与 mypy 的 untyped def 对齐）：
- 函数参数（含 *args / **kwargs）均无注解，且没有返回值注解，
  即一个注解都没有的函数；
- 只注解了部分参数或只注解返回值（mypy 判为 incomplete def）不计入，
  这类由 disallow_incomplete_defs 在核心目录单独把关。

新增带注解的函数不会触发失败；把已注解函数改回无注解、或新增无注解函数
会直接失败，此时应补充注解而不是抬高基线。
"""

import ast
from pathlib import Path

SRC_ROOT = Path(__file__).resolve().parents[1] / 'src'

#: 棘轮基线：只允许下降。收口后请同步下调该值。
#: 本轮为 data/services/sync_stock.py 与 tech_analysis/technical_analyzer.py
#: 的热点函数补注解后实测值（903 -> 845）。
UNTYPED_BASELINE = 845

#: 已由 pyproject 强制注解的目录，必须恒为 0
STRICT_DIRS = ('backtest', 'risk', 'api')


def _function_is_untyped(node: ast.FunctionDef | ast.AsyncFunctionDef) -> bool:
    """函数是否完全没有注解（参数与返回值都没有）。"""
    if node.returns is not None:
        return False

    args = node.args
    all_args = [*args.posonlyargs, *args.args, *args.kwonlyargs]
    all_args += [a for a in (args.vararg, args.kwarg) if a is not None]
    return all(arg.annotation is None for arg in all_args)


def _count_untyped(tree: ast.AST) -> int:
    return sum(
        1
        for node in ast.walk(tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and _function_is_untyped(node)
    )


def _untyped_counts(root: Path) -> dict[str, int]:
    """按一级子目录统计未注解函数数量。"""
    counts: dict[str, int] = {}
    for path in root.rglob('*.py'):
        tree = ast.parse(path.read_text(encoding='utf-8'))
        count = _count_untyped(tree)
        if not count:
            continue
        bucket = path.relative_to(root).parts[0]
        counts[bucket] = counts.get(bucket, 0) + count
    return counts


def test_untyped_function_count_does_not_grow():
    counts = _untyped_counts(SRC_ROOT)
    total = sum(counts.values())

    assert total <= UNTYPED_BASELINE, (
        f'src 下未注解函数数从基线 {UNTYPED_BASELINE} 增长到 {total}，'
        f'请为新增函数补注解，而不是抬高基线。按目录：{counts}'
    )


def test_strict_directories_have_no_untyped_functions():
    """pyproject 强制注解的核心目录里不允许出现完全无注解的函数。"""
    counts = _untyped_counts(SRC_ROOT)

    for directory in STRICT_DIRS:
        assert counts.get(directory, 0) == 0, (
            f'src/{directory} 仍有 {counts.get(directory)} 个无注解函数，'
            f'与 pyproject 的 disallow_untyped_defs 门禁不一致'
        )
