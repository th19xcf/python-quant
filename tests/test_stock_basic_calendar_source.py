"""钉子：``build_stock_basic`` 的交易日历不得取自 ``breadth_daily``。

背景：``meta/breadth/breadth_daily.parquet`` 是 L2 派生宽度表，由个股行情聚合而来。
名录表 ``stock_basic`` 是 L1，取它当日历会让 L1 反向依赖 L2——任何一次宽度重算
都会改变 ``status`` / ``latest_trade_date``，进而触发下游整链返工。

权威日历入口是 ``src/data/trading_calendar.py``：以本地基准指数（``000001.SH``）
的实际交易日为中段，尾部工作日为近似段（``authoritative_dates()`` 只取中段）。

运行：``.venv\\Scripts\\python.exe -m pytest tests/test_stock_basic_calendar_source.py -q``
"""

from __future__ import annotations

import ast
import inspect
from datetime import date
from pathlib import Path

import polars as pl

from tools import build_stock_basic


def _code_without_docstrings() -> str:
    """去掉文档字符串后的代码文本：只用来判「是否还有路径引用」，不做语义执行。"""
    tree = ast.parse(inspect.getsource(build_stock_basic))
    for node in ast.walk(tree):
        if not isinstance(node, (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef,
                                 ast.ClassDef)):
            continue
        body = node.body
        if (body and isinstance(body[0], ast.Expr)
                and isinstance(body[0].value, ast.Constant)
                and isinstance(body[0].value.value, str)):
            node.body = body[1:] or [ast.Pass()]
    return ast.unparse(tree)


def test_module_code_has_no_breadth_reference() -> None:
    """源码级钉子：代码里不得再出现 ``breadth`` 路径引用（改动前本用例为红）。

    只判代码，不判文档字符串——解释「为什么不用它」需要提到它。
    """
    code = _code_without_docstrings()
    hits = [line.strip() for line in code.splitlines() if 'breadth' in line.lower()]
    assert hits == [], 'build_stock_basic 代码仍引用宽度表（L1 反向依赖 L2）：\n' + '\n'.join(hits)


def test_trade_calendar_ignores_breadth_weekend_bars(tmp_path: Path) -> None:
    """行为级钉子：宽度表多出的周末 bar 不得进入交易日历。

    构造一个基准指数只有 3 个交易日、宽度表却多一个周六的最小库；
    日历必须只返回指数的 3 天。
    """
    index_path = tmp_path / 'quotes' / 'index' / 'sh' / '000001.SH.parquet'
    index_path.parent.mkdir(parents=True)
    index_days = [date(2024, 1, 2), date(2024, 1, 3), date(2024, 1, 4)]
    pl.DataFrame({
        'date': index_days,
        'close': [1.0, 1.0, 1.0],
    }, schema={'date': pl.Date, 'close': pl.Float64}).write_parquet(index_path)
    # 日历要求台账存在（DG-2 不回落单指数）；一行不影响本夹具的 3 个交易日。
    ledger = tmp_path / 'meta' / 'trading_calendar' / 'session_ledger.parquet'
    ledger.parent.mkdir(parents=True)
    pl.DataFrame({'date': [date(2099, 1, 3)], 'status': ['closed']},
                 schema={'date': pl.Date, 'status': pl.String}).write_parquet(ledger)

    breadth_path = tmp_path / 'meta' / 'breadth' / 'breadth_daily.parquet'
    breadth_path.parent.mkdir(parents=True)
    pl.DataFrame({
        'trade_date': [*index_days, date(2024, 1, 6)],  # 2024-01-06 是周六
    }, schema={'trade_date': pl.Date}).write_parquet(breadth_path)

    from src.data.trading_calendar import reset_calendar_cache  # noqa: PLC0415
    reset_calendar_cache()
    try:
        got = build_stock_basic.load_trade_calendar(tmp_path).to_list()
    finally:
        reset_calendar_cache()

    assert got == index_days, f'交易日历混入了非指数交易日: {got}'
