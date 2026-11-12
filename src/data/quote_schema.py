#!/usr/bin/env python3

"""行情分片 schema 契约：写入闸门 + 读取一致性断言。

背景（2026-11-12）：一次 19 行成交量修复给 7 个 ``quotes/stock`` 分片多加了
``basis`` 列，全库 schema 不再统一。``polars`` 对多文件 ``scan_parquet`` /
``read_parquet`` 的 schema 合并是「任一文件多列或少列即抛 ``SchemaError``」，
列投影下推**不能**豁免（本地 polars 2.1.0 实测：``scan(...).select('date')``
同样报 ``extra column``），于是 10 个消费方同时失效。

契约包含三件事，缺一不可：

- **列集合**：多一列或少一列都会让下游 glob 读取直接失败；
- **列顺序**：按位置拼列的写法（``concat``、``to_numpy`` 后按 index 取列）
  在顺序变化时会静默错列；
- **dtype**：``Float64`` 变 ``Float32`` / ``Int64`` 会在 join-asof、比较与
  无量纲化处产生静默偏差。

因此写入正式库（``<repo>/data/local_store/quotes/**``）时一律校验；需要额外
列的场合必须显式传 ``allow_extra_columns=True``，并在调用点写明理由。
临时目录（测试夹具、``_staging``）不受此闸门约束——那里允许用列子集构造
最小样本，闸门只保护正式数据层。
"""

from __future__ import annotations

import glob as _glob
from pathlib import Path
from typing import Iterable

import polars as pl

# 标准列集合（顺序即契约）。stock / fund / index 共用一套日线列；
# futures 沿用交易所原生列名，与日线不混用通配。
QUOTE_COLUMNS: dict[str, tuple[str, ...]] = {
    'stock': (
        'date', 'ts_code', 'open', 'high', 'low', 'close', 'pre_close',
        'volume', 'amount', 'pct_chg', 'change', 'qfq_factor', 'hfq_factor',
    ),
    'fund': (
        'date', 'ts_code', 'open', 'high', 'low', 'close', 'pre_close',
        'volume', 'amount', 'pct_chg', 'change', 'qfq_factor', 'hfq_factor',
    ),
    'index': (
        'date', 'ts_code', 'open', 'high', 'low', 'close', 'pre_close',
        'volume', 'amount', 'pct_chg', 'change', 'qfq_factor', 'hfq_factor',
    ),
    'futures': (
        'ts_code', 'trade_date', 'pre_close', 'pre_settle', 'open', 'high',
        'low', 'close', 'settle', 'change1', 'change2', 'vol', 'amount',
        'oi', 'oi_chg',
    ),
}

_FLOAT = 'Float64'
QUOTE_DTYPES: dict[str, dict[str, str]] = {
    'stock': {'date': 'Date', 'ts_code': 'String', **{
        c: _FLOAT for c in QUOTE_COLUMNS['stock'] if c not in ('date', 'ts_code')}},
    'fund': {'date': 'Date', 'ts_code': 'String', **{
        c: _FLOAT for c in QUOTE_COLUMNS['fund'] if c not in ('date', 'ts_code')}},
    'index': {'date': 'Date', 'ts_code': 'String', **{
        c: _FLOAT for c in QUOTE_COLUMNS['index'] if c not in ('date', 'ts_code')}},
    'futures': {'trade_date': 'String', 'ts_code': 'String', **{
        c: _FLOAT for c in QUOTE_COLUMNS['futures']
        if c not in ('trade_date', 'ts_code')}},
}

_REPO_ROOT = Path(__file__).resolve().parents[2]
FORMAL_QUOTES_ROOT = _REPO_ROOT / 'data' / 'local_store' / 'quotes'


class QuoteSchemaError(ValueError):
    """行情分片 schema 与契约不符（列集合 / 顺序 / dtype）。"""


def _resolve(path: Path | str) -> Path:
    try:
        return Path(path).resolve()
    except OSError:  # pragma: no cover - 路径不可解析时按相对判定
        return Path(path).absolute()


def quote_data_type_for_path(path: Path | str) -> str | None:
    """路径落在正式库 ``data/local_store/quotes/<data_type>/`` 下时返回该类型。"""
    try:
        rel = _resolve(path).relative_to(FORMAL_QUOTES_ROOT)
    except ValueError:
        return None
    head = rel.parts[0] if rel.parts else ''
    return head if head in QUOTE_COLUMNS else None


def is_formal_quote_root(store_root: Path | str, data_type: str) -> bool:
    """``store_root/quotes/<data_type>`` 是否就是正式库目录。"""
    return _resolve(Path(store_root) / 'quotes' / data_type) == _resolve(
        FORMAL_QUOTES_ROOT / data_type)


def _frame_schema(frame: pl.DataFrame | pl.LazyFrame) -> dict[str, str]:
    if isinstance(frame, pl.LazyFrame):
        schema = frame.collect_schema()
    else:
        schema = frame.schema
    return {name: str(dtype) for name, dtype in schema.items()}


def assert_quote_schema(
    data_type: str, frame: pl.DataFrame | pl.LazyFrame, *,
    allow_extra_columns: bool = False,
) -> None:
    """校验 ``frame`` 是否符合 ``data_type`` 的行情 schema 契约。

    ``allow_extra_columns=True`` 是例外通道：只放行**额外列**，缺列、列序、
    dtype 仍然严格校验。使用处必须在代码里写明为什么需要额外列。
    """
    expected = QUOTE_COLUMNS.get(data_type)
    if expected is None:
        raise QuoteSchemaError(
            f'未知行情类型 {data_type!r}，可选：{sorted(QUOTE_COLUMNS)}')

    schema = _frame_schema(frame)
    columns = tuple(schema)
    missing = [c for c in expected if c not in schema]
    extra = [c for c in columns if c not in expected]
    ordered = tuple(c for c in columns if c in expected)

    problems: list[str] = []
    if missing:
        problems.append(f'缺列 {missing}')
    if extra and not allow_extra_columns:
        problems.append(
            f'多列 {extra}（确需额外列的场合显式传 allow_extra_columns=True 并写明理由）')
    if ordered != expected:
        problems.append(f'列顺序不符：期望 {list(expected)}，实际 {list(ordered)}')
    for column, want in QUOTE_DTYPES[data_type].items():
        actual = schema.get(column)
        if actual is not None and actual != want:
            problems.append(f'{column} dtype 期望 {want}，实际 {actual}')
    if problems:
        raise QuoteSchemaError(
            f'{data_type} 行情 schema 不符合契约：' + '；'.join(problems))


def _expand_paths(paths: str | Iterable[str | Path]) -> list[Path]:
    if isinstance(paths, (str, Path)):
        text = str(paths)
        if any(ch in text for ch in '*?['):
            return sorted(Path(p) for p in _glob.glob(text, recursive=True))
        return [Path(text)]
    return [Path(p) for p in paths]


def assert_quote_glob_schema(
    data_type: str, paths: str | Iterable[str | Path],
) -> None:
    """断言一组行情分片逐文件 schema 一致且符合契约（读取侧闸门）。

    不能用 ``scan_parquet(paths).collect_schema()``：polars 2.1.0 在
    ``collect_schema`` 上会把多文件 schema **合并**成并集而不报错，实测
    「窄表 + 带 basis 的宽表」返回 4 列不抛异常；只有 ``collect`` 才抛
    ``SchemaError``。这里逐文件读 footer 比对签名，首处不一致立即失败，
    并给出带 ``data_type`` 的明确错误（polars 原生报错不含数据语义）。
    """
    expected = QUOTE_COLUMNS.get(data_type)
    if expected is None:
        raise QuoteSchemaError(
            f'未知行情类型 {data_type!r}，可选：{sorted(QUOTE_COLUMNS)}')
    files = _expand_paths(paths)
    if not files:
        raise QuoteSchemaError(f'{data_type} 行情分片为空：{paths}')
    baseline: tuple[tuple[tuple[str, str], ...], Path] | None = None
    for path in files:
        signature = tuple(
            (name, str(dtype))
            for name, dtype in pl.read_parquet_schema(path).items())
        if baseline is None:
            baseline = (signature, path)
        elif signature != baseline[0]:
            raise QuoteSchemaError(
                f'{data_type} 行情分片 schema 不一致：{path} 与 {baseline[1]} 不同；'
                f'{path} 为 {[n for n, _ in signature]}，'
                f'基准为 {[n for n, _ in baseline[0]]}')
    assert baseline is not None
    signature, _ = baseline
    columns = tuple(name for name, _ in signature)
    schema = dict(signature)
    problems: list[str] = []
    missing = [c for c in expected if c not in schema]
    extra = [c for c in columns if c not in expected]
    if missing:
        problems.append(f'缺列 {missing}')
    if extra:
        problems.append(f'多列 {extra}')
    if tuple(c for c in columns if c in expected) != expected:
        problems.append(f'列顺序不符：实际 {list(columns)}')
    for column, want in QUOTE_DTYPES[data_type].items():
        actual = schema.get(column)
        if actual is not None and actual != want:
            problems.append(f'{column} dtype 期望 {want}，实际 {actual}')
    if problems:
        raise QuoteSchemaError(
            f'{data_type} 行情分片 schema 不符合契约：' + '；'.join(problems))


def assert_formal_quote_schema(
    path: Path | str, frame: pl.DataFrame | pl.LazyFrame, *,
    allow_extra_columns: bool = False,
) -> None:
    """``path`` 落在正式库时校验 schema；其他路径（测试夹具等）直接放行。"""
    data_type = quote_data_type_for_path(path)
    if data_type is None:
        return
    assert_quote_schema(
        data_type, frame, allow_extra_columns=allow_extra_columns)


def assert_formal_quote_glob_schema(
    store_root: Path | str, data_type: str,
    paths: Iterable[str | Path] | str,
) -> None:
    """``store_root`` 是正式库时校验分片 schema；临时目录直接放行。"""
    if not is_formal_quote_root(store_root, data_type):
        return
    assert_quote_glob_schema(data_type, paths)
