#!/usr/bin/env python3

"""基金行情「非成交价」行级门控的回归测试。

背景（2026-11-12）：``quotes/fund/**`` 里混着两类不能当成交价用的行——
上市前的净值序列与源端填的占位桩（``close`` 恒 1.0），另有折算日残留行。
数据红线要求行不删除，只能由读取侧过滤，故引入
:mod:`src.data.quality.fund_price_basis`。

本文件锁住四条口径：

1. 侧车缺失或区间缺类别时必须**抛错**，不得静默放行（DG-27 的反面）；
2. 判定必须能按「标的 + 日期 + 类别」收窄，而不是只有整只开关；
3. 边界必须闭区间：区间末日剔除、次日保留（两行只剔一行的用例守护）；
4. ``enabled=False`` 必须原样返回，用于复现旧口径做对照。
"""

from __future__ import annotations

import datetime as dt
import json
from pathlib import Path

import polars as pl
import pytest

from src.data.quality import fund_price_basis as fpb
from src.data.quality.clean_set import EligibilityEngine

_D = dt.date


def _write_registry(root: Path, spans: list[tuple[str, str, str, int, str]],
                    *, skip_kind: str | None = None) -> None:
    """写一份最小侧车：CSV 区间清单 + JSON 类别映射。

    ``spans`` 每项为 ``(ts_code, start, end, rows, kind)``；``skip_kind`` 指定
    某个区间不在 JSON 里登记类别，用于构造「区间与类别不同步」。
    """
    base = root / 'meta' / 'anomalies'
    base.mkdir(parents=True, exist_ok=True)
    lines = ['ts_code,start,end,rows,total,full']
    kinds: dict[str, str] = {}
    for code, start, end, rows, kind in spans:
        lines.append(f'{code},{start},{end},{rows},9999,false')
        if kind != skip_kind:
            kinds[f'{code}|{start}|{end}'] = kind
    (base / fpb.CSV_NAME).write_text('\n'.join(lines) + '\n', encoding='utf-8')
    (base / fpb.JSON_NAME).write_text(
        json.dumps({'version': 1, 'kinds': kinds}, ensure_ascii=False),
        encoding='utf-8')


@pytest.fixture(autouse=True)
def _reset_cache() -> None:
    fpb.clear_cache()
    yield
    fpb.clear_cache()


def _frame(days: list[dt.date]) -> pl.DataFrame:
    return pl.DataFrame({
        'date': days,
        'close': [1.0] * len(days),
        'volume': [0] * len(days),
    })


# --------------------------------------------------------------------------
# 1. 缺侧车必须报错，不得放行
# --------------------------------------------------------------------------

def test_missing_registry_raises(tmp_path: Path) -> None:
    with pytest.raises(fpb.PseudoRegistryError, match='缺少基金伪数据侧车清单'):
        fpb.load_spans(tmp_path)
    # 接口层同样必须是报错，而不是返回 False / 原样返回
    with pytest.raises(fpb.PseudoRegistryError):
        fpb.is_pseudo(tmp_path, '160311.SZ', _D(2007, 5, 11))
    with pytest.raises(fpb.PseudoRegistryError):
        fpb.filter_tradable(_frame([_D(2007, 5, 11)]), tmp_path, '160311.SZ')


def test_only_csv_present_raises(tmp_path: Path) -> None:
    """只有 CSV 没有类别映射时同样报错，避免「有区间但不知类别」被当成可用。"""
    _write_registry(tmp_path, [('160311.SZ', '2007-05-11', '2007-05-11', 1,
                                'nav_series')])
    (tmp_path / 'meta' / 'anomalies' / fpb.JSON_NAME).unlink()
    with pytest.raises(fpb.PseudoRegistryError, match='缺少基金伪数据侧车清单'):
        fpb.load_spans(tmp_path)


def test_kind_desync_raises(tmp_path: Path) -> None:
    """CSV 有区间、JSON 没有对应类别 → 报错（不允许默认按某类处理）。"""
    _write_registry(
        tmp_path,
        [('160603.SZ', '2004-05-10', '2004-05-14', 5, 'nav_series'),
         ('163503.SZ', '2007-10-15', '2007-10-16', 2, 'converted_leftover')],
        skip_kind='converted_leftover',
    )
    with pytest.raises(fpb.PseudoRegistryError, match='缺少合法类别'):
        fpb.load_spans(tmp_path)


# --------------------------------------------------------------------------
# 2. 闭区间边界：两行只能剔一行
# --------------------------------------------------------------------------

def test_two_rows_one_misjudged(tmp_path: Path) -> None:
    """区间末日必须剔除、次日必须保留。

    这就是「两行判错一行」的守护：把过滤写成 ``<`` / ``>`` 或按自然月扩大
    一天，本用例会剩 0 行或 2 行，与期望的 1 行不符。
    """
    _write_registry(tmp_path, [('160311.SZ', '2007-05-21', '2007-05-25', 5,
                                'nav_series')])
    last_pseudo = _D(2007, 5, 25)
    first_real = _D(2007, 5, 28)
    out = fpb.filter_tradable(_frame([last_pseudo, first_real]), tmp_path,
                              '160311.SZ')
    assert out.height == 1, f'应只剔 1 行，实际剔了 {2 - out.height} 行'
    assert out['date'].to_list() == [first_real]


def test_span_boundaries_are_closed(tmp_path: Path) -> None:
    _write_registry(tmp_path, [('160311.SZ', '2007-05-11', '2007-05-26', 12,
                                'nav_series')])
    days = [_D(2007, 5, 10), _D(2007, 5, 11), _D(2007, 5, 26), _D(2007, 5, 27)]
    out = fpb.filter_tradable(_frame(days), tmp_path, '160311.SZ')
    assert out['date'].to_list() == [_D(2007, 5, 10), _D(2007, 5, 27)]


# --------------------------------------------------------------------------
# 3. 类别收窄
# --------------------------------------------------------------------------

def test_is_pseudo_by_kind(tmp_path: Path) -> None:
    _write_registry(tmp_path, [
        ('163503.SZ', '2005-11-21', '2007-07-27', 400, 'placeholder'),
        ('163503.SZ', '2007-10-15', '2007-10-16', 2, 'converted_leftover'),
    ])
    placeholder_day = _D(2006, 1, 5)
    leftover_day = _D(2007, 10, 16)
    real_day = _D(2007, 10, 17)

    assert fpb.is_pseudo(tmp_path, '163503.SZ', placeholder_day)
    assert fpb.is_pseudo(tmp_path, '163503.SZ', leftover_day)
    assert not fpb.is_pseudo(tmp_path, '163503.SZ', real_day)

    # 只判折算残留时，占位桩日不命中；反之亦然
    assert fpb.is_pseudo(tmp_path, '163503.SZ', leftover_day,
                         kinds=('converted_leftover',))
    assert not fpb.is_pseudo(tmp_path, '163503.SZ', placeholder_day,
                             kinds=('converted_leftover',))
    assert not fpb.is_pseudo(tmp_path, '163503.SZ', leftover_day,
                             kinds=('placeholder',))


def test_filter_by_kind_keeps_other_kinds(tmp_path: Path) -> None:
    _write_registry(tmp_path, [
        ('163503.SZ', '2005-11-21', '2007-07-27', 400, 'placeholder'),
        ('163503.SZ', '2007-10-15', '2007-10-16', 2, 'converted_leftover'),
    ])
    days = [_D(2006, 1, 5), _D(2007, 10, 16), _D(2007, 10, 17)]
    out = fpb.filter_tradable(_frame(days), tmp_path, '163503.SZ',
                              kinds=('converted_leftover',))
    assert out['date'].to_list() == [_D(2006, 1, 5), _D(2007, 10, 17)]


def test_first_tradable_day(tmp_path: Path) -> None:
    _write_registry(tmp_path, [
        ('163503.SZ', '2005-11-21', '2007-07-27', 400, 'placeholder'),
        ('163503.SZ', '2007-10-15', '2007-10-16', 2, 'converted_leftover'),
    ])
    assert fpb.first_tradable_day(tmp_path, '163503.SZ') == _D(2007, 10, 17)
    assert fpb.first_tradable_day(tmp_path, '999999.SZ') is None


# --------------------------------------------------------------------------
# 4. 开关：enabled=False 复现旧行为
# --------------------------------------------------------------------------

def test_disabled_returns_original(tmp_path: Path) -> None:
    _write_registry(tmp_path, [('160311.SZ', '2007-05-11', '2007-05-26', 12,
                                'nav_series')])
    days = [_D(2007, 5, 11), _D(2007, 5, 26), _D(2007, 5, 28)]
    frame = _frame(days)
    out = fpb.filter_tradable(frame, tmp_path, '160311.SZ', enabled=False)
    assert out.equals(frame)


def test_unknown_code_passthrough(tmp_path: Path) -> None:
    """侧车存在但该标的没有任何区间时原样返回（不是报错）。"""
    _write_registry(tmp_path, [('160311.SZ', '2007-05-11', '2007-05-26', 12,
                                'nav_series')])
    frame = _frame([_D(2024, 1, 2)])
    assert fpb.filter_tradable(frame, tmp_path, '159915.SZ').equals(frame)


# --------------------------------------------------------------------------
# 5. clean_set 接入：只建索引的轻量验证（不跑整库，避免 70s 构造成本）
# --------------------------------------------------------------------------

def _bare_engine(root: Path, gate: bool) -> EligibilityEngine:
    engine = EligibilityEngine.__new__(EligibilityEngine)
    engine.root = root
    engine.gate_pseudo_rows = gate
    engine._load_pseudo_gate()
    return engine


def test_clean_set_gate_loads_ordinals(tmp_path: Path) -> None:
    _write_registry(tmp_path, [
        ('160311.SZ', '2007-05-21', '2007-05-25', 5, 'nav_series'),
        ('163503.SZ', '2007-10-15', '2007-10-16', 2, 'converted_leftover'),
    ])
    engine = _bare_engine(tmp_path, True)
    assert engine.pseudo_gate_status == 'on'
    assert _D(2007, 5, 25).toordinal() in engine._pseudo_ordinals['160311.SZ']
    assert _D(2007, 5, 28).toordinal() not in engine._pseudo_ordinals['160311.SZ']
    # 折算残留与占位桩同表，两类都要进同一份序数集合
    assert _D(2007, 10, 16).toordinal() in engine._pseudo_ordinals['163503.SZ']


def test_clean_set_gate_off_and_missing(tmp_path: Path) -> None:
    engine = _bare_engine(tmp_path, False)
    assert engine.pseudo_gate_status == 'off'
    assert engine._pseudo_ordinals == {}
    # 侧车缺失：不得静默当成真实数据，状态必须显式可见
    engine = _bare_engine(tmp_path, True)
    assert engine.pseudo_gate_status.startswith('missing:')
    assert engine._pseudo_ordinals == {}


# --------------------------------------------------------------------------
# 6. 真实侧车实测（不写盘，只读 data/local_store）
# --------------------------------------------------------------------------

def test_real_registry_spans() -> None:
    root = Path('data') / 'local_store'
    if not (root / 'meta' / 'anomalies' / fpb.JSON_NAME).is_file():
        pytest.skip('正式侧车不存在')
    # 160311：上市前 16 行净值序列，最后一段到 2007-05-25，05-28 上市
    assert fpb.is_pseudo(root, '160311.SZ', _D(2007, 5, 11),
                         kinds=('nav_series',))
    assert not fpb.is_pseudo(root, '160311.SZ', _D(2007, 5, 28))
    # 163503：折算残留只有 10-15/16 两天
    assert fpb.is_pseudo(root, '163503.SZ', _D(2007, 10, 16),
                         kinds=('converted_leftover',))
    assert not fpb.is_pseudo(root, '163503.SZ', _D(2007, 10, 17))
    # 510070.SH：最后真实成交 2020-06-24、文件止于 2020-08-12，尾段 33 行 close≡2.177
    # 为退市后结转（非真实停牌），2026-11-12 已补登
    assert fpb.is_pseudo(root, '510070.SH', _D(2020, 7, 1))
    # 全库合计 = CSV 登记的 25,770 行（2026-11-12 补登 130 段 / 2,732 行后；
    # 侧车覆盖不止 5 只焦点基金），并与 registry 自述的 registered_rows_after 对账
    spans_all = fpb.load_spans(root)
    total = sum(s.rows for spans in spans_all.values() for s in spans)
    assert total == 25770, total
    registry = json.loads((root / 'meta' / 'anomalies' / fpb.JSON_NAME).read_text(encoding='utf-8'))
    assert registry['registered_rows_after'] == total, (registry['registered_rows_after'], total)
    # 5 只焦点基金合计 2,748 行（G-062~G-066 的行级精确复算值）
    focus = ('160311.SZ', '160314.SZ', '160603.SZ', '163302.SZ', '163503.SZ')
    focus_rows = sum(s.rows for code in focus for s in spans_all.get(code, []))
    assert focus_rows == 2748, focus_rows
