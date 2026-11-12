"""通达信 .day 成交量「按手存储」量级判据（B1S1-02）。

修订背景：旧实现只在基金分支识别「成交量字段已被通达信写成手」，个股分支
一律按「股 ÷ 100」处理；沪市大盘股当日成交量超过 uint32 上限（2^32-1 股）后
同样被写成手，导致解析结果比真实手数小 100 倍（全量实测 19 行）。

新判据（个股与基金共用，不按基金/股票分叉）：
  ① raw × 100 > 2^32-1 × 0.995（量级已到溢出边界）
  ② amount / (close × raw) > 50（成交额反算说明原值已是手）
两条同时成立才保留原值，否则仍按「股/份 ÷ 100」。指数（本就像手）与
板块指数/国债逆回购（成交额语义不同）不套该规则。
"""

import struct

import pytest

from src.data.tdx_day_parser import (
    is_a_share_stock,
    is_fund_file,
    is_index_file,
    parse_day_file,
    read_latest_quote,
)


def _write_raw(path, records):
    """按真实 32 字节布局写记录，volume 直接给出 uint32 原值（不做换算）。"""
    mul = 1000 if is_fund_file(path) else 100   # 基金价格 3 位小数（×1000）
    with open(path, 'wb') as f:
        for day, open_, high, low, close, amount, raw_volume in records:
            f.write(struct.pack(
                '<IIIIIfII',
                int(day), int(open_ * mul), int(high * mul), int(low * mul),
                int(close * mul), float(amount), int(raw_volume), 0,
            ))


def test_fund_hand_row_keeps_raw_value(tmp_path):
    """基金原路径不变：溢出按手存储的基金行仍原样保留（旧口径同样如此）。"""
    price = 3.0
    hands = 50_000_000                      # 50,000,000 手 = 50 亿份 > uint32
    amount = price * hands * 100
    path = tmp_path / 'sh513060.day'
    _write_raw(path, [(20241120, price, price, price, price, amount, hands)])

    df = parse_day_file(path)

    assert df['volume'][0] == pytest.approx(hands)
    assert df['amount'][0] / (df['close'][0] * df['volume'][0] * 100) == pytest.approx(1.0)


def test_fund_share_row_still_divided_by_100(tmp_path):
    """基金常规行（份）仍按 ÷100 折成手。"""
    price = 4.343
    shares = 1_000_000
    amount = price * shares
    path = tmp_path / 'sh510300.day'
    _write_raw(path, [(20240103, price, price, price, price, amount, shares)])

    df = parse_day_file(path)

    assert df['volume'][0] == pytest.approx(shares / 100)


def test_stock_uint32_overflow_hand_row_restored(tmp_path):
    """个股成交量溢出 uint32 后按手存储，必须原样保留（不得再 ÷100）。

    取值取自真实数据 sh601288 2015-07-06：raw=43,720,880 手、
    close=3.86、amount≈1.6876e10，反算 amount/(close*raw)≈100。
    """
    price = 3.86
    hands = 43_720_880
    amount = price * hands * 100
    path = tmp_path / 'sh601288.day'
    _write_raw(path, [(20150706, price, price, price, price, amount, hands)])

    df = parse_day_file(path)

    assert df['volume'][0] == pytest.approx(hands)
    # 量纲契约：amount ≈ close × volume(手) × 100
    assert df['amount'][0] / (df['close'][0] * df['volume'][0] * 100) == pytest.approx(1.0)


def test_stock_normal_row_still_divided_by_100(tmp_path):
    """未溢出的普通股仍按「股 ÷ 100」解析，证明判据没有放宽。"""
    price = 10.0
    shares = 1_000_000
    amount = price * shares
    path = tmp_path / 'sh600000.day'
    _write_raw(path, [(20240103, price, price, price, price, amount, shares)])

    df = parse_day_file(path)

    assert df['volume'][0] == pytest.approx(shares / 100)
    assert df['amount'][0] / (df['close'][0] * df['volume'][0]) == pytest.approx(100.0)


def test_stock_hand_like_ratio_without_overflow_not_restored(tmp_path):
    """反算≈100 但量级远未到 uint32 上限的历史行，不得判为手。

    取值取自真实数据 sz000001 1995-11-17：raw=4,600、close=7.17、
    amount≈3,330,000，反算≈101，但 raw×100 仅 460,000。若只看反算会把
    1990 年代大量普通行放大 100 倍。
    """
    price = 7.17
    raw = 4_600
    amount = price * raw * 100.9
    path = tmp_path / 'sz000001.day'
    _write_raw(path, [(19951117, price, price, price, price, amount, raw)])

    df = parse_day_file(path)

    assert df['volume'][0] == pytest.approx(raw / 100)


def test_index_volume_kept_as_is(tmp_path):
    """指数文件成交量本来就是手，判据不参与（量级虽大但语义不同）。"""
    close = 3735.37
    hands = 408_693_062
    amount = 699_969_765_376.0
    path = tmp_path / 'sh000001.day'
    _write_raw(path, [(20261020, close, close, close, close, amount, hands)])

    df = parse_day_file(path)

    assert df['volume'][0] == pytest.approx(hands)


def test_non_a_share_segment_not_restored(tmp_path):
    """板块指数（880xxx）成交额语义不同，即使满足量级与反算也不得改口径。"""
    close = 1000.0
    raw = 50_000_000
    amount = close * raw * 100
    path = tmp_path / 'sh880001.day'
    _write_raw(path, [(20240103, close, close, close, close, amount, raw)])

    df = parse_day_file(path)

    assert df['volume'][0] == pytest.approx(raw / 100)


def test_read_latest_quote_uses_same_volume_rule(tmp_path):
    """快照读取路径（read_recent_records）与全量解析共用同一判据。"""
    price = 2.91
    hands = 51_712_529
    amount = price * hands * 100
    path = tmp_path / 'sh600010.day'
    _write_raw(path, [(20251014, price, price, price, price, amount, hands)])

    quote = read_latest_quote(path)

    assert quote is not None
    assert quote.volume == pytest.approx(hands)


def test_is_a_share_stock_segments():
    assert is_a_share_stock('sh600000.day')
    assert is_a_share_stock('sz000001.day')
    assert is_a_share_stock('bj430047.day')
    assert is_a_share_stock('sz200011.day')       # 深市 B 股
    assert not is_a_share_stock('sh000001.day')   # 上证指数
    assert not is_a_share_stock('sz399001.day')   # 深证指数
    assert not is_a_share_stock('sh999997.day')   # 通达信自编指数段
    assert not is_a_share_stock('sh880001.day')   # 板块指数
    assert not is_a_share_stock('sh204001.day')   # 国债逆回购
    assert not is_a_share_stock('sh510300.day')   # 基金（由 is_fund_file 覆盖）


def test_segment_helpers_are_disjoint():
    """指数 / 基金 / A 股个股三段互斥，避免判据叠加。"""
    for name in ('sh000001.day', 'sz399001.day', 'sh510300.day',
                 'sh600000.day', 'sh880001.day', 'sh204001.day'):
        flags = (is_index_file(name), is_fund_file(name), is_a_share_stock(name))
        assert sum(flags) <= 1, name
