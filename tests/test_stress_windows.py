#!/usr/bin/env python3

"""
四压力窗口回归集（V-34）：用本机真实行情复核撮合层的三条硬约束。

覆盖窗口（每个窗口都对应一次真实的 A 股流动性/涨跌停极端情形）：

- ``2015-07``：股灾，千股跌停与停牌潮；
- ``2018-10``：连续下跌至 2449 点；
- ``2020-02``：春节后首日集体跌停；
- ``2024-02``：小微盘流动性危机。

每个窗口断言三件事，三条都必须对窗口内**每一笔**成交成立：

1. 成交价不越当日 ``[low, high]``（撮合不得凭空造价）；
2. 涨停不可买：买入价必须严格低于按前收推导的涨停价；
3. 跌停不可卖：卖出价必须严格高于按前收推导的跌停价。

数据只读：只走 ``LocalQuoteStore.read_daily``，不写任何文件、不落 staging。
本机取不到某个窗口的数据时，用 ``pytest.mark.skipif`` 显式跳过并把原因写进
skip reason；取到数据但撮合未触发成交时不做降级断言——三条约束仍是硬断言，
不因为样本为 0 而放宽（DG-26）。

标的按窗口配置（见 ``WINDOWS`` 第四项）：每个窗口 5 只，覆盖沪主板 / 深主板 /
创业板 / 科创板 / 小盘股，且都必须在该窗口之前已上市（2015、2018 两个窗口
不含科创板与 301 号段，它们当时尚未开板）。``2024-02`` 的小微盘流动性危机
只在小盘股上体现，故该窗口整组换成创业板与深市小盘代码。

窗口内是否成交取决于策略，本用例不强制窗口内必须有成交；它保证的是
「凡在窗口内成交者必满足上述三条」。合成数据上的确定性涨跌停用例见
``tests/test_execution_core_consistency.py``，两者互补：
那边证规则本身，这边证规则在真实行情上不被绕过。
"""

from __future__ import annotations

from datetime import date, timedelta

import polars as pl
import pytest

from src.backtest.engine.input_contract import OUT_DATE
from src.backtest.engine.portfolio_backtest_engine import PortfolioBacktestEngine
from src.backtest.strategies.ma_strategy import MAStrategy
from src.data.local_store import get_local_quote_store
from src.data.local_store.expectation import MissingLocalDataError
from src.risk.execution_rules import ExecutionRules
from src.utils.config import get_config
from src.utils.dates import as_date

#: 价格空间比较的浮点噪声容差（元），与 ExecutionRules.PRICE_EPS 同口径：
#: 只吸收双精度误差，不是「允许越界半个点」的业务容差。
EPS = ExecutionRules.PRICE_EPS

#: 均线窗口：短周期足以在压力窗口内反复翻转，产生足够多的撮合样本。
MA_SHORT = 5
MA_LONG = 20

#: 预热点数（自然日）。均线需要 ``MA_LONG`` 根 K 线预热，多取一些冗余。
WARMUP_DAYS = 200

#: 窗口定义：key → (起始日, 结束日, 情形说明, 标的元组)。区间取整个自然月，
#: 与窗口名一致；只截取危机的某几天会让均线无翻转、样本退化为 0。
#: 标的 = 10 只、按成交额分三档覆盖大盘 / 中盘 / 小盘，且都只放该时点已上市的
#: 代码（科创板 2019-07 开板、301 号段 2021 才有，故 2015 / 2018 两个窗口不含）。
#: 为什么是 10 只而不是 4~5 只：策略是状态型均线信号，单只标的单月通常只翻转
#: 1~2 次。实测 5 只时四窗口只有 7 / 8 / 7 / 2 笔，8 只时 10 / 8 / 15 / 9 笔，
#: 10 只才让四个窗口都稳定过 10 笔——样本量不足的压力回归等于没回归。
#: ``2024-02`` 的小微盘流动性危机只在小盘股上体现，故该窗口整组换成创业板与
#: 深市小盘代码（成交额 0.3~1.2 亿）。
WINDOWS: dict[str, tuple[date, date, str, tuple[str, ...]]] = {
    '2015-07': (date(2015, 7, 1), date(2015, 7, 31), '2015-07 股灾：千股跌停与停牌潮',
                ('600000.SH', '000001.SZ', '601808.SH', '600125.SH', '300124.SZ',
                 '002001.SZ', '000753.SZ', '002732.SZ', '000089.SZ', '601069.SH')),
    '2018-10': (date(2018, 10, 1), date(2018, 10, 31), '2018-10 连续下跌至 2449 点',
                ('000001.SZ', '600000.SH', '601233.SH', '601138.SH', '002001.SZ',
                 '300124.SZ', '002365.SZ', '600736.SH', '000672.SZ', '601069.SH')),
    '2020-02': (date(2020, 2, 1), date(2020, 2, 29), '2020-02 春节后首日集体跌停',
                ('000001.SZ', '601138.SH', '600000.SH', '300001.SZ', '002001.SZ',
                 '688001.SH', '600850.SH', '002732.SZ', '601916.SH', '601069.SH')),
    '2024-02': (date(2024, 2, 1), date(2024, 2, 29), '2024-02 小微盘流动性危机',
                ('301000.SZ', '301063.SZ', '301129.SZ', '301277.SZ', '003000.SZ',
                 '003005.SZ', '001201.SZ', '001218.SZ', '301201.SZ', '003010.SZ')),
}

WINDOW_KEYS = tuple(WINDOWS)


def _window_data_available(codes: tuple[str, ...]) -> tuple[bool, str]:
    """窗口级数据可用性探针：只做路径存在性检查，不读任何 parquet。

    为什么不用 ``read_daily`` 探针：``pytest --collect-only`` 也会执行模块级
    代码，若在收集期读行情就会把「只是列用例」变成一次真实取数。这里只
    ``Path.is_file()``（一次 stat 调用），真正的数据读取留到用例体内。
    """
    try:
        store = get_local_quote_store(get_config())
    except Exception as exc:  # noqa: BLE001 —— 探针失败只影响跳过判定，不吞用例异常
        return False, f'本地行情库初始化失败：{type(exc).__name__}: {exc}'
    if not getattr(store, 'enabled', False):
        return False, '本地行情库未启用（local_store.enabled=false）'
    for code in codes:
        try:
            if store.quote_path('stock', code).is_file():
                return True, ''
        except Exception as exc:  # noqa: BLE001
            return False, f'行情路径解析失败：{type(exc).__name__}: {exc}'
    return False, f'{" / ".join(codes)} 均无本地日线 parquet'


_AVAILABILITY = {key: _window_data_available(item[3]) for key, item in WINDOWS.items()}


def _param(window: str):
    """按窗口生成参数化项；缺数据时挂 skipif 并把原因写进 reason。"""
    start, end, desc, codes = WINDOWS[window]
    available, why = _AVAILABILITY[window]
    reason = f'{window}（{desc}）本机无窗口数据：{why}'
    marks = pytest.mark.skipif(not available, reason=reason)
    return pytest.param(window, id=window, marks=marks)


def _normalize(frame: pl.DataFrame) -> pl.DataFrame:
    """统一列名与 dtype：``vol`` → ``volume``、date 转 pl.Date、按日升序。

    其余列（``raw_open`` / ``raw_close`` / ``total_mv_yi`` / ``category`` 等）
    原样保留：涨跌停判定在引擎里优先用原始价（``_select_check_prices``），
    本用例必须与引擎读同一层价格，不能擅自丢列改口径。
    """
    if 'volume' not in frame.columns and 'vol' in frame.columns:
        frame = frame.rename({'vol': 'volume'})
    return frame.with_columns(pl.col('date').cast(pl.Date)).sort('date')


def _check_trade(trade: dict, *, code: str, panel: pl.DataFrame,
                 positions: dict, rules: ExecutionRules, label: str) -> int:
    """对单笔成交校验三条硬约束；任一不成立即失败。

    返回是否真正执行了涨跌停比对（面板首根无前收、前收非正时为 0），
    供用例汇总「实际比对了几笔」。
    """
    trade_day = as_date(trade[OUT_DATE])
    # 成交记录的方向键是 'signal'（'buy'/'sell'），不是输入契约的 OUT_SIDE
    side = trade['signal']
    price = float(trade['price'])
    where = f'{label} {code} {trade_day} {side} price={price}'

    idx = positions.get(trade_day)
    assert idx is not None, f'{where}：成交日不在行情面板内'

    low = float(panel['low'][idx])
    high = float(panel['high'][idx])
    assert low - EPS <= price <= high + EPS, (
        f'{where}：成交价越出当日 [{low}, {high}] 区间')

    # 涨跌停判定用的价格层必须与引擎一致：面板带原始价时优先原始价。
    has_raw = 'raw_open' in panel.columns and 'raw_close' in panel.columns
    close_col = 'raw_close' if has_raw else 'close'
    if idx == 0:
        return 0  # 面板首根没有前收盘价，引擎同样不做涨跌停判定
    prev_close = float(panel[close_col][idx - 1])
    if not (prev_close > 0.0):
        return 0

    limit = rules.resolve_limit_pct(code, None, trade_date=trade_day)
    if side == 'buy':
        limit_up = rules.resolve_limit_price(prev_close, limit, 'buy')
        assert price < limit_up - EPS, (
            f'{where}：涨停价 {limit_up}（前收 {prev_close}，幅度 {limit}）仍成交，'
            f'违反「涨停不可买」')
    else:
        limit_down = rules.resolve_limit_price(prev_close, limit, 'sell')
        assert price > limit_down + EPS, (
            f'{where}：跌停价 {limit_down}（前收 {prev_close}，幅度 {limit}）仍成交，'
            f'违反「跌停不可卖」')
    return 1


@pytest.mark.parametrize('window', [_param(key) for key in WINDOW_KEYS])
def test_stress_window_fill_constraints(window: str) -> None:
    """压力窗口内每一笔成交都不得越界、不得逆涨跌停成交。"""
    start, end, desc, codes = WINDOWS[window]
    label = f'[{window}]'
    store = get_local_quote_store(get_config())
    read_start = start - timedelta(days=WARMUP_DAYS)

    panels: dict[str, pl.DataFrame] = {}
    missing: list[str] = []
    for code in codes:
        try:
            frame = store.read_daily('stock', code, read_start, end)
        except (MissingLocalDataError, FileNotFoundError) as exc:
            missing.append(f'{code}（{type(exc).__name__}）')
            continue
        if frame is None or frame.is_empty():
            missing.append(f'{code}（区间无行情行）')
            continue
        panels[code] = _normalize(frame)

    if not panels:
        pytest.skip(f'{label} {desc} 本机无可用行情：{"; ".join(missing)}')

    engine = PortfolioBacktestEngine(panels)
    for code in panels:
        engine.set_strategy(code, MAStrategy(MA_SHORT, MA_LONG))
    result = engine.run_backtest(initial_capital=1_000_000.0)

    trades = result.get('trades') or []
    # 引擎必须真的跑起来并产生撮合，否则本用例退化为空转
    assert trades, f'{label} {desc} 回测未产生任何成交，无法验证撮合约束'

    rules = ExecutionRules()
    checked = 0
    limit_checked = 0
    for trade in trades:
        code = trade['stock_code']
        panel = panels.get(code)
        if panel is None:
            continue
        trade_day = as_date(trade[OUT_DATE])
        if trade_day is None or trade_day < start or trade_day > end:
            continue
        positions = {as_date(d): i for i, d in enumerate(panel['date'].to_list())}
        limit_checked += _check_trade(trade, code=code, panel=panel,
                                      positions=positions, rules=rules, label=label)
        checked += 1

    # 窗口前有 200 天预热，窗口内成交在面板里必定不是首根，故每笔都应完成
    # 涨跌停比对；若某笔被跳过，说明预热不足或面板缺行，需先修数据而非放宽断言。
    assert limit_checked == checked, (
        f'{label} 窗口成交 {checked} 笔，但只有 {limit_checked} 笔完成涨跌停比对')

    # 窗口内一笔都没成交时不做降级断言：三条约束仍然成立（空集），
    # 但要在用例报告里可见「本窗口未触发撮合」。
    if checked == 0:
        pytest.skip(f'{label} {desc} 窗口内未触发成交（策略在 {start}~{end} 无信号翻转），'
                    f'本窗口不构成有效样本（全样本 {len(trades)} 笔均在该区间外）')
