"""AdjFactorCalculator 覆盖补齐：公式边界、取数路径、写库路径与批量流程。

除公式外，全部用 sqlite 内存库 + 假通达信 handler，不依赖网络、MySQL 或本地列存。
核心正确性断言：前复权后的除权前一日收盘价 == 交易所标准除权参考价。
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import date, datetime

import polars as pl
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.data import adj_factor_calculator as afc
from src.data.adj_factor_calculator import (
    AdjFactorCalculator,
    calc_dividend_event_factor,
)
from src.database.models.stock import StockAdjFactor, StockDaily, StockDividend

CODE = '600000.SH'
D1, D2, D3, D4 = date(2024, 6, 3), date(2024, 6, 4), date(2024, 6, 5), date(2024, 6, 6)


# ---------------------------------------------------------------- 桩与夹具
class _DbManager:
    """最小 db_manager：get_session + transaction（成功提交、异常回滚）。"""

    def __init__(self, session):
        self._session = session

    def get_session(self):
        return self._session

    @contextmanager
    def transaction(self):
        try:
            yield self._session
            self._session.commit()
        except Exception:
            self._session.rollback()
            raise


class _Tdx:
    """假通达信 handler：按构造参数返回 LazyFrame / DataFrame / list / 异常。"""

    def __init__(self, result=None, exc: Exception | None = None):
        self.result = result
        self.exc = exc
        self.calls: list[tuple[str, str, str]] = []

    def get_kline_data(self, symbol, start, end):
        self.calls.append((symbol, start, end))
        if self.exc is not None:
            raise self.exc
        return self.result


@pytest.fixture
def session():
    engine = create_engine('sqlite:///:memory:')
    for model in (StockDaily, StockDividend, StockAdjFactor):
        model.__table__.create(engine)
    db_session = sessionmaker(bind=engine)()
    yield db_session
    db_session.close()
    engine.dispose()


def _bars(closes=(10.0, 10.0, 9.0, 9.2), dates=(D1, D2, D3, D4)) -> list[dict]:
    return [{'date': d, 'open': c, 'high': c, 'low': c, 'close': c}
            for d, c in zip(dates, closes, strict=True)]


def _seed_daily(session, code=CODE, closes=(10.0, 10.0, 9.0, 9.2)):
    for d, c in zip((D1, D2, D3, D4), closes, strict=True):
        session.add(StockDaily(ts_code=code, trade_date=d, open=c, high=c,
                               low=c, close=c, qfq_factor=1.0, hfq_factor=1.0))
    session.commit()


def _seed_dividend(session, code=CODE, ex_date=D3, cash=0.5, share=0.0,
                   ratio=0.0, price=0.0, cash_fix=None, share_fix=None):
    session.add(StockDividend(
        ts_code=code, ex_date=ex_date, cash_div=cash, share_div=share,
        rights_issue_ratio=ratio, rights_issue_price=price,
        cash_div_fix=cash_fix, share_div_fix=share_fix))
    session.commit()


def _calc(session, tdx=None) -> AdjFactorCalculator:
    return AdjFactorCalculator(_DbManager(session), tdx_handler=tdx)


# ---------------------------------------------------------------- 单事件公式
@pytest.mark.parametrize('prev_close', [None, 0, -1.0])
def test_event_factor_rejects_invalid_prev_close(prev_close):
    assert calc_dividend_event_factor(prev_close, 0.5, 0.0) is None


def test_event_factor_none_when_no_corporate_action():
    assert calc_dividend_event_factor(10.0, None, None, None, None) is None


def test_event_factor_rejects_zero_denominator():
    # share_div = -1 → 分母 prev_close × 0
    assert calc_dividend_event_factor(10.0, 0.0, -1.0) is None


@pytest.mark.parametrize('cash,share,max_factor', [
    (10.0, 0.0, 10.0),    # 派现 ≥ 前收 → 因子 ≤ 0
    (12.0, 0.0, 10.0),
    (0.0, -0.95, 10.0),   # 反向折算因子 20 > 上限 10
])
def test_event_factor_rejects_out_of_range(cash, share, max_factor):
    assert calc_dividend_event_factor(10.0, cash, share,
                                      max_factor=max_factor) is None


def test_event_factor_max_factor_can_be_relaxed_for_fund_split():
    factor = calc_dividend_event_factor(1.0, 0.0, -0.95, max_factor=100.0)
    assert factor == pytest.approx(20.0)


@pytest.mark.parametrize('prev,cash,share,ratio,price', [
    (10.0, 0.5, 0.0, 0.0, 0.0),     # 纯派现
    (10.0, 0.0, 0.3, 0.0, 0.0),     # 纯送转
    (10.0, 0.2, 0.5, 0.0, 0.0),     # 派现 + 送转
    (12.0, 0.0, 0.0, 0.3, 6.0),     # 纯配股
    (15.0, 0.3, 0.2, 0.25, 8.0),    # 派现 + 送转 + 配股
])
def test_event_factor_matches_exchange_ex_rights_reference(prev, cash, share, ratio, price):
    """沪深交易所除权参考价：(前收 − 派现 + 配股价×配股比例) ÷ (1 + 送转 + 配股比例)。"""
    reference = (prev - cash + price * ratio) / (1 + share + ratio)
    factor = calc_dividend_event_factor(prev, cash, share, ratio, price)
    assert factor == pytest.approx(reference / prev, rel=1e-12)


# ---------------------------------------------------------------- calculate_factors 端到端口径
def test_qfq_adjusted_prev_close_equals_ex_rights_reference():
    prices = pl.DataFrame({'ts_code': [CODE] * 4, 'trade_date': [D1, D2, D3, D4],
                           'open': [10.0, 10.0, 7.4, 7.5],
                           'close': [10.0, 10.0, 7.4, 7.5]})
    dividends = pl.DataFrame({'ts_code': [CODE], 'ex_date': [D3],
                              'cash_div': [0.3], 'share_div': [0.3],
                              'rights_issue_ratio': [0.0], 'rights_issue_price': [0.0]})
    out = AdjFactorCalculator.calculate_factors(prices, dividends)
    reference = (10.0 - 0.3) / 1.3
    adjusted_prev = out['close'][1] * out['qfq_factor'][1]
    assert adjusted_prev == pytest.approx(reference, rel=1e-6)
    # 除权日及之后 qfq = 1，除权日之前 hfq = 1
    assert out['qfq_factor'].to_list()[2:] == pytest.approx([1.0, 1.0])
    assert out['hfq_factor'].to_list()[:2] == pytest.approx([1.0, 1.0])
    # hfq 与 qfq 的比值在全序列上恒定（= 全部事件因子之积）
    ratios = [q / h for q, h in zip(out['qfq_factor'], out['hfq_factor'], strict=True)]
    assert max(ratios) == pytest.approx(min(ratios), rel=1e-9)


def test_consecutive_events_multiply():
    prices = pl.DataFrame({'ts_code': [CODE] * 4, 'trade_date': [D1, D2, D3, D4],
                           'open': [10.0, 9.5, 9.0, 8.5],
                           'close': [10.0, 9.5, 9.0, 8.5]})
    dividends = pl.DataFrame({'ts_code': [CODE, CODE], 'ex_date': [D2, D4],
                              'cash_div': [0.5, 0.0], 'share_div': [0.0, 0.2],
                              'rights_issue_ratio': [0.0, 0.0],
                              'rights_issue_price': [0.0, 0.0]})
    out = AdjFactorCalculator.calculate_factors(prices, dividends)
    f1 = (10.0 - 0.5) / 10.0
    f2 = 1.0 / 1.2
    assert out['qfq_factor'].to_list() == pytest.approx([f1 * f2, f2, f2, 1.0])
    assert out['hfq_factor'].to_list() == pytest.approx([1.0, 1 / f1, 1 / f1, 1 / (f1 * f2)])


def test_all_events_invalid_returns_unit_factors():
    """事件全部被公式拒绝（派现超过前收）时退回全 1，不产出异常因子。"""
    prices = pl.DataFrame({'ts_code': [CODE] * 2, 'trade_date': [D1, D2],
                           'open': [1.0, 1.0], 'close': [1.0, 1.0]})
    dividends = pl.DataFrame({'ts_code': [CODE], 'ex_date': [D2],
                              'cash_div': [5.0], 'share_div': [0.0],
                              'rights_issue_ratio': [0.0], 'rights_issue_price': [0.0]})
    out = AdjFactorCalculator.calculate_factors(prices, dividends)
    assert out['qfq_factor'].to_list() == [1.0, 1.0]
    assert out['hfq_factor'].to_list() == [1.0, 1.0]


def test_unsorted_inputs_are_sorted_before_calculation():
    prices = pl.DataFrame({'ts_code': [CODE] * 3, 'trade_date': [D3, D1, D2],
                           'open': [9.0, 10.0, 10.0], 'close': [9.0, 10.0, 10.0]})
    dividends = pl.DataFrame({'ts_code': [CODE] * 2, 'ex_date': [D3, None],
                              'cash_div': [1.0, 0.3], 'share_div': [0.0, 0.0],
                              'rights_issue_ratio': [0.0, 0.0],
                              'rights_issue_price': [0.0, 0.0]})
    out = AdjFactorCalculator.calculate_factors(prices, dividends)
    assert out['trade_date'].to_list() == [D1, D2, D3]
    assert out['qfq_factor'].to_list() == pytest.approx([0.9, 0.9, 1.0])


# ---------------------------------------------------------------- 取数：数据库
def test_db_path_reads_dividends_with_fix_columns(session):
    _seed_daily(session)
    _seed_dividend(session, cash=0.5, share=0.56416, share_fix=0.0)
    out = _calc(session).calculate_adj_factors(CODE, use_tdx=False)
    assert out is not None and out.height == 4
    # share_div_fix=0 覆盖原始 0.56416：只按派现 0.5 计算
    assert out['qfq_factor'][0] == pytest.approx((10.0 - 0.5) / 10.0)


def test_db_path_honours_date_range(session):
    # 用 date 边界：SQLite 把 Date 存为 'YYYY-MM-DD' 文本，与 datetime 绑定值
    # 'YYYY-MM-DD 00:00:00' 按字符串比较会漏掉起始日（MySQL 无此问题），
    # 测试关注的是过滤逻辑本身，不是方言差异。
    _seed_daily(session)
    _seed_dividend(session)
    out = _calc(session).calculate_adj_factors(
        CODE, start_date=D2, end_date=D3,
        use_tdx=False)
    assert out['trade_date'].to_list() == [D2, D3]


def test_no_dividends_yields_default_unit_factors(session):
    _seed_daily(session)
    out = _calc(session).calculate_adj_factors(CODE, use_tdx=False)
    assert out.height == 4
    assert set(out['qfq_factor'].to_list()) == {1.0}
    assert set(out['hfq_factor'].to_list()) == {1.0}


def test_no_dividends_and_no_prices_returns_none(session):
    assert _calc(session).calculate_adj_factors(CODE, use_tdx=False) is None


def test_dividends_without_prices_returns_none(session):
    _seed_dividend(session)
    assert _calc(session).calculate_adj_factors(CODE, use_tdx=False) is None


def test_runtime_error_is_logged_and_returns_none(session, monkeypatch):
    calc = _calc(session)

    def boom(_code):
        raise RuntimeError('db down')

    monkeypatch.setattr(calc, '_get_dividends', boom)
    assert calc.calculate_adj_factors(CODE, use_tdx=False) is None


# ---------------------------------------------------------------- 取数：通达信
@pytest.mark.parametrize('kind', ['lazy', 'frame', 'list'])
def test_tdx_path_accepts_all_result_shapes(session, kind):
    bars = _bars()
    result = {'lazy': pl.DataFrame(bars).lazy(), 'frame': pl.DataFrame(bars),
              'list': bars}[kind]
    tdx = _Tdx(result)
    _seed_dividend(session)
    out = _calc(session, tdx).calculate_adj_factors(
        CODE, start_date=datetime(2024, 6, 1), end_date=datetime(2024, 6, 30))
    assert tdx.calls == [('600000', '2024-06-01', '2024-06-30')]
    assert out.columns[:6] == ['ts_code', 'trade_date', 'open', 'high', 'low', 'close']
    assert out['ts_code'].unique().to_list() == [CODE]
    assert out['qfq_factor'][0] == pytest.approx(0.95)


def test_tdx_default_date_range(session):
    tdx = _Tdx(pl.DataFrame(_bars()))
    _calc(session, tdx)._get_prices_from_tdx(CODE, None, None)
    symbol, start, end = tdx.calls[0]
    assert (symbol, start) == ('600000', '1990-01-01')
    assert len(end) == 10


@pytest.mark.parametrize('result', [
    [],                                              # 空列表
    None,                                            # 无返回
    pl.DataFrame(schema={'date': pl.Date, 'open': pl.Float64, 'high': pl.Float64,
                         'low': pl.Float64, 'close': pl.Float64}),   # 空帧
    pl.DataFrame({'date': [D1], 'close': [1.0]}),    # 缺列
])
def test_tdx_unusable_result_returns_none(session, result):
    assert _calc(session, _Tdx(result))._get_prices_from_tdx(CODE, None, None) is None


@pytest.mark.parametrize('exc', [OSError('io'), RuntimeError('rt'), TypeError('type')])
def test_tdx_exception_returns_none(session, exc):
    assert _calc(session, _Tdx(exc=exc))._get_prices_from_tdx(CODE, None, None) is None


def test_tdx_without_handler_returns_none(session):
    assert _calc(session)._get_prices_from_tdx(CODE, None, None) is None


def test_default_factors_via_tdx(session):
    out = _calc(session, _Tdx(pl.DataFrame(_bars())))._create_default_factors(
        CODE, None, None, use_tdx=True)
    assert out.height == 4 and set(out['qfq_factor'].to_list()) == {1.0}


# ---------------------------------------------------------------- 写库
def test_save_empty_frame_is_noop(session):
    calc = _calc(session)
    calc.save_adj_factors(pl.DataFrame())
    calc.save_adj_factors(None)
    assert session.query(StockAdjFactor).count() == 0


def test_save_failure_rolls_back_and_reraises_inside_transaction(session, monkeypatch):
    def broken(*_args, **_kwargs):
        raise RuntimeError('upsert failed')

    monkeypatch.setattr(afc, 'upsert_rows', broken)
    frame = pl.DataFrame({'ts_code': [CODE], 'trade_date': [D1],
                          'qfq_factor': [1.0], 'hfq_factor': [1.0]})
    calc = _calc(session)
    calc.save_adj_factors(frame, commit=True)          # 自提交模式：记录后吞掉
    with pytest.raises(RuntimeError):
        calc.save_adj_factors(frame, commit=False)     # 外层事务模式：必须上抛


def test_update_stock_daily_adj_writes_only_existing_rows(session):
    _seed_daily(session)
    frame = pl.DataFrame({'ts_code': [CODE, CODE, '000001.SZ'],
                          'trade_date': [D1, D2, D1],
                          'qfq_factor': [0.9, 0.95, 0.5],
                          'hfq_factor': [1.0, 1.05, 2.0]})
    _calc(session).update_stock_daily_adj(frame)
    rows = {r.trade_date: r for r in session.query(StockDaily).all()}
    assert (rows[D1].qfq_factor, rows[D1].hfq_factor) == (0.9, 1.0)
    assert (rows[D2].qfq_factor, rows[D2].hfq_factor) == (0.95, 1.05)
    assert rows[D3].qfq_factor == 1.0                      # 未出现的日期不动
    assert session.query(StockDaily).count() == 4          # 不存在的代码不新增


def test_update_stock_daily_adj_empty_is_noop(session):
    _calc(session).update_stock_daily_adj(pl.DataFrame())
    _calc(session).update_stock_daily_adj(None)


def test_update_stock_daily_adj_failure_reraises_inside_transaction(session, monkeypatch):
    calc = _calc(session)
    frame = pl.DataFrame({'ts_code': [CODE], 'trade_date': [D1],
                          'qfq_factor': [1.0], 'hfq_factor': [1.0]})

    def boom():
        raise RuntimeError('commit failed')

    _seed_daily(session)
    monkeypatch.setattr(session, 'commit', boom)
    calc.update_stock_daily_adj(frame, commit=True)        # 自提交：记录并回滚
    with pytest.raises(RuntimeError):
        monkeypatch.setattr(calc.session, 'query',
                            lambda *_a, **_k: (_ for _ in ()).throw(RuntimeError('q')))
        calc.update_stock_daily_adj(frame, commit=False)


# ---------------------------------------------------------------- 批量
def test_bulk_queries_return_typed_empty_frames(session):
    calc = _calc(session)
    divs = calc._get_dividends_bulk([CODE])
    prices = calc._get_prices_bulk([CODE], None, None)
    assert divs.is_empty() and divs.schema['ex_date'] == pl.Date
    assert prices.is_empty() and prices.schema['close'] == pl.Float64


def test_bulk_dividends_keep_none_fix_values(session):
    _seed_dividend(session, cash_fix=None, share_fix=0.0)
    divs = _calc(session)._get_dividends_bulk([CODE])
    assert divs['cash_div_fix'].to_list() == [None]
    assert divs['share_div_fix'].to_list() == [0.0]


def test_bulk_prices_honour_date_range(session):
    _seed_daily(session)
    # date 边界：避开 SQLite 把 Date 当文本比较、漏掉起始日的方言差异
    prices = _calc(session)._get_prices_bulk([CODE], D2, D3)
    assert prices['trade_date'].to_list() == [D2, D3]


def test_batch_db_path_writes_both_tables_in_one_transaction(session):
    _seed_daily(session)
    _seed_daily(session, code='000001.SZ')
    _seed_dividend(session, cash=0.5)
    _calc(session).batch_calculate_and_save([CODE, '000001.SZ', '000002.SZ'],
                                            chunk_size=2)
    factors = {(r.ts_code, r.trade_date): r.qfq_factor
               for r in session.query(StockAdjFactor).all()}
    assert len(factors) == 8                                # 000002.SZ 无行情被跳过
    assert factors[(CODE, D1)] == pytest.approx(0.95)
    assert factors[('000001.SZ', D1)] == pytest.approx(1.0)
    daily = {(r.ts_code, r.trade_date): r.qfq_factor for r in session.query(StockDaily).all()}
    assert daily[(CODE, D1)] == pytest.approx(0.95)


def test_batch_tdx_path(session):
    _seed_dividend(session, cash=0.5)
    tdx = _Tdx(pl.DataFrame(_bars()))
    _calc(session, tdx).batch_calculate_and_save([CODE], update_daily=False)
    assert len(tdx.calls) == 1
    assert session.query(StockAdjFactor).count() == 4


def test_batch_chunk_without_any_prices_writes_nothing(session):
    _calc(session).batch_calculate_and_save(['000002.SZ'])
    assert session.query(StockAdjFactor).count() == 0


def test_batch_chunk_failure_rolls_back_and_continues(session, monkeypatch):
    _seed_daily(session)
    _seed_daily(session, code='000001.SZ')
    calc = _calc(session)
    original = calc._get_dividends_bulk

    def flaky(codes):
        if CODE in codes:
            raise RuntimeError('chunk failed')
        return original(codes)

    monkeypatch.setattr(calc, '_get_dividends_bulk', flaky)
    calc.batch_calculate_and_save([CODE, '000001.SZ'], chunk_size=1)
    codes = {r.ts_code for r in session.query(StockAdjFactor).all()}
    assert codes == {'000001.SZ'}                           # 失败块不影响后续块
