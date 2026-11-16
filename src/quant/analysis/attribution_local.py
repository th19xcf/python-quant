"""收益归因：本地装配与 CLI 入口（只读，不新跑回测、不写 data/）。

数据来源：

- 回测 run：``data/local_store/backtest_runs/<run_id>/`` 的 ``metrics.json`` 与
  ``daily_nav.csv``（只含 date, equity）；
- 基准指数：``quotes/index/**/000300.SH.parquet``，与 run 区间对齐；
- 目标持仓与行业归属：复用 ``portfolio_risk_local`` 的读取函数；
- 市值/PB 分位与行业占比：``meta/financials/valuation_daily.parquet`` 与
  ``meta/industry_pit/industry_pit_sw.parquet``（按交易日截面）。

现有 run 无逐期持仓、无逐笔成交、无基准成分行业权重，因此只做 run 级超额 +
行业暴露 + 风格暴露，页面标注「口径降级」。
"""

from __future__ import annotations

import argparse
import json
from collections.abc import Mapping
from datetime import date, datetime, timedelta
from pathlib import Path

import polars as pl
from loguru import logger

from src.data.local_store.store import resolve_store_root
from src.quant.analysis.attribution_state import (
    BENCHMARK_LABELS,
    AttributionState,
    BrinsonPeriod,
    RunSummary,
    brinson_period,
    build_brinson,
    build_state,
    excess_stats,
    industry_exposure,
    style_exposure,
)
from src.quant.analysis.portfolio_risk_local import (
    BENCHMARK_CODE,
    collect_holdings,
    industry_map,
    latest_holdings_file,
    read_weights,
)
from src.utils.config import get_config

#: run 根目录（相对本地列存）
RUNS_SUBDIR = Path('backtest_runs')
#: 缺失数据项（页面原样展示）
MISSING_ITEMS = (
    '逐期持仓权重（run 只落 daily_nav.csv）',
    '逐笔成交明细（metrics 只有成交笔数与买卖金额汇总）',
    '基准成分行业权重（000300 无成分权重表）',
)
#: 归因对象要求的最少净值行数：跳过数十行的冒烟 run
MIN_NAV_ROWS = 250
#: 逐期持仓表（records.POSITIONS_FILE），老 run 没有它即走降级口径
POSITIONS_FILE = 'positions.parquet'
#: 可切基准
BENCHMARK_CHOICES = ('equal_all', 'equal_industry', 'csi300')
#: 计算区间收益时向前多读的日历天数（覆盖前一交易日）
RETURN_LOOKBACK_DAYS = 20


def _default_root() -> Path:
    return resolve_store_root(get_config())


def _latest_run(base: Path) -> tuple[Path | None, dict]:
    """按三级优先取 run：落有 positions.parquet 的最新 run → nav_rows 达标的最新
    run → 最新 run。第一档可做完整 Brinson，后两档只能降级。"""
    runs_dir = base / RUNS_SUBDIR
    if not runs_dir.is_dir():
        return None, {}
    with_positions: tuple[str, Path, dict] | None = None
    best: tuple[str, Path, dict] | None = None
    fallback: tuple[str, Path, dict] | None = None
    for metrics_path in runs_dir.glob('*/metrics.json'):
        try:
            payload = json.loads(metrics_path.read_text(encoding='utf-8'))
        except (OSError, ValueError):
            continue
        created = str(payload.get('created_at') or '')
        if fallback is None or created > fallback[0]:
            fallback = (created, metrics_path.parent, payload)
        meta = payload.get('positions') or {}
        position_rows = int(meta.get('rows') or 0) if isinstance(meta, dict) else 0
        if position_rows > 0 and (metrics_path.parent / POSITIONS_FILE).is_file():
            if with_positions is None or created > with_positions[0]:
                with_positions = (created, metrics_path.parent, payload)
            continue
        rows = int(payload.get('nav_rows') or 0)
        if rows < MIN_NAV_ROWS:
            continue
        if best is None or created > best[0]:
            best = (created, metrics_path.parent, payload)
    chosen = with_positions or best or fallback
    if chosen is None:
        return None, {}
    return chosen[1], chosen[2]


def _read_nav(run_dir: Path) -> tuple[list[float], list[str]]:
    path = run_dir / 'daily_nav.csv'
    if not path.is_file():
        return [], []
    frame = pl.read_csv(path, infer_schema_length=0)
    if 'equity' not in frame.columns:
        return [], []
    values = [float(item) for item in frame['equity'].to_list() if item not in ('', None)]
    dates = [str(item) for item in frame['date'].to_list()] if 'date' in frame.columns else []
    return values, dates


def _bench_closes(base: Path, start: str | None, end: str | None) -> list[float]:
    files = list((base / 'quotes' / 'index').glob(f'**/{BENCHMARK_CODE}.parquet'))
    if not files:
        return []
    frame = pl.read_parquet(files[0], columns=['date', 'close']).drop_nulls('close').sort('date')
    if start:
        frame = frame.filter(pl.col('date') >= pl.lit(start).str.to_date())
    if end:
        frame = frame.filter(pl.col('date') <= pl.lit(end).str.to_date())
    return [float(item) for item in frame['close'].to_list()]


def load_positions(run_dir: Path) -> pl.DataFrame | None:
    """读取 positions.parquet；老 run 没有该文件时返回 None（走降级口径）。"""
    path = run_dir / POSITIONS_FILE
    if not path.is_file():
        return None
    frame = pl.read_parquet(path)
    if frame.is_empty() or 'weight' not in frame.columns:
        return None
    frame = frame.drop_nulls(['weight']).filter(pl.col('weight') > 0)
    return frame if not frame.is_empty() else None


def snapshot_dates(positions: pl.DataFrame) -> list[str]:
    """持仓快照日期（升序，字符串）。"""
    return [str(item) for item in positions['date'].unique().sort().to_list()]


def _pit_frame(base: Path) -> pl.DataFrame | None:
    path = base / 'meta' / 'industry_pit' / 'industry_pit_sw.parquet'
    if not path.is_file():
        return None
    return pl.read_parquet(
        path, columns=['ts_code', 'industry_l1_name', 'in_date', 'out_date'])


def _industry_asof(pit: pl.DataFrame | None, day: date) -> dict[str, str]:
    """按交易日 PIT 取申万一级名称。"""
    if pit is None:
        return {}
    frame = (
        pit.filter(pl.col('in_date') <= day)
        .filter(pl.col('out_date').is_null() | (pl.col('out_date') > day))
        .filter(pl.col('industry_l1_name').is_not_null())
        .unique(subset=['ts_code'], keep='last')
    )
    return dict(zip(frame['ts_code'].to_list(),
                    frame['industry_l1_name'].to_list(), strict=False))


def _returns_on_dates(base: Path, dates: list[str]) -> dict[str, dict[str, float]]:
    """各快照日期的个股收益（后复权，前一交易日为基准）。"""
    if not dates:
        return {}
    first = datetime.strptime(dates[0], '%Y-%m-%d').date() - timedelta(
        days=RETURN_LOOKBACK_DAYS)
    last = datetime.strptime(dates[-1], '%Y-%m-%d').date()
    frame = (
        pl.scan_parquet(str(base / 'quotes' / 'stock' / '**' / '*.parquet'))
        .select(['date', 'ts_code', 'close', 'hfq_factor'])
        .filter(pl.col('date') >= pl.lit(first))
        .filter(pl.col('date') <= pl.lit(last))
        .collect()
        .sort(['ts_code', 'date'])
        .with_columns((pl.col('close') * pl.col('hfq_factor')).alias('adj'))
        .with_columns(pl.col('adj').shift(1).over('ts_code').alias('prev'))
        .with_columns((pl.col('adj') / pl.col('prev') - 1).alias('ret'))
        .filter(pl.col('ret').is_not_null())
    )
    wanted = [datetime.strptime(item, '%Y-%m-%d').date() for item in dates]
    frame = frame.filter(pl.col('date').is_in(wanted))
    result: dict[str, dict[str, float]] = {}
    for day, group in frame.group_by('date'):
        key = str(day[0]) if isinstance(day, tuple) else str(day)
        result[key] = dict(zip(group['ts_code'].to_list(),
                               group['ret'].to_list(), strict=False))
    return result


def _bench_weights(
    returns: Mapping[str, float],
    industry_of: Mapping[str, str],
    mode: str,
) -> dict[str, float]:
    """基准权重：全市场等权 / 行业等权（行业先等权、行业内再等权）。"""
    codes = list(returns)
    if not codes:
        return {}
    if mode == 'equal_industry':
        buckets: dict[str, list[str]] = {}
        for code in codes:
            buckets.setdefault(industry_of.get(code, '未归类'), []).append(code)
        share = 1.0 / len(buckets)
        return {code: share / len(members)
                for members in buckets.values() for code in members}
    return {code: 1.0 / len(codes) for code in codes}


def _index_return(base: Path, start: str, end: str) -> float:
    """基准指数在 [start, end] 区间的收益（沪深 300 无成分权重时用）。"""
    files = list((base / 'quotes' / 'index').glob(f'**/{BENCHMARK_CODE}.parquet'))
    if not files:
        return 0.0
    frame = pl.read_parquet(files[0], columns=['date', 'close']).drop_nulls('close').sort('date')
    start_day = datetime.strptime(start, '%Y-%m-%d').date()
    end_day = datetime.strptime(end, '%Y-%m-%d').date()
    before = frame.filter(pl.col('date') <= start_day)
    after = frame.filter(pl.col('date') <= end_day)
    if before.is_empty() or after.is_empty():
        return 0.0
    begin = float(before['close'][-1])
    finish = float(after['close'][-1])
    return finish / begin - 1.0 if begin > 0 else 0.0


def _periods_per_year(dates: list[str]) -> float:
    if len(dates) < 2:
        return 12.0
    first = datetime.strptime(dates[0], '%Y-%m-%d').date()
    last = datetime.strptime(dates[-1], '%Y-%m-%d').date()
    years = max((last - first).days / 365.25, 1e-6)
    return (len(dates) - 1) / years


def build_brinson_from_positions(
    base: Path,
    positions: pl.DataFrame,
    benchmark: str = 'equal_all',
):
    """用 positions.parquet 做逐期 Brinson 两段分解；快照不足两期返回 None。"""
    dates = snapshot_dates(positions)
    if len(dates) < 2:
        return None
    returns_by_date = _returns_on_dates(base, dates[1:])
    pit = _pit_frame(base)
    periods: list[BrinsonPeriod] = []
    for start, end in zip(dates, dates[1:], strict=False):
        rows = positions.filter(pl.col('date') == start)
        weights_p = dict(zip(rows['code'].to_list(),
                             rows['weight'].to_list(), strict=False))
        returns = returns_by_date.get(end, {})
        start_day = datetime.strptime(start, '%Y-%m-%d').date()
        industry_of = _industry_asof(pit, start_day)
        if benchmark == 'csi300':
            portfolio = sum(weight * returns.get(code, 0.0)
                            for code, weight in weights_p.items())
            periods.append(BrinsonPeriod(
                start, end, None, None, None,
                portfolio, _index_return(base, start, end)))
            continue
        weights_b = _bench_weights(returns, industry_of, benchmark)
        allocation, selection, interaction, portfolio, baseline = brinson_period(
            weights_p, weights_b, returns, industry_of)
        periods.append(BrinsonPeriod(
            start, end, allocation, selection, interaction, portfolio, baseline))
    return build_brinson(
        periods, BENCHMARK_LABELS.get(benchmark, benchmark), _periods_per_year(dates))


def _industry_share(base: Path, asof: str | None) -> dict[str, float]:
    """全 A 等权行业占比 = 该行业成分股数 / 有行业归属的股票数。"""
    if asof is None:
        return {}
    path = base / 'meta' / 'industry_pit' / 'industry_pit_sw.parquet'
    if not path.is_file():
        return {}
    day = datetime.strptime(asof, '%Y-%m-%d').date()
    frame = (
        pl.read_parquet(path, columns=['ts_code', 'industry_l1_name', 'in_date', 'out_date'])
        .filter(pl.col('in_date') <= day)
        .filter(pl.col('out_date').is_null() | (pl.col('out_date') > day))
        .filter(pl.col('industry_l1_name').is_not_null())
        .unique(subset=['ts_code'], keep='last')
    )
    total = frame.height
    if total == 0:
        return {}
    counts = frame.group_by('industry_l1_name').len()
    return {name: count / total for name, count in
            zip(counts['industry_l1_name'].to_list(), counts['len'].to_list(), strict=False)}


def _percentiles(base: Path, asof: str | None) -> tuple[dict[str, float], dict[str, float]]:
    """当日截面市值分位与 PB 分位（0~100，PB<=0 视为缺失）。"""
    if asof is None:
        return {}, {}
    path = base / 'meta' / 'financials' / 'valuation_daily.parquet'
    if not path.is_file():
        return {}, {}
    day = datetime.strptime(asof, '%Y-%m-%d').date()
    frame = (
        pl.scan_parquet(str(path))
        .filter(pl.col('trade_date') == day)
        .select(['ts_code', 'total_mv', 'pb'])
        .collect()
    )
    if frame.is_empty():
        return {}, {}
    frame = frame.with_columns(
        (pl.col('total_mv').rank('average') / pl.len() * 100).alias('mv_pct'),
        pl.when(pl.col('pb') > 0).then(pl.col('pb')).otherwise(None).alias('pb'),
    ).with_columns(
        (pl.col('pb').rank('average') / pl.col('pb').is_not_null().sum() * 100).alias('pb_pct'),
    )
    mv_map = {code: float(pct) for code, pct in
              zip(frame['ts_code'].to_list(), frame['mv_pct'].to_list(), strict=False)
              if pct is not None}
    pb_map = {code: float(pct) for code, pct in
              zip(frame['ts_code'].to_list(), frame['pb_pct'].to_list(), strict=False)
              if pct is not None}
    return mv_map, pb_map


def _holdings(base: Path) -> dict[str, object]:
    snapshot = latest_holdings_file()
    if snapshot is None:
        return {'weights': {}, 'industry_of': {}, 'asof': None,
                'source': 'reports/target_holdings/（未生成）'}
    weights, asof = read_weights(snapshot)
    return {
        'weights': weights,
        'industry_of': industry_map(base, list(weights), asof),
        'asof': asof,
        'source': str(snapshot).replace('\\', '/'),
    }


def build_attribution(root: Path | str | None = None,
                      asof: str | None = None,
                      benchmark: str = 'equal_all') -> AttributionState:
    """装配 run、基准与持仓：有 positions.parquet 走完整 Brinson，否则降级。"""
    base = Path(root) if root is not None else _default_root()
    run_dir, metrics = _latest_run(base)
    nav, _ = _read_nav(run_dir) if run_dir is not None else ([], [])
    period = metrics.get('period') or {}
    start = period.get('start') if isinstance(period, dict) else None
    end = period.get('end') if isinstance(period, dict) else None

    run = None
    if run_dir is not None:
        run = RunSummary(
            run_id=str(metrics.get('run_id') or run_dir.name),
            name=str(metrics.get('name') or ''),
            start=str(start or ''),
            end=str(end or ''),
        )
    bench = _bench_closes(base, start, end) if run else []
    excess = excess_stats(nav, bench) if nav and bench else None
    mode = benchmark if benchmark in BENCHMARK_CHOICES else 'equal_all'

    positions = load_positions(run_dir) if run_dir is not None else None
    brinson = build_brinson_from_positions(base, positions, mode) if positions is not None else None

    holdings = _holdings(base)
    holdings_asof = holdings['asof'] if isinstance(holdings['asof'], str) else None
    weights = holdings['weights'] if isinstance(holdings['weights'], dict) else {}
    industries = industry_exposure(
        weights,
        holdings['industry_of'] if isinstance(holdings['industry_of'], dict) else {},
        _industry_share(base, holdings_asof),
    )
    mv_map, pb_map = _percentiles(base, holdings_asof)
    styles = style_exposure(weights, mv_map, pb_map) if weights and (mv_map or pb_map) else ()

    notes = [
        f'归因对象：{run.name or "无 run"}（{run.run_id}，{run.start}~{run.end}）'
        if run else '未找到可归因的回测 run。',
        f'基准：{BENCHMARK_CODE}；持仓快照：{holdings_asof or "未生成"}'
        f'（{holdings["source"]}）。',
        f'run 选择：优先 created_at 最新且落有 positions.parquet 的 run，'
        f'其次 nav_rows ≥ {MIN_NAV_ROWS}（跳过冒烟 run）。',
    ]
    if brinson is not None:
        degraded = False
        missing: tuple[str, ...] = ()
        notes.append(
            f'完整 Brinson：基准 {brinson.benchmark}，{len(brinson.periods)} 个调仓区间；'
            '逐期用区间期初权重与区间收益分解，行业用申万 PIT。')
    else:
        degraded = True
        missing = MISSING_ITEMS
        notes.append(
            '口径降级：该 run 无 positions.parquet（只有净值序列），'
            'Brinson 两段分解不可做；此处为 run 级超额 + 单期暴露。')
    return build_state(
        asof=holdings_asof or end,
        run=run,
        excess=excess,
        industries=industries,
        styles=styles,
        degraded=degraded,
        missing=missing,
        notes=notes,
        brinson=brinson,
        benchmark=BENCHMARK_LABELS.get(mode, mode),
    )


def run_cli(asof: str | None = None, panel_root: str | None = None,
            benchmark: str = 'equal_all') -> None:
    state = build_attribution(panel_root, asof, benchmark)
    if state.run is not None:
        logger.info('归因 run：{}｜{}~{}', state.run.run_id, state.run.start, state.run.end)
    if state.brinson is not None:
        item = state.brinson
        logger.info('完整 Brinson（基准 {}，{} 期）：配置 {}｜选择 {}｜交叉 {}｜合计超额 {}',
                    item.benchmark, len(item.periods), _pct(item.allocation),
                    _pct(item.selection), _pct(item.interaction), _pct(item.excess))
        logger.info('累计：组合 {}｜基准 {}｜IR {}｜α {}｜t {}',
                    _pct(item.portfolio_return), _pct(item.benchmark_return),
                    _num(item.information_ratio), _pct(item.alpha), _num(item.alpha_t))
    else:
        logger.info('完整 Brinson：该 run 无 positions.parquet，走降级口径')
    if state.excess is not None:
        stats = state.excess
        logger.info(
            'run 超额：年化 {}｜基准 {}｜超额 {}｜beta {}｜跟踪误差 {}｜信息比率 {}',
            _pct(stats.annual_return), _pct(stats.bench_annual),
            _pct(stats.excess_annual), _num(stats.beta),
            _pct(stats.tracking_error), _num(stats.information_ratio),
        )
    else:
        logger.info('run 超额：净值或基准序列不足，未计算')
    for row in state.industries:
        logger.info('行业暴露：{} 权重 {}｜基准 {}｜偏离 {}',
                    row.name, _pct(row.weight), _pct(row.baseline), _pct(row.diff))
    for row in state.styles:
        logger.info('风格暴露：{} {}｜基准 {}｜偏离 {}',
                    row.name, _num(row.weight), _num(row.baseline), _num(row.diff))
    for note in state.notes:
        logger.info('说明：{}', note)


def _pct(value: float | None) -> str:
    return '—' if value is None else f'{value * 100:+.2f}%'


def _num(value: float | None) -> str:
    return '—' if value is None else f'{value:+.2f}'


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description='收益归因（只读本地数据）')
    parser.add_argument('--asof', help='持仓快照截止日，格式 YYYY-MM-DD')
    parser.add_argument('--panel-root', help='本地列存根目录，默认按配置解析')
    parser.add_argument('--benchmark', choices=BENCHMARK_CHOICES, default='equal_all',
                        help='基准：全市场等权 / 行业等权 / 沪深 300')
    args = parser.parse_args(argv)
    run_cli(asof=args.asof, panel_root=args.panel_root, benchmark=args.benchmark)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
