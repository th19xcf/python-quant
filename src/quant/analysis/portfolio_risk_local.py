"""组合仓位与风控：本地数据装配与 CLI 入口（只读，不写 data/，不改回测引擎）。

数据来源：

- 市场趋势总分：复用 ``market_trend_local.load_local_panel`` + ``evaluate_market_trend``；
- 基准指数序列：``data/local_store/quotes/index/**/<code>.parquet``，默认 ``000300.SH``；
- 目标持仓：``reports/target_holdings/目标持仓_<日期>.csv``（最新一份）与
  ``目标持仓台账.csv``（只取与持仓同交易日的变动金额算单边换手）；
- 行业归属：``meta/industry_pit/industry_pit_sw.parquet`` 按交易日 PIT 取申万一级。

打分与判定规则在 ``portfolio_risk_state.py``；本模块只做装配与命令行输出。
"""

from __future__ import annotations

import argparse
import time
from datetime import date, datetime
from pathlib import Path
from typing import TypedDict

import polars as pl
from loguru import logger

from src.data.local_store.store import resolve_store_root
from src.quant.analysis.market_trend_local import load_local_panel as load_trend_panel
from src.quant.analysis.market_trend_state import evaluate_market_trend
from src.quant.analysis.portfolio_risk_state import (
    PortfolioRiskState,
    evaluate_portfolio_risk,
)
from src.utils.config import get_config

#: 默认基准指数
BENCHMARK_CODE = '000300.SH'
#: 目标持仓目录（相对仓库根）
HOLDINGS_DIR = Path('reports') / 'target_holdings'
#: 台账文件名
LEDGER_NAME = '目标持仓台账.csv'


def _default_root() -> Path:
    return resolve_store_root(get_config())


def _project_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _holdings_dir() -> Path:
    return _project_root() / HOLDINGS_DIR


def _load_index_closes(base: Path, code: str = BENCHMARK_CODE) -> tuple[list[float], str | None]:
    """读取基准指数收盘序列，返回 (收盘序列, 最新交易日)。"""
    files = list((base / 'quotes' / 'index').glob(f'**/{code}.parquet'))
    if not files:
        logger.warning('基准指数行情缺失：{}', code)
        return [], None
    frame = (
        pl.read_parquet(files[0], columns=['date', 'close'])
        .drop_nulls('close')
        .sort('date')
    )
    if frame.is_empty():
        return [], None
    closes = [float(item) for item in frame['close'].to_list()]
    return closes, str(frame['date'][-1])


def latest_holdings_file() -> Path | None:
    """取目标持仓目录下日期最大的一份快照；没有返回 None。"""
    candidates = sorted(_holdings_dir().glob('目标持仓_*.csv'))
    return candidates[-1] if candidates else None


def read_weights(path: Path) -> tuple[dict[str, float], str | None]:
    """读取目标权重，返回 ({代码: 权重}, 参考价日期)。"""
    frame = pl.read_csv(path, infer_schema_length=0)
    if '代码' not in frame.columns or '目标权重' not in frame.columns:
        return {}, None
    mapped = {
        str(code): float(weight)
        for code, weight in zip(
            frame['代码'].to_list(), frame['目标权重'].to_list(), strict=False)
        if code and weight not in (None, '')
    }
    asof = None
    if '参考价日期' in frame.columns and not frame.is_empty():
        asof = str(frame['参考价日期'][-1])
    return mapped, asof


def _one_way_turnover(asof: str | None) -> float | None:
    """台账同交易日的单边换手 = Σ|变动金额| / 资金规模 / 2。"""
    ledger = _holdings_dir() / LEDGER_NAME
    if asof is None or not ledger.is_file():
        return None
    frame = pl.read_csv(ledger, infer_schema_length=0).filter(
        pl.col('交易日') == asof)
    if frame.is_empty():
        return None
    try:
        moved = sum(abs(float(item)) for item in frame['变动金额'].to_list())
        capital = float(frame['资金规模'][0])
    except (TypeError, ValueError):
        return None
    if capital <= 0:
        return None
    return moved / capital / 2.0


def industry_map(base: Path, codes: list[str], asof: str | None) -> dict[str, str]:
    """按交易日 PIT 取申万一级名称。"""
    if not codes or asof is None:
        return {}
    path = base / 'meta' / 'industry_pit' / 'industry_pit_sw.parquet'
    if not path.is_file():
        return {}
    day = datetime.strptime(asof, '%Y-%m-%d').date()
    frame = (
        pl.read_parquet(path, columns=[
            'ts_code', 'industry_l1_name', 'in_date', 'out_date'])
        .filter(pl.col('ts_code').is_in(codes))
        .filter(pl.col('in_date') <= day)
        .filter(pl.col('out_date').is_null() | (pl.col('out_date') > day))
        .filter(pl.col('industry_l1_name').is_not_null())
        .unique(subset=['ts_code'], keep='last')
    )
    return dict(zip(
        frame['ts_code'].to_list(), frame['industry_l1_name'].to_list(), strict=False))


class HoldingsSnapshot(TypedDict):
    """目标持仓读取结果：无文件时各字段为空、source 标明「未生成」。"""

    weights: dict[str, float]
    industry_of: dict[str, str]
    turnover: float | None
    asof: str | None
    source: str


def collect_holdings(base: Path) -> HoldingsSnapshot:
    """汇总目标持仓权重、行业归属与单边换手；无文件时如实返回空。"""
    snapshot = latest_holdings_file()
    if snapshot is None:
        return {'weights': {}, 'industry_of': {}, 'turnover': None,
                'asof': None, 'source': 'reports/target_holdings/（未生成）'}
    weights, asof = read_weights(snapshot)
    return {
        'weights': weights,
        'industry_of': industry_map(base, list(weights), asof),
        'turnover': _one_way_turnover(asof),
        'asof': asof,
        'source': str(snapshot).replace('\\', '/'),
    }


def build_state(root: Path | str | None = None,
                asof: str | None = None) -> PortfolioRiskState:
    """装配本地数据并计算仓位、风险预算与约束校验。"""
    base = Path(root) if root is not None else _default_root()
    started = time.time()
    target = datetime.strptime(asof, '%Y-%m-%d').date() if asof else None

    trend = evaluate_market_trend(load_trend_panel(base), asof=target)
    closes, index_asof = _load_index_closes(base)
    holdings = collect_holdings(base)
    if asof is None and index_asof:
        asof = index_asof

    notes = [f'基准：{BENCHMARK_CODE}，最新交易日 {index_asof or "缺失"}。']
    if not closes:
        notes.append('基准行情缺失：波动率与回撤两个环节不调整。')
    notes.append('目标仓位不含杠杆，缩放系数上限 1.00；回撤用基准指数代理组合净值。')

    state = evaluate_portfolio_risk(
        trend.total_score,
        trend.label,
        closes,
        asof=datetime.strptime(asof, '%Y-%m-%d').date() if asof else None,
        weights=holdings['weights'],
        industry_of=holdings['industry_of'],
        one_way_turnover=holdings['turnover'],
        holdings_asof=holdings['asof'],
        holdings_source=holdings['source'],
        notes=notes,
    )
    logger.debug('仓位风控装配完成：耗时 {:.1f}s', time.time() - started)
    return state


def run_cli(asof: str | None = None, panel_root: str | None = None) -> None:
    state = build_state(panel_root, asof)
    decision = state.decision
    logger.info('建议仓位 {:.0%}（{}）', decision.target_position, state.asof)
    for step in decision.steps:
        logger.info('环节：{}', step)
    budget = state.budget
    logger.info(
        '风险预算：目标波动 {:.0%}｜已实现 {}｜缺口 {}｜回撤 {}｜距制动 {}｜年化敞口 {}',
        budget.target_vol,
        '—' if budget.realized_vol is None else f'{budget.realized_vol:.2%}',
        '—' if budget.vol_gap is None else f'{budget.vol_gap:+.2%}',
        '—' if budget.drawdown is None else f'{budget.drawdown:.2%}',
        '—' if budget.drawdown_distance is None else f'{budget.drawdown_distance:+.2%}',
        '—' if budget.risk_exposure is None else f'{budget.risk_exposure:.2%}',
    )
    constraints = state.constraints
    logger.info('约束校验：{}（{}）', '已生成' if constraints.generated else '未生成',
                constraints.asof or '—')
    for item in constraints.items:
        logger.info('约束：{} 上限 {:.0%}｜实际 {}｜{}｜{}',
                    item.name, item.limit,
                    '—' if item.actual is None else f'{item.actual:.2%}',
                    '通过' if item.passed else '违规', item.detail)
    for note in constraints.notes:
        logger.info('说明：{}', note)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description='组合仓位与风控（只读本地数据）')
    parser.add_argument('--asof', help='判定截止日，格式 YYYY-MM-DD')
    parser.add_argument('--panel-root', help='本地列存根目录，默认按配置解析')
    args = parser.parse_args(argv)
    run_cli(asof=args.asof, panel_root=args.panel_root)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
