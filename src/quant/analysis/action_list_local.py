"""模型侧操作清单：读取目标持仓台账 + 三层状态，产出「现在该做什么」清单。

只读模型侧产出与本地行情：``reports/target_holdings/目标持仓台账.csv``、市场趋势 /
风格轮动 / 行业热度状态与本地行情。不读用户实际持仓、不接账户、不下单，也不跑回测。
执行时点口径复用回测执行内核：T 日收盘信号 → T+1 开盘成交，涨停/跌停不可成交、
停牌阻断；本模块只做文字标注，不模拟撮合。
"""

from __future__ import annotations

import argparse
import calendar
from datetime import date
from pathlib import Path

import polars as pl
from loguru import logger

from src.data.local_store.store import resolve_store_root
from src.data.trading_calendar import next_trading_day, trading_dates
from src.quant.analysis.action_list_state import (
    ActionListState,
    ActionRow,
    direction_of,
    lots_from_amount,
)
from src.quant.analysis.industry_heat_local import load_local_panel as load_heat_panel
from src.quant.analysis.industry_heat_state import evaluate_industry_heat
from src.quant.analysis.market_trend_local import load_local_panel as load_trend_panel
from src.quant.analysis.market_trend_state import evaluate_market_trend
from src.quant.analysis.portfolio_risk_local import industry_map
from src.quant.analysis.style_rotation_local import load_local_panel as load_style_panel
from src.quant.analysis.style_rotation_state import evaluate_style_rotation
from src.utils.config import get_config

#: 仓库根目录（本文件位于 src/quant/analysis/ 下三层）
PROJECT_ROOT = Path(__file__).resolve().parents[3]
#: 目标持仓台账（模型侧产出）
LEDGER_PATH = PROJECT_ROOT / 'reports' / 'target_holdings' / '目标持仓台账.csv'
#: 缺省资金规模（与 target_holdings 的 v3 容量上限一致）
DEFAULT_CAPITAL = 1e9
#: 模型侧边界声明
BOUNDARY_NOTE = '模型侧清单：只读模型产出与本地行情，不含用户实际持仓，不接账户、不下单。'
#: 执行口径声明
EXECUTION_NOTE = (
    '执行口径复用回测执行内核：T 日收盘信号 → T+1 开盘成交，涨停/跌停不可成交、'
    '停牌阻断；本页只标注时点与条件，不跑回测、不改引擎。')


def _default_root() -> Path:
    return resolve_store_root(get_config())


def _to_float(value: object, default: float = 0.0) -> float:
    try:
        return float(str(value))
    except (TypeError, ValueError):
        return default


def load_ledger() -> list[dict[str, str]]:
    """读取目标持仓台账；文件缺失返回空列表。"""
    if not LEDGER_PATH.is_file():
        return []
    frame = pl.read_csv(LEDGER_PATH, infer_schema_length=0)
    return frame.to_dicts()


def latest_trade_day(rows: list[dict[str, str]]) -> str | None:
    days = [str(row.get('交易日') or '') for row in rows]
    days = [day for day in days if day]
    return max(days) if days else None


def next_rebalance_date(trade_day: date, base: Path) -> str | None:
    """下一次再平衡执行日：下一个月末交易日的下一交易日。"""
    year, month = (trade_day.year + 1, 1) if trade_day.month == 12 \
        else (trade_day.year, trade_day.month + 1)
    last = calendar.monthrange(year, month)[1]
    month_days = trading_dates(date(year, month, 1), date(year, month, last), root=base)
    anchor = month_days[-1] if month_days else date(year, month, last)
    nxt = next_trading_day(anchor, root=base)
    return nxt.isoformat() if nxt else None


def _condition(direction: str, exec_day: str, next_reb: str | None) -> str:
    rebalance = next_reb or '下一个月末'
    if direction in ('买入', '加仓'):
        return (f'{exec_day} 开盘执行；涨停或停牌则顺延；趋势降档或 {rebalance} 再平衡时重核')
    if direction == '减持':
        return f'{exec_day} 开盘执行；跌停或停牌则顺延；趋势升档则暂停减持'
    if direction == '清仓':
        return f'{exec_day} 开盘执行；跌停或停牌则顺延至下一可成交日'
    return f'持有至 {rebalance} 再平衡或信号翻档'


def build_action_list(root: Path | str | None = None,
                      capital: float | None = None) -> ActionListState:
    """装配模型侧操作清单；台账缺失时返回带 missing 的空状态。"""
    base = Path(root) if root is not None else _default_root()
    cap = float(capital) if capital else DEFAULT_CAPITAL
    rows = load_ledger()
    trade_day = latest_trade_day(rows)
    if trade_day is None:
        return ActionListState(
            trade_day=None, signal_day=None, exec_date=None, next_rebalance=None,
            capital=cap, trend_label='', style_direction='',
            notes=(BOUNDARY_NOTE, EXECUTION_NOTE),
            missing=(str(LEDGER_PATH).replace('\\', '/'),),
        )

    day_rows = [row for row in rows if str(row.get('交易日')) == trade_day]
    ledger_capital = _to_float(day_rows[0].get('资金规模'), DEFAULT_CAPITAL) \
        if day_rows else DEFAULT_CAPITAL
    scale = cap / ledger_capital if ledger_capital > 0 else 1.0
    signal_day = str(day_rows[0].get('信号日') or '') if day_rows else ''
    trade_date = date.fromisoformat(trade_day)
    exec_day = next_trading_day(trade_date, root=base)
    exec_text = exec_day.isoformat() if exec_day else '下一交易日'
    next_reb = next_rebalance_date(trade_date, base)

    trend = evaluate_market_trend(load_trend_panel(base), asof=trade_date)
    style = evaluate_style_rotation(load_style_panel(base), asof=trade_date)
    heat = evaluate_industry_heat(load_heat_panel(base), asof=trade_date)
    codes = [str(row.get('代码') or '') for row in day_rows]
    industry_of = industry_map(base, codes, trade_day)
    heat_by_name = {item.name: item.label for item in heat.industries}

    actions: list[ActionRow] = []
    for row in day_rows:
        code = str(row.get('代码') or '')
        change = str(row.get('变动类型') or '不变')
        direction = direction_of(change)
        price = _to_float(row.get('参考价'))
        delta_amount = _to_float(row.get('变动金额')) * scale
        if direction in ('买入', '加仓'):
            shares = lots_from_amount(delta_amount, price)
        elif direction in ('减持', '清仓'):
            shares = lots_from_amount(abs(delta_amount), price)
        else:
            shares = 0
        heat_label = heat_by_name.get(industry_of.get(code, ''), '未映射')
        rationale = (
            f'v3低波 {signal_day} 月末信号 ｜ 趋势{trend.label} ｜ '
            f'行业热度{heat_label} ｜ 风格{style.direction}')
        actions.append(ActionRow(
            code=code,
            name=str(row.get('名称') or ''),
            direction=direction,
            shares=shares,
            price=price,
            target_weight=_to_float(row.get('目标权重')),
            exec_date=exec_text,
            condition=_condition(direction, exec_text, next_reb),
            rationale=rationale,
        ))

    notes = (
        BOUNDARY_NOTE,
        EXECUTION_NOTE,
        f'资金规模 {cap:.0f} 元，按台账基准 {ledger_capital:.0f} 元等比缩放后取整手。',
        f'信号日 {signal_day}，交易日 {trade_day}，执行时点 {exec_text}。',
    )
    return ActionListState(
        trade_day=trade_day, signal_day=signal_day, exec_date=exec_text,
        next_rebalance=next_reb, capital=cap, trend_label=trend.label,
        style_direction=style.direction, rows=tuple(actions), notes=notes,
    )


def format_state(state: ActionListState, top: int = 5) -> str:
    """文本摘要：标的数、方向分布与样例行。"""
    counts = state.direction_counts()
    lines = [
        f'交易日 {state.trade_day or "—"} 信号日 {state.signal_day or "—"} '
        f'执行 {state.exec_date or "—"} 资金 {state.capital:.0f}',
        f'标的数 {len(state.rows)} 方向分布 {counts}',
        f'趋势 {state.trend_label or "—"} 风格 {state.style_direction or "—"}',
    ]
    for row in state.rows[:top]:
        lines.append(
            f'{row.code} {row.name} {row.direction} {row.shares}股 '
            f'@{row.price:.2f} 权{row.target_weight:.4f} {row.rationale}')
    for note in state.notes:
        lines.append(f'说明：{note}')
    for miss in state.missing:
        lines.append(f'缺失：{miss}')
    return '\n'.join(lines)


def run_cli(capital: float | None = None, root: str | None = None,
            top: int = 5) -> None:
    state = build_action_list(root=root, capital=capital)
    for line in format_state(state, top=top).splitlines():
        logger.info(line)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description='模型侧操作清单（只读，不下单）')
    parser.add_argument('--capital', type=float, default=None,
                        help='资金规模（元，缺省 10 亿）')
    parser.add_argument('--root', default=None, help='列存根目录（缺省用配置默认值）')
    parser.add_argument('--top', type=int, default=5, help='样例行数')
    args = parser.parse_args(argv)
    run_cli(capital=args.capital, root=args.root, top=args.top)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
