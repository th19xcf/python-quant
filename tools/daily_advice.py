#!/usr/bin/env python3

r"""每日推荐一键入口：行情 → 因子 → 打分 → IC 门禁 → 风控/事件冷却 → 清单。

一条命令完成「读当日数据 → 出清单」，组合的是既有链路，不另造平行逻辑：

- 数据更新（可选 ``--update``）：直接调 ``python -m src.data.local_store.build build``；
- 选池（新增）：``src.advice.universe.list_local_stock_codes`` → ``src.data.universe_filter.filter_universe``
  （ST / 停牌 / 次新 / 面值 / 流动性 / 板块 / 旧码 / 复牌首日等，逐类剔除计数，阈值只在
  ``UniverseFilterConfig``）；规则说明见 ``docs/每日推荐筛选与判断清单.md``；
- 因子 / 打分 / IC 门禁 / 行业上限 / 事件冷却 / 换手控制：``src.advice.pipeline.AdviceEngine``；
- 数据新鲜度与停更判定：``src.data.quality.freshness.check_all``（与 ``tools/data_freshness_check.py``
  同一套分级告警）+ ``src.data.quality.stale_status``（股票池停更剔除）。

输出：

- ``reports/advice/daily/{YYYYMMDD}/daily_advice.md``：可直接阅读的清单；
- ``reports/advice/daily/{YYYYMMDD}/daily_advice.json``：同内容的结构化版本；
- ``reports/advice/daily/{YYYYMMDD}.json``：既有按日归档（``daily_store`` 口径，不重复实现）。

退出码：0 正常；1 数据更新失败；2 未到收盘后 15:30；3 数据陈旧被拦截；4 数据不足无产出。

用法::

    .venv\\Scripts\\python.exe tools/daily_advice.py --as-of 2026-11-09
    .venv\\Scripts\\python.exe tools/daily_advice.py --update --force
    .venv\\Scripts\\python.exe tools/daily_advice.py --allow-stale
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import subprocess
import sys
from datetime import date, datetime, time
from pathlib import Path
from typing import Any, Callable

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

DEFAULT_STORE_ROOT = ROOT / 'data' / 'local_store'
DEFAULT_FRESHNESS_CONFIG = ROOT / 'src' / 'data' / 'quality' / 'freshness_config.json'
DEFAULT_OUT_ROOT = ROOT / 'reports' / 'advice' / 'daily'

#: 与 ``src/advice/cli.py`` 同一收盘闸门：未到 15:30 不许出当日清单
ADVICE_CLOSE = time(15, 30)

EXIT_OK = 0
EXIT_UPDATE_FAILED = 1
EXIT_TIME_GATE = 2
EXIT_STALE = 3
EXIT_NO_DATA = 4

#: 分组顺序：新进 → 维持 → 退出 → 观察
GROUP_ORDER = ('新进', '维持', '退出', '观察')

FIELDS_NOTE = (
    '得分 = 档位内因子加权综合分（z 加权和，可超出 ±1，负值表示该档因子整体看空）；'
    '分位 = 当日截面排名分位（0~1）；'
    '主要因子贡献取该档位权重最高的前 3 个因子及其标准化取值；'
    '门禁状态 = 该条在 IC 门禁 / ST / 事件冷却 / 行业上限 / 流动性 / 换手上限上的实际结论。'
)

ROTATION_NOTE = (
    '每日出单 ≠ 每日调仓：清单每个交易日都会重出，但持仓受最小持有期、'
    '卖出确认日与单次换手上限约束，绝大多数标的在持有期内只会显示「维持」。'
    '只有「退出」与「新进」才对应本次需要处理的交易，且仍受 20% 单边换手上限。'
)

#: 可买只数时间序列：按 (信号日, 档位) 追加，判断空仓是市场状态还是规则冲突
COUNT_FILENAME = 'available_counts.csv'
COUNT_COLUMNS = (
    'signal_date', 'tier', 'universe_kept', 'panel_rows', 'soft_pass',
    'score_pass', 'all_gates_pass', 'new', 'hold', 'exit', 'avoid',
    'ic_gate_blocked', 'rules_version', 'generated_at',
)


def count_rows(result: Any, universe: Any, *,
               generated_at: datetime) -> list[dict[str, str]]:
    """把一次运行的分层计数摊平成 (信号日 × 档位) 的一行行记录。

    逐级口径：``universe_kept`` 选池后只数 → ``panel_rows`` 当日有行情的只数
    → ``soft_pass`` 软筛选通过 → ``score_pass`` 分数达买入分位 → ``all_gates_pass``
    同时过分数、可交易、成本、风控与事件冷却 → ``new/hold/exit`` 最终清单方向计数。

    判读方法：若 ``score_pass`` 高而 ``all_gates_pass`` 长期为 0，是规则自相矛盾；
    若两者都低，是市场状态（如全市场多头排列占比极低）。
    """
    version = getattr(universe, 'version', '')
    kept = getattr(universe, 'kept_count', len(getattr(universe, 'kept', [])))
    rows: list[dict[str, str]] = []
    for tier_key, advice in sorted(result.tiers.items()):
        stats = getattr(advice, 'filter_stats', {}) or {}
        rows.append({
            'signal_date': result.trade_date.isoformat(),
            'tier': tier_key,
            'universe_kept': str(kept),
            'panel_rows': str(stats.get('funnel_panel_rows', 0)),
            'soft_pass': str(stats.get('funnel_soft_pass', 0)),
            'score_pass': str(stats.get('funnel_score_pass', 0)),
            'all_gates_pass': str(stats.get('funnel_candidates', 0)),
            'new': str(stats.get('funnel_buys', 0)),
            'hold': str(stats.get('funnel_holds', 0)),
            'exit': str(stats.get('funnel_sells', 0)),
            'avoid': str(stats.get('funnel_avoid', 0)),
            'ic_gate_blocked': str(stats.get('ic_gate_blocked', 0)),
            'rules_version': str(version),
            'generated_at': generated_at.isoformat(timespec='seconds'),
        })
    return rows


def write_available_counts(out_root: Path, result: Any, universe: Any, *,
                           generated_at: datetime) -> Path:
    """按 (信号日, 档位) 幂等追加「可买只数」序列，返回 CSV 路径。"""
    from src.data.atomic_write import write_text_atomic  # noqa: PLC0415

    path = out_root / COUNT_FILENAME
    existing: list[dict[str, str]] = []
    if path.is_file():
        with path.open('r', encoding='utf-8', newline='') as handle:
            existing = list(csv.DictReader(handle))
    fresh = count_rows(result, universe, generated_at=generated_at)
    keys = {(row['signal_date'], row['tier']) for row in fresh}
    merged = [row for row in existing
              if (row.get('signal_date'), row.get('tier')) not in keys] + fresh
    merged.sort(key=lambda row: (row.get('signal_date') or '',
                                 row.get('tier') or ''))
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=COUNT_COLUMNS,
                            extrasaction='ignore')
    writer.writeheader()
    writer.writerows(merged)
    write_text_atomic(buffer.getvalue(), path)
    return path


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


# ----------------------------------------------------------------------
# 数据状态
# ----------------------------------------------------------------------
def probe_freshness(store_root: Path, config_path: Path,
                    reference: date) -> dict[str, Any]:
    """读取新鲜度分级告警，压成入口需要的「数据截止日 + stale 状态」。

    判据沿用 ``freshness_config.json``：``critical`` 视为不可出单，
    ``warn`` 视为「落后但可出单」，两者都必须在输出里显式写出来。
    """
    from src.data.quality.freshness import check_all, load_config  # noqa: PLC0415

    results = check_all(store_root, load_config(config_path), reference)
    quotes = next((item for item in results if item.key == 'quotes_stock'), None)
    alerts = [
        {
            'key': item.key,
            'label': item.label,
            'severity': item.severity,
            'latest': item.latest_date.isoformat() if item.latest_date else None,
            'lag_days': item.lag_calendar_days,
            'reasons': list(item.reasons),
        }
        for item in results if item.severity != 'ok'
    ]
    if quotes is None:
        return {
            'reference': reference.isoformat(), 'label': 'A股日线行情',
            'latest': None, 'severity': 'critical', 'lag_days': None,
            'reasons': ['新鲜度配置未登记 quotes_stock'], 'alerts': alerts,
            'blocking': True,
        }
    latest = quotes.latest_date
    return {
        'reference': reference.isoformat(),
        'label': quotes.label,
        'latest': latest.isoformat() if latest else None,
        'severity': quotes.severity,
        'lag_days': quotes.lag_calendar_days,
        'reasons': list(quotes.reasons),
        'alerts': alerts,
        'blocking': latest is None or quotes.severity == 'critical',
    }


def print_data_status(fresh: dict[str, Any]) -> None:
    """把数据截止日与 stale 状态打到 stdout，不允许静默。"""
    latest = fresh.get('latest') or '未知'
    severity = str(fresh.get('severity') or 'unknown').upper()
    lag = fresh.get('lag_days')
    lag_text = '未知' if lag is None else f'{lag} 个自然日'
    print(f'数据截止日: {latest}（{fresh.get("label")}，基准日 {fresh.get("reference")}）')
    print(f'数据状态  : {severity}，落后 {lag_text}')
    if severity != 'OK':
        for reason in fresh.get('reasons') or []:
            print(f'  [告警] {reason}')
    for item in fresh.get('alerts') or []:
        if item.get('key') == 'quotes_stock':
            continue
        detail = '；'.join(item.get('reasons') or []) or item.get('label')
        print(f'  [提示] {item.get("label")}({item.get("severity")}) {detail}')


def run_local_update(since: str | None) -> int:
    """复用既有列存构建入口做增量更新（不另写取数逻辑）。"""
    command = [sys.executable, '-m', 'src.data.local_store.build', 'build']
    command += ['--since', since] if since else ['--all']
    print('数据更新: ' + ' '.join(command))
    completed = subprocess.run(command, cwd=str(ROOT), check=False)
    return completed.returncode


# ----------------------------------------------------------------------
# 清单渲染
# ----------------------------------------------------------------------
def group_of(row: Any) -> str:
    """按方向 + 延后标记归入「新进 / 维持 / 退出 / 观察」。"""
    if row.direction == 'BUY':
        return '新进' if row.disposition != 'deferred' else '新进(延后)'
    if row.direction == 'HOLD':
        return '维持'
    if row.direction == 'SELL':
        return '退出' if row.disposition != 'deferred' else '退出(延后)'
    return '观察'


def gate_text(advice: Any, row: Any) -> str:
    """单条记录的门禁结论：IC 门禁 / 风控拦截 / 换手延后 / 通过。"""
    blocked = bool(advice.filter_stats.get('ic_gate_blocked'))
    if row.disposition == 'deferred':
        text = f'换手上限延后至 {row.defer_until}：{row.defer_reason}'
    elif row.direction == 'AVOID':
        reasons = [note for note in row.risk_notes if note.startswith('剔除原因')]
        detail = reasons[0] if reasons else (row.risk_notes[0] if row.risk_notes else '未通过门槛')
        text = f'拦截：{detail}'
    elif blocked:
        text = 'IC门禁拦截新建仓；本条为持仓管理'
    else:
        text = '通过（ST / 事件冷却 / 行业上限 / 流动性）'
    if getattr(row, 'limit_up_risk', False):
        basis = getattr(row, 'limit_up_basis', '') or 'T日已涨停'
        text = f'{text}；涨停风险（{basis}，次日可能买不进）'
    return text


def _factor_text(row: Any) -> str:
    if not row.top_factors:
        return '-'
    text = ' / '.join(f'{name} {value:+.2f}' for name, value in row.top_factors[:3])
    return text.replace('|', '/')


def _holding_text(row: Any, tier: Any) -> str:
    return (f'{row.horizon} {tier.min_hold_days}-{tier.validity_days} 个交易日'
            f'（剩余 {row.validity_days}）')


def _escape(value: Any) -> str:
    return str(value).replace('|', '/').replace('\n', ' ')


def _row_cells(advice: Any, tier: Any, row: Any) -> list[str]:
    return [
        row.ts_code,
        _escape(row.name),
        f'{row.score:.3f} / {row.score_percentile:.2f}',
        f'{row.direction}{"(延后)" if row.disposition == "deferred" else ""}',
        _escape(_factor_text(row)),
        _escape(gate_text(advice, row)),
        _escape(_holding_text(row, tier)),
    ]


HEADER = ['代码', '名称', '得分 / 分位', '建议动作', '主要因子贡献', '门禁状态', '建议持有期']


def _table(rows: list[list[str]]) -> list[str]:
    lines = ['| ' + ' | '.join(HEADER) + ' |',
             '|' + '|'.join(['---'] * len(HEADER)) + '|']
    lines += ['| ' + ' | '.join(cells) + ' |' for cells in rows]
    return lines


def _tier_block(tier_key: str, advice: Any, tiers: dict[str, Any]) -> list[str]:
    tier = tiers[tier_key]
    counts: dict[str, int] = {}
    for row in advice.rows:
        group = group_of(row)
        counts[group] = counts.get(group, 0) + 1
    lines = [f'## {advice.label}（{tier_key}）· 建议持有期 '
             f'{tier.min_hold_days}-{tier.validity_days} 个交易日', '']
    lines.append(
        f'候选 {advice.candidates} 条，输出 {len(advice.rows)} 条，'
        f'单边换手 {advice.turnover:.0%}，'
        + '，'.join(f'{name} {counts[name]}' for name in GROUP_ORDER if name in counts))
    ic_notes = [note for note in advice.notes if note.startswith('IC监控(')]
    for note in ic_notes:
        lines.append(f'门禁：{note}')
    lines.append('')
    for group in GROUP_ORDER:
        rows = [row for row in advice.rows if group_of(row) == group]
        if not rows:
            continue
        lines.append(f'### {group}（{len(rows)}）')
        lines.append('')
        lines += _table([_row_cells(advice, tier, row) for row in rows])
        lines.append('')
    for group in ('新进(延后)', '退出(延后)'):
        rows = [row for row in advice.rows if group_of(row) == group]
        if rows:
            lines.append(f'### {group}（{len(rows)}）')
            lines.append('')
            lines += _table([_row_cells(advice, tier, row) for row in rows])
            lines.append('')
    return lines


def _universe_block(universe: Any) -> list[str]:
    """选池过滤段落：版本号 + 逐类剔除计数（与 docs 清单一一对应）。"""
    from src.data.universe_filter import RULE_LABELS, RULE_ORDER  # noqa: PLC0415

    lines = ['## 选池过滤（通用规则）', '',
             f'规则版本 `{universe.version}`｜逐条说明见 `{universe.doc_path}`', '',
             f'输入 {universe.input_count} 只 → 保留 {universe.kept_count} 只'
             f'（剔除 {universe.dropped_count} 只）；按规则顺序判定，'
             f'命中首条即停，计数互斥。', '',
             '| 规则 | 剔除只数 |', '|---|---|']
    for rule in RULE_ORDER:
        count = len(universe.dropped.get(rule, ()))
        if count:
            lines.append(f'| {RULE_LABELS[rule]}（`{rule}`） | {count} |')
    if universe.dropped_count == 0:
        lines.append('| 无剔除 | 0 |')
    gaps = {key: value for key, value in universe.unknowns.items() if value}
    if gaps:
        lines.append('')
        lines.append(f'数据缺口（保留但无法确认）：{gaps}')
    lines.append('')
    return lines


def render_markdown(result: Any, fresh: dict[str, Any], *, tiers: dict[str, Any],
                    stale_override: bool, generated_at: datetime,
                    update_command: str = '', universe: Any = None) -> str:
    """渲染可读清单；数据状态与「出单≠调仓」写在最前面。"""
    signal = result.signal_date or result.trade_date
    lines = [f'# 每日推荐清单 · 信号日 {signal}', '']
    lines.append(f'- 生成时间：{generated_at:%Y-%m-%d %H:%M:%S}')
    lines.append(f'- 数据截止日：{fresh.get("latest") or "未知"}（{fresh.get("label")}）')
    lines.append(f'- 数据状态：{str(fresh.get("severity")).upper()}，'
                 f'落后 {fresh.get("lag_days")} 个自然日')
    lines.append(f'- 执行状态：{result.exec_status}，最早可执行 {result.earliest_exec_date}'
                 f'（{result.exec_date_basis}）')
    if universe is not None:
        lines.append(f'- 选池规则版本：{universe.version}（清单文件 `{universe.doc_path}`）')
    if update_command:
        lines.append(f'- 本次已执行数据更新：`{update_command}`')
    lines.append('')
    if stale_override:
        lines.append('> [STALE] 数据未更新到基准日，本次清单由 --allow-stale 显式放行，'
                     '不得当作当日信号使用。')
        lines.append('')
    lines.append('> ' + ROTATION_NOTE)
    lines.append('')
    lines.append('> ' + FIELDS_NOTE)
    lines.append('')
    if universe is not None:
        lines += _universe_block(universe)
    lines.append('## 数据状态告警')
    lines.append('')
    if not fresh.get('alerts'):
        lines.append('全部登记数据集均为 ok。')
    else:
        lines.append('| 数据集 | 最新日期 | 级别 | 落后 | 说明 |')
        lines.append('|---|---|---|---|---|')
        for item in fresh['alerts']:
            reason = '；'.join(item['reasons']) or '-'
            lines.append(f'| {_escape(item["label"])} | {item["latest"] or "缺失"} | '
                         f'{item["severity"]} | {item["lag_days"]} | {_escape(reason)} |')
    lines.append('')
    lines.append('## 汇总')
    lines.append('')
    lines.append('| 档位 | 候选 | 新进 | 维持 | 退出 | 观察 | 单边换手 | IC门禁 |')
    lines.append('|---|---|---|---|---|---|---|---|')
    for key, advice in result.tiers.items():
        counts: dict[str, int] = {}
        for row in advice.rows:
            group = group_of(row)
            counts[group] = counts.get(group, 0) + 1
        gate = '拦截' if advice.filter_stats.get('ic_gate_blocked') else '通过'
        lines.append(
            f'| {advice.label}({key}) | {advice.candidates} | {counts.get("新进", 0)} | '
            f'{counts.get("维持", 0)} | {counts.get("退出", 0)} | '
            f'{counts.get("观察", 0)} | {advice.turnover:.0%} | {gate} |')
    lines.append('')
    for key, advice in result.tiers.items():
        lines += _tier_block(key, advice, tiers)
    lines.append('## 使用边界')
    lines.append('')
    from src.advice.daily_store import usage_boundary  # noqa: PLC0415

    for label, text in usage_boundary().items():
        lines.append(f'- **{label}**：{text}')
    lines.append('')
    return '\n'.join(lines)


def _payload(result: Any, fresh: dict[str, Any], *, stale_override: bool,
             generated_at: datetime, update_command: str,
             universe: Any = None) -> dict[str, Any]:
    """与 Markdown 同源的结构化清单，便于事后核对。"""
    tiers: dict[str, Any] = {}
    for key, advice in result.tiers.items():
        tiers[key] = {
            'label': advice.label,
            'candidates': advice.candidates,
            'turnover_pct': round(advice.turnover * 100, 2),
            'ic_gate_blocked': bool(advice.filter_stats.get('ic_gate_blocked')),
            'notes': advice.notes,
            'items': [
                {
                    'group': group_of(row), 'ts_code': row.ts_code, 'name': row.name,
                    'score': row.score, 'score_percentile': row.score_percentile,
                    'direction': row.direction, 'disposition': row.disposition,
                    'action': f'{row.direction}'
                              f'{"(延后)" if row.disposition == "deferred" else ""}',
                    'top_factors': [f'{name} {value:+.2f}'
                                    for name, value in row.top_factors[:3]],
                    'gate': gate_text(advice, row),
                    'limit_up_risk': bool(getattr(row, 'limit_up_risk', False)),
                    'limit_up_basis': getattr(row, 'limit_up_basis', ''),
                    'horizon': row.horizon, 'validity_days': row.validity_days,
                    'defer_until': str(row.defer_until) if row.defer_until else None,
                }
                for row in advice.rows
            ],
        }
    return {
        'generated_at': generated_at.isoformat(timespec='seconds'),
        'signal_date': str(result.signal_date or result.trade_date),
        'earliest_exec_date': str(result.earliest_exec_date),
        'exec_status': result.exec_status,
        'data_status': fresh,
        'stale_override': stale_override,
        'update_command': update_command,
        'universe_filter': universe.as_dict() if universe is not None else None,
        'equity': result.equity,
        'tiers': tiers,
    }


def print_console(result: Any, fresh: dict[str, Any], stale_override: bool,
                  universe: Any = None) -> None:
    """终端摘要：数据状态 + 选池剔除 + 每个档位的新进/维持/退出。"""
    signal = result.signal_date or result.trade_date
    print('=' * 78)
    print(f'每日推荐清单 · 信号日 {signal} · 最早可执行 {result.earliest_exec_date}'
          f'（{result.exec_status}）')
    print_data_status(fresh)
    if universe is not None:
        print(f'选池规则: {universe.version}（清单 {universe.doc_path}）')
        print(f'选池结果: {universe.counts_text()}')
    if stale_override:
        print('[STALE] 数据未更新到基准日，已用 --allow-stale 放行；不得当作当日信号')
    print('[口径] ' + ROTATION_NOTE)
    print('=' * 78)
    for key, advice in result.tiers.items():
        gate = 'IC门禁=拦截' if advice.filter_stats.get('ic_gate_blocked') else 'IC门禁=通过'
        print(f'\n[{key}] {advice.label}  候选 {advice.candidates} 条，'
              f'单边换手 {advice.turnover:.0%}，{gate}')
        for group in GROUP_ORDER + ('新进(延后)', '退出(延后)'):
            rows = [row for row in advice.rows if group_of(row) == group]
            for row in rows:
                mark = '[涨停]' if getattr(row, 'limit_up_risk', False) else ''
                print(f'  {group:<6} {row.ts_code:<12} {row.name:<8} '
                      f'{mark:<6} 得分 {row.score:.3f}/{row.score_percentile:.2f}  '
                      f'{row.direction:<5} {_factor_text(row)[:40]:<40} '
                      f'{gate_text(advice, row)[:44]}')
    print()


# ----------------------------------------------------------------------
# 主流程
# ----------------------------------------------------------------------
def _build_engine(config: Any) -> Any:
    from src.advice.pipeline import AdviceEngine  # noqa: PLC0415

    return AdviceEngine(config=config)


def resolve_signal_date(store_root: Path, reference: date) -> date:
    """本地个股行情的最新日期（不晚于 ``reference``），读不到时退回 ``reference``。"""
    import polars as pl  # noqa: PLC0415

    path = store_root / 'meta' / 'daily_index.parquet'
    if not path.is_file():
        return reference
    frame = pl.read_parquet(path, columns=['data_type', 'end'])
    ends = [item for item in frame.filter(
        pl.col('data_type') == 'stock')['end'].to_list()
        if isinstance(item, date) and item <= reference]
    return max(ends) if ends else reference


def build_universe(config: Any, store_root: Path,
                   reference: date) -> tuple[date, Any]:
    """复用既有股票池入口 + 通用选池过滤，返回 (信号日, 过滤结果)。"""
    from src.advice.universe import list_local_stock_codes  # noqa: PLC0415
    from src.data.universe_filter import filter_universe  # noqa: PLC0415

    day = resolve_signal_date(store_root, reference)
    codes = list_local_stock_codes(config, ('sh', 'sz', 'bj'), exclude_stale=True)
    return day, filter_universe(day, codes, store_root=store_root)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog='python tools/daily_advice.py',
        description='每日推荐一键入口（因子→打分→IC门禁→风控/事件冷却→清单）')
    parser.add_argument('--as-of', type=_parse_date, default=None,
                        help='信号基准日 YYYY-MM-DD（默认最新交易日）')
    parser.add_argument('--equity', type=float, default=1_000_000.0,
                        help='组合总资金（元），默认 100 万')
    parser.add_argument('--out-root', type=Path, default=DEFAULT_OUT_ROOT,
                        help='输出根目录，按 YYYYMMDD 分子目录')
    parser.add_argument('--store-root', type=Path, default=DEFAULT_STORE_ROOT,
                        help='本地列存根目录，默认 data/local_store')
    parser.add_argument('--freshness-config', type=Path,
                        default=DEFAULT_FRESHNESS_CONFIG,
                        help='新鲜度分级配置 JSON')
    parser.add_argument('--update', action='store_true',
                        help='先复用列存构建入口增量更新行情，再出单')
    parser.add_argument('--full-update', action='store_true',
                        help='配合 --update：全市场全量重建（慢）')
    parser.add_argument('--force', action='store_true',
                        help='忽略收盘后 15:30 的时间检查')
    parser.add_argument('--allow-stale', action='store_true',
                        help='数据 critical 陈旧时仍出单（输出标记 STALE）')
    return parser


def main(argv: list[str] | None = None, *,
         engine_factory: Callable[[Any], Any] | None = None,
         freshness_probe: Callable[[Path, Path, date], dict[str, Any]] | None = None,
         update_runner: Callable[[str | None], int] | None = None,
         universe_builder: Callable[[Any, Path, date], tuple[date, Any]] | None = None,
         ) -> int:
    args = _build_parser().parse_args(argv)
    reference = args.as_of or date.today()
    generated_at = datetime.now()
    if args.as_of is None and not args.force and generated_at.time() < ADVICE_CLOSE:
        print('未到收盘后 15:30，今日清单可能不含完整个股行情；'
              '如需生成请加 --force 或显式 --as-of')
        return EXIT_TIME_GATE

    update_command = ''
    fresh = (freshness_probe or probe_freshness)(
        args.store_root, args.freshness_config, reference)
    if args.update:
        since = None if args.full_update else (fresh.get('latest') or None)
        code = (update_runner or run_local_update)(since)
        if code != 0:
            print(f'数据更新失败（退出码 {code}），已中止出单')
            return EXIT_UPDATE_FAILED
        update_command = ('build --all' if since is None else f'build --since {since}')
        fresh = (freshness_probe or probe_freshness)(
            args.store_root, args.freshness_config, reference)

    if fresh.get('blocking') and not args.allow_stale:
        print_data_status(fresh)
        print('[拒绝出单] 本地行情未更新到基准日或级别为 critical；'
              '请先补数（python -m src.data.local_store.build build --since <日期>），'
              '确需用旧数据请显式加 --allow-stale')
        return EXIT_STALE

    from src.advice.config import default_config, default_output_tiers  # noqa: PLC0415

    config = default_config()
    signal_day, universe = (universe_builder or build_universe)(
        config, args.store_root, reference)
    if not universe.kept:
        print_data_status(fresh)
        print(f'[数据不足] 选池后无剩余标的（信号日 {signal_day}）：{universe.counts_text()}；'
              f'先确认 {args.store_root} 是否有该日行情与 {universe.doc_path} 所述数据源')
        return EXIT_NO_DATA
    engine = (engine_factory or _build_engine)(config)
    args.out_root.mkdir(parents=True, exist_ok=True)
    result = engine.generate(as_of=signal_day, equity=args.equity,
                             tiers=default_output_tiers(),
                             daily_dir=args.out_root, persist_daily=True,
                             generated_at=generated_at, codes=universe.kept)
    total = sum(len(advice.rows) for advice in result.tiers.values())
    if not result.tiers or total == 0:
        print_data_status(fresh)
        print(f'[数据不足] 面板无可用行情或全部标的被剔除（信号日 {result.trade_date}）：'
              f'检查 {args.store_root} 是否有该日数据、meta/names.json 是否就绪')
        return EXIT_NO_DATA

    trade_dir = args.out_root / f'{result.trade_date:%Y%m%d}'
    trade_dir.mkdir(parents=True, exist_ok=True)
    counts_path = write_available_counts(args.out_root, result, universe,
                                         generated_at=generated_at)
    markdown = render_markdown(result, fresh, tiers=config.tiers,
                               stale_override=bool(fresh.get('blocking')),
                               generated_at=generated_at,
                               update_command=update_command,
                               universe=universe)
    markdown += (f'\n> 可买只数时间序列：`{counts_path}`'
                 f'（按信号日 × 档位幂等追加；`score_pass` 高而 `all_gates_pass`'
                 f' 长期为 0 说明规则冲突，两者都低说明是市场状态）\n')
    md_path = trade_dir / 'daily_advice.md'
    md_path.write_text(markdown, encoding='utf-8')
    json_path = trade_dir / 'daily_advice.json'
    json_path.write_text(json.dumps(
        _payload(result, fresh, stale_override=bool(fresh.get('blocking')),
                 generated_at=generated_at, update_command=update_command,
                 universe=universe),
        ensure_ascii=False, indent=2), encoding='utf-8')

    print_console(result, fresh, bool(fresh.get('blocking')), universe)
    print(f'清单: {md_path}')
    print(f'结构: {json_path}')
    print(f'可买只数序列: {counts_path}')
    return EXIT_OK


if __name__ == '__main__':
    raise SystemExit(main())
