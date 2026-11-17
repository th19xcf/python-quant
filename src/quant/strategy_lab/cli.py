"""策略试算命令行：解析规则、跑一次回测、只打印标量指标。

用法::

    # 列出可用字段（含 PIT 口径）
    .venv\\Scripts\\python.exe -m src.quant.strategy_lab.cli --fields

    # 列出已保存规则
    .venv\\Scripts\\python.exe -m src.quant.strategy_lab.cli --list-rules

    # 跑一条规则文件（dry 口径：只算不落盘）
    .venv\\Scripts\\python.exe -m src.quant.strategy_lab.cli --rule config/strategy_lab/rules/示例.json

    # 跑内置示例并落净值明细
    .venv\\Scripts\\python.exe -m src.quant.strategy_lab.cli --sample --apply
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .backtest import run_rule
from .fields import OPERATORS, field_catalog
from .rules import (
    BENCHMARKS,
    DEFAULT_COST,
    DEFAULT_PORTFOLIO,
    RULES_DIR,
    RuleError,
    list_rules,
    load_rule,
    rule_text,
    validate_rule,
)

ROOT = Path(__file__).resolve().parents[3]
NAV_DIR = ROOT / 'reports' / 'strategy_lab'


def _out(text: str) -> None:
    sys.stdout.write(text + '\n')


def _err(text: str) -> None:
    sys.stderr.write(text + '\n')

SAMPLE_RULE = {
    'rule_version': 'strategy_lab/v1',
    'rule_type': 'stock_select',
    'name': '示例_低估值低波月频',
    'description': '60日波动率最低50% 且 EP 前30%，按 EP 降序取前 50，月频等权',
    'universe': {'mode': 'all_market'},
    'conditions': [
        {'field': 'vol_60', 'op': '<=', 'operand': {'type': 'quantile', 'q': 0.5}},
        {'field': 'ep', 'op': '>=', 'operand': {'type': 'quantile', 'q': 0.7}},
    ],
    'sort': {'field': 'ep', 'direction': 'desc'},
    'top_n': 50,
    'rebalance': {'frequency': 'monthly'},
    'period': {'start': '2019-01-04', 'end': '2026-11-13'},
    'benchmark': '000300.SH',
}


def _write_nav(name: str, nav: list) -> Path:
    NAV_DIR.mkdir(parents=True, exist_ok=True)
    path = NAV_DIR / f'{name}_nav.csv'
    lines = ['date,equity'] + [f'{d},{e}' for d, e in nav]
    path.write_text('\n'.join(lines) + '\n', encoding='utf-8', newline='\n')
    return path


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description='策略试算（结构化条件 → 复用既有引擎）')
    source = parser.add_mutually_exclusive_group()
    source.add_argument('--rule', type=Path, help='规则 JSON 路径')
    source.add_argument('--sample', action='store_true', help='跑内置示例规则')
    source.add_argument('--fields', action='store_true', help='列出可用字段与 PIT 口径')
    source.add_argument('--list-rules', action='store_true', help='列出已保存规则')
    parser.add_argument('--apply', action='store_true', help='落净值明细到 reports/strategy_lab/')
    args = parser.parse_args(argv)

    if args.fields:
        _out(json.dumps({'fields': field_catalog(), 'operators': OPERATORS,
                         'benchmarks': BENCHMARKS, 'default_cost': DEFAULT_COST,
                         'default_portfolio': DEFAULT_PORTFOLIO,
                         'rules_dir': str(RULES_DIR)},
                        ensure_ascii=False, indent=2))
        return 0
    if args.list_rules:
        _out(json.dumps(list_rules(), ensure_ascii=False, indent=2))
        return 0

    try:
        if args.sample:
            rule = validate_rule(SAMPLE_RULE)
        elif args.rule is not None:
            rule = load_rule(args.rule)
        else:
            _err('错误：必须给出 --rule、--sample、--fields 或 --list-rules 之一')
            return 2
    except RuleError as exc:
        _err(f'规则错误：{exc}')
        return 2

    _err(rule_text(rule))
    result = run_rule(rule)
    if result.get('status') != 'ok':
        _out(json.dumps({'status': 'empty', 'reason': result.get('reason'),
                         'rule_text': result.get('rule_text')},
                        ensure_ascii=False, indent=2))
        return 1

    nav_path = None
    if args.apply and result.get('nav'):
        nav_path = _write_nav(rule['name'], result['nav'])
    summary = {
        'status': 'ok',
        'rule_type': rule['rule_type'],
        'metrics': result['metrics'],
        'markers': len(result.get('markers') or []),
        'plan_note': result.get('plan_note'),
        'used_codes': result['used_codes'],
        'engine_seconds': result['engine_seconds'],
        'elapsed_seconds': result['elapsed_seconds'],
        'skipped': result['skipped'],
        'dividend_coverage': result.get('dividend_coverage'),
        'nav_path': str(nav_path) if nav_path else None,
    }
    _out(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
