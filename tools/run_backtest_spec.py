#!/usr/bin/env python
"""统一回测 runner：中文配方为人工唯一入口，spec.json 为机器中间产物。

用法::

    # 只看解析结果（不跑回测、不写文件）
    .venv\\Scripts\\python.exe tools/run_backtest_spec.py \
        --recipe examples/backtest_recipes/v3_monthly.recipe.txt --parse-only

    # 试运行（校验规格、打印计划，不写文件）
    .venv\\Scripts\\python.exe tools/run_backtest_spec.py \
        --recipe examples/backtest_recipes/v3_monthly.recipe.txt

    # 实跑并落运行记录
    .venv\\Scripts\\python.exe tools/run_backtest_spec.py \
        --recipe examples/backtest_recipes/v3_monthly.recipe.txt --apply

    # 机器中间产物入口（调试用，人工请用 --recipe）
    .venv\\Scripts\\python.exe tools/run_backtest_spec.py --spec <spec.json> --apply
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

os.environ.setdefault('LOGURU_LEVEL', 'WARNING')

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from loguru import logger

from src.backtest.runner import pipeline, records
from src.backtest.runner import recipe as recipe_mod
from src.backtest.runner import spec as spec_mod
from src.backtest.runner.errors import SpecError
from src.backtest.runner.progress import ProgressReporter
from src.backtest.runner.registry import CONDITIONS, FACTORS


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description='统一回测 runner（默认 dry-run；--apply 才落运行记录）')
    source = parser.add_mutually_exclusive_group()
    source.add_argument('--recipe', type=Path, help=f'中文配方（{recipe_mod.RECIPE_SUFFIX}）')
    source.add_argument('--spec', type=Path, help='spec.json（机器中间产物，调试用）')
    parser.add_argument('--parse-only', action='store_true',
                        help='只解析并打印规格摘要与剔除链顺序，不跑回测')
    parser.add_argument('--apply', action='store_true', help='实跑并写运行记录')
    parser.add_argument('--runs-dir', type=Path, default=Path('_staging/backtest_runs'),
                        help='运行记录根目录（默认 _staging/backtest_runs）')
    parser.add_argument('--no-progress', action='store_true',
                        help='关闭 progress.json 与净值增量文件（默认开启）')
    parser.add_argument('--list-registry', action='store_true',
                        help='列出已实现的因子与剔除条件')
    return parser


def load_input(args: argparse.Namespace):
    if args.recipe is not None:
        recipe_path = args.recipe if args.recipe.is_absolute() else ROOT / args.recipe
        spec = recipe_mod.parse_recipe_file(recipe_path)
        return spec, recipe_mod.canonical_bytes(spec), recipe_mod.recipe_bytes(recipe_path), recipe_path
    spec_path = args.spec if args.spec.is_absolute() else ROOT / args.spec
    spec = spec_mod.load_spec(spec_path)
    # 用规范化字节而不是文件原文：保证 spec_sha256 与快照口径一致，
    # 手工改过缩进/键序的 spec.json 不会得出不同的短哈希。
    return spec, spec_mod.dump_spec(spec), None, spec_path


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    if args.list_registry:
        print(json.dumps({'factors': FACTORS.describe(),
                          'conditions': CONDITIONS.describe()},
                         ensure_ascii=False, indent=2))
        return 0
    if args.recipe is None and args.spec is None:
        print('错误：必须给出 --recipe（人工入口）或 --spec（机器中间产物）', file=sys.stderr)
        return 2
    if args.parse_only and args.apply:
        print('错误：--parse-only 与 --apply 互斥（只看解析结果不跑回测、不落盘）',
              file=sys.stderr)
        return 2

    try:
        spec, spec_bytes, recipe_bytes, source_path = load_input(args)
    except SpecError as exc:
        print(f'错误：{exc}', file=sys.stderr)
        return 2

    runs_dir = args.runs_dir if args.runs_dir.is_absolute() else ROOT / args.runs_dir
    if args.parse_only:
        payload = spec_mod.summary(spec)
        payload['spec_sha256'] = recipe_mod.spec_sha256(spec)
        payload['source'] = records.rel(source_path)
        payload['exclusion_order_note'] = '剔除行顺序即执行顺序；其余行序不影响解析结果'
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        print('\n未运行回测、未写任何文件。')
        return 0

    try:
        repro = records.collect_repro(
            spec_bytes, Path(__file__).resolve(), recipe_bytes)
    except Exception as exc:  # noqa: BLE001  采集失败即拒绝产出不可复现的 run
        print(f'错误：无法采集可复现信息：{exc}', file=sys.stderr)
        return 2
    run_id = records.make_run_id(spec['name'], repro['code_head']['full'])
    run_dir = runs_dir / run_id
    plan = {
        'mode': 'apply' if args.apply else 'dry-run',
        'run_id': run_id,
        'run_dir': records.rel(run_dir),
        'name': spec['name'],
        'period': spec['_resolved']['period'],
        'rebalance': spec['_resolved']['rebalance'],
        'exclusion_chain': [x['name'] for x in spec['_resolved']['exclusions']],
        'factor': spec['_resolved']['factor'],
        'progress': not args.no_progress,
        'repro': repro,
        'spec_sha256': recipe_mod.spec_sha256(spec),
        'registry': {'factors': FACTORS.names(), 'conditions': CONDITIONS.names()},
    }
    if not args.apply:
        print(json.dumps(plan, ensure_ascii=False, indent=2))
        print('\ndry-run：未跑回测、未写任何文件；确认后加 --apply。')
        return 0

    runs_dir.mkdir(parents=True, exist_ok=True)
    suffix = 2
    while run_dir.exists():
        run_dir = runs_dir / f'{run_id}_{suffix}'
        suffix += 1
    run_dir.mkdir(parents=True)
    sink = logger.add(run_dir / 'run.log', level='INFO', encoding='utf-8')
    reporter = ProgressReporter(run_dir, enabled=not args.no_progress)
    logger.info('run 开始：{}（配方/规格 {}）', run_dir.name, records.rel(source_path))
    try:
        result = pipeline.execute(spec, reporter)
        payload = records.write_run(run_dir, spec_bytes, result, repro, spec,
                                    recipe_bytes)
        reporter.finish(payload['metrics'])
        entry = records.index_entry(payload, run_dir, runs_dir)
        index_path = records.update_index(runs_dir, entry)
    except Exception as exc:  # noqa: BLE001  失败也要落 progress 现场并保留增量净值
        reporter.fail(f'{type(exc).__name__}: {exc}')
        logger.exception('run 失败：{}', run_dir.name)
        print(f'错误：{exc}', file=sys.stderr)
        print(f'失败现场保留在 {records.rel(run_dir)}（progress.json / daily_nav.partial.csv）',
              file=sys.stderr)
        return 1
    finally:
        reporter.close()
        logger.remove(sink)

    print(json.dumps({
        'status': 'ok',
        'run_id': run_dir.name,
        'run_dir': records.rel(run_dir),
        'index': records.rel(index_path),
        'metrics': {k: payload['metrics'].get(k) for k in
                    ('annual', 'max_drawdown', 'turnover_two_way',
                     'cost_share_of_gross', 'rebalances')},
        'timing': payload.get('timing', {}),
        'spec_sha256': repro['spec_sha256'],
        'recipe_sha256': repro['recipe_sha256'],
        'code_head': repro['code_head']['short'],
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == '__main__':
    sys.exit(main())
