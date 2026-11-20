"""策略试算结果归档：把一次性试算落成可复现的 run 目录。

目录结构与工作台（:mod:`src.backtest.runner.records`）对齐：run 目录放在
``data/local_store/backtest_runs/<run_id>/``，内含 ``spec.json``（规则快照）、
``metrics.json``、``daily_nav.csv``、``benchmark_nav.csv``、``drawdown.csv``、
``markers.json`` 与 ``manifest.json``。只新建目录、不覆盖已有 run；每个文件
先写临时文件再改名（``write_text_atomic``）。

根索引 ``backtest_runs/index.json`` 不改：它的条目由工作台跑批写死
``spec_sha256`` / ``script_sha256`` / ``period`` 等字段，策略试算没有对应语义，
写空值会污染工作台的运行清单；因此只在 run 目录内写 ``manifest.json``。
"""
from __future__ import annotations

import hashlib
import json
import re
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

from src.data.atomic_write import write_text_atomic
from src.data.local_store.paths import backtest_runs_dir

ROOT = Path(__file__).resolve().parents[3]
SCHEMA = 'strategy_lab_run/v1'
MANIFEST_SCHEMA = 'strategy_lab_run_manifest/v1'
DEFAULT_STORE = ROOT / 'data/local_store'

#: Windows 文件名非法字符与空白统一压成下划线，中文保持不变
_INVALID_NAME = re.compile(r'[\\/:*?"<>|\s]+')


def _safe_name(name: str) -> str:
    return _INVALID_NAME.sub('_', str(name)).strip('._') or 'unnamed'


def _claim_dir(runs_root: Path, base: str) -> Tuple[str, Path]:
    """占一个全新的 run 目录；同一秒重复保存时追加 ``-2``、``-3`` 后缀。"""
    for index in range(1, 100):
        run_id = base if index == 1 else f'{base}-{index}'
        path = runs_root / run_id
        try:
            path.mkdir()
        except FileExistsError:
            continue
        return run_id, path
    raise RuntimeError(f'同一秒内 run 目录过多，放弃保存：{base}')


def _git_head() -> Dict[str, Any]:
    """代码版本：取不到就如实记 unknown，不阻断保存。"""
    try:
        proc = subprocess.run(['git', 'rev-parse', 'HEAD'], cwd=ROOT,
                              capture_output=True, text=True)
    except OSError as exc:
        return {'status': 'unknown', 'full': None, 'short': None,
                'note': f'git 不可用：{type(exc).__name__}'}
    head = proc.stdout.strip()
    if proc.returncode != 0 or not head:
        return {'status': 'unknown', 'full': None, 'short': None,
                'note': 'git rev-parse 失败'}
    return {'status': 'recorded', 'full': head, 'short': head[:7], 'note': ''}


def _data_version(store_root: Path | None = None) -> Dict[str, Any]:
    """只读 ``meta/data_version.json`` 的 current 记录；缺失即 unknown。"""
    root = Path(store_root) if store_root else DEFAULT_STORE
    path = root / 'meta' / 'data_version.json'
    rel = str(path).replace('\\', '/')
    basis: Dict[str, Any] = {'status': 'unknown', 'version': None, 'digest': None,
                             'data_as_of': None, 'source': rel, 'note': ''}
    if not path.is_file():
        basis['note'] = '数据版本台账不存在（meta/data_version.json）'
        return basis
    try:
        payload = json.loads(path.read_text(encoding='utf-8'))
    except (OSError, ValueError) as exc:
        basis['note'] = f'数据版本台账不可读：{type(exc).__name__}'
        return basis
    current = payload.get('current') if isinstance(payload, dict) else None
    if not isinstance(current, dict) or not current.get('version'):
        basis['note'] = '数据版本台账无 current 记录'
        return basis
    basis.update({
        'status': 'recorded',
        'version': str(current.get('version')),
        'digest': current.get('digest'),
        'data_as_of': current.get('as_of'),
    })
    return basis


def _csv(header: str, rows: Iterable, fmt: str) -> str:
    lines = [header] + [f'{row[0]},{float(row[1]):{fmt}}' for row in rows]
    return '\n'.join(lines) + '\n'


def _json_text(payload: Any) -> str:
    return json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + '\n'


def _inventory(run_dir: Path, names: List[str]) -> List[Dict[str, Any]]:
    files: List[Dict[str, Any]] = []
    for name in names:
        data = (run_dir / name).read_bytes()
        files.append({'name': name, 'bytes': len(data),
                      'sha256': hashlib.sha256(data).hexdigest()})
    return files


def save_run(payload: Dict[str, Any], rule: Dict[str, Any], *,
             root: Path | None = None, store_root: Path | None = None,
             now: datetime | None = None) -> Dict[str, Any]:
    """把一次试算结果写成独立 run 目录；返回 run_id 与落盘路径。

    ``root`` 可指向临时的 runs 根目录（测试用），缺省为正式
    ``data/local_store/backtest_runs``；``store_root`` 用于定位数据版本台账。
    """
    if not isinstance(payload, dict) or payload.get('status') != 'ok':
        raise ValueError('没有可保存的试算结果（status != ok）')
    runs_root = Path(root) if root else backtest_runs_dir()
    runs_root.mkdir(parents=True, exist_ok=True)

    moment = now or datetime.now()
    created_at = moment.isoformat(timespec='seconds')
    name = str(rule.get('name') or payload.get('rule_text') or 'unnamed')
    base = f'strategy_lab_{_safe_name(name)}_{moment.strftime("%Y%m%d-%H%M%S")}'
    run_id, run_dir = _claim_dir(runs_root, base)

    nav = list(payload.get('nav') or [])
    bench = list(payload.get('benchmark_curve') or [])
    drawdown = list(payload.get('drawdown') or [])
    markers = list(payload.get('markers') or [])
    targets = list(payload.get('targets') or [])
    positions = dict(payload.get('current') or {})
    trades = list(payload.get('trades') or [])
    freshness = dict(payload.get('freshness') or {})
    head = _git_head()
    basis = _data_version(store_root)

    metrics_doc = {
        'schema': SCHEMA,
        'run_id': run_id,
        'created_at': created_at,
        'name': name,
        'rule_type': rule.get('rule_type'),
        'code_head': head,
        'data_version': basis,
        'summary': {key: payload.get(key) for key in
                    ('rule_text', 'elapsed_seconds', 'engine_seconds', 'used_codes')},
        'metrics': payload.get('metrics') or {},
        'dividend_coverage': payload.get('dividend_coverage') or {},
        'period_effective': payload.get('period_effective') or {},
        # 数据新鲜度：行情/因子面板末日落后目标日时的告警原文，不阻断试算
        'freshness': freshness,
        'rows': {'nav': len(nav), 'benchmark': len(bench),
                 'drawdown': len(drawdown), 'markers': len(markers),
                 'targets': len(targets), 'positions': len(positions.get('positions') or {}),
                 'trades': len(trades)},
    }

    write_text_atomic(_json_text(rule), run_dir / 'spec.json')
    write_text_atomic(_csv('date,equity', nav, '.2f'),
                      run_dir / 'daily_nav.csv')
    write_text_atomic(_csv('date,equity', bench, '.2f'),
                      run_dir / 'benchmark_nav.csv')
    write_text_atomic(_csv('date,drawdown', drawdown, '.8f'),
                      run_dir / 'drawdown.csv')
    write_text_atomic(_json_text(markers), run_dir / 'markers.json')
    write_text_atomic(_json_text(targets), run_dir / 'targets.json')
    write_text_atomic(_json_text(positions), run_dir / 'positions.json')
    write_text_atomic(_json_text(trades), run_dir / 'trades.json')
    write_text_atomic(_json_text(metrics_doc), run_dir / 'metrics.json')

    manifest = {
        'schema': MANIFEST_SCHEMA,
        'run_id': run_id,
        'created_at': created_at,
        'name': name,
        'rule_type': rule.get('rule_type'),
        'code_head': head['short'],
        'data_version': basis.get('version'),
        'metrics': {key: (payload.get('metrics') or {}).get(key) for key in
                    ('annual', 'max_drawdown', 'sharpe', 'volatility',
                     'final_equity', 'trades', 'rebalances')},
        'files': _inventory(run_dir, ['spec.json', 'metrics.json', 'daily_nav.csv',
                                      'benchmark_nav.csv', 'drawdown.csv',
                                      'markers.json', 'targets.json',
                                      'positions.json', 'trades.json']),
        'index': {
            'updated': False,
            'reason': '工作台 index.json 条目要求 spec/script/period 等跑批字段，'
                      '策略试算不写空值，仅在本目录 manifest 登记',
        },
    }
    write_text_atomic(_json_text(manifest), run_dir / 'manifest.json')
    return {'run_id': run_id, 'run_dir': str(run_dir),
            'manifest': str(run_dir / 'manifest.json'), 'index_updated': False}
