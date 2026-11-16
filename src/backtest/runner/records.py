"""运行记录：可复现四要素、run 目录、根索引、数据版本标记。

run 目录与索引落在正式层 ``data/local_store/backtest_runs/``（结果资产，随数据
一并镜像备份），不写 ``docs/``。路径唯一来源为
:func:`src.data.local_store.paths.backtest_runs_dir`。

数据版本标记只读 ``meta/data_version.json`` 的当前版本：新 run 写进
metrics.json / progress.json；历史 run 由 :func:`bind_run_data_basis` 凭证据
人工绑定到独立侧车 ``data_basis.json``，不按日期或文件名推断、不改写既有
metrics.json、不触发任何自动重跑。
"""
from __future__ import annotations

import hashlib
import json
import os
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

from src.data.atomic_write import write_text_atomic
from src.data.local_store.paths import backtest_runs_dir

from .errors import SpecError

ROOT = Path(__file__).resolve().parents[3]
DAILY_INDEX_PARQUET = ROOT / 'data/local_store/meta/daily_index.parquet'
DAILY_INDEX_MANIFEST = ROOT / 'data/local_store/meta/daily_index.manifest.json'
DATA_VERSION_FILE = ROOT / 'data/local_store/meta/data_version.json'
RUNNER_DIR = ROOT / 'src/backtest/runner'
DEFAULT_RUNS_DIR = backtest_runs_dir()
DEFAULT_INDEX_PATH = DEFAULT_RUNS_DIR / 'index.json'
INDEX_SCHEMA = 'backtest_run_index/v1'
#: 跑批「基于哪版数据」标记：新 run 写进 metrics.json / progress.json，
#: 历史 run 的人工绑定写独立侧车（不改写既有 metrics.json）。
RUN_BASIS_FILE = 'data_basis.json'
RUN_BASIS_SCHEMA = 'run_data_basis/v1'
#: 逐期持仓表（归因用）：成交日收盘快照，老 run 没有该文件时归因自动降级
POSITIONS_FILE = 'positions.parquet'
POSITIONS_SCHEMA = 'backtest_positions/v1'


def _write_positions(run_dir: Path,
                     snapshots: Any) -> Dict[str, Any]:
    """把成交日收盘持仓快照落成 positions.parquet，并返回登记元数据。"""
    import polars as pl

    rows: list[Dict[str, Any]] = []
    dates: list[str] = []
    for snap in snapshots or ():
        day = str(snap.get('date'))
        dates.append(day)
        equity = float(snap.get('equity') or 0.0)
        prices = snap.get('prices') or {}
        for code, qty in (snap.get('positions') or {}).items():
            price = prices.get(code)
            weight = None
            if price is not None and equity > 0:
                weight = float(qty) * float(price) / equity
            rows.append({
                'date': day,
                'code': str(code),
                'weight': weight,
                'shares': int(qty),
                'close_raw': None if price is None else float(price),
            })
    if not rows:
        return {'schema': POSITIONS_SCHEMA, 'file': None, 'rows': 0, 'snapshots': 0}
    frame = pl.DataFrame(rows, schema={
        'date': pl.Utf8,
        'code': pl.Utf8,
        'weight': pl.Float64,
        'shares': pl.Int64,
        'close_raw': pl.Float64,
    })
    path = run_dir / POSITIONS_FILE
    frame.write_parquet(path)
    return {
        'schema': POSITIONS_SCHEMA,
        'file': POSITIONS_FILE,
        'rows': frame.height,
        'snapshots': len(dates),
        'first_date': dates[0],
        'last_date': dates[-1],
    }


def sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, 'rb') as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b''):
            digest.update(chunk)
    return digest.hexdigest()


def rel(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(ROOT)).replace('\\', '/')
    except ValueError:
        return str(path)


def git_head() -> str:
    proc = subprocess.run(['git', 'rev-parse', 'HEAD'], cwd=ROOT,
                          capture_output=True, text=True)
    head = proc.stdout.strip()
    if proc.returncode != 0 or not head:
        raise RuntimeError(f'无法读取代码 HEAD：{proc.stderr.strip() or "git 不可用"}')
    return head


def data_version() -> Dict[str, Any]:
    for path in (DAILY_INDEX_PARQUET, DAILY_INDEX_MANIFEST):
        if not path.exists():
            raise SpecError(f'数据版本文件缺失：{rel(path)}')
    manifest = json.loads(DAILY_INDEX_MANIFEST.read_text(encoding='utf-8'))
    declared = {k: manifest.get(k) for k in
                ('sha256', 'generated_at', 'rows', 'coverage', 'start', 'end')
                if k in manifest}
    return {
        'dataset': 'daily_index',
        'parquet': rel(DAILY_INDEX_PARQUET),
        'parquet_sha256': sha256_file(DAILY_INDEX_PARQUET),
        'manifest': rel(DAILY_INDEX_MANIFEST),
        'manifest_sha256': sha256_file(DAILY_INDEX_MANIFEST),
        'manifest_declared': declared,
    }


def store_data_basis(store_root: Path | None = None) -> Dict[str, Any]:
    """只读 ``meta/data_version.json`` 的当前数据版本；取不到即 unknown，不猜。

    跑批启动时调用一次，结果原样写进该次运行的 metrics.json 与 progress.json，
    回答「这次回测基于哪版基础表」。台账缺失或不可读不阻断回测，只标记未知。
    """
    root = Path(store_root) if store_root else ROOT / 'data/local_store'
    path = root / 'meta' / 'data_version.json'
    basis: Dict[str, Any] = {
        'status': 'unknown',
        'version': None,
        'digest': None,
        'data_as_of': None,
        'source': rel(path),
        'read_at': datetime.now().isoformat(timespec='seconds'),
        'note': '',
    }
    if not path.is_file():
        basis['note'] = '数据版本台账不存在（meta/data_version.json）'
        return basis
    try:
        payload = json.loads(path.read_text(encoding='utf-8'))
    except (OSError, ValueError) as exc:
        basis['note'] = f'数据版本台账不可读：{type(exc).__name__}'
        return basis
    current = payload.get('current') if isinstance(payload, dict) else None
    if not isinstance(current, dict) or not current.get('version') or not current.get('digest'):
        basis['note'] = '数据版本台账无 current 记录'
        return basis
    basis.update({
        'status': 'recorded',
        'version': str(current.get('version')),
        'digest': str(current.get('digest')),
        'data_as_of': current.get('as_of'),
    })
    return basis


def _read_json_file(path: Path) -> Dict[str, Any] | None:
    if not path.is_file():
        return None
    try:
        payload = json.loads(path.read_text(encoding='utf-8'))
    except (OSError, ValueError):
        return None
    return payload if isinstance(payload, dict) else None


def read_run_data_basis(run_dir: Path, metrics: Dict[str, Any] | None = None,
                        progress: Dict[str, Any] | None = None) -> Dict[str, Any]:
    """读取一个 run 的「基于哪版数据」标记。

    优先级：metrics.json → data_basis.json（人工绑定侧车）→ progress.json。
    三处都没有就给 status=unknown；不按日期、目录名或文件名反推。
    """
    run_dir = Path(run_dir)
    if not isinstance(metrics, dict):
        metrics = _read_json_file(run_dir / 'metrics.json') or {}
    if not isinstance(progress, dict):
        progress = _read_json_file(run_dir / 'progress.json') or {}
    candidates = (
        ('metrics.json', metrics.get('data_basis')),
        (RUN_BASIS_FILE, _read_json_file(run_dir / RUN_BASIS_FILE)),
        ('progress.json', progress.get('data_basis')),
    )
    for source, payload in candidates:
        if isinstance(payload, dict) and (payload.get('version') or payload.get('status')):
            basis = dict(payload)
            basis['source'] = source
            return basis
    return {
        'status': 'unknown',
        'version': None,
        'digest': None,
        'data_as_of': None,
        'source': 'none',
        'note': '未找到数据版本标记（记录早于版本台账或未登记）',
    }


def _ledger_lookup(version: str, store_root: Path | None = None
                   ) -> tuple[str, str | None, str | None]:
    """在版本台账里查给定版本：返回 (current/history/not_found/ledger_missing, digest, as_of)。"""
    root = Path(store_root) if store_root else ROOT / 'data/local_store'
    payload = _read_json_file(root / 'meta' / 'data_version.json')
    if payload is None:
        return 'ledger_missing', None, None
    raw_current = payload.get('current')
    current: dict = raw_current if isinstance(raw_current, dict) else {}
    if current.get('version') == version:
        return 'current', current.get('digest'), current.get('as_of')
    for item in payload.get('versions') or []:
        if isinstance(item, dict) and item.get('version') == version:
            return 'history', item.get('digest'), item.get('as_of')
    return 'not_found', None, None


def bind_run_data_basis(run_dir: Path, *, version: str, evidence: str,
                        note: str = '', store_root: Path | None = None
                        ) -> Dict[str, Any]:
    """人工绑定历史 run 的数据版本（须同时给版本与证据，禁止推断）。

    写独立侧车 ``data_basis.json``，不改写既有 metrics.json；版本能在台账里
    找到时补 digest / 基准日，找不到就如实记 ``ledger_check=not_found``。
    """
    version = str(version or '').strip()
    evidence = str(evidence or '').strip()
    if not version or not evidence:
        raise ValueError('bind-run 必须同时给出 --version 与 --evidence；禁止按日期或文件名推断')
    run_dir = Path(run_dir)
    if not run_dir.is_dir():
        raise FileNotFoundError(f'run 目录不存在：{run_dir}')
    ledger_check, digest, as_of = _ledger_lookup(version, store_root)
    entry = {
        'schema': RUN_BASIS_SCHEMA,
        'status': 'bound',
        'version': version,
        'digest': digest,
        'data_as_of': as_of,
        'evidence': evidence,
        'bound_at': datetime.now().isoformat(timespec='seconds'),
        'ledger_check': ledger_check,
        'note': note or '人工绑定：证据由操作者提供，未按日期或文件名推断',
    }
    write_text_atomic(json.dumps(entry, ensure_ascii=False, indent=2) + '\n',
                      run_dir / RUN_BASIS_FILE)
    return entry


def runner_sources() -> Dict[str, Any]:
    """runner 各模块的联合哈希：CLI 脚本之外的执行逻辑也在 src/backtest/runner/。"""
    if not RUNNER_DIR.is_dir():
        raise SpecError(f'runner 源码目录缺失：{rel(RUNNER_DIR)}')
    files = sorted(RUNNER_DIR.glob('*.py'))
    if not files:
        raise SpecError(f'runner 源码目录为空：{rel(RUNNER_DIR)}')
    digest = hashlib.sha256()
    for path in files:
        digest.update(path.name.encode('utf-8'))
        digest.update(b'\0')
        digest.update(sha256_file(path).encode('ascii'))
        digest.update(b'\0')
    return {'dir': rel(RUNNER_DIR), 'files': len(files), 'sha256': digest.hexdigest()}


def collect_repro(spec_bytes: bytes, script_path: Path,
                  recipe_bytes: bytes | None = None) -> Dict[str, Any]:
    head = git_head()
    return {
        'code_head': {'short': head[:7], 'full': head},
        'script': {'path': rel(script_path), 'sha256': sha256_file(script_path)},
        'runner_sources': runner_sources(),
        'spec_sha256': sha256_bytes(spec_bytes),
        'recipe_sha256': sha256_bytes(recipe_bytes) if recipe_bytes is not None else None,
        'data_version': data_version(),
    }


def make_run_id(name: str, head: str, now: datetime | None = None) -> str:
    stamp = (now or datetime.now()).strftime('%Y%m%d-%H%M%S')
    return f'{stamp}_{name}_{head[:7]}'


def write_run(run_dir: Path, spec_bytes: bytes, result: Dict[str, Any],
              repro: Dict[str, Any], spec: Dict[str, Any],
              recipe_bytes: bytes | None = None,
              data_basis: Dict[str, Any] | None = None) -> Dict[str, Any]:
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / 'spec.json').write_bytes(spec_bytes)
    if recipe_bytes is not None:
        (run_dir / 'recipe.txt').write_bytes(recipe_bytes)

    resolved = spec['_resolved']
    payload = {
        'schema': 'backtest_run/v1',
        'run_id': run_dir.name,
        'created_at': datetime.now().isoformat(timespec='seconds'),
        'name': spec['name'],
        'description': spec['description'],
        'repro': repro,
        'data_basis': data_basis or {
            'status': 'unknown', 'version': None, 'digest': None,
            'data_as_of': None, 'note': '运行启动时未采集数据版本',
        },
        'period': resolved['period'],
        'universe': resolved['universe'],
        'factor': resolved['factor'],
        'exclusions': resolved['exclusions'],
        'rebalance': resolved['rebalance'],
        'portfolio': resolved['portfolio'],
        'cost': resolved['cost'],
        'experimental': bool(spec.get('experimental', False)),
        'cost_note': ('零成本实验，不得用于正式结论'
                      if resolved['cost']['stamp_tax_basis'] == 'fixed_0.0%' else ''),
        'metrics': result['metrics'],
        'timing': result.get('timing', {}),
        'skipped': result['skipped'],
        'contract_violations': result['contract_violations'],
        'nav_rows': len(result['nav']),
    }
    payload['positions'] = _write_positions(
        run_dir, result.get('position_snapshots'))
    (run_dir / 'metrics.json').write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + '\n',
        encoding='utf-8', newline='\n')
    nav_lines = ['date,equity'] + [f'{d},{e:.2f}' for d, e in result['nav']]
    (run_dir / 'daily_nav.csv').write_text('\n'.join(nav_lines) + '\n',
                                           encoding='utf-8', newline='\n')
    return payload


def index_entry(payload: Dict[str, Any], run_dir: Path,
                runs_dir: Path) -> Dict[str, Any]:
    metrics = payload['metrics']
    repro = payload['repro']
    basis = payload.get('data_basis') or {}
    return {
        'run_id': payload['run_id'],
        'created_at': payload['created_at'],
        'name': payload['name'],
        'dir': rel(run_dir),
        'relative_dir': str(run_dir.relative_to(runs_dir)).replace('\\', '/'),
        'code_head': repro['code_head']['short'],
        'script_sha256': repro['script']['sha256'],
        'spec_sha256': repro['spec_sha256'],
        'recipe_sha256': repro['recipe_sha256'],
        'data_version_sha256': repro['data_version']['parquet_sha256'],
        'data_basis_version': basis.get('version'),
        'data_basis_as_of': basis.get('data_as_of'),
        'data_basis_status': basis.get('status'),
        'period': payload['period'],
        'metrics': {k: metrics.get(k) for k in
                    ('annual', 'max_drawdown', 'turnover_two_way',
                     'cost_share_of_gross', 'cost_bps', 'rebalances')},
        'status': 'ok',
    }


def update_index(runs_dir: Path, entry: Dict[str, Any]) -> Path:
    runs_dir.mkdir(parents=True, exist_ok=True)
    path = runs_dir / 'index.json'
    if path.exists():
        data = json.loads(path.read_text(encoding='utf-8'))
        if not isinstance(data, dict) or not isinstance(data.get('runs'), list):
            raise RuntimeError(f'运行索引格式非法：{rel(path)}')
    else:
        data = {'schema': INDEX_SCHEMA, 'runs': []}
    data['schema'] = INDEX_SCHEMA
    data['runs'] = [r for r in data['runs'] if r.get('run_id') != entry['run_id']]
    data['runs'].insert(0, entry)
    data['updated_at'] = datetime.now().isoformat(timespec='seconds')
    tmp = path.with_name(path.name + '.tmp')
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True) + '\n',
                   encoding='utf-8', newline='\n')
    os.replace(tmp, path)
    return path
