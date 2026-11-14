"""运行记录：可复现四要素、run 目录、根索引。

run 目录与索引一律落在 ``_staging/`` 下，不写 ``docs/``、不写 ``data/``。
"""
from __future__ import annotations

import hashlib
import json
import os
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

from .errors import SpecError

ROOT = Path(__file__).resolve().parents[3]
DAILY_INDEX_PARQUET = ROOT / 'data/local_store/meta/daily_index.parquet'
DAILY_INDEX_MANIFEST = ROOT / 'data/local_store/meta/daily_index.manifest.json'
RUNNER_DIR = ROOT / 'src/backtest/runner'
INDEX_SCHEMA = 'backtest_run_index/v1'


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
              recipe_bytes: bytes | None = None) -> Dict[str, Any]:
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
        'period': resolved['period'],
        'universe': resolved['universe'],
        'factor': resolved['factor'],
        'exclusions': resolved['exclusions'],
        'rebalance': resolved['rebalance'],
        'portfolio': resolved['portfolio'],
        'cost': resolved['cost'],
        'metrics': result['metrics'],
        'timing': result.get('timing', {}),
        'skipped': result['skipped'],
        'contract_violations': result['contract_violations'],
        'nav_rows': len(result['nav']),
    }
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
