#!/usr/bin/env python3

"""
性能基线生成器（V-31）。

``tests/performance_test.py`` 里的 3 个 ``perf`` 用例一直是「只有阈值、没有
基线产物」：阈值能拦住明显退化，但说不清「这台机器上当时是多少」。本脚本
把每次运行的实测耗时连同环境标识一起落成 ``reports/perf_baseline.json``，
供后续跨机器 / 跨版本对比。

用法::

    .venv\\Scripts\\python.exe tools\\bench_perf_baseline.py
    .venv\\Scripts\\python.exe tools\\bench_perf_baseline.py --repeat 5
    .venv\\Scripts\\python.exe tools\\bench_perf_baseline.py --output reports/perf_baseline.json

设计约束：

* **不重写阈值判定**：是否通过仍由 ``tests/performance_test.py`` 里的断言
  决定，本脚本只负责取数与留痕，避免出现两套性能口径；
* **重复取中位数**：junit 的 ``time`` 是单次墙钟，机器噪声足以让两次运行差
  出几十个百分点，故默认跑 ``--repeat 3`` 轮、逐用例取中位数；
* **失败不产基线**：任一轮出现 fail / error 即以非零码退出且不覆盖已有基线，
  防止把一次劣化运行写成「新基线」；
* **写盘原子**：走 ``src.data.atomic_write.write_text_atomic``，写一半失败时
  旧基线保持原样。
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import statistics
import subprocess
import sys
import tempfile
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.data.atomic_write import write_text_atomic  # noqa: E402

#: 性能用例所在文件：基线的取数范围由它决定，换文件必须显式改这里
PERF_TARGET = 'tests/performance_test.py'

#: 默认输出位置
DEFAULT_OUTPUT = 'reports/perf_baseline.json'

#: 记录进基线的依赖版本：性能敏感项才有意义，不要罗列整个环境
TRACKED_PACKAGES = ('polars', 'numpy', 'pandas', 'pyarrow', 'pytest')


def _git(*args: str) -> str | None:
    """在仓库内执行只读 git 命令；失败返回 None（基线不因 git 不可用而中断）。"""
    try:
        proc = subprocess.run(  # noqa: S603 —— argv 固定，无 shell、无外部输入
            ['git', '-C', str(REPO_ROOT), *args],
            capture_output=True, text=True, check=False,
        )
    except (OSError, ValueError):
        return None
    if proc.returncode != 0:
        return None
    return proc.stdout.strip()


def _package_version(name: str) -> str | None:
    """按 importlib.metadata 取版本；未安装返回 None。"""
    from importlib import metadata

    try:
        return metadata.version(name)
    except metadata.PackageNotFoundError:
        return None


def _env_fingerprint() -> dict[str, Any]:
    """机器可识别的环境标识：跨机器对比时先比这一段是否一致。"""
    commit = _git('rev-parse', 'HEAD')
    status = _git('status', '--porcelain')
    return {
        'python': sys.version.split()[0],
        'implementation': platform.python_implementation(),
        'executable': sys.executable,
        'platform': platform.platform(),
        'system': platform.system(),
        'machine': platform.machine(),
        'processor': platform.processor(),
        'cpu_count': os.cpu_count(),
        'node': platform.node(),
        'memory_trace_env': os.environ.get('QUANT_MEM_TRACE'),
        'packages': {name: _package_version(name) for name in TRACKED_PACKAGES},
        'git': {
            'commit': commit,
            'short': commit[:7] if commit else None,
            'dirty': bool(status),
        },
    }


def _parse_junit(path: Path) -> dict[str, dict[str, Any]]:
    """解析 junit xml，返回 {用例键: {module, case, time, outcome}}。

    键用 ``<module>::<case>``，与 pytest 的 nodeid 形状一致，便于人工对照。
    非 pass 结果（failure / error / skipped）也返回，由调用方判定是否可用。
    """
    tree = ET.parse(path)  # noqa: S314 —— 只解析本脚本自己产出的临时文件
    cases: dict[str, dict[str, Any]] = {}
    for node in tree.getroot().iter('testcase'):
        module = node.get('classname') or ''
        case = node.get('name') or ''
        outcome = 'passed'
        if node.find('failure') is not None or node.find('error') is not None:
            outcome = 'failed'
        elif node.find('skipped') is not None:
            outcome = 'skipped'
        cases[f'{module}::{case}'] = {
            'module': module,
            'case': case,
            'time': float(node.get('time') or 0.0),
            'outcome': outcome,
        }
    return cases


def _run_round() -> dict[str, dict[str, Any]]:
    """跑一轮 perf 用例，返回解析后的用例耗时。"""
    with tempfile.TemporaryDirectory(prefix='perf_baseline_') as tmp:
        report = Path(tmp) / 'perf.xml'
        # -o addopts= 清掉 pyproject 里的默认 ``-q -m 'not perf'``，否则 -m perf 会被抵消
        argv = [sys.executable, '-m', 'pytest', PERF_TARGET,
                '-o', 'addopts=', '-m', 'perf', '-q', '--no-header',
                '-p', 'no:cacheprovider', f'--junitxml={report}']
        proc = subprocess.run(  # noqa: S603 —— argv 固定，无 shell、无外部输入
            argv, cwd=str(REPO_ROOT), capture_output=True, text=True, check=False)
        if proc.returncode != 0:
            tail = '\n'.join((proc.stdout or '').strip().splitlines()[-20:])
            raise RuntimeError(
                f'perf 用例未通过（returncode={proc.returncode}），不生成基线：\n{tail}')
        if not report.is_file():
            raise RuntimeError('pytest 未产出 junit xml，无法取耗时')
        return _parse_junit(report)


def _build_payload(rounds: list[dict[str, dict[str, Any]]],
                   repeat: int) -> dict[str, Any]:
    """把多轮结果合并成基线：逐用例取中位数，逐轮样本一并留痕。"""
    keys: list[str] = sorted({key for run in rounds for key in run})
    tests: dict[str, Any] = {}
    for key in keys:
        samples = [run[key]['time'] for run in rounds if key in run]
        first = next(run[key] for run in rounds if key in run)
        tests[key] = {
            'module': first['module'],
            'case': first['case'],
            'outcome': first['outcome'],
            'seconds': round(statistics.median(samples), 6),
            'samples': [round(value, 6) for value in samples],
            'min_seconds': round(min(samples), 6),
            'max_seconds': round(max(samples), 6),
        }
    return {
        'schema': 1,
        'generator': 'tools/bench_perf_baseline.py',
        'target': PERF_TARGET,
        'unit': 'seconds',
        'repeat': repeat,
        'generated_at': datetime.now().astimezone().isoformat(timespec='seconds'),
        'env': _env_fingerprint(),
        'tests': tests,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description='生成 perf 用例耗时基线')
    parser.add_argument('--output', default=DEFAULT_OUTPUT,
                        help=f'基线输出路径（默认 {DEFAULT_OUTPUT}，相对仓库根）')
    parser.add_argument('--repeat', type=int, default=3,
                        help='重复轮数，逐用例取中位数（默认 3）')
    args = parser.parse_args(argv)

    if args.repeat < 1:
        parser.error('--repeat 必须 >= 1')

    rounds: list[dict[str, dict[str, Any]]] = []
    for index in range(args.repeat):
        print(f'[perf-baseline] 第 {index + 1}/{args.repeat} 轮', flush=True)
        rounds.append(_run_round())

    payload = _build_payload(rounds, args.repeat)
    if not payload['tests']:
        print('[perf-baseline] 未收集到任何 perf 用例，拒绝产出空基线', file=sys.stderr)
        return 2

    not_passed = [key for key, item in payload['tests'].items()
                  if item['outcome'] != 'passed']
    if not_passed:
        print(f'[perf-baseline] 存在非 pass 用例 {not_passed}，拒绝写入基线',
              file=sys.stderr)
        return 1

    output = Path(args.output)
    if not output.is_absolute():
        output = REPO_ROOT / output
    text = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + '\n'
    write_text_atomic(text, output)

    print(f'[perf-baseline] 已写入 {output}')
    for key, item in payload['tests'].items():
        print(f'  {key}: {item["seconds"]:.4f}s '
              f'(min {item["min_seconds"]:.4f} / max {item["max_seconds"]:.4f})')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
