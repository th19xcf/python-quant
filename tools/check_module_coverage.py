"""模块级覆盖率门槛（V-13 落地工具）。

背景：全局 ``[tool.coverage.report] fail_under`` 只约束整套 src 的合计百分比，
关键链路可以被「其它目录覆盖率高」稀释。本工具按模块（目录前缀）汇总
``coverage json`` 的分子分母，并对照 ``[tool.coverage-gates]`` 里登记的
模块门槛逐个判定，任一不达标即退出 1。

门槛口径：
- 全局门槛保持原样：本地 ``pyproject.toml`` 的 ``fail_under = 63``、
  CI ``.github/workflows/ci.yml`` 的 ``--cov-fail-under=66`` 都不动；
- 关键模块另设 ≥85% 的独立门槛，数值锚定实测值、只允许上调。

用法（CI 与本地同一条命令）::

    python -m pytest --cov=src --cov-report=json:coverage.json --cov-fail-under=0
    python tools/check_module_coverage.py --coverage-json coverage.json

退出码：0 = 全部达标；1 = 有模块低于门槛，或覆盖率报告 / 配置缺失。
"""

from __future__ import annotations

import argparse
import json
import sys
import tomllib
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
PYPROJECT = REPO_ROOT / 'pyproject.toml'


def load_gates(pyproject_path: Path = PYPROJECT) -> dict[str, float]:
    """读取 ``[tool.coverage-gates] modules`` 的模块门槛表。"""
    data = tomllib.loads(pyproject_path.read_text(encoding='utf-8'))
    gates = (data.get('tool') or {}).get('coverage-gates') or {}
    modules = gates.get('modules') or {}
    if not modules:
        raise SystemExit(f'{pyproject_path} 缺少 [tool.coverage-gates] modules 配置')
    return {str(key).strip('/'): float(value) for key, value in modules.items()}


def _normalize(path: str) -> str:
    return path.replace('\\', '/').lstrip('./')


def aggregate(files: dict[str, Any], module: str) -> tuple[int, int]:
    """按目录前缀汇总覆盖率，返回 ``(已覆盖语句数, 语句总数)``。

    只统计 ``files`` 里落在该模块目录下的文件；分母用 ``num_statements``
    （与 coverage 自身的百分比口径一致），不使用 ``percent_covered`` 直接平均，
    避免大小文件被等权处理。
    """
    covered = 0
    total = 0
    for raw_path, payload in files.items():
        path = _normalize(raw_path)
        if not path.startswith(module + '/'):
            continue
        summary = payload.get('summary') or {}
        covered += int(summary.get('covered_lines') or 0)
        total += int(summary.get('num_statements') or 0)
    return covered, total


def percent(covered: int, total: int) -> float:
    return 100.0 * covered / total if total else 100.0


def check(coverage_json: Path, gates: dict[str, float]) -> list[tuple[str, int, int, float, float, bool]]:
    """返回逐模块判定结果：``(模块, 已覆盖, 总数, 实测百分比, 门槛, 是否达标)``。"""
    payload = json.loads(coverage_json.read_text(encoding='utf-8'))
    files = payload.get('files') or {}
    rows = []
    for module, threshold in gates.items():
        covered, total = aggregate(files, module)
        actual = percent(covered, total)
        rows.append((module, covered, total, actual, threshold, actual + 1e-9 >= threshold))
    return rows


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description='模块级覆盖率门槛判定（V-13）')
    parser.add_argument('--coverage-json', default='coverage.json',
                        help='coverage json 报告路径（默认 coverage.json）')
    parser.add_argument('--pyproject', default=str(PYPROJECT), help='门槛配置文件路径')
    args = parser.parse_args(argv)

    coverage_json = Path(args.coverage_json)
    if not coverage_json.is_file():
        print(f'[coverage-gate] 找不到覆盖率报告: {coverage_json}', file=sys.stderr)
        return 1

    rows = check(coverage_json, load_gates(Path(args.pyproject)))
    failed = False
    for module, covered, total, actual, threshold, ok in rows:
        flag = 'PASS' if ok else 'FAIL'
        print(f'[coverage-gate] {flag} {module:<24} {covered:>6}/{total:<6} '
              f'= {actual:6.2f}%  门槛 {threshold:.0f}%')
        failed = failed or not ok
    return 1 if failed else 0


if __name__ == '__main__':
    raise SystemExit(main())
