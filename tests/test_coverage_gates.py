"""V-13 模块级覆盖率门槛的配置自检。

目的：全局门禁（本地 ``fail_under = 71``，未接入 CI）与
``[tool.coverage-gates]`` 的模块门槛都是「删掉一行就静默失效」的配置。本文件把
它们钉成棘轮：配置被删、被改小、或门槛登记到未达标的模块上，都会立刻判红。

对应落地记录：V-13（全局门槛抬到实测向下取整，关键模块按实测值另设独立阶梯）。
"""

import json
import tomllib
from pathlib import Path

import pytest

from tools import check_module_coverage as gate

REPO_ROOT = Path(__file__).resolve().parents[1]
PYPROJECT = REPO_ROOT / 'pyproject.toml'

# 已登记门槛的模块 → 门槛值。2026-11-21 实测：src/backtest 73.51%（被 runner 47.51%、
# visualization 16.22% 稀释）、src/backtest/engine 89.41%、src/risk 95.18%、
# src/data/quality 76.78%。整包按 73 登记、撮合内核单列 89，避免可视化/编排把
# 关键路径门槛拉低；数值只允许上调，未达标模块不得为了让门禁变绿而调低。
EXPECTED_MODULE_GATES = {
    'src/backtest': 73,
    'src/backtest/engine': 89,
    'src/risk': 85,
    'src/data/quality': 76,
}


def _pyproject() -> dict:
    return tomllib.loads(PYPROJECT.read_text(encoding='utf-8'))


def test_local_global_gate_pinned():
    """本地全局门槛钉在实测向下取整值（2026-11-21 实测 71.04% → 71）"""
    assert _pyproject()['tool']['coverage']['report']['fail_under'] == 71


def test_module_gates_registry_is_pinned():
    """模块门槛登记表必须与已确认清单逐项相等（多一个、少一个都判红）"""
    assert gate.load_gates(PYPROJECT) == EXPECTED_MODULE_GATES


def test_core_chain_gates_at_least_85():
    """撮合内核与风控门槛不得低于 85：低于 85 属「下调门槛消红」，DG-26 禁止

    ``src/backtest`` 整包按实测 73.51% 登记为 73（runner / visualization 稀释），
    ``src/data/quality`` 按 76.78% 登记为 76；两者的防倒退由上面的登记表
    逐项相等保证（改小任何一项都会判红）。
    """
    gates = gate.load_gates(PYPROJECT)
    for module in ('src/backtest/engine', 'src/risk'):
        assert gates[module] >= 85, f'{module} 门槛被下调到 {gates[module]}%'


def _write_coverage_json(path: Path, files: dict) -> None:
    path.write_text(json.dumps({'files': files, 'totals': {}}), encoding='utf-8')


def _summary(covered: int, total: int) -> dict:
    return {'summary': {'covered_lines': covered, 'num_statements': total,
                        'missing_lines': total - covered,
                        'percent_covered': 100.0 * covered / total if total else 100.0}}


def test_aggregate_uses_statements_and_module_prefix_only(tmp_path):
    """聚合只在模块目录内、按语句数加权，不受目录外文件与百分比的干扰"""
    files = {
        'src\\risk\\rules.py': _summary(90, 100),          # 计入：90/100
        'src\\risk\\sub\\deep.py': _summary(10, 20),       # 计入：10/20（子目录也属该模块）
        'src\\risk_extra\\other.py': _summary(0, 50),      # 不计入：前缀不是 src/risk/
        'src\\backtest\\x.py': _summary(0, 10),            # 不计入
    }
    assert gate.aggregate(files, 'src/risk') == (100, 120)
    assert gate.percent(100, 120) == pytest.approx(83.3333, rel=1e-4)


def test_gate_passes_at_threshold(tmp_path):
    """恰好等于门槛即通过（各档均不得判红）"""
    cov = tmp_path / 'coverage.json'
    _write_coverage_json(cov, {
        'src\\risk\\rules.py': _summary(85, 100),
        # 其余已登记模块也要在场：缺文件会被判成「n/a → 不达标」
        'src\\backtest\\strategies\\x.py': _summary(85, 100),
        'src\\backtest\\engine\\base_engine.py': _summary(89, 100),
        'src\\data\\quality\\checker.py': _summary(76, 100),
    })
    assert gate.main(['--coverage-json', str(cov), '--pyproject', str(PYPROJECT)]) == 0


def test_gate_fails_below_threshold(tmp_path, capsys):
    """低于门槛必须退出 1 并打出分子分母"""
    cov = tmp_path / 'coverage.json'
    _write_coverage_json(cov, {
        'src\\risk\\rules.py': _summary(84, 100),
        'src\\backtest\\strategies\\x.py': _summary(90, 100),
        'src\\backtest\\engine\\base_engine.py': _summary(90, 100),
        'src\\data\\quality\\checker.py': _summary(80, 100),
    })
    assert gate.main(['--coverage-json', str(cov), '--pyproject', str(PYPROJECT)]) == 1
    out = capsys.readouterr().out
    assert 'FAIL src/risk' in out
    assert '84/100' in out


def test_gate_fails_when_config_missing(tmp_path):
    """配置段被删时必须报错退出 1，而不是静默通过"""
    bare = tmp_path / 'pyproject.toml'
    bare.write_text('[tool.coverage.report]\nfail_under = 63\n', encoding='utf-8')
    cov = tmp_path / 'coverage.json'
    _write_coverage_json(cov, {'src\\risk\\rules.py': _summary(100, 100)})
    with pytest.raises(SystemExit):
        gate.main(['--coverage-json', str(cov), '--pyproject', str(bare)])


def test_gate_fails_when_report_missing(tmp_path):
    """覆盖率报告缺失时必须退出 1，不能被当成「全达标」"""
    assert gate.main(['--coverage-json', str(tmp_path / 'nope.json'),
                      '--pyproject', str(PYPROJECT)]) == 1


def test_gate_fails_when_module_not_present_in_report(tmp_path, capsys):
    """登记了门槛但报告里一条文件都没命中时必须判红。

    这是「路径写错 / 目录改名 / 用例没收集到」的静默通过形态：分母为 0 时
    百分比按约定是 100.0，直接比大小就会显示 PASS，等于门槛根本没生效。
    """
    cov = tmp_path / 'coverage.json'
    # 只有 src/risk 的文件，src/backtest/engine 与 src/data/quality 一条都没有
    _write_coverage_json(cov, {'src\\risk\\rules.py': _summary(100, 100)})
    assert gate.main(['--coverage-json', str(cov), '--pyproject', str(PYPROJECT)]) == 1
    out = capsys.readouterr().out
    assert 'FAIL src/backtest/engine' in out
    assert 'FAIL src/data/quality' in out
    assert 'n/a' in out
