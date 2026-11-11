"""守护用例：因子方向的「三方差集」必须同号。

三方分别是：

1. **registry 标记**（``src/quant/factor_lib/registry.py``）：``'+'`` 越大越看多、
   ``'-'`` 越大越看空、``'?'`` 未定；
2. **活配置 direction**（``src/advice/config.py`` → ``src/advice/factor_sets.py``）：
   实际参与打分的权重与方向；
3. **固定 IC 快照**（``docs/因子IC快照_*.json``，判定期限 h=60 的 OOS IC 符号）。

背景：2026-11-11 一天内出现两次「配置与文档各说各话」——``cb55a9e`` 半程把
legacy 动量/趋势翻成 ``direction=-1`` 而基线快照未同步；守护用例读冻结快照导致
``F3`` 改方向后输出逐字不变、等于失效。人工核对已证明不可靠，改由本用例盯着。

口径（2026-11-11 用户确认）：registry 标记**以实测 OOS IC 符号为准**。
因此判定依据是 ``docs/因子IC快照_2026-11-11.json`` 的 h=60 OOS IC 符号，
不是公式直觉。快照数值来源与复现命令见该文件 ``_source_files``。

只读：不修改配置、不写 data/。
"""

from __future__ import annotations

import json
from pathlib import Path

from src.advice.config import default_config
from src.quant.factor_lib import registry
from src.quant.factor_lib.factors import load_all

ROOT = Path(__file__).resolve().parents[1]
SNAPSHOT_GLOB = '因子IC快照_*.json'
#: 方向判定统一按最长持有期（long 档 60 日）的 OOS IC 符号
REFERENCE_HORIZON = 60
REG_SIGN = {'+': 1, '-': -1}

#: 已知未对齐：配置 direction 与 h=60 OOS IC 反号，但 h=20 为正（期限依赖），
#: 处置方式是「先观察、不翻方向」，见 docs/因子方向冲突诊断_2026-11-11.md §15。
KNOWN_UNALIGNED: dict[str, str] = {
    'qlib_rsqr20': '期限依赖：h=60 −0.0189 / h=20 +0.0271，配置 +1',
    'qlib_rsqr10': '期限依赖：h=60 −0.0137 / h=20 +0.0036，配置 +1',
}

#: registry 登记为 '?'（未定）的生产因子：无法参与三方比对。集合型断言保证
#: 任何一方补标记后本用例立即失败，逼迫同步另外两方，而不是悄悄多出一条新口径。
KNOWN_UNMARKED: frozenset[str] = frozenset({
    'qlib_rsqr20', 'qlib_rsqr10', 'gtja_003', 'gtja_024', 'a101_024',
    'gtja_010', 'a101_012', 'a101_042', 'gtja_012', 'gtja_041',
})

#: 未进 factor_lib 登记簿的生产因子（legacy 12 因子在 ``src/alpha/factors/``，
#: 本就没有 registry 标记）。它们只受「配置 direction vs IC 符号」两方约束。
KNOWN_UNREGISTERED: frozenset[str] = frozenset({
    'momentum_1m', 'momentum_3m', 'momentum_6m', 'momentum_12m',
    'trend_slope_60', 'ma20_gap', 'rsi_14', 'sharpe_ratio_20',
    'volatility_20', 'max_drawdown_20', 'atr_pct', 'volume_momentum_20',
})


def _snapshot_path() -> Path:
    """取 docs/ 下最新的 IC 快照（重导出为 v2 后自动切换，避免读旧值）。"""
    files = sorted(ROOT.joinpath('docs').glob(SNAPSHOT_GLOB))
    assert files, f'docs/ 下找不到 {SNAPSHOT_GLOB}，方向判定缺少固定参照'
    return files[-1]


def _ic_sign(ic: float) -> int:
    if ic > 0:
        return 1
    if ic < 0:
        return -1
    raise AssertionError(f'IC 恰为 0，无法判定方向符号: {ic}')


def _config_directions() -> dict[str, set[float]]:
    """列名 -> 各档位 direction 集合（同一因子跨档必须一致）。"""
    out: dict[str, set[float]] = {}
    for tier in default_config().tiers.values():
        for spec in tier.factors:
            out.setdefault(spec.column, set()).add(float(spec.direction))
    return out


def test_ic_snapshot_covers_every_scored_factor() -> None:
    """凡参与打分的因子都必须在固定 IC 快照里，否则方向判定没有依据。"""
    snap = json.loads(_snapshot_path().read_text(encoding='utf-8'))
    factors = snap['factors']
    missing = sorted(set(_config_directions()) - set(factors))
    assert not missing, (
        f'IC 快照缺这些打分因子的实测值：{missing}\n'
        f'新增因子必须先补 OOS IC 实测并写进 {_snapshot_path().name}，'
        '不允许在没有实测符号的情况下进配置。')


def test_registry_config_and_ic_signs_agree() -> None:
    """registry 标记 / 活配置 direction / 固定 IC 符号三者必须同号。"""
    load_all()
    snap = json.loads(_snapshot_path().read_text(encoding='utf-8'))
    factors = snap['factors']
    config = _config_directions()

    problems: list[str] = []
    unmarked: set[str] = set()
    unregistered: set[str] = set()
    unaligned: set[str] = set()

    for column, dirs in sorted(config.items()):
        if len(dirs) != 1:
            problems.append(f'{column}: 跨档位 direction 不一致 {sorted(dirs)}')
            continue
        cfg_direction = dirs.pop()
        cfg_sign = 1 if cfg_direction > 0 else -1
        ic = factors[column][f'h{REFERENCE_HORIZON}']['oos_ic']
        ic_sign = _ic_sign(ic)
        try:
            spec = registry.get_spec(column)
        except KeyError:
            spec = None

        if spec is None:
            unregistered.add(column)
            reg_label = 'registry 未登记'
            reg_sign = None
        else:
            reg_label = f'registry={spec.direction!r}'
            reg_sign = REG_SIGN.get(spec.direction)
            if reg_sign is None:
                unmarked.add(column)
        if cfg_sign != ic_sign:
            unaligned.add(column)
            if column not in KNOWN_UNALIGNED:
                problems.append(
                    f'{column}: 配置 direction={cfg_direction:+.0f} 与 '
                    f'h{REFERENCE_HORIZON} OOS IC {ic:+.5f} 反号（{reg_label}）')
        if reg_sign is not None and cfg_sign != reg_sign:
            problems.append(
                f'{column}: {reg_label}（{reg_sign:+d}）与 '
                f'配置 direction={cfg_direction:+.0f} 不同号')
        if reg_sign is not None and reg_sign != ic_sign:
            problems.append(
                f'{column}: {reg_label}（{reg_sign:+d}）与 '
                f'h{REFERENCE_HORIZON} OOS IC {ic:+.5f} 反号')

    assert not problems, (
        '因子方向三方不一致（registry / 配置 / IC 快照）：\n  '
        + '\n  '.join(problems)
        + f'\n\n参照文件：{_snapshot_path().name}（h{REFERENCE_HORIZON} OOS IC）'
        + '\n处置：以实测 OOS IC 符号为准，同步另外两方；'
          '若确认是期限依赖，写进 KNOWN_UNALIGNED 并注明 h=20 的数值。')

    assert unmarked == set(KNOWN_UNMARKED), (
        'registry 未定方向（?）的生产因子集合发生变化：\n'
        f'  实际 {sorted(unmarked)}\n'
        f'  预期 {sorted(KNOWN_UNMARKED)}\n'
        '补标记时须同时核对配置 direction 与本快照 h=60 IC 符号，'
        '并同步更新 KNOWN_UNMARKED。')

    assert unregistered == set(KNOWN_UNREGISTERED), (
        '未进 factor_lib 登记簿的生产因子集合发生变化：\n'
        f'  实际 {sorted(unregistered)}\n'
        f'  预期 {sorted(KNOWN_UNREGISTERED)}\n'
        '新增生产因子应先进 registry 登记（含 direction），再更新本清单。')

    # 已知未对齐项必须「恰好」是实际反号的那批：修好一个却忘了删清单，也要失败。
    assert unaligned == set(KNOWN_UNALIGNED), (
        '已知未对齐清单与实际反号集合不符：\n'
        f'  实际反号 {sorted(unaligned)}\n'
        f'  清单记录 {sorted(KNOWN_UNALIGNED)}\n'
        '（清单条目：' + '；'.join(f'{k} {v}' for k, v in KNOWN_UNALIGNED.items()) + '）')
