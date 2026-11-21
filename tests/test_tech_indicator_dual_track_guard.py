"""技术指标双口径「禁止新增引用」守卫。

背景见 ``docs/技术指标双口径登记_2026-11-21.md``：``src/alpha`` 与
``src/quant/factor_lib`` 对同一批技术指标存在两套组装口径（窗口错位一期、
ddof 0/1），当前不允许继续扩散引用。本测试只锁**增量**：把既有引用点做成
白名单快照，新增引用即失败并给出文件:行；不清理存量。

判定口径：以「文件 → 被引用的符号集合」快照比对，而不是硬编码行号，
避免无关改动导致行号漂移时误报。失败信息仍逐条给出当前行号。
"""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCAN_DIRS = ('src', 'tools', 'tests', 'scripts', 'examples', 'config')
SKIP_DIRS = {'.git', '__pycache__', '.venv', 'node_modules', 'tmp', 'reports', 'work'}

# 存量引用白名单：相对仓库根路径 → 允许出现的符号集合。
# 值为空集合表示该文件允许出现但当前无命中（预留，不新增条目）。
WHITELIST: dict[str, frozenset[str]] = {
    # A 侧计算器入口
    'src/advice/factor_panel.py': frozenset({'src.alpha.calculator'}),
    'src/quant/learning/data.py': frozenset({'src.alpha.calculator'}),
    'examples/quickstart.py': frozenset({'src.alpha.calculator'}),
    'tests/test_alpha_cross_sectional.py': frozenset({'src.alpha.calculator'}),
    'tests/test_performance.py': frozenset({'src.alpha.calculator'}),
    # 本地未跟踪脚本（.gitignore 内），保留存量不改
    'scripts/analytics_validation.py': frozenset({'src.alpha.calculator'}),
    # B 侧 factor_lib 技术指标因子（定义处 + 消费方）
    'src/quant/factor_lib/factors/technical.py': frozenset({
        'tech_macd_hist', 'tech_rsi_14', 'tech_atr_14_pct', 'tech_kdj_k',
        'tech_kdj_j', 'tech_ema_bias_20', 'tech_boll_pos_20',
        'tech_volume_ratio_5', 'tech_mom_20', 'tech_ma_gap_5_20',
        'tech_up_streak',
    }),
    'src/advice/factor_eval.py': frozenset({
        'tech_macd_hist', 'tech_ema_bias_20', 'tech_ma_gap_5_20',
        'tech_boll_pos_20', 'tech_mom_20', 'tech_atr_14_pct',
        'tech_volume_ratio_5', 'tech_rsi_14', 'tech_kdj_k', 'tech_kdj_j',
        'tech_up_streak',
    }),
    'src/advice/extended_factors.py': frozenset({
        'tech_macd_hist', 'tech_ema_bias_20', 'tech_boll_pos_20',
        'tech_ma_gap_5_20', 'tech_mom_20', 'tech_atr_14_pct',
        'tech_volume_ratio_5', 'tech_rsi_14', 'tech_kdj_k', 'tech_kdj_j',
        'tech_up_streak',
    }),
    'tests/test_indicator_consistency.py': frozenset({'tech_boll_pos_20'}),
}

CALCULATOR_RE = re.compile(r'(?:from|import)\s+src\.alpha\.calculator')
TECH_FACTOR_RE = re.compile(
    r"""['"]tech_(?:macd_hist|rsi_14|atr_14_pct|kdj_k|kdj_j|ema_bias_20|"""
    r"""boll_pos_20|volume_ratio_5|mom_20|ma_gap_5_20|up_streak)['"]"""
)


def _iter_python_files():
    for name in SCAN_DIRS:
        base = ROOT / name
        if not base.is_dir():
            continue
        for path in base.rglob('*.py'):
            if any(part in SKIP_DIRS for part in path.parts):
                continue
            if path.resolve() == Path(__file__).resolve():
                continue
            yield path


def _scan_file(path: Path) -> list[tuple[int, str]]:
    """返回 [(行号, 符号)]，按行号升序。"""
    hits: list[tuple[int, str]] = []
    try:
        text = path.read_text(encoding='utf-8', errors='ignore')
    except OSError:
        return hits
    for lineno, line in enumerate(text.splitlines(), start=1):
        if CALCULATOR_RE.search(line):
            hits.append((lineno, 'src.alpha.calculator'))
        for match in TECH_FACTOR_RE.finditer(line):
            hits.append((lineno, match.group(0).strip('\'"')))
    return hits


def test_no_new_dual_track_references():
    """新增对两套存量口径的引用即失败。"""
    violations: list[str] = []

    for path in _iter_python_files():
        rel = path.relative_to(ROOT).as_posix()
        allowed = WHITELIST.get(rel, frozenset())
        for lineno, symbol in _scan_file(path):
            if symbol not in allowed:
                violations.append(f'{rel}:{lineno} → {symbol}')

    assert not violations, (
        '检测到对技术指标双口径的新增引用（详见 docs/技术指标双口径登记_2026-11-21.md）：\n'
        + '\n'.join(violations)
        + '\n新代码请只使用 src/tech_analysis/indicators/formulas.py 真源；'
          '如确需扩充白名单，请在登记文档中说明理由。'
    )
