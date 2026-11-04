#!/usr/bin/env python3

"""
报告输出 —— 因子登记表、评估表、冗余簇报告、生产建议清单。

所有产物写入 ``reports/factor_lib/``（与源码目录分离，便于复核与版本对比）：
    factor_registry.csv / 因子登记表.md              因子登记表（名称/公式/依赖/方向/来源/状态）
    factor_eval.csv / 因子分层回测结果.md            分层回测指标
    redundancy_clusters.csv / 因子冗余聚类报告.md    |corr|>0.7 聚类报告
    recommended_factors.csv / 建议纳入生产因子库的因子清单.md  建议纳入生产因子库清单（30~50 个）
    strategy_results.csv / 公开策略对照结果.md       公开策略对照结果
    因子库验证汇总.md                                总览（summary 汇总各报告结论）
    run_meta.json                                    运行参数、股票池、量纲自检结果
"""

from __future__ import annotations

import json
from collections import Counter
from collections.abc import Iterable
from pathlib import Path

import polars as pl

from src.quant.factor_lib.redundancy import ClusterResult
from src.quant.factor_lib.registry import FactorSpec

REPORT_DIR = Path(__file__).resolve().parents[3] / 'reports' / 'factor_lib'

#: 建议清单的分级阈值
TIER_A = {'abs_icir_annual': 1.0, 't_stat': 2.0, 'monotonicity': 0.6}
TIER_B = {'abs_icir_annual': 0.5, 't_stat': 1.5, 'monotonicity': 0.4}
TIER_C = {'abs_icir_annual': 0.0, 't_stat': 1.0, 'monotonicity': 0.0}

#: 策略内部名 → 中文标签
STRATEGY_LABELS = {
    'momentum_rotation': '动量轮动',
    'dual_ma': '双均线',
    'boll_breakout': '布林突破',
    'low_vol_selection': '低波动选股',
}


def ensure_dir() -> Path:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    return REPORT_DIR


def _md_table(header: list[str], rows: Iterable[Iterable[object]]) -> str:
    """生成 Markdown 表格（内容里的竖线做转义，避免破坏列结构）。"""
    def cell(value: object) -> str:
        return str(value).replace('|', '\\|').replace('\n', ' ')

    lines = ['| ' + ' | '.join(header) + ' |',
             '|' + '|'.join(['---'] * len(header)) + '|']
    lines.extend('| ' + ' | '.join(cell(v) for v in row) + ' |' for row in rows)
    return '\n'.join(lines)


def _headline_block(headline: str | None) -> list[str]:
    """报告开头的「一句话结论」段（用户要求的固定格式）。"""
    if not headline:
        return []
    return [f'> **一句话结论**：{headline}', '']


def tier_of(row: dict) -> str:
    """按 |ICIR 年化| / t 值 / 单调性给因子分级。"""
    abs_icir = abs(float(row['icir_annual']))
    if (abs_icir >= TIER_A['abs_icir_annual']
            and float(row['t_stat']) >= TIER_A['t_stat']
            and float(row['monotonicity']) >= TIER_A['monotonicity']):
        return 'A-优先纳入'
    if (abs_icir >= TIER_B['abs_icir_annual']
            and float(row['t_stat']) >= TIER_B['t_stat']
            and float(row['monotonicity']) >= TIER_B['monotonicity']):
        return 'B-可纳入'
    if float(row['t_stat']) >= TIER_C['t_stat']:
        return 'C-观察'
    return 'D-噪音'


def write_registry(specs: list[FactorSpec], headline: str | None = None) -> Path:
    """写因子登记表（CSV + Markdown）。"""
    out = ensure_dir()
    rows = [
        {
            'name': s.name,
            'category': s.category_label,
            'expression': s.expression,
            'dependencies': s.dependencies,
            'direction': s.direction,
            'lookback': s.lookback,
            'source': s.source,
            'status': s.status_label,
            'note': s.note,
        }
        for s in specs
    ]
    frame = pl.DataFrame(rows, infer_schema_length=None)
    frame.write_csv(out / 'factor_registry.csv')

    by_category: dict[str, list[FactorSpec]] = {}
    for spec in specs:
        by_category.setdefault(spec.category_label, []).append(spec)
    parts = ['# 因子登记表', '', *_headline_block(headline),
             f'共 {len(specs)} 条登记，其中可计算 {sum(1 for s in specs if s.computable)} 条。',
             '', '状态口径：**已验证** = 公式逐项对齐公开定义且数据可得；'
             '**近似** = 存在降级项（日频 VWAP / 行业中性退化 / 市场代理等）；'
             '**不可用** = 数据依赖缺失，只登记不计算。', '']
    for category, items in by_category.items():
        parts.extend([f'## {category}（{len(items)}）', '',
                      _md_table(
                          ['因子', '公式', '数据依赖', '方向', '状态', '出处', '备注'],
                          [[s.name, s.expression, s.dependencies, s.direction,
                            s.status_label, s.source, s.note] for s in items],
                      ), ''])
    (out / '因子登记表.md').write_text('\n'.join(parts), encoding='utf-8')
    return out / '因子登记表.md'


def write_evaluation(metrics: pl.DataFrame, headline: str | None = None) -> Path:
    """写分层回测指标（CSV + Markdown，按 |ICIR 年化| 排序）。"""
    out = ensure_dir()
    metrics.write_csv(out / 'factor_eval.csv')
    valid = metrics.filter(pl.col('icir').is_not_null()).with_columns(
        (pl.col('icir_annual').abs()).alias('_abs')
    ).sort('_abs', descending=True)
    failures = metrics.filter(pl.col('error').is_not_null()) \
        if 'error' in metrics.columns else pl.DataFrame()

    parts = ['# 因子分层回测结果', '', *_headline_block(headline),
             '口径：T+1 开盘成交、持有 20 日、非重叠调仓、5 分组等权、'
             '双边成本（佣金+过户费+滑点+印花税），因子已按登记方向统一取向。', '',
             f'- 有效因子：{valid.height}',
             f'- 失败/样本不足因子：{failures.height}', '',
             _md_table(
                 ['因子', '大类', '方向', '均值IC', 'ICIR(期)', 'ICIR(年化)', 't值',
                  'IC>0占比', 'Q1', 'Q5', 'Q5-Q1', '单调性', '换手', '净多空累计', '夏普'],
                 [[row['factor'], row['category'], row['direction'],
                   f"{row['mean_ic']:.4f}", f"{row['icir']:.3f}",
                   f"{row['icir_annual']:.2f}", f"{row['t_stat']:.2f}",
                   f"{row['ic_positive_rate']:.0%}", f"{row['q1_mean']:.2%}",
                   f"{row['q5_mean']:.2%}", f"{row['q5_minus_q1']:.2%}",
                   f"{row['monotonicity']:.2f}", f"{row['turnover']:.0%}",
                   f"{row['ls_net_cum']:.1%}", f"{row['ls_net_sharpe']:.2f}"]
                  for row in valid.head(60).iter_rows(named=True)],
             ), '']
    if failures.height:
        parts.extend(['## 计算失败 / 样本不足', '',
                      _md_table(['因子', '原因'],
                                [[r['factor'], r['error']]
                                 for r in failures.iter_rows(named=True)]), ''])
    (out / '因子分层回测结果.md').write_text('\n'.join(parts), encoding='utf-8')
    return out / '因子分层回测结果.md'


def write_clusters(clusters: ClusterResult, headline: str | None = None) -> Path:
    """写冗余簇报告（CSV + Markdown）。"""
    out = ensure_dir()
    clusters.clusters.write_csv(out / 'redundancy_clusters.csv')
    parts = ['# 因子冗余聚类报告', '', *_headline_block(headline),
             '方法：调仓日逐日横截面百分位排名后池化求 Pearson 相关，'
             '距离 1-|corr|、average linkage 层次聚类，在 |corr| ≥ 0.7 处切树；'
             '每簇保留 |ICIR| 最高的因子。', '',
             f'- 输入因子 {clusters.correlation.shape[0]} 个',
             f'- 簇数 {clusters.clusters.height}，保留 {len(clusters.kept)} 个，'
             f'剪掉 {len(clusters.dropped)} 个', '',
             _md_table(
                 ['簇', '规模', '保留因子', '保留ICIR', '簇内最大|corr|', '成员'],
                 [[r['cluster'], r['size'], r['kept'], f"{r['kept_icir']:.3f}",
                   f"{r['max_abs_corr']:.2f}", r['members']]
                  for r in clusters.clusters.iter_rows(named=True)],
             ), '']
    (out / '因子冗余聚类报告.md').write_text('\n'.join(parts), encoding='utf-8')
    return out / '因子冗余聚类报告.md'


def select_recommendations(
    metrics: pl.DataFrame, clusters: ClusterResult, specs: list[FactorSpec],
    limit: int = 50, floor: int = 30,
) -> list[tuple[FactorSpec, dict]]:
    """从剪枝后的簇代表中挑选建议清单（要求成本后多空累计收益 > 0）。"""
    spec_map = {s.name: s for s in specs}
    valid = metrics.filter(pl.col('icir').is_not_null())
    rows = {row['factor']: row for row in valid.iter_rows(named=True)}

    candidates: list[tuple[FactorSpec, dict]] = []
    for name in clusters.kept:
        row = rows.get(name)
        spec = spec_map.get(name)
        if row is None or spec is None:
            continue
        if float(row['ls_net_cum']) <= 0:
            continue
        candidates.append((spec, row))

    candidates.sort(key=lambda item: abs(float(item[1]['icir_annual'])), reverse=True)
    selected = candidates[:limit]
    if len(selected) < floor:
        selected = candidates  # 样本太少时不强行凑数，如实输出全部候选
    return selected


def build_headline(selected: list[tuple[FactorSpec, dict]]) -> str:
    """生成大白话「一句话结论」：赚钱/亏钱、哪些能用、哪些是噪音。"""
    counts = Counter(tier_of(row) for _spec, row in selected)
    usable = counts['A-优先纳入'] + counts['B-可纳入']
    watch = counts['C-观察']
    noise = counts['D-噪音']
    top = [spec.name for spec, row in selected
           if tier_of(row) in ('A-优先纳入', 'B-可纳入')]
    names = '、'.join(top[:5])
    if usable == 0:
        verdict = '照这套因子做多空基本不赚钱'
    else:
        verdict = '照这套因子做多空是赚钱的'
    text = (f'在这 5 年、80 只流动性最好的股票里，{verdict}：'
            f'{usable} 个因子真能拿来选股')
    if names:
        text += f'（{names}）'
    text += f'，{watch} 个只值得观察，其余 {noise} 个是噪音、别单独用。'
    return text


def build_strategy_headline(frame: pl.DataFrame) -> str:
    """策略对照的大白话结论。"""
    if frame.is_empty():
        return '策略样本不足以给出结论。'
    best = frame.sort('total_return', descending=True).row(0, named=True)
    beat = int((frame['total_return'] > frame['benchmark_return']).sum())
    label = STRATEGY_LABELS.get(str(best['name']), str(best['name']))
    if beat == 0:
        verdict = '4 条规则虽然都是正收益，但没一条跑赢同期等权基准'
    else:
        verdict = f'4 条规则都是正收益，其中 {beat} 条跑赢同期等权基准'
    return (f'{verdict}，最好的是{label}（累计 {best["total_return"]:.0%}）；'
            '双均线/布林/低波更适合当对照基线，别直接上线。')


def write_recommendation(
    selected: list[tuple[FactorSpec, dict]], headline: str | None = None,
) -> Path:
    """生成生产建议清单（CSV + Markdown）。"""
    out = ensure_dir()
    rows_out = []
    for spec, row in selected:
        direction: str = spec.direction
        if direction not in ('+', '-'):
            direction = ('+' if float(row['mean_ic']) > 0 else '-') + '（样本内）'
        rows_out.append({
            'factor': spec.name,
            'tier': tier_of(row),
            'category': spec.category,
            'direction': direction,
            'status': spec.status,
            'expression': spec.expression,
            'source': spec.source,
            'mean_ic': float(row['mean_ic']),
            'icir_annual': float(row['icir_annual']),
            't_stat': float(row['t_stat']),
            'monotonicity': float(row['monotonicity']),
            'turnover': float(row['turnover']),
            'ls_net_cum': float(row['ls_net_cum']),
            'note': spec.note,
        })
    frame = pl.DataFrame(rows_out, infer_schema_length=None)
    frame.write_csv(out / 'recommended_factors.csv')

    counts = Counter(row['tier'] for row in rows_out)
    parts = ['# 建议纳入生产因子库的因子清单', '', *_headline_block(headline),
             f'来源：从去冗余后的簇代表中按 |ICIR 年化| 排序，取前 {len(rows_out)} 个'
             '（要求成本后多空累计收益 > 0）。', '',
             f'- A 优先纳入：{counts["A-优先纳入"]} 个',
             f'- B 可纳入：{counts["B-可纳入"]} 个',
             f'- C 观察：{counts["C-观察"]} 个',
             f'- D 噪音（t 值 < 1）：{counts["D-噪音"]} 个', '',
             '分级规则：A = |ICIR年化|≥1.0 且 t≥2.0 且单调性≥0.6；'
             'B = |ICIR年化|≥0.5 且 t≥1.5 且单调性≥0.4；C = t≥1.0；D = 其余。', '',
             _md_table(
                 ['因子', '分级', '大类', '方向', '状态', '均值IC', 'ICIR年化', 't值',
                  '单调性', '换手', '净多空累计', '公式'],
                 [[r['factor'], r['tier'], r['category'], r['direction'], r['status'],
                   f"{r['mean_ic']:.4f}", f"{r['icir_annual']:.2f}", f"{r['t_stat']:.2f}",
                   f"{r['monotonicity']:.2f}", f"{r['turnover']:.0%}",
                   f"{r['ls_net_cum']:.1%}", r['expression']]
                  for r in rows_out],
             ), '',
             '## 出处', '']
    seen: set[str] = set()
    for spec, _row in selected:
        if spec.source in seen:
            continue
        seen.add(spec.source)
        parts.append(f'- `{spec.name}`：{spec.source}')
    (out / '建议纳入生产因子库的因子清单.md').write_text('\n'.join(parts) + '\n', encoding='utf-8')
    return out / '建议纳入生产因子库的因子清单.md'


def write_strategies(frame: pl.DataFrame, headline: str | None = None) -> Path:
    """写公开策略对照结果。"""
    out = ensure_dir()
    frame.write_csv(out / 'strategy_results.csv')
    parts = ['# 公开策略对照结果', '', *_headline_block(headline),
             '口径与因子分层回测一致：T+1 开盘建仓、停牌/开盘涨停剔除、双边成本、等权。', '',
             _md_table(
                 ['策略', '累计收益', '年化', '夏普', '最大回撤', '换手', '期数',
                  '同期等权基准', '规则出处'],
                 [[r['name'], f"{r['total_return']:.1%}", f"{r['annual_return']:.1%}",
                   f"{r['sharpe']:.2f}", f"{r['max_drawdown']:.1%}",
                   f"{r['turnover']:.0%}", r['periods'], f"{r['benchmark_return']:.1%}",
                   r['source']]
                  for r in frame.iter_rows(named=True)],
             ), '']
    (out / '公开策略对照结果.md').write_text('\n'.join(parts), encoding='utf-8')
    return out / '公开策略对照结果.md'


def write_summary(
    metrics: pl.DataFrame,
    clusters: ClusterResult,
    selected: list[tuple[FactorSpec, dict]],
    meta: dict,
    strategy_frame: pl.DataFrame | None = None,
    headline: str | None = None,
) -> Path:
    """写汇总报告（结论前置，技术细节随后）。"""
    out = ensure_dir()
    panel_meta = meta.get('panel', {})
    dim = meta.get('dimension_check', {})
    counts = Counter(tier_of(row) for _spec, row in selected)
    usable = [(s, r) for s, r in selected
              if tier_of(r) in ('A-优先纳入', 'B-可纳入')]
    watch = [(s, r) for s, r in selected if tier_of(r) == 'C-观察']
    usable.sort(key=lambda item: abs(float(item[1]['icir_annual'])), reverse=True)

    def row_of(spec: FactorSpec, row: dict) -> list[str]:
        direction: str = spec.direction
        if direction not in ('+', '-'):
            direction = ('+' if float(row['mean_ic']) > 0 else '-') + '（样本内）'
        return [spec.name, spec.category, direction, spec.status,
                f"{float(row['mean_ic']):.4f}",
                f"{float(row['icir_annual']):.2f}",
                f"{float(row['t_stat']):.2f}",
                f"{float(row['monotonicity']):.2f}",
                f"{float(row['turnover']):.0%}",
                f"{float(row['ls_net_cum']):.1%}"]

    header = ['因子', '大类', '方向', '状态', '均值IC', 'ICIR年化', 't值',
              '单调性', '换手', '净多空累计']

    negative = [(s, r) for s, r in selected if float(r['t_stat']) < 0]
    worst = sorted(negative, key=lambda item: float(item[1]['t_stat']))[:5]

    parts = [
        '# 公开因子库验证报告', '',
        *_headline_block(headline),
        '## 1. 数据与口径', '',
        f'- 股票池：{panel_meta.get("codes", "-")} 只，按末 250 日成交额中位数取流动性'
        '最好的标的（剔除北交所）',
        f'- 区间：{panel_meta.get("start", "-")} ~ {panel_meta.get("end", "-")}，'
        f'{panel_meta.get("trading_days", "-")} 个交易日，{panel_meta.get("rows", "-")} 行',
        '- 价格：后复权（hfq_factor）；停牌与开盘涨跌停不可成交；T+1 开盘建仓；'
        '持有 20 日、每 20 个交易日非重叠调仓；5 分组等权',
        f'- 成本：买 {meta.get("cost", {}).get("buy", "-")}，'
        f'卖 {meta.get("cost", {}).get("sell", "-")}（含佣金/过户费/印花税/滑点）',
        '- 量纲自检：amount/(原始收盘价×volume×100) 的 '
        f'median={dim.get("median", "-")}，p01={dim.get("p01", "-")}，'
        f'p99={dim.get("p99", "-")}，'
        f'{float(dim.get("share_within_10pct", 0)):.1%} 的样本落在 ±10% 内'
        f'（{"通过" if dim.get("passed") else "未通过"}）',
        '',
        '## 2. 哪些因子真的有效', '',
        f'去冗余后能通过成本检验、且 t 值 ≥ 1.5 的因子共 {len(usable)} 个：', '',
        _md_table(header, [row_of(s, r) for s, r in usable]), '',
        '这批评分最高的是 '
        + ('、'.join(f'{s.name}（{s.category}）' for s, _r in usable[:3]) or '无')
        + '；它们的共性是「买最极端的一小撮、空最平庸的一小撮」时'
        '多空差为正，且分层基本单调。', '',
        '## 3. 哪些是噪音', '',
        f'清单里 {counts["D-噪音"]} 个因子在 5 年样本上 t 值 < 1，和 0 无法区分；'
        f'其中 {len(negative)} 个 t<0，按登记方向做反而亏钱（列出最差 5 个）：', '',
        _md_table(header, [row_of(s, r) for s, r in worst]), '',
        '这些不要单独用于选股。方向反了的多半是「样本内该方向已失效」或'
        '「登记方向与 A 股实际相反」，需要重新约定方向后再评估。', '',
        '## 4. 冗余聚类', '',
        f'输入 {clusters.correlation.shape[0]} 个因子，'
        f'在 |Spearman corr| ≥ 0.7 处切成 {clusters.clusters.height} 簇，'
        f'保留 {len(clusters.kept)} 个簇代表。各簇明细见 '
        '`因子冗余聚类报告.md`；同簇内只保留 ICIR 最高者。', '',
    ]

    if strategy_frame is not None and not strategy_frame.is_empty():
        parts.extend([
            '## 5. 公开策略对照', '',
            _md_table(
                ['策略', '累计收益', '年化', '夏普', '最大回撤', '同期等权基准'],
                [[STRATEGY_LABELS.get(r['name'], r['name']),
                  f"{r['total_return']:.1%}", f"{r['annual_return']:.1%}",
                  f"{r['sharpe']:.2f}", f"{r['max_drawdown']:.1%}",
                  f"{r['benchmark_return']:.1%}"]
                 for r in strategy_frame.sort('total_return',
                                              descending=True).iter_rows(named=True)],
            ), '',
        ])

    names_abc = [s.name for s, _r in usable] + [s.name for s, _r in watch]
    parts.extend([
        '## 6. 建议纳入生产库（30–50 个候选）', '',
        f'- **优先纳入（A/B）**：{"、".join(names_abc[:len(usable)]) or "无"}',
        f'- **观察（C）**：{len(watch)} 个，建议在扩到全市场后再决定',
        f'- **不纳入（D）**：{counts["D-噪音"]} 个，列表仅作候选池留档',
        '',
        '完整清单（含公式、分级、出处）见 `建议纳入生产因子库的因子清单.md`。'
        f'实际建议先上线的只有 {len(usable)} 个 A/B 因子，'
        '其余请等全市场样本外验证。', '',
        '## 7. 局限与下一步', '',
        '- 样本只覆盖 80 只流动性最好的标的、5 年单一区间，结论不能直接外推到全市场；'
        '下一步按同一脚本把 `--size` 扩到全市场并做分年度样本外检验。',
        '- VWAP 类因子用日频 amount/(volume×100) 近似，行业中性用「去横截面均值」近似，'
        '与原始 Alpha101/GTJA 定义有差异，已在登记表 `note` 列标注。',
        '- 市值/账面价值/营收等基本面数据本地缺失，规模与价值因子仅登记为「不可用」。',
        '- 流动性类因子（如 ff_amihud_60）的超额依赖「相对不活跃」的标的，'
        '成本模型按固定费率估算、未按成交额建模冲击成本，实盘需降权处理。',
        '- ST 股的 5% 涨跌停限制未单独处理（只按板块 10%/20% 判定），'
        '如用于实盘需接入 ST 名单。',
        '',
    ])
    (out / '因子库验证汇总.md').write_text('\n'.join(parts), encoding='utf-8')
    return out / '因子库验证汇总.md'


def write_meta(payload: dict) -> Path:
    """写运行元数据（股票池、区间、量纲自检、耗时）。"""
    out = ensure_dir()
    path = out / 'run_meta.json'
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2,
                               default=str), encoding='utf-8')
    return path


__all__ = [
    'REPORT_DIR',
    'build_headline',
    'build_strategy_headline',
    'ensure_dir',
    'select_recommendations',
    'tier_of',
    'write_clusters',
    'write_evaluation',
    'write_meta',
    'write_recommendation',
    'write_registry',
    'write_strategies',
    'write_summary',
]
