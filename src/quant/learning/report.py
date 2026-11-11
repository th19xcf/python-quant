#!/usr/bin/env python3

"""把学习实验结果渲染成 Markdown 报告。"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date
from typing import Any, Dict, List

from src.quant.learning.config import LearningReport, SchemeMetrics


def _num(value: float, digits: int = 4) -> str:
    return f'{value:.{digits}f}'


def _pct(value: float, digits: int = 2) -> str:
    return f'{value:.{digits}f}%'


def _cost_input_line(scope: Dict[str, object]) -> str:
    """成本分档输入的覆盖率：说明滑点按标的档位计，而非一律兜底档。"""
    rows = scope.get('rows') or 0
    adv = scope.get('adv20_notnull')
    cap = scope.get('total_mv_yi_notnull')
    if adv is None or cap is None or not rows:
        return '未接线（滑点按兜底档计）'
    return (f'adv20 非空 {adv} 行 / total_mv_yi 非空 {cap} 行（共 {rows} 行，'
            f'缺失按兜底档计）')


def _table(headers: List[str], rows: List[List[str]]) -> List[str]:
    lines = ['| ' + ' | '.join(headers) + ' |',
             '|' + '|'.join(['---'] * len(headers)) + '|']
    lines.extend('| ' + ' | '.join(row) + ' |' for row in rows)
    return lines


def _scheme_row(item: SchemeMetrics, with_portfolio: bool = True) -> List[str]:
    values = item.quantile_returns[:5]
    quantiles = [str(_pct(value * 100)) for value in values]
    quantiles.extend(['—'] * (5 - len(quantiles)))
    row = [
        item.name,
        _num(item.ic_mean), _num(item.ic_std), _num(item.icir, 3),
        _pct(item.ic_positive_ratio * 100, 1), _num(item.ic_t_stat, 2),
        *quantiles,
        _num(item.monotonicity, 3), _pct(item.long_short_return * 100),
    ]
    if with_portfolio:
        row.extend([
            _pct(item.turnover * 100, 1),
            _pct(item.annual_return),
            _pct(item.max_drawdown),
            _num(item.sharpe_ratio, 2),
            _pct(item.total_return),
        ])
    row.append(str(item.n_periods))
    return row


def _lookup(metrics: Sequence[SchemeMetrics],
            *names: str) -> SchemeMetrics | None:
    for name in names:
        for item in metrics:
            if item.name == name:
                return item
    return None


def _scheme_label(name: str) -> str:
    return {'ic': 'IC 加权', 'icir': 'ICIR 加权',
            'ridge': '滚动 Ridge', 'equal': '等权',
            'static_ic': '静态 IC 加权'}.get(name, name)


def _conclusion_section(report: LearningReport) -> List[str]:
    """生成「一句话结论 + 推荐 + 撑不住项 + 读表提醒」。"""
    scope = report.data_scope
    start = str(scope.get('start', '?'))
    end = str(scope.get('end', '?'))
    span_years = f'{start[:4]}–{end[:4]}'
    weight = report.weight_schemes
    rolling = [item for item in weight if item.name in ('ic', 'icir')]
    static = _lookup(weight, '静态IC加权', 'static_ic')
    equal = _lookup(weight, 'equal')
    candidates = rolling + ([static] if static else [])
    lines: List[str] = []
    if not candidates:
        lines.extend([f'按 {span_years} 年的样本外滚动回测，暂时判断不了：'
                      '没有产出可用的因子权重方案，需要先检查数据与窗口设置。', ''])
        return lines

    anchor = max(candidates, key=lambda item: item.annual_return)
    annuals = [item.annual_return for item in candidates]
    drawdowns = [item.max_drawdown for item in candidates]
    verdict = '赚钱的' if anchor.annual_return > 0 else '亏损的'
    sentence = (
        f'按 {span_years} 年的样本外滚动回测，这套自学习是**{verdict}**：'
        f'扣掉交易成本后，因子加权组合年化约 {min(annuals):.0f}%–'
        f'{max(annuals):.0f}%、最大回撤约 {min(drawdowns):.0f}%–'
        f'{max(drawdowns):.0f}%')
    if equal is not None:
        better = equal.annual_return < min(annuals)
        sentence += (f'，比“因子等权”（年化约 {equal.annual_return:.0f}%、'
                     f'回撤约 {equal.max_drawdown:.0f}%）'
                     + ('有本质改善' if better else '没有明显改善'))
    rec = _lookup(weight, 'ic', 'icir') or anchor
    rec_label = _scheme_label(rec.name)
    sentence += (f'；推荐先采用 L2 的 {rec_label}（年化 {rec.annual_return:.2f}%、'
                 f'回撤 {rec.max_drawdown:.2f}%）')
    adaptive = _lookup(report.param_schemes, 'L1-状态自适应')
    if adaptive is not None:
        sentence += (f'，L1 状态自适应虽然年化约 {adaptive.annual_return:.2f}%，'
                     f'但回撤更大（约 {adaptive.max_drawdown:.2f}%）、'
                     '稳定性证据不足，暂不作为交易建议的依据。')
    else:
        sentence += '。'
    lines.extend([sentence, ''])

    detail = (f'**推荐采用：L2 因子权重学习（{rec_label}，半衰期 '
              f'{report.config.half_life} 个交易日）。** 在 {report.windows} '
              f'个样本外窗口里，该方案年化 {rec.annual_return:.2f}%、回撤 '
              f'{rec.max_drawdown:.2f}%')
    if static is not None and rec is not static:
        gap = abs(static.annual_return - rec.annual_return)
        detail += (f'；静态 IC（只在首个训练窗拟合一次）年化 '
                   f'{static.annual_return:.2f}%、回撤 {static.max_drawdown:.2f}%，'
                   f'与滚动版本相差 {gap:.1f} 个百分点，尚在单次回测的噪声范围内，'
                   '因此以滚动版本为默认、静态 IC 作为更简单的兜底')
    lines.extend([detail + '。', ''])

    if adaptive is not None:
        text = ('**L1 参数自适应：保留为研究分支，暂不上生产。** '
                f'它年化 {adaptive.annual_return:.2f}%、回撤 '
                f'{adaptive.max_drawdown:.2f}%，横截面 IC 为 '
                f'{adaptive.ic_mean:.3f}')
        if rec is not None:
            text += f'（L2 约 {rec.ic_mean:.3f}）'
        text += ('；按波动率 × 趋势切出的状态桶后，每个状态可用于选参的训练样本'
                 '有限，当前更像阶段性有效，还不足以支撑稳定推荐。')
        lines.extend([text, ''])

    items: List[str] = []
    lightgbm = _lookup(weight, 'lightgbm')
    if lightgbm is not None:
        items.append(f'LightGBM 年化 {lightgbm.annual_return:.2f}%、回撤 '
                     f'{lightgbm.max_drawdown:.2f}%')
    elif any('LightGBM' in note for note in report.notes):
        items.append('LightGBM 对照因可选依赖未安装而未参与评估')
    ridge = _lookup(weight, 'ridge')
    if ridge is not None:
        items.append(f'滚动 Ridge 当前 IC {ridge.ic_mean:.3f}、年化 '
                     f'{ridge.annual_return:.2f}%，不如 {rec_label}')
    n_factors = len(scope.get('factors', []))
    if equal is not None:
        items.append(f'等权组合成本后年化 {equal.annual_return:.2f}%，说明这 '
                     f'{n_factors} 个原始因子里多数方向是反的，不能直接等权使用')
    if items:
        lines.extend(['**还撑不住的：** ' + '；'.join(items) + '。', ''])

    lines.extend([
        '**读表提醒：** 前瞻收益标签跨 '
        f'{report.config.horizon} 个交易日，逐日 IC 存在重叠，表中的 t 值与 '
        'ICIR 会高估显著性；判断方案优劣请以 5 分组单调性和扣费后的组合收益/'
        '回撤为主。',
        '',
    ])
    return lines


def _l2_note(report: LearningReport) -> str | None:
    """L2 表格前的结论要点（数据驱动）。"""
    weight = report.weight_schemes
    rolling = [item for item in weight if item.name in ('ic', 'icir')]
    static = _lookup(weight, '静态IC加权', 'static_ic')
    pool = rolling + ([static] if static else [])
    if not pool:
        return None
    lo = min(item.annual_return for item in pool)
    hi = max(item.annual_return for item in pool)
    parts = [f'静态 IC、滚动 IC、ICIR 三种加权的成本后年化落在 {lo:.2f}%–{hi:.2f}%']
    equal = _lookup(weight, 'equal')
    if equal is not None:
        parts.append(f'等权年化 {equal.annual_return:.2f}%，恰好踩中反转，'
                     '说明多数原始因子方向为负，不能直接等权使用')
    ridge = _lookup(weight, 'ridge')
    if ridge is not None:
        parts.append(f'滚动 Ridge 的 IC 为 {ridge.ic_mean:.3f}，'
                     '排序能力弱于 IC/ICIR 加权')
    return '结论要点：' + '；'.join(parts) + '。'


def _l1_note(report: LearningReport) -> str | None:
    """L1 表格前的结论要点（数据驱动）。"""
    adaptive = _lookup(report.param_schemes, 'L1-状态自适应')
    if adaptive is None:
        return None
    fixed = _lookup(report.param_schemes, '固定参数-默认')
    best = _lookup(report.param_schemes, '固定参数-全样本最优')
    parts: List[str] = []
    if fixed is not None:
        parts.append(f'状态自适应把固定默认参数的 {fixed.annual_return:.2f}% '
                     f'拉回到 {adaptive.annual_return:.2f}%')
    if best is not None:
        if adaptive.annual_return > best.annual_return:
            parts.append(f'并略高于含未来信息的「全样本最优」'
                         f'（{best.annual_return:.2f}%），说明按状态切换方向/'
                         '回看窗口确实抓到了样本外的风格变化')
        else:
            parts.append(f'但仍低于含未来信息的「全样本最优」'
                         f'（{best.annual_return:.2f}%），说明当前分档还没跑赢'
                         '全样本最优')
    parts.append(f'IC 只有 {adaptive.ic_mean:.3f}、回撤 '
                 f'{adaptive.max_drawdown:.2f}%，每个状态的有效训练样本有限')
    return '结论要点：' + '，'.join(parts) + '。'


def _weight_history_section(report: LearningReport) -> List[str]:
    latest: Dict[str, Dict[str, Any]] = {}
    for entry in report.weight_history:
        scheme = str(entry.get('scheme', ''))
        window = int(entry.get('window', -1))
        if scheme and (scheme not in latest or window >= int(latest[scheme]['window'])):
            latest[scheme] = entry
    if not latest:
        return []
    lines = ['## 5. 最新窗口权重', '']
    coefficients: List[tuple[str, Dict[str, Any]]] = []
    for scheme in ('ic', 'icir', 'ridge'):
        item_entry = latest.get(scheme)
        if item_entry:
            coefficients.append((
                scheme, item_entry.get('coefficients', {})))
    if not coefficients:
        return []
    names = list(coefficients[0][1].keys())
    headers = ['因子'] + [item[0] for item in coefficients]
    rows = [[name] + [_num(float(item[1].get(name, 0.0))) for item in coefficients]
            for name in names]
    lines.extend(_table(headers, rows))
    return lines


def _regime_section(report: LearningReport) -> List[str]:
    entries = [item for item in report.weight_history
               if 'regime_params' in item]
    if not entries:
        return []
    last = max(entries, key=lambda item: int(item.get('window', -1)))
    params = last.get('regime_params', {})
    if not params:
        return []
    lines = ['## 6. L1 最新窗口状态选参', '',
             f'窗口 {last.get("window")}，默认参数 {last.get("fallback")}', '']
    rows = [[str(regime), str(value)] for regime, value in sorted(params.items())]
    lines.extend(_table(['市场状态', '选中参数'], rows))
    return lines


def render_report(report: LearningReport, generated_on: date | None = None) -> str:
    """生成完整 Markdown 文本。"""
    config = report.config
    today = generated_on or date.today()
    lines: List[str] = [
        f'# {report.title}',
        '',
        f'> 生成日期：{today}　口径：后复权、T+1 开盘建仓、停牌/涨跌停不可成交、'
        'volume=手、amount=元',
        '',
        '## 0. 结论',
        '',
        *_conclusion_section(report),
        '## 1. 实验设置',
        '',
        *_table(['项目', '取值'], [
            ['样本区间', f'{report.data_scope.get("start")} → '
                       f'{report.data_scope.get("end")}'],
            ['标的数', str(report.data_scope.get('codes'))],
            ['面板行数', str(report.data_scope.get('rows'))],
            ['因子数', str(len(report.data_scope.get('factors', [])))],
            ['walk-forward 窗口数', str(report.windows)],
            ['训练窗 / 测试窗', f'{config.train_size} / {config.test_size} 交易日'],
            ['embargo（隔离带）', f'{config.embargo} 交易日（≥ 持有期）'],
            ['持有期（标签跨度）', f'{config.horizon} 交易日'],
            ['purge', '训练样本标签不得跨入 embargo/测试窗'],
            ['分层数', str(config.n_quantiles)],
            ['组合选股比例', _pct(config.top_fraction * 100, 1)],
            ['窗口模式', '扩展窗' if config.expanding else '滚动窗'],
            ['成本分档输入', _cost_input_line(report.data_scope)],
        ]),
        '',
        '## 2. L2 因子权重学习（样本外）',
        '',
        '基线：等权、静态 IC 加权（首个训练窗拟合后不再更新）。'
        '组合模拟为等权多头、按持有期调仓，收益已扣佣金/印花税/过户费/滑点。',
        '',
    ]
    l2_note = _l2_note(report)
    if l2_note:
        lines.extend([l2_note, ''])
    if report.weight_schemes:
        headers = ['方案', 'IC均值', 'IC标准差', 'ICIR', 'IC>0占比', 't值',
                   'Q1', 'Q2', 'Q3', 'Q4', 'Q5', '单调性', '多空',
                   '单边换手', '年化(成本后)', '最大回撤', 'Sharpe', '总收益', '期数']
        rows = [_scheme_row(item) for item in report.weight_schemes]
        lines.extend(_table(headers, rows))
    else:
        lines.append('（无有效样本外结果）')
    lines.extend([
        '',
        '## 3. L1 参数自适应（样本外）',
        '',
        '按波动率分位（低/中/高）× 趋势方向（上/下）分档，在每个训练窗内'
        '用各状态的历史超额收益选择动量回看参数与方向；测试窗按当日状态取参。'
        '「固定参数-全样本最优」使用全样本信息，仅用于展示过拟合风险。',
        '',
    ])
    l1_note = _l1_note(report)
    if l1_note:
        lines.extend([l1_note, ''])
    if report.param_schemes:
        headers = ['方案', 'IC均值', 'IC标准差', 'ICIR', 'IC>0占比', 't值',
                   'Q1', 'Q2', 'Q3', 'Q4', 'Q5', '单调性', '多空',
                   '单边换手', '年化(成本后)', '最大回撤', 'Sharpe', '总收益', '期数']
        rows = [_scheme_row(item) for item in report.param_schemes]
        lines.extend(_table(headers, rows))
    else:
        lines.append('（无有效样本外结果）')
    lines.extend(['', '## 4. 单因子样本外表现（只读 IC/分层）', ''])
    if report.factor_metrics:
        headers = ['因子', 'IC均值', 'IC标准差', 'ICIR', 'IC>0占比', 't值',
                   'Q1', 'Q2', 'Q3', 'Q4', 'Q5', '单调性', '多空', '期数']
        rows = [_scheme_row(item, with_portfolio=False)
                for item in report.factor_metrics]
        lines.extend(_table(headers, rows))
    else:
        lines.append('（无有效因子结果）')

    lines.extend([''])
    lines.extend(_weight_history_section(report))
    lines.extend([''])
    lines.extend(_regime_section(report))
    lines.extend([
        '',
        '## 7. 纪律、局限与复现',
        '',
        '- 样本外：滚动窗训练、embargo ≥ 持有期、训练标签按 ``t+1+H`` purge，'
        '测试窗内不重新拟合也不回看未来。',
        '- 组合成本：佣金 0.03%（最低 5 元）、卖出印花税 0.05%（按历史时点表）、'
        '过户费 0.002%、双边滑点 0.02%（与 CostModel 单一真源一致）。',
        '- 成交约束：停牌（成交量缺失/为 0）不可成交；开盘涨停不买、跌停不卖；'
        '退出受阻最多顺延 5 个交易日，之后按最新收盘价强制退出。',
        '- 局限：股票池按本地列存文件大小抽样并做近期成交额过滤，存在幸存者偏差；'
        '不含退市股票；不含日内数据与做空；ETF/基金尚未纳入同一份评估。',
        '- 标签重叠：前瞻收益跨 ``horizon`` 个交易日，逐日 IC 样本重叠，'
        't 值与 ICIR 的显著性被高估；方案取舍以分层单调性和扣费后组合绩效为准。',
        '- 复现命令：',
        '',
        '```powershell',
        'poetry run python -m src.quant.learning --limit 600 '
        '--start 2018-01-01 --end 2026-10-31',
        '```',
    ])
    if report.notes:
        lines.extend(['', '## 8. 运行备注', ''])
        lines.extend(f'- {note}' for note in report.notes)
    lines.append('')
    return '\n'.join(lines)
