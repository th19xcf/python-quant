"""策略试算可用字段清单：只收录「已落库 + 有 PIT 时点」的字段。

PIT 口径（``pit`` 字段）只有三种，界面与文档按此展示：

- ``bar``：信号日收盘即可得的当日行情 bar 或其滚动派生（收盘价、成交额、
  过去 N 日波动率/动量等），窗口末端必须恰好是信号日，不使用任何未来 bar；
- ``asof_trade_date``：按交易日快照表（``valuation_daily``）取「不晚于信号日
  的最近一条」，不使用信号日之后的估值快照；
- ``asof_ann_date``：按公告日（``ann_date``）取「不晚于信号日的最近一期财报」，
  禁止用报告期（``end_date``）对齐——报告期早于公告日，按报告期取数会
  提前看到尚未披露的财务数据。

未列入本表的字段一律视为「不可用」：规则校验与界面下拉都只读本表，
不做任何动态探测，也不接受用户自带字段名。
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Dict, List, Tuple

CATEGORY_ORDER: Tuple[str, ...] = ('行情', '估值', '综合', '财务', '流动性', '分类')

#: 只用于分组的非数值字段：不能出现在条件与排序里
NON_NUMERIC_FIELDS: frozenset = frozenset({'industry'})

#: 运算符 → 界面中文名；``between`` 需要上下限两个数
OPERATORS: Dict[str, str] = {
    '>': '大于',
    '>=': '大于等于',
    '<': '小于',
    '<=': '小于等于',
    '==': '等于',
    '!=': '不等于',
    'between': '介于（含端点）',
}

#: 允许与「横截面分位」组合的运算符；分位语义见 rules.py
QUANTILE_OPERATORS: Tuple[str, ...] = ('>', '>=', '<', '<=')


@dataclass(frozen=True)
class FieldSpec:
    """一个可用字段的元数据。"""

    key: str
    label: str
    category: str
    source: str
    pit: str
    unit: str
    doc: str

    def to_dict(self) -> Dict[str, str]:
        return asdict(self)


FIELDS: Dict[str, FieldSpec] = {
    # ---- 行情（当日 bar 及其滚动派生）----
    'close': FieldSpec('close', '收盘价（后复权）', '行情', 'quotes/stock/**', 'bar', '元',
                       '信号日当日后复权收盘价；停牌日无 bar，不进入当日截面'),
    'pct_chg': FieldSpec('pct_chg', '当日涨跌幅', '行情', 'quotes/stock/**', 'bar', '%',
                         '信号日当日涨跌幅，按本地行情表原值'),
    'volume': FieldSpec('volume', '成交量', '行情', 'quotes/stock/**', 'bar', '手',
                        '信号日当日成交量'),
    'amount': FieldSpec('amount', '成交额', '行情', 'quotes/stock/**', 'bar', '元',
                        '信号日当日成交额'),
    'vol_20': FieldSpec('vol_20', '20日波动率', '行情', 'quotes/stock/**', 'bar', '日收益标准差',
                        '信号日为止最近 20 个交易日对数收益率的样本标准差'),
    'vol_60': FieldSpec('vol_60', '60日波动率', '行情', 'quotes/stock/**', 'bar', '日收益标准差',
                        '信号日为止最近 60 个交易日对数收益率的样本标准差'),
    'mom_20': FieldSpec('mom_20', '20日动量', '行情', 'quotes/stock/**', 'bar', '小数',
                        '信号日收盘 / 20 个交易日前收盘 - 1'),
    'mom_60': FieldSpec('mom_60', '60日动量', '行情', 'quotes/stock/**', 'bar', '小数',
                        '信号日收盘 / 60 个交易日前收盘 - 1'),
    'supertrend': FieldSpec('supertrend', 'Supertrend 趋势线', '行情', 'quotes/stock/**', 'bar', '元',
                            '信号日收盘计算的 Supertrend 趋势线（多头取下轨、空头取上轨），参数 ATR 10 / 倍数 3，路径依赖逐行递推'),
    'st_lower': FieldSpec('st_lower', 'Supertrend 下轨', '行情', 'quotes/stock/**', 'bar', '元',
                          'Supertrend 下轨支撑（hl2 − 3×ATR，轨道单向收紧）'),
    'st_upper': FieldSpec('st_upper', 'Supertrend 上轨', '行情', 'quotes/stock/**', 'bar', '元',
                          'Supertrend 上轨压力（hl2 + 3×ATR，轨道单向收紧）'),
    'st_dir': FieldSpec('st_dir', 'Supertrend 方向', '行情', 'quotes/stock/**', 'bar', '1=多/-1=空/0=未初始化',
                        '1 多头（收盘在上轨之上）、-1 空头（收盘在下轨之下）、0 未初始化（前 ATR 种子期）'),
    # ---- 估值（按交易日快照 as-of）----
    'pe': FieldSpec('pe', '市盈率 PE', '估值', 'meta/financials/valuation_daily.parquet',
                    'asof_trade_date', '倍', '不晚于信号日的最近一条快照；负值保留原样'),
    'pb': FieldSpec('pb', '市净率 PB', '估值', 'meta/financials/valuation_daily.parquet',
                    'asof_trade_date', '倍', '不晚于信号日的最近一条快照'),
    'ps': FieldSpec('ps', '市销率 PS', '估值', 'meta/financials/valuation_daily.parquet',
                    'asof_trade_date', '倍', '不晚于信号日的最近一条快照'),
    'dv_ratio': FieldSpec('dv_ratio', '股息率', '估值', 'meta/financials/valuation_daily.parquet',
                          'asof_trade_date', '%', '不晚于信号日的最近一条快照'),
    'total_mv': FieldSpec('total_mv', '总市值', '估值', 'meta/financials/valuation_daily.parquet',
                          'asof_trade_date', '万元', '不晚于信号日的最近一条快照'),
    'circ_mv': FieldSpec('circ_mv', '流通市值', '估值', 'meta/financials/valuation_daily.parquet',
                         'asof_trade_date', '万元', '不晚于信号日的最近一条快照'),
    'ep': FieldSpec('ep', 'EP（1/PE）', '估值', 'meta/financials/valuation_daily.parquet',
                    'asof_trade_date', '1/倍', '仅 PE>0 的标的入截面'),
    'bp': FieldSpec('bp', 'BP（1/PB）', '估值', 'meta/financials/valuation_daily.parquet',
                    'asof_trade_date', '1/倍', '仅 PB>0 的标的入截面'),
    # ---- 综合（因子面板合成，申万一级行业内分位）----
    'ff_bp': FieldSpec('ff_bp', 'BP（因子面板）', '估值',
                       'data/local_store/factors/factor_panel.parquet',
                       'asof_trade_date', '1/倍',
                       '正式日频因子面板 bp（1/PB，pb>0 才有值）'),
    'ff_ep': FieldSpec('ff_ep', 'EP（因子面板）', '估值',
                       'data/local_store/factors/factor_panel.parquet',
                       'asof_trade_date', '1/倍',
                       '正式日频因子面板 ep（1/PE，pe>0 才有值）'),
    'ff_dy': FieldSpec('ff_dy', '股息率（因子面板）', '估值',
                       'data/local_store/factors/factor_panel.parquet',
                       'asof_trade_date', '%',
                       '正式日频因子面板 dy（股息率 %）'),
    'vq_value': FieldSpec('vq_value', '价值综合分（行业内分位）', '综合',
                          '因子面板 + 申万一级行业 PIT', 'asof_trade_date', '分位',
                          'BP/EP/股息率三者申万一级行业内分位的均值，0~1，越高越低估'),
    'vq_quality': FieldSpec('vq_quality', '质量综合分（行业内分位）', '综合',
                            '因子面板 + 申万一级行业 PIT', 'asof_trade_date', '分位',
                            'ROE(TTM)、−应计利润、毛利率三者行业内分位的均值，0~1'),
    'vq_score': FieldSpec('vq_score', '价值质量合成分', '综合',
                          '因子面板 + 申万一级行业 PIT', 'asof_trade_date', '分位',
                          '0.5×价值综合分 + 0.5×质量综合分；任一缺失即无值'),
    # ---- 财务（按公告日 as-of）----
    'revenue_yoy': FieldSpec('revenue_yoy', '营收同比', '财务', 'meta/financials/income.parquet',
                             'asof_ann_date', '%', '按公告日取最近一期，禁止按报告期对齐'),
    'net_profit_yoy': FieldSpec('net_profit_yoy', '净利润同比', '财务',
                                'meta/financials/income.parquet', 'asof_ann_date', '%',
                                '按公告日取最近一期，禁止按报告期对齐'),
    'parent_net_profit': FieldSpec('parent_net_profit', '归母净利润', '财务',
                                   'meta/financials/income.parquet', 'asof_ann_date', '元',
                                   '按公告日取最近一期'),
    'total_assets': FieldSpec('total_assets', '总资产', '财务', 'meta/financials/balance.parquet',
                              'asof_ann_date', '元', '按公告日取最近一期'),
    'total_equity': FieldSpec('total_equity', '所有者权益', '财务',
                              'meta/financials/balance.parquet', 'asof_ann_date', '元',
                              '按公告日取最近一期'),
    'debt_ratio': FieldSpec('debt_ratio', '资产负债率', '财务',
                            'meta/financials/balance.parquet', 'asof_ann_date', '%',
                            '按公告日取最近一期'),
    'operating_cf': FieldSpec('operating_cf', '经营活动现金流', '财务',
                              'meta/financials/cashflow.parquet', 'asof_ann_date', '元',
                              '按公告日取最近一期'),
    'roe': FieldSpec('roe', 'ROE（归母净利/权益）', '财务',
                     'income.parquet + balance.parquet', 'asof_ann_date', '%',
                     '两表各自按公告日取最近一期后相除，仅权益>0 时给值'),
    'ff_roe_ttm': FieldSpec('ff_roe_ttm', 'ROE(TTM)', '财务',
                            'data/local_store/factors/factor_panel.parquet',
                            'asof_trade_date', '%',
                            '正式日频因子面板 roe_ttm（归母净利TTM/所有者权益 × 100）'),
    'ff_accruals': FieldSpec('ff_accruals', '应计利润', '财务',
                             'data/local_store/factors/factor_panel.parquet',
                             'asof_trade_date', '小数',
                             '正式日频因子面板 accruals（(净利−经营CF)/总资产，越低越好）'),
    'ff_gross_margin': FieldSpec('ff_gross_margin', '毛利率', '财务',
                                 'data/local_store/factors/factor_panel.parquet',
                                 'asof_trade_date', '%',
                                 '正式日频因子面板 gross_margin'),
    'cfp_ttm': FieldSpec('cfp_ttm', '股东盈余比（现金流/市值）', '财务',
                         'data/local_store/factors/factor_panel.parquet',
                         'asof_trade_date', '比值',
                         '正式日频因子面板 b_ocf_ttm（经营现金流TTM / 总市值）'),
    # ---- 流动性 ----
    'avg_amount_20': FieldSpec('avg_amount_20', '20日平均成交额', '流动性', 'quotes/stock/**',
                               'bar', '元', '信号日为止最近 20 个交易日成交额均值，窗口须满'),
    # ---- 分类 ----
    'industry': FieldSpec('industry', '申万一级行业', '分类',
                          'meta/industry_pit/industry_pit_sw.parquet', 'asof_trade_date', '—',
                          '按 in_date/out_date 有效区间取行业；仅用于分组，不能作为条件或排序字段'),
}


def field_catalog() -> List[Dict[str, object]]:
    """按类别分组导出字段词表，供界面下拉与文档共用。"""
    grouped: Dict[str, List[Dict[str, str]]] = {c: [] for c in CATEGORY_ORDER}
    for spec in FIELDS.values():
        grouped[spec.category].append(spec.to_dict())
    return [{'category': c, 'fields': grouped[c]} for c in CATEGORY_ORDER if grouped[c]]
