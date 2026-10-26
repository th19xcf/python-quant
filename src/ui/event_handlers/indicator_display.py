"""指标提示文本渲染（纯展示层，不依赖任何 GUI 库）。

窗口 2（成交量区）与窗口 3（KDJ 区）的底部提示文本规则集中在本模块：
输入是 main_window 上预计算好的数据数组、索引与数字格式化函数，
输出是 Qt 富文本片段；数据缺失或索引越界时返回空串。

本模块只依赖标准库与 typing，可在无 GUI 环境下单独测试。
"""

from collections.abc import Callable, Mapping
from typing import Any

NumberFormatter = Callable[[Any], str]
Renderer = Callable[[Mapping, int, NumberFormatter], str]


def _has(data: Mapping, key: str, index: int) -> bool:
    """判断数据字典中存在该键且索引未越界。"""
    return data is not None and key in data and 0 <= index < len(data[key])


def _at(data: Mapping, key: str, index: int, default: Any = None) -> Any:
    """安全取次级序列的值，键缺失或索引越界时返回默认值。"""
    values = data.get(key)
    if values is None or index >= len(values):
        return default
    return values[index]


# ---------------------------------------------------------------- 窗口 2 专用
def _w2_volume(data, index, fmt):
    if not _has(data, 'volume', index):
        return ""
    ma5 = _at(data, 'vol_ma5', index, 0)
    ma10 = _at(data, 'vol_ma10', index, 0)
    return (
        f"<font color='#C0C0C0'>VOLUME: {int(data['volume'][index]):,}</font>  "
        f"<font color='white'>MA5: {int(ma5):,}</font>  "
        f"<font color='cyan'>MA10: {int(ma10):,}</font>"
    )


def _w2_macd(data, index, fmt):
    if not _has(data, 'macd', index):
        return ""
    hist = data['macd_hist'][index]
    hist_color = '#FF0000' if hist >= 0 else '#00FF00'
    return (
        f"<font color='white'>MACD(12,26,9) </font>"
        f"<font color='#FFFFFF'>DIF: {data['macd'][index]:.2f}</font>  "
        f"<font color='#FFFF00'>DEA: {data['macd_signal'][index]:.2f}</font>  "
        f"<font color='{hist_color}'>MACD: {hist:.2f}</font>"
    )


def _w2_rsi(data, index, fmt):
    if not _has(data, 'rsi', index):
        return ""
    return f"<font color='blue'>RSI14: {data['rsi'][index]:.2f}</font>"


def _w2_kdj(data, index, fmt):
    if not _has(data, 'k', index):
        return ""
    return (
        f"<font color='white'>K: {data['k'][index]:.2f}</font>  "
        f"<font color='yellow'>D: {data['d'][index]:.2f}</font>  "
        f"<font color='magenta'>J: {data['j'][index]:.2f}</font>"
    )


def _w2_wr(data, index, fmt):
    if not _has(data, 'wr1', index):
        return ""
    return (
        f"<font color='white'>WR(10,6) </font>"
        f"<font color='yellow'>WR1: {data['wr1'][index]:.2f}</font> "
        f"<font color='white'>WR2: {data['wr2'][index]:.2f}</font>"
    )


def _w2_boll(data, index, fmt):
    if not _has(data, 'mb', index):
        return ""
    return (
        f"<font color='white'>MB: {data['mb'][index]:.2f}</font>  "
        f"<font color='red'>UP: {data['up'][index]:.2f}</font>  "
        f"<font color='#00FF00'>DN: {data['dn'][index]:.2f}</font>"
    )


def _w2_trix(data, index, fmt):
    if not _has(data, 'trix', index):
        return ""
    trma = _at(data, 'trma', index)
    return (
        f"<font color='#FFFFFF'>TRIX: {fmt(data['trix'][index])}</font>  "
        f"<font color='#FFFF00'>MATRIX: {fmt(trma)}</font>"
    )


def _w2_dmi(data, index, fmt):
    if not _has(data, 'pdi', index):
        return ""
    return (
        f"<font color='#FFFFFF'>PDI: {fmt(data['pdi'][index])}</font>  "
        f"<font color='#FFFF00'>NDI: {fmt(_at(data, 'ndi', index))}</font>  "
        f"<font color='#FF00FF'>ADX: {fmt(_at(data, 'adx', index))}</font>  "
        f"<font color='#00FF00'>ADXR: {fmt(_at(data, 'adxr', index))}</font>"
    )


def _w2_brar(data, index, fmt):
    if not _has(data, 'br', index):
        return ""
    return (
        f"<font color='#FFFF00'>BR: {fmt(data['br'][index])}</font>  "
        f"<font color='#FFFFFF'>AR: {fmt(_at(data, 'ar', index))}</font>"
    )


# ---------------------------------------------------------------- 窗口 3 专用
def _w3_macd(data, index, fmt):
    if not _has(data, 'macd', index):
        return ""
    hist = data['macd_hist'][index]
    hist_color = '#FF0000' if hist >= 0 else '#00FF00'
    return (
        f"<font color='white'>MACD(12,26,9) </font>"
        f"<font color='#FFFFFF'>DIF: {fmt(data['macd'][index])}</font>  "
        f"<font color='#FFFF00'>DEA: {fmt(data['macd_signal'][index])}</font>  "
        f"<font color='{hist_color}'>MACD: {fmt(hist)}</font>"
    )


def _w3_rsi(data, index, fmt):
    if not _has(data, 'rsi', index):
        return ""
    return f"<font color='blue'>RSI14: {fmt(data['rsi'][index])}</font>"


def _w3_kdj(data, index, fmt):
    if not _has(data, 'k', index):
        return ""
    return (
        f"<font color='white'>K: {fmt(data['k'][index])}</font>  "
        f"<font color='yellow'>D: {fmt(data['d'][index])}</font>  "
        f"<font color='magenta'>J: {fmt(data['j'][index])}</font>"
    )


def _w3_wr(data, index, fmt):
    if not _has(data, 'wr1', index):
        return ""
    return (
        f"<font color='white'>WR(10,6) </font>"
        f"<font color='yellow'>WR1: {fmt(data['wr1'][index])}</font>  "
        f"<font color='white'>WR2: {fmt(data['wr2'][index])}</font>"
    )


def _w3_boll(data, index, fmt):
    if not _has(data, 'mb', index):
        return ""
    return (
        f"<font color='white'>MB: {fmt(data['mb'][index])}</font>  "
        f"<font color='red'>UP: {fmt(data['up'][index])}</font>  "
        f"<font color='#00FF00'>DN: {fmt(data['dn'][index])}</font>"
    )


def _w3_trix(data, index, fmt):
    if not _has(data, 'trix', index):
        return ""
    return (
        f"<font color='#FFFFFF'>TRIX: {fmt(data['trix'][index])}</font>  "
        f"<font color='#FFFF00'>MATRIX: {fmt(data['trma'][index])}</font>"
    )


def _w3_dmi(data, index, fmt):
    if not _has(data, 'pdi', index):
        return ""
    return (
        f"<font color='#FFFFFF'>PDI: {fmt(data['pdi'][index])}</font>  "
        f"<font color='#FFFF00'>NDI: {fmt(data['ndi'][index])}</font>  "
        f"<font color='#FF00FF'>ADX: {fmt(data['adx'][index])}</font>  "
        f"<font color='#00FF00'>ADXR: {fmt(data['adxr'][index])}</font>"
    )


def _w3_brar(data, index, fmt):
    if not _has(data, 'br', index):
        return ""
    return (
        f"<font color='#FFFF00'>BR: {fmt(data['br'][index])}</font>  "
        f"<font color='#FFFFFF'>AR: {fmt(data['ar'][index])}</font>"
    )


def _w3_expma(data, index, fmt):
    if not _has(data, 'expma12', index):
        return ""
    return (
        f"<font color='white'>EXPMA(12,50) </font>"
        f"<font color='#FFFF00'>EXPMA12: {fmt(data['expma12'][index])}</font> "
        f"<font color='#FF00FF'>EXPMA50: {fmt(_at(data, 'expma50', index))}</font>"
    )


def _w3_bbi(data, index, fmt):
    if not _has(data, 'bbi', index):
        return ""
    return f"<font color='white'>BBI(3,6,12,24): {fmt(data['bbi'][index])}</font>"


def _w3_cyc(data, index, fmt):
    if not _has(data, 'cyc5', index):
        return ""
    text = (
        f"<font color='#FFFF00'>CYC5: {fmt(data['cyc5'][index])}</font> "
        f"<font color='#FFA500'>CYC13: {fmt(_at(data, 'cyc13', index))}</font>"
    )
    if 'cyc34' in data and index < len(data['cyc34']):
        text += f" <font color='#FF00FF'>CYC34: {fmt(data['cyc34'][index])}</font>"
    if 'cyc_inf' in data and index < len(data['cyc_inf']):
        text += f" <font color='#00FFFF'>CYC∞: {fmt(data['cyc_inf'][index])}</font>"
    return text


def _w3_cys(data, index, fmt):
    if not _has(data, 'cys', index):
        return ""
    cys = data['cys'][index]
    cys_color = '#FF0000' if cys > 0 else '#00FF00'
    text = f"<font color='{cys_color}'>CYS: {fmt(cys)}%</font>"
    if 'cys_ma5' in data and index < len(data['cys_ma5']):
        text += f"  <font color='white'>MA5: {fmt(data['cys_ma5'][index])}%</font>"
    return text


# ---------------------------------------------------------------- 两窗口通用
def _render_sar(data, index, fmt):
    if not _has(data, 'sar', index):
        return ""
    return f"<font color='white'>SAR: {fmt(data['sar'][index])}</font>"


def _render_dma(data, index, fmt):
    if not _has(data, 'dma', index):
        return ""
    return (
        f"<font color='white'>DMA(10,50): {fmt(data['dma'][index])}</font>  "
        f"<font color='yellow'>AMA: {fmt(_at(data, 'ama', index))}</font>"
    )


def _render_vr(data, index, fmt):
    if not _has(data, 'vr', index):
        return ""
    return (
        f"<font color='#FFFFFF'>VR: {fmt(data['vr'][index])}</font>  "
        f"<font color='#FFFF00'>MAVR: {fmt(_at(data, 'mavr', index, 0))}</font>"
    )


def _render_fsl(data, index, fmt):
    if not _has(data, 'swl', index):
        return ""
    return (
        f"<font color='white'>SWL: {fmt(data['swl'][index])}</font>  "
        f"<font color='yellow'>SWS: {fmt(_at(data, 'sws', index))}</font>"
    )


def _render_cr(data, index, fmt):
    if not _has(data, 'cr', index):
        return ""
    return f"<font color='white'>CR: {fmt(data['cr'][index])}</font>"


def _render_obv(data, index, fmt):
    if not _has(data, 'obv', index):
        return ""
    return f"<font color='white'>OBV: {fmt(data['obv'][index])}</font>"


def _render_asi(data, index, fmt):
    if not _has(data, 'asi', index):
        return ""
    text = f"<font color='white'>ASI: {fmt(data['asi'][index])}</font>"
    if 'asi_sig' in data and index < len(data['asi_sig']):
        text += f"  <font color='yellow'>SIG: {fmt(data['asi_sig'][index])}</font>"
    return text


def _render_hsl(data, index, fmt):
    if not _has(data, 'hsl', index):
        return ""
    hsl = data['hsl'][index]
    hsl_color = '#FF0000' if hsl > 10 else '#FFA500' if hsl > 5 else '#00BFFF'
    text = f"<font color='{hsl_color}'>HSL: {fmt(hsl)}%</font>"
    if 'hsl_ma5' in data and index < len(data['hsl_ma5']):
        text += f"  <font color='white'>MA5: {fmt(data['hsl_ma5'][index])}%</font>"
    if 'hsl_ma10' in data and index < len(data['hsl_ma10']):
        text += f"  <font color='cyan'>MA10: {fmt(data['hsl_ma10'][index])}%</font>"
    return text


def _render_lb(data, index, fmt):
    if not _has(data, 'lb', index):
        return ""
    lb = data['lb'][index]
    lb_color = '#FF0000' if lb > 2 else '#FFA500' if lb > 1.5 else '#00FF7F' if lb > 1 else '#00BFFF'
    return f"<font color='{lb_color}'>LB: {fmt(lb)}</font>"


def _single(key, label, color):
    """生成单值指标的渲染函数，避免重复样板代码。"""
    def render(data, index, fmt):
        if not _has(data, key, index):
            return ""
        return f"<font color='{color}'>{label}: {fmt(data[key][index])}</font>"
    return render


render_emv = _single('emv', 'EMV', 'white')
render_cci = _single('cci', 'CCI', 'white')
render_roc = _single('roc', 'ROC', 'white')
render_mtm = _single('mtm', 'MTM', 'white')
render_psy = _single('psy', 'PSY', 'white')
render_mcst = _single('mcst', 'MCST', 'white')


WINDOW2_TEXTS: dict[str, Renderer] = {
    "VOL": _w2_volume,
    "MACD": _w2_macd,
    "RSI": _w2_rsi,
    "KDJ": _w2_kdj,
    "WR": _w2_wr,
    "BOLL": _w2_boll,
    "SAR": _render_sar,
    "TRIX": _w2_trix,
    "DMI": _w2_dmi,
    "DMA": _render_dma,
    "BRAR": _w2_brar,
    "FSL": _render_fsl,
    "VR": _render_vr,
    "CR": _render_cr,
    "OBV": _render_obv,
    "HSL": _render_hsl,
    "LB": _render_lb,
    "ASI": _render_asi,
    "EMV": render_emv,
    "CCI": render_cci,
    "ROC": render_roc,
    "MTM": render_mtm,
    "PSY": render_psy,
    "MCST": render_mcst,
}

WINDOW3_TEXTS: dict[str, Renderer] = {
    "KDJ": _w3_kdj,
    "RSI": _w3_rsi,
    "MACD": _w3_macd,
    "WR": _w3_wr,
    "BOLL": _w3_boll,
    "DMI": _w3_dmi,
    "TRIX": _w3_trix,
    "BRAR": _w3_brar,
    "VR": _render_vr,
    "DMA": _render_dma,
    "SAR": _render_sar,
    "FSL": _render_fsl,
    "CR": _render_cr,
    "OBV": _render_obv,
    "EXPMA": _w3_expma,
    "BBI": _w3_bbi,
    "CYC": _w3_cyc,
    "CYS": _w3_cys,
    "ASI": _render_asi,
    "EMV": render_emv,
    "CCI": render_cci,
    "ROC": render_roc,
    "MTM": render_mtm,
    "PSY": render_psy,
    "MCST": render_mcst,
    "HSL": _render_hsl,
    "LB": _render_lb,
}


def render_indicator_text(texts: Mapping[str, Renderer], indicator: str,
                          data: Mapping, index: int,
                          fmt: NumberFormatter) -> str:
    """按指标名查表渲染提示文本；未注册的指标名返回空串。"""
    renderer = texts.get(indicator)
    if renderer is None:
        return ""
    return renderer(data, index, fmt)
