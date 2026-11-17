#!/usr/bin/env python3

"""
基金板块（ETF / LOF / 分级）

菜单按本地名录 ``meta/fund_basic.parquet`` 的 ``fund_type`` 分类，不再用
SH/SZ 代码段推断类别；名录只读本地列存，不连数据库，数据库不可用时
也能正常展示。代码段谓词 ``is_etf_fund_ts_code`` / ``is_closed_fund_ts_code``
仅作场内识别辅助，不决定菜单归属。

原先 ETF 与封闭式基金在主窗口事件 Mixin 中各有一份约 300 行的
重复实现，此处合并为参数化流程：数据源、结果键与文案按基金种类配置，
表格填充与任务编排只保留一份。
"""

import datetime

import polars as pl
from PySide6.QtCore import Qt
from PySide6.QtGui import QColor
from PySide6.QtWidgets import QApplication, QTableWidgetItem

from src.data.local_store.paths import resolve_store_root
from src.ui.table_sizing import apply_estimated_column_widths
from src.ui.task_manager import global_task_manager
from src.utils.logger import logger

#: 菜单分类：与 ``meta/fund_basic.parquet`` 的 ``fund_type`` 取值一一对应
FUND_TYPE_MENUS: tuple[str, ...] = ('ETF', 'LOF', '分级')

#: 表格列顺序（与主窗口行情表的 13 列一一对应），代码列显示带市场后缀的标准代码
_FUND_COLUMNS = (
    'date', 'code', 'name', 'change_pct', 'price', 'change',
    'volume', 'amount', 'open', 'high', 'low', 'preclose', 'amplitude',
)

# 表格数值列（右对齐）
_NUMERIC_COLUMNS = tuple(range(3, 13))
# 涨跌配色：涨红 / 跌绿 / 平灰
_COLOR_UP = QColor(255, 0, 0)
_COLOR_DOWN = QColor(0, 255, 0)
_COLOR_FLAT = QColor(204, 204, 204)


def is_etf_fund_ts_code(ts_code: str) -> bool:
    """判断是否 ETF 基金代码（沪市 51/52/56/58，深市 159）。"""
    if not ts_code or '.' not in ts_code:
        return False

    symbol, market = ts_code.split('.', 1)
    market = market.upper()
    if market == 'SH' and symbol.startswith(('51', '52', '56', '58')):
        return True
    return bool(market == 'SZ' and symbol.startswith('159'))


def is_closed_fund_ts_code(ts_code: str) -> bool:
    """判断是否封闭式基金代码（沪市 500xxx，排除 ETF 段）。"""
    if not ts_code or '.' not in ts_code:
        return False

    symbol, market = ts_code.split('.', 1)
    market = market.upper()
    return market == 'SH' and symbol.startswith('50') and not symbol.startswith(('51', '52', '56', '58'))


def load_local_fund_basic(config=None) -> pl.DataFrame:
    """读取本地基金名录；只读 ``meta/fund_basic.parquet``，不访问数据库。"""
    path = resolve_store_root(config) / 'meta' / 'fund_basic.parquet'
    if not path.exists():
        logger.warning(f'本地基金名录不存在: {path}')
        return pl.DataFrame()
    return pl.read_parquet(path)


def split_by_fund_type(frame: pl.DataFrame) -> tuple[dict[str, pl.DataFrame], dict[str, int]]:
    """按 ``fund_type`` 拆成三类菜单，并单独统计未归类计数。

    ``fund_type`` 为空或不在 :data:`FUND_TYPE_MENUS` 内的行不并入任何一类，
    更不会默认塞进 ETF；计数由调用方记录，便于发现名录脏值。
    """
    if frame.is_empty() or 'fund_type' not in frame.columns:
        return {}, {}
    menus = {kind: frame.filter(pl.col('fund_type') == kind) for kind in FUND_TYPE_MENUS}
    unknown = frame.filter(~pl.col('fund_type').is_in(FUND_TYPE_MENUS).fill_null(False))
    counts: dict[str, int] = {}
    if not unknown.is_empty():
        for row in unknown.group_by('fund_type').len().iter_rows(named=True):
            key = '空值' if row['fund_type'] is None else str(row['fund_type'])
            counts[key] = int(row['len'])
    return menus, counts


def _default_fund_data(code: str, name: str) -> dict:
    """真实行情不可用时的占位数据。"""
    return {
        "date": datetime.datetime.now().strftime("%Y-%m-%d"),
        "code": code,
        "name": name,
        "change_pct": "0.00%",
        "price": "0.00",
        "change": "0.00",
        "volume": "0",
        "amount": "0.00亿",
        "open": "0.00",
        "high": "0.00",
        "low": "0.00",
        "preclose": "0.00",
        "amplitude": "0.00%",
    }


def _change_color(text: str) -> QColor:
    """按涨跌文本着色的规则（与原实现保持一致）。"""
    if text.startswith('+') or (text.replace('.', '').isdigit() and float(text) > 0):
        return _COLOR_UP
    if text.startswith('-'):
        return _COLOR_DOWN
    return _COLOR_FLAT


class FundSection:
    """基金板块控制器：负责 ETF / LOF / 分级基金的加载与展示。"""

    def __init__(self, window):
        self._w = window

    # ------------------------------------------------------------------
    # 入口
    # ------------------------------------------------------------------

    def show_etf_funds(self):
        """显示 ETF 基金数据。"""
        self._show_funds('ETF')

    def show_lof_funds(self):
        """显示 LOF 基金数据。"""
        self._show_funds('LOF')

    def show_graded_funds(self):
        """显示分级基金数据。"""
        self._show_funds('分级')

    # ------------------------------------------------------------------
    # 参数化主流程
    # ------------------------------------------------------------------

    def _get_fund_source(self, kind: str):
        """按 ``fund_type`` 从本地名录取出一类基金；不访问数据库。"""
        data_manager = getattr(self._w, 'data_manager', None)
        config = getattr(data_manager, 'config', None)
        menus, unknown = split_by_fund_type(load_local_fund_basic(config))
        if unknown:
            logger.info(f"基金名录存在未归类 fund_type: {unknown}")

        source = menus.get(kind, pl.DataFrame())
        if source.is_empty():
            return source
        keep = [name for name in ('ts_code', 'name') if name in source.columns]
        return source.select(keep)

    def _show_funds(self, kind: str):
        title = f'{kind}基金'
        result_key = f'{kind}_funds'

        logger.info(f"显示{title}数据")
        window = self._w

        try:
            self._prepare_fund_display(window, title)

            current_task_id = global_task_manager.create_task(
                f"加载{title}数据",
                self._load_funds_task,
                (kind, title, result_key)
            )

            def on_task_completed(task_id, result):
                if task_id != current_task_id:
                    return
                try:
                    window.market_progress_bar.setVisible(False)

                    if result["success"]:
                        funds = result.get(result_key, [])
                        self._render_funds_result(window, funds, title)
                    else:
                        error_message = result.get("message", f"加载{title}数据失败")
                        window.statusBar().showMessage(error_message, 3000)
                        logger.error(error_message)
                except (OSError, RuntimeError, ValueError) as e:
                    logger.exception(f"显示{title}数据失败: {e}")
                    window.statusBar().showMessage(f"显示{title}数据失败: {str(e)}", 3000)
                finally:
                    window.market_progress_bar.setVisible(False)
                    try:
                        global_task_manager.task_completed.disconnect(on_task_completed)
                        global_task_manager.task_progress.disconnect(on_task_progress)
                    except Exception as exc:  # 信号可能已被其他路径断开，重复解绑失败不影响结果
                        logger.debug(f"解绑任务信号失败: {exc}")

            def on_task_progress(task_id, current, total):
                if task_id != current_task_id:
                    return
                try:
                    window.market_progress_bar.setValue(int((current / total) * 100))
                except Exception as e:  # 进度条更新失败只影响 UI 反馈，不阻断任务回调
                    logger.debug(f"更新市场进度条失败: {e}", exc_info=True)

            global_task_manager.task_completed.connect(on_task_completed)
            global_task_manager.task_progress.connect(on_task_progress)
        except (OSError, RuntimeError, ValueError) as e:
            logger.exception(f"显示{title}数据失败: {e}")
            window.statusBar().showMessage(f"显示{title}数据失败: {str(e)}", 3000)

            if kind == 'ETF' and hasattr(window, 'table_interaction_manager'):
                window.table_interaction_manager.restore_sort_state()

    def _prepare_fund_display(self, window, title):
        """切换到行情标签页、清空表格并显示加载状态。"""
        # 切换到行情标签页并清空表格
        window.tab_widget.setCurrentIndex(0)
        window.stock_table.setRowCount(0)
        window.stock_table.setSortingEnabled(False)

        # 显示状态栏消息和进度条
        window.statusBar().showMessage(f"加载{title}数据...", 0)
        window.market_progress_bar.setVisible(True)
        window.market_progress_bar.setValue(0)
        QApplication.processEvents()

    def _load_funds_task(self, kind, title, result_key, task_id=None, signals=None):
        """后台加载基金数据"""
        try:
            if signals:
                signals.progress.emit(task_id, 0, 100)

            funds = []
            source = self._get_fund_source(kind)

            if source is not None and not source.is_empty():
                fund_codes = []
                for row in source.iter_rows(named=True):
                    ts_code = str(row.get('ts_code', '') or '').strip().upper()
                    if not ts_code:
                        continue

                    name = row.get('name', '')
                    code = ts_code.split('.')[0]
                    if not name:
                        name = ts_code
                    fund_codes.append((code, name, ts_code))

                # 为每只基金获取真实交易数据
                added_codes = set()
                total = len(fund_codes)
                for i, (code, name, ts_code) in enumerate(fund_codes):
                    if ts_code in added_codes:
                        continue
                    real_data = self.get_real_fund_data(code, name, ts_code)
                    if real_data:
                        funds.append(real_data)
                    else:
                        logger.info(f"获取{title} {name}({ts_code}) 真实数据失败，使用默认数据")
                        funds.append(_default_fund_data(ts_code, name))
                    added_codes.add(ts_code)
                    if signals and total > 0:
                        signals.progress.emit(task_id, int((i + 1) / total * 100), 100)

            if not funds:
                logger.info(f"未从数据表读取到{title}名称或行情数据")

            if signals:
                signals.progress.emit(task_id, 100, 100)

            return {"success": True, result_key: funds}
        except (OSError, RuntimeError, ValueError) as e:
            logger.exception(f"加载{title}数据失败: {e}")
            return {"success": False, "message": f"加载{title}数据失败: {str(e)}"}

    def _render_funds_result(self, window, funds, title):
        """填充基金表格、恢复排序并提示加载结果。"""
        self._fill_fund_table(funds)
        if hasattr(window, 'table_interaction_manager'):
            window.table_interaction_manager.restore_sort_state()
        window.statusBar().showMessage(f"成功加载 {len(funds)} 只{title}数据", 3000)

    # ------------------------------------------------------------------
    # 表格填充
    # ------------------------------------------------------------------

    def _fill_fund_table(self, funds):
        """将基金数据填充到行情表格（13 列，含涨跌配色）。"""
        table = self._w.stock_table

        # 先关闭排序再填充，避免按代码排序时写入行被移动导致列错位
        table.setSortingEnabled(False)
        table.setRowCount(len(funds))

        for row, fund in enumerate(funds):
            preclose_text = fund.get('preclose', '0')

            for col, key in enumerate(_FUND_COLUMNS):
                item = QTableWidgetItem(fund.get(key, ''))
                if col in _NUMERIC_COLUMNS:
                    item.setTextAlignment(Qt.AlignRight | Qt.AlignVCenter)

                if col == 3:  # 涨跌幅%
                    item.setForeground(_change_color(fund.get('change_pct', '')))
                elif col == 5:  # 涨跌
                    item.setForeground(_change_color(fund.get('change', '')))
                elif col in (4, 8, 9, 10):  # 现价/今开/最高/最低，与昨收比较
                    color = self._price_color(fund.get(key, '0'), preclose_text)
                    if color is not None:
                        item.setForeground(color)

                table.setItem(row, col, item)

        # 与行情表同一口径：按每列最长内容实测列宽（代码带后缀、名称不再被截断）
        apply_estimated_column_widths(
            table, [[fund.get(key, '') for key in _FUND_COLUMNS] for fund in funds])

    @staticmethod
    def _price_color(price_text: str, preclose_text: str):
        """价格与昨收比较着色；解析失败时返回 None 表示不着色。"""
        try:
            price = float(price_text)
            preclose = float(preclose_text)
        except (TypeError, ValueError) as e:
            logger.debug(f"设置价格单元格颜色失败: {e}", exc_info=True)
            return None

        if price > preclose:
            return _COLOR_UP
        if price < preclose:
            return _COLOR_DOWN
        return _COLOR_FLAT

    # ------------------------------------------------------------------
    # 真实行情
    # ------------------------------------------------------------------

    def get_real_fund_data(self, code, name, ts_code=None):
        """
        从通达信数据文件获取基金的真实交易数据。

        Args:
            code: 基金代码
            name: 基金名称
            ts_code: 标准代码（如 510300.SH），可选

        Returns:
            dict: 基金交易数据；无法获取时返回 None
        """
        try:
            data_manager = getattr(self._w, 'data_manager', None)
            if not data_manager:
                return None

            logger.info(f"从通达信数据文件获取基金 {name}({code}) 的交易数据")
            quote = data_manager.get_latest_local_quote(code, ts_code)
            if quote is None:
                return None

            change_str = f"+{quote.change:.2f}" if quote.change >= 0 else f"{quote.change:.2f}"
            change_pct_str = f"+{quote.pct_chg:.2f}%" if quote.pct_chg >= 0 else f"{quote.pct_chg:.2f}%"

            return {
                "date": quote.date,
                "code": ts_code or code,
                "name": name,
                "change_pct": change_pct_str,
                "price": f"{quote.close:.2f}",
                "change": change_str,
                "volume": f"{quote.volume:,.0f}",
                "amount": f"{round(quote.amount / 100000000, 2)}亿",
                "open": f"{quote.open:.2f}",
                "high": f"{quote.high:.2f}",
                "low": f"{quote.low:.2f}",
                "preclose": f"{quote.preclose:.2f}",
                "amplitude": f"{quote.amplitude:.2f}%",
            }
        except (OSError, RuntimeError, ValueError) as e:
            logger.warning(f"获取基金 {name}({code}) 的真实交易数据失败: {e}")

        return None
