#!/usr/bin/env python3

"""
基金板块（ETF / 封闭式基金）

原先 ETF 与封闭式基金在主窗口事件 Mixin 中各有一份约 300 行的
重复实现，此处合并为参数化流程：数据源、代码谓词、结果键与文案
按基金种类配置，表格填充与任务编排只保留一份。
"""

import datetime

from PySide6.QtCore import Qt
from PySide6.QtGui import QColor
from PySide6.QtWidgets import QApplication, QTableWidgetItem

from src.ui.task_manager import global_task_manager
from src.utils.logger import logger

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
    """基金板块控制器：负责 ETF / 封闭式基金的加载与展示。"""

    def __init__(self, window):
        self._w = window

    # ------------------------------------------------------------------
    # 入口
    # ------------------------------------------------------------------

    def show_etf_funds(self):
        """显示 ETF 基金数据。"""
        self._show_funds('etf')

    def show_closed_funds(self):
        """显示封闭式基金数据。"""
        self._show_funds('closed')

    # ------------------------------------------------------------------
    # 参数化主流程
    # ------------------------------------------------------------------

    def _get_fund_source(self, kind: str):
        """按基金种类获取基础数据表。"""
        data_manager = getattr(self._w, 'data_manager', None)
        if not data_manager:
            return None

        if kind == 'etf':
            return data_manager.get_fund_basic()

        # 封闭式基金优先读 closed_fund_basic，空表时回退 fund_basic 按代码规则筛选
        source = data_manager.get_closed_fund_basic()
        if source.is_empty():
            source = data_manager.get_fund_basic()
        return source

    def _show_funds(self, kind: str):
        title = 'ETF基金' if kind == 'etf' else '封闭式基金'
        result_key = f'{kind}_funds'
        is_fund_code = is_etf_fund_ts_code if kind == 'etf' else is_closed_fund_ts_code

        logger.info(f"显示{title}数据")
        window = self._w

        try:
            self._prepare_fund_display(window, title)

            current_task_id = global_task_manager.create_task(
                f"加载{title}数据",
                self._load_funds_task,
                (kind, title, result_key, is_fund_code)
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
                    except Exception as exc:
                        # 信号可能已被其他路径断开，重复解绑失败可忽略
                        logger.debug(f"解绑任务信号失败: {exc}")

            def on_task_progress(task_id, current, total):
                if task_id != current_task_id:
                    return
                try:
                    window.market_progress_bar.setValue(int((current / total) * 100))
                except Exception as e:
                    logger.debug(f"更新市场进度条失败: {e}", exc_info=True)

            global_task_manager.task_completed.connect(on_task_completed)
            global_task_manager.task_progress.connect(on_task_progress)
        except (OSError, RuntimeError, ValueError) as e:
            logger.exception(f"显示{title}数据失败: {e}")
            window.statusBar().showMessage(f"显示{title}数据失败: {str(e)}", 3000)

            if kind == 'etf' and hasattr(window, 'table_interaction_manager'):
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

    def _load_funds_task(self, kind, title, result_key, is_fund_code, task_id=None, signals=None):
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
                    if not ts_code or not is_fund_code(ts_code):
                        continue

                    name = row.get('name', '')
                    code = ts_code.split('.')[0]
                    if not name:
                        name = code
                    fund_codes.append((code, name, ts_code))

                # 为每只基金获取真实交易数据
                added_codes = set()
                total = len(fund_codes)
                for i, (code, name, ts_code) in enumerate(fund_codes):
                    if code in added_codes:
                        continue
                    real_data = self.get_real_fund_data(code, name, ts_code)
                    if real_data:
                        funds.append(real_data)
                    else:
                        logger.info(f"获取{title} {name}({code}) 真实数据失败，使用默认数据")
                        funds.append(_default_fund_data(code, name))
                    added_codes.add(code)
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

            for col, key in enumerate((
                'date', 'code', 'name', 'change_pct', 'price', 'change',
                'volume', 'amount', 'open', 'high', 'low', 'preclose', 'amplitude',
            )):
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
                "code": code,
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
