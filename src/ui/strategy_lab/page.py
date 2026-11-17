"""策略试算标签页：结构化条件构建 → 复用既有引擎回测 → 净值与指标展示。

界面只做三件事：编辑规则、调用 :mod:`src.quant.strategy_lab` 的解析与回测适配层、
展示结果。不执行用户输入的任何代码，字段与运算符只能从白名单下拉中选择。
表格列宽统一走 ``src.ui.table_sizing``，不在页面内写死宽度。
"""
from __future__ import annotations

from typing import Any, Dict, List

from loguru import logger
from PySide6.QtCore import QDate, Qt, QThread, Signal
from PySide6.QtGui import QColor
from PySide6.QtWidgets import (
    QAbstractItemView,
    QComboBox,
    QDateEdit,
    QDoubleSpinBox,
    QFormLayout,
    QGroupBox,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QLineEdit,
    QMessageBox,
    QPushButton,
    QScrollArea,
    QSpinBox,
    QSplitter,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
)

from src.quant.strategy_lab.fields import (
    FIELDS,
    OPERATORS,
    QUANTILE_OPERATORS,
    field_catalog,
)
from src.quant.strategy_lab.rules import (
    BENCHMARKS,
    RuleError,
    list_rules,
    load_rule,
    rule_text,
    save_rule,
    validate_rule,
)
from src.ui.table_sizing import apply_content_widths, use_interactive_columns

COLOR_EQUITY = '#2f7ed8'
COLOR_BENCH = '#c0504d'
PLOT_LEGEND_EQUITY = '策略净值'
PLOT_LEGEND_BENCH = '基准净值'

_METRIC_ROWS = (
    ('annual', '年化收益', 'pct'),
    ('max_drawdown', '最大回撤', 'pct'),
    ('sharpe', 'Sharpe', 'num'),
    ('volatility', '年化波动', 'pct'),
    ('turnover_one_way', '单边年换手', 'x'),
    ('annual_cost_pct', '年化成本', 'pct'),
    ('benchmark_annual', '基准年化', 'pct'),
    ('excess_annual', '年化超额', 'pct'),
    ('information_ratio', '信息比率 IR', 'num'),
    ('final_equity', '期末净值', 'money'),
    ('trades', '成交笔数', 'int'),
    ('rebalances', '调仓次数', 'int'),
    ('members_median', '成分中位', 'int'),
    ('members_min', '成分最少', 'int'),
    ('members_max', '成分最多', 'int'),
)


def _fill_field_combo(combo: QComboBox) -> None:
    """按类别分组填充字段下拉；分组标题不可选。"""
    combo.clear()
    for group in field_catalog():
        combo.addItem(f'— {group["category"]} —')
        combo.model().item(combo.count() - 1).setEnabled(False)
        for spec in group['fields']:
            combo.addItem(spec['label'], spec['key'])


def _set_field(combo: QComboBox, key: str | None) -> None:
    index = combo.findData(key) if key else -1
    combo.setCurrentIndex(index if index >= 0 else 1)


class _ConditionRow(QWidget):
    """一条条件：字段 + 运算符 +（数值 / 分位）。"""

    def __init__(self, on_change, on_remove, parent=None):
        super().__init__(parent)
        self._on_change = on_change
        layout = QHBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)

        self.field_combo = QComboBox()
        _fill_field_combo(self.field_combo)
        self.op_combo = QComboBox()
        for key, label in OPERATORS.items():
            self.op_combo.addItem(label, key)
        self.type_combo = QComboBox()
        self.type_combo.addItem('数值', 'value')
        self.type_combo.addItem('分位', 'quantile')

        self.spin_a = QDoubleSpinBox()
        self.spin_a.setDecimals(4)
        self.spin_a.setRange(-1e9, 1e9)
        self.spin_a.setSingleStep(0.1)
        self.spin_b = QDoubleSpinBox()
        self.spin_b.setDecimals(4)
        self.spin_b.setRange(-1e9, 1e9)
        self.spin_b.setSingleStep(0.1)
        self.spin_b.setVisible(False)
        self.sep_label = QLabel('~')
        self.sep_label.setVisible(False)

        self.remove_btn = QPushButton('删除')
        self.remove_btn.setFixedWidth(56)
        self.remove_btn.clicked.connect(lambda: on_remove(self))

        for widget in (self.field_combo, self.op_combo, self.type_combo,
                       self.spin_a, self.sep_label, self.spin_b):
            layout.addWidget(widget)
        layout.addStretch(1)
        layout.addWidget(self.remove_btn)

        self.op_combo.currentIndexChanged.connect(self._sync_operand)
        self.type_combo.currentIndexChanged.connect(self._sync_operand)
        for widget in (self.field_combo, self.op_combo, self.type_combo):
            widget.currentIndexChanged.connect(lambda *_: self._on_change())
        self.spin_a.valueChanged.connect(lambda *_: self._on_change())
        self.spin_b.valueChanged.connect(lambda *_: self._on_change())

    def _sync_operand(self) -> None:
        is_quantile = self.type_combo.currentData() == 'quantile'
        is_between = self.op_combo.currentData() == 'between'
        if is_quantile and self.op_combo.currentData() not in QUANTILE_OPERATORS:
            self.op_combo.setCurrentIndex(0)
        self.spin_b.setVisible(is_between and not is_quantile)
        self.sep_label.setVisible(is_between and not is_quantile)
        if is_quantile:
            self.spin_a.setRange(0.01, 0.99)
            self.spin_a.setDecimals(2)
            self.spin_a.setSingleStep(0.05)
            if self.spin_a.value() > 0.99:
                self.spin_a.setValue(0.5)
        else:
            self.spin_a.setRange(-1e9, 1e9)
            self.spin_a.setDecimals(4)
            self.spin_a.setSingleStep(0.1)
        self._on_change()

    def to_condition(self) -> Dict[str, Any]:
        op = self.op_combo.currentData()
        if self.type_combo.currentData() == 'quantile':
            operand = {'type': 'quantile', 'q': round(self.spin_a.value(), 2)}
        elif op == 'between':
            operand = {'type': 'value', 'low': self.spin_a.value(),
                       'high': self.spin_b.value()}
        else:
            operand = {'type': 'value', 'value': self.spin_a.value()}
        return {'field': self.field_combo.currentData(), 'op': op, 'operand': operand}

    def set_condition(self, cond: Dict[str, Any]) -> None:
        _set_field(self.field_combo, cond['field'])
        self.op_combo.setCurrentIndex(max(0, self.op_combo.findData(cond['op'])))
        operand = cond['operand']
        is_quantile = operand['type'] == 'quantile'
        self.type_combo.setCurrentIndex(1 if is_quantile else 0)
        if is_quantile:
            self.spin_a.setValue(float(operand['q']))
        elif 'low' in operand:
            self.spin_a.setValue(float(operand['low']))
            self.spin_b.setValue(float(operand['high']))
        else:
            self.spin_a.setValue(float(operand['value']))
        self._sync_operand()


class _RunWorker(QThread):
    ok = Signal(dict)
    failed = Signal(str)

    def __init__(self, rule: Dict[str, Any], parent=None):
        super().__init__(parent)
        self.rule = rule

    def run(self) -> None:
        try:
            from src.quant.strategy_lab.backtest import run_rule

            self.ok.emit(run_rule(self.rule))
        except Exception as exc:  # noqa: BLE001 - 线程顶层必须兜底，异常不能穿透事件循环
            logger.exception('策略试算回测失败')
            self.failed.emit(str(exc))


class StrategyLabPage(QWidget):
    """策略试算主页面。"""

    def __init__(self, parent=None):
        super().__init__(parent)
        self._rows: List[_ConditionRow] = []
        self._worker: _RunWorker | None = None
        self._build_ui()
        self._add_condition(default={'field': 'vol_60', 'op': '<=',
                                     'operand': {'type': 'quantile', 'q': 0.5}})
        self._refresh_rule_text()

    # -- 布局 --------------------------------------------------------------
    def _build_ui(self) -> None:
        root = QVBoxLayout(self)
        root.setContentsMargins(8, 8, 8, 8)
        splitter = QSplitter(Qt.Vertical)
        splitter.addWidget(self._build_editor())
        splitter.addWidget(self._build_result())
        splitter.setStretchFactor(0, 3)
        splitter.setStretchFactor(1, 4)
        root.addWidget(splitter)

    def _build_editor(self) -> QWidget:
        box = QGroupBox('规则编辑')
        outer = QVBoxLayout(box)

        head = QHBoxLayout()
        head.addWidget(QLabel('规则名'))
        self.name_edit = QLineEdit('未命名规则')
        self.name_edit.setMaxLength(40)
        self.name_edit.textChanged.connect(lambda *_: self._refresh_rule_text())
        head.addWidget(self.name_edit, 2)
        self.save_btn = QPushButton('保存规则')
        self.save_btn.clicked.connect(self._on_save)
        head.addWidget(self.save_btn)
        self.load_combo = QComboBox()
        self.load_combo.setMinimumWidth(160)
        head.addWidget(self.load_combo, 1)
        self.load_btn = QPushButton('载入')
        self.load_btn.clicked.connect(self._on_load)
        head.addWidget(self.load_btn)
        self.run_btn = QPushButton('运行试算')
        self.run_btn.setDefault(True)
        self.run_btn.clicked.connect(self._on_run)
        head.addWidget(self.run_btn)
        outer.addLayout(head)

        form = QFormLayout()
        self.universe_combo = QComboBox()
        self.universe_combo.addItem('全市场洁净集', 'all_market')
        self.universe_combo.setEnabled(False)
        form.addRow('股票池', self.universe_combo)

        self.freq_combo = QComboBox()
        self.freq_combo.addItem('月频', 'monthly')
        self.freq_combo.setEnabled(False)
        form.addRow('调仓频率', self.freq_combo)

        self.bench_combo = QComboBox()
        for code, label in BENCHMARKS.items():
            self.bench_combo.addItem(f'{label}（{code}）', code)
        form.addRow('基准', self.bench_combo)

        period = QHBoxLayout()
        self.start_edit = QDateEdit(QDate.fromString('2019-01-04', 'yyyy-MM-dd'))
        self.end_edit = QDateEdit(QDate.currentDate())
        for edit in (self.start_edit, self.end_edit):
            edit.setCalendarPopup(True)
            edit.setDisplayFormat('yyyy-MM-dd')
            edit.dateChanged.connect(lambda *_: self._refresh_rule_text())
        period.addWidget(self.start_edit)
        period.addWidget(QLabel('~'))
        period.addWidget(self.end_edit)
        period.addStretch(1)
        form.addRow('回测区间', period)

        sort_row = QHBoxLayout()
        self.sort_field_combo = QComboBox()
        _fill_field_combo(self.sort_field_combo)
        self.sort_dir_combo = QComboBox()
        self.sort_dir_combo.addItem('降序', 'desc')
        self.sort_dir_combo.addItem('升序', 'asc')
        self.top_n_spin = QSpinBox()
        self.top_n_spin.setRange(1, 500)
        self.top_n_spin.setValue(50)
        sort_row.addWidget(self.sort_field_combo, 2)
        sort_row.addWidget(self.sort_dir_combo)
        sort_row.addWidget(QLabel('取前'))
        sort_row.addWidget(self.top_n_spin)
        sort_row.addStretch(1)
        form.addRow('排序 / 取前 N', sort_row)
        self.sort_field_combo.currentIndexChanged.connect(lambda *_: self._refresh_rule_text())
        self.sort_dir_combo.currentIndexChanged.connect(lambda *_: self._refresh_rule_text())
        self.top_n_spin.valueChanged.connect(lambda *_: self._refresh_rule_text())
        self.bench_combo.currentIndexChanged.connect(lambda *_: self._refresh_rule_text())
        outer.addLayout(form)

        cond_box = QGroupBox('筛选条件（自上而下依次生效，缺值视为不通过）')
        self.cond_layout = QVBoxLayout(cond_box)
        add_btn = QPushButton('添加条件')
        add_btn.clicked.connect(lambda: self._add_condition())
        self.cond_layout.addWidget(add_btn, alignment=Qt.AlignLeft)
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setWidget(cond_box)
        scroll.setMinimumHeight(150)
        outer.addWidget(scroll, 1)

        self.rule_text_label = QLabel()
        self.rule_text_label.setWordWrap(True)
        self.rule_text_label.setStyleSheet('color: palette(mid);')
        outer.addWidget(self.rule_text_label)
        return box

    def _build_result(self) -> QWidget:
        box = QGroupBox('回测结果')
        layout = QVBoxLayout(box)
        split = QSplitter(Qt.Horizontal)

        self.metrics_table = QTableWidget(0, 2)
        self.metrics_table.setHorizontalHeaderLabels(['指标', '数值'])
        self.metrics_table.verticalHeader().setVisible(False)
        self.metrics_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.metrics_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        use_interactive_columns(self.metrics_table)
        self.metrics_table.horizontalHeader().setSectionResizeMode(
            0, QHeaderView.ResizeToContents)
        split.addWidget(self.metrics_table)

        try:
            import pyqtgraph as pg

            self.plot = pg.PlotWidget()
            self.plot.setBackground(None)
            self.plot.showGrid(x=True, y=True, alpha=0.2)
            self.plot.addLegend()
            self.plot.setLabel('left', '净值（起点=1）')
        except Exception:  # noqa: BLE001 - 缺少 pyqtgraph 时降级为提示标签
            logger.warning('pyqtgraph 不可用，净值曲线改为文本提示')
            self.plot = QLabel('当前环境缺少 pyqtgraph，仅展示指标')
            self.plot.setWordWrap(True)
        split.addWidget(self.plot)
        split.setStretchFactor(0, 1)
        split.setStretchFactor(1, 2)
        layout.addWidget(split, 1)

        self.status_label = QLabel('填写规则后点击「运行试算」')
        self.status_label.setWordWrap(True)
        layout.addWidget(self.status_label)
        self._refresh_rule_list()
        return box

    # -- 条件行 ------------------------------------------------------------
    def _add_condition(self, default: Dict[str, Any] | None = None) -> None:
        row = _ConditionRow(self._refresh_rule_text, self._remove_condition)
        if default:
            row.set_condition(default)
        self._rows.append(row)
        # 插到「添加条件」按钮之前
        self.cond_layout.insertWidget(self.cond_layout.count() - 1, row)
        self._refresh_rule_text()

    def _remove_condition(self, row: _ConditionRow) -> None:
        if row in self._rows:
            self._rows.remove(row)
            row.setParent(None)
            row.deleteLater()
        self._refresh_rule_text()

    # -- 规则 --------------------------------------------------------------
    def build_raw_rule(self) -> Dict[str, Any]:
        return {
            'rule_version': 'strategy_lab/v1',
            'name': self.name_edit.text().strip(),
            'universe': {'mode': 'all_market'},
            'conditions': [row.to_condition() for row in self._rows],
            'sort': {'field': self.sort_field_combo.currentData(),
                     'direction': self.sort_dir_combo.currentData()},
            'top_n': self.top_n_spin.value(),
            'rebalance': {'frequency': 'monthly'},
            'period': {'start': self.start_edit.date().toString('yyyy-MM-dd'),
                       'end': self.end_edit.date().toString('yyyy-MM-dd')},
            'benchmark': self.bench_combo.currentData(),
        }

    def _refresh_rule_text(self) -> None:
        try:
            rule = validate_rule(self.build_raw_rule())
            self.rule_text_label.setText(rule_text(rule))
            self.rule_text_label.setStyleSheet('color: palette(mid);')
        except RuleError as exc:
            self.rule_text_label.setText(f'规则待完善：{exc}')
            self.rule_text_label.setStyleSheet(f'color: {COLOR_BENCH};')

    def apply_rule(self, rule: Dict[str, Any]) -> None:
        self.name_edit.setText(rule['name'])
        self.start_edit.setDate(QDate.fromString(rule['period']['start'], 'yyyy-MM-dd'))
        self.end_edit.setDate(QDate.fromString(rule['period']['end'], 'yyyy-MM-dd'))
        self.bench_combo.setCurrentIndex(max(0, self.bench_combo.findData(
            rule['benchmark'])))
        _set_field(self.sort_field_combo, rule['sort']['field'])
        self.sort_dir_combo.setCurrentIndex(
            max(0, self.sort_dir_combo.findData(rule['sort']['direction'])))
        self.top_n_spin.setValue(int(rule['top_n']))
        for row in list(self._rows):
            self._remove_condition(row)
        for cond in rule['conditions']:
            self._add_condition(default=cond)
        self._refresh_rule_text()

    # -- 动作 --------------------------------------------------------------
    def _refresh_rule_list(self) -> None:
        self.load_combo.clear()
        for item in list_rules():
            self.load_combo.addItem(item['name'], item['path'])

    def _on_save(self) -> None:
        try:
            rule = validate_rule(self.build_raw_rule())
        except RuleError as exc:
            QMessageBox.warning(self, '规则不完整', str(exc))
            return
        path = save_rule(rule)
        self._refresh_rule_list()
        index = self.load_combo.findText(rule['name'])
        if index >= 0:
            self.load_combo.setCurrentIndex(index)
        self.status_label.setText(f'已保存：{path}')

    def _on_load(self) -> None:
        path = self.load_combo.currentData()
        if not path:
            QMessageBox.information(self, '暂无规则', '还没有已保存的规则')
            return
        try:
            rule = load_rule(path)
        except RuleError as exc:
            QMessageBox.warning(self, '载入失败', str(exc))
            return
        self.apply_rule(rule)
        self.status_label.setText(f'已载入：{path}')

    def _on_run(self) -> None:
        try:
            rule = validate_rule(self.build_raw_rule())
        except RuleError as exc:
            QMessageBox.warning(self, '规则不完整', str(exc))
            return
        if self._worker is not None and self._worker.isRunning():
            self.status_label.setText('已有一次试算在运行，请等待完成')
            return
        self.run_btn.setEnabled(False)
        self.status_label.setText('试算中：加载本地行情与 PIT 字段面板…')
        self._worker = _RunWorker(rule, self)
        self._worker.ok.connect(self._on_result)
        self._worker.failed.connect(self._on_failed)
        self._worker.finished.connect(lambda: self.run_btn.setEnabled(True))
        self._worker.start()

    def _on_failed(self, message: str) -> None:
        self.status_label.setText(f'试算失败：{message}')

    def _on_result(self, payload: Dict[str, Any]) -> None:
        if payload.get('status') != 'ok':
            self.status_label.setText(f'空结果：{payload.get("reason", "无可用成分")}')
            self._fill_metrics({"rebalances": 0})
            self._plot_curves(payload.get('nav') or [], payload.get('benchmark_curve') or [])
            return
        metrics = payload['metrics']
        self._fill_metrics(metrics)
        self._plot_curves(payload.get('nav') or [], payload.get('benchmark_curve') or [])
        self.status_label.setText(
            f'{payload["rule_text"]}  用时 {payload["elapsed_seconds"]}s '
            f'（引擎 {payload["engine_seconds"]}s，参与标的 {payload["used_codes"]} 只）')

    def _fill_metrics(self, metrics: Dict[str, Any]) -> None:
        rows = [(label, self._format(kind, metrics.get(key)))
                for key, label, kind in _METRIC_ROWS]
        self.metrics_table.setRowCount(len(rows))
        for i, (label, value) in enumerate(rows):
            self.metrics_table.setItem(i, 0, QTableWidgetItem(label))
            item = QTableWidgetItem(value)
            item.setTextAlignment(Qt.AlignRight | Qt.AlignVCenter)
            self.metrics_table.setItem(i, 1, item)
        apply_content_widths(self.metrics_table)

    @staticmethod
    def _format(kind: str, value: Any) -> str:
        if value is None:
            return '—'
        if kind == 'pct':
            return f'{float(value) * 100:.2f}%'
        if kind == 'num':
            return f'{float(value):.3f}'
        if kind == 'x':
            return f'{float(value):.2f}x'
        if kind == 'money':
            return f'{float(value):,.0f}'
        return str(value)

    def _plot_curves(self, nav: List[List[Any]], bench: List[List[Any]]) -> None:
        if not hasattr(self.plot, 'plot'):
            return
        self.plot.clear()
        if nav:
            base = float(nav[0][1]) or 1.0
            xs = list(range(len(nav)))
            ys = [float(v) / base for _, v in nav]
            self.plot.plot(xs, ys, pen=QColor(COLOR_EQUITY), name=PLOT_LEGEND_EQUITY)
            self.plot.setLabel('bottom', f'{nav[0][0]} ~ {nav[-1][0]}（交易日序号）')
        if bench:
            base = float(bench[0][1]) or 1.0
            xs = list(range(len(bench)))
            ys = [float(v) / base for _, v in bench]
            self.plot.plot(xs, ys, pen=QColor(COLOR_BENCH), name=PLOT_LEGEND_BENCH,
                           style=Qt.DashLine)
