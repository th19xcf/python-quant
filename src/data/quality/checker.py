#!/usr/bin/env python3

"""数据契约巡检编排与报告输出。

巡检只读：支持按标的、按日期区间、全市场三种范围，异常只报警并输出
JSON/CSV 报告，绝不修改任何数据。数据源可切换为：

- ``local``：``data/local_store`` 的 Parquet 列存（当前主读路径）；
- ``tdx``：通达信 ``.day`` 原文件（用于核对源端瑕疵，未经构建期清洗）。
"""

from __future__ import annotations

import csv
import json
import time
from dataclasses import dataclass, field, replace
from datetime import date, datetime
from pathlib import Path
from typing import Any, Protocol, Sequence

import polars as pl
from loguru import logger

from src.data.quality.anomalies import find_anomalies
from src.data.quality.exemptions import is_exempt
from src.data.quality.rules import (
    NUMERIC_COLUMNS,
    RULES,
    RuleContext,
    empty_violations,
)
from src.data.quality.semantics import is_statistic_series
from src.utils.code_converter import is_index_code, to_ts_code


def coerce_date(value: Any) -> date | None:
    """把 str/date/datetime 统一成 date；空值返回 None。"""
    if value is None or value == '':
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value).strip()
    for fmt in ('%Y-%m-%d', '%Y%m%d'):
        try:
            return datetime.strptime(text[:10] if '-' in text else text, fmt).date()
        except ValueError:
            continue
    raise ValueError(f'无法解析日期: {value!r}')


def _safe_code(ts_code: str) -> str:
    try:
        return to_ts_code(str(ts_code))
    except (TypeError, ValueError):
        return str(ts_code).strip().upper()


def prepare_frame(frame: pl.DataFrame) -> pl.DataFrame:
    """统一巡检输入：date 为 Date，数值列为 Float64。"""
    frame = frame.clone()
    if 'date' not in frame.columns and 'trade_date' in frame.columns:
        frame = frame.rename({'trade_date': 'date'})
    if 'date' in frame.columns:
        dtype = frame.schema['date']
        if dtype == pl.Date:
            pass
        elif dtype == pl.Datetime:
            frame = frame.with_columns(pl.col('date').dt.date().alias('date'))
        elif dtype == pl.Utf8:
            frame = frame.with_columns(
                pl.col('date').str.strptime(pl.Date, format='%Y-%m-%d', strict=False)
            )
        elif dtype.is_integer():
            frame = frame.with_columns(
                pl.col('date').cast(pl.Utf8)
                .str.strptime(pl.Date, format='%Y%m%d', strict=False)
            )
        else:
            frame = frame.with_columns(pl.col('date').cast(pl.Date))
    casts = [
        pl.col(column).cast(pl.Float64)
        for column in NUMERIC_COLUMNS
        if column in frame.columns and frame.schema[column] != pl.Float64
    ]
    if casts:
        frame = frame.with_columns(casts)
    return frame


class FrameSource(Protocol):
    """巡检数据源契约。"""

    name: str

    def codes(self, data_type: str) -> list[str]:
        """返回该类别下的全部标的。"""
        ...

    def load(self, data_type: str, ts_code: str) -> pl.DataFrame | None:
        """加载单标的全历史行情；不存在返回 None。"""
        ...


class LocalStoreFrameSource:
    """本地 Parquet 列存数据源（默认，与线上读路径一致）。"""

    name = 'local'

    def __init__(self, store: Any) -> None:
        self._store = store

    def codes(self, data_type: str) -> list[str]:
        """返回标的列表；元数据缺失时回退目录枚举。

        ``daily_index.parquet`` 是派生的覆盖度索引，可能被后续构建覆盖成
        单类别；巡检以文件为准，避免因索引残缺而漏扫整类标的。
        """
        codes = set(self._store.codes(data_type))
        root = getattr(self._store, 'root', None)
        if root is not None:
            quotes = Path(root) / 'quotes' / data_type
            if quotes.is_dir():
                codes.update(path.stem for path in quotes.glob('*/*.parquet'))
        return sorted(codes)

    def load(self, data_type: str, ts_code: str) -> pl.DataFrame | None:
        return self._store.read_file(data_type, ts_code)


class TdxFrameSource:
    """通达信 ``.day`` 原始数据源（不经构建期清洗，用于源端核对）。"""

    name = 'tdx'

    def __init__(self, tdx_root: Path | str) -> None:
        self._root = Path(tdx_root)
        self._index: dict[str, dict[str, Path]] | None = None

    def _ensure_index(self) -> dict[str, dict[str, Path]]:
        if self._index is None:
            from src.data.local_store.build import discover_local_files  # noqa: PLC0415

            index: dict[str, dict[str, Path]] = {'stock': {}, 'index': {}}
            for data_type, ts_code, path in discover_local_files(self._root, 'all'):
                index.setdefault(data_type, {})[ts_code] = path
            self._index = index
        return self._index

    def codes(self, data_type: str) -> list[str]:
        return sorted(self._ensure_index().get(data_type, {}))

    def load(self, data_type: str, ts_code: str) -> pl.DataFrame | None:
        path = self._ensure_index().get(data_type, {}).get(ts_code)
        if path is None or not path.exists():
            return None
        from src.data.tdx_day_parser import parse_day_file  # noqa: PLC0415

        frame = parse_day_file(path)
        return prepare_frame(frame)


@dataclass(frozen=True)
class QualityViolation:
    """单条巡检违规（机器可读）。"""

    ts_code: str
    trade_date: date | None
    rule_id: str
    severity: str
    field: str
    value: float | None
    detail: str
    known_anomaly: bool = False
    anomaly_action: str | None = None
    anomaly_ids: tuple[str, ...] = ()
    exempt: bool = False

    def as_dict(self) -> dict[str, Any]:
        return {
            'ts_code': self.ts_code,
            'trade_date': str(self.trade_date) if self.trade_date else None,
            'rule_id': self.rule_id,
            'severity': self.severity,
            'field': self.field,
            'value': self.value,
            'detail': self.detail,
            'known_anomaly': self.known_anomaly,
            'anomaly_action': self.anomaly_action,
            'anomaly_ids': list(self.anomaly_ids),
            'exempt': self.exempt,
        }


@dataclass
class QualityReport:
    """一次巡检的完整结果。"""

    source: str
    scope: dict[str, Any]
    elapsed_s: float
    symbols_scanned: int
    rows_scanned: int
    violations: list[QualityViolation] = field(default_factory=list)
    missing_symbols: list[str] = field(default_factory=list)
    by_rule: dict[str, int] = field(default_factory=dict)
    by_rule_unique: dict[str, int] = field(default_factory=dict)
    by_severity: dict[str, int] = field(default_factory=dict)
    by_data_type: dict[str, int] = field(default_factory=dict)
    known_by_anomaly: dict[str, int] = field(default_factory=dict)
    unique_violation_rows: int = 0
    #: 未登记为已知基线的 error 级违规数；--fail-on-error 只认这个数字
    actionable_errors: int = 0
    exempt_errors: int = 0
    #: 被识别为涨跌/停板家数统计指标、跳过价格类规则的代码与规则清单
    statistic_series: list[str] = field(default_factory=list)
    skipped_price_rules: list[str] = field(default_factory=list)
    report_files: dict[str, str] = field(default_factory=dict)

    def summary(self) -> dict[str, Any]:
        return {
            'source': self.source,
            'scope': self.scope,
            'elapsed_s': round(self.elapsed_s, 3),
            'symbols_scanned': self.symbols_scanned,
            'rows_scanned': self.rows_scanned,
            'missing_symbols': len(self.missing_symbols),
            'total_violations': len(self.violations),
            'unique_violation_rows': self.unique_violation_rows,
            'by_rule': dict(sorted(self.by_rule.items())),
            'by_rule_unique': dict(sorted(self.by_rule_unique.items())),
            'by_severity': dict(sorted(self.by_severity.items())),
            'by_data_type': dict(sorted(self.by_data_type.items())),
            'known_anomaly_hits': dict(sorted(self.known_by_anomaly.items())),
            'actionable_errors': self.actionable_errors,
            'exempt_errors': self.exempt_errors,
            'statistic_series': list(self.statistic_series),
            'skipped_price_rules': list(self.skipped_price_rules),
        }

    def summary_text(self) -> str:
        parts = [
            f"source={self.source}",
            f"symbols={self.symbols_scanned}",
            f"rows={self.rows_scanned}",
            f"violations={len(self.violations)}",
            f"actionable_errors={self.actionable_errors}",
            f"exempt_errors={self.exempt_errors}",
            f"elapsed={self.elapsed_s:.2f}s",
        ]
        if self.statistic_series:
            parts.append(
                f'statistic_series={len(self.statistic_series)}'
                f'(skipped: {",".join(self.skipped_price_rules) or "none"})')
        if self.by_rule:
            detail = ', '.join(f'{k}={v}' for k, v in sorted(self.by_rule.items()))
            parts.append(f'by_rule[{detail}]')
        return ' '.join(parts)

    def write(self, output_dir: Path | str = 'logs') -> dict[str, str]:
        """写出 JSON + 违规 CSV + 规则汇总 CSV，返回文件路径。"""
        target = Path(output_dir)
        target.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now().strftime('%Y%m%d_%H%M%S_%f')
        base = f'data_quality_{self.source}_{stamp}'
        json_path = target / f'{base}.json'
        csv_path = target / f'{base}_violations.csv'
        summary_path = target / f'{base}_summary.csv'

        payload = {
            'generated_at': datetime.now().isoformat(timespec='seconds'),
            'summary': self.summary(),
            'missing_symbols_list': sorted(self.missing_symbols),
            'rules': [
                {
                    'rule_id': rule.rule_id,
                    'description': rule.description,
                    'severity': rule.severity,
                    'data_types': list(rule.data_types),
                }
                for rule in RULES
            ],
            'violations': [violation.as_dict() for violation in self.violations],
        }
        json_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding='utf-8',
        )

        fieldnames = [
            'ts_code', 'trade_date', 'rule_id', 'severity', 'field', 'value',
            'detail', 'known_anomaly', 'anomaly_action', 'anomaly_ids', 'exempt',
        ]
        with csv_path.open('w', encoding='utf-8', newline='') as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            for violation in self.violations:
                row = violation.as_dict()
                row['anomaly_ids'] = '|'.join(row['anomaly_ids'])
                writer.writerow(row)

        with summary_path.open('w', encoding='utf-8', newline='') as handle:
            writer = csv.DictWriter(handle, fieldnames=['rule_id', 'severity', 'count'])
            writer.writeheader()
            for rule in RULES:
                writer.writerow({
                    'rule_id': rule.rule_id,
                    'severity': rule.severity,
                    'count': self.by_rule.get(rule.rule_id, 0),
                })

        return {
            'json': str(json_path),
            'violations_csv': str(csv_path),
            'summary_csv': str(summary_path),
        }


@dataclass
class _Accumulator:
    """单次巡检的累积状态（拆分 ``run`` 圈复杂度）。"""

    violations: list[QualityViolation] = field(default_factory=list)
    missing: list[str] = field(default_factory=list)
    by_rule: dict[str, int] = field(default_factory=dict)
    by_rule_unique: dict[str, int] = field(default_factory=dict)
    by_severity: dict[str, int] = field(default_factory=dict)
    by_data_type: dict[str, int] = field(default_factory=dict)
    known_by_anomaly: dict[str, int] = field(default_factory=dict)
    unique_keys: set[tuple[str, date | None]] = field(default_factory=set)
    rule_unique_keys: dict[str, set[tuple[str, date | None]]] = field(
        default_factory=dict)
    symbols_scanned: int = 0
    rows_scanned: int = 0
    actionable_errors: int = 0
    exempt_errors: int = 0
    #: 因「统计指标序列」语义而跳过的价格类规则（用于报告口径披露）
    skipped_price_rules: set[str] = field(default_factory=set)

    def add(self, data_type: str, ts_code: str, rule: Any,
            row: dict[str, Any], source_name: str) -> None:
        trade_date = coerce_date(row.get('trade_date'))
        hits = find_anomalies(ts_code, trade_date, context=source_name)
        anomaly_ids = tuple(item.anomaly_id for item in hits)
        exempt = is_exempt(rule.rule_id, ts_code, trade_date)
        self.violations.append(QualityViolation(
            ts_code=ts_code,
            trade_date=trade_date,
            rule_id=rule.rule_id,
            severity=rule.severity,
            field=str(row.get('field') or rule.rule_id),
            value=row.get('value'),
            detail=str(row.get('detail') or ''),
            known_anomaly=bool(hits),
            anomaly_action=hits[0].action if hits else None,
            anomaly_ids=anomaly_ids,
            exempt=exempt,
        ))
        if rule.severity == 'error':
            if exempt:
                self.exempt_errors += 1
            else:
                self.actionable_errors += 1
        self.by_rule[rule.rule_id] = self.by_rule.get(rule.rule_id, 0) + 1
        self.unique_keys.add((ts_code, trade_date))
        self.rule_unique_keys.setdefault(rule.rule_id, set()).add(
            (ts_code, trade_date))
        self.by_severity[rule.severity] = self.by_severity.get(rule.severity, 0) + 1
        self.by_data_type[data_type] = self.by_data_type.get(data_type, 0) + 1
        for anomaly_id in anomaly_ids:
            self.known_by_anomaly[anomaly_id] = (
                self.known_by_anomaly.get(anomaly_id, 0) + 1)

    def finalize_unique(self) -> dict[str, int]:
        return {rule_id: len(keys) for rule_id, keys in self.rule_unique_keys.items()}


class QualityChecker:
    """巡检编排器：按范围加载数据、逐规则执行、汇总并可选落报告。"""

    def __init__(self, source: FrameSource, latest_trade_date: date | None = None,
                 amount_tolerance: float = 0.25,
                 price_tolerance: float = 1e-6) -> None:
        self._source = source
        self._base_context = RuleContext(
            latest_trade_date=latest_trade_date or date.today(),
            amount_tolerance=amount_tolerance,
            price_tolerance=price_tolerance,
        )

    def _select(self, codes: Sequence[str] | None,
                requested_types: Sequence[str]) -> dict[str, list[str]]:
        selected: dict[str, list[str]] = {}
        if codes is not None:
            normalized = sorted({_safe_code(code) for code in codes})
            for data_type in requested_types:
                selected[data_type] = [
                    code for code in normalized
                    if is_index_code(code) == (data_type == 'index')
                ]
            return selected
        for data_type in requested_types:
            selected[data_type] = sorted(set(self._source.codes(data_type)))
        return selected

    def _scan_symbol(self, data_type: str, ts_code: str, context: RuleContext,
                     start_date: date | None, end_date: date | None,
                     acc: _Accumulator) -> None:
        frame = self._source.load(data_type, ts_code)
        if frame is None or frame.is_empty():
            acc.missing.append(ts_code)
            return
        frame = prepare_frame(frame)
        if frame.is_empty():
            acc.missing.append(ts_code)
            return
        acc.symbols_scanned += 1
        acc.rows_scanned += frame.height
        statistic = is_statistic_series(ts_code)
        for rule in RULES:
            if data_type not in rule.data_types:
                continue
            # 涨跌/停板家数序列的 OHLC 装的是家数，价格类规则必然误报
            if statistic and rule.price_semantics:
                acc.skipped_price_rules.add(rule.rule_id)
                continue
            bad = rule.func(frame, context)
            if bad.is_empty():
                continue
            if start_date is not None or end_date is not None:
                bad = self._filter_range(bad, start_date, end_date)
            for row in bad.iter_rows(named=True):
                acc.add(data_type, ts_code, rule, row, self._source.name)

    def run(self, codes: Sequence[str] | None = None,
            data_types: Sequence[str] | None = None,
            start: Any = None, end: Any = None,
            write_report: bool = True,
            output_dir: Path | str = 'logs') -> QualityReport:
        start_date = coerce_date(start)
        end_date = coerce_date(end)
        requested_types = tuple(data_types or ('stock', 'index'))
        selected = self._select(codes, requested_types)

        started = time.perf_counter()
        acc = _Accumulator()
        for data_type in requested_types:
            context = replace(self._base_context, data_type=data_type)
            for ts_code in selected.get(data_type, []):
                self._scan_symbol(data_type, ts_code, context, start_date, end_date, acc)

        elapsed = time.perf_counter() - started
        # 空扫描必须喊出来：源路径指向错误目录时 selected 为空，报告会以
        # 「0 只标的、0 违规」收场，看起来像「数据全部合格」（DG-27）。
        if acc.symbols_scanned == 0:
            logger.error(
                f'[quality] 本次质检扫描 0 只标的（源 {self._source.name}，'
                f'类型 {list(requested_types)}）；此情形下「零问题」不代表数据正常，'
                '请检查源路径是否指向正确目录')
        elif acc.missing:
            logger.warning(
                f'[quality] {len(acc.missing)} 只标的在源 {self._source.name} 中'
                f'读不到数据（明细见报告 missing_symbols）: {acc.missing[:5]}')
        report = QualityReport(
            source=self._source.name,
            scope={
                'codes': 'all' if codes is None else sorted({_safe_code(c) for c in codes}),
                'data_types': list(requested_types),
                'start': str(start_date) if start_date else None,
                'end': str(end_date) if end_date else None,
                'latest_trade_date': str(self._base_context.latest_trade_date),
                'amount_tolerance': self._base_context.amount_tolerance,
            },
            elapsed_s=elapsed,
            symbols_scanned=acc.symbols_scanned,
            rows_scanned=acc.rows_scanned,
            violations=acc.violations,
            missing_symbols=mixed_sorted(acc.missing),
            by_rule=acc.by_rule,
            by_rule_unique=acc.finalize_unique(),
            by_severity=acc.by_severity,
            by_data_type=acc.by_data_type,
            known_by_anomaly=acc.known_by_anomaly,
            unique_violation_rows=len(acc.unique_keys),
            actionable_errors=acc.actionable_errors,
            exempt_errors=acc.exempt_errors,
            statistic_series=sorted(
                code for code in selected.get('index', [])
                if is_statistic_series(code)),
            skipped_price_rules=sorted(acc.skipped_price_rules),
        )
        if write_report:
            report.report_files = report.write(output_dir)
        return report

    @staticmethod
    def _filter_range(bad: pl.DataFrame, start: date | None,
                      end: date | None) -> pl.DataFrame:
        if 'trade_date' not in bad.columns:
            return bad
        condition = pl.lit(True)
        if start is not None:
            condition = condition & (
                pl.col('trade_date').is_null()
                | (pl.col('trade_date') >= pl.lit(start, dtype=pl.Date))
            )
        if end is not None:
            condition = condition & (
                pl.col('trade_date').is_null()
                | (pl.col('trade_date') <= pl.lit(end, dtype=pl.Date))
            )
        return bad.filter(condition)


def mixed_sorted(values: list[str]) -> list[str]:
    """去重并稳定排序（避免报告里 missing_symbols 顺序漂移）。"""
    return sorted(set(values))


def empty_report(source: str) -> QualityReport:
    """空报告（供测试或异常场景兜底）。"""
    return QualityReport(source=source, scope={}, elapsed_s=0.0,
                         symbols_scanned=0, rows_scanned=0)


__all__ = [
    'LocalStoreFrameSource',
    'QualityChecker',
    'QualityReport',
    'QualityViolation',
    'TdxFrameSource',
    'coerce_date',
    'empty_report',
    'empty_violations',
    'prepare_frame',
]
