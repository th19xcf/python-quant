#!/usr/bin/env python3

"""数据新鲜度只读检查器。

按 ``freshness_config.json`` 登记的数据集，逐个读取「权威新鲜度字段」，计算
最新日期、距今天数、相对正常更新频率的超期倍数，并输出分级（ok/warn/critical）
结果与覆盖标的数/覆盖占比。

只读约束：
- 不写 ``data/**``，不触发任何采集、构建或修复；
- 结果文件只允许写到 ``artifacts/``（由调用方指定，默认
  ``artifacts/data_freshness``）。

口径说明：
- 交易日按「周一至周五」近似；法定节假日可通过配置 ``defaults.holidays``
  排除，避免节假日误报；
- 超期倍数 = 距今天数（按 cadence.unit 计） / cadence.expected_interval；
- ``warn_multiple`` 与 ``critical_multiple`` 全部来自配置，代码内不写死；
- 覆盖占比 = 覆盖标的数 / 应有标的数，分母由配置声明；无分母的数据集只报
  覆盖数量，占比输出 null。
"""

from __future__ import annotations

import csv
import json
import unicodedata
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import polars as pl
from polars.exceptions import PolarsError

_UNIT_TRADING = 'trading_day'
_UNIT_CALENDAR = 'calendar_day'
_UNITS = {_UNIT_TRADING, _UNIT_CALENDAR}
_SEVERITY_ORDER = {'ok': 0, 'warn': 1, 'critical': 2}
_DEFAULT_CONFIG = Path(__file__).with_name('freshness_config.json')
_READ_ERRORS = (OSError, ValueError, KeyError, TypeError, PolarsError)


@dataclass(frozen=True)
class Cadence:
    """正常更新频率与分级阈值。"""

    unit: str
    expected_interval: float
    warn_multiple: float
    critical_multiple: float
    label: str


@dataclass(frozen=True)
class DatasetSpec:
    """单个数据集的检查规格。"""

    key: str
    label: str
    category: str
    freshness_field: str
    source: dict[str, Any]
    cadence: Cadence
    coverage: dict[str, Any]
    note: str


@dataclass
class FreshnessResult:
    """单个数据集的检查结果。"""

    key: str
    label: str
    category: str
    freshness_field: str
    latest_date: date | None
    lag_units: float | None
    lag_calendar_days: int | None
    unit: str
    expected_interval: float
    cadence_label: str
    overdue_ratio: float | None
    state: str
    severity: str
    covered_count: int | None
    expected_count: int | None
    coverage_ratio: float | None
    coverage_severity: str | None
    reasons: list[str] = field(default_factory=list)
    note: str = ''

    def to_dict(self) -> dict[str, Any]:
        """转成 JSON/CSV 可直接序列化的字典。"""
        return {
            'key': self.key,
            'label': self.label,
            'category': self.category,
            'freshness_field': self.freshness_field,
            'latest_date': self.latest_date.isoformat() if self.latest_date else None,
            'lag_units': self.lag_units,
            'lag_calendar_days': self.lag_calendar_days,
            'unit': self.unit,
            'expected_interval': self.expected_interval,
            'cadence_label': self.cadence_label,
            'overdue_ratio': (
                round(self.overdue_ratio, 4) if self.overdue_ratio is not None else None
            ),
            'state': self.state,
            'severity': self.severity,
            'covered_count': self.covered_count,
            'expected_count': self.expected_count,
            'coverage_ratio': (
                round(self.coverage_ratio, 4) if self.coverage_ratio is not None else None
            ),
            'coverage_severity': self.coverage_severity,
            'reasons': list(self.reasons),
            'note': self.note,
        }


def _coerce_date(value: Any) -> date | None:
    """把 parquet 读出的 date/datetime/str 统一成 date。"""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value).strip()
    if not text:
        return None
    for fmt in ('%Y-%m-%d', '%Y%m%d'):
        try:
            return datetime.strptime(text[:10] if '-' in text else text, fmt).date()
        except ValueError:
            continue
    return None


def _latest_file(root: Path, pattern: str) -> Path | None:
    """返回 glob 中 mtime 最新的文件。"""
    candidates = [path for path in root.glob(pattern) if path.is_file()]
    if not candidates:
        return None
    return max(candidates, key=lambda path: path.stat().st_mtime_ns)


def _read_max(root: Path, source: dict[str, Any]) -> tuple[date | None, str]:
    """按 source 配置读取权威新鲜度字段的最大值。"""
    kind = str(source.get('kind', ''))
    if kind == 'file_mtime':
        path = root / str(source['path'])
        if not path.is_file():
            return None, str(path)
        return datetime.fromtimestamp(path.stat().st_mtime).date(), str(path)
    if kind == 'latest_glob_max':
        latest_path = _latest_file(root, str(source['path_glob']))
        if latest_path is None:
            return None, str(root / str(source['path_glob']))
        return (
            _parquet_max(latest_path, str(source['column']), source.get('filter')),
            str(latest_path),
        )
    if kind in {'parquet_max', 'meta_max'}:
        path = root / str(source['path'])
        if not path.is_file():
            return None, str(path)
        return _parquet_max(path, str(source['column']), source.get('filter')), str(path)
    raise ValueError(f'未知的数据源类型: {kind!r}')


def _parquet_max(
    path: Path,
    column: str,
    filter_spec: dict[str, Any] | None,
) -> date | None:
    """读取 parquet 指定列的最大日期，可选单值过滤。"""
    frame = _select_parquet(
        path,
        [column],
        filter_spec,
        pl.col(column).max().alias('_max'),
    )
    if frame.height == 0:
        return None
    return _coerce_date(frame['_max'][0])


def _select_parquet(
    path: Path,
    columns: list[str],
    filter_spec: dict[str, Any] | None,
    aggregation: pl.Expr,
) -> pl.DataFrame:
    """对 parquet 做惰性扫描，只读需要的列并聚合。"""
    needed = list(columns)
    if filter_spec:
        needed.append(str(filter_spec['column']))
    frame = pl.scan_parquet(path).select(needed)
    if filter_spec:
        frame = frame.filter(pl.col(str(filter_spec['column'])) == filter_spec['value'])
    return frame.select(aggregation).collect()


def _count_distinct(root: Path, spec: dict[str, Any] | None) -> int | None:
    """按 count 配置统计覆盖标的数。"""
    if not spec:
        return None
    kind = str(spec.get('kind', ''))
    if kind == 'json_keys':
        path = root / str(spec['path'])
        if not path.is_file():
            return None
        with path.open(encoding='utf-8') as handle:
            payload = json.load(handle)
        return len(payload) if isinstance(payload, dict) else None
    if kind == 'file_lines':
        path = root / str(spec['path'])
        if not path.is_file():
            return None
        with path.open(encoding='utf-8') as handle:
            return sum(1 for line in handle if line.strip())
    if kind == 'latest_glob_distinct':
        latest_path = _latest_file(root, str(spec['path_glob']))
        if latest_path is None:
            return None
        path = latest_path
    elif kind in {'parquet_distinct', 'meta_distinct'}:
        path = root / str(spec['path'])
    else:
        raise ValueError(f'未知的覆盖统计类型: {kind!r}')
    if not path.is_file():
        return None
    column = str(spec['column'])
    frame = _select_parquet(
        path,
        [column],
        spec.get('filter'),
        pl.col(column).n_unique().alias('_count'),
    )
    return int(frame['_count'][0]) if frame.height else None


def _expected_count(root: Path, spec: dict[str, Any] | None) -> int | None:
    """按 expected 配置读取应有标的数。"""
    if not spec:
        return None
    if str(spec.get('kind', '')) == 'fixed':
        return int(spec['value'])
    return _count_distinct(root, spec)


def _count_trading_days(
    latest: date,
    today: date,
    holidays: set[date],
) -> int:
    """统计 (latest, today] 内的工作日数，剔除配置节假日。"""
    count = 0
    cursor = latest + timedelta(days=1)
    while cursor <= today:
        if cursor.weekday() < 5 and cursor not in holidays:
            count += 1
        cursor += timedelta(days=1)
    return count


def _parse_cadence(raw: dict[str, Any], key: str) -> Cadence:
    """解析并校验 cadence 配置。"""
    unit = str(raw.get('unit', _UNIT_CALENDAR))
    if unit not in _UNITS:
        raise ValueError(f'{key}: cadence.unit 必须是 {sorted(_UNITS)}，实际 {unit!r}')
    interval = float(raw.get('expected_interval', 1))
    warn = float(raw.get('warn_multiple', 1))
    critical = float(raw.get('critical_multiple', 3))
    if interval <= 0:
        raise ValueError(f'{key}: expected_interval 必须大于 0')
    if warn <= 0 or critical <= warn:
        raise ValueError(f'{key}: 需满足 critical_multiple > warn_multiple > 0')
    return Cadence(unit, interval, warn, critical, str(raw.get('label', '') or ''))


def parse_config(config: dict[str, Any]) -> list[DatasetSpec]:
    """把原始 JSON 配置解析成数据集规格列表。"""
    datasets = config.get('datasets')
    if not isinstance(datasets, list) or not datasets:
        raise ValueError('配置缺少非空 datasets 列表')
    specs: list[DatasetSpec] = []
    seen: set[str] = set()
    for raw in datasets:
        if not isinstance(raw, dict):
            raise TypeError('datasets 条目必须是对象')
        key = str(raw.get('key', ''))
        if not key or key in seen:
            raise ValueError(f'数据集 key 缺失或重复: {key!r}')
        seen.add(key)
        specs.append(
            DatasetSpec(
                key=key,
                label=str(raw.get('label', key)),
                category=str(raw.get('category', '')),
                freshness_field=str(raw.get('freshness_field', '')),
                source=dict(raw.get('source') or {}),
                cadence=_parse_cadence(dict(raw.get('cadence') or {}), key),
                coverage=dict(raw.get('coverage') or {}),
                note=str(raw.get('note', '')),
            )
        )
    return specs


def load_config(path: str | Path = _DEFAULT_CONFIG) -> dict[str, Any]:
    """读取 JSON 配置（不做解析，交给 :func:`parse_config`）。"""
    with Path(path).open(encoding='utf-8') as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise TypeError('新鲜度配置根节点必须是对象')
    return payload


def _coverage_severity(
    covered: int | None,
    expected: int | None,
    coverage: dict[str, Any],
) -> tuple[float | None, str | None]:
    """计算覆盖占比与覆盖级别。"""
    if covered is None or expected is None or expected <= 0:
        return None, None
    ratio = min(1.0, covered / expected)
    critical_ratio = coverage.get('critical_ratio')
    min_ratio = coverage.get('min_ratio')
    if critical_ratio is not None and ratio < float(critical_ratio):
        return ratio, 'critical'
    if min_ratio is not None and ratio < float(min_ratio):
        return ratio, 'warn'
    return ratio, 'ok'


def _freshness_severity(ratio: float, cadence: Cadence) -> str:
    """按超期倍数给出新鲜度级别。"""
    if ratio >= cadence.critical_multiple:
        return 'critical'
    if ratio >= cadence.warn_multiple:
        return 'warn'
    return 'ok'


def _unit_text(unit: str) -> str:
    return '交易日' if unit == _UNIT_TRADING else '自然日'


def _missing_result(
    spec: DatasetSpec,
    covered: int | None,
    expected: int | None,
    coverage_ratio: float | None,
    coverage_severity: str | None,
    source_label: str,
) -> FreshnessResult:
    """数据源缺失时的结果（固定 critical）。"""
    return FreshnessResult(
        key=spec.key,
        label=spec.label,
        category=spec.category,
        freshness_field=spec.freshness_field,
        latest_date=None,
        lag_units=None,
        lag_calendar_days=None,
        unit=spec.cadence.unit,
        expected_interval=spec.cadence.expected_interval,
        cadence_label=spec.cadence.label,
        overdue_ratio=None,
        state='missing',
        severity='critical',
        covered_count=covered,
        expected_count=expected,
        coverage_ratio=coverage_ratio,
        coverage_severity=coverage_severity,
        reasons=[f'数据源不存在或无法读取: {source_label}'],
        note=spec.note,
    )


def check_dataset(
    store_root: Path | str,
    spec: DatasetSpec,
    today: date | None = None,
    holidays: set[date] | None = None,
) -> FreshnessResult:
    """检查单个数据集。"""
    root = Path(store_root)
    today = today or date.today()
    holidays = holidays or set()
    latest, source_label = _read_max(root, spec.source)
    covered = _count_distinct(root, spec.coverage.get('count'))
    expected = _expected_count(root, spec.coverage.get('expected'))
    coverage_ratio, coverage_severity = _coverage_severity(
        covered, expected, spec.coverage
    )

    if latest is None:
        return _missing_result(
            spec, covered, expected, coverage_ratio, coverage_severity, source_label
        )

    lag_calendar = max(0, (today - latest).days)
    if spec.cadence.unit == _UNIT_TRADING:
        lag_units = float(_count_trading_days(latest, today, holidays))
    else:
        lag_units = float(lag_calendar)
    ratio = lag_units / spec.cadence.expected_interval
    freshness_severity = _freshness_severity(ratio, spec.cadence)
    severity = freshness_severity
    if coverage_severity and _SEVERITY_ORDER[coverage_severity] > _SEVERITY_ORDER[severity]:
        severity = coverage_severity

    reasons: list[str] = []
    if latest > today:
        state = 'future'
        reasons.append(f'最新日期 {latest.isoformat()} 晚于今天，按 0 天计')
    else:
        state = 'stale' if freshness_severity != 'ok' else 'fresh'
    if freshness_severity != 'ok':
        reasons.append(
            f'距最新 {lag_units:g} {_unit_text(spec.cadence.unit)}，'
            f'超期 {ratio:.2f} 个周期（warn={spec.cadence.warn_multiple:g}，'
            f'critical={spec.cadence.critical_multiple:g}）'
        )
    if coverage_severity in {'warn', 'critical'} and coverage_ratio is not None:
        reasons.append(
            f'覆盖 {covered}/{expected}={coverage_ratio:.1%}，'
            f'低于下限 {float(spec.coverage.get("min_ratio", 0)):.1%}'
        )

    return FreshnessResult(
        key=spec.key,
        label=spec.label,
        category=spec.category,
        freshness_field=spec.freshness_field,
        latest_date=latest,
        lag_units=lag_units,
        lag_calendar_days=lag_calendar,
        unit=spec.cadence.unit,
        expected_interval=spec.cadence.expected_interval,
        cadence_label=spec.cadence.label,
        overdue_ratio=ratio,
        state=state,
        severity=severity,
        covered_count=covered,
        expected_count=expected,
        coverage_ratio=coverage_ratio,
        coverage_severity=coverage_severity,
        reasons=reasons,
        note=spec.note,
    )


def _parse_holidays(config: dict[str, Any]) -> set[date]:
    raw = (config.get('defaults') or {}).get('holidays') or []
    holidays: set[date] = set()
    for item in raw:
        parsed = _coerce_date(item)
        if parsed is not None:
            holidays.add(parsed)
    return holidays


def check_all(
    store_root: Path | str,
    config: dict[str, Any] | None = None,
    today: date | None = None,
) -> list[FreshnessResult]:
    """检查配置中登记的全部数据集。

    单类数据读取异常只把该数据集记为 critical，不影响其它数据集。
    """
    config = config or load_config()
    specs = parse_config(config)
    holidays = _parse_holidays(config)
    results: list[FreshnessResult] = []
    for spec in specs:
        try:
            results.append(check_dataset(store_root, spec, today, holidays))
        except _READ_ERRORS as exc:
            results.append(
                FreshnessResult(
                    key=spec.key,
                    label=spec.label,
                    category=spec.category,
                    freshness_field=spec.freshness_field,
                    latest_date=None,
                    lag_units=None,
                    lag_calendar_days=None,
                    unit=spec.cadence.unit,
                    expected_interval=spec.cadence.expected_interval,
                    cadence_label=spec.cadence.label,
                    overdue_ratio=None,
                    state='error',
                    severity='critical',
                    covered_count=None,
                    expected_count=None,
                    coverage_ratio=None,
                    coverage_severity=None,
                    reasons=[f'读取失败: {type(exc).__name__}: {exc}'],
                    note=spec.note,
                )
            )
    return results


def summarize(results: list[FreshnessResult]) -> dict[str, int]:
    """统计三档数量。"""
    summary = {'total': len(results), 'ok': 0, 'warn': 0, 'critical': 0}
    for result in results:
        summary[result.severity] = summary.get(result.severity, 0) + 1
    return summary


def results_payload(
    results: list[FreshnessResult],
    today: date,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    """组装机器可读 JSON 载荷。"""
    generated_at = generated_at or datetime.now()
    return {
        'schema_version': 1,
        'as_of': today.isoformat(),
        'generated_at': generated_at.isoformat(timespec='seconds'),
        'readonly': True,
        'summary': summarize(results),
        'datasets': [result.to_dict() for result in results],
    }


def write_outputs(
    out_dir: Path | str,
    results: list[FreshnessResult],
    today: date,
) -> tuple[Path, Path]:
    """写 JSON + CSV 结果，返回两个路径。"""
    directory = Path(out_dir)
    directory.mkdir(parents=True, exist_ok=True)
    stem = f'freshness_{today.strftime("%Y%m%d")}'
    json_path = directory / f'{stem}.json'
    csv_path = directory / f'{stem}.csv'
    payload = results_payload(results, today)
    json_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + '\n',
        encoding='utf-8',
    )
    rows = [result.to_dict() for result in results]
    fieldnames = list(rows[0].keys()) if rows else []
    with csv_path.open('w', encoding='utf-8', newline='') as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({**row, 'reasons': '；'.join(row['reasons'])})
    return json_path, csv_path


def _display_width(text: str) -> int:
    """按中英文宽度估算显示宽度，用于对齐。"""
    return sum(
        2 if unicodedata.east_asian_width(char) in {'W', 'F'} else 1 for char in text
    )


def _pad(text: str, width: int) -> str:
    return text + ' ' * max(0, width - _display_width(text))


def _join_cells(cells: list[str], widths: list[int]) -> str:
    return '  '.join(_pad(cell, widths[index]) for index, cell in enumerate(cells)).rstrip()


def _lag_text(result: FreshnessResult) -> str:
    if result.lag_units is None:
        return '-'
    if result.unit == _UNIT_TRADING:
        return f'{result.lag_units:g} 交易日（{result.lag_calendar_days} 天）'
    return f'{result.lag_units:g} 天'


def _coverage_text(result: FreshnessResult) -> tuple[str, str]:
    expected_text = (
        str(result.expected_count) if result.expected_count is not None else '-'
    )
    count_text = (
        f'{result.covered_count}/{expected_text}'
        if result.covered_count is not None
        else '-'
    )
    ratio_text = (
        f'{result.coverage_ratio:.1%}' if result.coverage_ratio is not None else '-'
    )
    return count_text, ratio_text


def format_table(results: list[FreshnessResult], today: date) -> str:
    """输出等宽文本表（中文按双宽对齐）。"""
    headers = ['类别', '数据', '最新日期', '距今', '预期频率', '超期倍', '级别',
               '覆盖', '占比', '提示']
    rows: list[list[str]] = []
    for result in results:
        coverage_count, coverage_ratio = _coverage_text(result)
        reason = result.reasons[0] if result.reasons else ''
        if len(reason) > 44:
            reason = reason[:42] + '…'
        rows.append([
            result.category,
            result.label,
            result.latest_date.isoformat() if result.latest_date else '缺失',
            _lag_text(result),
            result.cadence_label,
            f'{result.overdue_ratio:.2f}' if result.overdue_ratio is not None else '-',
            result.severity,
            coverage_count,
            coverage_ratio,
            reason,
        ])
    widths = [
        max(_display_width(headers[index]), *(_display_width(row[index]) for row in rows))
        for index in range(len(headers))
    ]
    lines = [_join_cells(headers, widths)]
    lines.append(_join_cells(['-' * width for width in widths], widths))
    lines.extend(_join_cells(row, widths) for row in rows)
    summary = summarize(results)
    lines.append('')
    lines.append(
        f'汇总（as_of={today.isoformat()}）：共 {summary["total"]} 类，'
        f'ok {summary["ok"]} / warn {summary["warn"]} / critical {summary["critical"]}'
    )
    return '\n'.join(lines)
