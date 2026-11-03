"""腾讯分钟 K 线取数工具的单测（离线，锁定字段与单位契约）。"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import polars as pl
import pytest

from src.data.tencent.client import MAX_MINUTE_BARS, parse_minute_rows
from src.data.tencent.minute import (
    fetch_minute_batch,
    format_minute_brief,
    normalize_volume,
    write_summary_csv,
)


def _minute_frame(rows: int = MAX_MINUTE_BARS) -> pl.DataFrame:
    stamps = [f'20261102{930 + index:04d}' for index in range(rows)]
    return pl.DataFrame({
        'datetime': stamps,
        'open': [10.0] * rows,
        'high': [10.1] * rows,
        'low': [9.9] * rows,
        'close': [10.05] * rows,
        'volume': [100.0] * rows,
        'amount': [None] * rows,
    }, schema_overrides={'datetime': pl.Utf8}).with_columns(
        pl.col('datetime').str.strptime(pl.Datetime, format='%Y%m%d%H%M', strict=False)
    ).sort('datetime')


class FakeMinuteClient:
    def __init__(self, frame: pl.DataFrame | None = None) -> None:
        self.frame = _minute_frame() if frame is None else frame
        self.counts: list[int] = []

    def fetch_minute(self, ts_code: str, freq: str = 'm1',
                     count: int = MAX_MINUTE_BARS) -> pl.DataFrame:
        self.counts.append(count)
        return self.frame


def test_parse_minute_rows_field_order_and_null_amount() -> None:
    rows = [['202611031035', '9.89', '9.91', '9.93', '9.88', '15821.00', {}, '0.47']]
    frame = parse_minute_rows(rows)
    assert frame.height == 1
    row = frame.row(0, named=True)
    assert row['datetime'] == datetime(2026, 11, 3, 10, 35)
    assert row['open'] == pytest.approx(9.89)
    assert row['close'] == pytest.approx(9.91)  # 第 3 列是 close
    assert row['high'] == pytest.approx(9.93)
    assert row['low'] == pytest.approx(9.88)
    assert row['volume'] == pytest.approx(15821.0)
    # 第 7 列为空对象 → 无成交额，保持 null，绝不反推
    assert row['amount'] is None


def test_normalize_volume_locks_hand_contract() -> None:
    frame = pl.DataFrame({'volume': [100.0, 250.0]})
    assert normalize_volume(frame, 'hand')['volume'].to_list() == [100.0, 250.0]
    assert normalize_volume(frame, 'share')['volume'].to_list() == [1.0, 2.5]
    with pytest.raises(ValueError):
        normalize_volume(frame, 'unknown')


def test_fetch_minute_batch_clamps_to_320_and_reports_no_amount(tmp_path) -> None:
    client = FakeMinuteClient()
    summary = fetch_minute_batch(client, ['600000.SH'], 'm1', count=500,
                                 output_dir=tmp_path)
    assert client.counts == [MAX_MINUTE_BARS]  # 500 被截断到 320
    assert summary['max_bars_hard_limit'] == 320
    assert summary['total_rows'] == MAX_MINUTE_BARS
    assert summary['amount_available'] is False
    assert summary['can_ingest_stock_minute'] is False
    assert 'stock_minute' in summary['ingest_reason']
    csv_path = Path(summary['items'][0]['csv_path'])
    assert csv_path.exists() and csv_path.name == '600000.SH_m1.csv'
    assert '每档上限 320 根' in format_minute_brief(summary)


def test_fetch_minute_batch_rejects_unknown_frequency() -> None:
    with pytest.raises(ValueError):
        fetch_minute_batch(FakeMinuteClient(), ['600000.SH'], 'm7', count=320)


def test_write_summary_csv(tmp_path) -> None:
    summary = fetch_minute_batch(FakeMinuteClient(), ['600000.SH'], 'm5')
    path = write_summary_csv(tmp_path / 'summary.csv', summary)
    text = Path(path).read_text(encoding='utf-8')
    assert 'ts_code,freq,rows' in text
    assert '600000.SH' in text
