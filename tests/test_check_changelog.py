"""`tools/check_changelog.py` 的收口校验用例。

重点自证两类必须失败的情形：末行被截断、行数倒退（另含历史被原地改写）。
"""

from __future__ import annotations

from tools.check_changelog import EXPECTED_FIELDS, check_ledger, main

HEADER = ','.join(EXPECTED_FIELDS)


def _row(
    idx: int,
    *,
    action: str = 'create',
    path: str = 'reports/a.parquet',
    size: str = '10',
    digest: str = 'a' * 64,
) -> str:
    prev = 'b' * 64 if action == 'update' else ''
    return f'r{idx},2026-11-12T00:00:0{idx},t,{action},{path},{size},{digest},{prev},system,r'


def _write(path, lines, *, trailing_newline=True):
    text = '\n'.join(lines) + ('\n' if trailing_newline else '')
    path.write_bytes(text.encode('utf-8'))


def test_truncated_last_line_fails(tmp_path):
    """末行少一个换行（写一半）必须报错，不能当成正常文件放过。"""
    ledger = tmp_path / 'data_changelog.csv'
    _write(ledger, [HEADER, _row(1), _row(2)], trailing_newline=False)

    result = check_ledger(ledger, None)

    assert not result.ok
    assert result.ledger.ends_with_newline is False
    assert any('末行未以换行结尾' in item for item in result.failures)


def test_last_row_with_missing_columns_fails(tmp_path):
    """末行有换行但列数不足，同样属「末行不完整」。"""
    ledger = tmp_path / 'data_changelog.csv'
    _write(ledger, [HEADER, _row(1), 'r2,2026-11-12T00:00:02,t,create'])

    result = check_ledger(ledger, None)

    assert not result.ok
    assert result.ledger.ends_with_newline is True
    assert result.ledger.last_row_complete is False
    assert any('列数不符' in item for item in result.failures)


def test_row_count_regression_fails(tmp_path):
    """当前行数少于基线（只增不减被违反）必须报错。"""
    baseline = tmp_path / 'mirror.csv'
    ledger = tmp_path / 'data_changelog.csv'
    _write(baseline, [HEADER, _row(1), _row(2), _row(3)])
    _write(ledger, [HEADER, _row(1), _row(2)])

    result = check_ledger(ledger, baseline)

    assert not result.ok
    assert result.baseline is not None
    assert result.baseline.rows_regressed is True
    assert any('行数倒退' in item for item in result.failures)


def test_pure_append_passes(tmp_path):
    baseline = tmp_path / 'mirror.csv'
    ledger = tmp_path / 'data_changelog.csv'
    _write(baseline, [HEADER, _row(1), _row(2)])
    _write(ledger, [HEADER, _row(1), _row(2), _row(3)])

    result = check_ledger(ledger, baseline)

    assert result.ok, result.failures
    assert result.baseline is not None
    assert result.baseline.is_prefix is True
    assert result.baseline.rows_regressed is False


def test_history_rewritten_in_place_fails(tmp_path):
    """行数相同但字节不同 = 历史被原地改写，必须报错。"""
    baseline = tmp_path / 'mirror.csv'
    ledger = tmp_path / 'data_changelog.csv'
    _write(baseline, [HEADER, _row(1), _row(2)])
    _write(ledger, [HEADER, _row(1), _row(2, size='99')])

    result = check_ledger(ledger, baseline)

    assert not result.ok
    assert result.baseline is not None
    assert result.baseline.is_prefix is False
    assert result.baseline.same_row_count_bytes_differ is True
    assert any('历史内容被改写' in item for item in result.failures)


def test_missing_baseline_fails_when_required(tmp_path):
    ledger = tmp_path / 'data_changelog.csv'
    _write(ledger, [HEADER, _row(1)])

    result = check_ledger(ledger, tmp_path / 'not_there.csv')

    assert not result.ok
    assert any('基线文件不存在' in item for item in result.failures)


def test_historical_value_defects_are_reported_but_not_fatal(tmp_path):
    """历史行的 bytes=-1 / action=move / 空 sha 只统计，不阻断收口。"""
    ledger = tmp_path / 'data_changelog.csv'
    _write(ledger, [
        HEADER,
        'r1,2026-11-05T00:00:00,t,move,research/a -> b,-1,,,system,r',
        r'r2,2026-11-12T00:00:00,t,create,data\local_store\x.parquet,1,' + 'a' * 64 + ',,system,r',
    ])

    result = check_ledger(ledger, None)

    assert result.ok, result.failures
    assert result.ledger.value_violations.get('action') == 1
    assert result.ledger.value_violations.get('bytes') == 1
    assert result.ledger.value_violations.get('sha256') == 1
    assert result.ledger.value_violations.get('path') == 1


def test_main_exit_codes(tmp_path):
    ledger = tmp_path / 'data_changelog.csv'
    _write(ledger, [HEADER, _row(1)])
    assert main(['--file', str(ledger), '--no-baseline']) == 0

    _write(ledger, [HEADER, _row(1)], trailing_newline=False)
    assert main(['--file', str(ledger), '--no-baseline']) == 1
