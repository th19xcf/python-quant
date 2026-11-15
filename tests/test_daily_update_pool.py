"""日更候选池周更的回归测试。

覆盖：
1. 距上次重建未超过阈值时不重建、到期才重建；
2. 重建后输出一行进出变动（新入池 / 出池）与排除清单原因条目数；
3. 重建失败只告警，不改日更结果。
"""

import json
import os
import types
from datetime import datetime, timedelta

import polars as pl

from tools import daily_update as du


def _write_pool(store, codes: list[str], end: str = '2026-11-13') -> None:
    path = store / 'meta' / 'multi_asset' / 'etf_pool.parquet'
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({'ts_code': codes, 'coverage_end': [end] * len(codes)}).write_parquet(path)


def _write_excluded(store, codes: list[str]) -> None:
    path = store / 'meta' / 'multi_asset' / 'etf_pool_excluded.json'
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        'rule': 'equity_watch_no_candidate',
        'criteria': '权益 ETF 且（adv_60d < 20,000,000 或上市不足 365 天）',
        'count': len(codes),
        'codes': codes,
        'evaluated_at': '2026-11-13',
        'items': [{'ts_code': code, 'reason': 'equity_watch_no_candidate',
                   'evaluated_at': '2026-11-13'} for code in codes],
    }
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding='utf-8')


def _args(days: int = 0, skip: bool = False) -> types.SimpleNamespace:
    return types.SimpleNamespace(pool_refresh_days=days, skip_pool_refresh=skip)


def test_pool_due_by_threshold(tmp_path):
    _write_pool(tmp_path, ['510300.SH'])
    assert du._pool_due(tmp_path, 7) is False

    old = (datetime.now() - timedelta(days=8)).timestamp()
    os.utime(tmp_path / 'meta' / 'multi_asset' / 'etf_pool.parquet', (old, old))
    assert du._pool_due(tmp_path, 7) is True
    # 文件缺失一律视为到期
    (tmp_path / 'meta' / 'multi_asset' / 'etf_pool.parquet').unlink()
    assert du._pool_due(tmp_path, 7) is True


def test_pool_refresh_reports_entered_and_exited(tmp_path, monkeypatch):
    _write_pool(tmp_path, ['510300.SH', '159915.SZ'])
    _write_excluded(tmp_path, ['159001.SZ', '158005.SZ'])

    def fake_run(cmd, label):
        _write_pool(tmp_path, ['510300.SH', '512880.SH'])
        return 0

    monkeypatch.setattr(du, '_run_module', fake_run)
    plan = types.SimpleNamespace(root=tmp_path)

    line = du._run_pool_refresh(plan, _args())

    assert '池内 2 只' in line
    assert '新入池 1 / 出池 1' in line
    assert '按规则排除 2 只' in line
    assert '2 条带原因与评估日期' in line


def test_pool_refresh_failure_only_warns(tmp_path, monkeypatch):
    _write_pool(tmp_path, ['510300.SH'])

    monkeypatch.setattr(du, '_run_module', lambda cmd, label: 1)
    line = du._run_pool_refresh(types.SimpleNamespace(root=tmp_path), _args())

    assert '重建未完成' in line


def test_pool_refresh_skipped_when_not_due(tmp_path, monkeypatch):
    _write_pool(tmp_path, ['510300.SH'])

    def fail_run(cmd, label):
        raise AssertionError('未到期不应触发重建')

    monkeypatch.setattr(du, '_run_module', fail_run)
    assert du._run_pool_refresh(types.SimpleNamespace(root=tmp_path), _args(days=7)) == ''
