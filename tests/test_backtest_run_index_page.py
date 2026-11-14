"""运行清单页对「残缺 run」与夹具目录的判定。

口径：index.json 里没有、但目录存在的 run 不能隐形——按 progress.json 的
status 显示为已停止/失败/进行中；下划线开头的夹具目录不显示。
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / 'tools') not in sys.path:
    sys.path.insert(0, str(ROOT / 'tools'))

from backtest_run_index_page import scan_unindexed_runs  # noqa: E402


def _write(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding='utf-8')


def test_scan_unindexed_labels_stopped_and_skips_fixtures(tmp_path):
    stopped = tmp_path / '20261114-120000_stopped_demo'
    _write(stopped / 'progress.json',
           {'status': 'stopped', 'started_at': '2026-11-14T12:00:00',
            'percent': 5.96, 'current_date': '2010-08-10',
            'metrics_window': {'start': '2009-08-03', 'end': '2010-08-10'}})
    (stopped / 'daily_nav.partial.csv').write_text(
        'date,equity\n2009-08-03,100000000\n2010-08-10,120000000\n', encoding='utf-8')
    _write(tmp_path / '_fixtures' / 'fake' / 'progress.json', {'status': 'failed'})

    runs = scan_unindexed_runs(tmp_path, indexed=set())

    assert [r['index']['run_id'] for r in runs] == ['20261114-120000_stopped_demo']
    run = runs[0]
    assert run['index']['status'] == 'stopped'
    assert run['metrics'] is None
    assert run['summary'], '残缺 run 也要有可见摘要，不能渲染成空行'
    assert run['nav'], '残缺 run 的净值应取 daily_nav.partial.csv'


def test_scan_unindexed_marks_dir_without_progress_incomplete(tmp_path):
    (tmp_path / '20261114-130000_no_files').mkdir()

    runs = scan_unindexed_runs(tmp_path, indexed=set())

    assert runs[0]['index']['status'] == 'incomplete'
    assert runs[0]['summary'] == '未完成（无 spec 快照）'
