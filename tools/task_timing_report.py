#!/usr/bin/env python3
"""任务耗时 / 调用数周报（只读）。

数据源：``docs/ops/task_timing.csv``（字段定义见 ``.trae/rules/token-efficiency.md`` R16）。

口径：

- 周按 ISO 周（``date.isocalendar``），与 ``docs/token/ledger_daily.csv`` 的日期同一来源；
- ``task_id`` 去掉日期后缀（``YYYY-MM-DD`` / ``YYYYMMDD``）视为同族，用于跨周比较；
- 环比恶化 = 同族任务本周平均耗时或平均调用的涨幅，取涨幅最大的前 3；
- 耗时 / 调用为 ``unknown`` 的行不参与均值，但计入任务数。

用法::

    python tools/task_timing_report.py
    python tools/task_timing_report.py --weeks 12
    python tools/task_timing_report.py --json

退出码恒为 0：本脚本只报告，不判定失败。
"""

from __future__ import annotations

import argparse
import csv
import json
import re
from collections import defaultdict
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_FILE = ROOT / 'docs' / 'ops' / 'task_timing.csv'
DATE_TOKEN = re.compile(r'20\d{2}-?\d{2}-?\d{2}')


def _num(value: object) -> float | None:
    raw = str(value or '').strip().lower()
    if raw in ('', 'unknown'):
        return None
    try:
        return float(raw)
    except ValueError:
        return None


def family_of(task_id: str) -> str:
    name = DATE_TOKEN.sub('', task_id)
    return re.sub(r'[_\-\s]+$', '', name) or task_id


def week_of(day: str) -> str:
    iso = date.fromisoformat(day).isocalendar()
    return f'{iso.year}-W{iso.week:02d}'


def load(path: Path) -> list[dict]:
    if not path.is_file():
        raise SystemExit(f'找不到 {path}，先按 R16 落库')
    with path.open(encoding='utf-8-sig', newline='') as handle:
        return list(csv.DictReader(handle))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description='任务耗时 / 调用数周报（只读）')
    parser.add_argument('--file', default=str(DEFAULT_FILE))
    parser.add_argument('--weeks', type=int, default=8)
    parser.add_argument('--json', action='store_true')
    args = parser.parse_args(argv)

    rows = load(Path(args.file))
    weeks: dict[str, dict[str, list[tuple[float | None, float | None]]]] = defaultdict(lambda: defaultdict(list))
    over: dict[str, int] = defaultdict(int)
    for row in rows:
        day = str(row.get('date') or '').strip()
        if not day:
            continue
        wk = week_of(day)
        weeks[wk][family_of(str(row.get('task_id') or 'unknown'))].append(
            (_num(row.get('duration_sec')), _num(row.get('tool_calls'))))
        if str(row.get('over_threshold') or 'none').strip().lower() not in ('', 'none'):
            over[wk] += 1

    def avg(values: list[float]) -> float | None:
        values = [v for v in values if v is not None]
        return sum(values) / len(values) if values else None

    stats = []
    for wk in sorted(weeks):
        fams = weeks[wk]
        durs = [d for pairs in fams.values() for d, _ in pairs if d is not None]
        calls = [c for pairs in fams.values() for _, c in pairs if c is not None]
        stats.append({
            'week': wk,
            'tasks': sum(len(v) for v in fams.values()),
            'avg_duration': avg(durs),
            'avg_calls': avg(calls),
            'over': over[wk],
        })

    window = stats[-args.weeks:]
    ranked = []
    for prev, cur in zip(window, window[1:]):
        prev_map = weeks[prev['week']]
        cur_map = weeks[cur['week']]
        for fam in set(prev_map) & set(cur_map):
            pd = avg([d for d, _ in prev_map[fam] if d is not None])
            cd = avg([d for d, _ in cur_map[fam] if d is not None])
            pc = avg([c for _, c in prev_map[fam] if c is not None])
            cc = avg([c for _, c in cur_map[fam] if c is not None])
            d_pct = (cd - pd) / pd * 100 if pd and cd is not None else None
            c_pct = (cc - pc) / pc * 100 if pc and cc is not None else None
            scores = [p for p in (d_pct, c_pct) if p is not None]
            if not scores:
                continue
            scored = max(scores)
            if scored <= 0:
                continue
            ranked.append({'week': cur['week'], 'task': fam, 'duration_pct': d_pct,
                           'calls_pct': c_pct, 'score': scored})
    ranked.sort(key=lambda x: x['score'], reverse=True)
    top = ranked[:3]

    if args.json:
        print(json.dumps({'weekly': window, 'worsened_top3': top}, ensure_ascii=False, indent=2))
        return 0

    print('周          任务数  平均耗时(s)  平均调用  超阈数')
    for s in window:
        dur = '-' if s['avg_duration'] is None else f"{s['avg_duration']:.0f}"
        cal = '-' if s['avg_calls'] is None else f"{s['avg_calls']:.0f}"
        print(f"{s['week']:<10}  {s['tasks']:>5}  {dur:>10}  {cal:>7}  {s['over']:>5}")
    print('\n环比恶化 top3（本周 vs 上周，仅列涨幅为正）')
    if not top:
        print('  无同族任务可比或均未恶化。')
    for i, item in enumerate(top, 1):
        parts = []
        if item['duration_pct'] is not None:
            parts.append(f"耗时 {item['duration_pct']:+.0f}%")
        if item['calls_pct'] is not None:
            parts.append(f"调用 {item['calls_pct']:+.0f}%")
        print(f"  {i}. {item['task']}（{item['week']}）：" + ' / '.join(parts))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
