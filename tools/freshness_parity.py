#!/usr/bin/env python3

"""两套保鲜判据只读对拍。

对同一 ``store_root`` 分别跑：

- 支A ``src.data.quality.freshness.check_all``（配置驱动，24 数据集）；
- 支B ``src.data.quality.freshness_check.run_check``（硬编码 13 行）。

按「被测正式表相对路径」把两支对齐后逐条比较三态判定，把红（critical/error）
与绿（ok）的交叉翻转单独列出。不写 ``data/**``，只可选写一份 JSON 结果。

归一后两支共用同一 ``quotes_end`` 相对锚点，验收线是 **红绿翻转 0 条**。

用法::

    .venv/Scripts/python.exe tools/freshness_parity.py
    .venv/Scripts/python.exe tools/freshness_parity.py --out docs/xxx.json
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.data.quality import freshness  # noqa: E402

DEFAULT_STORE = ROOT / 'data' / 'local_store'


def _class(sev: str) -> str:
    """把任一支的级别折叠成 红/黄/绿/豁免，便于跨口径比较。"""
    return {
        'ok': '绿', 'warn': '黄', 'warning': '黄',
        'critical': '红', 'error': '红', 'known_exception': '豁免',
    }.get(sev, sev)


def _config_paths(config: dict) -> dict[str, str]:
    """配置支：数据集 key → 其权威正式表路径（对齐键）。"""
    out: dict[str, str] = {}
    for raw in config['datasets']:
        source = raw.get('source') or {}
        path = source.get('path') or ''
        if not path and source.get('path_glob'):
            path = str(source['path_glob']).split('_[')[0]
        out[raw['key']] = path
    return out


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description='两套保鲜判据只读对拍')
    parser.add_argument('--store-root', type=Path, default=DEFAULT_STORE)
    parser.add_argument('--out', type=Path, default=None, help='可选：JSON 结果落盘路径')
    args = parser.parse_args(argv)

    root = args.store_root
    today = date.today()
    q_end = freshness.quotes_end(root)
    config = freshness.load_config()

    cfg_results = {r.key: r for r in freshness.check_all(root, config, today)}
    hc_error = None
    try:
        hc_rows = list(freshness.run_check(root, doc_path=None).rows)
    except Exception as exc:  # noqa: BLE001
        hc_rows = []
        hc_error = f'{type(exc).__name__}: {exc}'

    cfg_paths = _config_paths(config)
    comparisons: list[dict] = []

    universe_row = next(
        (r for r in hc_rows if r.kind == freshness.KIND_UNIVERSE), None)

    paired_paths: set[str] = set()
    for row in hc_rows:
        if row.kind == freshness.KIND_UNIVERSE:
            continue
        match = next((k for k, p in cfg_paths.items() if p == row.rel_path), None)
        if match is None:
            comparisons.append({'path': row.rel_path, 'branch': 'only_hardcoded',
                                'hardcoded': row.verdict})
            continue
        paired_paths.add(row.rel_path)
        cfg_sev = cfg_results[match].severity
        comparisons.append({
            'path': row.rel_path, 'branch': 'both', 'config_key': match,
            'config': cfg_sev, 'hardcoded': row.verdict,
            'config_class': _class(cfg_sev), 'hc_class': _class(row.verdict),
            'agree_class': _class(cfg_sev) == _class(row.verdict),
        })
    for key, path in cfg_paths.items():
        if path not in paired_paths:
            comparisons.append({'path': path, 'branch': 'only_config',
                                'config_key': key,
                                'config': cfg_results[key].severity})

    both = [c for c in comparisons if c['branch'] == 'both']
    flips = [c for c in both if not c['agree_class']]
    payload = {
        'generated_at': today.isoformat(),
        'store_root': str(root),
        'quotes_end': q_end.isoformat() if q_end else None,
        'anchor': 'quotes_end',
        'hardcoded_error': hc_error,
        'universe': None if universe_row is None else {
            'verdict': universe_row.verdict, 'gap': universe_row.gap,
            'detail': universe_row.detail},
        'counts': {
            'config_datasets': len(cfg_paths),
            'hardcoded_rows': len(hc_rows),
            'compared_both': len(both),
            'only_config': sum(1 for c in comparisons if c['branch'] == 'only_config'),
            'only_hardcoded': sum(1 for c in comparisons if c['branch'] == 'only_hardcoded'),
            'red_green_flips': len(flips),
        },
        'flips': flips,
        'comparisons': comparisons,
    }
    text = json.dumps(payload, ensure_ascii=False, indent=2)
    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(text + '\n', encoding='utf-8')
    print(text)
    return 0 if not flips else 1


if __name__ == '__main__':
    raise SystemExit(main())
