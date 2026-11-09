"""实体去重校验：确认 `load_entity_index()` 归并后不丢行、不重复计数。

用途
----
后续任何「拼横截面 / 建 universe / 批量回测」的脚本，都应在加载代码列表后调用
`assert_dedupe_consistent()`，把「旧码被新码完整覆盖」这一前提变成可执行断言，
避免 000043/001914、300114/302132 这类同实体双计。

命令行
------
    python tools/check_entity_dedupe.py [--store data/local_store] [--pair 000043.SZ:001914.SZ]

退出码非 0 表示存在丢行或重复计数风险。
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.data.quality.code_migration import EntityIndex, load_entity_index  # noqa: E402


def _date_span(store: Path, code: str) -> tuple[pd.Timestamp | None, pd.Timestamp | None]:
    path = store / 'quotes' / 'stock' / code.rsplit('.', 1)[-1].lower() / f'{code}.parquet'
    if not path.is_file():
        return None, None
    s = pd.read_parquet(path, columns=['date'])['date']
    return pd.to_datetime(s.min()), pd.to_datetime(s.max())


def _compare_pair(store: Path, old: str, new: str) -> dict:
    po = store / 'quotes' / 'stock' / old.rsplit('.', 1)[-1].lower() / f'{old}.parquet'
    pn = store / 'quotes' / 'stock' / new.rsplit('.', 1)[-1].lower() / f'{new}.parquet'
    if not po.is_file() or not pn.is_file():
        return {'old': old, 'new': new, 'status': 'skipped_missing_file'}
    a = pd.read_parquet(po, columns=['date', 'close']).rename(columns={'close': 'c_old'})
    b = pd.read_parquet(pn, columns=['date', 'close']).rename(columns={'close': 'c_new'})
    a['date'], b['date'] = pd.to_datetime(a['date']), pd.to_datetime(b['date'])
    m = a.merge(b, on='date', how='outer', indicator=True)
    both = m[m['_merge'] == 'both']
    rel = ((both['c_old'] - both['c_new']).abs() / both['c_new'].abs().clip(lower=1e-9))
    return {
        'old': old,
        'new': new,
        'status': 'compared',
        'old_rows': len(a),
        'new_rows': len(b),
        'overlap_rows': len(both),
        'only_old_rows': int((m['_merge'] == 'left_only').sum()),
        'only_new_rows': int((m['_merge'] == 'right_only').sum()),
        'close_max_rel_dev': float(rel.max()) if len(both) else None,
        'old_start': a['date'].min(),
        'old_end': a['date'].max(),
        'new_start': b['date'].min(),
        'new_end': b['date'].max(),
    }


def _only_old_dates(store: Path, old: str, new: str) -> list[str]:
    """旧码有、新码没有的交易日（YYYY-MM-DD）。"""
    po = store / 'quotes' / 'stock' / old.rsplit('.', 1)[-1].lower() / f'{old}.parquet'
    pn = store / 'quotes' / 'stock' / new.rsplit('.', 1)[-1].lower() / f'{new}.parquet'
    a = pd.to_datetime(pd.read_parquet(po, columns=['date'])['date'])
    b = pd.to_datetime(pd.read_parquet(pn, columns=['date'])['date'])
    return [d.strftime('%Y-%m-%d') for d in sorted(set(a) - set(b))]


def _implied_placeholder(store: Path) -> set[tuple[str, str]]:
    """已登记的停牌占位 bar（②产出的 implied_suspension 表）。"""
    path = store / 'meta' / 'suspension' / 'implied_suspension.parquet'
    if not path.is_file():
        return set()
    d = pd.read_parquet(path, columns=['ts_code', 'trade_date'])
    return set(zip(d['ts_code'].astype(str),
                   pd.to_datetime(d['trade_date']).dt.strftime('%Y-%m-%d'), strict=False))


def assert_dedupe_consistent(
    store_root: Path | str | None = None,
    codes: list[str] | None = None,
    *,
    verbose: bool = True,
) -> dict:
    """校验实体去重一致性；返回统计字典，发现丢行时抛 AssertionError。

    若传入 `codes`（拟用于横截面的代码列表），会额外检查其中是否残留被覆盖的旧码。
    「旧码独有行」会区分有效行与已登记的停牌占位 bar：只有有效行丢失才算失败。
    """
    store = Path(store_root) if store_root is not None else REPO_ROOT / 'data' / 'local_store'
    index: EntityIndex = load_entity_index(store, refresh=True)
    placeholder = _implied_placeholder(store)
    rows = [_compare_pair(store, old, new) for old, new in index.canonical.items()]
    df = pd.DataFrame(rows)

    problems: list[str] = []
    lossless = 0
    lost_placeholder = 0
    for r in rows:
        if r['status'] != 'compared':
            continue
        if r['old'] not in index.superseded:
            continue
        if r['only_old_rows'] > 0:
            for d in _only_old_dates(store, r['old'], r['new']):
                if (r['old'], d) in placeholder:
                    lost_placeholder += 1
                else:
                    problems.append(f'{r["old"]}→{r["new"]}: 丢失有效行 {d}')
        if (r['new_start'] > r['old_start']) or (r['new_end'] < r['old_end']):
            problems.append(f'{r["old"]}→{r["new"]}: 新码区间未包住旧码，归并会丢行')
        lossless += 1

    if codes is not None:
        leftover = [c for c in codes if index.is_superseded(c)]
        if leftover:
            problems.append(f'传入代码列表仍含 {len(leftover)} 个被覆盖旧码: {leftover[:5]}')
        dup = len(codes) - len(set(codes))
        if dup:
            problems.append(f'传入代码列表有 {dup} 个重复码')

    if verbose:
        print(f'股票代码 {len(rows)} 对映射，其中 {len(index.superseded)} 对被判定为可去重')
        if len(df) and (df['status'] == 'compared').any():
            d = df[df['status'] == 'compared']
            print(f'重叠段最大收盘价相对偏差 {d["close_max_rel_dev"].max():.3g}；'
                  f'旧码独有行合计 {int(d["only_old_rows"].sum())}'
                  f'（其中已登记停牌占位 bar {lost_placeholder} 行，有效行 '
                  f'{int(d["only_old_rows"].sum()) - lost_placeholder} 行）')
        for note in index.notes:
            print(f'  索引注记: {note}')
        print('PASS：归并无丢行、无重复计数' if not problems else 'FAIL：' + '；'.join(problems))

    stats = {'pairs': len(rows), 'superseded': len(index.superseded),
             'lossless_pairs': lossless, 'lost_placeholder_rows': lost_placeholder,
             'problems': problems}
    if problems:
        raise AssertionError('；'.join(problems))
    return stats


def main() -> int:
    ap = argparse.ArgumentParser(description='实体去重一致性校验')
    ap.add_argument('--store', default=None, help='local_store 根目录')
    ap.add_argument('--pair', default=None, help='只查一对，格式 OLD:NEW')
    args = ap.parse_args()
    store = Path(args.store) if args.store else REPO_ROOT / 'data' / 'local_store'
    if args.pair:
        old, new = args.pair.split(':')
        print(pd.DataFrame([_compare_pair(store, old, new)]).to_string(index=False))
        idx = load_entity_index(store, refresh=True)
        print(f'superseded={old in idx.superseded}  notes={list(idx.notes)}')
        return 0
    assert_dedupe_consistent(store)
    return 0


if __name__ == '__main__':
    sys.exit(main())
