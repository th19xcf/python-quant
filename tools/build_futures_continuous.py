#!/usr/bin/env python3

r"""从 staging 的单合约日线构建期货正式表：合约清单 / 分合约日线 / 主力连续 / 换月事件。

为什么自建主力连续
==================
第三方「连续合约」（新浪 IF0 等）起点被截断（IF/IC/IH/T/TF 一律从 2017-01-17 起）、
持仓量与结算价列恒为 0、换月时点不透明，无法审计。改为从交易所口径的单合约日线自建：

1. **主力判定**：当日 ``oi`` 最大的合约；并列取 ``vol`` 更大者；再并列取 ``delist_date``
   更近者。``oi == 0`` 的合约（交割日已无持仓）不参选。
2. **换月生效**：T 日收盘确定信号，T+1 日切换，不使用未来信息。
3. **连续价格**：``close_raw`` 保持活跃合约原始收盘价（可与交易所逐值对拍）；
   ``close_adj`` 按逐日收益链式累乘（换月日收益 = 新合约收盘 / 旧合约前收盘 - 1），
   等价于后向比例复权，不含跳空但保留展期损益。
4. **换月事件**：``futures_roll_events.parquet`` 逐次记录两腿收盘与价差，展期成本可算。

产物（``--apply`` 后）
=====================
- ``quotes/futures/<exchange>/<ts_code>.parquet``：单合约原始日线（1:1 来自 staging）
- ``meta/multi_asset/futures_contracts.parquet``：合约清单（含乘数、上市/最后交易日）
- ``meta/multi_asset/futures_continuous.parquet``：主力连续
- ``meta/multi_asset/futures_roll_events.parquet``：换月事件
- ``meta/multi_asset/futures_manifest.json``：行数 / 覆盖 / 口径 / SHA256

用法::

    .venv\\Scripts\\python.exe tools/build_futures_continuous.py            # 只构建 + 校验
    .venv\\Scripts\\python.exe tools/build_futures_continuous.py --apply    # 构建并落正式路径
"""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import date, datetime
from pathlib import Path

import polars as pl

from src.data.atomic_write import write_parquet_atomic

ROOT = Path(__file__).resolve().parents[1]
STORE = ROOT / 'data' / 'local_store'
STAGING = ROOT / '_staging' / 'multi_asset_tier2_20261110'
DAILY = STAGING / 'daily'
OUT_DIR = STORE / 'meta' / 'multi_asset'
QUOTES_DIR = STORE / 'quotes' / 'futures'

#: 品种 → 资产类别
ASSET_CLASS = {
    'IF': 'index_equity', 'IH': 'index_equity',
    'IC': 'index_equity', 'IM': 'index_equity',
    'T': 'govt_bond', 'TF': 'govt_bond', 'TS': 'govt_bond', 'TL': 'govt_bond',
    'AG': 'commodity', 'SC': 'commodity',
}

NUM_COLS = ('pre_close', 'pre_settle', 'open', 'high', 'low', 'close', 'settle',
            'vol', 'amount', 'oi', 'oi_chg')


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, 'rb') as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b''):
            h.update(chunk)
    return h.hexdigest()


def load_contracts() -> pl.DataFrame:
    frames = []
    for ex in ('cffex', 'shfe', 'ine'):
        path = STAGING / f'contracts_{ex}.csv'
        if not path.exists():
            continue
        # 全部按 Utf8 读入：各交易所 CSV 里 multiplier / per_unit 的类型不一致（有的写 1000，
        # 有的写 1000.0），先统一成字符串再显式转型，避免 concat 时 schema 冲突
        frames.append(pl.read_csv(path, infer_schema_length=0))
    if not frames:
        raise SystemExit('没有 contracts_*.csv，先跑 tools/fetch_futures_daily.py')
    df = pl.concat(frames, how='vertical_relaxed').unique(subset=['ts_code'], keep='first')
    return df.with_columns([
        pl.col('list_date').cast(pl.Utf8).str.strptime(pl.Date, '%Y%m%d', strict=False),
        pl.col('delist_date').cast(pl.Utf8).str.strptime(pl.Date, '%Y%m%d', strict=False),
        pl.col('last_ddate').cast(pl.Utf8),
        pl.col('multiplier').cast(pl.Float64, strict=False),
    ])


def load_daily() -> tuple[pl.DataFrame, dict[str, int]]:
    frames = []
    counts: dict[str, int] = {}
    for ex in ('cffex', 'shfe', 'ine'):
        files = sorted((DAILY / ex).glob('*.parquet'))
        counts[ex] = len(files)
        for f in files:
            frames.append(pl.read_parquet(f))
    if not frames:
        raise SystemExit('staging/daily 为空，先跑采集')
    df = pl.concat(frames, how='vertical_relaxed').unique(subset=['ts_code', 'trade_date'])
    df = df.rename({'ts_code': 'contract'}).with_columns(
        pl.col('trade_date').cast(pl.Utf8).str.strptime(pl.Date, '%Y%m%d', strict=False),
        *[pl.col(c).cast(pl.Float64) for c in NUM_COLS],
    )
    return df, counts


def pick_main(daily: pl.DataFrame) -> tuple[pl.DataFrame, int]:
    """按「站立合约只向更晚交割切换」选主力。

    规则（全部可用当日及以前信息判定，无未来函数）::

        候选 = 交割日晚于当前主力、且 oi>0 的合约中 oi 最大者
        切换条件（满足其一）：候选 oi ≥ 当前 oi × 1.05
                              当前主力距最后交易日 ≤ 7 个自然日
        硬约束：绝不回退到交割日更早的合约

    「不回退」是这条规则的关键：纯 oi 取最大会在换月前后反复横跳
    （实测 IC 2018 年出现 IC1804→IC1806→IC1805、IC1809→IC1808→IC1809 这类回退，
    一年多出 3 次无意义换月），回退等价于「重新买入即将交割的合约」，不可执行。

    返回 ``(主力日序列, oi<=0 兜底行数)``。
    """
    rows: list[dict[str, object]] = []
    fallback = 0
    for (variety,), vdf in daily.group_by(['variety'], maintain_order=True):
        vdf = vdf.sort('trade_date')
        by_date: dict[date, list[dict]] = {}
        for rec in vdf.iter_rows(named=True):
            by_date.setdefault(rec['trade_date'], []).append(rec)
        curr: dict | None = None
        for day in sorted(by_date):
            todays = by_date[day]
            if curr is None:
                curr = max(todays, key=lambda r: (r['oi'], r['vol'], -r['delist_date'].toordinal()))
            else:
                cur_row = next((r for r in todays if r['contract'] == curr['contract']), None)
                if cur_row is None:  # 当前合约当日无数据（已摘牌），强制重选
                    curr = max(todays, key=lambda r: (r['oi'], r['vol']))
                else:
                    later = [r for r in todays
                             if r['oi'] > 0 and r['delist_date'] > cur_row['delist_date']]
                    if later:
                        cand = max(later, key=lambda r: r['oi'])
                        days_left = (cur_row['delist_date'] - day).days
                        if (cur_row['oi'] <= 0 or days_left <= 7
                                or cand['oi'] >= cur_row['oi'] * 1.05):
                            curr = cand
            chosen = next((r for r in todays if r['contract'] == curr['contract']), curr)
            if chosen['oi'] <= 0:
                fallback += 1
            rows.append({**chosen})
    main_df = pl.DataFrame(rows).sort(['variety', 'trade_date'])
    return main_df, fallback


def build_continuous(main: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    cont = main.with_columns([
        pl.col('contract').shift(1).over('variety').alias('_prev_contract'),
        pl.col('close').shift(1).over('variety').alias('_prev_close'),
        pl.col('oi').shift(1).over('variety').alias('_prev_oi'),
    ])
    cont = cont.with_columns([
        (pl.col('close') / pl.col('_prev_close') - 1).alias('ret'),
        (pl.col('contract') != pl.col('_prev_contract')).fill_null(False).alias('is_roll'),
    ])
    cont = cont.with_columns(
        (pl.col('close').first().over('variety') * (1 + pl.col('ret').fill_null(0)).cum_prod().over('variety'))
        .alias('close_adj')
    )
    out = cont.select([
        'variety', 'asset_class', 'exchange', 'trade_date', 'contract',
        pl.col('close').alias('close_raw'), pl.col('settle').alias('settle_raw'),
        'close_adj', pl.col('vol').alias('volume'), 'oi', 'is_roll', 'ret',
        pl.col('delist_date').alias('contract_delist_date'),
    ]).sort(['variety', 'trade_date'])

    rolls = (cont.filter(pl.col('is_roll'))
             .select([
                 'variety', 'trade_date',
                 pl.col('_prev_contract').alias('from_contract'), pl.col('contract').alias('to_contract'),
                 pl.col('_prev_close').alias('from_close_prev_day'), pl.col('close').alias('to_close'),
                 pl.col('_prev_oi').alias('from_oi'), pl.col('oi').alias('to_oi'),
                 ((pl.col('close') / pl.col('_prev_close') - 1) * 100).alias('return_pct'),
             ]).sort(['variety', 'trade_date']))
    return out, rolls


def annotate_rolls(rolls: pl.DataFrame, daily: pl.DataFrame) -> pl.DataFrame:
    """补出换月当日两腿同收盘价，分离「当日行情」与「展期价差」。

    ``return_pct`` 是新合约收盘 / 旧合约**前一日**收盘 - 1，含当日行情涨跌；
    ``roll_gap_pct`` 是新旧合约**同日**收盘之差，才是纯展期成本（contango/backwardation）。
    """
    same_day = daily.select([
        pl.col('contract').alias('from_contract'), pl.col('trade_date'),
        pl.col('close').alias('from_close_same_day'),
    ])
    out = rolls.join(same_day, on=['from_contract', 'trade_date'], how='left')
    return out.with_columns(
        ((pl.col('to_close') / pl.col('from_close_same_day') - 1) * 100).alias('roll_gap_pct'))


def validate(cont: pl.DataFrame, rolls: pl.DataFrame, fallback: int) -> list[str]:
    issues: list[str] = []
    dup = cont.group_by(['variety', 'trade_date']).len().filter(pl.col('len') > 1)
    if dup.height:
        issues.append(f'主键重复 {dup.height} 组')
    if cont.filter(pl.col('close_adj').is_null() | pl.col('close_raw').is_null()).height:
        issues.append('存在空收盘价')
    if fallback:
        issues.append(f'主力行 oi<=0 共 {fallback} 行（当日无任何 oi>0 合约）')
    if cont.filter(pl.col('ret').abs() > 0.20).height:
        bad = cont.filter(pl.col('ret').abs() > 0.20)
        issues.append(f'单日收益绝对值 >20% 共 {bad.height} 行（品种：{bad["variety"].unique().to_list()}）')
    # 换月频率：金融期货按季月，年均换月不应超过 13 次
    yearly = (rolls.with_columns(pl.col('trade_date').dt.year().alias('y'))
              .group_by(['variety', 'y']).len().filter(pl.col('len') > 13))
    if yearly.height:
        issues.append(f'换月过于频繁：{yearly.to_dicts()}')
    if rolls.filter(pl.col('roll_gap_pct').is_null()).height:
        issues.append('换月事件缺少同日两腿收盘价，无法算展期价差')
    return issues


def summarize(cont: pl.DataFrame, rolls: pl.DataFrame) -> dict[str, object]:
    per = cont.group_by('variety').agg([
        pl.len().alias('rows'),
        pl.col('trade_date').min().alias('first_date'),
        pl.col('trade_date').max().alias('last_date'),
        pl.col('contract').n_unique().alias('contracts_used'),
    ]).sort('variety')
    roll_stat = rolls.group_by('variety').agg([
        pl.len().alias('rolls'),
        pl.col('roll_gap_pct').abs().mean().round(4).alias('mean_abs_roll_gap_pct'),
        pl.col('roll_gap_pct').abs().max().round(4).alias('max_abs_roll_gap_pct'),
    ]).sort('variety')
    return {
        'per_variety': per.with_columns([
            pl.col('first_date').cast(pl.Utf8), pl.col('last_date').cast(pl.Utf8),
        ]).to_dicts(),
        'roll_stat': roll_stat.to_dicts(),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description='构建期货合约清单 / 主力连续 / 换月事件')
    parser.add_argument('--apply', action='store_true', help='落正式路径')
    args = parser.parse_args(argv)

    contracts = load_contracts()
    daily, file_counts = load_daily()
    print(f'staging 合约文件数: {file_counts}，日线总行数: {daily.height}')

    meta = contracts.select([
        'ts_code', 'fut_code', 'exchange', 'name', 'multiplier',
        'list_date', 'delist_date', 'last_ddate',
    ]).rename({'fut_code': 'variety', 'name': 'contract_name'})
    daily = daily.join(meta.select(['ts_code', 'variety', 'exchange', 'delist_date']),
                       left_on='contract', right_on='ts_code', how='left')
    unmapped = daily.filter(pl.col('variety').is_null())
    if unmapped.height:
        print(f'[WARN] {unmapped.height} 行找不到合约元数据，品种取前缀兜底')
        daily = daily.with_columns(
            pl.when(pl.col('variety').is_null())
            .then(pl.col('contract').str.replace(r'\d.*$', '').str.replace(r'\.(CFX|SHF|INE)$', ''))
            .otherwise(pl.col('variety')).alias('variety'))
    daily = daily.with_columns(
        pl.col('variety').replace_strict(ASSET_CLASS, default='other').alias('asset_class'))

    main_df, fallback = pick_main(daily)
    cont, rolls = build_continuous(main_df)
    rolls = annotate_rolls(rolls, daily)
    issues = validate(cont, rolls, fallback)
    summary = summarize(cont, rolls)
    for item in summary['per_variety']:
        print(f"  {item['variety']:>3} {item['rows']:>5} 行 {item['first_date']}~{item['last_date']} "
              f"合约 {item['contracts_used']}")
    for item in summary['roll_stat']:
        print(f"  换月 {item['variety']:>3}: {item['rolls']:>3} 次，"
              f"|展期价差| 均值 {item['mean_abs_roll_gap_pct']}%，"
              f"最大 {item['max_abs_roll_gap_pct']}%")
    print('校验问题:', issues if issues else '无')
    if issues:
        print('[STOP] 校验未通过，不落盘。')
        return 2

    if not args.apply:
        print('dry-run 完成（加 --apply 落正式路径）')
        return 0

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    hashes: dict[str, str] = {}
    # 1) 分合约日线
    written = 0
    for ex in ('cffex', 'shfe', 'ine'):
        target_dir = QUOTES_DIR / ex
        target_dir.mkdir(parents=True, exist_ok=True)
        for f in sorted((DAILY / ex).glob('*.parquet')):
            dst = target_dir / f.name
            dst.write_bytes(f.read_bytes())
            written += 1
    # 2) 元数据表
    write_parquet_atomic(contracts, OUT_DIR / 'futures_contracts.parquet')
    write_parquet_atomic(cont, OUT_DIR / 'futures_continuous.parquet')
    write_parquet_atomic(rolls, OUT_DIR / 'futures_roll_events.parquet')
    for name in ('futures_contracts.parquet', 'futures_continuous.parquet',
                 'futures_roll_events.parquet'):
        hashes[name] = _sha256(OUT_DIR / name)

    manifest = {
        'dataset': 'futures_cffex_shfe_ine',
        'produced_by': 'tools/build_futures_continuous.py',
        'fetched_by': 'tools/fetch_futures_daily.py',
        'source': 'tushare:fut_daily + fut_basic',
        'built_at': datetime.now().isoformat(timespec='seconds'),
        'staging': str(STAGING.relative_to(ROOT)),
        'contract_files': file_counts,
        'written_contract_files': written,
        'rows': {
            'contracts': contracts.height,
            'daily_total': daily.height,
            'continuous': cont.height,
            'roll_events': rolls.height,
        },
        'files': [
            {'path': f'meta/multi_asset/{name}',
             'rows': frame.height,
             'bytes': (OUT_DIR / name).stat().st_size,
             'sha256': hashes[name]}
            for name, frame in (
                ('futures_contracts.parquet', contracts),
                ('futures_continuous.parquet', cont),
                ('futures_roll_events.parquet', rolls),
            )
        ],
        'coverage': summary['per_variety'],
        'roll_stat': summary['roll_stat'],
        'rule': {
            'main_contract': '当日 oi 最大；并列取 vol 更大；再并列取 delist_date 更近；oi==0 不参选',
            'switch_effective': 'T 日收盘定信号，T+1 生效',
            'close_raw': '活跃合约原始收盘价（可与交易所逐值对拍）',
            'close_adj': '逐日收益链式累乘，换月日收益 = 新合约收盘/旧合约前收盘 - 1',
            'known_bias': '展期成本按两腿价差计入收益；未扣手续费与保证金占用（历史费率无免费源，见 G-041）',
        },
        'price_unit': {'index_equity': '指数点', 'govt_bond': '元/百元面值',
                       'commodity': '元/千克（AG）、元/桶（SC）'},
        'sha256': hashes,
        'verify': 'python tools/build_futures_continuous.py',
    }
    from src.data.manifest_semantics import apply_semantics  # noqa: PLC0415
    apply_semantics(manifest, 'meta/multi_asset/futures_manifest.json')
    (OUT_DIR / 'futures_manifest.json').write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2), encoding='utf-8')
    print(f'落盘完成：{written} 个合约文件 + 3 张元数据表')
    print(json.dumps(hashes, ensure_ascii=False, indent=2))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
