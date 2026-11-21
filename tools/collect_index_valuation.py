"""指数估值表 ``meta/index_valuation``：tushare 官方成分权重 × 个股估值重建。

口径与 ``tools/dividend_official_probe.py``（研究侧官方口径）一致：

- 成分权重取 ``index_weight`` 月末 PIT 快照（000015.SH / 000922.CSI / 000300.SH）；
- 个股估值取 ``daily_basic`` 同一交易日的 ``pe`` / ``pe_ttm`` / ``pb`` / ``dv_ratio``；
- 指数 PE/PB = 成分按权重加权的**调和均值**（整体法，仅纳入有限正值）；
- 指数股息率 = 成分 ``dv_ratio`` 的权重算术均值（与官方口径一致，不是调和）。

之所以不用 ``index_dailybasic``：该接口对 000015.SH / 000922.CSI 返回 0 行，
且字段中本来就没有股息率；000300.SH 虽可取，但与本表混用会形成两套口径。

token 只从环境变量 ``TS_TOKEN`` 读取；本地开发环境允许回落到
``.secrets/tushare_token.txt``（该目录已在 .gitignore），任何 token 都不写入代码或数据。

用法::

    set TS_TOKEN=...                      # PowerShell: $env:TS_TOKEN='...'
    .venv\\Scripts\\python.exe tools/collect_index_valuation.py --apply
    .venv\\Scripts\\python.exe tools/collect_index_valuation.py --gate
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import time
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np
import polars as pl
from loguru import logger

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.data.atomic_write import write_parquet_atomic  # noqa: E402

STORE = ROOT / 'data/local_store'
OUT_DIR = STORE / 'meta/index_valuation'
TABLE = OUT_DIR / 'index_valuation.parquet'
MANIFEST = OUT_DIR / 'manifest.json'
CACHE = ROOT / '_staging/index_valuation'
TOKEN_FILE = ROOT / '.secrets/tushare_token.txt'

#: 表内指数代码 -> (index_weight 的 index_code, 起始年)。
#: 000922 的官方权重代码是 .CSI；000922.SH 在 tushare index_daily 中也不存在。
INDEXES: Dict[str, tuple[str, int]] = {
    '000015.SH': ('000015.SH', 2012),
    '000922.CSI': ('000922.CSI', 2008),
    '000300.SH': ('000300.SH', 2005),
}
SOURCE = 'tushare.index_weight+daily_basic'
BATCH_SLEEP = 0.4          # <=150 次/分钟
PE_BOUNDS = (0.1, 200.0)
MAX_GAP_DAYS = 70          # 月末序列相邻两条的最大间隔（含春节/长假冗余）

COLUMNS = ['index_code', 'trade_date', 'pe', 'pe_ttm', 'pb', 'dividend_yield',
           'pe_cov', 'n', 'source']


class CollectError(RuntimeError):
    """采集或口径校验失败。"""


def _token() -> str:
    token = (os.environ.get('TS_TOKEN') or '').strip()
    if token:
        return token
    if TOKEN_FILE.is_file():
        token = TOKEN_FILE.read_text(encoding='utf-8').strip()
    if not token:
        raise CollectError('TS_TOKEN 未设置，且 .secrets/tushare_token.txt 不存在或为空')
    return token


def _pro() -> Any:
    import tushare as ts  # noqa: PLC0415

    return ts.pro_api(_token())


def _retry(fn, *args, attempts: int = 3, **kwargs):
    last: Exception | None = None
    for attempt in range(attempts):
        try:
            return fn(*args, **kwargs)
        except Exception as exc:  # noqa: BLE001
            last = exc
            time.sleep(2.0 * (attempt + 1))
    raise CollectError(f'tushare 调用连续失败：{last}')


# ---------------------------------------------------------------------------
# 原始数据抓取（带本地缓存与断点续跑）
# ---------------------------------------------------------------------------

def _weight_cache(code: str, year: int) -> Path:
    return CACHE / 'weight' / f'{code}_{year}.parquet'


def _daily_cache(day: date) -> Path:
    return CACHE / 'daily_basic' / f'{day:%Y%m%d}.parquet'


def fetch_weights(pro: Any, code: str, start_year: int, end_year: int) -> pl.DataFrame:
    frames: List[pl.DataFrame] = []
    for year in range(start_year, end_year + 1):
        path = _weight_cache(code, year)
        if path.is_file():
            frame = pl.read_parquet(path)
        else:
            raw = _retry(pro.index_weight, index_code=code,
                         start_date=f'{year}0101', end_date=f'{year}1231')
            time.sleep(BATCH_SLEEP)
            if raw is None or raw.shape[0] == 0:
                logger.warning('index_weight 空返回：{} {}', code, year)
                continue
            frame = (pl.from_pandas(raw)
                     .with_columns(pl.col('trade_date').str.strptime(pl.Date, '%Y%m%d'))
                     .select(['index_code', 'con_code',
                              pl.col('trade_date').alias('trade_date'),
                              pl.col('weight').cast(pl.Float64)]))
            path.parent.mkdir(parents=True, exist_ok=True)
            write_parquet_atomic(frame, path)
        frames.append(frame)
    if not frames:
        return pl.DataFrame(schema={'index_code': pl.Utf8, 'con_code': pl.Utf8,
                                    'trade_date': pl.Date, 'weight': pl.Float64})
    return (pl.concat(frames)
            .unique(subset=['index_code', 'con_code', 'trade_date'], keep='last')
            .sort(['trade_date', 'con_code']))


def fetch_daily(pro: Any, days: List[date]) -> pl.DataFrame:
    frames: List[pl.DataFrame] = []
    for i, day in enumerate(days, 1):
        path = _daily_cache(day)
        if path.is_file():
            cached = pl.read_parquet(path)
            # 研究缓存只保留红利成分股（约 350 行/日），不能用于 000300 等宽基；
            # 全市场截面约 5000 行，低于 1000 行视为不完整并重取覆盖。
            if cached.height >= 1000:
                frames.append(cached)
                continue
        raw = _retry(pro.daily_basic, trade_date=f'{day:%Y%m%d}',
                     fields='ts_code,trade_date,pe,pe_ttm,pb,dv_ratio')
        time.sleep(BATCH_SLEEP)
        if raw is None or raw.shape[0] == 0:
            logger.warning('daily_basic 空返回：{}', day)
            continue
        frame = (pl.from_pandas(raw)
                 .with_columns(pl.col('trade_date').str.strptime(pl.Date, '%Y%m%d'))
                 .select(['ts_code', 'trade_date', 'pe', 'pe_ttm', 'pb', 'dv_ratio']))
        path.parent.mkdir(parents=True, exist_ok=True)
        write_parquet_atomic(frame, path)
        frames.append(frame)
        if i % 20 == 0:
            logger.info('daily_basic 抓取进度 {}/{}', i, len(days))
    if not frames:
        return pl.DataFrame(schema={'ts_code': pl.Utf8, 'trade_date': pl.Date,
                                    'pe': pl.Float64, 'pe_ttm': pl.Float64,
                                    'pb': pl.Float64, 'dv_ratio': pl.Float64})
    return pl.concat(frames).unique(subset=['ts_code', 'trade_date'], keep='last')


# ---------------------------------------------------------------------------
# 重建估值
# ---------------------------------------------------------------------------

def _harmonic(values: np.ndarray, weights: np.ndarray) -> float | None:
    mask = np.isfinite(values) & (values > 0) & np.isfinite(weights)
    if not mask.any() or weights[mask].sum() <= 0:
        return None
    return float(weights[mask].sum() / (weights[mask] / values[mask]).sum())


def build(index_code: str, weights: pl.DataFrame, daily: pl.DataFrame) -> pl.DataFrame:
    rows: List[Dict[str, Any]] = []
    for (day,), group in weights.group_by(['trade_date']):
        joined = group.join(daily.filter(pl.col('trade_date') == day),
                            left_on='con_code', right_on='ts_code', how='inner')
        if joined.height < 10:
            continue
        w = joined['weight'].to_numpy()
        w = w / w.sum()
        pe = joined['pe'].to_numpy()
        pe_ttm = joined['pe_ttm'].to_numpy()
        pb = joined['pb'].to_numpy()
        dv = joined['dv_ratio'].to_numpy()
        dv_mask = np.isfinite(dv)
        rows.append({
            'index_code': index_code,
            'trade_date': day,
            'pe': _harmonic(pe, w),
            'pe_ttm': _harmonic(pe_ttm, w),
            'pb': _harmonic(pb, w),
            'dividend_yield': (float((w[dv_mask] * dv[dv_mask]).sum()
                                     / w[dv_mask].sum())
                               if dv_mask.any() else None),
            'pe_cov': round(float(w[np.isfinite(pe_ttm) & (pe_ttm > 0)].sum()), 4),
            'n': joined.height,
            'source': SOURCE,
        })
    if not rows:
        raise CollectError(f'{index_code} 重建后 0 行，检查 index_weight / daily_basic 覆盖')
    return pl.DataFrame(rows, infer_schema_length=None, schema_overrides={
        'pe': pl.Float64, 'pe_ttm': pl.Float64, 'pb': pl.Float64,
        'dividend_yield': pl.Float64, 'pe_cov': pl.Float64, 'n': pl.Int64,
    }).select(COLUMNS)


def _existing() -> pl.DataFrame:
    if not TABLE.is_file():
        return pl.DataFrame(schema=dict.fromkeys(COLUMNS, pl.Utf8))
    return pl.read_parquet(TABLE).with_columns(pl.col('trade_date').cast(pl.Date))


def collect(full: bool, end_year: int) -> pl.DataFrame:
    pro = _pro()
    existing = _existing()
    frames: List[pl.DataFrame] = []
    for index_code, (weight_code, start_year) in INDEXES.items():
        have = existing.filter(pl.col('index_code') == index_code)
        first_year = start_year
        if not full and have.height:
            last = have['trade_date'].max()
            first_year = max(start_year, last.year - 1)
        logger.info('{}：权重年份 {}~{}', index_code, first_year, end_year)
        weights = fetch_weights(pro, weight_code, first_year, end_year)
        if weights.height == 0:
            raise CollectError(f'{index_code} 无 index_weight 数据')
        known = set(have['trade_date'].to_list())
        need = sorted({d for d in weights['trade_date'].unique().to_list()
                       if d not in known and d <= date.today()})
        if not need:
            frames.append(have)
            continue
        daily = fetch_daily(pro, need)
        frames.append(build(index_code, weights.filter(pl.col('trade_date').is_in(need)),
                            daily))
    fresh = pl.concat([f for f in frames if f.height])
    combined = pl.concat([existing, fresh]) if 'index_code' in existing.columns and existing.height \
        else fresh
    return (combined.unique(subset=['index_code', 'trade_date'], keep='last')
            .sort(['index_code', 'trade_date'])
            .with_columns(pl.col('trade_date').cast(pl.Date)))


# ---------------------------------------------------------------------------
# 落盘、manifest 与门禁
# ---------------------------------------------------------------------------

def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open('rb') as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b''):
            digest.update(chunk)
    return digest.hexdigest()


def write_table(frame: pl.DataFrame) -> None:
    from src.data.atomic_write import write_parquet_atomic  # noqa: PLC0415

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    write_parquet_atomic(frame, TABLE)
    stats = {}
    for code, group in frame.group_by('index_code'):
        stats[code[0] if isinstance(code, tuple) else code] = {
            'rows': group.height,
            'start': str(group['trade_date'].min()),
            'end': str(group['trade_date'].max()),
            'latest_pe_ttm': group.sort('trade_date')['pe_ttm'][-1],
            'latest_pb': group.sort('trade_date')['pb'][-1],
        }
    payload = {
        'dir': 'meta/index_valuation',
        'path': 'data/local_store/meta/index_valuation/index_valuation.parquet',
        'rows': frame.height,
        'columns': frame.columns,
        'sha256': _sha256(TABLE),
        'built_at': datetime.now().isoformat(timespec='seconds'),
        'produced_by': 'tools/collect_index_valuation.py',
        'dataset': '指数月末估值：一行 = 指数 × 月末 PIT 快照。',
        'semantics': ('分量权重取 tushare index_weight 月末 PIT，个股取同日 daily_basic；'
                      'pe/pe_ttm/pb 为权重调和均值（整体法，仅有限正值），'
                      'dividend_yield 为权重算术均值。'),
        'source': 'tushare index_weight + daily_basic；token 仅从 TS_TOKEN 读取，不落盘。',
        'authority': ('与 tools/dividend_official_probe.py 研究口径一致；'
                      'index_dailybasic 对 000015/000922 无数据且不含股息率，故不采用。'),
        'indexes': stats,
        'files': [{'path': 'index_valuation.parquet', 'rows': frame.height,
                   'bytes': TABLE.stat().st_size, 'sha256': _sha256(TABLE)}],
    }
    from src.data.atomic_write import write_text_atomic  # noqa: PLC0415
    write_text_atomic(json.dumps(payload, ensure_ascii=False, indent=2) + '\n', MANIFEST)


def gate(frame: pl.DataFrame) -> int:
    problems: List[str] = []
    today = date.today()
    for code in INDEXES:
        sub = frame.filter(pl.col('index_code') == code).sort('trade_date')
        if sub.height == 0:
            problems.append(f'{code}: 无数据')
            continue
        lag = (today - sub['trade_date'].max()).days
        if lag > MAX_GAP_DAYS:
            problems.append(f'{code}: 末日 {sub["trade_date"].max()} 滞后 {lag} 天')
        for column in ('pe_ttm', 'pb'):
            bad = sub.filter((pl.col(column) < PE_BOUNDS[0])
                             | (pl.col(column) > PE_BOUNDS[1])).height
            if bad:
                problems.append(f'{code}.{column}: {bad} 行越界 {PE_BOUNDS}')
        dates = sub['trade_date'].to_list()
        gaps = [(b - a).days for a, b in zip(dates, dates[1:], strict=False)]
        if gaps and max(gaps) > MAX_GAP_DAYS:
            problems.append(f'{code}: 连续缺失，最大间隔 {max(gaps)} 天')
    if problems:
        for item in problems:
            logger.error('门禁失败：{}', item)
        return 1
    logger.info('门禁通过：{} 行，{} 只指数',
                frame.height, frame['index_code'].n_unique())
    return 0


def main(argv: List[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description='指数估值表采集/门禁')
    parser.add_argument('--apply', action='store_true', help='抓取并写入正式表')
    parser.add_argument('--full', action='store_true', help='忽略已有表，从头回补')
    parser.add_argument('--gate', action='store_true', help='只跑门禁，不抓取')
    parser.add_argument('--end-year', type=int, default=date.today().year)
    args = parser.parse_args(argv)
    if args.gate:
        if not TABLE.is_file():
            logger.error('缺少 {}，先运行 --apply', TABLE)
            return 1
        return gate(_existing())
    if not args.apply:
        parser.print_help()
        return 0
    frame = collect(full=args.full, end_year=args.end_year)
    write_table(frame)
    logger.info('写入 {} 行 -> {}', frame.height, TABLE)
    return gate(frame)


if __name__ == '__main__':
    raise SystemExit(main())
