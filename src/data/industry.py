#!/usr/bin/env python3

"""通达信本地行业分类解析 → 本地快照 + 只读数据 API。

数据源（全部为本地文件，不联网）：

- ``{tdx_root}/T0002/hq_cache/tdxhy.cfg``
  个股 → 通达信行业代码，行格式 ``市场|代码|T代码|||X代码``，
  市场 0/1/2 分别对应深/沪/北。
- ``{tdx_root}/T0002/hq_cache/tdxzs.cfg`` 与 ``tdxzs3.cfg``
  行业代码 → 中文名，行格式 ``名称|板块代码|...|T代码``。

通达信行业编码为树形：``T01`` 一级（TDX 能源）、``T0101`` 二级（煤炭）、
``T010101`` 三级；因此一级取前 3 位、二级取前 5 位。

落盘路径：``{store_root}/meta/industry/industry_YYYYMMDD.parquet``，
按日期分文件保存，便于将来做点位回溯；查询取不晚于 ``as_of`` 的最新一份。
"""

import argparse
from datetime import date, datetime
from pathlib import Path
from typing import Any

import polars as pl
from loguru import logger

from src.data.atomic_write import write_parquet_atomic

UNKNOWN = 'unknown'
SOURCE_TDX = 'tdx_hq_cache'
SOURCE_MISSING = 'missing'
_MARKETS = {'0': 'SZ', '1': 'SH', '2': 'BJ'}
_NAME_FILES = ('tdxzs.cfg', 'tdxzs3.cfg')

_SNAPSHOT_CACHE: dict[str, tuple[int, pl.DataFrame]] = {}
#: 目录扫描结果缓存：``{目录: (目录 mtime_ns, [(日期, 路径)])}``
_FILES_CACHE: dict[str, tuple[int, list[tuple[date, Path]]]] = {}


def resolve_tdx_cache_dir(tdx_data_path: str | Path) -> Path:
    """由 ``vipdoc`` 路径推出 ``T0002/hq_cache`` 目录。"""
    root = Path(str(tdx_data_path))
    return root.parent / 'T0002' / 'hq_cache'


def _as_date(value: Any) -> date | None:
    if value is None or value == '':
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value)
    for fmt in ('%Y-%m-%d', '%Y%m%d'):
        try:
            return datetime.strptime(text[:10] if '-' in text else text, fmt).date()
        except ValueError:
            continue
    return None


def parse_tdxhy(path: Path) -> dict[str, str]:
    """解析 ``tdxhy.cfg``，返回 ``{ts_code: 行业代码}``。"""
    if not path.is_file():
        logger.warning(f'[industry] 行业映射文件缺失: {path}')
        return {}
    mapping: dict[str, str] = {}
    for line in path.read_bytes().decode('gbk', 'replace').splitlines():
        parts = [item.strip() for item in line.split('|')]
        if len(parts) < 3 or not parts[2]:
            continue
        market = _MARKETS.get(parts[0])
        code = parts[1]
        if not market or not (code.isdigit() and len(code) == 6):
            continue
        mapping[f'{code}.{market}'] = parts[2]
    return mapping


def parse_industry_names(paths: list[Path] | tuple[Path, ...]) -> dict[str, str]:
    """解析 ``tdxzs`` 类文件，返回 ``{行业代码: 中文名}``（后读覆盖先读）。"""
    names: dict[str, str] = {}
    for path in paths:
        if not path.is_file():
            continue
        for line in path.read_bytes().decode('gbk', 'replace').splitlines():
            parts = [item.strip() for item in line.split('|')]
            if len(parts) < 6 or not parts[5].startswith('T'):
                continue
            names[parts[5]] = parts[0]
    return names


def _levels(industry_code: str, names: dict[str, str]) -> tuple[str, str]:
    """行业代码 → (一级名, 二级名)，名称缺失时显式返回 unknown。"""
    if not industry_code.startswith('T'):
        return UNKNOWN, UNKNOWN
    l1 = names.get(industry_code[:3], UNKNOWN)
    l2 = names.get(industry_code[:5], UNKNOWN)
    return l1, l2


def snapshot_path(store_root: Path | str, as_of: date) -> Path:
    """单日行业快照路径。"""
    return Path(store_root) / 'meta' / 'industry' \
        / f'industry_{as_of:%Y%m%d}.parquet'


def build_snapshot(
    store_root: Path | str, cache_dir: Path | str, as_of: Any = None, *,
    codes: list[str] | None = None,
) -> pl.DataFrame:
    """用通达信本地 cfg 构建当日行业快照并落盘（只覆盖个股）。

    Args:
        codes: 显式标的列表（测试用）；缺省读取本地列存 stock 全量。
    """
    from src.data.local_store.store import LocalQuoteStore  # noqa: PLC0415

    snapshot_date = _as_date(as_of) or date.today()
    cache = Path(cache_dir)
    mapping = parse_tdxhy(cache / 'tdxhy.cfg')
    names = parse_industry_names([cache / name for name in _NAME_FILES])
    if codes is None:
        codes = LocalQuoteStore(Path(store_root)).codes('stock')

    rows: list[dict[str, Any]] = []
    for code in codes:
        industry_code = mapping.get(code)
        if industry_code:
            l1, l2 = _levels(industry_code, names)
            rows.append({
                'code': code, 'industry_l1': l1, 'industry_l2': l2,
                'industry_code': industry_code, 'source': SOURCE_TDX,
            })
        else:
            rows.append({
                'code': code, 'industry_l1': UNKNOWN, 'industry_l2': UNKNOWN,
                'industry_code': UNKNOWN, 'source': SOURCE_MISSING,
            })
    frame = pl.DataFrame(rows).with_columns(pl.lit(snapshot_date).alias('as_of'))
    path = snapshot_path(store_root, snapshot_date)
    path.parent.mkdir(parents=True, exist_ok=True)
    write_parquet_atomic(frame, path)
    logger.info(f'[industry] 行业快照已落盘: {path}（{frame.height} 个标的）')
    return frame


def _snapshot_files(store_root: Path | str) -> list[tuple[date, Path]]:
    """列出 ``meta/industry`` 下的日快照文件，按目录 mtime 缓存。

    调用方是**按标的**查询（``RiskContext._industry_map`` 每日数千次），
    每次都 glob 一遍目录会占掉推荐单日成本的两成以上。目录内新增/删除文件
    必然改变目录 mtime，据此失效即可；文件内容变化由 ``load_snapshot`` 的
    文件级 mtime 校验负责，两者互不替代。
    """
    dir_path = Path(store_root) / 'meta' / 'industry'
    if not dir_path.is_dir():
        return []
    try:
        mtime = dir_path.stat().st_mtime_ns
    except OSError:
        return []
    cached = _FILES_CACHE.get(str(dir_path))
    if cached is not None and cached[0] == mtime:
        return cached[1]
    files: list[tuple[date, Path]] = []
    for path in dir_path.glob('industry_*.parquet'):
        day = _as_date(path.stem.split('_', 1)[1])
        if day is not None:
            files.append((day, path))
    files.sort()
    _FILES_CACHE[str(dir_path)] = (mtime, files)
    return files


def load_snapshot(
    store_root: Path | str, as_of: Any = None,
) -> tuple[pl.DataFrame | None, date | None]:
    """取不晚于 ``as_of`` 的最新行业快照；无可用快照返回 ``(None, None)``。"""
    target = _as_date(as_of) or date.today()
    files = [item for item in _snapshot_files(store_root) if item[0] <= target]
    if not files:
        return None, None
    day, path = files[-1]
    stat = path.stat()
    cached = _SNAPSHOT_CACHE.get(str(path))
    if cached is not None and cached[0] == stat.st_mtime_ns:
        return cached[1], day
    frame = pl.read_parquet(path)
    _SNAPSHOT_CACHE[str(path)] = (stat.st_mtime_ns, frame)
    return frame, day


def get_industry(
    code: str, as_of: Any = None, *, store_root: Path | str | None = None,
) -> dict[str, Any]:
    """查询标的行业（点位只读，不做历史外推）。

    Returns:
        ``{code, industry_l1, industry_l2, industry_code, source, as_of, status}``
        ``status`` 为 ``known`` / ``unknown``；无快照、快照未覆盖或名称缺失
        一律返回 ``unknown``，调用方不得把 unknown 当成某个具体行业。
    """
    from src.data.services.st_status_service import normalize_ts_code  # noqa: PLC0415

    result: dict[str, Any] = {
        'code': normalize_ts_code(code) or str(code).upper(),
        'industry_l1': UNKNOWN, 'industry_l2': UNKNOWN,
        'industry_code': UNKNOWN, 'source': SOURCE_MISSING,
        'as_of': None, 'status': 'unknown',
    }
    if store_root is None:
        from src.data.local_store.store import resolve_store_root  # noqa: PLC0415
        from src.utils.config import get_config  # noqa: PLC0415

        store_root = resolve_store_root(get_config())
    frame, day = load_snapshot(store_root, as_of)
    if frame is None or day is None:
        result['reason'] = 'no_snapshot'
        return result
    result['as_of'] = day.isoformat()
    rows = frame.filter(pl.col('code') == result['code']).to_dicts()
    if not rows:
        result['reason'] = 'code_not_covered'
        return result
    row = rows[0]
    result.update({
        'industry_l1': row['industry_l1'], 'industry_l2': row['industry_l2'],
        'industry_code': row['industry_code'], 'source': row['source'],
    })
    if row['source'] == SOURCE_MISSING:
        result['reason'] = 'tdx_cfg_missing'
        return result
    result['status'] = 'known'
    return result


def coverage_report(
    frame: pl.DataFrame, codes_by_kind: dict[str, list[str]],
) -> dict[str, Any]:
    """按资产类型统计行业覆盖率（基金/指数无行业口径，恒为 0）。"""
    known = set(frame.filter(
        pl.col('source') != SOURCE_MISSING)['code'].to_list())
    report: dict[str, Any] = {}
    for kind, codes in codes_by_kind.items():
        hit = [code for code in codes if code in known]
        total = len(codes)
        report[kind] = {
            'total': total, 'covered': len(hit),
            'coverage': f'{len(hit) / total:.2%}' if total else '0.00%',
        }
    return report


def _main(args: argparse.Namespace) -> int:
    from src.data.local_store.store import LocalQuoteStore, resolve_store_root  # noqa: PLC0415
    from src.utils.config import get_config  # noqa: PLC0415

    config = get_config()
    store_root = Path(args.store_root) if args.store_root else resolve_store_root(config)
    if args.command == 'refresh':
        cache = Path(args.cache_dir) if args.cache_dir \
            else resolve_tdx_cache_dir(config.data.tdx_data_path)
        frame = build_snapshot(store_root, cache, args.as_of)
        store = LocalQuoteStore(store_root)
        codes = {kind: store.codes(kind) for kind in ('stock', 'fund', 'index')}
        report = coverage_report(frame, codes)
        for kind, item in report.items():
            logger.info(f'[industry] {kind}: {item["covered"]}/{item["total"]} '
                        f'({item["coverage"]})')
        return 0

    snapshot, day = load_snapshot(store_root, args.as_of)
    if snapshot is None:
        logger.warning('[industry] 尚无行业快照，请先执行 refresh')
        return 1
    logger.info(f'[industry] 快照日期 {day}，标的 {snapshot.height}，'
                f'行业名缺失 {snapshot.filter(pl.col("industry_l2") == UNKNOWN).height}')
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description='通达信行业分类快照')
    parser.add_argument('command', choices=['refresh', 'stats'])
    parser.add_argument('--as-of', default=None, help='快照日期 YYYY-MM-DD')
    parser.add_argument('--cache-dir', default=None, help='hq_cache 目录')
    parser.add_argument('--store-root', default=None, help='本地列存根目录')
    return _main(parser.parse_args())


if __name__ == '__main__':
    raise SystemExit(main())
