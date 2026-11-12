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

落盘路径：``{store_root}/meta/industry/industry_history.parquet``，单表按
``as_of`` 纵向累积（路径不带日期，DG-4）；查询取不晚于 ``as_of`` 的最大一层。
"""

import argparse
from datetime import date, datetime
from pathlib import Path
from typing import Any, TypedDict

import polars as pl
from loguru import logger

from src.data.atomic_write import write_parquet_atomic
from src.data.snapshot_manifest import write_snapshot_manifest

UNKNOWN = 'unknown'
SOURCE_TDX = 'tdx_hq_cache'
SOURCE_MISSING = 'missing'
_MARKETS = {'0': 'SZ', '1': 'SH', '2': 'BJ'}
_NAME_FILES = ('tdxzs.cfg', 'tdxzs3.cfg')

_SNAPSHOT_CACHE: dict[str, tuple[int, pl.DataFrame]] = {}
#: 正式路径的文件名：单表按 ``as_of`` 纵向累积，路径不带日期（DG-4）
HISTORY_NAME = 'industry_history.parquet'


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


def snapshot_path(store_root: Path | str) -> Path:
    """行业快照正式路径（唯一一份，取哪一天由 ``as_of`` 列决定）。"""
    return Path(store_root) / 'meta' / 'industry' / HISTORY_NAME


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
    path = snapshot_path(store_root)
    path.parent.mkdir(parents=True, exist_ok=True)
    _upsert_history(frame, path)
    logger.info(f'[industry] 行业快照已并入 {path}（as_of={snapshot_date}，{frame.height} 个标的）')
    write_manifest(store_root)
    return frame


def _upsert_history(frame: pl.DataFrame, path: Path) -> None:
    """把当日快照按 ``as_of`` 覆盖式并入单表（同一 as_of 重跑不重复累积）。"""
    as_of = frame['as_of'][0]
    if path.is_file():
        existing = pl.read_parquet(path).filter(pl.col('as_of') != as_of)
        frame = pl.concat([existing, frame], how='vertical')
    write_parquet_atomic(frame.sort(['as_of', 'code']), path)


#: 目录级 manifest 的登记内容（``files`` / 汇总由 :func:`write_manifest` 实测填充）。
class _ManifestPayload(TypedDict):
    """``write_snapshot_manifest`` 的关键字参数集（逐键声明，避免 ``**`` 展开被放宽）。"""

    prefix: str
    dataset: str
    semantics: str
    source: str
    authority: str
    path_pattern: str
    consumers: list[str]
    produced_by: str


_MANIFEST_PAYLOAD: _ManifestPayload = {
    'prefix': 'industry_',
    'dataset': 'industry_history',
    'semantics': (
        '单表日期快照序列：每行是「某标的某日的通达信行业分类」，'
        '按 as_of 纵向累积、只增不重写；正式路径不带日期。'
        '不是行业 PIT，历史行业归属仍须走 meta/industry_pit/ 区间表'
    ),
    'source': '通达信本地 cfg（tdxhy.cfg 分类 + tdxzs.cfg/tdxzs3.cfg 名称）解析的当日行业分类',
    'authority': (
        'docs/数据权威路径清单.md 登记的 meta/industry/industry_history.parquet；'
        '选择函数 src/data/industry.py::load_snapshot（取 as_of 不晚于目标日的'
        '最大 as_of 那一层）'
    ),
    'path_pattern': 'meta/industry/industry_history.parquet',
    'consumers': [
        'src/data/industry.py::load_snapshot / snapshot_days（as_of 不晚于目标日）',
        'src/advice/risk_filters.py 推荐/风控逐日行业分散度上限',
        'src/data/services/finance_service.py get_industry',
        'src/data/quality/extended_builders.py::_industry_snapshots 逐层抽取 '
        'meta/industry_pit/industry_pit.parquet',
        'tools/research/build_analysis_monthly.py 研究侧同一选择函数',
        'tools/single_window_diag.py --industry 默认取 as_of == 2026-11-04 那一层',
    ],
    'produced_by': 'src/data/industry.py::build_snapshot（按 as_of 覆盖同日后合并落盘）',
}


def write_manifest(store_root: Path | str) -> Path:
    """重算 ``meta/industry/manifest.json``（只写 manifest，不重建任何快照）。"""
    root = Path(store_root)
    return write_snapshot_manifest(root, root / 'meta' / 'industry', **_MANIFEST_PAYLOAD)


def _read_history(store_root: Path | str) -> pl.DataFrame | None:
    """读整张 history 表并按文件 mtime 缓存；表不存在返回 ``None``。

    调用方是**按标的**查询（``RiskContext._industry_map`` 每日数千次），
    每次都整表读盘会占掉推荐单日成本的两成以上；history 是唯一一份文件，
    文件内容一变 mtime 必变，据此失效即可，不需要再扫目录。
    """
    path = snapshot_path(store_root)
    try:
        stat = path.stat()
    except OSError:
        return None
    cached = _SNAPSHOT_CACHE.get(str(path))
    if cached is not None and cached[0] == stat.st_mtime_ns:
        return cached[1]
    frame = pl.read_parquet(path)
    _SNAPSHOT_CACHE[str(path)] = (stat.st_mtime_ns, frame)
    return frame


def snapshot_days(store_root: Path | str) -> list[date]:
    """表内已有的快照日（升序）；无表或全空返回空列表。"""
    frame = _read_history(store_root)
    if frame is None or frame.height == 0:
        return []
    return sorted(day for day in frame['as_of'].unique().to_list() if day is not None)


def load_snapshot(
    store_root: Path | str, as_of: Any = None,
) -> tuple[pl.DataFrame | None, date | None]:
    """取 ``as_of`` 不晚于目标日的最大一层行业快照；无可用层返回 ``(None, None)``。"""
    target = _as_date(as_of) or date.today()
    frame = _read_history(store_root)
    if frame is None or frame.height == 0:
        return None, None
    days = [day for day in frame.filter(pl.col('as_of') <= target)['as_of']
            .unique().to_list() if day is not None]
    if not days:
        return None, None
    day = max(days)
    return frame.filter(pl.col('as_of') == day), day


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
