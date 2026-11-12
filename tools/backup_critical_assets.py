#!/usr/bin/env python3

r"""不可重建数据资产备份 + SHA256 校验。

项目里有一批资产**代码无法重建**（见 ``docs/主机迁移本地数据清单.md``）：
名称码表、分红快照、ST/行业历史日快照、推荐状态，以及 P0-1 新增的
契约巡检基线。它们全部落在被 ``.gitignore`` 排除的目录里，此前只依赖
手工 ``robocopy`` 全量镜像，存在单点丢失风险。

本工具把它们按原相对路径复制到一个独立目录，并生成 ``manifest.json``
（路径 / 字节数 / sha256 / mtime）；``--verify`` 逐项复算哈希。
只做复制与校验，**绝不删除任何文件**。

典型用法::

    python tools/backup_critical_assets.py --dest G:/git/python-quant-critical
    python tools/backup_critical_assets.py --dest G:/git/python-quant-critical --verify
    python tools/backup_critical_assets.py --dry-run   # 只看清单与体积

生产环境请把 ``--dest`` 指向**另一块物理盘或另一台机器**；默认的
``backups/critical_assets`` 只是同盘副本，用于本地快速回滚。
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

from loguru import logger

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DEST = PROJECT_ROOT / 'backups' / 'critical_assets'

#: 单个文件（相对项目根）
#:
#: 2026-11-07 扩充：此前只备份了 stock_dividend 一个因子快照，其余 5 个
#: 权威快照丢失后只能靠 _backup/ 里的零散时间戳副本救回。这里把全部
#: 「代码无法重建 / 重建代价高」的资产显式列全。
#: 刻意排除：factors/_bench_last.parquet（基准临时产物）、
#: meta/financials/batch_*.parquet（采集中间分片，可由 collect_financials 重建）、
#: quotes/**（可由通达信 .day 重建，体积过大）。
CRITICAL_FILES: tuple[str, ...] = (
    # 权威因子与事件快照（读侧 overlay 的唯一依据）
    'data/local_store/factors/stock_adj_factor.parquet',
    'data/local_store/factors/stock_dividend.parquet',
    'data/local_store/factors/fund_adj_factor.parquet',
    'data/local_store/factors/fund_dividend.parquet',
    'data/local_store/factors/fund_share_change.parquet',
    'data/local_store/factors/stock_turnover.parquet',
    'data/local_store/factors/meta.json',
    'data/local_store/factors/fund_meta.json',
    'data/local_store/factors/covered_codes.txt',
    'data/local_store/factors/fund_covered_codes.txt',
    # 元数据与码表
    'data/local_store/meta/names.json',
    # 名称历史（PIT 唯一权威表）+ 覆盖度声明；tushare 重建后需再跑
    # tools/fix_name_history_end_dates.py 与 tools/infer_post_g_segments.py
    'data/local_store/meta/name_history.parquet',
    'data/local_store/meta/name_history_coverage.json',
    'data/local_store/meta/daily_index.parquet',
    'data/local_store/meta/state.json',
    # 股票基础表：2026-11-10 前只有读取方、没有写入方（内容由一次性脚本拼出），
    # 丢失无法重建；现由 tools/build_stock_basic.py 从本地行情重建，仍需备份
    'data/local_store/meta/stock_basic.parquet',
    # 基金/ETF 基础表：含逐只取证后的名称、上市日、退市状态（commit ccd96ea 8f5f1a5），
    # 库值只会被合并式 **补齐空值**，取证值不可由 MySQL 反向重建
    'data/local_store/meta/fund_basic.parquet',
    'data/local_store/meta/fund_basic.manifest.json',
    # 指数基础表：2026-11-10 由 1,333 扩到 1,335 行（新增 000985.SH / 000922.SH），
    # 表内 base_point 全空、无重建脚本可还原，丢失只能靠本备份
    'data/local_store/meta/index_basic.parquet',
    'data/local_store/meta/factors/stock_shares.parquet',
    'data/local_store/meta/factors/stock_shares.meta.json',
    'data/local_store/meta/anomalies/quality_exemptions.csv',
    'data/local_store/meta/events/coverage.json',
    # 财务三表与日频估值（最终表，不含采集分片）
    'data/local_store/meta/financials/income.parquet',
    'data/local_store/meta/financials/balance.parquet',
    'data/local_store/meta/financials/cashflow.parquet',
    'data/local_store/meta/financials/valuation_daily.parquet',
    # 宏观/资金输入：仓库内无驻留采集脚本，只能由外部接口（中债/交易所/东财）
    # 一次性抓取得到，丢失不可本地重建；`source` 列分段口径见同目录 manifest.json
    'data/local_store/meta/macro/cn_treasury_yield_10y.parquet',
    'data/local_store/meta/macro/cn_market_margin_daily.parquet',
    'data/local_store/meta/macro/manifest.json',
    # 推荐状态
    'reports/advice/state/long.json',
    'reports/advice/state/mid.json',
    'reports/advice/state/short.json',
    # 通达信码表缓存（2026-11-12 补）：`meta/names.json` 的唯一重建来源
    # （tools/build_names_json.py:3-12）。`.tnf` 无法从网络重下，且
    # `EXTERNAL_ASSET_KEYS` 指向的 D:/zxzq 在本机不可达，项目内 `external/`
    # 这一份就是唯一现存副本，必须进不可重建清单。
    'external/tdx_hq_cache/shs.tnf',
    'external/tdx_hq_cache/szs.tnf',
    'external/tdx_hq_cache/bjs.tnf',
    # 公告原始采集件（2026-11-12 补，DG-32 入层三问 ③ 尾句）：`research/events/data/`
    # 整体不是正式数据路径，但这两份是外部源（巨潮/东财）的一次性原始抓取，
    # 重抓的字节不可复现；真实消费者在 tools 侧（backfill_events.py:205/210 读
    # backfill/ 分片、update_events.py:188/231 读日更分片，两者最终写
    # data/local_store/meta/events/events.parquet）。零 src/ 消费者、不进正式层，
    # 第二份留存由 G:\git\python-quant 镜像承担。
    'research/events/data/announcements_raw.parquet',
    'research/events/data/yjyg_raw.parquet',
)

#: 整目录（相对项目根），保留目录内相对结构
CRITICAL_DIRS: tuple[str, ...] = (
    'data/local_store/meta/st_status',
    'data/local_store/meta/industry',
    'data/local_store/meta/clean',
    'data/local_store/meta/universe',
    'data/local_store/meta/anomalies',
    'data/local_store/meta/events',
    'data/local_store/meta/suspension',
    # 指数成分权威表（index_weight_monthly + 派生的 membership_intervals）
    'data/local_store/meta/index_members',
    'data/local_store/meta/industry_pit',
    'data/local_store/meta/st',
    'data/local_store/factors/rebuild',
    # 2026-11-07 数据审计资产：手写对拍脚本、历史契约巡检快照、基线、
    # 交付清单与核验报告、导入前事件表回滚副本。重跑只能得到当前状态，
    # 历史证据不可重建；此前误放 tmp/，现归位到 reports/ 并纳入备份契约。
    'reports/data_audit_2026-11-07',
    # 数据新鲜度与归档审计留痕（2026-11-12 补）：内容可由脚本重算，
    # 但重算要跑全链路脚本且历史值会变，留一份成本仅 0.6 MB。
    'artifacts',
    # 公告原始采集分片（2026-11-12 补）：83 个月度分片 + 日更分片 + 36 个补数分片，
    # 合计 122 文件 / 8,978,792 B，外部源不可重放（源端会变、翻页截断、响应为空）；
    # 补数分片的生成脚本已不在仓库，尤其不可重建。目录条目按整目录收录，会连带
    # 3 个可重算的 `_merged_for_import.parquet` 与 1 个 `_audit.json`（合计约 1.09 MB），
    # 属收录范围的包含关系，不单列。该树零 src/ 消费者，不是正式路径，见
    # docs/数据权威路径清单.md「research/events/data/ 整体不是正式数据路径」条。
    'research/events/data/backfill',
    'research/events/data/backfill_supplement',
)

#: 项目外的不可重建源数据：(相对通达信安装目录, 备份目录内相对键)。
#: ``names.json`` 只能由这三份 .tnf 码表（tools/build_names_json.py）重建，
#: 此前 D/E/F 三处均无副本，主机层面重建链不完整。用 QUANT_TDX_ROOT 覆盖
#: 默认安装目录；备份键统一放 external/ 前缀，避免与项目内相对路径混淆。
_TDX_ROOT = Path(os.environ.get('QUANT_TDX_ROOT', 'D:/zxzq'))
EXTERNAL_ASSET_KEYS: tuple[tuple[str, str], ...] = (
    ('T0002/hq_cache/shs.tnf', 'external/tdx_hq_cache/shs.tnf'),
    ('T0002/hq_cache/szs.tnf', 'external/tdx_hq_cache/szs.tnf'),
    ('T0002/hq_cache/bjs.tnf', 'external/tdx_hq_cache/bjs.tnf'),
)

MANIFEST_NAME = 'manifest.json'


def external_assets() -> list[tuple[Path, str]]:
    """展开项目外源数据清单，返回 (源绝对路径, 备份键)。"""
    return [(_TDX_ROOT / source, key) for source, key in EXTERNAL_ASSET_KEYS]


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open('rb') as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b''):
            digest.update(chunk)
    return digest.hexdigest()


def collect_assets(root: Path) -> list[tuple[Path, str]]:
    """展开清单，返回 (源绝对路径, 备份键)，按备份键稳定排序。"""
    found: dict[str, Path] = {}
    for relative in CRITICAL_FILES:
        path = root / relative
        if path.is_file():
            found[relative] = path
        else:
            logger.warning(f'不可重建资产缺失: {relative}')
    for relative in CRITICAL_DIRS:
        directory = root / relative
        if not directory.is_dir():
            logger.warning(f'不可重建资产目录缺失: {relative}')
            continue
        for path in directory.rglob('*'):
            if path.is_file():
                found[path.relative_to(root).as_posix()] = path
    for path, key in external_assets():
        if path.is_file():
            found[key] = path
        else:
            logger.warning(f'外部不可重建资产缺失: {path}')
    return sorted(((path, key) for key, path in found.items()),
                  key=lambda item: item[1])


def _entry(path: Path, key: str) -> dict[str, Any]:
    stat = path.stat()
    return {
        'relative_path': key,
        'source_path': str(path),
        'bytes': stat.st_size,
        'sha256': _sha256(path),
        'mtime': datetime.fromtimestamp(stat.st_mtime).isoformat(timespec='seconds'),
    }


def backup(root: Path, dest: Path) -> dict[str, Any]:
    """复制资产并写 manifest，返回清单载荷。"""
    assets = collect_assets(root)
    if not assets:
        raise RuntimeError('没有找到任何不可重建资产，拒绝生成空备份')
    dest.mkdir(parents=True, exist_ok=True)
    entries: list[dict[str, Any]] = []
    for path, key in assets:
        target = dest / key
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, target)
        entries.append(_entry(path, key))
    payload = {
        'generated_at': datetime.now().isoformat(timespec='seconds'),
        'source_root': str(root),
        'file_count': len(entries),
        'total_bytes': sum(item['bytes'] for item in entries),
        'files': entries,
    }
    (dest / MANIFEST_NAME).write_text(
        json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8'
    )
    return payload


def verify(root: Path, dest: Path) -> tuple[int, int, list[str]]:
    """按 manifest 复算哈希；返回 (通过数, 问题数, 问题描述)。"""
    manifest = dest / MANIFEST_NAME
    if not manifest.is_file():
        return 0, 1, [f'manifest 缺失: {manifest}']
    payload = json.loads(manifest.read_text(encoding='utf-8'))
    problems: list[str] = []
    passed = 0
    for item in payload.get('files', []):
        relative = str(item['relative_path'])
        # 新 manifest 带 source_path（支持项目外源数据）；旧 manifest 回退到项目内路径
        raw_source = item.get('source_path')
        source = Path(raw_source) if raw_source else root / relative
        target = dest / relative
        if not target.is_file():
            problems.append(f'备份文件缺失: {relative}')
            continue
        actual = _sha256(target)
        if actual != item['sha256']:
            problems.append(f'备份哈希不符: {relative}')
            continue
        if source.is_file() and _sha256(source) != actual:
            problems.append(f'源与备份不一致（源已更新，需重跑备份）: {relative}')
            continue
        passed += 1
    return passed, len(problems), problems


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='不可重建数据资产备份与校验')
    parser.add_argument('--dest', default=str(DEFAULT_DEST),
                        help='备份目标目录（建议指向另一块盘）')
    parser.add_argument('--source-root', default=str(PROJECT_ROOT))
    parser.add_argument('--verify', action='store_true', help='只校验，不复制')
    parser.add_argument('--dry-run', action='store_true', help='只列出清单与体积')
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    root = Path(args.source_root).resolve()
    dest = Path(args.dest).resolve()

    if args.verify:
        passed, failed, problems = verify(root, dest)
        logger.info(f'校验完成: 通过 {passed} 项，问题 {failed} 项')
        for problem in problems:
            logger.error(problem)
        return 1 if failed else 0

    assets = collect_assets(root)
    total = sum(path.stat().st_size for path, _ in assets)
    logger.info(f'不可重建资产 {len(assets)} 个文件，合计 {total / 1e6:.2f} MB → {dest}')
    if args.dry_run:
        for _, key in assets:
            logger.info(f'  {key}')
        logger.info('dry-run：未复制')
        return 0

    payload = backup(root, dest)
    logger.info(f'备份完成: {payload["file_count"]} 个文件，manifest: {dest / MANIFEST_NAME}')
    passed, failed, problems = verify(root, dest)
    for problem in problems:
        logger.warning(problem)
    return 1 if failed else 0


if __name__ == '__main__':
    sys.exit(main())
