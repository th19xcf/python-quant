#!/usr/bin/env python3

"""从通达信本地码表重建 ``data/local_store/meta/names.json``。

现有 ``names.json`` 的历史来源已核实为通达信 ``T0002/hq_cache`` 下的
``shs.tnf`` / ``szs.tnf`` / ``bjs.tnf`` 三张码表快照：每条记录 360 字节，
代码为偏移 20 起的 6 位 ASCII，名称为偏移 51 起的 16 字节 GBK（超长截断）。
键形如 ``600000.SH``，值为当前简称（不含曾用名、上市状态）。

重建优先级：通达信本地码表（覆盖股票/基金/指数/债券/REITs/退市代码，
共 5 万余键，是唯一能复现现有文件的数据源）；MySQL 基础表仅约 9,700 行，
无法覆盖债券与历史代码，因此只作为报告里的覆盖对照，不用于生成。

用法::

    .venv\\Scripts\\python.exe tools\\build_names_json.py --dry-run
    .venv\\Scripts\\python.exe tools\\build_names_json.py
    .venv\\Scripts\\python.exe tools\\build_names_json.py --strict

默认行为：源中缺失、但基线里已有的代码保留原值（不臆造名称），并在报告里
列为「源缺口」；``--strict`` 则完全以源为准，基线里源缺失的键会被剔除。
写盘前若目标文件已存在且内容不同，先备份到 ``{store_root}/_backup/``。
"""

import argparse
import json
import shutil
import sys
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

from loguru import logger

ROOT = Path(__file__).resolve().parents[1]
#: 回滚快照统一根（仓库根 ``_backup/`` 下按数据层分子目录，DG-11/DG-13 路径口径）
BACKUP_ROOT = ROOT / '_backup'
sys.path.insert(0, str(ROOT))

from src.data.industry import resolve_tdx_cache_dir  # noqa: E402
from src.data.local_store.store import resolve_store_root  # noqa: E402
from src.utils.config import get_config  # noqa: E402

# .tnf 记录布局：30 字节文件头 + N 条 360 字节记录 + 20 字节文件尾
HEADER_SIZE = 30
RECORD_SIZE = 360
CODE_OFFSET = 20
CODE_LENGTH = 6
NAME_OFFSET = 51
NAME_LENGTH = 16
TRAILER_SIZE = 20

MARKET_FILES = (('shs.tnf', 'SH'), ('szs.tnf', 'SZ'), ('bjs.tnf', 'BJ'))


@dataclass
class NameDiff:
    """基线 → 重建结果的逐键差异。"""

    added: list[str] = field(default_factory=list)
    missing: list[str] = field(default_factory=list)
    changed: list[tuple[str, str, str]] = field(default_factory=list)

    @property
    def total(self) -> int:
        return len(self.added) + len(self.missing) + len(self.changed)


def parse_tnf(path: Path) -> dict[str, str]:
    """解析单个 .tnf 码表，返回 ``{6 位代码: 名称}``。

    空名称与非法记录跳过；名称按 16 字节字段截断，尾部被截断的半个 GBK
    字符用 ``errors='ignore'`` 丢弃（与现有 names.json 的生成结果一致）。
    """
    if not path.is_file():
        logger.warning(f'[names] 码表缺失: {path}')
        return {}
    payload = path.read_bytes()
    names: dict[str, str] = {}
    start = HEADER_SIZE
    while start + RECORD_SIZE <= len(payload):
        code = payload[start + CODE_OFFSET:start + CODE_OFFSET + CODE_LENGTH]
        if code.isdigit() and len(code) == CODE_LENGTH:
            raw = payload[start + NAME_OFFSET:start + NAME_OFFSET + NAME_LENGTH]
            name = raw.split(b'\x00', 1)[0].decode('gbk', 'ignore').strip()
            if name:
                names[code.decode()] = name
        start += RECORD_SIZE
    return names


def build_names(cache_dir: Path) -> tuple[dict[str, str], dict[str, int]]:
    """按市场拼接三张码表，返回 ``({ts_code: 名称}, {文件: 条数})``。"""
    merged: dict[str, str] = {}
    counts: dict[str, int] = {}
    for filename, market in MARKET_FILES:
        parsed = parse_tnf(Path(cache_dir) / filename)
        counts[filename] = len(parsed)
        for code, name in parsed.items():
            merged[f'{code}.{market}'] = name
    return merged, counts


def diff_names(baseline: dict[str, str], rebuilt: dict[str, str]) -> NameDiff:
    """逐键比对基线文件与重建结果。"""
    return NameDiff(
        added=sorted(key for key in rebuilt if key not in baseline),
        missing=sorted(key for key in baseline if key not in rebuilt),
        changed=sorted(
            (key, baseline[key], rebuilt[key])
            for key in baseline
            if key in rebuilt and baseline[key] != rebuilt[key]
        ),
    )


def merge_missing(
    baseline: dict[str, str], rebuilt: dict[str, str]
) -> tuple[dict[str, str], list[str]]:
    """源缺失的基线键保留原值，并返回缺口清单（不臆造名称）。"""
    gaps = sorted(key for key in baseline if key not in rebuilt)
    merged = dict(rebuilt)
    for key in gaps:
        merged[key] = baseline[key]
    return merged, gaps


def load_baseline(path: Path) -> dict[str, str]:
    """读取现有 names.json；缺失或损坏时按空基线处理。"""
    if not path.is_file():
        return {}
    try:
        payload = json.loads(path.read_text(encoding='utf-8'))
    except (OSError, ValueError) as exc:
        logger.warning(f'[names] 基线读取失败 {path}: {exc}')
        return {}
    if not isinstance(payload, dict):
        logger.warning(f'[names] 基线不是 JSON 对象: {type(payload).__name__}')
        return {}
    return {str(key): str(value) for key, value in payload.items() if value}


def dump_names(names: dict[str, str]) -> str:
    """生成与现有文件一致的紧凑 JSON 文本（键排序、无空格、无换行）。"""
    return json.dumps(names, ensure_ascii=False, sort_keys=True, separators=(',', ':'))


def write_names(path: Path, names: dict[str, str],
                backup_root: Path | None = None) -> bool:
    """写盘；内容相同时不动文件，覆盖前先备份到根 ``_backup``。

    ``backup_root`` 缺省为模块级 ``BACKUP_ROOT``；单测可传入临时目录隔离。

    Returns:
        True 表示发生了写入，False 表示内容已一致、未改动。
    """
    text = dump_names(names)
    if path.is_file():
        current = path.read_text(encoding='utf-8')
        if current == text:
            logger.info(f'[names] 内容一致，跳过写入: {path}')
            return False
        backup_dir = Path(backup_root) if backup_root else BACKUP_ROOT
        backup_dir.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup = backup_dir / f'names.{stamp}.json'
        index = 1
        while backup.exists():
            backup = backup_dir / f'names.{stamp}_{index}.json'
            index += 1
        shutil.copy2(path, backup)
        logger.info(f'[names] 原文件已备份: {backup}')
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + '.tmp')
    tmp.write_text(text, encoding='utf-8', newline='\n')
    tmp.replace(path)
    logger.info(f'[names] 已写入 {len(names)} 键: {path}')
    return True


def _print_diff(diff: NameDiff, gaps: list[str], *, strict: bool) -> None:
    print(f'新增: {len(diff.added)}')
    for key in diff.added:
        print(f'  + {key}')
    if strict:
        print(f'缺失（基线有、源无，已剔除）: {len(diff.missing)}')
        for key in diff.missing:
            print(f'  - {key}')
    else:
        print(f'缺失（基线有、源无，已保留原值）: {len(gaps)}')
        for key in gaps:
            print(f'  ! {key} -> 保留原值')
    print(f'变更: {len(diff.changed)}')
    for key, old, new in diff.changed:
        print(f'  ~ {key}: {old} -> {new}')


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description='从通达信码表重建 names.json')
    parser.add_argument('--store-root', default=None, help='本地列存根目录')
    parser.add_argument('--tdx-root', default=None, help='通达信 vipdoc 目录')
    parser.add_argument('--cache-dir', default=None, help='直接指定 T0002/hq_cache')
    parser.add_argument('--output', default=None, help='输出路径（默认 meta/names.json）')
    parser.add_argument('--dry-run', action='store_true', help='只比对，不写盘')
    parser.add_argument(
        '--strict', action='store_true',
        help='完全以源为准；基线里源缺失的键剔除（默认保留原值）',
    )
    args = parser.parse_args(argv)

    cfg = get_config()
    store_root = Path(args.store_root) if args.store_root else resolve_store_root(cfg)
    if args.cache_dir:
        cache_dir = Path(args.cache_dir)
    else:
        raw = args.tdx_root or getattr(cfg.data, 'tdx_data_path', '')
        if not raw:
            logger.error('[names] 未配置通达信路径，请传 --tdx-root 或 --cache-dir')
            return 1
        cache_dir = resolve_tdx_cache_dir(raw)

    rebuilt, counts = build_names(cache_dir)
    if not rebuilt:
        logger.error(f'[names] 未解析到任何名称，检查码表目录: {cache_dir}')
        return 1

    output = Path(args.output) if args.output else store_root / 'meta' / 'names.json'
    baseline = load_baseline(output)
    if args.strict:
        final, gaps = dict(rebuilt), sorted(k for k in baseline if k not in rebuilt)
    else:
        final, gaps = merge_missing(baseline, rebuilt)
    diff = diff_names(baseline, final)

    print(f'源目录: {cache_dir}')
    print('码表条数: ' + ', '.join(f'{name}={count}' for name, count in counts.items()))
    print(f'重建键数: {len(rebuilt)}，输出键数: {len(final)}，基线键数: {len(baseline)}')
    _print_diff(diff, gaps, strict=args.strict)
    aligned = diff.total == 0 and not gaps
    print('对齐: ' + ('是（新增/缺失/变更均为 0）' if aligned else '否（见上方差异）'))

    if not args.dry_run:
        write_names(output, final)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
