#!/usr/bin/env python3

"""每日推荐归档：一个交易日一份 JSON，可幂等重写。

设计约束：

- **不动 ``state/*.json``**：归档只读 ``AdviceResult``，不参与 L1/L2 学习链路；
- **一天一份**：路径 ``{daily_dir}/{YYYYMMDD}.json``，同一日重跑直接原子覆盖，
  不会产生重复记录（幂等）；
- **推荐价口径写死**：``ref_price`` 为推荐日**未复权收盘价**（人工下单看到的价格），
  成交口径为**次日开盘**（见 ``execution_basis``），两者都写进文件，避免事后误读；
- 归档目录默认 ``reports/advice/daily``，与 ``latest.json``（最新快照，覆盖写）
  和 ``state/``（最新持仓，覆盖写）分离。

字段：``trade_date / tier / rank / ts_code / direction / action / score /
score_percentile / target_weight_pct / shares / ref_price / ref_price_basis /
validity_days / confidence / industry``。
"""

from __future__ import annotations

import json
import os
from datetime import date, datetime
from pathlib import Path
from typing import Any

from src.advice.models import (
    DIRECTION_AVOID,
    DIRECTION_BUY,
    DIRECTION_HOLD,
    DIRECTION_SELL,
    AdviceResult,
)

#: ``ref_price`` 的口径：推荐日收盘价（未复权）
REF_PRICE_BASIS = 'close'
#: 推荐动作对应的成交口径：信号日 T 收盘生成，T+1 开盘执行
EXECUTION_BASIS = 'next_open'

ACTION_LABELS = {
    DIRECTION_BUY: '买入',
    DIRECTION_SELL: '调出',
    DIRECTION_HOLD: '持有',
    DIRECTION_AVOID: 'AVOID',
}


def default_daily_dir(project_root: Path | None = None) -> Path:
    """默认归档目录 ``reports/advice/daily``。"""
    root = project_root or Path(__file__).resolve().parents[2]
    return root / 'reports' / 'advice' / 'daily'


def snapshot_path(daily_dir: Path | str, trade_date: date) -> Path:
    """单日归档路径 ``{daily_dir}/{YYYYMMDD}.json``。"""
    return Path(daily_dir) / f'{trade_date:%Y%m%d}.json'


def build_payload(result: AdviceResult,
                  generated_at: datetime | None = None) -> dict[str, Any]:
    """把一次推荐结果压成按日归档结构（每个档位的行按分数降序带 rank）。"""
    payload: dict[str, Any] = {
        'trade_date': result.trade_date.isoformat(),
        'generated_at': (generated_at or datetime.now()).isoformat(timespec='seconds'),
        'equity': round(float(result.equity), 2),
        'ref_price_basis': REF_PRICE_BASIS,
        'execution_basis': EXECUTION_BASIS,
        'notes': [
            'ref_price 为推荐日未复权收盘价（raw_close），非复权价',
            '动作在 T+1 开盘执行；涨跌停/停牌当日不可成交',
        ],
        'tiers': [],
    }
    for key, advice in result.tiers.items():
        rows = sorted(advice.rows,
                      key=lambda rec: rec.score_percentile, reverse=True)
        items = []
        for rank, rec in enumerate(rows, start=1):
            items.append({
                'trade_date': result.trade_date.isoformat(),
                'rank': rank,
                'ts_code': rec.ts_code,
                'name': rec.name,
                'direction': rec.direction,
                'action': ACTION_LABELS.get(rec.direction, rec.direction),
                'score': round(float(rec.score), 4),
                'score_percentile': round(float(rec.score_percentile), 4),
                'top_factors': ' | '.join(
                    f'{name} {value:+.2f}' for name, value in rec.top_factors[:3]),
                'risk_notes': '；'.join(rec.risk_notes),
                'target_weight_pct': round(float(rec.target_weight) * 100, 2),
                'shares': int(rec.shares),
                'ref_price': round(float(rec.ref_price), 3),
                'ref_price_basis': REF_PRICE_BASIS,
                'validity_days': int(rec.validity_days),
                'confidence': round(float(rec.confidence), 3),
                'industry': rec.industry,
            })
        payload['tiers'].append({
            'tier': key,
            'label': advice.label,
            'counts': _counts(rows),
            'items': items,
        })
    return payload


def _counts(rows: list[Any]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for rec in rows:
        counts[rec.direction] = counts.get(rec.direction, 0) + 1
    return counts


def save_daily_snapshot(result: AdviceResult, daily_dir: Path | str,
                        generated_at: datetime | None = None) -> Path:
    """原子写入当日归档；同日重跑覆盖同一文件（幂等）。"""
    path = snapshot_path(daily_dir, result.trade_date)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = build_payload(result, generated_at)
    tmp = path.with_suffix('.json.tmp')
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2),
                   encoding='utf-8')
    os.replace(tmp, path)
    return path


def load_daily_snapshot(daily_dir: Path | str, trade_date: date) -> dict[str, Any] | None:
    """读取指定交易日的归档；不存在返回 ``None``。"""
    path = snapshot_path(daily_dir, trade_date)
    if not path.is_file():
        return None
    try:
        return json.loads(path.read_text(encoding='utf-8'))
    except (OSError, ValueError):
        return None


def list_daily_snapshots(daily_dir: Path | str) -> list[dict[str, Any]]:
    """列出归档目录里已有的交易日（升序），附带各档条数。"""
    directory = Path(daily_dir)
    if not directory.is_dir():
        return []
    entries: list[dict[str, Any]] = []
    for path in sorted(directory.glob('[0-9]' * 8 + '.json')):
        try:
            payload = json.loads(path.read_text(encoding='utf-8'))
        except (OSError, ValueError):
            continue
        counts = {item['tier']: sum(item.get('counts', {}).values())
                  for item in payload.get('tiers', [])}
        entries.append({
            'trade_date': payload.get('trade_date', path.stem),
            'path': str(path),
            'items': counts,
        })
    return entries
