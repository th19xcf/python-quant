"""逐位等价 golden 门禁：改动撮合/风控/审计字段实现前后必须保持结果一致。

背景：网页模型第三轮评审把「golden 快照 + 差分 harness」列为 R1，理由是
压缩 P0 固定开销、RiskGate 预分组或 `_finite_float` 向量化都会触及撮合热路径，
必须先有一道与耗时无关的等价门槛。模型随后又对首版提出三条结构性意见：
① 基线应存**逐字段 SHA + 字段存在性**，而非单一整包 hash（合法新增字段不应
   逼迫重生成基线，而删除既有审计字段必须报错）；
② 集合类型必须归一，避免 ``PYTHONHASHSEED`` 让门禁偶发红绿；
③ 必须有**阴性对照**（mutation），证明 gate 真的能变红。
本文件按这三条落地，并补齐其点名的审计字段与停牌/回退场景。

本测试**只读**，不修改 ``src/``。生成/更新基线（仅在确认结果正确后）：
    ./.venv/Scripts/python.exe tests/test_golden_snapshot.py save
"""
from __future__ import annotations

import hashlib
import json
import sys
import warnings
from datetime import date, datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import polars as pl
import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

GOLDEN = Path(__file__).resolve().parent / "golden" / "baseline.json"
SEED = 20261106
SCENARIOS: tuple[dict[str, Any], ...] = (
    {"name": "small_2x60", "ncodes": 2, "days": 60, "kind": "plain"},
    {"name": "large_8x250", "ncodes": 8, "days": 250, "kind": "plain"},
    {"name": "tiered_4x120", "ncodes": 4, "days": 120, "kind": "tiered"},
    {"name": "suspended_8x120", "ncodes": 8, "days": 120, "kind": "suspended"},
    {"name": "tiered_partial_4x120", "ncodes": 4, "days": 120,
     "kind": "tiered_partial"},
)
#: 逐字段枚举：新增字段不打断门禁，但这些字段一旦缺失或变化即失败。
RESULT_KEYS = (
    "trades", "expired_orders", "rejected_orders", "equity_curve",
    "final_equity", "performance", "cost_inputs", "risk_gate",
    "data_quality", "risk_summary", "positions", "st_unknown_bars",
    "st_unknown_seen", "overwritten_orders", "filtered_signals",
    "slow_path_strategies",
    # A1 / 3b 新增审计字段：风控兜底装配痕迹、跌停受阻记录与作废原因计数
    "blocked_orders", "blocked_reason_counts", "expired_reason_counts",
    "input_contract",
)


def _jsonable(value: Any) -> Any:
    """归一为可稳定哈希的结构；不做 round，避免掩盖逐位差异。

    集合显式排序，消除 ``PYTHONHASHSEED`` 对迭代顺序的影响。
    """
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.floating):
        return float(value)
    if isinstance(value, np.bool_):
        return bool(value)
    if isinstance(value, dict):
        return {str(k): _jsonable(v)
                for k, v in sorted(value.items(), key=lambda kv: str(kv[0]))}
    if isinstance(value, (list, tuple)):
        return [_jsonable(v) for v in value]
    if isinstance(value, (set, frozenset)):
        return sorted(str(v) for v in value)
    if hasattr(value, "item"):
        return _jsonable(value.item())
    return value


def _encode(payload: Any) -> bytes:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"), allow_nan=False).encode("utf-8")


def _sha(payload: Any) -> str:
    return hashlib.sha256(_encode(payload)).hexdigest()


def _panel(scene: dict[str, Any], seed: int) -> dict[str, pl.DataFrame]:
    rng = np.random.default_rng(seed)
    days, kind = scene["days"], scene["kind"]
    dates = pd.date_range("2021-01-04", periods=days, freq="B")
    panel: dict[str, pl.DataFrame] = {}
    for i in range(scene["ncodes"]):
        close = np.maximum(10.0 + np.cumsum(rng.normal(0, 0.08, days)), 1.0)
        open_ = close * (1 + rng.normal(0, 0.004, days))
        high = np.maximum(open_, close) * (1 + np.abs(rng.normal(0, 0.003, days)))
        low = np.minimum(open_, close) * (1 - np.abs(rng.normal(0, 0.003, days)))
        volume = np.full(days, 1_000_000.0)
        # 多标的同日停牌：检验 suspended 集合与「上一个数据行」语义（P2）
        if kind == "suspended" and i < 5:
            volume[20:30] = 0.0
        frame = pl.DataFrame({
            "date": dates, "open": open_, "high": high, "low": low,
            "close": close, "volume": volume,
        })
        if kind in {"tiered", "tiered_partial"}:
            adv = [None if (kind == "tiered_partial" and j % 7 == 0)
                   else 1.0e8 for j in range(days)]
            frame = frame.with_columns([
                pl.Series("adv20", adv, dtype=pl.Float64),
                pl.Series("total_mv_yi", np.linspace(50.0, 5000.0, days)),
                pl.Series("category", ["stock"] * days),
            ])
        panel[f"{600000 + i:06d}.SH"] = frame
    return panel


def _capture() -> dict[str, Any]:
    from src.backtest.engine.portfolio_backtest_engine import PortfolioBacktestEngine
    from src.backtest.strategies.ma_strategy import MAStrategy

    out: dict[str, Any] = {}
    for idx, scene in enumerate(SCENARIOS):
        data = _panel(scene, SEED + idx)
        engine = PortfolioBacktestEngine(data)
        engine.set_strategies({code: MAStrategy(5, 20) for code in data})
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            result = engine.run_backtest(initial_capital=1_000_000.0)
        payload = {k: _jsonable(result[k]) for k in RESULT_KEYS if k in result}
        # warning 序列按原序哈希：元素与顺序都是 P3/P6 契约的一部分
        payload["warnings"] = [
            f"{w.category.__name__}:{str(w.message)}" for w in caught
        ]
        out[scene["name"]] = payload
    return out


def _field_digests(payload: dict[str, Any]) -> dict[str, dict[str, str]]:
    return {name: {k: _sha(v) for k, v in sorted(fields.items())}
            for name, fields in sorted(payload.items())}


def _baseline() -> dict[str, Any]:
    assert GOLDEN.exists(), f"基线文件不存在，请先执行 save 模式：{GOLDEN}"
    return json.loads(GOLDEN.read_text(encoding="utf-8"))


def _gate_report(expected: dict[str, dict[str, str]],
                 actual: dict[str, dict[str, str]]) -> str:
    """只校验基线枚举过的字段：新增字段放行，缺失/变更一律报出。"""
    problems: list[str] = []
    for scene, fields in sorted(expected.items()):
        if scene not in actual:
            problems.append(f"缺少场景 {scene}")
            continue
        for field, digest in sorted(fields.items()):
            if field not in actual[scene]:
                problems.append(f"审计字段被删除 {scene}.{field}（P5）")
            elif actual[scene][field] != digest:
                problems.append(
                    f"字段变更 {scene}.{field}: expected={digest} "
                    f"actual={actual[scene][field]}")
    return "\n".join(problems)


def _gate_would_fail(payload: dict[str, Any]) -> bool:
    return bool(_gate_report(_baseline()["fields"], _field_digests(payload)))


def test_golden_snapshot_matches() -> None:
    """任何改变成交/拒单/审计字段的撮合改动都会在这里失败。"""
    report = _gate_report(_baseline()["fields"], _field_digests(_capture()))
    assert not report, f"golden 快照不一致：\n{report}"


def test_golden_is_deterministic() -> None:
    """同一 commit 连跑两次字段摘要必须一致（不依赖哈希随机化）。"""
    assert _field_digests(_capture()) == _field_digests(_capture())


def test_jsonable_normalizes_set_order() -> None:
    """集合归一：避免 PYTHONHASHSEED 让门禁偶发红绿。"""
    assert _jsonable({"c", "a", "b"}) == ["a", "b", "c"]
    assert (_sha(_jsonable({"x": {"b", "a"}}))
            == _sha(_jsonable({"x": {"a", "b"}})))


def test_golden_detects_one_bp_cost_change(monkeypatch) -> None:
    """真实注入：买入成本 +1bp 必须让门禁变红。"""
    from src.backtest.costs.cost_model import CostModel

    original = CostModel.get_buy_cost_rate

    def bumped(self, *args, **kwargs):
        return original(self, *args, **kwargs) + 1.0e-4

    monkeypatch.setattr(CostModel, "get_buy_cost_rate", bumped)
    assert _gate_would_fail(_capture()), "1bp 成本变化未被门禁发现"


@pytest.mark.parametrize("mutation", ["warning_order", "drop_field", "null_fold"])
def test_golden_negative_controls(mutation: str) -> None:
    """阴性对照：三类语义漂移都必须让 gate 变红，否则门禁等于摆设。"""
    payload = _capture()
    scene = SCENARIOS[2]["name"]  # tiered_4x120
    fields = dict(payload[scene])
    if mutation == "warning_order":
        # 构造两条不同 warning 并交换顺序，专测 P3 的顺序敏感性
        ordered = ["RiskDisabledWarning:first", "UserWarning:second"]
        fields["warnings"] = ordered
        payload[scene] = fields
        assert _gate_would_fail(payload)
        fields["warnings"] = list(reversed(ordered))
        payload[scene] = fields
        assert _gate_would_fail(payload)
    elif mutation == "drop_field":
        # 删掉一个审计字段，专测 P5 的字段存在性断言
        fields.pop("cost_inputs", None)
        payload[scene] = fields
        assert _gate_would_fail(payload)
    else:
        # 模拟 M2 把 None 折成 0.0：null 表示法变化必须被发现
        assert _sha({"x": None}) != _sha({"x": 0.0})
        fields["cost_inputs"] = {**fields.get("cost_inputs", {}),
                                 "_null_probe": 0.0}
        payload[scene] = fields
        assert _gate_would_fail(payload)


if __name__ == "__main__":
    if len(sys.argv) != 2 or sys.argv[1] != "save":
        raise SystemExit("用法: python tests/test_golden_snapshot.py save")
    payload = _capture()
    base = {
        "seed": SEED,
        "env": {"python": sys.version.split()[0],
                "pandas": pd.__version__, "polars": pl.__version__},
        "note": "逐字段 SHA；新增字段不打断，既有字段缺失或变更即失败",
        "fields": _field_digests(payload),
    }
    GOLDEN.write_text(json.dumps(base, ensure_ascii=False, sort_keys=True,
                                 indent=1), encoding="utf-8")
    lines = [f"FIELD_COUNT={sum(len(v) for v in base['fields'].values())}"]
    for name, fields in payload.items():
        lines.append(f"  {name}: fields={len(fields)} "
                     f"trades={len(fields.get('trades', []))} "
                     f"warnings={len(fields.get('warnings', []))}")
    lines.append(f"SAVED -> {GOLDEN}")
    sys.stdout.write("\n".join(lines) + "\n")
