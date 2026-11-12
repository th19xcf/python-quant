#!/usr/bin/env python
"""指数历史成分（月度快照）重建：tushare index_weight 逐指数 × 逐月拉取。

产物：
  data/local_store/meta/index_members/index_weight_monthly.parquet
  data/local_store/meta/index_members/manifest.json
  tmp/index_weight_raw/<index_code>/<yyyymm>.parquet   原始响应分片缓存

用法：
  python tools/pull_index_weight.py --phase fetch      # 拉取（可断点续跑）
  python tools/pull_index_weight.py --phase build      # 合并主表 + manifest
  python tools/pull_index_weight.py --phase validate   # 校验（输出 JSON 到 tmp）
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import sys
import time
from datetime import datetime
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
STORE_ROOT = ROOT / "data" / "local_store"
OUT_DIR = STORE_ROOT / "meta" / "index_members"
RAW_DIR = ROOT / "tmp" / "index_weight_raw"
TOKEN_FILE = ROOT / ".secrets" / "tushare_token.txt"
#: 官方日快照（2026-11-12 按 DG-32 迁出正式层）在 staging 下的任务名
SNAPSHOT_TASK = "index_membership_snapshots_20261112"

sys.path.insert(0, str(ROOT))
from src.data.staging import staging_dir_for_store  # noqa: E402


def snapshot_dir(store_root: Path | None = None) -> Path:
    """官方日快照目录：随 store 根推导，临时 store 不会写到仓库 ``_staging``。"""
    return staging_dir_for_store(store_root or STORE_ROOT, SNAPSHOT_TASK)


# 起点为「已用年度探测确认为空」的上一月：更早月份不再请求，避免无谓开销
INDICES = {
    "000016.SH": {"name": "上证50", "size": 50, "from": "200901"},
    "000300.SH": {"name": "沪深300", "size": 300, "from": "200501"},
    "000905.SH": {"name": "中证500", "size": 500, "from": "200701"},
    "000852.SH": {"name": "中证1000", "size": 1000, "from": "201410"},
    "000688.SH": {"name": "科创50", "size": 50, "from": "202001"},
    "399006.SZ": {"name": "创业板指", "size": 100, "from": "201001"},
}
END_MONTH = "202611"
INTERVAL = 0.32  # 秒；约 187 次/分钟，低于 200 次/分钟上限

SOURCE = "tushare.index_weight"
COLS = ["index_code", "ts_code", "trade_date", "weight", "source", "fetched_at"]

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("index_weight")


def now_iso() -> str:
    return datetime.now().astimezone().replace(microsecond=0).isoformat()


def month_range(start: str, end: str) -> list[str]:
    y, m = int(start[:4]), int(start[4:])
    out = []
    while True:
        cur = f"{y:04d}{m:02d}"
        if cur > end:
            break
        out.append(cur)
        m += 1
        if m > 12:
            y, m = y + 1, 1
    return out


def month_bounds(yyyymm: str) -> tuple[str, str]:
    y, m = int(yyyymm[:4]), int(yyyymm[4:])
    last = (pd.Timestamp(year=y, month=m, day=1) + pd.offsets.MonthEnd(0)).day
    return f"{yyyymm}01", f"{yyyymm}{last:02d}"


def get_pro() -> object:
    import tushare as ts

    token = TOKEN_FILE.read_text(encoding="utf-8").strip()
    if not token:
        raise RuntimeError("tushare token 为空")
    return ts.pro_api(token)


def fetch_one(pro, index_code: str, yyyymm: str) -> pd.DataFrame:
    start, end = month_bounds(yyyymm)
    last_err = None
    for attempt in range(5):
        try:
            df = pro.index_weight(index_code=index_code, start_date=start, end_date=end)
            return df
        except Exception as exc:  # noqa: BLE001
            last_err = exc
            wait = 30 * (attempt + 1)
            log.warning("%s %s 请求失败（%s），%ss 后重试 %d/5",
                        index_code, yyyymm, exc, wait, attempt + 1)
            time.sleep(wait)
    raise RuntimeError(f"{index_code} {yyyymm} 连续失败: {last_err}")


def phase_fetch() -> None:
    pro = get_pro()
    total = 0
    reuse = 0
    t0 = time.time()
    for code, cfg in INDICES.items():
        months = month_range(cfg["from"], END_MONTH)
        idir = RAW_DIR / code
        idir.mkdir(parents=True, exist_ok=True)
        done = empty = 0
        for i, ym in enumerate(months, 1):
            path = idir / f"{ym}.parquet"
            if path.exists():
                reuse += 1
                df = pd.read_parquet(path)
            else:
                df = fetch_one(pro, code, ym)
                cols = ["index_code", "con_code", "trade_date", "weight"]
                if df is None or df.empty:
                    df = pd.DataFrame(columns=cols + ["source", "fetched_at"])
                else:
                    df = df.rename(columns={"con_code": "ts_code"})
                    df["source"] = SOURCE
                    df["fetched_at"] = now_iso()
                df.to_parquet(path, index=False)
                total += 1
                time.sleep(INTERVAL)
            if len(df):
                done += 1
            else:
                empty += 1
            if i % 25 == 0 or i == len(months):
                log.info("%s %s 进度 %d/%d（有数据 %d，空 %d）请求 %d 复用 %d 用时 %.1fs",
                         code, cfg["name"], i, len(months), done, empty,
                         total, reuse, time.time() - t0)
    log.info("拉取完成：新请求 %d 次，复用缓存 %d 个分片，耗时 %.1fs",
             total, reuse, time.time() - t0)


def load_all() -> pd.DataFrame:
    frames = []
    for code in INDICES:
        idir = RAW_DIR / code
        if not idir.exists():
            continue
        for path in sorted(idir.glob("*.parquet")):
            df = pd.read_parquet(path)
            if df is not None and len(df):
                frames.append(df)
    if not frames:
        raise RuntimeError("没有可用原始分片，请先执行 --phase fetch")
    out = pd.concat(frames, ignore_index=True)
    out = out[COLS]
    out["trade_date"] = out["trade_date"].astype(str)
    out["weight"] = pd.to_numeric(out["weight"], errors="coerce")
    out = out.sort_values(["index_code", "trade_date", "ts_code"]).reset_index(drop=True)
    return out


def membership_manifest_block() -> dict | None:
    """同目录 membership_intervals.parquet 的逐文件记账（存在才登记）。

    该表由 tools/rebuild_index_constituents.py 从本目录月末快照推导，
    不是本脚本产物；但目录级 manifest 归本脚本重写，必须一并登记，
    否则每跑一次 --phase build 就把条目抹掉。
    """
    path = OUT_DIR / "membership_intervals.parquet"
    if not path.exists():
        return None
    df = pd.read_parquet(path)
    block = {
        "path": "meta/index_members/membership_intervals.parquet",
        "semantics": "由月末快照推导的成分区间表，一行 = 一个 (指数, 标的, 纳入→剔除) 区间；"
                     "in_date/out_date 精度受快照粒度限制，见 in_date_known/out_date_known",
        "produced_by": "tools/rebuild_index_constituents.py",
        "rows": int(len(df)),
        "cols": int(df.shape[1]),
        "columns": [str(c) for c in df.columns],
        "bytes": path.stat().st_size,
        "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        "registered_at": "2026-11-12",
        "registered_reason": "补登：该表与本 manifest 描述的 index_weight_monthly 同目录且被成分 PIT 消费，"
                             "此前只有目录级 coverage/total_rows，缺本文件条目",
    }
    if "ts_code" in df.columns:
        block["codes"] = int(df["ts_code"].nunique())
    for col, key in (("in_date", "in_date_range"), ("out_date", "out_date_range")):
        if col in df.columns:
            series = pd.to_datetime(df[col], errors="coerce").dropna()
            if len(series):
                block[key] = [str(series.min().date()), str(series.max().date())]
    return block


MANIFEST_PATH = OUT_DIR / "manifest.json"


def _load_existing_manifest() -> dict:
    """读现有 manifest 作为合并基底；读不出来就报错，绝不静默整文件重写。"""
    if not MANIFEST_PATH.exists():
        return {}
    try:
        data = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(
            f"现有 manifest 无法读取（{MANIFEST_PATH}：{exc}）；"
            "人工补登字段与 files 段无法判定，拒绝整文件重写"
        ) from exc
    if not isinstance(data, dict):
        raise TypeError(
            f"现有 manifest 顶层不是对象（{type(data).__name__}）：{MANIFEST_PATH}；拒绝整文件重写"
        )
    return data


def _merge_manifest(previous: dict, fresh: dict) -> dict:
    """以现有 manifest 为基底，只刷新工具能算出的字段。

    工具只拥有 `fresh` 里出现的键；`previous` 独有的键（人工补登的 `files`、
    `registered_at` 等）原样保留，不删不改。算不出来的字段宁可留着旧值，
    也不静默丢。
    """
    merged = dict(previous)
    merged.update(fresh)
    return merged


def phase_build() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    previous = _load_existing_manifest()
    df = load_all()
    out_path = OUT_DIR / "index_weight_monthly.parquet"
    df.to_parquet(out_path, index=False)

    coverage = {}
    for code, cfg in INDICES.items():
        sub = df[df["index_code"] == code]
        periods = sorted(sub["trade_date"].unique())
        coverage[code] = {
            "name": cfg["name"],
            "expected_size": cfg["size"],
            "start": periods[0] if periods else None,
            "end": periods[-1] if periods else None,
            "periods": len(periods),
            "rows": int(len(sub)),
            "requested_from": cfg["from"],
        }
    manifest = {
        "dataset": "index_members",
        "generated_at": now_iso(),
        "source": SOURCE,
        "semantics": "tushare 返回的月末最后交易日成分与权重快照；不是精确纳入/剔除日",
        "fields": {
            "index_code": "指数代码（带交易所后缀）",
            "ts_code": "成分券代码（带交易所后缀）",
            "trade_date": "快照交易日（该月最后一个交易日）",
            "weight": "该期权重（百分数，同一期合计约 100）",
            "source": "数据来源标识",
            "fetched_at": "抓取时间（带时区）",
        },
        "raw_cache": str(RAW_DIR.relative_to(ROOT)).replace("\\", "/"),
        "coverage": coverage,
    }
    membership = membership_manifest_block()
    if membership is not None:
        manifest["membership_intervals"] = membership
    manifest["total_rows"] = int(len(df))
    manifest["total_periods"] = int(df["trade_date"].nunique())
    merged = _merge_manifest(previous, manifest)
    preserved = sorted(set(previous) - set(manifest))
    if preserved:
        log.info("manifest 合并：保留工具未管理的键 %s", preserved)
    MANIFEST_PATH.write_text(
        json.dumps(merged, ensure_ascii=False, indent=2), encoding="utf-8")
    log.info("主表写入 %s：%d 行，%d 期", out_path, len(df), merged["total_periods"])


def phase_validate() -> None:
    out_path = OUT_DIR / "index_weight_monthly.parquet"
    if not out_path.exists():
        raise RuntimeError("主表不存在，请先执行 --phase build")
    df = pd.read_parquet(out_path)
    result: dict = {"generated_at": now_iso(), "dup": {}, "counts": {}, "weights": {},
                    "coverage": {}, "snapshot_compare": []}

    # 1. 重复键
    dup = df.duplicated(subset=["index_code", "trade_date", "ts_code"], keep=False)
    result["dup"] = {
        "duplicate_rows": int(dup.sum()),
        "samples": df[dup].head(20)[["index_code", "trade_date", "ts_code"]]
        .to_dict("records"),
    }

    # 2. 每期成分数
    cnt = (df.groupby(["index_code", "trade_date"]).size()
           .rename("n").reset_index())
    for code, cfg in INDICES.items():
        sub = cnt[cnt["index_code"] == code]
        bad = sub[(sub["n"] - cfg["size"]).abs() > cfg["size"] * 0.10]
        result["counts"][code] = {
            "expected": cfg["size"],
            "periods": int(len(sub)),
            "min": int(sub["n"].min()) if len(sub) else None,
            "max": int(sub["n"].max()) if len(sub) else None,
            "bad_periods": len(bad),
            "bad_samples": bad.head(30).to_dict("records"),
        }

    # 3. 权重合计
    wsum = (df.groupby(["index_code", "trade_date"])["weight"].sum()
            .rename("wsum").reset_index())
    for code in INDICES:
        sub = wsum[wsum["index_code"] == code]
        bad = sub[(sub["wsum"] < 90) | (sub["wsum"] > 101)]
        result["weights"][code] = {
            "periods": int(len(sub)),
            "min": round(float(sub["wsum"].min()), 3) if len(sub) else None,
            "max": round(float(sub["wsum"].max()), 3) if len(sub) else None,
            "out_of_range": len(bad),
            "bad_samples": bad.head(30).round(3).to_dict("records"),
        }

    # 4. 月份覆盖
    all_months = set(month_range("200501", END_MONTH))
    for code, cfg in INDICES.items():
        sub = df[df["index_code"] == code]
        periods = sorted(sub["trade_date"].unique())
        if not periods:
            result["coverage"][code] = {"periods": 0}
            continue
        have = {p[:6] for p in periods}
        span = sorted(m for m in all_months if cfg["from"] <= m <= END_MONTH)
        missing = [m for m in span if m not in have]
        result["coverage"][code] = {
            "start": periods[0], "end": periods[-1],
            "periods": len(periods),
            "missing_months": missing,
            "missing_from_requested_range": len(missing),
        }

    # 5. 与现有 2 天快照比对（tushare 仅月末，日期通常不重叠 → 记录最近日期差异）
    #    快照无运行期消费者，2026-11-12 按 DG-32 由 meta/index_membership 迁出正式层。
    snap_dir = snapshot_dir()
    for f in sorted(snap_dir.glob("*.parquet")):
        stem = f.stem.split("_")
        if len(stem) < 4:
            continue
        # 快照文件名为 snapshot_000300_SH_20261105.parquet
        code_raw, suffix, day = stem[1], stem[-2], stem[-1]
        code = f"{code_raw}.{suffix}"
        if code not in INDICES:
            continue
        official = set(pd.read_parquet(f)["ts_code"])
        sub = df[df["index_code"] == code]
        periods = sorted(sub["trade_date"].unique())
        same = periods and any(p.replace("-", "") == day for p in periods)
        nearest = min(periods, key=lambda p: abs(
            (datetime.strptime(p.replace("-", ""), "%Y%m%d") -
             datetime.strptime(day, "%Y%m%d")).days)) if periods else None
        ours = set(sub[sub["trade_date"] == nearest]["ts_code"]) if nearest else set()
        result["snapshot_compare"].append({
            "index_code": code, "snapshot_date": day,
            "exact_overlap": bool(same),
            "nearest_month_end": nearest,
            "official_n": len(official), "tushare_n": len(ours),
            "only_official": sorted(official - ours),
            "only_tushare": sorted(ours - official),
            "match": official == ours,
        })

    out = ROOT / "tmp" / "index_weight_validation.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    log.info("校验结果写入 %s", out)
    print(json.dumps({k: result[k] for k in ("dup", "counts", "weights", "coverage")},
                     ensure_ascii=False, indent=1)[:6000])


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--phase", choices=["fetch", "build", "validate"], required=True)
    args = ap.parse_args()
    if args.phase == "fetch":
        phase_fetch()
    elif args.phase == "build":
        phase_build()
    else:
        phase_validate()


if __name__ == "__main__":
    sys.exit(main())
