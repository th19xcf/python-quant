#!/usr/bin/env python
"""指数历史成分股重建（沪深300 / 中证500 / 中证1000）。

数据源（全部免费公开）：
  * 中证指数官网（csindex.com.cn）：现行成分文件 cons.xls、指数调样公告（正文内联名单或附件 PDF/XLS）
  * baostock：沪深300 / 中证500 的按日历史成分快照（中证1000 无此数据）

产物（重建证据，非权威表）：
  data/local_store/meta/index_members/rebuild/intervals.parquet
  data/local_store/meta/index_members/rebuild/manifest.json
  data/local_store/meta/index_members/rebuild/raw/**

权威成分区间表是 ``meta/index_members/membership_intervals.parquet``
（由 ``index_weight_monthly`` 派生，见 ``src/data/quality/extended_builders.py``）；
本脚本的输出只用于交叉校验其精度，不写权威路径，避免同一事实出现两份表示。

用法：
  python tools/rebuild_index_constituents.py --phase fetch
  python tools/rebuild_index_constituents.py --phase build
"""

from __future__ import annotations

import argparse
import hashlib
import html as html_lib
import io
import json
import logging
import re
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
import requests

ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "data" / "local_store" / "meta" / "index_members" / "rebuild"
RAW_DIR = OUT_DIR / "raw"
CONS_DIR = RAW_DIR / "csindex" / "cons"
ANN_DIR = RAW_DIR / "csindex" / "announcements"
BS_DIR = RAW_DIR / "baostock"

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")
CS_BASE = "https://www.csindex.com.cn"
ANN_LIST_URL = f"{CS_BASE}/csindex-home/announcement/queryAnnouncementByVo"
ANN_DETAIL_URL = f"{CS_BASE}/csindex-home/announcement/queryAnnouncementById"

INDICES = {
    "000300": {"name": "沪深300", "size": 300, "bs": "query_hs300_stocks",
               "cons": "000300cons.xls"},
    "000905": {"name": "中证500", "size": 500, "bs": "query_zz500_stocks",
               "cons": "000905cons.xls"},
    "000852": {"name": "中证1000", "size": 1000, "bs": None,
               "cons": "000852cons.xls"},
}
SECTION_TO_INDEX = {
    "沪深300": "000300", "中证500": "000905", "中证1000": "000852",
    "中证A50": None, "中证A100": None, "中证A500": None,
}
SECTION_RE = re.compile(r"^(沪深\s*300|中证\s*500|中证\s*1000|中证\s*A50|中证\s*A100|"
                        r"中证\s*A500)\s*指数样本股?\s*调整名单[：:]$")
SECTION_SEARCH = re.compile(r"(沪深\s*300|中证\s*500|中证\s*1000|中证\s*A50|中证\s*A100|"
                            r"中证\s*A500)\s*指数样本股?\s*调整名单[：:]")
CODE_RE = re.compile(r"^\d{6}$")


def canon_section(name: str) -> str:
    return re.sub(r"\s+", "", name)

# 三期校验样本（公告 id）：2015-06、2019-06、2024-12
SAMPLE_ANNOUNCEMENT_IDS = [4084, 12934, 15471]

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("index_const")


def now_iso() -> str:
    return datetime.now().replace(microsecond=0).isoformat()


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def to_ts_code(raw: str) -> str:
    """sh.600000 / 600000.SH / 600000 -> 600000.SH。"""
    raw = raw.strip().lower()
    if raw.startswith("sh."):
        return raw[3:].upper() + ".SH"
    if raw.startswith("sz."):
        return raw[3:].upper() + ".SZ"
    if raw.startswith("bj."):
        return raw[3:].upper() + ".BJ"
    raw = raw.upper()
    if "." in raw:
        code, mkt = raw.split(".", 1)
        return f"{code}.{mkt}"
    return f"{raw}.SH" if raw.startswith(("6", "9", "5")) else f"{raw}.SZ"


class BlockedError(RuntimeError):
    """命中反爬/WAF，必须停止加压并如实上报。"""


class HttpClient:
    """串行 + 节流的 HTTP 客户端；403/429 时长时间冷却，连续命中则中止。"""

    def __init__(self, interval: float = 1.0, retries: int = 3, timeout: int = 30):
        self.interval = interval
        self.retries = retries
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": UA,
            "Referer": "https://www.csindex.com.cn/mobile-web/",
            "Accept": "application/json, text/plain, */*",
            "Content-Type": "application/json;charset=UTF-8",
            "Origin": "https://www.csindex.com.cn",
        })
        self._last = 0.0
        self.requests = 0

    def _wait(self) -> None:
        gap = self.interval - (time.time() - self._last)
        if gap > 0:
            time.sleep(gap)

    def get(self, url: str, **kwargs) -> requests.Response:
        params = kwargs.pop("params", None)
        for attempt in range(self.retries):
            self._wait()
            r = self.session.get(url, params=params, timeout=self.timeout, **kwargs)
            self._last = time.time()
            self.requests += 1
            if r.status_code in (403, 429):
                wait = 60 * (attempt + 1)
                log.warning("HTTP %s（疑似限速/反爬）%s，冷却 %ss 后重试 %d/%d",
                            r.status_code, url, wait, attempt + 1, self.retries)
                time.sleep(wait)
                continue
            if r.status_code >= 500:
                wait = 10 * (attempt + 1)
                log.warning("HTTP %s %s，%ss 后重试 %d/%d", r.status_code, url, wait,
                            attempt + 1, self.retries)
                time.sleep(wait)
                continue
            r.raise_for_status()
            return r
        raise BlockedError(f"GET 被拒绝: {url}")

    def post_json(self, url: str, payload: dict) -> dict:
        for attempt in range(self.retries):
            self._wait()
            r = self.session.post(url, data=json.dumps(payload), timeout=self.timeout)
            self._last = time.time()
            self.requests += 1
            if r.status_code in (403, 429):
                wait = 60 * (attempt + 1)
                log.warning("HTTP %s（疑似限速/反爬）%s，冷却 %ss 后重试 %d/%d",
                            r.status_code, url, wait, attempt + 1, self.retries)
                time.sleep(wait)
                continue
            if r.status_code >= 500:
                wait = 10 * (attempt + 1)
                log.warning("HTTP %s %s，%ss 后重试 %d/%d", r.status_code, url, wait,
                            attempt + 1, self.retries)
                time.sleep(wait)
                continue
            r.raise_for_status()
            data = r.json()
            if not data.get("success"):
                raise RuntimeError(f"业务失败: {str(data)[:200]}")
            return data
        raise BlockedError(f"POST 被拒绝: {url}")


# ---------------------------------------------------------------------------
# 阶段一：抓取
# ---------------------------------------------------------------------------


def fetch_cons(client: HttpClient) -> dict:
    CONS_DIR.mkdir(parents=True, exist_ok=True)
    meta = {}
    for code, cfg in INDICES.items():
        url = f"https://oss-ch.csindex.com.cn/static/html/csindex/public/uploads/file/autofile/cons/{cfg['cons']}"
        r = client.get(url)
        path = CONS_DIR / cfg["cons"]
        path.write_bytes(r.content)
        meta[code] = {"url": url, "file": str(path.relative_to(OUT_DIR)),
                      "sha256": sha256(path), "captured_at": now_iso()}
        log.info("成分文件 %s -> %s (%d bytes)", code, path.name, len(r.content))
    (CONS_DIR / "_manifest.json").write_text(
        json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    return meta


def query_announcement_list(client: HttpClient, keyword: str, year: int) -> list[dict]:
    items: list[dict] = []
    page = 1
    while True:
        payload = {
            "lang": "cn", "searchInput": keyword,
            "startDate": f"{year}-01-01", "endDate": f"{year}-12-31",
            "page": {"key": "", "order": None, "page": page, "rows": 100, "sortBy": ""},
            "classList": [], "indexList": [], "relatedTopics": [], "typeList": [],
        }
        data = client.post_json(ANN_LIST_URL, payload)
        rows = data.get("data") or []
        items.extend(rows)
        total = data.get("total") or 0
        if not rows or len(items) >= total or page >= 20:
            break
        page += 1
    return items


def fetch_announcement_lists(client: HttpClient, years: range,
                             keywords: list[str]) -> dict[int, dict]:
    ANN_DIR.mkdir(parents=True, exist_ok=True)
    list_meta: dict = {}
    catalogue: dict[int, dict] = {}
    for year in years:
        for kw in keywords:
            path = ANN_DIR / f"list_{year}_{kw}.json"
            if path.exists() and path.stat().st_size > 2:
                rows = json.loads(path.read_text(encoding="utf-8"))
                log.info("公告列表 %s/%s: 复用已落盘 %d 条", year, kw, len(rows))
            else:
                rows = query_announcement_list(client, kw, year)
                path.write_text(json.dumps(rows, ensure_ascii=False, indent=1),
                                encoding="utf-8")
                log.info("公告列表 %s/%s: %d 条", year, kw, len(rows))
            list_meta[f"{year}_{kw}"] = {"file": str(path.relative_to(OUT_DIR)),
                                         "count": len(rows), "captured_at": now_iso()}
            for row in rows:
                catalogue[int(row["id"])] = row
    (ANN_DIR / "list_manifest.json").write_text(
        json.dumps(list_meta, ensure_ascii=False, indent=2), encoding="utf-8")
    return catalogue


def select_wanted(catalogue: dict[int, dict]) -> list[dict]:
    """2014 年起全部指数调样公告（覆盖中证1000 全生命周期）+ 三期人工校验公告。"""
    wanted: list[dict] = []
    for aid, row in sorted(catalogue.items()):
        title = row.get("title") or ""
        theme = row.get("theme") or ""
        pub = row.get("publishDate") or ""
        if aid in SAMPLE_ANNOUNCEMENT_IDS:
            wanted.append(row)
            continue
        if theme != "指数调样" or pub < "2014-01-01":
            continue
        if any(k in title for k in ("精明", "港股通", "香港", "ETF", "AH", "行业", "主题",
                                    "风格")) and "1000" not in title:
            continue
        wanted.append(row)
    return wanted


def fetch_announcement_details(client: HttpClient, wanted: list[dict]) -> list[dict]:
    ANN_DIR.mkdir(parents=True, exist_ok=True)
    detail_index = []
    skipped = 0
    for row in wanted:
        aid = int(row["id"])
        adir = ANN_DIR / str(aid)
        adir.mkdir(parents=True, exist_ok=True)
        detail_path = adir / "detail.json"
        if detail_path.exists() and detail_path.stat().st_size > 10:
            skipped += 1
            detail = json.loads(detail_path.read_text(encoding="utf-8"))
        else:
            try:
                detail = client.get(ANN_DETAIL_URL, params={"id": aid}).json()
            except BlockedError as exc:
                log.exception("命中反爬，停止本次抓取（已完成 %d/%d）：%s",
                              len(detail_index), len(wanted), exc)
                break
            except Exception as exc:  # noqa: BLE001
                log.exception("详情抓取失败 id=%s: %s", aid, exc)
                continue
            detail_path.write_text(json.dumps(detail, ensure_ascii=False, indent=1),
                                   encoding="utf-8")
        files = []
        for enc in (detail.get("data") or {}).get("enclosureList") or []:
            furl = enc.get("fileUrl") or ""
            if not furl:
                continue
            fname = re.sub(r'[\\/:*?"<>|]', "_", furl.rsplit("/", 1)[-1])
            fpath = adir / fname
            if not (fpath.exists() and fpath.stat().st_size > 0):
                try:
                    fr = client.get(furl)
                except BlockedError as exc:
                    log.exception("命中反爬，停止本次抓取：%s", exc)
                    return detail_index
                except Exception as exc:  # noqa: BLE001
                    log.exception("附件下载失败 id=%s %s: %s", aid, furl, exc)
                    continue
                fpath.write_bytes(fr.content)
            files.append({"name": enc.get("fileName") or fname, "url": furl,
                          "file": str(fpath.relative_to(OUT_DIR)),
                          "sha256": sha256(fpath), "bytes": fpath.stat().st_size})
        detail_index.append({"id": aid, "title": row.get("title"),
                             "publish_date": row.get("publishDate"),
                             "theme": row.get("theme"), "files": files,
                             "captured_at": now_iso()})
        log.info("详情 %s/%s id=%s 附件 %d 个", len(detail_index), len(wanted), aid,
                 len(files))
    (ANN_DIR / "detail_manifest.json").write_text(
        json.dumps(detail_index, ensure_ascii=False, indent=2), encoding="utf-8")
    log.info("详情抓取完成：新抓/复用 %d 条，跳过已存在 %d 条", len(detail_index), skipped)
    return detail_index


# ---------------------------------------------------------------------------
# 阶段二：解析公告名单
# ---------------------------------------------------------------------------


@dataclass
class Event:
    index_code: str
    effective_date: date
    in_codes: list[str]
    out_codes: list[str]
    source_id: int
    title: str
    parse_kind: str
    stated_pairs: dict[str, int] = field(default_factory=dict)


def strip_html(text: str) -> str:
    text = html_lib.unescape(text or "")
    text = re.sub(r"<[^>]+>", " ", text)
    return re.sub(r"[\s\u3000\xa0]+", " ", text).strip()


def parse_effective_date(text: str) -> date | None:
    m = re.search(r"于?\s*(\d{4})\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})\s*日", text)
    if not m:
        return None
    try:
        return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
    except ValueError:
        return None


def parse_stated_counts(text: str) -> dict[str, int]:
    out: dict[str, int] = {}
    for name, count in re.findall(
            r"(沪深\s*300|中证\s*500|中证\s*1000)\s*指数更换\s*(\d+)\s*只", text):
        key = name.replace(" ", "")
        out[key] = int(count)
    return out


def scan_grid(rows: list[list[str]], state: dict,
              acc: dict[str, dict[str, list[str]]]) -> None:
    """在二维单元格中扫描「调出/调入」名单；state 跨表保持章节与列语义。"""
    for row in rows:
        cells = [re.sub(r"[\s\u3000\xa0]+", " ", "" if c is None else str(c)).strip()
                 for c in row]
        non_empty = [c for c in cells if c]
        if not non_empty:
            continue
        if any("备选" in c for c in non_empty) or any(c == "排序" for c in non_empty):
            state["skip"] = True
            continue
        if len(non_empty) == 1 and SECTION_RE.match(non_empty[0]):
            state["section"] = canon_section(SECTION_RE.match(non_empty[0]).group(1))
            state["skip"] = False
            state["cols"] = None
            acc.setdefault(state["section"], {"in": [], "out": []})
            continue
        joined = " ".join(non_empty)
        sec_match = SECTION_SEARCH.search(joined)
        if sec_match:
            state["section"] = canon_section(sec_match.group(1))
            state["skip"] = False
            state["cols"] = None
            acc.setdefault(state["section"], {"in": [], "out": []})
            continue
        if state["skip"]:
            continue
        code_positions = [i for i, c in enumerate(cells) if "代码" in c]
        if len(code_positions) >= 2:
            state["cols"] = (code_positions[0], code_positions[-1])
            continue
        section = state["section"]
        if section is None or SECTION_TO_INDEX.get(section) is None:
            continue
        cols = state["cols"]
        if cols is None:
            six = [i for i, c in enumerate(cells) if CODE_RE.match(c)]
            if len(six) >= 2:
                cols = (six[0], six[-1])
                state["cols"] = cols
        if not cols:
            continue
        if len(cells) > max(cols) and CODE_RE.match(cells[cols[0]]) \
                and CODE_RE.match(cells[cols[1]]):
            acc[section]["out"].append(cells[cols[0]])
            acc[section]["in"].append(cells[cols[1]])


def parse_pdf(path: Path, acc: dict[str, dict[str, list[str]]]) -> None:
    import pdfplumber

    state: dict = {"section": None, "cols": None, "skip": False}
    with pdfplumber.open(path) as pdf:
        for page in pdf.pages:
            lines: dict[float, list[tuple[float, str]]] = {}
            for w in page.extract_words():
                key = round(w["top"], 0)
                lines.setdefault(key, []).append((w["x0"], w.get("text") or ""))
            headers = []
            for top, parts in lines.items():
                line = re.sub(r"\s+", "", "".join(t for _, t in sorted(parts)))
                match = SECTION_SEARCH.search(line)
                if match:
                    headers.append((top, canon_section(match.group(1))))
            headers.sort()
            for table in page.find_tables():
                top = table.bbox[1]
                for htop, name in headers:
                    if htop <= top + 2:
                        state["section"] = name
                        state["skip"] = False
                        state["cols"] = None
                        acc.setdefault(name, {"in": [], "out": []})
                scan_grid(table.extract(), state, acc)


def parse_excel(path: Path, acc: dict[str, dict[str, list[str]]]) -> None:
    xls = pd.ExcelFile(path)
    state: dict = {"section": None, "cols": None, "skip": False}
    for sheet in xls.sheet_names:
        df = pd.read_excel(path, sheet_name=sheet, header=None).astype(str)
        scan_grid(df.values.tolist(), state, acc)


def parse_inline_text(text: str, acc: dict[str, dict[str, list[str]]]) -> None:
    """解析正文纯文本中的名单（老公告内联表格被拍平后的形态）。"""
    target_re = re.compile(r"(沪深\s*300|中证\s*500|中证\s*1000)\s*指数样本股?\s*(?:调整)?名单[：:]")
    stop_re = re.compile(r"指数样本股?\s*(?:调整)?名单[：:]|备选名单|备选股票")
    code_re = re.compile(r"(?<!\d)(\d{6})(?!\d)")
    for marker in target_re.finditer(text):
        start = marker.end()
        nxt = stop_re.search(text, start)
        end = nxt.start() if nxt else len(text)
        segment = text[start:end]
        section = canon_section(marker.group(1))
        acc.setdefault(section, {"in": [], "out": []})
        codes = code_re.findall(segment)
        for i in range(0, len(codes) - 1, 2):
            acc[section]["out"].append(codes[i])
            acc[section]["in"].append(codes[i + 1])


def parse_announcement_files() -> list[Event]:  # noqa: C901
    if not ANN_DIR.exists():
        return []
    events: list[Event] = []
    for adir in sorted([d for d in ANN_DIR.iterdir() if d.is_dir() and d.name.isdigit()],
                       key=lambda d: int(d.name)):
        detail_path = adir / "detail.json"
        if not detail_path.exists():
            continue
        detail = json.loads(detail_path.read_text(encoding="utf-8")).get("data") or {}
        raw_content = detail.get("content") or ""
        text = strip_html(raw_content)
        eff = parse_effective_date(text)
        stated = parse_stated_counts(text)
        acc: dict[str, dict[str, list[str]]] = {}
        kinds = []
        for f in sorted(adir.iterdir()):
            suffix = f.suffix.lower()
            try:
                if suffix == ".pdf":
                    parse_pdf(f, acc)
                    kinds.append("pdf")
                elif suffix in (".xls", ".xlsx"):
                    parse_excel(f, acc)
                    kinds.append("excel")
            except Exception as exc:  # noqa: BLE001
                log.exception("附件解析失败 %s: %s", f, exc)
        if not acc:
            try:
                if "<table" in raw_content.lower():
                    html_state: dict = {"section": None, "cols": None, "skip": False}
                    for df in pd.read_html(io.StringIO(raw_content)):
                        scan_grid(df.astype(str).values.tolist(), html_state, acc)
                    if acc:
                        kinds.append("html")
            except Exception as exc:  # noqa: BLE001
                log.warning("HTML 表格解析失败 id=%s: %s", adir.name, exc)
        if not acc:
            parse_inline_text(text, acc)
            if acc:
                kinds.append("inline_text")
        if not acc:
            log.warning("公告 %s 未解析出任何名单：%s", adir.name, detail.get("title"))
            continue
        if eff is None:
            log.warning("公告 %s 未解析出生效日：%s", adir.name, detail.get("title"))
            continue
        for section, lists in acc.items():
            code = SECTION_TO_INDEX.get(section)
            if code is None:
                continue
            ins = [c for c in lists["in"] if CODE_RE.match(c)]
            outs = [c for c in lists["out"] if CODE_RE.match(c)]
            if not ins and not outs:
                continue
            events.append(Event(
                index_code=code, effective_date=eff,
                in_codes=sorted(set(ins)), out_codes=sorted(set(outs)),
                source_id=int(adir.name), title=detail.get("title") or "",
                parse_kind="+".join(sorted(set(kinds))), stated_pairs=stated))
    return events


# ---------------------------------------------------------------------------
# 阶段三：baostock 快照重建（000300 / 000905）
# ---------------------------------------------------------------------------


def bs_login():
    import baostock as bs
    lg = bs.login()
    if lg.error_code != "0":
        raise RuntimeError(f"baostock 登录失败: {lg.error_msg}")
    return bs


def bs_trade_days(bs, start: date, end: date) -> list[date]:
    rs = bs.query_trade_dates(start_date=start.isoformat(), end_date=end.isoformat())
    days = []
    while rs.error_code == "0" and rs.next():
        row = rs.get_row_data()
        if row[1] == "1":
            days.append(date.fromisoformat(row[0]))
    return days


def bs_snapshot(bs, fn_name: str, day: date) -> set[str]:
    rs = getattr(bs, fn_name)(date=day.isoformat())
    if rs.error_code != "0":
        raise RuntimeError(f"{fn_name} {day} 失败: {rs.error_msg}")
    rows = []
    while rs.next():
        rows.append(rs.get_row_data())
    return {to_ts_code(r[1]) for r in rows}


def build_from_baostock(code: str, client: HttpClient) -> tuple[list[dict], dict]:  # noqa: C901
    cfg = INDICES[code]
    bs = bs_login()
    try:
        calendar = bs_trade_days(bs, date(2005, 1, 1), date(2026, 11, 6))
        cache: dict[date, set[str]] = {}
        cache_csv = BS_DIR / f"snapshots_{code}.csv"
        if cache_csv.exists():
            cached = pd.read_csv(cache_csv, dtype=str)
            for qd, grp in cached.groupby("query_date"):
                cache[date.fromisoformat(qd)] = set(grp["ts_code"])
            log.info("%s 复用已落盘快照 %d 个日期", code, len(cache))

        def snap(d: date) -> set[str]:
            if d not in cache:
                cache[d] = bs_snapshot(bs, cfg["bs"], d)
                time.sleep(0.05)
            return cache[d]

        # 覆盖起点：逐月试查早期首个交易日，取首个成分数量正确的快照日
        start_day = None
        probes = []
        last_month = None
        for d in calendar:
            if d.year > 2015:
                break
            key = (d.year, d.month)
            if key != last_month:
                probes.append(d)
                last_month = key
        for probe in probes:
            if len(snap(probe)) == cfg["size"]:
                start_day = probe
                break
        if start_day is None:
            raise RuntimeError(f"{code} 未找到有效起点")
        end_day = calendar[-1]
        days = [d for d in calendar if start_day <= d <= end_day]

        # 每月最后一个交易日
        month_ends = [d for i, d in enumerate(days)
                      if i == len(days) - 1 or d.month != days[i + 1].month]
        if days[0] != month_ends[0]:
            month_ends.insert(0, days[0])

        log.info("%s 月度快照 %d 个（%s ~ %s）", code, len(month_ends),
                 start_day, month_ends[-1])

        pos = {d: i for i, d in enumerate(calendar)}
        changes: dict[date, set[str]] = {start_day: snap(start_day)}
        log.info("%s 起点 %s 成分数 %d", code, start_day, len(changes[start_day]))

        # 逐月对比，差异月份在交易日序列上递归二分定位所有变更日
        stack: list[tuple[date, date]] = []
        prev = start_day
        for d in month_ends[1:]:
            if snap(d) != snap(prev):
                stack.append((prev, d))
            prev = d
        while stack:
            lo, hi = stack.pop()
            if snap(lo) == snap(hi):
                continue
            if pos[hi] - pos[lo] == 1:
                changes[hi] = snap(hi)
                continue
            mid = calendar[(pos[lo] + pos[hi]) // 2]
            if snap(mid) == snap(lo):
                stack.append((mid, hi))
            elif snap(mid) == snap(hi):
                stack.append((lo, mid))
            else:
                stack.append((lo, mid))
                stack.append((mid, hi))
        ordered = sorted(changes.items(), key=lambda x: x[0])

        # 校验数量守恒
        bad = [(d, len(s)) for d, s in ordered if len(s) != cfg["size"]]
        if bad:
            log.error("%s 成分数量漂移: %s", code, bad[:10])
        month_bad = [(d, len(snap(d))) for d in month_ends if len(snap(d)) != cfg["size"]]
        if month_bad:
            log.error("%s 月末快照数量漂移: %s", code, month_bad[:10])

        records = _intervals_from_changes(code, ordered, end_day, "baostock_snapshot")
        # 原始快照留档
        BS_DIR.mkdir(parents=True, exist_ok=True)
        rows = [(d.isoformat(), ts) for d, s in sorted(cache.items()) for ts in sorted(s)]
        df = pd.DataFrame(rows, columns=["query_date", "ts_code"])
        df.to_csv(BS_DIR / f"snapshots_{code}.csv", index=False, encoding="utf-8")
        stats = {"start": start_day.isoformat(), "end": end_day.isoformat(),
                 "change_points": len(ordered),
                 "month_ends": len(month_ends),
                 "size_mismatch": [{"date": d.isoformat(), "size": n} for d, n in bad],
                 "month_end_mismatch": [{"date": d.isoformat(), "size": n}
                                        for d, n in month_bad],
                 "queries": len(cache)}
        return records, stats
    finally:
        bs.logout()


def _intervals_from_changes(code: str, changes: list[tuple[date, set[str]]],
                            end_day: date, source: str) -> list[dict]:
    captured = now_iso()
    open_since: dict[str, date] = dict.fromkeys(changes[0][1], changes[0][0])
    records: list[dict] = []
    for prev, cur in zip(changes, changes[1:], strict=False):
        day, new_set = cur
        prev_set = prev[1]
        for ts in prev_set - new_set:
            records.append(_record(code, ts, open_since.pop(ts), day, source, captured))
        for ts in new_set - prev_set:
            open_since[ts] = day
    for ts, since in open_since.items():
        records.append(_record(code, ts, since, None, source, captured))
    return records


def _record(code, ts, in_date: date, out_date: date | None, source, captured) -> dict:
    return {"index_code": code, "ts_code": ts,
            "in_date": in_date.isoformat(),
            "out_date": out_date.isoformat() if out_date else None,
            "source": source, "captured_at": captured}


# ---------------------------------------------------------------------------
# 阶段四：公告事件逆推（000852）
# ---------------------------------------------------------------------------


def build_from_events(code: str, events: list[Event], base: set[str],
                      base_date: date, end_day: date) -> tuple[list[dict], dict]:
    raw = sorted([e for e in events if e.index_code == code and e.effective_date <= base_date],
                 key=lambda e: e.effective_date, reverse=True)
    # 同一生效日的多条公告合并
    merged: list[Event] = []
    for ev in raw:
        if merged and merged[-1].effective_date == ev.effective_date:
            last = merged[-1]
            merged[-1] = Event(
                index_code=code, effective_date=last.effective_date,
                in_codes=sorted(set(last.in_codes) | set(ev.in_codes)),
                out_codes=sorted(set(last.out_codes) | set(ev.out_codes)),
                source_id=last.source_id, title=last.title,
                parse_kind=last.parse_kind, stated_pairs=last.stated_pairs)
        else:
            merged.append(ev)

    current = set(base)
    states: list[tuple[date, set[str]]] = []
    issues = []
    if not merged:
        return [], {"events": 0, "start": None, "end": end_day.isoformat(),
                    "coverage_basis": "无可用公告名单，无法逆推",
                    "size_mismatch": []}
    for ev in merged:
        # 当前 current 是「自该公告生效日起」的成分集合
        states.append((ev.effective_date, set(current)))
        ins, outs = set(ev.in_codes), set(ev.out_codes)
        if len(ins) != len(outs):
            issues.append({"id": ev.source_id, "date": ev.effective_date.isoformat(),
                           "issue": f"调入{len(ins)}≠调出{len(outs)}"})
        missing = ins - current
        already = outs & current
        if missing:
            issues.append({"id": ev.source_id, "date": ev.effective_date.isoformat(),
                           "issue": f"调入名单不在当期成分: {sorted(missing)[:10]}"})
        if already:
            issues.append({"id": ev.source_id, "date": ev.effective_date.isoformat(),
                           "issue": f"调出名单本就不在成分: {sorted(already)[:10]}"})
        current = (current - ins) | outs
        if len(current) != INDICES[code]["size"]:
            issues.append({"id": ev.source_id, "date": ev.effective_date.isoformat(),
                           "issue": f"逆推后成分数={len(current)}"})

    changes = sorted(states, key=lambda x: x[0])
    records = _intervals_from_changes(code, changes, end_day, "csindex_announcement")
    stats = {"events": len(merged),
             "start": changes[0][0].isoformat(),
             "end": end_day.isoformat(),
             "coverage_basis": "最早可逆推公告的生效日（此前区间未知，不回填）",
             "size_mismatch": issues}
    return records, stats


# ---------------------------------------------------------------------------
# 阶段五：校验与输出
# ---------------------------------------------------------------------------


def load_cons(code: str) -> tuple[set[str], date]:
    df = pd.read_excel(CONS_DIR / INDICES[code]["cons"], dtype=str)
    code_col = next(c for c in df.columns if "成分券代码" in str(c) or "代码" in str(c))
    date_col = next((c for c in df.columns if "日期" in str(c)), None)
    codes = {to_ts_code(str(v).strip()) for v in df[code_col] if str(v).strip().isdigit()}
    d = date(2026, 10, 30)
    if date_col:
        raw = str(df[date_col].iloc[0]).strip()
        m = re.match(r"(\d{4})[-/]?(\d{2})[-/]?(\d{2})", raw)
        if m:
            d = date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
    return codes, d


def membership_on(records: list[dict], day: date) -> set[str]:
    out = set()
    iso = day.isoformat()
    for r in records:
        if r["in_date"] <= iso and (r["out_date"] is None or r["out_date"] > iso):
            out.add(r["ts_code"])
    return out


def validate(records_by_index: dict[str, list[dict]], events: list[Event],
             end_day: date) -> dict:
    report = {"snapshot_checks": [], "sample_checks": [], "warnings": []}
    snap_dir = ROOT / "_staging/index_membership_snapshots_20261112"  # 2026-11-12 按 DG-32 迁出正式层
    for f in sorted(snap_dir.glob("*.parquet")):
        m = re.search(r"(\d{8})\.parquet$", f.name)
        if not m:
            continue
        code = f.name.split("_")[1]
        if not records_by_index.get(code):
            continue
        day = date(int(m.group(1)[:4]), int(m.group(1)[4:6]), int(m.group(1)[6:]))
        official = set(pd.read_parquet(f)["ts_code"])
        ours = membership_on(records_by_index[code], day)
        report["snapshot_checks"].append({
            "index_code": code, "date": day.isoformat(),
            "official": len(official), "rebuilt": len(ours),
            "only_official": sorted(official - ours)[:10],
            "only_rebuilt": sorted(ours - official)[:10],
            "match": official == ours})
    for ev in events:
        if ev.source_id not in SAMPLE_ANNOUNCEMENT_IDS:
            continue
        code = ev.index_code
        if not records_by_index.get(code):
            continue
        official_in, official_out = set(ev.in_codes), set(ev.out_codes)
        day = ev.effective_date
        # 找该指数在该生效日附近的重建进出
        recs = records_by_index[code]
        eff_days = sorted({r["in_date"] for r in recs} |
                          {r["out_date"] for r in recs if r["out_date"]})
        cand = [d for d in eff_days if abs((date.fromisoformat(d) - day).days) <= 10]
        if not cand:
            report["warnings"].append(
                f"{code} 公告 {ev.source_id} 生效日 {day} 附近无重建变更点")
            continue
        eff_day = min(cand, key=lambda d: abs((date.fromisoformat(d) - day).days))
        before = membership_on(recs, date.fromisoformat(eff_day) - timedelta(days=1))
        after = membership_on(recs, date.fromisoformat(eff_day))
        rebuilt_in, rebuilt_out = after - before, before - after
        report["sample_checks"].append({
            "index_code": code, "announcement_id": ev.source_id,
            "title": ev.title, "announced_effective": day.isoformat(),
            "rebuilt_effective": eff_day,
            "official_in": len(official_in), "official_out": len(official_out),
            "rebuilt_in": len(rebuilt_in), "rebuilt_out": len(rebuilt_out),
            "in_match": official_in == rebuilt_in,
            "out_match": official_out == rebuilt_out,
            "in_diff": sorted(official_in ^ rebuilt_in)[:10],
            "out_diff": sorted(official_out ^ rebuilt_out)[:10]})
    return report


def write_outputs(records_by_index: dict[str, list[dict]], manifest: dict) -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    all_recs = [r for recs in records_by_index.values() for r in recs]
    df = pd.DataFrame(all_recs, columns=["index_code", "ts_code", "in_date", "out_date",
                                         "source", "captured_at"])
    df = df.sort_values(["index_code", "ts_code", "in_date"]).reset_index(drop=True)
    df.to_parquet(OUT_DIR / "intervals.parquet", index=False)
    (OUT_DIR / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    log.info("写入 intervals.parquet: %d 行", len(df))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--phase", choices=["lists", "details", "bs", "build", "all"],
                    default="all")
    ap.add_argument("--interval", type=float, default=1.0)
    args = ap.parse_args()

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    client = HttpClient(interval=args.interval)

    if args.phase in ("lists", "all"):
        fetch_cons(client)
        catalogue = fetch_announcement_lists(
            client, range(2005, 2027),
            ["沪深300", "中证500", "中证1000", "定期调整"])
        log.info("公告目录 %d 条，HTTP 请求 %d 次", len(catalogue), client.requests)
        if args.phase == "lists":
            return

    if args.phase in ("details", "all"):
        cat = {}
        for path in ANN_DIR.glob("list_*.json"):
            for row in json.loads(path.read_text(encoding="utf-8")):
                cat[int(row["id"])] = row
        wanted = select_wanted(cat)
        log.info("待抓详情公告 %d 条", len(wanted))
        fetch_announcement_details(client, wanted)
        if args.phase == "details":
            return

    events = parse_announcement_files()
    log.info("解析出事件 %d 条，涉及指数 %s", len(events),
             sorted({e.index_code for e in events}))

    records_by_index: dict[str, list[dict]] = {}
    stats: dict[str, dict] = {}
    end_day = date(2026, 11, 6)

    for code in ("000300", "000905"):
        recs, st = build_from_baostock(code, client)
        records_by_index[code] = recs
        stats[code] = st
    if args.phase == "bs":
        log.info("baostock 重建完成：%s", json.dumps(stats, ensure_ascii=False))
        return

    base_852, base_date = load_cons("000852")
    recs, st = build_from_events("000852", events, base_852, base_date, end_day)
    records_by_index["000852"] = recs
    stats["000852"] = st

    validation = validate(records_by_index, events, end_day)

    gaps: dict[str, dict] = {}
    if not records_by_index.get("000852"):
        gaps["000852"] = {
            "coverage": "none",
            "affected_range": "2014-10-17（指数发布）~ 2026-10-30（现行成分文件数据日）",
            "reason": ("中证指数官网公告详情接口（queryAnnouncementById）持续返回 HTTP 403 / "
                       "WAF 挑战页（attack.jinxibei.com），无法取得 2014 年以来的调样名单附件；"
                       "baostock 免费库不提供中证1000 历史成分"),
            "policy": "不回填：未用当前成分向前套用",
        }
    if not any(e.source_id == 12934 for e in events):
        gaps["sample_2019_06"] = {
            "announcement_id": 12934,
            "reason": "该公告详情/附件因 WAF 403 未能获取，2019-06 三期样本尚未完成逐条比对",
            "status": "待 WAF 解除后补抓",
        }

    manifest = {
        "dataset": "index_members_rebuild",
        "generated_at": now_iso(),
        "method": {
            "000300": "baostock 月末成分快照 + 递归二分定位变更日（有效成分/全量）",
            "000905": "baostock 月末成分快照 + 递归二分定位变更日（有效成分/全量）",
            "000852": "中证指数公告调入调出名单，从现行成分反向累积",
        },
        "sources": [
            {"name": "csindex_cons", "url": "https://oss-ch.csindex.com.cn/.../cons/{code}cons.xls",
             "desc": "中证指数官网现行成分文件"},
            {"name": "csindex_announcement", "url": ANN_LIST_URL,
             "desc": "中证指数官网指数调样公告（正文/PDF 名单）",
             "status": "公告列表可访问；详情接口被 WAF 403 阻断"},
            {"name": "baostock", "url": "baostock 免费行情库 query_hs300_stocks/query_zz500_stocks",
             "desc": "按日历史成分快照"},
        ],
        "captured_at": now_iso(),
        "coverage": {
            code: {"start": st["start"], "end": st["end"],
                   "intervals": len(records_by_index[code]),
                   "change_points": st.get("change_points", st.get("events"))}
            for code, st in stats.items()},
        "gaps": gaps,
        "validation": validation,
        "field_semantics": {
            "in_date": "该票进入指数的首个交易日（含）",
            "out_date": "该票退出指数的首个交易日（不含该日），仍在成分则为空",
            "source": "baostock_snapshot / csindex_announcement",
            "coverage_start_in_date": "覆盖起点已在成分内的个股，in_date 记为覆盖起点，真实纳入日早于该日",
        },
    }
    write_outputs(records_by_index, manifest)


if __name__ == "__main__":
    main()
