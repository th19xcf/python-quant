#!/usr/bin/env python3
"""回测运行清单页生成器：读 `_staging/backtest_runs/` 运行记录 → 单文件 HTML。

只读消费侧工具：不跑回测、不改任何 run 目录与 data/，只把既有记录渲染成清单页。

数据来源
--------
- ``index.json``：run_id / 名称 / created_at / status / 指标 / 各哈希
- 各 run 目录：``spec.json``、``recipe.txt``、``metrics.json``、
  ``daily_nav.csv``、``run.log``、``progress.json``（可能不存在）

缺失字段统一显示「未记录」，不猜、不补算；某 run 缺 daily_nav 则不画曲线。

页面结构
--------
窄标题栏 → 筛选输入 + 计数 → 运行列表（首屏即表格，默认时间倒序）
→ 点行展开：净值曲线 + 四要素（规格快照 / 代码 HEAD / 脚本 sha256 / 数据版本哈希）
→ 「查看原配方」折叠区 → 进行中进度条 / 失败日志尾部折叠。

视觉变量与色板直接复用 ``tools/backtest_result_dashboard.py``，两页互链。

用法
----
    .venv\\Scripts\\python.exe tools\\backtest_run_index_page.py \\
        --runs _staging\\backtest_runs\\index.json \\
        --dashboard 回测结果看板_价量估值等权_2026-11-14.html \\
        --out docs\\回测运行清单_2026-11-14.html
"""
from __future__ import annotations

import argparse
import hashlib
import html
import json
import sys
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional

ROOT = Path(__file__).resolve().parents[1]
for _p in (ROOT, ROOT / 'tools'):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

from backtest_result_dashboard import CSS_TEXT, PALETTE  # 共用同一套视觉变量与色板

PAGE_SIZE = 50
LOG_TAIL_LINES = 12
LOG_LINE_MAX = 260

STATUS_MAP = {
    'ok': ('完成', 'ok'),
    'finished': ('完成', 'ok'),
    'success': ('完成', 'ok'),
    'running': ('进行中', 'run'),
    'started': ('进行中', 'run'),
    'failed': ('失败', 'fail'),
    'error': ('失败', 'fail'),
    'aborted': ('失败', 'fail'),
    'stopped': ('已停止', 'stop'),
    'cancelled': ('已停止', 'stop'),
}

LIST_CSS = r"""
  /* ---- 清单页专用 ---- */
  [hidden]{display:none !important}
  .filter-bar{display:flex;align-items:center;gap:10px;flex-wrap:wrap;margin:0 0 10px}
  .filter-bar input{
    font:inherit;font-size:13px;padding:6px 10px;border:1px solid var(--line);
    border-radius:8px;background:var(--panel);color:var(--ink);min-width:240px;outline:none;
  }
  .filter-bar input:focus{border-color:var(--v1)}
  .filter-bar .count{font-size:12px;color:var(--muted);font-family:var(--mono)}
  .filter-bar .spacer{flex:1}
  a.ghost{
    font-size:12.5px;color:var(--muted);text-decoration:none;border:1px solid var(--line);
    border-radius:8px;padding:4px 10px;background:var(--panel);
  }
  a.ghost:hover{color:var(--ink)}

  table.runs{width:100%;border-collapse:collapse;font-size:12.8px}
  table.runs th,table.runs td{padding:7px 9px;border-bottom:1px solid var(--line2);text-align:right}
  table.runs th:first-child,table.runs td:first-child{text-align:left}
  table.runs thead th{
    color:var(--muted);font-weight:600;font-size:11.5px;border-bottom:1px solid var(--line);
    background:var(--panel);position:sticky;top:38px;z-index:5;
  }
  table.runs tbody tr.run{cursor:pointer}
  table.runs tbody tr.run:hover td{background:var(--line2)}
  table.runs tr.run.open td{background:var(--line2)}
  .rname{font-weight:650;font-size:13px;display:flex;align-items:center;gap:7px}
  .rname .caret{
    width:14px;height:14px;border-radius:4px;border:1px solid var(--line);background:var(--panel);
    display:inline-flex;align-items:center;justify-content:center;font-size:9px;color:var(--muted);
    flex:0 0 auto;
  }
  tr.run.open .rname .caret{color:var(--ink);border-color:var(--axis)}
  .rid{display:block;font-family:var(--mono);font-size:10px;color:var(--faint);margin-top:1px}
  .recipe{
    text-align:left !important;font-size:11.5px;color:var(--muted);max-width:330px;
    white-space:nowrap;overflow:hidden;text-overflow:ellipsis;
  }
  .status{
    display:inline-flex;align-items:center;gap:5px;font-size:11.5px;font-weight:600;
    border-radius:999px;padding:1px 9px;border:1px solid var(--line);
  }
  .status::before{content:"";width:6px;height:6px;border-radius:50%;background:var(--faint)}
  .status.ok{color:var(--pos)} .status.ok::before{background:var(--pos)}
  .status.run{color:var(--v2)} .status.run::before{background:var(--v2)}
  .status.fail{color:var(--neg)} .status.fail::before{background:var(--neg)}
  .status.stop{color:var(--muted)} .status.stop::before{background:var(--faint)}
  a.rlink{color:var(--ink);text-decoration:none;border-bottom:1px dashed var(--axis)}
  a.rlink:hover{color:var(--v1);border-bottom-color:var(--v1)}
  .hash8{font-family:var(--mono);font-size:11px;color:var(--muted)}
  td.num{font-family:var(--mono);font-variant-numeric:tabular-nums}

  tr.detail > td{padding:0 !important;background:var(--bg);border-bottom:1px solid var(--line)}
  .detail-box{padding:14px 16px 16px;display:grid;grid-template-columns:1.15fr 1fr;gap:14px;align-items:start}
  .detail-box .full{grid-column:1 / -1}
  .detail-panel{background:var(--panel);border:1px solid var(--line);border-radius:11px;padding:12px 14px}
  .detail-panel h4{margin:0 0 8px;font-size:12.5px;font-weight:650}
  .kv{display:grid;grid-template-columns:auto 1fr;gap:3px 12px;font-size:11.8px}
  .kv dt{color:var(--muted);white-space:nowrap}
  .kv dd{margin:0;color:var(--ink);font-family:var(--mono);word-break:break-all}
  .nav-chart{width:100%}
  .nav-chart svg{display:block;width:100%}
  .chart-empty{font-size:12px;color:var(--faint);padding:22px 0;text-align:center}
  details.recipe-box{margin-top:10px}
  details.recipe-box summary{
    cursor:pointer;list-style:none;font-size:12px;color:var(--muted);
    border:1px solid var(--line);border-radius:8px;padding:3px 10px;display:inline-block;background:var(--panel);
  }
  details.recipe-box summary::-webkit-details-marker{display:none}
  details.recipe-box pre{
    margin:8px 0 0;padding:10px 12px;background:var(--panel);border:1px solid var(--line);
    border-radius:9px;font-family:var(--mono);font-size:11.5px;line-height:1.7;color:var(--ink);
    white-space:pre-wrap;word-break:break-word;max-height:280px;overflow:auto;
  }
  .bar{height:6px;border-radius:999px;background:var(--line);overflow:hidden;margin:6px 0 4px}
  .bar i{display:block;height:100%;background:var(--v2);border-radius:999px}
  .tag-prov{
    font-family:var(--mono);font-size:10px;border:1px solid var(--v2);color:var(--v2);
    border-radius:4px;padding:0 4px;margin-left:4px;
  }
  pre.logtail{
    margin:8px 0 0;padding:9px 11px;background:var(--panel);border:1px solid var(--line);
    border-radius:9px;font-family:var(--mono);font-size:11px;line-height:1.6;color:var(--muted);
    white-space:pre-wrap;word-break:break-all;max-height:200px;overflow:auto;
  }
  .muted{color:var(--faint)}
  .more-wrap{margin-top:12px;text-align:center}

  @media (max-width:820px){
    table.runs thead{display:none}
    table.runs,table.runs tbody,table.runs tr,table.runs td{display:block;width:100%}
    table.runs tr.run{
      border:1px solid var(--line);border-radius:12px;background:var(--panel);
      box-shadow:var(--shadow);margin-bottom:10px;padding:10px 12px;
    }
    table.runs tr.run td{border:0;padding:2px 0;text-align:left;display:flex;justify-content:space-between;gap:10px}
    table.runs tr.run td::before{
      content:attr(data-label);color:var(--muted);font-size:11.5px;flex:0 0 auto;
    }
    table.runs tr.run td:first-child::before{display:none}
    table.runs tr.run td:first-child{display:block}
    .recipe{max-width:none;white-space:normal;text-align:left !important}
    tr.detail > td{border:0;background:transparent;margin-top:-6px;margin-bottom:10px}
    .detail-box{grid-template-columns:1fr;padding:10px 12px 14px;background:var(--panel);
      border:1px solid var(--line);border-radius:12px}
  }
"""

LIST_TEMPLATE = r"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{{TITLE}}</title>
<style>{{STYLE}}</style>
</head>
<body>

<header class="topbar">
  <div class="topbar-in">
    <h1>{{H1}}</h1>
    <div class="meta">{{META}}</div>
  </div>
</header>

<nav class="navbar"><div class="navbar-in">{{ANCHORS}}</div></nav>

<main>
  <section id="runs">
    <div class="filter-bar">
      <input id="q" type="search" placeholder="按名称或日期筛选，如 v3 或 2026-11-14" aria-label="按名称或日期筛选">
      <span class="count" id="count"></span>
      <span class="spacer"></span>
      <a class="ghost" href="{{DASHBOARD}}">← 回结果看板</a>
    </div>
    <div class="tbl-wrap">
      <table class="runs">
        <thead><tr>
          <th>运行</th><th>日期时间</th><th>配方摘要</th><th>年化</th><th>最大回撤</th>
          <th>年换手</th><th>成本占比</th><th>状态</th><th>数据版本</th>
        </tr></thead>
        <tbody id="rows">{{ROWS}}</tbody>
      </table>
    </div>
    <div class="more-wrap"><button class="more" id="more" type="button" hidden>显示更多</button></div>
    <details class="src"><summary>来源</summary><div class="src-body">{{SOURCE_NOTE}}</div></details>
  </section>
</main>

<footer class="page-foot">{{FOOT}}</footer>

<script id="runs-data" type="application/json">{{RUNS_JSON}}</script>
<script>{{JS}}</script>
</body>
</html>
"""

LIST_JS = r"""
(function () {
  'use strict';
  var RUNS = JSON.parse(document.getElementById('runs-data').textContent);
  var PAGE = {{PAGE_SIZE}};
  var NS = 'http://www.w3.org/2000/svg';
  var shown = PAGE;

  function el(tag, attrs, style) {
    var e = document.createElementNS(NS, tag);
    if (attrs) { for (var k in attrs) { if (attrs.hasOwnProperty(k)) { e.setAttribute(k, attrs[k]); } } }
    if (style) { for (var s in style) { if (style.hasOwnProperty(s)) { e.style[s] = style[s]; } } }
    return e;
  }
  function money(v) {
    var a = Math.abs(v);
    if (a >= 1e8) { return (v / 1e8).toFixed(2) + ' 亿'; }
    if (a >= 1e4) { return (v / 1e4).toFixed(2) + ' 万'; }
    return v.toFixed(0);
  }
  function tick(v) {
    var a = Math.abs(v);
    if (a >= 1e8) { return (v / 1e8).toFixed(0) + '亿'; }
    if (a >= 1e4) { return (v / 1e4).toFixed(0) + '万'; }
    return v.toFixed(0);
  }
  function niceTicks(a, b, n) {
    var span = b - a;
    if (!(span > 0)) { return [a]; }
    var step0 = span / n, mag = Math.pow(10, Math.floor(Math.log10(step0))), norm = step0 / mag, step;
    if (norm <= 1) { step = 1; } else if (norm <= 2) { step = 2; }
    else if (norm <= 2.5) { step = 2.5; } else if (norm <= 5) { step = 5; } else { step = 10; }
    step *= mag;
    var out = [];
    for (var v = Math.ceil(a / step) * step; v <= b + step * 1e-6; v += step) { out.push(v); }
    return out;
  }

  function drawNav(host, run) {
    if (!run.nav || !run.nav.length) {
      host.innerHTML = '<div class="chart-empty">该运行未保存日净值（daily_nav.csv），不绘制曲线。</div>';
      return;
    }
    var W = host.clientWidth || 560, H = 210, M = { l: 58, r: 14, t: 10, b: 24 };
    var iw = W - M.l - M.r, ih = H - M.t - M.b;
    var dates = run.nav.map(function (p) { return Date.parse(p[0] + 'T00:00:00Z'); });
    var eq = run.nav.map(function (p) { return p[1]; });
    var t0 = dates[0], t1 = dates[dates.length - 1];
    var lo = Math.min.apply(null, eq), hi = Math.max.apply(null, eq);
    var pad = (hi - lo) * 0.08 || 1;
    lo -= pad; hi += pad;
    var X = function (t) { return M.l + (t - t0) / (t1 - t0) * iw; };
    var Y = function (v) { return M.t + (1 - (v - lo) / (hi - lo)) * ih; };
    host.innerHTML = '';
    var svg = el('svg', { viewBox: '0 0 ' + W + ' ' + H, width: W, height: H, role: 'img', 'aria-label': run.name + ' 净值曲线' });

    niceTicks(lo, hi, 4).forEach(function (v) {
      var y = Y(v);
      svg.appendChild(el('line', { x1: M.l, x2: W - M.r, y1: y, y2: y, 'stroke-width': 1 }, { stroke: 'var(--grid)' }));
      var tx = el('text', { x: M.l - 8, y: y + 3.5, 'text-anchor': 'end', 'font-size': '10.5' },
        { fill: 'var(--faint)', fontFamily: 'var(--mono)' });
      tx.textContent = tick(v);
      svg.appendChild(tx);
    });
    var y0 = new Date(t0).getUTCFullYear(), y1 = new Date(t1).getUTCFullYear();
    for (var y = y0; y <= y1; y += 2) {
      var t = Date.parse(y + '-01-01T00:00:00Z');
      if (t < t0 || t > t1) { continue; }
      var x = X(t);
      svg.appendChild(el('line', { x1: x, x2: x, y1: M.t, y2: M.t + ih, 'stroke-width': 1 }, { stroke: 'var(--grid)' }));
      var lx = el('text', { x: x, y: H - 7, 'text-anchor': 'middle', 'font-size': '10.5' },
        { fill: 'var(--faint)', fontFamily: 'var(--mono)' });
      lx.textContent = y;
      svg.appendChild(lx);
    }
    var dArea = 'M' + X(t0).toFixed(2) + ' ' + (M.t + ih).toFixed(2);
    var dLine = '';
    for (var i = 0; i < dates.length; i++) {
      dLine += (i ? 'L' : 'M') + X(dates[i]).toFixed(2) + ' ' + Y(eq[i]).toFixed(2);
    }
    dArea += dLine.replace('M', 'L') + 'L' + X(t1).toFixed(2) + ' ' + (M.t + ih).toFixed(2) + 'Z';
    var gid = 'g' + run.id.replace(/[^a-zA-Z0-9]/g, '');
    var defs = el('defs');
    var grad = el('linearGradient', { id: gid, x1: '0', y1: '0', x2: '0', y2: '1' });
    grad.appendChild(el('stop', { offset: '0%', 'stop-opacity': '.18' }, { stopColor: run.color }));
    grad.appendChild(el('stop', { offset: '100%', 'stop-opacity': '.01' }, { stopColor: run.color }));
    defs.appendChild(grad); svg.appendChild(defs);
    svg.appendChild(el('path', { d: dArea, fill: 'url(#' + gid + ')' }));
    svg.appendChild(el('path', { d: dLine, fill: 'none', 'stroke-width': '1.7' }, { stroke: run.color, strokeLinejoin: 'round' }));
    var lastX = X(t1), lastY = Y(eq[eq.length - 1]);
    svg.appendChild(el('circle', { cx: lastX, cy: lastY, r: 3, 'stroke-width': 1.4 }, { fill: run.color, stroke: 'var(--panel)' }));
    var lt = el('text', { x: lastX - 6, y: lastY - 7, 'text-anchor': 'end', 'font-size': '10.5' },
      { fill: 'var(--ink)', fontFamily: 'var(--mono)' });
    lt.textContent = money(eq[eq.length - 1]);
    svg.appendChild(lt);
    host.appendChild(svg);
  }

  function renderDetail(idx) {
    var run = RUNS[idx];
    var row = document.querySelector('tr.run[data-i="' + idx + '"]');
    var det = document.querySelector('tr.detail[data-i="' + idx + '"]');
    if (!row || !det) { return; }
    var open = !det.hidden;
    if (open) {
      det.hidden = true; row.classList.remove('open');
      row.querySelector('.caret').textContent = '▸';
      row.setAttribute('aria-expanded', 'false');
      return;
    }
    det.hidden = false; row.classList.add('open');
    row.querySelector('.caret').textContent = '▾';
    row.setAttribute('aria-expanded', 'true');
    var host = det.querySelector('.nav-chart');
    if (host && !host.getAttribute('data-done')) {
      drawNav(host, run);
      host.setAttribute('data-done', '1');
    }
  }

  Array.prototype.forEach.call(document.querySelectorAll('tr.run'), function (row) {
    row.addEventListener('click', function (evt) {
      if (evt.target.closest && evt.target.closest('a')) { return; }
      renderDetail(parseInt(row.getAttribute('data-i'), 10));
    });
    row.addEventListener('keydown', function (e) {
      if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); renderDetail(parseInt(row.getAttribute('data-i'), 10)); }
    });
  });

  var q = document.getElementById('q');
  var counter = document.getElementById('count');
  var more = document.getElementById('more');
  function apply() {
    var term = (q.value || '').trim().toLowerCase();
    var matched = 0;
    RUNS.forEach(function (run, i) {
      var row = document.querySelector('tr.run[data-i="' + i + '"]');
      var det = document.querySelector('tr.detail[data-i="' + i + '"]');
      var hit = !term || run.search.toLowerCase().indexOf(term) >= 0;
      var visible = hit && matched < shown;
      if (hit) { matched += 1; }
      row.hidden = !visible;
      if (!visible) { det.hidden = true; row.classList.remove('open'); }
      det.hidden = true;
    });
    var total = RUNS.filter(function (r) { return !term || r.search.toLowerCase().indexOf(term) >= 0; }).length;
    counter.textContent = '共 ' + total + ' 条' + (term ? '（筛选自 ' + RUNS.length + ' 条）' : '');
    more.hidden = total <= shown;
    more.textContent = '显示更多（还有 ' + Math.max(0, total - shown) + ' 条）';
  }
  q.addEventListener('input', apply);
  more.addEventListener('click', function () { shown += PAGE; apply(); });
  var pending = null;
  window.addEventListener('resize', function () {
    if (pending) { return; }
    pending = requestAnimationFrame(function () {
      pending = null;
      document.querySelectorAll('.nav-chart[data-done]').forEach(function (host) {
        var idx = parseInt(host.closest('tr.detail').getAttribute('data-i'), 10);
        drawNav(host, RUNS[idx]);
      });
    });
  });
  apply();
})();
"""


# ---------------------------------------------------------------- 读取运行记录

def read_json(path: Path) -> Optional[Dict[str, Any]]:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding='utf-8'))
    except (json.JSONDecodeError, OSError):
        return None


def load_nav(path: Path, start: Optional[str], end: Optional[str]) -> List[List[Any]]:
    if not path.exists():
        return []
    rows: List[List[Any]] = []
    with path.open('r', encoding='utf-8-sig', newline='') as fh:
        header = fh.readline().strip().split(',')
        if len(header) < 2:
            return []
        for line in fh:
            parts = line.strip().split(',')
            if len(parts) < 2:
                continue
            day, eq = parts[0], parts[1]
            if start and day < start:
                continue
            if end and day > end:
                continue
            try:
                rows.append([day, int(round(float(eq)))])
            except ValueError:
                continue
    return rows


def tail_log(path: Path, lines: int = LOG_TAIL_LINES) -> List[str]:
    if not path.exists():
        return []
    try:
        text = path.read_text(encoding='utf-8', errors='replace')
    except OSError:
        return []
    out = []
    for line in text.splitlines()[-lines:]:
        line = line.rstrip()
        out.append(line[:LOG_LINE_MAX] + ('…' if len(line) > LOG_LINE_MAX else ''))
    return out


def recipe_summary(spec: Optional[Dict[str, Any]]) -> str:
    if not spec:
        return '未记录'
    parts = []
    universe = spec.get('universe') or {}
    if universe.get('mode') == 'all_market':
        parts.append('全市场')
    elif universe.get('codes'):
        parts.append(f'指定 {len(universe["codes"])} 只')
    elif universe.get('mode'):
        parts.append(str(universe['mode']))
    for ex in spec.get('exclusions') or []:
        name = ex.get('name')
        params = ex.get('params') or {}
        if name == 'exclude_vol_high_pct':
            pct = params.get('pct')
            parts.append(f'剔高波 {int(float(pct) * 100)}%（{params.get("window")} 日）' if pct is not None else '剔高波')
        elif name == 'exclude_new_listing':
            parts.append(f'剔次新（<{params.get("min_trading_days")} 日）')
        else:
            parts.append(str(name))
    factor = spec.get('factor')
    if isinstance(factor, dict) and factor.get('name'):
        parts.append(f'因子 {factor["name"]}')
    else:
        parts.append('无因子')
    freq = (spec.get('rebalance') or {}).get('frequency')
    parts.append({'monthly': '月频', 'weekly': '周频', 'daily': '日频'}.get(freq, str(freq) if freq else '频率未记录'))
    return ' · '.join(parts)


def status_of(meta: Dict[str, Any], progress: Optional[Dict[str, Any]]) -> str:
    raw = str(meta.get('status') or (progress or {}).get('status') or '').lower()
    return STATUS_MAP.get(raw, (raw or '未记录', ''))[0]


def status_class(meta: Dict[str, Any], progress: Optional[Dict[str, Any]]) -> str:
    raw = str(meta.get('status') or (progress or {}).get('status') or '').lower()
    return STATUS_MAP.get(raw, (raw, ''))[1]


def pct(v: Any) -> str:
    if v is None:
        return '<span class="muted">未记录</span>'
    return f'{float(v) * 100:.2f}%'


def num(v: Any, unit: str = '') -> str:
    if v is None:
        return '<span class="muted">未记录</span>'
    return f'{float(v):.2f}{unit}'


def short(h: Optional[str], n: int = 8) -> str:
    return str(h)[:n] if h else '未记录'


def esc(v: Any) -> str:
    return html.escape(str(v), quote=True)


def fmt_dt(s: Optional[str]) -> str:
    if not s:
        return '未记录'
    return str(s).replace('T', ' ')[:16]


def kv_rows(items: List[tuple]) -> str:
    out = []
    for k, v in items:
        out.append(f'<dt>{esc(k)}</dt><dd>{v if str(v).startswith("<span") else esc(v)}</dd>')
    return ''.join(out)


def render_detail(run: Dict[str, Any], idx: int) -> str:
    spec = run['spec'] or {}
    metrics = (run['metrics'] or {}).get('metrics') or {}
    repro = (run['metrics'] or {}).get('repro') or {}
    progress = run['progress'] or {}
    status = status_of(run['index'], progress)
    cls = status_class(run['index'], progress)

    universe = spec.get('universe') or {}
    exclusions = '；'.join(
        f'{e.get("name")} {json.dumps(e.get("params") or {}, ensure_ascii=False)}'
        for e in (spec.get('exclusions') or [])) or '未记录'
    rebal = spec.get('rebalance') or {}
    cost = spec.get('cost') or {}
    portfolio = spec.get('portfolio') or {}
    spec_rows = kv_rows([
        ('股票池', '全市场' if universe.get('mode') == 'all_market'
         else (f'指定 {len(universe.get("codes") or [])} 只' if universe.get('codes') else '未记录')),
        ('剔除链', exclusions),
        ('因子', (spec.get('factor') or {}).get('name') if isinstance(spec.get('factor'), dict) else '无因子'),
        ('调仓', f'{rebal.get("frequency", "未记录")}，首次 {rebal.get("first_date", "未记录")}'),
        ('成本', f'滑点 {cost.get("slippage_bps", "未记录")}bp · 印花税 {cost.get("stamp_tax_basis", "未记录")} · '
                 f'佣金 {cost.get("commission_rate", "未记录")} · 冲击 {cost.get("impact_coef", "未记录")}'),
        ('资金', f'{float(portfolio.get("initial_capital", 0)) / 1e8:g} 亿 · 每手 {portfolio.get("lot_size", "未记录")} · '
                 f'现金缓冲 {portfolio.get("cash_buffer", "未记录")}'),
        ('指标补充', f'期末权益 {money_text(metrics.get("final_equity"))} · 交易 {metrics.get("trades", "未记录")} 笔 · '
                     f'调仓 {metrics.get("rebalances", "未记录")} 次'),
        ('耗时', f'{((run["metrics"] or {}).get("timing") or {}).get("elapsed_seconds", "未记录")} 秒'
                 f'（引擎 {((run["metrics"] or {}).get("timing") or {}).get("engine_seconds", "未记录")} 秒）'),
    ])
    code = ((repro.get('code_head') or {}).get('full') or run['index'].get('code_head') or '')
    script = repro.get('script') or {}
    data = repro.get('data_version') or {}
    repro_rows = kv_rows([
        ('代码 HEAD', f'<span class="hash8">{esc(short(code, 12))}</span>' +
                      (f' <span class="muted">{esc(str(code)[12:])}</span>' if code and len(str(code)) > 12 else '')),
        ('脚本 sha256', (f'{esc(script.get("path", ""))} · <span class="hash8">{esc(short(script.get("sha256"), 16))}</span>'
                        if script.get('sha256') else f'<span class="hash8">{esc(short(run["index"].get("script_sha256"), 16))}</span>')),
        ('数据版本', f'<span class="hash8">{esc(short(data.get("parquet_sha256") or run["index"].get("data_version_sha256"), 16))}</span>'),
        ('数据清单', f'{esc(data.get("manifest", "未记录"))} · <span class="hash8">{esc(short(data.get("manifest_sha256"), 12))}</span>'),
        ('配方 sha256', f'<span class="hash8">{esc(short(repro.get("recipe_sha256") or run["index"].get("recipe_sha256"), 16))}</span>'),
        ('规格 sha256', f'<span class="hash8">{esc(short(repro.get("spec_sha256") or run["index"].get("spec_sha256"), 16))}</span>'),
    ])

    extras = []
    if progress:
        pctv = progress.get('percent')
        mw = progress.get('metrics_window') or {}
        prov = ' <span class="tag-prov">provisional</span>' if mw.get('provisional') else ''
        extras.append(
            '<div class="detail-panel full"><h4>进度</h4>'
            f'<div class="bar"><i style="width:{float(pctv or 0):.1f}%"></i></div>'
            f'<div class="kv" style="grid-template-columns:auto 1fr">'
            f'<dt>进度</dt><dd>{float(pctv or 0):.1f}% · {progress.get("processed_days", "未记录")}/'
            f'{progress.get("total_days", "未记录")} 个交易日 · 当前 {progress.get("current_date", "未记录")}</dd>'
            f'<dt>已算区间年化</dt><dd>{pct(mw.get("annual"))}{prov} · 回撤 {pct(mw.get("max_drawdown"))}</dd>'
            '</div></div>')
    if cls == 'fail' and run['log_tail']:
        extras.append(
            '<div class="detail-panel full"><h4>失败日志（末 ' + str(len(run['log_tail'])) + ' 行）</h4>'
            '<details class="recipe-box"><summary>展开 / 收起 run.log 末尾</summary>'
            f'<pre class="logtail">{esc(chr(10).join(run["log_tail"]))}</pre></details></div>')
    recipe_pre = esc(run['recipe']) if run['recipe'] else '该运行未记录 recipe.txt。'
    return (
        f'<tr class="detail" data-i="{idx}" hidden><td colspan="9"><div class="detail-box">'
        '<div class="detail-panel"><h4>净值曲线</h4><div class="nav-chart"></div></div>'
        f'<div class="detail-panel"><h4>规格快照（spec.json）</h4><dl class="kv">{spec_rows}</dl></div>'
        f'<div class="detail-panel full"><h4>复现指纹</h4><dl class="kv">{repro_rows}</dl></div>'
        + ''.join(extras) +
        '<div class="detail-panel full">'
        '<details class="recipe-box"><summary>查看原配方（recipe.txt 原文）</summary>'
        f'<pre>{recipe_pre}</pre></details></div>'
        '</div></td></tr>'
    )


def money_text(v: Any) -> str:
    if v is None:
        return '未记录'
    return f'{float(v) / 1e8:.2f} 亿'


def render_row(run: Dict[str, Any], idx: int) -> str:
    meta = run['index']
    metrics = meta.get('metrics') or (run['metrics'] or {}).get('metrics') or {}
    status = status_of(meta, run['progress'])
    cls = status_class(meta, run['progress'])
    search = ' '.join([str(meta.get('name') or ''), str(meta.get('created_at') or ''),
                       str(meta.get('run_id') or ''), status])
    name_html = esc(meta.get("name") or "未记录")
    if run.get('dashboard_url'):
        name_html = f'<a class="rlink" href="{esc(run["dashboard_url"])}">{name_html}</a>'
    return (
        f'<tr class="run" data-i="{idx}" data-search="{esc(search)}" tabindex="0" aria-expanded="false">'
        f'<td><span class="rname"><span class="caret">▸</span>{name_html}</span>'
        f'<span class="rid">{esc(meta.get("run_id") or "未记录")}</span></td>'
        f'<td class="num" data-label="日期时间">{esc(fmt_dt(meta.get("created_at")))}</td>'
        f'<td class="recipe" data-label="配方摘要" title="{esc(run["summary"])}">{esc(run["summary"])}</td>'
        f'<td class="num" data-label="年化">{pct(metrics.get("annual"))}</td>'
        f'<td class="num" data-label="最大回撤">{pct(metrics.get("max_drawdown"))}</td>'
        f'<td class="num" data-label="年换手">{num(metrics.get("turnover_two_way"), "x")}</td>'
        f'<td class="num" data-label="成本占比">{pct(metrics.get("cost_share_of_gross"))}</td>'
        f'<td data-label="状态"><span class="status {cls}">{esc(status)}</span></td>'
        f'<td class="num hash8" data-label="数据版本" title="{esc(meta.get("data_version_sha256") or "")}">'
        f'{esc(short(meta.get("data_version_sha256")))}</td>'
        '</tr>'
    ) + render_detail(run, idx)


def main() -> None:
    ap = argparse.ArgumentParser(description='回测运行清单页生成器（单文件 HTML）')
    ap.add_argument('--runs', default='_staging/backtest_runs/index.json', help='index.json 路径')
    ap.add_argument('--out', help='输出 HTML 路径')
    ap.add_argument('--title', default='回测运行清单')
    ap.add_argument('--dashboard', default='回测结果看板_价量估值等权_2026-11-14.html',
                    help='看板页文件名或相对链接，用于互链')
    ap.add_argument('--dashboard-pattern', default='',
                    help='每行看板链接模板，用 {run_id} 占位；留空则不渲染行内链接')
    args = ap.parse_args()

    index_path = Path(args.runs)
    if not index_path.is_absolute():
        index_path = ROOT / index_path
    index = json.loads(index_path.read_text(encoding='utf-8'))
    runs_meta = index.get('runs') or []
    if not runs_meta:
        raise SystemExit(f'index.json 中没有 runs：{index_path}')
    base_dir = index_path.parent

    runs: List[Dict[str, Any]] = []
    for meta in runs_meta:
        rel_dir = meta.get('relative_dir') or Path(str(meta.get('dir') or '')).name
        run_dir = base_dir / rel_dir
        spec = read_json(run_dir / 'spec.json')
        metrics = read_json(run_dir / 'metrics.json')
        progress = read_json(run_dir / 'progress.json')
        period = (spec or {}).get('period') or meta.get('period') or {}
        package = {
            'index': meta,
            'spec': spec,
            'metrics': metrics,
            'progress': progress,
            'recipe': (run_dir / 'recipe.txt').read_text(encoding='utf-8', errors='replace')
            if (run_dir / 'recipe.txt').exists() else '',
            'log_tail': tail_log(run_dir / 'run.log'),
            'summary': recipe_summary(spec),
            'nav': load_nav(run_dir / 'daily_nav.csv', period.get('start'), period.get('end')),
        }
        runs.append(package)

    runs.sort(key=lambda r: str(r['index'].get('created_at') or ''), reverse=True)
    if args.dashboard_pattern:
        for run in runs:
            rid = str(run['index'].get('run_id') or '')
            if rid:
                run['dashboard_url'] = args.dashboard_pattern.replace('{run_id}', rid)

    rows = []
    payload = []
    for idx, run in enumerate(runs):
        rows.append(render_row(run, idx))
        payload.append({
            'id': str(run['index'].get('run_id') or idx),
            'name': str(run['index'].get('name') or ''),
            'color': PALETTE[idx % len(PALETTE)],
            'nav': run['nav'],
            'search': ' '.join([str(run['index'].get('name') or ''),
                                str(run['index'].get('created_at') or ''),
                                str(run['index'].get('run_id') or '')]),
        })

    counts: Dict[str, int] = {}
    for run in runs:
        label = status_of(run['index'], run['progress'])
        counts[label] = counts.get(label, 0) + 1
    dist = ' / '.join(f'{k} {v}' for k, v in sorted(counts.items()))
    index_sha = hashlib.sha256(index_path.read_bytes()).hexdigest()[:8]
    first_nav = next((r for r in runs if r['nav']), None)
    meta_bits = [
        f'运行 <b>{len(runs)}</b> 条',
        f'状态 <b>{esc(dist)}</b>',
        f'索引更新 <b>{esc(fmt_dt(index.get("updated_at")))}</b>',
        f'索引 sha256 <b>{index_sha}</b>',
    ]
    if first_nav:
        nav_rows = sum(len(r['nav']) for r in runs)
        meta_bits.append(f'净值序列 <b>{nav_rows:,}</b> 点')

    anchors = ['<a href="' + esc(args.dashboard) + '">回结果看板</a>',
               '<a href="#runs">运行列表</a>']
    source_note = (f'索引：{index_path.relative_to(ROOT).as_posix() if index_path.is_relative_to(ROOT) else index_path} · '
                   f'sha256 <span class="hash">{index_sha}</span> · 更新 {fmt_dt(index.get("updated_at"))}<br>'
                   f'各 run 目录只读读取 spec.json / recipe.txt / metrics.json / daily_nav.csv / run.log / progress.json；'
                   '缺失字段显示「未记录」，未做任何补算。')

    html_text = LIST_TEMPLATE
    html_text = html_text.replace('{{TITLE}}', args.title)
    html_text = html_text.replace('{{H1}}', args.title)
    html_text = html_text.replace('{{META}}', ''.join(f'<span>{b}</span>' for b in meta_bits))
    html_text = html_text.replace('{{ANCHORS}}', ''.join(anchors))
    html_text = html_text.replace('{{DASHBOARD}}', esc(args.dashboard))
    html_text = html_text.replace('{{ROWS}}', ''.join(rows))
    html_text = html_text.replace('{{SOURCE_NOTE}}', source_note)
    html_text = html_text.replace('{{FOOT}}', f'{args.title} · 生成于 {date.today().isoformat()} · 只读展示')
    html_text = html_text.replace('{{RUNS_JSON}}',
                                  json.dumps(payload, ensure_ascii=False, separators=(',', ':')))
    html_text = html_text.replace('{{JS}}', LIST_JS.replace('{{PAGE_SIZE}}', str(PAGE_SIZE)))
    html_text = html_text.replace('{{STYLE}}', CSS_TEXT + LIST_CSS)

    out = Path(args.out) if args.out else ROOT / 'docs' / f'回测运行清单_{date.today().isoformat()}.html'
    if not out.is_absolute():
        out = ROOT / out
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(html_text, encoding='utf-8', newline='\n')
    total_nav = sum(len(r['nav']) for r in runs)
    print(f'已生成 {out}（{len(html_text.encode("utf-8")):,} 字节，{len(runs)} 个 run，'
          f'状态 {dist}，净值 {total_nav:,} 点）')


if __name__ == '__main__':
    sys.exit(main())
