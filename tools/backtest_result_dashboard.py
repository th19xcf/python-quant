#!/usr/bin/env python3
"""回测结果看板生成器：结果 JSON（+ 可选净值 CSV）→ 单文件 HTML。

只读消费侧工具：不跑回测、不写 data/，只把既有结果渲染成可双击打开的页面。

输入契约
--------
必需：一份结果 JSON，支持两种形态。

A. 本族基线 JSON（docs/基线_*.json）
   - ``metrics_10bp``   v1 指标
   - ``v2.m10bp`` / ``v2.seg`` / ``v2.seg_cols`` / ``v2.real_tax``
   - ``v3.m10bp`` / ``v3.vs``
   - ``real_tax`` / ``cost`` / ``data`` / ``command`` / ``head`` ...
   缺少的版本或字段整块跳过，不补空值。

B. 通用形态
   ``{"title": ..., "versions": [{"id": "v1", "name": ..., "role": ...,
   "metrics": {...}, "segments": {"2010-2016": {...}}, "real_tax": {...}}]}``

可选：净值 CSV（``--equity v1=path.csv``，可重复），列名 ``date,equity``。
没有净值 CSV 时自动跳过净值图与回撤图，其余照常生成。

页面结构（结论在前，越往下越细）
--------------------------------
窄标题栏 → 锚点条（结论/曲线/分段/明细/来源）→ 四张指标卡（三版并排）
→ 净值 + 回撤双图（共用时间轴、十字线联动）→ 分段与增量 → 税制对照
→ 五分位/明细 → 来源脚注（折叠）。

用法
----
    .venv\\Scripts\\python.exe tools\\backtest_result_dashboard.py \\
        --json docs\\基线_价量估值等权_2026-11-13.json \\
        --equity v1=_staging\\baseline_pv_val_equal_weight_equity.csv \\
        --title "价量 + 估值等权基线：回测结果看板" \\
        --out docs\\回测结果看板_价量估值等权_2026-11-14.html
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import re
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

ROOT = Path(__file__).resolve().parents[1]

# Okabe-Ito 色盲友好配色；通过 CSS 变量穿透到深/浅色主题
PALETTE = ['var(--v1)', 'var(--v2)', 'var(--v3)', 'var(--v4)', 'var(--v5)', 'var(--v6)']

DEFAULT_LABELS: Dict[str, Dict[str, str]] = {
    'v1': {'name': '价量 + 估值等权', 'role': '策略对比对照', 'hero': '对照', 'seg': '对照'},
    'v2': {'name': '剔 60 日波动率最高 20%', 'role': '改进构造对照', 'hero': '改进构造', 'seg': '剔高波 20%'},
    'v3': {'name': '剔高波 + 次新', 'role': '现基线', 'hero': '现基线', 'seg': '剔高波 + 次新'},
}

# 首屏固定四项指标：年化、最大回撤、年换手、成本占毛收益
METRIC_CARDS: List[Tuple[str, str, str]] = [
    ('annual', '年化收益', 'pct'),
    ('max_drawdown', '最大回撤', 'pct'),
    ('turnover_two_way', '年换手（双边）', 'x'),
    ('cost_share_of_gross', '成本占毛收益', 'pct'),
]

CSS_TEXT = r"""  :root{
    --bg:#f5f7fa; --panel:#ffffff; --ink:#15202f; --muted:#657184; --faint:#93a0b3;
    --line:#e2e7ef; --line2:#edf0f6;
    --v1:#0072b2; --v2:#e69f00; --v3:#009e73; --v4:#cc79a7; --v5:#56b4e9; --v6:#8b6cc9;
    --neg:#c8453f; --pos:#17835f;
    --grid:#eef1f7; --axis:#cbd4e1;
    --shadow:0 1px 2px rgba(16,32,64,.05),0 10px 26px rgba(16,32,64,.06);
    --mono:ui-monospace,SFMono-Regular,Menlo,Consolas,"Cascadia Mono",monospace;
  }
  /* 深色令牌只在 DARK_DECLS 里写一份：系统深色与显式 data-theme="dark" 共用 */
  @media (prefers-color-scheme: dark){
    :root{__DARK__}
  }
  html[data-theme="dark"]{__DARK__}
  *{box-sizing:border-box}
  html{scroll-behavior:smooth}
  html,body{margin:0;padding:0}
  body{
    background:var(--bg); color:var(--ink);
    font-family:"Microsoft YaHei","PingFang SC","Hiragino Sans GB","Segoe UI",system-ui,-apple-system,sans-serif;
    font-size:14px; line-height:1.55; -webkit-font-smoothing:antialiased;
  }
  a{color:var(--v1)}
  code{font-family:var(--mono);font-size:11.5px}

  /* 顶部窄标题栏 */
  .topbar{background:var(--panel);border-bottom:1px solid var(--line)}
  .topbar-in{
    max-width:1220px;margin:0 auto;padding:9px 20px;
    display:flex;align-items:center;gap:16px;flex-wrap:wrap;
  }
  .topbar h1{font-size:15.5px;margin:0;font-weight:650;letter-spacing:.01em}
  .topbar .meta{display:flex;gap:14px;flex-wrap:wrap;font-size:12px;color:var(--muted)}
  .topbar .meta span{white-space:nowrap}
  .topbar .meta b{font-family:var(--mono);font-weight:600;color:var(--ink)}

  /* 锚点条 */
  .navbar{
    position:sticky;top:0;z-index:30;background:var(--panel);
    border-bottom:1px solid var(--line);box-shadow:0 2px 10px rgba(16,32,64,.04);
  }
  .navbar-in{
    max-width:1220px;margin:0 auto;padding:0 14px;display:flex;gap:2px;overflow-x:auto;
  }
  .navbar a{
    font-size:13px;color:var(--muted);text-decoration:none;padding:8px 12px;
    border-bottom:2px solid transparent;white-space:nowrap;
  }
  .navbar a:hover{color:var(--ink)}
  .navbar a.on{color:var(--ink);border-bottom-color:var(--v1);font-weight:650}
  .navbar a.back{color:var(--muted);border-right:1px solid var(--line);margin-right:6px;padding-right:14px}

  main{max-width:1220px;margin:0 auto;padding:16px 20px 40px}
  section{margin:0 0 26px;scroll-margin-top:50px}
  .sec-head{display:flex;align-items:baseline;gap:12px;flex-wrap:wrap;margin:0 0 10px}
  .sec-head h2{font-size:16px;margin:0;font-weight:650}
  .sec-head .hint{font-size:12.5px;color:var(--muted)}
  .panel{
    background:var(--panel);border:1px solid var(--line);border-radius:12px;
    box-shadow:var(--shadow);padding:14px 16px;
  }

  /* 指标卡：四张并排，每张内三版同宽 */
  .mcards{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:12px}
  .mcard{
    background:var(--panel);border:1px solid var(--line);border-radius:12px;
    box-shadow:var(--shadow);padding:11px 13px 10px;
  }
  .mname{font-size:12px;color:var(--muted);display:flex;justify-content:space-between;gap:6px;margin-bottom:7px}
  .mname .dim{font-family:var(--mono);font-size:10.5px}
  .mrow{display:grid;gap:8px}
  .mcell{min-width:0}
  .mtag{
    display:inline-flex;align-items:center;gap:5px;font-family:var(--mono);
    font-size:10.5px;color:var(--muted);margin-bottom:2px;
  }
  .mtag i{width:8px;height:8px;border-radius:50%;background:var(--c);display:inline-block}
  .mval{
    font-family:var(--mono);font-size:25px;font-weight:700;letter-spacing:-.03em;
    line-height:1.1;white-space:nowrap;
  }
  .mval small{font-size:11.5px;font-weight:600;color:var(--muted);margin-left:1px;letter-spacing:0}
  .mval.dim{font-size:13px;font-weight:500;letter-spacing:0}
  .msub{font-family:var(--mono);font-size:10.5px;color:var(--faint);margin-top:2px;white-space:nowrap}
  .neg{color:var(--neg)}

  /* 结论补充小表 */
  table.mini{width:100%;border-collapse:collapse;font-size:12.5px}
  table.mini th,table.mini td{padding:6px 10px;text-align:right;border-bottom:1px solid var(--line2)}
  table.mini th:first-child,table.mini td:first-child{text-align:left}
  table.mini thead th{color:var(--muted);font-weight:600;font-size:11.5px;border-bottom:1px solid var(--line)}
  table.mini td.num{font-family:var(--mono);font-variant-numeric:tabular-nums}

  .note-box{
    margin-top:10px;border:1px solid var(--line);background:var(--line2);border-radius:9px;
    padding:8px 12px;font-size:12.5px;color:var(--muted);
  }
  .note-box b{color:var(--ink)}
  .info-box{
    border:1px solid var(--line);background:var(--line2);border-radius:9px;padding:8px 12px;
    font-size:12.5px;color:var(--muted);
  }
  .info-box b{color:var(--ink)}

  /* 图表 */
  .toolbar{display:flex;align-items:center;justify-content:space-between;gap:10px;flex-wrap:wrap;margin-bottom:6px}
  .legend{display:flex;align-items:center;gap:14px;font-size:12.5px;color:var(--muted);flex-wrap:wrap}
  .legend i{display:inline-block;width:14px;height:3px;border-radius:2px;vertical-align:middle;margin-right:6px}
  .switch{display:inline-flex;border:1px solid var(--line);border-radius:8px;overflow:hidden;background:var(--bg)}
  .switch button{
    border:0;background:transparent;color:var(--muted);font:inherit;font-size:12px;
    padding:5px 12px;cursor:pointer;
  }
  .switch button[aria-pressed="true"]{background:var(--panel);color:var(--ink);font-weight:650;box-shadow:0 1px 3px rgba(16,32,64,.14)}
  .chart-wrap{position:relative;width:100%}
  .chart-wrap svg{display:block;width:100%}
  .chart-cap{font-size:11.5px;color:var(--faint);margin:2px 0 0}
  #tip{
    position:fixed;pointer-events:none;z-index:40;background:rgba(16,24,38,.95);color:#fff;
    border-radius:8px;padding:7px 10px;font-size:12px;line-height:1.55;white-space:nowrap;
    box-shadow:0 6px 20px rgba(10,20,40,.3);opacity:0;transform:translate(-50%,-118%);
    transition:opacity .08s ease;
  }
  #tip b{font-family:var(--mono);font-weight:650}

  /* 表格 */
  .tbl-wrap{overflow-x:auto}
  table{width:100%;border-collapse:collapse;font-size:13px}
  th,td{padding:7px 9px;text-align:right;border-bottom:1px solid var(--line2);vertical-align:top}
  th:first-child,td:first-child{text-align:left}
  thead th{color:var(--muted);font-weight:600;font-size:11.5px;border-bottom:1px solid var(--line)}
  tbody tr.grp td{
    background:var(--line2);color:var(--ink);font-weight:650;font-size:12px;
    text-align:left;border-bottom:1px solid var(--line);
  }
  td.num{font-family:var(--mono);font-variant-numeric:tabular-nums}
  td .small{display:block;font-family:var(--mono);font-size:10.5px;color:var(--faint);margin-top:1px}
  .delta-pos{color:var(--pos)}
  .delta-neg{color:var(--neg)}
  .dim{color:var(--faint)}
  tbody tr.extra{display:none}
  table.expanded tbody tr.extra{display:table-row}
  tbody tr.more-row td{padding:6px 9px;border-bottom:0;text-align:left}
  button.more{
    border:1px solid var(--line);background:var(--panel);color:var(--muted);font:inherit;
    font-size:12px;padding:3px 10px;border-radius:7px;cursor:pointer;
  }
  button.more:hover{color:var(--ink)}

  .two-col{display:grid;grid-template-columns:1.3fr 1fr;gap:12px;align-items:start}

  /* 来源：折叠小图标 */
  details.src{margin-top:10px;font-size:11.5px;color:var(--faint)}
  details.src summary{
    cursor:pointer;list-style:none;display:inline-flex;align-items:center;gap:5px;
    font-family:var(--mono);font-size:11px;color:var(--faint);
    border:1px solid var(--line);border-radius:999px;padding:1px 9px;
  }
  details.src summary::-webkit-details-marker{display:none}
  details.src summary::before{content:"ⓘ";font-size:11px}
  details.src[open] summary{color:var(--muted)}
  details.src .src-body{margin-top:7px;line-height:1.75;word-break:break-all}
  details.src .hash{font-family:var(--mono);color:var(--v1)}

  /* 补充口径折叠：首屏只留结论 */
  details.ext{margin-top:8px}
  details.ext summary{
    cursor:pointer;list-style:none;display:inline-flex;align-items:center;gap:6px;
    font-size:12px;color:var(--muted);border:1px solid var(--line);
    border-radius:999px;padding:2px 10px;background:var(--panel);
  }
  details.ext summary::-webkit-details-marker{display:none}
  details.ext summary::before{content:"＋";font-family:var(--mono);font-size:11px}
  details.ext[open] summary::before{content:"－"}
  details.ext .ext-body{margin-top:8px}

  .src-list{margin:0;padding-left:18px;font-size:12.5px;color:var(--muted)}
  .src-list li{margin:6px 0}
  .src-list code{background:var(--line2);border-radius:4px;padding:1px 5px}
  .src-list .hash{font-family:var(--mono);color:var(--v1)}
  footer.page-foot{max-width:1220px;margin:0 auto;padding:0 20px 30px;color:var(--faint);font-size:11.5px}

  @media (max-width:1000px){
    .mcards{grid-template-columns:repeat(2,minmax(0,1fr))}
    .two-col{grid-template-columns:1fr}
  }
  @media (max-width:620px){
    .mcards{grid-template-columns:1fr}
    .mval{font-size:22px}
  }"""

JS_TEXT = r"""
(function () {
  'use strict';
  var SERIES = JSON.parse(document.getElementById('series-data').textContent);
  var NS = 'http://www.w3.org/2000/svg';

  /* ---------- 通用 ---------- */
  function el(tag, attrs, style) {
    var e = document.createElementNS(NS, tag);
    if (attrs) {
      for (var k in attrs) {
        if (Object.prototype.hasOwnProperty.call(attrs, k)) { e.setAttribute(k, attrs[k]); }
      }
    }
    if (style) {
      for (var s in style) {
        if (Object.prototype.hasOwnProperty.call(style, s)) { e.style[s] = style[s]; }
      }
    }
    return e;
  }
  function pct(v, d) { return (v * 100).toFixed(d === undefined ? 2 : d) + '%'; }
  function money(v) {
    var a = Math.abs(v);
    if (a >= 1e8) { return (v / 1e8).toFixed(2) + ' 亿'; }
    if (a >= 1e4) { return (v / 1e4).toFixed(2) + ' 万'; }
    return v.toFixed(0);
  }
  function tickLabel(v) {
    var a = Math.abs(v);
    if (a >= 1e8) { return (v / 1e8).toFixed(0) + '亿'; }
    if (a >= 1e4) { return (v / 1e4).toFixed(0) + '万'; }
    return v.toFixed(0);
  }
  function clamp(x, a, b) { return Math.max(a, Math.min(b, x)); }
  function niceTicks(a, b, n) {
    var span = b - a;
    if (!(span > 0)) { return [a]; }
    var step0 = span / n;
    var mag = Math.pow(10, Math.floor(Math.log10(step0)));
    var norm = step0 / mag, step;
    if (norm <= 1) { step = 1; } else if (norm <= 2) { step = 2; }
    else if (norm <= 2.5) { step = 2.5; } else if (norm <= 5) { step = 5; } else { step = 10; }
    step *= mag;
    var out = [];
    for (var v = Math.ceil(a / step) * step; v <= b + step * 1e-6; v += step) { out.push(v); }
    return out;
  }
  function logTicks(lo, hi) {
    var mults = [1, 1.5, 2, 2.5, 3, 4, 5, 6, 8];
    var out = [];
    for (var d = Math.floor(Math.log10(lo)) - 1; d <= Math.ceil(Math.log10(hi)); d++) {
      for (var m = 0; m < mults.length; m++) {
        var v = mults[m] * Math.pow(10, d);
        if (v >= lo && v <= hi) { out.push(v); }
      }
    }
    out.sort(function (a, b) { return a - b; });
    return out;
  }
  function nearestIn(s, t) {
    var lo = 0, hi = s.ts.length - 1;
    while (lo < hi) {
      var mid = (lo + hi) >> 1;
      if (s.ts[mid] < t) { lo = mid + 1; } else { hi = mid; }
    }
    if (lo > 0 && Math.abs(s.ts[lo - 1] - t) < Math.abs(s.ts[lo] - t)) { return lo - 1; }
    return lo;
  }

  /* ---------- 表格展开/收起 ---------- */
  Array.prototype.forEach.call(document.querySelectorAll('table[data-collapse]'), function (tb) {
    var n = parseInt(tb.getAttribute('data-collapse'), 10) || 6;
    if (!tb.tBodies.length || !tb.tHead) { return; }
    var rows = Array.prototype.slice.call(tb.tBodies[0].rows);
    if (rows.length <= n + 1) { return; }
    rows.slice(n).forEach(function (r) { r.classList.add('extra'); });
    var tr = document.createElement('tr');
    tr.className = 'more-row';
    var td = document.createElement('td');
    td.colSpan = tb.tHead.rows[0].cells.length;
    var btn = document.createElement('button');
    btn.type = 'button';
    btn.className = 'more';
    btn.textContent = '展开全部（+' + (rows.length - n) + ' 行）';
    btn.addEventListener('click', function () {
      var open = tb.classList.toggle('expanded');
      btn.textContent = open ? '收起' : ('展开全部（+' + (rows.length - n) + ' 行）');
    });
    td.appendChild(btn);
    tr.appendChild(td);
    tb.tBodies[0].appendChild(tr);
  });

  /* ---------- 锚点条高亮 ---------- */
  (function () {
    var links = Array.prototype.slice.call(document.querySelectorAll('.navbar a[href^="#"]'));
    if (!links.length) { return; }
    var targets = links.map(function (a) { return document.getElementById(a.getAttribute('href').slice(1)); });
    function sync() {
      var best = 0, bestTop = -Infinity;
      targets.forEach(function (t, i) {
        if (!t) { return; }
        var top = t.getBoundingClientRect().top - 60;
        if (top <= 0 && top > bestTop) { bestTop = top; best = i; }
      });
      links.forEach(function (a, i) { a.classList.toggle('on', i === best); });
    }
    window.addEventListener('scroll', sync, { passive: true });
    sync();
  })();

  /* ---------- 图表 ---------- */
  if (!SERIES.length) { return; }

  var T0 = Infinity, T1 = -Infinity, ALL_MIN = Infinity, ALL_MAX = -Infinity, DD_MIN = 0;
  SERIES.forEach(function (s) {
    s.ts = [];
    for (var i = 0; i < s.dates.length; i++) { s.ts.push(Date.parse(s.dates[i] + 'T00:00:00Z')); }
    s.dd = [];
    var peak = -Infinity, pi = 0;
    s.mdd = 0; s.peakIdx = 0; s.troughIdx = 0;
    for (var j = 0; j < s.equity.length; j++) {
      var e = s.equity[j];
      if (e > peak) { peak = e; pi = j; }
      var v = e / peak - 1;
      s.dd.push(v);
      if (v < s.mdd) { s.mdd = v; s.peakIdx = pi; s.troughIdx = j; }
      if (e < ALL_MIN) { ALL_MIN = e; }
      if (e > ALL_MAX) { ALL_MAX = e; }
      if (v < DD_MIN) { DD_MIN = v; }
    }
    s.peakT = s.ts[s.peakIdx];
    s.troughT = s.ts[s.troughIdx];
    T0 = Math.min(T0, s.ts[0]);
    T1 = Math.max(T1, s.ts[s.ts.length - 1]);
  });
  var DEEPEST = SERIES.slice().sort(function (a, b) { return a.mdd - b.mdd; })[0];

  var navWrap = document.getElementById('navWrap');
  var ddWrap = document.getElementById('ddWrap');
  var tip = document.createElement('div');
  tip.id = 'tip';
  document.body.appendChild(tip);

  var logScale = false;
  var charts = [];
  var M = { l: 60, r: 16, t: 12, b: 26 };

  function navH() {
    var wrap = document.getElementById('navWrap');
    if (!wrap) { return 320; }
    var top = wrap.getBoundingClientRect().top + (window.pageYOffset || 0);
    return Math.max(200, Math.min(400, Math.round(window.innerHeight - top - 28)));
  }
  function ddH() { return Math.max(110, Math.min(200, Math.round(navH() * 0.48))); }

  function yearTicks(width) {
    var every = width < 620 ? 4 : 2;
    var y0 = new Date(T0).getUTCFullYear();
    var y1 = new Date(T1).getUTCFullYear();
    var out = [];
    for (var y = y0; y <= y1; y++) { if ((y - y0) % every === 0) { out.push(Date.parse(y + '-01-01T00:00:00Z')); } }
    return out;
  }

  function makeChart(kind) {
    var wrap = kind === 'nav' ? navWrap : ddWrap;
    var host = document.getElementById(kind + 'Chart');
    if (!wrap || !host) { return null; }
    var W = wrap.clientWidth;
    if (!W) { return null; }
    var H = kind === 'nav' ? navH() : ddH();
    var iw = W - M.l - M.r, ih = H - M.t - M.b;
    host.innerHTML = '';
    var svg = el('svg', {
      viewBox: '0 0 ' + W + ' ' + H, width: W, height: H, role: 'img',
      'aria-label': kind === 'nav' ? '净值曲线' : '回撤曲线'
    });
    var X = function (t) { return M.l + (t - T0) / (T1 - T0) * iw; };

    var lo, hi, yv;
    if (kind === 'nav') {
      var pad = (ALL_MAX - ALL_MIN) * 0.07;
      lo = ALL_MIN - pad; hi = ALL_MAX + pad;
      yv = function (v) { return logScale ? Math.log10(v) : v; };
    } else {
      lo = DD_MIN * 1.18; hi = 0;
      yv = function (v) { return v; };
    }
    var y0 = yv(lo), y1 = yv(hi);
    var Y = function (v) { return M.t + (1 - (yv(v) - y0) / (y1 - y0)) * ih; };

    /* 最深回撤区间底色 */
    svg.appendChild(el('rect', {
      x: X(DEEPEST.peakT), y: M.t,
      width: Math.max(1, X(DEEPEST.troughT) - X(DEEPEST.peakT)), height: ih
    }, { fill: 'rgba(200,69,63,.06)' }));

    /* 横向网格：弱化 */
    var ticks = kind === 'nav'
      ? (logScale ? logTicks(lo, hi) : niceTicks(lo, hi, 5))
      : [-0.1, -0.2, -0.3, -0.4, -0.5, -0.6].filter(function (v) { return v >= lo; });
    ticks.forEach(function (v) {
      var y = Y(v);
      svg.appendChild(el('line', { x1: M.l, x2: W - M.r, y1: y, y2: y, 'stroke-width': 1 },
        { stroke: 'var(--grid)' }));
      var t = el('text', { x: M.l - 8, y: y + 3.5, 'text-anchor': 'end', 'font-size': '10.5' },
        { fill: 'var(--faint)', fontFamily: 'var(--mono)' });
      t.textContent = kind === 'nav' ? tickLabel(v) : (v * 100).toFixed(0) + '%';
      svg.appendChild(t);
    });
    if (kind === 'dd') {
      svg.appendChild(el('line', { x1: M.l, x2: W - M.r, y1: Y(0), y2: Y(0), 'stroke-width': 1 },
        { stroke: 'var(--axis)' }));
      var z = el('text', { x: M.l - 8, y: Y(0) + 3.5, 'text-anchor': 'end', 'font-size': '10.5' },
        { fill: 'var(--faint)', fontFamily: 'var(--mono)' });
      z.textContent = '0%';
      svg.appendChild(z);
    }
    /* 纵向年份网格：只在两图都画同一条，保证共轴 */
    yearTicks(W).forEach(function (t) {
      var x = X(t);
      svg.appendChild(el('line', { x1: x, x2: x, y1: M.t, y2: M.t + ih, 'stroke-width': 1 },
        { stroke: 'var(--grid)' }));
      var tx = el('text', { x: x, y: H - 8, 'text-anchor': 'middle', 'font-size': '10.5' },
        { fill: 'var(--faint)', fontFamily: 'var(--mono)' });
      tx.textContent = new Date(t).getUTCFullYear();
      svg.appendChild(tx);
    });
    if (kind === 'nav') {
      svg.appendChild(el('line', { x1: M.l, x2: W - M.r, y1: M.t + ih, y2: M.t + ih, 'stroke-width': 1 },
        { stroke: 'var(--axis)' }));
    }

    /* 面积 + 曲线 */
    var first = SERIES[0];
    var gradId = kind + 'Grad';
    var grad = el('linearGradient', { id: gradId, x1: '0', y1: '0', x2: '0', y2: '1' });
    grad.appendChild(el('stop', { offset: '0%', 'stop-opacity': '.20' }, { stopColor: first.color }));
    grad.appendChild(el('stop', { offset: '100%', 'stop-opacity': '.01' }, { stopColor: first.color }));
    var defs = el('defs'); defs.appendChild(grad); svg.appendChild(defs);

    var baseY = kind === 'nav' ? Y(first.equity[0]) : Y(0);
    var dArea = 'M' + X(first.ts[0]).toFixed(2) + ' ' + baseY.toFixed(2);
    for (var i = 0; i < first.ts.length; i++) {
      dArea += 'L' + X(first.ts[i]).toFixed(2) + ' ' + Y(kind === 'nav' ? first.equity[i] : first.dd[i]).toFixed(2);
    }
    dArea += 'L' + X(first.ts[first.ts.length - 1]).toFixed(2) + ' ' + baseY.toFixed(2) + 'Z';
    svg.appendChild(el('path', { d: dArea, fill: 'url(#' + gradId + ')' }));

    SERIES.forEach(function (s) {
      var d = '';
      for (var k = 0; k < s.ts.length; k++) {
        d += (k ? 'L' : 'M') + X(s.ts[k]).toFixed(2) + ' ' + Y(kind === 'nav' ? s.equity[k] : s.dd[k]).toFixed(2);
      }
      var p = el('path', { d: d, fill: 'none', 'stroke-width': kind === 'nav' ? '1.8' : '1.2' },
        { stroke: kind === 'nav' ? s.color : (s.ddcolor || s.color), strokeLinejoin: 'round' });
      svg.appendChild(p);
    });

    if (kind === 'nav') {
      /* 末尾数值标签 */
      var byEnd = SERIES.slice().sort(function (a, b) { return b.equity[b.equity.length - 1] - a.equity[a.equity.length - 1]; });
      byEnd.forEach(function (s, rank) {
        var last = s.equity[s.equity.length - 1];
        var x = X(s.ts[s.ts.length - 1]);
        svg.appendChild(el('circle', { cx: x, cy: Y(last), r: 3, 'stroke-width': 1.4 },
          { fill: s.color, stroke: 'var(--panel)' }));
        var t = el('text', { x: x - 6, y: Y(last) - 8 - rank * 13, 'text-anchor': 'end', 'font-size': '10.5' },
          { fill: 'var(--ink)', fontFamily: 'var(--mono)' });
        t.textContent = money(last);
        svg.appendChild(t);
      });
      /* 最深回撤峰谷竖线 */
      [DEEPEST.peakT, DEEPEST.troughT].forEach(function (t) {
        svg.appendChild(el('line', { x1: X(t), x2: X(t), y1: M.t, y2: M.t + ih, 'stroke-width': 1, 'stroke-dasharray': '3 3' },
          { stroke: 'rgba(200,69,63,.45)' }));
      });
    } else {
      svg.appendChild(el('line', {
        x1: M.l, x2: W - M.r, y1: Y(DEEPEST.mdd), y2: Y(DEEPEST.mdd), 'stroke-width': 1, 'stroke-dasharray': '5 4'
      }, { stroke: 'var(--neg)' }));
      var labText = '最深 ' + pct(DEEPEST.mdd) + '（' + DEEPEST.dates[DEEPEST.peakIdx] + ' → ' + DEEPEST.dates[DEEPEST.troughIdx] + '）';
      var asciiN = (labText.match(/[\x00-\x7F]/g) || []).length;
      var labW = asciiN * 6.4 + (labText.length - asciiN) * 11.2 + 16;
      var labX = M.l + 10;
      var labY = clamp(Y(DEEPEST.mdd) + 16, M.t + 14, M.t + ih - 2);
      svg.appendChild(el('rect', {
        x: labX - 8, y: labY - 13, width: labW, height: 19, rx: 5, 'stroke-width': 1
      }, { fill: 'var(--panel)', stroke: 'var(--line)' }));
      var lab = el('text', { x: labX, y: labY, 'font-size': '10.5' },
        { fill: 'var(--neg)', fontFamily: 'var(--mono)' });
      lab.textContent = labText;
      svg.appendChild(lab);
    }

    /* 十字线 + 悬停点 */
    var cross = el('line', {
      x1: 0, x2: 0, y1: M.t, y2: M.t + ih, 'stroke-width': 1,
      'stroke-dasharray': '3 3', opacity: '0'
    }, { stroke: 'var(--axis)' });
    svg.appendChild(cross);
    var dots = SERIES.map(function (s) {
      var c = el('circle', { cx: 0, cy: 0, r: 3, 'stroke-width': 2, opacity: '0' },
        { fill: 'var(--panel)', stroke: kind === 'nav' ? s.color : (s.ddcolor || s.color) });
      svg.appendChild(c); return c;
    });

    var hit = el('rect', { x: M.l, y: M.t, width: iw, height: ih, fill: 'transparent' },
      { cursor: 'crosshair' });
    hit.addEventListener('mousemove', function (evt) {
      var r = svg.getBoundingClientRect();
      var px = (evt.clientX - r.left) * (W / r.width);
      var t = T0 + (px - M.l) / iw * (T1 - T0);
      setCursor(t, evt, wrap);
    });
    hit.addEventListener('mouseleave', clearCursor);
    svg.appendChild(hit);

    host.appendChild(svg);
    return { kind: kind, wrap: wrap, X: X, Y: Y, cross: cross, dots: dots };
  }

  function setCursor(t, evt, hostWrap) {
    if (!charts.length) { return; }
    var ref = SERIES[0];
    var idx = nearestIn(ref, t);
    var dateStr = ref.dates[idx];
    charts.forEach(function (ch) {
      var x = ch.X(ref.ts[idx]);
      ch.cross.setAttribute('x1', x); ch.cross.setAttribute('x2', x); ch.cross.setAttribute('opacity', '1');
      ch.dots.forEach(function (dot, n) {
        var s = SERIES[n];
        var i = nearestIn(s, ref.ts[idx]);
        var v = ch.kind === 'nav' ? s.equity[i] : s.dd[i];
        dot.setAttribute('cx', ch.X(s.ts[i]));
        dot.setAttribute('cy', ch.Y(v));
        dot.setAttribute('opacity', '1');
      });
    });
    var html = '<b>' + dateStr + '</b>';
    SERIES.forEach(function (s) {
      var i = nearestIn(s, ref.ts[idx]);
      html += '<br>' + s.name + ' · 净值 ' + money(s.equity[i]) + ' · 回撤 ' + pct(s.dd[i]);
    });
    tip.innerHTML = html;
    tip.style.opacity = '1';
    tip.style.left = clamp(evt.clientX, 90, window.innerWidth - 90) + 'px';
    tip.style.top = clamp(evt.clientY, 70, window.innerHeight - 20) + 'px';
  }

  function clearCursor() {
    charts.forEach(function (ch) {
      ch.cross.setAttribute('opacity', '0');
      ch.dots.forEach(function (d) { d.setAttribute('opacity', '0'); });
    });
    tip.style.opacity = '0';
  }

  function renderAll() {
    charts = [];
    var nav = makeChart('nav');
    if (nav) { charts.push(nav); }
    var dd = makeChart('dd');
    if (dd) { charts.push(dd); }
  }

  var btnLinear = document.getElementById('btnLinear');
  var btnLog = document.getElementById('btnLog');
  function setScale(isLog) {
    logScale = isLog;
    if (btnLinear) { btnLinear.setAttribute('aria-pressed', isLog ? 'false' : 'true'); }
    if (btnLog) { btnLog.setAttribute('aria-pressed', isLog ? 'true' : 'false'); }
    renderAll();
  }
  if (btnLinear) { btnLinear.addEventListener('click', function () { setScale(false); }); }
  if (btnLog) { btnLog.addEventListener('click', function () { setScale(true); }); }

  var pending = null;
  function schedule() {
    if (pending) { return; }
    pending = requestAnimationFrame(function () { pending = null; renderAll(); });
  }
  window.addEventListener('resize', schedule);
  if (window.ResizeObserver) {
    var ro = new ResizeObserver(schedule);
    if (navWrap) { ro.observe(navWrap); }
    if (ddWrap) { ro.observe(ddWrap); }
  }
  renderAll();
})();
"""

#: 深色主题令牌（唯一一份）：系统深色与显式 data-theme="dark" 共用同一批值
DARK_DECLS = r"""
    --bg:#0d131b; --panel:#151d27; --ink:#e6ecf5; --muted:#9aa8bb; --faint:#6f7d90;
    --line:#232f3e; --line2:#1c2734;
    --v1:#56b4e9; --v2:#f0b64a; --v3:#4fd1a5; --v4:#e39ac2; --v5:#8ccdf2; --v6:#b39cf0;
    --neg:#ff8f86; --pos:#5fd3a3;
    --grid:#1d2734; --axis:#2b3949;
    --shadow:0 1px 2px rgba(0,0,0,.5),0 10px 26px rgba(0,0,0,.35);
"""
CSS_TEXT = CSS_TEXT.replace('__DARK__', DARK_DECLS)

PAGE = r"""<!DOCTYPE html>
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
{{CONCL}}
{{CURVES}}
{{SEGMENTS}}
{{DETAIL}}
{{SOURCES}}
</main>

<footer class="page-foot">{{FOOT}}</footer>

<script id="series-data" type="application/json">{{SERIES_JSON}}</script>
<script>{{JS}}</script>
</body>
</html>
"""


# ---------------------------------------------------------------- 工具函数

def pct(v: Any, digits: int = 2) -> str:
    return f"{float(v) * 100:.{digits}f}%"


def xnum(v: Any, digits: int = 2) -> str:
    return f"{float(v):.{digits}f}x"


def pp(v: Any) -> str:
    return f"{float(v) * 100:+.2f}pp"


def big_value(v: Any, fmt: str, digits: int = 2) -> str:
    """指标卡：大数字 + 小单位（fmt 取 pct / x）。"""
    unit = '%' if fmt == 'pct' else 'x'
    num = f"{float(v) * 100:.{digits}f}" if unit == '%' else f"{float(v):.{digits}f}"
    return f'{num}<small>{unit}</small>'


def rel(path: Path) -> str:
    try:
        return path.resolve().relative_to(ROOT).as_posix()
    except ValueError:
        return path.as_posix()


def short_sha(path: Path) -> str:
    h = hashlib.sha256()
    with path.open('rb') as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b''):
            h.update(chunk)
    return h.hexdigest()[:8]


def mtime_text(path: Path) -> str:
    return datetime.fromtimestamp(path.stat().st_mtime).strftime('%Y-%m-%d %H:%M')


def src_details(lines: List[str]) -> str:
    """各节脚注：小图标折叠区，不逐条占行。"""
    if not lines:
        return ''
    body = '<br>'.join(lines)
    return f'<details class="src"><summary>来源</summary><div class="src-body">{body}</div></details>'


def load_equity(path: Path) -> Tuple[List[str], List[float], str]:
    with path.open('r', encoding='utf-8-sig', newline='') as fh:
        reader = csv.DictReader(fh)
        if not reader.fieldnames:
            raise ValueError(f'CSV 无表头：{path}')
        names = [n.strip() for n in reader.fieldnames]
        date_col = 'date' if 'date' in names else names[0]
        eq_col = 'equity' if 'equity' in names else names[-1]
        dates: List[str] = []
        equity: List[float] = []
        for row in reader:
            dates.append(str(row[date_col]).strip())
            equity.append(float(row[eq_col]))
    if len(dates) < 2:
        raise ValueError(f'CSV 行数不足：{path}')
    return dates, equity, f'{date_col},{eq_col}'


def curve_stats(dates: Sequence[str], equity: Sequence[float], start: Optional[date] = None,
                end: Optional[date] = None) -> Dict[str, Any]:
    rows = list(zip(dates, equity))
    if start is not None:
        rows = [(d, e) for d, e in rows if date.fromisoformat(d) >= start]
    if end is not None:
        rows = [(d, e) for d, e in rows if date.fromisoformat(d) <= end]
    if len(rows) < 2:
        return {'missing': True}
    years = (date.fromisoformat(rows[-1][0]) - date.fromisoformat(rows[0][0])).days / 365.25
    annual = (rows[-1][1] / rows[0][1]) ** (1.0 / years) - 1.0 if years > 0 else 0.0
    peak, peak_day, mdd, trough = rows[0][1], rows[0][0], 0.0, rows[0][0]
    mdd_peak_day = rows[0][0]
    for d, e in rows:
        if e > peak:
            peak, peak_day = e, d
        v = e / peak - 1.0
        if v < mdd:
            mdd, trough, mdd_peak_day = v, d, peak_day
    return {'annual': annual, 'max_drawdown': mdd, 'peak': mdd_peak_day, 'trough': trough,
            'start': rows[0][0], 'end': rows[-1][0], 'final_equity': rows[-1][1]}


def parse_segments(specs: Sequence[str]) -> List[Tuple[str, date, date]]:
    out = []
    for spec in specs:
        a, b = spec.split(':', 1)
        d0, d1 = date.fromisoformat(a), date.fromisoformat(b)
        out.append((f'{d0.year}–{d1.year}', d0, d1))
    return out


def deep_get(d: Any, *keys: str) -> Any:
    cur = d
    for k in keys:
        if not isinstance(cur, dict) or k not in cur:
            return None
        cur = cur[k]
    return cur


# ---------------------------------------------------------------- 归一化

def normalize(payload: Dict[str, Any], equity_map: Dict[str, Tuple[List[str], List[float]]],
              seg_bounds: List[Tuple[str, date, date]]) -> List[Dict[str, Any]]:
    labels = payload.get('labels') or {}
    versions: List[Dict[str, Any]] = []

    def entry(vid: str, metrics: Optional[Dict[str, Any]], seg: Optional[Dict[str, Any]] = None,
              seg_cols: Optional[List[str]] = None, real_tax: Optional[Dict[str, Any]] = None,
              definition: Optional[str] = None, vs: Optional[Dict[str, Any]] = None,
              boundary: Optional[str] = None, verdict: Optional[str] = None) -> Dict[str, Any]:
        meta = dict(DEFAULT_LABELS.get(vid, {}))
        meta.update(labels.get(vid) or {})
        eq = equity_map.get(vid)
        stats = curve_stats(eq[0], eq[1]) if eq else None
        return {
            'id': vid,
            'name': meta.get('name') or f'版本 {vid}',
            'role': meta.get('role') or '',
            'hero': meta.get('hero') or meta.get('role') or '',
            'seg_label': meta.get('seg') or meta.get('name') or vid,
            'metrics': metrics or {},
            'seg': seg,
            'seg_cols': seg_cols or ['annual', 'dd', 'turnover', 'cost_share'],
            'real_tax': real_tax,
            'def': definition,
            'vs': vs or {},
            'boundary': boundary,
            'verdict': verdict,
            'equity': eq,
            'stats': stats,
            'seg_stats': {},
        }

    if payload.get('versions'):
        for item in payload['versions']:
            vid = str(item.get('id') or f'v{len(versions) + 1}')
            versions.append(entry(
                vid, item.get('metrics'), item.get('segments'), item.get('seg_cols'),
                item.get('real_tax'), item.get('def'), item.get('vs'), item.get('boundary'),
                item.get('verdict')))
    else:
        if payload.get('metrics_10bp'):
            versions.append(entry('v1', payload['metrics_10bp'], None, None,
                                  payload.get('real_tax'), payload.get('usage')))
        v2 = payload.get('v2') or {}
        if v2.get('m10bp'):
            versions.append(entry('v2', v2['m10bp'], v2.get('seg'), v2.get('seg_cols'),
                                  v2.get('real_tax'), v2.get('def'), None, v2.get('boundary')))
        v3 = payload.get('v3') or {}
        if v3.get('m10bp'):
            versions.append(entry('v3', v3['m10bp'], None, None, v3.get('real_tax'),
                                  v3.get('def'), v3.get('vs'), None, v3.get('verdict')))

    for ver in versions:
        if not ver['equity']:
            continue
        dates, equity = ver['equity']
        for label, d0, d1 in seg_bounds:
            ver['seg_stats'][label] = curve_stats(dates, equity, d0, d1)
        if ver['stats'] is None:
            ver['stats'] = curve_stats(dates, equity)
    return versions


# ---------------------------------------------------------------- 版块渲染

def render_conclusion(versions: List[Dict[str, Any]], slippage: Optional[str],
                      src_lines: List[str]) -> str:
    """首屏结论：四张指标卡（每张内三版同宽并排）+ 补充小表 + 判定原文。"""
    shown = [v for v in versions if v['metrics']]
    if not shown:
        return ''
    n = len(shown)
    cards = []
    for key, name, unit in METRIC_CARDS:
        if not any(v['metrics'].get(key) is not None for v in shown):
            continue
        cells = []
        for ver in shown:
            idx = versions.index(ver)
            color = PALETTE[idx % len(PALETTE)]
            val = ver['metrics'].get(key)
            tag = f'<div class="mtag"><i style="background:{color}"></i>{ver["id"]}</div>'
            if val is None:
                cells.append(f'<div class="mcell">{tag}<div class="mval dim">未落盘</div></div>')
                continue
            cls = 'mval neg' if key == 'max_drawdown' else 'mval'
            sub = ''
            if key == 'max_drawdown':
                dd_date = ver['metrics'].get('max_drawdown_date')
                if not dd_date and ver.get('stats'):
                    dd_date = ver['stats'].get('trough')
                sub = f'<div class="msub">{dd_date or "日期未落盘"}</div>'
            cells.append(f'<div class="mcell">{tag}<div class="{cls}">{big_value(val, unit)}</div>{sub}</div>')
        cards.append(
            f'<article class="mcard"><div class="mname"><span>{name}</span>'
            f'<span class="dim">{n} 版</span></div>'
            f'<div class="mrow" style="grid-template-columns:repeat({n},minmax(0,1fr))">'
            f'{"".join(cells)}</div></article>')

    if not cards:
        return ''

    # 补充小表：期末权益、单位成本、相对年化、序列落盘情况
    has_final = any(v['metrics'].get('final_equity') is not None for v in shown)
    has_bps = any(v['metrics'].get('cost_bps') is not None for v in shown)
    has_rel = any(k.endswith('_annual') for v in shown for k in (v.get('vs') or {}))
    cols = ['版本'] + (['期末权益'] if has_final else []) + (['单位成本'] if has_bps else []) \
        + (['相对 v1 年化'] if has_rel else []) + ['净值序列']
    rows = []
    for ver in shown:
        m = ver['metrics']
        cells = [f'<td>{ver["id"]} {ver["name"]}</td>']
        if has_final:
            fe = m.get('final_equity')
            cells.append(f'<td class="num">{float(fe) / 1e8:.2f} 亿</td>' if fe is not None else '<td class="num dim">未落盘</td>')
        if has_bps:
            cb = m.get('cost_bps')
            cells.append(f'<td class="num">{float(cb):.2f}bp</td>' if cb is not None else '<td class="num dim">未落盘</td>')
        if has_rel:
            bits = []
            for key, val in sorted((ver.get('vs') or {}).items()):
                if key.endswith('_annual'):
                    bits.append(f'{pp(val)}（vs {key[:-7]}）')
            cells.append(f'<td class="num">{("；".join(bits) or "—")}</td>')
        cells.append(f'<td>{"已落盘" if ver["equity"] else "仅指标"}</td>')
        rows.append('<tr>' + ''.join(cells) + '</tr>')
    mini = ('<details class="ext"><summary>补充口径（期末权益 / 单位成本 / 相对增量 / 序列落盘）</summary>'
            '<div class="ext-body"><table class="mini"><thead><tr>'
            + ''.join(f'<th>{c}</th>' for c in cols) + '</tr></thead><tbody>'
            + ''.join(rows) + '</tbody></table></div></details>')

    notes = []
    for ver in versions:
        if ver['id'] == 'v3' and ver.get('verdict'):
            notes.append(f'<b>v3 判定（原文）：</b>{ver["verdict"]}')
    for ver in versions:
        if ver['boundary']:
            notes.append(f'<b>{ver["id"]} 边界（原文）：</b>{ver["boundary"]}')
    note_html = f'<div class="note-box">{" &nbsp;".join(notes)}</div>' if notes else ''
    hint = f'全部为 {slippage}bp 滑点 + 冲击口径' if slippage else '全部为结果 JSON 记录的默认成本口径'

    return (
        '<section id="concl">'
        '<div class="sec-head"><h2>结论</h2>'
        f'<span class="hint">{hint}；最大回撤负值越小越深</span></div>'
        f'<div class="mcards">{"".join(cards)}</div>{mini}{note_html}'
        + src_details(src_lines) +
        '</section>'
    )


def render_curves(series: List[Dict[str, Any]], missing: List[str], capital: Any,
                  src_lines: List[str]) -> str:
    if not series:
        return ''
    legend = ''.join(
        f'<span><i style="background:{s["color"]}"></i>{s["id"]} {s["name"]}</span>' for s in series)
    if capital:
        legend += f'<span class="dim">起点 {float(capital) / 1e8:.2f} 亿</span>'
    cover = (f'本区叠加 {len(series)} 条日净值序列；'
             + (f'{" / ".join(missing)} 未提供日净值，图中不出现，也未做缩放或拟合。' if missing
                else '未做任何缩放或拟合。'))
    return (
        '<section id="curves">'
        '<div class="sec-head"><h2>曲线</h2>'
        '<span class="hint">净值与回撤共用时间轴；移动鼠标两图十字线联动</span></div>'
        '<div class="panel"><div class="toolbar">'
        f'<div class="legend">{legend}</div>'
        '<div class="switch" role="group" aria-label="净值纵轴刻度">'
        '<button type="button" id="btnLinear" aria-pressed="true">线性轴</button>'
        '<button type="button" id="btnLog" aria-pressed="false">对数轴</button>'
        '</div></div>'
        '<div class="chart-wrap" id="navWrap"><div id="navChart"></div></div>'
        '<div class="chart-cap">净值（以上为净值曲线，纵轴可切线性/对数）</div>'
        '<div class="chart-wrap" id="ddWrap"><div id="ddChart"></div></div>'
        '<div class="chart-cap">回撤（相对历史峰值的水下深度，红色虚线为最深回撤）</div>'
        f'<div class="note-box"><b>覆盖：</b>{cover}</div>'
        + src_details(src_lines) +
        '</div></section>'
    )


def render_segments(versions: List[Dict[str, Any]], periods: List[str],
                    seg_bounds: List[Tuple[str, date, date]],
                    src_lines: List[str], tax_def: str = '') -> str:
    labels = [p[0] for p in seg_bounds]
    seg_versions = [v for v in versions if v['seg'] or v['seg_stats']]
    increments: List[Tuple[str, List[Optional[float]]]] = []

    def seg_value(ver: Dict[str, Any], label: str, col: str) -> Optional[float]:
        alias = {'max_drawdown': 'dd'}.get(col, col)
        if ver['seg']:
            key = label if label in ver['seg'] else label.replace('–', '-')
            if key in ver['seg']:
                cols = ver['seg_cols']
                if alias in cols:
                    raw = ver['seg'][key]
                    return raw[cols.index(alias)] if isinstance(raw, (list, tuple)) else raw.get(alias)
                return None
        st = ver['seg_stats'].get(label)
        if st and col in st:
            return st[col]
        return None

    v1 = next((v for v in versions if v['id'] == 'v1'), None)
    v2 = next((v for v in versions if v['id'] == 'v2'), None)
    if v1 and v2 and v1['seg_stats'] and v2['seg']:
        deltas = []
        for l in labels:
            a1 = seg_value(v1, l, 'annual')
            a2 = seg_value(v2, l, 'annual')
            deltas.append(a2 - a1 if a1 is not None and a2 is not None else None)
        increments.append(('Δ 年化：v2 − v1 <span class="dim">†</span>', deltas))
    v3 = next((v for v in versions if v['id'] == 'v3'), None)
    if v3 and v3['vs'].get('v1_seg'):
        vals = v3['vs']['v1_seg']
        increments.append(('Δ 年化：v3 − v1', [vals[i] if i < len(vals) else None for i in range(len(labels))]))

    if not seg_versions and not increments:
        return ''

    body = []
    for ver in versions:
        if not (ver['seg'] or ver['seg_stats']):
            continue
        body.append(f'<tr class="grp"><td colspan="{len(labels) + 1}">{ver["id"]} {ver["seg_label"]}</td></tr>')
        annual_row = []
        dd_row = []
        for label in labels:
            a = seg_value(ver, label, 'annual')
            annual_row.append(f'<td class="num">{pct(a)}</td>' if a is not None else '<td class="num dim">未落盘</td>')
            d = seg_value(ver, label, 'max_drawdown')
            st = ver['seg_stats'].get(label) or {}
            if d is not None:
                small = f'<span class="small">{st["peak"]}→{st["trough"]}</span>' if st.get('peak') else ''
                dd_row.append(f'<td class="num delta-neg">{pct(d)}{small}</td>')
            else:
                dd_row.append('<td class="num dim">未落盘</td>')
        dagger = ' <span class="dim">†</span>' if ver['seg_stats'] else ''
        body.append(f'<tr><td>年化收益{dagger}</td>{"".join(annual_row)}</tr>')
        body.append(f'<tr><td>最大回撤（区间）{dagger}</td>{"".join(dd_row)}</tr>')
        for col, name in (('turnover', '年换手（双边）'), ('cost_share', '成本占毛收益')):
            cells = []
            any_val = False
            for label in labels:
                val = seg_value(ver, label, col)
                if val is None:
                    cells.append('<td class="num dim">未落盘</td>')
                else:
                    any_val = True
                    cells.append(f'<td class="num">{xnum(val) if col == "turnover" else pct(val)}</td>')
            if any_val:
                body.append(f'<tr><td>{name}</td>{"".join(cells)}</tr>')

    if increments:
        body.append('<tr class="grp"><td colspan="' + str(len(labels) + 1) + '">增量</td></tr>')
        for label, vals in increments:
            cells = ''.join(
                f'<td class="num {"delta-pos" if (v is not None and v > 0) else "dim"}">{pp(v) if v is not None else "未落盘"}</td>'
                for v in vals)
            body.append(f'<tr><td>{label}</td>{cells}</tr>')
    for ver in versions:
        if ver['seg'] or ver['seg_stats']:
            continue
        if increments:
            body.append(f'<tr><td>{ver["id"]} 绝对年化 / 回撤</td>' +
                        ''.join('<td class="num dim">未落盘</td>' for _ in labels) + '</tr>')

    head = ''.join(f'<th>{l}</th>' for l in labels)
    seg_table = (
        '<div class="panel"><div class="tbl-wrap"><table data-collapse="6">'
        '<thead><tr><th>口径</th>' + head + '</tr></thead>'
        f'<tbody>{"".join(body)}</tbody></table></div></div>'
    )

    tax_rows = []
    biases = []
    for ver in versions:
        rt = ver['real_tax']
        m = ver['metrics']
        if not rt or not m:
            continue
        tax_rows.append(
            f'<tr><td>{ver["id"]} 固定千 1</td><td class="num">{pct(m["annual"])}</td>'
            f'<td class="num">{pct(m["max_drawdown"])}</td><td class="num">{xnum(m["turnover_two_way"])}</td>'
            f'<td class="num">{pct(m["cost_share_of_gross"])}</td></tr>')
        tax_rows.append(
            f'<tr><td>{ver["id"]} 真值税制</td><td class="num">{pct(rt["annual"])}</td>'
            f'<td class="num">{pct(rt["dd"])}</td><td class="num">{xnum(rt["turn"])}</td>'
            f'<td class="num">{pct(rt["cost_share"])}</td></tr>')
        if rt.get('bias'):
            biases.append(f'{ver["id"]} {rt["bias"]}')

    tax_panel = ''
    if tax_rows:
        tax_def_txt = f'真值口径：{tax_def}。' if tax_def else ''
        tax_panel = (
            '<div class="panel"><div class="sec-head" style="margin:0 0 8px">'
            '<h2 style="font-size:13.5px">税制对照</h2></div>'
            '<div class="tbl-wrap"><table data-collapse="8">'
            '<thead><tr><th>版本 / 口径</th><th>年化</th><th>最大回撤</th><th>年换手</th><th>成本占比</th></tr></thead>'
            f'<tbody>{"".join(tax_rows)}</tbody></table></div>'
            + (f'<div class="info-box" style="margin-top:10px">偏差（原文）：{"；".join(biases)}。{tax_def_txt}</div>'
               if biases else '')
            + '</div>')

    if tax_panel:
        tax_panel = tax_panel.replace('<div class="panel">', '<div class="panel" style="margin-top:12px">', 1)
    layout = seg_table + tax_panel
    span = ' / '.join(f'{d0.isoformat()}~{d1.isoformat()}' for _, d0, d1 in seg_bounds)
    dagger_note = '由净值 CSV 按段切片计算，非指标文件原有字段。'
    if any(v['seg'] for v in versions):
        dagger_note += ' 带 † 的增量与 v2 分段数值来自指标 JSON 的落盘结果，与全样本指标存在四舍五入级差异。'
    return (
        '<section id="segments">'
        '<div class="sec-head"><h2>分段与增量</h2>'
        f'<span class="hint">两段：{span}；表格默认 6 行，可展开全部</span></div>'
        f'{layout}'
        f'<div class="info-box" style="margin-top:10px"><span class="dim">†</span> {dagger_note}</div>'
        + src_details(src_lines) +
        '</section>'
    )


def render_quantiles(payload: Dict[str, Any], json_path: Path, src_lines: List[str]) -> str:
    quantiles = payload.get('quantiles')
    if isinstance(quantiles, dict) and quantiles.get('rows'):
        periods = quantiles.get('periods') or []
        head = ''.join(f'<th>{p}</th>' for p in periods)
        rows = ''.join('<tr><td>' + str(r.get('name', '')) + '</td>' +
                       ''.join(f'<td class="num">{r.get("values", {}).get(p, "")}</td>' for p in periods) + '</tr>'
                       for r in quantiles['rows'])
        inner = (f'<div class="tbl-wrap"><table data-collapse="8"><thead><tr><th>分组</th>{head}</tr></thead>'
                 f'<tbody>{rows}</tbody></table></div>')
    else:
        inner = ('<div class="info-box">核查结论：'
                 f'<code>{rel(json_path)}</code> '
                 '中未提供五分位结果（quantiles）字段，本节留空；不猜数、不补造图表。</div>')
    return ('<section id="detail"><div class="sec-head"><h2>明细</h2>'
            '<span class="hint">五分位 / 分组结果；未落盘则留空说明</span></div>'
            f'<div class="panel">{inner}'
            + src_details(src_lines) +
            '</div></section>')


def render_sources(payload: Dict[str, Any], json_path: Path,
                   series_src: List[Dict[str, Any]], read_at: str = '') -> str:
    items = []
    script = payload.get('script') or {}
    head = payload.get('head')
    json_bits = [f'sha256 <span class="hash">{short_sha(json_path)}</span>',
                 f'冻结 {mtime_text(json_path)}']
    if head:
        json_bits.append(f'代码 head <code>{head}</code>')
    if script.get('path'):
        json_bits.append(f'脚本 <code>{script["path"]}</code>' +
                         (f'（sha256 <span class="hash">{str(script.get("sha256", ""))[:8]}</span>）' if script.get('sha256') else ''))
    if payload.get('command'):
        json_bits.append(f'命令 <code>{payload["command"]}</code>')
    items.append(f'<li>指标与分段：<code>{rel(json_path)}</code><br>{" · ".join(json_bits)}</li>')

    for meta in series_src:
        path = meta['path']
        read_bit = f' · 读取 {read_at}' if read_at else ''
        items.append(
            f'<li>日净值：<code>{rel(path)}</code>（列 <code>{meta["cols"]}</code>，{meta["rows"]:,} 行）<br>'
            f'sha256 <span class="hash">{short_sha(path)}</span> · 冻结 {mtime_text(path)} · '
            f'覆盖 {meta["start"]} ~ {meta["end"]} · 期末 {meta["final"]:,.2f}{read_bit}</li>')

    rt_script = payload.get('real_tax_script') or {}
    if rt_script.get('p'):
        items.append(f'<li>真值税制：<code>{rt_script["p"]}</code>'
                     f'（sha256 <span class="hash">{str(rt_script.get("sha", ""))[:8]}</span>）'
                     f'{"：" + rt_script["def"] if rt_script.get("def") else ""}</li>')

    cost = payload.get('cost') or {}
    if cost:
        parts = []
        if cost.get('commission'):
            parts.append(str(cost['commission']))
        if cost.get('stamp_tax'):
            stamp = str(cost['stamp_tax'])
            if cost.get('stamp_tax_note'):
                stamp += f'（{cost["stamp_tax_note"]}）'
            parts.append(stamp)
        if cost.get('transfer_fee'):
            parts.append(str(cost['transfer_fee']))
        if cost.get('impact_coef') is not None:
            parts.append(f'冲击系数 {cost["impact_coef"]}')
        items.append(f'<li>成本口径（原文）：{"；".join(parts)}。</li>')

    data = payload.get('data') or {}
    if data:
        data_txt = f'{data.get("price_basis", "")}；使用 {int(data.get("used", 0)):,} 只' if data.get('used') else str(data.get('price_basis', ''))
        if data.get('excluded') is not None:
            data_txt += f'，剔除 {int(data["excluded"]):,} 只'
            if data.get('excluded_reason'):
                data_txt += f'（{data["excluded_reason"]}）'
        items.append(f'<li>数据口径（原文）：{data_txt}。</li>')

    return (
        '<section id="sources">'
        '<div class="sec-head"><h2>来源</h2>'
        '<span class="hint">指纹为 sha256 前 8 位；冻结时间为文件修改时间</span></div>'
        f'<div class="panel"><ol class="src-list">{"".join(items)}</ol>'
        '<div class="info-box" style="margin-top:12px"><b>只读声明：</b>本页只读取既有结果 JSON 与净值 CSV，'
        '未修改回测引擎、未改动 <code>data/local_store</code>、未改成本默认值、未重跑回测；'
        '页面内所有数值与源文件一致。净值数据已内嵌进本 HTML（非读取同目录文件），双击即可离线打开。</div>'
        '</div></section>'
    )


# ---------------------------------------------------------------- 主流程

def series_for_js(series: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [{'id': s['id'], 'name': s['name'], 'color': s['color'], 'ddcolor': s['ddcolor'],
             'dates': s['dates'], 'equity': s['equity']} for s in series]


def main() -> None:
    ap = argparse.ArgumentParser(description='回测结果看板生成器（单文件 HTML）')
    ap.add_argument('--json', required=True, help='结果 JSON 路径')
    ap.add_argument('--equity', action='append', default=[],
                    help='净值 CSV，格式 版本ID=路径，可重复')
    ap.add_argument('--out', help='输出 HTML 路径（默认 docs/回测结果看板_<slug>_<日期>.html）')
    ap.add_argument('--title', default='回测结果看板', help='页面标题')
    ap.add_argument('--slug', default='回测结果', help='默认输出文件名中的主题')
    ap.add_argument('--seg', action='append', default=None,
                    help='分段边界 START:END，可重复；默认 2010-01-04:2016-12-30 与 2017-01-03:2026-11-13')
    ap.add_argument('--runs-page', default='', help='回链表页的文件名或链接；给出则在锚点条前加返回链接')
    args = ap.parse_args()

    json_path = Path(args.json)
    if not json_path.is_absolute():
        json_path = Path.cwd() / json_path
    payload = json.loads(json_path.read_text(encoding='utf-8'))

    equity_map: Dict[str, Tuple[List[str], List[float]]] = {}
    csv_meta: List[Dict[str, Any]] = []
    for item in args.equity:
        if '=' not in item:
            raise SystemExit(f'--equity 需要 版本ID=路径：{item}')
        vid, path_s = item.split('=', 1)
        path = Path(path_s)
        if not path.is_absolute():
            path = Path.cwd() / path
        dates, equity, cols = load_equity(path)
        equity_map[vid.strip()] = (dates, equity)
        csv_meta.append({'path': path, 'rows': len(dates), 'start': dates[0],
                         'end': dates[-1], 'final': equity[-1], 'cols': cols})

    seg_specs = args.seg or ['2010-01-04:2016-12-30', '2017-01-03:2026-11-13']
    seg_bounds = parse_segments(seg_specs)
    versions = normalize(payload, equity_map, seg_bounds)

    slippage = None
    m = re.search(r'--slippage\s+([0-9.]+)', str(payload.get('command') or ''))
    if m:
        slippage = m.group(1)

    series = []
    for idx, ver in enumerate(versions):
        if not ver['equity']:
            continue
        dates, equity = ver['equity']
        meta = next((c for c in csv_meta if c['rows'] == len(dates) and c['start'] == dates[0]), None)
        path = meta['path'] if meta else None
        series.append({
            'id': ver['id'], 'name': ver['name'], 'color': PALETTE[idx % len(PALETTE)],
            'dates': dates, 'equity': [int(round(e)) for e in equity],
            'src': rel(path) if path else ver['id'],
            'stats': ver['stats'],
        })
    missing = [f'{v["id"]} {v["name"]}' for v in versions if not v['equity']]
    for s in series:
        s['ddcolor'] = 'var(--neg)' if len(series) == 1 else s['color']

    periods = []
    for ver in versions:
        if ver['seg']:
            periods = list(ver['seg'].keys())
            break
    if not periods:
        periods = [p[0] for p in seg_bounds]

    first_date = min((s['dates'][0] for s in series), default=None)
    last_date = max((s['dates'][-1] for s in series), default=None)
    capital = payload.get('capital')
    json_sha = short_sha(json_path)

    # 顶部窄标题栏：组合名 + 区间 + 数据版本（短哈希）
    meta_bits = []
    if first_date and last_date:
        meta_bits.append(f'区间 <b>{first_date} ~ {last_date}</b>')
    if capital:
        meta_bits.append(f'初始资金 <b>{float(capital) / 1e8:g} 亿</b>')
    meta_bits.append(f'数据版本 sha256 <b>{json_sha}</b> · 指标冻结 <b>{mtime_text(json_path)}</b>')
    if csv_meta:
        meta_bits.append(f'净值冻结 <b>{mtime_text(csv_meta[0]["path"])}</b>')
    meta_html = ''.join(f'<span>{b}</span>' for b in meta_bits)

    # 锚点条：按实际生成的分节显示
    anchors = [('#concl', '结论')]
    if series:
        anchors.append(('#curves', '曲线'))
    anchors += [('#segments', '分段'), ('#detail', '明细'), ('#sources', '来源')]
    anchor_html = ''.join(f'<a href="{href}">{label}</a>' for href, label in anchors)
    if args.runs_page:
        anchor_html = f'<a class="back" href="{args.runs_page}">← 运行清单</a>' + anchor_html

    src_metric = (f'指标：{rel(json_path)} · sha256 <span class="hash">{json_sha}</span> · '
                  f'冻结 {mtime_text(json_path)}'
                  + (f' · head {payload["head"]}' if payload.get('head') else ''))
    read_at = datetime.now().strftime('%Y-%m-%d %H:%M')
    src_series = [f'净值：{rel(c["path"])} · sha256 <span class="hash">{short_sha(c["path"])}</span> · '
                  f'冻结 {mtime_text(c["path"])} · 读取 {read_at} · {c["rows"]:,} 行 · 期末 {c["final"]:,.2f}'
                  for c in csv_meta]

    html = PAGE
    html = html.replace('{{TITLE}}', args.title)
    html = html.replace('{{H1}}', payload.get('h1') or args.title)
    html = html.replace('{{META}}', meta_html)
    html = html.replace('{{ANCHORS}}', anchor_html)
    html = html.replace('{{CONCL}}', render_conclusion(versions, slippage, [src_metric] + src_series))
    html = html.replace('{{CURVES}}', render_curves(series, missing, capital, src_series))
    html = html.replace('{{SEGMENTS}}', render_segments(versions, periods, seg_bounds,
                                                        [src_metric] + src_series,
                                                        str((payload.get('real_tax_script') or {}).get('def') or '')))
    html = html.replace('{{DETAIL}}', render_quantiles(payload, json_path, [src_metric]))
    html = html.replace('{{SOURCES}}', render_sources(payload, json_path, csv_meta, read_at))
    html = html.replace('{{FOOT}}', f'{args.title} · 生成于 {date.today().isoformat()} · 只读展示，不构成投资建议')
    html = html.replace('{{SERIES_JSON}}', json.dumps(series_for_js(series), ensure_ascii=False, separators=(',', ':')))
    html = html.replace('{{JS}}', JS_TEXT)
    html = html.replace('{{STYLE}}', CSS_TEXT)

    out = Path(args.out) if args.out else ROOT / 'docs' / f'回测结果看板_{args.slug}_{date.today().isoformat()}.html'
    if not out.is_absolute():
        out = Path.cwd() / out
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(html, encoding='utf-8', newline='\n')
    print(f'已生成 {out}（{len(html.encode("utf-8")):,} 字节，{len(series)} 条净值，{len(versions)} 个版本）')


if __name__ == '__main__':
    sys.exit(main())
