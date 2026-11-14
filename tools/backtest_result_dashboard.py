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
PALETTE = ['#5b6ef5', '#d99327', '#0f9e8a', '#8b5cf6', '#d4514f', '#2f8fbf']

DEFAULT_LABELS: Dict[str, Dict[str, str]] = {
    'v1': {'name': '价量 + 估值等权', 'role': '策略对比对照', 'hero': '对照', 'seg': '对照'},
    'v2': {'name': '剔 60 日波动率最高 20%', 'role': '改进构造对照', 'hero': '改进构造', 'seg': '剔高波 20%'},
    'v3': {'name': '剔高波 + 次新', 'role': '现基线', 'hero': '现基线', 'seg': '剔高波 + 次新'},
}

CSS_TEXT = r"""  :root{
    --bg:#f4f6f9; --panel:#ffffff; --ink:#16202f; --muted:#6a7486; --faint:#95a0b3;
    --line:#e4e8ef; --line2:#eef1f6;
    --v1:#5b6ef5; --v2:#d99327; --v3:#0f9e8a;
    --neg:#d4514f; --pos:#1f9d6b;
    --shadow:0 1px 2px rgba(16,32,64,.05),0 8px 24px rgba(16,32,64,.06);
    --mono:ui-monospace,SFMono-Regular,Menlo,Consolas,"Cascadia Mono",monospace;
  }
  *{box-sizing:border-box}
  html,body{margin:0;padding:0}
  body{
    background:var(--bg); color:var(--ink);
    font-family:"Microsoft YaHei","PingFang SC","Hiragino Sans GB","Segoe UI",system-ui,-apple-system,sans-serif;
    font-size:14px; line-height:1.6; -webkit-font-smoothing:antialiased;
  }
  a{color:#3f56d6}
  .hero{
    background:
      radial-gradient(1200px 420px at 12% -20%, rgba(91,110,245,.35), transparent 60%),
      radial-gradient(900px 380px at 92% 0%, rgba(15,158,138,.22), transparent 62%),
      linear-gradient(135deg,#0d1626 0%,#172741 60%,#1b2f4d 100%);
    color:#fff; padding:38px 28px 30px;
  }
  .hero-inner{max-width:1180px;margin:0 auto}
  .kicker{
    font-size:12px;letter-spacing:.16em;color:#9db0d0;text-transform:uppercase;
    display:flex;align-items:center;gap:8px;margin-bottom:10px;
  }
  .kicker::before{content:"";width:22px;height:1px;background:#9db0d0;display:inline-block}
  h1{margin:0 0 10px;font-size:27px;line-height:1.3;font-weight:650;letter-spacing:.01em}
  .sub{color:#b9c7de;font-size:13.5px;max-width:1000px}
  .badges{display:flex;flex-wrap:wrap;gap:8px;margin-top:18px}
  .badge{
    font-size:12px;color:#d7e0f0;border:1px solid rgba(255,255,255,.22);
    background:rgba(255,255,255,.06);border-radius:999px;padding:4px 12px;
  }
  main{max-width:1180px;margin:0 auto;padding:26px 24px 56px}
  section{margin-bottom:30px}
  .sec-head{display:flex;align-items:baseline;gap:12px;flex-wrap:wrap;margin:0 0 12px}
  .sec-head h2{font-size:17px;margin:0;font-weight:650}
  .sec-head .hint{font-size:12.5px;color:var(--muted)}
  .panel{
    background:var(--panel);border:1px solid var(--line);border-radius:14px;
    box-shadow:var(--shadow);padding:18px 20px;
  }
  .cards{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:16px}
  .card{
    background:var(--panel);border:1px solid var(--line);border-radius:14px;
    box-shadow:var(--shadow);padding:16px 18px 14px;position:relative;overflow:hidden;
  }
  .card::before{content:"";position:absolute;left:0;top:0;bottom:0;width:4px;background:var(--c)}
  .card.v1{--c:var(--v1)} .card.v2{--c:var(--v2)} .card.v3{--c:var(--v3)}
  .card-head{display:flex;align-items:center;gap:8px;flex-wrap:wrap;margin-bottom:12px}
  .vtag{
    font-family:var(--mono);font-size:12px;font-weight:700;color:#fff;background:var(--c);
    border-radius:6px;padding:1px 8px;letter-spacing:.04em;
  }
  .card-head h3{font-size:14px;margin:0;font-weight:650}
  .role{font-size:11.5px;color:var(--muted);border:1px solid var(--line);border-radius:999px;padding:1px 8px}
  .metric{display:flex;align-items:baseline;justify-content:space-between;gap:8px;padding:7px 0;border-top:1px dashed var(--line2)}
  .metric:first-of-type{border-top:0}
  .metric .label{font-size:12.5px;color:var(--muted)}
  .metric .value{font-family:var(--mono);font-size:16.5px;font-weight:650;letter-spacing:-.01em}
  .metric .note{font-family:var(--mono);font-size:11px;color:var(--faint)}
  .neg{color:var(--neg)}
  .card-foot{margin-top:10px;padding-top:9px;border-top:1px solid var(--line2);font-size:11.5px;color:var(--muted)}
  .card-foot b{font-family:var(--mono);color:var(--ink);font-weight:600}

  .toolbar{display:flex;align-items:center;justify-content:space-between;gap:12px;flex-wrap:wrap;margin-bottom:8px}
  .legend{display:flex;align-items:center;gap:14px;font-size:12.5px;color:var(--muted);flex-wrap:wrap}
  .legend i{display:inline-block;width:16px;height:3px;border-radius:2px;background:var(--v1);vertical-align:middle;margin-right:6px}
  .seg{display:inline-flex;border:1px solid var(--line);border-radius:9px;overflow:hidden;background:#fafbfd}
  .seg button{
    border:0;background:transparent;color:var(--muted);font:inherit;font-size:12.5px;
    padding:6px 14px;cursor:pointer;
  }
  .seg button[aria-pressed="true"]{background:#fff;color:var(--ink);font-weight:650;box-shadow:0 1px 3px rgba(16,32,64,.12)}
  .chart-wrap{position:relative;width:100%}
  .chart-wrap svg{display:block;width:100%}
  #tip{
    position:absolute;pointer-events:none;z-index:8;background:rgba(18,28,44,.94);color:#fff;
    border-radius:8px;padding:7px 10px;font-size:12px;line-height:1.55;white-space:nowrap;
    box-shadow:0 6px 20px rgba(10,20,40,.28);opacity:0;transform:translate(-50%,-118%);
    transition:opacity .08s ease;
  }
  #tip b{font-family:var(--mono);font-weight:650}
  #tip .t-neg{color:#ff9d9a}
  .note-box{
    margin-top:12px;border:1px solid #eadcc2;background:#fdf8ee;border-radius:10px;
    padding:10px 13px;font-size:12.5px;color:#7a5c22;
  }
  .note-box b{color:#5d4415}
  .info-box{
    border:1px solid var(--line);background:#f8fafd;border-radius:10px;padding:11px 14px;
    font-size:12.5px;color:var(--muted);
  }
  .info-box b{color:var(--ink)}
  .src-line{
    margin-top:10px;font-size:11px;color:var(--faint);font-family:var(--mono);
    line-height:1.7;word-break:break-all;
  }

  table{width:100%;border-collapse:collapse;font-size:13px}
  th,td{padding:9px 10px;text-align:right;border-bottom:1px solid var(--line2);vertical-align:top}
  th:first-child,td:first-child{text-align:left}
  thead th{color:var(--muted);font-weight:600;font-size:12px;border-bottom:1px solid var(--line)}
  tbody tr.grp td{
    background:#f8fafd;color:var(--ink);font-weight:650;font-size:12.5px;
    text-align:left;border-bottom:1px solid var(--line);
  }
  td.num{font-family:var(--mono);font-variant-numeric:tabular-nums}
  td .small{display:block;font-family:var(--mono);font-size:10.5px;color:var(--faint);margin-top:1px}
  .delta-pos{color:var(--pos)}
  .delta-neg{color:var(--neg)}
  .dim{color:var(--faint)}
  .two-col{display:grid;grid-template-columns:1.35fr 1fr;gap:16px;align-items:start}
  .src-list{margin:0;padding-left:20px;font-size:12.5px;color:var(--muted)}
  .src-list li{margin:7px 0}
  .src-list code{font-family:var(--mono);font-size:11.5px;color:var(--ink);background:#f2f4f8;border-radius:4px;padding:1px 5px}
  .src-list .hash{font-family:var(--mono);font-size:11.5px;color:#3f56d6}
  footer.page-foot{max-width:1180px;margin:0 auto;padding:0 24px 40px;color:var(--faint);font-size:11.5px}
  @media (max-width:900px){
    .cards{grid-template-columns:1fr}
    .two-col{grid-template-columns:1fr}
    h1{font-size:22px}
  }"""

JS_TEXT = r"""
(function () {
  'use strict';
  var SERIES = JSON.parse(document.getElementById('series-data').textContent);
  if (!SERIES.length) { return; }
  var NS = 'http://www.w3.org/2000/svg';
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
  var navChart = document.getElementById('navChart');
  var ddWrap = document.getElementById('ddWrap');
  var ddChart = document.getElementById('ddChart');
  var tip = document.createElement('div');
  tip.id = 'tip';
  document.body.appendChild(tip);

  var logScale = false;
  var btnLinear = document.getElementById('btnLinear');
  var btnLog = document.getElementById('btnLog');

  function el(tag, attrs) {
    var e = document.createElementNS(NS, tag);
    if (attrs) {
      for (var k in attrs) {
        if (Object.prototype.hasOwnProperty.call(attrs, k)) { e.setAttribute(k, attrs[k]); }
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
    var mults = [1, 1.5, 2, 2.5, 3, 3.5, 4, 5, 6, 8];
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

  function yearTicks(width) {
    var every = width < 640 ? 4 : 2;
    var y0 = new Date(T0).getUTCFullYear();
    var y1 = new Date(T1).getUTCFullYear();
    var out = [];
    for (var y = y0; y <= y1; y++) { if ((y - y0) % every === 0) { out.push(Date.parse(y + '-01-01T00:00:00Z')); } }
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

  function showTip(evt, host, html) {
    var r = host.getBoundingClientRect();
    tip.innerHTML = html;
    tip.style.opacity = '1';
    tip.style.left = clamp(evt.clientX - r.left, 70, r.width - 70) + 'px';
    tip.style.top = clamp(evt.clientY - r.top, 66, r.height) + 'px';
  }
  function hideTip() { tip.style.opacity = '0'; }

  function hoverIndex(evt, svg, W, m, iw) {
    var r = svg.getBoundingClientRect();
    var px = (evt.clientX - r.left) * (W / r.width);
    var t = T0 + (px - m.l) / iw * (T1 - T0);
    return t;
  }

  /* ---------- 净值图 ---------- */
  function renderNav() {
    var W = navWrap.clientWidth;
    if (!W) { return; }
    var H = W < 640 ? 300 : 420;
    var m = { l: 64, r: 18, t: 18, b: 34 };
    var iw = W - m.l - m.r, ih = H - m.t - m.b;
    navChart.innerHTML = '';
    var svg = el('svg', { viewBox: '0 0 ' + W + ' ' + H, width: W, height: H, role: 'img', 'aria-label': '净值曲线' });

    var lo = ALL_MIN, hi = ALL_MAX;
    var pad = (hi - lo) * 0.06;
    lo -= pad; hi += pad;

    function yv(v) { return logScale ? Math.log10(v) : v; }
    var y0 = yv(lo), y1 = yv(hi);
    function X(t) { return m.l + (t - T0) / (T1 - T0) * iw; }
    function Y(v) { return m.t + (1 - (yv(v) - y0) / (y1 - y0)) * ih; }

    svg.appendChild(el('rect', {
      x: X(DEEPEST.peakT), y: m.t, width: Math.max(1, X(DEEPEST.troughT) - X(DEEPEST.peakT)), height: ih,
      fill: 'rgba(212,81,79,.06)'
    }));

    var ticks = logScale ? logTicks(lo, hi) : niceTicks(lo, hi, 5);
    ticks.forEach(function (v) {
      var y = Y(v);
      svg.appendChild(el('line', { x1: m.l, x2: W - m.r, y1: y, y2: y, stroke: '#eef1f6', 'stroke-width': 1 }));
      var t = el('text', { x: m.l - 10, y: y + 4, 'text-anchor': 'end', 'font-size': '11', fill: '#95a0b3', 'font-family': 'ui-monospace,Menlo,monospace' });
      t.textContent = tickLabel(v);
      svg.appendChild(t);
    });

    yearTicks(W).forEach(function (t) {
      var x = X(t);
      svg.appendChild(el('line', { x1: x, x2: x, y1: m.t, y2: m.t + ih, stroke: '#f3f5f9', 'stroke-width': 1 }));
      var tx = el('text', { x: x, y: H - 12, 'text-anchor': 'middle', 'font-size': '11', fill: '#95a0b3', 'font-family': 'ui-monospace,Menlo,monospace' });
      tx.textContent = new Date(t).getUTCFullYear();
      svg.appendChild(tx);
    });

    svg.appendChild(el('line', { x1: m.l, x2: W - m.r, y1: m.t + ih, y2: m.t + ih, stroke: '#cfd6e2', 'stroke-width': 1 }));

    var first = SERIES[0];
    var dFirst = '';
    for (var i = 0; i < first.ts.length; i++) { dFirst += (i ? 'L' : 'M') + X(first.ts[i]).toFixed(2) + ' ' + Y(first.equity[i]).toFixed(2); }
    var grad = el('linearGradient', { id: 'navGrad', x1: '0', y1: '0', x2: '0', y2: '1' });
    grad.appendChild(el('stop', { offset: '0%', 'stop-color': first.color, 'stop-opacity': '.22' }));
    grad.appendChild(el('stop', { offset: '100%', 'stop-color': first.color, 'stop-opacity': '.01' }));
    var defs = el('defs'); defs.appendChild(grad); svg.appendChild(defs);
    var dArea = dFirst + 'L' + X(first.ts[first.ts.length - 1]).toFixed(2) + ' ' + (m.t + ih) +
                'L' + X(first.ts[0]).toFixed(2) + ' ' + (m.t + ih) + 'Z';
    svg.appendChild(el('path', { d: dArea, fill: 'url(#navGrad)' }));

    SERIES.forEach(function (s) {
      var d = '';
      for (var k = 0; k < s.ts.length; k++) { d += (k ? 'L' : 'M') + X(s.ts[k]).toFixed(2) + ' ' + Y(s.equity[k]).toFixed(2); }
      svg.appendChild(el('path', { d: d, fill: 'none', stroke: s.color, 'stroke-width': 1.7, 'stroke-linejoin': 'round' }));
    });

    [DEEPEST.peakT, DEEPEST.troughT].forEach(function (t) {
      svg.appendChild(el('line', { x1: X(t), x2: X(t), y1: m.t, y2: m.t + ih, stroke: 'rgba(212,81,79,.5)', 'stroke-width': 1, 'stroke-dasharray': '3 3' }));
    });

    var byEnd = SERIES.slice().sort(function (a, b) { return b.equity[b.equity.length - 1] - a.equity[a.equity.length - 1]; });
    byEnd.forEach(function (s, rank) {
      var last = s.equity[s.equity.length - 1];
      var x = X(s.ts[s.ts.length - 1]);
      svg.appendChild(el('circle', { cx: x, cy: Y(last), r: 3.2, fill: s.color, stroke: '#fff', 'stroke-width': 1.5 }));
      var t = el('text', { x: x - 6, y: Y(last) - 10 - rank * 14, 'text-anchor': 'end', 'font-size': '11.5', fill: '#3f4c63', 'font-family': 'ui-monospace,Menlo,monospace' });
      t.textContent = money(last);
      svg.appendChild(t);
    });

    var cross = el('line', { x1: 0, x2: 0, y1: m.t, y2: m.t + ih, stroke: '#8f9bb0', 'stroke-width': 1, 'stroke-dasharray': '3 3', opacity: '0' });
    var dots = SERIES.map(function (s) {
      var c = el('circle', { cx: 0, cy: 0, r: 3.2, fill: '#fff', stroke: s.color, 'stroke-width': 2, opacity: '0' });
      svg.appendChild(c); return c;
    });
    svg.appendChild(cross);

    var hit = el('rect', { x: m.l, y: m.t, width: iw, height: ih, fill: 'transparent', style: 'cursor:crosshair' });
    hit.addEventListener('mousemove', function (evt) {
      var t = hoverIndex(evt, svg, W, m, iw);
      var ref = SERIES[0];
      var idx = nearestIn(ref, t);
      var cx = X(ref.ts[idx]);
      cross.setAttribute('x1', cx); cross.setAttribute('x2', cx); cross.setAttribute('opacity', '1');
      var html = '<b>' + ref.dates[idx] + '</b>';
      SERIES.forEach(function (s, n) {
        var i = nearestIn(s, ref.ts[idx]);
        dots[n].setAttribute('cx', X(s.ts[i])); dots[n].setAttribute('cy', Y(s.equity[i])); dots[n].setAttribute('opacity', '1');
        html += '<br>' + s.name + ' ' + money(s.equity[i]) + ' · 回撤 ' + pct(s.dd[i]);
      });
      showTip(evt, navWrap, html);
    });
    hit.addEventListener('mouseleave', function () {
      cross.setAttribute('opacity', '0'); dots.forEach(function (d) { d.setAttribute('opacity', '0'); }); hideTip();
    });
    svg.appendChild(hit);
    navChart.appendChild(svg);
  }

  /* ---------- 回撤图 ---------- */
  function renderDD() {
    var W = ddWrap.clientWidth;
    if (!W) { return; }
    var H = W < 640 ? 270 : 340;
    var m = { l: 64, r: 18, t: 18, b: 34 };
    var iw = W - m.l - m.r, ih = H - m.t - m.b;
    ddChart.innerHTML = '';
    var svg = el('svg', { viewBox: '0 0 ' + W + ' ' + H, width: W, height: H, role: 'img', 'aria-label': '回撤曲线' });

    var lo = DD_MIN * 1.18, hi = 0;
    function X(t) { return m.l + (t - T0) / (T1 - T0) * iw; }
    function Y(v) { return m.t + (1 - (v - lo) / (hi - lo)) * ih; }

    svg.appendChild(el('rect', {
      x: X(DEEPEST.peakT), y: m.t, width: Math.max(1, X(DEEPEST.troughT) - X(DEEPEST.peakT)), height: ih,
      fill: 'rgba(212,81,79,.05)'
    }));

    [-0.1, -0.2, -0.3, -0.4, -0.5, -0.6].forEach(function (v) {
      if (v < lo) { return; }
      var y = Y(v);
      svg.appendChild(el('line', { x1: m.l, x2: W - m.r, y1: y, y2: y, stroke: '#eef1f6', 'stroke-width': 1 }));
      var t = el('text', { x: m.l - 10, y: y + 4, 'text-anchor': 'end', 'font-size': '11', fill: '#95a0b3', 'font-family': 'ui-monospace,Menlo,monospace' });
      t.textContent = (v * 100).toFixed(0) + '%';
      svg.appendChild(t);
    });
    svg.appendChild(el('line', { x1: m.l, x2: W - m.r, y1: Y(0), y2: Y(0), stroke: '#cfd6e2', 'stroke-width': 1 }));
    var zero = el('text', { x: m.l - 10, y: Y(0) + 4, 'text-anchor': 'end', 'font-size': '11', fill: '#95a0b3', 'font-family': 'ui-monospace,Menlo,monospace' });
    zero.textContent = '0%';
    svg.appendChild(zero);

    yearTicks(W).forEach(function (t) {
      var x = X(t);
      svg.appendChild(el('line', { x1: x, x2: x, y1: m.t, y2: m.t + ih, stroke: '#f3f5f9', 'stroke-width': 1 }));
      var tx = el('text', { x: x, y: H - 12, 'text-anchor': 'middle', 'font-size': '11', fill: '#95a0b3', 'font-family': 'ui-monospace,Menlo,monospace' });
      tx.textContent = new Date(t).getUTCFullYear();
      svg.appendChild(tx);
    });

    var first = SERIES[0];
    var grad = el('linearGradient', { id: 'ddGrad', x1: '0', y1: '0', x2: '0', y2: '1' });
    grad.appendChild(el('stop', { offset: '0%', 'stop-color': '#e07a78', 'stop-opacity': '.10' }));
    grad.appendChild(el('stop', { offset: '100%', 'stop-color': '#d4514f', 'stop-opacity': '.38' }));
    var defs = el('defs'); defs.appendChild(grad); svg.appendChild(defs);
    var dArea = 'M' + X(first.ts[0]).toFixed(2) + ' ' + Y(0).toFixed(2);
    for (var i = 0; i < first.ts.length; i++) { dArea += 'L' + X(first.ts[i]).toFixed(2) + ' ' + Y(first.dd[i]).toFixed(2); }
    dArea += 'L' + X(first.ts[first.ts.length - 1]).toFixed(2) + ' ' + Y(0).toFixed(2) + 'Z';
    svg.appendChild(el('path', { d: dArea, fill: 'url(#ddGrad)' }));

    SERIES.forEach(function (s) {
      var d = '';
      for (var k = 0; k < s.ts.length; k++) { d += (k ? 'L' : 'M') + X(s.ts[k]).toFixed(2) + ' ' + Y(s.dd[k]).toFixed(2); }
      svg.appendChild(el('path', { d: d, fill: 'none', stroke: s.ddcolor || s.color, 'stroke-width': 1.2 }));
    });

    svg.appendChild(el('line', {
      x1: m.l, x2: W - m.r, y1: Y(DEEPEST.mdd), y2: Y(DEEPEST.mdd),
      stroke: '#b23a38', 'stroke-width': 1, 'stroke-dasharray': '5 4'
    }));
    [DEEPEST.peakT, DEEPEST.troughT].forEach(function (t) {
      svg.appendChild(el('line', { x1: X(t), x2: X(t), y1: m.t, y2: m.t + ih, stroke: 'rgba(178,58,56,.45)', 'stroke-width': 1, 'stroke-dasharray': '3 3' }));
    });

    var labText = '最大回撤 ' + pct(DEEPEST.mdd) + '（' + DEEPEST.dates[DEEPEST.peakIdx] + ' → ' + DEEPEST.dates[DEEPEST.troughIdx] + '）';
    var asciiN = (labText.match(/[\x00-\x7F]/g) || []).length;
    var labW = asciiN * 6.6 + (labText.length - asciiN) * 11.6 + 18;
    var labX = m.l + 12;
    var labY = Math.min(Y(DEEPEST.mdd) + 21, m.t + ih - 3);
    svg.appendChild(el('rect', {
      x: labX - 9, y: labY - 14, width: labW, height: 21, rx: 5,
      fill: 'rgba(255,255,255,.94)', stroke: 'rgba(178,58,56,.35)'
    }));
    var lab = el('text', { x: labX, y: labY, 'font-size': '11.5', fill: '#b23a38', 'font-family': 'ui-monospace,Menlo,monospace' });
    lab.textContent = labText;
    svg.appendChild(lab);

    var cross = el('line', { x1: 0, x2: 0, y1: m.t, y2: m.t + ih, stroke: '#8f9bb0', 'stroke-width': 1, 'stroke-dasharray': '3 3', opacity: '0' });
    var dots = SERIES.map(function (s) {
      var c = el('circle', { cx: 0, cy: 0, r: 3.2, fill: '#fff', stroke: s.ddcolor || s.color, 'stroke-width': 2, opacity: '0' });
      svg.appendChild(c); return c;
    });
    svg.appendChild(cross);

    var hit = el('rect', { x: m.l, y: m.t, width: iw, height: ih, fill: 'transparent', style: 'cursor:crosshair' });
    hit.addEventListener('mousemove', function (evt) {
      var t = hoverIndex(evt, svg, W, m, iw);
      var ref = SERIES[0];
      var idx = nearestIn(ref, t);
      var cx = X(ref.ts[idx]);
      cross.setAttribute('x1', cx); cross.setAttribute('x2', cx); cross.setAttribute('opacity', '1');
      var html = '<b>' + ref.dates[idx] + '</b>';
      SERIES.forEach(function (s, n) {
        var i = nearestIn(s, ref.ts[idx]);
        dots[n].setAttribute('cx', X(s.ts[i])); dots[n].setAttribute('cy', Y(s.dd[i])); dots[n].setAttribute('opacity', '1');
        html += '<br>' + s.name + ' 回撤 ' + pct(s.dd[i]);
      });
      showTip(evt, ddWrap, html);
    });
    hit.addEventListener('mouseleave', function () {
      cross.setAttribute('opacity', '0'); dots.forEach(function (d) { d.setAttribute('opacity', '0'); }); hideTip();
    });
    svg.appendChild(hit);
    ddChart.appendChild(svg);
  }

  function setScale(isLog) {
    logScale = isLog;
    btnLinear.setAttribute('aria-pressed', isLog ? 'false' : 'true');
    btnLog.setAttribute('aria-pressed', isLog ? 'true' : 'false');
    renderNav();
  }
  btnLinear.addEventListener('click', function () { setScale(false); });
  btnLog.addEventListener('click', function () { setScale(true); });

  var pending = null;
  function schedule() {
    if (pending) { return; }
    pending = requestAnimationFrame(function () { pending = null; renderNav(); renderDD(); });
  }
  window.addEventListener('resize', schedule);
  if (window.ResizeObserver) {
    var ro = new ResizeObserver(schedule);
    ro.observe(navWrap);
    ro.observe(ddWrap);
  }
  renderNav();
  renderDD();
})();
"""

PAGE = r"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{{TITLE}}</title>
<style>{{STYLE}}</style>
</head>
<body>

<header class="hero">
  <div class="hero-inner">
    <div class="kicker">只读看板 · 不重跑回测 · 不改任何既有数值</div>
    <h1>{{H1}}</h1>
    <div class="sub">{{HERO_SUB}}</div>
    <div class="badges">{{BADGES}}</div>
  </div>
</header>

<main>
{{METRICS}}
{{NAV}}
{{DD}}
{{SEG}}
{{QUANT}}
{{SOURCES}}
</main>

<footer class="page-foot">
  {{FOOT}}
</footer>

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

def render_metrics(versions: List[Dict[str, Any]], slippage: Optional[str]) -> str:
    cards = []
    for idx, ver in enumerate(versions):
        m = ver['metrics']
        if not m:
            continue
        color = PALETTE[idx % len(PALETTE)]
        rows = []
        if m.get('annual') is not None:
            rows.append(f'<div class="metric"><span class="label">年化收益</span><span class="value">{pct(m["annual"])}</span></div>')
        if m.get('max_drawdown') is not None:
            rows.append(f'<div class="metric"><span class="label">最大回撤</span><span class="value neg">{pct(m["max_drawdown"])}</span></div>')
            dd_date = m.get('max_drawdown_date')
            if not dd_date and ver.get('stats'):
                dd_date = ver['stats'].get('trough')
            note = dd_date or '未落盘'
            rows.append(f'<div class="metric"><span class="label">回撤谷底日期</span><span class="value note" style="font-size:12.5px">{note}</span></div>')
        if m.get('turnover_two_way') is not None:
            rows.append(f'<div class="metric"><span class="label">年换手（双边）</span><span class="value">{xnum(m["turnover_two_way"])}</span></div>')
        if m.get('cost_share_of_gross') is not None:
            rows.append(f'<div class="metric"><span class="label">成本占毛收益</span><span class="value">{pct(m["cost_share_of_gross"])}</span></div>')

        bits = []
        for key, val in sorted((ver.get('vs') or {}).items()):
            if key.endswith('_annual'):
                bits.append(f'相对 {key[:-7]} 年化 <b>{pp(val)}</b>')
        if m.get('final_equity') is not None:
            bits.append(f'期末权益 <b>{float(m["final_equity"]) / 1e8:.2f} 亿元</b>')
        if m.get('cost_bps') is not None:
            bits.append(f'单位成本 <b>{float(m["cost_bps"]):.2f}bp</b>')
        bits.append('净值序列已落盘' if ver['equity'] else '仅指标落盘，无日净值序列')
        foot = ' · '.join(bits)

        cards.append(
            f'<article class="card {ver["id"]}" style="--c:{color}">'
            f'<div class="card-head"><span class="vtag">{ver["id"]}</span><h3>{ver["name"]}</h3>'
            + (f'<span class="role">{ver["role"]}</span>' if ver['role'] else '') +
            '</div>' + ''.join(rows) + f'<div class="card-foot">{foot}</div></article>')

    hints = f'全部为 {slippage}bp 滑点 + 冲击口径；最大回撤负值越小越深' if slippage else '全部为结果 JSON 记录的默认成本口径'
    notes = []
    for ver in versions:
        if ver['id'] == 'v3' and ver.get('verdict'):
            notes.append(f'<b>v3 判定（原文）：</b>{ver["verdict"]}')
    for ver in versions:
        if ver['boundary']:
            notes.append(f'<b>{ver["id"]} 边界（原文）：</b>{ver["boundary"]}')
    note_html = f'<div class="note-box">{" &nbsp;".join(notes)}</div>' if notes else ''
    return (
        '<section id="metrics">'
        '<div class="sec-head"><h2>一、核心指标（按版本并排）</h2>'
        f'<span class="hint">{hints}</span></div>'
        f'<div class="cards">{"".join(cards)}</div>{note_html}'
        '</section>'
    )


def render_nav(series: List[Dict[str, Any]], missing: List[str], capital: Any = None) -> str:
    if not series:
        return ''
    legend = ''.join(
        f'<span><i style="background:{s["color"]}"></i>{s["id"]} {s["name"]}</span>' for s in series)
    if capital:
        legend += f'<span class="dim">起点 = {float(capital) / 1e8:.2f} 亿</span>'
    total = len(series)
    note = (f'<b>覆盖：</b>本图叠加 {total} 条日净值序列；'
            + (f'{" / ".join(missing)} 未提供日净值，图中不出现，也未做任何缩放或拟合。' if missing else '未做任何缩放或拟合。'))
    return (
        '<section id="nav">'
        '<div class="sec-head"><h2>二、净值曲线</h2>'
        '<span class="hint">日频净值；可切换对数轴</span></div>'
        '<div class="panel"><div class="toolbar">'
        f'<div class="legend">{legend}</div>'
        '<div class="seg" role="group" aria-label="纵轴刻度">'
        '<button type="button" id="btnLinear" aria-pressed="true">线性轴</button>'
        '<button type="button" id="btnLog" aria-pressed="false">对数轴</button>'
        '</div></div>'
        '<div class="chart-wrap" id="navWrap"><div id="navChart"></div></div>'
        f'<div class="note-box">{note}</div>'
        f'<div class="src-line">来源：{"；".join(s["src"] for s in series)}</div>'
        '</div></section>'
    )


def render_dd(series: List[Dict[str, Any]]) -> str:
    if not series:
        return ''
    deepest = min(series, key=lambda s: s['stats']['max_drawdown'])
    st = deepest['stats']
    legend = ''.join(
        f'<span><i style="background:{s.get("ddcolor", s["color"])}"></i>{s["id"]} 回撤</span>' for s in series)
    return (
        '<section id="dd">'
        '<div class="sec-head"><h2>三、回撤曲线</h2>'
        '<span class="hint">相对历史峰值的水下深度，红色虚线标出最深回撤</span></div>'
        '<div class="panel"><div class="toolbar">'
        f'<div class="legend">{legend}'
        f'<span class="dim">最深回撤区间：{st["peak"]} → {st["trough"]}</span></div></div>'
        '<div class="chart-wrap" id="ddWrap"><div id="ddChart"></div></div>'
        f'<div class="note-box"><b>标注：</b>{deepest["id"]} 最大回撤 <b>{pct(st["max_drawdown"])}</b>，'
        f'峰值日 {st["peak"]}，谷底日 {st["trough"]}（由净值序列逐日计算，与指标文件一致）。</div>'
        f'<div class="src-line">来源：{"；".join(s["src"] for s in series)}</div>'
        '</div></section>'
    )


def render_segments(versions: List[Dict[str, Any]], periods: List[str],
                    seg_bounds: List[Tuple[str, date, date]],
                    src_line: str = '', tax_def: str = '') -> str:
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
        has = bool(ver['seg'] or ver['seg_stats'])
        if not has:
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

    missing_versions = [v for v in versions if not (v['seg'] or v['seg_stats'])]
    if missing_versions:
        body.append('<tr class="grp"><td colspan="' + str(len(labels) + 1) + '">增量</td></tr>')
        for label, vals in increments:
            cells = ''.join(
                f'<td class="num {"delta-pos" if (v is not None and v > 0) else "dim"}">{pp(v) if v is not None else "未落盘"}</td>'
                for v in vals)
            body.append(f'<tr><td>{label}</td>{cells}</tr>')
        for ver in missing_versions:
            body.append(f'<tr><td>{ver["id"]} 绝对年化 / 回撤</td>' +
                        ''.join('<td class="num dim">未落盘</td>' for _ in labels) + '</tr>')
    elif increments:
        body.append('<tr class="grp"><td colspan="' + str(len(labels) + 1) + '">增量</td></tr>')
        for label, vals in increments:
            cells = ''.join(
                f'<td class="num {"delta-pos" if (v is not None and v > 0) else "dim"}">{pp(v) if v is not None else "未落盘"}</td>'
                for v in vals)
            body.append(f'<tr><td>{label}</td>{cells}</tr>')

    head = ''.join(f'<th>{l}</th>' for l in labels)
    seg_table = (
        '<div class="panel"><table><thead><tr><th>口径</th>' + head + '</tr></thead>'
        f'<tbody>{"".join(body)}</tbody></table></div>'
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
            '<h2 style="font-size:14px">真值税制对照</h2></div>'
            '<table><thead><tr><th>版本 / 口径</th><th>年化</th><th>最大回撤</th><th>年换手</th><th>成本占比</th></tr></thead>'
            f'<tbody>{"".join(tax_rows)}</tbody></table>'
            + (f'<div class="info-box" style="margin-top:12px">偏差（原文）：{"；".join(biases)}。{tax_def_txt}</div>' if biases else '')
            + '</div>')

    layout = '<div class="two-col">' + seg_table + tax_panel + '</div>' if tax_panel else seg_table
    span = ' / '.join(f'{d0.isoformat()}~{d1.isoformat()}' for _, d0, d1 in seg_bounds)
    dagger_note = '由净值 CSV 按段切片计算，非指标文件原有字段。'
    if any(v['seg'] for v in versions):
        dagger_note += ' 带 † 的增量与 v2 分段数值来自指标 JSON 的落盘结果，与全样本指标存在四舍五入级差异。'
    return (
        '<section id="segments">'
        '<div class="sec-head"><h2>四、分段与增量</h2>'
        f'<span class="hint">两段：{span}</span></div>'
        f'{layout}'
        f'<div class="info-box" style="margin-top:12px"><span class="dim">†</span> {dagger_note}</div>'
        + (f'<div class="src-line">{src_line}</div>' if src_line else '') +
        '</section>'
    )


def render_quantiles(payload: Dict[str, Any], json_path: Path) -> str:
    quantiles = payload.get('quantiles')
    if isinstance(quantiles, dict) and quantiles.get('rows'):
        periods = quantiles.get('periods') or []
        head = ''.join(f'<th>{p}</th>' for p in periods)
        rows = ''.join('<tr><td>' + str(r.get('name', '')) + '</td>' +
                       ''.join(f'<td class="num">{r.get("values", {}).get(p, "")}</td>' for p in periods) + '</tr>'
                       for r in quantiles['rows'])
        inner = f'<table><thead><tr><th>分组</th>{head}</tr></thead><tbody>{rows}</tbody></table>'
    else:
        inner = ('<div class="info-box">核查结论：'
                 f'<code style="font-family:var(--mono)">{rel(json_path)}</code> '
                 '中未提供五分位结果（quantiles）字段，本节留空；不猜数、不补造图表。</div>')
    return ('<section id="quantiles"><div class="sec-head"><h2>五、五分位结果</h2>'
            '<span class="hint">低波 / 估值族：未落盘，不绘制</span></div>'
            f'<div class="panel">{inner}</div></section>')


def render_sources(payload: Dict[str, Any], json_path: Path,
                   series_src: List[Dict[str, Any]]) -> str:
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
        items.append(
            f'<li>日净值：<code>{rel(path)}</code>（列 <code>{meta["cols"]}</code>，{meta["rows"]:,} 行）<br>'
            f'sha256 <span class="hash">{short_sha(path)}</span> · 冻结 {mtime_text(path)} · '
            f'覆盖 {meta["start"]} ~ {meta["end"]} · 期末 {meta["final"]:,.2f}</li>')

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
        '<div class="sec-head"><h2>六、数据来源与可回溯信息</h2>'
        '<span class="hint">指纹为 sha256 前 8 位；冻结时间为文件修改时间</span></div>'
        f'<div class="panel"><ol class="src-list">{"".join(items)}</ol>'
        '<div class="info-box" style="margin-top:14px"><b>只读声明：</b>本页只读取既有结果 JSON 与净值 CSV，'
        '未修改回测引擎、未改动 <code style="font-family:var(--mono)">data/local_store</code>、未改成本默认值、未重跑回测；'
        '页面内所有数值与源文件一致。净值数据已内嵌进本 HTML（非读取同目录文件），因此双击即可离线打开。</div>'
        '</div></section>'
    )


# ---------------------------------------------------------------- 主流程

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
    args = ap.parse_args()

    json_path = Path(args.json)
    if not json_path.is_absolute():
        json_path = (Path.cwd() / json_path)
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
        meta = next((m for m in csv_meta if m['rows'] == len(dates) and m['start'] == dates[0]), None)
        path = meta['path'] if meta else None
        src = (f'{rel(path)} · sha256 {short_sha(path)} · 冻结 {mtime_text(path)}' if path else ver['id'])
        series.append({
            'id': ver['id'], 'name': ver['name'], 'color': PALETTE[idx % len(PALETTE)],
            'dates': dates, 'equity': [int(round(e)) for e in equity],
            'src': src, 'stats': ver['stats'],
        })
    missing = [f'{v["id"]} {v["name"]}' for v in versions if not v['equity']]
    for s in series:
        s['ddcolor'] = '#d4514f' if len(series) == 1 else s['color']

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
    hero_bits = [' / '.join(f'{v["id"]} {v["hero"]}'.strip() for v in versions)]
    if first_date and last_date:
        hero_bits.append(f'区间 {first_date} ~ {last_date}')
    if capital:
        hero_bits.append(f'初始资金 {float(capital) / 1e8:g} 亿元')
    if slippage:
        hero_bits.append(f'成本口径 {slippage}bp 滑点 + 冲击成本')
    for lim in payload.get('known_limitations') or []:
        hero_bits.append(str(lim))
    hero_sub = ' · '.join(b for b in hero_bits if b)

    badges = []
    if csv_meta:
        badges.append(f'数据冻结：净值 {mtime_text(csv_meta[0]["path"])} / 指标 {mtime_text(json_path)}')
    else:
        badges.append(f'数据冻结：指标 {mtime_text(json_path)}')
    badges.append(f'覆盖：{len(versions)} 个版本指标 · {len(series)} 条日净值 · {len(periods)} 个分段')
    if not payload.get('quantiles'):
        badges.append('五分位结果未落盘，未绘制')

    src_metric = (f'来源：{rel(json_path)} · sha256 {short_sha(json_path)} · 冻结 {mtime_text(json_path)}'
                  + (f' · head {payload["head"]}' if payload.get('head') else ''))

    html = PAGE
    html = html.replace('{{TITLE}}', args.title)
    html = html.replace('{{H1}}', payload.get('h1') or args.title)
    html = html.replace('{{HERO_SUB}}', hero_sub)
    html = html.replace('{{BADGES}}', ''.join(f'<span class="badge">{b}</span>' for b in badges))
    metrics = render_metrics(versions, slippage)
    if metrics:
        metrics = metrics.replace('</section>', f'<div class="src-line">{src_metric}</div></section>')
    html = html.replace('{{METRICS}}', metrics)
    html = html.replace('{{NAV}}', render_nav(series, missing, capital))
    html = html.replace('{{DD}}', render_dd(series))
    seg_src = f'来源：{rel(json_path)}（sha256 {short_sha(json_path)} · 冻结 {mtime_text(json_path)}）'
    if csv_meta:
        seg_src += ' + ' + '；'.join(
            f'{rel(m["path"])}（sha256 {short_sha(m["path"])} · 冻结 {mtime_text(m["path"])}）' for m in csv_meta)
    tax_def = str((payload.get('real_tax_script') or {}).get('def') or '')
    html = html.replace('{{SEG}}', render_segments(versions, periods, seg_bounds, seg_src, tax_def))
    html = html.replace('{{QUANT}}', render_quantiles(payload, json_path))
    html = html.replace('{{SOURCES}}', render_sources(payload, json_path, csv_meta))
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


def series_for_js(series: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [{'id': s['id'], 'name': s['name'], 'color': s['color'], 'ddcolor': s['ddcolor'],
             'dates': s['dates'], 'equity': s['equity']} for s in series]


if __name__ == '__main__':
    sys.exit(main())
