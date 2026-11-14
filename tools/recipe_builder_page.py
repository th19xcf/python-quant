#!/usr/bin/env python3
"""回测配方生成页：词表取自注册表与解析器常量 → 单文件 HTML。

只读工具：不跑回测、不写数据，只把「注册表 + 解析器规则」导出成前端可用的词表，
并生成 ``docs/配方生成_TIMESTAMP.html``（双击可开、无外网依赖）。

词表来源
--------
- 因子 / 剔除条件名、参数类型与范围：``registry.Registry.export()``
- 配方中文模板与中文名：``recipe.CONDITION_RECIPE`` / ``FACTOR_LABELS``
- 载入示例的初值：用 ``recipe.parse_recipe`` 解析
  ``examples/backtest_recipes/v3_monthly.recipe.txt`` 得到，不在页面里手写
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
for _p in (ROOT, ROOT / 'tools'):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

from backtest_result_dashboard import CSS_TEXT  # 与其他页面共用视觉变量
from src.backtest.runner import recipe as R
from src.backtest.runner.registry import CONDITIONS, FACTORS

DEFAULT_EXAMPLE = ROOT / 'examples' / 'backtest_recipes' / 'v3_monthly.recipe.txt'

FORM_CSS = r"""
  [hidden]{display:none !important}
  .wrap{max-width:1280px;margin:0 auto;padding:16px 20px 46px}
  .layout{display:grid;grid-template-columns:minmax(0,1.06fr) minmax(0,1fr);gap:16px;align-items:start}
  .panel{background:var(--panel);border:1px solid var(--line);border-radius:12px;box-shadow:var(--shadow)}
  .form-panel{padding:14px 16px 16px}
  h2.sec{font-size:13px;margin:16px 0 8px;font-weight:650;display:flex;align-items:center;gap:8px}
  h2.sec:first-child{margin-top:0}
  h2.sec .tag{font-family:var(--mono);font-size:10px;color:var(--faint);font-weight:500}
  .row{display:grid;gap:10px;margin-bottom:8px}
  .row.c2{grid-template-columns:repeat(2,minmax(0,1fr))}
  .row.c3{grid-template-columns:repeat(3,minmax(0,1fr))}
  .row.c4{grid-template-columns:repeat(4,minmax(0,1fr))}
  label.f{display:flex;flex-direction:column;gap:4px;font-size:12px;color:var(--muted);min-width:0}
  label.f > span{display:flex;justify-content:space-between;gap:6px}
  label.f em{font-style:normal;font-family:var(--mono);font-size:10px;color:var(--faint)}
  input[type=text],input[type=number],textarea,select{
    font:inherit;font-size:12.8px;padding:6px 9px;border:1px solid var(--line);border-radius:8px;
    background:var(--bg);color:var(--ink);outline:none;width:100%;min-width:0;
  }
  input:focus,textarea:focus,select:focus{border-color:var(--v1)}
  textarea{resize:vertical;line-height:1.6}
  textarea.mono,pre,code{font-family:var(--mono)}
  .radios{display:flex;gap:14px;font-size:12.8px;color:var(--ink);align-items:center;padding:2px 0}
  .radios label{display:inline-flex;align-items:center;gap:6px;cursor:pointer}
  .hint{font-size:11.5px;color:var(--faint);margin-top:4px}
  .chk{display:inline-flex;align-items:center;gap:7px;font-size:13px;color:var(--ink);cursor:pointer}
  .excl-row{
    display:grid;grid-template-columns:minmax(0,1.25fr) minmax(0,2fr) auto;gap:8px;
    align-items:center;border:1px solid var(--line2);border-radius:10px;padding:7px 8px;margin-bottom:7px;
    background:var(--bg);
  }
  .ex-params{display:flex;gap:7px;flex-wrap:wrap;align-items:center}
  .ex-params label{display:flex;align-items:center;gap:5px;font-size:11.5px;color:var(--muted)}
  .ex-params input{width:86px;padding:4px 7px;font-size:12px;background:var(--panel)}
  .ex-ops{display:flex;gap:4px}
  .ex-ops button,.mini{
    border:1px solid var(--line);background:var(--panel);color:var(--muted);font:inherit;
    font-size:12px;line-height:1;padding:5px 7px;border-radius:7px;cursor:pointer;
  }
  .ex-ops button:hover,.mini:hover{color:var(--ink)}
  .btn{
    font:inherit;font-size:12.5px;padding:6px 12px;border-radius:8px;cursor:pointer;
    border:1px solid var(--line);background:var(--panel);color:var(--ink);
  }
  .btn.primary{background:var(--v1);border-color:var(--v1);color:#fff;font-weight:600}
  .btn:disabled{opacity:.45;cursor:not-allowed}
  .err-box{
    border:1px solid var(--neg);border-radius:11px;padding:9px 12px;margin-bottom:10px;
    background:color-mix(in srgb, var(--neg) 7%, var(--panel));font-size:12.5px;color:var(--ink);
  }
  .err-box h3{margin:0 0 5px;font-size:12.5px;color:var(--neg)}
  .err-box ul{margin:0;padding-left:18px}
  .err-box li{margin:2px 0}
  .out-col{position:sticky;top:12px}
  .out-panel{display:flex;flex-direction:column;min-height:420px}
  .out-head{display:flex;align-items:center;justify-content:space-between;gap:10px;padding:11px 14px;border-bottom:1px solid var(--line)}
  .out-head h2{margin:0;font-size:13.5px;font-weight:650;display:flex;align-items:center;gap:8px}
  .out-head .state{font-family:var(--mono);font-size:10.5px;color:var(--muted)}
  .out-head .state.bad{color:var(--neg)}
  .out-body{padding:10px 14px 12px;display:flex;flex-direction:column;gap:8px;flex:1}
  pre#out{
    margin:0;flex:1;min-height:330px;max-height:calc(100vh - 210px);overflow:auto;
    background:var(--bg);border:1px solid var(--line2);border-radius:10px;padding:11px 12px;
    font-size:12.2px;line-height:1.72;white-space:pre-wrap;word-break:break-word;color:var(--ink);
  }
  pre#out.empty{color:var(--faint)}
  .out-foot{display:flex;gap:8px;align-items:center;flex-wrap:wrap}
  .out-foot .note{font-size:11.5px;color:var(--faint)}
  @media (max-width:980px){
    .layout{grid-template-columns:1fr}
    .out-col{position:static}
    pre#out{max-height:520px}
    .row.c4{grid-template-columns:repeat(2,minmax(0,1fr))}
  }
  @media (max-width:620px){
    .row.c2,.row.c3,.row.c4{grid-template-columns:1fr}
    .excl-row{grid-template-columns:1fr;gap:6px}
    .ex-ops{justify-content:flex-end}
  }
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

<header class="topbar">
  <div class="topbar-in">
    <h1>{{TITLE}}</h1>
    <div class="meta">
      <span>词表与校验规则来自 <b>runner/registry.py</b> 与 <b>runner/recipe.py</b></span>
      <span>生成 <b>{{BUILD_DATE}}</b></span>
      <span>索引 <b>{{VOCAB_SHA}}</b></span>
    </div>
  </div>
</header>

<main class="wrap">
  <div class="layout">
    <section class="panel form-panel">
      <div id="errors" class="err-box" hidden>
        <h3 id="err-title"></h3><ul id="err-list"></ul>
      </div>

      <h2 class="sec">基础 <span class="tag">名称 / 说明 / 注释头</span></h2>
      <div class="row c2">
        <label class="f"><span>名称 <em>必填 · snake_case</em></span>
          <input type="text" id="f_name" placeholder="v3_monthly" autocomplete="off"></label>
        <label class="f"><span>注释头 <em>可选 · 原样写入，不随参数更新</em></span>
          <textarea id="f_comments" class="mono" rows="2" placeholder="# 一行说明"></textarea></label>
      </div>
      <div class="row">
        <label class="f"><span>说明 <em>可选</em></span>
          <input type="text" id="f_desc" placeholder="一句话描述口径" autocomplete="off"></label>
      </div>

      <h2 class="sec">时间区间与预热 <span class="tag">区间 / 预热</span></h2>
      <div class="row c3">
        <label class="f"><span>起点 <em>YYYY-MM-DD</em></span>
          <input type="text" id="f_start" placeholder="2010-01-04" autocomplete="off"></label>
        <label class="f"><span>终点 <em>YYYY-MM-DD</em></span>
          <input type="text" id="f_end" placeholder="2026-11-13" autocomplete="off"></label>
        <label class="f"><span>预热天数 <em>交易日</em></span>
          <input type="text" id="f_warm" placeholder="103" inputmode="numeric" autocomplete="off"></label>
      </div>

      <h2 class="sec">股票池 <span class="tag">全市场 / 指定</span></h2>
      <div class="radios">
        <label><input type="radio" name="universe" value="all_market" id="u_all" checked> 全市场</label>
        <label><input type="radio" name="universe" value="explicit" id="u_explicit"> 指定代码清单</label>
      </div>
      <div class="row" id="codes-wrap" hidden>
        <label class="f"><span>代码清单 <em>一行一个 · 000001.SZ</em></span>
          <textarea id="f_codes" class="mono" rows="4" placeholder="000001.SZ&#10;600000.SH"></textarea></label>
      </div>

      <h2 class="sec">剔除条件链 <span class="tag">顺序即执行顺序</span></h2>
      <div id="excl"></div>
      <button type="button" class="mini" id="add-excl">＋ 添加剔除条件</button>

      <h2 class="sec">因子与分档 <span class="tag">可选</span></h2>
      <label class="chk"><input type="checkbox" id="f_factor_on"> 启用因子分档（不勾选则等权全持）</label>
      <div id="factor-wrap" hidden>
        <div class="row c4" style="margin-top:8px">
          <label class="f"><span>因子</span><select id="f_factor_name"></select></label>
          <label class="f"><span>档数 <em>2~20</em></span>
            <input type="text" id="f_factor_buckets" value="5" inputmode="numeric"></label>
          <label class="f"><span>方向</span>
            <select id="f_factor_side">
              <option value="low">最低档</option>
              <option value="high">最高档</option>
            </select></label>
          <label class="f"><span>动作</span>
            <select id="f_factor_action">
              <option value="hold_bucket">取该档</option>
              <option value="exclude_bucket">剔除该档</option>
            </select></label>
        </div>
        <div class="row c2" id="factor-window-wrap" hidden>
          <label class="f"><span>因子窗口 <em>交易日</em></span>
            <input type="text" id="f_factor_window" value="60" inputmode="numeric"></label>
          <div class="hint" id="factor-hint"></div>
        </div>
      </div>

      <h2 class="sec">调仓 <span class="tag">频率 / 首次</span></h2>
      <div class="row c2">
        <label class="f"><span>频率</span>
          <select id="f_freq"></select></label>
        <label class="f"><span>首次调仓日 <em>可留空</em></span>
          <input type="text" id="f_first" placeholder="2009-09-01" autocomplete="off"></label>
      </div>

      <h2 class="sec">成本 <span class="tag">滑点 / 税 / 佣金</span></h2>
      <div class="row c3">
        <label class="f"><span>滑点 <em>bp</em></span>
          <input type="text" id="f_slip" inputmode="decimal"></label>
        <label class="f"><span>印花税口径</span>
          <select id="f_stamp"></select></label>
        <label class="f"><span>佣金 <em>万分之</em></span>
          <input type="text" id="f_comm" inputmode="decimal"></label>
      </div>
      <div class="row c2">
        <label class="f"><span>最低佣金 <em>元</em></span>
          <input type="text" id="f_mincomm" inputmode="decimal"></label>
        <label class="f"><span>冲击系数</span>
          <input type="text" id="f_impact" inputmode="decimal"></label>
      </div>

      <h2 class="sec">资金 <span class="tag">初始资金 / 取整 / 缓冲</span></h2>
      <div class="row c4">
        <label class="f"><span>初始资金 <em>亿</em></span>
          <input type="text" id="f_capital" inputmode="decimal"></label>
        <label class="f"><span>价格取整</span>
          <select id="f_lot"></select></label>
        <label class="f"><span>现金缓冲 <em>%</em></span>
          <input type="text" id="f_buffer" inputmode="decimal"></label>
        <label class="f"><span>现金池</span>
          <select id="f_pooled"></select></label>
      </div>

      <div class="row" style="margin-top:14px;grid-template-columns:auto auto;justify-content:start">
        <button type="button" class="btn" id="load-example">载入 v3 示例</button>
        <button type="button" class="btn" id="reset">重置为默认值</button>
      </div>
    </section>

    <aside class="out-col">
      <div class="panel out-panel">
        <div class="out-head">
          <h2>配方文本 <span class="state" id="out-state"></span></h2>
          <button type="button" class="btn primary" id="copy">复制配方</button>
        </div>
        <div class="out-body">
          <pre id="out" class="empty" aria-live="polite"></pre>
          <div class="out-foot">
            <span class="note">保存为 *.recipe.txt 后可用 <code>run_backtest_spec.py --recipe ... --parse-only</code> 校验。</span>
          </div>
        </div>
      </div>
    </aside>
  </div>
</main>

<script id="vocab" type="application/json">{{VOCAB}}</script>
<script id="example-state" type="application/json">{{EXAMPLE}}</script>
<script id="default-state" type="application/json">{{DEFAULT}}</script>
<script>{{JS}}</script>
</body>
</html>
"""

JS = r"""
(function () {
  'use strict';
  var VOCAB = JSON.parse(document.getElementById('vocab').textContent);
  var EXAMPLE = JSON.parse(document.getElementById('example-state').textContent);
  var DEFAULT = JSON.parse(document.getElementById('default-state').textContent);
  var COND = {}, FACT = {};
  VOCAB.conditions.forEach(function (c) { COND[c.name] = c; });
  VOCAB.factors.forEach(function (f) { FACT[f.name] = f; });

  var $ = function (id) { return document.getElementById(id); };
  var out = $('out'), stateEl = $('out-state'), copyBtn = $('copy');
  var errBox = $('errors'), errTitle = $('err-title'), errList = $('err-list');

  function trim(v) {
    if (v === null || v === undefined || v === '' || !isFinite(Number(v))) { return ''; }
    return String(parseFloat(Number(v).toFixed(10)));
  }
  function pctText(v) { return trim(Number(v) * 100) + '%'; }
  function money(v) {
    var n = Number(v);
    if (Math.abs(n % 1e8) < 1e-6) { return trim(n / 1e8) + ' 亿'; }
    if (Math.abs(n % 1e4) < 1e-6) { return trim(n / 1e4) + ' 万'; }
    return trim(n);
  }
  function isDate(s) { return /^\d{4}-\d{2}-\d{2}$/.test(s) && !isNaN(Date.parse(s + 'T00:00:00Z')); }

  // ---------- 剔除条件行 ----------
  function paramInput(row, p, value) {
    var wrap = document.createElement('label');
    var label = VOCAB.param_labels[p.name] || p.name;
    wrap.appendChild(document.createTextNode(label));
    var inp = document.createElement('input');
    inp.type = 'text';
    inp.setAttribute('inputmode', p.kind === 'int' ? 'numeric' : 'decimal');
    inp.dataset.param = p.name;
    var percent = VOCAB.percent_params.indexOf(p.name) >= 0;
    if (percent) { inp.dataset.percent = '1'; }
    var v = value;
    if (v === null || v === undefined) { v = p.default; }
    inp.value = (v === null || v === undefined) ? '' : trim(percent ? Number(v) * 100 : v);
    var range = [];
    if (p.kind === 'int') { range.push('整数'); }
    if (p.minimum !== null && p.minimum !== undefined) { range.push('≥' + p.minimum); }
    if (p.maximum !== null && p.maximum !== undefined) { range.push('≤' + p.maximum); }
    if (p.required) { range.push('必填'); }
    inp.title = label + (range.length ? '（' + range.join('，') + '）' : '');
    wrap.appendChild(inp);
    return wrap;
  }

  function fillParams(row, values) {
    var box = row.querySelector('.ex-params');
    box.innerHTML = '';
    var entry = COND[row.querySelector('.ex-name').value];
    if (!entry) { return; }
    entry.params.forEach(function (p) {
      box.appendChild(paramInput(row, p, values ? values[p.name] : undefined));
    });
  }

  function addRow(name, params) {
    var row = document.createElement('div');
    row.className = 'excl-row';
    var sel = document.createElement('select');
    sel.className = 'ex-name';
    var opt0 = document.createElement('option');
    opt0.value = ''; opt0.textContent = '— 选择条件 —';
    sel.appendChild(opt0);
    VOCAB.conditions.forEach(function (c) {
      var o = document.createElement('option');
      o.value = c.name;
      o.textContent = c.name;
      o.title = c.doc;
      sel.appendChild(o);
    });
    sel.value = name || '';
    var box = document.createElement('div');
    box.className = 'ex-params';
    var ops = document.createElement('div');
    ops.className = 'ex-ops';
    [['up', '↑'], ['down', '↓'], ['del', '✕']].forEach(function (it) {
      var b = document.createElement('button');
      b.type = 'button';
      b.dataset.op = it[0];
      b.textContent = it[1];
      b.title = { up: '上移', down: '下移', del: '删除' }[it[0]];
      ops.appendChild(b);
    });
    row.appendChild(sel); row.appendChild(box); row.appendChild(ops);
    $('excl').appendChild(row);
    sel.addEventListener('change', function () { fillParams(row, null); render(); });
    row.addEventListener('input', render);
    row.addEventListener('change', render);
    ops.addEventListener('click', function (ev) {
      var op = ev.target.dataset.op;
      if (!op) { return; }
      if (op === 'del') { row.remove(); }
      if (op === 'up' && row.previousElementSibling) {
        $('excl').insertBefore(row, row.previousElementSibling);
      }
      if (op === 'down' && row.nextElementSibling) {
        $('excl').insertBefore(row.nextElementSibling, row);
      }
      render();
    });
    fillParams(row, params);
  }

  function readRows() {
    var rows = [];
    Array.prototype.forEach.call(document.querySelectorAll('.excl-row'), function (row) {
      var name = row.querySelector('.ex-name').value;
      var params = {};
      var missing = {};
      Array.prototype.forEach.call(row.querySelectorAll('input[data-param]'), function (inp) {
        var key = inp.dataset.param;
        var raw = inp.value.trim();
        if (raw === '') { missing[key] = true; params[key] = null; return; }
        var v = Number(raw);
        params[key] = inp.dataset.percent === '1' ? v / 100 : v;
      });
      rows.push({ name: name, params: params, missing: missing });
    });
    return rows;
  }

  // ---------- 读取表单 ----------
  function readState() {
    var mode = $('u_explicit').checked ? 'explicit' : 'all_market';
    var codes = $('f_codes').value.split(/\r?\n/).map(function (s) { return s.trim().toUpperCase(); })
      .filter(function (s) { return s.length > 0; });
    return {
      comments: $('f_comments').value,
      name: $('f_name').value.trim(),
      description: $('f_desc').value.trim(),
      start: $('f_start').value.trim(),
      end: $('f_end').value.trim(),
      warmup: $('f_warm').value.trim(),
      universeMode: mode,
      codes: codes,
      exclusions: readRows(),
      factorOn: $('f_factor_on').checked,
      factor: {
        name: $('f_factor_name').value,
        buckets: $('f_factor_buckets').value.trim(),
        action: $('f_factor_action').value,
        side: $('f_factor_side').value,
        window: $('f_factor_window').value.trim()
      },
      freq: $('f_freq').value,
      firstDate: $('f_first').value.trim(),
      cost: {
        slippage: $('f_slip').value.trim(),
        stamp: $('f_stamp').value,
        commission: $('f_comm').value.trim(),
        minCommission: $('f_mincomm').value.trim(),
        impact: $('f_impact').value.trim()
      },
      capital: $('f_capital').value.trim(),
      lot: $('f_lot').value,
      buffer: $('f_buffer').value.trim(),
      pooled: $('f_pooled').value === 'true'
    };
  }

  // ---------- 校验（与解析器同源：词表、参数范围、重复、必填） ----------
  function validate(s) {
    var e = [];
    if (!s.name) { e.push('名称：必填（示例 v3_monthly）'); }
    else if (!/^[a-z0-9_]+$/.test(s.name)) { e.push('名称：只能用小写字母/数字/下划线（snake_case）'); }
    else if (/^\d/.test(s.name)) { e.push('名称：不能以数字开头'); }

    (s.comments || '').split(/\r?\n/).forEach(function (line, i) {
      var t = line.trim();
      if (t && t.charAt(0) !== '#') { e.push('注释头第 ' + (i + 1) + ' 行：必须以 # 开头，否则会被解析器当作规则行'); }
    });

    if (!s.start) { e.push('区间：起点必填（YYYY-MM-DD）'); }
    else if (!isDate(s.start)) { e.push('区间：起点不是合法日期（YYYY-MM-DD）'); }
    if (!s.end) { e.push('区间：终点必填（YYYY-MM-DD）'); }
    else if (!isDate(s.end)) { e.push('区间：终点不是合法日期（YYYY-MM-DD）'); }
    if (isDate(s.start) && isDate(s.end) && s.start > s.end) { e.push('区间：起点不能晚于终点'); }
    if (!/^\d+$/.test(s.warmup)) { e.push('预热天数：必须是非负整数（按交易日折算）'); }

    if (s.universeMode === 'explicit') {
      if (!s.codes.length) { e.push('股票池：选择「指定」时代码清单不能为空'); }
      s.codes.forEach(function (c) {
        if (!/^\d{6}\.(SH|SZ|BJ)$/.test(c)) { e.push('股票池：代码 ' + c + ' 格式非法，应为 000001.SZ / 600000.SH'); }
      });
    }

    var seen = [];
    s.exclusions.forEach(function (row, i) {
      var at = '剔除链第 ' + (i + 1) + ' 条：';
      if (!row.name) { e.push(at + '未选择条件'); return; }
      if (seen.indexOf(row.name) >= 0) { e.push(at + '条件 ' + row.name + ' 重复出现，同一条件只允许出现一次'); }
      seen.push(row.name);
      var entry = COND[row.name];
      entry.params.forEach(function (p) {
        var v = row.params[p.name];
        if (row.missing[p.name]) {
          if (p.required) { e.push(at + '条件 ' + row.name + ' 缺少必填参数 ' + p.name); }
          return;
        }
        if (v === null || v === undefined || !isFinite(v)) { e.push(at + '参数 ' + p.name + ' 必须是数字'); return; }
        if (p.kind === 'int' && Math.abs(v - Math.round(v)) > 1e-9) { e.push(at + '参数 ' + p.name + ' 必须是整数'); }
        if (p.minimum !== null && p.minimum !== undefined && v < p.minimum) { e.push(at + '参数 ' + p.name + '=' + trim(v) + ' 小于下限 ' + p.minimum); }
        if (p.maximum !== null && p.maximum !== undefined && v > p.maximum) { e.push(at + '参数 ' + p.name + '=' + trim(v) + ' 大于上限 ' + p.maximum); }
        if (VOCAB.unit_fraction_conditions.indexOf(row.name) >= 0 && p.name === 'pct') {
          var n = Math.round(1 / v);
          if (!(v > 0 && v < 1) || n < VOCAB.unit_fraction_range[0] || n > VOCAB.unit_fraction_range[1]
              || Math.abs(n * v - 1) > 1e-9) {
            e.push(at + '比例 ' + pctText(v) + ' 无法换算成整档：请写成 1/n（n=' +
              VOCAB.unit_fraction_range[0] + '~' + VOCAB.unit_fraction_range[1] + '），例如 20% → 5 档');
          }
        }
      });
    });

    if (s.factorOn) {
      if (!FACT[s.factor.name]) { e.push('因子：未选择已注册的因子'); }
      if (!/^\d+$/.test(s.factor.buckets) || Number(s.factor.buckets) < 2 || Number(s.factor.buckets) > 20) {
        e.push('因子：档数必须是 2~20 的整数');
      }
      if (s.factor.name === 'vol' && !/^\d+$/.test(s.factor.window)) {
        e.push('因子：波动率窗口必须是整数交易日（≥2）');
      } else if (s.factor.name === 'vol' && Number(s.factor.window) < 2) {
        e.push('因子：波动率窗口必须 ≥2');
      }
    }

    if (!s.freq) { e.push('调仓：频率必选'); }
    if (s.firstDate && !isDate(s.firstDate)) { e.push('调仓：首次日期不是合法日期（YYYY-MM-DD）'); }

    if (s.cost.slippage === '') { e.push('成本：滑点必填（bp）'); }
    else if (!isFinite(Number(s.cost.slippage)) || Number(s.cost.slippage) < 0) { e.push('成本：滑点必须是非负数字'); }
    if (!s.cost.stamp) { e.push('成本：印花税口径必选'); }
    if (s.cost.commission !== '' && (!isFinite(Number(s.cost.commission)) || Number(s.cost.commission) < 0)) { e.push('成本：佣金必须是非负数字'); }
    if (s.cost.minCommission !== '' && (!isFinite(Number(s.cost.minCommission)) || Number(s.cost.minCommission) < 0)) { e.push('成本：最低佣金必须是非负数字'); }
    if (s.cost.impact !== '' && (!isFinite(Number(s.cost.impact)) || Number(s.cost.impact) < 0)) { e.push('成本：冲击系数必须是非负数字'); }

    if (s.capital === '' || !isFinite(Number(s.capital)) || Number(s.capital) <= 0) { e.push('资金：初始资金必须是正数（亿）'); }
    if (s.buffer === '' || !isFinite(Number(s.buffer)) || Number(s.buffer) < 0 || Number(s.buffer) >= 100) {
      e.push('资金：现金缓冲必须在 0~100%（不含 100%）');
    }
    return e;
  }

  // ---------- 生成配方 ----------
  function condLine(row) {
    var entry = COND[row.name];
    var tpl = VOCAB.condition_recipe[row.name] || ('剔除 ' + row.name);
    return tpl.replace(/\{(\w+)(?::(\w+))?\}/g, function (m, key, fmt) {
      var v = row.params[key];
      if (v === null || v === undefined) {
        var p = null;
        entry.params.forEach(function (x) { if (x.name === key) { p = x; } });
        v = p ? p.default : 0;
      }
      return fmt === 'percent' ? pctText(v) : trim(v);
    });
  }

  function buildRecipe(s) {
    var lines = [];
    (s.comments || '').split(/\r?\n/).forEach(function (line) {
      if (line.trim()) { lines.push(line.trim()); }
    });
    lines.push('名称 ' + s.name);
    if (s.description) { lines.push('说明 ' + s.description); }
    lines.push('区间 ' + s.start + ' ~ ' + s.end + '，预热 ' + s.warmup + ' 个交易日');
    lines.push('股票池 ' + (s.universeMode === 'all_market' ? '全市场' : '指定 ' + s.codes.join(', ')));
    s.exclusions.forEach(function (row) { lines.push(condLine(row)); });
    if (s.factorOn) {
      var f = s.factor, n = Number(f.buckets);
      var line = '因子 ' + VOCAB.factor_labels[f.name] + ' 分 ' + n + ' 档，' +
        (f.action === 'hold_bucket' ? '取' : '剔除') + ' ' +
        (f.side === 'low' ? '最低' : '最高') + ' ' + trim(100 / n) + '%';
      if (f.name === 'vol') { line += '，窗口 ' + f.window + ' 个交易日'; }
      lines.push(line);
    }
    var freqText = VOCAB.frequency_labels[s.freq] || s.freq;
    lines.push('调仓 ' + freqText + (s.firstDate ? '，首次 ' + s.firstDate : ''));
    var cost = ['滑点 ' + trim(s.cost.slippage) + 'bp',
      '印花税 ' + (s.cost.stamp === 'historical' ? '真值' : '固定千1')];
    if (s.cost.commission !== '') { cost.push('佣金 万' + trim(Number(s.cost.commission))); }
    if (s.cost.minCommission !== '') { cost.push('最低佣金 ' + trim(Number(s.cost.minCommission)) + ' 元'); }
    if (s.cost.impact !== '') { cost.push('冲击 ' + trim(Number(s.cost.impact))); }
    lines.push('成本 ' + cost.join('，'));
    var lotText = s.lot === '100' ? '按手' : '按股';
    lines.push('资金 ' + money(Number(s.capital) * 1e8) + '，价格取整' + lotText +
      '，现金缓冲 ' + trim(Number(s.buffer)) + '%，' + (s.pooled ? '共用现金池' : '独立现金池'));
    return lines.join('\n') + '\n';
  }

  // ---------- 渲染 ----------
  function render() {
    var s = readState();
    var errs = validate(s);
    if (errs.length) {
      errBox.hidden = false;
      errTitle.textContent = '配方未生成：' + errs.length + ' 处需要修正';
      errList.innerHTML = '';
      errs.forEach(function (msg) {
        var li = document.createElement('li');
        li.textContent = msg;
        errList.appendChild(li);
      });
      out.classList.add('empty');
      out.textContent = '配方未生成。左侧还有 ' + errs.length + ' 处问题，修正后会自动刷新。';
      stateEl.textContent = '未生成';
      stateEl.className = 'state bad';
      copyBtn.disabled = true;
      copyBtn.textContent = '复制配方';
      window.__recipe = '';
      return;
    }
    errBox.hidden = true;
    var recipe = buildRecipe(s);
    out.classList.remove('empty');
    out.textContent = recipe;
    stateEl.textContent = '可直接保存为 .recipe.txt';
    stateEl.className = 'state';
    copyBtn.disabled = false;
    window.__recipe = recipe;
  }

  // ---------- 填充 ----------
  function applyState(st) {
    $('f_comments').value = st.comments || '';
    $('f_name').value = st.name || '';
    $('f_desc').value = st.description || '';
    $('f_start').value = st.start || '';
    $('f_end').value = st.end || '';
    $('f_warm').value = st.warmup === null || st.warmup === undefined ? '' : String(st.warmup);
    $('u_all').checked = st.universeMode !== 'explicit';
    $('u_explicit').checked = st.universeMode === 'explicit';
    $('codes-wrap').hidden = st.universeMode !== 'explicit';
    $('f_codes').value = (st.codes || []).join('\n');
    $('excl').innerHTML = '';
    (st.exclusions || []).forEach(function (row) { addRow(row.name, row.params); });
    $('f_factor_on').checked = !!st.factorOn;
    $('factor-wrap').hidden = !st.factorOn;
    if (st.factor) {
      $('f_factor_name').value = st.factor.name || 'vol';
      $('f_factor_buckets').value = String(st.factor.buckets === undefined ? 5 : st.factor.buckets);
      $('f_factor_action').value = st.factor.action || 'hold_bucket';
      $('f_factor_side').value = st.factor.side || 'low';
      if (st.factor.window !== null && st.factor.window !== undefined) {
        $('f_factor_window').value = String(st.factor.window);
      }
    }
    $('f_freq').value = st.freq || 'monthly';
    $('f_first').value = st.firstDate || '';
    $('f_slip').value = st.cost.slippage === null || st.cost.slippage === undefined ? '' : String(st.cost.slippage);
    $('f_stamp').value = st.cost.stamp || 'fixed_0.1%';
    $('f_comm').value = st.cost.commission === null || st.cost.commission === undefined ? '' : String(st.cost.commission);
    $('f_mincomm').value = st.cost.minCommission === null || st.cost.minCommission === undefined ? '' : String(st.cost.minCommission);
    $('f_impact').value = st.cost.impact === null || st.cost.impact === undefined ? '' : String(st.cost.impact);
    $('f_capital').value = st.capital === null || st.capital === undefined ? '' : String(st.capital);
    $('f_lot').value = String(st.lot || '100');
    $('f_buffer').value = st.buffer === null || st.buffer === undefined ? '' : String(st.buffer);
    $('f_pooled').value = st.pooled === false ? 'false' : 'true';
    syncFactor();
    render();
  }

  function syncFactor() {
    var on = $('f_factor_on').checked;
    $('factor-wrap').hidden = !on;
    var name = $('f_factor_name').value;
    $('factor-window-wrap').hidden = !(on && name === 'vol');
    var entry = FACT[name];
    if (entry) {
      $('factor-hint').textContent = entry.doc;
    }
  }

  // ---------- 初始化选项 ----------
  VOCAB.factors.forEach(function (f) {
    var o = document.createElement('option');
    o.value = f.name;
    o.textContent = f.name + '（' + (VOCAB.factor_labels[f.name] || f.name) + '）';
    $('f_factor_name').appendChild(o);
  });
  VOCAB.frequencies.forEach(function (it) {
    var o = document.createElement('option');
    o.value = it[0]; o.textContent = it[1];
    $('f_freq').appendChild(o);
  });
  VOCAB.stamps.forEach(function (it) {
    var o = document.createElement('option');
    o.value = it[0]; o.textContent = it[1];
    $('f_stamp').appendChild(o);
  });
  VOCAB.lots.forEach(function (it) {
    var o = document.createElement('option');
    o.value = it[0]; o.textContent = it[1];
    $('f_lot').appendChild(o);
  });
  [['true', '共用现金池'], ['false', '独立现金池']].forEach(function (it) {
    var o = document.createElement('option');
    o.value = it[0]; o.textContent = it[1];
    $('f_pooled').appendChild(o);
  });

  // ---------- 事件 ----------
  Array.prototype.forEach.call(document.querySelectorAll('input,textarea,select'), function (el) {
    el.addEventListener('input', render);
    el.addEventListener('change', render);
  });
  $('u_all').addEventListener('change', function () { $('codes-wrap').hidden = true; render(); });
  $('u_explicit').addEventListener('change', function () { $('codes-wrap').hidden = false; render(); });
  $('f_factor_on').addEventListener('change', function () { syncFactor(); render(); });
  $('f_factor_name').addEventListener('change', syncFactor);
  $('add-excl').addEventListener('click', function () { addRow('', null); render(); });
  $('load-example').addEventListener('click', function () { applyState(EXAMPLE); });
  $('reset').addEventListener('click', function () { applyState(DEFAULT); });
  copyBtn.addEventListener('click', function () {
    var text = window.__recipe || '';
    if (!text) { return; }
    var done = function () {
      copyBtn.textContent = '已复制';
      setTimeout(function () { copyBtn.textContent = '复制配方'; }, 1400);
    };
    if (navigator.clipboard && navigator.clipboard.writeText) {
      navigator.clipboard.writeText(text).then(done, function () { fallbackCopy(text, done); });
    } else {
      fallbackCopy(text, done);
    }
  });
  function fallbackCopy(text, done) {
    var ta = document.createElement('textarea');
    ta.value = text;
    ta.style.position = 'fixed';
    ta.style.opacity = '0';
    document.body.appendChild(ta);
    ta.select();
    try { document.execCommand('copy'); done(); } catch (e) { /* 忽略：浏览器不允许时用户可手动选中复制 */ }
    document.body.removeChild(ta);
  }

  applyState(DEFAULT);
})();
"""


def leading_comments(text: str) -> list:
    out = []
    for line in text.splitlines():
        if line.startswith('#'):
            out.append(line)
        elif line.strip():
            break
    return out


def build_example(path: Path) -> dict:
    text = path.read_text(encoding='utf-8-sig')
    spec = R.parse_recipe(text)
    resolved = spec['_resolved']
    warm = re.search(r'预热\s*(\d+)\s*个交易日', text)
    factor = resolved['factor']
    rb = resolved['rebalance']
    cost = resolved['cost']
    port = resolved['portfolio']
    return {
        'comments': '\n'.join(leading_comments(text)),
        'name': spec['name'],
        'description': spec['description'],
        'start': resolved['period']['start'],
        'end': resolved['period']['end'],
        'warmup': int(warm.group(1)) if warm else 0,
        'universeMode': resolved['universe']['mode'],
        'codes': resolved['universe']['codes'],
        'exclusions': [{'name': x['name'], 'params': x['params']} for x in resolved['exclusions']],
        'factorOn': factor is not None,
        'factor': None if factor is None else {
            'name': factor['name'],
            'buckets': factor['quantile']['buckets'],
            'action': factor['quantile']['action'],
            'side': 'low' if factor['quantile']['bucket'] == 1 else 'high',
            'window': factor['params'].get('window'),
        },
        'freq': rb['frequency'],
        'firstDate': rb.get('first_date', ''),
        'cost': {
            'slippage': cost['slippage_bps'],
            'stamp': cost['stamp_tax_basis'],
            'commission': cost['commission_rate'] * 10000.0,
            'minCommission': cost['min_commission'],
            'impact': cost['impact_coef'],
        },
        'capital': port['initial_capital'] / 1e8,
        'lot': str(port['lot_size']),
        'buffer': port['cash_buffer'] * 100.0,
        'pooled': port['pooled_cash'],
    }


def build_default() -> dict:
    return {
        'comments': '',
        'name': '',
        'description': '',
        'start': '2010-01-04',
        'end': '2026-11-13',
        'warmup': 103,
        'universeMode': 'all_market',
        'codes': [],
        'exclusions': [],
        'factorOn': False,
        'factor': {'name': 'vol', 'buckets': 5, 'action': 'hold_bucket',
                   'side': 'low', 'window': 60},
        'freq': 'monthly',
        'firstDate': '2009-09-01',
        'cost': {'slippage': 10, 'stamp': 'fixed_0.1%', 'commission': 2.5,
                 'minCommission': 5, 'impact': 0.015},
        'capital': 10,
        'lot': '100',
        'buffer': 1,
        'pooled': True,
    }


def main() -> None:
    ap = argparse.ArgumentParser(description='回测配方生成页（单文件 HTML）')
    ap.add_argument('--example', default=str(DEFAULT_EXAMPLE), help='载入示例用的配方文件')
    ap.add_argument('--out', default='', help='输出 HTML 路径')
    args = ap.parse_args()

    example = build_example(Path(args.example))
    vocab = {
        'factors': FACTORS.export(),
        'conditions': CONDITIONS.export(),
        'condition_recipe': R.CONDITION_RECIPE,
        'param_labels': R.PARAM_LABELS,
        'percent_params': list(R.PERCENT_PARAMS),
        'unit_fraction_conditions': list(R.UNIT_FRACTION_CONDITIONS),
        'unit_fraction_range': list(R.UNIT_FRACTION_RANGE),
        'factor_labels': R.FACTOR_LABELS,
        'frequency_labels': {'monthly': '月频', 'quarterly': '季频', 'semi': '半年频'},
        'frequencies': [['monthly', '月频'], ['quarterly', '季频'], ['semi', '半年频']],
        'stamps': [['fixed_0.1%', '固定千1'], ['historical', '真值（历史）']],
        'lots': [['100', '按手（100 股）'], ['1', '按股']],
        'buckets': [2, 20],
    }
    vocab_bytes = json.dumps(vocab, ensure_ascii=False, sort_keys=True, separators=(',', ':')).encode('utf-8')
    import hashlib
    vocab_sha = hashlib.sha256(vocab_bytes).hexdigest()[:8]

    html_text = PAGE
    html_text = html_text.replace('{{TITLE}}', '回测配方生成器')
    html_text = html_text.replace('{{BUILD_DATE}}', date.today().isoformat())
    html_text = html_text.replace('{{VOCAB_SHA}}', vocab_sha)
    html_text = html_text.replace('{{STYLE}}', CSS_TEXT + FORM_CSS)
    html_text = html_text.replace('{{VOCAB}}', vocab_bytes.decode('utf-8'))
    html_text = html_text.replace('{{EXAMPLE}}', json.dumps(example, ensure_ascii=False, separators=(',', ':')))
    html_text = html_text.replace('{{DEFAULT}}', json.dumps(build_default(), ensure_ascii=False, separators=(',', ':')))
    html_text = html_text.replace('{{JS}}', JS)

    out = Path(args.out) if args.out else ROOT / 'docs' / f'配方生成_{date.today().isoformat()}.html'
    if not out.is_absolute():
        out = ROOT / out
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(html_text, encoding='utf-8', newline='\n')
    print(f'已生成 {out}（{len(html_text.encode("utf-8")):,} 字节，词表 {vocab_sha}，'
          f'示例 {Path(args.example).name}）')


if __name__ == '__main__':
    main()
