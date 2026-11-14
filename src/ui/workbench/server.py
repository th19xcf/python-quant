#!/usr/bin/env python3
"""回测工作台的内核：单页 Shell + 队列 + 页面生成 + 本地回环服务。

形态
----
- 桌面通道：``src.main`` 主窗口「量化分析 → 回测工作台」标签页内嵌本服务页面。
- 浏览器通道：``python -m src.main --workbench`` 只起本服务并提示地址。

两条通道共用本模块的视图渲染、队列与输出目录，没有第二份实现：
新建回测直接嵌 ``docs/配方生成_*.html``，看板与清单由既有
``tools/backtest_result_dashboard.py`` / ``tools/backtest_run_index_page.py`` 子进程生成。

安全边界
--------
- 只监听 127.0.0.1（端口由系统分配，对用户隐藏），不连外网。
- 同一时刻只跑一个真实回测，其余排队；停止只中断子进程并保留 partial 文件。
- 不改引擎、不改 ``data/local_store`` 行情与元数据、不改成本默认值。
"""
from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import traceback
import webbrowser
from collections import deque
from datetime import date, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlparse

ROOT = Path(__file__).resolve().parents[3]
for _p in (ROOT, ROOT / 'tools'):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

from src.data.local_store.paths import backtest_runs_dir  # 运行记录根的唯一来源

RUNS_DIR = backtest_runs_dir()
INDEX_PATH = RUNS_DIR / 'index.json'
RUNNER = ROOT / 'tools' / 'run_backtest_spec.py'
DASH_TOOL = ROOT / 'tools' / 'backtest_result_dashboard.py'
LIST_TOOL = ROOT / 'tools' / 'backtest_run_index_page.py'
PY = sys.executable
TMP = Path(tempfile.mkdtemp(prefix='quant_app_'))
LOG_TAIL = 12
CREATE_NO_WINDOW = 0x08000000 if os.name == 'nt' else 0

try:
    from backtest_result_dashboard import CSS_TEXT
except Exception:  # pragma: no cover - 极端情况下退回最小样式
    CSS_TEXT = ':root{--bg:#f5f7fa;--panel:#fff;--ink:#15202f;--muted:#657184}'

APP_CSS = r"""
  html,body{height:100%;overflow:hidden}
  .app{display:flex;height:100vh}
  aside.side{
    width:168px;flex:0 0 168px;background:var(--panel);border-right:1px solid var(--line);
    display:flex;flex-direction:column;padding:12px 10px;gap:4px;
  }
  .brand{font-size:13.5px;font-weight:700;padding:4px 8px 10px;letter-spacing:.01em}
  aside.side button{
    font:inherit;font-size:13px;text-align:left;border:0;background:transparent;color:var(--muted);
    padding:8px 10px;border-radius:8px;cursor:pointer;
  }
  aside.side button:hover{background:var(--line2);color:var(--ink)}
  aside.side button.on{background:var(--line2);color:var(--ink);font-weight:650}
  .side-foot{margin-top:auto;font-size:10.5px;color:var(--faint);padding:6px 8px;line-height:1.5}
  main.appmain{flex:1;min-width:0;display:flex;flex-direction:column;background:var(--bg)}
  .view{display:none;flex:1;min-height:0;flex-direction:column;padding:10px 12px 12px}
  .view.on{display:flex}
  iframe{flex:1;width:100%;border:1px solid var(--line);border-radius:12px;background:var(--panel)}
  .bar{
    display:flex;align-items:center;gap:10px;flex-wrap:wrap;padding:8px 4px 0;
    font-size:12.5px;color:var(--muted);
  }
  .bar .spacer{flex:1}
  .btn{
    font:inherit;font-size:12.5px;padding:6px 12px;border-radius:8px;cursor:pointer;
    border:1px solid var(--line);background:var(--panel);color:var(--ink);
  }
  .btn.primary{background:var(--v1);border-color:var(--v1);color:#fff;font-weight:600}
  .btn.danger{background:var(--panel);border-color:var(--neg);color:var(--neg)}
  .btn:disabled{opacity:.45;cursor:not-allowed}
  .qlist{flex:1;overflow:auto;padding-right:2px}
  .qgroup{margin:0 0 12px}
  .qgroup h3{font-size:12px;color:var(--muted);margin:8px 2px 6px;font-weight:650}
  .job{
    background:var(--panel);border:1px solid var(--line);border-radius:11px;
    padding:9px 12px;margin-bottom:8px;box-shadow:var(--shadow);
  }
  .job-top{display:flex;align-items:center;gap:10px;flex-wrap:wrap}
  .job-name{font-weight:650;font-size:13px}
  .job-meta{font-family:var(--mono);font-size:10.5px;color:var(--faint)}
  .pill{
    display:inline-flex;align-items:center;gap:5px;font-size:11.5px;font-weight:600;
    border:1px solid var(--line);border-radius:999px;padding:1px 9px;
  }
  .pill::before{content:"";width:6px;height:6px;border-radius:50%;background:var(--faint)}
  .pill.run{color:var(--v2)} .pill.run::before{background:var(--v2)}
  .pill.ok{color:var(--pos)} .pill.ok::before{background:var(--pos)}
  .pill.fail{color:var(--neg)} .pill.fail::before{background:var(--neg)}
  .pill.stop,.pill.wait{color:var(--muted)}
  .job-nums{display:flex;gap:14px;flex-wrap:wrap;font-size:11.8px;color:var(--muted);margin-top:5px}
  .job-nums b{font-family:var(--mono);color:var(--ink);font-weight:600}
  .bar-track{height:6px;border-radius:999px;background:var(--line);overflow:hidden;margin-top:7px}
  .bar-track i{display:block;height:100%;background:var(--v2)}
  .job details{margin-top:6px}
  .job summary{cursor:pointer;font-size:11.5px;color:var(--muted)}
  .job pre{
    margin:6px 0 0;padding:8px 10px;background:var(--bg);border:1px solid var(--line2);
    border-radius:8px;font-family:var(--mono);font-size:11px;line-height:1.6;color:var(--muted);
    white-space:pre-wrap;max-height:180px;overflow:auto;
  }
  .empty-line{color:var(--faint);font-size:12.5px;padding:18px;text-align:center}
  .view .toolbar{display:flex;gap:8px;align-items:center;flex-wrap:wrap;margin-bottom:8px;font-size:12.5px;color:var(--muted)}
  .view select{font:inherit;font-size:12.5px;padding:5px 8px;border:1px solid var(--line);border-radius:8px;background:var(--panel);color:var(--ink)}
  .toast{font-size:12px;color:var(--muted)}
  @media (max-width:760px){
    .app{flex-direction:column}
    aside.side{width:auto;flex:0 0 auto;flex-direction:row;align-items:center;gap:2px;padding:6px 8px;border-right:0;border-bottom:1px solid var(--line);overflow-x:auto}
    .brand{display:none}
    aside.side button{white-space:nowrap;padding:6px 9px;font-size:12.5px}
    .side-foot{display:none}
    main.appmain{min-height:0}
    .view{padding:8px}
    .bar{padding-top:6px}
  }
"""

SHELL = r"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>量化回测工作台</title>
<style>{{STYLE}}</style>
</head>
<body>
<div class="app">
  <aside class="side">
    <div class="brand">量化回测工作台</div>
    <button type="button" data-view="new" class="on">新建回测</button>
    <button type="button" data-view="queue">运行队列</button>
    <button type="button" data-view="dash">结果看板</button>
    <button type="button" data-view="list">运行清单</button>
    <div class="side-foot">仅本机 127.0.0.1<br>同一时刻只跑一个回测</div>
  </aside>
  <main class="appmain">
    <section class="view on" id="view-new">
      <iframe id="recipeFrame" src="/recipe"></iframe>
      <div class="bar">
        <span id="newMsg" class="toast">在表单里设定后点「开始运行」；配方未通过校验时不会入队。</span>
        <span class="spacer"></span>
        <button type="button" class="btn primary" id="startBtn">开始运行</button>
      </div>
    </section>
    <section class="view" id="view-queue">
      <div class="bar">
        <span id="queueMsg" class="toast">队列每秒刷新；进行中读取 progress.json。</span>
        <span class="spacer"></span>
        <button type="button" class="btn danger" id="stopBtn" disabled>停止当前回测</button>
      </div>
      <div class="qlist" id="qlist"><div class="empty-line">加载中…</div></div>
    </section>
    <section class="view" id="view-dash">
      <div class="toolbar">
        <span>选择运行：</span>
        <select id="runSelect"><option value="">— 暂无已完成运行 —</option></select>
        <span class="spacer"></span>
        <span id="dashMsg" class="toast"></span>
      </div>
      <iframe id="dashFrame"></iframe>
    </section>
    <section class="view" id="view-list">
      <iframe id="listFrame" src="/list"></iframe>
    </section>
  </main>
</div>
<script>
(function () {
  'use strict';
  var current = 'new';
  var dashLoaded = '';
  var alive = {};

  function $(id) { return document.getElementById(id); }
  function esc(t) {
    return String(t == null ? '' : t).replace(/[&<>"]/g, function (c) {
      return {'&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;'}[c];
    });
  }
  function switchView(v) {
    current = v;
    Array.prototype.forEach.call(document.querySelectorAll('aside.side button'), function (b) {
      b.classList.toggle('on', b.dataset.view === v);
    });
    Array.prototype.forEach.call(document.querySelectorAll('.view'), function (s) {
      s.classList.toggle('on', s.id === 'view-' + v);
    });
    if (v === 'dash') { refreshRuns(); }
  }
  Array.prototype.forEach.call(document.querySelectorAll('aside.side button'), function (b) {
    b.addEventListener('click', function () { switchView(b.dataset.view); });
  });

  // 内嵌页面去掉重复的标题栏，保持窗口内一体
  function clean(frame) {
    try {
      var d = frame.contentDocument;
      if (!d || d.getElementById('__embedStyle')) { return; }
      var s = d.createElement('style');
      s.id = '__embedStyle';
      s.textContent = '.topbar,.navbar,footer.page-foot{display:none!important} main{padding-top:8px!important}';
      (d.head || d.documentElement).appendChild(s);
      // 深色令牌与 Shell 保持一致：内嵌页只换 data-theme，不重写渲染
      var theme = document.documentElement.dataset.theme;
      if (theme) { d.documentElement.dataset.theme = theme; }
      // 清单页的行内看板链接交给 Shell 打开，避免只把 iframe 自身导航走
      if (frame.id === 'listFrame') {
        d.addEventListener('click', function (evt) {
          var a = evt.target.closest && evt.target.closest('a.rlink');
          if (!a) { return; }
          var m = (a.getAttribute('href') || '').match(/^\/run\/([^/]+)\/dashboard/);
          if (m) { evt.preventDefault(); window.__openDash(decodeURIComponent(m[1])); }
        }, true);
      }
    } catch (e) { /* 同源失败则保留原样 */ }
  }
  ['recipeFrame', 'dashFrame', 'listFrame'].forEach(function (id) {
    $(id).addEventListener('load', function () { clean($(id)); });
  });

  function post(url, body) {
    return fetch(url, {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(body || {})
    }).then(function (r) { return r.json(); });
  }

  $('startBtn').addEventListener('click', function () {
    var frame = $('recipeFrame');
    var recipe = '';
    try { recipe = frame.contentWindow.__recipe || ''; } catch (e) { recipe = ''; }
    if (!recipe) {
      $('newMsg').textContent = '配方未生成：请先修正表单里的提示，再开始运行。';
      return;
    }
    post('/api/run', {recipe: recipe}).then(function (d) {
      if (d.ok) {
        // 先记账再轮询：极短的回测可能在两次轮询之间就跑完，不能只靠看到 running
        if (d.job) { alive[d.job] = true; }
        $('newMsg').textContent = '已加入队列：' + d.name + '（同一时刻只跑一个）';
        switchView('queue');
        refreshQueue();
      } else {
        $('newMsg').textContent = '未入队：' + (d.error || '未知原因');
      }
    }).catch(function (e) {
      $('newMsg').textContent = '请求失败：' + e;
    });
  });

  $('stopBtn').addEventListener('click', function () {
    post('/api/stop', {}).then(function (d) {
      $('queueMsg').textContent = d.ok ? d.message : ('停止失败：' + (d.error || ''));
      refreshQueue();
    });
  });

  function statusPill(status) {
    var m = {queued: ['wait', '排队中'], running: ['run', '进行中'], done: ['ok', '完成'],
             failed: ['fail', '失败'], stopped: ['stop', '已停止']};
    var it = m[status] || ['wait', status || '未知'];
    return '<span class="pill ' + it[0] + '">' + it[1] + '</span>';
  }
  function fmtPct(v) { return (v == null || !isFinite(v)) ? '未落盘' : (v * 100).toFixed(2) + '%'; }

  function jobHtml(j) {
    var nums = '';
    if (j.metrics && j.status === 'done') {
      nums = '<div class="job-nums">' +
        '<span>年化 <b>' + fmtPct(j.metrics.annual) + '</b></span>' +
        '<span>最大回撤 <b>' + fmtPct(j.metrics.max_drawdown) + '</b></span>' +
        '<span>年换手 <b>' + (j.metrics.turnover_two_way == null ? '未落盘' : Number(j.metrics.turnover_two_way).toFixed(2) + 'x') + '</b></span>' +
        '</div>';
    }
    if (j.status === 'running') {
      var prov = j.provisional || {};
      nums += '<div class="bar-track"><i style="width:' + (j.percent || 0) + '%"></i></div>' +
        '<div class="job-nums">' +
        '<span>进度 <b>' + (j.percent == null ? '—' : Number(j.percent).toFixed(1) + '%') + '</b></span>' +
        '<span>已算区间年化 <b>' + fmtPct(prov.annual) + '</b> <em style="font-style:normal;color:var(--v2)">provisional</em></span>' +
        '<span>回撤 <b>' + fmtPct(prov.max_drawdown) + '</b></span>' +
        '</div>';
    }
    if (j.status === 'queued') { nums = '<div class="job-nums"><span>等待前一个回测结束</span></div>'; }
    var log = '';
    if ((j.status === 'failed' || j.status === 'stopped') && j.log_tail && j.log_tail.length) {
      log = '<details><summary>run.log 末尾 ' + j.log_tail.length + ' 行</summary><pre>' +
        esc(j.log_tail.join('\n')) + '</pre></details>';
    }
    var actions = '';
    if (j.run_id) {
      actions = '<span class="spacer"></span><button type="button" class="btn" data-dash="' +
        esc(j.run_id) + '">看板</button>';
    }
    return '<article class="job"><div class="job-top">' +
      '<span class="job-name">' + esc(j.name) + '</span>' + statusPill(j.status) +
      '<span class="job-meta">' + esc(j.created_at || '') + '</span>' + actions + '</div>' +
      (nums || '') + log + '</article>';
  }

  function renderQueue(jobs) {
    var groups = {run: [], done: [], fail: []};
    jobs.forEach(function (j) {
      if (j.status === 'running' || j.status === 'queued') { groups.run.push(j); }
      else if (j.status === 'done') { groups.done.push(j); }
      else { groups.fail.push(j); }
    });
    var html = '';
    [['run', '进行中'], ['done', '已完成'], ['fail', '失败 / 已停止']].forEach(function (g) {
      html += '<div class="qgroup"><h3>' + g[1] + ' · ' + groups[g[0]].length + '</h3>' +
        (groups[g[0]].length ? groups[g[0]].map(jobHtml).join('') :
          '<div class="empty-line">暂无</div>') + '</div>';
    });
    $('qlist').innerHTML = html;
    Array.prototype.forEach.call(document.querySelectorAll('[data-dash]'), function (b) {
      b.addEventListener('click', function () { openDash(b.dataset.dash); });
    });
    var running = groups.run.some(function (j) { return j.status === 'running'; });
    $('stopBtn').disabled = !running;
  }

  function refreshQueue() {
    fetch('/api/queue').then(function (r) { return r.json(); }).then(function (d) {
      var jobs = d.jobs || [];
      renderQueue(jobs);
      jobs.forEach(function (j) {
        if (j.status === 'running' || j.status === 'queued') { alive[j.id] = true; }
        if (j.status === 'done' && alive[j.id]) {
          delete alive[j.id];
          if (current === 'queue') {
            openDash(j.run_id);
            $('queueMsg').textContent = '回测完成，已自动打开看板：' + j.name;
          }
        }
      });
    }).catch(function () {});
  }

  function openDash(runId) {
    if (!runId) { return; }
    dashLoaded = runId;
    $('dashFrame').src = '/run/' + encodeURIComponent(runId) + '/dashboard';
    $('dashMsg').textContent = '查看：' + runId;
    switchView('dash');
    var sel = $('runSelect');
    for (var i = 0; i < sel.options.length; i++) {
      if (sel.options[i].value === runId) { sel.value = runId; break; }
    }
  }
  window.__openDash = openDash;

  function refreshRuns() {
    fetch('/api/runs').then(function (r) { return r.json(); }).then(function (d) {
      var sel = $('runSelect');
      var keep = dashLoaded || sel.value;
      var opts = ['<option value="">— 选择运行 —</option>'];
      (d.runs || []).forEach(function (r) {
        opts.push('<option value="' + esc(r.run_id) + '">' + esc(r.name) + ' · ' +
          esc(r.created_at || '') + '</option>');
      });
      sel.innerHTML = opts.join('');
      if (keep) { sel.value = keep; }
      if (keep && !dashLoaded) { $('dashFrame').src = '/run/' + encodeURIComponent(keep) + '/dashboard'; }
    }).catch(function () {});
  }

  $('runSelect').addEventListener('change', function () {
    var v = $('runSelect').value;
    if (v) { openDash(v); }
  });
  document.getElementById('view-dash').addEventListener('mouseenter', refreshRuns);

  refreshQueue();
  setInterval(refreshQueue, 1000);
  window.__switchView = switchView;
})();
</script>
</body>
</html>
"""


# ---------------------------------------------------------------- 服务端工具
def say(msg: str) -> None:
    """pythonw（双击 .pyw）下 sys.stdout 为 None，打印需静默降级。"""
    if sys.stdout is None:
        return
    try:
        print(msg, flush=True)
    except Exception:
        pass


def read_json(path: Path):
    try:
        return json.loads(path.read_text(encoding='utf-8'))
    except Exception:
        return None


def rel(path: Path) -> str:
    try:
        return path.relative_to(ROOT).as_posix()
    except Exception:
        return str(path)


def tail_lines(path: Path, n: int = LOG_TAIL) -> list:
    try:
        lines = path.read_text(encoding='utf-8', errors='replace').splitlines()
    except Exception:
        return []
    out = []
    for line in lines[-n:]:
        out.append(line[:260])
    return out


def fmt_dt(value) -> str:
    if not value:
        return ''
    return str(value).replace('T', ' ')[:19]


class AppState:
    """运行队列与当前子进程。所有访问经 self.lock。"""

    def __init__(self):
        self.lock = threading.Lock()
        self.jobs = []
        self.queue = deque()
        self.current = None
        self.counter = 0
        self.worker = threading.Thread(target=self._loop, daemon=True)
        self.worker.start()

    def enqueue(self, recipe: str, name: str) -> dict:
        with self.lock:
            self.counter += 1
            job = {
                'id': f'job-{self.counter}',
                'name': name,
                'recipe': recipe,
                'status': 'queued',
                'created_at': datetime.now().isoformat(timespec='seconds'),
                'run_id': '',
                'percent': 0.0,
                'provisional': None,
                'metrics': None,
                'log': deque(maxlen=300),
                'message': '',
                'proc': None,
                'stop_requested': False,
            }
            self.jobs.append(job)
            self.queue.append(job)
        return job

    # ---- 工作线程 ----
    def _loop(self):
        while True:
            with self.lock:
                job = self.queue.popleft() if self.queue else None
                if job:
                    self.current = job
            if not job:
                time.sleep(0.3)
                continue
            try:
                self._run_job(job)
            finally:
                with self.lock:
                    self.current = None

    def _run_job(self, job):
        job['status'] = 'running'
        before = {d.name for d in RUNS_DIR.iterdir() if d.is_dir()} if RUNS_DIR.exists() else set()
        recipe_path = TMP / f"{job['id']}.recipe.txt"
        recipe_path.write_text(job['recipe'], encoding='utf-8', newline='\n')
        args = [PY, str(RUNNER), '--recipe', str(recipe_path), '--apply']
        try:
            env = dict(os.environ, PYTHONUNBUFFERED='1')
            proc = subprocess.Popen(
                args, cwd=str(ROOT), stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True, encoding='utf-8', errors='replace', bufsize=1,
                env=env, creationflags=CREATE_NO_WINDOW)
        except Exception as exc:  # noqa: BLE001
            job['status'] = 'failed'
            job['message'] = f'启动失败：{exc}'
            job['log'].append(job['message'])
            return
        job['proc'] = proc

        def reader():
            for line in proc.stdout:
                job['log'].append(line.rstrip())
        threading.Thread(target=reader, daemon=True).start()

        run_dir = None
        while proc.poll() is None:
            if run_dir is None:
                run_dir = self._detect_run_dir(before)
            if run_dir:
                job['run_id'] = run_dir.name
                progress = read_json(run_dir / 'progress.json') or {}
                if progress.get('status') == 'running':
                    job['percent'] = progress.get('percent')
                    job['provisional'] = progress.get('metrics_window')
            time.sleep(0.4)

        rc = proc.wait()
        if run_dir is None:
            run_dir = self._detect_run_dir(before)
        if run_dir:
            job['run_id'] = run_dir.name
        if job.get('stop_requested'):
            job['status'] = 'stopped'
            job['message'] = '已停止：partial 净值保留在 run 目录，未删除任何正式数据'
            if run_dir:
                self._mark_stopped(run_dir)
        elif rc == 0:
            job['status'] = 'done'
            job['percent'] = 100.0
            if run_dir:
                metrics = read_json(run_dir / 'metrics.json') or {}
                job['metrics'] = metrics.get('metrics')
        else:
            job['status'] = 'failed'
            job['message'] = f'退出码 {rc}'

    def _detect_run_dir(self, before):
        try:
            now = {d.name: d for d in RUNS_DIR.iterdir() if d.is_dir()}
        except Exception:
            return None
        new = [d for k, d in now.items() if k not in before]
        if not new:
            return None
        return max(new, key=lambda p: p.stat().st_mtime)

    def _mark_stopped(self, run_dir: Path):
        progress_path = run_dir / 'progress.json'
        payload = read_json(progress_path)
        if not isinstance(payload, dict) or payload.get('status') != 'running':
            return
        payload['status'] = 'stopped'
        payload['updated_at'] = datetime.now().isoformat(timespec='seconds')
        payload['error'] = '用户停止：未完成，partial 净值保留在 run 目录'
        payload['nav_partial_policy'] = 'kept_on_stop'
        tmp = progress_path.with_name(progress_path.name + '.tmp')
        tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + '\n',
                       encoding='utf-8', newline='\n')
        os.replace(tmp, progress_path)

    def stop_all(self) -> str:
        with self.lock:
            pending = list(self.queue)
            self.queue.clear()
            for job in pending:
                job['status'] = 'stopped'
                job['message'] = '排队中被取消，未启动'
            job = self.current
            if not job or job.get('proc') is None:
                return '当前没有进行中的回测'
            job['stop_requested'] = True
            proc = job['proc']
        try:
            proc.terminate()
            proc.wait(timeout=6)
        except Exception:
            try:
                proc.kill()
            except Exception:
                pass
        return '已发送停止；partial 文件由 runner 保留，正式数据未动'

    def snapshot(self) -> dict:
        with self.lock:
            jobs = [self._public(j) for j in self.jobs]
        history = self._history()
        seen = {j['run_id'] for j in jobs if j.get('run_id')}
        for item in history:
            if item['run_id'] not in seen:
                jobs.append(item)
        jobs.sort(key=lambda j: str(j.get('created_at') or ''), reverse=True)
        return {'jobs': jobs}

    @staticmethod
    def _public(job) -> dict:
        return {
            'id': job['id'], 'name': job['name'], 'status': job['status'],
            'created_at': fmt_dt(job['created_at']), 'run_id': job['run_id'],
            'percent': job['percent'], 'provisional': job['provisional'],
            'metrics': job['metrics'], 'message': job['message'],
            'log_tail': list(job['log'])[-LOG_TAIL:],
        }

    def _history(self) -> list:
        index = read_json(INDEX_PATH) or {}
        out = []
        for meta in index.get('runs') or []:
            run_dir = RUNS_DIR / str(meta.get('relative_dir') or Path(str(meta.get('dir') or '')).name)
            progress = read_json(run_dir / 'progress.json') or {}
            status = str(progress.get('status') or meta.get('status') or '').lower()
            status = {'ok': 'done', 'finished': 'done', 'success': 'done'}.get(status, status)
            if status not in ('done', 'failed', 'stopped', 'running'):
                status = 'done' if meta.get('metrics') else 'failed'
            item = {
                'id': 'idx-' + str(meta.get('run_id') or ''),
                'name': str(meta.get('name') or meta.get('run_id') or '未记录'),
                'status': status,
                'created_at': fmt_dt(meta.get('created_at')),
                'run_id': str(meta.get('run_id') or ''),
                'percent': progress.get('percent', 100.0 if status == 'done' else 0),
                'provisional': progress.get('metrics_window'),
                'metrics': meta.get('metrics'),
                'message': '',
                'log_tail': tail_lines(run_dir / 'run.log') if status in ('failed', 'stopped') else [],
            }
            out.append(item)
        return out


STATE = AppState()


# ---------------------------------------------------------------- 页面生成
_list_cache = {'key': None, 'html': None}
_dash_cache = {}
_cache_lock = threading.Lock()


def recipe_page_html() -> bytes:
    candidates = sorted((ROOT / 'docs').glob('配方生成_*.html'))
    if not candidates:
        return '<h1>缺少配方生成页</h1><p>请先运行 tools/recipe_builder_page.py 生成。</p>'.encode('utf-8')
    return candidates[-1].read_bytes()


def list_page_html() -> bytes:
    key = INDEX_PATH.stat().st_mtime if INDEX_PATH.exists() else 0
    with _cache_lock:
        if _list_cache['key'] == key and _list_cache['html']:
            return _list_cache['html']
    out = TMP / 'list.html'
    args = [PY, str(LIST_TOOL), '--runs', str(INDEX_PATH), '--out', str(out),
            '--dashboard', '/', '--dashboard-pattern', '/run/{run_id}/dashboard']
    subprocess.run(args, cwd=str(ROOT), capture_output=True, timeout=180)
    html = out.read_bytes() if out.exists() else '<h1>清单生成失败</h1>'.encode('utf-8')
    with _cache_lock:
        _list_cache.update(key=key, html=html)
    return html


def dashboard_page_html(run_id: str) -> bytes:
    run_dir = RUNS_DIR / run_id
    if not run_dir.is_dir():
        return f'<h1>未找到运行</h1><p>{run_id}</p>'.encode('utf-8')
    files = ['metrics.json', 'daily_nav.csv', 'spec.json']
    key = tuple((run_dir / f).stat().st_mtime if (run_dir / f).exists() else 0 for f in files)
    with _cache_lock:
        cached = _dash_cache.get(run_id)
        if cached and cached[0] == key:
            return cached[1]
    metrics = (read_json(run_dir / 'metrics.json') or {}).get('metrics') or {}
    spec = read_json(run_dir / 'spec.json') or {}
    name = spec.get('name') or run_id
    payload = {
        'title': f'回测结果看板：{name}',
        'versions': [{'id': 'v1', 'name': name, 'role': '本次运行', 'metrics': metrics}],
    }
    tmp_json = TMP / f'{run_id}.json'
    tmp_html = TMP / f'{run_id}.html'
    tmp_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + '\n',
                        encoding='utf-8', newline='\n')
    args = [PY, str(DASH_TOOL), '--json', str(tmp_json),
            '--equity', f'v1={run_dir / "daily_nav.csv"}',
            '--title', f'回测结果看板：{name}', '--out', str(tmp_html)]
    subprocess.run(args, cwd=str(ROOT), capture_output=True, timeout=180)
    html = tmp_html.read_bytes() if tmp_html.exists() else '<h1>看板生成失败</h1>'.encode('utf-8')
    with _cache_lock:
        _dash_cache[run_id] = (key, html)
    return html


# ---------------------------------------------------------------- HTTP
class Handler(BaseHTTPRequestHandler):
    server_version = 'QuantDesktop/1.0'

    def log_message(self, fmt, *args):  # 静默：桌面程序不需要访问日志
        return

    def _send(self, data: bytes, ctype: str = 'text/html; charset=utf-8', code: int = 200):
        self.send_response(code)
        self.send_header('Content-Type', ctype)
        self.send_header('Content-Length', str(len(data)))
        self.send_header('Cache-Control', 'no-store')
        self.end_headers()
        try:
            self.wfile.write(data)
        except Exception:
            pass

    def _json(self, payload, code: int = 200):
        self._send(json.dumps(payload, ensure_ascii=False).encode('utf-8'),
                   'application/json; charset=utf-8', code)

    def do_GET(self):  # noqa: N802
        path = unquote(urlparse(self.path).path)
        try:
            if path == '/':
                query = parse_qs(urlparse(self.path).query)
                theme = (query.get('theme') or [''])[0]
                page = SHELL.replace('{{STYLE}}', CSS_TEXT + APP_CSS)
                if theme in ('dark', 'light'):
                    page = page.replace('<html lang="zh-CN">',
                                        f'<html lang="zh-CN" data-theme="{theme}">')
                self._send(page.encode('utf-8'))
            elif path == '/recipe':
                self._send(recipe_page_html())
            elif path == '/list':
                self._send(list_page_html())
            elif path.startswith('/run/') and path.endswith('/dashboard'):
                rid = path[len('/run/'):-len('/dashboard')]
                self._send(dashboard_page_html(rid))
            elif path == '/api/queue':
                self._json(STATE.snapshot())
            elif path == '/api/runs':
                runs = []
                for j in STATE.snapshot()['jobs']:
                    if j.get('run_id'):
                        runs.append({'run_id': j['run_id'], 'name': j['name'],
                                     'created_at': j['created_at']})
                self._json({'runs': runs})
            else:
                self._send(b'not found', 'text/plain; charset=utf-8', 404)
        except Exception:
            if sys.stderr is not None:
                traceback.print_exc()
            self._send(b'internal error', 'text/plain; charset=utf-8', 500)

    def do_POST(self):  # noqa: N802
        path = urlparse(self.path).path
        length = int(self.headers.get('Content-Length') or 0)
        body = self.rfile.read(length) if length else b'{}'
        try:
            payload = json.loads(body.decode('utf-8') or '{}')
        except Exception:
            payload = {}
        if path == '/api/run':
            recipe = str(payload.get('recipe') or '')
            try:
                from src.backtest.runner.recipe import parse_recipe
                spec = parse_recipe(recipe)
            except Exception as exc:  # noqa: BLE001
                self._json({'ok': False, 'error': f'配方解析失败：{exc}'})
                return
            job = STATE.enqueue(recipe, spec['name'])
            self._json({'ok': True, 'name': spec['name'], 'job': job['id']})
        elif path == '/api/stop':
            message = STATE.stop_all()
            self._json({'ok': True, 'message': message})
        else:
            self._json({'ok': False, 'error': 'unknown endpoint'}, 404)


def serve():
    httpd = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    return httpd


# ---------------------------------------------------------------- 对外入口
_server_lock = threading.Lock()
_httpd = None


def ensure_server() -> str:
    """启动（或复用）本地回环服务，返回 base URL。桌面标签页与浏览器通道共用同一实例。"""
    global _httpd
    with _server_lock:
        if _httpd is None:
            _httpd = serve()
        port = _httpd.server_address[1]
    return f'http://127.0.0.1:{port}/'


def shutdown() -> None:
    """退出前收尾：停当前回测、关服务、清临时目录；不动任何正式数据。"""
    global _httpd
    with _server_lock:
        httpd, _httpd = _httpd, None
    try:
        STATE.stop_all()
    except Exception:
        pass
    if httpd is not None:
        try:
            httpd.shutdown()
        except Exception:
            pass
    shutil.rmtree(TMP, ignore_errors=True)


def run_browser_channel(open_browser: bool = True) -> int:
    """浏览器通道：只绑 127.0.0.1，打印地址，Ctrl+C 结束。"""
    url = ensure_server()
    say(f'回测工作台（浏览器通道）已启动：{url}')
    say('仅监听 127.0.0.1；Ctrl+C 结束。桌面通道：python -m src.main，菜单「量化分析 → 回测工作台」')
    if open_browser:
        try:
            webbrowser.open(url)
        except Exception:
            pass
    try:
        while True:
            time.sleep(0.5)
    except KeyboardInterrupt:
        say('已停止')
    finally:
        shutdown()
    return 0
