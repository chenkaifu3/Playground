#!/usr/bin/env python3
import datetime as dt
import html
import json
import os
import shlex
import subprocess
import sys
import urllib.parse
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


BASE_DIR = Path("/Users/openclaw/Documents/Playground/tender-watch")
OUTPUT_DIR = BASE_DIR / "output"
LOGS_DIR = BASE_DIR / "logs"
PORT = 8765

PROFILES = [
    {
        "id": "hunan_operating_bot_concession_expressway_maintenance_design",
        "name": "Profile 1",
        "desc": "湖南省内已确认经营性/BOT/特许经营高速项目中的养护设计类信息",
    },
    {
        "id": "non_hunan_expressway_maintenance_design",
        "name": "Profile 2",
        "desc": "省外高速公路养护设计类招标信息",
    },
    {
        "id": "non_hunan_local_road_maintenance_design",
        "name": "Profile 3",
        "desc": "省外地方道路养护设计类招标信息",
    },
    {
        "id": "hunan_local_road_maintenance_construction",
        "name": "Profile 4",
        "desc": "省内地方道路/普通公路/国省道养护类信息",
    },
]

MODES = [
    {"id": "incremental", "label": "增量采集"},
    {"id": "snapshot", "label": "全量快照"},
]

SOURCE_LIST_FILE = OUTPUT_DIR / "profile_source_lists_20260311.md"


def profile_label(profile_id: str) -> str:
    for profile in PROFILES:
        if profile["id"] == profile_id:
            return profile["name"]
    return profile_id


def profile_desc(profile_id: str) -> str:
    for profile in PROFILES:
        if profile["id"] == profile_id:
            return profile["desc"]
    return ""


def load_json(path: Path):
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def summary_path(profile_id: str, mode: str) -> Path:
    suffix = "incremental" if mode == "incremental" else "full"
    return LOGS_DIR / f"summary_{suffix}_{profile_id}.json"


def incremental_run_file(profile_id: str) -> Path:
    return BASE_DIR / "data" / f"last_incremental_run_{profile_id}.txt"


def load_incremental_cutoff(profile_id: str) -> str:
    path = incremental_run_file(profile_id)
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8").strip()


def latest_state(profile_id: str, mode: str):
    summary = load_json(summary_path(profile_id, mode))
    if not summary:
        return {
            "summary_file": "",
            "output_file": "",
            "issues_file": "",
            "count": None,
            "cutoff": "",
            "wall_time_sec": None,
            "issues_count": None,
            "issue_summary": "",
        }
    issue_summary = ""
    issues_file = summary.get("issues_file", "")
    effective_issues_count = summary.get("issues_count")
    if issues_file:
        issues = load_json(Path(issues_file))
        if isinstance(issues, list) and issues:
            effective_issues_count = len(issues)
            if issues:
                first = issues[0]
                source = first.get("source", "未知来源")
                problem = first.get("problem", "未知问题")
                action = first.get("action", "无动作")
                issue_summary = f"{source}: {problem} -> {action}"
    return {
        "summary_file": str(summary_path(profile_id, mode)),
        "output_file": summary.get("output_file", ""),
        "issues_file": issues_file,
        "count": summary.get("count"),
        "cutoff": load_incremental_cutoff(profile_id) if mode == "incremental" else summary.get("cutoff", ""),
        "wall_time_sec": summary.get("wall_time_sec"),
        "issues_count": effective_issues_count,
        "issue_summary": issue_summary,
    }


def all_states():
    return {
        profile["id"]: {
            "profile": profile,
            "incremental": latest_state(profile["id"], "incremental"),
            "snapshot": latest_state(profile["id"], "snapshot"),
        }
        for profile in PROFILES
    }


def latest_output_by_profile(states: dict) -> dict:
    result = {}
    for profile in PROFILES:
        pid = profile["id"]
        candidates = []
        for mode in ("incremental", "snapshot"):
            path = states[pid][mode].get("output_file", "")
            if not path:
                continue
            p = Path(path)
            if p.exists():
                candidates.append((p.stat().st_mtime, path))
        result[pid] = max(candidates, default=(0, ""))[1]
    return result


def classify_output_file(path: Path) -> str:
    name = path.name
    for profile in PROFILES:
        if profile["id"] in name:
            return profile["id"]
    return "legacy"


def collect_history_hits():
    groups = {profile["id"]: [] for profile in PROFILES}
    groups["legacy"] = []
    seen = set()
    for path in sorted(OUTPUT_DIR.glob("hits*.json")):
        if path.name == "hits_full_fast_summary.json":
            continue
        data = load_json(path)
        if not isinstance(data, list) or not data:
            continue
        bucket = classify_output_file(path)
        for item in data:
            title = str(item.get("title", "")).strip()
            url = str(item.get("url", "")).strip()
            item_id = str(item.get("id", "")).strip()
            published_at = str(item.get("published_at", "")).strip()
            if not title and not url:
                continue
            key = (bucket, item_id or "", title, url)
            if key in seen:
                continue
            seen.add(key)
            groups[bucket].append(
                {
                    "title": title,
                    "url": url,
                    "source": str(item.get("source", "")).strip(),
                    "published_at": published_at,
                    "file": str(path),
                }
            )
    for bucket, items in groups.items():
        items.sort(key=lambda x: (x.get("published_at", ""), x.get("title", "")), reverse=True)
    return groups


def render_card(profile: dict, state: dict) -> str:
    def status_text(current: dict) -> str:
        issues = current.get("issues_count")
        if isinstance(issues, (int, float)) and issues > 0:
            return "有问题"
        return "正常"

    def status_class(current: dict) -> str:
        issues = current.get("issues_count")
        if isinstance(issues, (int, float)) and issues > 0:
            return "state-bad"
        return "state-idle"

    def fmt(value, suffix=""):
        if value in (None, ""):
            return "-"
        return f"{html.escape(str(value))}{suffix}"

    def js_str(value: str) -> str:
        return json.dumps(value, ensure_ascii=False)

    def disabled_attr(path: str) -> str:
        return ' disabled aria-disabled="true"' if not path else ""

    mode_sections = []
    for mode in MODES:
        current = state[mode["id"]]
        pid = profile["id"]
        mid = mode["id"]
        mode_sections.append(
            f"""
            <div class="mode" data-profile="{html.escape(pid)}" data-mode="{html.escape(mid)}">
              <div class="mode-title">
                <strong>{html.escape(mode["label"])}</strong>
                <div class="mode-tags">
                  <span class="mode-state {status_class(current)}" id="state-{html.escape(pid)}-{html.escape(mid)}">{status_text(current)}</span>
                  <span class="pill">{html.escape(profile["id"])}</span>
                </div>
              </div>
              <div class="meta">
                <div><span>命中数</span><b id="count-{html.escape(pid)}-{html.escape(mid)}">{fmt(current.get("count"))}</b></div>
                <div><span>时间窗</span><b id="cutoff-{html.escape(pid)}-{html.escape(mid)}">{fmt(current.get("cutoff"))}</b></div>
                <div><span>总耗时</span><b id="wall-{html.escape(pid)}-{html.escape(mid)}">{fmt(current.get("wall_time_sec"), " s")}</b></div>
                <div><span>问题数</span><b id="issues-{html.escape(pid)}-{html.escape(mid)}">{fmt(current.get("issues_count"))}</b></div>
              </div>
              <div class="issue-note{' show' if current.get('issue_summary') else ''}" id="issue-note-{html.escape(pid)}-{html.escape(mid)}">{html.escape(current.get("issue_summary", ""))}</div>
              <div class="actions">
                <button onclick='runCmd({js_str(mode["id"])}, {js_str(profile["id"])})'>弹出终端运行</button>
                <button class="secondary" id="output-btn-{html.escape(pid)}-{html.escape(mid)}" onclick='viewFile({js_str(current.get("output_file", ""))})'{disabled_attr(current.get("output_file", ""))}>查看最新结果</button>
                <button class="alt" id="open-btn-{html.escape(pid)}-{html.escape(mid)}" onclick='openPath({js_str(current.get("output_file", ""))})'{disabled_attr(current.get("output_file", ""))}>打开结果文件</button>
                <button class="secondary" id="issues-btn-{html.escape(pid)}-{html.escape(mid)}" onclick='viewFile({js_str(current.get("issues_file", ""))})'{disabled_attr(current.get("issues_file", ""))}>查看问题日志</button>
              </div>
            </div>
            """
        )

    return f"""
      <article class="card">
        <div class="profile-head">
          <h2 class="profile-title">{html.escape(profile["name"])} <span class="profile-desc-inline">{html.escape(profile["desc"])}</span></h2>
        </div>
        {''.join(mode_sections)}
      </article>
    """


def dashboard_html():
    states = all_states()
    cards_html = "".join(render_card(profile, states[profile["id"]]) for profile in PROFILES)
    generated_at = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Tender Watch 控制台</title>
  <style>
    :root {{
      --bg: #eef3e7;
      --panel: #f8fbf3;
      --ink: #1d2a1f;
      --muted: #617066;
      --line: #cfd8c8;
      --accent: #235c3d;
      --accent-2: #b7cf73;
      --warn: #8a3b12;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: "PingFang SC", "Hiragino Sans GB", "Noto Sans CJK SC", sans-serif;
      color: var(--ink);
      background:
        radial-gradient(circle at top left, rgba(183,207,115,.35), transparent 30%),
        radial-gradient(circle at bottom right, rgba(35,92,61,.12), transparent 35%),
        var(--bg);
    }}
    .wrap {{
      max-width: 1380px;
      margin: 0 auto;
      padding: 28px 24px 36px;
    }}
    .hero {{ margin-bottom: 24px; }}
    .hero-card, .card {{
      background: linear-gradient(180deg, rgba(255,255,255,.72), rgba(248,251,243,.96));
      border: 1px solid var(--line);
      border-radius: 20px;
      padding: 18px 18px 16px;
      box-shadow: 0 12px 30px rgba(34, 56, 36, 0.07);
      backdrop-filter: blur(10px);
    }}
    h1 {{
      margin: 0 0 8px;
      font-size: 30px;
      line-height: 1.1;
    }}
    .sub {{
      color: var(--muted);
      line-height: 1.6;
      font-size: 14px;
    }}
    .toolbar {{
      display: flex;
      gap: 10px;
      margin-top: 14px;
      flex-wrap: nowrap;
      overflow-x: auto;
      padding-bottom: 2px;
      align-items: center;
    }}
    .toolbar::-webkit-scrollbar {{
      height: 8px;
    }}
    .toolbar::-webkit-scrollbar-thumb {{
      background: rgba(35,92,61,.2);
      border-radius: 999px;
    }}
    .grid {{
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 18px;
    }}
    .profile-head {{
      display: flex;
      justify-content: space-between;
      align-items: start;
      gap: 14px;
      margin-bottom: 12px;
    }}
    .profile-title {{
      margin: 0;
      font-size: 20px;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
    }}
    .profile-desc-inline {{
      color: var(--muted);
      font-size: 14px;
      font-weight: 400;
      margin-left: 10px;
    }}
    .pill {{
      display: inline-block;
      padding: 6px 10px;
      border-radius: 999px;
      background: rgba(35,92,61,.08);
      color: var(--accent);
      font-size: 12px;
      font-weight: 600;
      white-space: nowrap;
    }}
    .mode {{
      margin-top: 14px;
      padding-top: 14px;
      border-top: 1px dashed var(--line);
    }}
    .mode:first-of-type {{
      margin-top: 0;
      padding-top: 0;
      border-top: 0;
    }}
    .mode-title {{
      display: flex;
      justify-content: space-between;
      align-items: center;
      margin-bottom: 10px;
      gap: 10px;
    }}
    .mode-title strong {{
      font-size: 15px;
    }}
    .mode-tags {{
      display: flex;
      align-items: center;
      gap: 8px;
      flex-wrap: wrap;
    }}
    .mode-state {{
      display: inline-block;
      padding: 6px 10px;
      border-radius: 999px;
      font-size: 12px;
      font-weight: 700;
      white-space: nowrap;
    }}
    .state-idle {{
      background: #d9ecd3;
      color: #1d5b25;
    }}
    .state-bad {{
      background: #f6d7cb;
      color: #8a3b12;
    }}
    .meta {{
      display: grid;
      grid-template-columns: minmax(0, 1fr) minmax(180px, 2.2fr) minmax(0, 1fr) minmax(0, 1fr);
      gap: 8px;
      margin-bottom: 10px;
      align-items: stretch;
    }}
    .meta div {{
      padding: 10px;
      border-radius: 12px;
      background: rgba(35,92,61,.04);
      min-height: 64px;
    }}
    .meta div:nth-child(2) {{
      min-width: 0;
    }}
    .meta span {{
      display: block;
      color: var(--muted);
      font-size: 12px;
      margin-bottom: 5px;
    }}
    .meta b {{
      font-size: 13px;
      word-break: break-word;
    }}
    .meta div:nth-child(2) b {{
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
      word-break: normal;
    }}
    .actions {{
      display: flex;
      flex-wrap: nowrap;
      gap: 8px;
      align-items: stretch;
    }}
    .issue-note {{
      display: none;
      min-height: 20px;
      margin: 0 0 10px;
      color: var(--warn);
      font-size: 12px;
      line-height: 1.5;
    }}
    .issue-note.show {{
      display: block;
    }}
    button {{
      border: 0;
      border-radius: 12px;
      padding: 9px 10px;
      font-size: 11px;
      cursor: pointer;
      background: var(--accent);
      color: white;
      white-space: nowrap;
    }}
    .actions button {{
      flex: 1 1 0;
      min-width: 0;
    }}
    button.secondary {{
      background: #dce7d7;
      color: var(--ink);
    }}
    button.alt {{
      background: var(--accent-2);
      color: #1d2a1f;
    }}
    button.warn {{
      background: #f2d8b8;
      color: var(--warn);
    }}
    button:disabled {{
      opacity: .45;
      cursor: not-allowed;
    }}
    .status-row {{
      display: contents;
    }}
    .status-chip {{
      display: inline-flex;
      align-items: center;
      gap: 8px;
      padding: 8px 12px;
      border-radius: 999px;
      background: rgba(35,92,61,.08);
      color: var(--accent);
      font-size: 12px;
      font-weight: 600;
    }}
    .foot {{
      margin-top: 22px;
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
    }}
    .toast {{
      position: fixed;
      right: 18px;
      bottom: 18px;
      padding: 12px 14px;
      background: rgba(29,42,31,.92);
      color: white;
      border-radius: 12px;
      opacity: 0;
      transform: translateY(8px);
      transition: .18s ease;
      pointer-events: none;
      max-width: 420px;
      font-size: 13px;
    }}
    .toast.show {{
      opacity: 1;
      transform: translateY(0);
    }}
    @media (max-width: 1100px) {{
      .grid {{ grid-template-columns: 1fr; }}
      .meta {{ grid-template-columns: repeat(2, minmax(0, 1fr)); }}
    }}
    @media (max-width: 680px) {{
      .meta {{ grid-template-columns: 1fr; }}
      .wrap {{ padding: 18px 14px 28px; }}
    }}
  </style>
</head>
<body>
  <div class="wrap">
    <section class="hero">
      <div class="hero-card">
        <h1>Tender Watch 控制台</h1>
        <div class="sub">
          4 个 profile、2 种采集模式的本地控制页。支持弹出终端运行、查看命中/结果/问题日志，以及打开工作文件夹。
        </div>
        <div class="toolbar">
          <span class="status-chip">最近渲染 <span id="last-refresh">{generated_at}</span></span>
          <button class="warn" onclick="refreshState()">刷新状态</button>
          <button onclick="runAllIncremental()">一键全部增量采集</button>
          <button class="secondary" onclick="window.open('/latest_hits_overview', '_blank')">查看最新命中汇总</button>
          <button class="secondary" onclick="window.open('/history_hits_overview', '_blank')">查看所有历史命中汇总</button>
          <button class="secondary" onclick="openPath('/Users/openclaw/Documents/Playground/tender-watch')">打开工作文件夹目录</button>
          <button class="alt" onclick="window.open('/viewer?kind=markdown&path=' + encodeURIComponent('{SOURCE_LIST_FILE}'), '_blank')">查看 4 个 Profile 源清单</button>
        </div>
      </div>
    </section>
    <section class="grid">{cards_html}</section>
  </div>
  <div class="toast" id="toast"></div>
  <script>
    const profiles = {json.dumps([p["id"] for p in PROFILES], ensure_ascii=False)};
    const modes = {json.dumps([m["id"] for m in MODES], ensure_ascii=False)};

    function statusClass(current) {{
      if (typeof current.issues_count === 'number' && current.issues_count > 0) return 'state-bad';
      return 'state-idle';
    }}

    function statusText(current) {{
      if (typeof current.issues_count === 'number' && current.issues_count > 0) return '有问题';
      return '正常';
    }}

    function setButtonState(id, path, kind) {{
      const el = document.getElementById(id);
      if (!el) return;
      const hasPath = !!path;
      el.disabled = !hasPath;
      el.setAttribute('aria-disabled', hasPath ? 'false' : 'true');
      if (!hasPath) {{
        el.onclick = null;
        return;
      }}
      if (kind === 'view') {{
        el.onclick = () => viewFile(path);
      }} else if (kind === 'latest') {{
        el.onclick = () => viewLatestHits(path);
      }} else {{
        el.onclick = () => openPath(path);
      }}
    }}

    function setText(id, value) {{
      const el = document.getElementById(id);
      if (el) el.textContent = (value === null || value === undefined || value === '') ? '-' : String(value);
    }}

    function toast(msg) {{
      const el = document.getElementById('toast');
      el.textContent = msg;
      el.classList.add('show');
      clearTimeout(window.__toastTimer);
      window.__toastTimer = setTimeout(() => el.classList.remove('show'), 2200);
    }}

    async function runCmd(mode, profileId) {{
      try {{
        const resp = await fetch('/api/run', {{
          method: 'POST',
          headers: {{ 'Content-Type': 'application/json' }},
          body: JSON.stringify({{ mode, profile: profileId }})
        }});
        const data = await resp.json();
        toast(data.message || '已发起运行');
      }} catch (e) {{
        toast('终端启动失败');
      }}
    }}

    async function runAllIncremental() {{
      try {{
        const resp = await fetch('/api/run_all_incremental', {{
          method: 'POST',
          headers: {{ 'Content-Type': 'application/json' }},
          body: JSON.stringify({{}})
        }});
        const data = await resp.json();
        toast(data.message || '已启动全部增量采集');
      }} catch (e) {{
        toast('批量启动失败');
      }}
    }}

    async function openPath(path) {{
      if (!path) {{
        toast('当前没有可打开文件');
        return;
      }}
      try {{
        const resp = await fetch('/api/open', {{
          method: 'POST',
          headers: {{ 'Content-Type': 'application/json' }},
          body: JSON.stringify({{ path }})
        }});
        const data = await resp.json();
        toast(data.message || '已打开');
      }} catch (e) {{
        toast('打开失败');
      }}
    }}

    function view(path, kind='json') {{
      if (!path) {{
        toast('当前没有可查看文件');
        return;
      }}
      window.open(`/viewer?kind=${{encodeURIComponent(kind)}}&path=${{encodeURIComponent(path)}}`, '_blank');
    }}
    function viewFile(path) {{
      view(path, 'json');
    }}

    function viewLatestHits(path) {{
      if (!path) {{
        toast('当前没有可查看文件');
        return;
      }}
      window.open(`/latest_hits?path=${{encodeURIComponent(path)}}`, '_blank');
    }}

    function applyState(profileId, modeId, current) {{
      const suffix = `${{profileId}}-${{modeId}}`;
      setText(`count-${{suffix}}`, current.count);
      setText(`cutoff-${{suffix}}`, current.cutoff);
      setText(`wall-${{suffix}}`, current.wall_time_sec === null || current.wall_time_sec === undefined ? '-' : `${{current.wall_time_sec}} s`);
      setText(`issues-${{suffix}}`, current.issues_count);
      const noteEl = document.getElementById(`issue-note-${{suffix}}`);
      if (noteEl) {{
        const note = current.issue_summary || '';
        noteEl.textContent = note;
        noteEl.className = note ? 'issue-note show' : 'issue-note';
      }}
      const stateEl = document.getElementById(`state-${{suffix}}`);
      if (stateEl) {{
        stateEl.textContent = statusText(current);
        stateEl.className = `mode-state ${{statusClass(current)}}`;
      }}
      setButtonState(`output-btn-${{suffix}}`, current.output_file, 'view');
      setButtonState(`issues-btn-${{suffix}}`, current.issues_file, 'view');
      setButtonState(`open-btn-${{suffix}}`, current.output_file, 'open');
    }}

    async function refreshState(silent = false) {{
      try {{
        const resp = await fetch('/api/state', {{ cache: 'no-store' }});
        const data = await resp.json();
        profiles.forEach(profileId => {{
          modes.forEach(modeId => applyState(profileId, modeId, data[profileId][modeId]));
        }});
        document.getElementById('last-refresh').textContent = new Date().toLocaleString('zh-CN', {{ hour12: false }});
        if (!silent) toast('状态已刷新');
      }} catch (e) {{
        if (!silent) toast('刷新失败');
      }}
    }}

  </script>
</body>
</html>"""


def run_in_terminal(mode: str, profile_id: str):
    cmd = f"cd {shlex.quote(str(BASE_DIR))} && ./run_monitor.sh {shlex.quote(mode)} {shlex.quote(profile_id)}"
    script = f'''
tell application "Terminal"
    activate
    do script "{cmd.replace('\\\\', '\\\\\\\\').replace('\"', '\\\\\"')}"
end tell
'''
    subprocess.run(["osascript", "-e", script], check=True)


def run_all_incremental_in_terminal():
    lines = [f"cd {shlex.quote(str(BASE_DIR))}"]
    for profile in PROFILES:
        lines.append(f"echo '===== incremental: {profile['id']} ====='")
        lines.append(f"./run_monitor.sh incremental {shlex.quote(profile['id'])}")
    cmd = " && ".join(lines)
    script = f'''
tell application "Terminal"
    activate
    do script "{cmd.replace('\\\\', '\\\\\\\\').replace('\"', '\\\\\"')}"
end tell
'''
    subprocess.run(["osascript", "-e", script], check=True)


def open_path(path_str: str):
    subprocess.run(["open", path_str], check=True)


def render_latest_hits(path: Path) -> str:
    data = load_json(path)
    if not isinstance(data, list):
        body = "<p>结果文件格式不是列表，无法预览。</p>"
    elif not data:
        body = "<p>当前结果文件为空，没有命中。</p>"
    else:
        rows = []
        for idx, item in enumerate(data[:10], start=1):
            title = html.escape(str(item.get("title", "")))
            source = html.escape(str(item.get("source", "")))
            published_at = html.escape(str(item.get("published_at", "")))
            url = html.escape(str(item.get("url", "")))
            rows.append(
                f"<li><strong>{idx}. {title}</strong><br>"
                f"<span>{source} | {published_at}</span><br>"
                f"<a href=\"{url}\" target=\"_blank\">{url}</a></li>"
            )
        body = f"<ol>{''.join(rows)}</ol>"
    title = html.escape(path.name)
    return f"""<!doctype html><html><head><meta charset="utf-8"><title>{title}</title>
    <style>
    body{{margin:0;background:#f5f7f2;color:#1d2a1f;font-family:\"PingFang SC\",\"Hiragino Sans GB\",\"Noto Sans CJK SC\",sans-serif}}
    header{{padding:14px 18px;background:#dfe8d6;border-bottom:1px solid #c8d3c0}}
    main{{padding:18px 22px;line-height:1.6}}
    ol{{padding-left:22px;margin:0}}
    li{{margin:0 0 16px}}
    a{{color:#235c3d;word-break:break-all}}
    span{{color:#617066;font-size:13px}}
    </style></head><body><header>{html.escape(str(path))}</header><main>{body}</main></body></html>"""


def render_latest_hits_overview() -> str:
    states = all_states()
    latest_outputs = latest_output_by_profile(states)
    blocks = []
    for profile in PROFILES:
        pid = profile["id"]
        heading = f"{profile['name']} {profile['desc']}"
        path_str = latest_outputs.get(pid, "")
        if not path_str:
            blocks.append(
                f"<section><h3>{html.escape(heading)}</h3><p>当前没有结果文件。</p></section>"
            )
            continue
        path = Path(path_str)
        data = load_json(path) if path.exists() else None
        if not isinstance(data, list):
            blocks.append(
                f"<section><h3>{html.escape(heading)}</h3><p>结果文件不可读：{html.escape(path_str)}</p></section>"
            )
            continue
        if not data:
            blocks.append(
                f"<section><h3>{html.escape(heading)}</h3><p>命中数 0。</p></section>"
            )
            continue
        rows = []
        for idx, item in enumerate(data[:5], start=1):
            title = html.escape(str(item.get("title", "")))
            source = html.escape(str(item.get("source", "")))
            published_at = html.escape(str(item.get("published_at", "")))
            url = html.escape(str(item.get("url", "")))
            rows.append(
                f"<li><strong>{idx}. {title}</strong><br>"
                f"<span>{source} | {published_at}</span><br>"
                f"<a href=\"{url}\" target=\"_blank\">{url}</a></li>"
            )
        blocks.append(
            f"<section><h3>{html.escape(heading)}（命中 {len(data)}）</h3><ol>{''.join(rows)}</ol></section>"
        )
    body = "".join(blocks)
    return f"""<!doctype html><html><head><meta charset="utf-8"><title>最新命中汇总</title>
    <style>
    body{{margin:0;background:#f5f7f2;color:#1d2a1f;font-family:\"PingFang SC\",\"Hiragino Sans GB\",\"Noto Sans CJK SC\",sans-serif}}
    header{{padding:14px 18px;background:#dfe8d6;border-bottom:1px solid #c8d3c0;font-weight:700}}
    main{{padding:18px 22px;line-height:1.6}}
    section{{margin-bottom:22px;padding:14px;border:1px solid #d3dccb;border-radius:12px;background:#fbfdf8}}
    h3{{margin:0 0 10px;font-size:16px}}
    ol{{padding-left:22px;margin:0}}
    li{{margin:0 0 14px}}
    a{{color:#235c3d;word-break:break-all}}
    span{{color:#617066;font-size:13px}}
    p{{margin:0;color:#617066}}
    </style></head><body><header>4 个 Profile 最新命中汇总</header><main>{body}</main></body></html>"""


def render_history_hits_overview() -> str:
    groups = collect_history_hits()
    sections = []
    ordered = [p["id"] for p in PROFILES] + ["legacy"]
    for bucket in ordered:
        items = groups.get(bucket, [])
        title = "兼容历史文件" if bucket == "legacy" else f"{profile_label(bucket)} {profile_desc(bucket)}"
        if not items:
            sections.append(f"<section><h3>{html.escape(title)}</h3><p>暂无历史命中。</p></section>")
            continue
        rows = []
        for idx, item in enumerate(items[:30], start=1):
            rows.append(
                f"<li><strong>{idx}. {html.escape(item['title'])}</strong><br>"
                f"<span>{html.escape(item['source'])} | {html.escape(item['published_at'])}</span><br>"
                f"<a href=\"{html.escape(item['url'])}\" target=\"_blank\">{html.escape(item['url'])}</a><br>"
                f"<span>文件：{html.escape(item['file'])}</span></li>"
            )
        sections.append(
            f"<section><h3>{html.escape(title)}（历史去重命中 {len(items)}）</h3><ol>{''.join(rows)}</ol></section>"
        )
    body = "".join(sections)
    return f"""<!doctype html><html><head><meta charset="utf-8"><title>所有历史命中汇总</title>
    <style>
    body{{margin:0;background:#f5f7f2;color:#1d2a1f;font-family:\"PingFang SC\",\"Hiragino Sans GB\",\"Noto Sans CJK SC\",sans-serif}}
    header{{padding:14px 18px;background:#dfe8d6;border-bottom:1px solid #c8d3c0;font-weight:700}}
    main{{padding:18px 22px;line-height:1.6}}
    section{{margin-bottom:22px;padding:14px;border:1px solid #d3dccb;border-radius:12px;background:#fbfdf8}}
    h3{{margin:0 0 10px;font-size:16px}}
    ol{{padding-left:22px;margin:0}}
    li{{margin:0 0 14px}}
    a{{color:#235c3d;word-break:break-all}}
    span{{color:#617066;font-size:13px}}
    p{{margin:0;color:#617066}}
    </style></head><body><header>所有历史命中汇总</header><main>{body}</main></body></html>"""


class Handler(BaseHTTPRequestHandler):
    def _send_text(self, text, status=200, content_type="text/html; charset=utf-8"):
        payload = text.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
        self.send_header("Pragma", "no-cache")
        self.send_header("Expires", "0")
        self.end_headers()
        self.wfile.write(payload)

    def _send_json(self, data, status=200):
        self._send_text(json.dumps(data, ensure_ascii=False), status, "application/json; charset=utf-8")

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        params = urllib.parse.parse_qs(parsed.query)
        if parsed.path == "/":
            return self._send_text(dashboard_html())
        if parsed.path == "/api/state":
            return self._send_json(all_states())
        if parsed.path == "/latest_hits":
            raw_path = params.get("path", [""])[0]
            if not raw_path:
                return self._send_text("missing path", HTTPStatus.BAD_REQUEST)
            path = Path(raw_path)
            if not path.exists():
                return self._send_text("file not found", HTTPStatus.NOT_FOUND)
            return self._send_text(render_latest_hits(path))
        if parsed.path == "/latest_hits_overview":
            return self._send_text(render_latest_hits_overview())
        if parsed.path == "/history_hits_overview":
            return self._send_text(render_history_hits_overview())
        if parsed.path == "/viewer":
            raw_path = params.get("path", [""])[0]
            kind = params.get("kind", ["json"])[0]
            if not raw_path:
                return self._send_text("missing path", HTTPStatus.BAD_REQUEST)
            path = Path(raw_path)
            if not path.exists():
                return self._send_text("file not found", HTTPStatus.NOT_FOUND)
            text = path.read_text(encoding="utf-8", errors="ignore")
            title = html.escape(path.name)
            if kind == "markdown":
                body = f"<pre>{html.escape(text)}</pre>"
            else:
                body = f"<pre>{html.escape(text)}</pre>"
            return self._send_text(f"""<!doctype html><html><head><meta charset="utf-8"><title>{title}</title>
            <style>body{{margin:0;background:#f5f7f2;color:#1d2a1f;font-family:ui-monospace,SFMono-Regular,Menlo,monospace}}header{{padding:14px 18px;background:#dfe8d6;border-bottom:1px solid #c8d3c0}}pre{{white-space:pre-wrap;word-break:break-word;padding:18px;margin:0;line-height:1.5}}</style>
            </head><body><header>{html.escape(str(path))}</header>{body}</body></html>""")
        return self._send_text("not found", HTTPStatus.NOT_FOUND)

    def do_POST(self):
        parsed = urllib.parse.urlparse(self.path)
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length).decode("utf-8") if length else "{}"
        try:
            data = json.loads(raw or "{}")
        except Exception:
            data = {}
        if parsed.path == "/api/run":
            mode = data.get("mode", "")
            profile = data.get("profile", "")
            if mode not in {"incremental", "snapshot"}:
                return self._send_json({"message": "unsupported mode"}, 400)
            if profile not in {p["id"] for p in PROFILES}:
                return self._send_json({"message": "unsupported profile"}, 400)
            try:
                run_in_terminal(mode, profile)
                return self._send_json({"message": f"已在 Terminal 启动 {mode} / {profile}"})
            except Exception as exc:
                return self._send_json({"message": f"启动失败: {exc}"}, 500)
        if parsed.path == "/api/run_all_incremental":
            try:
                run_all_incremental_in_terminal()
                return self._send_json({"message": "已在 Terminal 顺序启动 4 个 profile 的增量采集"})
            except Exception as exc:
                return self._send_json({"message": f"批量启动失败: {exc}"}, 500)
        if parsed.path == "/api/open":
            path = data.get("path", "")
            if not path:
                return self._send_json({"message": "missing path"}, 400)
            try:
                open_path(path)
                return self._send_json({"message": "已打开"})
            except Exception as exc:
                return self._send_json({"message": f"打开失败: {exc}"}, 500)
        return self._send_json({"message": "not found"}, 404)

    def log_message(self, fmt, *args):
        return


def main():
    host = "127.0.0.1"
    server = ThreadingHTTPServer((host, PORT), Handler)
    print(f"http://{host}:{PORT}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
