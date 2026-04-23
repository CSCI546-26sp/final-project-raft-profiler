from __future__ import annotations
import json
from pathlib import Path

from .critical_path      import CriticalPathResult
from .runtime_breakdown  import TaskBreakdown


def generate_dashboard(
    timing:        dict[str, dict],
    resolved_deps: dict[str, list[str]],
    cp:            CriticalPathResult,
    breakdowns:    dict[str, TaskBreakdown],
    output_path:   str = "ray_profiler_dashboard.html",
    job_label:     str = "Ray Job",
) -> str:
    tasks_json = _build_tasks_json(timing, resolved_deps, cp, breakdowns)
    edges_json = _build_edges_json(resolved_deps, timing, cp)
    cp_json    = json.dumps(cp.path)
    label_json = json.dumps(job_label)
    cp_ms      = cp.cp_value.get(cp.terminal, 0)
    summary_json = json.dumps({
        "total_tasks":    len(timing),
        "cp_tasks":       len(cp.path),
        "cp_ms":          round(cp_ms, 1),
        "job_label":      job_label,
    })

    html = _TEMPLATE
    html = html.replace("__TASKS_JSON__",   tasks_json)
    html = html.replace("__EDGES_JSON__",   edges_json)
    html = html.replace("__CP_JSON__",      cp_json)
    html = html.replace("__LABEL_JSON__",   label_json)
    html = html.replace("__SUMMARY_JSON__", summary_json)

    out_file = Path(output_path)
    out_file.parent.mkdir(parents=True, exist_ok=True)
    out_file.write_text(html, encoding="utf-8")
    print(f"[dashboard] written → {output_path}")
    return output_path


def _build_tasks_json(
    timing:        dict[str, dict],
    resolved_deps: dict[str, list[str]],
    cp:            CriticalPathResult,
    breakdowns:    dict[str, TaskBreakdown],
) -> str:
    tasks = []
    for tid, info in timing.items():
        bd = breakdowns.get(tid)
        on_cp = tid in cp.path_set

        waiting_ms  = round(bd.waiting_ms,  2) if bd else 0.0
        compute_ms  = round(bd.compute_ms,  2) if bd else round(info["exec_ms"], 2)
        waiting_pct = round(bd.waiting_pct(), 1) if bd else 0.0
        compute_pct = round(bd.compute_pct(), 1) if bd else 100.0

        tasks.append({
            "id":          tid,
            "name":        info["name"],
            "start_ms":    info["start_ms"],
            "end_ms":      info["end_ms"],
            "exec_ms":     round(info["exec_ms"], 2),
            "cp_value_ms": round(cp.cp_value.get(tid, 0), 2),
            "on_cp":       on_cp,
            "waiting_ms":  waiting_ms,
            "compute_ms":  compute_ms,
            "waiting_pct": waiting_pct,
            "compute_pct": compute_pct,
            "num_deps":    len(resolved_deps.get(tid, [])),
        })
    return json.dumps(tasks)


def _build_edges_json(
    resolved_deps: dict[str, list[str]],
    timing:        dict[str, dict],
    cp:            CriticalPathResult,
) -> str:
    edges = []
    for tid, deps in resolved_deps.items():
        if tid not in timing:
            continue
        for dep in deps:
            if dep in timing:
                on_cp = (tid in cp.path_set and dep in cp.path_set
                         and cp.cp_prev.get(tid) == dep)
                edges.append({"source": dep, "target": tid, "on_cp": on_cp})
    return json.dumps(edges)


_TEMPLATE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<title>Ray Critical-Path Profiler</title>
<script src="https://cdnjs.cloudflare.com/ajax/libs/d3/7.9.0/d3.min.js"></script>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:"Segoe UI",sans-serif;background:#0f1117;color:#e2e8f0;min-height:100vh}

header{background:linear-gradient(135deg,#1a2e4a,#0f1117);padding:18px 32px;
       border-bottom:1px solid #2d3748;display:flex;align-items:center;gap:24px}
header h1{font-size:1.3rem;color:#63b3ed}
header p{font-size:.8rem;color:#718096;margin-top:3px}
.stat-box{background:#1a202c;border:1px solid #2d3748;border-radius:8px;
          padding:10px 18px;text-align:center;min-width:110px}
.stat-val{font-size:1.3rem;font-weight:700;color:#63b3ed}
.stat-val.red{color:#fc8181}
.stat-lbl{font-size:.7rem;color:#718096;margin-top:2px}

nav{display:flex;gap:4px;padding:10px 32px;background:#1a202c;border-bottom:1px solid #2d3748}
nav button{padding:6px 18px;border:none;border-radius:6px;background:#2d3748;
           color:#a0aec0;cursor:pointer;font-size:.82rem;transition:background .12s}
nav button.active{background:#3182ce;color:#fff}
nav button:hover:not(.active){background:#4a5568}

.panel{display:none;padding:24px 32px}
.panel.active{display:block}

.legend{display:flex;gap:16px;margin-bottom:14px;font-size:.78rem}
.legend span{display:flex;align-items:center;gap:6px}
.dot{width:11px;height:11px;border-radius:3px}
#tl-scroll{overflow-x:auto}
.task-row{display:flex;align-items:center;margin-bottom:5px}
.task-label{width:200px;min-width:200px;font-size:.75rem;color:#a0aec0;
            white-space:nowrap;overflow:hidden;text-overflow:ellipsis;
            padding-right:8px;text-align:right}
.task-label.cp{color:#fc8181;font-weight:600}
.bar-track{flex:1;position:relative;height:20px;background:#1a202c;border-radius:4px}
.bar{position:absolute;height:100%;border-radius:4px;display:flex;align-items:center;
     padding:0 5px;font-size:.67rem;color:#fff;white-space:nowrap;overflow:hidden;
     cursor:pointer;transition:opacity .12s}
.bar:hover{opacity:.8}
.bar.waiting{background:#3d4f63}
.bar.compute{background:#2b6cb0}
.bar.compute.cp{background:#c53030}

#dag-svg{width:100%;border:1px solid #2d3748;border-radius:8px;background:#1a202c}
.node circle{stroke-width:2;cursor:pointer}
.node text{font-size:10px;fill:#cbd5e0;pointer-events:none}
.link{fill:none;stroke:#3d4f63;stroke-width:1.5}
.link.cp{stroke:#c53030;stroke-width:2.5}

table{width:100%;border-collapse:collapse;font-size:.8rem}
th{background:#2d3748;padding:9px 12px;text-align:left;color:#a0aec0;
   cursor:pointer;user-select:none;white-space:nowrap}
th:hover{background:#4a5568}
td{padding:8px 12px;border-bottom:1px solid #1e2535;white-space:nowrap}
tr:hover td{background:#1a2535}
tr.cp-row td{color:#fc8181}
.badge{display:inline-block;padding:1px 7px;border-radius:999px;font-size:.7rem}
.badge.yes{background:#742a2a;color:#fc8181}
.badge.no{background:#2d3748;color:#718096}
.mini-bar{display:flex;height:7px;border-radius:3px;overflow:hidden;width:100px}
.mini-w{background:#3d4f63}
.mini-c{background:#2b6cb0}
.mini-c.cp{background:#c53030}

#tip{position:fixed;pointer-events:none;display:none;background:#2d3748;
     border:1px solid #4a5568;border-radius:6px;padding:8px 12px;
     font-size:.76rem;line-height:1.65;z-index:9999;max-width:280px}
</style>
</head>
<body>
<header>
  <div>
    <h1>⚡ Ray Critical-Path Profiler</h1>
    <p id="sub">Tasks on the critical path are highlighted in red</p>
  </div>
  <div style="margin-left:auto;display:flex;gap:10px" id="stats"></div>
</header>
<nav>
  <button class="active" onclick="showTab('timeline',this)">📊 Timeline</button>
  <button onclick="showTab('dag',this)">🔗 DAG</button>
  <button onclick="showTab('table',this)">📋 Breakdown Table</button>
</nav>
<div id="timeline" class="panel active"></div>
<div id="dag"      class="panel"></div>
<div id="table"    class="panel"></div>
<div id="tip"></div>

<script>
const TASKS   = __TASKS_JSON__;
const EDGES   = __EDGES_JSON__;
const CP      = __CP_JSON__;
const LABEL   = __LABEL_JSON__;
const SUMMARY = __SUMMARY_JSON__;
const CP_SET  = new Set(CP);

document.getElementById("sub").textContent =
  `${LABEL} — tasks on the critical path are highlighted in red`;
document.getElementById("stats").innerHTML = `
  <div class="stat-box">
    <div class="stat-val">${SUMMARY.total_tasks}</div>
    <div class="stat-lbl">Total Tasks</div>
  </div>
  <div class="stat-box">
    <div class="stat-val red">${SUMMARY.cp_tasks}</div>
    <div class="stat-lbl">On Critical Path</div>
  </div>
  <div class="stat-box">
    <div class="stat-val">${(SUMMARY.cp_ms/1000).toFixed(2)}s</div>
    <div class="stat-lbl">Critical Path</div>
  </div>`;

function showTab(name, btn) {
  document.querySelectorAll(".panel").forEach(p => p.classList.remove("active"));
  document.querySelectorAll("nav button").forEach(b => b.classList.remove("active"));
  document.getElementById(name).classList.add("active");
  btn.classList.add("active");
  if (name === "dag") renderDAG();
}

const tip = document.getElementById("tip");
function showTip(e, html) {
  tip.innerHTML = html;
  tip.style.display = "block";
  tip.style.left = (e.clientX + 14) + "px";
  tip.style.top  = (e.clientY + 14) + "px";
}
document.addEventListener("mousemove", e => {
  tip.style.left = (e.clientX + 14) + "px";
  tip.style.top  = (e.clientY + 14) + "px";
});
function hideTip() { tip.style.display = "none"; }

(function buildTimeline() {
  const wrap = document.getElementById("timeline");
  if (!TASKS.length) { wrap.innerHTML = "<p style='color:#718096'>No tasks.</p>"; return; }

  const minT = Math.min(...TASKS.map(t => t.start_ms));
  const maxT = Math.max(...TASKS.map(t => t.end_ms));
  const span = (maxT - minT) || 1;

  wrap.innerHTML = `
    <div class="legend">
      <span><div class="dot" style="background:#3d4f63"></div>Waiting / Scheduling</span>
      <span><div class="dot" style="background:#2b6cb0"></div>Compute</span>
      <span><div class="dot" style="background:#c53030"></div>Critical Path</span>
    </div>
    <div id="tl-scroll"></div>`;

  const container = document.getElementById("tl-scroll");

  const sorted = [...TASKS].sort((a, b) =>
    (b.on_cp - a.on_cp) || (a.start_ms - b.start_ms));

  for (const t of sorted) {
    const row   = document.createElement("div");
    row.className = "task-row";

    const label = document.createElement("div");
    label.className = "task-label" + (t.on_cp ? " cp" : "");
    label.textContent = t.name;
    label.title = `${t.name}\n${t.id}`;

    const track = document.createElement("div");
    track.className = "bar-track";

    const relStart = (t.start_ms - minT) / span * 100;
    const wPct     = t.waiting_ms / span * 100;
    const cPct     = t.compute_ms / span * 100;

    const tipHTML = (extra) =>
      `<b>${t.name}</b>${t.on_cp ? " ⚡ Critical" : ""}<br>` +
      `Task ID: ${t.id.slice(0,16)}…<br>` +
      `Exec: ${t.exec_ms.toFixed(1)} ms<br>` +
      `Waiting: ${t.waiting_ms.toFixed(1)} ms (${t.waiting_pct}%)<br>` +
      `Compute: ${t.compute_ms.toFixed(1)} ms (${t.compute_pct}%)` + extra;

    if (wPct > 0.05) {
      const w = document.createElement("div");
      w.className = "bar waiting";
      w.style.left  = relStart + "%";
      w.style.width = wPct + "%";
      w.textContent = wPct > 4 ? `wait ${t.waiting_pct}%` : "";
      w.addEventListener("mouseover", e => showTip(e, tipHTML("<br><i>← waiting segment</i>")));
      w.addEventListener("mouseleave", hideTip);
      track.appendChild(w);
    }

    const c = document.createElement("div");
    c.className = "bar compute" + (t.on_cp ? " cp" : "");
    c.style.left  = (relStart + wPct) + "%";
    c.style.width = Math.max(cPct, 0.2) + "%";
    c.textContent = cPct > 4 ? `${(t.exec_ms/1000).toFixed(2)}s` : "";
    c.addEventListener("mouseover", e => showTip(e, tipHTML("<br><i>← compute segment</i>")));
    c.addEventListener("mouseleave", hideTip);
    track.appendChild(c);

    row.appendChild(label);
    row.appendChild(track);
    container.appendChild(row);
  }
})();

let dagDone = false;
function renderDAG() {
  if (dagDone) return;
  dagDone = true;

  const panel = document.getElementById("dag");
  const W = Math.max(panel.clientWidth, 800), H = 560;

  const svg = d3.select("#dag").append("svg")
    .attr("id","dag-svg").attr("viewBox",`0 0 ${W} ${H}`);

  const defs = svg.append("defs");
  for (const [id, color] of [["arr","#3d4f63"],["arr-cp","#c53030"]]) {
    defs.append("marker").attr("id",id).attr("viewBox","0 -5 10 10")
      .attr("refX",22).attr("refY",0).attr("markerWidth",6).attr("markerHeight",6)
      .attr("orient","auto")
      .append("path").attr("d","M0,-5L10,0L0,5").attr("fill",color);
  }

  const nodeIds = new Set();
  EDGES.forEach(e => { nodeIds.add(e.source); nodeIds.add(e.target); });
  CP.forEach(id => nodeIds.add(id));

  const nodeMap = {};
  TASKS.filter(t => nodeIds.has(t.id)).forEach(t => {
    nodeMap[t.id] = { ...t, x: W/2 + Math.random()*10, y: H/2 + Math.random()*10 };
  });

  const nodes = Object.values(nodeMap);
  const links = EDGES
    .filter(e => nodeMap[e.source] && nodeMap[e.target])
    .map(e => ({ source: nodeMap[e.source], target: nodeMap[e.target], on_cp: e.on_cp }));

  const sim = d3.forceSimulation(nodes)
    .force("link",   d3.forceLink(links).id(d => d.id).distance(110).strength(0.8))
    .force("charge", d3.forceManyBody().strength(-250))
    .force("center", d3.forceCenter(W/2, H/2))
    .force("x",      d3.forceX(W/2).strength(0.03))
    .force("y",      d3.forceY(H/2).strength(0.03));

  const link = svg.append("g").selectAll("line").data(links).join("line")
    .attr("class", d => "link" + (d.on_cp ? " cp" : ""))
    .attr("marker-end", d => d.on_cp ? "url(#arr-cp)" : "url(#arr)");

  const node = svg.append("g").selectAll("g").data(nodes).join("g")
    .attr("class","node")
    .call(d3.drag()
      .on("start",(e,d)=>{ if(!e.active) sim.alphaTarget(0.3).restart(); d.fx=d.x;d.fy=d.y; })
      .on("drag", (e,d)=>{ d.fx=e.x; d.fy=e.y; })
      .on("end",  (e,d)=>{ if(!e.active) sim.alphaTarget(0); d.fx=null;d.fy=null; }));

  node.append("circle").attr("r",16)
    .attr("fill",   d => d.on_cp ? "#742a2a" : "#2d3748")
    .attr("stroke", d => d.on_cp ? "#c53030" : "#4a5568");

  node.append("text").attr("dy","0.35em").attr("text-anchor","middle")
    .text(d => d.name.length > 11 ? d.name.slice(0,10)+"…" : d.name);

  node.on("mouseover", (e,d) => showTip(e,
    `<b>${d.name}</b>${d.on_cp?" ⚡":""}<br>` +
    `Exec: ${d.exec_ms.toFixed(1)} ms<br>` +
    `CP value: ${d.cp_value_ms.toFixed(1)} ms<br>` +
    `Waiting: ${d.waiting_ms.toFixed(1)} ms | Compute: ${d.compute_ms.toFixed(1)} ms`))
    .on("mouseleave", hideTip);

  sim.on("tick", () => {
    link.attr("x1",d=>clamp(d.source.x,20,W-20))
        .attr("y1",d=>clamp(d.source.y,20,H-20))
        .attr("x2",d=>clamp(d.target.x,20,W-20))
        .attr("y2",d=>clamp(d.target.y,20,H-20));
    node.attr("transform",d=>`translate(${clamp(d.x,20,W-20)},${clamp(d.y,20,H-20)})`);
  });

  function clamp(v,lo,hi){ return Math.max(lo,Math.min(hi,v)); }
}

(function buildTable() {
  let sortKey="on_cp", sortDir=-1;

  function render() {
    const rows = [...TASKS].sort((a,b) => {
      let va=a[sortKey], vb=b[sortKey];
      if (typeof va==="string"){ va=va.toLowerCase(); vb=vb.toLowerCase(); }
      return sortDir*(va<vb?-1:va>vb?1:0);
    });

    document.getElementById("table").innerHTML = `
    <table>
      <thead><tr>
        <th onclick="rs('name')">Task Name</th>
        <th onclick="rs('on_cp')">Critical?</th>
        <th onclick="rs('exec_ms')">Exec (ms)</th>
        <th onclick="rs('waiting_ms')">Waiting (ms)</th>
        <th onclick="rs('compute_ms')">Compute (ms)</th>
        <th onclick="rs('cp_value_ms')">CP Value (ms)</th>
        <th>W / C Split</th>
      </tr></thead>
      <tbody>${rows.map(t=>`
        <tr class="${t.on_cp?'cp-row':''}">
          <td title="${t.id}">${t.name}</td>
          <td><span class="badge ${t.on_cp?'yes':'no'}">${t.on_cp?'⚡ Yes':'No'}</span></td>
          <td>${t.exec_ms.toFixed(1)}</td>
          <td>${t.waiting_ms.toFixed(1)} <small style="color:#718096">(${t.waiting_pct}%)</small></td>
          <td>${t.compute_ms.toFixed(1)} <small style="color:#718096">(${t.compute_pct}%)</small></td>
          <td>${t.cp_value_ms.toFixed(1)}</td>
          <td><div class="mini-bar">
            <div class="mini-w" style="width:${t.waiting_pct}%"></div>
            <div class="mini-c ${t.on_cp?'cp':''}" style="width:${t.compute_pct}%"></div>
          </div></td>
        </tr>`).join("")}
      </tbody>
    </table>`;
  }

  window.rs = function(key) {
    if (sortKey===key) sortDir*=-1; else { sortKey=key; sortDir=-1; }
    render();
  };
  render();
})();
</script>
</body>
</html>
"""


if __name__ == "__main__":
    import sys, ast, json as _json
    import os; sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from profiler.critical_path     import compute_critical_path
    from profiler.runtime_breakdown import breakdown_all_tasks

    json_path = sys.argv[1] if len(sys.argv) > 1 else "critical_path_profile.json"

    with open(json_path) as f:
        data = _json.load(f)

    raw_tasks = data["tasks"]

    timing = {}
    for t in raw_tasks:
        if t.get("start_ms") and t.get("end_ms"):
            timing[t["task_id"]] = {
                "name":     t["name"],
                "start_ms": t["start_ms"],
                "end_ms":   t["end_ms"],
                "exec_ms":  t["end_ms"] - t["start_ms"],
            }

    resolved_deps = {tid: [] for tid in timing}

    cp         = compute_critical_path(timing, resolved_deps)
    breakdowns = breakdown_all_tasks(raw_tasks)

    out = generate_dashboard(
        timing, resolved_deps, cp, breakdowns,
        output_path="ray_profiler_dashboard.html",
        job_label=json_path,
    )
    print(f"\nOpen in browser:\n  file://{Path(out).resolve()}")
