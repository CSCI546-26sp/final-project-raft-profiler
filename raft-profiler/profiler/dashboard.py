"""
Dashboard with recommendations, legends, and explanations.
"""

from __future__ import annotations
import json
from pathlib import Path
from .critical_path import CriticalPathResult
from .runtime_breakdown import TaskBreakdown


def generate_dashboard(
    timing, resolved_deps, cp, breakdowns,
    stragglers=None, output_path="ray_profiler_dashboard.html", job_label="Ray Job",
):
    tasks_json = _build_tasks_json(timing, resolved_deps, cp, breakdowns, stragglers)
    edges_json = _build_edges_json(resolved_deps, timing, cp)
    cp_json = json.dumps(cp.path)
    deps_json = json.dumps(resolved_deps)
    cp_ms = cp.cp_value.get(cp.terminal, 0)
    summary_json = json.dumps({
        "total_tasks": len(timing), "cp_tasks": len(cp.path),
        "cp_ms": round(cp_ms, 1), "off_cp": len(timing) - len(cp.path),
        "job_label": job_label,
    })
    html = _TEMPLATE
    for k, v in [("__TASKS_JSON__", tasks_json), ("__EDGES_JSON__", edges_json),
                  ("__DEPS_JSON__", deps_json), ("__CP_JSON__", cp_json),
                  ("__SUMMARY_JSON__", summary_json)]:
        html = html.replace(k, v)
    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(html, encoding="utf-8")
    print(f"[dashboard] written to {output_path}")
    return output_path


def _build_tasks_json(timing, resolved_deps, cp, breakdowns, stragglers=None):
    smap = {}
    if stragglers:
        for s in stragglers:
            smap[s.task_id] = {"ratio": round(s.ratio, 1), "median_ms": round(s.median_ms, 1)}
    name_counter = {}
    sorted_tids = sorted(timing.keys(), key=lambda t: timing[t]["start_ms"])
    tid_index = {}
    for tid in sorted_tids:
        name = timing[tid]["name"]
        idx = name_counter.get(name, 0)
        name_counter[name] = idx + 1
        tid_index[tid] = idx
    tasks = []
    for tid, info in timing.items():
        bd = breakdowns.get(tid)
        on_cp = tid in cp.path_set
        tasks.append({
            "id": tid, "name": info["name"],
            "label": f"{info['name']}[{tid_index[tid]}]",
            "index": tid_index[tid],
            "start_ms": info["start_ms"], "end_ms": info["end_ms"],
            "exec_ms": round(info["exec_ms"], 2),
            "cp_value_ms": round(cp.cp_value.get(tid, 0), 2), "on_cp": on_cp,
            "dependency_ms": round(getattr(bd, "dependency_ms", 0), 2) if bd else 0.0,
            "queue_ms": round(getattr(bd, "queue_ms", 0), 2) if bd else 0.0,
            "overhead_ms": round(getattr(bd, "overhead_ms", 0), 2) if bd else 0.0,
            "compute_ms": round(bd.compute_ms, 2) if bd else round(info["exec_ms"], 2),
            "waiting_ms": round(bd.waiting_ms, 2) if bd else 0.0,
            "total_ms": round(bd.total_ms, 2) if bd else round(info["exec_ms"], 2),
            "num_deps": len(resolved_deps.get(tid, [])),
            "straggler": smap.get(tid),
        })
    return json.dumps(tasks)


def _build_edges_json(resolved_deps, timing, cp):
    edges = []
    for tid, deps in resolved_deps.items():
        if tid not in timing: continue
        for dep in deps:
            if dep in timing:
                on_cp = (tid in cp.path_set and dep in cp.path_set and cp.cp_prev.get(tid) == dep)
                edges.append({"source": dep, "target": tid, "on_cp": on_cp})
    return json.dumps(edges)


_TEMPLATE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>Ray profiler</title>
<script src="https://cdnjs.cloudflare.com/ajax/libs/d3/7.9.0/d3.min.js"></script>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;
  background:#fff;color:#1a1a1a;font-size:13px;line-height:1.5}
.mono{font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:11px}

.topbar{display:flex;align-items:center;gap:14px;padding:10px 20px;border-bottom:1px solid #e5e5e5}
.logo{font-weight:600;font-size:14px}
.sep{width:1px;height:16px;background:#ddd}
.jlabel{font-size:13px;color:#888}
.pills{display:flex;gap:8px;margin-left:auto}
.pill{background:#f5f5f5;border-radius:6px;padding:4px 12px;font-size:12px;color:#888}
.pill b{font-weight:600;color:#1a1a1a}
.pill b.r{color:#993d3d}

.cpflow{padding:16px 20px;border-bottom:1px solid #e5e5e5}
.sec-hd{font-size:11px;color:#999;text-transform:uppercase;letter-spacing:.5px;margin-bottom:10px;font-weight:600}
.cpchain{display:flex;align-items:center;gap:0;overflow-x:auto;padding-bottom:4px}
.cpn{border:1.5px solid #993d3d;border-radius:6px;padding:8px 14px;text-align:center;min-width:88px;background:#fff}
.cpn .nm{font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:11px;font-weight:600;color:#993d3d}
.cpn .tm{font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:16px;font-weight:600;margin-top:1px}
.cpn .sub{font-size:10px;color:#aaa;margin-top:2px}
.cparr{padding:0 4px;color:#ccc;font-size:14px}

.tabs{display:flex;gap:0;border-bottom:1px solid #e5e5e5;padding:0 20px}
.tab{padding:10px 16px;font-size:13px;color:#999;cursor:pointer;border-bottom:2px solid transparent;background:none;border-top:0;border-left:0;border-right:0;font-family:inherit}
.tab:hover{color:#666}
.tab.a{color:#1a1a1a;border-bottom-color:#1a1a1a;font-weight:600}

.panel{display:none;padding:16px 20px}
.panel.a{display:block}

.legend{display:flex;gap:14px;margin-bottom:12px;flex-wrap:wrap}
.lg{display:flex;align-items:center;gap:4px;font-size:11px;color:#999}
.ld{width:8px;height:8px;border-radius:2px}

.stage-hd{font-size:11px;color:#aaa;text-transform:uppercase;letter-spacing:.5px;font-weight:600;margin:12px 0 4px 124px;padding-bottom:3px;border-bottom:1px solid #f0f0f0}
.row{display:flex;align-items:center;margin-bottom:3px}
.rl{width:118px;min-width:118px;text-align:right;padding-right:8px;font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:11px;color:#888;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.rl.cp{color:#993d3d;font-weight:600}
.track{flex:1;height:20px;background:#f7f7f7;border-radius:3px;position:relative;overflow:hidden}
.track.cp{outline:1.5px solid #993d3d;outline-offset:-1px}
.b{position:absolute;height:100%;min-width:2px;cursor:pointer}
.b:hover{filter:brightness(1.15)}
.tl-time{position:absolute;right:5px;top:50%;transform:translateY(-50%);font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:10px;color:#999;pointer-events:none}
.stag-i{font-size:10px;color:#996600;margin-left:4px}

.dag-wrap{border:1px solid #e5e5e5;border-radius:6px;overflow:auto;background:#fafafa}
#dag-svg text{font-family:ui-monospace,SFMono-Regular,Menlo,monospace}

.tbl{width:100%;border-collapse:collapse;font-size:12px}
.tbl th{text-align:left;padding:8px 10px;font-weight:600;font-size:11px;color:#999;text-transform:uppercase;letter-spacing:.3px;border-bottom:1px solid #e5e5e5;cursor:pointer;user-select:none;background:#fafafa}
.tbl th:hover{color:#1a1a1a}
.tbl td{padding:7px 10px;border-bottom:1px solid #f0f0f0}
.tbl tr:hover td{background:#f9f9f9}
.tbl tr.cpr td{color:#993d3d}
.bcp{display:inline-block;padding:2px 8px;border-radius:4px;font-size:10px;font-weight:600}
.bcp.y{background:#faf0f0;color:#993d3d}
.bcp.n{background:#f5f5f5;color:#aaa}
.stag-b{display:inline-block;padding:1px 5px;border-radius:3px;font-size:10px;font-weight:600;background:#fdf6e3;color:#996600;margin-left:4px}
.mb{display:flex;height:4px;border-radius:99px;overflow:hidden;width:80px;background:#f0f0f0}
.mb>div{height:100%}

.wg{display:grid;grid-template-columns:repeat(auto-fill,minmax(280px,1fr));gap:12px}
.wc{background:#fafafa;border:1px solid #e5e5e5;border-radius:6px;padding:14px}
.wc h3{font-size:12px;font-weight:600;margin-bottom:8px;display:flex;align-items:center;gap:6px}
.wsr{display:flex;align-items:center;gap:6px}
.wsr span{font-size:10px;color:#aaa}
input[type=range]{flex:1;height:4px;-webkit-appearance:none;appearance:none;background:#e0e0e0;border-radius:99px;outline:none}
input[type=range]::-webkit-slider-thumb{-webkit-appearance:none;width:14px;height:14px;border-radius:50%;background:#1a1a1a;cursor:pointer;border:2px solid #fff;box-shadow:0 0 0 1px #ccc}
.wspd{font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:12px;font-weight:600;min-width:30px;text-align:center}
.wres{margin-top:6px;font-size:11px;color:#888;line-height:1.6}
.wsv{color:#2d7a3a;font-weight:600}
.wsh{color:#996600}
.hint{background:#f5f5f5;border-radius:6px;padding:10px 14px;margin-bottom:14px;font-size:12px;color:#888;line-height:1.6}

.rec-item{border-left:3px solid #ddd;padding:8px 14px;margin-bottom:10px;font-size:12px;line-height:1.6;color:#555}
.rec-item.high{border-left-color:#993d3d}
.rec-item.med{border-left-color:#ba7517}
.rec-item.low{border-left-color:#3a7bd5}
.rec-item .rec-tag{font-size:10px;font-weight:600;text-transform:uppercase;letter-spacing:.3px;margin-bottom:2px}
.rec-item.high .rec-tag{color:#993d3d}
.rec-item.med .rec-tag{color:#ba7517}
.rec-item.low .rec-tag{color:#3a7bd5}

#tip{position:fixed;background:#1a1a1a;color:#e5e5e5;padding:8px 12px;border-radius:6px;font-size:11px;pointer-events:none;display:none;z-index:9999;max-width:300px;line-height:1.5;font-family:ui-monospace,SFMono-Regular,Menlo,monospace}
</style>
</head>
<body>
<div id="tip"></div>

<div class="topbar">
<div class="logo">Ray profiler</div>
<div class="sep"></div>
<div class="jlabel" id="jl"></div>
<div class="pills">
<div class="pill"><b id="v1"></b> tasks</div>
<div class="pill"><b class="r" id="v2"></b> on critical path</div>
<div class="pill">CP length: <b class="r" id="v3"></b></div>
</div>
</div>

<div class="cpflow">
<div class="sec-hd">Critical path</div>
<div class="cpchain" id="cpf"></div>
</div>

<div class="tabs" id="tabbar">
<button class="tab a" data-t="rec">Recommendations</button>
<button class="tab" data-t="tl">Timeline</button>
<button class="tab" data-t="dag">Dependency graph</button>
<button class="tab" data-t="tbl">Breakdown</button>
<button class="tab" data-t="wif">What-if</button>
</div>

<div class="panel a" id="p-rec"></div>
<div class="panel" id="p-tl"></div>
<div class="panel" id="p-dag"></div>
<div class="panel" id="p-tbl"></div>
<div class="panel" id="p-wif"></div>

<script>
var T=__TASKS_JSON__,E=__EDGES_JSON__,D=__DEPS_JSON__,CP=__CP_JSON__,S=__SUMMARY_JSON__;
var tip=document.getElementById("tip"),tm={};
T.forEach(function(t){tm[t.id]=t});

document.getElementById("jl").textContent=S.job_label;
document.getElementById("v1").textContent=S.total_tasks;
document.getElementById("v2").textContent=S.cp_tasks;
document.getElementById("v3").textContent=(S.cp_ms/1000).toFixed(2)+"s";

function st(e,h){tip.style.display="block";tip.innerHTML=h;
  tip.style.left=Math.min(e.clientX+12,innerWidth-320)+"px";tip.style.top=(e.clientY-10)+"px"}
function ht(){tip.style.display="none"}
function fms(ms){return ms>=1000?(ms/1000).toFixed(2)+"s":Math.round(ms)+"ms"}
function pct(part,total){return total>0?Math.round(part/total*100):0}

document.getElementById("tabbar").addEventListener("click",function(e){
  var btn=e.target;if(!btn.dataset.t)return;
  document.querySelectorAll(".tab").forEach(function(b){b.classList.remove("a")});
  document.querySelectorAll(".panel").forEach(function(p){p.classList.remove("a")});
  btn.classList.add("a");document.getElementById("p-"+btn.dataset.t).classList.add("a");
  if(btn.dataset.t==="dag")rDAG();
});

// critical path flow
(function(){
  var f=document.getElementById("cpf");
  CP.forEach(function(id,i){
    var t=tm[id];if(!t)return;
    var biggest=Math.max(t.dependency_ms,t.queue_ms,t.overhead_ms,t.compute_ms);
    var reason="compute bound";
    if(t.dependency_ms===biggest&&t.dependency_ms>0)reason="dep wait";
    else if(t.queue_ms===biggest&&t.queue_ms>0)reason="queued";
    else if(t.overhead_ms===biggest&&t.overhead_ms>0)reason="overhead";
    var strag=t.straggler?" / "+t.straggler.ratio+"x straggler":"";
    var n=document.createElement("div");n.className="cpn";
    n.innerHTML='<div class="nm">'+t.label+'</div><div class="tm">'+fms(t.exec_ms)+'</div><div class="sub">'+reason+strag+'</div>';
    f.appendChild(n);
    if(i<CP.length-1){var a=document.createElement("div");a.className="cparr";a.innerHTML="&rarr;";f.appendChild(a)}
  });
})();

// what-if recomputation (shared)
function recomputeCP(mx){
  var ids=Object.keys(tm),ig={},ch={};
  ids.forEach(function(id){ig[id]=0;ch[id]=[]});
  Object.keys(D).forEach(function(tid){
    if(!tm[tid])return;(D[tid]||[]).forEach(function(d){
      if(tm[d]){ig[tid]=(ig[tid]||0)+1;ch[d].push(tid)}
    });
  });
  var q=ids.filter(function(id){return(ig[id]||0)===0}),o=[];
  while(q.length){var t=q.shift();o.push(t);(ch[t]||[]).forEach(function(c){ig[c]--;if(ig[c]===0)q.push(c)})}
  var cv={},cp2={};
  o.forEach(function(tid){
    var ex=mx[tid]!==undefined?mx[tid]:tm[tid].exec_ms;
    var bd=null,bv=0;(D[tid]||[]).forEach(function(d){if(cv[d]!==undefined&&cv[d]>bv){bv=cv[d];bd=d}});
    cv[tid]=ex+bv;cp2[tid]=bd;
  });
  var tr=o[0];o.forEach(function(t2){if(cv[t2]>cv[tr])tr=t2});
  var pa=[],cu=tr;while(cu){pa.push(cu);cu=cp2[cu]}pa.reverse();
  return{total:cv[tr],path:pa};
}
var origCP=recomputeCP({});

// recommendations
(function(){
  var p=document.getElementById("p-rec");
  var recs=[];

  // per critical-path task analysis
  CP.forEach(function(id){
    var t=tm[id];if(!t)return;
    var total=t.total_ms||t.exec_ms||1;
    var depP=pct(t.dependency_ms,total);
    var queueP=pct(t.queue_ms,total);
    var compP=pct(t.compute_ms,total);

    if(t.straggler){
      recs.push({pri:"high",text:
        '<b>'+t.label+'</b> is a straggler. It took '+fms(t.exec_ms)+
        ' but the median for <span class="mono">'+t.name+'</span> is '+fms(t.straggler.median_ms)+
        ' ('+t.straggler.ratio+'x slower). This single instance is dragging down the entire job. '+
        'Investigate why this specific task was slow: resource contention, data skew, or a slow machine.'});
    }
    if(depP>50){
      recs.push({pri:"med",text:
        '<b>'+t.label+'</b> spent '+depP+'% of its time waiting for upstream tasks to finish. '+
        'The bottleneck is not this task itself but the tasks it depends on. '+
        'Look at what feeds into it and optimize those first.'});
    }
    if(queueP>30){
      recs.push({pri:"med",text:
        '<b>'+t.label+'</b> spent '+queueP+'% of its time waiting for a free worker. '+
        'The cluster may be under-provisioned. Adding more CPUs or machines could reduce this wait.'});
    }
    if(compP>80&&!t.straggler){
      recs.push({pri:"low",text:
        '<b>'+t.label+'</b> is compute-bound ('+compP+'% compute). '+
        'To speed it up, optimize the function code itself or reduce the data it processes.'});
    }
  });

  // what-if insights
  CP.forEach(function(id){
    var t=tm[id];if(!t)return;
    var mx={};mx[id]=t.exec_ms/2;
    var res=recomputeCP(mx);
    var saved=origCP.total-res.total;
    var shifted=JSON.stringify(res.path)!==JSON.stringify(CP);
    if(shifted&&saved<t.exec_ms*0.1){
      recs.push({pri:"low",text:
        'Making <b>'+t.label+'</b> 2x faster only saves '+Math.round(saved)+
        'ms because the bottleneck shifts to a different path. '+
        'Optimizing this task alone has limited impact.'});
    }else if(!shifted&&saved>100){
      recs.push({pri:"high",text:
        'Making <b>'+t.label+'</b> 2x faster saves '+fms(saved)+
        ' with no bottleneck shift. This is the highest-impact optimization target.'});
    }
  });

  // off-path summary
  if(S.off_cp>0){
    recs.push({pri:"low",text:
      S.off_cp+' out of '+S.total_tasks+' tasks are not on the critical path. '+
      'No matter how slow these tasks are, they do not affect total runtime. '+
      'Focus optimization effort on the '+S.cp_tasks+' critical-path tasks only.'});
  }

  if(recs.length===0){
    recs.push({pri:"low",text:'No significant issues detected. All tasks appear to be running efficiently.'});
  }

  var html='<div class="sec-hd">Recommendations</div>';
  html+='<div class="hint">These are generated automatically from the profiler data. No AI involved: just rules applied to the timing breakdown, straggler detection, and what-if simulation results.</div>';
  recs.forEach(function(r){
    html+='<div class="rec-item '+r.pri+'"><div class="rec-tag">'+
      (r.pri==="high"?"high priority":r.pri==="med"?"medium priority":"info")+
      '</div>'+r.text+'</div>';
  });
  p.innerHTML=html;
})();

// timeline
(function(){
  var p=document.getElementById("p-tl");
  var sorted=T.slice().sort(function(a,b){return a.start_ms-b.start_ms});
  var minT=Infinity,maxT=-Infinity;
  sorted.forEach(function(t){if(t.start_ms<minT)minT=t.start_ms;if(t.end_ms>maxT)maxT=t.end_ms});
  var span=maxT-minT||1;
  var stages=[],smap={};
  sorted.forEach(function(t){if(!smap[t.name]){smap[t.name]=[];stages.push(t.name)}smap[t.name].push(t)});

  var html='<div class="legend">';
  html+='<div class="lg"><div class="ld" style="background:#7f77dd"></div>Dep wait: task waiting for upstream results</div>';
  html+='<div class="lg"><div class="ld" style="background:#ba7517"></div>Queue: waiting for a free worker</div>';
  html+='<div class="lg"><div class="ld" style="background:#999"></div>Overhead: scheduling/data transfer</div>';
  html+='<div class="lg"><div class="ld" style="background:#3a7bd5"></div>Compute: running function code</div>';
  html+='<div class="lg"><div class="ld" style="background:#993d3d"></div>Critical: on the critical path</div>';
  html+='</div>';

  stages.forEach(function(name){
    var tasks=smap[name];
    html+='<div class="stage-hd">'+name+' ('+tasks.length+')</div>';
    tasks.forEach(function(t){
      var isCP=t.on_cp;
      var rs=(t.start_ms-minT)/span*100;var ts=t.end_ms-t.start_ms||1;var tp=ts/span*100;
      var th='<b>'+t.label+'</b>'+(isCP?' critical':'')+(t.straggler?'<br>'+t.straggler.ratio+'x straggler':'')+
        '<br>exec: '+t.exec_ms.toFixed(1)+'ms<br>dep: '+t.dependency_ms.toFixed(1)+' queue: '+t.queue_ms.toFixed(1)+
        '<br>overhead: '+t.overhead_ms.toFixed(1)+' compute: '+t.compute_ms.toFixed(1);
      var bars='';var o=rs;
      var segs=[["#7f77dd",t.dependency_ms],["#ba7517",t.queue_ms],["#999",t.overhead_ms],[isCP?"#993d3d":"#3a7bd5",t.compute_ms]];
      segs.forEach(function(s){var w=s[1]/ts*tp;if(w>0.05)bars+='<div class="b" style="left:'+o+'%;width:'+w+'%;background:'+s[0]+'" onmouseover="st(event,\''+th.replace(/'/g,"\\&#39;")+'\')" onmouseleave="ht()"></div>';o+=w});
      var stragI=t.straggler?'<span class="stag-i">'+t.straggler.ratio+'x</span>':'';
      html+='<div class="row"><div class="rl'+(isCP?' cp':'')+'">'+t.label+stragI+'</div>';
      html+='<div class="track'+(isCP?' cp':'')+'">'+bars;
      if(tp>3)html+='<div class="tl-time">'+fms(t.exec_ms)+'</div>';
      html+='</div></div>';
    });
  });
  p.innerHTML=html;
})();

// dag
var dagDone=false;
function rDAG(){
  if(dagDone)return;dagDone=true;
  var p=document.getElementById("p-dag");
  var depth={};
  function gd(id){if(depth[id]!==undefined)return depth[id];var deps=(D[id]||[]).filter(function(d){return tm[d]});if(deps.length===0){depth[id]=0;return 0}depth[id]=1+Math.max.apply(null,deps.map(gd));return depth[id]}
  T.forEach(function(t){gd(t.id)});
  var layers={},maxD=0;T.forEach(function(t){var d=depth[t.id]||0;if(!layers[d])layers[d]=[];layers[d].push(t);if(d>maxD)maxD=d});
  var nw=106,nh=30,lg2=150,ng=6,pos={};
  for(var d=0;d<=maxD;d++){var nodes=layers[d]||[];var x=32+d*lg2;var totalH=nodes.length*(nh+ng);var sy=Math.max(16,(380-totalH)/2);nodes.forEach(function(t,i){pos[t.id]={x:x,y:sy+i*(nh+ng),w:nw,h:nh}})}
  var W=32+(maxD+1)*lg2+40;var maxY=0;Object.keys(pos).forEach(function(id){var p2=pos[id];if(p2.y+p2.h>maxY)maxY=p2.y+p2.h});var H=Math.max(maxY+32,300);

  var svg='<svg id="dag-svg" viewBox="0 0 '+W+' '+H+'" width="'+W+'" height="'+H+'" xmlns="http://www.w3.org/2000/svg">';
  svg+='<defs><marker id="ah" viewBox="0 -4 8 8" refX="8" refY="0" markerWidth="5" markerHeight="5" orient="auto"><path d="M0,-4L8,0L0,4" fill="#ccc"/></marker>';
  svg+='<marker id="ahc" viewBox="0 -4 8 8" refX="8" refY="0" markerWidth="5" markerHeight="5" orient="auto"><path d="M0,-4L8,0L0,4" fill="#993d3d"/></marker></defs>';
  E.forEach(function(e){var s=pos[e.source],t2=pos[e.target];if(!s||!t2)return;var x1=s.x+s.w,y1=s.y+s.h/2,x2=t2.x,y2=t2.y+t2.h/2;var cx1=x1+(x2-x1)*.5,cx2=x2-(x2-x1)*.5;var col=e.on_cp?"#993d3d":"#ddd";var sw=e.on_cp?2:1;var mk=e.on_cp?"url(#ahc)":"url(#ah)";svg+='<path d="M'+x1+','+y1+' C'+cx1+','+y1+' '+cx2+','+y2+' '+x2+','+y2+'" stroke="'+col+'" stroke-width="'+sw+'" fill="none" marker-end="'+mk+'" opacity="'+(e.on_cp?1:.35)+'"/>'});
  T.forEach(function(t){var pp=pos[t.id];if(!pp)return;var isCP=t.on_cp,isSt=!!t.straggler;var fill=isSt?"#fdf6e3":isCP?"#faf0f0":"#fff";var stroke=isSt?"#d4a03c":isCP?"#993d3d":"#ddd";var tc=isSt?"#996600":isCP?"#993d3d":"#888";svg+='<rect x="'+pp.x+'" y="'+pp.y+'" width="'+pp.w+'" height="'+pp.h+'" rx="4" fill="'+fill+'" stroke="'+stroke+'" stroke-width="'+(isCP?1.5:1)+'"/>';svg+='<text x="'+(pp.x+pp.w/2)+'" y="'+(pp.y+12)+'" text-anchor="middle" font-size="10" fill="'+tc+'" font-weight="'+(isCP?600:400)+'">'+(t.label.length>13?t.label.slice(0,12)+"\u2026":t.label)+'</text>';svg+='<text x="'+(pp.x+pp.w/2)+'" y="'+(pp.y+24)+'" text-anchor="middle" font-size="9" fill="#aaa">'+fms(t.exec_ms)+'</text>'});
  svg+='</svg>';

  var legendHtml='<div class="legend" style="margin-top:12px">';
  legendHtml+='<div class="lg"><div class="ld" style="background:#faf0f0;border:1.5px solid #993d3d"></div>Red border: on the critical path</div>';
  legendHtml+='<div class="lg"><div class="ld" style="background:#fdf6e3;border:1.5px solid #d4a03c"></div>Yellow border: straggler (abnormally slow)</div>';
  legendHtml+='<div class="lg"><div class="ld" style="background:#fff;border:1px solid #ddd"></div>Gray border: normal task</div>';
  legendHtml+='<div class="lg"><div style="width:20px;height:2px;background:#993d3d;border-radius:1px"></div>Red edge: critical path dependency</div>';
  legendHtml+='<div class="lg"><div style="width:20px;height:1px;background:#ddd"></div>Gray edge: non-critical dependency</div>';
  legendHtml+='</div>';

  p.innerHTML='<div class="sec-hd">Dependency graph</div><div class="hint">Tasks flow left to right. Each arrow means the downstream task cannot start until the upstream task finishes. The critical path (red) is the longest chain through this graph.</div>'+legendHtml+'<div class="dag-wrap">'+svg+'</div>';
}

// table
(function(){
  var sk="on_cp",sd=-1;
  function rn(){var rw=T.slice().sort(function(a,b){var va=a[sk],vb=b[sk];if(typeof va==="string"){va=va.toLowerCase();vb=vb.toLowerCase()}return sd*(va<vb?-1:va>vb?1:0)});
    document.getElementById("p-tbl").innerHTML='<table class="tbl"><thead><tr><th onclick="rs(\'label\')">Task</th><th onclick="rs(\'on_cp\')">Critical</th><th onclick="rs(\'exec_ms\')">Exec</th><th onclick="rs(\'dependency_ms\')">Dep wait</th><th onclick="rs(\'queue_ms\')">Queue</th><th onclick="rs(\'overhead_ms\')">Overhead</th><th onclick="rs(\'compute_ms\')">Compute</th><th>Split</th></tr></thead><tbody>'+rw.map(function(t){var tot=t.total_ms||t.exec_ms||1;return '<tr class="'+(t.on_cp?'cpr':'')+'"><td class="mono">'+t.label+(t.straggler?'<span class="stag-b">'+t.straggler.ratio+'x</span>':'')+'</td><td><span class="bcp '+(t.on_cp?'y':'n')+'">'+(t.on_cp?'yes':'no')+'</span></td><td class="mono">'+t.exec_ms.toFixed(1)+'</td><td class="mono">'+t.dependency_ms.toFixed(1)+'</td><td class="mono">'+t.queue_ms.toFixed(1)+'</td><td class="mono">'+t.overhead_ms.toFixed(1)+'</td><td class="mono">'+t.compute_ms.toFixed(1)+'</td><td><div class="mb"><div style="width:'+pct(t.dependency_ms,tot)+'%;background:#7f77dd"></div><div style="width:'+pct(t.queue_ms,tot)+'%;background:#ba7517"></div><div style="width:'+pct(t.overhead_ms,tot)+'%;background:#999"></div><div style="width:'+pct(t.compute_ms,tot)+'%;background:'+(t.on_cp?'#993d3d':'#3a7bd5')+'"></div></div></td></tr>'}).join('')+'</tbody></table>';}
  window.rs=function(k){if(sk===k)sd*=-1;else{sk=k;sd=-1}rn()};rn();
})();

// what-if
(function(){
  var p=document.getElementById("p-wif");
  var ct=CP.map(function(id){return tm[id]}).filter(Boolean);
  var h='<div class="sec-hd">What-if analysis</div>';
  h+='<div class="hint">If you speed up a task, does the job get faster? Or does the bottleneck just move somewhere else? Drag the sliders to simulate making each critical-path task faster and see the impact on total runtime.</div>';
  h+='<div class="hint">Original total: <b>'+fms(origCP.total)+'</b> ('+ct.map(function(t){return t.label}).join(' > ')+')</div>';
  h+='<div class="wg">';
  ct.forEach(function(t){
    h+='<div class="wc"><h3><span class="mono" style="color:#993d3d">'+t.label+'</span> <span style="color:#aaa;font-weight:400;font-size:11px">'+fms(t.exec_ms)+'</span>'+(t.straggler?'<span class="stag-b">straggler</span>':'')+'</h3><div class="wsr"><span>1x</span><input type="range" min="1" max="20" value="1" step="0.5" data-tid="'+t.id+'" data-orig="'+t.exec_ms+'"><span>20x</span><div class="wspd" id="ws-'+t.id+'">1x</div></div><div class="wres" id="wr-'+t.id+'">Drag to simulate</div></div>';
  });
  h+='</div>';p.innerHTML=h;
  p.querySelectorAll("input[type=range]").forEach(function(sl){
    sl.addEventListener("input",function(){
      var tid=sl.dataset.tid,orig=parseFloat(sl.dataset.orig),spd=parseFloat(sl.value),nm=orig/spd;
      document.getElementById("ws-"+tid).textContent=spd+"x";
      var mx={};mx[tid]=nm;var res=recomputeCP(mx);
      var saved=origCP.total-res.total;var shifted=JSON.stringify(res.path)!==JSON.stringify(CP);
      var r='<span class="mono">'+fms(nm)+'</span> total: <span class="mono">'+fms(res.total)+'</span><br>';
      r+=saved>0?'<span class="wsv">saves '+fms(saved)+'</span>':'no improvement';
      if(shifted)r+='<br><span class="wsh">bottleneck shifts: '+res.path.map(function(id){return tm[id]?tm[id].label:"?"}).join(" > ")+'</span>';
      document.getElementById("wr-"+tid).innerHTML=r;
    });
  });
})();
</script>
</body>
</html>
"""