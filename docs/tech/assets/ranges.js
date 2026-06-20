/* Range-variation views for appendix-biomarkers. Reads window.PURESCORE_RANGES (generated from
 * data/range-variations.json). Four switchable views over markers whose reference range shifts by
 * dimension: A hover/click card · B inline matrix · C stacked range-lanes (ruler) · D context selector
 * (pick sex/age/life-stage/condition/med → the table's ranges adapt with Δ + citation). Each variation
 * citation reuses window.PureCite (citations.js). Illustrative (README §5.6). */
(function () {
  "use strict";
  var R = window.PURESCORE_RANGES; if (!R || !R.markers) return;
  function esc(s){ return String(s == null ? "" : s).replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;"); }
  function val(id){ var e = document.getElementById(id); return e ? e.value : ""; }
  function reCite(el){ if (window.PureCite) window.PureCite.bind(el); }
  function chip(tok, mk){ return tok ? '<span class="cite" data-src="' + esc(tok) + '" data-mk="' + esc(mk) + '" tabindex="0">' + esc(tok) + '</span>' : ""; }
  var state = { view: "A" };

  if (!document.getElementById("rv-css")) {
    var st = document.createElement("style"); st.id = "rv-css";
    st.textContent =
      ".rv-switch{display:flex;align-items:center;gap:6px;flex-wrap:wrap;margin:14px 0 4px}" +
      ".rv-lab{font-size:11px;font-weight:700;color:var(--mut,#9bb0c5);text-transform:uppercase;letter-spacing:.04em;margin-right:4px}" +
      ".rv-vb{font:inherit;font-size:12px;padding:5px 11px;border-radius:8px;border:1px solid var(--line,#2a3340);background:var(--panel,#0c1320);color:inherit;cursor:pointer}" +
      ".rv-vb.active{background:#16335e;border-color:#3a6ea5;color:#fff}" +
      ".rv-hint{font-size:11px;color:var(--mut,#9bb0c5);margin-left:6px}" +
      ".rv-ctx{display:flex;gap:6px;flex-wrap:wrap;align-items:center;margin:6px 0 4px}" +
      ".rv-sel{font:inherit;font-size:12px;padding:4px 7px;border-radius:7px;border:1px solid var(--line,#2a3340);background:var(--panel,#0c1320);color:inherit}" +
      ".rv-btn{font:inherit;font-size:12px;padding:4px 10px;border-radius:7px;border:1px solid #2b5a86;background:#16335e;color:#fff;cursor:pointer}" +
      ".rv-name{cursor:pointer;border-bottom:1px dotted #3a6ea5}.rv-name.open{color:#fff}" +
      ".rv-badge{color:#9cc7f0;font-size:10px}" +
      "tr.rv-exp>td{background:#0a1018;padding:10px 14px!important}" +
      ".rv-card .rv-line{display:flex;flex-wrap:wrap;align-items:center;gap:6px;padding:3px 0;font-size:12.5px;border-bottom:1px solid #141d2b}" +
      ".rv-dim{font-size:9px;text-transform:uppercase;letter-spacing:.04em;color:#6b7d92;border:1px solid #2a3a4d;border-radius:4px;padding:0 5px}" +
      ".rv-rng{font-size:11px;padding:1px 6px;border-radius:5px;font-variant-numeric:tabular-nums}" +
      ".rv-rng.g{background:rgba(58,214,160,.14);color:#7de3bd}.rv-rng.y{background:rgba(237,193,74,.14);color:#edc14a}.rv-rng.r{background:rgba(240,96,110,.14);color:#f0606e}" +
      ".rv-delta{font-size:11px;color:#9cc7f0}" +
      ".rv-mx{border-collapse:collapse;font-size:12px;width:100%}.rv-mx th,.rv-mx td{border:1px solid #1c2636;padding:3px 8px;text-align:left}.rv-mx th{color:#9bb0c5;font-size:10px;text-transform:uppercase}" +
      ".rv-ruler{font-size:11px}.rv-lane{display:flex;align-items:center;gap:8px;margin:3px 0}.rv-lane-l{width:170px;flex:none;color:#cdd9e8}" +
      ".rv-track{flex:1;display:flex;border-radius:5px;overflow:hidden;border:1px solid #1c2636}" +
      ".rv-track i{padding:2px 6px;font-style:normal;font-size:10px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;flex:1}" +
      ".rv-track i.g{background:rgba(58,214,160,.18);color:#bff0d0}.rv-track i.y{background:rgba(237,193,74,.18);color:#f0dca0}.rv-track i.r{background:rgba(240,96,110,.18);color:#ffc2c9}" +
      ".rv-rnote{font-size:10px;color:#6b7d92;margin-top:4px}" +
      "tr.rv-shift{outline:1px solid #2b5a86;background:rgba(73,198,216,.05)}" +
      ".rv-applied{font-size:10px;color:#edc14a;border:1px solid #7a5f24;border-radius:5px;padding:0 5px;margin-left:4px}" +
      ".rv-sep{width:10px}.rv-sel:disabled{opacity:.4}.rv-sel option:disabled{color:#5b7790}" +
      ".rv-pillars{display:flex;flex-wrap:wrap;gap:5px;margin:4px 0 10px}" +
      ".rv-pill{font-size:10.5px;padding:2px 10px;border-radius:12px;border:1px solid #2a3a4d;background:#0c1320;color:#9bb0c5;cursor:pointer;user-select:none}" +
      ".rv-pill.on{background:#16335e;border-color:#3a6ea5;color:#fff}";
    document.head.appendChild(st);
  }

  function base(mk){ return (R.markers[mk] || {}).base || {}; }
  function vars(mk){ return (R.markers[mk] || {}).var || []; }

  function line(dim, key, g, y, r, delta, cite, mk){
    var label = dim === "base" ? "<b>base</b>" : ('<span class="rv-dim">' + esc(dim) + "</span> " + esc(key));
    var rng = [["g", g], ["y", y], ["r", r]].filter(function (x){ return x[1]; })
      .map(function (x){ return '<span class="rv-rng ' + x[0] + '">' + esc(x[1]) + "</span>"; }).join(" ");
    return '<div class="rv-line">' + label + " " + rng + (delta ? ' <span class="rv-delta">' + esc(delta) + "</span>" : "") + " " + chip(cite, mk) + "</div>";
  }
  function renderCard(mk){ var b = base(mk), h = ['<div class="rv-card">', line("base", "", b.g, b.y, b.r, "", b.cite, mk)];
    vars(mk).forEach(function (v){ h.push(line(v.dim, v.key, v.g, v.y, v.r, v.delta, v.cite, mk)); }); h.push("</div>"); return h.join(""); }
  function mxRow(dim, key, g, y, r, delta, cite, mk){
    var c = dim === "base" ? "<b>base</b>" : (esc(dim) + " · " + esc(key));
    return "<tr><td>" + c + "</td><td>" + esc(g || "—") + "</td><td>" + esc(y || "—") + "</td><td>" + esc(r || "—") + "</td><td>" + esc(delta || "") + "</td><td>" + chip(cite, mk) + "</td></tr>";
  }
  function renderMatrix(mk){ var b = base(mk), h = ['<table class="rv-mx"><thead><tr><th>context</th><th>green</th><th>yellow</th><th>red</th><th>Δ</th><th>citation</th></tr></thead><tbody>', mxRow("base", "", b.g, b.y, b.r, "", b.cite, mk)];
    vars(mk).forEach(function (v){ h.push(mxRow(v.dim, v.key, v.g, v.y, v.r, v.delta, v.cite, mk)); }); h.push("</tbody></table>"); return h.join(""); }
  function lane(label, o){ return '<div class="rv-lane"><span class="rv-lane-l">' + esc(label) + '</span><span class="rv-track"><i class="g">' + esc(o.g || "—") + '</i><i class="y">' + esc(o.y || "—") + '</i><i class="r">' + esc(o.r || "—") + "</i></span></div>"; }
  function renderRuler(mk){ var b = base(mk), lanes = [lane("base", b)];
    vars(mk).forEach(function (v){ if (v.g || v.y || v.r) lanes.push(lane(v.dim + " · " + v.key, v)); });
    return '<div class="rv-ruler">' + lanes.join("") + '<div class="rv-rnote">Each lane = that context’s green / yellow / red zones; compare lanes to see the shift from base (qualitative — bands are categorical, not a numeric axis).</div></div>'; }

  function rowEl(nameEl){ var n = nameEl; while (n && n.tagName !== "TR") n = n.parentNode; return n; }
  function toggle(nameEl){
    var mk = nameEl.getAttribute("data-rv"), tr = rowEl(nameEl); if (!tr) return;
    if (tr._rvexp){ tr._rvexp.remove(); tr._rvexp = null; nameEl.classList.remove("open"); return; }
    var exp = document.createElement("tr"); exp.className = "rv-exp";
    var td = document.createElement("td"); td.colSpan = tr.children.length;
    td.innerHTML = state.view === "B" ? renderMatrix(mk) : state.view === "C" ? renderRuler(mk) : renderCard(mk);
    exp.appendChild(td); tr.parentNode.insertBefore(exp, tr.nextSibling); tr._rvexp = exp; nameEl.classList.add("open"); reCite(td);
  }

  /* ---- view D: context selector ---- */
  function pickVar(vs, c){ var hit = null;
    vs.forEach(function (v){ var m = false; var k = (v.key || "").toLowerCase();
      if (v.dim === "sex" && c.sex && k.indexOf(c.sex) >= 0) m = true;
      if (v.dim === "age_band" && c.age === "older") m = true;
      if (v.dim === "life_stage" && c.life){ if (c.life === "pregnancy" && /pregnan/i.test(v.key)) m = true; if (c.life === "postmenopause" && /meno/i.test(v.key)) m = true; }
      if (v.dim === "condition" && c.cond && v.key.indexOf(c.cond) >= 0) m = true;
      if (v.dim === "medication" && c.med && v.key.indexOf(c.med) >= 0) m = true;
      if (m) hit = v;   // last (most specific) wins
    }); return hit; }
  function ctxApply(){
    var c = { sex: val("rvSex"), age: val("rvAge"), life: val("rvLife"), cond: val("rvCond"), med: val("rvMed") };
    [].forEach.call(document.querySelectorAll("tr[data-mk]"), function (tr){
      var mk = tr.getAttribute("data-mk"); if (!R.markers[mk]) return;
      if (tr._orig == null) tr._orig = tr.innerHTML;
      tr.innerHTML = tr._orig; tr.classList.remove("rv-shift");
      var v = pickVar(vars(mk), c);
      if (v){ var cells = tr.querySelectorAll("td .b-green, td .b-yellow, td .b-red");
        if (v.g != null && cells[0]) cells[0].innerHTML = esc(v.g);
        if (v.y != null && cells[1]) cells[1].innerHTML = esc(v.y);
        if (v.r != null && cells[2]) cells[2].innerHTML = esc(v.r);
        tr.classList.add("rv-shift");
        var last = tr.lastElementChild; last.innerHTML += ' <span class="rv-applied">' + esc(v.dim + " " + v.key) + (v.delta ? " · " + esc(v.delta) : "") + "</span> " + chip(v.cite, mk);
      }
      reCite(tr);
    });
    var out = document.getElementById("rvCtxOut"), parts = [];
    var lbl = [c.sex, c.age, c.life, c.cond, c.med].filter(Boolean).join(" · ") || "none";
    parts.push("context: " + esc(lbl));
    if (c.med && R.med_introduces){ for (var k in R.med_introduces){ if (k.indexOf(c.med) >= 0){ var mi = R.med_introduces[k]; parts.push("+ " + esc(mi.label) + ": " + esc(mi.g) + " " + chip(mi.cite, "INR")); } } }
    if (out){ out.innerHTML = parts.join(" &nbsp;·&nbsp; "); reCite(out); }
  }
  function ctxReset(){
    [].forEach.call(document.querySelectorAll("tr[data-mk]"), function (tr){ if (tr._orig != null){ tr.innerHTML = tr._orig; tr.classList.remove("rv-shift"); reCite(tr); } });
    ["rvSex", "rvAge", "rvLife", "rvCond", "rvMed"].forEach(function (id){ var e = document.getElementById(id); if (e) e.value = ""; });
    var o = document.getElementById("rvCtxOut"); if (o) o.innerHTML = "";
  }
  function populateCtx(){
    var cond = document.getElementById("rvCond"), med = document.getElementById("rvMed"); if (!cond || !med) return;
    var dis = (R._conditions || {}).diseases || {}, meds = (R._conditions || {}).meds || {};
    cond.innerHTML = '<option value="">condition…</option>' + Object.keys(dis).map(function (x){ return '<option value="' + x + '">' + x + " · " + esc(dis[x].name) + "</option>"; }).join("");
    med.innerHTML = '<option value="">medication…</option>' + Object.keys(meds).map(function (x){ return '<option value="' + x + '">' + x + " · " + esc(meds[x].name) + "</option>"; }).join("");
  }

  function clearExpansions(){
    [].forEach.call(document.querySelectorAll("tr.rv-exp"), function (e){ e.remove(); });
    [].forEach.call(document.querySelectorAll(".rv-name.open"), function (n){ n.classList.remove("open"); var tr = rowEl(n); if (tr) tr._rvexp = null; });
  }
  function setView(v){ state.view = v;
    [].forEach.call(document.querySelectorAll(".rv-vb"), function (b){ b.classList.toggle("active", b.getAttribute("data-view") === v); });
    var ctx = document.getElementById("rvCtx"); if (ctx) ctx.style.display = v === "D" ? "flex" : "none";
    var hint = document.getElementById("rvHint"); if (hint) hint.style.display = v === "D" ? "none" : "";
    clearExpansions();
    if (v !== "D") ctxReset();
  }
  /* ---- conditional dropdown cascade (compat rules + auto-resolve) ---- */
  function opt(sel, fn){ var e = document.getElementById(sel); if (e) [].forEach.call(e.options, fn); return e; }
  function cascade(){
    var cm = R._compat || {}, ls = cm.life_stage || {}, cc = cm.condition || {};
    var sex = val("rvSex"), age = val("rvAge"), life = val("rvLife");
    var lifeSel = opt("rvLife", function (o){ if (!o.value) return; var r = ls[o.value];
      var okS = !r || !r.sex || !sex || r.sex.indexOf(sex) >= 0, okA = !r || !r.age || !age || r.age.indexOf(age) >= 0; o.disabled = !(okS && okA); });
    if (lifeSel && lifeSel.value && lifeSel.options[lifeSel.selectedIndex].disabled) lifeSel.value = "";
    life = lifeSel ? lifeSel.value : life;
    var rule = life ? ls[life] : null;
    opt("rvSex", function (o){ if (!o.value) return; o.disabled = !!(rule && rule.sex && rule.sex.indexOf(o.value) < 0); });
    if (rule && rule.sex && rule.sex.length === 1){ var ss = document.getElementById("rvSex"); if (ss) ss.value = rule.sex[0]; sex = rule.sex[0]; }
    var ageSel = opt("rvAge", function (o){ if (!o.value) return; o.disabled = !!(rule && rule.age && rule.age.indexOf(o.value) < 0); });
    if (ageSel && ageSel.value && ageSel.options[ageSel.selectedIndex].disabled) ageSel.value = "";
    age = ageSel ? ageSel.value : age;
    opt("rvCond", function (o){ if (!o.value) return; var r = cc[o.value];
      var okS = !r || !r.sex || !sex || r.sex.indexOf(sex) >= 0, okA = !r || !r.age || !age || r.age.indexOf(age) >= 0; o.disabled = !(okS && okA); });
    var cs = document.getElementById("rvCond"); if (cs && cs.value && cs.options[cs.selectedIndex].disabled) cs.value = "";
  }
  /* ---- expand / collapse all ---- */
  function expandAll(){ [].forEach.call(document.querySelectorAll(".rv-name"), function (n){ var tr = rowEl(n); if (tr && !tr._rvexp) toggle(n); }); }
  /* ---- per-pillar filter ---- */
  function pillarSecs(){ return [].filter.call(document.querySelectorAll("h2[id]"), function (h){ return /^[a-z]{2,4}$/.test(h.getAttribute("id")); }); }
  function showPillars(list){ pillarSecs().forEach(function (h){ var show = !list || list.indexOf(h.getAttribute("id")) >= 0;
    h.style.display = show ? "" : "none"; var el = h.nextElementSibling;
    while (el && el.tagName !== "H2"){ el.style.display = show ? "" : "none"; el = el.nextElementSibling; } }); }
  function buildPillars(){ var host = document.getElementById("rvPillars"); if (!host) return;
    var secs = pillarSecs(); if (!secs.length) return;
    host.innerHTML = '<span class="rv-pill on" data-pf="all">all pillars</span>' +
      secs.map(function (h){ return '<span class="rv-pill on" data-pf="' + h.getAttribute("id") + '">' + esc(h.getAttribute("id").toUpperCase()) + "</span>"; }).join("");
    [].forEach.call(host.querySelectorAll(".rv-pill"), function (ch){ ch.onclick = function (){
      var pf = ch.getAttribute("data-pf");
      if (pf === "all"){ [].forEach.call(host.querySelectorAll(".rv-pill"), function (c){ c.classList.add("on"); }); showPillars(null); return; }
      ch.classList.toggle("on"); host.querySelector('.rv-pill[data-pf="all"]').classList.remove("on");
      var on = []; [].forEach.call(host.querySelectorAll(".rv-pill"), function (c){ var k = c.getAttribute("data-pf"); if (k !== "all" && c.classList.contains("on")) on.push(k); });
      if (!on.length){ [].forEach.call(host.querySelectorAll(".rv-pill"), function (c){ c.classList.add("on"); }); showPillars(null); }
      else showPillars(on);
    }; });
  }
  function init(){
    [].forEach.call(document.querySelectorAll(".rv-vb"), function (b){ b.onclick = function (){ setView(b.getAttribute("data-view")); }; });
    [].forEach.call(document.querySelectorAll(".rv-name"), function (n){
      n.onclick = function (){ if (state.view !== "D") toggle(n); };
      n.onkeydown = function (e){ if (e.key === "Enter" && state.view !== "D") toggle(n); };
    });
    populateCtx();
    ["rvSex", "rvAge", "rvLife", "rvCond", "rvMed"].forEach(function (id){ var e = document.getElementById(id); if (e) e.addEventListener("change", cascade); });
    cascade(); buildPillars();
    var a = document.getElementById("rvApply"); if (a) a.onclick = ctxApply;
    var r = document.getElementById("rvReset"); if (r) r.onclick = ctxReset;
    var ex = document.getElementById("rvExpand"); if (ex) ex.onclick = expandAll;
    var co = document.getElementById("rvCollapse"); if (co) co.onclick = function (){ clearExpansions(); };
  }
  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", init); else init();
})();
