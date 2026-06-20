/* Reference-range resolver — step-through "slice & dice" of how a marker's range resolves by
 * context (sex → age → life-stage → condition → medication → effective range). Reads
 * window.PURESCORE_RANGES; reuses window.PureCite for each step's citation. Dropdowns cascade with
 * the same compat rules as the appendix (male ≠ pregnant). Illustrative (README §5.6). */
(function () {
  "use strict";
  var R = window.PURESCORE_RANGES; if (!R || !R.markers) return;
  function esc(s){ return String(s == null ? "" : s).replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;"); }
  function val(id){ var e = document.getElementById(id); return e ? e.value : ""; }
  function chip(tok){ return tok ? '<span class="cite" data-src="' + esc(tok) + '" data-mk="" tabindex="0">' + esc(tok) + "</span>" : ""; }
  if (!document.getElementById("rr-css")) {
    var st = document.createElement("style"); st.id = "rr-css";
    st.textContent =
      ".rr-tool{display:flex;flex-wrap:wrap;gap:6px;align-items:center;margin:8px 0}" +
      ".rr-out{margin-top:10px}" +
      ".rr-step{display:flex;flex-wrap:wrap;align-items:center;gap:8px;padding:6px 10px;border-left:3px solid #243042;margin:3px 0;background:#0a1018;font-size:12.5px}" +
      ".rr-n{width:18px;height:18px;border-radius:50%;background:#16335e;color:#fff;font-size:10px;display:inline-flex;align-items:center;justify-content:center;flex:none}" +
      ".rr-lab{min-width:150px;color:#cdd9e8}" +
      ".rr-eff{margin-top:6px;padding:8px 11px;border:1px solid #2b5a86;border-radius:8px;background:#0c1726;font-weight:600;display:flex;flex-wrap:wrap;gap:8px;align-items:center}" +
      ".rr-out .muted{color:#6b7d92}";
    document.head.appendChild(st);
  }
  // populate marker + condition/med selects
  var mkSel = document.getElementById("rrMarker");
  if (mkSel) Object.keys(R.markers).forEach(function (n){ var o = document.createElement("option"); o.value = n; o.textContent = n; mkSel.appendChild(o); });
  (function populate(){
    var dis = (R._conditions || {}).diseases || {}, meds = (R._conditions || {}).meds || {};
    var c = document.getElementById("rrCond"), m = document.getElementById("rrMed");
    if (c) c.innerHTML = '<option value="">condition…</option>' + Object.keys(dis).map(function (x){ return '<option value="' + x + '">' + x + " · " + esc(dis[x].name) + "</option>"; }).join("");
    if (m) m.innerHTML = '<option value="">medication…</option>' + Object.keys(meds).map(function (x){ return '<option value="' + x + '">' + x + " · " + esc(meds[x].name) + "</option>"; }).join("");
  })();
  // cascade (same compat rules)
  function opt(id, fn){ var e = document.getElementById(id); if (e) [].forEach.call(e.options, fn); return e; }
  function cascade(){
    var cm = R._compat || {}, ls = cm.life_stage || {}, cc = cm.condition || {};
    var sex = val("rrSex"), age = val("rrAge"), life = val("rrLife");
    var lifeSel = opt("rrLife", function (o){ if (!o.value) return; var r = ls[o.value];
      o.disabled = !((!r || !r.sex || !sex || r.sex.indexOf(sex) >= 0) && (!r || !r.age || !age || r.age.indexOf(age) >= 0)); });
    if (lifeSel && lifeSel.value && lifeSel.options[lifeSel.selectedIndex].disabled) lifeSel.value = "";
    life = lifeSel ? lifeSel.value : life; var rule = life ? ls[life] : null;
    opt("rrSex", function (o){ if (!o.value) return; o.disabled = !!(rule && rule.sex && rule.sex.indexOf(o.value) < 0); });
    if (rule && rule.sex && rule.sex.length === 1){ var ss = document.getElementById("rrSex"); if (ss) ss.value = rule.sex[0]; sex = rule.sex[0]; }
    var ageSel = opt("rrAge", function (o){ if (!o.value) return; o.disabled = !!(rule && rule.age && rule.age.indexOf(o.value) < 0); });
    if (ageSel && ageSel.value && ageSel.options[ageSel.selectedIndex].disabled) ageSel.value = "";
    age = ageSel ? ageSel.value : age;
    opt("rrCond", function (o){ if (!o.value) return; var r = cc[o.value];
      o.disabled = !((!r || !r.sex || !sex || r.sex.indexOf(sex) >= 0) && (!r || !r.age || !age || r.age.indexOf(age) >= 0)); });
    var cs = document.getElementById("rrCond"); if (cs && cs.value && cs.options[cs.selectedIndex].disabled) cs.value = "";
  }
  function dimMatch(dim, v, c){ var k = (v.key || "").toLowerCase();
    if (dim === "sex") return !!c.sex && k.indexOf(c.sex) >= 0;
    if (dim === "age_band") return c.age === "older";
    if (dim === "life_stage"){ if (c.life === "pregnancy") return /pregnan/i.test(v.key); if (c.life === "postmenopause") return /meno/i.test(v.key); return false; }
    if (dim === "condition") return !!c.cond && v.key.indexOf(c.cond) >= 0;
    if (dim === "medication") return !!c.med && v.key.indexOf(c.med) >= 0;
    return false; }
  function band(o){ return [["g", o.g], ["y", o.y], ["r", o.r]].filter(function (x){ return x[1] && x[1] !== "—"; })
      .map(function (x){ return '<span class="rv-rng ' + x[0] + '">' + esc(x[1]) + "</span>"; }).join(" ") || '<span class="muted">(no numeric change — see note)</span>'; }
  function resolve(){
    var name = val("rrMarker"), M = R.markers[name]; var out = document.getElementById("rrOut"); if (!M || !out) return;
    var c = { sex: val("rrSex"), age: val("rrAge"), life: val("rrLife"), cond: val("rrCond"), med: val("rrMed") };
    var steps = [{ label: "base", o: M.base }], eff = M.base;
    ["sex", "age_band", "life_stage", "condition", "medication"].forEach(function (dim){
      var hit = null; (M.var || []).forEach(function (v){ if (v.dim === dim && dimMatch(dim, v, c)) hit = v; });
      if (hit){ steps.push({ label: dim + " · " + hit.key, o: hit }); if (hit.g || hit.y || hit.r) eff = hit; }
    });
    var intro = null; if (c.med && R.med_introduces){ for (var k in R.med_introduces){ if (k.indexOf(c.med) >= 0) intro = R.med_introduces[k]; } }
    var h = steps.map(function (s, i){ return '<div class="rr-step"><span class="rr-n">' + (i + 1) + '</span><span class="rr-lab">' + esc(s.label) + "</span> " + band(s.o) + (s.o.delta ? ' <span class="rv-delta">' + esc(s.o.delta) + "</span>" : "") + " " + chip(s.o.cite) + "</div>"; });
    h.push('<div class="rr-eff">EFFECTIVE → ' + band(eff) + " " + chip(eff.cite) + "</div>");
    if (intro) h.push('<div class="rr-eff" style="border-color:#7a5f24">' + esc(intro.label) + " → " + band(intro) + " " + chip(intro.cite) + "</div>");
    out.innerHTML = h.join(""); if (window.PureCite) window.PureCite.bind(out);
  }
  function init(){
    ["rrSex", "rrAge", "rrLife", "rrCond", "rrMed"].forEach(function (id){ var e = document.getElementById(id); if (e) e.addEventListener("change", cascade); });
    var run = document.getElementById("rrRun"); if (run) run.onclick = resolve;
    var mk = document.getElementById("rrMarker"); if (mk) mk.addEventListener("change", resolve);
    cascade(); resolve();
  }
  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", init); else init();
})();
