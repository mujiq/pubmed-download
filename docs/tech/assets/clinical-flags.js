/* Inline clinical-audit ⚠ badges (read-only). Reads window.PURESCORE_FLAGS (generated from
 * data/clinical-flags.json — output of the adversarial multi-agent audit). Marker-keyed flags
 * (range:/rangevar:/critical:) badge their appendix row; ALL flags also appear in a collapsible
 * inline panel grouped by area. No values are changed — these are 'needs verification' markers.
 * Illustrative; NOT a clinical sign-off (README §5.6). */
(function () {
  "use strict";
  var FD = window.PURESCORE_FLAGS || {}, F = FD.flags || {}, keys = Object.keys(F);
  if (!keys.length) return;
  function esc(s){ return String(s == null ? "" : s).replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;"); }
  function sevNorm(s){ return s === "medium" ? "med" : (s || "med"); }
  function sevColor(s){ s = sevNorm(s); return s === "high" ? "#f0606e" : s === "low" ? "#9bb0c5" : "#edc14a"; }
  var AREA = { ranges_base: "Base ranges", ranges_var: "Range variations", pillar_weights: "Pillar weights",
    reservoir_weights: "Reservoir weights", self_report: "Self-report inputs", constants_critical: "Constants & critical set" };
  if (!document.getElementById("cf-css")) {
    var st = document.createElement("style"); st.id = "cf-css";
    st.textContent =
      ".cf-badge{display:inline-block;margin-left:5px;cursor:pointer;font-size:11px;line-height:1;vertical-align:middle}" +
      ".cf-pop{position:absolute;z-index:9999;max-width:380px;background:#0d1422;border:1px solid #7a3344;border-radius:10px;padding:11px 13px;font-size:12px;color:#cdd9e8;box-shadow:0 12px 30px rgba(0,0,0,.55)}" +
      ".cf-pop h4{margin:0 0 5px;font-size:12px;color:#ffb9c6}.cf-pop .x{float:right;cursor:pointer;color:#9bb0c5;font-size:16px;margin:-3px -2px 0 6px}" +
      ".cf-pop .it{margin:7px 0;border-top:1px solid #241019;padding-top:6px}.cf-pop .it:first-of-type{border-top:0}" +
      ".cf-pop .rs{color:#e8c7cf}.cf-pop .ac{color:#9cc7f0;margin-top:3px}.cf-pop .dot{display:inline-block;width:8px;height:8px;border-radius:50%;margin-right:5px;vertical-align:middle}" +
      ".cf-panel{border:1px solid #7a3344;border-radius:12px;background:#160c10;margin:12px 0}" +
      ".cf-h{display:flex;align-items:center;gap:8px;flex-wrap:wrap;padding:9px 13px;cursor:pointer;font-size:13px;color:#ffd0d6;font-weight:700}" +
      ".cf-h .mut{color:#c79aa3;font-weight:400;font-size:11.5px}.cf-h .sp{flex:1}" +
      ".cf-b{display:none;padding:2px 13px 12px;border-top:1px solid #2a1118}.cf-panel.open .cf-b{display:block}" +
      ".cf-grp{font-size:10px;text-transform:uppercase;letter-spacing:.05em;color:#c79aa3;margin:10px 0 3px}" +
      ".cf-row{padding:5px 0;border-bottom:1px solid #1f0f14;font-size:12px}" +
      ".cf-row .lab{font-weight:600;color:#f1dbe0}.cf-row .key{font-family:ui-monospace,Menlo,monospace;font-size:10px;color:#8a6b72;margin-left:5px}" +
      ".cf-row .rs{color:#cdb6bc;margin-top:2px}.cf-row .ac{color:#9cc7f0;margin-top:2px}";
    document.head.appendChild(st);
  }
  function mkName(k){ var p = k.split(":"), t = p[0]; return (t === "range" || t === "rangevar" || t === "critical") ? p[1] : null; }
  function flagList(ks){ return ks.map(function (k){ var f = F[k]; return { k: k, f: f, sev: sevNorm(f.severity) }; })
      .sort(function (a, b){ var o = { high: 0, med: 1, low: 2 }; return o[a.sev] - o[b.sev]; }); }
  function worst(ks){ var s = "low"; ks.forEach(function (k){ var v = sevNorm(F[k].severity); if (v === "high") s = "high"; else if (v === "med" && s !== "high") s = "med"; }); return s; }

  var pop = null; function closePop(){ if (pop){ pop.remove(); pop = null; } }
  function showPop(anchor, ks){
    closePop(); pop = document.createElement("div"); pop.className = "cf-pop";
    pop.innerHTML = '<span class="x">&times;</span><h4>⚠ ' + ks.length + ' audit flag' + (ks.length > 1 ? "s" : "") + ' — verify</h4>' +
      flagList(ks).map(function (o){ var f = o.f; return '<div class="it"><span class="dot" style="background:' + sevColor(o.sev) + '"></span><b>' + esc(f.item || o.k) + '</b>' +
        '<div class="rs">' + esc((f.reasons || []).join(" · ")) + "</div>" +
        ((f.actions && f.actions.length) ? '<div class="ac">→ ' + esc(f.actions.join(" · ")) + "</div>" : "") + "</div>"; }).join("");
    document.body.appendChild(pop);
    var r = anchor.getBoundingClientRect(), sx = window.scrollX || 0, sy = window.scrollY || 0;
    var left = r.left + sx; if (left + pop.offsetWidth > sx + document.documentElement.clientWidth - 12) left = sx + document.documentElement.clientWidth - pop.offsetWidth - 12;
    pop.style.left = Math.max(8, left) + "px"; pop.style.top = (r.bottom + sy + 6) + "px";
    pop.querySelector(".x").onclick = closePop; pop.addEventListener("click", function (e){ e.stopPropagation(); });
  }
  function badge(ks){ var b = document.createElement("span"); b.className = "cf-badge"; b.textContent = "⚠"; b.style.color = sevColor(worst(ks));
    b.title = ks.length + " clinical-audit flag(s) — click to verify"; b.onclick = function (e){ e.stopPropagation(); showPop(b, ks); }; return b; }

  // 1) badge marker rows (normalised + alias matching: SBP→Systolic BP, Hb→Hemoglobin, …)
  function norm(s){ return (s || "").toLowerCase().replace(/₂/g, "2").replace(/[^a-z0-9]/g, ""); }
  var ALIAS = { sbp: "systolicbp", dbp: "diastolicbp", hb: "hemoglobin", ldl: "ldlc", egfr: "egfrcreatinine",
    vo2: "vo2maxest", vo2max: "vo2maxest", spo2: "spo2", phq: "phq9depression", phq9: "phq9depression",
    gad: "gad7anxiety", almi: "almileanmassindex", waist: "waistcircumference", ferritin: "ferritinironstores",
    crp: "hscrp", b12: "vitaminb12", vitd: "25ohvitamind", vitamind: "25ohvitamind", fib4: "fib4index",
    tg: "triglycerides", k: "potassium", tscore: "bmdtscoredexa", bmdtscore: "bmdtscoredexa", sleepduration: "sleepduration14daymean" };
  function canon(n){ var c = norm(n); return ALIAS[c] || c; }
  var byMk = {}, placed = {};
  keys.forEach(function (k){ var n = mkName(k); if (n){ var c = canon(n); (byMk[c] = byMk[c] || []).push(k); } });
  [].forEach.call(document.querySelectorAll("tr[data-mk]"), function (tr){
    var rn = norm(tr.getAttribute("data-mk")), ks = [];
    for (var c in byMk){ if (c && (c === rn || rn.indexOf(c) === 0 || c.indexOf(rn) === 0)) ks = ks.concat(byMk[c]); }
    if (!ks.length) return; ks.forEach(function (k){ placed[k] = 1; });
    var cell = tr.querySelector("td"); if (cell) cell.appendChild(badge(ks));
  });

  // 2) inline panel — every flag, grouped by area (collapsible)
  var host = document.getElementById("cfFlags"); if (!host) return;
  var hi = keys.filter(function (k){ return sevNorm(F[k].severity) === "high"; }).length;
  var byArea = {}; keys.forEach(function (k){ var a = F[k].area || "other"; (byArea[a] = byArea[a] || []).push(k); });
  var body = Object.keys(byArea).sort().map(function (a){
    return '<div class="cf-grp">' + esc(AREA[a] || a) + " · " + byArea[a].length + "</div>" +
      flagList(byArea[a]).map(function (o){ var f = o.f; return '<div class="cf-row"><span class="dot" style="display:inline-block;width:8px;height:8px;border-radius:50%;background:' + sevColor(o.sev) + ';margin-right:6px"></span>' +
        '<span class="lab">' + esc(f.item || o.k) + '</span><span class="key">' + esc(o.k) + (placed[o.k] ? " · badged" : "") + '</span>' +
        '<div class="rs">' + esc((f.reasons || []).join(" · ")) + "</div>" +
        ((f.actions && f.actions.length) ? '<div class="ac">→ ' + esc(f.actions.join(" · ")) + "</div>" : "") + "</div>"; }).join("");
  }).join("");
  var panel = document.createElement("div"); panel.className = "cf-panel";
  panel.innerHTML = '<div class="cf-h" id="cfH">⚠ Clinical-validity audit <span class="mut">' + keys.length + " flags · " + hi + " high-severity · adversarial multi-agent · <b>read-only, not a sign-off</b></span><span class=\"sp\"></span><span class=\"mut\" id=\"cfTog\">show ▾</span></div><div class=\"cf-b\">" + body + "</div>";
  host.appendChild(panel);
  var hdr = document.getElementById("cfH"); hdr.onclick = function (){ panel.classList.toggle("open"); document.getElementById("cfTog").textContent = panel.classList.contains("open") ? "hide ▴" : "show ▾"; };
  document.addEventListener("click", closePop);
  document.addEventListener("keydown", function (e){ if (e.key === "Escape") closePop(); });
})();
