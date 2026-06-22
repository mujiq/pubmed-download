/* Inline clinical-audit ⚠ badges + remediation tracker (schema v2). Reads window.PURESCORE_FLAGS
 * (generated from data/clinical-flags.json — adversarial multi-agent audit). Marker-keyed flags
 * (range:/rangevar:/critical:) badge their appendix row; ALL flags appear in a collapsible panel
 * grouped by area, with a status filter + remediation progress. Each flag carries a remediation
 * status (open/proposed/fixed/accepted/deferred), a governing_standard, a resolution note, and a
 * clinician verdict. Illustrative; NOT a clinical sign-off (README §5.6). */
(function () {
  "use strict";
  var FD = window.PURESCORE_FLAGS || {}, F = FD.flags || {}, keys = Object.keys(F);
  if (!keys.length) return;
  function esc(s){ return String(s == null ? "" : s).replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;"); }

  // ---- severity (high/medium/low; tolerate legacy 'med') ----
  function sevNorm(s){ return s === "med" ? "medium" : (s || "medium"); }
  function sevColor(s){ s = sevNorm(s); return s === "high" ? "#f0606e" : s === "low" ? "#9bb0c5" : "#edc14a"; }
  var SEVO = { high: 0, medium: 1, low: 2 };

  // ---- remediation status ----
  function stNorm(s){ return s || "open"; }
  var ST = {
    open:     { lab: "open",     col: "#f0a35a", icon: "○" },
    proposed: { lab: "proposed", col: "#5aa0f0", icon: "◐" },
    fixed:    { lab: "fixed",    col: "#3ec98a", icon: "●" },
    accepted: { lab: "accepted", col: "#9bb0c5", icon: "✓" },
    deferred: { lab: "deferred", col: "#7a6b8a", icon: "»" },
  };
  var STORDER = ["open", "proposed", "fixed", "accepted", "deferred"];
  function stMeta(s){ return ST[stNorm(s)] || ST.open; }
  function resolved(s){ s = stNorm(s); return s === "fixed" || s === "accepted"; }

  var AREA = { ranges_base: "Base ranges", ranges_var: "Range variations", pillar_weights: "Pillar weights",
    reservoir_weights: "Reservoir weights", self_report: "Self-report inputs", constants_critical: "Constants & critical set" };

  if (!document.getElementById("cf-css")) {
    var st = document.createElement("style"); st.id = "cf-css";
    st.textContent =
      ".cf-badge{display:inline-block;margin-left:5px;cursor:pointer;font-size:11px;line-height:1;vertical-align:middle}" +
      ".cf-pop{position:absolute;z-index:9999;max-width:400px;background:#0d1422;border:1px solid #7a3344;border-radius:10px;padding:11px 13px;font-size:12px;color:#cdd9e8;box-shadow:0 12px 30px rgba(0,0,0,.55)}" +
      ".cf-pop h4{margin:0 0 5px;font-size:12px;color:#ffb9c6}.cf-pop .x{float:right;cursor:pointer;color:#9bb0c5;font-size:16px;margin:-3px -2px 0 6px}" +
      ".cf-pop .it{margin:7px 0;border-top:1px solid #241019;padding-top:6px}.cf-pop .it:first-of-type{border-top:0}" +
      ".cf-pop .rs{color:#e8c7cf}.cf-pop .ac{color:#9cc7f0;margin-top:3px}.cf-pop .dot{display:inline-block;width:8px;height:8px;border-radius:50%;margin-right:5px;vertical-align:middle}" +
      ".cf-panel{border:1px solid #7a3344;border-radius:12px;background:#160c10;margin:12px 0}" +
      ".cf-h{display:flex;align-items:center;gap:8px;flex-wrap:wrap;padding:9px 13px;cursor:pointer;font-size:13px;color:#ffd0d6;font-weight:700}" +
      ".cf-h .mut{color:#c79aa3;font-weight:400;font-size:11.5px}.cf-h .sp{flex:1}" +
      ".cf-b{display:none;padding:2px 13px 12px;border-top:1px solid #2a1118}.cf-panel.open .cf-b{display:block}" +
      ".cf-prog{display:flex;height:7px;border-radius:5px;overflow:hidden;margin:10px 0 6px;border:1px solid #2a1118}" +
      ".cf-prog span{display:block}" +
      ".cf-filters{display:flex;flex-wrap:wrap;gap:6px;margin:8px 0 4px}" +
      ".cf-fb{font-size:11px;padding:3px 9px;border-radius:20px;border:1px solid #3a2530;background:#1c0f15;color:#c79aa3;cursor:pointer}" +
      ".cf-fb.on{background:#2a1118;border-color:#7a3344;color:#ffd0d6}.cf-fb .c{opacity:.7;margin-left:4px}" +
      ".cf-grp{font-size:10px;text-transform:uppercase;letter-spacing:.05em;color:#c79aa3;margin:12px 0 3px}" +
      ".cf-row{padding:5px 0;border-bottom:1px solid #1f0f14;font-size:12px}.cf-row.hide{display:none}" +
      ".cf-row .lab{font-weight:600;color:#f1dbe0}.cf-row .key{font-family:ui-monospace,Menlo,monospace;font-size:10px;color:#8a6b72;margin-left:5px}" +
      ".cf-st{display:inline-block;font-size:10px;padding:1px 7px;border-radius:20px;margin-left:6px;vertical-align:middle;font-weight:600}" +
      ".cf-gov{display:inline-block;font-size:10px;color:#9cc7f0;margin-left:6px}" +
      ".cf-row .rs{color:#cdb6bc;margin-top:2px}.cf-row .ac{color:#9cc7f0;margin-top:2px}" +
      ".cf-row .rz{color:#7fd1a8;margin-top:2px}.cf-row .vd{color:#c8a6e8;margin-top:2px;font-size:11px}";
    document.head.appendChild(st);
  }

  function stChip(s){ var m = stMeta(s); return '<span class="cf-st" style="background:' + m.col + '22;color:' + m.col + ';border:1px solid ' + m.col + '55">' + m.icon + " " + m.lab + "</span>"; }

  function mkName(k){ var p = k.split(":"), t = p[0]; return (t === "range" || t === "rangevar" || t === "critical") ? p[1] : null; }
  function flagList(ks){ return ks.map(function (k){ var f = F[k]; return { k: k, f: f, sev: sevNorm(f.severity) }; })
      .sort(function (a, b){ return SEVO[a.sev] - SEVO[b.sev]; }); }
  function worst(ks){ var s = "low"; ks.forEach(function (k){ var v = sevNorm(F[k].severity); if (v === "high") s = "high"; else if (v === "medium" && s !== "high") s = "medium"; }); return s; }
  function allResolved(ks){ return ks.every(function (k){ return resolved(F[k].status); }); }

  // ---- popover (marker badge) ----
  var pop = null; function closePop(){ if (pop){ pop.remove(); pop = null; } }
  function rowDetail(f){
    return (f.governing_standard ? '<div class="ac">standard: ' + esc(f.governing_standard) + "</div>" : "") +
      '<div class="rs">' + esc((f.reasons || []).join(" · ")) + "</div>" +
      ((f.actions && f.actions.length) ? '<div class="ac">→ ' + esc(f.actions.join(" · ")) + "</div>" : "") +
      (f.resolution ? '<div class="rz">✔ ' + esc(f.resolution) + "</div>" : "") +
      (f.verdict ? '<div class="vd">clinician: ' + esc(f.verdict) + "</div>" : "");
  }
  function showPop(anchor, ks){
    closePop(); pop = document.createElement("div"); pop.className = "cf-pop";
    pop.innerHTML = '<span class="x">&times;</span><h4>⚠ ' + ks.length + ' audit flag' + (ks.length > 1 ? "s" : "") + ' — verify</h4>' +
      flagList(ks).map(function (o){ var f = o.f; return '<div class="it"><span class="dot" style="background:' + sevColor(o.sev) + '"></span><b>' + esc(f.item || o.k) + '</b>' + stChip(f.status) + rowDetail(f) + "</div>"; }).join("");
    document.body.appendChild(pop);
    var r = anchor.getBoundingClientRect(), sx = window.scrollX || 0, sy = window.scrollY || 0;
    var left = r.left + sx; if (left + pop.offsetWidth > sx + document.documentElement.clientWidth - 12) left = sx + document.documentElement.clientWidth - pop.offsetWidth - 12;
    pop.style.left = Math.max(8, left) + "px"; pop.style.top = (r.bottom + sy + 6) + "px";
    pop.querySelector(".x").onclick = closePop; pop.addEventListener("click", function (e){ e.stopPropagation(); });
  }
  function badge(ks){ var b = document.createElement("span"); b.className = "cf-badge"; var done = allResolved(ks);
    b.textContent = done ? "✓" : "⚠"; b.style.color = done ? "#3ec98a" : sevColor(worst(ks));
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

  // 2) inline panel — every flag, grouped by area, with status filter + progress
  var host = document.getElementById("cfFlags"); if (!host) return;
  var hi = keys.filter(function (k){ return sevNorm(F[k].severity) === "high"; }).length;
  var stCount = {}; STORDER.forEach(function (s){ stCount[s] = 0; });
  keys.forEach(function (k){ stCount[stNorm(F[k].status)] = (stCount[stNorm(F[k].status)] || 0) + 1; });
  var doneN = stCount.fixed + stCount.accepted;

  var byArea = {}; keys.forEach(function (k){ var a = F[k].area || "other"; (byArea[a] = byArea[a] || []).push(k); });
  var body = Object.keys(byArea).sort().map(function (a){
    return '<div class="cf-grp">' + esc(AREA[a] || a) + " · " + byArea[a].length + "</div>" +
      flagList(byArea[a]).map(function (o){ var f = o.f; return '<div class="cf-row" data-st="' + stNorm(f.status) + '"><span class="dot" style="display:inline-block;width:8px;height:8px;border-radius:50%;background:' + sevColor(o.sev) + ';margin-right:6px"></span>' +
        '<span class="lab">' + esc(f.item || o.k) + '</span>' + stChip(f.status) +
        (f.governing_standard ? '<span class="cf-gov">▸ ' + esc(f.governing_standard) + "</span>" : "") +
        '<span class="key">' + esc(o.k) + (placed[o.k] ? " · badged" : "") + '</span>' +
        '<div class="rs">' + esc((f.reasons || []).join(" · ")) + "</div>" +
        ((f.actions && f.actions.length) ? '<div class="ac">→ ' + esc(f.actions.join(" · ")) + "</div>" : "") +
        (f.resolution ? '<div class="rz">✔ ' + esc(f.resolution) + "</div>" : "") +
        (f.verdict ? '<div class="vd">clinician: ' + esc(f.verdict) + "</div>" : "") + "</div>"; }).join("");
  }).join("");

  // progress bar segments
  var prog = STORDER.map(function (s){ var n = stCount[s]; if (!n) return ""; return '<span title="' + n + " " + s + '" style="width:' + (100 * n / keys.length) + "%;background:" + ST[s].col + '"></span>'; }).join("");
  // filter buttons
  var filters = '<button class="cf-fb on" data-f="all">all<span class="c">' + keys.length + "</span></button>" +
    STORDER.filter(function (s){ return stCount[s]; }).map(function (s){ return '<button class="cf-fb" data-f="' + s + '" style="color:' + ST[s].col + '">' + ST[s].lab + '<span class="c">' + stCount[s] + "</span></button>"; }).join("");

  var panel = document.createElement("div"); panel.className = "cf-panel";
  panel.innerHTML = '<div class="cf-h" id="cfH">⚠ Clinical-validity audit ' +
    '<span class="mut">' + keys.length + " flags · " + hi + " high · " +
    '<b style="color:#3ec98a">' + doneN + " resolved</b> · " + stCount.proposed + " proposed · " + stCount.open + " open · <b>not a sign-off</b></span>" +
    '<span class="sp"></span><span class="mut" id="cfTog">show ▾</span></div>' +
    '<div class="cf-b"><div class="cf-prog">' + prog + "</div>" +
    '<div class="cf-filters">' + filters + "</div>" + body + "</div>";
  host.appendChild(panel);

  var hdr = document.getElementById("cfH"); hdr.onclick = function (){ panel.classList.toggle("open"); document.getElementById("cfTog").textContent = panel.classList.contains("open") ? "hide ▴" : "show ▾"; };
  // status filter
  [].forEach.call(panel.querySelectorAll(".cf-fb"), function (b){
    b.onclick = function (e){ e.stopPropagation();
      [].forEach.call(panel.querySelectorAll(".cf-fb"), function (x){ x.classList.remove("on"); }); b.classList.add("on");
      var f = b.getAttribute("data-f");
      [].forEach.call(panel.querySelectorAll(".cf-row"), function (row){ row.classList.toggle("hide", f !== "all" && row.getAttribute("data-st") !== f); });
    };
  });
  document.addEventListener("click", closePop);
  document.addEventListener("keydown", function (e){ if (e.key === "Escape") closePop(); });
})();
