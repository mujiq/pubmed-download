/* SI-canonical unit layer (dual display + conversion). Reads window.PURESCORE_UNITS (generated from
 * data/units.json). Renders a conversions reference table into #unitTable and augments appendix/resolver
 * marker rows (tr[data-mk]) with their SI equivalent. Canonical = SI; conventional shown alongside.
 * Lp(a) is intentionally non-convertible. Illustrative; re-verify (README §5.6). */
(function () {
  "use strict";
  var U = (window.PURESCORE_UNITS || {}).markers || {};
  var keys = Object.keys(U); if (!keys.length) return;
  function esc(s){ return String(s == null ? "" : s).replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;"); }
  function norm(s){ return (s || "").toLowerCase().replace(/₂/g,"2").replace(/[^a-z0-9]/g,""); }

  // name → entry index (by short key, label, and a few aliases that differ from calc-graph labels)
  var ALIAS = { ldlc:"ldl", systolicbp:"sbp", egfrcreatinine:"egfr", egfr:"egfr", waistcircumference:"waist",
    triglycerides:"tg", "25ohvitamind":"vitd", vitamind:"vitd", vitd:"vitd", vitaminb12:"b12", b12:"b12",
    hscrp:"crp", ferritinironstores:"ferr", ferritin:"ferr", hemoglobin:"hb", lipoproteina:"lpa", lpa:"lpa",
    apolipoproteinb:"apob", apob:"apob", fastingglucose:"glu", glucose:"glu" };
  var IDX = {};
  keys.forEach(function (k){ IDX[norm(k)] = k; IDX[norm(U[k].label)] = k; });
  Object.keys(ALIAS).forEach(function (a){ if (U[ALIAS[a]]) IDX[a] = ALIAS[a]; });
  function lookup(name){ var n = norm(name); if (IDX[n]) return IDX[n];
    for (var a in ALIAS){ if (n === a || n.indexOf(a) === 0 || a.indexOf(n) === 0){ if (U[ALIAS[a]]) return ALIAS[a]; } }
    return IDX[n]; }

  function thFmt(th){ return th ? (th[0] + " / " + th[1]) : "—"; }

  if (!document.getElementById("u-css")) {
    var st = document.createElement("style"); st.id = "u-css";
    st.textContent =
      ".u-si{display:inline-block;margin-left:6px;font-size:10.5px;color:#7fd1c4;white-space:nowrap}" +
      ".u-si.nc{color:#e0a05a}" +
      ".u-panel{border:1px solid #1f6e63;border-radius:12px;background:#0b1614;margin:14px 0}" +
      ".u-h{padding:9px 13px;font-size:13px;font-weight:700;color:#9fe8da;border-bottom:1px solid #143028}" +
      ".u-h .mut{font-weight:400;color:#7fae9f;font-size:11.5px}" +
      ".u-tbl{width:100%;border-collapse:collapse;font-size:12px}" +
      ".u-tbl th,.u-tbl td{text-align:left;padding:6px 13px;border-bottom:1px solid #102420}" +
      ".u-tbl th{color:#7fae9f;font-weight:600;font-size:10.5px;text-transform:uppercase;letter-spacing:.04em}" +
      ".u-tbl .can{color:#9fe8da;font-weight:600}.u-tbl .mono{font-family:ui-monospace,Menlo,monospace;color:#bcd}" +
      ".u-tbl .nc{color:#e0a05a}.u-tbl .nt{color:#88a89f;font-size:11px}";
    document.head.appendChild(st);
  }

  // 1) conversions reference table into #unitTable
  var host = document.getElementById("unitTable");
  if (host) {
    var dual = keys.filter(function (k){ return U[k].dual; });
    var rows = dual.map(function (k){
      var e = U[k];
      var convCol = e.convertible
        ? (e.lin ? '<span class="mono">SI = ' + e.lin[0] + "·conv" + (e.lin[1] ? " " + (e.lin[1] < 0 ? "−" : "+") + " " + Math.abs(e.lin[1]) : "") + "</span>" : "1:1")
        : '<span class="nc">non-convertible</span>';
      var thCol = e.convertible && e.th_conv
        ? '<span class="mono">' + thFmt(e.th_conv) + " " + esc(e.conv.u) + " → " + thFmt(e.th_si) + " " + esc(e.si.u) + "</span>"
        : '<span class="nt">store native assay unit</span>';
      return "<tr><td><b>" + esc(e.label) + '</b></td><td>' + esc(e.conv.u) + ' <span class="nt">' + esc(e.conv.sys) + "</span></td>" +
        '<td class="can">' + esc(e.si.u) + ' <span class="nt">' + esc(e.si.sys) + "</span></td><td>" + convCol + "</td><td>" + thCol +
        '</td></tr><tr><td colspan="5" class="nt">' + esc(e.note) + "</td></tr>";
    }).join("");
    var panel = document.createElement("div"); panel.className = "u-panel";
    panel.innerHTML = '<div class="u-h">SI-canonical units &amp; conversion <span class="mut">canonical = SI (UK/ESC/Gulf); conventional (US) shown alongside · inbound feeds converted to SI before scoring · ' + dual.length + " dual-unit markers</span></div>" +
      '<table class="u-tbl"><thead><tr><th>Marker</th><th>Conventional</th><th>SI (canonical)</th><th>Convert</th><th>Bands (conv → SI)</th></tr></thead><tbody>' + rows + "</tbody></table>";
    host.appendChild(panel);
  }

  // 2) augment appendix/resolver marker rows with SI equivalent (best-effort name match)
  [].forEach.call(document.querySelectorAll("tr[data-mk]"), function (tr){
    var k = lookup(tr.getAttribute("data-mk")); if (!k) return; var e = U[k]; if (!e.dual) return;
    var tds = tr.querySelectorAll("td"); if (tds.length < 2) return;
    var cell = tds[1]; // unit column
    var span = document.createElement("span");
    if (!e.convertible){ span.className = "u-si nc"; span.textContent = "≢ " + e.si.u + " (store native)"; span.title = e.note; }
    else { span.className = "u-si"; span.textContent = "≈ " + e.si.u; span.title = e.note + (e.th_si ? "  ·  bands " + thFmt(e.th_si) + " " + e.si.u : ""); }
    cell.appendChild(span);
  });
})();
