/* Pillar-fan enhancer. On any rendered mermaid diagram whose nodes are the 12 pillar codes
 * (the "PureScore → 12 pillars" fan, e.g. Doc 02), each pillar box gets:
 *   • a rich hover card — full name, weight W_k, marker count, core markers, description
 *   • click → jump to that pillar's section on the current page (fallback: Appendix A · Markers)
 * Reads window.PURESCORE_PILLARS (generated from pillars.json + pillar-weights.json). No-ops if the
 * page has no pillar-fan. Illustrative; re-verify (README §5.6). */
(function () {
  "use strict";
  var P = window.PURESCORE_PILLARS || {};
  var CODES = Object.keys(P); if (!CODES.length) return;
  var SET = {}; CODES.forEach(function (c) { SET[c] = 1; });
  function esc(s) { return String(s == null ? "" : s).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;"); }

  if (!document.getElementById("pm-css")) {
    var st = document.createElement("style"); st.id = "pm-css";
    st.textContent =
      ".pm-node{cursor:pointer}.pm-node:hover [class*='label'],.pm-node:hover text{font-weight:700}" +
      ".pm-tip{position:absolute;z-index:9999;max-width:300px;background:#0d1422;border:1px solid #2b5a86;" +
      "border-radius:10px;padding:10px 12px;font-size:12px;color:#cfe0f5;box-shadow:0 12px 30px rgba(0,0,0,.55);pointer-events:none}" +
      ".pm-tip .h{font-weight:700;color:#fff;margin-bottom:3px}" +
      ".pm-tip .w{color:#7fd1c4;font-size:11.5px;margin-bottom:4px}" +
      ".pm-tip .d{color:#bcd;margin-bottom:5px}" +
      ".pm-tip .c{color:#9cc7f0;font-size:11px}.pm-tip .c b{color:#cfe0f5}" +
      ".pm-tip .go{margin-top:6px;color:#8aa0b6;font-size:10.5px;border-top:1px solid #1c2a3d;padding-top:5px}";
    document.head.appendChild(st);
  }

  var tip = null;
  function hideTip() { if (tip) { tip.remove(); tip = null; } }
  function showTip(node, code) {
    var p = P[code]; if (!p) return;
    hideTip();
    tip = document.createElement("div"); tip.className = "pm-tip";
    var w = (p.weight != null) ? (Math.round(p.weight * 1000) / 10) + "% of PureScore" : "weight uncited";
    var core = (p.core && p.core.length) ? '<div class="c"><b>Covers:</b> ' + esc(p.core.join(", ")) + (p.n > p.core.length ? " +" + (p.n - p.core.length) + " more" : "") + "</div>" : "";
    tip.innerHTML = '<div class="h">' + esc(code) + " · " + esc(p.name) + "</div>" +
      '<div class="w">' + esc(w) + " · " + p.n + " markers</div>" +
      (p.desc ? '<div class="d">' + esc(p.desc) + "</div>" : "") + core +
      '<div class="go">click → jump to this pillar’s section</div>';
    document.body.appendChild(tip);
    var r = node.getBoundingClientRect(), sx = window.scrollX || 0, sy = window.scrollY || 0;
    var left = r.left + sx + r.width / 2 - tip.offsetWidth / 2;
    left = Math.max(8, Math.min(left, sx + document.documentElement.clientWidth - tip.offsetWidth - 8));
    var top = r.top + sy - tip.offsetHeight - 8;
    if (top < sy + 4) top = r.bottom + sy + 8;            // flip below if no room above
    tip.style.left = left + "px"; tip.style.top = top + "px";
  }

  // same-page anchor for a pillar: a heading whose text contains the code as a token, or the pillar name
  function anchorFor(code) {
    var name = (P[code] && P[code].name) || "";
    var hs = document.querySelectorAll("h1[id],h2[id],h3[id]");
    var re = new RegExp("(^|[^A-Za-z])" + code + "([^A-Za-z]|$)");
    for (var i = 0; i < hs.length; i++) {
      var t = hs[i].textContent || "";
      if (re.test(t) || (name && t.indexOf(name) >= 0)) return hs[i].id;
    }
    return null;
  }
  function go(code) {
    var a = anchorFor(code);
    if (a) { var el = document.getElementById(a); if (el) { el.scrollIntoView({ behavior: "smooth", block: "start" }); try { history.replaceState(null, "", "#" + a); } catch (e) {} return; } }
    window.location.href = "appendix-biomarkers.html#" + encodeURIComponent(code);
  }

  function wire() {
    var labels = document.querySelectorAll(".mermaid .nodeLabel, .mermaid span.nodeLabel, .mermaid g.node text, .mermaid .label");
    var found = 0;
    [].forEach.call(labels, function (el) {
      var txt = (el.textContent || "").trim();
      if (!SET[txt]) return;
      var node = el.closest ? (el.closest("g.node") || el.closest(".node")) : null;
      if (!node || node.getAttribute("data-pm")) return;
      node.setAttribute("data-pm", "1"); node.classList.add("pm-node"); found++;
      node.setAttribute("tabindex", "0"); node.setAttribute("role", "link");
      node.setAttribute("aria-label", txt + " · " + ((P[txt] && P[txt].name) || "") + " — open section");
      node.addEventListener("mouseenter", function () { showTip(node, txt); });
      node.addEventListener("mouseleave", hideTip);
      node.addEventListener("click", function (e) { e.preventDefault(); hideTip(); go(txt); });
      node.addEventListener("keydown", function (e) { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); go(txt); } });
    });
    return found;
  }

  // mermaid renders async (startOnLoad) — poll until the pillar nodes exist, then stop.
  var tries = 0;
  (function wait() { if (wire() > 0 || tries++ > 160) return; setTimeout(wait, 50); })();
  window.addEventListener("scroll", hideTip, { passive: true });
})();
