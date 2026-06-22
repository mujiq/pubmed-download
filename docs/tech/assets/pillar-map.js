/* Pillar-fan enhancer. On any rendered mermaid pillar-fan (the "PureScore → 12 pillars" diagram,
 * e.g. Doc 02), each pillar box gets an INTERACTIVE popover (hover opens; stays open while the mouse
 * is inside it) showing the pillar's full input coverage grouped by channel — biomarkers, wearables,
 * self-report/PROs — with every item deep-linked to its Appendix A row, plus the intake-question
 * count linking to the question bank. Clicking the pillar box jumps to that pillar's section.
 * Reads window.PURESCORE_PILLARS. No-ops if the page has no pillar-fan. Illustrative (README §5.6). */
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
      ".pm-pop{position:absolute;z-index:9999;width:320px;max-height:72vh;overflow:auto;background:#0d1422;" +
      "border:1px solid #2b5a86;border-radius:11px;padding:11px 13px;font-size:12px;color:#cfe0f5;" +
      "box-shadow:0 14px 36px rgba(0,0,0,.6)}" +
      ".pm-pop .h{font-weight:700;color:#fff;font-size:13px}" +
      ".pm-pop .w{color:#7fd1c4;font-size:11.5px;margin:1px 0 4px}" +
      ".pm-pop .d{color:#9fb3c8;margin-bottom:7px;font-size:11.5px}" +
      ".pm-g{margin:7px 0 0;border-top:1px solid #16263a;padding-top:5px}" +
      ".pm-gt{font-size:10px;text-transform:uppercase;letter-spacing:.05em;color:#8aa6c0;margin-bottom:3px}" +
      ".pm-gt b{color:#cfe0f5}" +
      ".pm-gi{display:flex;flex-wrap:wrap;gap:4px}" +
      ".pm-none{color:#5d7088;font-size:11px}" +
      ".pm-i{display:inline-flex;align-items:center;gap:3px;background:#10202f;border:1px solid #21384d;" +
      "border-radius:6px;padding:2px 6px;font-size:11px;color:#dbe7f3;text-decoration:none}" +
      ".pm-i:hover{border-color:#2ee6c9;color:#fff}" +
      ".pm-ch{font-size:8.5px;text-transform:uppercase;letter-spacing:.03em;opacity:.8;border-radius:4px;padding:0 3px}" +
      ".pm-ch-bio{background:#10241c;color:#7fd1a8}.pm-ch-wear{background:#0c1c2b;color:#7fbfe6}.pm-ch-pro{background:#241f0c;color:#e6c97f}" +
      ".pm-crit{color:#f0606e}" +
      ".pm-q{display:block;margin-top:8px;color:#9cc7f0;font-size:11.5px;text-decoration:none}.pm-q:hover{color:#fff}" +
      ".pm-go{margin-top:8px;color:#8aa0b6;font-size:10.5px;border-top:1px solid #16263a;padding-top:5px}";
    document.head.appendChild(st);
  }

  var pop = null, node = null, hideT = null;
  function close() { if (pop) { pop.remove(); pop = null; node = null; } }
  function scheduleHide() { cancelHide(); hideT = setTimeout(close, 220); }
  function cancelHide() { if (hideT) { clearTimeout(hideT); hideT = null; } }

  function grp(title, items, kind) {
    var n = items ? items.length : 0;
    var body = n ? items.map(function (it) {
      var chip = it.ch ? '<span class="pm-ch pm-ch-' + kind + '">' + esc(it.ch) + "</span>" : "";
      var crit = it.crit ? '<span class="pm-crit" title="critical marker">★</span>' : "";
      return '<a class="pm-i" href="appendix-biomarkers.html#' + encodeURIComponent(it.a) + '">' + esc(it.n) + crit + chip + "</a>";
    }).join("") : '<span class="pm-none">none in this pillar</span>';
    return '<div class="pm-g"><div class="pm-gt">' + title + " <b>" + n + "</b></div><div class=\"pm-gi\">" + body + "</div></div>";
  }

  function open(target, code) {
    var p = P[code]; if (!p) return;
    close();
    node = target;
    pop = document.createElement("div"); pop.className = "pm-pop";
    var w = (p.weight != null) ? (Math.round(p.weight * 1000) / 10) + "% of PureScore" : "weight uncited";
    var q = (p.q && p.q.n) ? '<a class="pm-q" href="appendix-question-bank.html">❓ ' + p.q.n + " " + esc(p.q.label) + " intake questions &rarr;</a>" : "";
    pop.innerHTML = '<div class="h">' + esc(code) + " · " + esc(p.name) + "</div>" +
      '<div class="w">' + esc(w) + " · " + p.n + " markers</div>" +
      (p.desc ? '<div class="d">' + esc(p.desc) + "</div>" : "") +
      grp("🧪 Biomarkers", p.bio, "bio") +
      grp("⌚ Wearables", p.wear, "wear") +
      grp("📋 Self-report / PROs", p.pro, "pro") + q +
      '<div class="pm-go">click the pillar box → jump to its full section</div>';
    document.body.appendChild(pop);
    pop.addEventListener("mouseenter", cancelHide);
    pop.addEventListener("mouseleave", scheduleHide);
    // position: above the node, flip below if no room, clamp to viewport
    var r = target.getBoundingClientRect(), sx = window.scrollX || 0, sy = window.scrollY || 0;
    var left = r.left + sx + r.width / 2 - pop.offsetWidth / 2;
    left = Math.max(8, Math.min(left, sx + document.documentElement.clientWidth - pop.offsetWidth - 8));
    var top = r.top + sy - pop.offsetHeight - 9;
    if (top < sy + 6) top = r.bottom + sy + 9;
    pop.style.left = left + "px"; pop.style.top = top + "px";
  }

  // same-page anchor for a pillar (a heading whose text contains the code as a token, or the name)
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
      var nd = el.closest ? (el.closest("g.node") || el.closest(".node")) : null;
      if (!nd || nd.getAttribute("data-pm")) return;
      nd.setAttribute("data-pm", "1"); nd.classList.add("pm-node"); found++;
      nd.setAttribute("tabindex", "0"); nd.setAttribute("role", "link");
      nd.setAttribute("aria-label", txt + " · " + ((P[txt] && P[txt].name) || "") + " — open section");
      nd.addEventListener("mouseenter", function () { cancelHide(); open(nd, txt); });
      nd.addEventListener("mouseleave", scheduleHide);
      nd.addEventListener("click", function (e) { e.preventDefault(); close(); go(txt); });
      nd.addEventListener("keydown", function (e) {
        if (e.key === "Enter" || e.key === " ") { e.preventDefault(); go(txt); }
        else if (e.key === "Escape") close();
      });
    });
    return found;
  }

  // mermaid renders async (startOnLoad) — poll until the pillar nodes exist, then stop.
  var tries = 0;
  (function wait() { if (wire() > 0 || tries++ > 160) return; setTimeout(wait, 50); })();
  document.addEventListener("keydown", function (e) { if (e.key === "Escape") close(); });
  document.addEventListener("click", function (e) { if (pop && !pop.contains(e.target) && (!node || !node.contains(e.target))) close(); });
})();
