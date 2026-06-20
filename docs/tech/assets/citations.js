/* Citation popovers for marker reference ranges. A .cite chip (data-src token, data-mk marker)
 * resolves against window.PURESCORE_CITATIONS (generated from data/citations.json) and shows a
 * popover: full reference, the table/section where the range lives, DOI/PMID, a verified/document
 * badge, and an 'open source ↗' button that opens the document (at its section anchor) in a NEW TAB.
 * Generic tags → PubMed search / internal wiki link / muted method chip. Illustrative (README §5.6). */
(function () {
  "use strict";
  var C = window.PURESCORE_CITATIONS || {};
  if (!document.getElementById("cite-css")) {
    var st = document.createElement("style"); st.id = "cite-css";
    st.textContent =
      ".cite{color:#9cc7f0;border-bottom:1px dotted #3a6ea5;cursor:pointer;white-space:nowrap}" +
      ".cite:hover{color:#fff}.cite::after{content:' \\2197';font-size:9px;color:#6b7d92}" +
      ".cite.method{color:#9bb0c5;border-bottom:0;cursor:help}.cite.method::after{content:''}" +
      ".cite-pop{position:absolute;z-index:9999;max-width:350px;background:#0d1422;border:1px solid #2b5a86;border-radius:10px;padding:11px 13px;font-size:12.5px;color:#cdd9e8;box-shadow:0 12px 30px rgba(0,0,0,.55)}" +
      ".cite-pop h4{margin:0 0 5px;font-size:12px;color:#fff;line-height:1.35}" +
      ".cite-pop .sec{color:#9cc7f0;margin:5px 0}" +
      ".cite-pop .ids{display:flex;flex-wrap:wrap;gap:5px;margin:7px 0;align-items:center}" +
      ".cite-pop .id{font-size:10px;padding:1px 7px;border-radius:5px;border:1px solid #2a3a4d;color:#cfe0f5;text-decoration:none}" +
      ".cite-pop a.id:hover{border-color:#2ee6c9}" +
      ".cite-pop .badge{font-size:9.5px;padding:1px 6px;border-radius:5px;border:1px solid}" +
      ".cite-pop .badge.v{color:#3ad6a0;border-color:#2f6b48}.cite-pop .badge.d{color:#9bb0c5;border-color:#2a3a4d}" +
      ".cite-pop a.open{display:inline-block;margin-top:8px;font-size:12px;font-weight:700;padding:6px 12px;border-radius:7px;background:#16335e;border:1px solid #2b5a86;color:#fff;text-decoration:none}" +
      ".cite-pop a.open:hover{background:#1d4a86}.cite-pop .x{float:right;cursor:pointer;color:#9bb0c5;margin:-2px -2px 0 6px;font-size:16px}" +
      ".cite-pop .muted{color:#9bb0c5}" +
      ".cite-key{font-size:10.5px;color:#9bb0c5;margin-left:10px;white-space:nowrap}" +
      ".cite-dot{display:inline-block;width:8px;height:8px;border-radius:50%;vertical-align:middle;margin-right:3px}" +
      ".cite-dot.v{background:#3ad6a0}.cite-dot.d{background:#5b7790}";
    document.head.appendChild(st);
  }
  function esc(s){ return String(s == null ? "" : s).replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;"); }
  function resolve(src){
    if (C.sources && C.sources[src]) return { kind: "guideline", key: src, e: C.sources[src] };
    if (C.aliases && C.aliases[src] && C.sources[C.aliases[src]]) return { kind: "guideline", key: C.aliases[src], e: C.sources[C.aliases[src]] };
    if (C.internal_map) { for (var k in C.internal_map) { if (src.indexOf(k) >= 0) return { kind: "internal", href: C.internal_map[k], label: k }; } }
    if (/^see\s+/i.test(src)) return { kind: "see", label: src };
    if ((C.pubmed_tags || []).indexOf(src) >= 0 || /literature|research/i.test(src)) return { kind: "pubmed" };
    return { kind: "method" };
  }
  function openUrl(e){ var u = e.url || ""; if (e.anchor === "page" && e.page) u += "#page=" + e.page; else if (e.anchor === "fragment" && e.fragment) u += "#:~:text=" + encodeURIComponent(e.fragment); return u; }

  var pop = null;
  function closePop(){ if (pop) { pop.remove(); pop = null; } }
  function showPop(chip){
    closePop();
    var src = chip.getAttribute("data-src"), mk = chip.getAttribute("data-mk") || "", r = resolve(src), html;
    if (r.kind === "guideline") {
      var e = r.e, ids = [];
      if (e.doi) ids.push('<a class="id" href="https://doi.org/' + esc(e.doi) + '" target="_blank" rel="noopener noreferrer">DOI</a>');
      if (e.pmid) ids.push('<a class="id" href="https://pubmed.ncbi.nlm.nih.gov/' + esc(e.pmid) + '/" target="_blank" rel="noopener noreferrer">PMID ' + esc(e.pmid) + '</a>');
      ids.push(e.verified ? '<span class="badge v">✓ link verified</span>' : '<span class="badge d">link not verified</span>');
      ids.push('<span class="badge d">' + (e.anchor === "document" ? "document-level" : "section link") + '</span>');
      html = '<span class="x">&times;</span><h4>' + esc(r.key) + '</h4>' +
             '<div>' + esc(e.cite) + '</div>' +
             (e.section ? '<div class="sec">§ ' + esc(e.section) + '</div>' : '') +
             '<div class="ids">' + ids.join("") + '</div>' +
             '<a class="open" href="' + esc(openUrl(e)) + '" target="_blank" rel="noopener noreferrer">↗ open source</a>';
    } else if (r.kind === "internal") {
      html = '<span class="x">&times;</span><h4>Internal reference</h4><div class="muted">This range is defined in another wiki section (' + esc(r.label) + ').</div><a class="open" href="' + esc(r.href) + '" target="_blank" rel="noopener noreferrer">↗ open page</a>';
    } else if (r.kind === "see") {
      html = '<span class="x">&times;</span><h4>Cross-reference</h4><div class="muted">' + esc(src) + ' — see the referenced pillar’s markers for the cited range.</div>';
    } else if (r.kind === "pubmed") {
      var term = encodeURIComponent((mk ? mk + " " : "") + "reference range");
      html = '<span class="x">&times;</span><h4>Literature</h4><div class="muted">No single guideline document; this range is literature-derived. Search the primary literature for ' + esc(mk || "this marker") + '.</div><a class="open" href="https://pubmed.ncbi.nlm.nih.gov/?term=' + term + '" target="_blank" rel="noopener noreferrer">↗ search PubMed</a>';
    } else {
      return; // method tags use a native tooltip, no popover
    }
    pop = document.createElement("div"); pop.className = "cite-pop"; pop.innerHTML = html;
    document.body.appendChild(pop);
    var rc = chip.getBoundingClientRect(), sx = window.scrollX || window.pageXOffset, sy = window.scrollY || window.pageYOffset;
    var left = rc.left + sx, top = rc.bottom + sy + 6;
    if (left + pop.offsetWidth > sx + document.documentElement.clientWidth - 12) left = sx + document.documentElement.clientWidth - pop.offsetWidth - 12;
    pop.style.left = Math.max(8, left) + "px"; pop.style.top = top + "px";
    var x = pop.querySelector(".x"); if (x) x.onclick = closePop;
    pop.addEventListener("click", function (ev) { ev.stopPropagation(); });
  }
  function bind(root){
    [].forEach.call((root || document).querySelectorAll(".cite"), function (chip) {
      if (chip._cb) return; chip._cb = 1;
      var r = resolve(chip.getAttribute("data-src"));
      if (r.kind === "method") { chip.classList.add("method"); chip.title = "measurement method / internal tag — not a guideline citation"; return; }
      chip.addEventListener("click", function (ev) { ev.stopPropagation(); showPop(chip); });
      chip.addEventListener("keydown", function (ev) { if (ev.key === "Enter" || ev.key === " ") { ev.preventDefault(); showPop(chip); } });
    });
  }
  window.PureCite = { bind: bind, show: showPop, close: closePop, resolve: resolve };
  function init(){
    bind(document);
    document.addEventListener("click", closePop);
    document.addEventListener("keydown", function (ev) { if (ev.key === "Escape") closePop(); });
    window.addEventListener("resize", closePop);
  }
  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", init); else init();
})();
