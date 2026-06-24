/* Wiki-wide hover affordances: acronym glossary, internal-link preview, section anchor-copy, page-title desc.
   Consumes window.PURESCORE_ACRONYMS (assets/acronyms.js) + window.PURESCORE_PAGEMETA (assets/page-meta.js). */
(function () {
  var AC = (window.PURESCORE_ACRONYMS) || {};
  var PM = (window.PURESCORE_PAGEMETA) || {};
  var main = document.querySelector("main.main");
  if (!main) return;
  var here = (location.pathname.split("/").pop() || "index.html");

  // shared tooltip
  var tt = document.createElement("div");
  tt.className = "htt";
  tt.setAttribute("role", "tooltip");
  document.body.appendChild(tt);
  function tipFor(el, html) {
    tt.innerHTML = html;
    tt.style.display = "block";
    var r = el.getBoundingClientRect(), w = tt.offsetWidth, h = tt.offsetHeight;
    var left = r.left + window.scrollX;
    if (left + w > window.scrollX + window.innerWidth - 12) left = window.scrollX + window.innerWidth - w - 12;
    if (left < 8) left = 8;
    var top = r.bottom + window.scrollY + 6;
    if (r.bottom + h + 24 > window.innerHeight) top = r.top + window.scrollY - h - 6;
    tt.style.left = left + "px";
    tt.style.top = top + "px";
  }
  function hide() { tt.style.display = "none"; }

  // 1) ACRONYM WRAPPING — combined boundary regex, walk text nodes inside main only
  var keys = Object.keys(AC).sort(function (a, b) { return b.length - a.length; });
  if (keys.length) {
    var esc = function (s) { return s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"); };
    var src = "(?:^|[^A-Za-z0-9])(" + keys.map(esc).join("|") + ")(?![A-Za-z0-9])";
    var SKIP = { A: 1, ABBR: 1, CODE: 1, PRE: 1, SCRIPT: 1, STYLE: 1, BUTTON: 1, SELECT: 1, svg: 1, SVG: 1, TEXTAREA: 1, INPUT: 1 };
    function walk(node) {
      var kids = node.childNodes, i;
      for (i = kids.length - 1; i >= 0; i--) {
        var c = kids[i];
        if (c.nodeType === 3) wrap(c);
        else if (c.nodeType === 1 && !SKIP[c.tagName] && !(c.classList && c.classList.contains("gloss"))) walk(c);
      }
    }
    function wrap(tn) {
      var text = tn.nodeValue;
      if (!text || text.length < 2) return;
      var rg = new RegExp(src, "g"), m, last = 0, frag = null;
      while ((m = rg.exec(text))) {
        var tok = m[1], start = m.index + (m[0].length - tok.length);
        if (!frag) frag = document.createDocumentFragment();
        if (start > last) frag.appendChild(document.createTextNode(text.slice(last, start)));
        var ab = document.createElement("abbr");
        ab.className = "gloss";
        ab.setAttribute("data-x", AC[tok]);
        ab.textContent = tok;
        frag.appendChild(ab);
        last = start + tok.length;
        rg.lastIndex = last;
      }
      if (frag) {
        if (last < text.length) frag.appendChild(document.createTextNode(text.slice(last)));
        tn.parentNode.replaceChild(frag, tn);
      }
    }
    // Defer the acronym walk until mermaid has finished rendering: this walk mutates the DOM and
    // reflows, and doing it WHILE mermaid asynchronously measures htmlLabels corrupts wide flowcharts.
    // The shared renderer fires "mermaid-ready" (immediately if a page has no diagrams).
    var doWalk = function () { try { walk(main); } catch (e) { /* never break the page */ } };
    if (document.querySelector("pre.mermaid")) {
      var walked = false, run = function () { if (walked) return; walked = true; doWalk(); };
      window.addEventListener("mermaid-ready", run);
      setTimeout(run, 8000); // fallback if the event never fires
    } else { doWalk(); }
  }

  // 2) SECTION HEADINGS — reveal a copy-link anchor on hover
  [].forEach.call(main.querySelectorAll("h2[id], h3[id]"), function (h) {
    var a = document.createElement("a");
    a.className = "hanchor";
    a.href = "#" + h.id;
    a.setAttribute("aria-label", "Copy link to section");
    a.textContent = "¶";
    a.addEventListener("click", function (e) {
      e.preventDefault();
      var url = location.origin + location.pathname + "#" + h.id;
      if (navigator.clipboard) navigator.clipboard.writeText(url);
      if (history.replaceState) history.replaceState(null, "", "#" + h.id);
      tipFor(h, "Link copied");
      setTimeout(hide, 1100);
    });
    h.appendChild(a);
  });

  // 3) HOVER DELEGATION — glossary, internal links, headings, page title
  main.addEventListener("mouseover", function (e) {
    var t = e.target;
    if (t.classList && t.classList.contains("gloss")) {
      tipFor(t, "<b>" + t.textContent + "</b> &mdash; " + t.getAttribute("data-x"));
      return;
    }
    var a = t.closest && t.closest('a[href]');
    if (a) {
      var href = a.getAttribute("href") || "";
      if (href.indexOf(".html") >= 0 && href.indexOf("://") < 0) {
        var fn = href.split("#")[0].split("/").pop();
        var pm = PM[fn];
        if (pm) { tipFor(a, "<b>" + pm.title + "</b>" + (pm.desc ? "<br><span class='hd'>" + pm.desc + "</span>" : "")); return; }
      }
    }
    var h1 = t.closest && t.closest("h1");
    if (h1) {
      var me = PM[here];
      if (me && me.desc) { tipFor(h1, "<span class='hd'>" + me.desc + "</span>"); return; }
    }
    var hx = t.closest && t.closest("h2[id], h3[id]");
    if (hx && t.className !== "hanchor") { tipFor(hx, "Click <b>&para;</b> to copy a link to this section"); return; }
  });
  main.addEventListener("mouseout", function (e) {
    var to = e.relatedTarget;
    if (!to || (to !== tt && !main.contains(to))) hide();
    else if (to && to.classList && !to.classList.contains("gloss") && !(to.closest && to.closest('a[href],h1,h2[id],h3[id]'))) hide();
  });
  window.addEventListener("scroll", hide, { passive: true });
})();
