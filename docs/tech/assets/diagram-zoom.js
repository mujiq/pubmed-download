/* diagram-zoom.js — fullscreen + zoom/pan overlay for every diagram in the wiki.
 * Targets `.diagram` wrappers (per-doc mermaid + hand-built SVGs) and bare `pre.mermaid`
 * blocks (the tabbed multi-view pages). Leaves the interactive cockpit and tiny UI glyphs
 * alone. No dependencies, file:// safe, ES5-ish to match the other assets. */
(function () {
  "use strict";
  var overlay, stage, panner, art, baseW = 1, scale = 1, tx = 0, ty = 0, drag = null;

  function build() {
    overlay = document.createElement("div");
    overlay.className = "dgm-overlay";
    overlay.innerHTML =
      '<div class="dgm-bar">' +
        '<span class="dgm-cap"></span><span class="dgm-sp"></span>' +
        '<button class="dgm-b" data-a="out" title="Zoom out (−)">−</button>' +
        '<button class="dgm-b" data-a="reset" title="Reset (0)">reset</button>' +
        '<button class="dgm-b" data-a="in" title="Zoom in (+)">+</button>' +
        '<button class="dgm-b dgm-x" data-a="close" title="Close (Esc)">✕ Close</button>' +
      '</div><div class="dgm-stage"></div>';
    document.body.appendChild(overlay);
    stage = overlay.querySelector(".dgm-stage");
    overlay.addEventListener("click", function (e) {
      if (e.target === overlay) { close(); return; }
      var b = e.target.closest && e.target.closest(".dgm-b");
      if (!b) return;
      var a = b.getAttribute("data-a");
      if (a === "close") close();
      else if (a === "in") zoomBy(1.25);
      else if (a === "out") zoomBy(0.8);
      else if (a === "reset") { fit(); scale = 1; tx = 0; ty = 0; apply(); }
    });
    stage.addEventListener("wheel", function (e) { e.preventDefault(); zoomBy(e.deltaY < 0 ? 1.12 : 0.893); }, { passive: false });
    stage.addEventListener("pointerdown", function (e) { drag = { x: e.clientX, y: e.clientY, tx: tx, ty: ty }; try { stage.setPointerCapture(e.pointerId); } catch (_) {} });
    stage.addEventListener("pointermove", function (e) { if (!drag) return; tx = drag.tx + (e.clientX - drag.x); ty = drag.ty + (e.clientY - drag.y); apply(); });
    window.addEventListener("pointerup", function () { drag = null; });
    document.addEventListener("keydown", function (e) {
      if (!overlay || overlay.className.indexOf("on") < 0) return;
      if (e.key === "Escape") close();
      else if (e.key === "+" || e.key === "=") zoomBy(1.25);
      else if (e.key === "-" || e.key === "_") zoomBy(0.8);
      else if (e.key === "0") { fit(); scale = 1; tx = 0; ty = 0; apply(); }
    });
  }

  // Zoom by re-sizing the SVG itself (the browser re-rasterises the vector crisply at the new
  // size) and pan with a translate on the wrapper. Never scale() the SVG — that bitmap-scales a
  // cached layer and softens the text. So fullscreen text stays sharp at any zoom.
  function apply() {
    if (!art) return;
    art.style.width = Math.round(baseW * scale) + "px";
    art.style.height = "auto";
    panner.style.transform = "translate(" + tx + "px," + ty + "px)";
  }
  function zoomBy(f) { scale = Math.max(0.2, Math.min(16, scale * f)); apply(); }
  function fit() {
    // start sized to fit the stage (whole diagram visible), using the SVG's own aspect ratio
    var vb = ((art.getAttribute && art.getAttribute("viewBox")) || "").split(/[\s,]+/).map(Number);
    var ar = (vb.length === 4 && vb[2] > 0 && vb[3] > 0) ? vb[2] / vb[3] : 1.6;
    var sw = stage.clientWidth * 0.96, sh = stage.clientHeight * 0.96;
    baseW = Math.max(140, Math.min(sw, sh * ar));
  }

  function open(host, caption) {
    if (!overlay) build();
    // Clone the rendered SVG (mermaid output or inline). Keep its id — mermaid scopes its
    // internal <style> by that id, so removing it would strip the theme colours.
    var svg = host.querySelector("svg");
    art = (svg || host).cloneNode(true);
    art.removeAttribute("width"); art.removeAttribute("height");
    art.style.maxWidth = "none"; art.style.maxHeight = "none"; art.style.height = "auto";
    panner = document.createElement("div"); panner.className = "dgm-pan";
    panner.appendChild(art);
    stage.innerHTML = ""; stage.appendChild(panner);
    overlay.querySelector(".dgm-cap").textContent = caption || "Diagram";
    overlay.className = "dgm-overlay on";              // show first so the stage has dimensions
    document.documentElement.style.overflow = "hidden";
    fit(); scale = 1; tx = 0; ty = 0; apply();
  }
  function close() {
    if (!overlay) return;
    overlay.className = "dgm-overlay";
    document.documentElement.style.overflow = "";
    stage.innerHTML = ""; art = null; panner = null;
  }

  function caption(host) {
    var dt = host.querySelector && host.querySelector(".dt");
    if (dt) return dt.textContent;
    // bare pre.mermaid: borrow the nearest preceding heading text
    var p = host.previousElementSibling;
    while (p) { if (/^H[1-6]$/.test(p.tagName)) return p.textContent; p = p.previousElementSibling; }
    return "Diagram";
  }

  function decorate(host) {
    if (host.getAttribute("data-dgm") === "1") return;
    host.setAttribute("data-dgm", "1");
    var cs = window.getComputedStyle(host);
    if (cs.position === "static") host.style.position = "relative";
    host.classList.add("dgm-host");
    var btn = document.createElement("button");
    btn.type = "button"; btn.className = "dgm-zoom-btn";
    btn.title = "View fullscreen"; btn.setAttribute("aria-label", "View diagram fullscreen");
    btn.innerHTML = "⛶"; // ⛶
    btn.addEventListener("click", function (e) { e.stopPropagation(); e.preventDefault(); open(host, caption(host)); });
    host.appendChild(btn);
    host.addEventListener("click", function (e) {
      // let mermaid node-links / real controls work; the button has its own handler
      if (e.target.closest && (e.target.closest("a") || e.target.closest("button") || e.target.closest("input"))) return;
      open(host, caption(host));
    });
  }

  function init() {
    var i, host;
    var dgs = document.querySelectorAll(".diagram");
    for (i = 0; i < dgs.length; i++) {
      host = dgs[i];
      if (host.querySelector("pre.mermaid, svg")) decorate(host); // skip empty wrappers
    }
    var pres = document.querySelectorAll("pre.mermaid");
    for (i = 0; i < pres.length; i++) {
      host = pres[i];
      if (!(host.closest && host.closest(".diagram"))) decorate(host); // bare mermaid (multi-view pages)
    }
  }
  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", init);
  else init();
})();
