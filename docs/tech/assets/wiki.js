/* PureScore Tech Wiki — collapsible nav, search, active-link. Mermaid is loaded per-page (module). */
(function () {
  var here = location.pathname.split('/').pop() || 'index.html';
  var side = document.querySelector('.side');
  var groups = [].slice.call(document.querySelectorAll('.navgrp'));
  var KEY = 'psNavOpen';

  // active link
  document.querySelectorAll('.side a').forEach(function (a) {
    var t = a.getAttribute('href');
    if (t === here || (here === '' && t === 'index.html')) a.classList.add('active');
  });

  function load() { try { return JSON.parse(localStorage.getItem(KEY)); } catch (e) { return null; } }
  function save() {
    try { localStorage.setItem(KEY, JSON.stringify(groups.filter(function (g) { return g.classList.contains('open'); })
                                                            .map(function (g) { return g.getAttribute('data-grp'); }))); } catch (e) {}
  }
  function setOpen(g, open) {
    g.classList.toggle('open', open);
    var h = g.querySelector('.grp-h'); if (h) h.setAttribute('aria-expanded', open ? 'true' : 'false');
  }

  // restore persisted open-state (merged with the active group); first visit => only active open (collapsed default)
  var stored = load();
  if (stored) groups.forEach(function (g) { setOpen(g, stored.indexOf(g.getAttribute('data-grp')) > -1); });
  var ag = side && side.querySelector('a.active');
  if (ag) { var p = ag.closest('.navgrp'); if (p) setOpen(p, true); try { ag.scrollIntoView({ block: 'center' }); } catch (e) {} }

  // toggle a group
  groups.forEach(function (g) {
    var h = g.querySelector('.grp-h');
    if (h) h.addEventListener('click', function () { setOpen(g, !g.classList.contains('open')); save(); });
  });
  // expand / collapse all
  document.querySelectorAll('.side-allbtn').forEach(function (b) {
    b.addEventListener('click', function () {
      var exp = b.getAttribute('data-act') === 'expand';
      groups.forEach(function (g) { setOpen(g, exp); }); save();
    });
  });

  // mobile menu
  var btn = document.querySelector('.menu-btn'), main = document.querySelector('.main');
  if (btn && side) btn.addEventListener('click', function () { side.classList.toggle('open'); });
  if (main && side) main.addEventListener('click', function () { side.classList.remove('open'); });

  // (the top-bar search box is owned by search.js — full-text content search, not a sidebar filter)

  /* ---- facet filters: narrow the sidebar by module / type / audience / maturity / persona / region ---- */
  var fwrap = side && side.querySelector('.nav-filter');
  if (fwrap) {
    var selects = [].slice.call(fwrap.querySelectorAll('.nf-sel'));
    var countEl = fwrap.querySelector('.nf-count');
    var activeEl = fwrap.querySelector('.nf-active');
    var clearBtn = fwrap.querySelector('.nf-clear');
    var navlinks = [].slice.call(side.querySelectorAll('.grp-b a.navlink'));

    function tokens(a, facet) { return (a.getAttribute('data-' + facet) || '').split(/\s+/).filter(Boolean); }
    function apply() {
      var active = selects.filter(function (s) { return s.value; });
      var shown = 0;
      navlinks.forEach(function (a) {
        var ok = active.every(function (s) { return tokens(a, s.getAttribute('data-facet')).indexOf(s.value) > -1; });
        a.style.display = ok ? '' : 'none'; if (ok) shown++;
      });
      groups.forEach(function (g) {
        var any = [].slice.call(g.querySelectorAll('.grp-b a.navlink')).some(function (a) { return a.style.display !== 'none'; });
        g.style.display = (active.length && !any) ? 'none' : '';
        if (active.length && any) setOpen(g, true);          // open groups that still have hits
      });
      var n = active.length;
      fwrap.classList.toggle('filtering', n > 0);
      if (activeEl) activeEl.textContent = n ? ' · ' + n : '';
      if (countEl) countEl.textContent = n ? (shown + ' of ' + navlinks.length + ' pages match') : '';
      if (clearBtn) clearBtn.style.display = n ? '' : 'none';
      if (!n) {                                              // restore persisted open-state on clear
        var st = load();
        groups.forEach(function (g) { setOpen(g, (st ? st.indexOf(g.getAttribute('data-grp')) > -1 : false) || !!g.querySelector('a.active')); });
      }
    }
    selects.forEach(function (s) { s.addEventListener('change', apply); });
    if (clearBtn) clearBtn.addEventListener('click', function () { selects.forEach(function (s) { s.value = ''; }); apply(); });
    apply();
  }
})();

/* ---- reading progress: mark visited pages + % through the book ---- */
(function () {
  var here = location.pathname.split('/').pop() || 'index.html';
  var KEY = 'psVisited', v;
  try { v = JSON.parse(localStorage.getItem(KEY)) || {}; } catch (e) { v = {}; }
  v[here] = 1; try { localStorage.setItem(KEY, JSON.stringify(v)); } catch (e) {}
  var links = [].slice.call(document.querySelectorAll('.side a[href]'));
  var all = {}; links.forEach(function (a) { var h = a.getAttribute('href'); if (h && h.indexOf('.html') > -1) all[h] = 1; });
  var total = Object.keys(all).length || 1;
  var seen = Object.keys(v).filter(function (h) { return all[h]; }).length;
  var pct = Math.round(seen / total * 100);
  links.forEach(function (a) { if (v[a.getAttribute('href')]) a.classList.add('visited'); });
  var tools = document.querySelector('.side-tools');
  if (tools) {
    var pr = document.createElement('div'); pr.className = 'nav-prog';
    pr.innerHTML = '<div class="np-bar"><i style="width:' + pct + '%"></i></div>' +
      '<div class="np-t">' + seen + ' / ' + total + ' · ' + pct + '% read <button class="np-reset" title="reset progress">reset</button></div>';
    tools.parentNode.insertBefore(pr, tools.nextSibling);
    var rb = pr.querySelector('.np-reset');
    if (rb) rb.addEventListener('click', function () { try { localStorage.removeItem(KEY); } catch (e) {} location.reload(); });
  }
})();

/* ---- "On this page" mini-TOC (collapsible, under the h1) ---- */
(function () {
  var main = document.querySelector('.main'); if (!main) return;
  var h1 = main.querySelector('h1'); if (!h1) return;
  var hs = [].slice.call(main.querySelectorAll('h2[id], h3[id]')).filter(function (h) { return h.id && h.textContent.trim(); });
  if (hs.length < 3) return;
  var d = document.createElement('details'); d.className = 'onthispage';
  var li = hs.map(function (h) {
    return '<li class="otp-' + h.tagName.toLowerCase() + '"><a href="#' + h.id + '">' +
      h.textContent.replace(/\s+/g, ' ').trim() + '</a></li>';
  }).join('');
  d.innerHTML = '<summary>On this page · ' + hs.length + '</summary><ul>' + li + '</ul>';
  h1.parentNode.insertBefore(d, h1.nextSibling);
})();

/* command palette (⌘K / Ctrl-K) and the top-bar search box now live in assets/search.js */
