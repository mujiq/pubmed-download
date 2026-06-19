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

  // search — filters links and auto-opens groups with matches; restores on clear
  var s = document.getElementById('search');
  if (s) {
    s.addEventListener('input', function () {
      var q = s.value.trim().toLowerCase();
      groups.forEach(function (g) {
        var any = false;
        [].slice.call(g.querySelectorAll('.grp-b a')).forEach(function (a) {
          var hit = !q || a.textContent.toLowerCase().indexOf(q) > -1;
          a.style.display = hit ? '' : 'none'; if (hit && q) any = true;
        });
        if (q) { g.style.display = any ? '' : 'none'; if (any) setOpen(g, true); }
        else { g.style.display = ''; }
      });
      if (!q) {
        var st = load();
        groups.forEach(function (g) {
          setOpen(g, (st ? st.indexOf(g.getAttribute('data-grp')) > -1 : false) || !!g.querySelector('a.active'));
        });
      }
    });
    document.addEventListener('keydown', function (e) {
      if (e.key === '/' && document.activeElement !== s) { e.preventDefault(); s.focus(); }
    });
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

/* ---- command palette (⌘K / Ctrl-K) — jump to any page ---- */
(function () {
  var links = [].slice.call(document.querySelectorAll('.side a[href]'));
  var seen = {}, items = [];
  links.forEach(function (a) {
    var h = a.getAttribute('href');
    if (h && h.indexOf('.html') > -1 && !seen[h]) { seen[h] = 1; items.push({ h: h, l: a.textContent.replace(/\s+/g, ' ').trim() }); }
  });
  var modal = document.createElement('div'); modal.className = 'cmdk';
  modal.innerHTML = '<div class="cmdk-box"><input class="cmdk-in" placeholder="Jump to a page…  (⌘K · Esc to close)"><ul class="cmdk-list"></ul></div>';
  document.body.appendChild(modal);
  var inp = modal.querySelector('.cmdk-in'), list = modal.querySelector('.cmdk-list'), sel = 0, shown = [];
  function render(q) {
    q = (q || '').toLowerCase();
    shown = items.filter(function (it) { return !q || it.l.toLowerCase().indexOf(q) > -1; }).slice(0, 50);
    sel = 0;
    list.innerHTML = shown.map(function (it, i) { return '<li class="' + (i === 0 ? 'sel' : '') + '" data-h="' + it.h + '">' + it.l + '</li>'; }).join('');
  }
  function paint() { [].slice.call(list.children).forEach(function (li, i) { li.classList.toggle('sel', i === sel); if (i === sel) li.scrollIntoView({ block: 'nearest' }); }); }
  function open() { modal.classList.add('on'); inp.value = ''; render(''); setTimeout(function () { inp.focus(); }, 10); }
  function close() { modal.classList.remove('on'); }
  document.addEventListener('keydown', function (e) {
    if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === 'k') { e.preventDefault(); modal.classList.contains('on') ? close() : open(); return; }
    if (!modal.classList.contains('on')) return;
    if (e.key === 'Escape') close();
    else if (e.key === 'ArrowDown') { e.preventDefault(); sel = Math.min(sel + 1, shown.length - 1); paint(); }
    else if (e.key === 'ArrowUp') { e.preventDefault(); sel = Math.max(sel - 1, 0); paint(); }
    else if (e.key === 'Enter') { e.preventDefault(); var li = list.children[sel]; if (li) location.href = li.getAttribute('data-h'); }
  });
  inp.addEventListener('input', function () { render(inp.value); });
  list.addEventListener('click', function (e) { var li = e.target.closest('li'); if (li) location.href = li.getAttribute('data-h'); });
  modal.addEventListener('click', function (e) { if (e.target === modal) close(); });
})();
