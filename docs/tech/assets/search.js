/* PureScore Tech Wiki — full-text content search.
   Drives the top-bar search box (dropdown) AND the ⌘K / Ctrl-K palette, both off the same index:
   window.PURESCORE_SEARCH = [{f,t,m,secs:[{i,h,s}]}]  (built by build_wiki.py:_build_search_index).
   Matches page titles, section headings and snippets; results deep-link to the section #anchor. */
(function () {
  var DATA = window.PURESCORE_SEARCH || [];
  var REC = [];
  DATA.forEach(function (p) {
    (p.secs || []).forEach(function (s) {
      REC.push({
        f: p.f, t: p.t, m: p.m || '', id: s.i || '', h: s.h || p.t, s: s.s || '',
        hay: (p.t + ' ' + (p.m || '') + ' ' + (s.h || '') + ' ' + (s.s || '')).toLowerCase()
      });
    });
  });

  function esc(x) { return ('' + x).replace(/[&<>"]/g, function (c) { return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]; }); }
  function rx(t) { return t.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'); }
  function hl(text, toks) {
    var out = esc(text);
    toks.forEach(function (t) { if (t) out = out.replace(new RegExp('(' + rx(t) + ')', 'ig'), '<mark>$1</mark>'); });
    return out;
  }
  function search(q) {
    q = (q || '').trim().toLowerCase();
    if (!q) return [];
    var toks = q.split(/\s+/), scored = [];
    REC.forEach(function (r) {
      if (!toks.every(function (t) { return r.hay.indexOf(t) > -1; })) return;
      var ht = r.h.toLowerCase(), tt = r.t.toLowerCase(), st = r.s.toLowerCase(), sc = 0;
      toks.forEach(function (t) {
        if (ht.indexOf(t) > -1) sc += 4;
        if (tt.indexOf(t) > -1) sc += 2;
        if (st.indexOf(t) > -1) sc += 1;
      });
      if (ht.indexOf(q) > -1) sc += 6;           // exact-phrase bonuses
      if (tt.indexOf(q) > -1) sc += 4;
      scored.push({ r: r, sc: sc });
    });
    scored.sort(function (a, b) { return b.sc - a.sc; });
    return scored.slice(0, 40).map(function (x) { return x.r; });
  }
  function href(r) { return r.f + (r.id ? '#' + r.id : ''); }

  /* ---- shared result-row markup ---- */
  function rowHTML(r, toks, selClass) {
    return '<li class="sr-item' + (selClass ? ' sel' : '') + '" data-h="' + esc(href(r)) + '">' +
      '<div class="sr-h">' + hl(r.h, toks) + '</div>' +
      '<div class="sr-meta">' + esc(r.t) + (r.m ? ' <span class="sr-mod">' + esc(r.m) + '</span>' : '') + '</div>' +
      (r.s ? '<div class="sr-s">' + hl(r.s, toks) + '</div>' : '') + '</li>';
  }

  /* ---- top-bar dropdown ---- */
  var box = document.getElementById('search');
  if (box) {
    box.setAttribute('placeholder', 'Search the wiki…  ( / )');
    box.setAttribute('autocomplete', 'off');
    var panel = document.createElement('div'); panel.className = 'sr-panel'; panel.setAttribute('role', 'listbox');
    (document.querySelector('.topbar') || document.body).appendChild(panel);
    var shown = [], sel = 0;
    function close() { panel.classList.remove('on'); }
    function open() { if (panel.innerHTML.trim()) panel.classList.add('on'); }
    function paint() {
      [].slice.call(panel.querySelectorAll('.sr-item')).forEach(function (li, i) {
        li.classList.toggle('sel', i === sel); if (i === sel) li.scrollIntoView({ block: 'nearest' });
      });
    }
    function render() {
      var q = box.value, toks = q.trim().toLowerCase().split(/\s+/);
      shown = search(q); sel = 0;
      if (!q.trim()) { panel.innerHTML = ''; close(); return; }
      if (!shown.length) {
        panel.innerHTML = '<div class="sr-empty">No matches for “' + esc(q.trim()) + '”. Try ⌘K to jump by page.</div>';
      } else {
        panel.innerHTML = '<div class="sr-head">' + shown.length + ' result' + (shown.length === 1 ? '' : 's') +
          ' · ↑↓ to move · ↵ to open</div><ul class="sr-list">' +
          shown.map(function (r, i) { return rowHTML(r, toks, i === 0); }).join('') + '</ul>';
      }
      open();
    }
    box.addEventListener('input', render);
    box.addEventListener('focus', function () { if (box.value.trim()) render(); });
    box.addEventListener('keydown', function (e) {
      if (e.key === 'ArrowDown') { e.preventDefault(); sel = Math.min(sel + 1, shown.length - 1); paint(); }
      else if (e.key === 'ArrowUp') { e.preventDefault(); sel = Math.max(sel - 1, 0); paint(); }
      else if (e.key === 'Enter') { if (shown[sel]) { e.preventDefault(); location.href = href(shown[sel]); } }
      else if (e.key === 'Escape') { box.blur(); close(); }
    });
    panel.addEventListener('mousedown', function (e) { var li = e.target.closest('.sr-item'); if (li) location.href = li.getAttribute('data-h'); });
    document.addEventListener('click', function (e) { if (e.target !== box && !panel.contains(e.target)) close(); });
    document.addEventListener('keydown', function (e) {
      if (e.key === '/' && document.activeElement !== box && !/^(INPUT|TEXTAREA|SELECT)$/.test((document.activeElement || {}).tagName || '')) {
        e.preventDefault(); box.focus();
      }
    });
  }

  /* ---- ⌘K / Ctrl-K palette (content-aware) ---- */
  var modal = document.createElement('div'); modal.className = 'cmdk';
  modal.innerHTML = '<div class="cmdk-box"><input class="cmdk-in" placeholder="Search pages & sections…  (⌘K · Esc to close)" autocomplete="off"><ul class="cmdk-list"></ul></div>';
  document.body.appendChild(modal);
  var inp = modal.querySelector('.cmdk-in'), list = modal.querySelector('.cmdk-list'), msel = 0, mshown = [];
  // page-level fallback list (so an empty query still lets you jump to any page)
  var PAGES = DATA.map(function (p) { return { f: p.f, t: p.t, m: p.m || '', id: '', h: p.t, s: p.m || '' }; });
  function mrender(q) {
    var toks = (q || '').trim().toLowerCase().split(/\s+/);
    mshown = q.trim() ? search(q) : PAGES.slice(0, 60); msel = 0;
    list.innerHTML = mshown.length
      ? mshown.map(function (r, i) { return rowHTML(r, q.trim() ? toks : [], i === 0); }).join('')
      : '<li class="sr-empty">No matches.</li>';
  }
  function mpaint() { [].slice.call(list.children).forEach(function (li, i) { li.classList.toggle('sel', i === msel); if (i === msel) li.scrollIntoView({ block: 'nearest' }); }); }
  function mopen() { modal.classList.add('on'); inp.value = ''; mrender(''); setTimeout(function () { inp.focus(); }, 10); }
  function mclose() { modal.classList.remove('on'); }
  document.addEventListener('keydown', function (e) {
    if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === 'k') { e.preventDefault(); modal.classList.contains('on') ? mclose() : mopen(); return; }
    if (!modal.classList.contains('on')) return;
    if (e.key === 'Escape') mclose();
    else if (e.key === 'ArrowDown') { e.preventDefault(); msel = Math.min(msel + 1, mshown.length - 1); mpaint(); }
    else if (e.key === 'ArrowUp') { e.preventDefault(); msel = Math.max(msel - 1, 0); mpaint(); }
    else if (e.key === 'Enter') { e.preventDefault(); if (mshown[msel]) location.href = href(mshown[msel]); }
  });
  inp.addEventListener('input', function () { mrender(inp.value); });
  list.addEventListener('mousedown', function (e) { var li = e.target.closest('.sr-item, li[data-h]'); if (li && li.getAttribute('data-h')) location.href = li.getAttribute('data-h'); });
  modal.addEventListener('mousedown', function (e) { if (e.target === modal) mclose(); });
})();
