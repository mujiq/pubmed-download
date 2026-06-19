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
