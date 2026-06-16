/* PureScore Tech Wiki — navigation, search, active-link. Mermaid is loaded per-page (module). */
(function () {
  var here = location.pathname.split('/').pop() || 'index.html';

  // active sidebar link + scroll into view
  document.querySelectorAll('.side a').forEach(function (a) {
    var t = a.getAttribute('href');
    if (t === here || (here === '' && t === 'index.html')) {
      a.classList.add('active');
      try { a.scrollIntoView({ block: 'center' }); } catch (e) {}
    }
  });

  // mobile menu
  var btn = document.querySelector('.menu-btn'), side = document.querySelector('.side');
  if (btn && side) {
    btn.addEventListener('click', function () { side.classList.toggle('open'); });
    document.querySelector('.main') && document.querySelector('.main')
      .addEventListener('click', function () { side.classList.remove('open'); });
  }

  // sidebar filter
  var s = document.getElementById('search');
  if (s) {
    s.addEventListener('input', function () {
      var q = s.value.trim().toLowerCase();
      document.querySelectorAll('.side a').forEach(function (a) {
        var hit = !q || a.textContent.toLowerCase().indexOf(q) > -1;
        a.style.display = hit ? '' : 'none';
      });
      document.querySelectorAll('.side h4').forEach(function (h) {
        // hide a group header if all its links are hidden
        var n = h.nextElementSibling, any = false;
        while (n && n.tagName === 'A') { if (n.style.display !== 'none') any = true; n = n.nextElementSibling; }
        h.style.display = any ? '' : 'none';
      });
    });
    if (location.hash) { /* keep */ }
    document.addEventListener('keydown', function (e) {
      if (e.key === '/' && document.activeElement !== s) { e.preventDefault(); s.focus(); }
    });
  }
})();
