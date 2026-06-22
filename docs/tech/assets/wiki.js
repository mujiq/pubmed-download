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
    var h = g.querySelector('.grp-tog'); if (h) h.setAttribute('aria-expanded', open ? 'true' : 'false');
  }

  // restore persisted open-state (merged with the active group); first visit => only active open (collapsed default)
  var stored = load();
  if (stored) groups.forEach(function (g) { setOpen(g, stored.indexOf(g.getAttribute('data-grp')) > -1); });
  var ag = side && side.querySelector('a.active');
  if (ag) { var p = ag.closest('.navgrp'); if (p) setOpen(p, true); try { ag.scrollIntoView({ block: 'center' }); } catch (e) {} }

  // toggle a group (caret only — the chapter title is a link to the landing page)
  groups.forEach(function (g) {
    var h = g.querySelector('.grp-tog');
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

/* Acronym glossary — hover tooltips for pillar codes, guideline bodies and lab markers.
   Scoped to the Pillars & Marker Catalogue (Doc 02); widen the path test to go wiki-wide.
   Uses native <abbr title> so tooltips are never clipped by scrollable marker tables. */
(function () {
  if (!/02-pillars-and-marker-catalog\.html$/.test(location.pathname)) return;
  var main = document.querySelector('.main'); if (!main) return;
  var G = {
    // 12 pillars
    "CV":"Cardiovascular & Vascular pillar — heart and blood-vessel health (BP, ApoB/LDL, Lp(a), CAC).",
    "MET":"Metabolic & Glycemic pillar — glucose and energy metabolism (HbA1c, fasting glucose, triglycerides).",
    "REN":"Renal pillar — kidney filtration and electrolyte handling (eGFR, UACR, cystatin C).",
    "HEP":"Hepatic pillar — liver health (ALT, AST, GGT, hepatic fat / FIB-4).",
    "INF":"Inflammation & Immune pillar — systemic inflammation and immune status (hs-CRP, WBC, ferritin).",
    "HEM":"Hematologic & Oxygen-transport pillar — blood and oxygen carriage (haemoglobin, iron studies, RDW).",
    "ENDO":"Endocrine & Hormonal pillar — hormones, often sex-specific (TSH, testosterone/oestradiol, cortisol).",
    "BCM":"Body Composition & Musculoskeletal pillar — fat / muscle / bone (waist, DEXA body fat, grip strength).",
    "NUT":"Nutrition & Micronutrients pillar — diet quality and micronutrient status (vitamin D, B12, omega-3).",
    "SLP":"Sleep & Circadian Recovery pillar — sleep quantity, quality and regularity (duration, SRI, OSA risk).",
    "FIT":"Physical Activity & Cardiorespiratory Fitness pillar — exercise and aerobic capacity (VO2max, steps, MVPA).",
    "MCS":"Mental, Cognitive & Social Health pillar — mood, stress and cognition (PHQ-9, GAD-7, PSS).",
    // guideline bodies & references
    "AHA":"American Heart Association — US cardiovascular guideline body.",
    "ACC":"American College of Cardiology — US cardiology guidelines (ACC/AHA risk thresholds).",
    "ESC":"European Society of Cardiology — European cardiovascular guidelines.",
    "ADA":"American Diabetes Association — diabetes standards (e.g. HbA1c cut-points).",
    "KDIGO":"Kidney Disease: Improving Global Outcomes — international kidney-disease staging.",
    "AASLD":"American Association for the Study of Liver Diseases — hepatology guidelines.",
    "ATA":"American Thyroid Association — thyroid-disease guidelines.",
    "ISCD":"International Society for Clinical Densitometry — bone-density (DEXA) standards.",
    "WHO":"World Health Organization.",
    "NHANES":"US National Health and Nutrition Examination Survey — population reference distributions.",
    // labs, markers & instruments
    "BP":"Blood Pressure — systolic/diastolic (mmHg).",
    "SBP":"Systolic Blood Pressure (mmHg).",
    "HR":"Heart Rate (beats per minute).",
    "HRV":"Heart-Rate Variability — autonomic-recovery marker (ms, e.g. RMSSD).",
    "LDL":"Low-Density Lipoprotein cholesterol — atherogenic lipid.",
    "ApoB":"Apolipoprotein B — count of atherogenic particles; preferred lipid-risk marker.",
    "HbA1c":"Glycated haemoglobin — ~3-month average glucose (% or mmol/mol).",
    "hs-CRP":"high-sensitivity C-Reactive Protein — systemic inflammation marker (mg/L).",
    "ALT":"Alanine aminotransferase — liver enzyme (U/L).",
    "AST":"Aspartate aminotransferase — liver enzyme (U/L).",
    "WBC":"White Blood Cell count — immune / inflammation marker.",
    "UACR":"Urine Albumin-to-Creatinine Ratio — early kidney-damage marker (mg/g).",
    "eGFR":"estimated Glomerular Filtration Rate — kidney function (mL/min/1.73m²).",
    "CAC":"Coronary Artery Calcium score — CT measure of coronary plaque burden.",
    "CGM":"Continuous Glucose Monitor — wearable glucose sensor.",
    "DEXA":"Dual-energy X-ray Absorptiometry — body-composition and bone-density scan.",
    "DXA":"Dual-energy X-ray Absorptiometry — body-composition and bone-density scan.",
    "FIB-4":"Fibrosis-4 index — non-invasive liver-fibrosis estimate (age, AST, ALT, platelets).",
    "MRI":"Magnetic Resonance Imaging.",
    "OSA":"Obstructive Sleep Apnoea.",
    "PHQ-9":"Patient Health Questionnaire-9 — validated depression screener.",
    "GAD-7":"Generalized Anxiety Disorder-7 — validated anxiety screener.",
    "PSS":"Perceived Stress Scale — validated stress questionnaire.",
    "PRO":"Patient-Reported Outcome — standardized self-report instrument.",
    "VO2max":"Maximal oxygen uptake — gold-standard cardiorespiratory-fitness measure (mL/kg/min).",
    "ASCVD":"Atherosclerotic Cardiovascular Disease — 10-year risk estimate.",
    "Lp(a)":"Lipoprotein(a) — largely genetic atherogenic lipid particle.",
    "FINDRISC":"Finnish Diabetes Risk Score — type-2-diabetes risk questionnaire.",
    "MEDAS":"Mediterranean Diet Adherence Screener.",
    "SRI":"Sleep Regularity Index."
  };
  function esc(s){ return s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'); }
  var keys = Object.keys(G).sort(function (a, b) { return b.length - a.length; });
  var src = '(?<![A-Za-z0-9])(' + keys.map(esc).join('|') + ')(?![A-Za-z0-9])';
  var probe = new RegExp(src);
  var rxg = new RegExp(src, 'g');
  var SKIP = { A:1, ABBR:1, CODE:1, PRE:1, SCRIPT:1, STYLE:1, BUTTON:1, H1:1 };
  var walker = document.createTreeWalker(main, NodeFilter.SHOW_TEXT, {
    acceptNode: function (n) {
      if (!n.nodeValue || !n.nodeValue.trim()) return NodeFilter.FILTER_REJECT;
      for (var p = n.parentNode; p && p !== main; p = p.parentNode) { if (SKIP[p.nodeName]) return NodeFilter.FILTER_REJECT; }
      return probe.test(n.nodeValue) ? NodeFilter.FILTER_ACCEPT : NodeFilter.FILTER_REJECT;
    }
  });
  var nodes = [], t; while ((t = walker.nextNode())) nodes.push(t);
  nodes.forEach(function (node) {
    var s = node.nodeValue, frag = document.createDocumentFragment(), last = 0, m; rxg.lastIndex = 0;
    while ((m = rxg.exec(s))) {
      if (m.index > last) frag.appendChild(document.createTextNode(s.slice(last, m.index)));
      var ab = document.createElement('abbr');
      ab.className = 'gloss'; ab.title = G[m[1]]; ab.setAttribute('tabindex', '0'); ab.textContent = m[1];
      frag.appendChild(ab); last = m.index + m[1].length;
    }
    if (last < s.length) frag.appendChild(document.createTextNode(s.slice(last)));
    node.parentNode.replaceChild(frag, node);
  });
  var st = document.createElement('style');
  st.textContent = '.main abbr.gloss{border-bottom:1px dotted var(--accent,#5cd6c0);cursor:help;text-decoration:none}';
  document.head.appendChild(st);
})();
