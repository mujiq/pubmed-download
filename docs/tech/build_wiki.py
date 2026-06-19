#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""PureScore tech wiki generator. Renders docs/purescore/*.md into an interlinked
multi-page wiki under docs/tech/, plus reference appendices and Doctor's-board admin mockups.

Run:  python3 build_wiki.py   (from docs/tech/)
"""
import os, re, glob, json
import wiki_content as C
import wiki_admin as A

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.normpath(os.path.join(HERE, "..", "purescore"))

# ----------------------------------------------------------------- doc registry
DOC_FILES = sorted(glob.glob(os.path.join(SRC, "[0-9][0-9]-*.md")))
DOC_NUM = {}                                   # num -> md path
for p in DOC_FILES:
    DOC_NUM[os.path.basename(p)[:2]] = p
def html_of(mdpath):                            # 03-scoring-formula.md -> 03-scoring-formula.html
    return os.path.basename(mdpath)[:-3] + ".html"
DOCMAP = {n: html_of(p) for n, p in DOC_NUM.items()}     # for "Doc NN" linkify

SHORT = {
 "00":"Vision & principles","01":"Data model & ranges","02":"Pillars & markers","03":"Scoring formula",
 "04":"Reservoir dynamics","05":"Sex-specific models","06":"Acute events & life-stage","07":"Daily nudge engine",
 "08":"Clinical scores","09":"Cohorts & validation","10":"Actuarial & insurance","11":"Safety & governance",
 "12":"PureScore 2.0","13":"Validation harness","14":"Evidence registry","15":"UAE localization",
 "16":"Actions catalogue","17":"Clinician red-team","18":"Data streams & experience",
}

# ----------------------------------------------------------------- markdown -> html
def esc(s):
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

def linkify(s):
    def rep(m):
        n = m.group(1).zfill(2)
        return ('<a class="xref" href="%s">Doc %s</a>' % (DOCMAP[n], m.group(1))) if n in DOCMAP else m.group(0)
    s = re.sub(r"\bDoc (\d{1,2})\b", rep, s)
    s = re.sub(r"\bREADME\b", '<a class="xref" href="conventions.html">README</a>', s)
    return s

def inline(t):
    codes = []
    def stash(m):
        codes.append("<code>" + esc(m.group(1)) + "</code>")
        return "\x00%d\x01" % (len(codes) - 1)
    t = re.sub(r"`([^`]*)`", stash, t)          # protect code spans, so bold/italic can span them
    s = esc(t)
    s = linkify(s)
    s = re.sub(r"\[([^\]]+)\]\(([^)]+)\)", r'<a href="\2">\1</a>', s)
    s = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", s)
    s = re.sub(r"(?<![\*\w])\*([^*\n]+?)\*(?!\*)", r"<em>\1</em>", s)
    s = re.sub(r"\x00(\d+)\x01", lambda m: codes[int(m.group(1))], s)
    return s

def slug(t):
    t = re.sub(r"[`*]", "", t)
    return re.sub(r"[^a-z0-9]+", "-", t.lower()).strip("-")[:60] or "s"

def split_row(line):
    line = line.strip()
    if line.startswith("|"): line = line[1:]
    if line.endswith("|"): line = line[:-1]
    return [c.strip() for c in line.split("|")]

def is_sep(line):
    return bool(re.match(r"^\s*\|?[\s:\-|]+\|?\s*$", line)) and "-" in line

def build_list(items):
    levels = sorted(set(it[0] for it in items))
    lvl = {v: i for i, v in enumerate(levels)}
    out, stack = [], []
    for indent, ordered, text in items:
        d = lvl[indent]; tag = "ol" if ordered else "ul"
        while stack and stack[-1][1] > d: out.append("</li></%s>" % stack.pop()[0])
        if stack and stack[-1][1] == d: out.append("</li>")
        if not stack or stack[-1][1] < d:
            out.append("<%s>" % tag); stack.append((tag, d))
        out.append("<li>" + inline(text))
    while stack: out.append("</li></%s>" % stack.pop()[0])
    return "".join(out)

def md_to_html(md):
    lines = md.split("\n"); n = len(lines); i = 0; out = []; h2 = []
    def special(idx):
        ln = lines[idx]
        if ln.strip() == "" : return True
        if re.match(r"^#{1,6}\s", ln): return True
        if ln.startswith(">"): return True
        if ln.startswith("```"): return True
        if re.match(r"^---+\s*$", ln): return True
        if re.match(r"^\s*([-*]|\d+\.)\s", ln): return True
        if "|" in ln and idx + 1 < n and is_sep(lines[idx + 1]): return True
        return False
    while i < n:
        ln = lines[i]
        if ln.startswith("```"):
            buf = []; i += 1
            while i < n and not lines[i].startswith("```"): buf.append(lines[i]); i += 1
            i += 1
            out.append('<pre class="code"><code>' + esc("\n".join(buf)) + "</code></pre>"); continue
        if ln.strip() == "": i += 1; continue
        m = re.match(r"^(#{1,6})\s+(.*)$", ln)
        if m:
            lv = len(m.group(1)); tx = m.group(2).rstrip("#").strip(); an = slug(tx)
            if lv == 2: h2.append((an, tx))
            out.append('<h%d id="%s">%s</h%d>' % (lv, an, inline(tx), lv)); i += 1; continue
        if re.match(r"^---+\s*$", ln): out.append("<hr>"); i += 1; continue
        if ln.startswith(">"):
            buf = []
            while i < n and lines[i].startswith(">"):
                buf.append(re.sub(r"^>\s?", "", lines[i])); i += 1
            paras, cur = [], []
            for b in buf:
                if b.strip() == "":
                    if cur: paras.append(" ".join(cur)); cur = []
                else: cur.append(b)
            if cur: paras.append(" ".join(cur))
            out.append("<blockquote>" + "".join("<p>" + inline(p) + "</p>" for p in paras) + "</blockquote>"); continue
        if "|" in ln and i + 1 < n and is_sep(lines[i + 1]):
            head = split_row(ln); i += 2; rows = []
            while i < n and "|" in lines[i] and lines[i].strip(): rows.append(split_row(lines[i])); i += 1
            th = "".join("<th>" + inline(c) + "</th>" for c in head)
            body = ""
            for r in rows:
                r = (r + [""] * len(head))[:len(head)]
                body += "<tr>" + "".join("<td>" + inline(c) + "</td>" for c in r) + "</tr>"
            out.append('<div class="tablewrap"><table><thead><tr>%s</tr></thead><tbody>%s</tbody></table></div>' % (th, body)); continue
        if re.match(r"^\s*([-*]|\d+\.)\s", ln):
            items = []
            while i < n:
                mm = re.match(r"^(\s*)([-*]|\d+\.)\s+(.*)$", lines[i])
                if mm:
                    items.append([len(mm.group(1)), mm.group(2).endswith("."), mm.group(3)]); i += 1
                elif lines[i].strip() and items and (len(lines[i]) - len(lines[i].lstrip())) >= 2:
                    items[-1][2] += " " + lines[i].strip(); i += 1
                else: break
            out.append(build_list(items)); continue
        buf = [ln]; i += 1
        while i < n and not special(i): buf.append(lines[i]); i += 1
        out.append("<p>" + inline(" ".join(buf)) + "</p>")
    return "".join(out), h2

# ----------------------------------------------------------------- navigation
# Progressive "book" arc — a newcomer can read top-to-bottom: orient → how scoring works →
# inputs → personalize → act → trust & govern → see the system → build → reference/tools.
# Each chapter has a one-line blurb (NAV_BLURB). Doc number badges float; identity preserved.
NAV = [
 ("Start here", [("index.html", "Home"), ("purescore-uber-map.html", "How it all connects"),
                 (DOCMAP["00"], SHORT["00"], "00"), ("conventions.html", "Conventions & glossary"),
                 ("decisions.html", "Decision log")]),
 ("1 · How scoring works", [("purescore-overview.html", "Overview"),
                 (DOCMAP["02"], SHORT["02"], "02"), (DOCMAP["03"], SHORT["03"], "03"),
                 (DOCMAP["04"], SHORT["04"], "04"), (DOCMAP["12"], SHORT["12"], "12")]),
 ("2 · The inputs (what feeds it)", [(DOCMAP["01"], SHORT["01"], "01"), (DOCMAP["18"], SHORT["18"], "18"),
                 ("appendix-biomarkers.html", "A · Markers (all channels)"), ("appendix-wearables.html", "B · Wearables"),
                 ("purescore-wearable-baselines.html", "Baselines (Wearables)"),
                 ("questions-hub.html", "Intake — overview"), ("appendix-onboarding.html", "K · Onboarding & first-run"),
                 ("appendix-questions.html", "C · Screeners & PROs"), ("appendix-question-bank.html", "E · Question bank"),
                 ("appendix-lifestyles.html", "F · Lifestyles"), ("appendix-wearable-corroboration.html", "L · Wearable corroboration"),
                 ("eligibility-gating.html", "Eligibility & gating")]),
 ("3 · Making it personal", [(DOCMAP["05"], SHORT["05"], "05"), (DOCMAP["06"], SHORT["06"], "06"),
                 (DOCMAP["08"], SHORT["08"], "08"), ("appendix-personas.html", "D · Personas"),
                 ("appendix-persona-matrix.html", "I · Persona matrix"),
                 ("purescore-male.html", "PureScore — Male"), ("purescore-female.html", "PureScore — Female")]),
 ("4 · Acting on it", [(DOCMAP["07"], SHORT["07"], "07"), (DOCMAP["16"], SHORT["16"], "16"),
                 ("appendix-adherence.html", "H · Adherence"), ("appendix-goals.html", "J · Goals"),
                 ("feedback-loop.html", "Feedback-loop demo")]),
 ("5 · Trust & govern", [(DOCMAP["09"], SHORT["09"], "09"), (DOCMAP["13"], SHORT["13"], "13"),
                 (DOCMAP["14"], SHORT["14"], "14"), (DOCMAP["11"], SHORT["11"], "11"),
                 (DOCMAP["17"], SHORT["17"], "17"), (DOCMAP["15"], SHORT["15"], "15"),
                 (DOCMAP["10"], SHORT["10"], "10"), ("appendix-coverage-audit.html", "G · Coverage audit")]),
 ("6 · See the system (maps)", [("purescore-dataflow.html", "PureScore calculation DFD"),
                 ("states.html", "Patient life-state machine"), ("engagement-state-machines.html", "Engagement state machines"),
                 ("behemoth-class-diagram.html", "Behemoth class diagram"), ("wearable-baselines.html", "Wearable baselines (mobile)")]),
 ("7 · Build it (engineering)", [("class-explorer.html", "Class explorer"), ("dossier-erd.html", "Data model (ERD)"),
                 ("dossier-c4.html", "C4 architecture"), ("dossier-api.html", "API contracts"),
                 ("dossier-sequences.html", "Sequences"), ("dossier-stories.html", "User stories")]),
 ("8 · Reference — data tables", [("grid-questions.html", "Question bank"), ("grid-biomarkers.html", "Biomarkers"),
                 ("grid-wearables.html", "Wearables"), ("grid-personas.html", "Personas"),
                 ("grid-lifestyles.html", "Lifestyles"), ("grid-adherence.html", "Adherence"),
                 ("grid-goals.html", "Goals"), ("grid-persona-matrix.html", "Persona matrix"),
                 ("grid-onboarding.html", "Onboarding"), ("grid-wearable-corroboration.html", "Wearable corroboration"),
                 ("grid-erd.html", "Data model (ERD)")]),
 ("9 · Doctor's board (admin)", [("admin-index.html", "Dashboard"), ("admin-lab-ranges.html", "Lab ranges"),
                 ("admin-weights.html", "Weights & constants"), ("admin-lifestyle.html", "Lifestyle / PRO"),
                 ("admin-personas.html", "Personas & frames"), ("admin-governance.html", "Governance & sign-off")]),
]
NAV_BLURB = {
 "Start here": "Orient yourself — what PureScore is and how the pieces connect.",
 "1 · How scoring works": "The core engine: pillars → markers → score, reservoirs, and PureScore 2.0.",
 "2 · The inputs (what feeds it)": "Where the data comes from: labs, wearables, and the question intake.",
 "3 · Making it personal": "Context that reshapes the score: sex, life-stage, conditions, personas.",
 "4 · Acting on it": "Turning the score into nudges, actions, adherence and goals.",
 "5 · Trust & govern": "Validation, safety, evidence, fairness, localization and the audit.",
 "6 · See the system (maps)": "Diagrams that show the whole machine end-to-end.",
 "7 · Build it (engineering)": "The production object model: classes, ERD, C4, API, stories.",
 "8 · Reference — data tables": "Sortable/filterable spreadsheets of every catalogue.",
 "9 · Doctor's board (admin)": "The clinician configuration & sign-off surfaces.",
}

# prev/next follows the sidebar reading order exactly (derived from NAV)
ORDER = [it[0] for grp, items in NAV for it in items]
PTITLE = {"index.html":"Home","conventions.html":"Conventions & glossary","decisions.html":"Decision log",
          "appendix-biomarkers.html":"Appendix A · Markers (all channels)","appendix-wearables.html":"Appendix B · Wearables",
          "appendix-questions.html":"Appendix C · Screeners & PROs","appendix-personas.html":"Appendix D · Personas",
          "appendix-question-bank.html":"Appendix E · Question bank","appendix-lifestyles.html":"Appendix F · Lifestyles",
          "appendix-coverage-audit.html":"Appendix G · Coverage audit","appendix-adherence.html":"Appendix H · Adherence",
          "appendix-persona-matrix.html":"Appendix I · Persona matrix","appendix-goals.html":"Appendix J · Goals",
          "questions-hub.html":"Questions & intake","eligibility-gating.html":"Eligibility & gating",
          "appendix-onboarding.html":"Appendix K · Onboarding & first-run","grid-onboarding.html":"Onboarding grid",
          "appendix-wearable-corroboration.html":"Appendix L · Wearable corroboration","grid-wearable-corroboration.html":"Wearable-corroboration grid",
          "admin-index.html":"Admin · Dashboard","admin-lab-ranges.html":"Admin · Lab ranges",
          "admin-weights.html":"Admin · Weights","admin-lifestyle.html":"Admin · Lifestyle",
          "admin-personas.html":"Admin · Personas","admin-governance.html":"Admin · Governance",
          "feedback-loop.html":"Live feedback-loop demo","states.html":"Patient life-state machine",
          "behemoth-class-diagram.html":"Behemoth Class Diagram",
          "engagement-state-machines.html":"Engagement state machines",
          "purescore-dataflow.html":"PureScore calculation — data flow",
          "purescore-uber-map.html":"PureScore uber-map",
          "wearable-baselines.html":"Wearable Baselines",
          "purescore-overview.html":"PureScore · Overview","purescore-wearable-baselines.html":"Baselines (Wearables)",
          "purescore-male.html":"PureScore — Male","purescore-female.html":"PureScore — Female",
          "class-explorer.html":"Class Explorer","dossier-sequences.html":"Sequences",
          "dossier-erd.html":"Data model (ERD)","dossier-c4.html":"C4 architecture",
          "dossier-api.html":"API contracts","dossier-stories.html":"User stories",
          "grid-biomarkers.html":"Biomarkers grid","grid-wearables.html":"Wearables grid","grid-personas.html":"Personas grid",
          "grid-lifestyles.html":"Lifestyles grid","grid-adherence.html":"Adherence grid","grid-goals.html":"Goals grid",
          "grid-persona-matrix.html":"Persona-matrix grid","grid-questions.html":"Question-bank grid","grid-erd.html":"Data-model grid"}
for n in DOCMAP: PTITLE[DOCMAP[n]] = "Doc %s · %s" % (n, SHORT[n])

def sidebar(active):
    s = ['<aside class="side">',
         '<div class="side-tools"><button class="side-allbtn" data-act="expand">expand all</button>'
         '<button class="side-allbtn" data-act="collapse">collapse all</button></div>']
    for gi, (grp, items) in enumerate(NAV):
        is_active = active in [it[0] for it in items]
        s.append('<div class="navgrp%s" data-grp="g%d">' % (" open" if is_active else "", gi))
        s.append('<button class="grp-h" aria-expanded="%s"><span class="grp-car">▸</span>'
                 '<span class="grp-t">%s</span></button>' % ("true" if is_active else "false", esc(grp)))
        s.append('<div class="grp-b">')
        blurb = NAV_BLURB.get(grp, "")
        if blurb:
            s.append('<div class="grp-blurb">%s</div>' % esc(blurb))
        for it in items:
            href, label = it[0], it[1]
            num = ('<span class="n">%s</span>' % it[2]) if len(it) > 2 else ""
            s.append('<a href="%s"%s>%s%s</a>' % (href, ' class="active"' if href == active else "", num, label))
        s.append('</div></div>')
    s.append("</aside>")
    return "".join(s)

MERMAID_HEAD = ('<script type="module">import mermaid from '
  '"https://cdn.jsdelivr.net/npm/mermaid@11/dist/mermaid.esm.min.mjs";'
  'mermaid.initialize({startOnLoad:true,theme:"dark",securityLevel:"loose",'
  'flowchart:{htmlLabels:true,curve:"basis"},themeVariables:{fontSize:"14px"}});</script>')

def fix_mermaid(html):
    def f(m):
        inner = m.group(1).replace("<br/>", " · ").replace("<br>", " · ").replace("&amp;", "&")
        return '<pre class="mermaid">' + inner + "</pre>"
    return re.sub(r'<pre class="mermaid">(.*?)</pre>', f, html, flags=re.S)

def prevnext(fn):
    try: idx = ORDER.index(fn)
    except ValueError: return ""
    parts = ['<nav class="pn">']
    if idx > 0:
        p = ORDER[idx - 1]
        parts.append('<a class="prev" href="%s"><div class="lbl">← Previous</div><div class="ttl">%s</div></a>' % (p, PTITLE.get(p, p)))
    else:
        parts.append('<a class="prev disabled"></a>')
    if idx < len(ORDER) - 1:
        nx = ORDER[idx + 1]
        parts.append('<a class="next" href="%s"><div class="lbl">Next →</div><div class="ttl">%s</div></a>' % (nx, PTITLE.get(nx, nx)))
    else:
        parts.append('<a class="next disabled"></a>')
    parts.append("</nav>")
    return "".join(parts)

def _chapter_strip(fn):
    """A 'what's in this chapter' context banner shown at the top of every content page."""
    if fn == "index.html":
        return ""
    for g, items in NAV:
        hrefs = [it[0] for it in items]
        if fn in hrefs:
            blurb = NAV_BLURB.get(g, "")
            sibs = "".join('<a class="chx-l%s" href="%s">%s</a>'
                           % (" cur" if it[0] == fn else "", it[0], esc(it[1])) for it in items)
            return ('<div class="chapter-ctx"><div class="chx-top"><span class="chx-name">%s</span>'
                    '<span class="chx-blurb">%s</span></div>'
                    '<details class="chx-d"><summary>%d pages in this chapter</summary>'
                    '<nav class="chx-list">%s</nav></details></div>'
                    % (esc(g), esc(blurb), len(items), sibs))
    return ""

def page(fn, tab_title, body):
    html = ("""<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>%s — HikmaEngine Tech Wiki</title>
<link rel="stylesheet" href="assets/wiki.css">
<style>.topbar .brand b{color:#2ecc71}</style>
%s</head><body>
<div class="topbar"><button class="menu-btn" aria-label="menu">☰</button>
<a class="brand" href="index.html">Hikma<b>Engine</b> · Tech Wiki</a>
<span class="tag">design spec — not clinically validated</span><span class="grow"></span>
<input id="search" type="search" placeholder="Filter pages…  ( / )"></div>
<div class="shell">%s<main class="main">%s%s%s<footer class="wf">HikmaEngine Tech Wiki · generated from <code>docs/purescore</code> · """
"""illustrative design, re-verify before production (README §5.6). Diagrams render via mermaid (CDN).</footer></main></div>
<script src="assets/wiki.js"></script></body></html>""") % (
        esc(tab_title), MERMAID_HEAD, sidebar(fn), _chapter_strip(fn), body, prevnext(fn))
    with open(os.path.join(HERE, fn), "w", encoding="utf-8") as f:
        f.write(fix_mermaid(html))

# ----------------------------------------------------------------- doc pages
def crumbs_for(num):
    grp = next((g for g, items in NAV for it in items if len(it) > 2 and it[2] == num), "Docs")
    return '<div class="crumbs"><a href="index.html">Home</a> › %s › Doc %s</div>' % (esc(grp), num)

def render_doc(num):
    md = open(DOC_NUM[num], encoding="utf-8").read()
    html, h2 = md_to_html(md)
    cap, code = C.MERMAID.get(num, ("", ""))
    diagram = ('<div class="diagram"><div class="dt">%s</div><pre class="mermaid">%s</pre></div>'
               % (esc(cap), code)) if code else ""
    summ = ('<p class="lead">%s</p>' % esc(C.SUMMARY.get(num, ""))) if C.SUMMARY.get(num) else ""
    inject = summ + diagram
    pos = html.find("</h1>")
    body = crumbs_for(num) + (html[:pos + 5] + inject + html[pos + 5:] if pos >= 0 else inject + html)
    page(DOCMAP[num], "Doc %s · %s" % (num, SHORT[num]), body)

def render_simple(mdname, fn, tab, crumb_label):
    md = open(os.path.join(SRC, mdname), encoding="utf-8").read()
    html, _ = md_to_html(md)
    body = '<div class="crumbs"><a href="index.html">Home</a> › %s</div>' % esc(crumb_label) + html
    page(fn, tab, body)

# ----------------------------------------------------------------- index landing
def render_index():
    try: NQ = len(C._load("question-bank.json").get("questions", []))
    except Exception: NQ = 0
    flow = """flowchart TD
  R["README · conventions"] --> D0["00 Vision"]
  D0 --> D1["01 Data model"] --> D2["02 Pillars & markers"] --> D3["03 Scoring"] --> D4["04 Reservoirs"]
  D3 --> D12["12 PureScore 2.0"]
  D2 --> D5["05 Sex-specific"]
  D3 --> D7["07 Nudge engine"]
  D12 --> D13["13 Validation"]
  D1 --> D18["18 Data streams"] --> D7
  D7 --> D16["16 Actions"]
  D3 --> D11["11 Safety"]
  D18 --> ADMIN["Doctor's board (admin)"]
  D7 --> FB["Feedback-loop demo"]"""
    b = ['<div class="hero"><h1>Hikma<span style="color:#2ecc71">Engine</span> — Technical Wiki</h1>',
         '<p class="lead">The HikmaEngine health-intelligence platform: data &amp; intake, the '
         '<a href="purescore-overview.html"><b>PureScore</b> scoring engine</a> (one subsystem — pillars, '
         'reservoirs, metrics &amp; weights), interpretation &amp; personalization, action &amp; engagement, and '
         'validation/governance — plus reference appendices, the engineering build and live demos. Start at '
         '<a href="00-vision-principles-and-lessons.html">Doc 00</a>, the '
         '<a href="purescore-overview.html">PureScore Overview</a>, or jump anywhere.</p></div>',
         ('<div class="section-h">Where do I start?</div><div class="grid c3 roles">'
          '<a class="card role" href="00-vision-principles-and-lessons.html"><div class="role-i">🧭</div>'
          '<h3>New here</h3><p>Read the book front-to-back. Start with the vision, then the Overview and the system map.</p>'
          '<div class="role-links"><a href="purescore-overview.html">Overview</a>'
          '<a href="purescore-uber-map.html">How it all connects</a><a href="03-scoring-formula.html">Scoring formula</a></div></a>'
          '<a class="card role" href="admin-index.html"><div class="role-i">🩺</div>'
          '<h3>Clinician</h3><p>How the score is kept safe, contextual and reviewable. Jump to the Doctor\'s board.</p>'
          '<div class="role-links"><a href="11-safety-governance-and-regulatory.html">Safety &amp; governance</a>'
          '<a href="08-clinical-scores-integration.html">Clinical scores</a><a href="appendix-coverage-audit.html">Coverage audit</a></div></a>'
          '<a class="card role" href="class-explorer.html"><div class="role-i">🛠️</div>'
          '<h3>Engineer</h3><p>The production object model and contracts. Start with the class explorer.</p>'
          '<div class="role-links"><a href="dossier-erd.html">Data model (ERD)</a>'
          '<a href="dossier-c4.html">C4 architecture</a><a href="dossier-api.html">API contracts</a></div></a></div>'),
         C.ILLUS,
         '<div class="diagram"><div class="dt">How the documents connect</div><pre class="mermaid">%s</pre></div>' % flow]
    # doc-card sections are driven by the NAV pipeline groups, so the landing page stays in
    # sync with the sidebar automatically (only groups whose items carry a Doc number).
    for grp, items in NAV:
        nums = [it[2] for it in items if len(it) > 2]
        if not nums: continue
        b.append('<div class="section-h">%s</div><div class="grid c3">' % grp.replace("&", "&amp;"))
        for n in nums:
            b.append('<a class="card" href="%s"><span class="n">Doc %s</span><h3>%s</h3><p>%s</p></a>'
                     % (DOCMAP[n], n, SHORT[n], esc(C.SUMMARY[n])))
        b.append('</div>')
    b.append('<div class="section-h">Reference appendices</div><div class="grid c4">')
    for href, t, d in [("appendix-biomarkers.html","Markers (all channels)","Every marker across labs · wearables · self-report"),
                       ("appendix-wearables.html","Wearable metrics","Layers, trust tiers (D22), pillars"),
                       ("appendix-personas.html","Personas","Cohort frames & edge cases"),
                       ("appendix-coverage-audit.html","Coverage audit","Product-loop gaps: persona, goals, adherence")]:
        b.append('<a class="card" href="%s"><h3>%s</h3><p>%s</p></a>' % (href, t, d))
    b.append('</div><div class="section-h">Questions &amp; intake</div><div class="grid c4">')
    for href, t, d in [("questions-hub.html","Overview","5 content sections × 3 phases — the unified question layer"),
                       ("appendix-questions.html","Questionnaires (C)","PHQ-9, GAD-7, AUDIT-C, ISI… full items"),
                       ("appendix-question-bank.html","Question bank (E)","%d items grouped by section × phase" % NQ),
                       ("appendix-lifestyles.html","Lifestyles (F)","Axes, archetypes, perceived-vs-actual"),
                       ("appendix-adherence.html","Adherence (H)","41 EHR-triggered micro check-ins"),
                       ("appendix-persona-matrix.html","Persona matrix (I)","Signals → persona posterior"),
                       ("appendix-goals.html","Goals (J)","Catalogue keyed by applicability"),
                       ("eligibility-gating.html","Eligibility & gating","Rules connecting onboarding → questions")]:
        b.append('<a class="card" href="%s"><h3>%s</h3><p>%s</p></a>' % (href, t, d))
    b.append('</div><div class="section-h">Doctor\'s board — quarterly configuration review</div><div class="grid c3">')
    for href, t, d in [("admin-index.html","Dashboard","All config domains &amp; review status"),
                       ("admin-lab-ranges.html","Lab ranges","Band review + guideline-match"),
                       ("admin-weights.html","Weights &amp; constants","Pillar weights, κ, imputation"),
                       ("admin-lifestyle.html","Lifestyle / PRO","Instruments &amp; scoring"),
                       ("admin-personas.html","Personas & frames","Cohort overrides"),
                       ("admin-governance.html","Governance & sign-off","Workflow, gates, audit")]:
        b.append('<a class="card" href="%s"><h3>%s</h3><p>%s</p></a>' % (href, t, d))
    b.append('</div><div class="section-h">Diagrams &amp; system maps</div><div class="grid c3">'
             '<a class="card" href="behemoth-class-diagram.html"><h3>Behemoth class diagram</h3>'
             '<p>The full PureScore object model — markers, pillars, reservoirs, engine and feedback loop — in one class diagram.</p></a>'
             '<a class="card" href="states.html"><h3>Patient life-state machine</h3>'
             '<p>An interactive statechart of every life state a patient can occupy — a concurrent vector across Care status, '
             'Life stage and Engagement, with drill-down sub-machines (acute, chronic, pregnancy, aging, onboarding), '
             'filtered by sex, age, condition and persona.</p></a>'
             '<a class="card" href="purescore-uber-map.html"><h3>PureScore uber-map</h3>'
             '<p>Interactive full-lifecycle map — pan/zoom, click a box for its calc, 10 sample profiles with step-through animation.</p></a>'
             '<a class="card" href="purescore-dataflow.html"><h3>PureScore calculation — data flow</h3>'
             '<p>Leveled data-flow diagram of the full marker→pillar→score pipeline with every decision gate and rule.</p></a>'
             '<a class="card" href="engagement-state-machines.html"><h3>Engagement state machines</h3>'
             '<p>Technical lifecycle states for nudges, check-ins, self-reports and adherence checks — Mermaid state diagrams with transition tables and data fields.</p></a></div>'
             '<div class="section-h">Demos &amp; visual prototypes</div><div class="grid c3">'
             '<a class="card" href="feedback-loop.html"><h3>Live feedback-loop demo</h3>'
             '<p>Tap the top-5 actions or log a bad night and watch PureScore and the companion dimensions respond '
             '— the continuous, personalized scoring of D23 / Doc 03 §2b in motion.</p></a>'
             '<a class="card" href="wearable-baselines.html"><h3>Wearable Baselines</h3>'
             '<p>Mobile baseline-band visualizations: category summaries with composite bands, data gaps and anomalies, drilling into per-metric detail.</p></a></div>')
    b.append(('<div class="section-h">Spreadsheets</div>'
             '<p class="lead" style="margin:-4px 0 10px">Flat, sortable &amp; per-column-filterable grids of the catalog data '
             '— biomarkers, wearables, personas, lifestyles, adherence, goals, persona-matrix, the %d-item question bank and the data model.</p>'
             '<div class="grid c3">') % NQ)
    for href, t, d in [("grid-questions.html","Question bank grid","%d questions · sort/filter every column" % NQ),
                       ("grid-biomarkers.html","Biomarkers grid","Every marker · pillar, bands, weight, source"),
                       ("grid-goals.html","Goals grid","Goals by applicability · persona, target, modifiability"),
                       ("grid-adherence.html","Adherence grid","Check-ins · family, reservoir, responses"),
                       ("grid-persona-matrix.html","Persona-matrix grid","Signal×persona weights (long form)"),
                       ("grid-erd.html","Data-model grid","Every entity column across the ERD")]:
        b.append('<a class="card" href="%s"><h3>%s</h3><p>%s</p></a>' % (href, t, d))
    b.append('</div>')
    page("index.html", "Home", "".join(b))

# ----------------------------------------------------------------- main
def main():
    C.write_calc_data()   # generate assets/calc-data.js from canonical JSON (engine source)
    render_index()
    for n in sorted(DOCMAP): render_doc(n)
    render_simple("README.md", "conventions.html", "Conventions & glossary", "Conventions & glossary")
    render_simple("decisions.md", "decisions.html", "Decision log", "Decision log")
    # builders resolved by name at call time so a not-yet-present builder (multi-agent edits) is skipped, not fatal
    for fn, bname in [("appendix-biomarkers.html", "build_biomarkers"), ("appendix-wearables.html", "build_wearables"),
                      ("appendix-questions.html", "build_questions"), ("appendix-personas.html", "build_personas"),
                      ("appendix-question-bank.html", "build_question_bank"), ("appendix-lifestyles.html", "build_lifestyles"),
                      ("appendix-coverage-audit.html", "build_coverage_audit"),
                      ("appendix-adherence.html", "build_adherence"), ("appendix-persona-matrix.html", "build_persona_matrix"),
                      ("appendix-goals.html", "build_goals"),
                      ("questions-hub.html", "build_questions_hub"), ("eligibility-gating.html", "build_eligibility"),
                      ("appendix-onboarding.html", "build_onboarding"), ("grid-onboarding.html", "build_grid_onboarding"),
                      ("appendix-wearable-corroboration.html", "build_wearable_corroboration"),
                      ("grid-wearable-corroboration.html", "build_grid_wearable_corroboration"),
                      ("behemoth-class-diagram.html", "build_behemoth"),
                      ("purescore-dataflow.html", "build_purescore_dataflow"),
                      ("purescore-uber-map.html", "build_purescore_uber"),
                      ("purescore-overview.html", "build_purescore_overview"),
                      ("purescore-wearable-baselines.html", "build_wearable_baselines"),
                      ("purescore-male.html", "build_purescore_male"), ("purescore-female.html", "build_purescore_female"),
                      ("class-explorer.html", "build_class_explorer"), ("dossier-sequences.html", "build_sequences"),
                      ("dossier-erd.html", "build_erd"), ("dossier-c4.html", "build_c4"),
                      ("dossier-api.html", "build_api"), ("dossier-stories.html", "build_stories"),
                      ("grid-biomarkers.html", "build_grid_biomarkers"), ("grid-wearables.html", "build_grid_wearables"),
                      ("grid-personas.html", "build_grid_personas"), ("grid-lifestyles.html", "build_grid_lifestyles"),
                      ("grid-adherence.html", "build_grid_adherence"), ("grid-goals.html", "build_grid_goals"),
                      ("grid-persona-matrix.html", "build_grid_persona_matrix"), ("grid-questions.html", "build_grid_questions"),
                      ("grid-erd.html", "build_grid_erd")]:
        build = getattr(C, bname, None)
        if build is None:
            print("  ! skip %s — wiki_content.%s not defined yet" % (fn, bname)); continue
        tab, body = build(); page(fn, tab, body)
    for fn, build in A.ADMIN_PAGES:
        tab, body = build(); page(fn, tab, body)
    built = len(ORDER)
    for fn, tab in [("feedback-loop.html", "Live feedback-loop demo"), ("states.html", "Patient state machine"),
                    ("engagement-state-machines.html", "Engagement state machines")]:
        body = os.path.join(HERE, fn[:-5] + ".body.html")
        if os.path.exists(body):
            page(fn, tab, open(body, encoding="utf-8").read())
        else:
            built -= 1
    print("Generated %d pages → %s" % (built, HERE))
    _inject_connects()
    _consistency_check()
    _engine_guard()

def _engine_guard():
    """HARD guard (per D32): data/*.json is canonical for the scoring engine. Fail the build if
    weights don't sum to 1, calc-graph refs don't resolve, assets/calc-data.js is stale vs the
    JSON, or any generated page reintroduces a hardcoded 'island' scorer (the class of bug that
    let the uber-map drift to CV=0.14 while pillar-weights.json said 0.13)."""
    errs = []
    w = [v for _, v in C._load("pillar-weights.json")["weights"]]
    if abs(sum(w) - 1.0) > 1e-6:
        errs.append("pillar weights sum to %.4f, not 1.0" % sum(w))
    resolved = None
    try:
        resolved = C.resolve_calc_data()
    except Exception as e:
        errs.append("calc-graph.json references unresolved: %s" % e)
    cdp = os.path.join(HERE, "assets", "calc-data.js")
    if resolved is not None and os.path.exists(cdp):
        want = "window.PURESCORE_DATA=" + json.dumps(resolved, ensure_ascii=False, separators=(",", ":"))
        if want not in open(cdp, encoding="utf-8").read():
            errs.append("assets/calc-data.js is stale vs data/*.json — rerun build_wiki.py")
    banned = ["var TH={", "var PILLMETA=", "function scoreProfile", "var PILL={cv:"]
    for fn in sorted(glob.glob(os.path.join(HERE, "*.html"))):
        h = open(fn, encoding="utf-8").read()
        for b in banned:
            if b in h:
                errs.append("%s reintroduces a hardcoded scorer (%r) — use assets/engine.js" % (os.path.basename(fn), b))
    if errs:
        for e in errs:
            print("  [engine-guard] FAIL: %s" % e)
        raise SystemExit("  [engine-guard] %d JSON-canonical violation(s) — build aborted" % len(errs))
    print("  [engine-guard] OK — engine values are canonical from data/*.json")

def _consistency_check():
    """Build-time guard: warn if any generated page's count claims drift from the live data."""
    try:
        nq = len(C._load("question-bank.json").get("questions", []))
        pm = C._load("persona-matrix.json")
        nclin = sum(1 for p in pm["personas"] if p.get("type") == "clinical")
        narch = len(pm["personas"]) - nclin; nptot = len(pm["personas"])
        nadh = len(C._load("adherence.json").get("items", []))
        ngoal = len(C._load("goals.json").get("goals", []))
    except Exception as e:
        print("  [consistency] skipped: %s" % e); return
    checks = [  # (regex w/ number group(s), expected, label)
        (r"(\d+)[- ]item question bank", nq, "question count"),
        (r"All (\d+) questions", nq, "question count"),
        (r"(\d+) questions · sort", nq, "question count"),
        (r"(\d+) items grouped by section", nq, "question count"),
        (r"(\d+) clinical [×x] (\d+) archetype", (nclin, narch), "persona split"),
        (r"(\d+) personas \((\d+) clinical \+ (\d+) lifestyle", (nptot, nclin, narch), "persona split"),
        (r"ships (\d+) adherence", nadh, "adherence count"),
        (r"ships (\d+) goals", ngoal, "goals count"),
    ]
    warn = 0
    for fn in sorted(glob.glob(os.path.join(HERE, "*.html"))):
        h = open(fn, encoding="utf-8").read(); base = os.path.basename(fn)
        for pat, exp, label in checks:
            for m in re.finditer(pat, h):
                got = tuple(int(x) for x in m.groups()) if isinstance(exp, tuple) else int(m.group(1))
                if got != exp:
                    print("  [consistency] DRIFT in %s: %s = %s (live %s)" % (base, label, got, exp)); warn += 1
    print("  [consistency] OK — count claims match live data" if not warn else "  [consistency] %d drift(s) above" % warn)

# ----------------------------------------------------------------- "Connects to" footer (auto-derived)
_DIAGRAMS = {"purescore-uber-map.html", "purescore-dataflow.html", "states.html", "engagement-state-machines.html",
             "behemoth-class-diagram.html", "dossier-erd.html", "dossier-c4.html", "dossier-sequences.html",
             "class-explorer.html", "wearable-baselines.html", "purescore-wearable-baselines.html"}
_GRID_OF = {"appendix-biomarkers.html": "grid-biomarkers.html", "appendix-wearables.html": "grid-wearables.html",
            "appendix-personas.html": "grid-personas.html", "appendix-lifestyles.html": "grid-lifestyles.html",
            "appendix-adherence.html": "grid-adherence.html", "appendix-goals.html": "grid-goals.html",
            "appendix-persona-matrix.html": "grid-persona-matrix.html", "appendix-question-bank.html": "grid-questions.html",
            "appendix-onboarding.html": "grid-onboarding.html", "appendix-wearable-corroboration.html": "grid-wearable-corroboration.html",
            "dossier-erd.html": "grid-erd.html"}
_CONNECTS_EXTRA = {
 "index.html": ["purescore-uber-map.html", "purescore-overview.html", DOCMAP["00"]],
 "purescore-overview.html": [DOCMAP["03"], DOCMAP["02"], "purescore-uber-map.html"],
 DOCMAP["02"]: ["appendix-biomarkers.html", "grid-biomarkers.html", DOCMAP["03"]],
 DOCMAP["03"]: ["purescore-dataflow.html", DOCMAP["04"], "behemoth-class-diagram.html"],
 DOCMAP["04"]: [DOCMAP["03"], "behemoth-class-diagram.html", "purescore-dataflow.html"],
 DOCMAP["12"]: ["states.html", "purescore-dataflow.html", DOCMAP["03"]],
 DOCMAP["01"]: ["dossier-erd.html", DOCMAP["18"]],
 DOCMAP["18"]: ["appendix-wearables.html", "appendix-biomarkers.html", "questions-hub.html"],
 DOCMAP["07"]: ["engagement-state-machines.html", "appendix-adherence.html", "appendix-goals.html", DOCMAP["16"]],
 DOCMAP["16"]: [DOCMAP["07"], "appendix-adherence.html"],
 DOCMAP["06"]: ["states.html", DOCMAP["05"]],
 DOCMAP["05"]: ["appendix-personas.html", "purescore-male.html", "purescore-female.html"],
 DOCMAP["11"]: ["states.html", DOCMAP["17"]],
 "appendix-coverage-audit.html": ["questions-hub.html", "appendix-question-bank.html", "states.html"],
 "questions-hub.html": ["appendix-onboarding.html", "appendix-question-bank.html", "eligibility-gating.html"],
 "states.html": [DOCMAP["06"], "appendix-goals.html", DOCMAP["12"]],
 "behemoth-class-diagram.html": ["class-explorer.html", "dossier-erd.html", DOCMAP["03"]],
}
def _navlabel(fn):
    for g, items in NAV:
        for it in items:
            if it[0] == fn:
                return it[1]
    return PTITLE.get(fn, fn)
def _inject_connects():
    files = glob.glob(os.path.join(HERE, "*.html"))
    pages = {os.path.basename(f): open(f, encoding="utf-8").read() for f in files}
    grp_of = {}; sibs_of = {}
    for g, items in NAV:
        hs = [it[0] for it in items]
        for it in items:
            grp_of[it[0]] = g; sibs_of[it[0]] = hs
    out = {}
    for fn, h in pages.items():
        m = re.search(r'<main class="main">(.*)</main>', h, re.S)
        body = m.group(1) if m else h
        out[fn] = set(l for l in re.findall(r'href="([a-z0-9-]+\.html)(?:#[^"]*)?"', body) if l in pages and l != fn)
    inbound = {}
    for fn, ls in out.items():
        for l in ls:
            inbound.setdefault(l, set()).add(fn)
    n = 0
    for fn, h in pages.items():
        if fn not in PTITLE or '<nav class="pn">' not in h:
            continue
        rel, seen = [], set()
        cand = []
        if fn in _GRID_OF: cand.append(_GRID_OF[fn])
        for a, b in _GRID_OF.items():
            if b == fn: cand.append(a)
        cand += _CONNECTS_EXTRA.get(fn, [])
        cand += sorted(out.get(fn, set()), key=lambda x: (-len(inbound.get(x, ())), x))[:6]
        cand += [s for s in sibs_of.get(fn, []) if s != fn][:4]
        for r in cand:
            if r == fn or r in seen or r not in pages:
                continue
            seen.add(r); rel.append(r)
            if len(rel) >= 10:
                break
        if not rel:
            continue
        pg = [r for r in rel if r not in _DIAGRAMS and not r.startswith("grid-")]
        dg = [r for r in rel if r in _DIAGRAMS]
        tb = [r for r in rel if r.startswith("grid-")]
        def _chips(items):
            return " ".join('<a class="cx-chip" href="%s">%s</a>' % (r, esc(_navlabel(r))) for r in items)
        block = '<div class="connects"><div class="cx-h">Connects to</div>'
        if pg: block += '<div class="cx-row"><span class="cx-k">Pages</span>%s</div>' % _chips(pg)
        if dg: block += '<div class="cx-row"><span class="cx-k">Diagrams</span>%s</div>' % _chips(dg)
        if tb: block += '<div class="cx-row"><span class="cx-k">Tables</span>%s</div>' % _chips(tb)
        block += '</div>'
        nh = h.replace('<nav class="pn">', block + '<nav class="pn">', 1)
        if nh != h:
            open(os.path.join(HERE, fn), "w", encoding="utf-8").write(nh); n += 1
    print("  [connects] injected 'Connects to' on %d pages" % n)

if __name__ == "__main__":
    main()
