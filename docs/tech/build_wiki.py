#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""PureScore tech wiki generator. Renders docs/purescore/*.md into an interlinked
multi-page wiki under docs/tech/, plus reference appendices and Doctor's-board admin mockups.

Run:  python3 build_wiki.py   (from docs/tech/)
"""
import os, re, glob
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
NAV = [
 ("Start here", [("index.html", "Home"), ("conventions.html", "Conventions & glossary"), ("decisions.html", "Decision log")]),
 ("Foundations", [(DOCMAP[n], SHORT[n], n) for n in ["00","01","02","03","04"]]),
 ("Models & engine", [(DOCMAP[n], SHORT[n], n) for n in ["05","06","07","08"]]),
 ("Validation & governance", [(DOCMAP[n], SHORT[n], n) for n in ["09","10","11","12","13","14"]]),
 ("UAE, actions & experience", [(DOCMAP[n], SHORT[n], n) for n in ["15","16","17","18"]]),
 ("Reference appendices", [("appendix-biomarkers.html","A · Biomarkers"),("appendix-wearables.html","B · Wearables"),
                           ("appendix-questions.html","C · Questionnaires"),("appendix-personas.html","D · Personas")]),
 ("Doctor's board (admin)", [("admin-index.html","Dashboard"),("admin-lab-ranges.html","Lab ranges"),
                             ("admin-weights.html","Weights & constants"),("admin-lifestyle.html","Lifestyle / PRO"),
                             ("admin-personas.html","Personas & frames"),("admin-governance.html","Governance & sign-off")]),
 ("Interactive", [("feedback-loop.html", "Live feedback-loop demo")]),
]

ORDER = (["index.html"] + [DOCMAP[n] for n in sorted(DOCMAP)] +
         ["conventions.html","decisions.html",
          "appendix-biomarkers.html","appendix-wearables.html","appendix-questions.html","appendix-personas.html",
          "admin-index.html","admin-lab-ranges.html","admin-weights.html","admin-lifestyle.html",
          "admin-personas.html","admin-governance.html","feedback-loop.html"])
PTITLE = {"index.html":"Home","conventions.html":"Conventions & glossary","decisions.html":"Decision log",
          "appendix-biomarkers.html":"Appendix A · Biomarkers","appendix-wearables.html":"Appendix B · Wearables",
          "appendix-questions.html":"Appendix C · Questionnaires","appendix-personas.html":"Appendix D · Personas",
          "admin-index.html":"Admin · Dashboard","admin-lab-ranges.html":"Admin · Lab ranges",
          "admin-weights.html":"Admin · Weights","admin-lifestyle.html":"Admin · Lifestyle",
          "admin-personas.html":"Admin · Personas","admin-governance.html":"Admin · Governance",
          "feedback-loop.html":"Live feedback-loop demo"}
for n in DOCMAP: PTITLE[DOCMAP[n]] = "Doc %s · %s" % (n, SHORT[n])

def sidebar(active):
    s = ['<aside class="side">']
    for grp, items in NAV:
        s.append('<h4>%s</h4>' % grp)
        for it in items:
            href, label = it[0], it[1]
            num = ('<span class="n">%s</span>' % it[2]) if len(it) > 2 else ""
            s.append('<a href="%s"%s>%s%s</a>' % (href, ' class="active"' if href == active else "", num, label))
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

def page(fn, tab_title, body):
    html = ("""<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>%s — PureScore Tech Wiki</title>
<link rel="stylesheet" href="assets/wiki.css">
%s</head><body>
<div class="topbar"><button class="menu-btn" aria-label="menu">☰</button>
<a class="brand" href="index.html">Pure<b>Score</b> · Tech Wiki</a>
<span class="tag">design spec — not clinically validated</span><span class="grow"></span>
<input id="search" type="search" placeholder="Filter pages…  ( / )"></div>
<div class="shell">%s<main class="main">%s%s<footer class="wf">PureScore Tech Wiki · generated from <code>docs/purescore</code> · """
"""illustrative design, re-verify before production (README §5.6). Diagrams render via mermaid (CDN).</footer></main></div>
<script src="assets/wiki.js"></script></body></html>""") % (
        esc(tab_title), MERMAID_HEAD, sidebar(fn), body, prevnext(fn))
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
    b = ['<div class="hero"><h1>PureScore — Technical Wiki</h1>',
         '<p class="lead">A navigable, interlinked rendering of the full PureScore design specification: the 19 '
         'core documents, exhaustive reference appendices (biomarkers, wearables, questionnaires, personas), the '
         'Doctor\'s-board configuration screens, and a live feedback-loop demo. Start at '
         '<a href="00-vision-principles-and-lessons.html">Doc 00</a> or jump anywhere.</p></div>',
         C.ILLUS,
         '<div class="diagram"><div class="dt">How the documents connect</div><pre class="mermaid">%s</pre></div>' % flow,
         '<div class="section-h">Foundations &amp; engine</div><div class="grid c3">']
    for n in ["00","01","02","03","04","05","06","07","08"]:
        b.append('<a class="card" href="%s"><span class="n">Doc %s</span><h3>%s</h3><p>%s</p></a>'
                 % (DOCMAP[n], n, SHORT[n], esc(C.SUMMARY[n])))
    b.append('</div><div class="section-h">Validation, governance, UAE &amp; experience</div><div class="grid c3">')
    for n in ["09","10","11","12","13","14","15","16","17","18"]:
        b.append('<a class="card" href="%s"><span class="n">Doc %s</span><h3>%s</h3><p>%s</p></a>'
                 % (DOCMAP[n], n, SHORT[n], esc(C.SUMMARY[n])))
    b.append('</div><div class="section-h">Reference appendices</div><div class="grid c4">')
    for href, t, d in [("appendix-biomarkers.html","Biomarker catalogue","Every marker, band, tier &amp; weight"),
                       ("appendix-wearables.html","Wearable metrics","Layers, trust tiers (D22), pillars"),
                       ("appendix-questions.html","Questionnaires","PHQ-9, GAD-7, AUDIT-C, ISI… full items"),
                       ("appendix-personas.html","Personas","Cohort frames & edge cases")]:
        b.append('<a class="card" href="%s"><h3>%s</h3><p>%s</p></a>' % (href, t, d))
    b.append('</div><div class="section-h">Doctor\'s board — quarterly configuration review</div><div class="grid c3">')
    for href, t, d in [("admin-index.html","Dashboard","All config domains &amp; review status"),
                       ("admin-lab-ranges.html","Lab ranges","Band review + guideline-match"),
                       ("admin-weights.html","Weights &amp; constants","Pillar weights, κ, imputation"),
                       ("admin-lifestyle.html","Lifestyle / PRO","Instruments &amp; scoring"),
                       ("admin-personas.html","Personas & frames","Cohort overrides"),
                       ("admin-governance.html","Governance & sign-off","Workflow, gates, audit")]:
        b.append('<a class="card" href="%s"><h3>%s</h3><p>%s</p></a>' % (href, t, d))
    b.append('</div><div class="section-h">Interactive</div><div class="grid c2">'
             '<a class="card" href="feedback-loop.html"><h3>Live feedback-loop demo</h3>'
             '<p>Tap the top-5 actions or log a bad night and watch PureScore and the companion dimensions respond '
             '— the continuous, personalized scoring of D23 / Doc 03 §2b in motion.</p></a>'
             '<a class="card" href="conventions.html"><h3>Conventions &amp; glossary</h3>'
             '<p>The binding symbols, the 12 pillars, band semantics and the hard safety non-negotiables.</p></a></div>')
    page("index.html", "Home", "".join(b))

# ----------------------------------------------------------------- main
def main():
    render_index()
    for n in sorted(DOCMAP): render_doc(n)
    render_simple("README.md", "conventions.html", "Conventions & glossary", "Conventions & glossary")
    render_simple("decisions.md", "decisions.html", "Decision log", "Decision log")
    for fn, build in [("appendix-biomarkers.html", C.build_biomarkers), ("appendix-wearables.html", C.build_wearables),
                      ("appendix-questions.html", C.build_questions), ("appendix-personas.html", C.build_personas)]:
        tab, body = build(); page(fn, tab, body)
    for fn, build in A.ADMIN_PAGES:
        tab, body = build(); page(fn, tab, body)
    fbody = os.path.join(HERE, "feedback-loop.body.html")
    if os.path.exists(fbody):
        page("feedback-loop.html", "Live feedback-loop demo", open(fbody, encoding="utf-8").read())
        built = len(ORDER)
    else:
        built = len(ORDER) - 1
    print("Generated %d pages → %s" % (built, HERE))

if __name__ == "__main__":
    main()
