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
 "01":"Vision & principles","06":"Data model & ranges","02":"Pillars & markers","03":"Scoring formula",
 "04":"Reservoir dynamics","08":"Sex-specific models","09":"Acute events & life-stage","11":"Nudge engine & delivery",
 "10":"Clinical scores","13":"Cohorts & validation","19":"Actuarial & insurance","16":"Safety & governance",
 "05":"PureScore 2.0","14":"Validation harness","15":"Evidence registry","18":"UAE localization",
 "12":"Actions catalogue","17":"Clinician red-team","07":"Data streams & experience",
}

# ----------------------------------------------------------------- markdown -> html
def esc(s):
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

def linkify(s):
    def rep(m):
        n = m.group(1).zfill(2)
        return ('<a class="xref" href="%s">Doc %s</a>' % (DOCMAP[n], m.group(1))) if n in DOCMAP else m.group(0)
    s = re.sub(r"\bDoc (\d{1,2})\b", rep, s)
    s = re.sub(r"\bREADME\b", '<a class="xref" href="conventions.html">Conventions</a>', s)
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
 ("Start here", [("index.html", "Home"), ("purescore-uber-map.html", "Calculation Explorer"),
                 (DOCMAP["01"], SHORT["01"], "01"), ("conventions.html", "Conventions & glossary"),
                 ("decisions.html", "Decision log")]),
 ("1 · How scoring works", [("purescore-overview.html", "Overview"),
                 (DOCMAP["02"], SHORT["02"], "02"), (DOCMAP["03"], SHORT["03"], "03"),
                 (DOCMAP["04"], SHORT["04"], "04"), ("reservoir-sim.html", "Reservoir simulator"),
                 (DOCMAP["05"], SHORT["05"], "05")]),
 ("2 · Inputs & intake", [(DOCMAP["06"], SHORT["06"], "06"), (DOCMAP["07"], SHORT["07"], "07"),
                 ("appendix-biomarkers.html", "Markers (all channels)"), ("reference-range-resolver.html", "Reference-range resolver"),
                 ("appendix-wearables.html", "Wearables"),
                 ("purescore-wearable-baselines.html", "Wearable baselines"),
                 ("questions-hub.html", "Intake — overview"), ("appendix-onboarding.html", "Onboarding & first-run"),
                 ("appendix-questions.html", "Screeners & PROs"), ("appendix-question-bank.html", "Question bank"),
                 ("appendix-lifestyles.html", "Lifestyles"), ("appendix-wearable-corroboration.html", "Wearable corroboration"),
                 ("eligibility-gating.html", "Eligibility & gating")]),
 ("3 · Making it personal", [(DOCMAP["08"], SHORT["08"], "08"), (DOCMAP["09"], SHORT["09"], "09"),
                 (DOCMAP["10"], SHORT["10"], "10"), ("appendix-personas.html", "Personas"),
                 ("appendix-persona-matrix.html", "Persona matrix"),
                 ("purescore-sex.html", "PureScore by sex")]),
 ("4 · Acting on it", [(DOCMAP["11"], SHORT["11"], "11"), (DOCMAP["12"], SHORT["12"], "12"),
                 ("appendix-adherence.html", "Adherence"), ("appendix-goals.html", "Goals"),
                 ("feedback-loop.html", "Feedback-loop demo")]),
 ("5 · Care pathways & delivery", [("care-pathways.html", "Condition pathways"),
                 ("care-roles.html", "Care team & roles"),
                 ("prevention-engagement.html", "Prevention & engagement")]),
 ("6 · Trust & govern", [(DOCMAP["13"], SHORT["13"], "13"), (DOCMAP["14"], SHORT["14"], "14"),
                 (DOCMAP["15"], SHORT["15"], "15"), (DOCMAP["16"], SHORT["16"], "16"),
                 ("consent-onboarding.html", "Consent & onboarding"),
                 (DOCMAP["18"], SHORT["18"], "18"),
                 (DOCMAP["19"], SHORT["19"], "19")]),
 ("7 · Gaps, blind-spots & roadmap", [("production-gaps.html", "Production readiness — gaps"),
                 ("spec-audit.html", "Spec build-readiness"),
                 ("editorial-review.html", "Editorial & cohesion review"),
                 (DOCMAP["17"], SHORT["17"], "17"), ("appendix-coverage-audit.html", "Coverage audit")]),
 ("8 · System & build", [("purescore-system.html", "System at a glance"),
                 ("purescore-calc-uber.html", "PureScore Calc Diagram"),
                 ("states.html", "Patient life-state machine"),
                 ("engagement-state-machines.html", "Engagement state machines"),
                 ("class-model.html", "Class model"),
                 ("dossier-erd.html", "Data model (ERD)"), ("dossier-c4.html", "C4 architecture"),
                 ("dossier-api.html", "API contracts"), ("dossier-sequences.html", "Sequences"),
                 ("dossier-stories.html", "User stories")]),
 ("9 · Admin (clinician config)", [("admin-index.html", "Dashboard"), ("admin-lab-ranges.html", "Lab ranges"),
                 ("admin-weights.html", "Weights & constants"), ("admin-lifestyle.html", "Lifestyle / PRO"),
                 ("admin-personas.html", "Personas & frames"), ("admin-governance.html", "Governance & sign-off")]),
]
NAV_BLURB = {
 "Start here": "Orient yourself — what PureScore is and how the pieces connect.",
 "1 · How scoring works": "The core engine: pillars → markers → score, reservoirs, and PureScore 2.0.",
 "2 · Inputs & intake": "Where the data comes from: labs, wearables, the question intake, and coverage.",
 "3 · Making it personal": "Context that reshapes the score: sex, life-stage, conditions, personas.",
 "4 · Acting on it": "Turning the score into nudges, actions, adherence and goals.",
 "5 · Care pathways & delivery": "How the engine plugs into real care: pathways, roles, and prevention/engagement.",
 "6 · Trust & govern": "Validation, safety, evidence, fairness, localization and actuarial.",
 "7 · Gaps, blind-spots & roadmap": "What's missing or partial — product gaps, the spec audit, the clinician red-team and coverage audit.",
 "8 · System & build": "The whole machine end-to-end: state machines, the object model, ERD, C4, API, stories.",
 "9 · Admin (clinician config)": "The clinician configuration & sign-off surfaces.",
}

# prev/next follows the sidebar reading order exactly (derived from NAV)
ORDER = [it[0] for grp, items in NAV for it in items]

# ----------------------------------------------------------------- display numbering + chapter landings
# Display-only "chapter.section" numbers, derived from NAV order. Files and the Doc NN system are
# untouched — this is purely a presentation layer (sidebar badges + page header + landing pages).
SEC_NO, CHAP_NO, CHAP_LANDING = {}, {}, {}
for _grp, _items in NAV:
    _m = re.match(r"^\s*(\d+)\s*·", _grp)
    _cno = _m.group(1) if _m else "0"          # "Start here" -> chapter 0 (front-matter; items unnumbered)
    CHAP_NO[_grp] = _cno
    CHAP_LANDING[_grp] = "chapter-%s.html" % _cno
    if _m:
        for _i, _it in enumerate(_items, 1):
            SEC_NO.setdefault(_it[0], "%s.%d" % (_cno, _i))

# Curated chapter intros + per-audience "start here" pointers (the auto page-list is derived from NAV).
# Keyed by the live NAV group label so it tracks concurrent NAV edits; missing keys fall back gracefully.
CHAPTERS = {
 "Start here": {"intro": "New here? This is the orientation layer. PureScore turns labs, wearables and a structured intake into one explainable health score — start with the vision, then watch the math run live in the Calculation Explorer.",
   "starts": [("For clinicians", "01-vision-principles-and-lessons.html", "Vision & principles"),
              ("For developers", "purescore-uber-map.html", "Calculation Explorer"),
              ("For product", "conventions.html", "Conventions & glossary")]},
 "1 · How scoring works": {"intro": "The core engine, end to end: how markers roll up through 12 pillars into a single score, how the MONIAC-style reservoirs (a hydraulic-analogy model) give the score momentum and memory, and what PureScore 2.0 adds. This is the math everything else builds on.",
   "starts": [("For clinicians", "purescore-overview.html", "Overview"),
              ("For developers", "03-scoring-formula.html", "Scoring formula"),
              ("For product", "reservoir-sim.html", "Reservoir simulator")]},
 "2 · Inputs & intake": {"intro": "Where every number comes from — labs, wearables and the question intake — and how confidence, coverage and reference ranges are tracked so a score is never more certain than its data.",
   "starts": [("For clinicians", "appendix-biomarkers.html", "Markers (all channels)"),
              ("For developers", "06-data-model-and-reference-ranges.html", "Data model & ranges"),
              ("For product", "appendix-onboarding.html", "Onboarding & first-run")]},
 "3 · Making it personal": {"intro": "The context that reshapes a score: biological sex, life-stage and acute events, established clinical-risk equations, and the persona models used to pressure-test it across very different patients.",
   "starts": [("For clinicians", "08-sex-specific-models.html", "Sex-specific models"),
              ("For developers", "appendix-persona-matrix.html", "Persona matrix"),
              ("For product", "appendix-personas.html", "Personas")]},
 "4 · Acting on it": {"intro": "Turning a score into action: the daily nudge engine, the recommended-actions catalogue, and how adherence and goals feed back into the model.",
   "starts": [("For clinicians", "12-recommended-actions-catalogue.html", "Actions catalogue"),
              ("For developers", "11-daily-nudge-engine.html", "Nudge engine"),
              ("For product", "feedback-loop.html", "Feedback-loop demo")]},
 "5 · Care pathways & delivery": {"intro": "How the engine plugs into real care: condition-specific pathways, the care team and their roles, and the prevention/engagement layer that keeps patients in the loop.",
   "starts": [("For clinicians", "care-pathways.html", "Condition pathways"),
              ("For developers", "care-roles.html", "Care team & roles"),
              ("For product", "prevention-engagement.html", "Prevention & engagement")]},
 "6 · Trust & govern": {"intro": "Why you can trust the number: cohort validation and calibration, the evidence registry behind every band, safety/escalation governance, consent, localization and actuarial fairness.",
   "starts": [("For clinicians", "16-safety-governance-and-regulatory.html", "Safety & governance"),
              ("For developers", "14-validation-and-calibration-harness.html", "Validation harness"),
              ("For product", "consent-onboarding.html", "Consent & onboarding")]},
 "7 · Gaps, blind-spots & roadmap": {"intro": "An honest ledger of what is not done. Product-readiness gaps for the patient app, the spec build-readiness audit, the clinician red-team, and the coverage audit — the work still ahead.",
   "starts": [("For clinicians", "17-clinician-red-team-and-blind-spots.html", "Clinician red-team"),
              ("For developers", "spec-audit.html", "Spec build-readiness"),
              ("For product", "production-gaps.html", "Production readiness — gaps")]},
 "8 · System & build": {"intro": "The whole machine as software: the system-at-a-glance map, patient and engagement state machines, the object/class model, and the ERD / C4 / API / sequence / story dossiers an engineer builds from.",
   "starts": [("For clinicians", "states.html", "Patient life-state machine"),
              ("For developers", "dossier-c4.html", "C4 architecture"),
              ("For product", "dossier-stories.html", "User stories")]},
 "9 · Admin (clinician config)": {"intro": "The clinician-facing configuration surfaces — lab ranges, weights, lifestyle/PRO settings, personas and the governance sign-off flow — shown as interactive mockups.",
   "starts": [("For clinicians", "admin-governance.html", "Governance & sign-off"),
              ("For developers", "admin-weights.html", "Weights & constants"),
              ("For product", "admin-index.html", "Dashboard")]},
}

# One-line page descriptions for the chapter landing lists (fall back to the NAV label when absent).
PDESC = {
 "index.html": "The wiki home and role-based entry points.",
 "purescore-uber-map.html": "Run the full scoring pipeline live — audit tree, dataflow map and editable leaves.",
 "01-vision-principles-and-lessons.html": "What PureScore is, the principles it holds to, and lessons that shaped it.",
 "conventions.html": "Notation, glossary and the conventions every other page assumes.",
 "decisions.html": "The running decision log (D-numbers) referenced throughout the specs.",
 "purescore-overview.html": "A plain-language tour of pillars → markers → score before the formal math.",
 "02-pillars-and-marker-catalog.html": "The 12 pillars and every marker, with bands, weights and sources.",
 "03-scoring-formula.html": "The marker-risk → pillar → PureScore math, stage by stage.",
 "04-moniac-reservoir-dynamics.html": "The reservoir/flow model that gives the score momentum and memory.",
 "reservoir-sim.html": "An interactive sandbox for the reservoir dynamics.",
 "05-critical-review-and-purescore-2.0.html": "A self-critique of v1 and the PureScore 2.0 redesign (companion vector, managed states).",
 "06-data-model-and-reference-ranges.html": "Core entities, marker definitions, confidence and reference-range resolution.",
 "07-data-streams-and-experience.html": "The data streams (labs, wearables, intake) and how they reach the engine.",
 "appendix-biomarkers.html": "The full marker catalogue across every channel, sortable.",
 "reference-range-resolver.html": "How a raw value resolves to a band given age, sex and cohort.",
 "appendix-wearables.html": "Every wearable-derived metric and how it is used.",
 "purescore-wearable-baselines.html": "Personal-baseline logic for wearable signals.",
 "questions-hub.html": "An overview of the question intake and how it is structured.",
 "appendix-onboarding.html": "The onboarding and first-run question flow.",
 "appendix-questions.html": "Validated screeners and patient-reported outcomes.",
 "appendix-question-bank.html": "The complete question bank, grouped and searchable.",
 "appendix-lifestyles.html": "Lifestyle inputs and how they modify the score.",
 "appendix-wearable-corroboration.html": "How wearable signals corroborate self-reported answers.",
 "eligibility-gating.html": "Eligibility rules and the gating that decides what is asked.",
 "08-sex-specific-models.html": "How biological sex reshapes bands, weights and reservoirs.",
 "09-acute-events-and-life-stage-plans.html": "Acute-event handling and life-stage (pregnancy, menopause) plans.",
 "10-clinical-scores-integration.html": "Established clinical risk scores and how they feed PureScore.",
 "appendix-personas.html": "The persona library used to pressure-test the model.",
 "appendix-persona-matrix.html": "Marker × persona weights in flat, sortable form.",
 "purescore-sex.html": "The score viewed through a sex-specific lens (segmented toggle).",
 "11-daily-nudge-engine.html": "How the engine selects, ranks and delivers daily nudges.",
 "12-recommended-actions-catalogue.html": "The catalogue of recommended actions with effects and safety filters.",
 "appendix-adherence.html": "Adherence check-ins and how they feed back.",
 "appendix-goals.html": "The goal catalogue and goal-aware weighting.",
 "feedback-loop.html": "A live demo of the score → action → adherence feedback loop.",
 "care-pathways.html": "Condition-specific care pathways the engine plugs into.",
 "care-roles.html": "The care team, their roles and hand-offs.",
 "prevention-engagement.html": "The prevention and engagement layer that retains patients.",
 "13-cohort-percentiles-and-validation.html": "Cohort percentiles, shrinkage and the validation approach.",
 "14-validation-and-calibration-harness.html": "The calibration/validation harness and its release gates.",
 "15-evidence-registry-and-provenance.html": "The evidence registry and provenance behind every band.",
 "16-safety-governance-and-regulatory.html": "Safety escalation, governance and regulatory posture.",
 "consent-onboarding.html": "Jurisdiction-aware patient consent and the audit record.",
 "18-uae-localization.html": "UAE/Gulf clinical localization and Ramadan handling.",
 "19-actuarial-pricing-and-insurance.html": "Actuarial pricing, credibility and fairness testing.",
 "production-gaps.html": "What a production-grade patient app still needs (product surface).",
 "spec-audit.html": "Build-readiness audit of the specs themselves (can an engineer build this?).",
 "17-clinician-red-team-and-blind-spots.html": "A clinician's adversarial review of the engine's blind spots.",
 "appendix-coverage-audit.html": "Marker / gate / self-report coverage register.",
 "purescore-system.html": "A single-screen map of the whole engine — how every subsystem connects end to end.",
 "states.html": "The patient life-state machine — how a patient moves between baseline, acute and life-stage states.",
 "engagement-state-machines.html": "The nudge-delivery and engagement lifecycle machines (firewall, channels, escalation).",
 "class-model.html": "The full object model — class diagram plus a per-class engineering explorer.",
 "dossier-erd.html": "The entity-relationship model: every persisted entity and its fields.",
 "dossier-c4.html": "C4 architecture views — system context, containers and components.",
 "dossier-api.html": "Request/response contracts for the scoring and intake endpoints.",
 "dossier-sequences.html": "Key runtime sequences — scoring, intake, nudge delivery and escalation.",
 "dossier-stories.html": "User stories mapped to the engine capabilities that satisfy them.",
 "admin-index.html": "The clinician admin dashboard mockup.",
 "admin-lab-ranges.html": "Configure lab reference ranges.",
 "admin-weights.html": "Configure pillar weights and constants.",
 "admin-lifestyle.html": "Configure lifestyle / PRO inputs.",
 "admin-personas.html": "Configure personas and framing.",
 "admin-governance.html": "Governance and sign-off workflow.",
}
PTITLE = {"index.html":"Home","conventions.html":"Conventions & glossary","decisions.html":"Decision log",
          "production-gaps.html":"Production readiness — gaps",
          "spec-audit.html":"Spec build-readiness audit",
          "editorial-review.html":"Editorial & cohesion review",
          "care-pathways.html":"Care pathways — condition pathways","care-roles.html":"Care team & roles",
          "prevention-engagement.html":"Prevention & engagement",
          "appendix-biomarkers.html":"Appendix A · Markers (all channels)","reference-range-resolver.html":"Reference-range resolver",
          "appendix-wearables.html":"Appendix B · Wearables",
          "appendix-questions.html":"Appendix C · Screeners & PROs","appendix-personas.html":"Appendix D · Personas",
          "appendix-question-bank.html":"Appendix E · Question bank","appendix-lifestyles.html":"Appendix F · Lifestyles",
          "appendix-coverage-audit.html":"Appendix G · Coverage audit","appendix-adherence.html":"Appendix H · Adherence",
          "appendix-persona-matrix.html":"Appendix I · Persona matrix","appendix-goals.html":"Appendix J · Goals",
          "questions-hub.html":"Questions & intake","eligibility-gating.html":"Eligibility & gating",
          "appendix-onboarding.html":"Appendix K · Onboarding & first-run","appendix-onboarding.html#spreadsheet":"Onboarding grid",
          "appendix-wearable-corroboration.html":"Appendix L · Wearable corroboration","appendix-wearable-corroboration.html#spreadsheet":"Wearable-corroboration grid",
          "admin-index.html":"Admin · Dashboard","admin-lab-ranges.html":"Admin · Lab ranges",
          "admin-weights.html":"Admin · Weights","admin-lifestyle.html":"Admin · Lifestyle",
          "admin-personas.html":"Admin · Personas","admin-governance.html":"Admin · Governance",
          "purescore-system.html":"System at a glance",
          "purescore-calc-uber.html":"PureScore Calc Diagram",
          "feedback-loop.html":"Live feedback-loop demo","states.html":"Patient life-state machine",
          "class-model.html":"Class model",
          "engagement-state-machines.html":"Engagement state machines",
          "purescore-uber-map.html":"Calculation Explorer",
          "wearable-baselines.html":"Wearable Baselines",
          "reservoir-sim.html":"Reservoir simulator",
          "consent-onboarding.html":"Consent & onboarding",
          "purescore-overview.html":"PureScore · Overview","purescore-wearable-baselines.html":"Baselines (Wearables)",
          "purescore-sex.html":"PureScore by sex",
          "dossier-sequences.html":"Sequences",
          "dossier-erd.html":"Data model (ERD)","dossier-c4.html":"C4 architecture",
          "dossier-api.html":"API contracts","dossier-stories.html":"User stories",
          "appendix-biomarkers.html#spreadsheet":"Biomarkers grid","appendix-wearables.html#spreadsheet":"Wearables grid","appendix-personas.html#spreadsheet":"Personas grid",
          "appendix-lifestyles.html#spreadsheet":"Lifestyles grid","appendix-adherence.html#spreadsheet":"Adherence grid","appendix-goals.html#spreadsheet":"Goals grid",
          "appendix-persona-matrix.html#spreadsheet":"Persona-matrix grid","appendix-question-bank.html#spreadsheet":"Question-bank grid","dossier-erd.html#spreadsheet":"Data-model grid"}
for n in DOCMAP: PTITLE[DOCMAP[n]] = "Doc %s · %s" % (n, SHORT[n])
for _grp, _fn in CHAP_LANDING.items(): PTITLE[_fn] = "%s — chapter overview" % _grp

# ----------------------------------------------------------------- left-pane facet taxonomy
# Six filter dimensions. `module` is derived from the NAV chapter; `content-type` and `maturity`
# are derived from the filename / flagged content; `audience`, `persona` and `jurisdiction` are
# curated with sensible defaults. This is a first-pass taxonomy — refined during the content audit.
def _mod_label(grp):                                   # "5 · Trust & govern" -> "Trust & govern"
    return re.sub(r"^\s*\d+\s*·\s*", "", grp)
MOD_LABEL, _MOD_OF = {}, {}                            # slug->label ; href->slug
for _g, _items in NAV:
    _lab = _mod_label(_g); _sl = slug(_lab); MOD_LABEL[_sl] = _lab
    for _it in _items: _MOD_OF.setdefault(_it[0], _sl)

CT_LABELS = {"spec":"Spec doc","catalog":"Reference catalog","diagram":"Diagram / model",
             "explorer":"Interactive explorer","admin":"Admin mockup","meta":"Guide / index"}
def _ctype(fn):
    if fn in ("index.html", "conventions.html", "decisions.html"): return "meta"
    if fn.startswith("admin-"): return "admin"
    if fn in ("purescore-uber-map.html", "feedback-loop.html", "reference-range-resolver.html",
              "consent-onboarding.html"): return "explorer"
    if fn.startswith("dossier-") or fn in ("states.html", "engagement-state-machines.html", "class-model.html"):
        return "diagram"
    if fn.startswith("appendix-"): return "catalog"
    return "spec"

MAT_LABELS = {"draft":"Draft","counsel":"Needs counsel review","roadmap":"Gap / roadmap"}
_MAT = {"consent-onboarding.html":"counsel", DOCMAP["16"]:"counsel", DOCMAP["18"]:"counsel",
        "production-gaps.html":"roadmap", "spec-audit.html":"roadmap", "editorial-review.html":"roadmap",
        DOCMAP["17"]:"roadmap", "appendix-coverage-audit.html":"roadmap"}
def _maturity(fn): return _MAT.get(fn, "draft")

AUD_LABELS = {"eng":"Engineer","clin":"Clinician","ds":"Data scientist","comp":"Compliance / legal","prod":"Product"}
ALL_AUD = ["eng","clin","ds","comp","prod"]
_AUD = {
  DOCMAP["02"]:["eng","ds","clin"], DOCMAP["03"]:["eng","ds"], DOCMAP["04"]:["eng","ds"],
  DOCMAP["05"]:["eng","ds","clin"], DOCMAP["06"]:["eng","ds"], DOCMAP["07"]:["eng","prod"],
  DOCMAP["08"]:["clin","ds"], DOCMAP["09"]:["clin","prod"], DOCMAP["10"]:["clin","ds"],
  DOCMAP["11"]:["prod","clin","eng"], DOCMAP["12"]:["prod","clin"], DOCMAP["13"]:["ds","comp"],
  DOCMAP["14"]:["ds","eng"], DOCMAP["15"]:["comp","ds"], DOCMAP["16"]:["comp","clin"],
  DOCMAP["17"]:["clin","comp"], DOCMAP["18"]:["comp","prod"], DOCMAP["19"]:["ds","comp","prod"],
  "purescore-uber-map.html":["eng","ds"], "class-model.html":["eng"], "purescore-overview.html":["prod","clin"],
  "dossier-erd.html":["eng"], "dossier-c4.html":["eng"], "dossier-api.html":["eng"],
  "dossier-sequences.html":["eng"], "dossier-stories.html":["prod","eng"],
  "consent-onboarding.html":["comp","prod"], "production-gaps.html":["eng","prod","comp"],
  "spec-audit.html":["eng","ds","comp"],
  "states.html":["eng","clin"], "engagement-state-machines.html":["eng","prod"],
  "appendix-coverage-audit.html":["ds","comp"],
}
for _fn in ("admin-index.html","admin-lab-ranges.html","admin-weights.html","admin-lifestyle.html",
            "admin-personas.html","admin-governance.html"):
    _AUD[_fn] = ["clin","prod"]
def _audience(fn): return _AUD.get(fn, ALL_AUD)

PER_LABELS = {"all":"All patients","female":"Female-specific","male":"Male-specific",
              "lifestage":"Life-stage / acute","persona":"Persona-modelled"}
_PER = {
  DOCMAP["08"]:["female","male"], "purescore-sex.html":["female","male"],
  DOCMAP["09"]:["lifestage"], "states.html":["lifestage"],
  "appendix-personas.html":["persona"], "appendix-persona-matrix.html":["persona"],
}
def _persona(fn): return _PER.get(fn, ["all"])

JUR_LABELS = {"global":"Global","uae":"UAE","gcc":"GCC","eu":"EU","us":"US"}
_JUR = {
  DOCMAP["18"]:["uae","gcc"], "consent-onboarding.html":["global","uae","gcc","eu","us"],
  DOCMAP["16"]:["global","uae","eu","us"], DOCMAP["19"]:["global","uae"],
}
def _juris(fn): return _JUR.get(fn, ["global"])

def page_facets(fn):
    return {"mod":[_MOD_OF.get(fn, "other")], "ct":[_ctype(fn)], "mat":[_maturity(fn)],
            "aud":_audience(fn), "per":_persona(fn), "jur":_juris(fn)}

# (key, dropdown title, slug->label map) — order = display order of the filter dropdowns
FACET_DEFS = [("mod","Module",MOD_LABEL), ("ct","Type",CT_LABELS), ("aud","Audience",AUD_LABELS),
              ("mat","Maturity",MAT_LABELS), ("per","Persona",PER_LABELS), ("jur","Region",JUR_LABELS)]

def _facet_data_attrs(fn):
    pf = page_facets(fn)
    return "".join(' data-%s="%s"' % (k, " ".join(pf[k])) for k, _, _ in FACET_DEFS)

def _filter_panel():
    used = {k: set() for k, _, _ in FACET_DEFS}
    for _g, _items in NAV:
        for _it in _items:
            pf = page_facets(_it[0])
            for k in used: used[k].update(pf[k])
    rows = []
    for key, title, labels in FACET_DEFS:
        opts = ['<option value="">%s · all</option>' % esc(title)]
        for v in sorted(used.get(key, []), key=lambda x: labels.get(x, x).lower()):
            opts.append('<option value="%s">%s</option>' % (esc(v), esc(labels.get(v, v))))
        rows.append('<select class="nf-sel" data-facet="%s" aria-label="Filter by %s">%s</select>'
                    % (key, esc(title), "".join(opts)))
    return ('<details class="nav-filter"><summary><span class="nf-ic">⚲</span> Filter pages'
            '<span class="nf-active"></span></summary>'
            '<div class="nf-body">%s'
            '<button type="button" class="nf-clear">Clear filters</button>'
            '<div class="nf-count" aria-live="polite"></div></div></details>'
            % "".join(rows))

def sidebar(active):
    s = ['<aside class="side">',
         '<div class="side-tools"><button class="side-allbtn" data-act="expand">expand all</button>'
         '<button class="side-allbtn" data-act="collapse">collapse all</button></div>',
         _filter_panel()]
    for gi, (grp, items) in enumerate(NAV):
        hrefs = [it[0] for it in items]
        landing = CHAP_LANDING.get(grp)
        is_active = active in hrefs or active == landing
        s.append('<div class="navgrp%s" data-grp="g%d">' % (" open" if is_active else "", gi))
        title_html = (('<a class="grp-t%s" href="%s">%s</a>' % (" active" if active == landing else "", landing, esc(grp)))
                      if landing else ('<span class="grp-t">%s</span>' % esc(grp)))
        s.append('<div class="grp-h"><button class="grp-tog" aria-expanded="%s" aria-label="Toggle %s">'
                 '<span class="grp-car">▸</span></button>%s</div>'
                 % ("true" if is_active else "false", esc(grp), title_html))
        s.append('<div class="grp-b">')
        blurb = NAV_BLURB.get(grp, "")
        if blurb:
            s.append('<div class="grp-blurb">%s</div>' % esc(blurb))
        for it in items:
            href, label = it[0], it[1]
            sn = SEC_NO.get(href, "")
            secno = ('<span class="secno">%s</span>' % sn) if sn else '<span class="secno secno-x">·</span>'
            docb = (' <span class="ndoc">Doc %s</span>' % it[2]) if len(it) > 2 else ""
            s.append('<a class="navlink%s" href="%s"%s>%s<span class="nl-t">%s</span>%s</a>'
                     % (" active" if href == active else "", href, _facet_data_attrs(href), secno, label, docb))
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
            landing = CHAP_LANDING.get(g)
            sn = SEC_NO.get(fn, "")
            secbadge = ('<span class="chx-sec">%s</span>' % sn) if sn else ""
            name = (('<a class="chx-name" href="%s">%s</a>' % (landing, esc(g))) if landing
                    else ('<span class="chx-name">%s</span>' % esc(g)))
            sibs = "".join('<a class="chx-l%s" href="%s">%s%s</a>'
                           % (" cur" if it[0] == fn else "", it[0],
                              ('<span class="chx-n">%s</span>' % SEC_NO[it[0]]) if it[0] in SEC_NO else "",
                              esc(it[1])) for it in items)
            return ('<div class="chapter-ctx"><div class="chx-top">%s%s'
                    '<span class="chx-blurb">%s</span></div>'
                    '<details class="chx-d"><summary>%d pages in this chapter · overview ›</summary>'
                    '<nav class="chx-list">%s</nav></details></div>'
                    % (secbadge, name, esc(blurb), len(items), sibs))
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
"""illustrative design, re-verify before production (<a class="xref" href="conventions.html">Conventions</a> §5.6). Diagrams render via mermaid (CDN).</footer></main></div>
<script src="assets/search-index.js"></script><script src="assets/search.js"></script><script src="assets/wiki.js"></script><script src="assets/pillars-data.js"></script><script src="assets/pillar-map.js"></script></body></html>""") % (
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

# ----------------------------------------------------------------- chapter landing pages
def render_chapters():
    """One landing page per NAV chapter — curated intro + per-audience 'start here' + auto page list.
    Fully NAV-derived so it tracks concurrent NAV edits; intros/starts/descriptions degrade gracefully."""
    for grp, items in NAV:
        fn = CHAP_LANDING.get(grp)
        if not fn:
            continue
        meta = CHAPTERS.get(grp, {})
        intro = meta.get("intro") or NAV_BLURB.get(grp, "")
        cno = CHAP_NO.get(grp, "")
        b = ['<div class="crumbs"><a href="index.html">Home</a> › %s</div>' % esc(grp),
             '<h1>%s</h1>' % esc(grp)]
        if intro:
            b.append('<p class="lead">%s</p>' % esc(intro))
        starts = meta.get("starts", [])
        if starts:
            b.append('<div class="ch-starts">')
            for aud, href, label in starts:
                b.append('<a class="ch-start" href="%s"><span class="cs-aud">%s</span>'
                         '<span class="cs-go">%s →</span></a>' % (href, esc(aud), esc(label)))
            b.append('</div>')
        b.append('<h2 id="pages" class="ch-list-h">Pages in this chapter</h2><div class="ch-list">')
        for it in items:
            href, label = it[0], it[1]
            sn = SEC_NO.get(href, "")
            secno = ('<span class="cl-no">%s</span>' % sn) if sn else '<span class="cl-no cl-no-x"></span>'
            docb = (' <span class="ndoc">Doc %s</span>' % it[2]) if len(it) > 2 else ""
            desc = PDESC.get(href, "")
            b.append('<a class="ch-row" href="%s">%s<span class="cl-main">'
                     '<span class="cl-t">%s%s</span><span class="cl-d">%s</span></span></a>'
                     % (href, secno, esc(label), docb, esc(desc)))
        b.append('</div>')
        page(fn, "%s — chapter overview" % grp, "".join(b))

# ----------------------------------------------------------------- index landing
def render_index():
    try: NQ = len(C._load("question-bank.json").get("questions", []))
    except Exception: NQ = 0
    flow = """flowchart TD
  R["README · conventions"] --> D0["01 Vision"]
  D0 --> D1["06 Data model"] --> D2["02 Pillars & markers"] --> D3["03 Scoring"] --> D4["04 Reservoirs"]
  D3 --> D12["05 PureScore 2.0"]
  D2 --> D5["08 Sex-specific"]
  D3 --> D7["11 Nudge engine"]
  D12 --> D13["14 Validation"]
  D1 --> D18["07 Data streams"] --> D7
  D7 --> D16["12 Actions"]
  D7 --> DLV["Notify · firewall<br/>channels · quiet hours · escalate"]
  DLV --> D11["16 Safety"]
  D3 --> D11
  D3 --> CARE["Care pathways &amp; delivery<br/>(IEAT · priority · roles)"]
  D18 --> CARE
  CARE --> D7
  CARE --> D16
  CARE --> D13
  D18 --> ADMIN["Admin (clinician config)"]
  D7 --> FB["Feedback-loop demo"]
  SYS["System at a glance<br/>(board overview · 3 lenses)"] -.-> D0
  SYS -.-> D3
  SYS -.-> D7
  SYS -.-> D16"""
    b = ['<div class="hero"><h1>Hikma<span style="color:#2ecc71">Engine</span> — Technical Wiki</h1>',
         '<p class="lead">The HikmaEngine health-intelligence platform: data &amp; intake, the '
         '<a href="purescore-overview.html"><b>PureScore</b> scoring engine</a> (one subsystem — pillars, '
         'reservoirs, metrics &amp; weights), interpretation &amp; personalization, action &amp; engagement, and '
         'validation/governance — plus reference appendices, the engineering build and live demos. Start at '
         '<a href="01-vision-principles-and-lessons.html">Doc 01</a>, the '
         '<a href="purescore-overview.html">PureScore Overview</a>, or jump anywhere.</p></div>',
         ('<div class="section-h">Where do I start?</div><div class="grid c3 roles">'
          '<a class="card role" href="01-vision-principles-and-lessons.html"><div class="role-i">🧭</div>'
          '<h3>New here</h3><p>Read the book front-to-back. Start with the vision, then the Overview and the system map.</p>'
          '<div class="role-links"><a href="purescore-system.html">System at a glance</a>'
          '<a href="purescore-uber-map.html">How it all connects</a><a href="03-scoring-formula.html">Scoring formula</a></div></a>'
          '<a class="card role" href="admin-index.html"><div class="role-i">🩺</div>'
          '<h3>Clinician</h3><p>How the score is kept safe, contextual and reviewable. Jump to the Admin area.</p>'
          '<div class="role-links"><a href="purescore-system.html">System at a glance</a>'
          '<a href="16-safety-governance-and-regulatory.html">Safety &amp; governance</a>'
          '<a href="10-clinical-scores-integration.html">Clinical scores</a><a href="appendix-coverage-audit.html">Coverage audit</a></div></a>'
          '<a class="card role" href="class-model.html"><div class="role-i">🛠️</div>'
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
    b.append('</div><div class="section-h">Admin (clinician config) — quarterly configuration review</div><div class="grid c3">')
    for href, t, d in [("admin-index.html","Dashboard","All config domains &amp; review status"),
                       ("admin-lab-ranges.html","Lab ranges","Band review + guideline-match"),
                       ("admin-weights.html","Weights &amp; constants","Pillar weights, κ, imputation"),
                       ("admin-lifestyle.html","Lifestyle / PRO","Instruments &amp; scoring"),
                       ("admin-personas.html","Personas & frames","Cohort overrides"),
                       ("admin-governance.html","Governance & sign-off","Workflow, gates, audit")]:
        b.append('<a class="card" href="%s"><h3>%s</h3><p>%s</p></a>' % (href, t, d))
    b.append('</div><div class="section-h">Diagrams &amp; system maps</div><div class="grid c3">'
             '<a class="card" href="class-model.html"><h3>Class model</h3>'
             '<p>The full PureScore object model — markers, pillars, reservoirs, engine and feedback loop — in one class diagram.</p></a>'
             '<a class="card" href="states.html"><h3>Patient life-state machine</h3>'
             '<p>An interactive statechart of every life state a patient can occupy — a concurrent vector across Care status, '
             'Life stage and Engagement, with drill-down sub-machines (acute, chronic, pregnancy, aging, onboarding), '
             'filtered by sex, age, condition and persona.</p></a>'
             '<a class="card" href="purescore-uber-map.html"><h3>PureScore uber-map</h3>'
             '<p>Interactive full-lifecycle map — pan/zoom, click a box for its calc, 10 sample profiles with step-through animation.</p></a>'
             '<a class="card" href="purescore-uber-map.html"><h3>PureScore calculation — data flow</h3>'
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
    for href, t, d in [("appendix-question-bank.html#spreadsheet","Question bank grid","%d questions · sort/filter every column" % NQ),
                       ("appendix-biomarkers.html#spreadsheet","Biomarkers grid","Every marker · pillar, bands, weight, source"),
                       ("appendix-goals.html#spreadsheet","Goals grid","Goals by applicability · persona, target, modifiability"),
                       ("appendix-adherence.html#spreadsheet","Adherence grid","Check-ins · family, reservoir, responses"),
                       ("appendix-persona-matrix.html#spreadsheet","Persona-matrix grid","Signal×persona weights (long form)"),
                       ("dossier-erd.html#spreadsheet","Data-model grid","Every entity column across the ERD")]:
        b.append('<a class="card" href="%s"><h3>%s</h3><p>%s</p></a>' % (href, t, d))
    b.append('</div>')
    page("index.html", "Home", "".join(b))

# ----------------------------------------------------------------- main
def main():
    C.write_calc_data()   # generate assets/calc-data.js from canonical JSON (engine source)
    C.write_cite_data()   # generate assets/cite-data.js from data/citations.json (citation popovers)
    C.write_range_data()  # generate assets/range-data.js from data/range-variations.json (range-variation views)
    C.write_flag_data()   # generate assets/flags-data.js from data/clinical-flags.json (inline audit ⚠ badges)
    C.write_unit_data()   # generate assets/units-data.js from data/units.json (SI-canonical units + conversions)
    C.write_pillar_data() # generate assets/pillars-data.js (pillar-fan hover cards + click-to-section)
    render_index()
    render_chapters()
    for n in sorted(DOCMAP): render_doc(n)
    render_simple("README.md", "conventions.html", "Conventions & glossary", "Conventions & glossary")
    render_simple("decisions.md", "decisions.html", "Decision log", "Decision log")
    # builders resolved by name at call time so a not-yet-present builder (multi-agent edits) is skipped, not fatal
    # IA cleanup: each dataset's companion grid is folded into its appendix as a "Spreadsheet view" (no standalone grid pages)
    MERGE_GRID = {"appendix-biomarkers.html": "build_grid_biomarkers", "appendix-wearables.html": "build_grid_wearables",
                  "appendix-personas.html": "build_grid_personas", "appendix-lifestyles.html": "build_grid_lifestyles",
                  "appendix-adherence.html": "build_grid_adherence", "appendix-goals.html": "build_grid_goals",
                  "appendix-persona-matrix.html": "build_grid_persona_matrix", "appendix-question-bank.html": "build_grid_questions",
                  "appendix-onboarding.html": "build_grid_onboarding", "appendix-wearable-corroboration.html": "build_grid_wearable_corroboration",
                  "dossier-erd.html": "build_grid_erd"}
    for fn, bname in [("appendix-biomarkers.html", "build_biomarkers"), ("reference-range-resolver.html", "build_range_resolver"), ("appendix-wearables.html", "build_wearables"),
                      ("appendix-questions.html", "build_questions"), ("appendix-personas.html", "build_personas"),
                      ("appendix-question-bank.html", "build_question_bank"), ("appendix-lifestyles.html", "build_lifestyles"),
                      ("appendix-coverage-audit.html", "build_coverage_audit"),
                      ("appendix-adherence.html", "build_adherence"), ("appendix-persona-matrix.html", "build_persona_matrix"),
                      ("appendix-goals.html", "build_goals"),
                      ("questions-hub.html", "build_questions_hub"), ("eligibility-gating.html", "build_eligibility"),
                      ("appendix-onboarding.html", "build_onboarding"),
                      ("appendix-wearable-corroboration.html", "build_wearable_corroboration"),
                      ("class-model.html", "build_class_model"),
                      ("production-gaps.html", "build_production_gaps"),
                      ("spec-audit.html", "build_spec_audit"),
                      ("editorial-review.html", "build_editorial_review"),
                      ("care-pathways.html", "build_care_pathways"), ("care-roles.html", "build_care_roles"),
                      ("prevention-engagement.html", "build_prevention"),
                      ("purescore-uber-map.html", "build_purescore_uber"),
                      ("purescore-overview.html", "build_purescore_overview"),
                      ("purescore-wearable-baselines.html", "build_wearable_baselines"),
                      ("purescore-sex.html", "build_purescore_sex"),
                      ("dossier-sequences.html", "build_sequences"),
                      ("dossier-erd.html", "build_erd"), ("dossier-c4.html", "build_c4"),
                      ("dossier-api.html", "build_api"), ("dossier-stories.html", "build_stories")]:
        build = getattr(C, bname, None)
        if build is None:
            print("  ! skip %s — wiki_content.%s not defined yet" % (fn, bname)); continue
        tab, body = build()
        if fn in MERGE_GRID:                       # fold the companion grid in as an in-page "Spreadsheet view"
            gb = getattr(C, MERGE_GRID[fn], None)
            if gb is not None:
                _, gbody = gb(); gi = gbody.find('<div class="dgwrap">')
                if gi >= 0:
                    body += ('<h2 id="spreadsheet" style="margin-top:30px">Spreadsheet view</h2>'
                             '<p class="small muted">The same catalogue, flat &amp; sortable.</p>' + gbody[gi:])
        page(fn, tab, body)
    for fn, build in A.ADMIN_PAGES:
        tab, body = build(); page(fn, tab, body)
    built = len(ORDER)
    for fn, tab in [("feedback-loop.html", "Live feedback-loop demo"), ("states.html", "Patient state machine"),
                    ("engagement-state-machines.html", "Engagement state machines"),
                    ("purescore-system.html", "System at a glance"),
                    ("purescore-calc-uber.html", "PureScore Calc Diagram")]:
        body = os.path.join(HERE, fn[:-5] + ".body.html")
        if os.path.exists(body):
            page(fn, tab, open(body, encoding="utf-8").read())
        else:
            built -= 1
    print("Generated %d pages → %s" % (built, HERE))
    _inject_connects()
    _build_search_index()
    _consistency_check()
    _engine_guard()
    _flag_guard()
    _care_guard()

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
_DIAGRAMS = {"purescore-uber-map.html", "purescore-uber-map.html", "states.html", "engagement-state-machines.html",
             "class-model.html", "dossier-erd.html", "dossier-c4.html", "dossier-sequences.html",
             "class-model.html", "wearable-baselines.html", "purescore-wearable-baselines.html",
             "consent-onboarding.html", "purescore-system.html", "reservoir-sim.html",
             "purescore-calc-uber.html"}
_GRID_OF = {"appendix-biomarkers.html": "appendix-biomarkers.html#spreadsheet", "appendix-wearables.html": "appendix-wearables.html#spreadsheet",
            "appendix-personas.html": "appendix-personas.html#spreadsheet", "appendix-lifestyles.html": "appendix-lifestyles.html#spreadsheet",
            "appendix-adherence.html": "appendix-adherence.html#spreadsheet", "appendix-goals.html": "appendix-goals.html#spreadsheet",
            "appendix-persona-matrix.html": "appendix-persona-matrix.html#spreadsheet", "appendix-question-bank.html": "appendix-question-bank.html#spreadsheet",
            "appendix-onboarding.html": "appendix-onboarding.html#spreadsheet", "appendix-wearable-corroboration.html": "appendix-wearable-corroboration.html#spreadsheet",
            "dossier-erd.html": "dossier-erd.html#spreadsheet"}
_CONNECTS_EXTRA = {
 "index.html": ["purescore-uber-map.html", "purescore-overview.html", DOCMAP["01"]],
 "purescore-overview.html": [DOCMAP["03"], DOCMAP["02"], "purescore-uber-map.html"],
 DOCMAP["02"]: ["appendix-biomarkers.html", "appendix-biomarkers.html#spreadsheet", DOCMAP["03"]],
 DOCMAP["03"]: ["purescore-uber-map.html", DOCMAP["04"], "class-model.html"],
 DOCMAP["04"]: [DOCMAP["03"], "class-model.html", "purescore-uber-map.html"],
 DOCMAP["05"]: ["states.html", "purescore-uber-map.html", DOCMAP["03"]],
 DOCMAP["06"]: ["dossier-erd.html", DOCMAP["07"]],
 DOCMAP["07"]: ["appendix-wearables.html", "appendix-biomarkers.html", "questions-hub.html"],
 DOCMAP["11"]: ["engagement-state-machines.html", "prevention-engagement.html", "appendix-adherence.html", "appendix-goals.html", DOCMAP["12"]],
 DOCMAP["12"]: [DOCMAP["11"], "care-pathways.html", "appendix-adherence.html"],
 DOCMAP["09"]: ["states.html", "care-pathways.html", DOCMAP["08"]],
 DOCMAP["08"]: ["appendix-personas.html", "purescore-sex.html"],
 DOCMAP["16"]: ["states.html", "care-roles.html", DOCMAP["17"]],
 DOCMAP["18"]: ["care-pathways.html", DOCMAP["08"]],
 DOCMAP["19"]: ["prevention-engagement.html", DOCMAP["16"]],
 "appendix-coverage-audit.html": ["questions-hub.html", "appendix-question-bank.html", "states.html"],
 "questions-hub.html": ["appendix-onboarding.html", "appendix-question-bank.html", "eligibility-gating.html"],
 "states.html": [DOCMAP["09"], "care-pathways.html", "appendix-goals.html", DOCMAP["05"]],
 "class-model.html": ["dossier-erd.html", DOCMAP["03"]],
 "care-pathways.html": ["care-roles.html", "prevention-engagement.html", "reference-range-resolver.html", "eligibility-gating.html", DOCMAP["11"], DOCMAP["09"]],
 "care-roles.html": ["care-pathways.html", "prevention-engagement.html", DOCMAP["16"], "admin-index.html"],
 "prevention-engagement.html": ["care-pathways.html", DOCMAP["11"], DOCMAP["12"], DOCMAP["19"]],
 "eligibility-gating.html": ["care-pathways.html", "questions-hub.html", "appendix-question-bank.html"],
 "reference-range-resolver.html": ["care-pathways.html", "appendix-biomarkers.html"],
 "engagement-state-machines.html": ["care-pathways.html", DOCMAP["11"]],
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

# ----------------------------------------------------------------- full-text search index
# Indexes every generated page at section granularity: page title + each h1/h2/h3 (with its
# #anchor) + a short text snippet. Compact (headings+snippets, not full body) so it loads fast
# on every page. Consumed by assets/search.js (top-bar dropdown + ⌘K palette).
_TAGS = re.compile(r"<[^>]+>")
_ENT = {"&amp;": "&", "&lt;": "<", "&gt;": ">", "&nbsp;": " ", "&#39;": "'", "&quot;": '"',
        "&mdash;": "—", "&middot;": "·", "&times;": "×", "&rarr;": "→", "&larr;": "←", "&hellip;": "…"}
def _plain(html):
    t = re.sub(r"<(script|style)[^>]*>.*?</\1>", " ", html, flags=re.S | re.I)
    t = _TAGS.sub(" ", t)
    for k, v in _ENT.items(): t = t.replace(k, v)
    t = re.sub(r"&#\d+;", " ", t)
    return re.sub(r"\s+", " ", t).strip()

def _build_search_index():
    recs = []
    for f in sorted(glob.glob(os.path.join(HERE, "*.html"))):
        fn = os.path.basename(f)
        h = open(f, encoding="utf-8").read()
        m = re.search(r'<main class="main">(.*)</main>', h, re.S)
        body = m.group(1) if m else h
        body = body.split('<div class="connects">')[0].split('<nav class="pn">')[0]
        title = PTITLE.get(fn) or _navlabel(fn) or fn
        secs, heads = [], list(re.finditer(r'<h([1-3])(?:\s+id="([^"]+)")?[^>]*>(.*?)</h\1>', body, flags=re.S))
        for i, mm in enumerate(heads):
            htext = _plain(mm.group(3))
            if not htext: continue
            end = heads[i + 1].start() if i + 1 < len(heads) else len(body)
            secs.append({"i": mm.group(2) or "", "h": htext, "s": _plain(body[mm.end():end])[:160]})
            if len(secs) >= 80: break          # cap pathological pages (e.g. the question bank)
        if not secs:
            secs = [{"i": "", "h": title, "s": _plain(body)[:160]}]
        recs.append({"f": fn, "t": title, "m": MOD_LABEL.get(_MOD_OF.get(fn, ""), ""), "secs": secs})
    js = "window.PURESCORE_SEARCH=" + json.dumps(recs, ensure_ascii=False, separators=(",", ":")) + ";"
    open(os.path.join(HERE, "assets", "search-index.js"), "w", encoding="utf-8").write(js)
    nsec = sum(len(r["secs"]) for r in recs)
    print("  [search] indexed %d pages · %d sections → assets/search-index.js (%d KB)"
          % (len(recs), nsec, len(js.encode("utf-8")) // 1024))

def _care_guard():
    """Care-pathways integrity (Care pathways & delivery): HARD-fail if a pathway references a role
    not in care-roles.json, a priority owner is unknown, or a threshold-reconciliation status is not
    one of aligned/partial/divergent (so the reconciliation can't silently drift)."""
    cp = C._load("care-pathways.json")["pathways"]
    roles = set(C._load("care-roles.json")["roles"])
    VALID = {"aligned", "partial", "divergent"}
    bad = []
    for k, p in cp.items():
        for rk in p.get("roles", []):
            if rk not in roles:
                bad.append("%s: role %r not in care-roles.json" % (k, rk))
        for t in p.get("priority", []):
            ow = t.get("owner", "")
            if ow.lower() not in roles and ow not in ("FM", "GP"):
                bad.append("%s: priority owner %r unknown" % (k, ow))
        for r in p.get("reconcile", []):
            if r.get("status") not in VALID:
                bad.append("%s: reconcile status %r invalid" % (k, r.get("status")))
    ndiv = sum(1 for p in cp.values() for r in p.get("reconcile", []) if r.get("status") == "divergent")
    print("[care-guard] %d pathways · %d roles · %d threshold divergences flagged" % (len(cp), len(roles), ndiv))
    if bad:
        print("  ! FAIL:")
        for b in bad:
            print("     -", b)
        raise SystemExit("! build failed: care-pathways guard")
    print("  [care-guard] OK — roles resolve, priority owners & reconciliation statuses valid")


def _flag_guard():
    """Audit-rerun regression (per Package F): HARD-fail the build if any clinical-audit flag is
    left untriaged, a remediations.json key dangles (renamed/removed flag), or a resolved flag's
    canonical value was reverted. Single source: validate_flags.py."""
    import validate_flags
    if validate_flags.main() != 0:
        raise SystemExit("! build failed: clinical-flag guard — see validate_flags.py")


if __name__ == "__main__":
    main()
