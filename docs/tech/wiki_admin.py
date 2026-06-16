# -*- coding: utf-8 -*-
"""Doctor's-board admin screen mockups (high-fidelity, static) for the PureScore tech wiki.
These are DESIGN mockups for the quarterly clinical-configuration review board. Non-functional:
buttons and fields are illustrative. Governance: Doc 11 §5, release gates Doc 13, evidence Doc 14."""

from wiki_content import (_esc, ILLUS, PILLARS, PILLAR_W, CONSTANTS, QSOURCE,
                          INSTRUMENTS, PERSONAS, WEARABLES, _rngbar)

QUARTER = "Q3 2026"
def _crumb(label):
    return ('<div class="crumbs"><a href="index.html">Home</a> › '
            '<a href="admin-index.html">Doctor\'s board</a> › %s</div>' % _esc(label))

def _adminbar(sub):
    return ('<div class="admin-top"><div><b style="color:#fff;font-size:15px">PureScore — Clinical '
            'Configuration Board</b><div class="small muted">%s · %s review cycle</div></div>'
            '<div class="who">Signed in as <b>Dr. A. Rahman</b><br>Chair, Clinical Governance · '
            'review &amp; sign-off</div></div>' % (_esc(sub), QUARTER))

def _st(kind, label):
    return '<span class="status st-%s">%s</span>' % (kind, _esc(label))

GOV_NOTE = ('<div class="callout note"><div class="ct">How this board works</div>'
  'Every configuration item below is owned by a clinical lead and <b>reviewed each quarter</b>. '
  'A proposed change moves <b>Draft → Under review → Approved / Changes-requested → Versioned</b>. '
  'Approval requires panel quorum + chair sign-off, and <b>any change is a model-version bump that '
  'must pass the release gates</b> (<a class="xref" href="13-validation-and-calibration-harness.html">Doc 13</a>), '
  'update the evidence registry (<a class="xref" href="14-evidence-registry-and-provenance.html">Doc 14</a>), '
  'and is written to the immutable audit log (<a class="xref" href="11-safety-governance-and-regulatory.html">Doc 11</a> §4.3, §5).</div>')

# config-domain registry shown on the dashboard
DOMAINS = [
 ("Lab / biomarker ranges","admin-lab-ranges.html","Dr. S. Mehta (Lab Medicine)", 80, "2026-03-12","due","12 markers flagged — guideline refresh"),
 ("PureScore weights &amp; constants","admin-weights.html","Dr. L. Haddad (Biostatistics)", 28, "2026-03-12","review","Stage 2b κ added (D23) — under review"),
 ("Lifestyle / PRO instruments","admin-lifestyle.html","Dr. N. Okoye (Psychiatry)", 12, "2026-03-10","ok","No change; PHQ-9 safety rule locked"),
 ("Personas &amp; cohort frames","admin-personas.html","Dr. R. Costa (Internal Med)", 15, "2026-03-11","ok","Pregnancy T3 frame proposed"),
 ("Wearable trust tiers (D22)","appendix-wearables.html","Dr. K. Lin (Cardiology)", 15, "2026-03-12","review","CGM weight 0.85→0.9 proposed"),
 ("UAE localization &amp; Ramadan","15-uae-localization.html","Dr. F. Al-Sayed (Endocrinology)", 9, "2026-03-09","ok","IDF-DAR 2024 verified"),
 ("Reservoir / dynamics params","04-moniac-reservoir-dynamics.html","Dr. L. Haddad (Biostatistics)", 11, "2026-03-12","ok","λ/κ unchanged"),
 ("Escalation thresholds","11-safety-governance-and-regulatory.html","Dr. A. Rahman (Chair)", 18, "2026-03-12","chg","Sepsis screen WBC cut under revision"),
]

def build_admin_index():
    flagged = sum(1 for d in DOMAINS if d[5] in ("chg",))
    review = sum(1 for d in DOMAINS if d[5] == "review")
    due = sum(1 for d in DOMAINS if d[5] == "due")
    h = [_crumb("Dashboard"), '<h1>Doctor\'s Board — Configuration Dashboard</h1>',
         _adminbar("Quarterly clinical configuration review"),
         '<p class="lead">Every clinical knob in PureScore — lab ranges, weights, questionnaires, personas, '
         'wearable tiers — surfaced for the clinical board to review, challenge and sign off each quarter. '
         'Nothing reaches patients without passing this board and the release gates.</p>', ILLUS,
         '<div class="kpis">'
         '<div class="kpi"><div class="v">8</div><div class="l">config domains</div><div class="s muted">all owned &amp; versioned</div></div>'
         '<div class="kpi"><div class="v">%d</div><div class="l">items under review</div><div class="s" style="color:var(--acc)">awaiting panel</div></div>'
         '<div class="kpi"><div class="v">%d</div><div class="l">guideline-refresh due</div><div class="s" style="color:var(--gold)">re-verify cadence</div></div>'
         '<div class="kpi"><div class="v">%s</div><div class="l">next sign-off</div><div class="s muted">2026-06-30</div></div>'
         '</div>' % (review, due, QUARTER),
         GOV_NOTE,
         '<div class="panel"><h3>Configuration domains</h3>'
         '<div class="toolbar"><span class="btn pri">+ Propose change</span><span class="btn">Export changeset (JSON)</span>'
         '<span class="btn">Compare vs last quarter</span><span class="input" style="margin-left:auto">🔎 filter domains…</span></div>'
         '<div class="tablewrap"><table><thead><tr><th>Domain</th><th>Clinical owner</th><th>Items</th>'
         '<th>Last reviewed</th><th>Status</th><th>Note</th><th></th></tr></thead><tbody>']
    stmap = {"ok":("ok","Approved"),"review":("rev","Under review"),"due":("due","Refresh due"),"chg":("chg","Changes requested")}
    for (name, link, owner, items, last, stt, note) in DOMAINS:
        sk, sl = stmap[stt]
        h.append('<tr><td><b>%s</b></td><td class="small muted">%s</td><td class="mono">%d</td>'
                 '<td class="small mono">%s</td><td>%s</td><td class="small muted">%s</td>'
                 '<td><a class="btn" href="%s">Open ›</a></td></tr>'
                 % (name, _esc(owner), items, _esc(last), _st(sk, sl), _esc(note), link))
    h.append('</tbody></table></div></div>')
    h.append('<div class="grid c2"><div class="panel"><h3>Review workflow</h3>'
             '<p class="small muted">Each item follows the same path; see the full design on the '
             '<a href="admin-governance.html">Governance &amp; sign-off</a> screen.</p>'
             '<div class="tagrow"><span class="btn">Draft</span>→<span class="btn pri">Under review</span>'
             '→<span class="btn warn">Changes requested</span>/<span class="btn ok">Approved</span>'
             '→<span class="btn">Versioned &amp; gated</span></div></div>'
             '<div class="panel"><h3>This cycle at a glance</h3><ul class="small">'
             '<li>12 lab markers flagged for guideline re-verification (cadence, README §5.6)</li>'
             '<li>Stage 2b continuous-scoring constant <code>κ</code> added — biostatistics review (D23)</li>'
             '<li>CGM trust-tier weight increase proposed (Cardiology)</li>'
             '<li>Sepsis-screen WBC escalation threshold under revision (Chair)</li></ul></div></div>')
    return "Admin · Dashboard", "".join(h)

def build_admin_lab():
    h = [_crumb("Lab / biomarker ranges"), '<h1>Review — Lab &amp; Biomarker Ranges</h1>',
         _adminbar("Lab / biomarker range review"),
         '<p class="lead">The green/yellow/red bands that anchor every marker sub-score, with provenance and a '
         '<b>guideline-match</b> check. Source of truth mirrors '
         '<a class="xref" href="02-pillars-and-marker-catalog.html">Doc 02</a> / '
         '<a class="xref" href="appendix-biomarkers.html">Appendix A</a>.</p>', ILLUS,
         '<div class="toolbar"><span class="sel">Pillar: All ▾</span><span class="sel">Tier: All ▾</span>'
         '<span class="btn pri">Compare vs latest guideline</span><span class="btn">Bulk re-verify</span>'
         '<span class="input" style="margin-left:auto">🔎 marker…</span></div>',
         '<div class="callout note"><div class="ct">Guideline-match</div>'
         '<span class="status st-ok">✓ matches</span> band equals the cited guideline as last verified · '
         '<span class="status st-due">⚠ refresh</span> the guideline body has published an update since last review '
         '(re-verification cadence, README §5.6).</div>']
    # a representative slice across pillars with review metadata
    sample = [("CV", PILLARS[0][3][2]), ("CV", PILLARS[0][3][0]), ("MET", PILLARS[1][3][0]),
              ("REN", PILLARS[2][3][0]), ("REN", PILLARS[2][3][3]), ("INF", PILLARS[4][3][0]),
              ("NUT", PILLARS[8][3][0]), ("MCS", PILLARS[11][3][0])]
    meta = [("⚠ refresh","due","Dr. K. Lin","ESC/EAS 2025 update"),
            ("✓ matches","ok","Dr. S. Mehta","—"),("✓ matches","ok","Dr. S. Mehta","—"),
            ("⚠ refresh","due","Dr. R. Costa","KDIGO 2024 cystatin pref."),("✓ matches","ok","Dr. S. Mehta","—"),
            ("✓ matches","ok","Dr. S. Mehta","—"),("⚠ refresh","due","Dr. N. Okoye","Endocrine Soc. review"),
            ("locked","ok","Dr. N. Okoye","PHQ-9 item-9 hard rule")]
    h.append('<div class="tablewrap"><table><thead><tr><th>Marker</th><th>Pillar</th><th>Green</th>'
             '<th>Yellow</th><th>Red</th><th>Source</th><th>Guideline-match</th><th>Reviewer</th><th>Action</th></tr></thead><tbody>')
    for (pid, row), (gm, gk, rv, nx) in zip(sample, meta):
        (mk, unit, t, ts, g, y, r, w, src, crit) = row
        gmpill = _st("ok","✓ matches") if gm=="✓ matches" else (_st("due","⚠ refresh") if gm=="⚠ refresh" else _st("rev","🔒 locked"))
        act = '<span class="btn ok">Approve</span>' if gk=="ok" else '<span class="btn warn">Propose</span>'
        h.append('<tr><td><b>%s</b> <span class="small muted">%s</span></td><td><span class="chip b-mut">%s</span></td>'
                 '<td><span class="chip b-green">%s</span></td><td><span class="chip b-yellow">%s</span></td>'
                 '<td><span class="chip b-red">%s</span></td><td class="small muted">%s%s</td>'
                 '<td>%s</td><td class="small muted">%s</td><td>%s</td></tr>'
                 % (_esc(mk), _esc(unit), pid, _esc(g), _esc(y), _esc(r), _esc(src),
                    (" · "+_esc(nx)) if nx!="—" else "", gmpill, _esc(rv), act))
    h.append('</tbody></table></div>')
    # proposed-change diff card
    h.append('<div class="panel"><h3>Proposed change · ApoB high-risk threshold <span class="status st-rev">under review</span></h3>'
             '<div class="grid c2"><div><div class="small muted">Current (ESC/EAS, last verified 2024)</div>'
             '<p>Green <span class="chip b-green">&lt;80 (&lt;65 high-risk)</span> · Red <span class="chip b-red">≥100</span></p></div>'
             '<div><div class="small muted">Proposed (ESC/EAS 2025)</div>'
             '<p>Green <span class="chip b-green">&lt;80 (&lt;55 very-high-risk)</span> · Red <span class="chip b-red">≥100</span></p></div></div>'
             '<p class="small"><b>Rationale:</b> lower very-high-risk target per updated secondary-prevention guidance. '
             '<b>Evidence:</b> registry entry <code>EV-APOB-2025</code> (<a class="xref" href="14-evidence-registry-and-provenance.html">Doc 14</a>). '
             '<b>Impact sim:</b> affects 4.1% of cohort by ≤2 PureScore pts; no new criticals (re-validation pending, '
             '<a class="xref" href="13-validation-and-calibration-harness.html">Doc 13</a>).</p>'
             '<div class="toolbar"><span class="btn ok">Approve</span><span class="btn warn">Request changes</span>'
             '<span class="btn">Defer to next cycle</span><span class="btn">View impact report</span></div></div>')
    return "Admin · Lab ranges", "".join(h)

def build_admin_weights():
    h = [_crumb("Weights &amp; constants"), '<h1>Review — Weights, Constants &amp; Imputation</h1>',
         _adminbar("PureScore weights &amp; scoring constants"),
         '<p class="lead">Pillar weights, scoring constants (including the continuous Stage 2b responsiveness), '
         'within-pillar marker weights, and the imputation/confidence policy that governs fallback values. '
         'Math: <a class="xref" href="03-scoring-formula.html">Doc 03</a>.</p>', ILLUS,
         '<div class="panel"><h3>Base pillar weights <span class="muted small">W_k^base — personalized at runtime by cohort × goal × acute (Doc 03 §5.1)</span></h3>'
         '<div class="tablewrap"><table><thead><tr><th>Pillar</th><th>Base weight</th><th></th><th>Runtime multipliers</th></tr></thead><tbody>']
    mult = {"CV":"↑ diabetic, FH, post-menopause","MET":"↑ pre-diabetes, Ramadan T2D","REN":"↑ CKD, dialysis",
            "SLP":"↑ insomnia goal","FIT":"↑ fitness goal","MCS":"↑ mental-health goal","BCM":"↑ frailty/elderly"}
    for pid, w in PILLAR_W:
        h.append('<tr><td><b>%s</b></td><td class="mono">%.2f</td>'
                 '<td><span class="bar" style="width:160px"><i style="width:%d%%"></i></span></td>'
                 '<td class="small muted">%s</td></tr>' % (pid, w, int(w*100/0.13*100/100*100/100), _esc(mult.get(pid,"—"))))
    h.append('</tbody></table></div><p class="small muted">Weights are normalized to Σ=1 after personalization. '
             'Values illustrative — longevity/all-cause-mortality contribution drives the defaults.</p></div>')
    # constants
    h.append('<div class="panel"><h3>Scoring constants <span class="status st-rev">κ under review (D23)</span></h3>'
             '<div class="tablewrap"><table><thead><tr><th>Symbol</th><th>Meaning</th><th>Current</th><th>Ref</th><th>Status</th></tr></thead><tbody>')
    for sym, mean, val, ref in CONSTANTS:
        stt = _st("rev","new · D23") if sym=="κ" else _st("ok","approved")
        h.append('<tr><td class="mono"><b>%s</b></td><td class="small">%s</td><td class="mono">%s</td>'
                 '<td class="small muted">%s</td><td>%s</td></tr>' % (_esc(sym), _esc(mean), _esc(val), _esc(ref), stt))
    h.append('</tbody></table></div></div>')
    # imputation / q_source
    h.append('<div class="panel"><h3>Imputation &amp; confidence policy <span class="muted small">q_source — Doc 01 §2; D22</span></h3>'
             '<p class="small muted">When the patient\'s own data is missing or low-grade, the value is imputed from the '
             'cohort/literature with a reduced confidence that lowers pillar coverage and caps how green a pillar can display.</p>'
             '<div class="tablewrap"><table><thead><tr><th>Source</th><th>q_source</th><th>Handling</th></tr></thead><tbody>')
    for s, q, hd in QSOURCE:
        h.append('<tr><td class="small"><b>%s</b></td><td class="mono">%s</td><td class="small muted">%s</td></tr>'
                 % (_esc(s), _esc(q), _esc(hd)))
    h.append('</tbody></table></div></div>')
    # within-pillar weights (CV + MET examples)
    h.append('<div class="panel"><h3>Within-pillar marker weights <span class="muted small">w_i — examples; full set in Appendix A</span></h3>')
    for idx in (0,1):
        pid, pname, res, rows = PILLARS[idx]
        h.append('<h4>%s · %s</h4><div class="tablewrap"><table><thead><tr><th>Marker</th><th>w</th><th></th></tr></thead><tbody>' % (pid, _esc(pname)))
        for (mk, unit, t, ts, g, y, r, w, src, crit) in rows:
            wf = float(w)
            h.append('<tr><td class="small">%s</td><td class="mono">%s</td>'
                     '<td><span class="bar" style="width:200px"><i style="width:%d%%"></i></span></td></tr>'
                     % (_esc(mk), _esc(w), int(wf*100/0.35*100)//100 if False else int(min(100, wf*280))))
        h.append('</tbody></table></div>')
    h.append('</div>')
    return "Admin · Weights", "".join(h)

def build_admin_lifestyle():
    h = [_crumb("Lifestyle / PRO"), '<h1>Review — Lifestyle &amp; PRO Instruments</h1>',
         _adminbar("Lifestyle / PRO questionnaire review"),
         '<p class="lead">The validated questionnaires, their scoring, band cut-offs and pillar mapping. Full item '
         'text in <a class="xref" href="appendix-questions.html">Appendix C</a>.</p>', ILLUS,
         '<div class="callout safety"><div class="ct">Locked governance item</div>The <b>PHQ-9 item-9 suicidality '
         'hard rule</b> (forces MCS critical, fires crisis pathway) is a <b>locked safety control</b> — it cannot be '
         'edited in a routine quarterly cycle; changing it requires a full safety review '
         '(<a class="xref" href="11-safety-governance-and-regulatory.html">Doc 11</a> §2.2).</div>',
         '<div class="tablewrap"><table><thead><tr><th>Instrument</th><th>Pillar</th><th>Items</th>'
         '<th>Scoring</th><th>Bands</th><th>Cadence</th><th>Status</th></tr></thead><tbody>']
    for q in INSTRUMENTS:
        locked = q["id"] in ("PHQ-9","PHQ-2")
        stt = _st("rev","🔒 safety-locked") if locked else _st("ok","approved")
        h.append('<tr><td><b>%s</b><br><span class="small muted">%s</span></td><td class="small"><span class="chip b-teal">%s</span></td>'
                 '<td class="mono center">%d</td><td class="small muted">%s</td><td class="small">%s</td>'
                 '<td class="small muted">%s</td><td>%s</td></tr>'
                 % (_esc(q["id"]), _esc(q["src"]), _esc(q["pillar"]), len(q["items"]),
                    _esc(q["scoring"]), _esc(q["bands"]), _esc(q["cadence"]), stt))
    h.append('</tbody></table></div>')
    return "Admin · Lifestyle", "".join(h)

def build_admin_personas():
    h = [_crumb("Personas &amp; frames"), '<h1>Review — Personas &amp; Cohort Frames</h1>',
         _adminbar("Persona / cohort-frame review"),
         '<p class="lead">Cohort frames adjust interpretation (ranges, weight multipliers, confidence) without ever '
         'relaxing the clinical safety anchor. Detail in <a class="xref" href="appendix-personas.html">Appendix D</a>; '
         'sex frames <a class="xref" href="05-sex-specific-models.html">Doc 05</a>.</p>', ILLUS,
         '<div class="tablewrap"><table><thead><tr><th>Persona</th><th>Age</th><th>rep → Confidence</th>'
         '<th>Weight multipliers</th><th>Frame</th><th>Status</th></tr></thead><tbody>']
    for i,(k, label, age, meds, rep, wm, note) in enumerate(PERSONAS):
        stt = _st("rev","proposed") if k=="pregnancy" else _st("ok","approved")
        h.append('<tr><td><b>%s</b></td><td>%s</td><td class="mono">%s</td><td class="small mono">%s</td>'
                 '<td class="small muted">%s</td><td>%s</td></tr>'
                 % (_esc(label), age, _esc(rep), _esc(wm), _esc(note[:90]+("…" if len(note)>90 else "")), stt))
    h.append('</tbody></table></div>')
    h.append('<div class="panel"><h3>Band-override example · Older adult (84)</h3>'
             '<p class="small muted">Age-adjusted frames widen/shift bands where the general-adult cut-point would '
             'mislead — reviewed for clinical justification, never to mask danger.</p>'
             '<div class="tablewrap"><table><thead><tr><th>Marker</th><th>General-adult</th><th>Age-84 frame</th><th>Justification</th></tr></thead><tbody>'
             '<tr><td>BMI</td><td><span class="chip b-green">18.5–24.9</span></td><td><span class="chip b-green">23–28</span></td><td class="small muted">obesity-paradox / reverse-epidemiology in frailty</td></tr>'
             '<tr><td>Systolic BP</td><td><span class="chip b-green">90–119</span></td><td><span class="chip b-green">120–139</span></td><td class="small muted">J-curve; avoid over-treatment/falls</td></tr>'
             '<tr><td>eGFR</td><td><span class="chip b-yellow">60–89 yellow</span></td><td><span class="chip b-green">≥60 green</span></td><td class="small muted">age-expected nephron loss</td></tr>'
             '</tbody></table></div></div>')
    return "Admin · Personas", "".join(h)

def build_admin_governance():
    h = [_crumb("Governance &amp; sign-off"), '<h1>Governance — Quarterly Review &amp; Sign-off</h1>',
         _adminbar("Workflow, sign-off &amp; audit"),
         '<p class="lead">The control process behind every screen in this section: who proposes, who reviews, what '
         'gates a change must clear, and how it is signed off and audited.</p>', ILLUS, GOV_NOTE,
         '<div class="diagram"><div class="dt">Review workflow</div><pre class="mermaid">flowchart LR\n'
         '  D["Draft<br/>proposer + rationale + evidence"] --> R["Under review<br/>panel"]\n'
         '  R -->|quorum + chair| A["Approved"]\n'
         '  R -->|concerns| C["Changes requested"] --> D\n'
         '  A --> G{"release gates<br/>Doc 13"}\n'
         '  G -->|pass| V["Versioned &amp; released<br/>+ evidence registry (Doc 14)"]\n'
         '  G -->|fail| C\n'
         '  V --> AU["Immutable audit log (Doc 11)"]</pre></div>',
         '<div class="grid c2"><div class="panel"><h3>Sign-off panel · %s</h3>'
         '<div class="tablewrap"><table><tbody>'
         '<tr><td>Dr. A. Rahman <span class="small muted">— Chair</span></td><td class="right">%s</td></tr>'
         '<tr><td>Dr. S. Mehta <span class="small muted">— Lab Medicine</span></td><td class="right">%s</td></tr>'
         '<tr><td>Dr. L. Haddad <span class="small muted">— Biostatistics</span></td><td class="right">%s</td></tr>'
         '<tr><td>Dr. N. Okoye <span class="small muted">— Psychiatry</span></td><td class="right">%s</td></tr>'
         '<tr><td>Dr. K. Lin <span class="small muted">— Cardiology</span></td><td class="right">%s</td></tr>'
         '</tbody></table></div><p class="small muted">Quorum 4/5 + chair. E-signature + timestamp recorded per item.</p>'
         '<div class="toolbar"><span class="btn ok">Sign off approved items</span><span class="btn">Export minutes</span></div></div>'
         % (QUARTER, _st("ok","✓ signed"), _st("ok","✓ signed"), _st("rev","pending"),
            _st("ok","✓ signed"), _st("rev","pending")),
         '<div class="panel"><h3>Gates every change must clear</h3><ul class="small">'
         '<li><b>Release gates</b> — discrimination, calibration, early-warning PPV/lead-time, fairness slices, drift '
         '(<a class="xref" href="13-validation-and-calibration-harness.html">Doc 13</a>)</li>'
         '<li><b>Evidence registry</b> — provenance, citation, vintage updated (<a class="xref" href="14-evidence-registry-and-provenance.html">Doc 14</a>)</li>'
         '<li><b>No-proxy fairness</b> — no protected-class proxy worsens access/price/care (<a class="xref" href="11-safety-governance-and-regulatory.html">Doc 11</a> §6)</li>'
         '<li><b>Model-version bump</b> + re-validation before scale (<a class="xref" href="09-cohort-percentiles-and-validation.html">Doc 09</a>)</li></ul></div></div>',
         '<div class="panel"><h3>Audit log <span class="muted small">immutable · who / what / when / why</span></h3>'
         '<div class="tablewrap"><table><thead><tr><th>When</th><th>Who</th><th>Item</th><th>Change</th><th>Why</th></tr></thead><tbody>'
         '<tr><td class="small mono">2026-06-14 09:12</td><td class="small">Dr. L. Haddad</td><td class="small">constant κ</td><td class="small">add 0.10 (Stage 2b)</td><td class="small muted">D23 continuous responsiveness</td></tr>'
         '<tr><td class="small mono">2026-06-12 14:40</td><td class="small">Dr. K. Lin</td><td class="small">CGM tier weight</td><td class="small">0.85 → 0.90 (proposed)</td><td class="small muted">device validation data</td></tr>'
         '<tr><td class="small mono">2026-03-12 11:05</td><td class="small">Dr. A. Rahman</td><td class="small">Q1 cycle</td><td class="small">42 items signed off</td><td class="small muted">quarterly review</td></tr>'
         '</tbody></table></div></div>']
    return "Admin · Governance", "".join(h)

ADMIN_PAGES = [
 ("admin-index.html", build_admin_index),
 ("admin-lab-ranges.html", build_admin_lab),
 ("admin-weights.html", build_admin_weights),
 ("admin-lifestyle.html", build_admin_lifestyle),
 ("admin-personas.html", build_admin_personas),
 ("admin-governance.html", build_admin_governance),
]
APPENDIX_PAGES_NOTE = None
