# -*- coding: utf-8 -*-
"""Data + appendix/admin page builders for the PureScore tech wiki.
All values are illustrative, literature/guideline-anchored, and must be re-verified
(README §5.6) before production. Compiled from docs 00-18."""

import json, os, re
_HERE = os.path.dirname(os.path.abspath(__file__))
def _load(name):
    with open(os.path.join(_HERE, "data", name), encoding="utf-8") as f:
        return json.load(f)

# ----------------------------------------------------------------- calc-graph resolver
# data/calc-graph.json + data/pillar-weights.json + data/constants.json are CANONICAL.
# resolve_calc_data() only RESOLVES references into one dict; it invents no values.
# write_calc_data() emits assets/calc-data.js (generated) consumed by assets/engine.js,
# so every compute page shares one source over file:// (no runtime fetch / CORS).
def _const_num(s):
    """First float out of a constants.json value string ('0.60', '0.15 / 0.50' → 0.6/0.15)."""
    m = re.search(r"-?\d+(?:\.\d+)?", str(s))
    return float(m.group(0)) if m else None

def resolve_calc_data():
    g = _load("calc-graph.json")
    weights = {k: v for k, v in _load("pillar-weights.json")["weights"]}
    consts = {c[0]: c[2] for c in _load("constants.json")["constants"]}
    missing = []
    for pid, p in g["pillars"].items():
        if p["wRef"] not in weights:
            missing.append("pillar %s wRef %s" % (pid, p["wRef"])); continue
        p["weight"] = weights[p["wRef"]]
    sym = g["_meta"]["uses_constants"]
    for s in sym.values():
        if s not in consts: missing.append("constant %s" % s)
    if missing:
        raise ValueError("calc-graph.json references unresolved: " + "; ".join(missing))
    g["const"] = {"delta": _const_num(consts[sym["delta"]]), "gamma": _const_num(consts[sym["gamma"]]),
                  "rho": _const_num(consts[sym["rho"]]), "kappa_resp": _const_num(consts[sym["kappa_resp"]]),
                  "r_crit": _const_num(consts[sym["r_crit"]]), "pure_crit_cap": _const_num(consts[sym["pure_crit_cap"]]),
                  "phi": _const_num(consts[sym["phi"]]),
                  "q_impute": _const_num(consts[sym["q_impute"]]), "rp_impute": _const_num(consts[sym["rp_impute"]]),
                  "cov_green_floor": _const_num(consts[sym["cov_green_floor"]])}
    g["_constants_raw"] = _load("constants.json")["constants"]
    g["_weights_raw"] = _load("pillar-weights.json")["weights"]
    _attach_panels(g)
    return g

# Attach (build-time, from canonical JSON) the PRO instruments and wearable metadata the
# Flight-Deck cockpit shows: PHQ-9/GAD-7/ISI items so a PRO marker renders as a real
# questionnaire, and trust-tier/accuracy/devices for wearable markers.
_MARKER_INSTRUMENT = {"phq": "PHQ-9", "gad": "GAD-7", "isi": "ISI"}
def _attach_panels(g):
    try:
        instruments = {i["id"]: i for i in _load("instruments.json")["instruments"]}
    except Exception:
        instruments = {}
    g["instruments"] = {}
    for mk, iid in _MARKER_INSTRUMENT.items():
        it = instruments.get(iid)
        if not it or not it.get("items"):
            continue
        nums = [int(n) for n in re.findall(r"(\d+)\s*=", it.get("scale", ""))]
        maxlvl = max(nums) if nums else 3
        g["instruments"][mk] = {"id": iid, "name": it.get("name", iid), "stem": it.get("stem", ""),
                                "scale": it.get("scale", ""), "items": it["items"],
                                "maxlvl": maxlvl, "scoring": it.get("scoring", "")}
    # wearable metadata by loose name match (wearables.json rows: [name, agg, tier, accuracy, pillars, devices])
    try:
        wrows = _load("wearables.json")["wearables"]
    except Exception:
        wrows = []
    def _match(label):
        low = label.lower()
        keys = {"rhr": "resting hr", "hrv": "hrv", "spo2": "spo", "sleepdur": "sleep",
                "sleepeff": "sleep", "vo2": "vo", "steps": "step", "mvpa": "mvpa"}
        for r in wrows:
            nm = str(r[0]).lower()
            if low in keys and keys[low] in nm:
                return {"metric": r[0], "tier": r[2], "accuracy": r[3], "devices": r[5] if len(r) > 5 else ""}
        return None
    g["wearmeta"] = {}
    for mk, mm in g["markers"].items():
        if mm.get("source") == "wearable":
            wm = _match(mk)
            if wm:
                g["wearmeta"][mk] = wm
    # adherence check-in items (Appendix H) — for the patient check-in card
    try:
        ad = _load("adherence.json")["items"]
    except Exception:
        ad = []
    g["adherence_items"] = [{"ref": x.get("ref"), "family": x.get("nudge_family"), "pillars": x.get("pillars", []),
                             "cadence": x.get("cadence"), "stem": x.get("stem"),
                             "responses": [{"label": r.get("label"), "adherence": r.get("adherence")} for r in x.get("responses", [])]}
                            for x in ad]
    # patient goals (Appendix J) — for the next-actions queue
    try:
        gl = _load("goals.json")["goals"]
    except Exception:
        gl = []
    g["goals"] = [{"id": x.get("id"), "title": x.get("title"), "pillar": x.get("pillar"),
                   "metric": {"name": x.get("metric", {}).get("name"), "target": x.get("metric", {}).get("target"),
                              "horizon_wk": x.get("metric", {}).get("horizon_wk"), "source": x.get("metric", {}).get("source")},
                   "app": {"sex": x.get("applicability", {}).get("sex", "any"),
                           "age_range": x.get("applicability", {}).get("age_range", [0, 120]),
                           "life_stage": x.get("applicability", {}).get("life_stage", "any")}}
                  for x in gl]
    try: g["conditions"] = _load("conditions.json")
    except Exception: g["conditions"] = {"diseases": {}, "meds": {}}
    try: g["adherence_actions"] = _load("adherence-actions.json")
    except Exception: g["adherence_actions"] = {"barriers": {}, "generic": []}
    try: qb = _load("question-bank.json")["questions"]
    except Exception: qb = []
    def _qtrim(q):
        dim = q.get("dimensions", {}) or {}; age = dim.get("age", "all"); amin, amax = 0, 120
        if isinstance(age, dict): amin, amax = age.get("min", 0), age.get("max", 120)
        pset = set()
        for r in q.get("responses", []):
            for pk in (r.get("pillars") or {}): pset.add(pk)
        resp = [{"l": r.get("label"), "p": sorted((r.get("pillars") or {}).keys())} for r in q.get("responses", [])][:6]
        return {"id": q.get("id"), "text": q.get("text"), "cat": q.get("category"), "cadence": q.get("cadence"),
                "sex": dim.get("sex", ["all"]), "amin": amin, "amax": amax, "pillars": sorted(pset),
                "conditions": (q.get("conditions") or [])[:4], "responses": resp}
    onb = [q for q in qb if q.get("cadence") in ("Core", "once")]
    per = sorted([q for q in qb if q.get("cadence") in ("quarterly", "annual")], key=lambda q: q.get("priority", 5))[:60]
    g["questions"] = [_qtrim(q) for q in (onb + per)][:130]
    return g

def write_calc_data():
    g = resolve_calc_data()
    js = ("/* GENERATED by build_wiki.py from data/calc-graph.json + pillar-weights.json + constants.json. "
          "Do NOT edit — edit the JSON. */\nwindow.PURESCORE_DATA=" +
          json.dumps(g, ensure_ascii=False, separators=(",", ":")) + ";\n")
    with open(os.path.join(_HERE, "assets", "calc-data.js"), "w", encoding="utf-8") as f:
        f.write(js)
    return g

def write_cite_data():
    """Emit assets/cite-data.js (window.PURESCORE_CITATIONS) from data/citations.json so the
    citation popovers work over file:// without a runtime fetch."""
    try:
        c = _load("citations.json")
    except Exception:
        c = {"sources": {}, "aliases": {}}
    js = ("/* GENERATED from data/citations.json — do NOT edit, edit the JSON. */\n"
          "window.PURESCORE_CITATIONS=" + json.dumps(c, ensure_ascii=False, separators=(",", ":")) + ";\n")
    with open(os.path.join(_HERE, "assets", "cite-data.js"), "w", encoding="utf-8") as f:
        f.write(js)
    return c

# Citation chip: wraps a marker's source token; assets/citations.js wires the click-popover from
# window.PURESCORE_CITATIONS. data-mk carries the marker name (for PubMed-search of generic tags).
_CITE_ASSETS = ('<script src="assets/cite-data.js"></script><script src="assets/citations.js"></script>')
def _cite_chip(src, mk=""):
    s = (src or "").strip()
    if not s:
        return ""
    return '<span class="cite" data-src="%s" data-mk="%s" tabindex="0">%s</span>' % (_esc(s), _esc(mk or ""), _esc(s))

def write_range_data():
    """Emit assets/range-data.js (window.PURESCORE_RANGES) from data/range-variations.json + the
    conditions catalogue (for the context-selector dropdowns) so the range-variation views work offline."""
    try:
        rv = _load("range-variations.json")
    except Exception:
        rv = {"markers": {}, "med_introduces": {}}
    try:
        rv["_conditions"] = _load("conditions.json")
    except Exception:
        rv["_conditions"] = {"diseases": {}, "meds": {}}
    js = ("/* GENERATED from data/range-variations.json — do NOT edit, edit the JSON. */\n"
          "window.PURESCORE_RANGES=" + json.dumps(rv, ensure_ascii=False, separators=(",", ":")) + ";\n")
    with open(os.path.join(_HERE, "assets", "range-data.js"), "w", encoding="utf-8") as f:
        f.write(js)
    return rv

try:
    _RV_NAMES = set(_load("range-variations.json")["markers"])     # marker names whose range varies by dimension
except Exception:
    _RV_NAMES = set()
_RANGE_ASSETS = '<script src="assets/range-data.js"></script><script src="assets/ranges.js"></script>'

def write_flag_data():
    """Emit assets/flags-data.js (window.PURESCORE_FLAGS) from data/clinical-flags.json — the
    adversarial clinical-audit flags that drive inline ⚠ badges (read-only)."""
    try:
        f = _load("clinical-flags.json")
    except Exception:
        f = {"flags": {}}
    js = ("/* GENERATED from data/clinical-flags.json — adversarial clinical-validity audit flags (read-only). */\n"
          "window.PURESCORE_FLAGS=" + json.dumps(f, ensure_ascii=False, separators=(",", ":")) + ";\n")
    with open(os.path.join(_HERE, "assets", "flags-data.js"), "w", encoding="utf-8") as fh:
        fh.write(js)
    return f
_FLAG_ASSETS = '<script src="assets/flags-data.js"></script><script src="assets/clinical-flags.js"></script>'

def write_unit_data():
    """Emit assets/units-data.js (window.PURESCORE_UNITS) from data/units.json — the SI-canonical
    unit registry + conversions that drive dual-unit display and the inbound conversion layer.
    Merges each marker's calc-graph label so the UI can match appendix/resolver rows by name."""
    try:
        u = _load("units.json")
    except Exception:
        u = {"_meta": {}, "markers": {}}
    try:
        cg = _load("calc-graph.json")
        labels = {k: v.get("label", k) for k, v in cg.get("markers", {}).items()}
    except Exception:
        labels = {}
    extra = {"lpa": "Lp(a)"}
    for mk, v in u.get("markers", {}).items():
        v["label"] = labels.get(mk, extra.get(mk, mk))
    js = ("/* GENERATED from data/units.json — SI-canonical unit registry & conversions. Edit the JSON. */\n"
          "window.PURESCORE_UNITS=" + json.dumps(u, ensure_ascii=False, separators=(",", ":")) + ";\n")
    with open(os.path.join(_HERE, "assets", "units-data.js"), "w", encoding="utf-8") as fh:
        fh.write(js)
    return u
_UNIT_ASSETS = '<script src="assets/units-data.js"></script><script src="assets/units.js"></script>'

# pillar → intake-question domain (split domains attribute to both pillars)
_PILLAR_DOMAIN = {"CV": "CARDIOMETABOLIC", "MET": "CARDIOMETABOLIC", "REN": "RENAL_HEPATIC", "HEP": "RENAL_HEPATIC",
    "INF": "INFLAMM_IMMUNE_ALLERGY", "HEM": "HEMATOLOGIC", "ENDO": "ENDOCRINE_HORMONAL", "BCM": "BODYCOMP_MSK",
    "NUT": "NUTRITION_GI", "SLP": "SLEEP_FATIGUE", "FIT": "FITNESS_ACTIVITY", "MCS": "MENTAL_COGNITIVE_SOCIAL"}
# precise sub-channel → bucket + short chip label
_CHAN_BUCKET = {"biomarker-lab": ("bio", "lab"), "derived": ("bio", "derived"),
    "wearable-clinical": ("wear", "clinical"), "wearable-consumer": ("wear", "wearable"),
    "wearable-inferential": ("wear", "inferred"), "self-report": ("pro", "PRO")}

def write_pillar_data():
    """Emit assets/pillars-data.js (window.PURESCORE_PILLARS) — per-pillar weight W_k, description, and
    the FULL input coverage grouped by channel (biomarkers · wearables · self-report/PROs) with each item
    deep-linked to its Appendix A row, plus the intake-question count for the pillar's domain. Drives the
    interactive pillar-fan hover popover."""
    try:
        W = dict(_load("pillar-weights.json")["weights"])
    except Exception:
        W = {}
    try:
        qdom = {d["domain"]: d["count"] for d in _load("question-bank.json")["meta"]["domain_codes"]}
    except Exception:
        qdom = {}
    out = {}
    for pid, name, desc, rows in PILLARS:
        groups = {"bio": [], "wear": [], "pro": []}
        for r in rows:
            mk = r[0]; src = r[8] if len(r) > 8 else ""
            bucket, chip = _CHAN_BUCKET.get(marker_channel(mk, src), ("bio", "lab"))
            groups[bucket].append({"n": mk, "ch": chip,
                                   "crit": bool(r[9]) if len(r) > 9 else False,
                                   "a": "mk-%s-%s" % (pid.lower(), _slug_metric(mk))})
        dom = _PILLAR_DOMAIN.get(pid)
        out[pid] = {"name": name, "desc": desc, "weight": W.get(pid), "n": len(rows),
                    "bio": groups["bio"], "wear": groups["wear"], "pro": groups["pro"],
                    "q": {"n": qdom.get(dom, 0), "domain": dom,
                          "label": (dom or "").lower().replace("_", " ")}}
    js = ("/* GENERATED from pillars.json + pillar-weights.json + question-bank.json — pillar-fan hover popover. Edit the JSON. */\n"
          "window.PURESCORE_PILLARS=" + json.dumps(out, ensure_ascii=False, separators=(",", ":")) + ";\n")
    with open(os.path.join(_HERE, "assets", "pillars-data.js"), "w", encoding="utf-8") as f:
        f.write(js)
    return out

def write_weights_data():
    """Emit assets/weights-data.js (window.PURESCORE_WEIGHTS) — the weighted graph for the pillar
    weights & correlations diagram: pillar W_k + marker→pillar weights/channels, reservoir→pillar
    feeds, and reservoir↔reservoir coupling (from calc-graph.json)."""
    try:
        W = dict(_load("pillar-weights.json")["weights"])
    except Exception:
        W = {}
    try:
        GLOSS = _load("marker-glossary.json")["markers"]
    except Exception:
        GLOSS = {}
    def fw(s):
        try:
            return round(float(str(s).strip()), 4)
        except Exception:
            return 0.0
    # calc-graph marker key → display label (to resolve reservoir inputs)
    cg = {}
    try:
        cg = _load("calc-graph.json")
    except Exception:
        cg = {}
    cglabel = {k: v.get("label", k) for k, v in cg.get("markers", {}).items()}
    # reservoir inputs (display labels) + feeds
    reservoirs = {}; coupling = []
    for rk, rv in cg.get("reservoirs", {}).items():
        reservoirs[rk] = {"label": rv.get("label", rk), "basis": rv.get("basis", ""),
                          "feeds": (rv.get("feeds", "") or "").upper(), "polarity": rv.get("polarity", ""),
                          "inputs": [cglabel.get(i[0], i[0]) for i in rv.get("inputs", [])]}
    for c in cg.get("reservoir_coupling", []):
        t = c.get("t")
        for s, coef in c.get("add", []):
            coupling.append({"from": s, "to": t, "coef": coef, "kind": "add"})
        for s, coef in c.get("subDeficit", []):
            coupling.append({"from": s, "to": t, "coef": coef, "kind": "sub"})
    def norm(s):
        return _re.sub(r"[^a-z0-9]", "", (s or "").lower())
    # marker-name → reservoirs that consume it (lenient normalized match within the fed pillar)
    def res_for_marker(pid, mkname):
        out2 = []
        nm = norm(mkname)
        for rk, rv in reservoirs.items():
            if rv["feeds"] != pid:
                continue
            for inp in rv["inputs"]:
                ni = norm(inp)
                if ni and (ni == nm or nm.startswith(ni) or ni.startswith(nm)):
                    out2.append(rk); break
        return out2
    # ---- sex/age applicability: range-variations.json dims + curated supplement ----
    try:
        _rv = _load("range-variations.json")["markers"]
    except Exception:
        _rv = {}
    rvsex, rvage, rvstage = set(), set(), {}
    for nm2, m2 in _rv.items():
        dims = set(v.get("dim") for v in m2.get("var", []))
        if "sex" in dims: rvsex.add(nm2)
        if "age_band" in dims: rvage.add(nm2)
        lsk = [v.get("key", "") for v in m2.get("var", []) if v.get("dim") == "life_stage"]
        if lsk:
            sg = set()
            for k in lsk:
                kl = k.lower()
                if "pregnan" in kl: sg.add("pregnancy")
                if "menopaus" in kl: sg.add("menopause")
            rvstage[nm2] = sorted(sg) or ["life-stage"]
    CURATED = {
        "Sex hormones (E2/Prog/FSH/LH/T/SHBG/AMH)": {"sex": "diff", "age": "adj", "agn": "shifts at menopause / andropause", "stage": ["menopause"]},
        "Body fat % (DEXA)": {"sex": "diff"}, "BMD T-score (DEXA)": {"sex": "diff", "age": "gated", "agn": "screened in older adults"},
        "Grip strength": {"sex": "diff", "age": "adj"}, "Grip strength (shared w/ BCM)": {"sex": "diff", "age": "adj"},
        "CAC": {"age": "gated", "agn": "typically measured ≥40 y"}, "Cognitive screen (age-adj)": {"age": "gated", "agn": "older adults; age-normed"},
        "Gait speed": {"age": "gated", "agn": "frailty / older adults"}, "Hematocrit": {"sex": "diff"}, "RDW": {"sex": "diff"},
        "Iron saturation": {"sex": "diff"}, "Prolactin": {"sex": "diff"}, "eGFR (creatinine)": {"sex": "diff"},
        "Free T4": {"age": "adj"}, "VO₂max (est.)": {"sex": "diff", "age": "adj"},
    }
    def applic(mk):
        cur = CURATED.get(mk, {})
        sx = cur.get("sex") or ("diff" if mk in rvsex else "all")
        ag = cur.get("age") or ("adj" if mk in rvage else "all")
        stg = sorted(set(rvstage.get(mk, [])) | set(cur.get("stage", [])))
        return sx, ag, cur.get("agn", ""), stg
    pillars = {}
    for pid, name, desc, rows in PILLARS:
        ms = []
        for r in rows:
            mk = r[0]; src = r[8] if len(r) > 8 else ""
            _, chip = _CHAN_BUCKET.get(marker_channel(mk, src), ("bio", "lab"))
            gl = GLOSS.get(mk, {}); sx, ag, agn, stg = applic(mk)
            ms.append({"n": mk, "full": gl.get("full", mk), "desc": gl.get("desc", ""),
                       "w": fw(r[7] if len(r) > 7 else 0), "ch": chip,
                       "unit": r[1] if len(r) > 1 else "", "tier": r[2] if len(r) > 2 else "",
                       "g": r[4] if len(r) > 4 else "", "y": r[5] if len(r) > 5 else "", "r": r[6] if len(r) > 6 else "",
                       "src": src, "crit": bool(r[9]) if len(r) > 9 else False,
                       "res": res_for_marker(pid, mk), "sx": sx, "ag": ag, "agn": agn, "stg": stg,
                       "a": "mk-%s-%s" % (pid.lower(), _slug_metric(mk))})
        pillars[pid] = {"name": name, "desc": desc, "w": W.get(pid, 0), "markers": ms}
    out = {"pillars": pillars, "reservoirs": reservoirs, "coupling": coupling}
    js = ("/* GENERATED from pillar-weights.json + pillars.json + calc-graph.json — pillar weights & correlations diagram. */\n"
          "window.PURESCORE_WEIGHTS=" + json.dumps(out, ensure_ascii=False, separators=(",", ":")) + ";\n")
    with open(os.path.join(_HERE, "assets", "weights-data.js"), "w", encoding="utf-8") as f:
        f.write(js)
    return out

# ----------------------------------------------------------------- per-doc summaries
SUMMARY = {
 "01":"Vision, design principles, and the institutional lessons (Babylon / Kaiser / Mayo) PureScore is engineered around.",
 "06":"Canonical FHIR-aligned data model, the four flagged input streams, measurement tiers, provenance/confidence, and literature reference-range strategy.",
 "02":"The 12 pillars and the full marker catalogue — tiers, two-sidedness, green/yellow/red bands, within-pillar weights and sources.",
 "03":"The deterministic marker→pillar→PureScore math, continuous personalized scoring (Stage 2b), the critical cascade and the feedback loop.",
 "04":"MONIAC reservoir dynamics: stocks, leakage, cross-pillar interference and valves that give PureScore memory.",
 "08":"Sex-specific models: cycle, fertility, pregnancy, post-partum, menopause, andropause, and hormone-therapy-aware ranges.",
 "09":"Acute-event override & revert, and care/nutrition/exercise plans by life stage.",
 "11":"The daily top-5 nudge engine (impact-attributed, ease-weighted, diverse, safe) plus the delivery engine: channels, send-time, quiet hours, consent, PHI-safe payloads and guaranteed safety-critical escalation.",
 "10":"Validated clinical scores (FINDRISC, ASCVD/SCORE2, KDIGO, FIB-4, FRAX, PhenoAge) integrated for clinicians — feeding max risk, never relaxing it.",
 "13":"Cohort construction, empirical-Bayes shrinkage, calibration, fairness slices and drift monitoring.",
 "19":"The actuarial/pricing layer — gated, firewalled, and heavily flagged for regulatory & fairness risk.",
 "16":"Clinician-in-the-loop safety, escalation tiers, the crisis pathway, FMEA, privacy/consent and model governance.",
 "05":"Beyond the number: context-aware interpretation, the companion meta-vector, personal-baseline early-warning and honest dual framing (the PureScore 2.0 layer).",
 "14":"Validation & calibration gates for 2.0 — discrimination, calibration, early-warning PPV/lead-time, fairness, drift, release gates.",
 "15":"Machine-readable evidence registry, per-band provenance and the cold-start ‘ignition’ model (guideline → cohort → personal).",
 "18":"UAE localization: ethnicity-aware cut-points & screening (context, never penalty), regional epidemiology and Ramadan (IDF-DAR) safety.",
 "12":"The exhaustive, UAE-prioritized recommended-action / nudge library by condition, with adherence & screening actions.",
 "17":"Adversarial self-review: every clinician/statistician/ethicist/regulator objection → how handled → honest residual.",
 "07":"Input taxonomy & trust flags, wearable trust-tiering (D22), wearables×pillars, the effortless product surfaces and household accounts.",
}

# ----------------------------------------------------------------- per-doc mermaid
MERMAID = {
 "01":("Lessons → principles", """flowchart LR
  B["Babylon<br/>overclaim, opaque"] -->|avoid| P(("PureScore"))
  K["Kaiser<br/>care-gap closure"] -->|emulate| P
  M["Mayo<br/>clinician-in-loop"] -->|emulate| P
  P --> S["Safety dominates"]
  P --> X["Explainable"]
  P --> E["Effortless"]"""),
 "06":("Four streams → measurement", """flowchart TD
  L["LAB · clinical-grade"] --> M["Measurement<br/>source + confidence"]
  W["WEAR · tiered (D22)"] --> M
  G["GOAL · intent"] --> M
  F["LIFE · self-report"] --> M
  M --> C{"own recent data?"}
  C -->|yes| U["use value"]
  C -->|no| R["literature fallback<br/>low confidence"]"""),
 "02":("PureScore → 12 pillars", """flowchart TB
  PS(("PureScore")) --> CV & MET & REN & HEP
  PS --> INF & HEM & ENDO & BCM
  PS --> NUT & SLP & FIT & MCS"""),
 "03":("The scoring pipeline — uber-view (hover any node for detail · click a stage to jump to its section)", """flowchart TB
  subgraph IN["Inputs · raw marker values x_i"]
    direction LR
    LAB["Labs<br/>clinical-grade"]:::io
    WEAR["Wearables<br/>tiered · D22"]:::io
    PRO["PROs / LIFE<br/>self-report"]:::io
  end
  IN --> S1
  S1["<b>Stage 1 · clinical band</b><br/>r_i&#94;clin ∈ [0,1] · continuous<br/>optimum → 0.15 → 0.50 → saturating"]:::safe
  S2["<b>Stage 2 · cohort blend</b><br/>r_i = max( r_clin , φ·r_cohort )<br/>raise-only · φ = 0.6"]:::safe
  S2B["<b>Stage 2b · personal baseline</b><br/>+ κ·tanh(z_i/2) · band_clamp<br/>responsiveness · κ = 0.10"]:::resp
  S3["<b>Stage 3 · pillar risk R_k</b><br/>γ-power-mean(ŵ_i , r_i) + ρ·B̃_k<br/>worst-sensitive · γ = 3"]:::worst
  S4["<b>Stage 4 · status + critical override</b><br/>critical-marker red ⇒ R_k ← R_crit 0.60"]:::safe
  S5["<b>Stage 5 · PureScore°</b><br/>δ-power-mean(W_k , R_k) · δ = 2<br/>W_k = base · cohort · goal · acute"]:::worst
  S1 --> S2 --> S2B --> S3 --> S4 --> S5
  S4 -.-> PILL["<b>per-pillar layer</b><br/>S_k = 100·(1 - R_k) · status R/Y/G<br/>coverage · binding constraint"]:::ps
  S5 --> CR{"any pillar<br/>critical?"}:::safe
  CR -->|yes| CAP["<b>Critical cascade</b><br/>PureScore ≤ 40 · status CRITICAL<br/>emergency ⇒ escalate · Doc 16"]:::crit
  CR -->|no| OUT["<b>PureScore 0–100</b><br/>+ overall_status"]:::ps
  RNG["Personalized ranges<br/>sex · age · condition · range-variations<br/>Doc 08 / 09"]:::pers -->|reshapes bands| S1
  COH["Cohort percentile g(q_i) · shrinkage<br/>low representativeness ⇒ suppress φ<br/>Doc 13"]:::meta -->|r_cohort| S2
  RES[("Reservoirs B̃_k<br/>chronic burden · Doc 04")]:::reser -->|ρ_k = 0.2| S3
  CONF["confidence_i · coverage cov_k<br/>missing ⇒ impute median / drop · D33<br/>Doc 06 §2"]:::meta -->|ŵ_i = w_i·conf| S3
  CS["Clinical scores · gated · Doc 10<br/>ASCVD · FINDRISC · KDIGO · FIB-4 · FRAX · PhenoAge"]:::pers -->|feedback as max · raise-only| S3
  PERS["personal z_i · EB baseline μ,σ<br/>Doc 05 §5.1"]:::meta --> S2B
  WT["cohort · goal · acute multipliers<br/>Doc 08 / 09 / 10"]:::pers --> S5
  S2B -.->|same z_i stream| COMP["Companion vector<br/>Confidence · Trajectory · Early-warning · Modifiability…<br/>Doc 05 §4"]:::meta
  OUT --> EXP["Explainability object<br/>binding constraint · top contributors<br/>§7 → nudges Doc 11"]:::meta
  classDef io fill:#0c1c2b,stroke:#4aa3df,color:#cfe8ff;
  classDef safe fill:#10202a,stroke:#4aa3df,color:#cfe8ff;
  classDef resp fill:#241f0c,stroke:#edb14a,color:#ffe6b0;
  classDef worst fill:#141a2e,stroke:#8b97f0,color:#dfe4ff;
  classDef crit fill:#241016,stroke:#f0606e,color:#ffd0d6,stroke-width:2px;
  classDef ps fill:#10241c,stroke:#3ad6a0,color:#bdf5e0,stroke-width:2px;
  classDef reser fill:#0c2018,stroke:#2dd4bf,color:#bff5ec;
  classDef meta fill:#1c1226,stroke:#a98bd6,color:#e6d8ff;
  classDef pers fill:#2a1606,stroke:#f0903a,color:#ffd9b0;
  click S1 "#1-stage-1-marker-risk-from-clinical-bands" "Stage 1 — clinical band: raw x_i becomes marker risk r in [0,1] from green/yellow/red cuts (0.15 / 0.50), optimum-centred within green. Continuous and monotone, smooth except intentional critical steps."
  click S2 "#2-stage-2-blend-with-cohort-percentile-safety-dominant" "Stage 2 — cohort blend: r = max(r_clin, phi*r_cohort), phi 0.6. Raise-only — the cohort percentile can add concern but never lowers the clinical anchor; powers stack-ranking."
  click S2B "#2b-stage-2b-personal-baseline-responsiveness-the-feedback-te" "Stage 2b — personal baseline: z = (x - mu)/sigma (empirical-Bayes, shrinks to cohort at cold-start). Feedback kappa*tanh(z/2), kappa 0.10 cap, band-clamped so it never relaxes red or critical. The same z drives Trajectory and Early-warning."
  click S3 "#3-stage-3-pillar-risk-worst-sensitive-aggregation-confidence" "Stage 3 — pillar risk: R_k = gamma-power-mean of confidence-weighted marker risks (w_hat = w*confidence), gamma 3 (worst-sensitive), plus rho*B_tilde reservoir load. S_k = 100*(1 - R_k)."
  click S4 "#4-stage-4-pillar-status-and-the-critical-override" "Stage 4 — critical override: a red critical marker forces R_k up to at least R_crit 0.60; the pillar status becomes green, yellow or red."
  click S5 "#5-stage-5-purescore-personalized-weights-critical-cascade" "Stage 5 — PureScore: W_k = base*cohort*goal*acute (normalized to sum 1); R_total = delta-power-mean(W_k, R_k), delta 2; PureScore = 100*(1 - R_total)."
  click PILL "#4-stage-4-pillar-status-and-the-critical-override" "Per-pillar layer: each pillar exposes S_k = 100*(1 - R_k), a status (green/yellow/red), coverage, and its binding constraint and top contributors."
  click CAP "#5-3-critical-cascade-the-safety-cap" "Critical cascade: any critical pillar caps PureScore at 40 and sets status CRITICAL; an emergency tier escalates to a human (Doc 16). It cannot be averaged away."
  click RNG "08-sex-specific-models.html" "Personalized ranges: sex, age-band, life-stage and condition shift the Stage-1 bands (range-variations) — for example J-curve BP for the elderly, pregnancy frames, and tighter CKD or diabetes targets."
  click COH "13-cohort-percentiles-and-validation.html" "Cohort percentile g(q): adverse percentile mapped to risk with empirical-Bayes shrinkage. If representativeness is low (out-of-distribution or a small cell) the cohort blend phi is suppressed and confidence is reduced."
  click CS "10-clinical-scores-integration.html" "Clinical scores (gated, clinician-facing): ASCVD/SCORE2, FINDRISC, KDIGO/KFRE, FIB-4, FRAX, PhenoAge. They feed back as max() — raise risk or weight only, and never clear a critical pillar or lift the cap."
  click CONF "06-data-model-and-reference-ranges.html" "Confidence: coverage x source quality x stability. Missing markers impute to the age x sex cohort median (D33) or drop and lower confidence; low-confidence markers are down-weighted (w_hat)."
  click PERS "05-critical-review-and-purescore-2.0.html" "Personal baseline z from empirical-Bayes mu and sigma (Doc 05 section 5.1)."
  click WT "08-sex-specific-models.html" "Pillar weights W_k = base longevity weight x cohort (disease) x goal x acute multipliers, normalized to sum 1 (Doc 08/09/10)."
  click RES "04-moniac-reservoir-dynamics.html" "Reservoirs (Doc 04): 15 MONIAC stocks with kappa coupling; each adds rho*B_tilde of chronic burden or asset load to its pillar."
  click COMP "05-critical-review-and-purescore-2.0.html" "Companion vector (Doc 05 section 4): Confidence, Data-sufficiency, Criticality, Trajectory, Early-warning (Watch to Advisory to Alert), Representativeness, Skew, Volatility, Modifiability and Stress-load — they ride alongside the number and never move the anchor."
  click EXP "11-daily-nudge-engine.html" "Explainability: the binding constraint, top contributors and weights feed the nudge engine (Doc 11).\""""),
 "04":("Reservoir hydraulics", """flowchart LR
  IN["inflows<br/>behaviours / values"] --> RES[("Reservoir B_j<br/>stock + memory")]
  V["valves<br/>interventions"] --> IN
  RES -->|leak λ| H["heal / decay"]
  RES -->|interference κ| RES2[("coupled reservoir")]
  RES --> RK["pillar risk + ρ·B̃"]"""),
 "08":("Hormonal milieu → ranges", """flowchart TD
  N["natal sex"] --> B["physiology baseline"]
  H["hormone milieu<br/>cycle / preg / meno / HRT"] --> B
  B --> R["phase-aware reference ranges"]"""),
 "09":("Acute mode lifecycle", """stateDiagram-v2
  [*] --> Baseline
  Baseline --> AcuteMode : life event (override weights)
  AcuteMode --> Baseline : recovery + hysteresis
  AcuteMode --> AcuteMode : escalation unaffected"""),
 "11":("The daily nudge loop", """flowchart LR
  SC["score + companion vector"] --> CAND["candidate actions (Doc 12)"]
  CAND --> SAFE["safety / contraindication filter"]
  SAFE --> U["U_a = impact · adherence · ease"]
  U --> TOP["top-5 · diverse · positive Δ"]
  TOP --> DELIV["deliver · channel · send-time · quiet hours · consent"]
  DELIV --> ACT["patient acts"] --> SC"""),
 "10":("Clinical scores feed max", """flowchart LR
  M["markers"] --> CS["FINDRISC · ASCVD/SCORE2<br/>KDIGO · FIB-4 · FRAX"]
  CS --> MX["feed the max risk"]
  MX -.never.-> RX["relax r_i / clear critical"]"""),
 "13":("Cohort & shrinkage", """flowchart TD
  P["patient"] --> C["cohort stratum<br/>age × sex × D × Mx"]
  C --> EB["empirical-Bayes shrinkage"]
  EB --> CAL["calibration · fairness · drift"]"""),
 "19":("Gated actuarial firewall", """flowchart LR
  S["PureScore (wellness)"] -. separate consent .-> ACT["actuarial layer"]
  ACT --> FW["firewall: no protected-class proxy<br/>must not worsen price/access"]"""),
 "16":("Escalation tiers", """flowchart TD
  R["red / critical marker"] --> E{"escalation tier"}
  E -->|emergency| ER["seek care now + on-call clinician"]
  E -->|urgent| UR["expedited reconfirm"]
  E -->|routine| RT["clinician follow-up"]"""),
 "05":("Number → companion vector", """flowchart LR
  N["PureScore number"] --> CV["companion vector"]
  CV --> CF["Confidence"] & TR["Trajectory"] & EW["Early-warning"]
  CV --> MO["Modifiability"] & CRb["Criticality"] & RP["Representativeness"]"""),
 "14":("Release gates", """flowchart LR
  M["model + config change"] --> G["gates:<br/>discrimination · calibration<br/>PPV/lead-time · fairness · drift"]
  G -->|all pass| REL["release"]
  G -->|any fail| BLK["blocked + audit"]"""),
 "15":("Cold-start ignition", """flowchart LR
  GUIDE["guideline band"] --> COH["cohort distribution"] --> PERS["personal baseline"]
  GUIDE --> REG[("evidence registry<br/>provenance + vintage")]
  COH --> REG"""),
 "18":("Ethnicity = context", """flowchart TD
  E["ethnicity"] --> CTX["context + screening selector"]
  CTX -.never.-> PEN["score penalty"]
  R["Ramadan"] --> SAFE["IDF-DAR med timing<br/>+ fast-break safety rule"]"""),
 "12":("Condition → ranked actions", """flowchart LR
  COND["condition / med class"] --> LIB["typed action library"]
  GOAL["goal stream (Doc 07)"] --> RANK["rank by U_a"]
  LIB --> RANK --> TOP["top-5"]"""),
 "17":("Objection → residual", """flowchart LR
  OBJ["objection<br/>(clinician / stats / ethics / reg)"] --> H["how handled"] --> RES["honest residual + register"]"""),
 "07":("Streams → score → surfaces", """flowchart TD
  LAB["LAB · anchor"] --> SC(("PureScore"))
  WEAR["WEAR · tiered"] --> SC
  GOAL["GOAL · steer"] --> SC
  LIFE["LIFE · fill"] --> SC
  SC --> EXP["scribe · chat · scheduling<br/>in-home · insurance-auth"]"""),
}

# ----------------------------------------------------------------- canonical metadata (single source of truth)
# All scoring/catalogue metadata now lives in data/*.json (extracted from the former inline
# Python defs — see data/_extract_catalogs.py). Edit the JSON, NOT this file. Every builder
# and page reads these same objects, so the wiki has one source of truth.
#   pillars.json  marker = [name, unit, tier, two_sided, green, yellow, red, w, source, critical?]
PILLARS     = _load("pillars.json")["pillars"]
MODIFIERS   = _load("modifiers.json")["modifiers"]
WEARABLES   = _load("wearables.json")["wearables"]            # [metric, layer, tier, q_source, pillars, devices]
INSTRUMENTS = _load("instruments.json")["instruments"]
PERSONAS    = _load("personas.json")["personas"]             # [id, name, age, meds, coverage, multipliers, notes]
PILLAR_W    = _load("pillar-weights.json")["weights"]
CONSTANTS   = _load("constants.json")["constants"]
QSOURCE     = _load("qsource.json")["qsource"]

# =================================================================== HTML helpers
def _esc(s):
    return str(s).replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")

def _chip(text, cls):
    return '<span class="chip %s">%s</span>' % (cls, _esc(text))

def _rngbar(two_sided):
    if two_sided:
        return ('<span class="range-mini" title="low red · yellow · green · yellow · high red">'
                '<i class="rg-lo" style="width:14%"></i><i class="rg-y1" style="width:18%"></i>'
                '<i class="rg-g" style="width:36%"></i><i class="rg-y2" style="width:18%"></i>'
                '<i class="rg-hi" style="width:14%"></i></span>')
    return ('<span class="range-mini" title="green · yellow · red">'
            '<i class="rg-g" style="width:54%"></i><i class="rg-y2" style="width:28%"></i>'
            '<i class="rg-hi" style="width:18%"></i></span>')

ILLUS = ('<div class="callout note"><div class="ct">Illustrative</div>All bands, weights and cut-points '
         'on this page are literature/guideline-anchored examples and <b>must be re-verified and '
         'cohort-adjusted before any production use</b> (<a class="xref" href="conventions.html">Conventions</a> &sect;5.6). This is design documentation, '
         'not a validated medical device.</div>')

# =================================================================== typed source channel per marker (closes F7)
_CHANNELS = ["biomarker-lab", "wearable-clinical", "wearable-consumer", "wearable-inferential", "self-report", "derived"]
_CHAN_META = {  # channel -> (short label, chip class, accuracy note)
    "biomarker-lab":        ("lab",          "b-green",  "venous/clinical assay or imaging — highest accuracy"),
    "wearable-clinical":    ("wear-clinical","b-green",  "CGM / validated cuff / ECG / HSAT — clinical-grade (D22)"),
    "wearable-consumer":    ("wear-consumer","b-acc",    "consumer sensor — trajectory & early-warning, Watch-capped (D22)"),
    "wearable-inferential": ("wear-inferred","b-yellow", "device-inferred (VO₂max, sleep-stages) — informational only (D22)"),
    "self-report":          ("PRO",          "b-yellow", "patient-reported / administered instrument"),
    "derived":              ("derived",      "b-mut",    "computed index (eGFR, HOMA-IR, FIB-4, BMI…)"),
}
def _has_kw(s, kws):
    s = s.lower(); return any(k in s for k in kws)
def marker_channel(name, src):
    """Deterministic typed source-channel for a marker (name + guideline source). Total over the catalogue."""
    n = name.lower(); s = (src or "").lower()
    if _has_kw(n, ["phq", "gad", "pss", "ucla", "who-5", "loneliness", "perceived stress", "cognitive screen",
                   "diet-quality", "fiber intake"]) or s in ("survey", "validated"):
        return "self-report"
    if _has_kw(n, ["cgm", "single-lead", "ecg", "systolic bp", "diastolic bp", "hsat"]) or s in ("consensus tir", "consensus", "hsat"):
        return "wearable-clinical"
    if _has_kw(n, ["vo₂max", "vo2max", "deep + rem", "rem proportion", "readiness"]):
        return "wearable-inferential"
    if _has_kw(n, ["homa", "egfr", "fib-4", "almi", "lean mass", "neutrophil", "iron saturation", "diurnal slope", "cortisol rhythm"]) or n.startswith("bmi"):
        return "derived"
    if "wearable" in s or _has_kw(n, ["resting hr", "hrv", "sleep duration", "sleep regularity", "sleep efficiency",
                                      "spo", "steps", "mvpa", "sedentary", "hr recovery", "gait speed", "nighttime hr"]):
        return "wearable-consumer"
    return "biomarker-lab"
def _chan_chip(ch):
    lbl, cls, note = _CHAN_META.get(ch, (ch, "b-mut", ""))
    return '<span class="chip %s" title="%s">%s</span>' % (cls, _esc(note), _esc(lbl))

# =================================================================== APPENDIX BUILDERS
def build_biomarkers():
    h = ['<div class="crumbs"><a href="index.html">Home</a> › Reference › Markers (all channels)</div>',
         '<h1>Appendix A — Markers (all channels)</h1>',
         '<p class="lead">Every marker across the 12 pillars and <b>all input channels</b> (lab · wearable · self-report): tier, two-sidedness, green/yellow/red bands, '
         'within-pillar weight and source. Compiled from <a class="xref" href="02-pillars-and-marker-catalog.html">Doc 02</a>; '
         'the scoring math is <a class="xref" href="03-scoring-formula.html">Doc 03</a>.</p>', ILLUS,
         '<div class="tagrow" style="margin:10px 0 18px">'
         '<span class="tier C">C</span> Core <span class="tier P">P</span> Peripheral '
         '<span class="tier X">X</span> Comprehensive &nbsp; · &nbsp; <span class="chip b-mut">2s</span> two-sided '
         '(low <i>and</i> high adverse)</div>',
         '<div class="callout note"><div class="ct">Marker pipeline</div><a class="xref" href="appendix-wearables.html">ingest &amp; trust-tier (Appendix B)</a> &rarr; <b>bands (you are here)</b> &rarr; <a class="xref" href="wearable-baseline-pipeline.html">personal baseline (Wearable baselines)</a> &middot; scoring math <a class="xref" href="03-scoring-formula.html">Doc 03</a>.</div>']
    h.append('<div id="cfFlags"></div>')
    h.append(
        '<div class="rv-switch"><span class="rv-lab">Range variations</span>'
        '<button class="rv-vb active" data-view="A">Hover card</button>'
        '<button class="rv-vb" data-view="B">Inline matrix</button>'
        '<button class="rv-vb" data-view="C">Range ruler</button>'
        '<button class="rv-vb" data-view="D">Context selector</button>'
        '<span class="rv-sep"></span>'
        '<button class="rv-btn" id="rvExpand">expand all</button>'
        '<button class="rv-btn" id="rvCollapse">collapse all</button>'
        '<a class="rv-btn" href="reference-range-resolver.html">slice/dice resolver &rsaquo;</a>'
        '<span class="rv-hint" id="rvHint">&#9651; marks markers whose reference range shifts by sex / age / life-stage / condition / medication — click to explore (each variation carries its own citation).</span></div>'
        '<div class="rv-pillars" id="rvPillars"></div>'
        '<div class="rv-ctx" id="rvCtx" style="display:none">'
        '<select id="rvSex" class="rv-sel"><option value="">sex…</option><option value="male">Male</option><option value="female">Female</option></select>'
        '<select id="rvAge" class="rv-sel"><option value="">age…</option><option value="young">&lt;40</option><option value="mid">40–64</option><option value="older">65+</option></select>'
        '<select id="rvLife" class="rv-sel"><option value="">life-stage…</option><option value="pregnancy">Pregnancy</option><option value="postmenopause">Post-menopause</option></select>'
        '<select id="rvCond" class="rv-sel"></select><select id="rvMed" class="rv-sel"></select>'
        '<button class="rv-btn" id="rvApply">apply</button><button class="rv-btn" id="rvReset">reset</button>'
        '<span class="rv-hint" id="rvCtxOut"></span></div>')
    for pid, pname, res, rows in PILLARS:
        h.append('<h2 id="%s">%s · %s</h2>' % (pid, pid, _esc(pname)))
        h.append('<p class="small muted">Reservoir links: %s</p>' % _esc(res))
        h.append('<div class="tablewrap"><table><thead><tr>'
                 '<th>Marker</th><th>Unit</th><th>T</th><th>Channel</th><th>Band shape</th>'
                 '<th>Green</th><th>Yellow</th><th>Red</th><th>w</th><th>Source</th></tr></thead><tbody>')
        for (mk, unit, t, ts, g, y, r, w, src, crit) in rows:
            star = ' <span title="critical marker — can make its pillar critical" style="color:var(--red)">★</span>' if crit else ''
            nm = ('<b class="rv-name" data-rv="%s" tabindex="0">%s <span class="rv-badge">&#9651;</span></b>' % (_esc(mk), _esc(mk))) if mk in _RV_NAMES else ('<b>%s</b>' % _esc(mk))
            h.append('<tr id="mk-%s-%s" data-mk="%s"><td>%s%s</td><td class="small muted">%s</td>'
                     '<td><span class="tier %s">%s</span></td><td>%s</td><td>%s</td>'
                     '<td><span class="chip b-green">%s</span></td>'
                     '<td><span class="chip b-yellow">%s</span></td>'
                     '<td><span class="chip b-red">%s</span></td>'
                     '<td class="mono">%s</td><td class="small muted">%s</td></tr>'
                     % (pid.lower(), _slug_metric(mk), _esc(mk), nm, star, _esc(unit), t, t.replace("/","/"), _chan_chip(marker_channel(mk, src)),
                        _rngbar(ts), _esc(g), _esc(y), _esc(r), _esc(w), _cite_chip(src, mk)))
        h.append('</tbody></table></div>')
    h.append('<h2 id="units">Units &amp; SI conversion</h2>')
    h.append('<p class="small muted">Canonical unit is <b>SI</b> (UK/ESC/Gulf convention); the US conventional unit is shown alongside. '
             'Every inbound feed is converted to SI before scoring, so a UK/Gulf lab reporting mmol/mol or mmol/L is scored correctly. '
             'Source of truth: <code>data/units.json</code>. Lp(a) is intentionally non-convertible (store the native assay unit).</p>')
    h.append('<div id="unitTable"></div>')
    h.append('<h2 id="modifiers">Cross-cutting modifiers</h2>')
    h.append('<div class="tablewrap"><table><thead><tr><th>Modifier</th><th>Source</th><th>Effect</th></tr></thead><tbody>')
    for nm, sr, ef in MODIFIERS:
        h.append('<tr><td><b>%s</b></td><td class="small muted">%s</td><td>%s</td></tr>' % (_esc(nm), _esc(sr), _esc(ef)))
    h.append('</tbody></table></div>')
    h.append('<p class="small muted">★ = critical marker — a red value can make its pillar critical and cascade '
             'PureScore into the critical band (<a class="xref" href="03-scoring-formula.html">Doc 03</a> §4). '
             'Sex-specific overrides: <a class="xref" href="08-sex-specific-models.html">Doc 08</a>.</p>')
    h.append('<p class="small muted">Click any <b>Source</b> to verify the citation — the popover shows the full reference, '
             'the table/section where the range lives, DOI/PMID, and opens the document in a new tab. '
             '<span class="cite-key"><span class="cite-dot v"></span>verified link</span> '
             '<span class="cite-key"><span class="cite-dot d"></span>document-level</span></p>')
    h.append(_CITE_ASSETS); h.append(_RANGE_ASSETS); h.append(_FLAG_ASSETS); h.append(_UNIT_ASSETS)
    return "Appendix A · Markers (all channels)", "".join(h)

def build_range_resolver():
    flow = ('flowchart LR\n'
            '  B["base range<br/>(appendix)"] --> SX{"sex?"}\n'
            '  SX --> AG{"age band?"}\n'
            '  AG --> LS{"life-stage?<br/>(sex/age-gated)"}\n'
            '  LS --> CO{"condition?<br/>(ICD-10)"}\n'
            '  CO --> MD{"medication?<br/>(ATC)"}\n'
            '  MD --> EF["EFFECTIVE range<br/>+ citation"]\n'
            '  classDef k fill:#0c1726,stroke:#2b5a86,color:#cfe0f5;\n  class B,EF k')
    h = ['<div class="crumbs"><a href="index.html">Home</a> &rsaquo; Reference &rsaquo; Reference-range resolver</div>',
         '<h1>Reference-range Resolver <span class="small muted">&middot; how a range is sliced &amp; diced by context</span></h1>',
         '<p class="lead">A base reference range is adjusted by the patient’s <b>sex &middot; age &middot; life-stage &middot; condition &middot; medication</b>. '
         'This page shows the <b>resolution order</b> and lets you <b>step through</b> any marker + context to see each slice applied — '
         'with the citation behind every shift — so a developer or clinician can verify the effective range. '
         'Single source of truth: <code>data/range-variations.json</code>. Illustrative (<a class="xref" href="conventions.html">Conventions</a> &sect;5.6).</p>', ILLUS,
         '<div class="diagram"><div class="dt">Resolution order &middot; precedence: medication / condition &gt; life-stage &gt; sex / age</div>'
         '<pre class="mermaid">%s</pre></div>' % flow,
         '<div id="cfFlags"></div>',
         '<h2 id="resolver">Step-through resolver</h2>',
         '<p class="small muted">Pick a marker and a context; incompatible options are auto-disabled (a male cannot be pregnant). Each step shows what changed and its citation.</p>',
         '<div class="rr-tool">'
         '<select id="rrMarker" class="rv-sel"></select>'
         '<select id="rrSex" class="rv-sel"><option value="">sex…</option><option value="male">Male</option><option value="female">Female</option></select>'
         '<select id="rrAge" class="rv-sel"><option value="">age…</option><option value="young">&lt;40</option><option value="mid">40–64</option><option value="older">65+</option></select>'
         '<select id="rrLife" class="rv-sel"><option value="">life-stage…</option><option value="pregnancy">Pregnancy</option><option value="postmenopause">Post-menopause</option></select>'
         '<select id="rrCond" class="rv-sel"></select><select id="rrMed" class="rv-sel"></select>'
         '<button class="rv-btn" id="rrRun">resolve &rarr;</button></div>'
         '<div id="rrOut" class="rr-out"></div>',
         '<h2 id="units">Units &amp; SI conversion layer</h2>',
         '<p class="small muted">Before any range is applied, the inbound value is converted to its <b>canonical SI unit</b> '
         '(UK/ESC/Gulf convention). The resolver therefore behaves identically whether a feed arrives in US conventional '
         '(mg/dL, %, ng/mL) or SI (mmol/L, mmol/mol, nmol/L). Lp(a) is non-convertible — store the native assay unit.</p>',
         '<div id="unitTable"></div>',
         '<div class="callout note"><div class="ct">Referenced everywhere</div>These ranges are the single source rendered on '
         '<a class="xref" href="appendix-biomarkers.html">Appendix A · Markers</a> (4 views), the sex-specific model, and the '
         'Doctor’s-board lab-ranges page. Every variation’s citation opens its source in a new tab.</div>',
         _CITE_ASSETS, _RANGE_ASSETS, '<script src="assets/resolver.js"></script>', _FLAG_ASSETS, _UNIT_ASSETS]
    return "Reference-range resolver", "".join(h)

def build_wearables():
    h = ['<div class="crumbs"><a href="index.html">Home</a> › Reference › Wearable metrics</div>',
         '<h1>Appendix B — Wearable Metrics &amp; Trust Tiers</h1>',
         '<p class="lead">Every wearable signal, its abstraction layer (raw → aggregated → derived), '
         'its <b>trust tier</b> (D22), the confidence weight <code>q_source</code> it earns, the pillars it feeds and '
         'representative devices. See <a class="xref" href="07-data-streams-and-experience.html">Doc 07</a> and '
         '<a class="xref" href="06-data-model-and-reference-ranges.html">Doc 06</a> §2.</p>', ILLUS,
         '<div class="tagrow" style="margin:10px 0 16px">'
         '<span class="chip b-green">clinical-grade ≈ lab</span>'
         '<span class="chip b-acc">consumer-validated · discounted</span>'
         '<span class="chip b-yellow">inferential · informational only</span></div>',
         '<div class="callout safety"><div class="ct">Safety rule (D22)</div>A consumer or inferential wearable '
         'anomaly may raise <b>Watch/Advisory</b> but <b>cannot drive a red/critical without a clinical-grade '
         'confirmation</b> (CGM / validated cuff / single-lead ECG, or a lab). '
         '(<a class="xref" href="05-critical-review-and-purescore-2.0.html">Doc 05</a> §3.4)</div>',
         '<div class="callout note"><div class="ct">Marker pipeline</div>ingest &amp; <b>trust-tier (you are here)</b> &rarr; <a class="xref" href="appendix-biomarkers.html">bands (Appendix A · Markers)</a> &rarr; <a class="xref" href="wearable-baseline-pipeline.html">personal baseline (Wearable baselines)</a> &middot; aggregated via <b>Terra</b> (below).</div>',
         '<div class="tablewrap"><table><thead><tr><th>Metric</th><th>Layer</th><th>Trust tier</th>'
         '<th>q_source</th><th>Pillars</th><th>Devices</th><th>Role</th></tr></thead><tbody>']
    tcls = {"clinical-grade":"b-green","consumer-validated":"b-acc","inferential":"b-yellow"}
    for (m, layer, tier, q, pil, dev) in WEARABLES:
        role = ('<span class="chip b-yellow">informational (D22)</span>' if tier=="inferential"
                else '<span class="chip b-mut">alert/event</span>' if "confirmed" in layer
                else '<span class="chip b-green">banded → A</span>')
        h.append('<tr><td><b>%s</b></td><td class="small muted">%s</td>'
                 '<td><span class="chip %s">%s</span></td><td class="mono small">%s</td>'
                 '<td class="small">%s</td><td class="small muted">%s</td><td>%s</td></tr>'
                 % (_esc(m), _esc(layer), tcls.get(tier,"b-mut"), _esc(tier), _esc(q), _esc(pil), _esc(dev), role))
    h.append('</tbody></table></div>')
    h.append('<div class="callout spec"><div class="ct">Terra — the ingestion API</div>'
             'PureScore ingests wearable data through <b>Terra</b>, the aggregation API that normalizes 30+ devices '
             '(Apple Health, Google Fit, Oura, Whoop, Garmin, Fitbit, Samsung, Dexcom, Freestyle Libre…) into uniform '
             'models. Every signal above arrives via a Terra model; the trust tier and <code>q_source</code> still apply per signal.</div>')
    h.append('<h2 id="terra">Terra metric families → PureScore coverage</h2>')
    h.append('<div class="tablewrap"><table><thead><tr><th>Terra family</th><th>Example metrics</th><th>Feeds</th><th>Coverage</th></tr></thead><tbody>')
    for fam, ex, feeds, cov in [
      ("Body","HR · HRV · SpO₂ · skin temp · glucose · BP · ECG/AFib · body-comp","CV · MET · HEM · ENDO · INF",'<span class="chip b-green">banded (Appendix A); ECG = alert</span>'),
      ("Activity / Daily","steps · distance · calories · active-duration · MET-min · HR-zones","FIT · MET · CV",'<span class="chip b-acc">steps/MVPA banded; volume metrics feed FIT</span>'),
      ("Sleep","total + stage durations · efficiency · latency · HR/HRV · respiration","SLP · MCS · CV",'<span class="chip b-acc">duration/efficiency/regularity banded; stages informational (D22)</span>'),
      ("Menstruation","cycle phase · period · ovulation · temp shift","ENDO (Doc 08)",'<span class="chip b-mut">routed to sex-specific models (Doc 08)</span>'),
      ("Nutrition","calories · macros · hydration","NUT (lifestyle)",'<span class="chip b-mut">routed to NUT via lifestyle / question bank</span>')]:
        h.append('<tr><td><b>%s</b></td><td class="small">%s</td><td class="small muted">%s</td><td>%s</td></tr>' % (fam, ex, feeds, cov))
    h.append('</tbody></table></div>')
    h.append('<p class="small muted">Coverage: <span class="chip b-green">banded</span> = a banded marker in '
             '<a class="xref" href="appendix-biomarkers.html">Appendix A</a> · '
             '<span class="chip b-yellow">informational</span> = D22 inferential, never sets a band · '
             '<span class="chip b-mut">routed</span> = scored elsewhere.</p>')
    return "Appendix B · Wearables", "".join(h)

def build_questions():
    h = ['<div class="crumbs"><a href="index.html">Home</a> › Questions &amp; intake › Screeners & PROs</div>',
         '<h1>Appendix C — Clinical Screeners &amp; PROs</h1>',
         '<p class="lead">The <b>validated, standardized patient-reported-outcome (PRO) instruments and clinical '
         'screeners</b> HikmaEngine administers — full item text, scoring and band cut-offs. These are the '
         'literature-validated instruments (PHQ-9, GAD-7, AUDIT-C, ISI…), distinct from the broad lifestyle/condition '
         '<a class="xref" href="appendix-question-bank.html">Question bank (E)</a> and the '
         '<a class="xref" href="appendix-lifestyles.html">Lifestyles (F)</a> model — the bank\'s '
         '<code>validated_by</code> links escalate a crude item to the matching instrument here. They are the '
         '<span class="flag fl-life">LIFE</span> stream (<a class="xref" href="07-data-streams-and-experience.html">'
         'Doc 07</a> §1) and the MCS/behavioural markers of '
         '<a class="xref" href="02-pillars-and-marker-catalog.html">Doc 02</a>.</p>', ILLUS,
         '<p class="small muted"><b>%d instruments</b> across mental-health, sleep, respiratory/atopy, '
         'metabolic/activity, substance, frailty, cognition and sex-specific domains.</p>' % len(INSTRUMENTS)]
    for q in INSTRUMENTS:
        h.append('<div class="panel"><h3 id="%s">%s</h3>' % (_esc(q["id"]), _esc(q["name"])))
        h.append('<div class="tagrow" style="margin-bottom:8px">'
                 '<span class="chip b-teal">%s</span><span class="chip b-mut">cadence: %s</span>'
                 '<span class="chip b-mut">source: %s</span></div>' % (_esc(q["pillar"]), _esc(q["cadence"]), _esc(q["src"])))
        if q.get("stem"): h.append('<p class="small muted"><i>%s</i></p>' % _esc(q["stem"]))
        if q.get("scale"): h.append('<p class="small"><b>Response scale:</b> %s</p>' % _esc(q["scale"]))
        h.append('<ol class="small">')
        for it in q["items"]: h.append('<li>%s</li>' % _esc(it))
        h.append('</ol>')
        h.append('<table style="margin-top:6px"><tbody>'
                 '<tr><td style="width:120px" class="small muted">Scoring</td><td class="small">%s</td></tr>'
                 '<tr><td class="small muted">Bands</td><td class="small">%s</td></tr>' % (_esc(q["scoring"]), _esc(q["bands"])))
        if q.get("safety") and q["safety"] != "—":
            h.append('<tr><td class="small muted">Safety</td><td class="small" style="color:#ff9bae">%s</td></tr>' % _esc(q["safety"]))
        h.append('</tbody></table></div>')
    return "Appendix C · Screeners & PROs", "".join(h)

def build_personas():
    h = ['<div class="crumbs"><a href="index.html">Home</a> › Reference › Personas</div>',
         '<h1>Appendix D — Personas &amp; Edge Cases</h1>',
         '<p class="lead">The cohort/edge-case personas that exercise the engine — each with age, medications, '
         'representativeness <code>rep</code> (drives Confidence), pillar weight multipliers, and the interpretation '
         'frame they test. Used by <a class="xref" href="08-sex-specific-models.html">Doc 08</a>, '
         '<a class="xref" href="18-uae-localization.html">Doc 18</a> and the live calculator.</p>', ILLUS,
         '<div class="callout note"><div class="ct">Clinical pathway personas</div>'
         'The condition-anchored personas (Mariam · CKD, Abdullah · dyslipidaemia, Meera · hypertension, '
         'Fatima · mental health, Hala · T2 diabetes, Omar · obesity) live with their care pathway on '
         '<a class="xref" href="care-pathways.html">Condition care pathways</a> (canonical in '
         '<code>data/care-pathways.json</code>) — they drive the Identify→Engage→Assess→Transform delivery model.</div>',
         '<div class="tablewrap"><table><thead><tr><th>Persona</th><th>Age</th><th>Meds</th>'
         '<th>rep</th><th>Weight multipliers</th><th>Frame it tests</th></tr></thead><tbody>']
    for (k, label, age, meds, rep, wm, note) in PERSONAS:
        h.append('<tr><td><b>%s</b><br><span class="small muted mono">%s</span></td><td>%s</td>'
                 '<td class="small">%s</td><td class="mono">%s</td><td class="small">%s</td>'
                 '<td class="small muted">%s</td></tr>'
                 % (_esc(label), _esc(k), age, _esc(meds), _esc(rep), _esc(wm), _esc(note)))
    h.append('</tbody></table></div>')
    return "Appendix D · Personas", "".join(h)

# =================================================================== APPENDIX E — question bank
_BURDEN = {"atherogenic_burden", "vascular_bp_load", "glycemic_burden", "adiposity",
           "hepatic_fat", "inflammatory_load", "allostatic_load", "sleep_debt"}

def _dir_chip(d):
    return {"increase_risk": '<span class="chip b-red">↑ risk</span>',
            "decrease_risk": '<span class="chip b-green">↓ risk</span>',
            "neutral": '<span class="chip b-mut">·</span>'}.get(d, '<span class="chip b-mut">·</span>')

def _pdeltas(d):
    if not d: return '<span class="small muted">—</span>'
    out = []
    for k, v in d.items():
        cls = "b-red" if (v or 0) > 0 else ("b-green" if (v or 0) < 0 else "b-mut")
        out.append('<span class="chip %s mono">%s %+.2f</span>' % (cls, _esc(k), v))
    return " ".join(out)

def _rdeltas(d):
    if not d: return '<span class="small muted">—</span>'
    out = []
    for k, v in d.items():
        v = v or 0
        # burden ↑ = bad (red); reserve ↑ = good (green)
        if k in _BURDEN:
            cls = "b-red" if v > 0 else ("b-green" if v < 0 else "b-mut")
        else:
            cls = "b-green" if v > 0 else ("b-red" if v < 0 else "b-mut")
        out.append('<span class="chip %s mono">%s %+.2f</span>' % (cls, _esc(k), v))
    return " ".join(out)

def _deps_line(dep):
    if not dep: return ""
    bits = []
    for cond in dep.get("show_if", []):
        bits.append("show if %s ∈ %s" % (cond.get("q", "?"), cond.get("in", cond.get("op", "?"))))
    for cond in dep.get("skip_if", []):
        bits.append("skip if %s ∈ %s" % (cond.get("q", "?"), cond.get("in", cond.get("op", "?"))))
    for t in dep.get("triggers", []):
        bits.append("if “%s” → ask %s" % (t.get("if_response", "?"), t.get("ask", "?")))
    if dep.get("validated_by"):
        bits.append('validated by <b>%s</b>' % _esc(dep["validated_by"]))
    if not bits: return ""
    return '<p class="small muted"><b>Dependencies:</b> %s</p>' % " · ".join(_esc(b) if "<b>" not in b else b for b in bits)

def _reflink(r):
    return '<a class="mono" href="#%s">%s</a>' % (_esc(r), _esc(r)) if r else ""

# Question-bank entries carry no explicit `section`/`phase`, but they DO carry semantic
# `dimensions.axis_tags`, `category`, `cadence` and `prerequisites`. Derive the content-section
# and intake-phase from those so the bank and the intake-overview agree (no fabricated stored field).
def _q_section(q):
    t = set((q.get("dimensions") or {}).get("axis_tags", []))
    c = (q.get("category") or "").lower()
    if t & {"pain", "cognition", "aesthetic"} or "symptom" in c or "self-percept" in c:
        return "symptoms"
    if t & {"diet", "activity", "sleep", "substance", "stress"} or any(
            k in c for k in ("diet", "nutrition", "activity", "exercise", "sleep", "substance", "alcohol", "smok", "stress")):
        return "lifestyle"
    if t & {"condition_load", "genetic_familial", "reproductive"} or any(
            k in c for k in ("history", "condition", "medication", "diagnos", "supplement")):
        return "medical_history"
    if t & {"anthropometric", "occupation", "environment", "social", "lifestage"} or any(
            k in c for k in ("demograph", "anthropom", "socioecon", "occupation", "environment")):
        return "demographics"
    return "medical_history"

def _q_phase(q):
    cad = (q.get("cadence") or "").lower()
    if cad in ("core", "once"): return "onboarding"
    if q.get("prerequisites"): return "progressive"
    return "ongoing"

def build_question_bank():
    qb = _load("question-bank.json")
    m = qb["meta"]; qs = qb["questions"]
    SECTION_ORDER = ["demographics", "medical_history", "lifestyle", "symptoms", "adherence"]
    SECTION_LABELS = {"demographics": "Demographics", "medical_history": "Medical history & conditions",
                      "lifestyle": "Lifestyle & behaviour", "symptoms": "Symptoms & PROs", "adherence": "Adherence"}
    PHASE_LABELS = {"onboarding": "① Onboarding · one-time intake",
                    "progressive": "② Progressive profiling · unlocked by earlier answers",
                    "ongoing": "③ Ongoing tracking"}
    PHASE_RANK = {"onboarding": 0, "progressive": 1, "ongoing": 2}
    by_sec = {}
    for q in qs: by_sec.setdefault(_q_section(q), []).append(q)
    dom_codes = m["domain_codes"]                      # retained for the per-question domain badge
    n_sec = len([s for s in SECTION_ORDER if by_sec.get(s)])
    total = len(qs)
    h = ['<div class="crumbs"><a href="index.html">Home</a> › Questions &amp; intake › Question bank</div>',
         '<h1>Appendix E — Lifestyle &amp; Condition Question Bank</h1>',
         '<p class="lead">The full engine-ready intake bank: <b>%d questions</b> grouped by <b>%d content sections × 3 phases</b> '
         '(onboarding → progressive → ongoing), covering <b>%d conditions</b>. Each question has a stable reference code <code>%s</code>, a place in '
         'the progressive <b>chain</b> (prerequisites → this → unlocks, with prev/next links), an '
         '<b>applicability vector</b> (sex · age · life-stage · persona) and signed weights onto the 12 '
         '<a class="xref" href="02-pillars-and-marker-catalog.html">pillars</a> and MONIAC '
         '<a class="xref" href="04-moniac-reservoir-dynamics.html">reservoirs</a>. The <span class="flag fl-life">'
         'LIFE</span> stream (<a class="xref" href="07-data-streams-and-experience.html">Doc 07</a>); '
         'validated PROs are in <a class="xref" href="appendix-questions.html">Appendix C</a>.</p>'
         % (m["total_questions"], n_sec, m["total_conditions"], m.get("ref_scheme", "DOM-NNN")), ILLUS]
    h.append('<div class="diagram"><div class="dt">Answer → score → nudge → adherence, and the question chain</div>'
             '<pre class="mermaid">flowchart LR\n'
             '  PRQ["prerequisite Qs"] --> Q["this question (ref)"] --> UNL["unlocked Qs"]\n'
             '  Q --> M["response: direction · magnitude · signed weight"]\n'
             '  M --> P["pillar Δrisk (Doc 03)"]\n  M --> R["reservoir inflow (Doc 04)"]\n'
             '  P --> PS(("PureScore"))\n  R --> PS\n'
             '  PS --> N["top-5 nudges (Doc 11/12)"] --> AD["adherence"] --> R\n'
             '  PS --> PA["perceived vs actual (Appendix F)"]</pre></div>')
    h.append('<div class="callout note"><div class="ct">Reference & chain</div>'
             '<code>ref</code> = 3-letter section code + sequence (e.g. <code>CMB-001</code>); '
             '<code>order</code> = global intake position; <code>prerequisites</code>/<code>unlocks</code> = the '
             'dependency DAG (sex/age/gender gating + show_if + triggers); <code>applicability</code> = '
             '{sex · age · life-stage · personas}. Machine-readable source: '
             '<code>data/question-bank.json</code> (rebuild via <code>data/enrich_bank.py</code>).</div>')
    h.append('<div class="tagrow" style="margin:10px 0 6px">'
             '<span class="chip b-red">↑ risk / adds burden</span>'
             '<span class="chip b-green">↓ risk / builds reserve</span>'
             '<span class="chip b-mut">neutral</span>'
             '<span class="chip b-teal">P1 = ask first</span>'
             '<span class="chip b-acc">♀/♂ = sex-specific</span>'
             '<span class="chip b-mut">⛓ prerequisite chain</span></div>')
    # interactive filter bar — text · pillar · axis · priority · gender · age · persona
    pills = m["pillars"]; axes = m["axes"]; personas = m.get("personas", [])
    opt = lambda v: '<option value="%s">%s</option>' % (_esc(v), _esc(v))
    h.append('<div class="panel" style="position:sticky;top:54px;z-index:5">'
             '<div class="tagrow" style="gap:8px;align-items:center">'
             '<input id="qbq" type="search" placeholder="filter questions…" '
             'style="flex:1;min-width:150px;padding:6px 10px;border-radius:8px;border:1px solid var(--line,#333);'
             'background:transparent;color:inherit">'
             '<select id="qbg"><option value="">any gender</option>'
             '<option value="male">male</option><option value="female">female</option></select>'
             '<select id="qbage"><option value="">any age</option>'
             '<option value="25">&lt;40</option><option value="50">40–59</option><option value="70">60+</option></select>'
             '<select id="qbper"><option value="">any persona</option>%s</select>'
             '<select id="qbp"><option value="">all pillars</option>%s</select>'
             '<select id="qba"><option value="">all axes</option>%s</select>'
             '<select id="qbr"><option value="">all priorities</option>'
             '<option value="1">P1</option><option value="2">P2</option><option value="3">P3</option>'
             '<option value="4">P4</option><option value="5">P5</option></select>'
             '<span id="qbn" class="small muted"></span></div></div>'
             % ("".join(opt(p) for p in personas), "".join(opt(p) for p in pills), "".join(opt(a) for a in axes)))
    # per content-section panels, sub-grouped by intake phase, ordered along the chain
    for sec in SECTION_ORDER:
        items = sorted(by_sec.get(sec, []), key=lambda x: (PHASE_RANK.get(_q_phase(x), 9), x.get("order", 999)))
        if not items: continue
        h.append('<h2 id="sec-%s">%s <span class="small muted mono">· %d questions</span></h2>'
                 % (_esc(sec), _esc(SECTION_LABELS.get(sec, sec)), len(items)))
        cur_ph = None
        for q in items:
            ph = _q_phase(q)
            if ph != cur_ph:
                cur_ph = ph
                _pn = sum(1 for x in items if _q_phase(x) == ph)
                h.append('<div class="section-h" style="margin:16px 0 6px;font-size:13px">%s <span class="small muted">· %d</span></div>'
                         % (_esc(PHASE_LABELS.get(ph, ph)), _pn))
            qp = sorted({k for r in q.get("responses", []) for k in (r.get("pillars") or {})})
            ax = q.get("dimensions", {}).get("axis_tags", [])
            pa = q.get("perceived_actual", {})
            ap = q.get("applicability", {})
            sex = ap.get("sex", ["all"]); ls = ap.get("life_stage", [])
            per = ap.get("personas", ["all"])
            data = ('data-text="%s" data-pillars="%s" data-axes="%s" data-prio="%s" '
                    'data-sex="%s" data-agemin="%s" data-agemax="%s" data-personas="%s"'
                    % (_esc((q["ref"] + " " + q["text"] + " " + " ".join(q.get("conditions", []))).lower()),
                       _esc(" ".join(qp)), _esc(" ".join(ax)), q.get("priority", ""),
                       _esc(" ".join(sex)), ap.get("age_min", 0), ap.get("age_max", 120),
                       _esc(" ".join(per))))
            h.append('<div class="panel qbq" %s>' % data)
            h.append('<h3 id="%s"><span class="mono" style="color:var(--acc,#6cf)">%s</span> · %s'
                     '<span id="%s"></span></h3>' % (_esc(q["ref"]), _esc(q["ref"]), _esc(q["text"]), _esc(q["id"])))
            # chip row
            chips = ['<span class="chip b-teal">P%s</span>' % q.get("priority", "?"),
                     '<span class="chip b-gold">phase: %s</span>' % _esc(_q_phase(q)),
                     '<span class="chip b-mut mono">%s</span>' % _esc(q.get("domain", "")),
                     '<span class="chip b-mut">%s</span>' % _esc(q.get("category", "")),
                     '<span class="chip b-mut">%s</span>' % _esc(q.get("type", "")),
                     '<span class="chip b-mut">%s</span>' % _esc(ap.get("stream", "LIFE")),
                     '<span class="chip b-mut">cadence: %s · %s</span>' % (_esc(q.get("cadence", "")), _esc(q.get("refresh", ""))),
                     '<span class="chip b-mut">%s</span>' % _esc(pa.get("kind", ""))]
            if sex != ["all"]:
                sym = {"female": "♀ female-only", "male": "♂ male-only"}.get(sex[0], " / ".join(sex))
                chips.append('<span class="chip b-acc">%s</span>' % _esc(sym))
            if (ap.get("age_min", 0), ap.get("age_max", 120)) != (0, 120):
                chips.append('<span class="chip b-acc">age %s–%s</span>' % (ap.get("age_min"), ap.get("age_max")))
            chips += ['<span class="chip b-acc">⌖ %s</span>' % _esc(x) for x in ls]
            chips += ['<span class="chip b-acc mono">%s</span>' % _esc(p) for p in qp]
            chips += ['<span class="chip b-mut">#%s</span>' % _esc(a) for a in ax]
            h.append('<div class="tagrow" style="margin-bottom:8px">%s</div>' % "".join(chips))
            # chain line: prev / order / next  + prerequisites / unlocks
            chain = ['<span class="small muted">chain:</span> ']
            chain.append(('← %s' % _reflink(q["prev_ref"])) if q.get("prev_ref") else '<span class="small muted">← start</span>')
            chain.append(' <span class="small muted mono">[#%d/%d]</span> ' % (q.get("order", 0), total))
            chain.append(('%s →' % _reflink(q["next_ref"])) if q.get("next_ref") else '<span class="small muted">end →</span>')
            if q.get("prerequisites"):
                chain.append(' &nbsp;·&nbsp; <span class="small muted">⛓ prerequisites:</span> ' + ", ".join(_reflink(r) for r in q["prerequisites"]))
            if q.get("unlocks"):
                chain.append(' &nbsp;·&nbsp; <span class="small muted">unlocks:</span> ' + ", ".join(_reflink(r) for r in q["unlocks"]))
            h.append('<p class="small">%s</p>' % "".join(chain))
            if q.get("conditions"):
                h.append('<p class="small muted">Informs: %s</p>' % _esc(", ".join(q["conditions"])))
            h.append('<div class="tablewrap"><table><thead><tr><th>Response</th><th>Δrisk</th>'
                     '<th>Mag</th><th>Pillar Δ</th><th>Reservoir Δ</th></tr></thead><tbody>')
            for r in q.get("responses", []):
                h.append('<tr><td class="small"><b>%s</b></td><td>%s</td><td class="small muted">%s</td>'
                         '<td>%s</td><td>%s</td></tr>'
                         % (_esc(r.get("label", "")), _dir_chip(r.get("direction")), _esc(r.get("magnitude", "")),
                            _pdeltas(r.get("pillars")), _rdeltas(r.get("reservoirs"))))
            h.append('</tbody></table></div>')
            h.append(_deps_line(q.get("dependencies")))
            aline = 'applies to — sex: %s · age: %s · life-stage: %s · personas: %s' % (
                _esc(", ".join(sex)),
                _esc("all" if (ap.get("age_min", 0), ap.get("age_max", 120)) == (0, 120) else "%s–%s" % (ap.get("age_min"), ap.get("age_max"))),
                _esc(", ".join(ls) if ls else "any"),
                _esc(", ".join(per)))
            if pa.get("corroborated_by"):
                aline += ' · corroborated by: %s' % _esc(", ".join(pa["corroborated_by"]))
            h.append('<p class="small muted"><b>Applicability:</b> %s</p>' % aline)
            h.append('<p class="small muted" style="opacity:.7">%s · <i>%s</i> · internal id <span class="mono">%s</span></p>'
                     % (_esc(q.get("source", "")), _esc(q.get("flag", "")), _esc(q["id"])))
            h.append('</div>')
    # filter script (page-local; independent of the topbar search in wiki.js)
    h.append('<script>(function(){'
             'var ids=["qbq","qbg","qbage","qbper","qbp","qba","qbr"].map(function(i){return document.getElementById(i);});'
             'var s=ids[0],g=ids[1],ag=ids[2],pe=ids[3],p=ids[4],a=ids[5],r=ids[6],n=document.getElementById("qbn");'
             'var cards=[].slice.call(document.querySelectorAll(".panel.qbq"));'
             'function has(d,v){return (" "+d+" ").indexOf(" "+v+" ")>-1;}'
             'function f(){var t=(s.value||"").toLowerCase(),gv=g.value,av=ag.value,pev=pe.value,pv=p.value,xv=a.value,rv=r.value,c=0;'
             'cards.forEach(function(el){var d=el.dataset;var ok=(!t||d.text.indexOf(t)>-1)'
             '&&(!gv||has(d.sex,"all")||has(d.sex,gv))'
             '&&(!av||(+d.agemin<=+av&&+d.agemax>=+av))'
             '&&(!pev||has(d.personas,"all")||has(d.personas,pev))'
             '&&(!pv||has(d.pillars,pv))&&(!xv||has(d.axes,xv))&&(!rv||d.prio===rv);'
             'el.style.display=ok?"":"none";if(ok)c++;});'
             'n.textContent=c+" / "+cards.length+" shown";}'
             'ids.forEach(function(e){e.addEventListener("input",f);e.addEventListener("change",f);});f();})();</script>')
    return "Appendix E · Question Bank", "".join(h)

# =================================================================== QUESTIONS HUB
_Q_SEC = ["demographics", "medical_history", "lifestyle", "symptoms", "adherence"]
_Q_SECL = {"demographics": "Demographics", "medical_history": "Medical history & conditions",
           "lifestyle": "Lifestyle & behaviour", "symptoms": "Symptoms & PROs", "adherence": "Adherence"}
_Q_PH = ["onboarding", "progressive", "ongoing"]
_Q_PHL = {"onboarding": "Onboarding", "progressive": "Progressive", "ongoing": "Ongoing"}

def build_questions_hub():
    qs = _load("question-bank.json")["questions"]
    adh = _load("adherence.json")["items"]
    cnt = {s: {p: 0 for p in _Q_PH} for s in _Q_SEC}
    for q in qs:
        s = _q_section(q); p = _q_phase(q)
        if s in cnt: cnt[s][p] += 1
    for a in adh:  # adherence check-ins fire on cadence → ongoing (the no-EHR screener is onboarding)
        cnt["adherence"]["ongoing"] += 1
    tot = len(qs) + len(adh)
    h = ['<div class="crumbs"><a href="index.html">Home</a> › Questions &amp; intake › Overview</div>',
         '<h1>Questions &amp; Intake — overview</h1>',
         '<p class="lead">Every question PureScore can ask, unified into <b>5 content sections × 3 phases</b> and '
         'gated so a patient is <b>only ever asked what is relevant</b>. The raw items live in the bank '
         '(<a class="xref" href="appendix-question-bank.html">Appendix E</a>, %d) and the adherence check-ins '
         '(<a class="xref" href="appendix-adherence.html">Appendix H</a>, %d); validated PROs in '
         '<a class="xref" href="appendix-questions.html">Appendix C</a>. The <a class="xref" href="eligibility-gating.html">'
         'Eligibility &amp; gating</a> page shows the rules that connect onboarding answers to which questions unlock.</p>'
         % (len(qs), len(adh)), ILLUS]
    h.append('<div class="diagram"><div class="dt">Content section × intake phase</div>'
             '<pre class="mermaid">flowchart LR\n'
             '  ON["① Onboarding\\none-time intake"] --> PR["② Progressive\\nunlocked by answers"] --> OG["③ Ongoing\\ntracking + adherence"]\n'
             '  ON -.gates.-> PR</pre></div>')
    # matrix
    h.append('<div class="tablewrap"><table><thead><tr><th>Content section</th>'
             + "".join('<th>%s</th>' % _Q_PHL[p] for p in _Q_PH) + '<th>Total</th></tr></thead><tbody>')
    for s in _Q_SEC:
        row = cnt[s]; rt = sum(row.values())
        h.append('<tr><td><b>%s</b></td>%s<td class="mono"><b>%d</b></td></tr>'
                 % (_Q_SECL[s], "".join('<td class="mono">%d</td>' % row[p] for p in _Q_PH), rt))
    coltot = {p: sum(cnt[s][p] for s in _Q_SEC) for p in _Q_PH}
    h.append('<tr><td class="muted">Total</td>%s<td class="mono"><b>%d</b></td></tr>'
             % ("".join('<td class="mono muted">%d</td>' % coltot[p] for p in _Q_PH), tot))
    h.append('</tbody></table></div>')
    # entry points
    h.append('<div class="section-h">The question library</div><div class="grid c3">')
    for href, t, dsc in [
        ("appendix-question-bank.html", "Question bank (Appendix E)", "The %d-item engine-ready bank, grouped by section × phase" % len(qs)),
        ("appendix-questions.html", "Validated PROs (Appendix C)", "PHQ-9, GAD-7, AUDIT-C, ISI, PSS-4… full instruments"),
        ("appendix-adherence.html", "Adherence check-ins (Appendix H)", "%d EHR-triggered micro check-ins (Patient360)" % len(adh)),
        ("appendix-lifestyles.html", "Lifestyles & axes (Appendix F)", "Axes, archetypes, perceived-vs-actual"),
        ("appendix-persona-matrix.html", "Persona matrix (Appendix I)", "Signals → persona posterior"),
        ("appendix-goals.html", "Goals (Appendix J)", "Goal catalogue keyed by applicability"),
        ("eligibility-gating.html", "Eligibility & gating", "Rules that decide who is asked each question"),
    ]:
        h.append('<a class="card" href="%s"><h3>%s</h3><p>%s</p></a>' % (href, _esc(t), _esc(dsc)))
    h.append('</div>')
    h.append('<div class="callout note"><div class="ct">Connected to onboarding</div>'
             'Onboarding (phase ①) captures identity, baseline medical history and the screeners (sex, BMI, smoking, '
             'alcohol). Those answers <b>gate</b> phases ② and ③ — see <a class="xref" href="eligibility-gating.html">Eligibility '
             '&amp; gating</a> and the data-streams experience (<a class="xref" href="07-data-streams-and-experience.html">Doc 07</a>).</div>')
    return "Questions & intake · overview", "".join(h)

# =================================================================== ELIGIBILITY & GATING
def build_eligibility():
    qs = _load("question-bank.json")["questions"]
    byid = {q["id"]: q for q in qs}
    def ap(q): return q.get("applicability", {}) or {}
    sexr = [q for q in qs if ap(q).get("sex") not in (None, ["all"], [])]
    lsr = [q for q in qs if ap(q).get("life_stage")]
    ager = [q for q in qs if (ap(q).get("age_min", 0), ap(q).get("age_max", 120)) != (0, 120)]
    showif = [q for q in qs if (q.get("dependencies", {}) or {}).get("show_if")]
    prereq = [q for q in qs if q.get("prerequisites")]
    h = ['<div class="crumbs"><a href="index.html">Home</a> › Questions &amp; intake › Eligibility &amp; gating</div>',
         '<h1>Eligibility &amp; Gating Rules <span class="small muted">· don\'t ask irrelevant questions</span></h1>',
         '<p class="lead">A patient is only ever asked what is <b>valid and relevant</b> for them. Onboarding answers '
         '(sex, BMI, smoking, alcohol, conditions) gate the rest: a male is never asked menopause questions; smoking '
         'follow-ups appear only if smoking was reported. Rules are real data on each question '
         '(<code>applicability</code> · <code>dependencies.show_if</code> · <code>prerequisites</code> · <code>unlocks</code>).</p>', ILLUS]
    h.append('<div class="diagram"><div class="dt">Onboarding answers gate the question set</div>'
             '<pre class="mermaid">flowchart TD\n'
             '  SEX["Q_CORE_SEX"] -->|male| HF["hide female-only Qs:\\nmenopause · PCOS · pregnancy"]\n'
             '  SEX -->|female| SF["unlock female reproductive Qs"]\n'
             '  BMI["Q_CORE_BMI"] -->|overweight / obese| WA["ask waist + metabolic follow-ups"]\n'
             '  SMK["Q_CORE_SMOKING"] -->|current / former| SQ["ask smoking-detail Qs"]\n'
             '  SMK -->|never| SK["skip smoking follow-ups"]\n'
             '  ALC["Q_CORE_ALCOHOL"] -->|drinks| AQ["ask alcohol-pattern Qs"]\n'
             '  classDef on fill:#0c1726,stroke:#2b5a86,color:#cfe3ff;\n'
             '  class SEX,BMI,SMK,ALC on</pre></div>')
    h.append('<div class="tagrow" style="margin:6px 0 12px">'
             '<span class="chip b-acc">%d sex-gated</span><span class="chip b-acc">%d life-stage-gated</span>'
             '<span class="chip b-acc">%d age-gated</span><span class="chip b-teal">%d show-if</span>'
             '<span class="chip b-mut">%d with prerequisites</span></div>'
             % (len(sexr), len(lsr), len(ager), len(showif), len(prereq)))

    def _tbl(title, anchor, rows, head):
        h.append('<h2 id="%s">%s <span class="small muted">· %d</span></h2>' % (anchor, title, len(rows)))
        h.append('<div class="tablewrap"><table><thead><tr>%s</tr></thead><tbody>'
                 % "".join("<th>%s</th>" % c for c in head))
        h.extend(rows)
        h.append('</tbody></table></div>')

    sym = {"female": "♀ female-only", "male": "♂ male-only"}
    rows = ['<tr><td class="mono small">%s</td><td><span class="chip b-acc">%s</span></td><td class="small">%s</td></tr>'
            % (_esc(q["ref"]), _esc(sym.get((ap(q)["sex"] or ["?"])[0], " / ".join(ap(q)["sex"]))), _esc(q["text"][:80]))
            for q in sexr]
    _tbl("Sex-restricted questions", "sex", rows, ["Ref", "Valid for", "Question"])

    rows = ['<tr><td class="mono small">%s</td><td>%s</td><td class="small">%s</td></tr>'
            % (_esc(q["ref"]), "".join('<span class="chip b-acc">⌖ %s</span>' % _esc(x) for x in ap(q)["life_stage"]), _esc(q["text"][:80]))
            for q in lsr]
    _tbl("Life-stage-gated questions", "lifestage", rows, ["Ref", "Life-stage", "Question"])

    rows = ['<tr><td class="mono small">%s</td><td class="small">%s</td><td class="small">%s</td></tr>'
            % (_esc(q["ref"]), _esc("%s–%s" % (ap(q).get("age_min", 0), ap(q).get("age_max", 120))), _esc(q["text"][:80]))
            for q in ager]
    _tbl("Age-restricted questions", "age", rows, ["Ref", "Age range", "Question"])

    rows = []
    for q in showif:
        for cond in q["dependencies"]["show_if"]:
            gate = byid.get(cond.get("q"))
            gref = gate["ref"] if gate else cond.get("q", "?")
            rows.append('<tr><td class="mono small">%s</td><td class="small">shown only if <b>%s</b> ∈ %s</td><td class="small">%s</td></tr>'
                        % (_esc(q["ref"]), _esc(gref), _esc(", ".join(cond.get("in", []))), _esc(q["text"][:70])))
    _tbl("Conditional (show-if) questions", "showif", rows, ["Ref", "Shown when", "Question"])

    h.append('<div class="callout spec"><div class="ct">How rules connect to onboarding</div>'
             'The gate questions above are all <b>phase-① onboarding</b> items. Their answers set the patient\'s '
             'applicability vector, which the engine evaluates before serving any later question — so phases ② and ③ '
             'only ever surface valid, relevant items. New gates are added by editing <code>data/question-bank.json</code> '
             '(re-run <code>migrate_questions_sections.py</code> to gap-fill systematically).</div>')
    return "Eligibility & gating", "".join(h)

# =================================================================== PURESCORE CALCULATION — DATA FLOW
def build_purescore_dataflow():
    L0 = """flowchart LR
  PT["Patient / consumer"] -->|survey · self-report| SYS
  WE["Wearables (D22 tiered)"] -->|streams| SYS
  LB["Labs (venous · DTC)"] -->|biomarkers| SYS
  EHR["Patient360 (EHR/EMR)"] -->|conditions · meds| SYS
  SYS(["PureScore engine"]) --> SCORE["PureScore 0–100 + bands"]
  SYS --> COMP["Companion vector (Doc 05)"]
  SYS --> NUD["Top-5 nudges (Doc 11)"]
  SYS --> ESC["Crisis escalation (Doc 16)"]
  CLIN["Clinician"] -->|review · sign-off| SYS
  classDef ext fill:#0c1726,stroke:#2b5a86,color:#cfe3ff;
  class PT,WE,LB,EHR,CLIN ext"""
    L1 = """flowchart TB
  IN["1 · Ingest + confidence"] --> MK["2 · Marker risk r_i"]
  MK --> PL["3 · Pillar risk R_k"]
  PL --> SC["4 · PureScore + critical cascade"]
  SC --> OUT["5 · Outputs (score · bands · companion)"]
  REFD[("Reference distributions — NHANES · guidelines")] -.-> MK
  BASE[("Personal baseline — empirical Bayes")] -.-> MK
  RES[("MONIAC reservoirs B_j — Doc 04")] -.-> PL
  WTS[("Pillar weights W_k · δ — admin")] -.-> PL
  WTS -.-> SC
  CRIT[("Critical-marker set — Doc 02")] -.-> SC
  classDef store fill:#10151e,stroke:#8f9bff,color:#cdd6ff;
  class REFD,BASE,RES,WTS,CRIT store"""
    L2 = """flowchart TB
  A["measurement x_i"] --> G1{"confidence ≥ floor?"}
  G1 -->|missing / stale| FB["impute cohort median — low conf → low coverage"]
  G1 -->|ok| G2{"wearable tier?"}
  G2 -->|inferential| INFO["informational only — cannot set a band"]
  G2 -->|clinical / consumer / lab| CL["clinical band risk r_clin (green / yellow / red bands)"]
  FB --> CL
  CL --> MX["r = max(r_clin, 0.6·r_cohort) — cohort can only RAISE"]
  MX --> G3{"critical marker?"}
  G3 -->|no| PERS["+ personalization, then band_clamp (can't relax red / flip band)"]
  G3 -->|yes| RI["r (personalization disabled)"]
  PERS --> RI
  RI --> AGG["pillar R_k = confidence-weighted δ-power-mean"]
  AGG --> RESV["R_k = clamp01(R_k + ρ·reservoir load) — Doc 04"]
  RESV --> G4{"critical-marker red in pillar?"}
  G4 -->|yes| FLOOR["status CRITICAL · R_k ← max(R_k, 0.60) — never averaged away"]
  G4 -->|no| STAT["status from R_k bands"]
  FLOOR --> G5{"acute-danger red?"}
  G5 -->|yes| ESCAL["crisis / clinician escalation — Doc 16"]
  G5 -->|no| TOT["R_total = δ-power-mean over W_k pillars"]
  STAT --> TOT
  ESCAL --> TOT
  TOT --> G6{"acute event active?"}
  G6 -->|yes| ACUTE["apply acute weights m_k_acute · override→revert — Doc 09"]
  G6 -->|no| FINAL["PureScore = 100·(1 − R_total)"]
  ACUTE --> FINAL
  FINAL --> G7{"pillar green only via imputed medians?"}
  G7 -->|yes| LCG["label 'low-coverage green' — suppress over-confident green"]
  G7 -->|no| DONE["PureScore + bands + companion vector (Doc 05)"]
  LCG --> DONE
  classDef gate fill:#1d1a0c,stroke:#edb14a,color:#ffe6b0;
  classDef danger fill:#1a0f15,stroke:#f0606e,color:#ffd0d6;
  class G1,G2,G3,G4,G5,G6,G7 gate
  class ESCAL,FLOOR danger"""
    h = ['<div class="crumbs"><a href="index.html">Home</a> › Diagrams &amp; system maps › PureScore calculation — data flow</div>',
         '<h1>PureScore Calculation — Data Flow Diagram <span class="small muted">· all decision gates &amp; rules</span></h1>',
         '<p class="lead">The end-to-end data flow of a PureScore computation — ingestion → marker risk → pillar risk → '
         'score → outputs — with <b>every decision gate and rule</b> made explicit. Leveled: <b>L0</b> context, '
         '<b>L1</b> pipeline + data stores, <b>L2</b> the full gate flow; the table below enumerates each rule with its '
         'source §. Derived from <a class="xref" href="06-data-model-and-reference-ranges.html">Doc 06</a>, '
         '<a class="xref" href="02-pillars-and-marker-catalog.html">02</a>, '
         '<a class="xref" href="03-scoring-formula.html">03</a>, '
         '<a class="xref" href="04-moniac-reservoir-dynamics.html">04</a>, '
         '<a class="xref" href="09-acute-events-and-life-stage-plans.html">06</a>, '
         '<a class="xref" href="16-safety-governance-and-regulatory.html">11</a>, '
         '<a class="xref" href="05-critical-review-and-purescore-2.0.html">12</a>.</p>', ILLUS]
    h.append('<div class="callout spec"><div class="ct">Reading the levels</div>'
             'Rounded boxes are <b>processes</b>, cylinders are <b>data stores</b>, diamonds are <b>decision gates</b>. '
             '<span class="b-yellow">Amber</span> = a gate; <span class="b-red">red</span> = a safety floor / escalation that '
             'can never be averaged or relaxed away. Every gate is numbered in the rules table.</div>')
    h.append('<h2 id="l0">L0 · Context</h2>')
    h.append('<div class="diagram"><div class="dt">External entities → engine → outputs</div><pre class="mermaid">%s</pre></div>' % L0)
    h.append('<h2 id="l1">L1 · Pipeline &amp; data stores</h2>')
    h.append('<div class="diagram"><div class="dt">Five stages, with the stores each reads</div><pre class="mermaid">%s</pre></div>' % L1)
    h.append('<h2 id="l2">L2 · Full decision-gate flow</h2>')
    h.append('<div class="diagram"><div class="dt">One marker → its pillar → the score, every gate explicit</div><pre class="mermaid">%s</pre></div>' % L2)

    h.append('<h2 id="rules">Decision gates &amp; rules <span class="small muted">· exhaustive</span></h2>')
    h.append('<div class="tablewrap"><table><thead><tr><th>#</th><th>Stage</th><th>Gate / rule</th>'
             '<th>Condition → action</th><th>Source</th></tr></thead><tbody>')
    rules = [
      ("G0", "Upstream", "Eligibility gating", "only valid/relevant questions captured (sex/age/show_if) before any input reaches scoring", "Eligibility &amp; gating"),
      ("G1", "Ingest", "Confidence floor", "confidence = q_source · recency-decay; if &lt; per-marker floor → mark stale, blend literature fallback at low confidence", "Doc 06 §2"),
      ("G2", "Ingest", "Missing data", "no usable measurement → impute cohort median, source=literature_fallback, reduce pillar coverage", "Doc 06 §4.3"),
      ("G3", "Ingest", "Wearable trust tier (D22)", "inferential metric → informational only (cannot set a band); clinical-grade may drive a band with confirmation", "Doc 07 §2"),
      ("G4", "Marker", "Continuous band", "risk read off a continuous curve; green &lt;0.15 / yellow &lt;0.5 / red ≥0.5 (labels, not cliffs)", "Doc 03 §1"),
      ("G5", "Marker", "Cohort max rule", "r_i = max(r_i^clin, φ·r_i^cohort), φ=0.6 — cohort can only RAISE concern, never dilute the clinical anchor", "Doc 03 §2 · Doc 06 §4.4"),
      ("G6", "Marker", "Personalization clamp", "r_i = band_clamp(… + r_i^pers); can't relax a red, clear a critical, or by itself flip a band", "Doc 03 §2b"),
      ("G7", "Marker", "Critical exclusion", "personalization disabled for critical markers (𝟙[non-critical])", "Doc 03 §2b"),
      ("G8", "Pillar", "Confidence weighting", "each marker's weight is scaled by its confidence — low-confidence inputs contribute less", "Doc 03 §3"),
      ("G9", "Pillar", "δ-power-mean (within)", "R_k^mark aggregates marker risks via a δ-power-mean with within-pillar weights w_i", "Doc 03 §5.2"),
      ("G10", "Pillar", "Reservoir add", "R_k = clamp₀₁(R_k^mark + ρ_k·B̃_k); MONIAC stocks, leakage λ, cross-pillar interference κ", "Doc 03 §3 · Doc 04"),
      ("G11", "Pillar", "Critical-marker override", "∃ critical-marker red → status critical; R_k ← max(R_k, 0.60) hard floor", "Doc 03 §4/§6 · Doc 02"),
      ("G12", "Score", "δ-power-mean (across)", "R_total = δ-power-mean over pillars with weights W_k", "Doc 03 §5.2"),
      ("G13", "Score", "Critical cascade", "a single critical marker floors its pillar and is never averaged away by the formula", "Doc 03 §6 · Doc 02"),
      ("G14", "Safety", "Acute-danger escalation", "acute-danger red → immediate clinician / crisis pathway (independent of the scalar score)", "Doc 16 · Doc 03 §6"),
      ("G15", "Modifier", "Acute-event override", "acute event active → apply acute weights m_k^acute, override then revert on resolution", "Doc 09"),
      ("G16", "Modifier", "Sex / life-stage model", "select hormone-/cycle-/pregnancy-/menopause-aware ranges before banding", "Doc 08 · Doc 09"),
      ("G17", "Output", "Coverage suppression", "pillar 'green' only via imputed medians → label low-coverage green, never a clean bill", "Doc 06 §4.3 · Doc 03"),
      ("G18", "Output", "Companion vector", "emit Confidence / Trajectory / early-warning alongside the scalar score", "Doc 05"),
    ]
    for gid, stage, rule, cond, src in rules:
        h.append('<tr><td class="mono small">%s</td><td class="small">%s</td><td class="small"><b>%s</b></td>'
                 '<td class="small">%s</td><td class="small muted">%s</td></tr>' % (gid, _esc(stage), _esc(rule), cond, src))
    h.append('</tbody></table></div>')

    # ================= DEEPENINGS =================
    # A · MONIAC reservoir internals (gate G10)
    RESV = """flowchart LR
  MKB["adverse markers + behaviours"] --> INF["inflow_j = κ_in·σ(Σ a·excess + Σ b·behaviour)"]
  INF --> TANK[("reservoir stock B_j")]
  TANK -->|"heal: − λ_j·(B_j − B_j*)"| TANK
  OTHER[("other reservoirs B_l")] -->|"+ κ_jl·sat(B_l)"| TANK
  TANK -->|"+ κ_lj·sat(B_j)"| OTHER
  TANK --> NORM["B̃_k normalized load"]
  NORM -->|"ρ_k coupling"| RK["R_k = clamp01(R_k_mark + ρ_k·B̃_k)"]
  classDef store fill:#10151e,stroke:#8f9bff,color:#cdd6ff;
  class TANK,OTHER store"""
    h.append('<h2 id="reservoir">Deep-dive · MONIAC reservoir internals <span class="small muted">· gate G10</span></h2>')
    h.append('<p class="small">Zoom into the reservoir term of pillar risk (<a class="xref" href="04-moniac-reservoir-dynamics.html">Doc 04</a>): each latent stock <code>B_j</code> integrates exposure, heals toward a set-point, and couples to other tanks before contributing <code>B̃_k</code> to its pillar.</p>')
    h.append('<div class="diagram"><div class="dt">Stock-and-flow for one reservoir B_j</div><pre class="mermaid">%s</pre></div>' % RESV)
    h.append('<pre class="code"><code>B_j(t+Δ) = B_j(t) + Δ · [ inflow_j(markers, behaviours)       # source (logistic σ)\n                         − λ_j · (B_j − B_j*)                # leakage toward set-point\n                         + Σ κ_jl · sat(B_l) ]               # cross-tank interference\n           clamped to  B_j ∈ [0, B_j^max]</code></pre>')
    h.append('<div class="tablewrap"><table><thead><tr><th>Coupling</th><th>Sign</th><th>Meaning</th></tr></thead><tbody>'
             '<tr><td class="mono small">κ(INFL←SLD)</td><td>+</td><td class="small">chronic sleep debt raises inflammatory load</td></tr>'
             '<tr><td class="mono small">κ(GLY←SLD)</td><td>+</td><td class="small">sleep debt worsens glycemic control</td></tr>'
             '<tr><td class="mono small">κ(GLY←CRF)</td><td>−</td><td class="small">cardiorespiratory reserve lowers glycemic burden (asset protects)</td></tr>'
             '<tr><td class="mono small">κ(ATH←GLY), κ(ATH←INFL)</td><td>+</td><td class="small">glycemic + inflammatory load accelerate atherogenesis</td></tr>'
             '<tr><td class="mono small">κ(ALLO↔SLD)</td><td>±</td><td class="small">stress and sleep debt are bidirectional</td></tr>'
             '<tr><td class="mono small">λ_j</td><td>—</td><td class="small">large = fast heal (sleep debt) · tiny = near-permanent (atherogenic, CAC)</td></tr>'
             '</tbody></table></div>')

    # B · Worked numeric example
    h.append('<h2 id="worked">Deep-dive · Worked numeric example <span class="small muted">· illustrative</span></h2>')
    h.append('<div class="callout note"><div class="ct">Illustrative only</div>A 52-year-old male, Cardiovascular pillar. r-values are hand-chosen to show the gates firing, not calibrated outputs.</div>')
    h.append('<div class="tablewrap"><table><thead><tr><th>#</th><th>Input</th><th>Gate</th><th>Result</th></tr></thead><tbody>')
    wrows = [
      ("1", "ApoB 124 mg/dL (≥100)", "G4 band", "red → r_clin ≈ 0.62"),
      ("2", "ApoB cohort ≈ p70", "G5 cohort max", "max(0.62, 0.6·0.50) = 0.62"),
      ("3", "ApoB is a critical marker", "G7 critical exclusion", "personalization disabled → r = 0.62"),
      ("4", "Systolic BP 134 (120–139)", "G4 band", "yellow → r ≈ 0.38 (critical marker, not red)"),
      ("5", "LDL 150 · HDL 42 · RHR 72 · HRV p30 · CAC 40", "G4 band", "yellow → r ≈ 0.30–0.42 each"),
      ("6", "9 CV markers · weights w_i", "G8 + G9 conf-weighted δ-power-mean", "R_CV_mark ≈ 0.48"),
      ("7", "atherogenic reservoir elevated (B̃ ≈ 0.60, ρ ≈ 0.15)", "G10 reservoir add", "R_CV = clamp01(0.48 + 0.09) = 0.57"),
      ("8", "ApoB red AND critical", "G11 critical override", "status CRITICAL · R_CV ← max(0.57, 0.60) = 0.60"),
      ("9", "pillars aggregated (W_k)", "G12 cross-pillar δ-power-mean", "R_total ≈ 0.34 (CV floored at 0.60 not averaged away — G13)"),
      ("10", "SBP not ≥180; no acute-danger red", "G14 escalation", "no crisis pathway, but CV flagged critical → priority action"),
      ("11", "final", "—", "<b>PureScore = 100·(1 − 0.34) = 66</b>"),
      ("12", "companion", "G18", "Confidence high · Trajectory ↑ if ApoB improving"),
    ]
    for n, inp, gate, res in wrows:
        h.append('<tr><td class="mono small">%s</td><td class="small">%s</td><td class="small">%s</td><td class="small">%s</td></tr>'
                 % (n, _esc(inp), _esc(gate), res))
    h.append('</tbody></table></div>')

    # C · Per-stage L2 sub-diagrams
    ING = """flowchart LR
  S["source: lab / wearable / survey"] --> Q["q_source lookup"]
  Q --> D["× recency decay exp(−Δt/τ)"]
  D --> C{"confidence ≥ floor?"}
  C -->|no / missing| F["stale → blend literature fallback (↓coverage)"]
  C -->|yes| T{"wearable tier?"}
  T -->|inferential| I["informational only"]
  T -->|clinical / consumer / lab| OK["usable x_i + confidence"]
  F --> OK"""
    MRK = """flowchart LR
  X["x_i + confidence"] --> CB["r_clin off band curve"]
  X --> CO["r_cohort = F_ic percentile"]
  CB --> MX["r = max(r_clin, 0.6·r_cohort)"]
  CO --> MX
  MX --> K{"critical marker?"}
  K -->|no| P["+ r_pers, band_clamp"]
  K -->|yes| R["r_i"]
  P --> R"""
    PIL = """flowchart LR
  RS["marker risks r_i"] --> W["× confidence weights w_i"]
  W --> PM["δ-power-mean → R_k_mark"]
  PM --> RZ["+ ρ_k·B̃_k → clamp01"]
  RZ --> OV{"critical-marker red?"}
  OV -->|yes| FL["status critical · max(R_k, 0.60)"]
  OV -->|no| ST["status from bands"]"""
    SCO = """flowchart LR
  RK["pillar risks R_k"] --> PM2["δ-power-mean over W_k → R_total"]
  PM2 --> AC{"acute event?"}
  AC -->|yes| AW["apply acute weights (Doc 09)"]
  AC -->|no| PS["PureScore = 100·(1 − R_total)"]
  AW --> PS
  PS --> CG{"low coverage?"}
  CG -->|yes| LC["low-coverage green label"]
  CG -->|no| OUT["score + bands + companion"]
  LC --> OUT"""
    h.append('<h2 id="stages">Deep-dive · Per-stage sub-flows</h2>')
    for title, dia in [("Stage 1 · Ingest + confidence", ING), ("Stage 2 · Marker risk", MRK),
                       ("Stage 3 · Pillar risk", PIL), ("Stage 4–5 · Score + output", SCO)]:
        h.append('<div class="diagram"><div class="dt">%s</div><pre class="mermaid">%s</pre></div>' % (_esc(title), dia))

    # D · Representative per-pillar DFD (Cardiovascular)
    CVP = """flowchart TB
  SBP["Systolic BP · w.15 · CRITICAL"] --> AGG
  DBP["Diastolic BP · w.10"] --> AGG
  APOB["ApoB · w.18 · CRITICAL"] --> AGG
  LDL["LDL-C · w.12 · CRITICAL"] --> AGG
  HDL["HDL-C · w.06"] --> AGG
  LPA["Lp(a) · w.10"] --> AGG
  RHR["Resting HR · w.07"] --> AGG
  HRV["HRV · w.07"] --> AGG
  CAC["CAC · w.15 · CRITICAL"] --> AGG
  AGG["confidence-weighted δ-power-mean → R_CV_mark"] --> RES["+ reservoirs: atherogenic · vascular/BP · cardiorespiratory"]
  RES --> OV{"any CRITICAL marker red?"}
  OV -->|yes| CR["status CRITICAL · R_CV ← max(R_CV, 0.60)"]
  OV -->|no| ST["status from R_CV bands"]
  classDef crit fill:#1a0f15,stroke:#f0606e,color:#ffd0d6;
  class SBP,APOB,LDL,CAC crit"""
    h.append('<h2 id="cv-pillar">Deep-dive · Per-pillar template (Cardiovascular)</h2>')
    h.append('<p class="small">The marker→pillar pattern for one pillar (<a class="xref" href="02-pillars-and-marker-catalog.html">Doc 02 · CV</a>); the other 11 pillars follow the same shape with their own markers, weights and critical set.</p>')
    h.append('<div class="diagram"><div class="dt">Cardiovascular: 9 markers → pillar risk → critical cascade</div><pre class="mermaid">%s</pre></div>' % CVP)

    h.append('<div class="callout note"><div class="ct">Keep this in sync</div>'
             'This diagram is <b>derived</b> from the scoring docs. Per the wiki guide (<code>CLAUDE.md</code> · diagram '
             'dependency map), any change to the scoring docs or the admin weights <b>must</b> trigger a review of this '
             'page and the other system diagrams, with the impact noted.</div>')
    return "PureScore calculation — data flow", "".join(h)

# =================================================================== PURESCORE UBER MAP (interactive)
def build_purescore_uber():
    body = r"""<div class="crumbs"><a href="index.html">Home</a> &rsaquo; Diagrams &amp; system maps &rsaquo; PureScore calculation explorer</div>
<h1>PureScore Calculation Explorer <span class="small muted">&middot; cockpit &middot; one engine, one JSON</span></h1>
<p class="lead">The live scoring engine you can poke at: edit any input and watch it flow through markers &rarr; pillars &rarr; PureScore. The <b>Cockpit</b> shows the audit tree; the <b>Flow map</b> shows the same computation as a dataflow. Every number is the real engine reading the canonical JSON &mdash; nothing here is hard-coded.</p>
<div id="fdConfig" class="fd-config"></div>

<div class="ce-tabs">
  <button class="ce-tab active" data-view="tree">&#9636; Cockpit</button>
  <button class="ce-tab" data-view="map">&#9638; Flow map</button>
  <label class="um-lab" style="margin-left:8px">profile <select id="ceProfile" class="um-sel"></select></label>
  <span class="ce-grow"></span>
  <button class="um-btn" id="ceFocus" title="hide the wiki chrome for a full-width cockpit">&#10530; focus</button>
</div>

<div id="ceCockpit">
  <div class="fd-band">
    <div class="fd-scorebox"><div class="n" id="fdScoreN">&mdash;</div><div class="ci" id="fdScoreCI"></div><div class="fd-scoreband" id="fdScoreBand"></div><div class="b" id="fdScoreB">PURESCORE</div></div>
    <div class="fd-companion" id="fdComp"></div>
    <div class="fd-pillwrap"><div class="fd-seclabel">12 pillars &middot; R_k (click to trace)</div><div class="fd-pillgrid" id="fdPillars"></div></div>
    <div class="fd-reswrap"><div class="fd-seclabel">15 reservoirs &middot; load</div><div class="fd-restanks" id="fdRes"></div></div>
  </div>

  <div class="ce-tools ce-treeonly">
    <label class="um-lab">root <select id="ceRoot" class="um-sel"></select></label>
    <input id="ceSearch" class="ce-search" type="search" placeholder="&#128269; filter the tree &mdash; marker, formula, constant, gate&hellip;">
    <span class="um-lab">missing&rarr;</span>
    <button class="um-btn ce-strm" data-src="lab">labs</button>
    <button class="um-btn ce-strm" data-src="wearable">wearable</button>
    <button class="um-btn ce-strm" data-src="pro">PRO</button>
    <button class="um-btn ce-strm" data-src="clinical">clinical</button>
    <button class="um-btn" id="ceExpand">expand</button>
    <button class="um-btn" id="ceCollapse">collapse</button>
    <button class="um-btn" id="ceResetMk">reset</button>
    <span class="um-grow"></span>
    <span id="ceCoverage" class="um-lab"></span>
  </div>

  <div class="fd-main ce-treeonly">
    <div class="ce-tree" id="ceTree"></div>
    <aside class="fd-trace" id="ceTrace"><div class="fd-trace-h">execution trace</div>
      <div id="ceTraceBody" class="fd-trace-b"><p class="muted small">Click any node (tree, pillar dial, or reservoir tank) to trace its complete execution path &mdash; formula, conditions, actual values and contribution at every level.</p></div></aside>
  </div>

  <div class="fd-strip ce-treeonly">
    <div class="fd-card"><div class="fd-card-h">Companion vector</div><div id="fdCompPanel"></div></div>
    <div class="fd-card"><div class="fd-card-h">Patient action queue &middot; next 5</div><div id="fdQueue"></div></div>
    <div class="fd-card"><div class="fd-card-h">Adherence check-ins to ask</div><div id="fdCheckins"></div></div>
    <div class="fd-card"><div class="fd-card-h">Adherence history &middot; 12 wk</div><div id="fdAdhHist"></div></div>
    <div class="fd-card"><div class="fd-card-h">Wearable baselines &middot; tap to drill <a href="wearable-baselines.html" style="float:right;color:#9cc7f0;text-decoration:none">full app &rsaquo;</a></div><div id="fdWearBase"></div></div>
    <div class="fd-card"><div class="fd-card-h">Diagnoses &amp; meds (Patient360) &middot; click for conditions</div><div id="fdDxMeds"></div>
      <div class="fd-sim"><select id="fdSimType" class="um-sel"><option value="lab">+ lab</option><option value="dx">+ diagnosis</option><option value="med">+ med</option></select><select id="fdSimItem" class="um-sel"></select><button class="um-btn" id="fdSimAdd">simulate EHR</button></div></div>
    <div class="fd-card"><div class="fd-card-h">Onboarding &amp; periodic questions answered</div><div id="fdQuestions"></div></div>
    <div class="fd-card"><div class="fd-card-h">Improve adherence &middot; barrier-matched + generic</div><div id="fdAdhActions"></div></div>
    <div class="fd-card"><div class="fd-card-h">Top nudges &middot; &Delta;PureScore</div><div id="fdNudge"></div></div>
    <div class="fd-card"><div class="fd-card-h">Forecast trajectory</div><div id="fdForecast"></div></div>
    <div class="fd-card"><div class="fd-card-h">Critical annunciator</div><div id="fdCrit"></div></div>
    <div class="fd-card"><div class="fd-card-h">Pillar contribution &middot; where points go</div><div id="fdWaterfall"></div></div>
    <div class="fd-card"><div class="fd-card-h">Data freshness &middot; systems check</div><div id="fdSystems"></div></div>
    <div class="fd-card"><div class="fd-card-h">What-if vs baseline</div><div id="fdWhatif"></div></div>
    <div class="fd-card" style="grid-column:1/-1"><div class="fd-card-h">Trends &middot; PureScore &amp; pillars <span id="fdRangeBtns" class="fd-range"></span></div><div id="fdTrend"></div></div>
  </div>
</div>

<div class="ce-tools ce-maponly" style="display:none">
  <label class="um-lab"><input type="checkbox" id="umGates"> firing gates only</label>
  <button class="um-btn" id="umFit">Fit</button>
  <button class="um-btn" id="umFs">&#10530; Fullscreen</button>
</div>
<div id="umAreas" class="um-areas ce-maponly" style="display:none"></div>
<div class="um-wrap ce-maponly" id="umWrap" style="display:none"><svg id="umSvg" width="100%" height="100%"></svg>
  <div class="um-hint">drag = pan &middot; scroll = zoom &middot; click = trace</div></div>
<div id="ceTip" class="ce-tip"></div>

<style>
/* ---- shared (map) ---- */
.um-sel,.um-btn{font:inherit;font-size:13px;padding:6px 10px;border-radius:8px;border:1px solid var(--line,#2a3340);background:var(--bg2,#0c1320);color:inherit;cursor:pointer}
.um-btn:hover{background:var(--line,#1a2230)}
.um-lab{font-size:12px;color:var(--dim,#9bb0c5);display:flex;align-items:center;gap:5px}
.um-grow{flex:1}
.um-score{font-weight:800;font-size:18px;padding:4px 12px;border-radius:9px;background:#0c1726;border:1px solid #2b5a86;min-width:46px;text-align:center}
.um-areas{display:flex;gap:6px;flex-wrap:wrap;margin-bottom:8px}
.um-areas span{font-size:11px;padding:3px 8px;border-radius:6px;border:1px solid;cursor:pointer;user-select:none}
.um-areas span.off{opacity:.32}
.um-wrap{position:relative;width:100%;height:600px;border:1px solid var(--line,#222c3a);border-radius:14px;overflow:hidden;background:radial-gradient(1200px 700px at 30% 0,#0e1726,#080d16)}
.um-wrap.fs{position:fixed;inset:0;height:100vh;width:100vw;z-index:9999;border-radius:0}
.um-hint{position:absolute;left:12px;bottom:10px;font-size:11px;color:#6b7d92;pointer-events:none}
#umSvg{display:block;cursor:grab;touch-action:none}
#umSvg.drag{cursor:grabbing}
.um-node{cursor:pointer}
.um-node text{font:600 11.5px Inter,system-ui,sans-serif;fill:#e8eef6;pointer-events:none}
.um-node .um-val{font-weight:800;font-size:11px;fill:#fff}
.um-node.on rect{stroke-width:3}
.um-edge{fill:none;stroke:#36424f;stroke-width:1.3}
.um-edge.loop{stroke-dasharray:5 4}
.um-areabg{opacity:.5}
.um-arealabel{font:700 11px Inter,sans-serif;letter-spacing:.04em;text-transform:uppercase}
/* ---- tabs / tools ---- */
.ce-tabs{display:flex;gap:8px;align-items:center;margin:14px 0 8px;border-bottom:1px solid var(--line,#222c3a);padding-bottom:8px}
.ce-tab{font:inherit;font-size:13px;font-weight:700;padding:7px 14px;border-radius:9px 9px 0 0;border:1px solid transparent;background:none;color:var(--dim,#9bb0c5);cursor:pointer}
.ce-tab.active{background:#0c1726;border-color:#2b5a86;color:#e8eef6}
.ce-grow{flex:1}
.ce-tools{display:flex;gap:7px;align-items:center;flex-wrap:wrap;margin:8px 0}
/* ---- flight-deck band ---- */
.fd-band{display:flex;gap:12px;align-items:stretch;flex-wrap:wrap;padding:9px 11px;border:1px solid var(--line,#222c3a);border-radius:14px;background:linear-gradient(180deg,#0c1422,#0a0f18);position:sticky;top:52px;z-index:30}
/* condensed (pinned) band on scroll */
.fd-band.cond{padding:5px 9px;gap:8px;box-shadow:0 6px 18px rgba(2,6,12,.5)}
.fd-band.cond .fd-companion,.fd-band.cond .fd-seclabel{display:none}
.fd-band.cond .fd-scorebox{min-width:64px;padding:2px 8px}
.fd-band.cond .fd-scorebox .n{font-size:26px}
.fd-band.cond .fd-pillgrid{grid-template-columns:repeat(12,1fr);gap:2px}
.fd-band.cond .fd-pill{padding:1px 2px}
.fd-band.cond .fd-pill .id span:first-child{display:none}
.fd-band.cond .fd-pill .r{font-size:10px}
.fd-band.cond .fd-restanks{height:30px}
.fd-band.cond .fd-tank{width:9px}
/* config / weights bar */
.fd-config{margin:10px 0;border:1px solid var(--line,#222c3a);border-radius:12px;background:#0a1018}
.fd-cfg-head{display:flex;align-items:center;gap:8px;flex-wrap:wrap;padding:7px 11px;cursor:pointer;font-size:11.5px;color:#cfe0f5}
.fd-cfg-head .k{font-family:ui-monospace,Menlo,monospace;color:#9bb0c5}
.fd-cfg-head .k b{color:#e8eef6}
.fd-cfg-head .sp{flex:1}
.fd-cfg-head .ed{font-size:10px;padding:2px 8px;border-radius:6px;border:1px solid #2b5a86;color:#9cc7f0}
.fd-cfg-body{display:none;padding:4px 11px 11px;border-top:1px solid #1c2636}
.fd-config.open .fd-cfg-body{display:block}
.fd-cfg-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:5px 14px;margin:6px 0}
.fd-knob{display:flex;align-items:center;gap:6px;font-size:11px;color:#9bb0c5}
.fd-knob label{width:62px;flex:none}
.fd-knob input[type=range]{flex:1;accent-color:#2ee6c9}
.fd-knob .vv{width:42px;text-align:right;font-weight:700;color:#e8eef6;font-variant-numeric:tabular-nums}
.fd-cfg-sub{font-size:9.5px;text-transform:uppercase;letter-spacing:.05em;color:#6b7d92;margin:9px 0 3px}
.fd-cfg-res{font-size:11px;color:#9bb0c5;line-height:1.7}
.fd-cfg-res b{color:#cfe0f5}
.fd-cfg-actions{display:flex;gap:8px;align-items:center;margin-top:8px}
.fd-cfg-warn{font-size:10.5px;color:#edc14a}
/* focus mode (hide wiki chrome) */
body.cefocus .topbar,body.cefocus .side,body.cefocus .chapter-ctx,body.cefocus .crumbs,body.cefocus .pn,body.cefocus .wf,body.cefocus .connects{display:none!important}
body.cefocus .shell{display:block}
body.cefocus .main{max-width:none;width:100%;padding:8px 14px}
body.cefocus .fd-band{top:0}
.fd-scorebox{display:flex;flex-direction:column;align-items:center;justify-content:center;min-width:92px;padding:4px 12px;border-radius:12px;background:#0a1726;border:1px solid #2b5a86}
.fd-scorebox .n{font-size:40px;font-weight:800;line-height:1;color:#fff;font-variant-numeric:tabular-nums}
.fd-scorebox .b{font-size:9px;letter-spacing:.1em;margin-top:4px;font-weight:700}
.fd-companion{display:grid;grid-template-columns:repeat(2,auto);gap:4px 12px;align-content:center;padding:2px 10px;border-left:1px solid #1c2636;border-right:1px solid #1c2636}
.fd-gauge{font-size:11px;color:#9bb0c5;white-space:nowrap}
.fd-gauge b{color:#e8eef6;font-weight:700;font-variant-numeric:tabular-nums}
.fd-seclabel{font-size:9px;text-transform:uppercase;letter-spacing:.06em;color:#6b7d92;margin-bottom:4px}
.fd-pillwrap{flex:1;min-width:250px}
.fd-pillgrid{display:grid;grid-template-columns:repeat(6,1fr);gap:4px}
.fd-pill{border:1px solid #243042;border-radius:8px;padding:3px 5px;background:#0c1320;cursor:pointer}
.fd-pill:hover{border-color:#3a6ea5}
.fd-pill .id{font-size:9.5px;font-weight:800;color:#cfe0f5;display:flex;justify-content:space-between}
.fd-pill .r{font-size:13px;font-weight:800;font-variant-numeric:tabular-nums}
.fd-pill .bar{height:4px;border-radius:3px;background:#1c2636;margin-top:2px;overflow:hidden}
.fd-pill .bar i{display:block;height:100%}
.fd-pill.crit{box-shadow:0 0 0 1px #f0606e inset}
.fd-pill.lowcov{border-style:dashed;opacity:.78}
.fd-reswrap{min-width:236px}
.fd-restanks{display:flex;gap:3px;align-items:flex-end;height:52px}
.fd-tank{width:12px;background:#0c1320;border:1px solid #243042;border-radius:3px;height:100%;display:flex;flex-direction:column-reverse;cursor:pointer}
.fd-tank i{display:block;width:100%}
.fd-tank.asset i{background:#3ad6a0}.fd-tank.burden i{background:#e0796b}
.fd-tank:hover{border-color:#3a6ea5}
/* ---- main: tree | trace ---- */
.fd-main{display:grid;grid-template-columns:1fr 360px;gap:10px;margin-top:8px;align-items:start}
.ce-tree{min-width:0;border:1px solid var(--line,#222c3a);border-radius:14px;background:#0a1018;padding:8px 6px;overflow:auto;max-height:72vh}
.fd-trace{border:1px solid var(--line,#222c3a);border-radius:14px;background:#0a1018;max-height:72vh;overflow:auto;position:sticky;top:8px}
.fd-trace-h{font-size:10.5px;text-transform:uppercase;letter-spacing:.05em;color:#9bb0c5;padding:9px 12px;border-bottom:1px solid #1c2636;position:sticky;top:0;background:#0c1422;z-index:2}
.fd-trace-b{padding:11px 13px;font-size:12.5px;color:#cdd9e8}
.fd-trace-b pre{background:#080d18;border:1px solid #1c2636;border-radius:8px;padding:8px 10px;font-size:11.5px;overflow:auto;color:#bcd2f0;white-space:pre-wrap;margin:5px 0}
.fd-k{font-size:9.5px;text-transform:uppercase;letter-spacing:.05em;color:#6b7d92;margin:11px 0 3px}
.fd-path{list-style:none;margin:0;padding:0}
.fd-path li{padding:4px 8px;border-left:2px solid #243042;margin:2px 0;font-size:12px;font-variant-numeric:tabular-nums}
.fd-path li b{color:#fff}
.fd-live{margin-top:8px;padding:8px 10px;border-radius:8px;background:#0c1726;border:1px solid #2b5a86}
/* ---- bottom strip ---- */
.fd-strip{display:grid;grid-template-columns:repeat(auto-fit,minmax(248px,1fr));gap:10px;margin-top:10px}
.fd-card{border:1px solid var(--line,#222c3a);border-radius:12px;background:#0a1018;padding:8px 11px;min-height:104px}
.fd-card-h{font-size:9.5px;text-transform:uppercase;letter-spacing:.05em;color:#6b7d92;margin-bottom:6px}
.fd-nud{display:flex;align-items:center;gap:8px;font-size:12px;padding:2px 0}
.fd-nud .d{margin-left:auto;font-weight:800;color:#3ad6a0;font-variant-numeric:tabular-nums}
.fd-ping{display:inline-block;font-size:11px;padding:2px 7px;border-radius:6px;border:1px solid #2a3a4d;margin:2px 3px 0 0;color:#cfe0f5}
/* companion vector bars */
.fd-cv{display:flex;align-items:center;gap:7px;font-size:11px;margin:3px 0}
.fd-cv .l{width:78px;color:#9bb0c5;flex:none}
.fd-cv .t{width:100%;height:6px;border-radius:4px;background:#1c2636;overflow:hidden}
.fd-cv .t i{display:block;height:100%;background:#49c6d8}
.fd-cv .v{width:54px;text-align:right;font-weight:700;color:#e8eef6;font-variant-numeric:tabular-nums;flex:none}
/* patient action queue */
.fd-q{border:1px solid #1c2636;border-radius:8px;padding:5px 8px;margin:4px 0;background:#0c1320}
.fd-q .t{font-size:12px;font-weight:700;color:#e8eef6}
.fd-q .m{font-size:10.5px;color:#9bb0c5;margin-top:1px}
.fd-q .tag{font-size:9px;font-weight:800;padding:1px 5px;border-radius:5px;border:1px solid #2b5a86;color:#9cc7f0;margin-right:5px}
.fd-q .d{float:right;font-weight:800;color:#3ad6a0;font-size:11px}
/* check-in questions */
.fd-ci{margin:5px 0;font-size:12px}
.fd-ci .s{color:#dbe6f3}
.fd-ci .o{display:flex;flex-wrap:wrap;gap:3px;margin-top:2px}
.fd-ci .o span{font-size:10px;padding:1px 6px;border-radius:5px;border:1px solid #2a3a4d;color:#9bb0c5}
/* adherence history */
.fd-hist-wk{display:flex;gap:3px;margin:4px 0}
.fd-hist-wk i{width:12px;height:12px;border-radius:3px;display:block}
.fd-hrow{display:flex;align-items:center;gap:6px;font-size:10.5px;color:#9bb0c5;margin:2px 0}
.fd-hrow .nm{width:84px;flex:none}
.fd-hrow .cells{display:flex;gap:2px}
.fd-hrow .cells i{width:10px;height:10px;border-radius:2px;display:block}
/* score ± CI */
.fd-scorebox .ci{font-size:10px;color:#9bb0c5;margin-top:2px;font-weight:700}
.fd-scorebox .ci .prov{color:#edc14a}
/* waterfall */
.fd-wf{display:flex;align-items:center;gap:6px;font-size:11px;margin:2px 0;cursor:pointer}
.fd-wf:hover{background:#101a28;border-radius:5px}
.fd-wf .id{width:34px;font-weight:800;color:#cfe0f5;flex:none}
.fd-wf .t{flex:1;height:9px;background:#1c2636;border-radius:4px;overflow:hidden}
.fd-wf .t i{display:block;height:100%}
.fd-wf .v{width:34px;text-align:right;color:#e8eef6;font-weight:700;font-variant-numeric:tabular-nums;flex:none}
/* systems check */
.fd-sys{display:flex;align-items:center;gap:7px;font-size:11px;margin:3px 0}
.fd-sys .ch{width:74px;flex:none;color:#cfe0f5;font-weight:600}
.fd-sys .st{font-weight:800}
.fd-sys .meta{margin-left:auto;color:#9bb0c5;font-variant-numeric:tabular-nums}
/* what-if */
.fd-wi{display:flex;align-items:center;gap:6px;font-size:11.5px;margin:2px 0;font-variant-numeric:tabular-nums}
.fd-wi .m{flex:1;color:#dbe6f3}
.fd-wi .d{font-weight:800}
.fd-wi .net{border-top:1px solid #1c2636;margin-top:4px;padding-top:4px;font-weight:800}
/* critical annunciator */
.fd-crit{border:1px solid #7a3344;border-radius:8px;background:#1a0c12;padding:6px 9px;margin:4px 0;font-size:11.5px}
.fd-crit .h{font-weight:800;color:#ffb9c6}
.fd-crit .x{color:#e8c7cf;margin-top:2px}
.fd-crit .sm{margin-top:3px;color:#cdd9e8}
.fd-crit .sm b{color:#fff}
.fd-ok{font-size:12px;color:#3ad6a0}
/* trend + range */
.fd-range{float:right}
.fd-range button{font:inherit;font-size:10px;padding:1px 7px;border-radius:6px;border:1px solid #2a3a4d;background:#0c1622;color:#9bb0c5;cursor:pointer;margin-left:3px}
.fd-range button.on{background:#16335e;border-color:#3a6ea5;color:#fff}
.fd-legend{display:flex;flex-wrap:wrap;gap:10px;font-size:10.5px;margin-top:4px;color:#9bb0c5}
.fd-legend span i{display:inline-block;width:10px;height:3px;vertical-align:middle;margin-right:3px}
/* wearable baselines */
.fd-wb{display:flex;align-items:center;gap:7px;font-size:11.5px;padding:3px 4px;border-radius:6px;cursor:pointer}
.fd-wb:hover{background:#101a28}
.fd-wb.off{opacity:.45;cursor:default}
.fd-wb .nm{width:96px;flex:none;color:#cfe0f5;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.fd-wb svg{flex:none}
.fd-wb .tv{margin-left:auto;font-weight:700;font-variant-numeric:tabular-nums;color:#e8eef6}
.fd-wb .zc{width:44px;text-align:right;font-weight:700;font-variant-numeric:tabular-nums}
.fd-wb .meta{margin-left:auto;color:#6b7d92}
.fd-band-viz{position:relative;height:14px;background:#1c2636;border-radius:7px;margin:6px 0}
.fd-band-viz .b1{position:absolute;top:0;bottom:0;left:25%;right:25%;background:rgba(58,214,160,.22);border-left:1px solid rgba(58,214,160,.6);border-right:1px solid rgba(58,214,160,.6)}
.fd-band-viz .mk{position:absolute;top:-3px;width:3px;height:20px;background:#fff;border-radius:2px}
/* score variance band */
.fd-scoreband{position:relative;height:6px;width:84px;background:#1c2636;border-radius:4px;margin:4px auto 0}
.fd-scoreband i{position:absolute;top:0;bottom:0;background:rgba(73,198,216,.45);border-radius:4px}
.fd-scoreband b{position:absolute;top:-2px;width:2px;height:10px;background:#fff}
/* diagnoses & meds */
.fd-dx{display:inline-block;font-size:11px;padding:2px 8px;margin:2px 4px 2px 0;border-radius:7px;border:1px solid #2b5a86;background:#0c1726;color:#cfe0f5;cursor:pointer}
.fd-dx:hover{border-color:#2ee6c9}
.fd-dx.med{border-color:#7a5f24;color:#edc7a0}
.fd-dx .c{color:#6b7d92;font-size:9px;margin-left:4px}
.fd-sim{display:flex;gap:5px;margin-top:8px;flex-wrap:wrap}
.fd-sim .um-sel{font-size:11px;padding:3px 6px}
/* questions answered */
.fd-qsub{font-size:9px;text-transform:uppercase;letter-spacing:.05em;color:#6b7d92;margin:6px 0 2px}
.fd-q2{display:flex;align-items:baseline;gap:6px;font-size:11.5px;padding:2px 4px;border-radius:5px;cursor:pointer}
.fd-q2:hover{background:#101a28}
.fd-q2 .qt{flex:1;color:#cdd9e8}
.fd-q2 .qa{color:#3ad6a0;font-weight:700;white-space:nowrap}
.fd-q2 .qp{color:#6b7d92;font-size:9px;white-space:nowrap}
/* adherence actions */
.fd-aa{display:flex;align-items:center;gap:7px;font-size:11.5px;padding:3px 4px;border-radius:6px;margin:2px 0}
.fd-aa .bar{font-size:9px;font-weight:800;padding:1px 5px;border-radius:5px;border:1px solid #7a3344;color:#ffb9c6;flex:none}
.fd-aa .bar.gen{border-color:#2a3a4d;color:#9bb0c5}
.fd-aa .t{flex:1;color:#dbe6f3}
.fd-aa .lift{color:#3ad6a0;font-weight:700;font-variant-numeric:tabular-nums}
.fd-aa button{font-size:10px;padding:2px 7px;border-radius:6px;border:1px solid #2b5a86;background:#16335e;color:#fff;cursor:pointer}
.fd-aa button.done{background:#10301f;border-color:#2f6b48;color:#bff0d0}
/* ---- tree rows ---- */
.ce-row{display:flex;align-items:center;gap:7px;padding:2px 8px;margin:1px 0;border-left:3px solid #2a3340;border-radius:0 7px 7px 0;cursor:pointer;font-size:13px}
.ce-row:hover{background:#101a28}
.ce-row.sel{background:#11233a;outline:1px solid #2b5a86}
.ce-row.has>.ce-lab{font-weight:600}
.ce-car{width:12px;color:#6b7d92;font-size:10px;flex:none}
.ce-car.open{color:#2ee6c9}
.ce-lab{color:#dbe6f3}
.ce-val{margin-left:auto;font-weight:700;font-size:12px;color:#cfe0f5;font-family:ui-monospace,Menlo,monospace;white-space:nowrap}
.ce-val.z-green{color:#3ad6a0}.ce-val.z-yellow{color:#edc14a}.ce-val.z-red{color:#f0606e}
.ce-rng{width:108px;accent-color:#2ee6c9;margin-left:auto}
.ce-rng+.ce-val{margin-left:8px;min-width:70px;text-align:right}
.ce-rng:disabled{opacity:.3}
.ce-i{flex:none;background:none;border:none;color:#5b7790;cursor:pointer;font-size:13px;padding:0 2px}
.ce-i:hover{color:#2ee6c9}
.ce-cap{font-size:10px;color:#6b7d92;margin-left:6px;white-space:nowrap}
/* ---- decision branches ---- */
.ce-row.ce-active{background:rgba(58,214,160,.08)}
.ce-row.ce-active>.ce-car{color:#3ad6a0}.ce-row.ce-active>.ce-val{color:#3ad6a0}
.ce-row.ce-inactive{opacity:.4}.ce-row.ce-inactive>.ce-car{color:#6b7d92}
/* ---- data-state ---- */
.ce-state{flex:none;width:20px;height:18px;border-radius:5px;border:1px solid #2a3a4d;background:#0c1622;color:#9bb0c5;font-size:10px;font-weight:800;cursor:pointer;padding:0}
.ce-state.s-present{border-color:#2f6b48;color:#3ad6a0}
.ce-state.s-stale{border-color:#7a5f24;color:#edc14a;background:#1d1a0c}
.ce-state.s-missing{border-color:#7a3344;color:#f0606e;background:#2a1118}
/* ---- PRO questionnaire ---- */
.ce-qitem{display:flex;align-items:center;gap:6px;padding:2px 8px 2px 26px;font-size:12px}
.ce-qitem .q{flex:1;color:#c5d3e6}
.ce-qopt{display:flex;gap:2px}
.ce-qopt button{width:18px;height:18px;border-radius:4px;border:1px solid #2a3a4d;background:#0c1622;color:#9bb0c5;font-size:10px;cursor:pointer;padding:0}
.ce-qopt button.on{background:#16335e;border-color:#3a6ea5;color:#fff}
/* ---- search + streams ---- */
.ce-search{flex:1;min-width:200px;font:inherit;font-size:13px;padding:6px 10px;border-radius:9px;border:1px solid var(--line,#2a3340);background:var(--bg2,#0c1320);color:inherit}
.ce-strm.on{background:#2a1118;border-color:#7a3344;color:#ffb9c6}
/* ---- chips + tip ---- */
.ce-chip{font-size:11px;padding:2px 8px;border-radius:7px;border:1px solid #2a3a4d;background:#0c1622;color:#cfe0f5;text-decoration:none}
a.ce-chip:hover{border-color:#2ee6c9;color:#fff}
.ce-chips{display:flex;flex-wrap:wrap;gap:5px}
.ce-tip{position:fixed;z-index:9999;pointer-events:none;background:#0d1422;border:1px solid #2b5a86;border-radius:8px;padding:7px 10px;font-size:11.5px;color:#cfe0f5;max-width:260px;display:none;box-shadow:0 8px 24px rgba(0,0,0,.5)}
.ce-tip b{color:#fff}
@media(max-width:900px){.fd-main{grid-template-columns:1fr}.fd-strip{grid-template-columns:1fr}.fd-pillgrid{grid-template-columns:repeat(4,1fr)}}
</style>
<div id="cfFlags" style="max-width:1180px;margin:20px auto 0"></div>
<script src="assets/calc-data.js"></script>
<script src="assets/engine.js"></script>
<script src="assets/calc-explorer.js"></script>
<script src="assets/flags-data.js"></script>
<script src="assets/clinical-flags.js"></script>"""
    return "Calculation Explorer", body

# =================================================================== APPENDIX F — lifestyles & personas
def build_lifestyles():
    pa = _load("persona-axes.json")
    m = pa["meta"]
    h = ['<div class="crumbs"><a href="index.html">Home</a> › Questions &amp; intake › Lifestyles</div>',
         '<h1>Appendix F — Lifestyles, Axes &amp; Perceived-vs-Actual</h1>',
         '<p class="lead">%s</p>' % _esc(m["idea"]), ILLUS]
    h.append('<div class="diagram"><div class="dt">Perceived vs actual → strategy</div>'
             '<pre class="mermaid">flowchart LR\n'
             '  SR["self-report (perceived)"] --> GAP{"gap"}\n'
             '  OBJ["labs + wearables + PRO (actual)"] --> GAP\n'
             '  GAP --> NUDGE["strategy + companion vector (Doc 05)"]\n'
             '  NUDGE --> ACT["nudges (Doc 11/12)"] --> AD["adherence"] --> OBJ</pre></div>')
    # axes
    h.append('<h2 id="axes">Lifestyle axes</h2>')
    h.append('<p class="small muted">Any person is a vector over these independent axes. Each axis names the '
             'self-report signal (perceived) and the objective signal (actual) used to place them, and the '
             'pillars it drives.</p>')
    h.append('<div class="tablewrap"><table><thead><tr><th>Axis</th><th>Levels</th><th>Pillars</th>'
             '<th>Perceived signal</th><th>Actual signal</th></tr></thead><tbody>')
    for ax in pa["axes"]:
        h.append('<tr><td><b>%s</b><br><span class="small muted mono">%s</span></td>'
                 '<td class="small">%s</td><td class="small">%s</td><td class="small muted">%s</td>'
                 '<td class="small muted">%s</td></tr>'
                 % (_esc(ax["name"]), _esc(ax["id"]),
                    " → ".join('<span class="chip b-mut">%s</span>' % _esc(l) for l in ax["levels"]),
                    ", ".join(_esc(p) for p in ax["pillars"]),
                    ", ".join(_esc(x) for x in ax["perceived_signal"]),
                    ", ".join(_esc(x) for x in ax["actual_signal"])))
    h.append('</tbody></table></div>')
    # archetypes
    h.append('<h2 id="archetypes">Named archetypes <span class="small muted">· %d presets</span></h2>' % len(pa["archetypes"]))
    h.append('<p class="small muted">Common presets = the nearest vector over the axes above. Each links to a '
             'clinical persona (<a class="xref" href="appendix-personas.html">Appendix D</a>) and carries pillar '
             'weight multipliers.</p>')
    h.append('<div class="tablewrap"><table><thead><tr><th>Archetype</th><th>Clinical persona</th>'
             '<th>Axis profile</th><th>Weight multipliers</th><th>Typical conditions</th><th>Frame</th></tr></thead><tbody>')
    for a in pa["archetypes"]:
        prof = " · ".join("%s:%s" % (_esc(k), _esc(v)) for k, v in a.get("axis_profile", {}).items())
        h.append('<tr><td><b>%s</b><br><span class="small muted mono">%s</span></td>'
                 '<td class="small mono">%s</td><td class="small">%s</td><td class="small mono">%s</td>'
                 '<td class="small">%s</td><td class="small muted">%s</td></tr>'
                 % (_esc(a["name"]), _esc(a["key"]), _esc(a.get("links_clinical_persona", "—")),
                    prof, _esc(a.get("weight_multipliers", "—")),
                    _esc(", ".join(a.get("typical_conditions", []))), _esc(a.get("frame", ""))))
    h.append('</tbody></table></div>')
    # perceived vs actual gaps
    pv = pa["perceived_vs_actual"]
    h.append('<h2 id="gaps">Perceived-vs-actual gaps → nudge strategy</h2>')
    h.append('<div class="callout note"><div class="ct">Method</div>%s</div>' % _esc(pv["method"]))
    h.append('<div class="tablewrap"><table><thead><tr><th>Gap</th><th>Axis</th><th>Perceived</th>'
             '<th>Actual</th><th>Detecting signal</th><th>Strategy</th><th>Companion vector</th></tr></thead><tbody>')
    for g in pv["gaps"]:
        h.append('<tr><td><b>%s</b></td><td class="small mono">%s</td><td class="small">%s</td>'
                 '<td class="small">%s</td><td class="small muted">%s</td><td class="small">%s</td>'
                 '<td class="small muted">%s</td></tr>'
                 % (_esc(g["id"]), _esc(g["axis"]), _esc(g["perceived"]), _esc(g["actual"]),
                    _esc(g["signal"]), _esc(g["strategy"]), _esc(g.get("companion_effect", ""))))
    h.append('</tbody></table></div>')
    h.append('<p class="small muted">Machine-readable source: <code>data/persona-axes.json</code>. '
             'Links: %s.</p>' % _esc(", ".join(m.get("links", []))))
    return "Appendix F · Lifestyles", "".join(h)

# =================================================================== APPENDIX G — coverage audit (versioned)
_PNAME = {p[0]: p[1] for p in PILLARS}                       # pillar id -> full name
_WEAR_HINTS = ("wear", "hrv", "rmssd", "cgm", "glucose monitor", "step", "actig", "spo2",
               "oximet", "resting hr", "resting heart", "sleep stage", "sleep track", "ring",
               "watch", "accelerom", "vo2", "cadence", "readiness", "skin temp")

def _is_wear(sigs):
    j = " ".join(str(s).lower() for s in (sigs or []))
    return any(hint in j for hint in _WEAR_HINTS)

def _audit_bar(frac, color, label=""):
    pct = max(2, min(100, int(round(frac * 100))))
    return ('<div style="display:flex;align-items:center;gap:8px">'
            '<div style="flex:1;background:#0c1322;border:1px solid var(--line);border-radius:5px;height:9px;overflow:hidden">'
            '<i style="display:block;height:100%%;width:%d%%;background:%s"></i></div>'
            '<span class="small mono" style="min-width:58px;text-align:right">%s</span></div>'
            % (pct, color, _esc(label)))

import re as _re
def _slug_metric(name):
    return _re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")

# ---- source channels & the canonical fusion scenarios -------------------------------
_CH = {                                                       # id -> (label, chip-class, note)
 "L":  ("biomarker · lab", "b-green", "highest accuracy · low frequency"),
 "Wc": ("wearable · clinical-grade", "b-green", "≈ lab · may drive red (D22)"),
 "Ww": ("wearable · consumer", "b-acc", "trajectory / early-warning · Watch-capped"),
 "Wi": ("wearable · inferential", "b-yellow", "informational only (D22)"),
 "S":  ("self-report · PRO", "b-yellow", "subjective · screening / context"),
}
_PILLAR_SRC = {                                               # pillar -> {channel: [signals]}
 "CV":  {"L": ["ApoB", "LDL-C", "Lp(a)", "clinic BP"], "Wc": ["validated BP cuff", "single-lead ECG"],
         "Ww": ["resting HR", "HRV"], "S": ["smoking PRO", "activity recall"]},
 "MET": {"L": ["HbA1c", "fasting glucose", "fasting insulin"], "Wc": ["CGM"],
         "Ww": ["weight (smart scale)"], "S": ["diet-quality", "IPAQ-SF"]},
 "REN": {"L": ["eGFR (creat/cystatin)", "UACR", "K⁺"], "Wc": ["validated BP cuff"],
         "S": ["NSAID-use PRO", "symptom PRO"]},
 "HEP": {"L": ["ALT/AST", "FIB-4", "GGT"], "S": ["AUDIT-C", "metabolic PRO"]},
 "INF": {"L": ["hs-CRP", "IL-6", "ferritin", "WBC"], "Ww": ["skin temp", "resting HR (illness)"],
         "S": ["symptom / illness PRO"]},
 "HEM": {"L": ["Hgb", "ferritin", "B12/folate"], "Wc": ["overnight SpO₂ (validated)"],
         "Ww": ["SpO₂ (spot)", "respiratory rate"], "S": ["fatigue / dyspnea PRO"]},
 "ENDO":{"L": ["TSH", "E2/FSH/LH/T", "cortisol"], "Ww": ["skin temp (cycle)", "HRV"],
         "S": ["PSS-4", "vasomotor PRO"]},
 "BCM": {"L": ["DEXA BMD", "ALMI"], "Wc": ["grip dynamometer"],
         "Ww": ["body-comp (smart scale)", "gait speed"], "S": ["falls PRO", "function PRO"]},
 "NUT": {"L": ["vit-D", "B12", "ferritin", "omega-3 index"], "S": ["diet-quality screener"]},
 "SLP": {"Wc": ["PSG (clinic)"], "Ww": ["sleep duration / efficiency / regularity", "overnight SpO₂"],
         "S": ["ISI", "sleep-hygiene PRO"]},
 "FIT": {"L": ["VO₂max (CPET)"], "Ww": ["steps", "VO₂max est.", "HR-recovery"],
         "Wi": ["readiness / recovery"], "S": ["IPAQ-SF"]},
 "MCS": {"Ww": ["HRV", "sleep"], "Wi": ["stress score"],
         "S": ["PHQ-9", "GAD-7", "UCLA-3", "WHO-5", "PSS-4"]},
}
# canonical scenarios: id, required-channel predicate, name, value rule, confidence, dominant, guardrail
_SCN = [
 ("Lonly",  lambda c: "L" in c,
  "Lab only", "take the lab value as the level", "high", "lab", "—"),
 ("Wconly", lambda c: "Wc" in c,
  "Clinical-wearable only", "take the device value (CGM / cuff / ECG)", "high", "wearable-clinical", "D22: may drive red"),
 ("Wwonly", lambda c: "Ww" in c and "L" not in c and "Wc" not in c,
  "Consumer-wearable only", "trend vs personal baseline; level provisional", "medium", "wearable-consumer",
  "D22: Watch/Advisory cap — no red without lab/clinical confirm"),
 ("Sonly",  lambda c: "S" in c and "L" not in c and "Wc" not in c and "Ww" not in c,
  "Self-report only", "screening estimate; flag low coverage", "low–med", "self-report",
  "never a band alone; prompts measurement"),
 ("LWw",    lambda c: "L" in c and "Ww" in c,
  "Lab + consumer-wearable", "lab anchors the level; wearable supplies trajectory + early-warning", "high",
  "lab (level) + wearable (trend)", "—"),
 ("WcWw",   lambda c: "Wc" in c and "Ww" in c,
  "Clinical + consumer wearable", "clinical anchors; consumer corroborates with denser sampling", "high",
  "wearable-clinical", "—"),
 ("LS",     lambda c: "L" in c and "S" in c,
  "Lab + self-report", "lab dominates the level; PRO adds adherence / symptom context", "high", "lab", "—"),
 ("ALL",    lambda c: ("L" in c or "Wc" in c) and "Ww" in c and "S" in c,
  "All sources present", "precision-weighted: lab/clinical level + wearable trajectory + PRO context", "very high",
  "lab/clinical (level)", "—"),
 ("CONF",   lambda c: ("L" in c or "Wc" in c) and ("Ww" in c or "Wi" in c),
  "Conflict (consumer red, lab green)", "trust hierarchy — lab/clinical wins; consumer anomaly → reconfirm, raise Watch only",
  "high", "lab/clinical", "Doc 05 §3.4 · D22 — no false red"),
]
_CONF_COL = {"high": "b-green", "very high": "b-green", "medium": "b-acc", "low–med": "b-yellow"}

# ---- wearable cadence × provider (illustrative design targets) ----------------------
_PROVIDERS = ["Terra", "Apple HealthKit", "Android Health Connect", "Samsung Health",
              "Fitbit", "Garmin", "Oura", "Whoop"]
_CADENCE = {
 # metric: [Terra, HealthKit, HealthConnect, Samsung, Fitbit, Garmin, Oura, Whoop]
 "Resting HR":      ["daily + intraday*", "per-sample 5–10 min + daily", "per-sample + daily", "intraday", "1-min intraday (API)", "intraday 15 min", "nightly + daytime", "continuous → cycle"],
 "HRV (RMSSD)":     ["nightly + samples", "beat-to-beat (SDNN) overnight", "HRV samples", "intraday", "nightly", "nightly + stress", "nightly", "continuous → cycle"],
 "Sleep stages":    ["per-session", "per-session (asleep/stages)", "per-session", "per-session", "per-session", "per-session", "per-session (gold-ish)", "per-cycle"],
 "Overnight SpO₂":  ["nightly avg/min", "per-sample (on-demand)", "per-sample", "spot/nightly", "nightly est.", "Pulse Ox nightly", "nightly avg/min", "nightly"],
 "Steps":           ["daily + intraday", "per-sample + daily", "per-sample + daily", "intraday", "15-min intraday", "intraday", "daily activity", "—"],
 "Skin temp":       ["nightly deviation", "wrist temp nightly", "nightly", "nightly", "nightly deviation", "—", "nightly deviation", "nightly"],
 "ECG / rhythm":    ["event (on-demand)", "single-lead on-demand", "—", "on-demand", "on-demand", "on-demand", "—", "—"],
 "VO₂max (est.)":   ["on-update", "cardio-fitness periodic", "—", "periodic", "periodic", "per-activity", "—", "—"],
 "Readiness/Stress":["daily score", "—", "—", "stress intraday", "daily readiness", "Body Battery intraday", "readiness daily", "recovery daily"],
 "Respiratory rate":["nightly", "nightly (sleep)", "nightly", "nightly", "nightly", "nightly", "nightly", "nightly"],
}
# ---- freshness SLA per metric (the contract F8 asked for: expected cadence · fresh-within · stale-after · drives) ----
_CADENCE_SLA = {
 "Resting HR":       ("daily", "24 h", "48 h", "stale-wearable state; RHR-trend Confidence"),
 "HRV (RMSSD)":      ("nightly", "24 h", "72 h", "autonomic/stress companion freshness"),
 "Sleep stages":     ("per night", "24 h", "48 h", "SLP pillar; sleep-debt reservoir"),
 "Overnight SpO₂":   ("per night (screen)", "7 d", "14 d", "OSA screen; HEM"),
 "Steps":            ("daily", "24 h", "48 h", "FIT activity; adherence corroboration"),
 "Skin temp":        ("nightly", "24 h", "72 h", "illness / cycle early-warning"),
 "ECG / rhythm":     ("on event", "last event", "event-based (no decay)", "AFib check — confirm vs 12-lead"),
 "VO₂max (est.)":    ("periodic", "30 d", "90 d", "FIT trend (informational, D22)"),
 "Readiness/Stress": ("daily", "24 h", "72 h", "informational companion only (D22)"),
 "Respiratory rate": ("nightly", "24 h", "72 h", "INF / SLP early-warning"),
}

# ---- metric calculation reference (individual / aggregated / derived / baseline) ----
_CALCS = {
 "HRV (RMSSD)": ("Single overnight RMSSD from beat-to-beat RR intervals during stable sleep; ms; reject motion/arrhythmia windows.",
                 "Nightly value = median of clean 5-min windows; report a 7-day EWMA to damp night-to-night noise.",
                 "RMSSD = √(mean(ΔRR²)); ln-transform for scoring; autonomic-load companion blends with resting HR.",
                 "Personal baseline = trimmed 30-night mean ± SD; z = (x−μ)/σ feeds Stage-2b (κ·tanh(z/2), Doc 03 §2b)."),
 "Resting HR": ("Lowest stable HR during sleep/inactivity; bpm; exclude wake/motion.",
                "Daily = sleeping-HR minimum or 10th-pct; 7-day median for trend.",
                "Used raw; combined with HRV for the autonomic/stress companion (Doc 05 §4.1).",
                "30-day personal baseline; β-blocker/illness flagged as confounders (down-weighted)."),
 "Sleep efficiency": ("Per-session = time-asleep ÷ time-in-bed; %.",
                "Nightly value; 7-night mean + regularity (onset-time SD) tracked separately.",
                "Composite of stage timing; regularity = SD of mid-sleep time over the window.",
                "Personal baseline per night-type (work vs free day); ISI corroborates (Appendix C)."),
 "Overnight SpO₂": ("Per-sample reflectance SpO₂; %; consumer-tier accuracy.",
                "Nightly mean + minimum + desaturation index (ODI).",
                "ODI = desaturation events/hr; flags possible OSA → ISI / clinical PSG.",
                "Personal nightly baseline; sustained low → clinical confirmation before any red (D22)."),
 "Steps / MVPA": ("Per-epoch accelerometer counts → steps; cadence ≥100 spm ≈ moderate.",
                "Daily steps; weekly MVPA minutes (bouts ≥10 min).",
                "MVPA derived from cadence/HR zones; cross-checked vs IPAQ-SF self-report.",
                "Personal 28-day baseline; goal pacing vs target (Goals catalogue)."),
 "VO₂max (est.)": ("Not measured directly on consumer devices; inferred — inferential tier.",
                "Periodic estimate; report trend not absolute.",
                "Derived from HR–pace during runs (firmware model); CPET is the lab reference.",
                "Personal trend baseline; absolute level informational only (D22)."),
 "eGFR": ("Computed from serum creatinine (± cystatin-C), age, sex; mL/min/1.73m².",
                "Per-draw; trend across draws (slope = decline rate).",
                "CKD-EPI 2021 (race-free); cystatin-C variant when available.",
                "Personal slope vs prior draws; pregnancy/elderly frames shift expectation (Doc 08)."),
 "ApoB / non-HDL": ("Single immunoassay (ApoB) or calculated non-HDL = TC − HDL; mg/dL.",
                "Per-draw; trend across draws under therapy.",
                "ApoB preferred particle-count proxy; med-responsiveness modelled (modifiability).",
                "Personal target vs guideline (e.g. <80 high-risk); FH = low modifiability (Doc 05)."),
 "Blood pressure": ("Single cuff reading; mmHg; clinical-grade cuff = clinical tier.",
                "Average of ≥2 readings/sitting; 7-day home-BP mean (preferred).",
                "MAP, pulse-pressure derived; white-coat / masked detected vs clinic.",
                "Personal home baseline; medication context (treated-to-target, D3)."),
 "HbA1c": ("Single lab assay; % (mmol/mol); reflects ~90-day glycemia.",
                "Per-draw; trend across draws; CGM TIR cross-check when present.",
                "Estimates mean glucose; discordance with CGM flags hemoglobinopathy/turnover.",
                "Personal trajectory; pre-DM/T2D thresholds; Ramadan/anemia frames adjust."),
}
_CALC_KEYS = {_slug_metric(m) for m in _CALCS}

def _stat_chip(kind):
    return {"ok": '<span class="chip b-green">✅ closes loop</span>',
            "warn": '<span class="chip b-gold">⚠️ partial</span>',
            "gap": '<span class="chip b-red">❌ gap</span>'}[kind]

_SEVCHIP = {"P0": '<span class="chip b-red">P0</span>', "P1": '<span class="chip b-gold">P1</span>',
            "P2": '<span class="chip b-mut">P2</span>'}
_STATUS_META = {"open": ("Open", "b-red"), "in_progress": ("In progress", "b-gold"),
                "addressed": ("Addressed", "b-green")}

def _eval_auto(rule, stats):
    if not rule:
        return None
    try:
        return bool(eval(rule, {"__builtins__": {}}, stats))
    except Exception:
        return None

def build_coverage_audit():
    qb = _load("question-bank.json"); qs = qb["questions"]; qm = qb["meta"]
    pa = _load("persona-axes.json")
    log = _load("audit-log.json")
    total = len(qs)
    from collections import Counter
    pillar_q = Counter(); pillar_wear = Counter(); res_q = Counter(); axis_q = Counter(); axis_wear = Counter()
    corr_n = 0; wear_n = 0; adher_n = 0; goal_n = 0; persona_det_n = 0
    for q in qs:
        tp = set(); tr = set()
        for r in q.get("responses", []):
            for p in (r.get("pillars") or {}): tp.add(p)
            for rv in (r.get("reservoirs") or {}): tr.add(rv)
        cb = q.get("perceived_actual", {}).get("corroborated_by", [])
        if cb: corr_n += 1
        wear = _is_wear(cb)
        if wear: wear_n += 1
        for p in tp:
            pillar_q[p] += 1
            if wear: pillar_wear[p] += 1
        for rv in tr: res_q[rv] += 1
        for a in q.get("dimensions", {}).get("axis_tags", []):
            axis_q[a] += 1
            if wear: axis_wear[a] += 1
        cat = (q.get("category", "") + " " + q.get("type", "")).lower()
        if "adher" in cat: adher_n += 1
        if q.get("goal") or "goal" in cat: goal_n += 1
        if q.get("persona_signal") or q.get("determines_persona"): persona_det_n += 1
    # closure artifacts (Appendices H/I/J) — their presence auto-closes F1/F2/F3
    def _safe_count(fname, key):
        try:
            return len(_load(fname).get(key, []))
        except Exception:
            return 0
    adher_cat = _safe_count("adherence.json", "items")
    persona_sig = _safe_count("persona-matrix.json", "signals")
    goal_struct = _safe_count("goals.json", "goals")
    try:
        _onb = _load("onboarding.json")
    except Exception:
        _onb = {}
    source_typed = sum(1 for _p in PILLARS for _m in _p[3] if marker_channel(_m[0], _m[8]))
    stats = {"total": total, "corr_n": corr_n, "wear_n": wear_n,
             "adher_n": adher_n + adher_cat,
             "goal_n": goal_n, "goal_struct": goal_struct,
             "persona_det_n": persona_det_n + persona_sig, "source_typed": source_typed,
             "onboarding_steps": len(_onb.get("steps", [])), "demographics_n": len(_onb.get("demographics", [])),
             "coldstart_items": len((_onb.get("cold_start", {}) or {}).get("bootstrap_items", [])),
             "consent_gates": len(_onb.get("consent_gating", [])),
             "wear_corr_typed": _safe_count("wearable-corroboration.json", "metrics"),
             "cadence_sla": len(_CADENCE_SLA),
             "hep_ren_symptoms": sum(1 for q in qs if q.get("id") in (
                 "Q_RENHEP_REN_SYMPTOMS", "Q_RENHEP_REN_NSAID", "Q_RENHEP_REN_URINE",
                 "Q_RENHEP_HEP_SYMPTOMS", "Q_RENHEP_HEP_RISK", "Q_RENHEP_HEP_TOXIN"))}
    pillars = qm.get("pillars", list(_PNAME))
    reservoirs = qm.get("reservoirs", sorted(res_q))
    pmax = max(pillar_q.values()) if pillar_q else 1
    rmax = max(res_q.values()) if res_q else 1
    n_clin = len(PERSONAS); n_arch = len(pa.get("archetypes", []))
    union_personas = n_clin + n_arch

    # resolve effective status for every item (auto may promote to addressed)
    items = log["items"]
    for it in items:
        eff = it.get("status", "open")
        auto = _eval_auto(it.get("auto_rule"), stats)
        if auto is True:
            eff = "addressed"
        it["_eff"] = eff
        it["_auto"] = auto
    buckets = {"open": [], "in_progress": [], "addressed": []}
    for it in items:
        buckets[it["_eff"]].append(it)

    meta = log["meta"]
    h = ['<div class="crumbs"><a href="index.html">Home</a> › Reference › Coverage audit</div>',
         '<h1>Appendix G — Questionnaire Coverage Audit <span class="small muted">(product-loop lens · cumulative)</span></h1>',
         '<p class="lead">A <b>cumulative, versioned</b> evaluation of the lifestyle intake — the validated PROs '
         '(<a class="xref" href="appendix-questions.html">Appendix C</a>), the %d-item question bank '
         '(<a class="xref" href="appendix-question-bank.html">Appendix E</a>) and the axes/archetypes '
         '(<a class="xref" href="appendix-lifestyles.html">Appendix F</a>) — judged on whether the captured data '
         '<b>closes the product loop</b>: capture → score → wearable-match → persona → goals → nudge → adherence → back. '
         'Findings are dated; each keeps its full history (open a finding’s <b>history</b> to view &amp; select older '
         'dated versions). Numbers compute live from <code>data/question-bank.json</code>.</p>' % total, ILLUS]
    # revision strip
    h.append('<div class="tagrow" style="margin:2px 0 8px">'
             + " ".join('<span class="chip b-mut">%s · %s — %s</span>' % (_esc(r["date"]), _esc(r["tag"]), _esc(r["note"]))
                        for r in meta.get("revisions", [])) + '</div>')

    # ===== status header (pinned) =====
    h.append('<div class="panel" style="position:sticky;top:54px;z-index:6">')
    h.append('<div class="tagrow" style="gap:10px;align-items:center">'
             '<b>Status</b>'
             '<span class="chip b-red">%d open</span>'
             '<span class="chip b-gold">%d in progress</span>'
             '<span class="chip b-green">%d addressed</span>'
             '<span class="small muted">· current %s</span></div>'
             % (len(buckets["open"]), len(buckets["in_progress"]), len(buckets["addressed"]), _esc(meta.get("current_version", ""))))
    # compact item chips grouped
    for key, lbl in [("open", "Open"), ("in_progress", "In progress"), ("addressed", "Addressed")]:
        if not buckets[key]:
            continue
        cls = _STATUS_META[key][1]
        bx = "☑ " if key == "addressed" else "☐ "
        chips = " ".join('<a class="chip %s" href="#%s">%s%s · %s</a>' % (cls, _esc(it["id"]), bx, _esc(it["id"]), _esc(it["title"]))
                         for it in buckets[key])
        h.append('<div class="small" style="margin-top:7px"><span class="muted">%s:</span> %s</div>' % (lbl, chips))
    h.append('</div>')

    # ===== findings register (versioned) =====
    h.append('<h2 id="findings">Findings register <span class="small muted">· dated &amp; versioned</span></h2>')
    h.append('<p class="small muted">Each finding shows its latest version. The status auto-advances to '
             '<b>Addressed</b> when its live rule is met (e.g. adherence count &gt; 0); otherwise it is set in '
             '<code>data/audit-log.json</code>. Click <b>history</b> for older dated versions.</p>')
    pop_blocks = []
    for it in items:
        latest = it["versions"][-1]
        slbl, scls = _STATUS_META[it["_eff"]]
        box = "☑" if it["_eff"] == "addressed" else "☐"   # completed items render checked + green
        autotag = ""
        if it.get("auto_rule"):
            autotag = (' <span class="chip b-green">auto ✓</span>' if it["_auto"]
                       else ' <span class="chip b-mut mono" title="auto-rule (not yet met)">auto: %s</span>' % _esc(it["auto_rule"]))
        h.append('<div class="panel" id="%s" style="margin:10px 0">' % _esc(it["id"]))
        h.append('<div class="tagrow" style="gap:8px;align-items:center;margin-bottom:6px">'
                 '%s<b class="mono">%s</b><span style="flex:1">%s</span>'
                 '<span class="chip %s">%s %s</span>'
                 '<span class="chip b-mut">stage %s</span>'
                 '<span class="chip b-mut">%s · %s</span>%s'
                 '<button class="chip b-acc" style="cursor:pointer" onclick="auditPop(\'%s\')">history (%d) ▾</button>'
                 '</div>'
                 % (_SEVCHIP[it["severity"]], _esc(it["id"]), _esc(it["title"]), scls, box, _esc(slbl),
                    _esc(it["stage"]), _esc(latest["date"]), _esc(latest["tag"]), autotag,
                    _esc(it["id"]), len(it["versions"])))
        h.append('<div class="small">%s</div>' % latest["body"])
        h.append('</div>')
        # popup source block: each version selectable
        vt = []
        for vi, v in enumerate(it["versions"]):
            vlbl, vcls = _STATUS_META.get(v["status"], ("?", "b-mut"))
            vt.append('<button class="chip %s av-tab" data-item="%s" data-v="%d" style="cursor:pointer">%s · %s · %s</button>'
                      % (vcls, _esc(it["id"]), vi, _esc(v["date"]), _esc(v["tag"]), _esc(vlbl)))
        bodies = []
        for vi, v in enumerate(it["versions"]):
            bodies.append('<div class="av-body" data-item="%s" data-v="%d" style="%s">'
                          '<div class="small muted" style="margin-bottom:6px">%s · %s · status: %s</div>%s</div>'
                          % (_esc(it["id"]), vi, "" if vi == len(it["versions"]) - 1 else "display:none",
                             _esc(v["date"]), _esc(v["tag"]), _esc(v["status"]), v["body"]))
        pop_blocks.append('<div class="popsrc" id="pop-%s"><h3 style="margin-top:0">%s · %s</h3>'
                          '<div class="tagrow" style="margin-bottom:10px">%s</div>%s</div>'
                          % (_esc(it["id"]), _esc(it["id"]), _esc(it["title"]), "".join(vt), "".join(bodies)))

    # ===== coverage analysis (v1) =====
    h.append('<h2 id="analysis">Coverage analysis <span class="small muted">· as of 2026-06-19 · v1</span></h2>')
    h.append('<div class="diagram"><div class="dt">The capture→adherence loop — and where it is open</div>'
             '<pre class="mermaid">flowchart LR\n'
             '  CAP["1 Capture<br/>%d Q · 12 domains<br/>catalogue OK · flow PARTIAL"] --> SCORE["2 Score<br/>12 pillars · 15 reservoirs<br/>OK"]\n'
             '  SCORE --> MATCH["3 Wearable-match<br/>%d/%d corroborated<br/>PARTIAL"]\n'
             '  MATCH --> PER["4 Persona determine<br/>no input→persona map<br/>GAP"]\n'
             '  PER --> GOAL["5 Goals<br/>no catalogue<br/>GAP"]\n'
             '  GOAL --> NUDGE["6 Nudge (Doc 11/12)<br/>OK"]\n'
             '  NUDGE --> ADH["7 Adherence<br/>%d check-ins<br/>GAP"]\n'
             '  ADH -.loop broken.-> CAP</pre></div>' % (total, corr_n, total, adher_n))
    h.append('<h3 id="scorecard">Loop-stage scorecard</h3>')
    h.append('<div class="tablewrap"><table><thead><tr><th>#</th><th>Loop stage</th><th>What exists</th>'
             '<th>Verdict</th><th>Where it breaks</th></tr></thead><tbody>')
    rows = [
      ("1", "Capture (multi-dimensional intake)",
       "%d questions · 12 domains · 17 axes · validated PROs for mental-health/anxiety (PHQ-9/2, GAD-7/2, PSS-4, UCLA-3, WHO-5), sleep (ISI), activity (IPAQ-SF), diet, substance" % total,
       "warn", "Question <i>catalogue</i> is broad, but the first-run <b>experience</b> is undocumented — onboarding flow, demographic capture, cold-start/progressive-profiling, consent + device-pairing (F9–F12, <a href=\"#onboarding\">matrix</a>)"),
      ("2", "Score (pillars + reservoirs)",
       "All 12 pillars and all 15 reservoirs reachable from ≥1 self-report item (signed Δrisk + reservoir inflow)",
       "ok", "Thin on HEP/REN (lab-dominated) — see source-fusion (F5)"),
      ("3", "Wearable-match (perceived ↔ actual)",
       "%d/%d questions carry <code>corroborated_by</code>; %d reference a wearable/CGM signal" % (corr_n, total, wear_n),
       "warn", "Free-text corroboration; %d uncorroborated — now partly advanced by source-tags + cadence (F4)" % (total - corr_n)),
      ("4", "Persona determination",
       "Archetypes (%d) link <i>to</i> clinical personas (%d); applicability gates questions <i>by</i> persona" % (n_arch, n_clin),
       "gap", "No <i>input→persona</i> matrix (F2); %d determination weights" % persona_det_n),
      ("5", "User goals", "Goals referenced in prose; no structured catalogue",
       "gap", "Nothing for the UserGoals stream to bind to (F3)"),
      ("6", "Nudge (Doc 11/12)", "Actions catalogue + daily top-5 engine consume pillar/reservoir Δ",
       "ok", "— well specified upstream"),
      ("7", "Adherence check-ins", "%d adherence items" % adher_n,
       "gap", "Loop never closes (F1): completed/skipped nudges don't feed reservoirs (Doc 04)"),
    ]
    for num, stage, exists, kind, brk in rows:
        h.append('<tr><td class="small mono">%s</td><td><b>%s</b></td><td class="small">%s</td>'
                 '<td>%s</td><td class="small muted">%s</td></tr>'
                 % (num, _esc(stage), exists, _stat_chip(kind), brk))
    h.append('</tbody></table></div>')

    # ----- onboarding / first-run coverage (v4 front-of-loop pass) -----
    h.append('<h3 id="onboarding">Onboarding / first-run coverage <span class="small muted">· v4 · front-of-loop pass</span></h3>')
    h.append('<p class="small muted">Stage-1 Capture rated <b>ok</b> on question <i>breadth</i>, but does any doc own the first-run <b>experience</b>? '
             'Each dimension maps to where it is defined and whether an onboarding flow ties it together. '
             '<span class="b-green">ok</span> = covered &amp; owned · <span class="b-gold">warn</span> = exists but not anchored to intake · '
             '<span class="b-red">gap</span> = undocumented.</p>')
    h.append('<div class="tablewrap"><table><thead><tr><th>Onboarding dimension</th><th>Where defined / owner</th>'
             '<th>Read</th><th>Finding</th></tr></thead><tbody>')
    ob_rows = [
      ("First-run flow &amp; sequence", "— <i>no owning doc</i> (Doc 07 lists effortless surfaces, not an intake flow)", "gap", "F9"),
      ("Demographics — sex · gender · DOB", "Doc 06 §1.1 <code>Patient</code>; drives physiology (Doc 08)", "ok", "—"),
      ("Demographics — ethnicity · locale · occupation · language · household role", "implicit in Doc 06 (trailing <code>…</code>); ethnicity used in §4.2 + Doc 18", "warn", "F10"),
      ("Lifestyle / PRO question content", "Appx C (PROs) · Appx E (bank) · Appx F (axes)", "ok", "—"),
      ("Goals capture", "Appx J · Doc 07 §7", "ok", "F3"),
      ("Persona inference from intake", "Appx I (signal→persona matrix)", "ok", "F2"),
      ("Cold-start — marker priors", "Doc 06 §4.3 (median fallback) · Doc 15 (ignition)", "ok", "—"),
      ("Cold-start — question order / progressive profiling", "— <i>undocumented</i>", "gap", "F11"),
      ("Consent / privacy in first-run", "Doc 16 owns consent; not sequenced into intake", "warn", "F12"),
      ("Device / wearable pairing in first-run", "Doc 07 §2/§4 lists devices; not sequenced into intake", "warn", "F12"),
    ]
    for dim, where, kind, find in ob_rows:
        h.append('<tr><td><b>%s</b></td><td class="small">%s</td><td>%s</td><td class="small mono">%s</td></tr>'
                 % (dim, where, _stat_chip(kind), find))
    h.append('</tbody></table></div>')

    h.append('<h3 id="pillars">Pillar coverage — self-report reach &amp; wearable corroboration</h3>')
    h.append('<div class="tablewrap"><table><thead><tr><th>Pillar</th><th>Self-report reach</th>'
             '<th>Wearable-corroborated</th><th>Read</th></tr></thead><tbody>')
    for p in sorted(pillars, key=lambda x: -pillar_q[x]):
        q = pillar_q[p]; w = pillar_wear[p]; wfrac = (w / q) if q else 0
        reach_color = "var(--green)" if q >= 0.45 * pmax else ("var(--gold)" if q >= 0.2 * pmax else "var(--red)")
        wcol = "var(--green)" if wfrac >= 0.33 else ("var(--gold)" if wfrac >= 0.12 else "var(--red)")
        read = "thin (lab-led)" if q < 0.2 * pmax else ("heard, unverified" if wfrac < 0.12 else "closed")
        h.append('<tr><td><b>%s</b> <span class="small muted">%s</span></td><td style="min-width:150px">%s</td>'
                 '<td style="min-width:150px">%s</td><td class="small muted">%s</td></tr>'
                 % (_esc(p), _esc(_PNAME.get(p, "")), _audit_bar(q / pmax, reach_color, "%d Q" % q),
                    _audit_bar(wfrac, wcol, "%d%%" % round(wfrac * 100)), _esc(read)))
    h.append('</tbody></table></div>')

    h.append('<h3 id="reservoirs">Reservoir reach (MONIAC, Doc 04)</h3>')
    h.append('<div class="tablewrap"><table><thead><tr><th>Reservoir</th><th>Questions feeding it</th></tr></thead><tbody>')
    for rv in sorted(reservoirs, key=lambda x: -res_q[x]):
        c = res_q[rv]
        col = "var(--green)" if c >= 0.45 * rmax else ("var(--gold)" if c >= 0.15 * rmax else "var(--red)")
        h.append('<tr><td class="small mono">%s</td><td style="min-width:220px">%s</td></tr>'
                 % (_esc(rv.replace("_", " ")), _audit_bar(c / rmax, col, "%d" % c)))
    h.append('</tbody></table></div>')

    h.append('<h3 id="dimensions">Dimensional coverage — captured <i>and</i> matched?</h3>')
    h.append('<div class="tablewrap"><table><thead><tr><th>Axis</th><th>Questions</th>'
             '<th>Wearable-matched?</th></tr></thead><tbody>')
    for a in sorted(axis_q, key=lambda x: -axis_q[x]):
        wm = axis_wear[a]
        chip = ('<span class="chip b-green">yes (%d)</span>' % wm) if wm else '<span class="chip b-red">self-report only</span>'
        h.append('<tr><td><b>%s</b></td><td class="small mono">%d</td><td>%s</td></tr>' % (_esc(a), axis_q[a], chip))
    h.append('</tbody></table></div>')

    # ===== source tagging + fusion scenarios (v2) =====
    h.append('<h2 id="fusion">Per-pillar source tagging &amp; fusion scenarios <span class="small muted">· added 2026-06-19 · v2</span></h2>')
    h.append('<p class="small muted">Each pillar’s inputs are tagged by <b>channel</b>; where ≥2 channels cover the '
             'same construct, the value is fused by <b>accuracy</b>. Channel legend:</p>')
    h.append('<div class="tagrow" style="margin-bottom:8px">'
             + " ".join('<span class="chip %s">%s — %s</span>' % (c[1], _esc(c[0]), _esc(c[2])) for c in _CH.values())
             + '</div>')
    h.append('<div class="callout note"><div class="ct">Now typed (F7)</div>Every one of the <b>%d</b> markers now '
             'carries a <b>typed source channel</b> — <code>marker_channel()</code> resolves each to one of '
             '{biomarker-lab · wearable-clinical · wearable-consumer · wearable-inferential · self-report · derived}, '
             'shown as the <b>Channel</b> column in <a class="xref" href="appendix-biomarkers.html">Appendix A</a> '
             '(filterable on the <a class="xref" href="appendix-biomarkers.html#spreadsheet">grid</a>). The fusion rules below are '
             'therefore <b>programmatically enforceable</b>, not just illustrative.</div>'
             % source_typed)
    # master scenario rule table
    h.append('<h3 id="fusion-rules">Fusion rules (master) — accuracy hierarchy</h3>')
    h.append('<div class="tablewrap"><table><thead><tr><th>Scenario</th><th>Value rule</th><th>Confidence</th>'
             '<th>Dominant source</th><th>Guardrail</th></tr></thead><tbody>')
    for _id, _pred, name, rule, conf, dom, guard in _SCN:
        h.append('<tr><td class="small"><b>%s</b></td><td class="small">%s</td>'
                 '<td><span class="chip %s">%s</span></td><td class="small">%s</td>'
                 '<td class="small muted">%s</td></tr>'
                 % (_esc(name), _esc(rule), _CONF_COL.get(conf, "b-mut"), _esc(conf), _esc(dom), _esc(guard)))
    h.append('</tbody></table></div>')
    # per-pillar: channels present + reachable scenarios
    h.append('<h3 id="fusion-pillars">Per-pillar channels &amp; reachable scenarios</h3>')
    for p in pillars:
        src = _PILLAR_SRC.get(p, {})
        present = set(src.keys())
        chips = " ".join('<span class="chip %s">%s: %s</span>' % (_CH[ch][1], ch, _esc(", ".join(src[ch])))
                         for ch in ["L", "Wc", "Ww", "Wi", "S"] if ch in src)
        reach = [s for s in _SCN if s[1](present)]
        h.append('<div class="panel" style="margin:8px 0"><div style="margin-bottom:6px"><b>%s</b> '
                 '<span class="small muted">%s</span></div>' % (_esc(p), _esc(_PNAME.get(p, ""))))
        h.append('<div class="tagrow" style="margin-bottom:7px">%s</div>' % chips)
        h.append('<div class="tablewrap"><table><thead><tr><th>Reachable scenario</th><th>Dominant</th>'
                 '<th>Confidence</th></tr></thead><tbody>')
        for _id, _pred, name, rule, conf, dom, guard in reach:
            h.append('<tr><td class="small">%s</td><td class="small muted">%s</td>'
                     '<td><span class="chip %s">%s</span></td></tr>'
                     % (_esc(name), _esc(dom), _CONF_COL.get(conf, "b-mut"), _esc(conf)))
        h.append('</tbody></table></div></div>')

    # ===== wearable cadence × provider (v2) =====
    h.append('<h2 id="cadence">Wearable cadence × provider <span class="small muted">· added 2026-06-19 · v2 · illustrative</span></h2>')
    h.append('<div class="callout note"><div class="ct">Illustrative</div>Typical sampling / ingest cadence per '
             'metric and source. <b>Design targets, re-verify per SDK/device version</b> — Terra is an aggregator; '
             'HealthKit / Health Connect / Samsung Health are OS platforms; the rest are device brands. '
             '<code>*</code> = intraday depends on the source device. Metric calc behind each row → '
             '<a href="#calcs">calculation reference</a>.</div>')
    h.append('<div class="tablewrap"><table><thead><tr><th>Metric</th>'
             + "".join('<th class="small">%s</th>' % _esc(pr) for pr in _PROVIDERS)
             + '</tr></thead><tbody>')
    for met, cells in _CADENCE.items():
        link = ('<a href="#" onclick="auditPop(\'calc-%s\');return false">%s</a>' % (_slug_metric(met), _esc(met))
                if _slug_metric(met) in _CALC_KEYS else _esc(met))
        h.append('<tr><td class="small"><b>%s</b></td>%s</tr>'
                 % (link, "".join('<td class="small mono muted">%s</td>' % _esc(c) for c in cells)))
    h.append('</tbody></table></div>')
    # freshness SLA (closes F8)
    h.append('<h3 id="cadence-sla">Freshness SLA <span class="small muted">· per-metric contract (closes F8)</span></h3>')
    h.append('<div class="callout note"><div class="ct">Staleness contract</div>The per-metric freshness contract the '
             'cadence trace was missing: <b>fresh-within</b> (data newer than this is trusted at full weight), '
             '<b>stale-after</b> (older → the <a class="xref" href="states.html">Wearables → stale</a> state fires and '
             'Confidence decays, Doc 05 §4). Provider determines <i>how</i> a metric is delivered (matrix above); the '
             'SLA window is the <i>clinical</i> staleness bound, mostly metric-driven.</div>')
    h.append('<div class="tablewrap"><table><thead><tr><th>Metric</th><th>Expected cadence</th>'
             '<th>Fresh within</th><th>Stale after</th><th>Drives</th></tr></thead><tbody>')
    for met, (exp, fresh, stale, drives) in _CADENCE_SLA.items():
        h.append('<tr><td class="small"><b>%s</b></td><td class="small">%s</td><td class="mono small" style="color:var(--green)">%s</td>'
                 '<td class="mono small" style="color:var(--gold)">%s</td><td class="small muted">%s</td></tr>'
                 % (_esc(met), _esc(exp), _esc(fresh), _esc(stale), _esc(drives)))
    h.append('</tbody></table></div>')

    # ===== metric calculation reference (popups) =====
    h.append('<h2 id="calcs">Metric calculation reference <span class="small muted">· added 2026-06-19 · v2</span></h2>')
    h.append('<p class="small muted">Click a metric to open its calculation across four levels — '
             '<b>individual</b> reading, <b>aggregated</b> rollup, <b>derived</b> formula/composite, and '
             '<b>baseline</b> (personal-baseline method, Doc 03 §2b).</p>')
    h.append('<div class="tagrow">')
    for met in _CALCS:
        h.append('<button class="chip b-acc" style="cursor:pointer" onclick="auditPop(\'calc-%s\')">%s ▾</button>'
                 % (_slug_metric(met), _esc(met)))
    h.append('</div>')
    for met, (ind, agg, der, base) in _CALCS.items():
        pop_blocks.append('<div class="popsrc" id="pop-calc-%s"><h3 style="margin-top:0">%s — calculation</h3>'
                          '<table><tbody>'
                          '<tr><td class="small muted" style="width:96px">Individual</td><td class="small">%s</td></tr>'
                          '<tr><td class="small muted">Aggregated</td><td class="small">%s</td></tr>'
                          '<tr><td class="small muted">Derived</td><td class="small">%s</td></tr>'
                          '<tr><td class="small muted">Baseline</td><td class="small">%s</td></tr>'
                          '</tbody></table></div>'
                          % (_slug_metric(met), _esc(met), _esc(ind), _esc(agg), _esc(der), _esc(base)))

    # ===== comprehensive persona set =====
    h.append('<h2 id="personas">Comprehensive persona set <span class="small muted">· determination target (F6)</span></h2>')
    h.append('<p class="small muted">The canonical union the persona matrix resolves to — %d clinical personas '
             '(<a class="xref" href="appendix-personas.html">Appendix D</a>) × %d lifestyle archetypes '
             '(<a class="xref" href="appendix-lifestyles.html">Appendix F</a>) = up to %d frames before dedup.</p>'
             % (n_clin, n_arch, union_personas))
    h.append('<div class="tagrow" style="gap:6px">')
    for pid, pname, *rest in PERSONAS:
        h.append('<span class="chip b-teal">%s</span>' % _esc(pname))
    for a in pa.get("archetypes", []):
        h.append('<span class="chip b-mut">%s</span>' % _esc(a.get("name", a.get("key", ""))))
    h.append('</div>')

    # ===== spec sketches =====
    h.append('<h2 id="sketches">Spec sketches (ready-to-build)</h2>')
    h.append('<h3 id="sk-adherence">Adherence check-in micro-instrument</h3>')
    h.append('<pre class="code"><code>{\n'
             '  "ref": "ADH-014", "nudge_id": "NUD-sleep-winddown",\n'
             '  "stem": "Did you do tonight’s wind-down?", "cadence": "daily",\n'
             '  "responses": [\n'
             '    {"label":"Yes",      "adherence":1.0, "reservoirs":{"sleep_debt":-0.4}},\n'
             '    {"label":"Partly",   "adherence":0.5, "reservoirs":{"sleep_debt":-0.2}},\n'
             '    {"label":"No — too busy", "adherence":0.0, "reason":"time"},\n'
             '    {"label":"No — forgot",   "adherence":0.0, "reason":"salience"}\n'
             '  ],\n'
             '  "feeds": ["reservoir_inflow(Doc04)","engagement(Doc12)","feasibility(Doc07)"],\n'
             '  "corroborated_by": ["wearable: sleep onset time","HRV overnight"]\n}</code></pre>')
    h.append('<h3 id="sk-goals">User-goals catalogue (keyed by applicability)</h3>')
    h.append('<div class="tablewrap"><table><thead><tr><th>goal_id</th><th>Title</th><th>Pillar / axis</th>'
             '<th>Target metric → goal</th><th>Applies to (persona · life-stage · age · sex · condition)</th>'
             '<th>Modifiability</th></tr></thead><tbody>')
    for gid, t, pa_, tgt, app, mod in [
      ("GL-MET-01", "Lower fasting glucose", "MET / metabolic", "FPG 112 → &lt;100 mg/dL @12wk (CGM TIR↑)",
       "prediabetic, southasian, ramadan_dm · adult · 35–65 · any · pre-DM/T2D", "high"),
      ("GL-FIT-03", "Build aerobic base", "FIT / activity", "VO₂max p35 → p55 @16wk (wearable est.)",
       "desk_sedentary, weekend_warrior · adult · 18–60 · any · —", "high"),
      ("GL-SLP-02", "Stabilize sleep timing", "SLP / sleep", "Onset SD 95→&lt;45 min @8wk (wearable)",
       "shift_worker, traveler, new_parent · any · any · any · insomnia", "high"),
      ("GL-BCM-04", "Preserve bone &amp; muscle", "BCM / anthropometric", "Grip+ALMI↑; BMD hold @26wk (DEXA)",
       "menopause, frail_elderly · peri/post-meno, 60+ · F-led · osteopenia", "moderate"),
      ("GL-CV-07", "Hit ApoB target (med-led)", "CV / metabolic", "ApoB 124 → &lt;80 mg/dL (statin+diet)",
       "fh, southasian · adult · any · any · FH / high-Lp(a)", "low (genetic) → clinician"),
      ("GL-MCS-05", "Reduce perceived stress", "MCS / stress", "PSS-4 11 → &lt;6; HRV↑ @8wk",
       "stress_eater, caregiver_burnout · adult · any · any · —", "high")]:
        h.append('<tr><td class="small mono">%s</td><td class="small"><b>%s</b></td><td class="small">%s</td>'
                 '<td class="small">%s</td><td class="small muted">%s</td><td class="small">%s</td></tr>'
                 % (gid, _esc(t), _esc(pa_), tgt, _esc(app), _esc(mod)))
    h.append('</tbody></table></div>')
    h.append('<h3 id="sk-persona">Input→persona determination matrix (many-to-many)</h3>')
    sig_rows = [
      ("AUDIT-C ≥ 8", {"social_drinker_pro": "+3", "corporate_stress_eater": "+1"}),
      ("IPAQ high + steps &gt;10k/d", {"endurance_athlete": "+3", "weekend_warrior": "+1", "desk_sedentary": "−3"}),
      ("Fasting + on SGLT2i/metformin", {"ramadan_faster_t2d": "+3", "prediabetic_unaware": "+1"}),
      ("South-Asian + waist↑ + FH-Hx", {"south_asian_metabolic": "+3", "fh_genetic_risk": "+2"}),
      ("Night-shift ≥3×/wk + onset SD↑", {"shift_worker": "+3", "anxious_insomniac": "+1"}),
      ("Peri/post-meno + BMD↓", {"postmenopausal_bone": "+3", "frail_elderly": "+1"}),
      ("PSS-4↑ + caregiving hrs↑", {"caregiver_burnout": "+3", "corporate_stress_eater": "+1"}),
    ]
    cols = ["endurance_athlete", "desk_sedentary", "shift_worker", "south_asian_metabolic",
            "ramadan_faster_t2d", "postmenopausal_bone", "caregiver_burnout", "social_drinker_pro"]
    h.append('<div class="tablewrap"><table><thead><tr><th>Signal \\ persona</th>%s</tr></thead><tbody>'
             % "".join('<th class="small mono" style="writing-mode:vertical-rl;transform:rotate(180deg);white-space:nowrap">%s</th>' % _esc(c) for c in cols))
    for sig, weights in sig_rows:
        cells = ""
        for c in cols:
            v = weights.get(c, "")
            cls = "" if not v else ("color:var(--green)" if v.startswith("+") else "color:var(--red)")
            cells += '<td class="small mono" style="text-align:center;%s">%s</td>' % (cls, _esc(v))
        h.append('<tr><td class="small"><b>%s</b></td>%s</tr>' % (_esc(sig), cells))
    h.append('</tbody></table></div>')
    h.append('<p class="small muted">Persona posterior = softmax over column sums; sex/age/life-stage act as hard '
             'priors (zeroing impossible columns) before soft evidence is summed.</p>')

    # ===== popup modal + sources + JS =====
    h.append('<div id="amodal" class="amodal"><div class="amodal-card">'
             '<button class="amodal-x" onclick="auditClose()">×</button>'
             '<div id="amodal-body"></div></div></div>')
    h.append('<div style="display:none">' + "".join(pop_blocks) + '</div>')
    h.append("""<style>
      .amodal{display:none;position:fixed;inset:0;z-index:50;background:rgba(4,7,14,.72);
        align-items:flex-start;justify-content:center;padding:6vh 16px;overflow:auto}
      .amodal.on{display:flex}
      .amodal-card{position:relative;max-width:680px;width:100%;background:var(--panel,#10182a);
        border:1px solid var(--line,#26314c);border-radius:14px;padding:20px 22px;box-shadow:0 20px 60px rgba(0,0,0,.5)}
      .amodal-x{position:absolute;top:10px;right:12px;background:transparent;border:0;color:var(--mut);
        font-size:24px;line-height:1;cursor:pointer}
      .av-tab{opacity:.6}.av-tab.on{opacity:1;outline:1px solid var(--acc)}
    </style>
    <script>(function(){
      function $(s,r){return (r||document).querySelector(s);}
      window.auditPop=function(id){
        var src=document.getElementById('pop-'+id); if(!src)return;
        var body=document.getElementById('amodal-body'); body.innerHTML=src.innerHTML;
        var tabs=[].slice.call(body.querySelectorAll('.av-tab'));
        var bodies=[].slice.call(body.querySelectorAll('.av-body'));
        function show(v){bodies.forEach(function(b){b.style.display=(b.dataset.v===v)?'':'none';});
          tabs.forEach(function(t){t.classList.toggle('on',t.dataset.v===v);});}
        tabs.forEach(function(t){t.classList.toggle('on',t.dataset.v===String(tabs.length-1));
          t.addEventListener('click',function(){show(t.dataset.v);});});
        document.getElementById('amodal').classList.add('on');
      };
      window.auditClose=function(){document.getElementById('amodal').classList.remove('on');};
      document.getElementById('amodal').addEventListener('click',function(e){if(e.target.id==='amodal')auditClose();});
      document.addEventListener('keydown',function(e){if(e.key==='Escape')auditClose();});
    })();</script>""")

    h.append('<p class="small muted" style="margin-top:18px">Verdict: capture and scoring are strong and genuinely '
             'multi-dimensional; the loop opens at <b>persona determination</b>, <b>goals</b> and <b>adherence</b> '
             '(F1–F3). Source-fusion and cadence (v2) make the wearable-match stage auditable; closing the three open '
             'P0/P1 items turns a rich intake into a learning loop.</p>')
    return "Appendix G · Coverage audit", "".join(h)


# =================================================================== APPENDIX H — adherence (closes F1)
def _res_delta_chips(d):
    if not d:
        return '<span class="small muted">—</span>'
    out = []
    for k, v in d.items():
        good = (v < 0) if ("reserve" not in k) else (v > 0)
        cls = "b-green" if good else "b-red"
        out.append('<span class="chip %s mono">%s %+.2f</span>' % (cls, _esc(k.replace("_", " ")), v))
    return " ".join(out)

def build_adherence():
    d = _load("adherence.json"); m = d["meta"]; items = d["items"]
    h = ['<div class="crumbs"><a href="index.html">Home</a> › Acting on it › Adherence</div>',
         '<h1>Appendix H — Adherence Micro Check-ins <span class="small muted">· closes F1</span></h1>',
         '<p class="lead">%s</p>' % _esc(m["idea"]), ILLUS]
    h.append('<div class="diagram"><div class="dt">How an adherence answer closes the loop</div>'
             '<pre class="mermaid">flowchart LR\n'
             '  NUD["nudge (Doc 11/12)"] --> ASK["micro check-in (this appendix)"]\n'
             '  ASK --> R["response · adherence ∈ [0,1] + reason"]\n'
             '  R --> RES["reservoir inflow (Doc 04)"]\n  R --> ENG["engagement / Trajectory (Doc 05)"]\n'
             '  R --> FEAS["nudge feasibility (Doc 11 §3)"] --> NUD</pre></div>')
    # gating / timing / source model (how we avoid asking irrelevant questions)
    h.append('<div class="callout spec"><div class="ct">Activation — only relevant check-ins fire</div>%s</div>' % _esc(m.get("activation_model", "")))
    h.append('<div class="callout note"><div class="ct">Timing — captured at onboarding</div>%s</div>' % _esc(m.get("timing_model", "")))
    h.append('<div class="callout note"><div class="ct">Patient360 (EHR/EMR graph)</div>%s</div>' % _esc(m.get("patient360", "")))
    h.append('<div class="callout note"><div class="ct">Scoring</div>%s</div>' % _esc(m["scoring"]))
    h.append('<div class="tagrow" style="margin:8px 0"><span class="small muted">reason taxonomy:</span> '
             + " ".join('<span class="chip b-mut">%s</span>' % _esc(r) for r in m["reason_taxonomy"]) + '</div>')
    h.append('<p class="small muted">%s</p>' % _esc(m.get("coding", "")))

    by_type = {}
    for it in items:
        by_type.setdefault(it.get("type", "lifestyle"), []).append(it)
    n_life = len(by_type.get("lifestyle", [])); n_med = len(by_type.get("medication", [])); n_dis = len(by_type.get("disease-management", []))
    h.append('<p class="small muted">%d check-ins · %d lifestyle · %d medication (EHR-triggered) · %d disease-management. '
             'Source: <code>data/adherence.json</code>.</p>' % (len(items), n_life, n_med, n_dis))

    def _codes(trig):
        out = []
        if trig.get("atc"): out.append('<span class="chip b-mut mono">ATC %s</span>' % _esc(", ".join(trig["atc"])))
        if trig.get("icd10"): out.append('<span class="chip b-mut mono">ICD-10 %s</span>' % _esc(", ".join(trig["icd10"])))
        return "".join(out)

    def _applies(ap):
        bits = []
        if ap.get("conditions"): bits.append("conditions: " + ", ".join(ap["conditions"]))
        if ap.get("med_classes"): bits.append("meds: " + ", ".join(ap["med_classes"]))
        if ap.get("personas"): bits.append("personas: " + ", ".join(ap["personas"]))
        if ap.get("age_range"): bits.append("age %s–%s" % (ap["age_range"][0], ap["age_range"][1]))
        if ap.get("sex") and ap["sex"] != "any": bits.append("sex: " + ap["sex"])
        if ap.get("life_stage") and ap["life_stage"] != "any": bits.append("life-stage: " + ap["life_stage"])
        return " · ".join(bits)

    GROUPS = [
        ("lifestyle", "Lifestyle &amp; behavioural", "Gated by the active nudge / goal (persona-applicable) — fired on the nudge's cadence."),
        ("medication", "Medication adherence — EHR-triggered (Patient360)", "Auto-activated by a coded <code>ehr_trigger</code> from the EHR med list; the onboarding screener is the fallback when no record exists. We never ask what the chart already tells us."),
        ("disease-management", "Disease-management routines", "Condition-triggered self-management (monitoring, technique, device use)."),
    ]
    for tkey, tlabel, tdesc in GROUPS:
        group = by_type.get(tkey, [])
        if not group:
            continue
        h.append('<h2 id="grp-%s">%s <span class="small muted">· %d</span></h2>' % (tkey, tlabel, len(group)))
        h.append('<p class="small muted">%s</p>' % tdesc)
        for it in group:
            h.append('<div class="panel" id="%s" style="margin:10px 0">' % _esc(it["ref"]))
            uae = ' <span class="chip b-gold">UAE priority</span>' if it.get("uae_priority") else ""
            h.append('<h3 style="margin:0"><span class="mono" style="color:var(--acc)">%s</span> · %s%s</h3>'
                     % (_esc(it["ref"]), _esc(it["nudge_family"]), uae))
            chips = ['<span class="chip b-acc mono">%s</span>' % _esc(p) for p in it.get("pillars", [])]
            chips.append('<span class="chip b-mut">reservoir: %s</span>' % _esc(it.get("reservoir") or "—"))
            if it.get("cadence"):
                chips.append('<span class="chip b-mut">cadence: %s</span>' % _esc(it["cadence"]))
            h.append('<div class="tagrow" style="margin:6px 0">%s</div>' % "".join(chips))
            act = it.get("activation")
            if act:
                trig = act.get("ehr_trigger", {})
                lbls = ", ".join(trig.get("labels", []))
                h.append('<p class="small"><b>Activates when</b> Patient360 shows <i>%s</i> &nbsp;%s</p>' % (_esc(lbls), _codes(trig)))
                if act.get("onboarding_fallback"):
                    h.append('<p class="small muted">↳ no-EHR fallback: %s</p>' % _esc(act["onboarding_fallback"]))
            tm = it.get("timing")
            if tm:
                seg = ["anchor: %s" % tm.get("anchor", "—")]
                if tm.get("schedule_from_onboarding"): seg.append("onboarding capture: " + tm["schedule_from_onboarding"])
                if tm.get("window"): seg.append("fire window: " + tm["window"])
                if tm.get("ema"): seg.append("EMA")
                h.append('<p class="small"><b>Timing</b> · %s</p>' % _esc(" · ".join(seg)))
            ap = it.get("applicability")
            if ap and _applies(ap):
                h.append('<p class="small muted">Applies to — %s</p>' % _esc(_applies(ap)))
            h.append('<p class="small"><b>Q:</b> <i>%s</i></p>' % _esc(it["stem"]))
            h.append('<div class="tablewrap"><table><thead><tr><th>Response</th><th>Adherence</th>'
                     '<th>Reservoir Δ</th><th>Reason</th></tr></thead><tbody>')
            for r in it["responses"]:
                flag = ' <span class="chip b-red">⚑ clinician</span>' if r.get("flag") == "clinician" else ""
                h.append('<tr><td class="small"><b>%s</b>%s</td><td class="mono small">%.2f</td>'
                         '<td>%s</td><td class="small muted">%s</td></tr>'
                         % (_esc(r["label"]), flag, r.get("adherence", 0), _res_delta_chips(r.get("reservoirs")),
                            _esc(r.get("reason", "—"))))
            h.append('</tbody></table></div>')
            if it.get("corroborated_by"):
                h.append('<p class="small muted">Corroborated by: %s</p>' % _esc(", ".join(it["corroborated_by"])))
            h.append('</div>')
    return "Appendix H · Adherence", "".join(h)

# =================================================================== APPENDIX I — persona matrix (closes F2)
def build_persona_matrix():
    d = _load("persona-matrix.json"); m = d["meta"]; personas = d["personas"]; signals = d["signals"]
    pname = {p["id"]: p["name"] for p in personas}
    _nclin = sum(1 for p in personas if p.get("type") == "clinical"); _narch = len(personas) - _nclin
    h = ['<div class="crumbs"><a href="index.html">Home</a> › Questions &amp; intake › Persona determination</div>',
         '<h1>Appendix I — Input → Persona Determination Matrix <span class="small muted">· closes F2</span></h1>',
         '<p class="lead">The matrix that lets the engine <b>infer</b> a persona from the answers, not just gate '
         'questions by one. %d personas (%d clinical + %d lifestyle archetypes) × %d signals.</p>'
         % (len(personas), _nclin, _narch, len(signals)), ILLUS]
    h.append('<div class="callout spec"><div class="ct">Method</div>%s</div>' % _esc(m["method"]))
    h.append('<div class="diagram"><div class="dt">Answers → persona posterior</div>'
             '<pre class="mermaid">flowchart LR\n'
             '  A["answers + derived flags + wearable/lab thresholds"] --> S["matched signals"]\n'
             '  P["hard priors: sex · age · life-stage · requires"] --> Z["zero impossible columns"]\n'
             '  S --> SUM["Σ signed weights per persona"] --> Z --> SM["softmax → posterior"]\n'
             '  SM --> FRAME["persona frame (Doc 08/05) · cohort · nudge tilt"]</pre></div>')
    h.append('<div class="callout note"><div class="ct">Worked example</div>A 52-y male, South-Asian, waist above '
             'the Asian cut-point, HbA1c 6.1%, &lt;5k steps/day → signals <code>south_asian_ancestry</code> (+3 '
             'south_asian_metabolic), <code>hba1c_pre</code> (+3 prediabetic), <code>waist_high_asian_cut</code> '
             '(+2), <code>sedentary_job_low_steps</code> (+3 desk_sedentary). Posterior peaks on '
             '<b>south_asian_metabolic / prediabetic</b>; pregnancy &amp; menopause columns are zeroed by sex/age priors.</div>')
    srccls = {"biomarker-lab": "b-green", "wearable-clinical": "b-green", "wearable-consumer": "b-acc",
              "self-report": "b-yellow", "derived": "b-mut"}
    h.append('<h2 id="signals">Signals → persona weights</h2>')
    h.append('<div class="tablewrap"><table><thead><tr><th>Signal</th><th>Source</th>'
             '<th>Persona evidence (signed weight)</th></tr></thead><tbody>')
    for s in signals:
        ws = sorted(s["weights"].items(), key=lambda kv: -kv[1])
        chips = " ".join('<span class="chip %s">%s %+d</span>'
                         % ("b-green" if v > 0 else "b-red", _esc(pname.get(k, k)), v) for k, v in ws)
        h.append('<tr><td class="small"><b>%s</b><br><span class="small muted mono">%s</span></td>'
                 '<td><span class="chip %s">%s</span></td><td>%s</td></tr>'
                 % (_esc(s["label"]), _esc(s["id"]), srccls.get(s.get("source"), "b-mut"),
                    _esc(s.get("source", "")), chips))
    h.append('</tbody></table></div>')
    h.append('<h2 id="personas">Persona index &amp; hard priors</h2>')
    h.append('<div class="tablewrap"><table><thead><tr><th>Persona</th><th>Type</th><th>Hard priors</th>'
             '<th>Top determining signals</th></tr></thead><tbody>')
    sig_for = {}
    for s in signals:
        for k, v in s["weights"].items():
            sig_for.setdefault(k, []).append((v, s["label"]))
    for p in personas:
        pr = p.get("priors", {})
        prtxt = ", ".join("%s: %s" % (k, v) for k, v in pr.items()) if pr else "—"
        tops = sorted(sig_for.get(p["id"], []), reverse=True)[:3]
        topt = ", ".join("%s (+%d)" % (lbl, w) for w, lbl in tops) if tops else "—"
        tcls = "b-teal" if p["type"] == "clinical" else "b-mut"
        h.append('<tr><td><b>%s</b> <span class="small muted mono">%s</span></td>'
                 '<td><span class="chip %s">%s</span></td><td class="small muted">%s</td>'
                 '<td class="small">%s</td></tr>'
                 % (_esc(p["name"]), _esc(p["id"]), tcls, _esc(p["type"]), _esc(prtxt), _esc(topt)))
    h.append('</tbody></table></div>')
    h.append('<p class="small muted">Source: <code>data/persona-matrix.json</code>. Targets reconcile the clinical '
             'personas (<a class="xref" href="appendix-personas.html">Appendix D</a>) and lifestyle archetypes '
             '(<a class="xref" href="appendix-lifestyles.html">Appendix F</a>) — the comprehensive set from '
             '<a class="xref" href="appendix-coverage-audit.html">Appendix G</a> (F6).</p>')
    return "Appendix I · Persona matrix", "".join(h)

# =================================================================== APPENDIX J — goals (closes F3)
def build_goals():
    d = _load("goals.json"); m = d["meta"]; goals = d["goals"]
    h = ['<div class="crumbs"><a href="index.html">Home</a> › Acting on it › Goals</div>',
         '<h1>Appendix J — User-Goals Catalogue <span class="small muted">· closes F3</span></h1>',
         '<p class="lead">%s</p>' % _esc(m["idea"]), ILLUS]
    h.append('<div class="tagrow" style="margin:6px 0"><span class="small muted">lifecycle:</span> '
             + " → ".join('<span class="chip b-mut">%s</span>' % _esc(s) for s in m["lifecycle"]) + '</div>')
    h.append('<p class="small muted">%d goals · ≥2 per pillar · each keyed by an applicability vector and a '
             'wearable/lab/PRO target. Source: <code>data/goals.json</code>. Drives the UserGoals lifecycle on the '
             '<a class="xref" href="states.html">state machine</a>; nudges from '
             '<a class="xref" href="appendix-adherence.html">Appendix H</a> / Doc 12.</p>' % len(goals))
    by_p = {}
    for g in goals:
        by_p.setdefault(g["pillar"], []).append(g)
    for pid in ["CV", "MET", "REN", "HEP", "INF", "HEM", "ENDO", "BCM", "NUT", "SLP", "FIT", "MCS"]:
        gs = by_p.get(pid, [])
        if not gs:
            continue
        h.append('<h2 id="%s">%s <span class="small muted">· %s · %d goals</span></h2>'
                 % (_esc(pid), _esc(_PNAME.get(pid, pid)), _esc(pid), len(gs)))
        h.append('<div class="tablewrap"><table><thead><tr><th>Goal</th><th>Target metric</th>'
                 '<th>Applies to</th><th>Modifiability</th><th>Nudges</th></tr></thead><tbody>')
        for g in gs:
            met = g["metric"]
            tgt = '%s → <b>%s</b> <span class="small muted">@%dwk · %s</span>' % (
                _esc(met.get("baseline_eg", "")), _esc(met.get("target", "")), met.get("horizon_wk", 0),
                _esc(met.get("source", "")))
            ap = g["applicability"]
            who = []
            if ap.get("personas"): who.append("personas: " + ", ".join(ap["personas"]))
            if ap.get("archetypes"): who.append("archetypes: " + ", ".join(ap["archetypes"]))
            seg = "age %s–%s · %s · %s" % (ap.get("age_range", [0, 120])[0], ap.get("age_range", [0, 120])[1],
                                            ap.get("sex", "any"), ap.get("life_stage", "any"))
            cond = (" · conditions: " + ", ".join(ap["conditions"])) if ap.get("conditions") else ""
            nud = " ".join('<span class="chip b-acc mono">%s</span>' % _esc(n) for n in g.get("linked_nudges", []))
            h.append('<tr id="%s"><td class="small"><span class="mono" style="color:var(--acc)">%s</span> <b>%s</b>'
                     '<br><span class="small muted">%s</span></td>'
                     '<td class="small">%s</td><td class="small muted">%s<br>%s%s</td>'
                     '<td class="small">%s</td><td>%s</td></tr>'
                     % (_esc(g["id"]), _esc(g["id"]), _esc(g["title"]), _esc(g.get("axis", "")), tgt,
                        _esc("; ".join(who)), _esc(seg), _esc(cond), _esc(g.get("modifiability", "")), nud))
        h.append('</tbody></table></div>')
    return "Appendix J · Goals", "".join(h)

# =================================================================== BEHEMOTH — uber class diagram (data-driven)
# Subject areas: (key, label, fill, stroke). Colour each area so subsystems are visible.
_BM_AREAS = [
 ("core",       "Core",                     "#5b21b6", "#a78bfa"),
 ("intake",     "Data & Streams",           "#1d4ed8", "#93c5fd"),
 ("markers",    "Markers & Bands",          "#0e7490", "#67e8f9"),
 ("scoring",    "Scoring Engine",           "#4338ca", "#a5b4fc"),
 ("context",    "Context (2.0)",            "#7e22ce", "#d8b4fe"),
 ("reservoir",  "Reservoirs",               "#15803d", "#86efac"),
 ("sexacute",   "Sex / Acute / Life-stage", "#b45309", "#fcd34d"),
 ("nudge",      "Nudges & Actions",         "#a16207", "#fde68a"),
 ("governance", "Governance & Validation",  "#b91c1c", "#fca5a5"),
 ("questions",  "Question Bank",            "#0891b2", "#22d3ee"),
 ("persona",    "Personas & Lifestyles",    "#be185d", "#f9a8d4"),
 ("care",       "Care Pathways & Delivery", "#0f766e", "#5eead4"),
 ("infra",      "Tooling / Calc / UI",      "#475569", "#cbd5e1"),
]
_BM_AREA_NODE = {"core":"Core","intake":"DataStreams","markers":"Markers","scoring":"ScoringEngine",
 "context":"Context","reservoir":"Reservoirs","sexacute":"SexAcute","nudge":"Nudges",
 "governance":"Governance","questions":"Questions","persona":"Personas","care":"CarePathways","infra":"Tooling"}

# (name, area, stereotype, [bare members])
_BM_CLASSES = [
 ("Patient","core","«core»",["id","age","sex","gender","ethnicity"]),
 ("InputStream","intake","«Doc01»",["flag","q_source"]),
 ("LabStream","intake","«LAB»",[]),
 ("WearableStream","intake","«WEAR»",[]),
 ("GoalStream","intake","«GOAL»",[]),
 ("LifeStream","intake","«LIFE»",[]),
 ("Measurement","intake","«Doc01»",["x_i","source","confidence","tier","timestamp"]),
 ("TrustTier","intake","«D22»",["tier","q_source"]),
 ("WearableMetric","intake","«AppxB»",["metric","layer","pillars","devices"]),
 ("Marker","markers","«Doc02»",["name","unit","tier","twoSided","w_i","critical","escalation","r_clin"]),
 ("Band","markers","«Doc02»",["L","U","Ly","Uy","Lr","Ur","optimum"]),
 ("Modifier","markers","«Doc02»",["name","effect"]),
 ("CohortStat","markers","«Doc01»",["q_i","g","phi"]),
 ("PersonalBaseline","markers","«Doc03_2b»",["mu","sigma","z_i","k_i","kappa_resp"]),
 ("ClinicalScore","markers","«Doc08»",["FINDRISC","ASCVD","KDIGO","FIB4","FRAX","PhenoAge"]),
 ("Pillar","scoring","«Doc03»",["code","R_mark","R_k","S_k","status","cov","W_k","gamma"]),
 ("ScoringConstants","scoring","«Doc03»",["phi","kappa_resp","gamma","delta","rho_k","R_crit","cap"]),
 ("PureScore","scoring","«Doc03_12»",["R_total","value","overall_status","delta","progressToBest"]),
 ("CriticalCascade","scoring","«Doc03_4»",["criticalMarkers","R_crit","cap40","escalation"]),
 ("Explanation","scoring","«Doc03_7»",["bindingConstraint","topContributors","weights"]),
 ("CompanionVector","context","«Doc12_4»",["Confidence","DataSufficiency","Criticality","Trajectory","EarlyWarning","Representativeness","Skew","Volatility","Modifiability","StressLoad"]),
 ("EarlyWarning","context","«Doc12_5»",["tier","D_M","syndromic","timeToThreshold"]),
 ("ManagedState","context","«Doc12_3»",["drugTargets","tag"]),
 ("ConfounderRule","context","«Doc12_3»",["confounds","c_conf"]),
 ("AgeFrame","context","«Doc12_3»",["ageBand","anchorRetained"]),
 ("ReservoirSystem","reservoir","«Doc04»",["deltaT","stateEq"]),
 ("Reservoir","reservoir","«Doc04»",["id","polarity","B_j","setpoint","B_max","lambda","B_tilde"]),
 ("InterferenceMatrix","reservoir","«Doc04»",["kappa_jl","hurwitz"]),
 ("Valve","reservoir","«Doc04_6»",["intervention","efficacy","latency"]),
 ("HormonalMilieu","sexacute","«Doc05»",["phase","rangeShift"]),
 ("AcuteEvent","sexacute","«Doc06»",["m_acute","hysteresis"]),
 ("CarePlan","sexacute","«Doc06»",["valveBundle","lifeStage","goals"]),
 ("PatientState","sexacute","«states»",["Baseline","AcuteMode"]),
 ("NudgeEngine","nudge","«Doc07»",["top5","safetyFilter","deltaScore","rank()"]),
 ("Nudge","nudge","«Doc11_9»",["delivery_class","channel","channels_tried","status","p_hat","ack_at","escalated_at"]),
 ("NotificationService","nudge","«Doc11_9»",["channels","sendWindow","quietHours","cap","consent","phiSafePayload","deliver()","escalate()"]),
 ("Action","nudge","«Doc16»",["id","condition","impact","ease"]),
 ("Adherence","nudge","«Doc07»",["completion","feedsInflow"]),
 ("Goal","nudge","«Doc18»",["stated","m_goal"]),
 ("Cohort","governance","«Doc09»",["stratum","shrinkage","calibration","fairness","drift"]),
 ("ValidationHarness","governance","«Doc13»",["discrimination","calibration","PPV","gates"]),
 ("EvidenceRegistry","governance","«Doc14»",["entry","provenance","ignition"]),
 ("Governance","governance","«Doc11»",["escalationTiers","crisisPathway","FMEA","consent","audit"]),
 ("ModelVersion","governance","«Doc11»",["version","configHash","bump"]),
 ("ActuarialLayer","governance","«Doc10»",["gated","firewall"]),
 ("UAELocalization","governance","«Doc15»",["ethnicityCutpoints","ramadan"]),
 ("QuestionBank","questions","«AppxE»",["meta"]),
 ("Question","questions","«AppxE»",["ref","seq","order","prev","next","priority","cadence","refresh","stream","type"]),
 ("Response","questions","«AppxE»",["label","direction","magnitude","pillars","reservoirs"]),
 ("Dependency","questions","«AppxE»",["showIf","skipIf","triggers","validatedBy","prerequisites","unlocks"]),
 ("Applicability","questions","«AppxE»",["sex","ageMin","ageMax","lifeStage","personas","gated"]),
 ("QuestionDomain","questions","«AppxE»",["code"]),
 ("Category","questions","«AppxE»",["theme"]),
 ("ValidatedPRO","questions","«AppxC»",["instrument","scoring","bands","cadence"]),
 ("Persona","persona","«abstract»",[]),
 ("ClinicalPersona","persona","«AppxD»",["key","age","meds","rep","weightMult","frame"]),
 ("LifestyleArchetype","persona","«AppxF»",["key","axisProfile","weightMult"]),
 ("LifestyleAxis","persona","«AppxF»",["id","levels","pillars","perceivedSignal","actualSignal"]),
 ("AxisVector","persona","«AppxF»",["levels"]),
 ("PerceivedVsActual","persona","«AppxF»",["perceived","actual","gap","strategy","companionEffect"]),
 ("WikiGenerator","infra","«gen»",["md_to_html()","page()","sidebar()"]),
 ("WikiContent","infra","«gen»",["PILLARS","INSTRUMENTS","PERSONAS","builders()"]),
 ("WikiAdmin","infra","«gen»",["ADMIN_PAGES"]),
 ("Calculator","infra","«impl»",["computes2_0()"]),
 ("DataArtifact","infra","«data»",["questionBank","personaAxes","evidence"]),
 ("AdminBoard","infra","«ui»",["labRanges","weights","lifestyle","personas"]),
 ("InteractiveDemo","infra","«ui»",["feedbackLoop","stateMachine"]),
 ("Pathway","care","«Care09»",["condition","stratifier","persona","ieat","reconcile"]),
 ("PriorityTier","care","«Care»",["tier","criteria","owner"]),
 ("CareRole","care","«Care»",["title","scope","responsibilities","appAssist","escalatesTo"]),
 ("IEATStage","care","«Care»",["identify","engage","assess","transform"]),
 ("PreventionScore","care","«Care»",["components","riskStatus"]),
 ("CareKPI","care","«Care»",["clinical","engagement","preventive","executive"]),
]
_BM_RELATIONS = [
 ("Patient","*--","InputStream",""),("Patient","*--","PureScore",""),("Patient","*--","ReservoirSystem",""),
 ("Patient","-->","Persona",""),("Patient","-->","CarePlan",""),("Patient","-->","PatientState",""),
 ("Patient","o--","Measurement",""),
 ("InputStream","<|--","LabStream",""),("InputStream","<|--","WearableStream",""),
 ("InputStream","<|--","GoalStream",""),("InputStream","<|--","LifeStream",""),
 ("WearableStream","o--","WearableMetric",""),("WearableMetric","-->","TrustTier",""),
 ("WearableMetric","-->","Pillar",""),("Measurement","-->","TrustTier",""),
 ("Measurement","..>","Marker","feeds x_i"),("GoalStream","-->","Goal",""),("LifeStream","-->","QuestionBank",""),
 ("Marker","-->","Band",""),("Marker","-->","Modifier",""),("Marker","-->","PersonalBaseline",""),
 ("Marker","-->","CohortStat",""),("Marker","..>","ManagedState",""),("Marker","..>","ConfounderRule",""),
 ("HormonalMilieu","-->","Band",""),("AgeFrame","-->","Band",""),("UAELocalization","-->","Band",""),
 ("EvidenceRegistry","-->","Band","provenance"),
 ("Pillar","o--","Marker",""),("Pillar","-->","ScoringConstants",""),
 ("PureScore","o--","Pillar",""),("PureScore","-->","ScoringConstants",""),("PureScore","-->","CriticalCascade",""),
 ("PureScore","-->","CompanionVector",""),("PureScore","-->","Explanation",""),
 ("CriticalCascade","-->","Governance","escalates"),("CompanionVector","-->","EarlyWarning",""),
 ("CompanionVector","..>","Reservoir","StressLoad reads ALLO"),("ClinicalScore","..>","Pillar","feeds max"),
 ("PersonalBaseline","-->","CompanionVector","z drives"),("PersonalBaseline","..>","PureScore","Stage2b"),
 ("ReservoirSystem","o--","Reservoir",""),("ReservoirSystem","-->","InterferenceMatrix",""),
 ("Reservoir","-->","Pillar","B_tilde feeds"),("Reservoir","<--","Valve",""),
 ("Valve","<..","Action",""),("Valve","<..","NudgeEngine",""),
 ("AcuteEvent","-->","Pillar","m_acute"),("AcuteEvent","-->","PatientState",""),("AcuteEvent","..>","ReservoirSystem",""),
 ("CarePlan","o--","Valve",""),("CarePlan","-->","Goal",""),("Goal","-->","Pillar","m_goal"),
 ("NudgeEngine","-->","Action","top5"),("NudgeEngine","..>","PureScore","deltaScore"),
 ("NudgeEngine","-->","Nudge","emits"),("Nudge","-->","NotificationService","delivered by"),
 ("NotificationService","..>","Governance","consent + safety-critical escalation"),
 ("NotificationService","..>","Patient","channels (quiet hours · cap)"),
 ("Action","-->","Adherence",""),("Adherence","-->","Reservoir","inflow"),
 ("Cohort","-->","CohortStat",""),("Cohort","-->","PersonalBaseline","prior"),
 ("ValidationHarness","..>","ModelVersion","gates"),("Governance","o--","ModelVersion",""),
 ("ActuarialLayer","..>","PureScore","gated"),
 ("QuestionBank","o--","Question",""),("Question","o--","Response",""),("Question","-->","Dependency",""),
 ("Question","-->","Applicability",""),("Question","-->","QuestionDomain",""),("Question","-->","Category",""),
 ("Question","..>","ValidatedPRO","validatedBy"),("Response","-->","Pillar","weights"),
 ("Response","-->","Reservoir","weights"),("Applicability","-->","ClinicalPersona",""),
 ("Dependency","..>","Question","prereq"),
 ("Persona","<|--","ClinicalPersona",""),("Persona","<|--","LifestyleArchetype",""),
 ("LifestyleArchetype","-->","ClinicalPersona","links"),("ClinicalPersona","-->","AxisVector",""),
 ("LifestyleArchetype","-->","AxisVector",""),("AxisVector","o--","LifestyleAxis",""),
 ("PerceivedVsActual","-->","AxisVector",""),("PerceivedVsActual","..>","NudgeEngine","strategy"),
 ("LifestyleAxis","-->","Pillar",""),("ClinicalPersona","-->","Pillar","weightMult"),
 ("QuestionDomain","..>","Pillar","routesTo"),("Reservoir","..>","Pillar","feeds"),
 ("Category","..>","Question","groups"),
 ("WikiGenerator","..>","DataArtifact","reads"),("WikiGenerator","..>","WikiContent",""),
 ("WikiGenerator","..>","WikiAdmin",""),("WikiContent","..>","QuestionBank","renders"),
 ("WikiContent","..>","Pillar","PILLARS"),("WikiAdmin","..>","AdminBoard",""),
 ("Calculator","..>","PureScore","computes"),("Calculator","..>","CompanionVector",""),
 ("DataArtifact","..>","QuestionBank",""),("DataArtifact","..>","PerceivedVsActual",""),
 ("InteractiveDemo","..>","PureScore","demoLoop"),("AdminBoard","..>","ScoringConstants","configures"),
 ("AdminBoard","..>","Band","labRanges"),("AdminBoard","..>","ClinicalPersona",""),
 ("ClinicalPersona","-->","Pathway","anchors"),("Pathway","o--","PriorityTier","stratifies"),
 ("Pathway","-->","CareRole","routed to"),("Pathway","o--","IEATStage","lifecycle"),
 ("Pathway","-->","CarePlan","produces"),("Pathway","..>","Band","reconciles thresholds"),
 ("Pathway","..>","NudgeEngine","engage / transform"),("Pathway","-->","PreventionScore",""),
 ("PreventionScore","-->","CareKPI",""),("CareRole","..>","Action","delivers"),
 ("CareRole","..>","Governance","escalation authority"),("PriorityTier","..>","AcuteEvent","P1 escalation"),
]
_BM_EXPECT = {"reservoirs":15,"pillars":12,"domains":12,"axes":12,"clin":13,"arch":24}

def _bm_counts():
    try: m = _load("question-bank.json").get("meta", {})
    except Exception: m = {}
    try: pa = _load("persona-axes.json")
    except Exception: pa = {}
    return {"reservoirs": len(m.get("reservoirs", [])), "questions": m.get("total_questions", 0),
            "domains": len(m.get("domain_codes", [])), "pillars": len(PILLAR_W),
            "clin": len(PERSONAS), "arch": len(pa.get("archetypes", [])), "axes": len(pa.get("axes", []))}

def _bm_drift(c):
    return ["%s: baseline %d -> data now %s" % (k, v, c.get(k)) for k, v in _BM_EXPECT.items() if c.get(k) != v]

def _bm_classdefs():
    return ["classDef %s fill:%s,stroke:%s,color:#fff,stroke-width:1px;" % (k, f, s) for k, _l, f, s in _BM_AREAS]

def _bm_diagram(level, c):
    extra = {"Reservoir": ["catalogue %d" % c["reservoirs"]], "QuestionBank": ["total %d" % c["questions"]],
             "QuestionDomain": ["count %d" % c["domains"]], "Pillar": ["count %d" % c["pillars"]],
             "ClinicalPersona": ["count %d" % c["clin"]], "LifestyleArchetype": ["count %d" % c["arch"]],
             "LifestyleAxis": ["count %d" % c["axes"]]}
    L = ["classDiagram", "direction LR"] + _bm_classdefs()
    area_of = {cl[0]: cl[1] for cl in _BM_CLASSES}
    if level == "areas":
        for k, _l, _f, _s in _BM_AREAS:
            L.append("class %s" % _BM_AREA_NODE[k]); L.append('cssClass "%s" %s' % (_BM_AREA_NODE[k], k))
        seen = set()
        for s, _op, d, _lab in _BM_RELATIONS:
            a, b = area_of.get(s), area_of.get(d)
            if not a or not b or a == b or (a, b) in seen: continue
            seen.add((a, b)); L.append("%s --> %s" % (_BM_AREA_NODE[a], _BM_AREA_NODE[b]))
        return "\n".join(L)
    by_area = {}
    for name, area, stereo, members in _BM_CLASSES:
        by_area.setdefault(area, []).append(name)
        if level == "fields":
            L.append("class %s {" % name); L.append(stereo)
            for mm in members + extra.get(name, []): L.append("+%s" % mm)
            L.append("}")
        else:
            L.append("class %s" % name)
    for k, _l, _f, _s in _BM_AREAS:
        names = by_area.get(k, [])
        if names: L.append('cssClass "%s" %s' % (",".join(names), k))
    for s, op, d, lab in _BM_RELATIONS:
        L.append("%s %s %s%s" % (s, op, d, (" : " + lab) if lab else ""))
    return "\n".join(L)

_BM_JS = """<style>
#bm-frame:fullscreen{height:100vh!important;width:100vw!important;border-radius:0}
.bm-tool button{cursor:pointer;border:1px solid var(--line,#334);background:transparent;color:inherit;border-radius:7px;padding:4px 9px}
.bm-tool button.on{background:var(--acc,#6cf);color:#04121f;border-color:var(--acc,#6cf);font-weight:600}
#bm-legend span[data-area]{cursor:pointer;border-radius:7px;padding:3px 9px;font-size:12px}
</style>
<script type="module">
import mermaid from "https://cdn.jsdelivr.net/npm/mermaid@11/dist/mermaid.esm.min.mjs";
mermaid.initialize({startOnLoad:false,theme:"dark",securityLevel:"loose",flowchart:{htmlLabels:true,curve:"basis"},themeVariables:{fontSize:"13px"}});
(function(){
 var frame=document.getElementById("bm-frame"),stage=document.getElementById("bm-stage"),hud=document.getElementById("bm-hud");
 if(!frame||!stage)return;
 var srcs={fields:document.getElementById("bm-src-fields").textContent,
           names:document.getElementById("bm-src-names").textContent,
           areas:document.getElementById("bm-src-areas").textContent};
 var level=localStorage.getItem("bmLevel")||"fields",z=1,tx=0,ty=0,area=null,n=0,natW=0,natH=0;
 function clamp(v,a,b){return Math.max(a,Math.min(b,v));}
 function apply(){stage.style.transform="translate("+tx+"px,"+ty+"px) scale("+z+")";if(hud)hud.textContent=Math.round(z*100)+"%";}
 var AREAS=window.BM_AREA_COLORS||{};
 function nodeArea(g){for(var k in AREAS){if(g.classList.contains(k))return k;}return null;}
 function colorize(){var s=stage.querySelector("svg");if(!s)return;
   s.querySelectorAll("g.node").forEach(function(g){var k=nodeArea(g);if(!k)return;var col=AREAS[k];
     g.querySelectorAll("path,rect,polygon").forEach(function(sh){
       var f=sh.getAttribute("fill")||getComputedStyle(sh).fill;
       if(f&&f!=="none"&&f!=="rgba(0, 0, 0, 0)"){sh.style.fill=col;}});
     g.querySelectorAll("text,tspan,.nodeLabel").forEach(function(t){t.style.fill="#fff";});});}
 function wireNav(){if(level==="areas")return;var s=stage.querySelector("svg");if(!s)return;
   s.querySelectorAll("g.node").forEach(function(g){var m=(g.id||"").match(/classId-(.+?)-\\d+$/);if(!m)return;
     g.style.cursor="pointer";g.setAttribute("title","double-click → "+m[1]+" dossier");});}
 function nodeName(t){var g=t&&t.closest&&t.closest("g.node");if(!g)return null;var m=(g.id||"").match(/classId-(.+?)-\\d+$/);return m?m[1]:null;}
 frame.addEventListener("dblclick",function(e){if(level==="areas")return;var nm=nodeName(e.target);if(nm){e.preventDefault();location.href="class-model.html#"+nm;}});
 function highlight(){var s=stage.querySelector("svg");if(!s)return;
   s.querySelectorAll("g.node").forEach(function(g){g.style.opacity=(!area||g.classList.contains(area))?"1":"0.18";});}
 function fit(){if(!natW||!natH)return;var fw=frame.clientWidth-24,fh=frame.clientHeight-24;
   z=clamp(Math.min(fw/natW,(fh-60)/natH),0.05,4);tx=Math.max(8,(frame.clientWidth-natW*z)/2);ty=66;apply();}
 function setActive(){["fields","names","areas"].forEach(function(k){var b=document.getElementById("bm-"+k);if(b)b.classList.toggle("on",k===level);});}
 function render(){mermaid.render("bmG"+(++n),srcs[level]).then(function(r){stage.innerHTML=r.svg;
     var s=stage.querySelector("svg");
     if(s){var vb=(s.viewBox&&s.viewBox.baseVal)?s.viewBox.baseVal:null;
       if(vb&&vb.width){natW=vb.width;natH=vb.height;}
       else{try{var bb=s.getBBox();natW=bb.width;natH=bb.height;}catch(e){natW=1400;natH=900;}}
       s.setAttribute("width",natW);s.setAttribute("height",natH);
       s.style.maxWidth="none";s.style.width=natW+"px";s.style.height=natH+"px";}
     setActive();fit();colorize();wireNav();highlight();}).catch(function(e){stage.innerHTML='<pre style="color:#ff8a8a;padding:14px;white-space:pre-wrap">'+(e&&e.message||e)+'</pre>';});}
 ["fields","names","areas"].forEach(function(k){var b=document.getElementById("bm-"+k);if(b)b.addEventListener("click",function(){level=k;localStorage.setItem("bmLevel",k);render();});});
 function zoomAt(cx,cy,f){var nz=clamp(z*f,0.08,6);tx=cx-((cx-tx)/z)*nz;ty=cy-((cy-ty)/z)*nz;z=nz;apply();}
 var byId=function(i){return document.getElementById(i);};
 if(byId("bm-zin"))byId("bm-zin").addEventListener("click",function(){zoomAt(frame.clientWidth/2,frame.clientHeight/2,1.2);});
 if(byId("bm-zout"))byId("bm-zout").addEventListener("click",function(){zoomAt(frame.clientWidth/2,frame.clientHeight/2,1/1.2);});
 if(byId("bm-reset"))byId("bm-reset").addEventListener("click",function(){z=1;tx=0;ty=0;apply();});
 if(byId("bm-fit"))byId("bm-fit").addEventListener("click",fit);
 if(byId("bm-full"))byId("bm-full").addEventListener("click",function(){try{if(!document.fullscreenElement){if(frame.requestFullscreen){var pr=frame.requestFullscreen();if(pr&&pr.catch)pr.catch(function(){});}}else if(document.exitFullscreen){document.exitFullscreen();}}catch(_){}});
 document.addEventListener("fullscreenchange",function(){setTimeout(fit,90);});
 frame.addEventListener("wheel",function(e){e.preventDefault();var r=frame.getBoundingClientRect();zoomAt(e.clientX-r.left,e.clientY-r.top,e.deltaY<0?1.1:1/1.1);},{passive:false});
 var gx=0,gy=0,sx=0,sy=0,pid=null,drag=false,moved=false;
 frame.addEventListener("pointerdown",function(e){if(e.target.closest&&e.target.closest("#bm-overlay"))return;drag=true;moved=false;pid=e.pointerId;sx=e.clientX;sy=e.clientY;gx=e.clientX-tx;gy=e.clientY-ty;});
 frame.addEventListener("pointermove",function(e){if(!drag)return;if(!moved&&Math.abs(e.clientX-sx)+Math.abs(e.clientY-sy)>4){moved=true;try{frame.setPointerCapture(pid);}catch(_){}frame.style.cursor="grabbing";}if(moved){tx=e.clientX-gx;ty=e.clientY-gy;apply();}});
 frame.addEventListener("pointerup",function(){drag=false;moved=false;frame.style.cursor="grab";try{frame.releasePointerCapture(pid);}catch(_){}});
 document.querySelectorAll("#bm-legend [data-area]").forEach(function(el){el.addEventListener("click",function(){var k=el.getAttribute("data-area");area=(area===k)?null:k;
   document.querySelectorAll("#bm-legend [data-area]").forEach(function(x){x.style.outline=(x===el&&area)?"2px solid #fff":"none";});highlight();});});
 render();
})();
</script>"""

def build_behemoth():
    c = _bm_counts(); warns = _bm_drift(c)
    if warns:
        try: print("  [behemoth] drift vs baseline: " + "; ".join(warns))
        except Exception: pass
    h = ['<div class="crumbs"><a href="index.html">Home</a> &rsaquo; System &amp; build &rsaquo; Class diagram (domain model)</div>',
         '<h1>Class diagram — domain model</h1>',
         '<p class="lead">A single uber class diagram mapping <b>every concept across <code>docs/</code></b> &mdash; '
         'scoring (<a class="xref" href="03-scoring-formula.html">Doc 03</a>), reservoirs '
         '(<a class="xref" href="04-moniac-reservoir-dynamics.html">Doc 04</a>), the 2.0 companion vector '
         '(<a class="xref" href="05-critical-review-and-purescore-2.0.html">Doc 05</a>), streams, sex/acute, '
         'nudges, governance, the question bank (<a class="xref" href="appendix-question-bank.html">Appx E</a>), '
         'lifestyles/personas (<a class="xref" href="appendix-lifestyles.html">Appx F</a>) and the tooling/'
         'calculator/UI. <b>%d classes</b> across <b>%d colour-coded subject areas</b>; live counts: '
         '%d reservoirs &middot; %d pillars &middot; %d question-domains &middot; %d questions &middot; %d clinical personas &middot; %d archetypes.</p>'
         % (len(_BM_CLASSES), len(_BM_AREAS), c["reservoirs"], c["pillars"], c["domains"], c["questions"], c["clin"], c["arch"])]
    toolbar = ('<div class="bm-tool" style="display:flex;gap:6px;flex-wrap:wrap;align-items:center;pointer-events:auto">'
               '<span class="small muted">detail:</span>'
               '<button id="bm-fields">Fields</button><button id="bm-names">Names</button><button id="bm-areas">Areas</button>'
               '<span style="width:10px"></span><span class="small muted">view:</span>'
               '<button id="bm-zout">&minus;</button><button id="bm-reset">reset</button><button id="bm-fit">fit</button>'
               '<button id="bm-zin">&plus;</button><button id="bm-full">&#9974; fullscreen</button>'
               '<span id="bm-hud" class="small muted mono" style="margin-left:6px">100%</span>'
               '<span class="small muted">wheel = zoom &middot; drag = pan &middot; <b>double-click a class &rarr; its dossier</b></span></div>')
    sfx = {"reservoir": " &middot;%d" % c["reservoirs"], "scoring": " &middot;%d pillars" % c["pillars"],
           "questions": " &middot;%dQ" % c["questions"], "persona": " &middot;%d+%d" % (c["clin"], c["arch"])}
    chips = ['<span data-area="%s" style="background:%s;color:#fff">%s%s</span>'
             % (k, f, _esc(lbl), sfx.get(k, "")) for k, lbl, f, s in _BM_AREAS]
    legend = ('<div id="bm-legend" class="tagrow" style="gap:6px;margin-top:6px;pointer-events:auto">'
              '<span class="small muted">areas (click to spotlight):</span>%s</div>' % "".join(chips))
    # controls live INSIDE the frame as a top overlay, so they survive fullscreen
    h.append('<div id="bm-frame" tabindex="0" style="position:relative;height:76vh;overflow:hidden;'
             'border:1px solid var(--line,#334);border-radius:10px;background:#0b0f17;cursor:grab">'
             '<div id="bm-overlay" style="position:absolute;top:0;left:0;right:0;z-index:4;padding:8px 10px;'
             'pointer-events:none;background:linear-gradient(180deg,rgba(11,15,23,.92),rgba(11,15,23,0))">%s%s</div>'
             '<div id="bm-stage" style="position:absolute;top:0;left:0;transform-origin:0 0"></div></div>'
             % (toolbar, legend))
    h.append('<p class="small muted" style="margin-top:8px">Generated from the <code>BEHEMOTH</code> spec in '
             '<code>wiki_content.py</code> (areas &middot; classes &middot; relations); the three views and colours regenerate on '
             'every <code>build_wiki.py</code> run and counts are pulled live from the data files, so the map stays '
             'in sync.</p>')
    # Illustrative + Reconciliation notes sit at the bottom of the page (below the diagram)
    h.append(ILLUS)
    h.append('<div class="callout note"><div class="ct">Reconciliations baked in</div>'
             '<b>Reservoirs</b> 15 canonical (Doc 04, incl. VBP/OXD/IRON). <b>Symbol</b> responsiveness cap = '
             '<code>kappa_resp</code> vs interference <code>kappa_jl</code>. <b>Scope</b> everything under <code>docs/</code>.</div>')
    if warns:
        h.append('<div class="callout safety"><div class="ct">Diagram drift</div>The data files moved away from the '
                 'diagram baseline &mdash; counts above auto-updated; review the spec: %s.</div>' % _esc("; ".join(warns)))
    for lv in ("fields", "names", "areas"):
        h.append('<script type="application/x-mermaid" id="bm-src-%s">%s</script>' % (lv, _bm_diagram(lv, c)))
    colors = "{" + ",".join('"%s":"%s"' % (k, f) for k, _l, f, _s in _BM_AREAS) + "}"
    h.append('<script>window.BM_AREA_COLORS=%s;</script>' % colors)
    h.append(_BM_JS)
    return "Class diagram (domain model)", "".join(h)


# =================================================================== ENGINEERING DOSSIER (per-class explorer + 5 index pages)
def _dossier():
    return _load("dossier.json")

_CE_CSS = ('<style>#ce-list a{display:block;padding:2px 7px;border-radius:6px;text-decoration:none;color:inherit}'
           '#ce-list a:hover{background:rgba(255,255,255,.06)}#ce-list a.on{background:var(--acc,#6cf);color:#04121f}'
           '#ce-main .panel{margin:10px 0}</style>')

_CE_JS = """<script>
(function(){
 var el=document.getElementById("ce-data"); if(!el)return;
 var D=JSON.parse(el.textContent);
 var seqT={}; D.sequences.forEach(function(s){seqT[s.id]=s.title;});
 var apiT={}; D.api.forEach(function(a){apiT[a.id]=a;});
 var main=document.getElementById("ce-main"),listEl=document.getElementById("ce-list"),search=document.getElementById("ce-search");
 function esc(s){return (s==null?"":String(s)).replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;");}
 function chip(t,bg){return '<span class="chip" style="background:'+bg+';color:#fff">'+esc(t)+'</span>';}
 function rel(r,dir){var o=dir==='out'?r.to:r.from;return '<a href="#'+o+'">'+esc(o)+'</a>'+(r.label?' <span class="small muted">('+esc(r.label)+')</span>':'');}
 function render(name){
  var c=D.classes[name]; if(!c){main.innerHTML='<p class="small muted">Pick a class.</p>';return;}
  var h=[];
  h.push('<div class="tagrow" style="gap:8px;align-items:center"><h2 style="margin:0">'+esc(c.name)+'</h2>'+chip(c.areaLabel,c.areaColor)+'<span class="chip b-mut">«'+esc(c.doc)+'»</span><span class="chip '+(c.tier==='core'?'b-teal':'b-mut')+'">'+c.tier+'</span></div>');
  if(c.purpose) h.push('<p class="lead" style="margin:8px 0">'+esc(c.purpose)+'</p>');
  var ql=['<a class="chip b-acc" href="class-model.html">← diagram</a>'];
  if(c.seqRefs.length) ql.push('<a class="chip b-acc" href="dossier-sequences.html">sequences ('+c.seqRefs.length+')</a>');
  if(c.apiRefs.length) ql.push('<a class="chip b-acc" href="dossier-api.html">API ('+c.apiRefs.length+')</a>');
  ql.push('<a class="chip b-acc" href="dossier-erd.html#'+name+'">data</a>');
  if(c.stories.length) ql.push('<a class="chip b-acc" href="dossier-stories.html#'+name+'">stories ('+c.stories.length+')</a>');
  ql.push('<a class="chip b-acc" href="dossier-c4.html">C4</a>');
  h.push('<div class="tagrow" style="gap:6px;margin:6px 0 14px">'+ql.join('')+'</div>');
  h.push('<div class="panel"><h3>Overview</h3>');
  if(c.fields.length) h.push('<p class="small"><b>Fields:</b> '+c.fields.map(esc).join(' · ')+'</p>');
  if(c.relationsOut.length) h.push('<p class="small"><b>Uses →</b> '+c.relationsOut.map(function(r){return rel(r,'out');}).join(', ')+'</p>');
  if(c.relationsIn.length) h.push('<p class="small"><b>← Used by</b> '+c.relationsIn.map(function(r){return rel(r,'in');}).join(', ')+'</p>');
  h.push('<p class="small muted"><b>C4:</b> '+esc(c.c4||'—')+(c.state?' &nbsp;·&nbsp; <b>State:</b> '+esc(c.state)+' (<a href="states.html">states</a>)':'')+'</p></div>');
  if(c.table){var t=c.table;h.push('<div class="panel"><h3>Data model — <span class="mono">'+esc(t.name)+'</span></h3><div class="tablewrap"><table><thead><tr><th>column</th><th>type</th><th>key</th><th>note</th></tr></thead><tbody>');
   t.columns.forEach(function(col){h.push('<tr><td class="mono small">'+esc(col.name)+'</td><td class="small">'+esc(col.type)+'</td><td class="small">'+esc(col.key||'')+'</td><td class="small muted">'+esc(col.note||'')+'</td></tr>');});
   h.push('</tbody></table></div>'+(t.indexes&&t.indexes.length?'<p class="small muted">indexes: '+t.indexes.map(esc).join(', ')+'</p>':'')+(t.notes?'<p class="small muted">'+esc(t.notes)+'</p>':'')+'</div>');}
  if(c.apiRefs.length){h.push('<div class="panel"><h3>API</h3><ul class="small">');
   c.apiRefs.forEach(function(id){var a=apiT[id];if(a)h.push('<li><a href="dossier-api.html#'+id+'"><span class="mono">'+a.method+' '+esc(a.path)+'</span></a> — '+esc(a.desc)+'</li>');});h.push('</ul></div>');}
  if(c.seqRefs.length){h.push('<div class="panel"><h3>Sequences</h3><ul class="small">');
   c.seqRefs.forEach(function(id){h.push('<li><a href="dossier-sequences.html#'+id+'">'+esc(seqT[id]||id)+'</a></li>');});h.push('</ul></div>');}
  if(c.stories.length){h.push('<div class="panel"><h3>User stories <span class="small muted">('+c.stories.length+')</span></h3>');
   c.stories.forEach(function(s){h.push('<div style="border-left:3px solid '+c.areaColor+';padding-left:10px;margin:10px 0">');
    h.push('<p class="small"><b class="mono">'+esc(s.id)+'</b> — As a <b>'+esc(s.role)+'</b>, I want '+esc(s.want)+' so that '+esc(s.soThat)+'.</p>');
    if(s.acceptance&&s.acceptance.length)h.push('<p class="small muted" style="margin:2px 0">Acceptance:</p><ul class="small">'+s.acceptance.map(function(a){return '<li>'+esc(a)+'</li>';}).join('')+'</ul>');
    if(s.edge&&s.edge.length)h.push('<p class="small" style="color:#ffb38a;margin:2px 0">Edge/failure: '+s.edge.map(esc).join(' · ')+'</p>');
    h.push('</div>');});h.push('</div>');}
  if(c.invariants.length)h.push('<div class="panel"><h3>Invariants &amp; safety</h3><ul class="small">'+c.invariants.map(function(i){return '<li>'+esc(i)+'</li>';}).join('')+'</ul></div>');
  if(c.decisions.length)h.push('<div class="panel"><h3>Decisions</h3><div class="tagrow">'+c.decisions.map(function(d){return '<a class="chip b-mut" href="decisions.html">'+esc(d)+'</a>';}).join('')+'</div></div>');
  main.innerHTML=h.join('');
  [].forEach.call(listEl.querySelectorAll('a'),function(a){a.classList.toggle('on',a.getAttribute('data-c')===name);});
 }
 var byArea={}; Object.keys(D.classes).forEach(function(k){var c=D.classes[k];(byArea[c.areaLabel]=byArea[c.areaLabel]||[]).push(c);});
 var lh=[]; Object.keys(byArea).forEach(function(al){lh.push('<div class="small muted" style="margin:8px 0 2px">'+esc(al)+'</div>');
  byArea[al].forEach(function(c){lh.push('<a data-c="'+c.name+'" href="#'+c.name+'"><span style="display:inline-block;width:8px;height:8px;border-radius:2px;background:'+c.areaColor+';margin-right:6px"></span>'+esc(c.name)+(c.tier==='core'?' <span style="color:#6cf">●</span>':'')+'</a>');});});
 listEl.innerHTML=lh.join('');
 function fromHash(){var n=decodeURIComponent((location.hash||'').replace('#',''));render(D.classes[n]?n:Object.keys(D.classes)[0]);}
 window.addEventListener('hashchange',fromHash);
 if(search)search.addEventListener('input',function(){var q=search.value.toLowerCase();[].forEach.call(listEl.querySelectorAll('a'),function(a){if(a.hasAttribute('data-c'))a.style.display=a.getAttribute('data-c').toLowerCase().indexOf(q)>-1?'block':'none';});});
 fromHash();
})();
</script>"""

def build_class_explorer():
    d = _dossier(); m = d["meta"]
    blob = json.dumps(d, ensure_ascii=False).replace("</", "<\\/")
    h = ['<div class="crumbs"><a href="index.html">Home</a> &rsaquo; Engineering &rsaquo; Class explorer</div>',
         '<h1>Class Explorer</h1>',
         '<p class="lead">The per-class engineering dossier for <b>%d</b> of the model\'s classes (<b>%d</b> fully curated; the class diagram above shows the full set): '
         'purpose, fields &amp; relations, data-model table, API, sequences, user stories, invariants and decisions. '
         '<b>Double-click any node in the <a class="xref" href="class-model.html#diagram">class diagram</a></b> '
         'to land on that class here. %d user stories &middot; %d endpoints &middot; %d sequences.</p>'
         % (m["total_classes"], m["core_classes"], sum(len(c["stories"]) for c in d["classes"].values()), m["api"], m["sequences"]),
         ILLUS, _CE_CSS,
         '<div style="display:flex;gap:14px;align-items:flex-start">'
         '<aside style="flex:0 0 230px;position:sticky;top:54px;max-height:84vh;overflow:auto">'
         '<input id="ce-search" type="search" placeholder="filter classes…" style="width:100%;padding:6px 9px;'
         'border-radius:8px;border:1px solid var(--line,#334);background:transparent;color:inherit;margin-bottom:6px">'
         '<div id="ce-list"></div></aside>'
         '<main id="ce-main" style="flex:1;min-width:0"></main></div>',
         '<script type="application/json" id="ce-data">%s</script>' % blob,
         _CE_JS]
    return "Class Explorer", "".join(h)

def build_sequences():
    d = _dossier()
    h = ['<div class="crumbs"><a href="index.html">Home</a> &rsaquo; Engineering &rsaquo; Sequences</div>',
         '<h1>Runtime Sequence Diagrams</h1>',
         '<p class="lead">The dynamic choreography behind the static model — how data and control actually flow at '
         'runtime. Each names the classes it touches (open them in the <a class="xref" href="class-model.html">Class '
         'Explorer</a>).</p>', ILLUS]
    for s in d["sequences"]:
        h.append('<div class="panel"><h3 id="%s">%s</h3>' % (_esc(s["id"]), _esc(s["title"])))
        h.append('<pre class="mermaid">%s</pre>' % s["mermaid"])
        links = ", ".join('<a href="class-model.html#%s">%s</a>' % (_esc(cn), _esc(cn)) for cn in s.get("classes", []))
        h.append('<p class="small muted">Classes: %s</p></div>' % links)
    return "Sequences", "".join(h)

def build_erd():
    d = _dossier()
    h = ['<div class="crumbs"><a href="index.html">Home</a> &rsaquo; Engineering &rsaquo; Data model</div>',
         '<h1>Data Model (ERD)</h1>',
         '<p class="lead">Production persistence sketch: one table per class (core classes hand-modelled; the rest '
         'auto-skeletoned from the spec). Time-series tables are partitioned; audit tables append-only. '
         '<span class="flag fl-life">LIFE</span>-stream and reservoir history are the main time-series stores.</p>', ILLUS]
    by_area = {}
    for cn, c in d["classes"].items(): by_area.setdefault((c["area"], c["areaLabel"], c["areaColor"]), []).append(c)
    for (ak, al, ac), cs in by_area.items():
        h.append('<h2 style="border-left:5px solid %s;padding-left:8px">%s</h2>' % (ac, _esc(al)))
        for c in sorted(cs, key=lambda x: (0 if x["tier"] == "core" else 1, x["name"])):
            t = c["table"]
            badge = '<span class="chip b-teal">core</span>' if c["tier"] == "core" else '<span class="chip b-mut">skeleton</span>'
            h.append('<div class="panel"><h3 id="%s"><span class="mono">%s</span> %s <span class="small muted">(%s)</span></h3>'
                     % (_esc(c["name"]), _esc(t["name"]), badge, _esc(c["name"])))
            h.append('<div class="tablewrap"><table><thead><tr><th>column</th><th>type</th><th>key</th><th>note</th></tr></thead><tbody>')
            for col in t["columns"]:
                h.append('<tr><td class="mono small">%s</td><td class="small">%s</td><td class="small">%s</td><td class="small muted">%s</td></tr>'
                         % (_esc(col["name"]), _esc(col["type"]), _esc(col.get("key", "")), _esc(col.get("note", ""))))
            h.append('</tbody></table></div>')
            if t.get("indexes"): h.append('<p class="small muted">indexes: %s</p>' % _esc(", ".join(t["indexes"])))
            if t.get("notes"): h.append('<p class="small muted">%s</p>' % _esc(t["notes"]))
            h.append('</div>')
    return "Data model (ERD)", "".join(h)

def build_c4():
    d = _dossier(); c4 = d["c4"]
    flow = ("flowchart LR\n"
            "  PA[\"Patient App\"] --> API[\"API Gateway\"]\n  CB[\"Clinician console\"] --> API\n"
            "  WH[\"Wearable webhooks\"] --> ING[\"Ingestion\"]\n  LF[\"Lab feed FHIR\"] --> ING\n  ING --> API\n"
            "  API --> SC[\"Scoring Engine\"]\n  API --> QS[\"Question Service\"]\n  API --> NS[\"Nudge Service\"]\n"
            "  SC --> RS[\"Reservoir Service\"]\n  SC --> REG[\"Model Registry\"]\n  SC --> DS[(\"Datastore\")]\n"
            "  RS --> DS\n  QS --> DS\n  NS --> DS\n  SC --> AUD[(\"Audit Store\")]\n"
            "  API -. consent .-> ACT[\"Actuarial firewalled\"]")
    h = ['<div class="crumbs"><a href="index.html">Home</a> &rsaquo; Engineering &rsaquo; C4 architecture</div>',
         '<h1>C4 Architecture (Container / Component)</h1>',
         '<p class="lead">How the domain model runs as a system: external actors, services (containers), their '
         'components, and the datastores. Each class maps to a component (see its Class Explorer page).</p>', ILLUS,
         '<div class="panel"><h3>Container diagram</h3><pre class="mermaid">%s</pre></div>' % flow,
         '<h2>Containers &amp; components</h2>']
    comps = c4.get("components", {})
    for ct in c4["containers"]:
        cc = comps.get(ct["id"], [])
        h.append('<div class="panel"><h3 id="%s">%s <span class="small muted">(%s)</span></h3>'
                 '<p class="small">%s</p>%s</div>'
                 % (_esc(ct["id"]), _esc(ct["name"]), _esc(ct["tech"]), _esc(ct["desc"]),
                    ('<div class="tagrow">' + "".join('<span class="chip b-mut">%s</span>' % _esc(x) for x in cc) + '</div>') if cc else ""))
    return "C4 architecture", "".join(h)

def build_api():
    d = _dossier()
    h = ['<div class="crumbs"><a href="index.html">Home</a> &rsaquo; Engineering &rsaquo; API</div>',
         '<h1>API Contracts</h1>',
         '<p class="lead">Illustrative service seams (re-verify before production): method, path, auth, idempotency, '
         'returns, errors, and the classes each endpoint touches.</p>', ILLUS,
         '<div class="tablewrap"><table><thead><tr><th>endpoint</th><th>auth</th><th>idem</th><th>summary</th></tr></thead><tbody>']
    for a in d["api"]:
        h.append('<tr><td class="small"><a href="#%s"><span class="mono">%s %s</span></a></td><td class="small">%s</td>'
                 '<td class="small">%s</td><td class="small muted">%s</td></tr>'
                 % (_esc(a["id"]), a["method"], _esc(a["path"]), _esc(a.get("auth", "")),
                    "yes" if a.get("idempotent") else "no", _esc(a["desc"])))
    h.append('</tbody></table></div>')
    for a in d["api"]:
        cls = ", ".join('<a href="class-model.html#%s">%s</a>' % (_esc(cn), _esc(cn)) for cn in a.get("classes", []))
        h.append('<div class="panel"><h3 id="%s"><span class="mono">%s %s</span></h3><p class="small">%s</p>'
                 '<p class="small muted">auth: %s &middot; idempotent: %s &middot; returns: <span class="mono">%s</span></p>'
                 '<p class="small" style="color:#ffb38a">errors: %s</p><p class="small muted">classes: %s</p></div>'
                 % (_esc(a["id"]), a["method"], _esc(a["path"]), _esc(a["desc"]), _esc(a.get("auth", "")),
                    "yes" if a.get("idempotent") else "no", _esc(a.get("returns", "")),
                    _esc(" · ".join(a.get("errors", []))), cls))
    return "API contracts", "".join(h)

def build_stories():
    d = _dossier()
    total = sum(len(c["stories"]) for c in d["classes"].values())
    h = ['<div class="crumbs"><a href="index.html">Home</a> &rsaquo; Engineering &rsaquo; User stories</div>',
         '<h1>User Stories &amp; Acceptance Criteria</h1>',
         '<p class="lead">The buildable backlog: <b>%d stories</b> across the curated core classes, with Gherkin '
         'acceptance criteria and edge/failure cases. Grouped by subject area; open any class in the '
         '<a class="xref" href="class-model.html">Class Explorer</a>.</p>' % total, ILLUS]
    by_area = {}
    for cn, c in d["classes"].items():
        if c["stories"]: by_area.setdefault((c["areaLabel"], c["areaColor"]), []).append(c)
    for (al, ac), cs in by_area.items():
        h.append('<h2 style="border-left:5px solid %s;padding-left:8px">%s</h2>' % (ac, _esc(al)))
        for c in cs:
            h.append('<div class="panel"><h3 id="%s">%s</h3>' % (_esc(c["name"]), _esc(c["name"])))
            for s in c["stories"]:
                h.append('<div style="border-left:3px solid %s;padding-left:10px;margin:10px 0">'
                         '<p class="small"><b class="mono">%s</b> &mdash; As a <b>%s</b>, I want %s so that %s.</p>'
                         % (ac, _esc(s.get("id", "")), _esc(s.get("role", "")), _esc(s.get("want", "")), _esc(s.get("soThat", ""))))
                if s.get("acceptance"):
                    h.append('<p class="small muted" style="margin:2px 0">Acceptance:</p><ul class="small">%s</ul>'
                             % "".join('<li>%s</li>' % _esc(a) for a in s["acceptance"]))
                if s.get("edge"):
                    h.append('<p class="small" style="color:#ffb38a;margin:2px 0">Edge/failure: %s</p>' % _esc(" · ".join(s["edge"])))
                h.append('</div>')
            h.append('</div>')
    return "User stories", "".join(h)


# =================================================================== SPREADSHEET GRIDS (reusable)
import re as _re_grid
def _gstrip(v): return _re_grid.sub("<[^>]+>", "", str(v)).strip()
def _gattr(v): return _esc(_gstrip(v)).replace('"', "&quot;")

_GRID_ASSETS = """<style>
/* grid pages use the full horizontal space (these overrides only ship on grid pages) */
.shell{max-width:none}
.main{max-width:none;padding-left:22px;padding-right:22px}
.dgwrap{margin:14px 0}
.dgbar{display:flex;gap:10px;align-items:center;flex-wrap:wrap;margin-bottom:8px}
.dgbar input.dgq{flex:1;min-width:200px;padding:7px 11px;border-radius:9px;border:1px solid var(--line);background:#0a1120;color:var(--ink)}
.dgbtn{cursor:pointer;border:1px solid var(--line);background:#0e1626;color:var(--ink);border-radius:8px;padding:6px 11px;font-size:12.5px;white-space:nowrap}
.dgbtn:hover{border-color:var(--acc);color:#fff}
.dgcount{font-size:12px;color:var(--mut);white-space:nowrap}
.dgwrap:fullscreen{background:var(--bg,#0a0e18);padding:16px}
/* all rows on one continuous page (no inner vertical scrollbar); the page scrolls */
.dgscroll{overflow:visible;border:1px solid var(--line);border-radius:12px}
.dgbar{position:sticky;top:53px;z-index:6;background:var(--bg,#0a0e18);padding:8px 0}
table.dgrid{border-collapse:separate;border-spacing:0;width:100%;font-size:12.5px}
table.dgrid th,table.dgrid td{padding:7px 10px;border-bottom:1px solid var(--line);text-align:left;vertical-align:top}
table.dgrid td{max-width:380px}
/* sticky headers stick to the page, just below the top bar (≈53px) + the filter bar */
table.dgrid thead th{position:sticky;top:100px;background:#0e1626;z-index:2;cursor:pointer;user-select:none;white-space:nowrap}
table.dgrid thead th:hover{background:#16223a}
table.dgrid thead tr.dgf th{position:sticky;top:134px;background:#0b1322;z-index:1;cursor:auto;padding:5px 7px}
/* fullscreen: the wrap is its own scroll context — pin headers to the top of it */
.dgwrap:fullscreen .dgscroll{overflow:auto;max-height:calc(100vh - 120px)}
.dgwrap:fullscreen .dgbar{top:0}
.dgwrap:fullscreen table.dgrid thead th{top:0}
.dgwrap:fullscreen table.dgrid thead tr.dgf th{top:34px}
table.dgrid thead tr.dgf input,table.dgrid thead tr.dgf select{width:100%;box-sizing:border-box;padding:4px 6px;border-radius:6px;border:1px solid var(--line);background:#0a1120;color:var(--ink);font-size:11.5px}
table.dgrid th .ar{opacity:.35;font-size:10px;margin-left:5px}
table.dgrid th.asc .ar,table.dgrid th.desc .ar{opacity:1;color:var(--acc)}
table.dgrid tbody tr:hover{background:#11192b}
table.dgrid td.num{text-align:right;font-variant-numeric:tabular-nums;white-space:nowrap}
</style>
<script>
(function(){
 function txt(el){var v=el.getAttribute("data-v");return (v!==null?v:el.textContent).trim();}
 document.querySelectorAll("table.dgrid").forEach(function(tbl){
   var gid=tbl.id, heads=[].slice.call(tbl.tHead.rows[0].cells);
   var fcells=tbl.tHead.rows[1]?[].slice.call(tbl.tHead.rows[1].cells):[];
   var body=tbl.tBodies[0], rows=[].slice.call(body.rows);
   var q=document.getElementById(gid+"-q"), count=document.getElementById(gid+"-count");
   var sortCol=-1,sortDir=0;
   function apply(){
     var gv=(q&&q.value||"").toLowerCase();
     var fv=fcells.map(function(c){var i=c.querySelector("input,select");return i?i.value.toLowerCase():"";});
     var shown=0;
     rows.forEach(function(r){
       var cells=r.cells, ok=true, i;
       if(gv){ ok=false; for(i=0;i<cells.length;i++){ if(txt(cells[i]).toLowerCase().indexOf(gv)>-1){ok=true;break;} } }
       if(ok){ for(var j=0;j<fv.length;j++){ if(fv[j]){ var cv=txt(cells[j]).toLowerCase(), sel=fcells[j].querySelector("select");
           if(sel){ if(cv!==fv[j]){ok=false;break;} } else if(cv.indexOf(fv[j])<0){ok=false;break;} } } }
       r.style.display=ok?"":"none"; if(ok)shown++;
     });
     if(count)count.textContent=shown+" of "+rows.length+" rows";
   }
   function sortBy(ci){
     var th=heads[ci], num=th.getAttribute("data-type")==="num";
     if(sortCol===ci){ sortDir = sortDir===1?-1:(sortDir===-1?0:1); } else { sortCol=ci; sortDir=1; }
     heads.forEach(function(h){h.classList.remove("asc","desc");var a=h.querySelector(".ar");if(a)a.textContent="↕";});
     if(sortDir!==0){ th.classList.add(sortDir===1?"asc":"desc"); var a=th.querySelector(".ar"); if(a)a.textContent=sortDir===1?"▲":"▼";
       var arr=rows.slice();
       arr.sort(function(a,b){ var x=txt(a.cells[ci]),y=txt(b.cells[ci]);
         if(num){ x=parseFloat(x.replace(/[^0-9.\\-]/g,""))||0; y=parseFloat(y.replace(/[^0-9.\\-]/g,""))||0; return (x-y)*sortDir; }
         return x.localeCompare(y,undefined,{numeric:true})*sortDir; });
       arr.forEach(function(r){body.appendChild(r);});
     } else { rows.forEach(function(r){body.appendChild(r);}); }
   }
   heads.forEach(function(th,ci){ th.addEventListener("click",function(){sortBy(ci);}); });
   fcells.forEach(function(c){ var i=c.querySelector("input,select"); if(i){i.addEventListener("input",apply);i.addEventListener("change",apply);} });
   if(q)q.addEventListener("input",apply);
   // download the current (filtered + sorted) view as an Excel-openable .xls
   function xesc(s){return String(s).replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;");}
   function exportXls(){
     var th="<tr>"; heads.forEach(function(h){th+="<th>"+xesc(h.textContent.replace(/[↕▲▼]/g,"").trim())+"</th>";}); th+="</tr>";
     var tb=""; [].slice.call(body.rows).forEach(function(r){ if(r.style.display==="none")return;
       var tr="<tr>"; for(var i=0;i<r.cells.length;i++){tr+="<td>"+xesc(txt(r.cells[i]))+"</td>";} tb+=tr+"</tr>"; });
     var doc='<html xmlns:x="urn:schemas-microsoft-com:office:excel"><head><meta charset="utf-8">'
       +'<style>table{border-collapse:collapse}td,th{border:1px solid #ccc;padding:3px 6px;mso-number-format:"\\\\@"}th{background:#eee;font-weight:bold}</style>'
       +'</head><body><table>'+th+tb+'</table></body></html>';
     var blob=new Blob(["\\ufeff"+doc],{type:"application/vnd.ms-excel"});
     var a=document.createElement("a"); a.href=URL.createObjectURL(blob); a.download=gid+".xls";
     document.body.appendChild(a); a.click(); setTimeout(function(){URL.revokeObjectURL(a.href);a.remove();},0);
   }
   var xb=document.getElementById(gid+"-xls"); if(xb)xb.addEventListener("click",exportXls);
   var fb=document.getElementById(gid+"-full"), wrap=tbl.closest(".dgwrap");
   if(fb&&wrap)fb.addEventListener("click",function(){ try{ if(!document.fullscreenElement){ if(wrap.requestFullscreen){var p=wrap.requestFullscreen();if(p&&p.catch)p.catch(function(){});} } else if(document.exitFullscreen){document.exitFullscreen();} }catch(_){}});
   apply();
 });
})();
</script>"""

def _grid(gid, columns, rows):
    for col in columns:
        if "filter" not in col:
            if col.get("type") == "num":
                col["filter"] = "text"
            else:
                vals = set(_gstrip(r.get(col["key"], "")) for r in rows)
                col["filter"] = "select" if (1 < len(vals) <= 16 and max((len(x) for x in vals), default=0) <= 26) else "text"
    h = ['<div class="dgwrap"><div class="dgbar">'
         '<input class="dgq" id="%s-q" type="search" placeholder="search all columns…">'
         '<span class="dgcount" id="%s-count"></span>'
         '<button class="dgbtn" id="%s-xls" type="button">&#11015; XLS</button>'
         '<button class="dgbtn" id="%s-full" type="button">&#9974; Fullscreen</button></div>'
         '<div class="dgscroll"><table class="dgrid" id="%s"><thead><tr>' % (gid, gid, gid, gid, gid)]
    for col in columns:
        h.append('<th data-type="%s">%s<span class="ar">↕</span></th>' % (col.get("type", "text"), _esc(col["label"])))
    h.append('</tr><tr class="dgf">')
    for col in columns:
        if col["filter"] == "select":
            vals = sorted(set(_gstrip(r.get(col["key"], "")) for r in rows if _gstrip(r.get(col["key"], "")) != ""))
            opts = '<option value="">all</option>' + "".join('<option value="%s">%s</option>' % (_gattr(v).lower(), _esc(v)) for v in vals)
            h.append('<th><select>%s</select></th>' % opts)
        elif col["filter"] == "none":
            h.append('<th></th>')
        else:
            h.append('<th><input type="text" placeholder="filter…"></th>')
    h.append('</tr></thead><tbody>')
    for r in rows:
        h.append('<tr>')
        for col in columns:
            val = r.get(col["key"], "")
            cls = ' class="num"' if col.get("type") == "num" else ''
            disp = val if (isinstance(val, str) and "<" in str(val)) else _esc(str(val))
            h.append('<td%s data-v="%s">%s</td>' % (cls, _gattr(val), disp))
        h.append('</tr>')
    h.append('</tbody></table></div></div>')
    return "".join(h)

def _grid_page(crumb, h1, lead, source_href, source_label, gridhtml, tab):
    h = ['<div class="crumbs"><a href="index.html">Home</a> › Spreadsheets › %s</div>' % _esc(crumb),
         '<h1>%s</h1>' % _esc(h1),
         '<p class="lead">%s <a class="xref" href="%s">%s ↗</a></p>' % (lead, source_href, _esc(source_label)),
         gridhtml, _GRID_ASSETS]
    return tab, "".join(h)

# ----- 1. biomarkers -----
def build_grid_biomarkers():
    rows = []
    for pid, pname, res, markers in PILLARS:
        for (mk, unit, tier, two, gr, ye, rd, w, src, crit) in markers:
            rows.append({"Pillar": pid, "Marker": mk, "Unit": unit, "Tier": tier, "Channel": marker_channel(mk, src),
                         "2-sided": "yes" if two else "—", "Green": gr, "Yellow": ye, "Red": rd,
                         "Weight": w, "Source": _cite_chip(src, mk), "Critical": "yes" if crit else "—"})
    cols = [{"key":"Pillar","label":"Pillar"},{"key":"Marker","label":"Marker","filter":"text"},{"key":"Unit","label":"Unit"},
            {"key":"Tier","label":"Tier"},{"key":"Channel","label":"Channel"},{"key":"2-sided","label":"2-sided"},
            {"key":"Green","label":"Green","filter":"text"},{"key":"Yellow","label":"Yellow","filter":"text"},
            {"key":"Red","label":"Red","filter":"text"},{"key":"Weight","label":"Weight","type":"num"},{"key":"Source","label":"Source"},
            {"key":"Critical","label":"Critical"}]
    tab, body = _grid_page("Biomarkers grid", "Biomarkers — spreadsheet", "Every marker across the 12 pillars, flat &amp; sortable. Click a Source to verify its citation. Source:",
                      "appendix-biomarkers.html", "Appendix A · Biomarkers", _grid("g-bio", cols, rows), "Biomarkers grid")
    return tab, body + _CITE_ASSETS

# ----- 2. wearables -----
def build_grid_wearables():
    rows = [{"Metric":m,"Layer":layer,"Trust tier":tier,"q_source":q,"Pillars":pil,"Devices":dev} for (m,layer,tier,q,pil,dev) in WEARABLES]
    cols = [{"key":"Metric","label":"Metric","filter":"text"},{"key":"Layer","label":"Layer"},{"key":"Trust tier","label":"Trust tier"},
            {"key":"q_source","label":"q_source"},{"key":"Pillars","label":"Pillars","filter":"text"},{"key":"Devices","label":"Devices","filter":"text"}]
    return _grid_page("Wearables grid", "Wearables — spreadsheet", "Every wearable metric, layer, trust tier &amp; pillars. Source:",
                      "appendix-wearables.html", "Appendix B · Wearables", _grid("g-wear", cols, rows), "Wearables grid")

# ----- 3. personas -----
def build_grid_personas():
    rows = [{"Id":pid,"Persona":name,"Age":age,"Meds":meds,"Coverage φ":cov,"Multipliers":mult,"Notes":notes}
            for (pid,name,age,meds,cov,mult,notes) in PERSONAS]
    cols = [{"key":"Id","label":"Id"},{"key":"Persona","label":"Persona","filter":"text"},{"key":"Age","label":"Age","type":"num"},
            {"key":"Meds","label":"Meds","filter":"text"},{"key":"Coverage φ","label":"Coverage φ","type":"num"},
            {"key":"Multipliers","label":"Pillar multipliers","filter":"text"},{"key":"Notes","label":"Notes","filter":"text"}]
    return _grid_page("Personas grid", "Clinical personas — spreadsheet", "The clinical persona frames, flat &amp; sortable. Source:",
                      "appendix-personas.html", "Appendix D · Personas", _grid("g-pers", cols, rows), "Personas grid")

# ----- 4. lifestyles (axes + archetypes) -----
def build_grid_lifestyles():
    pa = _load("persona-axes.json")
    axrows = [{"Axis":a["name"],"Id":a["id"],"Levels":" → ".join(a.get("levels",[])),"Pillars":", ".join(a.get("pillars",[])),
               "Perceived":", ".join(a.get("perceived_signal",[])),"Actual":", ".join(a.get("actual_signal",[]))} for a in pa.get("axes",[])]
    axcols = [{"key":"Axis","label":"Axis","filter":"text"},{"key":"Id","label":"Id"},{"key":"Levels","label":"Levels","filter":"text"},
              {"key":"Pillars","label":"Pillars","filter":"text"},{"key":"Perceived","label":"Perceived signal","filter":"text"},{"key":"Actual","label":"Actual signal","filter":"text"}]
    arrows = [{"Key":a["key"],"Name":a["name"],"Clinical persona":a.get("links_clinical_persona","—"),
               "Axis profile":" · ".join("%s:%s"%(k,v) for k,v in a.get("axis_profile",{}).items()),
               "Multipliers":a.get("weight_multipliers","—"),"Conditions":", ".join(a.get("typical_conditions",[])),"Frame":a.get("frame","")} for a in pa.get("archetypes",[])]
    arcols = [{"key":"Key","label":"Key"},{"key":"Name","label":"Name","filter":"text"},{"key":"Clinical persona","label":"Clinical persona"},
              {"key":"Axis profile","label":"Axis profile","filter":"text"},{"key":"Multipliers","label":"Weight multipliers","filter":"text"},
              {"key":"Conditions","label":"Typical conditions","filter":"text"},{"key":"Frame","label":"Frame","filter":"text"}]
    body = '<h2>Lifestyle axes</h2>' + _grid("g-lf-ax", axcols, axrows) + '<h2>Named archetypes</h2>' + _grid("g-lf-ar", arcols, arrows)
    return _grid_page("Lifestyles grid", "Lifestyles — spreadsheet", "Lifestyle axes and named archetypes, flat &amp; sortable. Source:",
                      "appendix-lifestyles.html", "Appendix F · Lifestyles", body, "Lifestyles grid")

# ----- 5. adherence -----
def build_grid_adherence():
    d = _load("adherence.json"); rows = []
    for it in d["items"]:
        resp = " / ".join(r.get("label","") for r in it.get("responses",[]))
        rows.append({"Ref":'<a href="appendix-adherence.html#%s">%s</a>'%(_esc(it["ref"]),_esc(it["ref"])),
                     "Nudge family":it.get("nudge_family",""),"Pillars":", ".join(it.get("pillars",[])),
                     "Reservoir":it.get("reservoir") or "—","Cadence":it.get("cadence",""),"Stem":it.get("stem",""),
                     "Responses":resp,"Corroborated by":", ".join(it.get("corroborated_by",[]))})
    cols = [{"key":"Ref","label":"Ref"},{"key":"Nudge family","label":"Nudge family","filter":"text"},{"key":"Pillars","label":"Pillars","filter":"text"},
            {"key":"Reservoir","label":"Reservoir"},{"key":"Cadence","label":"Cadence"},{"key":"Stem","label":"Stem","filter":"text"},
            {"key":"Responses","label":"Responses","filter":"text"},{"key":"Corroborated by","label":"Corroborated by","filter":"text"}]
    return _grid_page("Adherence grid", "Adherence check-ins — spreadsheet", "Every adherence micro check-in, flat &amp; sortable. Source:",
                      "appendix-adherence.html", "Appendix H · Adherence", _grid("g-adh", cols, rows), "Adherence grid")

# ----- 6. goals -----
def build_grid_goals():
    d = _load("goals.json"); rows = []
    for g in d["goals"]:
        m = g.get("metric",{}); ap = g.get("applicability",{}); ar = ap.get("age_range",[0,120])
        rows.append({"Goal":'<a href="appendix-goals.html#%s">%s</a>'%(_esc(g["id"]),_esc(g["id"])),"Title":g.get("title",""),
                     "Pillar":g.get("pillar",""),"Axis":g.get("axis",""),"Metric":m.get("name",""),
                     "Target":"%s → %s"%(m.get("baseline_eg",""),m.get("target","")),"Horizon (wk)":m.get("horizon_wk",""),"Source":m.get("source",""),
                     "Personas":", ".join(ap.get("personas",[])),"Archetypes":", ".join(ap.get("archetypes",[])),
                     "Age":"%s–%s"%(ar[0],ar[1]),"Sex":ap.get("sex","any"),"Life-stage":ap.get("life_stage","any"),
                     "Conditions":", ".join(ap.get("conditions",[])),"Modifiability":g.get("modifiability",""),"Nudges":", ".join(g.get("linked_nudges",[]))})
    cols = [{"key":"Goal","label":"Goal"},{"key":"Title","label":"Title","filter":"text"},{"key":"Pillar","label":"Pillar"},{"key":"Axis","label":"Axis"},
            {"key":"Metric","label":"Metric","filter":"text"},{"key":"Target","label":"Target","filter":"text"},{"key":"Horizon (wk)","label":"Horizon (wk)","type":"num"},
            {"key":"Source","label":"Source","filter":"text"},{"key":"Personas","label":"Personas","filter":"text"},{"key":"Archetypes","label":"Archetypes","filter":"text"},
            {"key":"Age","label":"Age"},{"key":"Sex","label":"Sex"},{"key":"Life-stage","label":"Life-stage"},{"key":"Conditions","label":"Conditions","filter":"text"},
            {"key":"Modifiability","label":"Modifiability","filter":"text"},{"key":"Nudges","label":"Nudges","filter":"text"}]
    return _grid_page("Goals grid", "User goals — spreadsheet", "Every goal keyed by applicability, flat &amp; sortable. Source:",
                      "appendix-goals.html", "Appendix J · Goals", _grid("g-goal", cols, rows), "Goals grid")

# ----- 7. persona-matrix (long: signal × persona) -----
def build_grid_persona_matrix():
    d = _load("persona-matrix.json"); pname = {p["id"]:p["name"] for p in d["personas"]}; ptype = {p["id"]:p.get("type","") for p in d["personas"]}
    rows = []
    for s in d["signals"]:
        for pid, w in s.get("weights",{}).items():
            rows.append({"Signal":s["label"],"Signal id":s["id"],"Source":s.get("source",""),
                         "Persona":pname.get(pid,pid),"Type":ptype.get(pid,""),"Weight":w})
    cols = [{"key":"Signal","label":"Signal","filter":"text"},{"key":"Signal id","label":"Signal id","filter":"text"},{"key":"Source","label":"Source"},
            {"key":"Persona","label":"Persona","filter":"text"},{"key":"Type","label":"Type"},{"key":"Weight","label":"Weight","type":"num"}]
    return _grid_page("Persona-matrix grid", "Persona determination — spreadsheet", "Every signal→persona weight in long form, sortable by weight. Source:",
                      "appendix-persona-matrix.html", "Appendix I · Persona matrix", _grid("g-pm", cols, rows), "Persona-matrix grid")

# ----- 8. question bank (one row per question) -----
def build_grid_questions():
    qb = _load("question-bank.json"); rows = []
    for q in qb["questions"]:
        ap = q.get("applicability",{}) or {}
        pil = sorted({k for r in q.get("responses",[]) for k in (r.get("pillars") or {})})
        ax = q.get("dimensions",{}).get("axis_tags",[])
        rows.append({"Ref":'<a href="appendix-question-bank.html#%s">%s</a>'%(_esc(q["ref"]),_esc(q["ref"])),
                     "Domain":q.get("domain","").replace("_"," ").title(),"Order":q.get("order",""),"Priority":q.get("priority",""),
                     "Category":q.get("category",""),"Type":q.get("type",""),"Cadence":q.get("cadence",""),
                     "Pillars":", ".join(pil),"Axes":", ".join(ax),"Sex":", ".join(ap.get("sex",["all"])),
                     "Age min":ap.get("age_min",0),"Age max":ap.get("age_max",120),"Personas":", ".join(ap.get("personas",["all"])),"Text":q.get("text","")})
    cols = [{"key":"Ref","label":"Ref"},{"key":"Domain","label":"Domain"},{"key":"Order","label":"Order","type":"num"},{"key":"Priority","label":"Priority","type":"num"},
            {"key":"Category","label":"Category","filter":"text"},{"key":"Type","label":"Type"},{"key":"Cadence","label":"Cadence"},
            {"key":"Pillars","label":"Pillars","filter":"text"},{"key":"Axes","label":"Axes","filter":"text"},{"key":"Sex","label":"Sex"},
            {"key":"Age min","label":"Age min","type":"num"},{"key":"Age max","label":"Age max","type":"num"},{"key":"Personas","label":"Personas","filter":"text"},
            {"key":"Text","label":"Question text","filter":"text"}]
    return _grid_page("Question-bank grid", "Question bank — spreadsheet", "All %d questions, one row each, sortable &amp; filterable. Source:" % len(qb["questions"]),
                      "appendix-question-bank.html", "Appendix E · Question bank", _grid("g-q", cols, rows), "Question-bank grid")

# ----- 9. ERD (long: class × column) -----
def build_grid_erd():
    d = _dossier(); rows = []
    for cn, c in d["classes"].items():
        t = c.get("table",{}) or {}
        for col in t.get("columns",[]):
            rows.append({"Area":c.get("areaLabel",""),"Class":c.get("name",""),"Tier":c.get("tier",""),"Table":t.get("name",""),
                         "Column":col.get("name",""),"Type":col.get("type",""),"Key":col.get("key",""),"Note":col.get("note","")})
    cols = [{"key":"Area","label":"Area"},{"key":"Class","label":"Class","filter":"text"},{"key":"Tier","label":"Tier"},{"key":"Table","label":"Table","filter":"text"},
            {"key":"Column","label":"Column","filter":"text"},{"key":"Type","label":"Type","filter":"text"},{"key":"Key","label":"Key"},{"key":"Note","label":"Note","filter":"text"}]
    return _grid_page("Data-model (ERD) grid", "Data model (ERD) — spreadsheet", "Every entity column across the data model, flat &amp; sortable. Source:",
                      "dossier-erd.html", "Data model (ERD)", _grid("g-erd", cols, rows), "Data-model grid")


# =================================================================== ONBOARDING & first-run (closes F9–F12)
def build_onboarding():
    d = _load("onboarding.json"); m = d["meta"]
    h = ['<div class="crumbs"><a href="index.html">Home</a> › Questions & intake › Onboarding & first-run</div>',
         '<h1>Appendix K — Onboarding &amp; First-Run Intake Flow</h1>',
         '<p class="lead">%s</p>' % _esc(m["idea"]), ILLUS]
    # flow diagram
    nodes = " --> ".join('%s["%s"]' % (s["id"], s["phase"]) for s in d["steps"])
    h.append('<div class="diagram"><div class="dt">First-run sequence — install → first score → progressive profiling</div>'
             '<pre class="mermaid">flowchart LR\n  %s</pre></div>' % nodes)
    h.append('<div class="callout safety"><div class="ct">Consent first</div>No PII or health data is captured before the '
             'privacy &amp; data-use consent step; device-pairing and EHR-connect gate the streams they unlock (Doc 16 · Doc 07 · D22). '
             'The actuarial layer is firewalled and off by default (Doc 19).</div>')
    # steps
    h.append('<h2 id="steps">First-run steps <span class="small muted">· <a class="xref" href="appendix-onboarding.html#spreadsheet">spreadsheet ↗</a></span></h2>')
    h.append('<div class="tablewrap"><table><thead><tr><th>#</th><th>Phase</th><th>Screen</th><th>Captures</th>'
             '<th>Gating</th><th>Why</th></tr></thead><tbody>')
    for s in d["steps"]:
        flags = []
        if s.get("consent"): flags.append('<span class="chip b-red">consent</span>')
        if s.get("pairs_device"): flags.append('<span class="chip b-acc">device</span>')
        gate = _esc(s.get("gated_until", "—")) + (" " + "".join(flags) if flags else "")
        h.append('<tr><td class="small mono">%d</td><td class="small"><b>%s</b></td><td class="small">%s</td>'
                 '<td class="small muted">%s</td><td class="small">%s</td><td class="small muted">%s</td></tr>'
                 % (s["order"], _esc(s["phase"]), _esc(s["screen"]), _esc(s.get("captures", "")), gate, _esc(s["why"])))
    h.append('</tbody></table></div>')
    # demographics (F10)
    h.append('<h2 id="demographics">Demographic field-set <span class="small muted">· closes F10</span></h2>')
    h.append('<p class="small muted">The full first-run demographic capture — what each field drives downstream.</p>')
    h.append('<div class="tablewrap"><table><thead><tr><th>Field</th><th>Type</th><th>Required</th><th>Drives</th><th>Source</th></tr></thead><tbody>')
    for f in d["demographics"]:
        h.append('<tr><td class="small mono">%s</td><td class="small">%s</td><td class="small">%s</td>'
                 '<td class="small muted">%s</td><td class="small muted">%s</td></tr>'
                 % (_esc(f["field"]), _esc(f["type"]), "✔" if f.get("required") else "—", _esc(f["drives"]), _esc(f["source"])))
    h.append('</tbody></table></div>')
    # cold-start (F11)
    cs = d["cold_start"]
    h.append('<h2 id="cold-start">Question-side cold-start &amp; progressive profiling <span class="small muted">· closes F11</span></h2>')
    h.append('<div class="callout note"><div class="ct">Rule</div>%s</div>' % _esc(cs["rule"]))
    h.append('<div class="tablewrap"><table><thead><tr><th>Bootstrap item</th><th>Pillar</th><th>Why it earns its place</th></tr></thead><tbody>')
    for it in cs["bootstrap_items"]:
        h.append('<tr><td class="small"><b>%s</b></td><td class="small mono">%s</td><td class="small muted">%s</td></tr>'
                 % (_esc(it["ref"]), _esc(it.get("pillar", "")), _esc(it["why"])))
    h.append('</tbody></table></div>')
    pp = cs["progressive_profiling"]
    h.append('<ul class="small">' + "".join('<li><b>%s:</b> %s</li>' % (_esc(k.replace("_", " ")), _esc(v)) for k, v in pp.items()) + '</ul>')
    # consent & device (F12)
    h.append('<h2 id="consent">Consent &amp; device-pairing sequence <span class="small muted">· closes F12</span></h2>')
    h.append('<div class="tablewrap"><table><thead><tr><th>Item</th><th>When</th><th>Gates</th><th>Owner</th></tr></thead><tbody>')
    for c in d["consent_gating"]:
        h.append('<tr><td class="small"><b>%s</b></td><td class="small">%s</td><td class="small muted">%s</td><td class="small muted">%s</td></tr>'
                 % (_esc(c["item"]), _esc(c["when"]), _esc(c["gates"]), _esc(c["doc"])))
    h.append('</tbody></table></div>')
    h.append('<p class="small muted">Source: <code>data/onboarding.json</code>. Hands off to the question bank '
             '(<a class="xref" href="appendix-question-bank.html">Appendix E</a>), persona determination '
             '(<a class="xref" href="appendix-persona-matrix.html">Appendix I</a>), goals '
             '(<a class="xref" href="appendix-goals.html">Appendix J</a>) and the Onboarding sub-machine on the '
             '<a class="xref" href="states.html">patient state machine</a>. Closes audit F9–F12 '
             '(<a class="xref" href="appendix-coverage-audit.html">Appendix G</a>).</p>')
    return "Onboarding & first-run", "".join(h)

def build_grid_onboarding():
    d = _load("onboarding.json")
    rows = [{"Order": s["order"], "Id": s["id"], "Phase": s["phase"], "Screen": s["screen"], "Captures": s.get("captures", ""),
             "Consent": "yes" if s.get("consent") else "—", "Device": "yes" if s.get("pairs_device") else "—",
             "Gated until": s.get("gated_until", "—"), "Feeds": s.get("feeds", ""), "Why": s["why"]} for s in d["steps"]]
    cols = [{"key": "Order", "label": "Order", "type": "num"}, {"key": "Id", "label": "Id"}, {"key": "Phase", "label": "Phase"},
            {"key": "Screen", "label": "Screen", "filter": "text"}, {"key": "Captures", "label": "Captures", "filter": "text"},
            {"key": "Consent", "label": "Consent"}, {"key": "Device", "label": "Device"}, {"key": "Gated until", "label": "Gated until", "filter": "text"},
            {"key": "Feeds", "label": "Feeds", "filter": "text"}, {"key": "Why", "label": "Why", "filter": "text"}]
    return _grid_page("Onboarding grid", "Onboarding first-run — spreadsheet", "The ordered first-run steps, flat &amp; sortable. Source:",
                      "appendix-onboarding.html", "Onboarding & first-run", _grid("g-onb", cols, rows), "Onboarding grid")


# =================================================================== PURESCORE SECTION (overview, wearable baselines, male, female)
def _ps_qcounts():
    try: qs = _load("question-bank.json")["questions"]
    except Exception: qs = []
    cnt = {}
    for q in qs:
        ps = set()
        for r in q.get("responses", []):
            for p in (r.get("pillars") or {}): ps.add(p)
        for p in ps: cnt[p] = cnt.get(p, 0) + 1
    return cnt, len(qs)

def _ps_wcounts():
    wc = {}
    for row in WEARABLES:
        for p in [x.strip() for x in row[4].split("·")]:
            wc[p] = wc.get(p, 0) + 1
    return wc

def _tier_counts(rows):
    c = {"C": 0, "P": 0, "X": 0}
    for r in rows:
        t = r[2]
        for k in c:
            if k in t: c[k] += 1
    return c

def _sexsplit(g):
    if "/" not in g: return None
    parts = [p.strip() for p in re.split(r"\s+/\s+", g)]   # split on " / " only — keep "(S-Asian/Gulf)" intact
    if len(parts) < 2: return None
    male = next((p for p in parts if "M" in p), None)
    fem = next((p for p in parts if "F" in p), None)
    return (male, fem) if (male and fem and male != fem) else None

def build_purescore_overview():
    qcnt, nq = _ps_qcounts(); wc = _ps_wcounts(); wp = dict(PILLAR_W)
    h = ['<div class="crumbs"><a href="index.html">Home</a> &rsaquo; PureScore &rsaquo; Overview</div>',
         '<h1>PureScore &mdash; the scoring engine</h1>',
         '<p class="lead"><b>PureScore</b> is the scoring subsystem of HikmaEngine. It turns three input streams '
         '&mdash; <b>biomarkers</b> (labs, <a class="xref" href="appendix-biomarkers.html">Appendix A</a>), '
         '<b>wearables</b> (<a class="xref" href="appendix-wearables.html">Appendix B</a>) and '
         '<b>questions</b> (<a class="xref" href="appendix-question-bank.html">Appendix E</a>, the LIFE stream) '
         '&mdash; into 12 <a class="xref" href="02-pillars-and-marker-catalog.html">pillars</a> and the MONIAC '
         '<a class="xref" href="04-moniac-reservoir-dynamics.html">reservoirs</a>, then a single score via the '
         '<a class="xref" href="03-scoring-formula.html">scoring formula</a> with its '
         '<a class="xref" href="admin-weights.html">weights &amp; constants</a> and the 2.0 '
         '<a class="xref" href="05-critical-review-and-purescore-2.0.html">companion vector</a>. '
         'Scored by sex: <a class="xref" href="purescore-sex.html">PureScore &mdash; Male</a> &middot; '
         '<a class="xref" href="purescore-sex.html">PureScore &mdash; Female</a>.</p>', ILLUS]
    h.append('<div class="diagram"><div class="dt">Inputs &rarr; pillars &amp; reservoirs &rarr; score</div>'
             '<pre class="mermaid">flowchart LR\n'
             '  LAB["Biomarkers (C/P/X)"] --> P["12 Pillars"]\n  WEAR["Wearables (+baselines)"] --> P\n'
             '  LIFE["Questions (LIFE)"] --> P\n  LIFE --> R[("Reservoirs")]\n  WEAR --> R\n  LAB --> R\n'
             '  R -->|rho*B_tilde| P\n  P -->|gamma-mean, W_k| PS(("PureScore"))\n'
             '  PS --> CV["Companion vector"]\n  PS --> SEX["by sex: Male / Female"]</pre></div>')
    h.append('<div class="tagrow" style="margin:6px 0"><span class="tier C">C</span> Core '
             '<span class="tier P">P</span> Peripheral <span class="tier X">X</span> Comprehensive '
             '&nbsp;&middot;&nbsp; metrics tiers drive coverage &amp; confidence (Doc 06 §4.3)</div>')
    h.append('<div class="tablewrap"><table><thead><tr><th>Pillar</th><th>base W_k</th><th>Reservoirs</th>'
             '<th>Metrics C/P/X</th><th>Wearables</th><th>Questions</th></tr></thead><tbody>')
    for pid, pname, res, rows in PILLARS:
        tc = _tier_counts(rows)
        h.append('<tr><td><a href="appendix-biomarkers.html#%s"><b>%s</b></a> <span class="small muted">%s</span></td>'
                 '<td class="mono">%.2f</td><td class="small muted">%s</td>'
                 '<td class="mono">%d / %d / %d</td><td class="mono">%d</td><td class="mono">%d</td></tr>'
                 % (pid, pid, _esc(pname), wp.get(pid, 0), _esc(res), tc["C"], tc["P"], tc["X"],
                    wc.get(pid, 0), qcnt.get(pid, 0)))
    h.append('</tbody></table></div>')
    h.append('<p class="small muted">Totals: %d markers across the catalogue, %d wearable signals, %d LIFE questions '
             '&mdash; all weighted into pillars &amp; reservoirs. Constants (φ, κ_resp, γ, δ, ρ_k, R_crit, cap) and '
             'per-marker weights live in <a class="xref" href="admin-weights.html">Weights &amp; constants</a>; '
             'per-metric baselines in <a class="xref" href="wearable-baseline-pipeline.html">Wearable baselines</a>.</p>'
             % (sum(len(r) for _a, _b, _c, r in PILLARS), len(WEARABLES), nq))
    return "PureScore · Overview", "".join(h)

_WB_BASELINE = {
 "Resting HR": ("personal EB μ/σ over 14d; lower = fitter", "50–70 bpm (adult)", "daily"),
 "HRV (RMSSD)": ("personal EB μ/σ; ↓ vs baseline = strain", "age-declining; track Δ", "daily"),
 "Steps / MVPA": ("14-day rolling mean vs WHO target", "≥8000 steps; ≥150 min MVPA/wk", "daily"),
 "Sleep duration / efficiency / regularity": ("14-day mean + regularity index", "7–9 h; SRI high", "nightly"),
 "SpO₂ (spot/overnight)": ("overnight nadir + ODI vs personal", "≥95%; ODI low", "nightly"),
 "Skin / body temperature": ("deviation from personal nightly baseline", "± personal band", "nightly"),
 "CGM glucose / time-in-range": ("TIR vs consensus + personal mean", "TIR ≥70%; CV <36%", "continuous"),
 "Blood pressure (cuff)": ("home mean vs guideline", "<120/80 optimal", "per reading"),
 "Single-lead ECG / rhythm": ("rhythm classification vs sinus baseline", "sinus; no AF", "on demand"),
 "Weight / body composition": ("trend vs personal baseline", "stable; BMI band", "daily/weekly"),
 "VO₂max (estimate)": ("age/sex-normed percentile", "≥ sex/age p60", "weekly"),
 "Readiness / recovery": ("composite vs personal baseline (informational)", "personal band", "daily"),
 "Stress score": ("autonomic composite vs baseline (informational)", "personal band", "daily"),
 "Sleep stages (REM/deep/light)": ("proportion vs age-norm (informational)", "age-normal", "nightly"),
 "Respiratory rate": ("deviation from personal nightly baseline", "12–20 /min", "nightly"),
}

def _sex_gated(sex):
    try: qs = _load("question-bank.json")["questions"]
    except Exception: qs = []
    return [q for q in qs if q.get("applicability", {}).get("sex") == [sex]]

_MALE_MK = [("Total testosterone", "ng/dL", "300–1000", "ENDO — hypogonadism/andropause if low"),
 ("Free testosterone", "pg/mL", "age-adjusted", "ENDO — symptomatic low-T"),
 ("PSA", "ng/mL", "age-banded (≈<4)", "prostate screening"),
 ("Hematocrit / Hemoglobin", "g/dL", "13.5–17", "HEM — polycythemia (T-therapy) / anemia"),
 ("HDL-C", "mg/dL", "≥40", "CV — protective threshold (male)"),
 ("Waist circumference", "cm", "<94 (raised ≥102)", "MET — central adiposity")]
_FEMALE_MK = [("Estradiol (E2)", "pg/mL", "cycle/menopause-dependent", "ENDO — menopause transition"),
 ("FSH", "mIU/mL", "↑ in menopause", "ENDO — ovarian reserve / menopause"),
 ("AMH", "ng/mL", "age-declining", "ENDO — ovarian reserve / fertility"),
 ("Progesterone", "ng/mL", "luteal-phase dependent", "ENDO — cycle"),
 ("Hemoglobin / Ferritin", "g/dL · ng/mL", "12–15.5; ferritin low-sensitive", "HEM/NUT — menstrual/GI iron loss"),
 ("HDL-C", "mg/dL", "≥50", "CV — protective threshold (female)"),
 ("Waist circumference", "cm", "<80 (raised ≥88)", "MET — central adiposity"),
 ("BMD T-score", "SD", "≥ −1.0; red sooner post-menopause", "BCM — bone reserve")]

def _build_purescore_sex(sex, label, markers, emphasis, lifestage):
    gated = _sex_gated(sex)
    h = ['<div class="crumbs"><a href="index.html">Home</a> &rsaquo; PureScore &rsaquo; %s</div>' % _esc(label),
         '<h1>PureScore &mdash; %s</h1>' % _esc(label),
         '<p class="lead">The %s view of the scoring engine: sex-specific reference cut-points, %s-specific markers, '
         'pillar-weight emphases and the %d %s-gated questions. The shared engine (formula, reservoirs, companion '
         'vector) is in the <a class="xref" href="purescore-overview.html">Overview</a>; sex handling is '
         '<a class="xref" href="08-sex-specific-models.html">Doc 08</a>.</p>' % (label.lower(), label.lower(), len(gated), label.lower()),
         ILLUS]
    h.append('<div class="callout note"><div class="ct">Pillar emphasis (%s)</div>%s</div>' % (_esc(label), _esc(emphasis)))
    # sex-specific markers (curated)
    h.append('<h2>%s-specific markers</h2><div class="tablewrap"><table><thead><tr><th>Marker</th><th>Unit</th>'
             '<th>%s reference</th><th>Pillar / note</th></tr></thead><tbody>' % (_esc(label), _esc(label)))
    for mk, unit, rng, note in markers:
        h.append('<tr><td><b>%s</b></td><td class="small muted">%s</td><td class="small mono">%s</td>'
                 '<td class="small">%s</td></tr>' % (_esc(mk), _esc(unit), _esc(rng), _esc(note)))
    h.append('</tbody></table></div>')
    # shared markers with sex-specific cut-points parsed from the catalogue
    idx = 0 if sex == "male" else 1
    rowsout = []
    for pid, pname, res, rows in PILLARS:
        for r in rows:
            ss = _sexsplit(r[4])
            if ss: rowsout.append((pid, r[0], r[1], ss[idx]))
    if rowsout:
        h.append('<h2>Shared markers, %s cut-points</h2><div class="tablewrap"><table><thead><tr><th>Pillar</th>'
                 '<th>Marker</th><th>Unit</th><th>%s green band</th></tr></thead><tbody>' % (_esc(label), _esc(label)))
        for pid, mk, unit, val in rowsout:
            h.append('<tr><td class="mono">%s</td><td><b>%s</b></td><td class="small muted">%s</td>'
                     '<td class="small mono">%s</td></tr>' % (pid, _esc(mk), _esc(unit), _esc(val)))
        h.append('</tbody></table></div>')
    h.append('<h2>Life-stage frames</h2><p class="small">%s</p>' % _esc(lifestage))
    # sex-gated questions
    h.append('<h2>%s-gated questions <span class="small muted">(%d)</span></h2>'
             '<p class="small muted">From the question bank, served only when sex = %s:</p><ul class="small">'
             % (_esc(label), len(gated), _esc(label.lower())))
    for q in gated[:40]:
        h.append('<li><span class="mono">%s</span> — %s</li>' % (_esc(q.get("ref", "")), _esc(q.get("text", ""))))
    h.append('</ul>')
    h.append('<div class="callout note"><div class="ct">Reference ranges by context</div>Sex is one of several axes that shift a '
             'marker’s range. Step through sex / age / life-stage / condition / medication in the '
             '<a class="xref" href="reference-range-resolver.html">Reference-range resolver</a>; per-marker variations + citations are on '
             '<a class="xref" href="appendix-biomarkers.html">Appendix A · Markers</a>.</div>')
    return "PureScore — %s" % label, "".join(h)

def build_purescore_male():
    return _build_purescore_sex("male", "Male", _MALE_MK,
        "CV carries earlier baseline risk; ENDO tracks testosterone/andropause; prostate (PSA) screening; HEM watches "
        "polycythemia. Hemoglobin/eGFR/urate read against male reference.",
        "Andropause — gradual testosterone decline from midlife; no cyclical hormonal frame.")

def build_purescore_female():
    return _build_purescore_sex("female", "Female", _FEMALE_MK,
        "ENDO dominates across cycle → perimenopause → menopause; BCM bone loss post-menopause (BMD red sooner); HEM "
        "iron/menstrual-loss sensitivity; CV risk rises post-menopause. Hemoglobin/HDL/waist read against female reference.",
        "Menstrual cycle (follicular/luteal phase-aware ranges) · pregnancy frame (physiologic shifts; pre-eclampsia/GDM "
        "anchors retained) · perimenopause/menopause (vasomotor, bone, CV).")

def build_purescore_sex():
    """Merged Male + Female view behind a segmented toggle (IA cleanup: was two pages)."""
    _, mbody = build_purescore_male()
    _, fbody = build_purescore_female()
    def body_only(b):
        i = b.find('<p class="lead">'); return b[i:] if i >= 0 else b
    head = ('<div class="crumbs"><a href="index.html">Home</a> &rsaquo; PureScore &rsaquo; By sex</div>'
            '<h1>PureScore &mdash; by sex</h1>'
            '<p class="lead">Sex-specific reference cut-points, markers, pillar emphases and sex-gated questions. '
            'The shared engine is in the <a class="xref" href="purescore-overview.html">Overview</a>; sex handling is '
            '<a class="xref" href="08-sex-specific-models.html">Doc 08</a>.</p>'
            '<div class="seg" id="sexseg"><button class="segbtn on" data-sex="male" type="button">Male</button>'
            '<button class="segbtn" data-sex="female" type="button">Female</button></div>')
    panels = ('<div class="sexpanel" data-sex="male">' + body_only(mbody) + '</div>'
              '<div class="sexpanel" data-sex="female" hidden>' + body_only(fbody) + '</div>')
    tail = ('<style>.seg{display:inline-flex;margin:12px 0;border:1px solid var(--line,#2a3340);border-radius:9px;overflow:hidden}'
            '.segbtn{font:inherit;font-size:13px;padding:6px 18px;background:var(--bg2,#0c1320);color:inherit;border:none;cursor:pointer}'
            '.segbtn.on{background:#2b5a86;color:#fff;font-weight:700}</style>'
            '<script>(function(){var s=document.getElementById("sexseg");if(!s)return;'
            's.addEventListener("click",function(e){var b=e.target.closest(".segbtn");if(!b)return;var x=b.getAttribute("data-sex");'
            '[].forEach.call(s.querySelectorAll(".segbtn"),function(k){k.classList.toggle("on",k===b);});'
            '[].forEach.call(document.querySelectorAll(".sexpanel"),function(p){p.hidden=(p.getAttribute("data-sex")!==x);});});})();</script>')
    return "PureScore by sex", head + panels + tail

def build_class_model():
    """Merged class diagram + class explorer into one 'Class model' page (IA cleanup)."""
    _, bbody = build_behemoth()
    _, cbody = build_class_explorer()
    def body_only(b):
        i = b.find('</h1>'); return b[i + 5:] if i >= 0 else b
    head = ('<div class="crumbs"><a href="index.html">Home</a> &rsaquo; System &amp; build &rsaquo; Class model</div>'
            '<h1>Class model &mdash; domain object model</h1>'
            '<p class="lead">The full PureScore object model two ways: the pan/zoom <b>class diagram</b> (all classes &amp; '
            'relations) and the interactive <b>class explorer</b> (pick a class for its fields, relations, table, API and '
            'stories). Both are derived from the same spec.</p>')
    return "Class model", (head + '<h2 id="diagram">Class diagram</h2>' + body_only(bbody)
                           + '<hr><h2 id="explorer">Class explorer</h2>' + body_only(cbody))


def build_production_gaps():
    """Production-readiness gap analysis for the patient-facing mobile app ('Gaps & roadmap' chapter)."""
    sev = {"missing": '<span class="chip b-red">missing</span>', "partial": '<span class="chip b-yellow">partial</span>', "addressed": '<span class="chip b-green">addressed</span>'}
    GROUPS = [
     ("Identity &amp; security", [
       ("Auth &amp; identity", "missing", "Consent/privacy policy (Doc 16)",
        "Signup/login, MFA, biometric unlock, session &amp; token lifecycle, account recovery, multi-device."),
       ("On-device security engineering", "missing", "PHI policy + HIPAA/GDPR pointers (Doc 16)",
        "Encryption-at-rest / Keychain, app-lock, jailbreak-root detection, cert pinning, screenshot/PHI masking, idle timeout."),
       ("Patient data-rights flows", "partial", "HIPAA/GDPR/GINA stated (Doc 16)",
        "Self-service export (portability), delete / right-to-be-forgotten, per-stream sharing toggles, patient-visible access log."),
     ]),
     ("App &amp; UX", [
       ("Mobile client architecture &amp; design system", "missing", "Onboarding spec + admin mockups",
        "Screen/flow inventory, navigation/IA, component library + design tokens, theming, state management."),
       ("Results &amp; explainability UX", "missing", "The math: score + companion vector + binding constraint + provenance (Docs 03/05, D33)",
        "Mobile rendering: trend charts, dual-framing, and the cohort-imputed <b>provisional</b> presentation."),
       ("Accessibility (incl. colour-blind-safe status)", "missing", "Green/yellow/red zone model",
        "WCAG 2.2, screen reader, dynamic type, and a <b>redundant icon/label cue for status colours</b> (safety, not cosmetic)."),
       ("Full-UI i18n / Arabic RTL", "partial", "Clinical localization &amp; Ramadan (Doc 18)",
        "UI-string i18n + right-to-left layout for the whole app."),
       ("Edge / empty / error / offline states", "missing", "&mdash;",
        "Loading, no-data, retry, connectivity-loss and stale-data indicators across every screen."),
     ]),
     ("Engagement &amp; care", [
       ("Notification &amp; nudge delivery", "addressed", "Doc 11 §9 delivery engine: 2 classes (engagement vs safety-critical), channels + fallback ladder, send-time, quiet hours, caps, consent, PHI-safe payloads, deep-links, streaks, measurement, guaranteed critical escalation",
        "Build + validate: server/outbox, push/SMS/WhatsApp/email, ack/escalation, delivery SLOs (Doc 14)."),
       ("Care-team &amp; crisis UX", "partial", "Escalation tiers + crisis pathway policy (Doc 16)",
        "On-device emergency surfacing (push takeover, &lsquo;call 999/112&rsquo;), clinician messaging, telehealth, scheduling, disclaimers."),
       ("Device / wearable integration UX", "partial", "Trust tiers (D22), streams (Doc 07)",
        "HealthKit / Google-Fit / Terra pairing flows, permission prompts, backfill, background sync, battery/data budget."),
     ]),
     ("Offline &amp; data", [
       ("Offline-first &amp; sync", "missing", "&mdash;",
        "Offline access, conflict resolution, caching strategy."),
     ]),
     ("Product &amp; compliance", [
       ("Product analytics &amp; in-app feedback", "missing", "Model validation (Doc 14)",
        "Event taxonomy, funnels/retention, experiment framework, client crash reporting, in-app support/bug-report."),
       ("SaMD / app-level regulatory", "partial", "Methodology regulatory posture (Docs 16/05)",
        "App-level Software-as-a-Medical-Device classification, intended-use/labeling, UAE MoHAP/DHA app registration, app-store medical compliance."),
       ("Content &amp; health literacy", "partial", "Some education references",
        "Educational / &lsquo;Learn&rsquo; content &amp; CMS, plain-language, reading-level, cultural fit."),
     ]),
    ]
    nmiss = sum(1 for _, items in GROUPS for it in items if it[1] == "missing")
    npart = sum(1 for _, items in GROUPS for it in items if it[1] == "partial")
    naddr = sum(1 for _, items in GROUPS for it in items if it[1] == "addressed")
    h = ['<div class="crumbs"><a href="index.html">Home</a> &rsaquo; Gaps &amp; roadmap &rsaquo; Production readiness</div>',
         '<h1>Production readiness &mdash; gap analysis</h1>',
         '<p class="lead">What a <b>production-grade, patient-facing mobile app</b> still needs beyond this methodology + '
         'backend spec (scope excludes CI/CD &amp; deployment). <span class="chip b-red">missing</span> = no spec yet; '
         '<span class="chip b-yellow">partial</span> = policy/partial only; <span class="chip b-green">addressed</span> = now specified. '
         'This page is the <b>product/app surface</b>; its companion <a class="xref" href="spec-audit.html">Spec build-readiness audit</a> '
         'asks the different question of whether the <i>methodology spec itself</i> is complete enough to build from. Sibling gap surfaces in this chapter: '
         '<a class="xref" href="17-clinician-red-team-and-blind-spots.html">Clinician red-team &amp; blind-spots</a> and '
         '<a class="xref" href="appendix-coverage-audit.html">Coverage audit</a>.</p>', ILLUS,
         '<div class="callout note"><div class="ct">Two to treat as near-term</div>'
         '<b>Colour-blind-safe status</b> &mdash; green/yellow/red alone fails ~8% of male users; add a redundant icon/label cue. '
         '<b>On-device security + auth</b> &mdash; the largest truly-missing surface for PHI, and it gates app-store / medical review.</div>',
         '<p class="small muted">%d areas &middot; %d missing &middot; %d partial &middot; %d addressed.</p>' % (nmiss + npart + naddr, nmiss, npart, naddr)]
    for gname, items in GROUPS:
        h.append('<h2>%s</h2><div class="tablewrap"><table><thead><tr><th>Area</th><th>Status</th>'
                 '<th>What exists today</th><th>What a patient app needs</th></tr></thead><tbody>' % gname)
        for area, st, exists, needed in items:
            h.append('<tr><td><b>%s</b></td><td>%s</td><td class="small muted">%s</td><td class="small">%s</td></tr>'
                     % (area, sev[st], exists, needed))
        h.append('</tbody></table></div>')
    # --- Clinical coverage register (data/coverage.json) — closes the audit's gap:/coverage flags ---
    try:
        cov = _load("coverage.json")
    except Exception:
        cov = {"markers": [], "gates": [], "self_report": []}
    cst = {"missing": '<span class="chip b-red">missing</span>', "partial": '<span class="chip b-yellow">partial</span>',
           "present": '<span class="chip b-green">present</span>', "derive": '<span class="chip b-acc">derive</span>',
           "gate": '<span class="chip b-mut">gate</span>'}
    h.append('<h2 id="coverage">Clinical coverage register</h2>')
    h.append('<p class="small muted">Marker / gate / self-report coverage from the adversarial clinical audit — the canonical answer to '
             '&ldquo;what does the engine score, what is a deliberate gate, and what is genuinely missing?&rdquo; '
             'Single source: <code>data/coverage.json</code>. Tracked against the audit flags on '
             '<a class="xref" href="appendix-biomarkers.html">Appendix A</a>.</p>')
    for sec, rows, c1 in (("Markers", cov.get("markers", []), "Marker"),
                          ("Gates &amp; modifiers", cov.get("gates", []), "Gate"),
                          ("Self-report representation", cov.get("self_report", []), "Area")):
        h.append('<h3>%s</h3><div class="tablewrap"><table><thead><tr><th>%s</th><th>Status</th><th>Plan</th></tr></thead><tbody>' % (sec, c1))
        for r in rows:
            nm = r.get("name") or r.get("area", "")
            h.append('<tr><td><b>%s</b></td><td>%s</td><td class="small">%s</td></tr>'
                     % (_esc(nm), cst.get(r.get("status"), _esc(r.get("status", ""))), _esc(r.get("plan", ""))))
        h.append('</tbody></table></div>')
    return "Production readiness — gaps", "".join(h)


def build_spec_audit():
    """Spec BUILD-READINESS audit — could an engineer build the engine from the specs as written?
    Distinct axis from production-gaps.html (product/app surface). Data-driven from
    data/spec-audit.json (a per-chapter content audit of Docs 01–19). Severity:
    blocker (cannot implement) / major (ambiguous, likely-wrong guess) / minor (clarity / edge case)."""
    try:
        data = _load("spec-audit.json")
    except Exception:
        data = {"method": "", "sections": []}
    SEV = {"blocker": '<span class="chip b-red">blocker</span>',
           "major": '<span class="chip b-yellow">major</span>',
           "minor": '<span class="chip b-mut">minor</span>'}
    secs = data.get("sections", [])
    allf = [f for s in secs for f in s.get("findings", [])]
    nb = sum(1 for f in allf if f.get("sev") == "blocker")
    nm = sum(1 for f in allf if f.get("sev") == "major")
    nk = sum(1 for f in allf if f.get("sev") == "minor")
    nfix = sum(1 for f in allf if f.get("status") == "fixed")
    h = ['<div class="crumbs"><a href="index.html">Home</a> &rsaquo; Gaps &amp; roadmap &rsaquo; Spec build-readiness</div>',
         '<h1>Spec build-readiness audit</h1>',
         '<p class="lead">A per-chapter content audit of the source specs (Docs 01&ndash;19) asking one question: '
         '<b>could a competent engineer implement exactly what is written, without guessing?</b> '
         'This is a different axis from the '
         '<a class="xref" href="production-gaps.html">Production-readiness</a> page (which tracks the product/app surface &mdash; '
         'auth, offline, analytics). Here the subject is the <b>methodology spec itself</b>. '
         '<span class="chip b-red">blocker</span> = cannot implement as written; '
         '<span class="chip b-yellow">major</span> = ambiguous / likely-wrong guess; '
         '<span class="chip b-mut">minor</span> = clarity or edge case.</p>',
         '<p class="small muted">%d findings &middot; %d blockers &middot; %d major &middot; %d minor &middot; %d already fixed &middot; across %d chapters.</p>'
         % (len(allf), nb, nm, nk, nfix, len(secs)),
         '<div class="callout safety"><div class="ct">The systemic blocker</div>'
         'Most blockers are <b>not</b> structural &mdash; the equations, gates and state machine are specified. '
         'They are <b>numeric</b>: coefficients, thresholds and reservoir constants (κ, λ, shrinkage k, calibration '
         'tolerances, clinical-score coefficients, pricing relativities) are repeatedly &ldquo;illustrative&rdquo; or '
         '&ldquo;awaits real cohort data&rdquo; (Doc&nbsp;17&nbsp;&sect;K, Doc&nbsp;19, Doc&nbsp;13). '
         'So the work splits cleanly into two classes:</div>',
         '<div class="tablewrap"><table><thead><tr><th>Resolution class</th><th>What it covers</th><th>Who closes it</th></tr></thead><tbody>'
         '<tr><td><b>Authorable now</b></td><td class="small">Undefined formulas that should simply be written out, '
         'enum / naming reconciliations, dangling references, missing edge-case rules &mdash; no new data needed.</td>'
         '<td class="small">Spec authors</td></tr>'
         '<tr><td><b>Needs calibration data</b></td><td class="small">Every numeric constant deferred to fitting on a '
         'consented cohort, plus the clinical-score coefficient sets and actuarial relativities.</td>'
         '<td class="small">Data science + the Doc&nbsp;13/14 harness, gated by counsel/clinical sign-off where flagged</td></tr>'
         '</tbody></table></div>', ILLUS]
    for s in secs:
        fs = s.get("findings", [])
        order = {"blocker": 0, "major": 1, "minor": 2}
        fs = sorted(fs, key=lambda f: order.get(f.get("sev"), 9))
        anchor = re.sub(r"[^a-z0-9]+", "-", s.get("title", "").lower()).strip("-") or "s"
        h.append('<h2 id="%s">%s <span class="kref">(%d)</span></h2>' % (anchor, _esc(s.get("title", "")), len(fs)))
        h.append('<div class="tablewrap"><table><thead><tr><th>Doc</th><th>Severity</th><th>Type</th>'
                 '<th>Gap</th><th>Suggested resolution</th></tr></thead><tbody>')
        for f in fs:
            fixed = ' <span class="chip b-green">fixed</span>' if f.get("status") == "fixed" else ""
            h.append('<tr><td class="small mono">%s</td><td>%s</td><td class="small"><code>%s</code></td>'
                     '<td class="small">%s</td><td class="small muted">%s%s</td></tr>'
                     % (_esc(f.get("doc", "")), SEV.get(f.get("sev"), _esc(f.get("sev", ""))),
                        _esc(f.get("type", "")), _esc(f.get("gap", "")), _esc(f.get("fix", "")), fixed))
        h.append('</tbody></table></div>')
    h.append('<p class="small muted">Method: %s</p>' % _esc(data.get("method", "")))
    return "Spec build-readiness audit", "".join(h)


def build_editorial_review():
    """Editorial & cohesion review — a multi-agent comb-through of every rendered page plus a
    mechanical link/anchor scan. Data-driven from data/editorial-review.json. A third, distinct
    axis from spec-audit (can an engineer build it?) and production-gaps (what does the app still
    need?): this asks whether the wiki READS as one cohesive product for clinicians, devs & product."""
    try:
        data = _load("editorial-review.json")
    except Exception:
        data = {"method": "", "sections": []}
    SEV = {"high": '<span class="chip b-red">high</span>', "med": '<span class="chip b-yellow">med</span>',
           "low": '<span class="chip b-mut">low</span>'}
    secs = data.get("sections", [])
    allf = [f for s in secs for f in s.get("findings", [])]
    nh = sum(1 for f in allf if f.get("sev") == "high")
    nm = sum(1 for f in allf if f.get("sev") == "med")
    nl = sum(1 for f in allf if f.get("sev") == "low")
    nfix = sum(1 for f in allf if f.get("status") == "fixed")
    h = ['<div class="crumbs"><a href="index.html">Home</a> &rsaquo; Gaps &amp; roadmap &rsaquo; Editorial &amp; cohesion review</div>',
         '<h1>Editorial &amp; cohesion review</h1>',
         '<p class="lead">A page-by-page comb-through of the whole wiki (six review agents, one per chapter cluster) '
         'plus a mechanical link/anchor scan, asking a single question: <b>does this read as one cohesive product '
         'for clinicians, developers and product?</b> It is a third axis, distinct from '
         '<a class="xref" href="spec-audit.html">Spec build-readiness</a> (can an engineer build it?) and '
         '<a class="xref" href="production-gaps.html">Production readiness</a> (what does the app still need?). '
         '<span class="chip b-red">high</span> = visibly wrong or breaks the &ldquo;one product&rdquo; feel; '
         '<span class="chip b-yellow">med</span> = an inconsistency a reader will notice; '
         '<span class="chip b-mut">low</span> = polish.</p>',
         '<p class="small muted">%d findings &middot; %d high &middot; %d med &middot; %d low &middot; %d already fixed &middot; across %d areas.</p>'
         % (len(allf), nh, nm, nl, nfix, len(secs)),
         '<div class="callout note"><div class="ct">How to use this</div>'
         'Per your direction these are <b>tracked, not yet applied</b> — except a handful of defects in the '
         'newly-added chapter landing pages (marked <span class="chip b-green">fixed</span>), which were corrected in place. '
         'Every fix lives in the source / builders (<code>../purescore/*.md</code>, <code>wiki_content.py</code>, '
         '<code>build_wiki.py</code>, <code>data/*.json</code>), never the generated HTML. The biggest wins are the '
         'cross-cutting items at the top: relabelling the &ldquo;README&rdquo; links and filling the glossary fix dozens of '
         'pages at once.</div>', ILLUS]
    order = {"high": 0, "med": 1, "low": 2}
    for s in secs:
        fs = sorted(s.get("findings", []), key=lambda f: order.get(f.get("sev"), 9))
        anchor = re.sub(r"[^a-z0-9]+", "-", s.get("title", "").lower()).strip("-") or "s"
        h.append('<h2 id="%s">%s <span class="kref">(%d)</span></h2>' % (anchor, _esc(s.get("title", "")), len(fs)))
        h.append('<div class="tablewrap"><table><thead><tr><th>Page</th><th>Severity</th><th>Dimension</th>'
                 '<th>Finding</th><th>Suggested fix</th></tr></thead><tbody>')
        for f in fs:
            fixed = (' <span class="chip b-green">fixed</span>') if f.get("status") == "fixed" else ""
            h.append('<tr><td class="small mono">%s</td><td>%s</td><td class="small"><code>%s</code></td>'
                     '<td class="small">%s</td><td class="small muted">%s%s</td></tr>'
                     % (_esc(f.get("page", "")), SEV.get(f.get("sev"), _esc(f.get("sev", ""))),
                        _esc(f.get("dim", "")), _esc(f.get("finding", "")), _esc(f.get("fix", "")), fixed))
        h.append('</tbody></table></div>')
    h.append('<p class="small muted">Method: %s</p>' % _esc(data.get("method", "")))
    return "Editorial & cohesion review", "".join(h)


def build_consolidation_plan():
    """Merge/consolidation plan — a 5-agent scan for overlap that should collapse to one canonical
    home. Data-driven from data/consolidation-plan.json. PLAN ONLY: nothing is merged until approved.
    Each candidate carries an id (CPn / Kn) you can reference when approving."""
    try:
        data = _load("consolidation-plan.json")
    except Exception:
        data = {"method": "", "framing": "", "sections": []}
    RISK = {"high": '<span class="chip b-red">high</span>', "med": '<span class="chip b-yellow">med</span>',
            "low": '<span class="chip b-mut">low</span>'}
    secs = data.get("sections", [])
    allf = [f for s in secs for f in s.get("findings", [])]
    merges = [f for f in allf if f.get("id", "").startswith("CP")]
    keeps = [f for f in allf if f.get("id", "").startswith("K")]
    excl = [f for f in allf if f.get("id", "").startswith("X")]
    nm = sum(1 for f in merges if f.get("risk") == "med")
    nl = sum(1 for f in merges if f.get("risk") == "low")
    h = ['<div class="crumbs"><a href="index.html">Home</a> &rsaquo; Gaps &amp; roadmap &rsaquo; Consolidation plan</div>',
         '<h1>Consolidation plan <span class="small muted">&middot; wiki pages &amp; diagrams</span></h1>',
         '<p class="lead">Where the wiki repeats itself &mdash; pages or sections that overlap enough to collapse to '
         '<b>one canonical home</b>, so each concept has a single update site. Five agents scanned every page (generated HTML, '
         'builders, data, diagrams). <b>Scoped to the wiki surface you manage</b> &mdash; the numbered spec spine (Docs 01&ndash;19 '
         'markdown) is out of scope; where a page restates a spec, the fix edits the <b>page</b> (point at the spec), never the spec. '
         '<b>Plan only</b> &mdash; nothing is merged until you approve a candidate by its id. '
         '<span class="chip b-yellow">med</span> = a standalone page or one that must preserve cross-refs / a diagram-data guard; '
         '<span class="chip b-mut">low</span> = generated-page render or label cleanup.</p>',
         '<p class="small muted">%d page &amp; diagram merge candidates &middot; %d low &middot; %d med &middot; plus %d confirmed keep-separate and %d out-of-scope (source spec / data).</p>'
         % (len(merges), nl, nm, len(keeps), len(excl)),
         '<div class="callout note"><div class="ct">Read this first</div>%s</div>' % _esc(data.get("framing", "")), ILLUS]
    order = {"high": 2, "med": 1, "low": 0}
    for s in secs:
        fs = s.get("findings", [])
        anchor = re.sub(r"[^a-z0-9]+", "-", s.get("title", "").lower()).strip("-") or "s"
        h.append('<h2 id="%s">%s <span class="kref">(%d)</span></h2>' % (anchor, _esc(s.get("title", "")), len(fs)))
        h.append('<div class="tablewrap"><table><thead><tr><th>#</th><th>Pages</th><th>Overlap</th>'
                 '<th>Canonical &amp; mechanism</th><th>Risk</th></tr></thead><tbody>')
        for f in fs:
            pages = " &middot; ".join('<code>%s</code>' % _esc(p) for p in f.get("pages", []))
            overlap = ('<b>%s</b> <span class="chip b-mut">%s</span><div class="small muted" style="margin-top:3px">%s</div>'
                       % (_esc(f.get("title", "")), _esc(f.get("type", "")), _esc(f.get("overlap", ""))))
            canon = '%s<div class="small" style="margin-top:3px"><b>%s</b></div>' % (_esc(f.get("canonical", "")), _esc(f.get("mechanism", "")))
            if f.get("gain") and f.get("gain") != "—":
                canon += '<div class="small muted" style="margin-top:2px">gain: %s</div>' % _esc(f["gain"])
            h.append('<tr><td class="mono small">%s</td><td class="small">%s</td><td class="small">%s</td>'
                     '<td class="small">%s</td><td>%s</td></tr>'
                     % (_esc(f.get("id", "")), pages, overlap, canon, RISK.get(f.get("risk"), _esc(f.get("risk", "")))))
        h.append('</tbody></table></div>')
    h.append('<p class="small muted">Method: %s</p>' % _esc(data.get("method", "")))
    return "Consolidation plan", "".join(h)


# =================================================================== CARE PATHWAYS & DELIVERY
_RC = {"aligned": '<span class="chip b-green">aligned</span>', "partial": '<span class="chip b-yellow">partial</span>',
       "divergent": '<span class="chip b-red">divergent</span>'}

def build_care_pathways():
    """Condition care-pathways (Care pathways & delivery). Server-rendered from data/care-pathways.json:
    per-condition IEAT lifecycle, 3-tier priority → role owner, referral, monitoring, UAE specifics, and
    the reconciliation of each threshold against PureScore's canonical ranges."""
    try:
        cp = _load("care-pathways.json")
    except Exception:
        cp = {"pathways": {}}
    P = cp["pathways"]
    h = ['<div class="crumbs"><a href="index.html">Home</a> &rsaquo; Care pathways &amp; delivery &rsaquo; Condition pathways</div>',
         '<h1>Condition Care Pathways <span class="small muted">&middot; from score to managed care</span></h1>',
         '<p class="lead">Where the PureScore engine meets the clinic. Each condition runs an '
         '<b>Identify &rarr; Engage &rarr; Assess &rarr; Transform</b> lifecycle, stratifies patients into a '
         '<b>3-tier priority</b> routed to a <a class="xref" href="care-roles.html">care-team role</a> '
         '(Priority 1-2 &rarr; Family Physician, Priority 3 &rarr; GP), and escalates to specialists. '
         'Source: Abu Dhabi primary-care pathways + PURA app workflows (DRAFT). Single source: '
         '<code>data/care-pathways.json</code>. Each threshold is <b>reconciled to the canonical ranges</b> '
         '(<a class="xref" href="reference-range-resolver.html">resolver</a>); divergences are flagged.</p>', ILLUS,
         '<div class="callout note"><div class="ct">IEAT lifecycle</div>'
         '<b>Identify</b> search &amp; stratify the cohort &middot; <b>Engage</b> invite, prepare, lifestyle brief &middot; '
         '<b>Assess</b> clinician review &amp; optimise &middot; <b>Transform</b> sustain, monitor &amp; escalate. '
         'The app/agent assists each stage (<a class="xref" href="prevention-engagement.html">prevention &amp; engagement</a>); '
         'clinicians make the decisions (<a class="xref" href="care-roles.html">hybrid human-in-the-loop</a>).</div>']
    h.append('<div class="tablewrap"><table><thead><tr><th>Pathway</th><th>Persona</th><th>Stratifier</th>'
             '<th>Priority owners</th><th>Source</th></tr></thead><tbody>')
    for key, p in P.items():
        owners = " / ".join(sorted({t.get("owner", "") for t in p.get("priority", [])}))
        per = p.get("persona", {})
        pname = per.get("name") or "&mdash;"
        h.append('<tr><td><b><a href="#%s">%s</a></b></td><td>%s%s</td><td class="small">%s</td>'
                 '<td class="small muted">%s</td><td class="small muted">%s</td></tr>'
                 % (_esc(key), _esc(p.get("title", key)), _esc(pname),
                    (" (" + str(per.get("age")) + ")") if per.get("age") else "",
                    _esc(p.get("stratifier", "")), _esc(owners), _esc(p.get("source", ""))))
    h.append('</tbody></table></div>')
    for key, p in P.items():
        per = p.get("persona", {})
        h.append('<h2 id="%s">%s</h2>' % (_esc(key), _esc(p.get("title", key))))
        if per.get("vignette"):
            h.append('<p class="small"><b>Persona &mdash; %s%s:</b> %s</p>'
                     % (_esc(per.get("name", "")), (", " + str(per.get("age"))) if per.get("age") else "",
                        _esc(per.get("vignette", ""))))
        h.append('<p class="small muted"><b>Stratifier:</b> %s</p>' % _esc(p.get("stratifier", "")))
        h.append('<div class="tablewrap"><table><thead><tr><th>Priority</th><th>Owner</th><th>Criteria</th></tr></thead><tbody>')
        for t in p.get("priority", []):
            h.append('<tr><td><b>%s</b></td><td>%s</td><td class="small">%s</td></tr>'
                     % (_esc(t.get("tier", "")), _esc(t.get("owner", "")), _esc("; ".join(t.get("criteria", [])))))
        h.append('</tbody></table></div>')
        ie = p.get("ieat", {})
        h.append('<div class="tablewrap"><table><thead><tr><th>Identify</th><th>Engage</th><th>Assess</th><th>Transform</th></tr></thead><tbody><tr>')
        for st in ("identify", "engage", "assess", "transform"):
            items = ie.get(st, [])
            h.append('<td class="small"><ul style="margin:0;padding-left:16px">%s</ul></td>'
                     % "".join("<li>%s</li>" % _esc(x) for x in items))
        h.append('</tr></tbody></table></div>')
        for label, field in (("Referral / escalation", "referral"), ("Monitoring", "monitoring"), ("UAE specifics", "uae")):
            vals = p.get(field, [])
            if vals:
                h.append('<p class="small"><b>%s:</b> %s</p>' % (label, " &middot; ".join(_esc(v) for v in vals)))
        rec = p.get("reconcile", [])
        if rec:
            h.append('<p class="small muted" style="margin-bottom:3px"><b>Threshold reconciliation vs canonical:</b></p>')
            h.append('<div class="tablewrap"><table><thead><tr><th>Pathway threshold</th><th>Canonical</th><th>Status</th><th>Note</th></tr></thead><tbody>')
            for r in rec:
                h.append('<tr><td class="small">%s</td><td class="small muted">%s</td><td>%s</td><td class="small">%s</td></tr>'
                         % (_esc(r.get("threshold", "")), _esc(r.get("canonical", "")),
                            _RC.get(r.get("status"), _esc(r.get("status", ""))), _esc(r.get("note", ""))))
            h.append('</tbody></table></div>')
    return "Care pathways", "".join(h)

def build_care_roles():
    """Care-team operating model (hybrid human-in-the-loop) from data/care-roles.json."""
    try:
        cr = _load("care-roles.json")
    except Exception:
        cr = {"roles": {}}
    h = ['<div class="crumbs"><a href="index.html">Home</a> &rsaquo; Care pathways &amp; delivery &rsaquo; Care team &amp; roles</div>',
         '<h1>Care Team &amp; Roles <span class="small muted">&middot; hybrid human-in-the-loop</span></h1>',
         '<p class="lead">The <a class="xref" href="care-pathways.html">condition pathways</a> are delivered by a '
         'primary-care team. PureScore is <b>digital-first but human-in-the-loop</b>: each role keeps its clinical '
         'responsibilities; the app/agent <b>prepares, automates, prioritises and escalates</b> &mdash; it never makes '
         'the clinical decision. Single source: <code>data/care-roles.json</code>.</p>', ILLUS]
    for key, r in cr.get("roles", {}).items():
        h.append('<h2 id="%s">%s</h2>' % (_esc(key), _esc(r.get("title", key))))
        h.append('<p class="small muted">%s</p>' % _esc(r.get("scope", "")))
        h.append('<div class="tablewrap"><table><thead><tr><th>Clinical responsibilities</th>'
                 '<th>App / agent assist</th></tr></thead><tbody><tr>')
        h.append('<td class="small"><ul style="margin:0;padding-left:16px">%s</ul></td>'
                 % "".join("<li>%s</li>" % _esc(x) for x in r.get("responsibilities", [])))
        h.append('<td class="small"><ul style="margin:0;padding-left:16px">%s</ul></td>'
                 % "".join("<li>%s</li>" % _esc(x) for x in r.get("app_assist", [])))
        h.append('</tr></tbody></table></div>')
        esc_to = r.get("escalates_to", [])
        if esc_to:
            h.append('<p class="small muted">Escalates to: %s</p>' % _esc(", ".join(esc_to)))
    return "Care team & roles", "".join(h)

def build_prevention():
    """Prevention engagement layer (PURA): reminders, interventions, alerts, CDS, Prevention Score, KPIs."""
    try:
        pn = _load("prevention-nudges.json")
    except Exception:
        pn = {}
    h = ['<div class="crumbs"><a href="index.html">Home</a> &rsaquo; Care pathways &amp; delivery &rsaquo; Prevention &amp; engagement</div>',
         '<h1>Prevention &amp; Engagement <span class="small muted">&middot; PURA nudges, alerts &amp; outcomes</span></h1>',
         '<p class="lead">The preventive layer that keeps patients on a <a class="xref" href="care-pathways.html">pathway</a> '
         'between clinic touch-points: reminders, a behavioural-intervention catalogue, the alert framework, the condition-level '
         '<b>Prevention Score</b>, and the KPI/outcome measures. Reminders should route through the '
         '<a class="xref" href="11-daily-nudge-engine.html">Doc 11 §9 delivery engine</a>, not a parallel sender. '
         'Single source: <code>data/prevention-nudges.json</code>.</p>', ILLUS]
    h.append('<h2 id="reminders">Preventive reminders</h2><div class="tablewrap"><table><thead><tr>'
             '<th>Reminder</th><th>Trigger</th><th>Timing / escalation</th><th>Message</th></tr></thead><tbody>')
    for r in pn.get("reminders", []):
        te = " &middot; ".join([x for x in [r.get("timing", ""), r.get("escalation", "")] if x])
        h.append('<tr><td><b>%s</b></td><td class="small">%s</td><td class="small muted">%s</td><td class="small">%s</td></tr>'
                 % (_esc(r.get("id", "")), _esc(r.get("trigger", "")), _esc(te).replace("&amp;middot;", "&middot;"), _esc(r.get("message", ""))))
    h.append('</tbody></table></div>')
    h.append('<h2 id="interventions">Behavioural &amp; lifestyle interventions</h2><div class="tablewrap"><table><thead><tr>'
             '<th>Intervention</th><th>Trigger</th><th>Frequency / goal</th><th>Example messages</th></tr></thead><tbody>')
    for i in pn.get("interventions", []):
        fg = " / ".join([x for x in [i.get("frequency", ""), i.get("goal", "")] if x])
        h.append('<tr><td><b>%s</b></td><td class="small">%s</td><td class="small muted">%s</td><td class="small">%s</td></tr>'
                 % (_esc(i.get("id", "")), _esc(i.get("trigger", "")), _esc(fg), _esc(" / ".join(i.get("messages", [])))))
    h.append('</tbody></table></div>')
    al = pn.get("alerts", {})
    h.append('<h2 id="alerts">Alert framework</h2><div class="tablewrap"><table><thead><tr><th>Audience</th><th>Alerts</th></tr></thead><tbody>')
    for aud in ("patient", "care_team", "physician"):
        h.append('<tr><td><b>%s</b></td><td class="small">%s</td></tr>' % (_esc(aud), " &middot; ".join(_esc(x) for x in al.get(aud, []))))
    h.append('</tbody></table></div>')
    for c in pn.get("cds_rules", []):
        h.append('<div class="callout note"><div class="ct">CDS rule &middot; %s &mdash; %s</div>'
                 '<b>IF</b> %s <b>THEN</b> %s</div>'
                 % (_esc(c.get("id", "")), _esc(c.get("name", "")), _esc(" / ".join(c.get("if", []))), " &middot; ".join(_esc(x) for x in c.get("then", []))))
    ps = pn.get("prevention_score", {})
    if ps:
        h.append('<h2 id="prevention-score">%s</h2>' % _esc(ps.get("name", "Prevention Score")))
        h.append('<p class="small">%s</p>' % _esc(ps.get("note", "")))
        h.append('<p class="small"><b>Components:</b> %s</p>' % " &middot; ".join(_esc(x) for x in ps.get("components", [])))
    k = pn.get("kpis", {})
    if k:
        h.append('<h2 id="kpis">KPI &amp; outcome measures</h2><div class="tablewrap"><table><thead><tr>'
                 '<th>Clinical</th><th>Engagement</th><th>Preventive</th><th>Executive</th></tr></thead><tbody><tr>')
        for col in ("clinical", "engagement", "preventive", "executive"):
            h.append('<td class="small"><ul style="margin:0;padding-left:16px">%s</ul></td>'
                     % "".join("<li>%s</li>" % _esc(x) for x in k.get(col, [])))
        h.append('</tr></tbody></table></div>')
    return "Prevention & engagement", "".join(h)

# =================================================================== WEARABLE BASELINE PIPELINE
def build_baseline_pipeline():
    """Wearable baseline ingestion, unification, canonicalisation, formulas & data-quality — the
    fool-proof pipeline from Terra/HealthKit/Health Connect/vendor clouds to a personal baseline and
    the mobile baseline UI. Server-rendered from data/wearable-metrics.json + baseline-sources.json
    + baseline-formulas.json + baseline-dq-rules.json."""
    def L(n):
        try:
            return _load(n)
        except Exception:
            return {}
    M = L("wearable-metrics.json"); SRC = L("baseline-sources.json")
    FM = L("baseline-formulas.json"); DQ = L("baseline-dq-rules.json"); MO = L("baseline-math-ops.json")
    metrics = M.get("metrics", {}); rules = DQ.get("rules", [])
    flow = ('flowchart LR\n'
            '  subgraph SRC["Sources · trust-tiered (D22)"]\n'
            '    direction TB\n'
            '    T["Terra"]\n    HK["Apple HealthKit"]\n    HC["Health Connect"]\n    VC["Vendor clouds<br/>Oura·Whoop·Garmin·Fitbit·Dexcom"]\n'
            '  end\n'
            '  SRC --> IN["Ingest<br/>idempotent · dedup"]\n'
            '  IN --> VAL{"Validate<br/>type · unit · plausibility"}\n'
            '  VAL -->|reject| Q[("Quarantine + log")]\n'
            '  VAL -->|pass| MG["Unify / merge<br/>trust ▸ resolution ▸ recency"]\n'
            '  MG --> CAN["Canonical units (SI)<br/>transform traps"]\n'
            '  CAN --> DQ{"Data-quality gate<br/>8 rule categories"}\n'
            '  DQ -->|winsorize · widen σ · drop| CAN\n'
            '  DQ -->|clean| BL["Robust baseline<br/>median/MAD + EWMA"]\n'
            '  COH[("Cohort prior μ_cohort")] -.->|empirical-Bayes shrink| BL\n'
            '  BL --> Z["Personal z<br/>Doc 03 §2b · κ_resp"]\n'
            '  Z --> UI["Mobile baseline UI"]\n'
            '  Z --> SC["PureScore Stage 2b"]\n'
            '  RAW[("Immutable raw store")] -.->|recompute · re-baseline| BL\n'
            '  classDef k fill:#0c1726,stroke:#2b5a86,color:#cfe0f5;\n'
            '  classDef g fill:#10241c,stroke:#3ad6a0,color:#bdf5e0;\n'
            '  class IN,VAL,MG,CAN,DQ,BL,Z k\n  class UI,SC g')
    h = ['<div class="crumbs"><a href="index.html">Home</a> &rsaquo; Inputs &amp; intake &rsaquo; Wearable baselines</div>',
         '<h1>Wearable Baselines <span class="small muted">&middot; Terra · HealthKit · device clouds &rarr; one fool-proof personal baseline</span></h1>',
         '<p class="lead">How raw wearable data from <b>Terra</b>, <b>Apple HealthKit</b>, <b>Google Health Connect</b> and '
         '<b>vendor cloud APIs</b> (Oura · Whoop · Garmin · Fitbit · Withings · Dexcom · Libre) is ingested, validated, '
         '<b>unified</b>, transformed to <b>canonical units</b>, and turned into a <b>robust personal baseline</b> that '
         'drives the <a class="xref" href="baseline-mob-viz.html">mobile baseline UI</a> and the personal z-score '
         '(<a class="xref" href="03-scoring-formula.html">Doc 03 §2b</a>, κ_resp). Every way the data can be wrong, late, '
         'duplicated or mis-united is a typed rule with a build-time completeness guard. Single sources: '
         '<code>data/wearable-metrics.json · baseline-sources.json · baseline-formulas.json · baseline-dq-rules.json</code>.</p>', ILLUS,
         '<div class="diagram"><div class="dt">Pipeline &mdash; ingest &rarr; validate &rarr; unify &rarr; canonicalise &rarr; baseline &rarr; z &rarr; UI</div>'
         '<pre class="mermaid">%s</pre></div>' % flow]

    # ---- diagram assets (3 hand-built SVGs + 3 mermaid) used across the sections below ----
    svg_anatomy = (
        '<svg viewBox="0 0 680 300" width="100%" style="max-width:680px;background:#0b1320;border:1px solid #1d2c44;border-radius:8px" font-family="system-ui,sans-serif">'
        '<defs><marker id="bah" markerWidth="9" markerHeight="9" refX="4" refY="4" orient="auto"><path d="M0,0 L9,4 L0,8 Z" fill="#3ad6a0"/></marker></defs>'
        '<path d="M60,150 L300,138 L500,128 L624,92 L624,200 L500,182 L300,184 L60,196 Z" fill="#173a55" fill-opacity="0.5" stroke="#2b6f9e" stroke-width="1"/>'
        '<path d="M60,173 L300,161 L500,155 L624,146" fill="none" stroke="#4aa3df" stroke-width="2"/>'
        '<g fill="#9cc7f0"><circle cx="110" cy="166" r="3"/><circle cx="160" cy="150" r="3"/><circle cx="210" cy="178" r="3"/><circle cx="260" cy="158" r="3"/><circle cx="320" cy="170" r="3"/><circle cx="380" cy="150" r="3"/><circle cx="440" cy="168" r="3"/><circle cx="478" cy="152" r="3"/></g>'
        '<line x1="500" y1="44" x2="500" y2="246" stroke="#edb14a" stroke-width="1" stroke-dasharray="4 3"/>'
        '<line x1="500" y1="155" x2="500" y2="124" stroke="#3ad6a0" stroke-width="2" marker-end="url(#bah)"/>'
        '<circle cx="500" cy="116" r="4.5" fill="#3ad6a0" stroke="#0b1320"/>'
        '<text x="64" y="28" fill="#cfe0f5" font-size="13" font-weight="600">value over time</text>'
        '<text x="506" y="40" fill="#edb14a" font-size="11">today</text>'
        '<text x="512" y="118" fill="#3ad6a0" font-size="11">z = (x &#8722; μ)/σ</text>'
        '<text x="556" y="140" fill="#7fb2e6" font-size="11">drift of μ ↗</text>'
        '<text x="556" y="84" fill="#9cc7f0" font-size="10">band widens =</text><text x="556" y="96" fill="#9cc7f0" font-size="10">less confidence</text>'
        '<g font-size="11"><rect x="64" y="270" width="22" height="10" fill="none" stroke="#4aa3df" stroke-width="2"/><text x="92" y="279" fill="#cfe0f5">μ centre</text>'
        '<rect x="168" y="270" width="22" height="10" fill="#173a55" stroke="#2b6f9e"/><text x="196" y="279" fill="#cfe0f5">μ ± k·σ (robust spread)</text>'
        '<circle cx="392" cy="275" r="4" fill="#3ad6a0"/><text x="402" y="279" fill="#cfe0f5">today → personal z</text></g>'
        '</svg>')
    svg_coldstart = (
        '<svg viewBox="0 0 680 280" width="100%" style="max-width:680px;background:#0b1320;border:1px solid #1d2c44;border-radius:8px" font-family="system-ui,sans-serif">'
        '<line x1="60" y1="50" x2="60" y2="230" stroke="#33486a" stroke-width="1"/><line x1="60" y1="230" x2="624" y2="230" stroke="#33486a" stroke-width="1"/>'
        '<path d="M60,230 L147,140 L247,107 L433,84 L624,74" fill="none" stroke="#3ad6a0" stroke-width="2.5"/>'
        '<path d="M60,50 L147,140 L247,173 L433,196 L624,206" fill="none" stroke="#8aa0bd" stroke-width="2" stroke-dasharray="5 4"/>'
        '<line x1="147" y1="50" x2="147" y2="230" stroke="#edb14a" stroke-width="1" stroke-dasharray="3 3"/>'
        '<circle cx="147" cy="140" r="4" fill="#edb14a"/>'
        '<text x="64" y="28" fill="#cfe0f5" font-size="13" font-weight="600">weight on each prior as data accrues</text>'
        '<text x="151" y="64" fill="#edb14a" font-size="11">n = k → 50 / 50</text>'
        '<text x="150" y="108" fill="#8aa0bd" font-size="11">cold-start: cohort prior dominates</text>'
        '<text x="360" y="120" fill="#3ad6a0" font-size="11">mature: personal baseline dominates</text>'
        '<g fill="#6f86a6" font-size="10"><text x="56" y="244">0</text><text x="140" y="244">14</text><text x="240" y="244">30</text><text x="426" y="244">60</text><text x="612" y="244">90</text><text x="300" y="262">days of personal data (n)</text></g>'
        '<g font-size="11"><line x1="64" y1="20" x2="86" y2="20" stroke="#3ad6a0" stroke-width="2.5"/><text x="92" y="24" fill="#cfe0f5">personal = n/(n+k)</text>'
        '<line x1="240" y1="20" x2="262" y2="20" stroke="#8aa0bd" stroke-width="2" stroke-dasharray="5 4"/><text x="268" y="24" fill="#cfe0f5">cohort = k/(n+k)</text></g>'
        '</svg>')
    svg_robust = (
        '<svg viewBox="0 0 680 260" width="100%" style="max-width:680px;background:#0b1320;border:1px solid #1d2c44;border-radius:8px" font-family="system-ui,sans-serif">'
        '<rect x="60" y="98" width="564" height="72" fill="#3a2410" fill-opacity="0.35" stroke="#edb14a" stroke-width="1" stroke-dasharray="5 4"/>'
        '<rect x="60" y="138" width="564" height="24" fill="#10241c" stroke="#3ad6a0" stroke-width="1"/>'
        '<line x1="60" y1="134" x2="624" y2="134" stroke="#edb14a" stroke-width="2"/>'
        '<line x1="60" y1="150" x2="624" y2="150" stroke="#3ad6a0" stroke-width="2"/>'
        '<g fill="#9cc7f0"><circle cx="100" cy="152" r="3"/><circle cx="150" cy="148" r="3"/><circle cx="200" cy="151" r="3"/><circle cx="250" cy="149" r="3"/><circle cx="300" cy="150" r="3"/><circle cx="400" cy="150" r="3"/><circle cx="450" cy="151" r="3"/><circle cx="500" cy="148" r="3"/><circle cx="560" cy="150" r="3"/></g>'
        '<circle cx="350" cy="66" r="5" fill="#f0606e" stroke="#0b1320"/><text x="330" y="56" fill="#f0606e" font-size="11">outlier day</text>'
        '<text x="64" y="26" fill="#cfe0f5" font-size="13" font-weight="600">one bad reading: robust band holds, naive band lurches</text>'
        '<text x="630" y="132" fill="#edb14a" font-size="10" text-anchor="end"></text>'
        '<g font-size="11"><line x1="64" y1="232" x2="86" y2="232" stroke="#3ad6a0" stroke-width="2"/><text x="92" y="236" fill="#cfe0f5">median + MAD — holds</text>'
        '<line x1="300" y1="232" x2="322" y2="232" stroke="#edb14a" stroke-width="2"/><text x="328" y="236" fill="#cfe0f5">mean ± SD — lurches up &amp; widens</text></g>'
        '</svg>')
    merge_tree = ('flowchart TB\n'
        '  M["Same metric, many sources<br/>(resting HR: Oura + Apple Watch + phone)"] --> A{"Trust tier differs?<br/>(D22)"}\n'
        '  A -->|yes| TR["Take higher trust tier<br/>(clinical &gt; consumer &gt; inferential)"]\n'
        '  A -->|equal| B{"Resolution differs?<br/>(continuous vs spot)"}\n'
        '  B -->|yes| RS["Prefer higher-resolution stream"]\n'
        '  B -->|equal| C{"Recency differs?"}\n'
        '  C -->|yes| RC["Prefer the most recent"]\n'
        '  C -->|same window| CF["Conflict rule:<br/>reconcile (median) · flag disagreement · widen σ"]\n'
        '  classDef k fill:#0c1726,stroke:#2b5a86,color:#cfe0f5;\n'
        '  classDef w fill:#3a2410,stroke:#edb14a,color:#ffe6b0;\n'
        '  class M,TR,RS,RC k\n'
        '  class CF w')
    dq_funnel = ('flowchart TB\n'
        '  S["Incoming sample"] --> G1{"Time / time-zone"}\n'
        '  G1 --> G2{"Gaps / missing"} --> G3{"Duplication / overlap"} --> G4{"Value anomalies"}\n'
        '  G4 --> G5{"Units / definitions"} --> G6{"Device / sensor"} --> G7{"Upstream / schema"} --> G8{"Structure / coherence"}\n'
        '  G8 -->|all pass| OK["Clean → into baseline"]\n'
        '  G1 & G3 & G5 & G7 -.->|fail| ACT["quarantine · winsorize · widen confidence · drop"]\n'
        '  G2 & G4 & G6 & G8 -.->|fail| ACT\n'
        '  classDef k fill:#0c1726,stroke:#2b5a86,color:#cfe0f5;\n'
        '  classDef r fill:#241016,stroke:#f0606e,color:#ffd0d6;\n'
        '  class S,OK k\n'
        '  class ACT r')
    lifecycle = ('stateDiagram-v2\n'
        '  [*] --> ColdStart\n'
        '  ColdStart --> Maturing : data accrues (n↑)\n'
        '  Maturing --> Stable : n ≥ n_min, low volatility\n'
        '  Stable --> Drifting : sustained trend in μ\n'
        '  Drifting --> Rebaseline : change-point detected\n'
        '  Rebaseline --> Maturing : new-regime baseline\n'
        '  Stable --> Insufficient : data gap / staleness\n'
        '  Insufficient --> Maturing : data resumes\n'
        '  ColdStart : cohort prior dominates, wide band\n'
        '  Stable : personal μ/σ, tight band\n'
        '  Rebaseline : reset window (illness, training block, device change)')

    # 0 · Metrics at a glance (folded from the retired Baselines (Wearables) page)
    tcls = {"clinical-grade": "b-green", "consumer-validated": "b-acc", "inferential": "b-yellow"}
    h.append('<h2 id="glance">0 &middot; Metrics at a glance</h2>')
    h.append('<p class="small muted">Every wearable metric is scored against a <b>personal baseline</b> (empirical-Bayes μ/σ that '
             'shrinks from the cohort prior toward the patient as data accrues), not a fixed cut-point. Trust tier (D22) caps how far a '
             'signal can move a band; consumer/inferential tiers are informational-only and never set a red/critical without '
             'clinical-grade confirmation. Tier values are canonical in <a class="xref" href="appendix-wearables.html">Wearables (Appendix B)</a>.</p>')
    h.append('<div class="tablewrap"><table><thead><tr><th>Metric</th><th>Trust tier</th><th>Baseline method</th>'
             '<th>Expected range</th><th>Cadence</th><th>Pillars</th></tr></thead><tbody>')
    for (m, layer, tier, q, pil, dev) in WEARABLES:
        meth, rng, cad = _WB_BASELINE.get(m, ("personal EB μ/σ", "personal band", "daily"))
        h.append('<tr><td><b>%s</b></td><td><span class="chip %s">%s</span></td><td class="small">%s</td>'
                 '<td class="small mono">%s</td><td class="small muted">%s</td><td class="small">%s</td></tr>'
                 % (_esc(m), tcls.get(tier, "b-mut"), _esc(tier), _esc(meth), _esc(rng), _esc(cad), _esc(pil)))
    h.append('</tbody></table></div>')
    # What a baseline is — anatomy + EB shrinkage
    h.append('<h2 id="anatomy">What a baseline is</h2>')
    h.append('<p class="small">A baseline is the output triple <code>{centre μ, robust spread σ, confidence, drift}</code>. '
             'Today\'s reading becomes a <b>personal z-score</b> against that triple &mdash; the quantity Stage 2b of the score, the '
             'Trajectory arrow and the Early-warning ladder all read.</p>')
    h.append('<div class="diagram"><div class="dt">Anatomy of a personal baseline</div>%s</div>' % svg_anatomy)
    h.append('<div class="callout note"><div class="ct">How a baseline is built (empirical-Bayes)</div>'
             '<code>μ_i = (n/(n+k))·x̄_personal + (k/(n+k))·μ_cohort</code> &middot; '
             '<code>z = (x − μ)/σ_robust</code>. Cold-start leans on the cohort prior; sensitivity grows with <code>n</code>. '
             'This shrinkage is the same model as Doc 03 §2b (<a class="xref" href="03-scoring-formula.html">scoring formula</a>) '
             'and Doc 05 §5.1.</div>')
    h.append('<div class="diagram"><div class="dt">Cold-start &rarr; maturity &mdash; the baseline shifts from cohort prior to personal as n grows</div>%s</div>' % svg_coldstart)

    # 1 · Sources & unification
    h.append('<h2 id="sources">1 &middot; Sources &amp; unification</h2>')
    h.append('<div class="tablewrap"><table><thead><tr><th>Source</th><th>Ingest</th><th>Trust</th><th>Quirks that bite baselines</th></tr></thead><tbody>')
    for k, s in SRC.get("sources", {}).items():
        h.append('<tr><td><b>%s</b></td><td class="small">%s</td><td class="small muted">%s</td><td class="small">%s</td></tr>'
                 % (_esc(s.get("label", k)), _esc(s.get("ingest", "")), _esc(s.get("trust", "")), _esc(" · ".join(s.get("quirks", [])))))
    h.append('</tbody></table></div>')
    mp = SRC.get("merge_policy", {})
    h.append('<div class="callout note"><div class="ct">Merge policy &mdash; same metric, many sources</div><ol class="small">%s</ol>%s</div>'
             % ("".join("<li>%s</li>" % _esc(x) for x in mp.get("order", [])),
                '<p class="small"><b>Conflict:</b> %s</p>' % _esc(mp.get("conflict", ""))))
    rob = SRC.get("robustness", {})
    h.append('<div class="callout spec"><div class="ct">Robustness contract &mdash; absorbing upstream change</div>'
             '<ul class="small">%s</ul></div>' % "".join("<li>%s</li>" % _esc(x) for x in rob.get("rules", [])))
    h.append('<div class="diagram"><div class="dt">Merge order &mdash; one metric arriving from several sources</div>'
             '<pre class="mermaid">%s</pre></div>' % merge_tree)
    # 2 · Canonical units + metric registry
    h.append('<h2 id="metrics">2 &middot; Canonical metrics registry</h2>')
    h.append('<p class="small muted">Every metric transformed to its canonical unit before any baseline math; SI stored internally. '
             'Groups: <b>individual</b> · <b>aggregated</b> · <b>derived</b> · <b>frequency</b>.</p>')
    h.append('<div class="tablewrap"><table><thead><tr><th>Metric</th><th>Pillar</th><th>Group</th><th>Canonical</th>'
             '<th>Raw&rarr;day</th><th>Sources (native unit &middot; trap)</th><th>Formula</th></tr></thead><tbody>')
    for mk, m in metrics.items():
        srcs = m.get("sources", {})
        scells = []
        for sk in ("terra", "healthkit", "healthconnect", "devicecloud"):
            sv = srcs.get(sk)
            if not sv:
                continue
            trap = sv.get("trap") or sv.get("us_unit")
            scells.append("<b>%s</b> %s%s" % (sk, _esc(sv.get("unit", "")), (' <span class="chip b-red">trap</span>' if trap else "")))
        h.append('<tr id="m-%s"><td><b>%s</b></td><td>%s</td><td>%s</td><td class="mono">%s</td><td class="small muted">%s</td>'
                 '<td class="small">%s</td><td class="small mono">%s</td></tr>'
                 % (_esc(mk), _esc(m.get("label", mk)), _esc(m.get("pillar", "")), _esc(m.get("group", "")),
                    _esc(m.get("canon", "")), _esc(m.get("agg", "")), " &middot; ".join(scells), _esc(m.get("formula", ""))))
    h.append('</tbody></table></div>')
    # the source traps, called out
    traps = []
    for mk, m in metrics.items():
        for sk, sv in m.get("sources", {}).items():
            if isinstance(sv, dict) and sv.get("trap"):
                traps.append((m.get("label", mk), sk, sv["trap"]))
    if traps:
        h.append('<h3>Unit / definition traps (must transform before pooling)</h3><div class="tablewrap"><table><thead><tr>'
                 '<th>Metric</th><th>Source</th><th>Trap</th></tr></thead><tbody>')
        for lab, sk, tr in traps:
            h.append('<tr><td><b>%s</b></td><td class="small muted">%s</td><td class="small">%s</td></tr>' % (_esc(lab), _esc(sk), _esc(tr)))
        h.append('</tbody></table></div>')
    # 3 · Baseline formulas
    h.append('<h2 id="formulas">3 &middot; Baseline formulas</h2>')
    h.append('<p class="small">%s</p>' % _esc(FM.get("_meta", {}).get("output_model", "")))
    h.append('<div class="tablewrap"><table><thead><tr><th>Family</th><th>Definition</th><th>When</th><th>Robustness</th></tr></thead><tbody>')
    for fk, f in FM.get("families", {}).items():
        h.append('<tr><td><b>%s</b>%s</td><td class="small">%s</td><td class="small muted">%s</td><td class="small">%s</td></tr>'
                 % (_esc(f.get("name", fk)), (' <span class="chip b-green">default</span>' if "DEFAULT" in f.get("name", "") else ""),
                    _esc(f.get("def", "")), _esc(f.get("use", "")), _esc(f.get("robust", ""))))
    h.append('</tbody></table></div>')
    dg = FM.get("defaults_by_group", {})
    h.append('<p class="small"><b>Default per group:</b> ' + " &middot; ".join("<b>%s</b> %s" % (_esc(g), _esc(v)) for g, v in dg.items()) + "</p>")
    h.append('<div class="diagram"><div class="dt">Why robust &mdash; median/MAD resists an outlier that mean &plusmn; SD absorbs</div>%s</div>' % svg_robust)
    # 4 · Data-quality rules
    cats = {"time": "Time &amp; time-zone", "gap": "Gaps &amp; missing", "duplication": "Duplication &amp; overlap",
            "value": "Value anomalies", "unit": "Units &amp; definitions", "device": "Device &amp; sensor",
            "upstream": "Upstream / schema", "structure": "Structure &amp; coherence"}
    h.append('<h2 id="dq">4 &middot; Data-quality rules <span class="small muted">&middot; %d rules, %d categories &mdash; the fool-proof layer</span></h2>' % (len(rules), len(cats)))
    h.append('<p class="small muted">Default stance: <b>%s</b>. Each rule: detect &rarr; action &rarr; effect-on-baseline &rarr; confidence impact. '
             'The build guard fails if any metric is left uncovered.</p>' % _esc(DQ.get("_meta", {}).get("default_stance", "")))
    h.append('<div class="diagram"><div class="dt">Data-quality gate &mdash; every sample runs the 8 categories before it can touch the baseline</div>'
             '<pre class="mermaid">%s</pre></div>' % dq_funnel)
    sevc = {"high": '<span class="chip b-red">high</span>', "med": '<span class="chip b-yellow">med</span>', "low": '<span class="chip b-mut">low</span>'}
    for ck, cl in cats.items():
        crules = [r for r in rules if r.get("cat") == ck]
        if not crules:
            continue
        h.append('<h3>%s <span class="small muted">&middot; %d</span></h3>' % (cl, len(crules)))
        h.append('<div class="tablewrap"><table><thead><tr><th>Rule</th><th>Detect</th><th>Action &rarr; effect on baseline</th><th>Sev</th></tr></thead><tbody>')
        for r in crules:
            h.append('<tr><td><b>%s</b><br><span class="small muted">→ %s</span></td><td class="small">%s</td>'
                     '<td class="small">%s <span class="muted">→ %s</span> <span class="muted">(conf: %s)</span></td><td>%s</td></tr>'
                     % (_esc(r.get("name", "")), _esc(r.get("applies", "")), _esc(r.get("detect", "")),
                        _esc(r.get("action", "")), _esc(r.get("effect", "")), _esc(r.get("conf", "")), sevc.get(r.get("sev"), _esc(r.get("sev", "")))))
        h.append('</tbody></table></div>')
    # 4.5 · Baseline lifecycle (operational states)
    h.append('<h2 id="lifecycle">5 &middot; Baseline lifecycle</h2>')
    h.append('<p class="small muted">A baseline is not static: it is born on the cohort prior, matures as personal data accrues, '
             'stabilises, and is <b>re-based</b> when a change-point (illness, a training block, a device swap) shifts the regime. '
             'Re-baselining is the mechanism the engine does not yet implement (see gaps).</p>')
    h.append('<div class="diagram"><div class="dt">Baseline lifecycle &mdash; cold-start &rarr; maturing &rarr; stable &rarr; drift &rarr; re-baseline</div>'
             '<pre class="mermaid">%s</pre></div>' % lifecycle)
    # 6 · Estimator & operational hazards
    mcats = MO.get("_meta", {}).get("categories", {})
    morules = MO.get("rules", [])
    h.append('<h2 id="mathops">6 &middot; Estimator &amp; operational hazards <span class="small muted">&middot; %d &mdash; even with perfect data</span></h2>' % len(morules))
    h.append('<p class="small muted">Distinct from data-quality: ways the robust+adaptive <em>statistic</em> and the <em>runtime</em> that serves it can mislead even when every input is clean.</p>')
    h.append('<div class="callout spec"><div class="ct">Two safety guardrails (highest stakes)</div><ul class="small">')
    for r in morules:
        if r.get("cat") == "safety":
            h.append('<li><b>%s</b> &mdash; %s <span class="muted">→ %s</span></li>' % (_esc(r.get("name", "")), _esc(r.get("detect", "")), _esc(r.get("action", ""))))
    h.append('</ul></div>')
    h.append('<div class="tablewrap"><table><thead><tr><th>Hazard</th><th>Class</th><th>Detect</th><th>Mitigation &rarr; effect</th><th>Sev</th></tr></thead><tbody>')
    for r in morules:
        h.append('<tr><td><b>%s</b></td><td class="small muted">%s</td><td class="small">%s</td>'
                 '<td class="small">%s <span class="muted">→ %s</span></td><td>%s</td></tr>'
                 % (_esc(r.get("name", "")), _esc(mcats.get(r.get("cat"), r.get("cat", ""))), _esc(r.get("detect", "")),
                    _esc(r.get("action", "")), _esc(r.get("effect", "")), sevc.get(r.get("sev"), _esc(r.get("sev", "")))))
    h.append('</tbody></table></div>')
    # 7 · UI connection
    h.append('<h2 id="ui">7 &middot; How the mobile UI connects to the baselines</h2>')
    h.append('<p class="small">The <a class="xref" href="baseline-mob-viz.html">baseline mobile prototype</a> renders the baseline '
             'output triple <code>{center μ, spread σ_robust, confidence, drift}</code> five ways:</p>')
    ui = [("Baseline Band", "one metric vs its personal μ ± k·σ_robust band over time", "{μ, σ_robust}"),
          ("Deviation Scan", "today's personal z per metric, dense morning scan", "z = (x − μ)/σ_robust"),
          ("Body Radar", "polar composite — outward = better-than-baseline, whole-body readiness", "z across metrics → pillars"),
          ("Baseline Drift", "how the baseline centre itself moves week-over-week", "drift_slope of μ"),
          ("Confidence & Trust", "band widens over data gaps — absence = widened uncertainty", "confidence = q_source · exp(−Δt/τ)")]
    h.append('<div class="tablewrap"><table><thead><tr><th>Screen</th><th>Shows</th><th>From the baseline model</th></tr></thead><tbody>')
    for nm, sh, fr in ui:
        h.append('<tr><td><b>%s</b></td><td class="small">%s</td><td class="small mono">%s</td></tr>' % (_esc(nm), _esc(sh), _esc(fr)))
    h.append('</tbody></table></div>')
    # 8 · Gaps
    h.append('<h2 id="gaps">8 &middot; Identified gaps</h2>')
    gaps = ["No live Terra/HealthKit/vendor schema is pinned here — the source paths are illustrative and must be reconciled against current API versions before production.",
            "The robust+adaptive formula params (windows, half-lives, winsor %) are expert-priors, not yet calibrated against labelled data (Doc 14).",
            "Conditioned baselines (per-device, per-context) multiply state — a device/context registry + storage model is specified but not built.",
            "Identity / shared-device disambiguation needs a real biometric-consistency model, not just a flag.",
            "Cross-metric coherence checks need the full physiological constraint set encoded as executable assertions.",
            "The personal-baseline math currently lives as Doc 03 §2b (κ_resp, 30-day) — this pipeline proposes the robust/adaptive upgrade and a re-baselining (change-point) mechanism that the engine does not yet implement."]
    h.append('<ul class="small">%s</ul>' % "".join("<li>%s</li>" % _esc(g) for g in gaps))
    h.append('<p class="small muted">Connects to: <a class="xref" href="03-scoring-formula.html">Doc 03 §2b (personal z)</a> · '
             '<a class="xref" href="appendix-wearables.html">Wearables (trust tiers)</a> · '
             '<a class="xref" href="appendix-wearable-corroboration.html">Wearable corroboration</a> · '
             '<a class="xref" href="06-data-model-and-reference-ranges.html">Doc 06 data model</a> · '
             '<a class="xref" href="baseline-mob-viz.html">Baseline mobile UI</a>.</p>')
    return "Wearable baselines", "".join(h)

# =================================================================== PROGRESSIVE DATA & GRACEFUL DEGRADATION
def build_progressive_data():
    """Progressive data collection & graceful degradation — when any input is missing/stale/decayed/invalid,
    substitute a cohort statistic, lower Accuracy, widen the forecast, expose it in the companion vector, and
    nudge to fill the gap. Server-rendered from data/degradation-model.json + data/data-completeness-nudges.json."""
    def L(n):
        try:
            return _load(n)
        except Exception:
            return {}
    DM = L("degradation-model.json"); NU = L("data-completeness-nudges.json")
    meta = DM.get("_meta", {})
    h = ['<div class="crumbs"><a href="index.html">Home</a> &rsaquo; How scoring works &rsaquo; Progressive data &amp; graceful degradation</div>',
         '<h1>Progressive Data &amp; Graceful Degradation <span class="small muted">&middot; missing &rarr; cohort &rarr; lower accuracy &rarr; nudge</span></h1>',
         '<p class="lead">PureScore never blocks on missing data. When any input &mdash; a lab, a wearable metric, a clinical reading, '
         'a lifestyle answer, a personal baseline, or a goal &mdash; is <b>missing, stale, decayed or invalid</b>, the system '
         'substitutes a <b>cohort statistic</b> (age&times;sex&times;life-stage median/mean, D33) so the score stays continuous, '
         '<b>lowers the reported Accuracy</b>, <b>widens the forecast</b>, <b>flags the substitution in the companion vector</b>, '
         'and <b>nudges the user over time</b> to fill the gap. Single sources: '
         '<code>data/degradation-model.json &middot; data-completeness-nudges.json</code>.</p>', ILLUS,
         '<div class="callout spec"><div class="ct">Safety invariant (load-bearing)</div><p class="small">%s</p></div>' % _esc(meta.get("safety_invariant", ""))]
    # 1 · Data states
    h.append('<h2 id="states">1 &middot; Data states</h2><div class="tablewrap"><table><thead><tr><th>State</th><th>Meaning &amp; effect</th></tr></thead><tbody>')
    for k, v in DM.get("data_states", {}).items():
        h.append('<tr><td><b>%s</b></td><td class="small">%s</td></tr>' % (_esc(k), _esc(v)))
    h.append('</tbody></table></div>')
    # 2 · Per input-type fallback
    h.append('<h2 id="inputs">2 &middot; Cohort fallback by input type</h2>')
    h.append('<p class="small muted">Every input type degrades gracefully to a cohort statistic. <span class="chip b-red">gap</span> = not yet wired in the engine today.</p>')
    h.append('<div class="tablewrap"><table><thead><tr><th>Input type</th><th>Cohort fallback</th><th>Confidence</th><th>Effect</th><th>Nudge</th><th>Today</th></tr></thead><tbody>')
    for k, t in DM.get("input_types", {}).items():
        today = t.get("today", "")
        badge = '<span class="chip b-green">live</span>' if today.startswith("IMPLEMENTED") else '<span class="chip b-red">gap</span>'
        h.append('<tr><td><b>%s</b><br><span class="small muted">%s</span></td><td class="small">%s</td><td class="small mono">%s</td>'
                 '<td class="small">acc&darr; · %s</td><td class="small mono">%s</td><td class="small">%s %s</td></tr>'
                 % (_esc(t.get("label", k)), _esc(t.get("channel", "")), _esc(t.get("fallback", "")), _esc(t.get("confidence", "")),
                    _esc(t.get("forecast_effect", "")), _esc(t.get("nudge", "")), badge, _esc(today)))
    h.append('</tbody></table></div>')
    h.append('<div class="callout note"><div class="ct">Why some inputs were dropped before</div><p class="small">Clinical &amp; wearable markers were '
             'previously <em>dropped</em> when missing (never fabricate a blood pressure). Under the universal-but-safe model they now '
             'cohort-fill for <em>scoring continuity</em> at very low confidence &mdash; but the safety invariant keeps a cohort value from '
             'ever triggering a red/critical pillar or a clinical alert, so the conservatism is preserved where it matters.</p></div>')
    # 3 · Accuracy + forecast
    am = DM.get("accuracy_model", {}); fm = DM.get("forecast_model", {})
    h.append('<h2 id="accuracy">3 &middot; Accuracy &amp; the widening forecast</h2>')
    h.append('<div class="callout spec"><div class="ct">Reported Accuracy</div><p class="small mono">%s</p><p class="small">%s</p><p class="small muted">%s</p></div>'
             % (_esc(am.get("def", "")), _esc(am.get("terms", "")), _esc(am.get("distinct_from", ""))))
    h.append('<div class="callout spec"><div class="ct">Forecast cone</div><p class="small mono">%s</p><p class="small">%s</p><p class="small muted">%s</p></div>'
             % (_esc(fm.get("def", "")), _esc(fm.get("behaviour", "")), _esc(fm.get("withhold", ""))))
    # 4 · Companion extension
    ce = DM.get("companion_extension", {})
    h.append('<h2 id="companion">4 &middot; Companion-vector extension</h2><p class="small">%s</p>' % _esc(ce.get("note", "")))
    h.append('<div class="tablewrap"><table><thead><tr><th>New dimension</th><th>Definition</th><th>Shows the user</th></tr></thead><tbody>')
    for d in ce.get("dims", []):
        h.append('<tr><td><b>%s</b></td><td class="small">%s</td><td class="small muted">%s</td></tr>' % (_esc(d.get("name", "")), _esc(d.get("def", "")), _esc(d.get("shows", ""))))
    h.append('</tbody></table></div>')
    # 5 · Invalid handling
    ih = DM.get("invalid_handling", {})
    h.append('<h2 id="invalid">5 &middot; Invalid-value handling</h2>')
    h.append('<p class="small"><b>Detect:</b> %s<br><b>Action:</b> %s</p><p class="small muted"><b>Gap:</b> %s</p>'
             % (_esc(ih.get("detect", "")), _esc(ih.get("action", "")), _esc(ih.get("gap", ""))))
    # 6 · Data-completeness nudge loop
    cls = NU.get("classes", {}); nudges = NU.get("nudges", []); esc = NU.get("escalation", {})
    h.append('<h2 id="nudges">6 &middot; Data-completeness nudge loop <span class="small muted">&middot; %d CTAs</span></h2>' % len(nudges))
    h.append('<p class="small">%s</p>' % _esc(NU.get("_meta", {}).get("ranking", "")))
    h.append('<p class="small muted">Classes: ' + " &middot; ".join("<b>%s</b> %s" % (_esc(k), _esc(v)) for k, v in cls.items()) + "</p>")
    h.append('<div class="tablewrap"><table><thead><tr><th>CTA</th><th>Class</th><th>Fires when</th><th>Raises</th><th>Effort</th></tr></thead><tbody>')
    for n in nudges:
        h.append('<tr><td><b>%s</b><br><span class="small muted">%s</span></td><td>%s</td><td class="small">%s</td><td class="small">%s</td><td class="small mono">%s</td></tr>'
                 % (_esc(n.get("id", "")), _esc(n.get("cta", "")), _esc(n.get("class", "")), _esc(n.get("trigger", "")), _esc(n.get("raises", "")), _esc(n.get("effort", ""))))
    h.append('</tbody></table></div>')
    h.append('<div class="callout note"><div class="ct">Escalation &amp; continuity</div><p class="small">%s</p><p class="small muted">%s</p></div>'
             % (_esc(esc.get("model", "")), _esc(esc.get("continuity", ""))))
    us = NU.get("user_surface", {})
    if us:
        h.append('<div class="callout spec"><div class="ct">User-facing &ldquo;Data Health&rdquo; surface</div><ul class="small">%s</ul></div>'
                 % "".join("<li>%s</li>" % _esc(x) for x in us.get("shows", [])))
    # 7 · Gaps
    h.append('<h2 id="gaps">7 &middot; Remaining gaps (loop)</h2><ul class="small">'
             '<li>Engine wiring: extend <code>assets/engine.js</code> imputation to wearable/clinical/PRO channels with per-channel <code>q_impute</code>, plus the invalid-value plausibility gate (today only labs impute; engine has no invalid state).</li>'
             '<li>Companion vector: add Cohort-fill %, Accuracy and Forecast-band as live dims in Doc 05 §4 / the calc-explorer, not just this spec.</li>'
             '<li>Per-marker volatility-aware &tau; (recency half-life) so staleness decays at the right rate per metric.</li>'
             '<li>The &ldquo;Data Health&rdquo; user surface + the data-completeness nudge class are specified here but not yet built into the mobile prototype or the nudge engine catalogue (adherence.json).</li>'
             '<li>&Delta;Accuracy-per-effort ranking needs a real information-gain estimate, not the effort heuristic used here.</li></ul>')
    h.append('<p class="small muted">Connects to: <a class="xref" href="06-data-model-and-reference-ranges.html">Doc 06 data model (imputation)</a> · '
             '<a class="xref" href="05-critical-review-and-purescore-2.0.html">Doc 05 §4 companion vector</a> · '
             '<a class="xref" href="wearable-baseline-pipeline.html">Baseline pipeline (cold-start)</a> · '
             '<a class="xref" href="appendix-onboarding.html">Progressive profiling</a> · '
             '<a class="xref" href="appendix-coverage-audit.html">Coverage audit</a> · '
             '<a class="xref" href="11-daily-nudge-engine.html">Nudge engine</a>.</p>')
    return "Progressive data &amp; graceful degradation", "".join(h)

# =================================================================== COHORT-FALLBACK GOVERNANCE (rounds 2-3)
def _render_governance(fname, crumb, h1, lead_html, locked_label, connects):
    try:
        G = _load(fname)
    except Exception:
        G = {}
    decisions = G.get("decisions", []); sections = G.get("sections", [])
    h = ['<div class="crumbs"><a href="index.html">Home</a> &rsaquo; How scoring works &rsaquo; %s</div>' % crumb,
         h1, lead_html, ILLUS,
         '<div class="callout spec"><div class="ct">%s</div><div class="tablewrap"><table><tbody>' % locked_label]
    for d in decisions:
        h.append('<tr><td><b>%s</b> <span class="chip b-green">locked</span></td><td class="small">%s</td></tr>' % (_esc(d.get("k", "")), _esc(d.get("v", ""))))
    h.append('</tbody></table></div></div>')
    for sec in sections:
        h.append('<h2 id="%s">%s</h2>' % (_esc(sec.get("id", "")), _esc(sec.get("title", ""))))
        h.append('<div class="tablewrap"><table><tbody>')
        for r in sec.get("rows", []):
            badge = ' <span class="chip b-green">locked</span>' if r.get("decision") else ""
            h.append('<tr><td style="white-space:nowrap"><b>%s</b>%s</td><td class="small">%s</td></tr>' % (_esc(r.get("k", "")), badge, _esc(r.get("v", ""))))
        h.append('</tbody></table></div>')
    h.append(connects)
    return "".join(h)

def build_cohort_governance():
    """Cohort-fallback governance & safeguards (round 2). From data/cohort-governance.json."""
    h1 = '<h1>Cohort Fallback — Governance &amp; Safeguards <span class="small muted">&middot; round 2: make degradation safe &amp; correct</span></h1>'
    lead = ('<p class="lead">The <a class="xref" href="progressive-data.html">graceful-degradation model</a> substitutes cohort '
            'statistics for missing data. This page is the governance around that: how the cohort is <b>built</b> (k-anonymity, '
            'back-off, drift, out-of-distribution), how missing values are <b>imputed</b> (conditional, not marginal), the '
            '<b>equity</b>, <b>longitudinal-stability</b>, <b>integration/provenance</b> and <b>UAE-locale</b> safeguards. '
            'Single source: <code>data/cohort-governance.json</code>.</p>')
    connects = ('<p class="small muted">Connects to: <a class="xref" href="progressive-data.html">Progressive data &amp; degradation</a> · '
                '<a class="xref" href="degradation-integrity.html">Integrity &amp; safety</a> · '
                '<a class="xref" href="06-data-model-and-reference-ranges.html">Doc 06 data model</a> · '
                '<a class="xref" href="13-cohort-percentiles-and-validation.html">Doc 13 cohort percentiles</a> · '
                '<a class="xref" href="consent-onboarding.html">Consent &amp; onboarding</a>.</p>')
    return "Cohort fallback — governance &amp; safeguards", _render_governance("cohort-governance.json", "Cohort fallback — governance &amp; safeguards", h1, lead, "Locked decisions (round 2)", connects)

def build_degradation_integrity():
    """Cohort-fallback integrity & safety (round 3). From data/degradation-integrity.json."""
    h1 = '<h1>Cohort Fallback — Integrity &amp; Safety <span class="small muted">&middot; round 3: anti-gaming, authenticity, degraded-mode safety</span></h1>'
    lead = ('<p class="lead">The final layer over <a class="xref" href="progressive-data.html">graceful degradation</a> and '
            '<a class="xref" href="cohort-governance.html">cohort governance</a>: stopping the model being <b>gamed</b> or creating a '
            '<b>perverse incentive</b> not to measure, keeping data <b>authentic</b> and the cohort from <b>collapsing</b> on its own '
            'output, keeping users <b>safe while degraded</b>, and meeting <b>care-pathway &amp; regulatory</b> duties. '
            'Single source: <code>data/degradation-integrity.json</code>.</p>')
    connects = ('<p class="small muted">Connects to: <a class="xref" href="progressive-data.html">Progressive data</a> · '
                '<a class="xref" href="cohort-governance.html">Cohort governance</a> · '
                '<a class="xref" href="care-pathways.html">Care pathways</a> · '
                '<a class="xref" href="consent-onboarding.html">Consent &amp; onboarding</a>.</p>')
    return "Cohort fallback — integrity &amp; safety", _render_governance("degradation-integrity.json", "Cohort fallback — integrity &amp; safety", h1, lead, "Locked decisions (round 3)", connects)

# =================================================================== WEARABLE CORROBORATION (closes F4)
def _wear_corr_counts():
    """Per-metric count of question-bank questions whose wearable corroborations resolve to it."""
    try:
        reg = _load("wearable-corroboration.json"); qb = _load("question-bank.json")["questions"]
    except Exception:
        return {}, {}
    alias2id = {a: m["id"] for m in reg["metrics"] for a in m["aliases"]}
    from collections import Counter
    mc = Counter()
    for q in qb:
        seen = set()
        for s in (q.get("perceived_actual", {}).get("corroborated_by", []) or []):
            mid = alias2id.get(str(s).strip())
            if mid:
                seen.add(mid)
        for mid in seen:
            mc[mid] += 1
    return dict(mc), reg

def build_wearable_corroboration():
    counts, reg = _wear_corr_counts(); m = reg["meta"]
    h = ['<div class="crumbs"><a href="index.html">Home</a> › Questions & intake › Wearable corroboration</div>',
         '<h1>Appendix L — Wearable Corroboration <span class="small muted">— question → metric → tolerance (closes F4)</span></h1>',
         '<p class="lead">%s</p>' % _esc(m["idea"]), ILLUS]
    h.append('<div class="diagram"><div class="dt">Perceived ↔ actual, with a typed tolerance</div>'
             '<pre class="mermaid">flowchart LR\n'
             '  SR["self-report answer (perceived)"] --> CMP{"|Δ| &gt; tolerance?"}\n'
             '  WM["wearable metric (actual, Appendix B)"] --> CMP\n'
             '  CMP -->|yes| GAP["perceived↔actual gap (Appendix F)\\nlower Confidence (Doc 05)"]\n'
             '  CMP -->|no| OK["corroborated → higher Confidence"]</pre></div>')
    h.append('<div class="callout note"><div class="ct">Tolerance</div>%s</div>' % _esc(m["tolerance_semantics"]))
    h.append('<div class="callout note"><div class="ct">Normalized</div>%s</div>' % _esc(m["normalizes"]))
    h.append('<p class="small muted">%d canonical metrics resolve all 23 of the bank\'s <code>wearable.*</code> '
             'corroboration refs · <a class="xref" href="appendix-wearable-corroboration.html#spreadsheet">spreadsheet ↗</a></p>' % len(reg["metrics"]))
    h.append('<div class="tablewrap"><table><thead><tr><th>Metric</th><th>Canonical (Appx B)</th><th>Channel</th>'
             '<th>Tolerance</th><th>Questions</th><th>Perceived ↔ actual</th><th>Aliases (normalized)</th></tr></thead><tbody>')
    for mt in sorted(reg["metrics"], key=lambda x: -counts.get(x["id"], 0)):
        h.append('<tr><td class="small"><b>%s</b><br><span class="small muted mono">%s</span></td>'
                 '<td class="small muted">%s</td><td>%s</td><td class="small mono">%s</td>'
                 '<td class="mono small">%d</td><td class="small muted">%s</td><td class="small mono">%s</td></tr>'
                 % (_esc(mt["name"]), _esc(mt["id"]), _esc(mt["appendix_b"]), _chan_chip(mt["channel"]),
                    _esc(mt["tolerance"]), counts.get(mt["id"], 0), _esc(mt["perceived_actual"]),
                    _esc(", ".join(a.replace("wearable.", "") for a in mt["aliases"]))))
    h.append('</tbody></table></div>')
    h.append('<p class="small muted">Source: <code>data/wearable-corroboration.json</code>. Closes audit <b>F4</b> '
             '(<a class="xref" href="appendix-coverage-audit.html">Appendix G</a>) — the wearable-match stage is now '
             'typed &amp; auditable: every <code>wearable.*</code> corroboration links to a canonical metric '
             '(<a class="xref" href="appendix-wearables.html">Appendix B</a>) with a tolerance the perceived-vs-actual '
             'gap engine (<a class="xref" href="appendix-lifestyles.html">Appendix F</a>) applies.</p>')
    return "Wearable corroboration", "".join(h)

def build_grid_wearable_corroboration():
    counts, reg = _wear_corr_counts()
    rows = [{"Metric": mt["name"], "Id": mt["id"], "Canonical (Appx B)": mt["appendix_b"], "Channel": mt["channel"],
             "Tolerance": mt["tolerance"], "Questions": counts.get(mt["id"], 0),
             "Perceived↔actual": mt["perceived_actual"], "Aliases": ", ".join(a.replace("wearable.", "") for a in mt["aliases"])}
            for mt in reg["metrics"]]
    cols = [{"key": "Metric", "label": "Metric", "filter": "text"}, {"key": "Id", "label": "Id"},
            {"key": "Canonical (Appx B)", "label": "Canonical (Appx B)", "filter": "text"}, {"key": "Channel", "label": "Channel"},
            {"key": "Tolerance", "label": "Tolerance", "filter": "text"}, {"key": "Questions", "label": "Questions", "type": "num"},
            {"key": "Perceived↔actual", "label": "Perceived ↔ actual", "filter": "text"}, {"key": "Aliases", "label": "Aliases", "filter": "text"}]
    return _grid_page("Wearable-corroboration grid", "Wearable corroboration — spreadsheet",
                      "Every wearable corroboration metric, tolerance &amp; question count, flat &amp; sortable. Source:",
                      "appendix-wearable-corroboration.html", "Wearable corroboration", _grid("g-wc", cols, rows), "Wearable-corroboration grid")
