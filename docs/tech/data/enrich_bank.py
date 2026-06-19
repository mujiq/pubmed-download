# -*- coding: utf-8 -*-
"""Merge the 12 qb-*.json chunks AND enrich them into question-bank.json:
  - ref code  DOM-NNN  (3-letter domain prefix + sequence within section)
  - seq (within section), order (global intake position)
  - prev_ref / next_ref  (prev/next walk of the whole questionnaire)
  - prerequisites[] / unlocks[]  (dependency DAG from show_if + triggers + sex/age/gender gating)
  - applicability {sex, age_band, life_stage, personas, gated, stream}
Re-runnable: python3 enrich_bank.py   (regenerates question-bank.json from chunks)
All values illustrative — re-verify before production (README §5.6)."""
import json, glob, os, collections, heapq

HERE = os.path.dirname(os.path.abspath(__file__))
ORDER_DOMAINS = [("CORE","COR"),("CARDIOMETABOLIC","CMB"),("RENAL_HEPATIC","RHP"),
 ("INFLAMM_IMMUNE_ALLERGY","IMM"),("HEMATOLOGIC","HEM"),("ENDOCRINE_HORMONAL","END"),
 ("BODYCOMP_MSK","MSK"),("NUTRITION_GI","NUT"),("SLEEP_FATIGUE","SLP"),
 ("FITNESS_ACTIVITY","FIT"),("MENTAL_COGNITIVE_SOCIAL","MND"),("DERM_AESTHETIC_PAIN","DRM")]
DCODE = dict(ORDER_DOMAINS)

# ---- load chunks
chunks = {}
for f in sorted(glob.glob(os.path.join(HERE, "qb-*.json"))):
    d = json.load(open(f, encoding="utf-8"))
    chunks[d["domain"]] = d["questions"]

allq, qById = [], {}
for dom, code in ORDER_DOMAINS:
    for i, q in enumerate(chunks.get(dom, [])):
        q["_idx"] = i; q["domain_code"] = code
        qById[q["id"]] = q; allq.append(q)

def norm_sex(dims):
    sx = dims.get("sex", ["all"])
    if not isinstance(sx, list): sx = [sx]
    out = set()
    for s in sx:
        s = str(s).strip().lower()
        if s in ("all", "any", ""): out.add("all")
        elif s in ("male", "m", "men"): out.add("male")
        elif s in ("female", "f", "women"): out.add("female")
        else: out.add(s)
    return ["all"] if ("all" in out or not out) else sorted(out)

def age_bounds(dims):
    a = dims.get("age", "all")
    if isinstance(a, dict): return int(a.get("min", 0)), int(a.get("max", 120))
    return 0, 120

# ---- prerequisites (DAG)
for q in allq: q["_pre"] = set()
dangling = []
for q in allq:
    dep = q.get("dependencies", {}) or {}
    for c in (dep.get("show_if", []) or []):
        qq = c.get("q")
        if qq:
            (q["_pre"].add(qq) if qq in qById else dangling.append((q["id"], "show_if", qq)))
    for c in (dep.get("skip_if", []) or []):
        qq = c.get("q")
        if qq and qq not in qById: dangling.append((q["id"], "skip_if", qq))
# triggers: A unlocks B  ⇒  B requires A
for q in allq:
    for t in ((q.get("dependencies", {}) or {}).get("triggers", []) or []):
        ask = t.get("ask")
        if not ask: continue
        if ask in qById: qById[ask]["_pre"].add(q["id"])
        else: dangling.append((q["id"], "trigger", ask))
# divergence gating: sex/age-gated questions require the relevant CORE anchor (sex-binary model)
ANCH = {"sex": "Q_CORE_SEX", "age": "Q_CORE_AGE"}
for q in allq:
    dims = q.get("dimensions", {}) or {}
    if norm_sex(dims) != ["all"] and ANCH["sex"] in qById and q["id"] != ANCH["sex"]:
        q["_pre"].add(ANCH["sex"])
    if age_bounds(dims) != (0, 120) and ANCH["age"] in qById and q["id"] != ANCH["age"]:
        q["_pre"].add(ANCH["age"])

# ---- topological sequence within each domain (priority, then original order as tie-break)
def topo(domqs):
    ids = {q["id"] for q in domqs}
    pos = {q["id"]: (q.get("priority", 9), q["_idx"]) for q in domqs}
    adj = collections.defaultdict(list); indeg = {q["id"]: 0 for q in domqs}
    for q in domqs:
        for p in q["_pre"]:
            if p in ids and p != q["id"]:
                adj[p].append(q["id"]); indeg[q["id"]] += 1
    h = [(pos[i], i) for i in ids if indeg[i] == 0]; heapq.heapify(h)
    out, seen = [], set()
    while h:
        _, cur = heapq.heappop(h)
        if cur in seen: continue
        seen.add(cur); out.append(cur)
        for nb in adj[cur]:
            indeg[nb] -= 1
            if indeg[nb] == 0: heapq.heappush(h, (pos[nb], nb))
    leftover = sorted((i for i in ids if i not in seen), key=lambda i: pos[i])  # cycle fallback
    return out + leftover

ordered = []
for dom, code in ORDER_DOMAINS:
    domqs = [q for q in allq if q["domain"] == dom]
    seqids = topo(domqs)
    for n, qid in enumerate(seqids, 1):
        q = qById[qid]; q["seq"] = n; q["ref"] = "%s-%03d" % (code, n)
        ordered.append(q)

# ---- global chain prev/next + applicability + resolve DAG to refs
ref_by_id = {q["id"]: q["ref"] for q in allq}
for gi, q in enumerate(ordered):
    q["order"] = gi + 1
    q["prev_ref"] = ordered[gi - 1]["ref"] if gi > 0 else None
    q["next_ref"] = ordered[gi + 1]["ref"] if gi < len(ordered) - 1 else None
    q["prev_id"] = ordered[gi - 1]["id"] if gi > 0 else None
    q["next_id"] = ordered[gi + 1]["id"] if gi < len(ordered) - 1 else None
for q in allq:
    dims = q.get("dimensions", {}) or {}
    lo, hi = age_bounds(dims)
    ls = set()
    for p in dims.get("personas", []):
        ls.update({"pregnancy": ["pregnancy"], "menopause": ["menopause"], "elderly": ["older_adult"]}.get(p, []))
    q["applicability"] = {
        "sex": norm_sex(dims), "age_min": lo, "age_max": hi,
        "life_stage": sorted(ls), "personas": dims.get("personas", ["all"]),
        "gated": bool((q.get("dependencies", {}) or {}).get("show_if")), "stream": q.get("stream", "LIFE")}
    q["prerequisites"] = sorted({ref_by_id[p] for p in q["_pre"] if p in ref_by_id})
    unl = set()
    for t in ((q.get("dependencies", {}) or {}).get("triggers", []) or []):
        if t.get("ask") in ref_by_id: unl.add(ref_by_id[t["ask"]])
    q["unlocks"] = sorted(unl)
    for k in ("_pre", "_idx"): q.pop(k, None)

# ---- write
conds = sorted({c for q in allq for c in q.get("conditions", []) if c != "all"})
personas_all = sorted({p for q in allq for p in q.get("applicability", {}).get("personas", []) if p != "all"})
meta = {
 "name": "PureScore Lifestyle & Condition Question Bank",
 "version": "1.1",
 "flag": "illustrative-reverify — design documentation, not a validated medical device (README §5.6)",
 "source": "OMICS/SelfDecode-style assessments + PureScore Docs 02/03/04/07/12/16/18",
 "total_questions": len(allq), "total_conditions": len(conds),
 "ref_scheme": "DOM-NNN — 3-letter domain code + sequence within section",
 "domain_codes": [{"domain": d, "code": c, "count": len([q for q in allq if q["domain"] == d])} for d, c in ORDER_DOMAINS],
 "pillars": ["CV","MET","REN","HEP","INF","HEM","ENDO","BCM","NUT","SLP","FIT","MCS"],
 "reservoirs": ["atherogenic_burden","vascular_bp_load","cardiorespiratory_reserve","glycemic_burden","adiposity",
   "hepatic_fat","renal_reserve","inflammatory_load","oxygen_delivery_reserve","iron_reserve","allostatic_load",
   "bone_reserve","muscle_reserve","micronutrient_reserve","sleep_debt"],
 "axes": ["activity","diet","sleep","substance","stress","social","lifestage","condition_load","metabolic",
   "environment","occupation","anthropometric","genetic_familial","cognition","reproductive","aesthetic","pain"],
 "personas": personas_all,
 "cadence_values": ["Core","quarterly","annual","biennial","once","on-trigger"],
 "refresh_values": ["static","slow","periodic","dynamic","event"],
 "priority_values": [1,2,3,4,5],
 "conditions": conds,
 "dangling_references": [{"from": a, "field": b, "missing": c} for a, b, c in dangling],
}
json.dump({"meta": meta, "questions": ordered}, open(os.path.join(HERE, "question-bank.json"), "w", encoding="utf-8"),
          ensure_ascii=False, indent=1)
print("questions:", len(allq), "| sections:", len(ORDER_DOMAINS), "| conditions:", len(conds),
      "| personas:", len(personas_all), "| dangling refs:", len(dangling))
if dangling:
    for d in dangling[:25]: print("   dangling:", d)
