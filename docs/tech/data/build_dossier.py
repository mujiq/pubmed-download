# -*- coding: utf-8 -*-
"""Merge dossier-core.json (sequences/c4/api) + dossier-<area>.json (agent-curated per-class)
+ the BEHEMOTH spec (wiki_content._BM_*) into a single dossier.json that the per-class
explorer and the 5 index pages render from. Re-runnable: python3 build_dossier.py
All illustrative — re-verify before production (README §5.6)."""
import json, os, glob, re, sys
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))          # docs/tech on path
import wiki_content as C

core = json.load(open(os.path.join(HERE, "dossier-core.json"), encoding="utf-8"))
curated = {}
for f in sorted(glob.glob(os.path.join(HERE, "dossier-*.json"))):
    if os.path.basename(f) == "dossier-core.json": continue
    for cn, c in json.load(open(f, encoding="utf-8"))["classes"].items():
        curated[cn] = c

AREA = {k: {"label": l, "fill": fa, "stroke": s} for k, l, fa, s in C._BM_AREAS}
seq_by_class, api_by_class = {}, {}
for s in core["sequences"]:
    for cn in s.get("classes", []): seq_by_class.setdefault(cn, []).append(s["id"])
for a in core["api"]:
    for cn in a.get("classes", []): api_by_class.setdefault(cn, []).append(a["id"])

rel_out, rel_in = {}, {}
for s, op, d, lab in C._BM_RELATIONS:
    rel_out.setdefault(s, []).append({"op": op, "to": d, "label": lab})
    rel_in.setdefault(d, []).append({"op": op, "from": s, "label": lab})

STATEFUL = {  # classes with a notable lifecycle (linked to states page)
 "PatientState": "Baseline ⇄ AcuteMode (override + hysteresis revert)",
 "EarlyWarning": "none → Watch → Advisory → Alert (multivariate/confirmed)",
 "Marker": "observed → plausible? → confirmed | reconfirm (D14 gate)",
 "Question": "eligible → asked → answered → stale → refresh (by cadence)",
 "ModelVersion": "draft → validated (gates) → active → superseded",
 "AcuteEvent": "triggered → acute-mode → recovering → reverted",
}

def num_guess(name):
    n = name.lower()
    return "numeric" if any(t in n for t in ("score","value","r_","b_","w_","mu","sigma","z_","rho","gamma","delta","phi","cap","count","total","min","max","level","cov","kappa","lambda")) else "text"

def auto_table(name, members):
    cols = [{"name": "id", "type": "uuid", "key": "PK", "note": ""},
            {"name": "patient_id", "type": "uuid", "key": "FK->patients(id)", "note": ""}]
    for m in members:
        cm = re.sub(r"[^a-z0-9]+", "_", m.lower()).strip("_") or "field"
        cols.append({"name": cm, "type": num_guess(m), "key": "", "note": ""})
    cols.append({"name": "computed_at", "type": "timestamptz", "key": "IDX", "note": ""})
    return {"name": re.sub(r"(?<!^)(?=[A-Z])", "_", name).lower() + "s", "columns": cols,
            "indexes": ["(patient_id, computed_at)"], "notes": "auto-derived skeleton — refine in production"}

classes = {}
for name, area, stereo, members in C._BM_CLASSES:
    cur = curated.get(name, {})
    a = AREA.get(area, {"label": area, "fill": "#475569", "stroke": "#ccc"})
    seqrefs = sorted(set(seq_by_class.get(name, []) + cur.get("seqRefs", [])))
    apirefs = sorted(set(api_by_class.get(name, []) + cur.get("apiRefs", [])))
    classes[name] = {
        "name": name, "area": area, "areaLabel": a["label"], "areaColor": a["fill"],
        "doc": stereo.strip("«»"), "tier": "core" if name in curated else "structural",
        "purpose": cur.get("purpose", ""),
        "fields": members,
        "relationsOut": rel_out.get(name, []), "relationsIn": rel_in.get(name, []),
        "c4": core["c4"]["classMap"].get(name, ""),
        "seqRefs": seqrefs, "apiRefs": apirefs,
        "table": cur.get("table") or auto_table(name, members),
        "stories": cur.get("stories", []),
        "invariants": cur.get("invariants", []),
        "decisions": cur.get("decisions", []),
        "state": STATEFUL.get(name, ""),
    }

out = {
    "meta": {"name": "PureScore Engineering Dossier", "flag": core["meta"]["flag"],
             "total_classes": len(classes), "core_classes": len(curated),
             "sequences": len(core["sequences"]), "api": len(core["api"]),
             "containers": len(core["c4"]["containers"])},
    "sequences": core["sequences"], "c4": core["c4"], "api": core["api"], "classes": classes,
}
json.dump(out, open(os.path.join(HERE, "dossier.json"), "w", encoding="utf-8"), ensure_ascii=False, indent=1)
nstories = sum(len(c["stories"]) for c in classes.values())
print("dossier.json:", len(classes), "classes |", len(curated), "core curated |", nstories, "user stories |",
      len(core["sequences"]), "sequences |", len(core["api"]), "endpoints")
