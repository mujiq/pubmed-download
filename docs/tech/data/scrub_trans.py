# -*- coding: utf-8 -*-
"""One-shot: remove trans references from the data layer and collapse gender->sex (Male/Female).
Operates on parsed JSON so it can't corrupt structure. Re-run-safe."""
import json, os
H = os.path.dirname(os.path.abspath(__file__))
TRASH = {"transfem", "transmasc", "gender_transition"}

def load(f): return json.load(open(os.path.join(H, f), encoding="utf-8"))
def save(f, d): json.dump(d, open(os.path.join(H, f), "w", encoding="utf-8"), ensure_ascii=False, indent=1)

def _keep(x):
    if isinstance(x, str): return x not in TRASH
    if isinstance(x, dict): return x.get("id") not in TRASH and x.get("key") not in TRASH
    return True

def clean_personas(obj):
    """Recursively drop trash items (string ids or {id|key}) from any 'personas'/'archetypes' lists."""
    if isinstance(obj, dict):
        for k, v in list(obj.items()):
            if k in ("personas", "archetypes") and isinstance(v, list):
                obj[k] = [x for x in v if _keep(x)]
                for x in obj[k]: clean_personas(x)
            else:
                clean_personas(v)
    elif isinstance(obj, list):
        for x in obj: clean_personas(x)

# ---- qb-00-core: drop Q_CORE_GENDER, make Q_CORE_SEX binary, drop its gender trigger
d = load("qb-00-core.json")
d["questions"] = [q for q in d["questions"] if q["id"] != "Q_CORE_GENDER"]
for q in d["questions"]:
    if q["id"] == "Q_CORE_SEX":
        q["text"] = "What is your sex?"
        q["responses"] = [r for r in q["responses"] if r["label"] in ("Male", "Female")]
        q["dependencies"]["triggers"] = [t for t in q["dependencies"].get("triggers", []) if t.get("ask") != "Q_CORE_GENDER"]
clean_personas(d)
save("qb-00-core.json", d)

# ---- qb-05: drop the gender-affirming HRT question
d = load("qb-05-endocrine.json")
d["questions"] = [q for q in d["questions"] if "gender-affirming hormone" not in q.get("text", "").lower()]
clean_personas(d)
save("qb-05-endocrine.json", d)

# ---- qb-11: just clean persona tags
d = load("qb-11-derm-pain.json"); clean_personas(d); save("qb-11-derm-pain.json", d)

# ---- persona-axes: drop gender_transition archetype + clean signals
d = load("persona-axes.json")
d["archetypes"] = [a for a in d["archetypes"] if a.get("key") not in TRASH]
for ax in d.get("axes", []):
    for key in ("perceived_signal", "actual_signal"):
        if key in ax: ax[key] = [s for s in ax[key] if "Q_CORE_GENDER" not in s and "trans" not in s.lower()]
clean_personas(d)
save("persona-axes.json", d)

# ---- goals: drop the HRT-transition goal + clean tags
d = load("goals.json")
arr = d.get("goals", d if isinstance(d, list) else [])
goals = [g for g in arr if g.get("id") != "GL-ENDO-03" and "gender-affirming" not in json.dumps(g).lower()]
if isinstance(d, dict): d["goals"] = goals
else: d = goals
clean_personas(d)
save("goals.json", d)

# ---- persona-matrix: drop trans personas, the archetype, and the HRT-only signals
d = load("persona-matrix.json")
for key in ("personas", "archetypes"):
    if key in d: d[key] = [x for x in d[key] if x.get("id") not in TRASH]
if "signals" in d:
    d["signals"] = [s for s in d["signals"] if s.get("id") not in ("estrogen_hrt", "testosterone_hrt")]
clean_personas(d)
save("persona-matrix.json", d)

print("scrubbed:", "qb-00-core (gender→sex M/F), qb-05 (HRT q), qb-11, persona-axes, goals, persona-matrix")
# report any residual
import glob, re
res = []
for f in glob.glob(os.path.join(H, "*.json")):
    if os.path.basename(f) in ("question-bank.json", "dossier.json"): continue
    t = open(f, encoding="utf-8").read()
    if re.search(r"transfem|transmasc|gender_transition|gender-affirming|Q_CORE_GENDER|Trans (wom|man)", t):
        res.append(os.path.basename(f))
print("residual source files still mentioning trans:", res or "none")
