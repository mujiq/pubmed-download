# One-off: extract inline Python catalogs from wiki_content.py into canonical JSON.
# Run from docs/tech:  python3 data/_extract_catalogs.py
# Safe & mechanical — imports the live module and dumps the resolved values, so the
# JSON is identical data (tuples become arrays; LIKERT_0_3 etc. resolve to their values).
import json, os, sys
HERE = os.path.dirname(os.path.abspath(__file__))          # docs/tech/data
sys.path.insert(0, os.path.dirname(HERE))                   # docs/tech
import wiki_content as C

# name -> (filename, "key for object-wrapped" or None for bare array)
CATALOGS = {
    "PILLARS":     ("pillars.json",        "pillars"),
    "MODIFIERS":   ("modifiers.json",      "modifiers"),
    "WEARABLES":   ("wearables.json",      "wearables"),
    "INSTRUMENTS": ("instruments.json",    "instruments"),
    "PERSONAS":    ("personas.json",       "personas"),
    "PILLAR_W":    ("pillar-weights.json", "weights"),
    "CONSTANTS":   ("constants.json",      "constants"),
    "QSOURCE":     ("qsource.json",        "qsource"),
}

for attr, (fname, key) in CATALOGS.items():
    data = getattr(C, attr)
    payload = {"_meta": {"source": "wiki_content.py:%s" % attr,
                         "note": "Canonical metadata. Edit here, not in Python."},
               key: data}
    path = os.path.join(HERE, fname)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=1)
        f.write("\n")
    print("wrote", fname, "-", len(data), "entries")
print("done")
