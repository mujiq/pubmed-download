#!/usr/bin/env python3
"""Apply governing-standard range corrections across the THREE canonical band sources so they no
longer drift: data/pillars.json (appendix display g/y/r/source), data/calc-graph.json (engine th /
unit / default), data/range-variations.json (resolver base + variation citations).

UAE/Gulf-first governing standard (UK NICE / ESC / IDF South-Asian + SI units). All value changes
are PROPOSED — pending clinician sign-off (tracked via data/remediations.json → clinical-flags.json).
Idempotent: sets targets, re-runnable, prints every change. Illustrative — NOT a clinical sign-off.
"""
import json, os, sys
HERE = os.path.dirname(os.path.abspath(__file__))
def L(n): return json.load(open(os.path.join(HERE, "data", n), encoding="utf-8"))
def W(n, d):
    p = os.path.join(HERE, "data", n)
    json.dump(d, open(p, "w", encoding="utf-8"), ensure_ascii=False, indent=1); open(p, "a").write("\n")

# pillars.json display rows: [name, unit, tier, two_sided, green, yellow, red, w, source, critical?]
# (g,y,r,src) per marker name. None = leave unchanged.
PILLARS = {
 "HbA1c":               ("<5.7 (39 mmol/mol)", "5.7–6.4 (39–47)", "≥6.5 (48)",            "ADA SoC 2024"),
 "Systolic BP":         ("90–119",             "120–139 (or <90)", "≥140 or <85",          "NICE NG136"),
 "Diastolic BP":        ("60–79",              "80–89",            "≥90 or <55",           "NICE NG136"),
 "LDL-C":               ("<100 (<70 high-risk; <55 very-high)", "100–159", "≥160",         "ESC/EAS 2019"),
 "ApoB":                ("<90 (<80 high-risk; <65 very-high)",  "90–119",  "≥120",         "ESC/EAS 2019"),
 "Lp(a)":               ("<75 nmol/L",         "75–125",           "≥125 (≥430 very-high)", "ESC/EAS 2022"),
 "TSH":                 ("0.4–4.0",            "4.0–4.5 / 0.3–0.4", ">4.5 or <0.3",         "ATA 2014"),
 "eGFR (creatinine)":   ("≥60 (G1–G2)",        "45–59 (G3a)",       "<45 (G3b–G5)",         "KDIGO 2024"),
 "Triglycerides":       ("<150 (1.7 mmol/L)",  "150–499",           "≥500 (pancreatitis)",  "NCEP ATP III"),
 "Waist circumference": ("<90 M / <80 F (S-Asian/Gulf)", "90–99 M / 80–87 F", "≥100 M / ≥88 F", "IDF 2006"),
 "25-OH Vitamin D":     ("≥20 ng/mL (≥50 nmol/L)", "12–19 (30–49 nmol/L)", "<12 (<30 nmol/L deficient)", "IOM 2011"),
}
# calc-graph engine: key -> {th, unit, default} (only keys present in markers)
CALC = {
 "apob":  {"th": [100, 120, "hi"]},                 # risk-tiered to ESC/EAS green <100
 "waist": {"th": [90, 100, "hi"]},                  # South-Asian/Gulf male cut
 "vitd":  {"th": [20, 12, "lo"], "default": 20},    # IOM/SACN sufficiency; Gulf-plausible default
 "egfr":  {"unit": "mL/min/1.73m²"},                # was "mL/min" (flag: unit wrong)
}
# range-variations base: name -> {g,y,r,cite} (set provided keys only)
RV_BASE = {
 "Systolic BP":       {"cite": "NICE NG136"},
 "HbA1c":             {"g": "<5.7% (39 mmol/mol)", "y": "5.7–6.4% (39–47)", "r": "≥6.5% (48)", "cite": "ADA SoC 2024"},
 "LDL-C":             {"g": "<100 (<70 high; <55 very-high)", "cite": "ESC/EAS 2019"},
 "ApoB":              {"g": "<90 (<80 high; <65 very-high)", "cite": "ESC/EAS 2019"},
 "TSH":               {"g": "0.4–4.0", "y": "4.0–4.5 / 0.3–0.4", "r": ">4.5 or <0.3", "cite": "ATA 2014"},
 "eGFR (creatinine)": {"g": "≥60 (G1–G2)", "y": "45–59 (G3a)", "r": "<45 (G3b–G5)", "cite": "KDIGO 2024"},
 "Waist circumference": {"g": "<90 M / <80 F (S-Asian/Gulf)", "cite": "IDF 2006"},
}
# range-variations variation citation fixes: (name, dim, key_substr) -> new cite
RV_VAR_CITE = {
 ("Systolic BP", "life_stage", "Pregnancy"): "NICE NG133",
}

def main():
    chg = []
    # --- pillars.json ---
    pil = L("pillars.json")
    for pid, name, desc, rows in pil["pillars"]:
        for r in rows:
            if r[0] in PILLARS:
                g, y, rr, src = PILLARS[r[0]]
                for idx, val in ((4, g), (5, y), (6, rr), (8, src)):
                    if val is not None and r[idx] != val:
                        chg.append("pillars %-22s [%d] %r → %r" % (r[0], idx, r[idx], val)); r[idx] = val
    W("pillars.json", pil)
    # --- calc-graph.json ---
    cg = L("calc-graph.json")
    for k, patch in CALC.items():
        m = cg["markers"].get(k)
        if not m: chg.append("!! calc-graph missing marker %s" % k); continue
        for f, val in patch.items():
            if m.get(f) != val:
                chg.append("calc-graph %-8s %s %r → %r" % (k, f, m.get(f), val)); m[f] = val
    W("calc-graph.json", cg)
    # --- range-variations.json ---
    rv = L("range-variations.json")
    for name, patch in RV_BASE.items():
        m = rv["markers"].get(name)
        if not m: chg.append("!! rv missing %s" % name); continue
        b = m.setdefault("base", {})
        for f, val in patch.items():
            if b.get(f) != val:
                chg.append("rv-base %-20s %s %r → %r" % (name, f, b.get(f), val)); b[f] = val
    for (name, dim, ks), cite in RV_VAR_CITE.items():
        m = rv["markers"].get(name) or {}
        for v in m.get("var", []):
            if v.get("dim") == dim and ks in str(v.get("key", "")):
                if v.get("cite") != cite:
                    chg.append("rv-var %-16s/%s %r → %r" % (name, ks, v.get("cite"), cite)); v["cite"] = cite
    W("range-variations.json", rv)

    print("range corrections: %d change(s)" % len([c for c in chg if not c.startswith("!!")]))
    for c in chg: print("  ", c)
    return 0

if __name__ == "__main__":
    sys.exit(main())
