#!/usr/bin/env python3
"""Generate data/cohort-percentiles.json — the illustrative cohort×marker percentile matrix for the
showcase demo. Data-driven from calc-graph.json (markers + base default/th/unit) with documented,
illustrative demographic shift rules (age × sex × ethnicity × life-stage). NOT real cohort statistics —
re-derive from NHANES/UK-Biobank-class data + local UAE cohorts before production (README §5.6).

Run: python3 data/_gen_cohort_percentiles.py   (writes data/cohort-percentiles.json)
"""
import json
import os

HERE = os.path.dirname(os.path.abspath(__file__))
COHORT_VERSION = "2026.06-demo"

AGE_BANDS = [("child", 8), ("adolescent", 15), ("18-39", 28), ("40-64", 52), ("65-79", 72), ("80+", 85)]
SEXES = ["male", "female"]
ETHNICITIES = ["emirati_gulf_arab", "south_asian", "other_arab", "filipino_se_asian", "african", "iranian", "western"]
# life_stage overlay: pregnancy only applies to female of reproductive age
PREG_BANDS = {"18-39", "40-64"}

# illustrative cell sizes (k-anonymity demo: cells with n<20 are unusable → back-off/OOD)
ETH_N = {"western": 900, "south_asian": 1300, "emirati_gulf_arab": 650, "other_arab": 420,
         "filipino_se_asian": 520, "african": 150, "iranian": 70}

# AGE trend per marker: fractional change per decade from the young-adult reference (age 28). Signed.
AGE_TREND = {"sbp": 0.045, "hba1c": 0.03, "glu": 0.025, "apob": 0.03, "ldl": 0.03, "tg": 0.02,
             "crp": 0.02, "uacr": 0.05, "fib4": 0.05, "egfr": -0.06, "hdl": -0.01, "vo2": -0.08,
             "almi": -0.045, "tscore": -0.09, "hb": -0.01, "rhr": 0.005, "vitd": -0.01}
# SEX shift (female relative to male) — fractional unless noted; from the range-variation sex bands
SEX_F = {"hdl": 0.18, "hb": -0.11, "ferr": -0.42, "waist": -0.10, "rhr": 0.05, "almi": -0.18,
         "vo2": -0.18, "sbp": -0.02, "egfr": 0.04, "tg": -0.05, "crp": 0.05, "vitd": -0.05}
# ETHNICITY shift vs western reference (fractional), key markers only
ETH_SHIFT = {
    "south_asian": {"apob": 0.08, "ldl": 0.05, "hdl": -0.10, "hba1c": 0.05, "glu": 0.04, "tg": 0.10, "vitd": -0.20},
    "emirati_gulf_arab": {"hba1c": 0.06, "glu": 0.05, "vitd": -0.30, "apob": 0.04, "waist": 0.05, "tg": 0.05},
    "other_arab": {"hba1c": 0.03, "glu": 0.03, "vitd": -0.20, "apob": 0.02},
    "filipino_se_asian": {"hba1c": 0.03, "vitd": -0.10, "hdl": -0.04},
    "african": {"sbp": 0.05, "hba1c": 0.04, "vitd": -0.15, "egfr": 0.03},
    "iranian": {"vitd": -0.15, "tg": 0.05},
    "western": {},
}
# markers that are not meaningfully defined in childhood (use pediatric-illustrative flag + wider spread)
PEDI_FLAG_BANDS = {"child", "adolescent"}


def reference(mk):
    """Base p50 + spread for a marker from calc-graph default/th."""
    base = mk.get("default")
    th = mk.get("th") or []
    nums = [x for x in th if isinstance(x, (int, float))]
    if base is None:
        base = (nums[0] + nums[1]) / 2 if len(nums) >= 2 else (nums[0] if nums else 1.0)
    if len(nums) >= 2:
        sigma = abs(nums[1] - nums[0]) / 2.0 or 0.18 * abs(base)
    else:
        sigma = 0.18 * abs(base) or 1.0
    return float(base), float(sigma)


def main():
    cg = json.load(open(os.path.join(HERE, "calc-graph.json")))
    markers = cg["markers"]
    rows = []
    for ab, age in AGE_BANDS:
        decades = (age - 28) / 10.0
        for sex in SEXES:
            for eth in ETHNICITIES:
                life_stages = ["general"]
                if sex == "female" and ab in PREG_BANDS:
                    life_stages.append("pregnancy")
                for ls in life_stages:
                    n = int(ETH_N[eth] * (0.10 if ab in PEDI_FLAG_BANDS else 1.0) * (0.25 if ls == "pregnancy" else 1.0))
                    for mid, mk in markers.items():
                        base, sigma = reference(mk)
                        p50 = base
                        p50 *= (1 + AGE_TREND.get(mid, 0.0) * decades)
                        if sex == "female" and mid in SEX_F:
                            p50 *= (1 + SEX_F[mid])
                        p50 *= (1 + ETH_SHIFT.get(eth, {}).get(mid, 0.0))
                        sp = sigma * (1.4 if ab in PEDI_FLAG_BANDS else 1.0)
                        lo = 0.0 if base >= 0 else None
                        def clamp(v):
                            return round(max(v, lo), 3) if lo is not None else round(v, 3)
                        src = "illustrative-pediatric" if ab in PEDI_FLAG_BANDS else ("illustrative-pregnancy" if ls == "pregnancy" else "illustrative-demo")
                        rows.append({
                            "cohort_version": COHORT_VERSION, "age_band": ab, "sex": sex, "life_stage": ls,
                            "ethnicity": eth, "marker": mid, "unit": mk.get("unit", ""),
                            "p5": clamp(p50 - 1.645 * sp), "p25": clamp(p50 - 0.674 * sp), "p50": clamp(p50),
                            "p75": clamp(p50 + 0.674 * sp), "p95": clamp(p50 + 1.645 * sp), "n": n, "source": src,
                        })
    out = {
        "_meta": {
            "note": "ILLUSTRATIVE cohort percentile matrix for the showcase demo — generated by _gen_cohort_percentiles.py from calc-graph markers + documented demographic shift rules. NOT real statistics; re-derive from NHANES/UK-Biobank-class + local UAE cohorts before production. Loaded into ClickHouse via cohort-percentiles.schema.sql.",
            "cohort_version": COHORT_VERSION,
            "dimensions": {"age_band": [a for a, _ in AGE_BANDS], "sex": SEXES, "life_stage": ["general", "pregnancy"], "ethnicity": ETHNICITIES},
            "k_anonymity": "a cell is usable only when n>=20; below that the engine backs off to a coarser cohort (hierarchical back-off) or flags OOD (cohort-governance §1).",
            "rows": len(rows), "markers": len(markers),
        },
        "rows": rows,
    }
    p = os.path.join(HERE, "cohort-percentiles.json")
    json.dump(out, open(p, "w"), ensure_ascii=False, indent=1)
    open(p, "a").write("\n")
    print("wrote %d rows (%d cohort cells × %d markers) -> cohort-percentiles.json" % (len(rows), len(rows) // len(markers), len(markers)))


if __name__ == "__main__":
    main()
