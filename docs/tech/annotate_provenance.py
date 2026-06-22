#!/usr/bin/env python3
"""Annotate the 'uncited basis' surface (Package D): pillar weights, engine constants, reservoir
anchors & couplings get an explicit basis + calibration_status, so an expert-prior is never mistaken
for an evidence-derived value. Also applies the unambiguous engine fixes (OSA AHI order-of-magnitude;
gly/micr reservoir anchors). All value changes are PROPOSED pending sign-off & cohort calibration
(Doc 13/14). Idempotent. Illustrative — NOT a clinical sign-off.
"""
import json, os, sys
HERE = os.path.dirname(os.path.abspath(__file__))
def L(n): return json.load(open(os.path.join(HERE, "data", n), encoding="utf-8"))
def W(n, d):
    p = os.path.join(HERE, "data", n)
    json.dump(d, open(p, "w", encoding="utf-8"), ensure_ascii=False, indent=1); open(p, "a").write("\n")

PILLAR_BASIS = {
 "CV":"highest single-cause mortality burden (expert-prior)", "MET":"diabetes/MetS centrality in a high-prevalence Gulf cohort (expert-prior)",
 "REN":"silent CKD progression weight (expert-prior)", "HEP":"MASLD prevalence (expert-prior)",
 "INF":"hsCRP adjunctive — deliberately modest (expert-prior)", "HEM":"anaemia/haemoglobinopathy relevance; under-weighted pending Gulf consanguinity review (expert-prior)",
 "ENDO":"thyroid-centric, thin coverage (expert-prior)", "BCM":"sarcopenia/osteoporosis & body-comp (expert-prior)",
 "NUT":"micronutrient adjunctive (expert-prior)", "SLP":"sleep as a modifiable lever — UX-weighted, NOT outcome-derived (expert-prior)",
 "FIT":"CRF strong predictor but wearable-estimated — UX-weighted (expert-prior)", "MCS":"mental-health centrality; 100% PRO (expert-prior)",
}
RESERVOIR_BASIS = {
 "ath":"apoB/LDL/SBP atherogenic load; anchors expert-prior, not risk-tiered (calibrate)",
 "vbp":"vascular BP load (expert-prior)", "gly":"glycaemic burden; bad raised off treatment-target saturation (proposed)",
 "adi":"adiposity; bodyfat anchors sex-blind — sex-gating pending (expert-prior)",
 "hepf":"hepatic fibrosis; FIB-4 age-blind — age-gating pending (expert-prior)",
 "infl":"chronic inflammation; CRP conflates acute-phase — acute gate pending (expert-prior)",
 "allo":"allostatic/mental load; 100% PRO (expert-prior)", "sld":"sleep debt (expert-prior)",
 "crf":"cardiorespiratory fitness; VO2 age/sex-blind — norming pending (expert-prior)",
 "mus":"muscle/ALMI; sex-blind cut pending (expert-prior)", "bon":"bone/T-score; age/menopause gate pending (expert-prior)",
 "renr":"renal reserve; UACR unit now SI-gated (expert-prior)", "micr":"micronutrient; vitD anchors re-based to IOM (proposed)",
 "oxd":"oxygen-carrying; Hb sex/pregnancy-blind — gating pending (expert-prior)", "iron":"iron stores (expert-prior)",
}
CONST_BASIS = {
 "φ":"cohort-blend weight — expert-prior (calibrate vs cohort variance)", "κ_resp":"personal-baseline cap — expert-prior",
 "γ":"pillar power-mean exponent — expert-prior (penalises weak pillars)", "δ":"PureScore power-mean exponent — expert-prior",
 "ρ_k":"reservoir contribution cap — expert-prior", "R_crit":"critical risk floor — expert-prior; SHOULD scale with confidence (flagged)",
 "PURE_CRIT_CAP":"flat critical cap — expert-prior; flat cap collapses severity (flagged, calibrate)",
 "zone cuts":"green/yellow/red on r — expert-prior", "q_impute":"imputed-lab confidence — expert-prior; too high for CRITICAL markers (flagged)",
 "Rp_impute":"cohort representativeness — expert-prior; lacks ethnicity/region dimension (flagged)",
 "cov_green_floor":"coverage floor for green — expert-prior; allows green w/o critical coverage (flagged)",
}
CALIB_NOTE = ("Expert-prior, NOT outcome-calibrated. No guideline assigns these. Calibration plan: fit to "
              "all-cause/CV mortality + competing risks on the local cohort with ethnicity/region strata "
              "(Doc 13 cohort percentiles · Doc 14 validation harness) before production.")

def main():
    chg = []
    # 1) pillar weights
    pw = L("pillar-weights.json")
    if pw.get("calibration_status") != "expert-prior":
        pw["calibration_status"] = "expert-prior"; chg.append("pillar-weights: calibration_status=expert-prior")
    pw["provenance"] = CALIB_NOTE
    pw["basis"] = PILLAR_BASIS
    W("pillar-weights.json", pw)
    # 2) constants
    cn = L("constants.json")
    cn["_meta"]["calibration_status"] = "expert-prior"
    cn["_meta"]["calibration"] = CALIB_NOTE
    cn["basis"] = CONST_BASIS
    W("constants.json", cn)
    # 3) calc-graph reservoirs + coupling + engine value fixes
    cg = L("calc-graph.json")
    try:
        units = {k: v for k, v in L("units.json")["markers"].items()}
    except Exception:
        units = {}
    res = cg["reservoirs"]
    for k, v in res.items():
        v["basis"] = RESERVOIR_BASIS.get(k, "expert-prior")
        v["calibration_status"] = "expert-prior"
        # attach SI unit per input marker (annotation; inputs arrays kept [marker,good,bad])
        v["input_units"] = {inp[0]: (units.get(inp[0], {}).get("si", {}).get("u")
                                     or units.get(inp[0], {}).get("conv", {}).get("u") or "") for inp in v.get("inputs", [])}
    # value fixes (proposed)
    def setres(rk, mk, good=None, bad=None):
        for inp in res[rk]["inputs"]:
            if inp[0] == mk:
                if good is not None and inp[1] != good: chg.append("res %s/%s good %s→%s" % (rk, mk, inp[1], good)); inp[1] = good
                if bad is not None and inp[2] != bad: chg.append("res %s/%s bad %s→%s" % (rk, mk, inp[2], bad)); inp[2] = bad
    setres("gly", "hba1c", bad=9)        # 7% saturates at treatment target → 9% burden
    setres("micr", "vitd", good=30, bad=12)  # IOM-aligned (replete 30, deficient <12)
    # OSA AHI order-of-magnitude fix on the marker threshold
    osa = cg["markers"].get("osa")
    if osa and osa.get("th") != [5, 30, "hi"]:
        chg.append("calc-graph osa th %s→[5,30,'hi'] (AHI mild/severe)" % osa.get("th")); osa["th"] = [5, 30, "hi"]
    cg.setdefault("_meta", {})["reservoir_calibration"] = ("Reservoir anchors [good,bad] and coupling coefficients are "
        "expert-priors (see each reservoir.basis / reservoir.calibration_status). " + CALIB_NOTE)
    for c in cg["reservoir_coupling"]:
        c.setdefault("basis", "expert-prior cross-system coupling — calibrate vs joint cohort covariance (Doc 14)")
    W("calc-graph.json", cg)

    print("provenance annotation + fixes: %d value change(s)" % len(chg))
    for c in chg: print("  ", c)
    return 0

if __name__ == "__main__":
    sys.exit(main())
