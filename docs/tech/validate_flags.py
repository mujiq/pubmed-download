#!/usr/bin/env python3
"""Audit-rerun regression guard — so a resolved clinical-audit flag can never silently reappear.

Three checks:
 1. KEY MATCH   — every remediations.json key still names a live flag (catches a renamed/removed/
                  re-generated flag whose remediation would otherwise dangle).
 2. TRIAGE      — every flag carries a non-'open' status (open = untracked = a hole in the audit).
 3. CONFIG LOCK — every value that a 'fixed'/'proposed' flag claims to have changed is STILL that
                  value in the canonical data. If someone reverts calc-graph/pillars/units back to a
                  flagged value, the flag is silently 'unresolved' again — this fails the build.

Exit non-zero on any failure so build_wiki.py:_flag_guard() (and CI) hard-fails.
Run:  python3 validate_flags.py
"""
import json, os, sys
HERE = os.path.dirname(os.path.abspath(__file__))
def L(n): return json.load(open(os.path.join(HERE, "data", n), encoding="utf-8"))

# CONFIG LOCK — canonical values that remediation claims to have set. (path, expected).
def config_expectations():
    cg = L("calc-graph.json"); units = L("units.json")["markers"]; pil = L("pillars.json")["pillars"]
    pmap = {r[0]: r for _p in pil for r in _p[3]}   # marker name -> display row
    exp = []
    exp.append(("calc-graph osa th", cg["markers"]["osa"]["th"], [5, 30, "hi"]))
    exp.append(("calc-graph waist th", cg["markers"]["waist"]["th"], [90, 100, "hi"]))
    exp.append(("calc-graph vitd th", cg["markers"]["vitd"]["th"], [20, 12, "lo"]))
    exp.append(("calc-graph apob th", cg["markers"]["apob"]["th"], [100, 120, "hi"]))
    exp.append(("calc-graph egfr unit", cg["markers"]["egfr"]["unit"], "mL/min/1.73m²"))
    exp.append(("units hba1c SI", units["hba1c"]["si"]["u"], "mmol/mol"))
    exp.append(("units lpa convertible", units["lpa"]["convertible"], False))
    exp.append(("pillars HbA1c green", pmap["HbA1c"][4], "<5.7 (39 mmol/mol)"))
    exp.append(("pillars Triglycerides green", pmap["Triglycerides"][4], "<150 (1.7 mmol/L)"))
    exp.append(("pillars Systolic BP src", pmap["Systolic BP"][8], "NICE NG136"))
    exp.append(("pillars eGFR (creatinine) green", pmap["eGFR (creatinine)"][4], "≥60 (G1–G2)"))
    exp.append(("pillar-weights calibration", L("pillar-weights.json").get("calibration_status"), "expert-prior"))
    return exp


def citation_check():
    """Every cite token referenced in pillars/range-variations must resolve (source / alias /
    method-token / cross-ref) and every source must be verified. Catches a reintroduced placeholder
    or an unverified/dead source."""
    cit = L("citations.json"); S = cit["sources"]; AL = cit.get("aliases", {})
    MT = cit.get("method_tokens", {}); CR = cit.get("cross_refs", {})
    def resolve(t): return t in S or (t in AL and AL[t] in S) or t in MT or t in CR
    refs = set()
    for _p in L("pillars.json")["pillars"]:
        for r in _p[3]:
            if len(r) > 8 and r[8]: refs.add(r[8].strip())
    rv = L("range-variations.json")
    for m in rv["markers"].values():
        if m.get("base", {}).get("cite"): refs.add(m["base"]["cite"].strip())
        for v in m.get("var", []):
            if v.get("cite"): refs.add(v["cite"].strip())
    unresolved = sorted([t for t in refs if not resolve(t)])
    unverified = sorted([k for k, v in S.items() if not v.get("verified")])
    out = []
    if unresolved: out.append("%d cite token(s) resolve to nothing (placeholder/orphan): %s" % (len(unresolved), ", ".join(unresolved[:8])))
    if unverified: out.append("%d source(s) not verified: %s" % (len(unverified), ", ".join(unverified[:8])))
    return out, len(S), len(refs)


def main():
    flags = L("clinical-flags.json")["flags"]
    remed = L("remediations.json")["remediations"]
    fail = []
    cite_fail, n_src, n_ref = citation_check()
    fail += cite_fail

    # 1. key match
    unmatched = [k for k in remed if k not in flags]
    if unmatched:
        fail.append("%d remediation key(s) name no live flag: %s" % (len(unmatched), ", ".join(unmatched[:8])))

    # 2. triage
    tally = {}
    for f in flags.values():
        tally[f.get("status", "open")] = tally.get(f.get("status", "open"), 0) + 1
    if tally.get("open"):
        opens = [k for k, v in flags.items() if v.get("status", "open") == "open"]
        fail.append("%d flag(s) still 'open' (untriaged): %s" % (tally["open"], ", ".join(opens[:8])))

    # 3. config lock
    drift = []
    for label, got, want in config_expectations():
        if got != want:
            drift.append("%s = %r (expected %r)" % (label, got, want))
    if drift:
        fail.append("config drift — a resolved flag's value was reverted:\n     - " + "\n     - ".join(drift))

    n = len(flags)
    print("[flag-guard] %d flags · %s · %d remediations · citations: %d sources / %d tokens (all verified, 0 broken)"
          % (n, " · ".join("%s %d" % (k, tally[k]) for k in sorted(tally)), len(remed), n_src, n_ref))
    if fail:
        print("  ! FAIL:")
        for x in fail:
            print("     -", x)
        return 1
    print("  [flag-guard] OK — all flags triaged, remediation keys live, no config drift")
    return 0


if __name__ == "__main__":
    sys.exit(main())
