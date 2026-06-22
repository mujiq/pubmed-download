#!/usr/bin/env python3
"""Apply data/remediations.json onto data/clinical-flags.json (idempotent).

Sets status / governing_standard / resolution / verdict on each flag named in remediations.json.
Reports any remediation key that does NOT match a live flag (catches typos / renamed/removed flags —
this is the audit-rerun regression guard). Run from docs/tech/ after editing remediations.json, then
rebuild. Exits non-zero on unmatched keys so build_wiki.py can hard-fail.

Usage:  python3 apply_remediations.py [--check]
        --check : do not write; just report what would change + unmatched keys (CI mode).
"""
import json, os, sys

HERE = os.path.dirname(os.path.abspath(__file__))
FLAGS = os.path.join(HERE, "data", "clinical-flags.json")
REMED = os.path.join(HERE, "data", "remediations.json")
VALID_STATUS = {"open", "proposed", "fixed", "accepted", "deferred"}
VALID_VERDICT = {None, "confirmed", "rejected", "needs-info"}


def main(check=False):
    flags_doc = json.load(open(FLAGS, encoding="utf-8"))
    flags = flags_doc["flags"]
    remed = json.load(open(REMED, encoding="utf-8")).get("remediations", {})

    unmatched, applied, bad = [], 0, []
    for key, r in remed.items():
        if key not in flags:
            unmatched.append(key)
            continue
        st = r.get("status", "open")
        if st not in VALID_STATUS:
            bad.append("%s: bad status %r" % (key, st)); continue
        if r.get("verdict", None) not in VALID_VERDICT:
            bad.append("%s: bad verdict %r" % (key, r.get("verdict"))); continue
        f = flags[key]
        f["status"] = st
        f["governing_standard"] = r.get("governing_standard")
        f["resolution"] = r.get("resolution")
        f["verdict"] = r.get("verdict", None)
        applied += 1

    # status tally over ALL flags
    tally = {}
    for f in flags.values():
        tally[f.get("status", "open")] = tally.get(f.get("status", "open"), 0) + 1

    print("remediations: %d applied / %d total · flag status tally: %s"
          % (applied, len(remed), tally))
    if bad:
        print("  ! invalid entries:")
        for b in bad:
            print("     -", b)
    if unmatched:
        print("  ! %d remediation key(s) match no live flag (regression):" % len(unmatched))
        for k in unmatched:
            print("     -", k)

    if not check and not unmatched and not bad:
        json.dump(flags_doc, open(FLAGS, "w", encoding="utf-8"),
                  ensure_ascii=False, indent=1)
        open(FLAGS, "a", encoding="utf-8").write("\n")

    return 1 if (unmatched or bad) else 0


if __name__ == "__main__":
    sys.exit(main(check="--check" in sys.argv))
