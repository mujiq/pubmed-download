#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""One-time migration: assign each question-bank item a content `section` + `phase`,
and gap-fill missing eligibility gates (sex / life-stage / age / show_if) so the
gating rules are authoritative data. Idempotent — safe to re-run.

  python3 migrate_questions_sections.py
"""
import json, os, re

HERE = os.path.dirname(os.path.abspath(__file__))
PATH = os.path.join(HERE, "data", "question-bank.json")

SECTIONS = ["demographics", "lifestyle", "medical_history", "symptoms", "adherence"]
PHASES = ["onboarding", "progressive", "ongoing"]

# ---- content-section assignment (priority-ordered keyword match on category) ----
def section_for(q):
    c = (q.get("category") or "").lower()
    t = (q.get("text") or "").lower()
    dom = (q.get("domain") or "")
    def has(*ks): return any(k in c for k in ks)
    # 1. demographics & identity / context
    if has("demographic", "anthropometric", "socioeconomic", "occupation", "environment",
           "family history", "prenatal", "clinical measures", "reproductive / lifestage", "lifestage"):
        return "demographics"
    # 2. adherence (medication-taking) — most adherence lives in adherence.json, but catch any here
    if has("adherence") or ("medication" in c and "adher" in t):
        return "adherence"
    # 3. medical history / diagnosed conditions / current meds
    if (has("history", "diagnosed", "comorbid", "medication", "medications", "clotting", "thrombo",
            "bleeding", "autoimmune", "inflammatory bowel", "ibd", "copd", "asthma", "psoriasis",
            "atopic march", "metabolic bone", "subfertility", "menopause", "pcos", "andropause",
            "hypogonadism", "prolactin", "gender-affirming", "reproductive health", "fertility",
            "safety screen", "addiction")):
        return "medical_history"
    # 4. symptoms & PROs
    if has("symptom", "pain", "mood", "depression", "anxiety", "migraine", "fatigue", "brain fog",
           "sleepiness", "restless", "acne", "skin", "hair loss", "dry eyes", "gerd", "reflux",
           "ibs", "bowel", "wrinkle", "rosacea", "fibromyalgia", "cognition", "memory", "wellbeing",
           "ocd", "adhd", "dementia", "polycythemia", "haemolysis", "intolerance", "hypochlorhydria",
           "sleep quality", "sleep-disordered", "self-perception", "self-rated", "deconditioning",
           "perceived-vs-actual", "characteristics", "aging"):
        return "symptoms"
    # 5. default: behavioural lifestyle (diet / sleep hygiene / activity / substance / stress / supps)
    return "lifestyle"

# ---- phase assignment ----
def is_gated(q):
    ap = q.get("applicability", {}) or {}
    dep = q.get("dependencies", {}) or {}
    return bool(ap.get("gated") or q.get("prerequisites") or dep.get("show_if") or dep.get("skip_if"))

def phase_for(q, section):
    if is_gated(q):
        return "progressive"
    if (q.get("cadence") or "") == "once":
        return "onboarding"
    if section in ("demographics", "medical_history"):
        return "onboarding"
    return "ongoing"

# ---- eligibility gap-fill ----
FEMALE_KW = ["menopaus", "pcos", "pregnan", "prenatal", "menstru", "gestational", "ovar", "uter",
             "female reproductive", "perimenopaus", "subfertilit", "endometri"]
MALE_KW = ["hypogonad", "andropaus", "prostate", "erectile", "male fertility", "testicular", "male hypogonad"]

def kw_in(q, kws):
    blob = ((q.get("category") or "") + " " + (q.get("text") or "")).lower()
    return any(k in blob for k in kws)

def smoker_labels(q):
    out = []
    for r in q.get("responses", []):
        lab = (r.get("label") or "")
        if not re.search(r"never|non[- ]?smoker|no\b|none", lab.lower()):
            out.append(lab)
    return out

def drinker_labels(q):
    out = []
    for r in q.get("responses", []):
        lab = (r.get("label") or "")
        if not re.search(r"none|never|0\b|don't|do not", lab.lower()):
            out.append(lab)
    return out


def main():
    d = json.load(open(PATH, encoding="utf-8"))
    qs = d["questions"]; byid = {q["id"]: q for q in qs}
    smoke_q = byid.get("Q_CORE_SMOKING"); alc_q = byid.get("Q_CORE_ALCOHOL")
    smoke_in = smoker_labels(smoke_q) if smoke_q else []
    alc_in = drinker_labels(alc_q) if alc_q else []

    stats = {"section": {}, "phase": {}, "sex_added": 0, "lifestage_added": 0,
             "age_added": 0, "showif_added": 0}

    for q in qs:
        sec = section_for(q); ph = phase_for(q, sec)
        q["section"] = sec; q["phase"] = ph
        stats["section"][sec] = stats["section"].get(sec, 0) + 1
        stats["phase"][ph] = stats["phase"].get(ph, 0) + 1

        ap = q.setdefault("applicability", {})
        dep = q.setdefault("dependencies", {})
        cur_sex = ap.get("sex")
        # --- sex gates (only fill when currently unrestricted) ---
        if cur_sex in (None, ["all"], []):
            if kw_in(q, FEMALE_KW) and not kw_in(q, MALE_KW):
                ap["sex"] = ["female"]; stats["sex_added"] += 1
            elif kw_in(q, MALE_KW):
                ap["sex"] = ["male"]; stats["sex_added"] += 1
        # --- life-stage gates ---
        ls = ap.setdefault("life_stage", [])
        blob = ((q.get("category") or "") + " " + (q.get("text") or "")).lower()
        if ("pregnan" in blob or "prenatal" in blob or "gestational" in blob) and "pregnancy" not in ls:
            ls.append("pregnancy"); stats["lifestage_added"] += 1
            if ap.get("sex") in (None, ["all"], []):
                ap["sex"] = ["female"]
        if "menopaus" in blob and "menopause" not in ls:
            ls.append("menopause"); stats["lifestage_added"] += 1
            if (ap.get("age_min") or 0) < 40:
                ap["age_min"] = 40; stats["age_added"] += 1
        # --- show_if gates for substance follow-ups (gate on the onboarding status Q) ---
        if q["id"] not in ("Q_CORE_SMOKING",) and smoke_in and re.search(r"smok|cigarette|tobacco|quit", blob) \
           and "second-hand" not in blob and "shisha" not in blob and not dep.get("show_if"):
            dep.setdefault("show_if", []).append({"q": "Q_CORE_SMOKING", "in": smoke_in}); stats["showif_added"] += 1
        if q["id"] not in ("Q_CORE_ALCOHOL",) and alc_in and re.search(r"alcohol|drink", blob) \
           and "sugar" not in blob and "water" not in blob and "coffee" not in blob and "tea" not in blob \
           and not dep.get("show_if"):
            dep.setdefault("show_if", []).append({"q": "Q_CORE_ALCOHOL", "in": alc_in}); stats["showif_added"] += 1

    # meta
    m = d["meta"]
    m["version"] = "1.2"
    m["sections"] = SECTIONS
    m["phases"] = PHASES
    m["section_phase_note"] = ("Each question carries a content `section` (demographics/lifestyle/"
        "medical_history/symptoms/adherence) and a `phase` (onboarding one-time / progressive unlocked / "
        "ongoing). Eligibility gates (applicability.sex/age/life_stage, dependencies.show_if, prerequisites, "
        "unlocks) gate which questions a given patient is ever asked — see the Eligibility & gating page.")

    json.dump(d, open(PATH, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    print("section:", stats["section"])
    print("phase:", stats["phase"])
    print("gap-fill — sex:", stats["sex_added"], "life_stage:", stats["lifestage_added"],
          "age:", stats["age_added"], "show_if:", stats["showif_added"])


if __name__ == "__main__":
    main()
