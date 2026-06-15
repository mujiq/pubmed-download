# 01 — Canonical Data Model, Tiers, and Reference-Range Strategy

> Binding conventions: `README.md §3`. This document defines *what data exists*, *how it is
> tiered*, and *how reference ranges (medians/percentiles/bands) are derived from literature when
> the patient's own data is missing.*

## 1. Data-model overview (FHIR-aligned)

PureScore consumes four input domains, all keyed by `patient`, `effectiveTime`, and a
**provenance/confidence** stamp.

| Domain | FHIR analogue | Examples |
|--------|---------------|----------|
| **Biomarkers** | `Observation` (laboratory) | ApoB, HbA1c, eGFR, hsCRP, ferritin, TSH, estradiol |
| **Wearables / device** | `Observation` (vital-signs, activity) | resting HR, HRV, sleep stages, SpO2, steps, VO2max estimate, CGM |
| **Clinical context** | `Condition`, `MedicationStatement`, `Procedure`, `Immunization` | disease flags `D`, medication classes `Mx`, CAC scan, DEXA |
| **Lifestyle / persona / PRO** | `QuestionnaireResponse`, `Observation` (survey) | diet pattern, alcohol, smoking, stress, PHQ-9/GAD-7, life stage, goals |

**Input taxonomy & trust flags.** These four domains map to the four flagged data streams
(🧪 LAB · ⌚ WEAR · 🎯 GOAL · 📝 LIFE) defined in **Doc 18 §1**; wearable inputs are further
**trust-tiered** there (D22) and that tiering is what sets `q_source` in §2 below. Doc 18 also
specifies how each stream is *used* (labs anchor bands; wearables drive trajectory/early-warning;
goals steer weighting; lifestyle fills gaps at lower confidence).

### 1.1 Core entities (logical schema)

```
Patient(patient_id, sex_at_birth, gender_identity, dob, ...)
Cohort(cohort_id, age_band, sex, disease_flags D, medication_classes Mx)   # README §3.1
MarkerDef(marker_id, name, unit, pillar_id, tier, two_sided?, w_i,
          loinc_code, optimal_band_fn, yellow_band_fn, red_band_fn,
          decay_class, reservoir_links[])
Measurement(patient_id, marker_id, value x_i, effectiveTime,
            source ∈ {lab, wearable, survey, derived, literature_fallback},
            confidence ∈ [0,1])
ReferenceDist(marker_id, cohort_id, source, F_cdf | {median, p10..p90}, sample_n, vintage)
LifeEvent(patient_id, type, onset, severity, status ∈ {active, resolving, resolved})
Goal(patient_id, goal_type, target, horizon, priority)
```

Key rules:
- **`sex_at_birth` drives physiology** (reference ranges, hormones); **`gender_identity` drives
  communication and some risk modifiers**. Both are stored; Doc 05 specifies how transgender and
  intersex patients are handled (hormone-therapy-aware ranges, not a binary fallback).
- Every `Measurement` carries `source` and `confidence`. A literature-fallback value has
  `source = literature_fallback` and reduced `confidence`, which lowers pillar **coverage**
  (README §4) and is surfaced to the patient and clinician.

## 2. Provenance, recency, and confidence

A measurement's usable confidence combines **source quality** and **recency decay**:

```
confidence_i(t) = q_source(source_i) · exp( -(t - effectiveTime_i) / τ_i )
```

- `q_source`: lab venous = 1.0; **wearable — tiered (D22, Doc 18 §2):** clinical-grade
  (CGM, validated cuff, single-lead ECG) = 0.85–1.0, consumer-validated (resting HR, steps,
  sleep duration) = 0.6–0.8, inferential/derived (readiness, stress, sleep-stages) = informational
  only (excluded from band-setting); consumer survey = 0.5–0.7; literature fallback = 0.2–0.4.
- `τ_i` (recency half-life-ish): differs by marker volatility — glucose/HRV/sleep are short
  (hours–days), ApoB/HbA1c medium (weeks–months), Lp(a)/genetics effectively permanent.
- When `confidence_i` drops below a per-marker floor, the marker is treated as **stale** and the
  literature fallback (with its own low confidence) is blended in (Doc 09 §3, empirical Bayes).

## 3. Measurement tiers (Core / Peripheral / Comprehensive)

Tiers package measurements into one visit at a sustainable cost and patient burden. They are an
**operational/cost grouping**, orthogonal to the green/yellow/red clinical bands.

| Tier | Cadence (default, persona-adjusted) | Cost / burden | Purpose |
|------|-------------------------------------|---------------|---------|
| **Core** | every visit; monthly–quarterly | low; point-of-care + basic panel + wearable | high-yield surveillance, drives most of PureScore |
| **Peripheral** | quarterly–biannual | moderate; specialty labs | refine risk, catch sub-clinical drift |
| **Comprehensive** | annual–biennial *or* triggered | high; imaging, deep phenotyping, -omics | deep phenotype, set baselines, confirm red signals |

### 3.1 Core tier (illustrative — full catalogue in Doc 02)
BP, resting HR + HRV (wearable), height/weight/BMI/waist, fasting glucose, HbA1c, lipid panel
(TC/LDL/HDL/TG), CBC, basic metabolic panel (electrolytes, creatinine→eGFR), ALT/AST, hsCRP,
25-OH vitamin D, steps/MVPA + sleep duration & regularity (wearable), PHQ-2/GAD-2.

### 3.2 Peripheral tier
ApoB, **Lp(a) once (lifetime, genetic)**, fasting insulin → HOMA-IR, GGT, FIB-4 inputs, ferritin
+ iron studies, TSH + free T4, UACR, cystatin-C eGFR, omega-3 index, B12/folate, magnesium,
sex-hormone baseline panel (Doc 05), 2-week CGM, estimated VO2max, grip strength, PHQ-9/GAD-7.

### 3.3 Comprehensive tier
Coronary artery calcium (CAC), DEXA (body composition + BMD/T-score), continuous CGM, NMR/ion-
mobility advanced lipoprotein particles, ApoE genotype + validated polygenic risk scores,
full adrenal/cortisol-rhythm and reproductive-hormone panels, gut microbiome, cognitive battery,
home sleep apnea test / overnight oximetry, spirometry, transient elastography (FibroScan),
PhenoAge / epigenetic BioAge, broad metabolomic/proteomic panels.

### 3.4 Tier triggering
A **red** Core marker can pull forward the relevant Peripheral/Comprehensive confirmation
(e.g., high fasting glucose → CGM + insulin; elevated ALT → FIB-4 → FibroScan). This is the
clinical-pathway version of the critical cascade (Doc 03 §6, Doc 06 acute pathways).

## 4. Reference-range strategy (literature-only mode)

Per the project decision, PureScore v0.1 derives medians, percentiles, and bands from
**published literature**, not from a proprietary patient warehouse. Two distinct objects:

### 4.1 Clinical optimal/caution/critical bands — from guidelines
The green/yellow/red **bands** (`[L_i^opt, U_i^opt]`, etc.) come from clinical practice
guidelines and are the **safety anchor**. Canonical sources by pillar (re-verify on cadence,
README §5.6):

| Pillar | Primary guideline sources |
|--------|---------------------------|
| CV | ACC/AHA blood-pressure & cholesterol guidelines; ESC/EAS dyslipidaemia (ApoB, Lp(a)); MESA (CAC) |
| MET | ADA *Standards of Care* (glucose, HbA1c); IDF/AHA metabolic-syndrome criteria |
| REN | KDIGO CKD classification (eGFR + UACR heatmap) |
| HEP | AASLD (MASLD/MASH); FIB-4 thresholds |
| INF | AHA/CDC hsCRP cardiovascular-risk strata |
| HEM | WHO anaemia thresholds; lab haematology reference intervals |
| ENDO | Endocrine Society; ATA (thyroid); STRAW+10 (menopause staging) |
| BCM | WHO BMI; NIH waist; ISCD/WHO DEXA T-score; EWGSOP2 (sarcopenia) |
| NUT | Endocrine Society (vit D); WHO/NIH micronutrient cut-offs; omega-3 index literature |
| SLP | AASM / National Sleep Foundation duration & quality recommendations |
| FIT | ACSM / Cooper-Institute VO2max norms; WHO physical-activity guidelines |
| MCS | PHQ-9 / GAD-7 validated severity bands; validated cognitive-screen cut-offs |

### 4.2 Cohort percentile distributions — from population datasets
The **percentile** function `F_{i,c}` (for stack-ranking and "normal for you") comes from
population reference datasets stratified by age × sex (× condition where available):

- **NHANES** (US, broad biomarker + anthropometric coverage, age×sex×race/ethnicity).
- **UK-Biobank-class** distributions for markers NHANES lacks.
- Disease-specific registries for diseased cohorts (e.g., CKD, T2D) where the general-population
  percentile would be misleading.

Each `ReferenceDist` stores `source`, `sample_n`, and `vintage` so coverage and staleness are
auditable.

### 4.3 Median fallback when the patient lacks a marker
If a patient has no usable measurement for marker `i`, PureScore imputes the **cohort median**
(`F_{i,c}^{-1}(0.5)`) as a *neutral prior*, flags `source = literature_fallback`, and assigns low
confidence. Effects:
- The marker contributes at **reduced weight** (weight scaled by confidence; Doc 03 §3).
- The pillar's **coverage** drops, which is shown to patient/clinician and **suppresses
  over-confident green claims** (a pillar that is "green" only because of imputed medians is
  labelled *low-coverage green*, not a clean bill of health).
- The nudge engine (Doc 07) may surface **"measure this"** as a high-value action when an
  imputed marker has high potential leverage on the score.

### 4.4 Why the clinical anchor must dominate the percentile (worked rationale)
Consider HbA1c in a cohort of poorly-controlled type-2 diabetics. A patient at HbA1c 7.5% might
sit at the cohort's 40th percentile (better than cohort peers) yet be clinically **yellow/red**.
PureScore therefore combines the two so the clinical anchor cannot be diluted:

```
r_i = max( r_i^clinical ,  φ · r_i^cohort )       with  φ < 1   (cohort is a refinement, not an override)
```

The cohort term can only *raise* concern (e.g., a value technically in-range but unusually
adverse for a healthy young cohort), never *lower* it below the clinical anchor. Full formula in
Doc 03 §2.

## 5. Data quality, missingness, and gaming resistance
- **Missing-not-at-random** is assumed: patients skip tests for reasons correlated with risk.
  Imputation is *neutral-prior + low-confidence*, never optimistic.
- **Anti-gaming:** wearable signals are cross-checked for plausibility (e.g., implausible step
  counts, HR/HRV inconsistency); self-reported survey items are weighted below objective labs;
  the score cannot be improved by *removing* an unfavourable measurement (stale/missing reduces
  coverage and confidence, it does not delete the burden held in the reservoir layer — Doc 04).
- **Units & LOINC:** every `MarkerDef` carries a LOINC code and canonical unit; ingestion
  normalizes units and rejects out-of-physiological-range values to a QA queue.

## 6. Privacy & security (pointer)
All four domains are PHI. Storage, access, minimization, consent, and the special handling of
genetic and reproductive data are specified in Doc 11 (governance) and constrained by HIPAA/GDPR
and, for genetics, GINA (which also bounds the actuarial layer — Doc 10).
