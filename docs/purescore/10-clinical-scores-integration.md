# 10 — Clinical-Scores Integration (Clinician Decision-Support Layer)

*Reading guide — clinicians and product can follow the prose and tables; the engineer/data-science notation (e.g. `r_i`, `W_k`, `m_k^cohort`) is implementation detail, not a prerequisite.*

> Binding conventions: `README.md §3`. This document connects established, **validated clinical
> risk scores** to the PureScore pillars and markers (Doc 02) and to the scoring formula (Doc 03).
> These scores live in the **gated clinician layer**: they are decision-support, **not** patient-
> facing diagnosis. PureScore itself stays **wellness-grade** (README §1, §5) — the patient sees a
> wellness number and a *"consult your clinician"* escalation, never an autonomous diagnosis.
>
> **Re-verify caveat.** Every equation, cut-point, risk band, and validated population named below
> is **literature/guideline-anchored as of the cited source and must be re-verified on a fixed
> cadence** (README §5.6) before production. Guideline thresholds (e.g. ACC/AHA risk bands, KDIGO
> categories, FRAX intervention thresholds) change; the catalogue is a mapping spec, not a frozen
> clinical reference. Out-of-distribution handling is delegated to Doc 13.

---

## 1. Positioning and hard boundaries

PureScore is wellness-grade and patient-facing. Clinical risk scores are a **separate, gated layer**
consumed only by credentialed clinicians (and, where lawful and separately governed, payers per
Doc 19). The boundary is non-negotiable:

1. **No auto-diagnosis.** Computing FINDRISC, ASCVD, FIB-4, FRAX, etc. produces a *risk estimate*
   for a clinician to interpret. The patient-facing app never renders these as a diagnosis or a
   treatment instruction. It renders, at most, a wellness-level prompt: *"some of your markers
   suggest it's worth discussing X with your clinician."*
2. **The safety anchor is never bypassed** (README §5.2, Doc 03 §2, §4). A clinical score may
   *raise* concern via the multiplier path in §3, but it can never *lower* a marker's `r_i` below
   its clinical anchor, never clear a critical pillar, and never lift the `PURE_CRIT_CAP` (Doc 03
   §5.3). A reassuring clinical score does not un-flag a red biomarker.
3. **Clinician-in-the-loop for the feedback path.** Abnormal scores feed back into `W_k`/`r_i`
   interpretation **only after** a clinician validates them (§3), or in a clearly-labelled
   *provisional* state that cannot trigger care actions on its own.
4. **Explainable and versioned.** Each score instance is emitted with its inputs, source guideline,
   model version, and OOD flags, alongside the Doc 03 §7 explainability object.

---

## 2. Catalogue — validated clinical scores mapped to PureScore pillars

Columns: **Inputs** (what the equation consumes); **Output / interpretation** (what it returns and
the standard bands); **Guideline / source**; **Pillar(s) → markers** it draws from (Doc 02 IDs).
Bands are the standard published bands and are subject to the re-verify caveat above.

### 2.1 Cardiovascular (Pillar CV)

| Score | Inputs | Output / interpretation | Guideline / source | Pillar → markers |
|-------|--------|-------------------------|--------------------|------------------|
| **ASCVD Pooled Cohort Equations (PCE)** | Age, sex, race, total-C, HDL-C, systolic BP, BP-treatment, diabetes, smoking | 10-yr hard-ASCVD risk %. Bands: <5% low, 5–7.5% borderline, 7.5–20% intermediate, ≥20% high | ACC/AHA 2013 / 2018 / 2019 prevention | CV (SBP, LDL-C/HDL-C); MET (diabetes flag); cross-cutting smoking |
| **SCORE2 / SCORE2-OP** | Age, sex, smoking, systolic BP, non-HDL-C, region risk-tier (OP = age ≥70) | 10-yr fatal+non-fatal CVD risk %, age-banded thresholds (e.g. <50: low/mod <2.5%, high ≥7.5%) | ESC 2021 CVD prevention | CV (SBP, non-HDL = total−HDL); cross-cutting smoking |
| **Framingham Risk Score** | Age, sex, total-C, HDL-C, systolic BP (+treatment), smoking, diabetes | 10-yr CHD / general CVD risk % (legacy; useful where PCE/SCORE2 out-of-population) | D'Agostino 2008 / Wilson 1998 | CV (SBP, lipids); MET (diabetes) |
| **CAC / MESA interpretation** | CAC Agatston score (+ MESA: age, sex, race, risk factors for percentile) | Agatston 0 / 1–99 / 100–299 / ≥300+; MESA percentile for age-sex; CAC=0 = strong de-risker, high CAC = up-classifier | MESA; ACC/AHA CAC as risk-decision aid | CV (CAC marker directly) |

> CAC=0 is a recognised **negative risk marker** that can *de-escalate* a clinician's statin
> decision — but per §1 it is surfaced to the clinician, and in the PureScore engine it can only
> reduce *cohort-percentile* pressure, never override an independent red marker.

### 2.2 Metabolic / Endocrine (Pillar MET, ENDO)

| Score | Inputs | Output / interpretation | Guideline / source | Pillar → markers |
|-------|--------|-------------------------|--------------------|------------------|
| **FINDRISC** | Age, BMI, waist, physical activity, fruit/veg intake, BP meds, history of high glucose, family history | 0–26 pts → 10-yr type-2 diabetes risk: <7 low, 7–11 slightly elevated, 12–14 moderate, 15–20 high, >20 very high | Lindström & Tuomilehto 2003 (Finnish Diabetes Assoc.) | MET (BMI/waist via BCM, glucose history); FIT (activity); NUT (diet); CV (BP meds) |
| **ADA risk test** | Age, sex, gestational-diabetes history, family history, hypertension, activity, BMI | Score ≥5 → elevated prediabetes/T2D risk → confirm with HbA1c/FPG | American Diabetes Assoc. | MET (BMI, HbA1c/FPG confirm); CV (HTN); FIT |

### 2.3 Renal (Pillar REN)

| Score | Inputs | Output / interpretation | Guideline / source | Pillar → markers |
|-------|--------|-------------------------|--------------------|------------------|
| **KDIGO eGFR × UACR heatmap** | eGFR category (G1–G5) × albuminuria category (A1–A3) | 2-D **green / yellow / orange / red** grid (§2.3.1) → CKD risk of progression & frequency-of-monitoring guidance | KDIGO 2024 CKD | REN (eGFR cystatin-C/creatinine, UACR) |
| **KFRE (Kidney Failure Risk Equation)** | 4-var: age, sex, eGFR, UACR (8-var adds Ca, PO₄, bicarb, albumin) | 2-yr and 5-yr risk % of kidney failure needing dialysis/transplant; ≥3–5% / ≥40% thresholds inform nephrology referral & access planning | Tangri 2011 / 2016 | REN (eGFR, UACR + electrolytes/albumin); INF (albumin) |

#### 2.3.1 The KDIGO 2-D risk heatmap

Rows = eGFR (G), columns = albuminuria (A). Cell colour = risk of CKD progression / adverse outcome.

| eGFR ↓ \ UACR → | **A1** <30 mg/g | **A2** 30–300 mg/g | **A3** >300 mg/g |
|----------------|:---:|:---:|:---:|
| **G1** ≥90 | 🟩 Green (low) | 🟨 Yellow (mod) | 🟧 Orange (high) |
| **G2** 60–89 | 🟩 Green (low) | 🟨 Yellow (mod) | 🟧 Orange (high) |
| **G3a** 45–59 | 🟨 Yellow | 🟧 Orange | 🟥 Red (very high) |
| **G3b** 30–44 | 🟧 Orange | 🟥 Red | 🟥 Red |
| **G4** 15–29 | 🟥 Red | 🟥 Red | 🟥 Red |
| **G5** <15 | 🟥 Red | 🟥 Red | 🟥 Red |

Mapping to PureScore: an **orange/red** KDIGO cell is a clinician-grade signal that, once validated,
raises `m_REN^cohort` (Doc 03 §5.1) and the UACR/eGFR within-pillar weights (Doc 02 note on diabetic
cohorts). A red KDIGO cell aligns with — but does not replace — the REN critical-marker logic
(Doc 03 §4); the pillar's own critical bands (eGFR <30, UACR ≥300, K⁺ extremes) remain the safety
anchor.

### 2.4 Hepatic (Pillar HEP)

| Score | Inputs | Output / interpretation | Guideline / source | Pillar → markers |
|-------|--------|-------------------------|--------------------|------------------|
| **FIB-4** | Age, AST, ALT, platelet count | <1.3 low fibrosis risk (rule-out), 1.3–2.67 indeterminate, >2.67 advanced-fibrosis risk → refer for elastography | AASLD; MASLD pathways | HEP (AST, ALT, FIB-4 marker); HEM (platelets) |
| **NAFLD/MASLD fibrosis staging** | NFS (age, BMI, glucose/IFG-DM, AST/ALT, platelets, albumin); or FibroScan VCTE kPa / MRI-PDFF | Steatosis grade + fibrosis stage (F0–F4); VCTE ≥8 kPa significant, ≥12 advanced (lab/device-specific) | AASLD MASLD 2023; EASL | HEP (liver-fat CAP/PDFF, ALT/AST, albumin); MET (glucose, BMI via BCM) |

> Terminology: NAFLD → **MASLD** (metabolic dysfunction–associated steatotic liver disease) under
> 2023 nomenclature; FIB-4 cut-points are **age-adjusted in >65s** (re-verify).

### 2.5 Body Composition / Bone (Pillar BCM)

| Score | Inputs | Output / interpretation | Guideline / source | Pillar → markers |
|-------|--------|-------------------------|--------------------|------------------|
| **FRAX** | Age, sex, weight, height, prior fracture, parental hip fracture, smoking, glucocorticoids, RA, secondary osteoporosis, alcohol, ± femoral-neck BMD | 10-yr probability of **major osteoporotic** and **hip** fracture %; country-calibrated; NOGG/NOF intervention thresholds | Kanis / WHO FRAX; NOGG; BHOF | BCM (BMD T-score); cross-cutting smoking, alcohol; ENDO (glucocorticoid use) |
| **Sarcopenia (EWGSOP2)** | Grip strength (or chair-stand), then ALMI/lean mass (DEXA), then gait speed for severity | Probable (low strength) → confirmed (+low muscle quantity) → severe (+low performance) | EWGSOP2 2019 | BCM (grip, ALMI, gait speed); FIT (grip, performance) |

### 2.6 Mental, Cognitive & Social (Pillar MCS)

| Score | Inputs | Output / interpretation | Guideline / source | Pillar → markers |
|-------|--------|-------------------------|--------------------|------------------|
| **PHQ-9** | 9 depression items | 0–4 none, 5–9 mild, 10–14 mod, 15–19 mod-severe, 20–27 severe. **Item 9 (suicidality) > 0 ⇒ red regardless** | Kroenke/Spitzer; USPSTF | MCS (PHQ-9 marker — already in Doc 02) |
| **GAD-7** | 7 anxiety items | 0–4 / 5–9 / 10–14 / 15–21 → minimal/mild/mod/severe | Spitzer 2006 | MCS (GAD-7 marker) |
| **C-SSRS (Columbia)** | Structured suicidality ideation/behaviour items | Ideation severity 1–5 + behaviour → triage to **crisis-escalation referral** | Posner 2011 C-SSRS | MCS — triggered by PHQ-9 item-9 positive |

> PHQ-9 / GAD-7 are **already pillar markers** (Doc 02, MCS). Their *clinical* use here is severity
> staging and treatment-response monitoring for the clinician. A positive PHQ-9 item 9 (or any
> suicidality signal) forces MCS to **red/critical** and fires the crisis pathway (Doc 02 hard rule,
> Doc 03 §4, Doc 16) **before** any clinical-score layering — C-SSRS is the structured referral
> instrument the clinician applies, never an in-app autonomous triage.

### 2.7 Aging / Biological Age (cross-pillar)

| Score | Inputs | Output / interpretation | Guideline / source | Pillar → markers |
|-------|--------|-------------------------|--------------------|------------------|
| **PhenoAge** | Chronological age + 9 labs (albumin, creatinine, glucose, CRP, lymphocyte %, MCV, RDW, ALP, WBC) | A **biological age** (years) & mortality-risk surrogate; BioAge − chronological = age acceleration | Levine 2018 | INF (CRP, WBC, albumin), HEP (ALP), REN (creatinine), MET (glucose), HEM (MCV, RDW, lymph) |
| **Epigenetic BioAge (DNAm clocks)** | DNA-methylation array (e.g. GrimAge, DunedinPACE) | Biological age / pace-of-aging; research-grade, lab-dependent | Horvath; Hannum; GrimAge; DunedinPACE | Cross-pillar; tied to aging reservoirs (Doc 04) |

> BioAge is presented to clinicians as a **composite contextualizer** ("biological 62 vs
> chronological 55 — age acceleration of +7y, driven mainly by INF and MET"), aligning naturally
> with PureScore's reservoir/burden model (Doc 04). It is **not** a diagnosis and not patient-facing
> as such; epigenetic clocks remain research-grade pending validation (Doc 13).

### 2.8 General / whole-person (cross-pillar)

| Score | Inputs | Output / interpretation | Guideline / source | Pillar → markers |
|-------|--------|-------------------------|--------------------|------------------|
| **Charlson Comorbidity Index (CCI)** | Weighted count of 17 comorbidity categories (± age) | Comorbidity burden score → 1-yr/10-yr mortality estimate; context for weighting & prognosis | Charlson 1987 | Draws on disease flags `D` in cohort `c(p)` (README §3.1); informs `m_k^cohort` |
| **Frailty Index (FI) / deficit accumulation** | Proportion of accumulated health deficits (labs, function, comorbidities); or clinical frailty scale | 0–1 fraction; higher = frailer; flags vulnerability to stressors | Rockwood / Mitnitski; Clinical Frailty Scale | Cross-pillar; maps to BCM (sarcopenia), FIT (gait/grip), MCS (cognition), and aging reservoirs |

---

## 3. Direction of data flow

Two directions, with the safety anchor (README §5.2) governing both.

```
            ┌──────────────────────────────────────────────────────────────┐
            │  (A) FORWARD: pillars/markers ──▶ compute clinical scores      │
   Doc 02   │      x_i, S_k, B̃_j  ──▶  ASCVD / FINDRISC / KDIGO / FIB-4 /     │  clinician
  markers   │                           FRAX / PhenoAge / KFRE / CCI / FI    │   dashboard
            │                          (decision-support, gated, §1)         │  (§4)
            └──────────────────────────────────────────────────────────────┘
            ┌──────────────────────────────────────────────────────────────┐
            │  (B) FEEDBACK: clinician-VALIDATED abnormal score              │
   gated    │      ──▶ risk multiplier into W_k / r_i interpretation         │  never bypasses
  clinician │      (raise m_k^cohort, raise within-pillar w_i, add caution)  │  safety anchor;
   layer    │      ──▶ NEVER lowers r_i below clinical anchor                │  never diagnoses
            │      ──▶ NEVER clears a critical pillar or lifts PURE_CRIT_CAP  │
            └──────────────────────────────────────────────────────────────┘
```

### 3.1 Forward (A): pillars/markers → clinical scores

The engine already holds the inputs each equation needs (the right-hand column of §2). For each
score it checks input completeness and OOD status (§5), computes the score, and emits it into the
clinician layer with provenance: input vector, equation version, source guideline, computed bands,
coverage, and OOD flags. Missing inputs → the score is shown as *not computable* (never silently
imputed into a clinical score) or as low-confidence with the imputed inputs flagged.

### 3.2 Feedback (B): validated abnormal score → risk multiplier

A clinician-validated abnormal score modulates **interpretation**, not the raw value:

- **Pillar weight nudge.** A validated high ASCVD or red-KDIGO cell raises `m_k^cohort` for the
  relevant pillar(s) (Doc 03 §5.1), e.g. high ASCVD → CV up; red KDIGO / high KFRE → REN up; high
  FINDRISC → MET, FIT, NUT up.
- **Within-pillar weight nudge.** It can raise specific `w_i` (Doc 02 §"Marker weights"), e.g. KDIGO
  A3 raises UACR's weight inside REN; high FIB-4 raises the FIB-4 marker's weight inside HEP.
- **Caution-only on `r_i`.** Following the Stage-2 pattern (Doc 03 §2), a validated abnormal score
  can act like the cohort term — `r_i ← max(r_i, score-derived caution)` — so it can only *raise*
  marker risk, never relax it. It feeds the **`max(...)`**, never replaces the clinical anchor.

**Three invariants on the feedback path (enforced, audited):**

1. It can **only raise** risk/weight, never lower (mirror of Doc 03 §2 `max`).
2. It **cannot clear** a critical pillar nor lift `PURE_CRIT_CAP` (Doc 03 §5.3). A green clinical
   score never overrides a red biomarker.
3. It applies only after **clinician validation** (or in a labelled *provisional* state that takes
   no care action), and every application is recorded in the audit trail (Doc 16).

---

## 4. The clinician dashboard view

The clinician view composes the Doc 03 §7 explainability object **plus** the §2 clinical scores into
one decision-support surface. It is gated, audited, and team-based-care framed (Mayo Clinic
care-team model — clinician + care team see the same explainable substrate, Doc 01).

**Layout (top to bottom):**

1. **Headline + binding constraint.** PureScore, overall_status, and *which pillar/marker capped it*
   (Doc 03 §7.1) — so the clinician immediately sees the safety-governing factor.
2. **Pillar strip.** Per-pillar `S_k`, `status_k`, coverage `cov_k`, and top contributors
   (Doc 03 §7.2), each pillar linking to its computed clinical scores.
3. **Clinical-score panel.** The §2 catalogue scores relevant to this patient, each as
   `value · band · source-guideline · model-version · OOD-flag`, e.g. *ASCVD 14.2% (intermediate,
   ACC/AHA PCE) — in-distribution* / *FIB-4 3.1 (advanced-fibrosis risk, AASLD) — refer elastography*.
4. **Reservoir contributions.** `ρ_k·B̃_k` narrative (Doc 03 §7.3) — "chronic sleep debt adding 6 pts
   CV risk" — and BioAge acceleration context (§2.7).
5. **Personalization rationale.** `W_k` and *why* (cohort/goal/acute, Doc 03 §7.4), including any
   §3.2 feedback multipliers currently active and the clinician who validated them.
6. **Discrepancy banner.** Any §5 reconciliation conflicts surfaced explicitly (next section).
7. **Action lane (team-based care).** Suggested referrals/monitoring intervals drawn from each
   score's guideline (e.g. KDIGO monitoring frequency, nephrology referral by KFRE, elastography by
   FIB-4, crisis pathway by C-SSRS) — **suggested to the care team, never auto-executed**.

Everything on this surface is **explainable and traceable to inputs and weights** (README §5.3); the
patient-facing app shows only the wellness number and a *"consult your clinician"* prompt.

---

## 5. Reconciliation — when a clinical score and the pillar disagree

A clinical score and the PureScore pillar can diverge (e.g. CV pillar green, but ASCVD intermediate
because age and a non-pillar input drive PCE; or HEP pillar yellow on enzymes while FIB-4 is low).

**Rule: within its domain, the validated clinical score is the higher-authority anchor.** It was
derived and validated against hard outcomes (events, mortality, progression) on defined populations;
the pillar is a wellness composite. Therefore:

1. **Surface the discrepancy** explicitly on the dashboard (§4.6) — never silently pick one. Show
   both values, the inputs that diverge, and the magnitude of disagreement.
2. **Clinical score anchors that domain.** When the validated clinical score is *more adverse* than
   the pillar, it pulls interpretation toward itself via the §3.2 feedback (`max`/weight-raise) —
   raising concern, consistent with README §5.4 (uncertainty defaults to caution).
3. **Asymmetry preserved.** When the clinical score is *more reassuring* than the pillar, it does
   **not** relax the pillar (the §3 invariants and Doc 03 §2 safety dominance hold). A reassuring
   ASCVD does not clear a red ApoB. The discrepancy is still surfaced for the clinician to
   adjudicate, but the safety anchor wins by construction.
4. **Direction of authority is domain-scoped.** ASCVD anchors CV-risk interpretation, KDIGO/KFRE
   anchor renal-progression interpretation, FIB-4 anchors hepatic-fibrosis interpretation — each
   only within its own domain, and only when in-distribution (§6).

Net effect: clinical scores can **add caution and re-weight**, the pillar's own critical bands
provide the **floor**, and disagreement is always **made visible** to the human, never resolved
silently.

---

## 6. Validation, governance, and out-of-distribution flagging

Each clinical equation was derived and validated on a **specific population**, and is only reliable
within it. The engine must know each equation's validated envelope and **flag out-of-distribution
(OOD)** use (ties to Doc 13 — cohort construction, calibration, fairness, drift).

| Score | Known population / distribution caveats (re-verify) |
|-------|------------------------------------------------------|
| **ASCVD PCE** | Derived on US Black & White adults **40–79**; mis-calibrates in other ancestries, <40/>79, modern cohorts (often over-predicts) |
| **SCORE2 / SCORE2-OP** | European risk-region calibrated (low/mod/high/very-high); SCORE2 **40–69**, SCORE2-OP **≥70**; needs region-appropriate recalibration |
| **Framingham** | Predominantly white US (Framingham, MA) cohort; legacy; tends to over-predict in low-risk populations |
| **FINDRISC** | Finnish/European derivation; performance varies by ethnicity & setting |
| **KDIGO / KFRE** | KFRE: validated multinationally but **region-recalibration** recommended; needs standardized eGFR & UACR assays |
| **FIB-4 / NFS** | Cut-points **age-dependent (>65 adjusted)**; reduced specificity at the extremes |
| **FRAX** | **Country-specific calibration required**; valid ~**40–90**; underestimates with recent/multiple fractures, high-dose steroids |
| **EWGSOP2 sarcopenia** | Population-specific grip/ALMI cut-offs (sex, ethnicity) |
| **PhenoAge / DNAm clocks** | Assay- and platform-dependent; epigenetic clocks **research-grade**, validation ongoing |
| **Charlson / Frailty Index** | Weights/derivation era- and setting-dependent; deficit lists vary |

**OOD policy (governance):**

1. **Compute the envelope check first.** For each score, test the patient against its validated
   ranges (age, sex, ancestry/region, assay, comorbidity exclusions). If outside, set `OOD = true`.
2. **OOD scores are flagged, down-weighted, or withheld** from the §3.2 feedback path. An OOD score
   may be *shown to the clinician with a prominent caveat* ("ASCVD PCE: patient age 35 — below
   validated range; interpret with caution") but **must not** silently drive `W_k`/`r_i`.
3. **Prefer an in-distribution alternative** where one exists (e.g. SCORE2-OP for ≥70; age-adjusted
   FIB-4 in >65).
4. **Calibration, fairness, and drift monitoring** for the whole layer — including whether borrowed
   equations are calibrated in *our* cohorts and whether OOD-flagging itself is equitable — are owned
   by **Doc 13**; the safety/governance, audit-trail, and clinician-in-loop requirements are owned by
   **Doc 16**. No protected-class attribute may be used to worsen access or care (README §5.5).

---

*Cross-references: Doc 02 (markers, bands), Doc 03 (`r_i`, `R_k`, `W_k`, critical cascade,
explainability §7), Doc 04 (reservoirs, aging burdens), Doc 13 (cohorts, calibration, OOD, fairness,
drift), Doc 19 (gated payer/actuarial layer), Doc 16 (safety, escalation, governance, audit).*
