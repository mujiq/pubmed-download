# 13 — Validation & Calibration Harness (PureScore 2.0)

> Binding conventions: `README.md §3`. This document specifies **how PureScore 2.0 would be
> validated and calibrated** — the headline score, every companion meta-vector dimension (Doc 12 §4),
> and the personal-baseline early-warning layer (Doc 12 §5) — as an executable harness of **gates**.
> It is the 2.0 extension of the anti-Babylon backbone (Doc 09 §4): *no claim ships ahead of its
> evidence* (Doc 00 §3.1). It governs evidence; it does not assert evidence has been gathered.
>
> Decisions referenced: **D1** (output model: score + vector + early-warning), **D2** (tiered
> Watch→Advisory→Alert), **D6** (empirical-Bayes personal baseline), **D7** (defensive bias),
> **D9–D10** (seeded synthetic substrate, diagonal-covariance approx.), **D13** (log baseline for
> skew), **D14** (reconfirm pathway). Gates inherit Doc 09 §4.6 and Doc 11 §8; nothing here relaxes
> the hard non-negotiables (README §5).
>
> **All thresholds, tolerances, and sample-size floors below are illustrative versioned config
> (Doc 03 §8) and must be set per population and re-verified on the fixed cadence (README §5.6)
> before any production use.** They define *what evidence would be gathered*; none is yet satisfied.

---

## 0. What this harness guarantees

1. Each **validation object** (§1) has its own pass/fail gate — the single number, every companion
   dimension, and each early-warning tier are validated **separately**, never bundled.
2. Every metric is reported with an **interval** and **per audited subgroup** (§6); a point estimate
   without a CI, or an aggregate without subgroup parity, is **not a pass** (Doc 09 §4.2, §5).
3. Synthetic data (§2a) **demonstrates the method**; it is never evidence. A claim is promoted only
   on **prospective, real, linked** data (§2b) passing §9 with governance sign-off (Doc 11 §8).

---

## 1. Scope — validation objects

Each row is an independently gated object. "Label?" = does it admit a hard outcome label, or only a
process/consistency criterion.

| # | Object (Doc 12 ref) | What is validated | Label? | Primary gate (§) |
|---|---|---|---|---|
| O0 | **Headline PureScore** (§4) | risk discrimination + calibration to outcomes | yes (proxy) | §3, §4 |
| O1 | **Confidence** `Cf` (§4) | does low `Cf` predict larger error? (error-vs-confidence monotonicity) | indirect | §3.4, §7 |
| O2 | **Data sufficiency** `Su` (§4) | `INSUFFICIENT` flag fires iff fresh-data fraction `< τ_su`; provisional scores marked | rule | §9 (rule check) |
| O3 | **Criticality** badge `Cr` (§4) | tier+count matches Doc 02/03 ground truth; no false-reassurance | yes | §5 (false-critical), Doc 09 §4.4 |
| O4 | **Trajectory** `Tr` (§4) | slope sign/rate accuracy vs held-out future; lead over level-crossing | yes | §3, §5 |
| O5 | **Volatility** `Vo` (§4) | within-patient variance estimate calibrated; damps `Cf`/`Tr` correctly | indirect | §7 |
| O6 | **Early-warning tiers** Watch/Advisory/Alert (§5.3) | lead-time, sens/spec/**PPV at realistic prevalence**, alarm budget | yes (proxy) | **§5** |
| O7 | **Representativeness** `Rp` (§4, §7) | low `Rp` ⇒ OOD truth; triggers `φ→0` suppression (D7) | yes (OOD) | §6, §7 |
| O8 | **Skew/robustness** `Sk` (§4, §7, D13) | log-baseline removes false anomalies on heavy-tailed markers | rule | §7 |
| O9 | **Modifiability** `Mo` (§4) | modifiable-risk share tracks realized response to intervention | yes (proxy) | §3 |
| O10 | **Dual framing** (§6, D11) | `Progress` monotone in modifiable improvement; fixed inputs inert | rule | §9 (consistency) |

---

## 2. Datasets

### 2a. Seeded synthetic-cohort generator (development only — **illustrative, not evidence**)

For harness development, threshold tuning, and edge-case coverage (the §2 persona catalogue), a
**deterministic seeded generator** (the D9 substrate, generalized to a cohort) produces virtual
patients with **known ground truth**, so detectors can be exercised before any real data exists.

```
 for each persona × marker:  series(t) = trend(persona) + AR(1) noise(σ_marker) + seeded events
 ground truth injected:      true onset times, true OOD flags, true confound (β-blocker), artifacts
 latest sample pinned to a live input; fully reproducible from seed (D9)
```

- Generates labelled incubation episodes (so lead-time §5 is *computable*), planted artifacts
  (hemolyzed K⁺, bad-contact SpO₂ — exercises the D14 reconfirm path), and OOD/small-cell cases.
- **Stated limitation:** the generator encodes our *assumptions*; metrics on it measure
  self-consistency, **not** real-world performance. It can show a detector is broken; it can never
  show it works. Any synthetic result is labelled `SYNTHETIC — not evidence` (Doc 12 §10).

### 2b. Real-data requirements for true validation (the actual evidence)

| Requirement | Spec |
|---|---|
| **Prospective longitudinal cohort** | repeated per-patient measurement over time in the *deployment* population; pre-registered protocol, pre-specified endpoints (Doc 09 §4.5) |
| **Outcome labels** | linked hard outcomes: all-cause/cause-specific mortality, incident MACE, T2D/CKD progression, hospitalization; event dates for time-to-event |
| **Linkage** | record-linkage to EHR/claims/registry/vital-records, consented (Doc 11 §4); enables the survival loss of Doc 09 §3.1 |
| **External + temporal split** | a second site and a *later* window than training (Doc 09 §4.5) — transported models decay |
| **Early-warning truth** | the threshold-crossing / clinical event each Watch/Advisory/Alert is meant to anticipate, with timestamps (for lead-time §5) |

### 2c. Reference-distribution sources (per Doc 09)

Population CDFs `F_{i,c}` come from **NHANES / UK-Biobank-class** datasets and disease registries,
stratified per the Doc 09 §1.1 tree, pooled per §1.3, carrying `source, sample_n, vintage` and a
**coverage flag** (own leaf vs pooled ancestor). These set reference distributions and OOD envelopes
(§7); they are **not** outcome-label sources — discrimination/calibration (§3–4) still require §2b.

---

## 3. Discrimination

Where outcome labels exist (§2b), the headline and label-bearing dimensions (O0, O3, O4, O6, O9) are
scored on ranking power.

| Metric | Use | Target (illustrative) |
|---|---|---|
| **AUROC** / time-dependent AUC | overall separation; horizon-specific | ≥ target, CI lower bound ≥ incumbent (Doc 08) |
| **Harrell's / Uno's C** | time-to-event concordance (risk ordering) | ≥ target |
| **AUPRC** | rare outcomes (AUROC flatters at low prevalence) | reported with prevalence; baseline = prevalence |
| **NRI / IDI** | added value over FINDRISC/ASCVD/SCORE2 (Doc 09 §4.2) | PureScore must *add*, not repackage |

All reported with **bootstrap CIs**; a point estimate alone is not a pass (Doc 09 §4.2).

**Label scarcity (a wellness score rarely has hard events):** handled with explicit caveats —
- **Proxy / surrogate endpoints** — validated intermediate markers (HbA1c progression, UACR rise,
  BP control, VO₂max change) stand in where hard events are sparse, **flagged as surrogate** and
  never claimed as the hard outcome. A surrogate gain is necessary, not sufficient (Doc 00 §3.1).
- **Composite & competing risks** — composite endpoints raise event counts but are reported
  decomposed; competing risks (death before the event of interest) use cause-specific / Fine–Gray.
- **Honest reporting** — every discrimination claim names whether its endpoint is **hard or
  surrogate**, the event count, and the follow-up; a surrogate-only result cannot satisfy the §9
  prospective gate for a hard-outcome claim.

---

## 4. Calibration

Calibration asks: do predicted risks match observed event rates? (Extends Doc 09 §4.1.)

| Instrument | Definition | Target (illustrative) |
|---|---|---|
| **Reliability diagram** | predicted-risk decile vs observed event rate | on diagonal within CI |
| **ECE** | `Σ_b (n_b/N)·|acc_b − conf_b|` over bins | ≤ 0.05 overall **and per subgroup** |
| **Brier + decomposition** | `BS=(1/N)Σ(p̂−y)²` = **reliability − resolution + uncertainty** (Murphy) | reliability term ≈ 0; positive skill `1−BS/BS_ref` |
| **Calibration slope** | slope of observed log-odds on predicted log-odds | ∈ [0.9, 1.1] |
| **Calibration intercept** (calibration-in-the-large) | mean predicted = mean observed | |E/O − 1| ≤ tol; Spiegelhalter Z n.s. |

**Recalibration** when out of bounds: **Platt** (logistic) for slope/intercept drift, **isotonic**
for monotone non-linear miscalibration (needs more data, can overfit small cells — guard with §6
pooling). Recalibration is **versioned like any parameter change** (Doc 09 §4.1, Doc 11 §5.1) — never
a silent threshold move (README §5; F5).

---

## 5. Early-warning evaluation (the novel part — critical)

The early-warning layer (Doc 12 §5, D2) is the main clinical value-add and the highest-risk surface
(it is where a false alarm becomes alarm fatigue and a missed signal becomes the Babylon failure).
It is evaluated **per tier** against labelled incubation episodes (§2a synthetic for development;
§2b real for evidence).

### 5.1 Lead-time

For each true threshold-crossing / event, **lead time** = (band/clinical-crossing time) − (first
tier fire). Report the **distribution** (median, IQR, fraction with positive lead) per tier, and
**time-to-detection vs a band-threshold baseline** (the v0.1 detector that fires only on crossing).
The claim "we detect earlier" is the *difference* of these two distributions, with a CI — asserted
nowhere, computed here. Negative lead time (fires *after* crossing) is a tracked failure.

### 5.2 Operating characteristics **at realistic prevalence**

Per tier (Watch / Advisory / Alert), at the operating point:

| Metric | Watch | Advisory | Alert |
|---|---|---|---|
| Sensitivity (recall) | high (internal queue absorbs noise) | moderate–high | high for true crossings |
| Specificity | low ok (internal) | higher (multivariate/2-read confirmed) | highest |
| **PPV @ realistic prevalence** | — | reported, not assumed | reported, not assumed |
| Routed to | clinician queue (D2) | patient nudge | clinician, may escalate |

> **PPV collapses at low prevalence — this is the Babylon lesson made quantitative.** With
> sensitivity `Se`, specificity `Sp`, prevalence `π`:
> ```
>  PPV = (Se·π) / (Se·π + (1−Sp)·(1−π))
> ```
> At `π = 0.5%`, even `Se = 0.95, Sp = 0.95` gives `PPV ≈ 8.7%` — **>91% of patient-facing alarms
> are false**. PPV is therefore **reported at the deployment prevalence**, never at a convenient
> balanced prevalence, and the Advisory→patient gate must clear a **minimum-PPV** bar. This is
> *exactly* why D2 keeps single-marker drift in the internal **Watch** queue and surfaces only
> multivariate- or multi-read-**confirmed** signals to the patient — confirmation raises the
> effective `(1−Sp)` term's denominator and lifts PPV into a usable range.

### 5.3 Alarm budget / alarm-fatigue

- **Alarm rate** measured as **patient-facing alarms per patient-month**; a hard **alarm-fatigue
  budget** caps it (illustrative ≤ ~1 Advisory/patient-month median; Alert governed separately by
  clinical necessity, never budget-suppressed). Exceeding budget is a **release blocker** (F3).
- Watch (internal) is **not** patient alarm volume but is monitored for clinician-queue load.
- Budget is **never** met by suppressing a genuine Alert (README §5.4; never trade sensitivity for
  quiet) — only by raising the Advisory confirmation bar (§5.2).

### 5.4 False-critical rate and the reconfirm pathway (Doc 12 §3.4, D14)

- **False-critical rate** = isolated implausible / low-device-quality / drug-expected values that
  would *wrongly* fire the emergency cascade. The D14 path routes these to **Watch + reconfirm**
  (confidence forced ≈0), so they cannot cascade — validated by replaying planted artifacts (§2a)
  and confirming **no false emergency** fired.
- **Hard counter-gate (safety):** the reconfirm path must **never** downgrade a *confirmed*
  (full-confidence, corroborated) critical. The suite asserts a true K⁺ 7.0 with corroboration still
  fires immediately (Doc 11 §2.1) — i.e. false-reassurance rate stays ≈ 0 (Doc 09 §4.4). A single
  suppressed true emergency is a hard fail regardless of every other metric.

---

## 6. Fairness & equity (ties to D7, Doc 09 §5, Doc 10/11)

Every §3–§5 metric is **recomputed per subgroup and per intersection** and must meet parity
tolerances; the fairness audit has **veto power** (Doc 09 §5, Doc 11 §8 G7).

### 6.1 Subgroup slices

Sex (`sex`, a legitimate physiological input — Doc 09 §5.3); **life stage** (cycle phase, pregnancy,
post-partum, menopause, andropause); age band; ancestry / genetic background; **representativeness /
OOD bucket** (`Rp` tier, §7); plus their intersections (e.g. menopausal × low-`Rp` × minority).

### 6.2 Metrics & gates

| Check | Definition | Gate |
|---|---|---|
| **Per-subgroup calibration** | slope, E/O, ECE within each slice | within §4 bounds **per slice** |
| **Error-rate parity** | equalized-odds-style **TPR/FPR gaps** of the critical cascade & early-warning, esp. **false-reassurance** | gap ≤ tol, CI excludes harm |
| **Predictive parity** | PPV/NPV across slices (tie to §5.2 prevalence) | within tol |
| **Max–min performance ratio** | worst-subgroup / best-subgroup for each metric | ≥ ratio floor (e.g. ≥ 0.8) |
| **No-proxy hard gate** | no protected attribute **or proxy** worsens score/access/price | **zero tolerance** (Doc 10/11; README §5.5) |

- **No-proxy gate (binding):** train an adversary to reconstruct each protected attribute from the
  feature set; the score's residual correlation with the attribute, after conditioning on legitimate
  clinical need, must not worsen access (Doc 09 §5.3). `sex_at_birth`/hormonal status are *kept* as
  clinical inputs; race-as-biology is **not** used (race-free eGFR, Doc 12 §7). Any proxy that
  worsens outcomes is removed/neutralized and the fix re-validated.
- **Small cells:** **partial pooling** toward parent strata (Doc 09 §1.3) — never scored from noise,
  never silently flattened; the audit checks both over-pooling (erases a real subgroup signal) and
  under-pooling (thin cell from noise). Subgroup recalibration is itself re-validated so fixing one
  group does not harm another.

---

## 7. Robustness

| Test | Method | Pass |
|---|---|---|
| **Skew / heavy-tail transform** (D13) | inject spikes in CRP/UACR/ALT/FIB-4/bilirubin; compare raw-z vs log-baseline z | log-baseline does **not** raise false anomalies; raw slope/band logic preserved |
| **Missing-data / imputation sensitivity** | ablate inputs; perturb imputations | `Su`/`INSUFFICIENT` fires; imputed-median pillar shows **low-coverage green**, never reassurance (Doc 11 §2.4); score stable to plausible imputations |
| **Device-quality sensitivity** | degrade wearable/assay quality | `Cf`/`Vo` respond; confounded/low-quality markers down-weighted (Doc 12 §3.2), not scored |
| **Adversarial / out-of-range inputs** | implausible & boundary values | plausibility/unit checks (Doc 01) catch; D14 reconfirm for criticals; no crash, no false cascade |
| **Unit-error detection** | mg/dL↔mmol/L, °F↔°C, etc. | flagged by range/plausibility; never silently scored (F6) |

Robustness is reported **alongside** responsiveness (Doc 09 §4.3): a detector stable only because it
is numb fails — it must absorb noise **and** still cross promptly on a true change.

---

## 8. Drift & monitoring (post-deployment)

Once live, monitored continuously, then recalibrated on cadence (extends Doc 09 §4.7, Doc 11 §5.2).

| Signal | Definition | Trigger |
|---|---|---|
| **PSI per marker & per pillar** | `Σ (p_i − q_i)·ln(p_i/q_i)` vs training reference | PSI > 0.1 watch, > 0.25 act → OOD handling (Doc 09 §4.5) |
| **Score drift** | distribution shift of headline & each `Cf/Su/Tr/Rp` dimension | beyond control band → review |
| **Calibration drift** | rolling slope / E/O / Brier over a moving window | out of §4 bounds → **recalibration trigger** |
| **Early-warning drift** | rolling alarm-rate, PPV, lead-time per tier | breach of §5 budget/PPV → recalibrate or re-gate |

- **Recalibration cadence:** fixed scheduled cadence (README §5.6) **plus** any drift-triggered
  off-cadence recalibration. Each recalibration is a **model-version bump** re-passing §9 (Doc 11 §5.1).
- **Shadow mode before promotion:** any new/recalibrated version runs **in shadow** (computed,
  logged, **not surfaced**) against live data until it meets §9 on the live population; safety-relevant
  regressions can **auto-roll-back** to the last validated version (Doc 11 §5.2).

---

## 9. Release gates / definition-of-done

A version of PureScore 2.0 reaches **any real-world use** only if **every** gate is green, for the
target population **and every audited subgroup (§6)**, signed by its named owner, and (for launches/
claim changes) the Oversight Board (Doc 11 §5.6–§5.7, §8). **Default is No-Go**; a previously-green
gate does **not** carry across a version bump.

```
 SHIP a 2.0 claim only if, for the target population AND every audited subgroup (§6):
   discrimination:   AUROC/C ≥ target, CI lower bound ≥ incumbent; endpoint labelled hard|surrogate (§3)
   calibration:      slope ∈ [0.9,1.1], |E/O−1| ≤ tol, ECE ≤ tol, overall AND per subgroup        (§4)
   early-warning:    positive median lead-time vs band baseline; per-tier PPV @ deployment
                     prevalence ≥ bar; alarm budget met; false-critical=0; reconfirm never
                     suppresses a confirmed critical                                               (§5)
   companion vector: each dimension passes its object gate (§1); low-Cf predicts larger error;
                     INSUFFICIENT/Managed/Reconfirm rules verified                                 (§1,§7)
   fairness:         calibration & error-rate parity within tol; max–min ratio ≥ floor;
                     no protected-attribute/proxy worsens score/access (hard gate)                 (§6)
   robustness:       skew/imputation/device/adversarial/unit tests pass; responsive on true change (§7)
   drift/ops:        PSI baselines set, shadow-mode passed, rollback live, model card published     (§8)
   prospective:      pre-registered PROSPECTIVE study PASSED in the deployment population (§2b)
   governance:       clinician sign-off + Oversight Board approval (Doc 11 §8 G1–G12)
 Any failure ⇒ NO-SHIP.  Versioning/re-validation rule: ANY change to a constant, band, weight,
 model, transform, threshold, or training/cohort dataset (Doc 03 §8, Doc 12 §9) is a version bump
 that RE-RUNS this entire gate before promotion — synthetic results never substitute for §2b.
```

---

## 10. Honest status

**None of these gates is satisfied.** This document is a *design-time* specification of the evidence
PureScore 2.0 *would* have to produce — the lead-time distributions, prevalence-correct PPVs, alarm
budgets, per-subgroup calibration, and false-critical counts that *would* justify each claim. They
are computable today only on the **seeded synthetic cohort (§2a)**, which demonstrates the harness
**but is not evidence** (Doc 12 §10): it can prove a detector broken, never prove it works. Real
**prospective, longitudinal, outcome-linked** data (§2b), passed through the §9 gate with the
clinical governance and sign-off of Doc 11, are hard prerequisites for any real-world use. Until
then PureScore 2.0 — the number, the companion vector, and especially the early-warning layer — is
**design, not evidence**, and is claimed as nothing more. That discipline *is* the Babylon lesson
(Doc 00): no claim ships ahead of its evidence.

---

## Executable harness (decision D17)

A runnable subset of these gates ships as **`assets/validation_harness.js`** (`node
docs/purescore/assets/validation_harness.js [N] [seed]`). It extracts the scoring engine from the
calculator (so it can never drift), generates a **seeded synthetic cohort** with an *independent*
ground-truth risk model (non-circular), and reports on a held-out test split:

- **Discrimination:** AUROC (gate ≥ 0.70).
- **Calibration:** adaptive (equal-frequency) ECE for the **raw** score-as-risk vs the
  **isotonic-recalibrated** score (gate ECE ≤ 0.05), Brier skill (gate ≥ 0.05) — demonstrating §4's
  point that *the raw PureScore is a wellness index, not a probability, and must be recalibrated*.
- **Fairness (D7/D15/D18):** AUROC/ECE parity by **natal-sex** and by **ethnicity cut-point tier**
  (asian vs standard, ~UAE mix), max–min ratio gate ≥ 0.80.
- A hard-coded **`PRODUCTION VERDICT: NO-SHIP`** — synthetic self-consistency is necessary, never
  sufficient. Early-warning lead-time / per-tier PPV are explicitly **deferred** to the longitudinal
  generator (§2a, §5), not claimed by the cross-sectional harness.

*Cross-references: README §3 (conventions), §5 (non-negotiables); Doc 00 §3.1 (anti-Babylon);
Doc 01 §4.2 (reference sources); Doc 02/03 §4–§5 (critical cascade, escalation); Doc 05 (sex-specific
/ reproductive frames); Doc 08 (incumbent clinical scores, OOD); Doc 09 (cohorts, calibration,
discrimination, fairness, drift — this doc is its 2.0 extension); Doc 10 (actuarial firewall);
Doc 11 §2 (escalation/fail-safe), §5 (MLOps/versioning/shadow), §8 (Go/No-Go); Doc 12 §3–§7
(context-aware interpretation, companion vector, early-warning, dual framing, defensive bias);
decisions.md D1–D14 (and D15 hormonal-milieu model).*
