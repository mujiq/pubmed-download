# PureScore — Decision Log (`decisions.md`)

> **Purpose.** Single source of truth for every material design decision behind PureScore:
> the question, every option considered, the option **selected**, and why. Maintained from
> 2026-06-14 onward. **Rule: no material design change lands without a corresponding entry here.**
>
> **Legend.** Status: `LOCKED` (decided) · `REVISIT` (provisional) · `SUPERSEDED` (by a later ID).
> Source: `user` (decided via an explicit question to the user) · `auto` (decided autonomously by
> the implementer under the "continue the improvement loop" mandate; logged for review and
> overridable by the user). ★ marks the selected option.

## Index

| ID | Title | Status | Source | Date |
|----|-------|--------|--------|------|
| D1 | Output model (single score vs vector vs early-warning) | LOCKED | user | 2026-06-14 |
| D2 | Early-warning sensitivity posture | LOCKED | user | 2026-06-14 |
| D3 | Treated-to-target / medication context | LOCKED | user | 2026-06-14 |
| D4 | Age & physiologic-state adaptation | LOCKED | user | 2026-06-14 |
| D5 | Honest-and-motivating framing for fixed burden | LOCKED | user | 2026-06-14 |
| D6 | Personal-baseline blend & cold-start | LOCKED | user | 2026-06-14 |
| D7 | Bias / skew / representativeness handling | LOCKED | user | 2026-06-14 |
| D8 | Deliverable scope for the 2.0 work | LOCKED | user | 2026-06-14 |
| D9 | Time-series substrate for the live calculator | LOCKED | auto | 2026-06-14 |
| D10 | Multivariate anomaly approximation in the demo | LOCKED | auto | 2026-06-14 |
| D11 | Dual-framing math (attainable-best ceiling) | LOCKED | auto | 2026-06-14 |
| D12 | Syndromic detector set | LOCKED | auto | 2026-06-14 |
| D13 | Heavy-tailed markers → log-scale personal baseline | LOCKED | auto | 2026-06-14 |
| D14 | Critical-value confirmation mechanism in the demo | LOCKED | auto | 2026-06-14 |
| D15 | Sex/gender reference model (production) | LOCKED | user | 2026-06-15 |
| D16 | Nudge-engine ranking & Modifiability gate | LOCKED | auto | 2026-06-15 |
| D17 | Validation-harness scope, recalibration & gates | LOCKED | auto | 2026-06-15 |
| D18 | Race/ethnicity handling (UAE) | LOCKED | user | 2026-06-15 |
| D19 | Evidence registry, provenance & cold-start ignition | LOCKED | auto | 2026-06-15 |
| D20 | Evidence-crawler change-control gate | LOCKED | auto | 2026-06-15 |
| D21 | UAE-localization scope & Ramadan handling | LOCKED | auto | 2026-06-15 |
| D22 | Wearable-metric trust tiering | LOCKED | user | 2026-06-15 |
| D23 | Continuous personalized scoring & feedback loop | LOCKED | user | 2026-06-16 |

---

## D1 — Output model
**Question.** Should PureScore be a single headline number, a richer multi-dimensional object, or
should it also carry an early-warning/anomaly layer?
**Options.**
- A ★ **Single PureScore + full companion meta-vector + personal-baseline early-warning** — one
  number for triage/engagement, surrounded by uncertainty/direction dimensions, plus a layer that
  flags incubating problems before any band is crossed.
- B — Single score + companion vector only (no early-warning).
- C — Single number only (v0.1).
**Decision.** A. **Rationale.** The single number is necessary for triage/communication but
*insufficient and over-confident* alone; the vector carries its own error bars/direction and the
early-warning layer is the main clinical value-add. **Affects.** Doc 12 §4–5; calculator companion
strip; diagram companion band.

## D2 — Early-warning sensitivity posture
**Question.** How sensitive vs. alarm-fatigue-prone should the early-warning layer be, and who
absorbs soft signals?
**Options.**
- A — High sensitivity, clinician-buffered (soft signals routed to a clinician queue, never the
  patient).
- B ★ **Tiered Watch → Advisory → Alert** — graduated; only multivariate- or
  multi-measurement-**confirmed** signals reach the patient; single-marker drift stays internal.
- C — Conservative, confirmed-trend-only (fewer alarms, later detection).
**Decision.** B. **Rationale.** Keeps sensitivity while protecting the patient from noise; gives
data-error edge cases (hemolyzed K⁺, bad-contact SpO₂) a home (Watch + reconfirm). **Affects.**
Doc 12 §5.3; calculator early-warning tiering; §3.4 confirmation logic.

## D3 — Treated-to-target / medication context
**Question.** How should the score treat a marker controlled by medication, and markers a drug
confounds?
**Options.**
- A ★ **Context-aware bands + 'Managed' state + confounder down-weighting** — controlled value
  shows green but tagged `MANAGED` (never "optimal"); disease keeps the pillar weight; confounded
  markers (β-blocker → HR/HRV/VO₂max) are down-weighted/confidence-reduced, not rewarded/penalized.
- B — Counterfactual untreated-risk scoring (estimate latent risk without the drug).
- C — Controlled = fully green (keep only disease weight).
**Decision.** A. **Rationale.** Most accurate and transparent; credits adherence without implying
"cured"; stops β-blockers corrupting fitness. Counterfactual (B) demotivates well-treated patients
and needs per-drug effect models. **Affects.** Doc 12 §3.1–3.2; calculator `CONFOUNDS`/`MANAGES`
tables, confidence-weighted aggregation, MNG/CNF tags.

## D4 — Age & physiologic-state adaptation
**Question.** How should the score adapt to age and special states (elderly J-curve, ESRD-on-
dialysis, pediatrics) without alarming healthy elders or under-flagging real risk?
**Options.**
- A ★ **Hybrid: guideline age-bands + special frames + actionability modifier** — age-adjust bands
  only where guidelines do (BMI, BP J-curve, age-normed fitness); distinct frames for
  pediatric/pregnancy/ESRD; keep absolute anchors for acute-danger markers (K⁺, SpO₂, glucose
  extremes); add an expected-benefit modifier that de-prioritizes (not hides) low-yield flags.
- B — Fully age/state-adjusted bands (simplest; risks under-flagging).
- C — Absolute anchors everywhere + actionability layer only (most yellows for elders).
**Decision.** A. **Rationale.** Truthful + non-alarmist + doesn't under-treat. **Affects.** Doc 12
§3.3; calculator persona band overrides (elderly/dialysis/pregnancy), eGFR exclusion frame.

## D5 — Honest-and-motivating framing for fixed burden
**Question.** How to stay honest about absolute risk yet motivating for patients with fixed/un-
fixable burden (genetics, age, irreversible damage, multimorbidity)?
**Options.**
- A ★ **Dual framing: absolute score + "progress to your attainable best" + competing-risk
  weighting** — keep the absolute truth; add a second coordinate grading effort against a
  realistically-achievable ceiling; mute pillars irrelevant to the patient's prognosis.
- B — Absolute only (lean on trajectory + modifiability).
- C — Competing-risk weighting only, no personal ceiling.
**Decision.** A. **Rationale.** Rewards controllable effort without lying about absolute risk;
`Modifiability` connects to nudges and ensures un-fixable risk isn't framed as personal failure.
**Affects.** Doc 12 §6; calculator dual-framing readout (D11).

## D6 — Personal-baseline blend & cold-start
**Question.** How should the personal baseline (basis for early-warning z-scores) blend with
population norms, and how fast should we trust it?
**Options.**
- A ★ **Empirical-Bayes shrinkage** — start from cohort prior, shrink toward personal baseline as
  measurements accumulate; per-marker minimum counts; more weight on stable/low-noise markers.
- B — Population until N readings then switch (discrete jump).
- C — Personal-first / aggressive (max sensitivity, more noise).
**Decision.** A. **Rationale.** Smooth, principled, no hard switch; sensitivity grows with data;
consistent with Doc 09 shrinkage. **Affects.** Doc 12 §5.1; calculator personal-baseline z-score
(D9).

## D7 — Bias / skew / representativeness handling
**Question.** When the patient is poorly represented by the reference cohort (OOD, small cell,
heavy-tailed marker), what should happen?
**Options.**
- A ★ **Defensive** — low representativeness lowers Confidence AND, below a threshold, suppresses
  the cohort-percentile component in favour of clinical anchors + personal baseline; plus fairness
  mechanics (race-free eGFR, robust/log transforms, partial pooling, subgroup calibration).
- B — Display representativeness as info only (don't change computation).
- C — Hard gate when OOD (withhold cohort-relative parts).
**Decision.** A. **Rationale.** Most equitable and robust; never leans on unreliable cohort stats;
representativeness adjusts *confidence*, never *penalty*. **Affects.** Doc 12 §7; calculator `rep`
factor on confidence.

## D8 — Deliverable scope
**Question.** What to build from the 7 design decisions?
**Options.**
- A ★ **Doc 12 + upgraded calculator + refreshed diagram.**
- B — Doc 12 spec only.
- C — Doc 12 + calculator (no diagram).
**Decision.** A. **Rationale.** The interactive calculator and diagram make the behaviour
inspectable; spec alone is harder to validate against intuition. **Affects.** All three artifacts.

---

## D9 — Time-series substrate for the live calculator *(auto)*
**Question.** The companion vector's Trajectory/Volatility/personal-baseline/early-warning need a
history, but the demo only has single values. How should the calculator obtain a time series?
**Options.**
- A ★ **Deterministic, seeded synthetic per-marker history** — generate an N≈90-day daily series
  per (persona, marker) from a seeded RNG, with the latest sample pinned to the live input value, a
  per-persona/per-marker trend, and marker-scaled noise. Fully reproducible; edits shift the
  endpoint; enables real slope/variance/z-score math.
- B — Random (unseeded) history regenerated each render (non-reproducible; flickers).
- C — Keep Trajectory tied to the manual "simulate days" button only (no live early-warning).
**Decision.** A. **Rationale.** Reproducible and inspectable; lets the early-detection thesis be
*computed*, not asserted; clearly labelled synthetic. **Affects.** calculator time-series engine,
personal-baseline z-score (D6), live Trajectory/Volatility/Early-warning, time-to-threshold.

## D10 — Multivariate anomaly approximation in the demo *(auto)*
**Question.** Doc 12 §5.3 calls for Mahalanobis multivariate anomaly, which needs a full covariance
matrix the demo can't credibly estimate. How to approximate "multivariate" early warning?
**Options.**
- A ★ **Diagonal-covariance aggregate z** — `D ≈ sqrt(Σ zᵢ²)` over independent personal-baseline
  z-scores (assume diagonal covariance), thresholded for the Advisory tier; clearly labelled as an
  approximation of the full Mahalanobis distance in the spec.
- B — Full empirical covariance + Mahalanobis (not credibly estimable from synthetic demo data;
  false precision).
- C — Skip multivariate in the demo (count concurrent single-marker z's only).
**Decision.** A. **Rationale.** Captures the "many small simultaneous shifts" signal without
pretending to a covariance estimate the demo can't support; honest and computable. The production
spec (Doc 12) retains full Mahalanobis. **Affects.** calculator early-warning Advisory trigger.

## D11 — Dual-framing math (attainable-best ceiling) *(auto)*
**Question.** How to compute "progress to your attainable best" (D5) concretely?
**Options.**
- A ★ **Modifiable-optimum ceiling** — recompute the score with every *modifiable* marker set to
  its optimal value while *fixed* markers (persona `fixed`, age/genetic) stay as-is; `Progress =
  (PureScore − floor) / (ceiling − floor)`, where the floor recomputes with modifiable markers at a
  poor reference. Reuses the live scoring engine; no new model.
- B — Heuristic "% of pillars green" (ignores severity/weighting).
- C — Fixed cohort-relative percentile of achievable improvement (needs external data).
**Decision.** A. **Rationale.** Uses the same scoring function for internal consistency; cleanly
separates fixed vs modifiable via the existing `isModifiable` tag. **Affects.** Doc 12 §6;
calculator dual-framing readout.

## D12 — Syndromic detector set *(auto)*
**Question.** Which named cross-pillar syndromes should the demo's pattern detectors cover?
**Options.**
- A ★ **Metabolic syndrome, frailty phenotype, CKD-progression** — three high-value, well-defined,
  multi-marker patterns spanning the pillars already modelled.
- B — A larger battery (sepsis, decompensation, thyroid storm, …) — many need markers/temporal
  resolution the demo lacks.
- C — None (rely on aggregate anomaly only).
**Decision.** A. **Rationale.** Highest signal-to-effort using existing markers; each maps to a
recognised clinical construct. Extensible later. **Affects.** Doc 12 §5.3; calculator syndromic
detectors.

## D13 — Heavy-tailed markers → log-scale personal baseline *(auto)*
**Question.** Right-skewed markers (hsCRP, UACR, ALT, FIB-4, bilirubin) make a naive personal-
baseline z misleading (a spike dominates). How should the anomaly z handle them?
**Options.**
- A ★ **Log-transform the personal baseline for skew-tagged markers** — compute z on `log(value)`
  for markers flagged `skew`, keeping slope/volatility on the raw scale (consistent with the bands);
  surface a `Robust(log) handling` readout when such a marker is elevated.
- B — Raw-scale z everywhere (simple; lets skew spikes produce false anomalies).
- C — Winsorize/clip the tail (loses real extreme signal).
**Decision.** A. **Rationale.** Matches Doc 12 §7 robust-transform mandate; the log scale is the
standard robust handling for these biomarkers; raw slope/band logic is preserved for consistency.
**Affects.** Doc 12 §4/§7; calculator `skew` tags + `baseline()` log branch + `robust` readout.

## D14 — Critical-value confirmation mechanism in the demo *(auto)*
**Question.** Doc 12 §3.4 routes an *isolated implausible* critical (hemolyzed K⁺, bad-contact
SpO₂) to Watch+reconfirm rather than an emergency cascade. How to model this safely in the demo
without ever auto-suppressing a genuine emergency?
**Options.**
- A ★ **Explicit data-quality flag drives the downgrade** — a marker marked `artifacts`
  (implausible / low device quality, *as determined by the data-quality layer*) has its confidence
  forced ≈0, so it cannot fire the cascade; it surfaces as `Reconfirm pending` + Watch. A
  *confirmed* critical (full confidence) still cascades. Corroboration would re-raise confidence.
- B — **Auto-downgrade any extreme value** beyond a plausibility band — **rejected: unsafe**, it
  would silently suppress real emergencies (true K⁺ 7.0 happens).
- C — Always cascade (current v0.1) — alarm fatigue from lab/device artifacts.
**Decision.** A. **Rationale.** Safety-first: the system never auto-suppresses a *confirmed*
critical; only data the quality layer has flagged as low-confidence is held, and only pending
reconfirmation. Mirrors the `exclude`/confidence mechanism already used for dialysis eGFR.
**Affects.** Doc 12 §3.4; calculator `ctxOf` artifact handling, `reconfirm` readout, `labartifact`
persona.

## D15 — Sex/gender reference model (production) *(user)*
**Question.** For production, how should PureScore select sex-specific reference ranges across the
whole marker catalogue (so male AND female handling is airtight, including trans/HRT, pregnancy,
menopause, intersex)?
**Options.**
- A ★ **Hormonal-milieu aware** — keep NATAL SEX as the genetic/organ baseline, but collect gender
  identity + active HRT + pregnancy/menopause status, and override hormone-sensitive markers
  (Hgb/hematocrit, ferritin, lipids/ApoB, sex hormones, BMD, creatinine-based eGFR, urate, ALT) by
  the CURRENT hormonal milieu. Moderate data model; inclusive; production-standard.
- B — Binary natal-sex toggle only (mis-scores trans/HRT, pregnancy, intersex).
- C — Full biological-context model (organ inventory + measured hormone levels per marker) — most
  rigorous but heaviest data model and most fields missing in practice.
**Decision.** A. **Rationale.** Correct biology drives ranges (Hgb tracks the androgen milieu, not
the birth certificate), inclusive and equitable, while remaining collectable; natal sex is retained
where it is genuinely the governing axis (organ/genetic). Consistent with Doc 12 §2's
"hormone-therapy-aware ranges." **Affects.** Doc 05 (authoritative production reference tables);
calculator hormonal-context selector (natal sex + HRT + life stage) and `band()` resolution;
new trans-HRT / menopause personas; fairness slices in Doc 13 (D17).
**Implementation note.** Per-marker `axis` ∈ {none (sex-invariant), gonadal (current hormonal
milieu), natal (immutable)}. Established-HRT threshold (≈6–12 mo) flips the gonadal axis; life-stage
modifier tables (cycle/pregnancy-trimester/postpartum/peri-&post-menopause/andropause) adjust bands;
acute-danger anchors (K⁺, SpO₂, glucose extremes) stay absolute regardless of context.

## D16 — Nudge-engine ranking & Modifiability gate *(auto)*
**Question.** How should the live nudge engine rank daily actions, and how should it use the new
Modifiability and Trajectory companion signals?
**Options.**
- A ★ **Ease-weighted utility (Doc 07 §3.1) with an exact impact + Modifiability gate** —
  `U = [ΔPureScore·ε]^α · p̂^β · (1−E)^η · ν`; impact is the **exact finite-difference ΔPureScore@30d**
  (recompute through the engine, not a lookup — preserves diminishing returns in green and refuses
  false hope on near-permanent burdens); **lifestyle actions are Modifiability-gated** (cannot move a
  fixed/genetic marker — e.g. FH ApoB), while **medication/clinical** actions can; the binding/worst
  pillar gets the `ν` bonus (Trajectory-aware); per-class diversity cap; safety/cohort caveats
  (CKD protein cap, pregnancy/lactation no-deficit, anticoagulant ω-3).
- B — Pure impact ranking (ignore effort) — surfaces high-impact actions patients won't do.
- C — Effort-only "easiest" (ignore impact) — trivial, low-yield actions.
**Decision.** A. **Rationale.** Faithful to the Doc 07 spec; honest (no credit for moving an
already-green or a genetically-fixed marker); when lifestyle is gated out on a fixed red, the engine
**routes to a clinician** rather than fabricating a lifestyle fix. **Affects.** Doc 07 §10 (impl
status); calculator `NUDGES` library + `nudgeImpact`/`nudgeRank` + daily-nudge panel.

## D17 — Validation-harness scope, recalibration & gates *(auto)*
**Question.** What should the **executable** validation harness compute, how should it recalibrate,
and how honest must its scope be?
**Options.**
- A ★ **Calibration-first, isotonic-recalibrated, fairness-sliced, cross-sectional, NO-SHIP-verdict** —
  on a seeded synthetic cohort with an *independent* ground-truth model (non-circular), held-out
  train/test: AUROC, adaptive ECE (raw **and** isotonic-recalibrated), Brier skill, natal-sex
  subgroup parity; early-warning lead-time/PPV explicitly **deferred** to the longitudinal generator;
  production verdict hard-coded **NO-SHIP**.
- B — Score-as-probability without recalibration — **misleading**: PureScore is a wellness index, not
  a calibrated probability; raw ECE is large by construction.
- C — Full longitudinal lead-time/PPV simulation now — large build, premature without the cohort
  generator; risks over-claiming the early-warning evidence.
**Decision.** A. **Rationale.** Faithful to Doc 13 §4 (recalibration mandatory before reading the
score as risk); **isotonic** chosen over Platt for robustness (1-param Platt left finite-sample ECE
at the gate boundary); scope honestly bounded to what a cross-sectional cohort can support; verdict
encodes the Babylon discipline (synthetic ≠ evidence). **Affects.** `assets/validation_harness.js`;
Doc 13 executable-harness section.

## D18 — Race/ethnicity handling (UAE) *(user)*
**Question.** How should the engine use race/ethnicity for the UAE population mix (Emirati/Gulf
Arab, South-Asian majority, other Arab, Filipino/SE-Asian, Western, African)?
**Options.**
- A ★ **Context & screening, never penalty** — ethnicity adjusts the *reference frame & screening*:
  (i) guideline-validated biology-based cutoffs (WHO South-Asian BMI ≥23/27.5, lower waist
  thresholds); (ii) select/recalibrate risk equations validated for the group (ASCVD under-predicts
  South Asians); (iii) raise screening for high-prevalence heritable conditions (G6PD,
  thalassemia/hemoglobinopathy, consanguinity-linked, FH); (iv) **race-free eGFR (CKD-EPI 2021)**;
  (v) self-reported & optional — absence lowers Confidence, never penalizes.
- B — Strictly ethnicity-blind (misses validated differences; a real blind spot for the South-Asian
  majority).
- C — Direct ethnicity risk multiplier — **rejected**: penalizes a protected class; the retracted
  eGFR race-coefficient mistake.
**Decision.** A. **Rationale.** Equitable *and* clinically complete: uses ethnicity only where a
guideline validates a biology-based difference or a screening priority; never as a penalty; race-free
eGFR; consistent with D7 (absence → confidence, not penalty) and the Doc 10/11 no-protected-class
rule. **Affects.** Doc 15 (UAE localization); calculator ethnicity selector → waist/BMI cutoffs +
screening flags + risk-equation note; evidence registry; Doc 13 ethnicity fairness slice.

## D19 — Evidence registry, provenance & cold-start "ignition" *(auto)*
**Question.** How should every band/threshold/rule/action carry its evidence, and how should the
effective range evolve from guideline → real-world cohort → personal baseline over time?
**Options.**
- A ★ **Machine-readable evidence registry + three-stage ignition provenance** — a versioned
  `evidence-registry.json` of stable-ID citations (body, title, year, version, jurisdiction, URL/DOI,
  `last_verified`, `applies_to[]`); every engine band/rule/action references one or more evidence IDs.
  Each effective band's value is a **provenance blend** `θ_clinical·anchor + θ_cohort·cohortRange +
  θ_personal·personalBaseline` whose weights shift over time (empirical-Bayes, D6/D9): **crank** on the
  clinical guideline (cold start), **warm up** with the real-world cohort percentile, then **run** on
  the personal-baseline z-score as data accrues — the "diesel ignition" model.
- B — Hard-coded citations in prose only (not machine-maintainable; crawler can't update).
- C — Personal-baseline only once available (discards guideline anchor; unsafe cold start, drift).
**Decision.** A. **Rationale.** Separates *evidence* (registry, crawler-maintained) from *engine*
(references IDs); the ignition model makes the cold-start→personalized transition explicit and
auditable and prevents both unsafe cold starts and over-fitting to a noisy personal baseline.
**Affects.** Doc 14; `assets/evidence-registry.json`; calculator provenance fields + readout; Doc 09
(shrinkage) cross-link.

## D20 — Evidence-crawler change-control gate *(auto)*
**Question.** When the external crawler detects a guideline change to a cited band/threshold, may it
update the engine automatically?
**Options.**
- A ★ **Detect-and-stage, human-in-loop gate** — the crawler flags drift (new version, changed
  value, dead URL, superseded guideline), writes a staged proposal with diff + evidence, and a
  clinician/governance reviewer **must approve** before any band changes; a band change is a
  model-version bump that re-triggers validation (Doc 13) and governance (Doc 11). `last_verified`
  refreshes automatically; *values* never do.
- B — Auto-apply guideline changes (fast, but an unreviewed/incorrect crawl could silently move a
  safety threshold — unacceptable).
- C — Manual-only (no crawler) — stale guidelines, the maintenance burden this task exists to solve.
**Decision.** A. **Rationale.** Safety-first currency: machines surface change, humans approve
clinical impact; nothing safety-relevant moves without sign-off + re-validation. **Affects.** Doc 14
crawler contract; registry `review_status`/`last_verified` fields.

## D21 — UAE-localization scope & Ramadan handling *(auto)*
**Question.** How should UAE localization be scoped — which ethnicity cut-point tiers, and how to
handle Ramadan fasting?
**Options.**
- A ★ **Tiered cut-points + race-free eGFR + raised heritable screening + IDF-DAR Ramadan safety** —
  WHO-Asian BMI / IDF waist for the UAE-prevalent higher-risk groups (South-Asian, SE-Asian/Filipino,
  Gulf/other-Arab), standard for Western/African; eGFR race-free; raise G6PD/thalassemia/FH/vitamin-D
  screening; Ramadan applies IDF-DAR med-timing + hydration + an **absolute break-the-fast safety
  rule** (glucose <70 / symptoms); ethnicity adjusts **Confidence only**, never penalty (D18).
- B — Single universal cut-point (ignore ethnicity) — under-calls the South-Asian-majority population.
- C — Per-nationality bespoke tables — over-fit, sparse data, brittle; little added validity over the
  tiered model.
**Decision.** A. **Rationale.** Uses only guideline-validated, biology-based group differences; tiers
keep it robust and maintainable; Ramadan safety is a hard rule, not a nudge; fully consistent with
D18 (context-not-penalty) and the Doc 10/11 equity firewall. **Affects.** Doc 15; calculator
ethnicity/Ramadan selectors, `ethTier()`/`band()` cut-points, localization readout, `southasian` &
`ramadan_dm` personas; Doc 16 actions; registry UAE entries.

## D22 — Wearable-metric trust tiering *(user)*
**Question.** Consumer wearables are rich but not medical-grade. How should their metrics feed
PureScore?
**Options.**
- A ★ **Tiered trust** — wearables fully power **trajectory, early-warning, and nudges**, and feed
  the **score weighted by device grade**: *clinical-grade* (CGM, validated BP cuff, single-lead
  ECG/AFib) ≈ lab weight; *consumer-validated* (resting HR, steps, sleep duration) discounted;
  *inferential/derived* (readiness, stress, sleep-stages) = **informational only**, never a band.
  A wearable-only anomaly may raise **Watch/Advisory**, but a **clinical-grade confirmation is
  required before it drives a red/critical**.
- B — Companion-only: wearables touch trajectory/early-warning + nudges but never move the headline
  score (under-uses the continuous data).
- C — Equal weight to labs (over-trusts consumer sensors — the Babylon risk).
**Decision.** A. **Rationale.** Uses the rich continuous signal where it is strongest (motion,
early-warning) while keeping the headline clinical score anchored and refusing to let an inferred
metric trigger a critical — Babylon-safe, and honest via the companion **Confidence**/data-quality
flags. **Affects.** Doc 18 (data streams & devices); the input-type taxonomy & reliability flags;
companion-vector confidence weighting; deck Data-&-Experience + Day-in-the-Life sections.

## D23 — Continuous personalized scoring & feedback loop *(user)*
**Question.** Should PureScore move with short-term behaviour so the patient gets feedback, and how
do biomarkers/wearables/baselines feed that without becoming clinically dishonest?
**Decision.** Make the score **continuous and personally responsive**, not discrete:
- **No band jumps in the number.** Stage 1 is already `C⁰`/`C¹`-continuous; green/yellow/red are
  *labels read off the curve*, never the generator (Doc 03 §1).
- **Within-green optimum-centering ON by default** — a small gradient toward each marker's optimum
  so the score moves even inside green (Doc 03 §1; Doc 12 §5.2).
- **New Stage 2b — personal-baseline z-score** (`r_i^pers = κ·tanh(z_i/2)`, `κ=0.10`,
  `band_clamp`): better-than-your-baseline nudges PureScore **up**, worse trends it **down**,
  updated daily for wearables/behaviour and per-measurement for labs (Doc 03 §2b).
- **Positive-`Δ` guarantee on the top-5** — every recommended action carries a strictly positive
  expected `ΔPureScore`; negative behaviour trends the number and the companion **Trajectory /
  Early-warning** dimensions down (Doc 07 §3.3–§3.4; Doc 12 §5).
**Guardrails.** Safety always dominates: `band_clamp` and the clinical/cohort `max` mean
personalization can never relax a red, clear a critical, or flip a band; fixed/irreversible burden
shows as low **Modifiability** so there is no false hope (D5/D16). **Affects.** Doc 03 (§1, new §2b,
§6.1, constants), Doc 07 (§3.3–§3.4), Doc 12 (§5.1); the `tech/` interactive feedback-loop page.

---

### Maintenance notes
- New decisions append as `D24+`. When a decision changes, mark the old one `SUPERSEDED → Dn` and
  add the replacement; never edit history in place.
- Each entry must name the artifacts it **Affects** so downstream code/docs stay traceable.
