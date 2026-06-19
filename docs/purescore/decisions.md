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
| D15 | Sex/gender reference model (production) | SUPERSEDED→D27 | user | 2026-06-15 |
| D16 | Nudge-engine ranking & Modifiability gate | LOCKED | auto | 2026-06-15 |
| D17 | Validation-harness scope, recalibration & gates | LOCKED | auto | 2026-06-15 |
| D18 | Race/ethnicity handling (UAE) | LOCKED | user | 2026-06-15 |
| D19 | Evidence registry, provenance & cold-start ignition | LOCKED | auto | 2026-06-15 |
| D20 | Evidence-crawler change-control gate | LOCKED | auto | 2026-06-15 |
| D21 | UAE-localization scope & Ramadan handling | LOCKED | auto | 2026-06-15 |
| D22 | Wearable-metric trust tiering | LOCKED | user | 2026-06-15 |
| D23 | Continuous personalized scoring & feedback loop | LOCKED | user | 2026-06-16 |
| D24 | Stress as a pillar? → companion Stress-load score | LOCKED | user | 2026-06-18 |
| D25 | Close the intake loop: adherence, persona-determination, goals | LOCKED | user | 2026-06-19 |
| D26 | Close the onboarding cluster: first-run flow, demographics, cold-start, consent | LOCKED | user | 2026-06-19 |
| D27 | Sex-binary scoring; collapse gender→sex; remove trans handling (supersedes D15) | LOCKED | user | 2026-06-19 |
| D28 | Typed source channel per marker (enforceable source-fusion) | LOCKED | user | 2026-06-19 |
| D29 | Typed wearable corroboration (question→metric→tolerance) | LOCKED | user | 2026-06-19 |
| D30 | Close the last P2 audit items: cadence freshness-SLA + HEP/REN symptom items | LOCKED | user | 2026-06-19 |
| D31 | Wiki navigation: progressive "book" arc, collapsible groups, Connects-to footers | LOCKED | user | 2026-06-19 |
| D32 | Calculation Explorer (audit tree + map) on a JSON-canonical shared engine | LOCKED | user | 2026-06-20 |
| D33 | Cohort-matched cold-start: impute missing LAB biomarkers from age×sex×life-stage cohort medians | LOCKED | user | 2026-06-20 |
| D34 | Full decision-tree explorer: missing/incomplete data, every branch shown, filter-search | LOCKED | user | 2026-06-20 |

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
> **SUPERSEDED by D27 (2026-06-19).** The production model is now **sex-binary (Male/Female)**;
> the `gender_identity`/HRT/affirmed-milieu (GAHT) machinery described below has been removed.
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
- **New Stage 2b — personal-baseline z-score** (`r_i^pers = κ_resp·tanh(z_i/2)`, `κ_resp=0.10`,
  `band_clamp`): better-than-your-baseline nudges PureScore **up**, worse trends it **down**,
  updated daily for wearables/behaviour and per-measurement for labs (Doc 03 §2b).
- **Positive-`Δ` guarantee on the top-5** — every recommended action carries a strictly positive
  expected `ΔPureScore`; negative behaviour trends the number and the companion **Trajectory /
  Early-warning** dimensions down (Doc 07 §3.3–§3.4; Doc 12 §5).
**Guardrails.** Safety always dominates: `band_clamp` and the clinical/cohort `max` mean
personalization can never relax a red, clear a critical, or flip a band; fixed/irreversible burden
shows as low **Modifiability** so there is no false hope (D5/D16). **Affects.** Doc 03 (§1, new §2b,
§6.1, constants), Doc 07 (§3.3–§3.4), Doc 12 (§5.1); the `tech/` interactive feedback-loop page.

## D24 — Stress as a 13th pillar? *(user)*
**Question.** Users intuitively want a "Stress score." Should **Stress** be promoted from a
cross-cutting reservoir + distributed markers to a standalone **scored pillar** with its own
PureScore weight — and if surfaced, what should it measure? *(Lens chosen: product/user clarity;
definition chosen: physiological stress load — HRV, resting-HR, cortisol slope, sleep disruption.)*
**Options.**
- A ★ **Companion "Stress-load" score, not a pillar.** Surface a prominent **Stress-load** dimension
  in the companion meta-vector (Doc 12 §4.1): physiological by definition (HRV depression, resting-HR
  elevation, cortisol slope, sleep disruption) blended with the existing **ALLO** allostatic-load
  reservoir (Doc 04). **Read-only** — reuses markers already owned by CV/ENDO/SLP and the ALLO
  `κ`-couplings, and carries **no weight in the PureScore aggregation**, so users get the score with
  zero double-counting.
- B — **True 13th pillar, markers relocated** into Stress (clean ownership, but depletes CV/ENDO/SLP,
  shifts their scores, and forces a full 13-pillar re-weight).
- C — **True 13th pillar, shared/down-weighted markers** (breaks one-marker-one-home, complicates
  audit, still risks double-count).
- D — **Promote the ALLO reservoir to a visible surface** but keep it a reservoir (close to A, minus
  the acute autonomic blend users feel day-to-day).
**Decision.** **A — Stress is *not* taken as a pillar; it is surfaced as the Stress-load companion
score.** **Rationale.** (1) Stress is *upstream and cross-cutting*, not an organ system — it already
propagates into CV/MET/SLP/MCS/ENDO via the ALLO reservoir's `κ` matrix (Doc 04); a weighted pillar
on top would **double-count** the same allostatic burden. (2) The physiological definition draws on
HRV/RHR/cortisol/sleep signals **already scored** inside CV/ENDO/SLP — relocating or sharing them
damages those pillars or the audit trail. (3) Inferred stress/readiness is **inferential/consumer-tier**;
**D22** already rules it *informational-only, never a band* — a companion score honours that, a
scored pillar would violate it. (4) The product goal is met without aggregation surgery: a headline
**Stress-load: low / elevated / high** chip, paired with **Confidence**, gives users the number they
want honestly. **Guardrails.** Stress-load never moves the headline PureScore, never sets or clears a
band, never triggers a critical; it may only raise **Early-warning** Watch/Advisory (Doc 12 §5) and
re-rank stress-reducing nudges (Doc 07). **Affects.** Doc 12 §4 (new companion dimension `St`, §4.1),
Doc 02 (cross-cutting modifiers note), Doc 04 (ALLO surfaced as the chronic component), Doc 07
(stress-down nudge ranking); the `tech/` companion-vector surfaces.

## D25 — Close the intake loop (adherence · persona-determination · goals) *(user)*
**Question.** The product-loop coverage audit (Appendix G) found the capture→…→adherence loop open at
three stages: **adherence check-ins** (F1, P0), **input→persona determination** (F2, P0), and a
**user-goals catalogue** (F3, P1). How should these be closed?
**Options.**
- A ★ **Machine-readable data + reference appendices, with the audit auto-closing the findings.** Add
  `data/adherence.json`, `data/persona-matrix.json`, `data/goals.json` (engine-ready), each surfaced as a
  new appendix (H · Adherence, I · Persona matrix, J · Goals). Wire the audit's live stats so F1/F2/F3
  flip to **Addressed** automatically on rebuild, with a dated v3 finding version recording closure.
- B — Fold into existing pages (adherence as a question-bank section, goals into Appendix F, matrix into
  the audit sketch). Fewer pages, but mixes new systems into existing ones.
- C — Documentation-only prose specs, manual status flip (doesn't feed the engine or auto-close).
**Decision.** **A.** **Rationale.** Keeps everything in the established `data/*.json` → builder pipeline;
the artifacts are engine-ready (reservoir inflows, softmax posterior, applicability-keyed targets) and the
audit *self-verifies* closure from live data rather than by hand. Depth = **comprehensive seed**: 41
adherence check-ins (one per nudge family), the full 37-persona × 36-signal matrix, 32 goals (≥2/pillar).
**Design notes.** (1) *Adherence* — each check-in writes `adherence ∈ [0,1]` + a reason-taxonomy code back
to its reservoir (Doc 04), the engagement/Trajectory dimension (Doc 12), and nudge feasibility (Doc 07 §3).
(2) *Persona determination* — posterior = softmax over signed per-column evidence; hard sex/age/life-stage
priors zero impossible columns first; many-to-many; resolves to the comprehensive persona set (F6). (3)
*Goals* — applicability vector (persona · archetype · life-stage · age · sex · condition) + a wearable/lab/PRO
target + modifiability + linked nudges; drives the UserGoals lifecycle on the state machine.
**Guardrails.** Illustrative weights/targets — calibrate before production; adherence is self-report
(trust-tiered, corroborated by wearables where possible); goals never relax a clinical anchor.
**Affects.** Appendix G (auto-close F1–F3, v3 versions), new Appendices H/I/J, `data/adherence.json`,
`data/persona-matrix.json`, `data/goals.json`; Doc 04 (reservoir inflows), Doc 07/16 (nudge↔adherence),
the `states.html` UserGoals lifecycle.

## D26 — Close the onboarding cluster (first-run flow · demographics · cold-start · consent) *(user)*
**Question.** The audit's front-of-loop pass (v4) found nobody owned the first-run experience: no onboarding flow
(F9), an under-specified demographic field-set (F10), no question-side cold-start / progressive-profiling strategy
(F11), and consent & device-pairing not sequenced into intake (F12). How to close them?
**Options.**
- A ★ **One machine-readable onboarding spec + appendix, auto-closing the audit.** Add `data/onboarding.json`
  (ordered first-run steps, demographic field-set, cold-start bootstrap + progressive profiling, consent/device
  gating) surfaced as an **Onboarding & first-run** appendix (+ a spreadsheet grid per the grid rule). Wire the
  audit's live stats so F9–F12 flip to **Addressed** on rebuild, with dated v5 versions.
- B — Fold the spec into Doc 01/18 prose (no structured data; manual status flip; doesn't auto-verify or feed the engine).
- C — Defer; leave the front-of-loop open.
**Decision.** **A.** **Rationale.** Same proven pattern as D25 (data → builder → audit auto-verifies). The flow is
engine-ready and integrates the pieces already built — question bank (Appx E), persona matrix (Appx I), goals
(Appx J) — into a coherent first-run that the **Onboarding sub-machine** on the state machine mirrors.
**Design notes.** (1) *Flow* — 11 ordered steps: welcome → account → **consent** → demographics → goal-seed →
bootstrap PROs → persona-resolve → **device-pair** → EHR-connect → first-score → progressive-profiling. (2)
*Demographics* — 11 enumerated fields, each tagged with what it drives (physiology · cohort stratification · UAE
cut-points · localization). (3) *Cold-start* — a 7-item bootstrap set seeds a day-0 score + resolves a persona;
progressive profiling deepens by P1→P5 applicability. (4) *Consent/device* — privacy consent gates all capture;
device-pairing gates wearable metrics; EHR-connect gates Patient360; actuarial layer firewalled/off by default.
**Guardrails.** Illustrative — calibrate the bootstrap set & demographic enums before production; consent is
informed and granular; nothing is captured before consent (Doc 11). **Affects.** Appendix G (auto-close F9–F12, v5
versions), new Onboarding appendix + grid, `data/onboarding.json`; integrates Doc 01 (demographics), Doc 11
(consent), Doc 14 (ignition/cold-start), Doc 15 (UAE), Doc 18 (device/household), and the `states.html` Onboarding
sub-machine.

---

## D27 — Sex-binary scoring; collapse gender→sex; remove trans handling *(user)*
**Question.** How should the engine represent sex/gender, given the platform is being scoped to a
sex-binary physiological model?
**Options.**
- A ★ **Sex-binary (Male/Female), gender collapsed into sex** — a single `sex ∈ {male, female}`
  field drives all physiology; the separate `gender_identity` field, the GAHT/HRT affirmed-milieu
  ranges, and the trans clinical personas/archetypes are removed.
- B — Keep the D15 two-attribute (`sex_at_birth` × `gender_identity`) hormonal-milieu model.
**Decision.** A (supersedes **D15**). **Rationale.** Product scoping decision: PureScore is split and
presented as **PureScore — Male** and **PureScore — Female**; the engine models sex as a binary
physiological input. **Affects.** Docs 01 §1, 05 §1 (§1.2 removed), 09 §1, 12 §2, 13 §6.1;
`Q_CORE_SEX` (now Male/Female, `Q_CORE_GENDER` removed); `wiki_content.PERSONAS` (transfem/transmasc
removed); `persona-axes.json` (gender_transition archetype removed); question-bank / goals /
persona-matrix / dossier (rebuilt); the PureScore section's Male & Female pages. Rare intersex/DSD
remain handled via `organ_inventory` (Doc 05 §1.3).

## D28 — Typed source channel per marker *(user)*
**Question.** The audit (F7) found markers carry a free-text guideline `source` but no **typed channel**, so the
per-pillar source-fusion rules (Appendix G §fusion) are illustrative, not enforceable: the engine can't tell which
channel a value came from. How to fix?
**Options.**
- A ★ **Deterministic `marker_channel()` classifier** resolving every marker to one of six typed channels
  {biomarker-lab · wearable-clinical · wearable-consumer · wearable-inferential · self-report · derived} from its
  name + source, surfaced as a **Channel** column (Appendix A + filterable on the grid); the audit's `source_typed`
  stat then auto-closes F7.
- B — Hand-tag a `channel` field on all 80 PILLARS tuples (most explicit, but 80 invasive edits and drifts from the
  authored source text).
- C — Leave as free-text source (status quo; fusion stays illustrative).
**Decision.** **A.** **Rationale.** Total and deterministic over the catalogue, maintainable (one classifier + clear
keyword precedence), and consistent with the audit's existing channel enum. Makes the source-fusion hierarchy
*programmatically enforceable* — the accuracy-weighted rules can read each marker's channel. (While doing this,
fixed a latent bug: the biomarkers **grid** had a mis-aligned 10-field tuple unpacking.) **Guardrails.** Channels are
design-time classifications — re-verify edge cases (imaging folded into biomarker-lab; clinical anthropometry) before
production; the channel never overrides the D22 trust-tier safety rule (consumer/inferential can't drive a red).
**Affects.** `wiki_content.marker_channel()` + `_CHANNELS`/`_CHAN_META`; Appendix A (Channel column) and the
biomarkers grid (Channel filter, + tuple-unpacking fix); Appendix G (`source_typed`, F7 auto-close v6, §fusion
"now typed" note); Doc 01 data model (channel enum).

## D29 — Typed wearable corroboration (question → metric → tolerance) *(user)*
**Question.** The audit (F4) found the question bank's wearable corroborations were semi-typed (`wearable.<metric>`)
but not linked to a canonical metric registry and, crucially, carried no **tolerance** — so the wearable-match
(perceived↔actual) stage couldn't be automated or audited. How to type it?
**Options.**
- A ★ **Canonical registry + per-metric tolerance** (`data/wearable-corroboration.json`): map all 23 `wearable.*`
  refs (normalizing casing/alias variants) to ~17 canonical metrics (Appendix B), each with a source channel and a
  **per-metric default tolerance** (the agreement band beyond which a perceived↔actual gap is flagged). Surfaced as
  an appendix + grid; `wear_corr_typed` auto-closes F4.
- B — Tiered-by-trust tolerance (a rule from the D22 tier, no per-metric values) — coarser.
- C — Per-question tolerances (278 values) — most precise, heavy to author.
- D — Edit the source qb-*.json to add typed corroboration — invasive, drifts from authored data.
**Decision.** **A**, scope **wearable.* only** (F4's stage; `lab.*`/`bcm.*` are a different "does-the-lab-agree"
semantic). **Rationale.** ~17 metrics × one tolerance is tractable and maintainable, normalizes casing
(`hrv`=`HRV`, `SpO2`=`spo2`), and makes stage-3 auditable — every wearable corroboration resolves to a metric +
tolerance the perceived-vs-actual gap engine (Appendix F) applies. **Guardrails.** Tolerances are design defaults
(calibrate per device/cohort); consumer/inferential metrics never drive a band alone (D22) — only flag gaps / move
Confidence. **Affects.** `data/wearable-corroboration.json`, new Wearable-corroboration appendix + grid; Appendix G
(`wear_corr_typed`, F4 auto-close v6); links Appendix B (metrics), E (questions), F (gap engine), Doc 12 (Confidence).

## D30 — Close the last two P2 audit items (cadence freshness-SLA · HEP/REN symptom items) *(user)*
**Question.** Two P2 findings remained: F8 (no per-provider cadence/freshness SLA — the 'stale wearable' state had no
threshold) and F5 (HEP/REN self-report thin). How to close them?
**Decision.** **F8** — add a per-metric **Freshness SLA** beneath the existing cadence matrix (Appendix G §cadence):
*expected cadence · fresh-within · stale-after · drives*. Provider determines delivery (the matrix); the SLA is the
clinical staleness bound that fires the `states.html` Wearables→stale state and decays Confidence (Doc 12 §4); a
metric-level contract (not provider×metric) — cleaner and the clinically-meaningful unit. `cadence_sla` auto-closes F8.
**F5** — take the documented-optional path and actually add it: **6 symptom/risk self-report items** to
`data/qb-02-renal-hepatic.json` (3 REN: oedema/foamy-urine/fatigue, NSAID use, urination change; 3 HEP: RUQ/jaundice
symptoms, fatty-liver/hepatitis history, hepatotoxin exposure), each corroborated by the relevant lab; re-enriched the
bank. HEP/REN stay lab-led (D28 fusion) but now have symptom/risk context. `hep_ren_symptoms >= 6` auto-closes F5.
**Guardrails.** SLA windows + symptom-item weights are illustrative — calibrate before production; the new PROs add
context/early-warning, they don't override lab-led REN/HEP scoring. **Affects.** Appendix G (`_CADENCE_SLA` table +
`cadence_sla`/`hep_ren_symptoms` stats, F5/F8 auto-close v6); `data/qb-02-renal-hepatic.json` (+6 items) →
`question-bank.json` re-enriched (now 354 Q) → ripples to Appendices E (question bank + grid), G loop diagram,
questions-hub / eligibility-gating. **All 12 audit findings now Addressed.**

## D31 — Wiki navigation: progressive "book" arc *(user)*
**Question.** The sidebar had grown to 13 groups / 70 links, all expanded, with incoherent grouping (appendix
letters scattered across 4 groups; Question bank & Wearable baselines duplicated) and no learning order. How should
the whole menu subsystem be redesigned?
**Decision.** Reorganize into a **progressive "book" arc** a newcomer reads top-to-bottom — order follows how the
system *connects*, not doc numbers: **Start here → 1 How scoring works → 2 The inputs → 3 Making it personal →
4 Acting on it → 5 Trust & govern → 6 See the system → 7 Build it → 8 Reference (data tables) → 9 Doctor's board.**
Appendices A–L live in their topical chapter (not a single dump); duplicates removed; each chapter carries a
one-line blurb. The sidebar is now **collapsible** — groups collapsed by default, the active group auto-opens,
state persists in `localStorage`, with expand/collapse-all and search-opens-matches. Every page gets a **"Connects
to" footer** (Pages · Diagrams · Tables), auto-derived from the content link-graph + chapter siblings + automatic
appendix↔grid pairing + a curated topic→diagram map, so the wiki is fully navigable like a good book (linear
prev/next remains the straight-through read).
**Guardrails.** Single source of truth = `NAV`/`NAV_BLURB` in `build_wiki.py`; cross-links from `_GRID_OF`/
`_CONNECTS_EXTRA`; the build runs a `[connects]` injection and a `[consistency]` count guard each time.
**Affects.** `build_wiki.py` (`NAV`, `NAV_BLURB`, `sidebar()`, `_inject_connects()`), `assets/wiki.js` (collapse +
search), `assets/wiki.css` (`.navgrp`/`.grp-*`/`.connects`); all 70 generated pages. See the `[[wiki-nav-book-arc]]`
memory.

## D32 — Calculation Explorer (audit tree + map) on a JSON-canonical shared engine *(user)*
**Question.** The full PureScore calculation lived only inside the `purescore-uber-map.html` JavaScript (its own
`DEF`/`TH`/`PILL`/`CRIT`/`W` scorer), and other pages computed their own values. How should we visualise the *entire*
calculation across the wiki, let users drill from any output down to its leaf inputs, and stop the scoring math from
drifting between pages?
**Options.**
- A ★ **Computation/audit tree (primary) + flow map (secondary), on one shared JSON-canonical engine.** A new
  `data/calc-graph.json` (thin: topology, formulas, gates; references catalog ids for weights/constants) is resolved at
  build time into `assets/calc-data.js` and consumed by a single `assets/engine.js`; the page becomes a `[Tree | Map]`
  explorer with 5 selectable roots, editable leaf sliders (live recompute), and a right sliding drawer (formula,
  constants, gates, inputs, live value, source JSON, deep-links). A `build_wiki.py` guard fails the build on drift.
- B — Keep the spatial uber-map only, hand-fix the weight desync (no structural fix; islands remain).
- C — Author a standalone diagram without unifying the engine (prettier, but the "all pages, one JSON" goal unmet).
**Decision.** **A.** **Rationale.** Makes the calculation *auditable* (trace every value to its inputs) and *single-source*:
`data/*.json` is now canonical for the engine, Python and JS both read it, and the build aborts if any page reintroduces a
hardcoded scorer or the weights stop summing to 1. Going canonical also corrected three latent uber-map desyncs —
`CV` weight `0.14→0.13` (matches `pillar-weights.json`), reservoir cap `ρ 0.15→0.20` (`constants.json` `ρ_k`), and the
previously-missing `PURE_CRIT_CAP=40` cap when any pillar is critical. **Design notes.** (1) *Roots* — PureScore (full
depth: R_total → pillar → reservoir coupling + marker → 6-stage marker pipeline → raw input), Companion vector, Nudge
utility `U_a`, Adherence inflow, Actuarial premium. (2) *Editable leaves* — sliders perturb any marker; the whole tree
recomputes through the shared engine. (3) *file:// safe* — no runtime `fetch`; build inlines JSON into `calc-data.js`.
(4) *Feedback-loop demo* keeps its distinct Stage-2b temporal model (personal-baseline `κ·tanh`, day-advance drift) but
now sources its pillar weights / γ / δ from the same canonical JSON, so it can't drift either. **Guardrails.** All numbers
illustrative — re-verify before production (README §5.6); consumer/inferential channels still can't drive a red (D22);
`PURE_CRIT_CAP` makes any critical pillar cap the score at 40. **Affects.** NEW `data/calc-graph.json`,
`assets/engine.js`, `assets/calc-explorer.js`, generated `assets/calc-data.js`; `wiki_content.py`
(`resolve_calc_data()`/`write_calc_data()`, rebuilt `build_purescore_uber()`); `build_wiki.py` (`write_calc_data()` call +
`_engine_guard()` hard drift-check); `feedback-loop.body.html` (canonical-sourced constants). The Male/Female and
dataflow pages have no scorer island (catalog tables / static DFD). *Follow-up:* flip the remaining `wiki_content`
catalogs (`PILLARS`, `MODIFIERS`, …) to load from their extracted JSON so the catalog axis is canonical too.

## D33 — Cohort-matched cold-start: impute missing LAB biomarkers from cohort medians *(user)*
**Question.** A patient with no EHR/EMR (Patient360) lab data still needs a score. How do we score them in the
absence of measured labs without faking certainty or hiding danger?
**Options.**
- A ★ **Cohort-median imputation for LAB markers only, with honest accuracy/completeness signalling.** When a
  *lab* biomarker has no measured value, impute the matched-cohort median (a neutral prior) and mark
  `source = cohort_imputed`; wearables and PRO/questionnaire inputs are never imputed (used when present, absent
  otherwise). Cohort match key = **age-band × sex-at-birth × pregnancy/life-stage only** (general-population
  reference; no ethnicity/race, lifestyle, or disease/condition strata in the match). Imputed labs contribute at
  reduced confidence weight (`q_impute`), drop pillar **coverage**, **can never display clean-green**
  (`cov_green_floor`) and **can never fire the critical cascade or escalation** — an unmeasured *critical* lab
  instead raises a high-priority "measure this" action. Surfaced via a NEW companion dimension
  **Provenance / Source-grade** (measured-vs-imputed lab mix) plus lowered **Confidence** and **Representativeness**
  (`Rp_impute`); the headline reads **Provisional** while any scored lab is imputed. Imputed/provisional scores are
  **excluded from the actuarial layer** (Doc 10). A real lab arriving replaces the imputed value per-marker and
  confidence jumps.
- B — Impute *all* missing clinical inputs (labs + vitals + DEXA + wearables) from cohort medians. *Rejected:* over-
  reaches the "labs only" intent and silently fabricates wearable/PRO signals.
- C — Refuse to score until labs exist (INSUFFICIENT only). *Rejected:* abandons the cold-start patient; the engine
  already supports neutral-prior imputation (Doc 01 §4.3) honestly.
**Decision.** **A.** **Rationale.** Extends the existing per-marker median fallback (Doc 01 §4.3) into a formal,
auditable cold-start path that is *safe both ways* — never optimistic (coverage cap + no clean-green), never alarmist
(imputed labs cannot escalate) — and *honest* (the score carries its own provenance/uncertainty rather than masquerading
as measured). Keeping the match key to age × sex × life-stage avoids protected-class-proxy fairness risk (Doc 11 §6.1)
and yields large, stable cells (simpler validation, Doc 09). **Design notes.** (1) *Scope* — imputable = the 20
blood/urine labs only (`source:"lab"`, `imputable:true` in `calc-graph.json`); SBP/waist/body-fat/DEXA/OSA
(`clinical`), wearables (`wearable`) and PHQ-9/GAD-7/ISI (`pro`) keep today's behaviour. (2) *Constants* —
`q_impute=0.30` (imputed-lab confidence), `Rp_impute=0.70` (age×sex×stage match representativeness),
`cov_green_floor=0.60` (coverage below which a pillar can't show clean-green); all illustrative, tunable, versioned.
(3) *Vectors* — "accuracy" = Confidence + Representativeness (both lowered) + the new Provenance axis; "completeness" =
Data-sufficiency + Coverage (both drop). (4) *Lifecycle* — acute mode and active life-stage plans (Doc 06) suppress/
override imputation; the matched cohort must respect sex-at-birth and pregnancy/menopause stage (Doc 05); intersex →
individual-baseline, not cohort. **Guardrails.** All numbers illustrative — re-verify (README §5.6). Imputed labs:
never green-clean, never critical/escalating, never priced (Doc 10), always flagged Provisional with a "measure this"
nudge. **Affects.** `data/calc-graph.json` (marker `source`/`imputable`, impute pipeline stage, Provenance companion
node, cold-start demo profiles), `data/constants.json` (`q_impute`/`Rp_impute`/`cov_green_floor`), `wiki_content.py`
(`resolve_calc_data()` + the affected mermaids 01/09/12/14 + `build_purescore_dataflow`), `assets/engine.js`
(measured-vs-imputed scoring, Provenance/coverage, imputed-critical block), `assets/calc-explorer.js` (drawer +
pipeline label); Docs 01/03/12 (engine truth) and 05/06/09/10/11/14/16/17/18 + eligibility-gating + admin
(propagation). Staged: Stage 1 = data SoT + this entry; Stage 2 = engine + diagrams; Stage 3 = doc/admin propagation.

## D34 — Full decision-tree explorer: missing/incomplete data, every branch shown, filter-search *(user)*
**Question.** The D32 Calculation Explorer only expanded the path actually taken for the selected profile, hid the
missing-data logic (every marker had a default), and had no way to jump to a step. How should it show the *complete*
calculation — including branches that don't apply — and let users probe incomplete data?
**Options.**
- A ★ **Explicit decision tree + interactive data-state + filter-search.** (1) Every conditional renders as a ◇ gate
  node with **all** branches; the branch active for the current profile is highlighted, the not-taken branches are
  shown dimmed with their condition (so the full path is visible *regardless of applicability*). (2) Applicability
  gates (sex · pregnancy · age-band · life-stage) sit atop **all** roots; per-marker pipeline conditionals
  (confidence/fallback · cohort blend · personalization on/off · critical cascade) sit under each marker. (3) Each leaf
  marker gets a **data-state control** (present → stale → missing) and there are **stream presets** (labs · wearable ·
  PRO · clinical) that flip a whole channel; the engine recomputes confidence, fires the D33 fallback branch, and shows
  **coverage + Confidence** live. (4) A **filter-search** box narrows the tree to matching nodes + their ancestors.
- B — Annotate a single linear path with the conditions inline (no separate branch nodes). *Rejected:* not a real
  decision tree; doesn't show not-taken paths.
- C — Static missing-data branch only (no interactivity). *Rejected:* can't explore how gaps move the score.
**Decision.** **A.** **Rationale.** Directly answers "show the full calculation formula and full decision tree path
regardless of applicability" and makes the D33 missing-data model *visible and testable* — you can knock out a channel
and watch coverage/Confidence fall and the imputation/drop branches light up, exactly the honest cold-start behaviour
D33 specifies (imputable→cohort median at `q_impute`, non-imputable→drop + Confidence↓, only fresh data hard-fires the
critical cascade). Stays on the D32 JSON-canonical spine: branches, gates and engine params are **data** in
`calc-graph.json`, evaluated by the shared engine. **Design notes.** (1) *Data* — new `gates`, per-stage `branches`,
profile `sex`/`age`/`lifestage`, and `engine_params` (`conf_floor`, `tau_days`, `stale_dt_days`, `q_source` by channel)
in `calc-graph.json`; `q_impute`/`Rp_impute`/`cov_green_floor` resolved generically from `constants.json`. (2) *Engine* —
`markerEval()` returns the active conf/blend/pers/crit branch + effective r, weight and confidence; `gateState()` maps
profile attrs to active gate branches; pillar/overall **coverage** and **Confidence** are computed and `cov_green_floor`
flags low-coverage. (3) *UI* — Tree-only; the Map already shows the whole graph. **Guardrails.** Illustrative — re-verify
(README §5.6); `Rp_impute` is used as the imputed-marker down-weight (D33 frames it as representativeness — reconcile
before production); cohort median is illustrated by the marker default (real build uses age×sex×life-stage tables, D33).
**Affects.** `data/calc-graph.json` (gates, branches, profile attrs, engine_params), `assets/engine.js` (missing-data +
branch + gate evaluation), `assets/calc-explorer.js` (◇ gate/branch nodes, data-state controls, stream presets,
filter-search, coverage readout), `wiki_content.py` `build_purescore_uber()` (search/stream/coverage markup + CSS).
Builds on **D32**, visualises **D33**.

### Maintenance notes
- New decisions append as `D24+`. When a decision changes, mark the old one `SUPERSEDED → Dn` and
  add the replacement; never edit history in place.
- Each entry must name the artifacts it **Affects** so downstream code/docs stay traceable.
