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

---

### Maintenance notes
- New decisions append as `D15+`. When a decision changes, mark the old one `SUPERSEDED → Dn` and
  add the replacement; never edit history in place.
- Each entry must name the artifacts it **Affects** so downstream code/docs stay traceable.
