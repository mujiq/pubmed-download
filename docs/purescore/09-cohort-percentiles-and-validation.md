# 09 — Cohort Percentiles, Calibration & Validation

> Binding conventions: `README.md §3`. This document is authoritative for **how cohorts `c(p)` are
> constructed**, how the percentile CDFs `F_{i,c}` are estimated and shrunk (the empirical-Bayes
> layer Doc 01 §2/§4 defers here), how the free parameters of the scoring and reservoir engines
> (`φ`, `W_k`, `λ_j`, `κ_{jl}`, `u_{kj}`, `ρ_k`) are **calibrated, regularized, and
> stability-checked**, and how the whole system is **validated, fairness-audited, drift-monitored,
> versioned, and made reproducible**.
>
> This is the **anti-Babylon backbone** (Doc 00 §3.1): *no claim ships ahead of its evidence*.
> Everything here is a **gate**, not a report. Positioning stays wellness-grade,
> clinician-in-the-loop (README §1); nothing here relaxes the hard non-negotiables (README §5).
>
> **All thresholds, gate values, and sample-size floors below are illustrative defaults; they are
> versioned config (Doc 03 §8) and must be set per population and re-verified on the fixed cadence
> (README §5.6) before any production use.**

---

## 0. What this document guarantees

1. A patient always maps to a **well-defined cohort** with a calibrated percentile, even when their
   own data is sparse — via hierarchical partial pooling, never via a silently tiny cell.
2. Every displayed percentile and every shrunken estimate has a **stated uncertainty**.
3. No parameter (`λ, κ, u, W, φ, ρ`) reaches production without passing **calibration, discrimination,
   reliability, stability, prospective, and fairness** gates, all recorded against a model version.
4. Cohorting **reduces, not encodes, disparity** (Doc 00 principle 8); the fairness audit (§5) has
   veto power over shipping and constrains the actuarial layer (Doc 10) and governance (Doc 11).

---

## 1. Cohort construction (strata, minimum cells, partial pooling)

### 1.1 The stratum tree

A cohort `c(p) = (age_band, sex, D, Mx)` (README §3.1) is a leaf of a **stratification tree**. The
tree is built coarse→fine so that every leaf has a chain of ancestors to borrow strength from:

```
 level 0  ALL                                   (grand mean / global prior)
 level 1  sex                                   (sex_at_birth — drives physiology, Doc 01 §1.1)
 level 2  sex × age_band                        (age bands: 18–29, 30–39, …, 80+; marker-specific)
 level 3  sex × age_band × D_primary            (primary disease flag: T2D, CKD, ASCVD, MASLD, …)
 level 4  sex × age_band × D_primary × Mx_class (medication class: statin, GLP-1/insulin, RAASi, …)
 level 5  full D-set × full Mx-set              (the exact (age,sex,D,Mx) leaf)
```

- **Age banding** is marker-specific: renal and bone markers need finer age resolution than, say,
  vitamin D. Bands are stored per `MarkerDef`, not globally.
- **Sex** is `sex` (Male/Female) for physiology (Doc 01 §1.1); rare intersex/DSD handling follows
  Doc 05 via organ inventory.
- **Disease flags `D`** and **medication classes `Mx`** are sets; the leaf is their full
  conjunction. Most patients carry 0–3 flags, so leaves are sparse — hence pooling (§1.3).

### 1.2 Minimum cell size and the credibility floor

For a leaf to be used **directly** (no pooling) for marker `i` it must satisfy an effective-sample
floor:

```
 n_{i,c}^eff  ≥  n_min        (default n_min = 200 for a percentile;
                               ≥ 30 ⇒ usable only after shrinkage;
                               < 30 ⇒ leaf is never displayed standalone)
```

`n^eff` is the *effective* count after recency/quality weighting (a leaf full of stale or
literature-fallback rows has a smaller `n^eff` than its raw count). Tail percentiles (p1, p99) need
more support than the median; the floor scales with the tail depth, `n_min(q) ≈ n_min / [q(1−q)]`,
so a stable p95 demands more data than a stable p50. Cells failing the floor **do not vanish** —
they are estimated by pooling toward their parent (§1.3).

### 1.3 Hierarchical / partial pooling (small cells borrow strength)

Treat each leaf parameter (e.g., the marker's cohort mean `μ_{i,c}` on a normalizing scale) as drawn
from its parent distribution. A two-level Normal random-effects model makes the mechanics explicit:

```
 x_{i,c}      ~  Normal(μ_{i,c}, σ_i²)            within-leaf
 μ_{i,c}      ~  Normal(μ_{i,parent(c)}, τ_i²)    leaf around its parent (partial pooling)
```

The pooled (James–Stein / empirical-Bayes) estimate of a leaf mean is the precision-weighted blend
of its own sample mean `x̄_{i,c}` and its parent estimate `μ̂_{i,parent}`:

```
 μ̂_{i,c}  =  B_{i,c} · x̄_{i,c}  +  (1 − B_{i,c}) · μ̂_{i,parent(c)}

                    n_{i,c}^eff / σ_i²
 B_{i,c}  =  ───────────────────────────────        (the credibility weight, Bühlmann form)
              n_{i,c}^eff / σ_i²  +  1 / τ_i²
```

- A **data-rich** leaf (`n^eff` large) ⇒ `B → 1` ⇒ uses its own distribution.
- A **sparse** leaf (`n^eff` small) ⇒ `B → 0` ⇒ falls back to the parent, recursively up the tree to
  level 0. Nothing is ever estimated from an unsupported handful of points.
- `τ_i²` (between-leaf variance) and `σ_i²` are themselves estimated by REML / the method of
  moments across the tree; large `τ_i` (leaves genuinely differ, e.g. eGFR across CKD stages) keeps
  more local detail, small `τ_i` pools harder.
- This is the same **Bühlmann-credibility** machinery the actuarial layer reuses (Doc 10) and is the
  population-data realization of the "empirical-Bayes mention" in Doc 01 §4.

For the full **percentile curve** (not just the mean) we pool quantiles with the same `B` weight, or
equivalently fit a parametric/semi-parametric quantile model (e.g., **GAMLSS / LMS** with
age-smoothed L, M, S parameters per sex×disease branch) whose random effects are pooled toward the
parent — this yields smooth, monotone, age-continuous percentile curves without ragged cell
boundaries.

### 1.4 Mapping a patient to one (and to many overlapping) cohorts

A patient rarely belongs to a single tidy leaf:

- **Primary cohort** `c(p)`: the most specific leaf whose pooled estimate passes the credibility
  floor — the one used for the binding `q_i = F_{i,c}(x_i)` in the score (Doc 03 §2).
- **Overlapping cohorts:** a 62-y-old woman with T2D *and* CKD *on* a statin is simultaneously a
  member of the `T2D`, the `CKD`, and the `statin` strata. For **display and analytics** PureScore
  can stack-rank her within each ("vs other T2D patients", "vs CKD stage-matched peers"); for the
  **binding score** it uses the single most-specific credible leaf to avoid double counting.
- **Conflict rule:** where overlapping cohorts disagree, the **safety-dominant** rule (README §5.2)
  still holds — the clinical anchor `r_i^clin` is cohort-independent and always wins; the percentile
  blend `φ·r_i^cohort` can only *raise* concern (Doc 01 §4.4, Doc 03 §2). A patient cannot be made to
  look healthy by selecting a sicker comparison cohort.
- **Cohort membership is logged** with the score (§6) so any percentile claim is reproducible and
  auditable.

---

## 2. Percentile engine and empirical-Bayes shrinkage of the patient's own data

There are **two distinct shrinkage problems**, and the document keeps them separate:

- **§1.3** shrinks *sparse cohort cells* toward parent cohorts (population estimation).
- **§2.2** shrinks a *single patient's sparse own measurements* toward their cohort prior
  (per-patient estimation). Both are empirical Bayes; they compose.

### 2.1 Estimating `F_{i,c}` from population data

`F_{i,c}` (the cohort CDF used for `q_i`) is built from the population datasets named in Doc 01 §4.2
(NHANES, UK-Biobank-class, disease registries), stratified per the tree of §1.1, pooled per §1.3,
and stored as a `ReferenceDist` (Doc 01 §1.1) carrying `source`, `sample_n`, `vintage`. Each curve
ships with a **coverage flag**: which leaf level actually supplied it (own leaf vs pooled ancestor),
so a percentile derived mostly from a grandparent stratum is labelled as such — the percentile
analogue of *low-coverage green* (Doc 01 §4.3, Doc 03 §3).

Disease-specific registries are used where the general-population percentile would mislead (a CKD
patient ranked against the general population would look catastrophic on eGFR by construction); the
clinical anchor still dominates regardless of which `F` is chosen.

### 2.2 Empirical-Bayes shrinkage of a patient's sparse own data (the formula)

A patient with few of their own measurements of marker `i` should not have a percentile estimated
from noise. Treat the patient's latent "true" value `θ_{p,i}` as drawn from the cohort prior and
shrink the patient's noisy observed mean toward the cohort median:

```
 Prior (cohort):     θ_{p,i}        ~  Normal( μ_{i,c},  τ_{i,c}² )       # μ_{i,c}=F_{i,c}^{-1}(0.5)
 Likelihood (own):   x̄_{p,i} | θ    ~  Normal( θ_{p,i},  σ_{i}² / n_{p,i} )

 Posterior mean (the value we percentile-rank):

     θ̂_{p,i}  =  ω_{p,i} · x̄_{p,i}  +  (1 − ω_{p,i}) · μ_{i,c}

                      n_{p,i} / σ_i²
     ω_{p,i}  =  ───────────────────────────          # the patient-level credibility weight
                   n_{p,i} / σ_i²  +  1 / τ_{i,c}²
```

- **One noisy reading** (`n_{p,i}=1`) ⇒ `ω` small ⇒ percentile sits near the cohort median (we do not
  over-react to a single spot value). **Repeated concordant readings** ⇒ `ω → 1` ⇒ percentile reflects
  the patient's own data. This is the statistical counterpart of the reservoir layer's refusal to let
  one good (or bad) day dominate (Doc 04 §4).
- This shrinkage applies to the **percentile/personalization** path only. It is **clipped out of the
  safety path**: the clinical anchor `r_i^clin` (Doc 03 §1) is computed on the *raw* observed value,
  never on the shrunken value, so shrinkage can never soften a red. (Per README §5.4, uncertainty
  defaults to caution: when shrinkage and raw disagree on the dangerous side, the raw value governs
  escalation.)
- `confidence_i` (Doc 01 §2) feeds `n_{p,i}` as an effective count (a stale or wearable-derived value
  counts as a fraction of a clean lab), unifying recency decay with shrinkage.

### 2.3 Producing display and actuarial stack-rank percentiles

- **Display percentile** = `F_{i,c}(θ̂_{p,i})` (or the pillar-level percentile from aggregated pillar
  risk), rendered with a **credible interval** so "you're at the 70th percentile" carries its
  uncertainty; a wide interval (sparse data / pooled-from-ancestor cohort) is shown as a band, not a
  point.
- **Pillar / PureScore percentiles** rank `S_k` and PureScore within the cohort using the same
  pooled `F`; these are **presentation/analytics, not a relaxation of `r_i`** (Doc 03 §2).
- **Actuarial layer (Doc 10)** consumes the *same* pooled, shrunken percentiles and their intervals
  — never a separate, looser estimate — and inherits the cohort credibility weights as Bühlmann
  credibility. The fairness constraints of §5 bind that consumption (Doc 10, Doc 11).

---

## 3. Parameter calibration and the stability gate

The scoring formula (Doc 03) and reservoir dynamics (Doc 04) expose free parameters:
`φ` (cohort blend), `W_k` (pillar weights), `ρ_k` (reservoir cap), and the reservoir physics
`λ_j` (decay), `κ_{jl}` (interference), `u_{kj}` (reservoir→pillar links). All are **versioned
config** (Doc 03 §8, Doc 04 §7) and are estimated against **longitudinal outcomes**, then
regularized, then stability-checked, before any version bump.

### 3.1 Calibration targets (what the parameters are fit to)

Parameters are fit so that PureScore and its parts **predict prospectively observed outcomes**:
all-cause and cause-specific mortality, incident MACE, incident T2D/CKD progression, hospitalization,
and validated intermediate endpoints. The loss couples the dynamic system to outcomes:

```
 min_{Θ}   Σ_{p,t}  ℓ( outcome_{p,t+h} ,  hazard( PureScore_Θ(p,t), pillars, reservoirs ) )
                     +  R_λ(Λ)  +  R_κ(K)  +  R_W(W)            # regularization
           s.t.      −Λ + K   is Hurwitz         (the stability gate, §3.3)
                     sign(κ_{jl}) = sign(prior)   (physiology-locked signs, Doc 04 §3)
```

- `Θ = {φ, W, ρ, λ, κ, u}`. `ℓ` is a survival/partial-likelihood loss (Cox / discrete-time hazard)
  so the score is fit to *time-to-event*, not a static label.
- Reservoir trajectories `B_j(t)` are rolled forward through the state equation (Doc 04 §3) under the
  candidate `Θ` and enter the hazard via `B̃_k` (Doc 04 §5) — i.e. the calibration sees the **memory**,
  not just the snapshot.

### 3.2 Regularization (priors keep the fit physiological and identifiable)

- **`κ` (interference):** L1/elastic-net for **sparsity** (the matrix is sparse by design, Doc 04 §3),
  with **sign constraints locked to the physiological prior** — calibration may change magnitudes,
  never flip a coupling's sign (e.g. `κ_{INFL,SLD}` stays `> 0`). Magnitudes are shrunk toward the
  literature-initialized values (ridge-to-prior).
- **`λ` (decay):** bounded to physiological time-scales per reservoir (Doc 04 §2): atherogenic/CAC
  near-zero (near-irreversible), sleep-debt/inflammation large (fast healing). Calibration tunes
  within the band; it cannot make CAC "heal" or sleep debt permanent.
- **`W_k`, `φ`, `ρ_k`:** ridge toward the Doc 03 defaults (`φ=0.6`, `ρ_k≈0.2`, `W_k^base`); bounded so
  no single pillar/cohort multiplier can dominate; `φ < 1` is a hard cap (cohort can never override the
  clinical anchor — Doc 01 §4.4).
- All fits use **nested cross-validation** (inner = hyperparameters, outer = honest performance) with
  patient-grouped, **temporally-ordered** folds (train past → test future) to prevent leakage.

### 3.3 The stability / Hurwitz gate (promised by Doc 04 §3 and §5)

The reservoir system must **converge, not diverge**. Linearize the state equation (Doc 04 §3) about a
set-point; the Jacobian of the continuous-time analogue is

```
 J  =  −Λ + K          Λ = diag(λ_1, …, λ_J) ⪰ 0  (decay)
                       K = [κ_{jl}]            (interference, zero diagonal)
```

**Requirement (binding):** `J` must be **Hurwitz** — *every eigenvalue of `J` has strictly negative
real part* — equivalently the discrete update `I + Δ·J` is a **contraction** (spectral radius
`< 1`). This guarantees burdens relax toward set-points rather than blowing up, and that cross-pillar
contagion (Doc 04 §4.3) is damped, not explosive.

**How it is verified, at every calibration:**
1. Compute `eig(−Λ + K)`; require `max Re(eig) ≤ −ε` for a margin `ε > 0` (not merely `< 0`).
2. **Sufficient quick check:** require `−Λ + K` to be **diagonally dominant by decay** —
   `λ_j ≥ (1+ε)·Σ_{l≠j} |κ_{jl}|` for every `j` (Gershgorin: each disc sits left of the imaginary
   axis). This is a convex constraint, cheap to enforce in the optimizer.
3. Verify the **discrete** map actually used (`Δ` from Doc 04 §7) is contractive: `ρ(I + Δ·J) < 1`.
4. Confirm a **Lyapunov certificate**: solve `Jᵀ P + P J = −Q` for some `P ≻ 0`, `Q ≻ 0`; success
   proves global stability of the linearized system.

**How it is enforced:** the constraint is **part of the optimization** (§3.1) — projected gradient /
the diagonal-dominance inequality keeps every candidate `Θ` inside the stable set; any `Θ` that fails
is rejected and **cannot be promoted to a model version** (Doc 04 §7, Doc 11). A failed stability check
is a hard release blocker, logged with the offending eigenpair for audit.

---

## 4. Validation (the anti-Babylon backbone)

No capability is claimed beyond what is **prospectively validated for the population in front of us**
(Doc 00 §2.7, §3.1). Validation is organized as **gates**; each must pass for the target population
*and every audited subgroup* (§5) before the corresponding claim ships.

### 4.1 Calibration (do predicted risks match observed outcomes?)

- **Calibration plot / reliability diagram** of predicted risk vs observed event rate by decile, plus
  **calibration-in-the-large** (mean predicted = mean observed) and **calibration slope** (target ≈ 1).
- **Brier score** `BS = (1/N) Σ (p̂ − y)²` and its **scaled** form `1 − BS/BS_ref`; Spiegelhalter's
  Z-test for calibration.
- **Expected/observed (E/O) ratio** and **ECE** (expected calibration error) overall and per subgroup.
- Mis-calibration is corrected by **recalibration** (Platt / isotonic / re-estimated baseline hazard),
  versioned like any parameter change — never by silently moving thresholds.

### 4.2 Discrimination (does the score separate who has events from who doesn't?)

- **C-statistic / AUROC** against hard outcomes and mortality; **Harrell's / Uno's C** for the
  time-to-event (survival) formulation; **time-dependent AUC**.
- **AUPRC** for rare outcomes (where AUROC flatters), and **Net Reclassification Improvement / IDI**
  versus the incumbent clinical scores PureScore claims to add to (FINDRISC, ASCVD/SCORE2, etc.,
  Doc 08) — PureScore must *add* discrimination, not merely re-package them.
- Reported with **bootstrap confidence intervals**; a point estimate without an interval is not a
  pass.

### 4.3 Test–retest reliability (is the score stable under noise?)

- **ICC** and within-subject **CV** across short-interval repeat measurements where the true state is
  unchanged; **Bland–Altman** for measurement agreement.
- The **anti-flap** hysteresis (Doc 03 §6) and reservoir smoothing (Doc 04) are validated here: the
  headline must not oscillate on assay/wearable noise, **yet must still cross promptly on a true
  acute change** (§4.4). Reliability and responsiveness are reported together — a score that is stable
  only because it is unresponsive fails.

### 4.4 Critical-cascade sensitivity/specificity (minimize false reassurance — the core safety claim)

The critical cascade (Doc 03 §4–5, README §5.1/§5.4) exists to **never tell a sick person they are
fine**. It is validated as a **screening test for true emergencies**:

- **Sensitivity (recall) for true emergencies is the dominant metric** — a missed emergency (false
  reassurance) is the Babylon failure mode (Doc 00 §3.1) and is weighted far above a false alarm. The
  operating point is chosen at a **high-sensitivity** target (e.g. ≥ 0.99 for `escalation=emergency`
  markers), accepting lower specificity by design.
- Report **sensitivity, specificity, PPV, NPV** for the `escalation ∈ {emergency, urgent, routine}`
  tiers (Doc 03 §4.1) separately; track the **false-reassurance rate** (emergency present, score
  non-critical) as a **named, monitored, near-zero-target metric** with mandatory root-cause review of
  every instance.
- Decision-curve / **net-benefit** analysis confirms the chosen operating point is clinically
  worthwhile across plausible cost ratios. Specificity is improved only by methods that **do not lower
  sensitivity** (uncertainty still defaults to caution, README §5.4).

### 4.5 Prospective validation as a release gate

- **Retrospective/internal performance never authorizes scaling.** A claim is promoted only after
  **prospective** validation in the deployment population (pre-registered protocol, pre-specified
  endpoints and analysis), echoing Doc 00 §3.1 *"validate before scale"*.
- **External & temporal validation:** performance is re-demonstrated on an **external site** and on a
  **later time window** than training (temporal split), because guideline drift, assay changes, and
  population shift erode transported models.
- **Out-of-distribution (OOD) detection at inference:** each patient's feature vector is scored for
  cohort membership / density (Mahalanobis distance to the cohort, conformal nonconformity, or an
  isolation-forest novelty score). A patient flagged OOD gets **widened intervals, a low-confidence
  banner, and biased-to-caution handling**, and the case is routed for human review rather than given
  an overconfident number (README §5.4).
- **Conformal prediction** supplies finite-sample coverage guarantees on the percentile/risk
  intervals, including the OOD-robust (Mondrian, per-cohort) variant so coverage holds *within each
  subgroup*, not just on average.

### 4.6 Release-gate summary

```
 SHIP a claim only if, for the target population AND every audited subgroup (§5):
   calibration:        slope ∈ [0.9,1.1], |E/O−1| ≤ tol, ECE ≤ tol
   discrimination:     C ≥ target, CI lower bound ≥ incumbent
   reliability:        ICC ≥ target, no flap on noise, responsive to true change
   critical cascade:   sensitivity(emergency) ≥ 0.99, false-reassurance rate ≈ 0
   stability:          −Λ+K Hurwitz with margin ε (§3.3)
   prospective:        pre-registered prospective study PASSED in deployment population
   fairness:           §5 audit PASSED (no subgroup parity violation, no proxy)
 Any failure ⇒ NO-SHIP. (Doc 00 §3.1; Doc 11 governance sign-off.)
```

### 4.7 Drift monitoring and recalibration cadence

Once live, the model is monitored continuously for three drift types, then recalibrated on cadence:

- **Data / covariate drift:** input distributions shift (new assay, new wearable firmware,
  population intake change). Detected via PSI, KL divergence, KS tests on feature distributions vs the
  training reference; large drift triggers OOD handling (§4.5) for affected markers.
- **Population drift:** the cohort mix changes (the `c(p)` membership distribution moves); monitored on
  the cohort registry so percentiles don't silently misrepresent a shifted population.
- **Label / concept drift:** the marker→outcome relationship changes (new therapy alters prognosis,
  guideline revision); detected by **rolling calibration** (slope, E/O, Brier over a moving window) and
  monitored AUC. A drop is a **recalibration trigger**.
- **Cadence & versioning:** scheduled recalibration on a fixed cadence (README §5.6) **plus**
  drift-triggered off-cadence recalibration. Every recalibration is a **model-version bump** requiring
  re-passing §4.6 (including fairness §5) and is recorded in the audit trail (§6, Doc 11). Reference
  ranges/guidelines are re-verified on the same discipline (README §5.6, Doc 01).

---

## 5. Fairness & equity audit (veto power over shipping)

**Binding principle (Doc 00 §8):** *cohorting must reduce, not encode, disparity; no protected-class
attribute or proxy may worsen access, price, or care* (README §5.5). The fairness audit is a **gate
with veto power** (§4.6), not a post-hoc check, and it **constrains the actuarial layer (Doc 10) and
governance (Doc 11)**.

### 5.1 Protected dimensions audited

Sex, age, race/ethnicity, and **socioeconomic status (SES)** at minimum, plus their intersections
(e.g. older × low-SES × minority), since single-axis parity can hide intersectional harm.

### 5.2 Subgroup calibration & error-rate parity (metrics)

Every §4 metric is recomputed **per subgroup and per intersection**, and must meet parity tolerances:

- **Calibration parity:** calibration slope, E/O, and ECE must hold **within each subgroup** — a model
  well-calibrated on average but mis-calibrated for a subgroup is **not shippable** (this is the
  classic equity failure).
- **Error-rate parity:** compare across subgroups —
  - **equalized odds:** TPR and FPR of the critical cascade (esp. **false-reassurance rate**, §4.4),
  - **predictive parity:** PPV/NPV,
  - **balance** for positive/negative class (mean predicted risk among true positives/negatives).
- Report disparities as ratios/differences with CIs; flag any exceeding the tolerance band. Because
  some criteria are mathematically incompatible at unequal base rates, the **chosen criteria are
  pre-registered and justified**, with safety (false-reassurance parity) prioritized.

### 5.3 Detecting and removing protected-class proxies

- **Proxy detection:** test whether a protected attribute is **reconstructable** from the feature set
  (train an adversary/classifier to predict the attribute from inputs; high accuracy ⇒ proxy present).
  Inspect feature→protected mutual information and the score's residual correlation with the protected
  attribute after conditioning on legitimate clinical need.
- **Legitimate vs illegitimate use:** `sex_at_birth` is a **legitimate physiological** input
  (reference ranges, hormones — Doc 01 §1.1, Doc 05) and is *kept*. Variables that act as **proxies for
  race/SES with no causal clinical justification** (e.g. ZIP code, certain utilization patterns) are
  **removed or neutralized**; race is **not** used as a biological correction unless a specific,
  defensible, guideline-endorsed and re-verified justification exists (the field has retired several
  race-based corrections, e.g. eGFR — Doc 01/Doc 08 must track this).
- **Mitigation:** reweighting, constrained optimization with fairness constraints in the loss (§3.1),
  and **subgroup recalibration**; mitigations are themselves re-validated (§4) so fixing one group
  does not silently harm another.

### 5.4 The cohorting must-reduce-disparity rule

Partial pooling (§1.3) is itself an **equity tool**: small/underrepresented subgroups borrow strength
from parents rather than being scored from noise or denied a score — provided pooling does not erase a
**clinically real** subgroup difference. The audit checks both failure modes: (a) **over-pooling** that
flattens a genuine subgroup signal, and (b) **under-pooling** that scores a thin subgroup from noise.
A cohort scheme that **widens** an outcome disparity (worse calibration, worse false-reassurance for a
protected group) is rejected — that is the literal meaning of *"reduce, not encode"* (Doc 00 §8).

### 5.5 Constraint on downstream layers

- **Actuarial layer (Doc 10):** consumes only audited, fairness-passed percentiles; **no
  protected-class proxy may enter pricing**, and the portfolio posture (Doc 00 §4, Doc 10) is bound by
  GINA/anti-discrimination law. The fairness audit's veto extends to any actuarial use.
- **Governance (Doc 11):** owns the sign-off; a failed fairness gate is escalated and blocks release;
  the audit, its tolerances, and any waiver are recorded and externally reviewable (Doc 00 §3.1
  transparency).

---

## 6. Reproducibility & audit

Every score must be **reconstructable byte-for-byte for audit** (README §5.3 explainability;
Doc 03 §6 determinism). The lineage chain:

```
 score_record = {
   patient inputs (values, source, confidence, effectiveTime),   # Doc 01
   cohort_id + pooling path (which leaf/ancestors supplied F),    # §1
   reference_dist versions (source, sample_n, vintage),           # Doc 01 §4.2
   parameter_version Θ = {φ, W, ρ, λ, κ, u} + config hash,        # Doc 03 §8, Doc 04 §7
   model_version + code commit,                                   # §3, §4.6
   computed: r_i, R_k, B_j, B̃_k, PureScore, status, binding constraint,   # Doc 03 §7
   validation_manifest (which §4/§5 gates this version passed, when),
   uncertainty (credible/conformal intervals), OOD flag           # §2.3, §4.5
 }
```

- **Versioning:** datasets (`ReferenceDist` vintages), parameters (`Θ`), code, and trained models are
  each content-addressed/version-pinned; a score names the exact versions used. Any change to a
  constant (Doc 03 §8) or reservoir parameter (Doc 04 §7) is a **version bump** that **re-triggers the
  §4.6 gate including fairness (§5)** before promotion.
- **Lineage:** the chain above ties a displayed number to its inputs, cohort, reference distributions,
  parameters, and the validation evidence current at compute time — so an auditor can both *reproduce*
  the number and *check it was authorized* by passing evidence (no claim ahead of its evidence,
  Doc 00 §3.1).
- **Reproducibility:** because the pipeline is deterministic given `{inputs, Θ, reference dists}`
  (Doc 03 §6), replaying a `score_record` reproduces the exact score, percentile, intervals, and
  binding constraint — the substrate for clinician review (Doc 08), governance audit (Doc 11), and any
  external/regulatory examination.

---

## 7. Cross-references

- Cohort definition & safety dominance: README §3.1, §5.2, §5.4 — Doc 01 §4.4.
- Percentile sources & median fallback: Doc 01 §4.2–§4.3; confidence/recency: Doc 01 §2.
- Scoring parameters fed/constrained here: Doc 03 §2 (`φ`), §3 (`ρ_k`), §5 (`W_k`), §8 (defaults).
- Reservoir parameters & the stability promise redeemed in §3.3: Doc 04 §3, §5, §7.
- Anti-Babylon principles, equity principle 8, "validate before scale": Doc 00 §2–§4.
- Downstream constraints: actuarial layer Doc 10; safety/governance/privacy Doc 11.
