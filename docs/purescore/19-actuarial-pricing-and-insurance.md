# 19 — Actuarial Pricing and Insurance (with heavy regulatory & fairness flags)

> Read `README.md` first for binding conventions (symbols `R_k`, `PureScore`, cohort `c(p)`,
> non-negotiables esp. §5.5 *no protected-class proxy may worsen price/access/care*). This
> document depends on Doc 01 §4 (the rejection of *"impossible to lose money"*), Doc 03 (pillar
> risks `R_k`, stack-rank percentiles, PureScore), and Doc 13 (calibration, fairness audit,
> credibility). Doc 13 is the *validation gate* for everything priced here. Doc 16 owns
> governance, filings, and appeals.
>
> **Reading-level warning.** This is the most legally dangerous document in the spec. Every
> pricing mechanism below is paired with a *bright line* it must not cross. Where a use is
> prohibited in a jurisdiction, the math in §2–§3 **does not apply** — it is fenced off in §1
> and §6. Treat the matrix in §1.3 as binding, not advisory. None of this is legal advice;
> every deployment requires licensed actuarial sign-off (ASA/FSA/FIA or local equivalent),
> counsel review, and DOI/regulator filing where applicable.

---

## 1. Intended uses, jurisdiction, and the permitted/prohibited line

### 1.1 What this layer is and is not
PureScore is a **wellness-grade** score (README, Doc 01). The actuarial layer is a *derived,
gated* product that maps health-state signals to expected cost **for permitted population-level
purposes only**. It is **not** a green light to risk-rate sick individuals out of coverage. The
default posture is: PureScore informs **prevention economics and group/portfolio management**;
it is structurally walled off from any use that reduces a sick person's access or affordability.

The engine produces three artefacts, with escalating governance:
1. **Population cost model** `Ĉ(cohort)` — actuarially-sound expected-cost estimates for *groups*.
2. **Risk-adjustment signal** — concurrent/prospective acuity for value-based and risk-adjusted
   payment (HCC-style), used to *move money toward sicker panels*, never away from sick people.
3. **Improvement-economics model** — expected cost *reduction* from PureScore improvement, the
   basis of shared-savings and pay-for-improvement contracts (the Kaiser model, §5).

### 1.2 Jurisdiction awareness (high level — verify per deployment)
- **US — individual & small-group health (ACA).** Community rating: premiums may vary **only**
  by the ACA-permitted factors (age within 3:1, geographic rating area, tobacco within 1.5:1,
  family size, plan tier). Health status, claims history, and any **health score** are
  **prohibited** rating factors. **PureScore must not enter individual or ACA small-group
  premium derivation.** Guaranteed issue + no pre-existing-condition exclusions are hard limits.
- **US — GINA.** Genetic information (incl. family history and many genetic biomarkers) may not
  be used in health-insurance underwriting or pricing. Any pillar input derived from genetics
  (e.g., Lp(a) as a partly-heritable marker, polygenic data) is **fenced** from underwriting use.
- **US — ADA / Section 1557.** Disability-status proxies and discrimination on the basis of
  disability are prohibited; wellness-program incentives are tightly bounded (voluntariness,
  reasonable-alternative-standard, incentive caps under HIPAA/ACA wellness rules — historically
  ~30%, ~50% tobacco; **re-verify current EEOC/HHS limits before any incentive ships**).
- **US — large-group / self-insured (ERISA).** More latitude for group rating and stop-loss, but
  Section 1557, HIPAA non-discrimination, and ADA wellness limits still bind. State DOI filings
  govern insured products.
- **US — state DOI.** Rate filings must be actuarially justified, non-discriminatory, and (in
  many states) demonstrate no unfair discrimination or proxy redlining. **File before you price.**
- **EU — Solvency II / IDD.** Capital adequacy (SCR/MCR), technical-provisions discipline, and
  product-oversight/governance (POG) under IDD; GDPR Art. 9 special-category (health) data
  constraints and automated-decision rights (Art. 22). Gender as a rating factor is prohibited
  for insurance pricing (Test-Achats); **so the sex-specific PureScore models (Doc 08) must not
  flow into EU premium differentiation by sex.**
- **Others (high level).** UK (FCA/PRA, Equality Act), Canada (provincial; genetic
  non-discrimination GNDA), Australia (life insurers' moratorium on genetic results below a
  threshold) — each adds constraints; none *loosens* the bright lines below.

### 1.3 PERMITTED vs PROHIBITED matrix (binding)

| Use case | Unit priced | Status | Conditions / guardrails |
|---|---|---|---|
| Value-based care / shared savings | provider panel / population | **Permitted** | Profit only from measured improvement (§5); no care-denial incentive |
| Wellness incentives | individual (reward only) | **Permitted, capped** | Voluntary; reasonable-alternative-standard; incentive within HIPAA/ADA caps; **never a surcharge that prices the sick out** |
| Group / population underwriting & reserving | group / book | **Permitted where legal** | Large/self-insured group rating per state law; portfolio reserving (§3); not individual rating |
| Risk-adjustment (HCC-style) | population payment pool | **Permitted** | Moves money **toward** sicker panels; budget-neutral; audited for up-coding |
| Reinsurance / stop-loss attachment | portfolio / layer | **Permitted** | §3.4; manages solvency at the book level |
| Individual medical underwriting / risk-rating | individual | **Prohibited** (ACA indiv/small-grp); **limited** elsewhere | Never sets a sick individual's health-plan premium; life/LTC only where lawful and filed |
| Genetic-info underwriting | individual | **Prohibited** (GINA / GDPR / GNDA) | Genetic-derived inputs fenced from pricing |
| Pre-existing-condition pricing/exclusion | individual | **Prohibited** (ACA) | No exclusions, surcharges, or score-based denial |
| Redlining / protected-class proxy rating | any | **Prohibited** (always) | README §5.5; §6 detection is a hard gate |
| Coverage denial / care rationing via score | individual | **Prohibited** (always) | PureScore must never reduce sick-person access (§6.1) |

> If a use is not affirmatively in the **Permitted** rows, it is treated as **Prohibited** until
> counsel + actuarial + DOI clear it. Default-deny.

---

## 2. The pricing engine (population-level, with math)

The engine estimates **expected annual claims cost** for a *cohort* and, within permitted group
contexts, blends toward group experience (§3). It never produces an individual health-plan
premium in a prohibited context.

### 2.1 Multiplicative cost model
For a cohort `c` (or a permitted group `G` built from cohorts), expected annual claims:

```
 Ĉ(c) = baseline_cost(c) · Π_k  μ_k(R_k, q_k)  ·  μ_res(B̃)  ·  μ_clin(Θ)  ·  TREND(t)
```

- `baseline_cost(c)` — the cohort's **manual/book base cost** (age band × sex-where-lawful ×
  geography × benefit design), from standard manual tables, *not* from PureScore. PureScore
  enters **only** through the multipliers, and only in permitted contexts.
- `μ_k(·)` — **pillar relativity** for pillar `k` (§2.2), a function of pillar risk `R_k` and the
  cohort-relative stack-rank percentile `q_k` (Doc 03 §2). Bounded; see §2.3.
- `μ_res(B̃)` — **reservoir-trajectory** multiplier (Doc 04): chronic accumulated burden and its
  *direction* (worsening reservoirs cost more than a stable spot value; §2.4).
- `μ_clin(Θ)` — **validated clinical-score** multiplier from Doc 10 equations (ASCVD/SCORE2,
  FINDRISC, KDIGO, FIB-4, FRAX), used as actuarial covariates where they are prospectively
  outcome-linked. **GINA/GDPR-fenced inputs excluded from any pricing use.**
- `TREND(t)` — medical-cost trend (utilization + unit-cost inflation), a standard actuarial
  factor independent of PureScore.

The log form is the working representation (additive, auditable, easy to constrain):

```
 ln Ĉ(c) = ln baseline_cost(c) + Σ_k βk·f_k(R_k, q_k) + β_res·h(B̃) + Σ_m β_m·g_m(Θ_m) + ln TREND(t)
```

### 2.2 From PureScore / pillar stack-rank to actuarially-sound relativities
A score is **not** a price. Relativities must be **fitted to observed cost**, not assumed from
the score scale. Procedure (gated by Doc 13 calibration):

1. **Bin** each pillar's risk/percentile into actuarial cells (e.g., deciles of `R_k` or `q_k`).
2. **Fit** a GLM (gamma or Tweedie with log link for cost; or two-part frequency×severity) of
   *observed annual cost* on the binned pillar signals + manual factors, on **historical,
   consented, de-identified** data for the population to be priced.
3. The fitted relativity for cell `b` of pillar `k` is
   `μ_k(b) = exp(β̂k·f_k(b))`, **smoothed/monotone-constrained** so adjacent cells move
   coherently (isotonic or penalized splines) and **credibility-capped** (§3) so thin cells do
   not produce wild factors.
4. **PureScore itself** may be used as a single coarse acuity tier for *population* models, but
   the pillar-level decomposition is preferred because it is more explainable and more auditable
   for fairness (§6). The critical-cascade cap (Doc 03 §5.3) means a top-line PureScore can be
   floored by one pillar; for cost-modelling, the **uncapped pillar vector is the covariate**, not
   the capped headline (the cap is a safety/UX device, not a cost predictor).

> **Hard rule.** Relativities are *empirical and validated*, never asserted. A pillar that does
> not improve out-of-sample cost prediction on the priced population gets `μ_k ≡ 1` (no effect).
> This is the anti-Babylon discipline (Doc 01 §3.1): no claim ahead of its evidence.

### 2.3 Bounding and the "no worsening the sick" envelope
- Each `μ_k` is clamped to a defensible, filed band (e.g., `[0.7, 1.6]`) so no single signal can
  produce extreme individual-level differentiation even where group differentiation is lawful.
- In **any individual-affecting context**, multipliers that would *raise* an individual's cost or
  *reduce* access are **disabled** (set to ≤1 for protective use only, or removed entirely). The
  engine may use risk *upward* only to (a) steer prevention resources and (b) justify
  risk-adjustment that pays *more* for sicker panels — never to charge a sick person more.

### 2.4 Reservoir-trajectory term (cost has memory and direction)
Using Doc 04 reservoirs `B_j` and leakage `λ_j`, define a trajectory feature:

```
 h(B̃) = a·B̃  +  b·(dB̃/dt)        # level + slope; worsening trajectory carries extra expected cost
```

A stable high burden, an improving high burden, and a worsening moderate burden have *different*
expected costs. This is the actuarial expression of MONIAC memory and is the lever value-based
contracts reward when `dB̃/dt < 0` (improvement, §5).

### 2.5 Expected vs catastrophic (split the distribution, don't average it)
Total cost is heavy-tailed; pricing the **mean** is not pricing the **risk**. Decompose:

```
 Cost = Cost_expected  +  Cost_catastrophic
 Cost_expected   ≈ frequency × severity   in the body of the distribution (priced via §2.1)
 Cost_catastrophic ≈ low-frequency, high-severity tail (priced via reinsurance/stop-loss, §3.4)
```

PureScore is most informative about the **body** (chronic-disease trajectory). It is *weakly*
informative about idiosyncratic catastrophes (trauma, rare cancers, random shocks). The tail is
managed by **portfolio diversification and reinsurance**, not by individual prediction — which is
exactly why "impossible to lose money" fails (§3.2).

---

## 3. Credibility, portfolio risk, and the honest reframing of "don't lose money"

### 3.1 Bühlmann–Straub credibility (individual/group vs manual)
A group's own experience is noisy; the manual rate is stable but generic. Blend them:

```
 Premium_blended = Z · Rate_experience  +  (1 − Z) · Rate_manual

           n
 Z = ───────────       (Bühlmann);   with exposure weights w_g (Bühlmann–Straub):
        n + K

        E[ Var(X|θ) ]            (expected process variance)
 K =  ──────────────────
        Var( E[X|θ] )            (variance of hypothetical means)
```

- `Z` rises with **exposure** (member-years, claim count): large groups earn more weight on their
  own experience; tiny groups stay near manual. This is the actuarial reason **individuals are
  near-uncreditable on their own** — `Z → 0` for a single life-year of experience.
- `K` is estimated from the book (Doc 13 credibility section). PureScore can **sharpen the manual
  rate** (a better `Rate_manual` via §2) and can **raise effective credibility** by explaining
  within-group heterogeneity — but it does **not** make an individual fully creditable.

### 3.2 Why perfect individual prediction is impossible (quantify it)
Decompose realized individual cost variance:

```
 Var(Cost_p) = σ²_irreducible  +  σ²_model  +  σ²_behavioral
```

- **σ²_irreducible** — genuine randomness (who slips on ice, who gets a rare cancer). No score
  removes it. The tail (§2.5) lives here.
- **σ²_model** — finite-data calibration error, drift, residual confounding (Doc 13).
- **σ²_behavioral** — moral hazard and adverse selection responses to the price itself (§4).

Even a perfectly calibrated `Ĉ(p)` is the **conditional mean**; the individual outcome scatters
around it with variance dominated by `σ²_irreducible`. **At the portfolio level**, by the law of
large numbers, `Var(mean cost) ≈ σ²/n` shrinks with `n` (less any correlated/systemic component).
**Solvency is therefore a property of the book, not the member.** This is the formal rebuttal to
*"impossible to lose money"*: you cannot price away irreducible variance on one life; you can only
**pool, diversify, and reinsure** it.

### 3.3 Loss-ratio discipline (the real objective)
Replace "never lose money" with **target loss-ratio management**:

```
 Loss Ratio = Incurred Claims / Earned Premium
 Target:  LR* (e.g., within filed bounds; ACA MLR floors of 80%/85% are *minimums* that must be met)
 Combined Ratio = (Claims + Expenses) / Premium   < 1  for underwriting profit
```

- Price to a **target LR with a risk margin**, monitor *actual* LR by cohort and subgroup (§6.4),
  and **re-rate the book**, not the sick individual, when LR drifts. ACA **MLR rebate** rules
  mean over-pricing is itself penalized — discipline cuts both ways.

### 3.4 Reserving and reinsurance/stop-loss (where solvency is actually managed)
- **Reserves.** Hold IBNR (incurred-but-not-reported) + case reserves + risk margin; under
  Solvency II, technical provisions = best-estimate + risk margin, backed by SCR/MCR capital.
- **Reinsurance / stop-loss.** Cap the tail:
  - *Specific (individual) stop-loss*: reinsurer pays claims on a member above attachment `A`.
  - *Aggregate stop-loss*: reinsurer pays book claims above `A_agg` (e.g., 125% of expected).
  - *Quota-share / excess-of-loss*: share or cap layers of the whole portfolio.
- **Diversification.** Pool across uncorrelated cohorts/geographies so idiosyncratic shocks net
  out; watch **correlated** shocks (pandemic, pharmacy-cost spikes) that defeat diversification —
  those are explicitly a reinsurance/capital problem, not a scoring problem.

> **The portfolio-risk posture, stated plainly:** profit = (disciplined manual + improvement
> economics) × (credibility-blended group experience) − (claims) − (expenses), with the tail
> reinsured and capital held to absorb the irreducible variance. No individual is ever the unit
> of solvency, and no sick individual is ever made the shock-absorber.

---

## 4. Adverse selection and moral hazard

A health score can **worsen or mitigate** both. Naming the failure modes is the guardrail.

### 4.1 Adverse selection (who buys)
- **Risk:** if low-risk members can self-identify (via their own PureScore) and exit, the pool
  deteriorates and the spiral begins; if the *insurer* uses the score to skim low-risk and shed
  high-risk, that is **prohibited cherry-picking / lemon-dropping** (§6, and unlawful under ACA).
- **Mitigations:** guaranteed issue + community rating (removes the insurer's selection lever);
  broad pooling and risk-adjustment (§1.1.2) that **transfers money to plans enrolling sicker
  members**, neutralizing the incentive to avoid them; single risk-pool rules; reinsurance for
  high-cost enrollees.

### 4.2 Moral hazard (behavior given coverage)
- **Risk:** insurance can blunt prevention incentives; a naive wellness *surcharge* punishes the
  sick (regressive, often illegal) without changing behavior.
- **Mitigations:** **pay-for-improvement** (reward `dPureScore/dt > 0`, §5) rather than penalize
  state; value-based provider incentives aligned with member health; nudges/care plans (Docs
  09/11) that lower expected cost by improving health, not by gatekeeping. Incentives must stay
  within wellness-program legal caps and always offer a **reasonable alternative standard** so a
  member who *cannot* improve for medical reasons is never penalized.

---

## 5. Aligned economics — the Kaiser model (profit from improvement, never from denial)

The economic thesis (Doc 01 §2.6, §3.2): **margin comes from raising PureScore**, i.e., from
prevention and avoided downstream cost — *not* from denying care.

### 5.1 Improvement-economics math
Let `ΔPureScore` (or pillar-level `ΔR_k` and reservoir slope `dB̃/dt`) over a contract period map
to **avoided expected cost** via the validated §2 model:

```
 Savings_expected = Ĉ(state_baseline)  −  Ĉ(state_improved)
 Shared_savings_payment = α · max(0, Savings_realized)     # α = contracted provider share
```

- Payment triggers on **realized** savings (measured claims) reconciled against a credible
  baseline/benchmark, with `ΔPureScore` as the *clinical evidence* the savings were earned
  through health improvement, not undertreatment.
- **Anti-stinting guardrail:** shared-savings contracts **must** carry quality/access gates
  (no increase in avoidable ED visits, no drop in necessary-care utilization, patient-reported
  access maintained). Savings achieved by *withholding* needed care **forfeit** the payment and
  trigger review (Doc 16). This is the explicit firewall against the HMO denial failure mode.

### 5.2 Pay-for-improvement structures
- **Member side:** rewards for engagement and measured improvement (within wellness caps;
  reasonable-alternative-standard mandatory). Never a penalty on the un-improvable sick.
- **Provider side:** capitation/quality blends, value-based contracts paying for closed care gaps
  and improved pillars (the nudge/care-plan engine, Docs 09/11, is the delivery mechanism).
- **Plan side:** lower expected claims from a healthier book *is* the return; it is captured at
  the **portfolio** level (§3), aligning insurer, provider, and patient toward the same `↑PureScore`.

### 5.3 How nudges/care plans lower cost ethically
Docs 09/11 lower `Ĉ` by draining reservoirs (`dB̃/dt < 0`) and moving pillars toward green —
i.e., by *making people healthier*. The cost reduction is a **consequence of health gain**, fully
explainable (Doc 03 §7), and auditable against quality gates. That is the only sanctioned profit
path in this spec.

---

## 6. Fairness and regulatory guardrails (heavy — these are gates, not preferences)

### 6.1 The bright line (non-negotiable, README §5.5)
> **PureScore must never reduce a sick person's access to, or affordability of, care.** No score,
> pillar, reservoir, or relativity may be used to deny coverage, exclude a pre-existing condition,
> surcharge illness, or proxy a protected class. Where pricing is permitted at all, it is
> *group/portfolio* pricing within filed, justified, non-discriminatory bounds. Any feature that
> fails this test is **removed**, not tuned.

### 6.2 Protected-class proxy detection (hard gate, ties to Doc 13)
- **Proxy audit.** For every covariate (pillar, marker, reservoir, clinical score, geography),
  test predictive association with protected attributes (race/ethnicity, sex, age beyond lawful
  bands, disability, genetic info). Flag high-leverage proxies (e.g., geography → redlining;
  certain biomarkers → ancestry). High-proxy features are **fenced from any price/access use**.
- **Genetic fence (GINA/GDPR/GNDA).** Genetically-derived inputs are excluded from underwriting
  by construction, not by policy promise.
- **Suppression vs justification.** A feature with disparate impact may be retained **only** with
  documented actuarial justification *and* no less-discriminatory alternative — and **never** in a
  prohibited individual-rating context.

### 6.3 Disparate-impact and subgroup loss-ratio testing
- **Disparate-impact tests.** Compare adverse outcomes (price level, access flags, denial-of-
  service signals) across protected subgroups; apply standard thresholds (e.g., four-fifths rule
  as a screen, plus statistical tests) and **calibration-within-group** checks (Doc 13 fairness).
- **Subgroup loss-ratio monitoring (§3.3 by subgroup).** Continuously monitor LR, relativities,
  and access metrics by protected subgroup. A subgroup systematically **over-charged** or
  **under-served** is a defect → halt and remediate (Doc 16). Equal LR is *necessary but not
  sufficient*; access and calibration parity are also required.

### 6.4 Actuarial justification, filings, transparency, appeal
- **Actuarial justification & filings.** Every relativity used in a regulated product carries a
  documented, data-backed justification and is **filed** with the relevant DOI/regulator
  (US state filings; EU IDD/POG + Solvency II provisions; UK FCA). **No unfiled factor prices.**
- **Transparency.** Methodology and the role (or non-role) of PureScore in pricing are disclosed;
  explainability output (Doc 03 §7) supports member-facing reason codes.
- **Appeal rights.** Members can contest a score, a relativity, or an adverse decision; human
  review and correction pathways are mandatory (GDPR Art. 22 automated-decision rights; general
  due-process). Appeals route through Doc 16 governance.

### 6.5 Tie-in
Doc 13 provides the calibration, credibility, drift, and fairness machinery that *gates* §2–§3.
Doc 16 owns governance: filings, audit trail, escalation, model-version control, and the appeal
process. This document defines **what** may be priced; Doc 13 proves it is **valid and fair**;
Doc 16 enforces **how** it ships.

---

## 7. Risk register (blunt — how this product blows up, and the mitigations)

| # | Failure mode | How it happens | Babylon/precedent parallel | Mitigation (binding) |
|---|---|---|---|---|
| R1 | **Illegal individual risk-rating** | Score leaks into ACA indiv/small-group premiums or pre-existing surcharge | Health-status underwriting pre-ACA | §1.3 default-deny matrix; score fenced from individual rating; counsel + DOI gate |
| R2 | **Proxy discrimination / redlining** | Geography or biomarker proxies race/SES; disparate price/access | Algorithmic redlining cases | §6.2 proxy audit; fence high-proxy features; §6.3 disparate-impact gate |
| R3 | **Genetic-info underwriting** | Heritable markers (Lp(a), PRS) used in pricing | GINA/GDPR violations | §6.2 genetic fence by construction |
| R4 | **Overpromise → "can't lose money"** | Marketing/clairvoyant-pricing claims outrun evidence | **Babylon** accuracy overclaim → insolvency | Doc 01 §4 rejection; §3.2 irreducible variance; portfolio posture only |
| R5 | **Care denial / stinting for savings** | Shared-savings paid for withholding care | HMO denial backlash | §5.1 quality/access gates; forfeit + review on stinting |
| R6 | **Adverse-selection spiral** | Low-risk exit / insurer cherry-picks | Death-spiral markets | §4.1 guaranteed issue, single pool, risk-adjustment, reinsurance |
| R7 | **Moral-hazard surcharge on the sick** | Regressive wellness penalty | Punitive wellness programs | §4.2 pay-for-improvement + reasonable-alternative-standard + caps |
| R8 | **Mis-calibration / drift** | Relativities fit on stale/biased data | Model decay | Doc 13 calibration & drift gate; `μ_k≡1` if no validated lift |
| R9 | **Tail / correlated-shock insolvency** | Pandemic, pharmacy spike defeats diversification | Catastrophe losses | §3.4 reinsurance, aggregate stop-loss, capital (SCR/MCR) |
| R10 | **Unfiled / unjustified factors** | Pricing on un-filed relativities | Unfair-discrimination findings | §6.4 file-before-price; actuarial justification required |
| R11 | **Privacy / consent breach** | Health data used beyond consent | GDPR Art. 9 / HIPAA breaches | Doc 16 privacy governance; gated access; consented data only |
| R12 | **Opaque automated adverse decision** | No explanation/appeal | GDPR Art. 22 violations | §6.4 transparency + appeal; Doc 03 §7 explainability |
| R13 | **Up-coding risk-adjustment** | Gaming acuity to extract payment | RADV/HCC audit findings | §1.1.2 budget-neutral, audited; coding-integrity controls |

> The single most important line in this document: **profit comes from improving PureScore, never
> from denying care or pricing out the sick.** Any mechanism that violates that is not a tuning
> question — it is removed.

---

## 8. Default constants and posture (all tunable, all versioned, all filed)

| Symbol | Meaning | Default / posture |
|---|---|---|
| `μ_k` band | per-pillar relativity clamp | `[0.7, 1.6]` (filed; tighter in individual-affecting contexts) |
| `Z` | Bühlmann credibility weight | exposure-driven; `→0` for individuals |
| `K` | Bühlmann parameter (EPV/VHM) | estimated per book (Doc 13) |
| `LR*` | target loss ratio | within filed bounds; ≥ ACA MLR floors |
| `A`, `A_agg` | stop-loss attachments | per reinsurance treaty (e.g., agg 125% of expected) |
| `α` | provider shared-savings share | contracted; gated on quality/access |
| four-fifths | disparate-impact screen | screen only; plus statistical + calibration parity |
| default-deny | any non-permitted use | **Prohibited until cleared** (§1.3) |

All constants live in versioned config; any change is a model-version bump requiring
re-validation (Doc 13), refiling where regulated, and an audit-trail entry (Doc 16).
