# 03 — The Core Scoring Formula

> Binding conventions: `README.md §3`. This document defines the full deterministic computation
> **marker → pillar → PureScore**, including the critical cascade. The dynamic reservoir terms
> that feed `r_i` and `R_k` are defined in Doc 04 and referenced here as `B_j`.

The pipeline has five stages:

```
 raw value x_i ─▶ (1) marker risk r_i^clin ─▶ (2) cohort blend ─▶ (2b) personal-baseline z
                                                              ─▶ (3) pillar risk R_k
                                                              ─▶ (4) critical cascade
                                                              ─▶ (5) PureScore
```

---

## 1. Stage 1 — Marker risk from clinical bands

> **In plain words.** Turn each lab/wearable/PRO number into a 0–1 "how bad is this" risk by
> measuring how far it has drifted from its personal optimum toward the guideline danger lines —
> as a smooth curve, not a traffic light, so even a one-day improvement nudges the score.

Each marker has guideline bands (Doc 02). Define the **clinical risk** `r_i^clin ∈ [0,1]` as a
continuous, monotone, two-sided penalty so the score is smooth (no cliff at a threshold) but
anchored to the guideline cut-points.

Let `m_i` be the **optimum** (band centre), and on each adverse side let `c_y` be the **yellow
cut-point** and `c_r` the **red cut-point** (Doc 02; for a one-sided marker the non-adverse side is
`±∞` and contributes 0). Define the one-sided **adverse distance** `t = max(0, s·(x − m_i))`, where
`s = +1` on the high side and `s = −1` on the low side. With `a_y = |c_y − m_i|`, `a_r = |c_r − m_i|`
and saturation width `w = |c_r − c_y|`:

```
            ⎧ 0,                                          t = 0           (at the optimum)
            ⎪ 0.15 · t / a_y,                             0 < t ≤ a_y     (green: optimum-centred ramp)
 r_side  =  ⎨ 0.15 + 0.35 · (t − a_y)/(a_r − a_y),        a_y < t ≤ a_r   (yellow → red edge)
            ⎪ 0.50 + 0.50 · (1 − e^{−(t − a_r)/w}),       t > a_r         (red, saturating → 1)
            ⎩
 r_i^clin = max( r_low , r_high )                    # two-sided markers; one-sided uses the adverse side only
```

**This single curve subsumes the old "flat green core + separate `r_green`".** There is no flat
`[L,U]` plateau in the number: the optimal band is represented by its centre `m_i`, and the green
branch `0.15 · t/a_y` **is** the within-green optimum-centering gradient `r_i^green ∈ [0, 0.15)`.
(This resolves the spec-audit contradiction between "`r=0` on `[L,U]`" and a separate green
gradient — they were the same thing. For a marker with a genuinely flat optimal plateau, set `m_i`
to the nearer plateau edge so the ramp begins at the plateau boundary, keeping a true `r=0` core.)

**Low-side penalty is fully specified, not "mirror of the above".** The low side uses the *same*
four-branch curve with `s = −1` and the low cut-points `c_y = L_y`, `c_r = L_r`; the high side uses
`s = +1`, `c_y = U_y`, `c_r = U_r`. For **negative-anchored markers** (e.g., BMD `T-score`, where
more-negative is worse) the optimum is the high anchor and only the low side is adverse, so
`r_i^clin = r_low` with `m_i = 0`, `c_y = −1.0`, `c_r = −2.5` — well-defined with no special case.

Properties: `r=0` at the optimum; `r=0.15` exactly at the yellow cut; `r=0.5` exactly at the red
cut; saturates toward `1` deep in the red so a single extreme value dominates but cannot make
`r>1`. The colour zones in README §3.5 fall out directly (`<0.15` green, `<0.5` yellow, `≥0.5` red).

**Continuity is a hard requirement, not a side-effect.** Every map in Stage 1 is `C⁰`-continuous
(and `C¹` except at the designed critical step, §6): an arbitrarily small change in `x_i` yields an
arbitrarily small change in `r_i`. There are **no discrete band jumps in the number** — the
green/yellow/red bands are *labels read off the continuous curve* (`r<0.15`/`<0.5`/`≥0.5`), never
the generator of the score. This is what lets a single day's action register as a real, visible
score change (§6.1; Doc 11; Doc 05). The shared engine (`assets/engine.js`) implements this exact
curve (`markerR` → `contRisk`); see §6.2 for the prose↔engine reconciliation.

---

## 2. Stage 2 — Blend with cohort percentile (safety-dominant)

> **In plain words.** Also ask "how unusual is this value *for people like you*?" If you're an
> outlier on the bad side, raise the risk — but this comparison can only ever make the score more
> cautious, never less.

The cohort percentile (Doc 06 §4.2) personalizes the score and powers stack-ranking, but it may
only **raise** concern, never lower it below the clinical anchor:

```
 q_i        = F_{i,c}(x_i)  ∈ [0,1]    # patient's value as a percentile within cohort c (Doc 06 §4.2)
 r_i^cohort = g(q_i)                   # adverse-percentile → risk; g(0.5)=0, rises toward the adverse tail
 r_i        = max( r_i^clin ,  φ · r_i^cohort )           # φ = 0.6 default  (Doc 06 §4.4)
```

**`g(q)` is now fully specified** (was qualitative-only — a spec-audit blocker). Orient `q_i` so the
**adverse tail is `q→1`** (Doc 06 §4.2 stores the sign per marker). Then `g` is a one-sided smooth
ramp that is flat across the healthy half and rises only past the median:

```
 g(q) =  0,                                  q ≤ 0.5
         ( (q − 0.5) / 0.5 )^2,              q > 0.5        # = (2q−1)², smooth, g(0.5)=0, g(1)=1
```

- `g` is `C¹` at `q=0.5` (value and slope `0`), monotone, and reaches `1` only at the extreme
  adverse percentile, so being an outlier *raises* risk progressively. The quadratic keeps mild
  outliers (60th–75th pct) gentle and lets only genuine tail values (`>90th`) approach `φ·1 = 0.6`.
- For a **healthy** reference cohort, `g` flags being an outlier on the bad side even while still
  technically in-range — an early-warning property.
- For a **diseased** cohort, the `max(...)` guarantees the clinical anchor cannot be diluted by
  looking "good for a sick group" (the HbA1c-7.5%-in-diabetics example, Doc 06 §4.4).

**Stack-ranking (for display and the actuarial layer)** uses `q_i` and pillar percentiles
directly; it is presentation/analytics, not a relaxation of `r_i`.

---

## 2b. Stage 2b — Personal-baseline responsiveness (the feedback term)

> **In plain words.** Compare today's reading to *your own* recent normal. A good night's sleep or
> an in-range glucose day nudges the score up *today*; a bad streak nudges it down — but only
> within your current colour band, so feedback never overrides safety. *(D23, locked.)*

Stage 2 personalizes *across people*; Stage 2b personalizes *across time for one person*, so the
score visibly tracks short-term action. Each marker carries a robust **personal baseline**
`μ_i^p, σ_i^p` (empirical-Bayes, log-scaled for heavy-tailed markers — D13; cold-start shrinks to
cohort — Doc 05 §5.1, Doc 15 ignition). The empirical-Bayes mean (the shrinkage that makes a
new member borrow the cohort until they have history — D6) is:

```
 μ_i^p = (n_i /(n_i + k_i)) · x̄_i^personal  +  (k_i /(n_i + k_i)) · μ_i^cohort     # k_i = per-marker prior strength
```

giving a **personal z-score** oriented so *adverse = positive*:

```
 z_i = sign_adverse · (x_i − μ_i^p) / σ_i^p          # on log(x) for heavy-tailed markers (D13)
```

A bounded, smooth gradient turns recent personal movement into a small score change:

```
 r_i^pers = κ_resp · tanh( z_i / 2 )                       # κ_resp = 0.10 cap; continuous, monotone
 r_i      = band_clamp(  s_i + r_i^pers · 𝟙[non-critical] ,  s_i )     # s_i = max(r_i^clin, φ·r_i^cohort)
```

- **`band_clamp(x, s)` is fully specified** (was load-bearing but undefined — a spec-audit major).
  Let `s_i` be the pre-personalization **safety risk** `max(r_i^clin, φ·r_i^cohort)`. `band_clamp`
  confines the personalized value to the half-open zone `s_i` already sits in, so Stage 2b can move
  the number continuously but can **never flip a band, relax a red, or clear a critical**:

```
 band_clamp(x, s) =  clamp( x, 0,    0.15⁻ )   if  s < 0.15     (green stays green)
                     clamp( x, 0.15, 0.50⁻ )   if  0.15 ≤ s < 0.50   (yellow stays yellow)
                     clamp( x, 0.50, 1.00  )   if  s ≥ 0.50     (red can only stay red)
```

  Safety (clinical anchor + cohort `max`) always dominates (README §5.2). The engine applies the
  term only on **fresh** (`present`) non-critical markers; stale/imputed markers carry no
  personal-baseline movement (no recent reading to compare against).
- **Direction = feedback.** Better-than-your-baseline (`z_i<0`) lowers `r_i` a little → PureScore
  ticks **up**; worse-than-baseline (`z_i>0`) raises it → PureScore ticks **down**. A good night's
  sleep, a 9 000-step day, an in-range glucose curve, a calmer week all move the number *today*,
  before any band is crossed.
- **Cadence.** `μ_i^p, σ_i^p` and `z_i` update on each new reading: daily for wearables/behaviours
  (sleep, steps, HRV, resting HR, CGM, stress check-ins), per-measurement for labs. The fast
  channels are exactly the modifiable ones the nudge engine acts on (Doc 11), closing the loop.
- Stage 2b is also the substrate for the **Trajectory/momentum** and **Early-warning** companion
  dimensions (Doc 05 §4–§5): the same `z_i` stream that nudges the number drives the arrows.

### 2b.1 The companion-dimension vector (what rides alongside the number)

The `z_i` stream and the per-pillar risk feed a **companion vector** of meta-dimensions reported
*next to* the PureScore — most are descriptive, never re-entering the score. Doc 03 names only
Trajectory and Early-warning above; the full set is defined in **Doc 05 §4**:

| # | Dimension | What it says | Re-enters score? |
|---|-----------|--------------|------------------|
| 1 | **Confidence** `Cf_k` | coverage × source-quality × stability | no (display + gating) |
| 2 | **Data sufficiency** `Su_k` | % fresh member data; flags INSUFFICIENT | no |
| 3 | **Criticality** `Cr` | escalation tier + count of red/critical markers | via §4 cascade |
| 4 | **Trajectory** `Tr_k` | slope of `S_k` over the window (↑ ↓ →) | no |
| 5 | **Early-warning** `Ew_k` | tiered anomaly: none / Watch / Advisory / Alert (Doc 05 §5.3) | no |
| 6 | **Representativeness** `Rp_k` | cohort-match credibility (OOD distance) | no |
| 7 | **Skew / robustness** `Sk_i` | per-marker heavy-tail flag → log-scale (D13) | indirect |
| 8 | **Volatility** `Vo_k` | short-window within-patient variance | no |
| 9 | **Modifiability** `Mo_k` | share of `R_k` from modifiable vs fixed inputs | no (honest-ceiling) |

A read-only **Stress-load** `St` (autonomic + ALLO reservoir, Doc 05 §4.1) is surfaced but **does
not weight the score**. Modifiability is what keeps the engine from selling false hope (§6.1.3).

---

## 3. Stage 3 — Pillar risk (worst-sensitive aggregation, confidence- and reservoir-aware)

> **In plain words.** Roll a pillar's markers into one 0–1 risk, but let the *worst* marker
> dominate (so one dangerous value can't be averaged away), trust shaky readings less, and add the
> slow-burning "chronic debt" the reservoirs are carrying.

A pillar must not let many mediocre markers average away one dangerous one. We use a **weighted
power mean with exponent γ > 1** (a soft-max over risks), with weights scaled by each marker's
confidence, plus an additive **reservoir term** carrying chronic burden (Doc 04).

```
 confidence_i = q_source · e^{−(t − t_i)/τ_i}    # source quality × staleness decay (Doc 06 §2; D33)
 ŵ_i          = w_i · confidence_i               # low-confidence/imputed markers contribute less

           ⎛  Σ_{i∈k} ŵ_i · r_i^γ  ⎞^{1/γ}
 R_k^mark = ⎜ ───────────────────── ⎟           γ = 3 (default; higher ⇒ more worst-case-sensitive)
           ⎝     Σ_{i∈k} ŵ_i        ⎠            … defined only when Σŵ_i > 0 (see empty-pillar rule)

 R_k = clamp_{[0,1]}(  R_k^mark  +  ρ_k · B̃_k  )      # B̃_k = normalized reservoir load for pillar k
```

**The reservoir load `B̃_k` is defined in Doc 04 §5** (burdens add, asset-depletion adds):

```
 B̃_k = clamp_{[0,1]}(  Σ_{j∈burden(k)}  u_{kj} · (B_j / B_j^max)
                      + Σ_{j∈asset(k)}   u_{kj} · (1 − B_j / B_j^target)  )    # u_{kj} = link weight, Doc 04 §2
```

where `B_j` are the reservoir stocks, drained by leak `λ_j` and coupled by interference `κ_jl`
(Doc 04; calibrated in Doc 13 — `λ_j`, `κ_jl`, `u_{kj}` are expert-prior, see §8).

**Empty-pillar rule** (was a missing edge-case — spec-audit major). If a pillar has *no observed
markers* (`Σŵ_i = 0`, all unobserved/dropped), `R_k^mark` is **undefined**, not `0`. The pillar is
marked **no-data / INSUFFICIENT**, cannot display green (coverage `0`), and is **excluded from the
Stage-5 weighted mean** (its weight is dropped and the remaining `W_k` renormalized) so an
all-missing pillar can never read as a healthy `0`. The engine sets `noData = true` and omits the
pillar from `R_total`.

- `γ = 3` makes the worst one or two markers dominate while still reflecting breadth.
- `B̃_k ∈ [0,1]` is the aggregate of the reservoirs linked to pillar `k` (Doc 04 §5), e.g. chronic
  sleep debt raises SLP, MCS, CV, MET even when their spot markers look acceptable. `ρ_k` (≈0.2)
  bounds how much chronic burden can move a pillar on top of its current markers.
- **Coverage** `cov_k = Σ ŵ_i over observed / Σ w_i over all` is reported alongside `R_k`. Low
  coverage caps how *green* a pillar may be displayed (a pillar that is green only via imputed
  medians is shown as *low-coverage green*, Doc 06 §4.3).

`S_k = 100 · (1 − R_k)`.

### 3.1 The 12 pillars `k` (codes used throughout this doc)

Stage 3 runs once per pillar `k`. The canonical roster (Doc 02; weights in §5.1 / `pillar-weights.json`):

| Code | Pillar | Base `W_k` | Key reservoirs (Doc 04) |
|------|--------|-----------|--------------------------|
| **CV**  | Cardiovascular & vascular              | 0.13 | ATH, VBP, CRF |
| **MET** | Metabolic & glycemic                   | 0.12 | GLY, ADI, HEPF |
| **SLP** | Sleep & circadian recovery             | 0.10 | SLD, ALLO |
| **FIT** | Physical activity & cardiorespiratory fitness | 0.10 | CRF, MUS |
| **MCS** | Mental, cognitive & social health      | 0.10 | ALLO |
| **REN** | Renal                                  | 0.07 | RENR |
| **INF** | Inflammation & immune                  | 0.07 | INFL |
| **BCM** | Body composition & musculoskeletal     | 0.07 | ADI, MUS, BON |
| **HEP** | Hepatic                                | 0.06 | HEPF |
| **HEM** | Hematologic & oxygen transport         | 0.06 | OXD, IRON |
| **ENDO**| Endocrine & hormonal                   | 0.06 | ALLO |
| **NUT** | Nutrition & micronutrients             | 0.06 | MICR |

`Σ_k W_k^base = 1.00` (enforced at build by `_engine_guard()`). Weights are **expert-prior**, not
outcome-calibrated (§8).

---

## 4. Stage 4 — Pillar status and the critical override

> **In plain words.** Decide each pillar's colour, and if a *critical* marker is red (e.g. a
> dangerous potassium), force the whole pillar to critical so it can't be smoothed over downstream.

```
 status_k =
   red/critical  if  (∃ i∈k with r_i ≥ 0.5 AND i ∈ CriticalMarkers_k)      # any critical-marker red
                 or  R_k ≥ 0.60
   yellow        else if  R_k ≥ 0.30  or  (∃ i with 0.15 ≤ r_i < 0.5)
   green         else
```

When a pillar is **critical**, its risk is floored so the cascade cannot be diluted:

```
 if status_k == critical:   R_k ← max(R_k, R_crit)        # R_crit = 0.60
```

`CriticalMarkers_k` are the marked rows in Doc 02 (e.g., systolic BP, eGFR, potassium, HbA1c,
hsCRP-acute, hemoglobin, PHQ-9 item 9). This is the realization of *"a red biomarker makes its
pillar critical."*

### 4.1 Acute-danger reds vs reserve-deficit reds
Doc 02 (FIT) notes some reds are **reserve deficits** (e.g., very low VO2max) — serious but not
emergencies — vs **acute-danger reds** (e.g., K⁺ 6.2, SpO2 88%, suicidality). Both floor the
pillar, but only **acute-danger reds** trigger the immediate clinician/crisis escalation pathway
(Doc 16); reserve-deficit reds drive **priority improvement** plans (Doc 09) and high-leverage
nudges (Doc 11). The marker catalogue tags each critical marker with `escalation ∈ {emergency,
urgent, routine}`.

---

## 5. Stage 5 — PureScore (personalized weights + critical cascade)

> **In plain words.** Combine the 12 pillars into one 0–100 headline, weighting the pillars that
> matter most *for this person* — again letting the worst pillar pull hardest — then hard-cap the
> result if anything is critical.

### 5.1 Personalized pillar weights
```
 W_k = W_k^base · m_k^cohort · m_k^goal · m_k^acute ,   then normalized so Σ_k W_k = 1
```
- `W_k^base`: default importance (longevity/all-cause-mortality contribution; CV, MET, FIT, MCS,
  SLP carry the largest defaults).
- `m_k^cohort`: raises pillars central to the member's conditions/meds (diabetic ⇒ MET, REN, CV
  up; CKD ⇒ REN up; on statin ⇒ CV interpretation adjusts).
- `m_k^goal`: raises pillars tied to the member's stated goals (Doc 09/11).
- `m_k^acute`: spikes during an acute event (Doc 09); reverts with hysteresis on recovery.

#### 5.1.1 Where personalization enters the pipeline

The base formula is universal; everything that makes the score *yours* enters at one of four
hooks. Each is **raise-only / safety-preserving** — none can clear a critical or relax a red.

| Hook | Stage | Driver (doc) | Effect |
|------|-------|--------------|--------|
| **Sex & life-stage bands** | 1 | Doc 08 | Phase-aware reference ranges (cycle / pregnancy / menopause); ENDO/HEM/REN band & `W_k` reshaped; anatomy gating (PSA/AMH) |
| **Cohort percentile** `g(q)` | 2 | Doc 06 | `q_i` computed within an age×sex×condition stratum; raises risk for adverse outliers |
| **Personal baseline** `z_i` | 2b | Doc 05 §5.1 | Today-vs-your-normal responsiveness; powers Trajectory/Early-warning |
| **Clinical scores** | 2 (max) | Doc 10 | Validated ASCVD/FINDRISC/KDIGO/FIB-4/FRAX raise `m_k^cohort` / `w_i` — never lower `r_i`, never clear a critical |
| **Goal reweight** `m_k^goal` | 5 | Doc 09/11 | Raises pillars tied to stated goals; auto-raised when a pillar goes red |
| **Acute event** `m_k^acute` | 5 | Doc 09 | Spikes weights during acute episodes; reverts with hysteresis on recovery |

The nudge engine (Doc 11 §2.5) reads the *same* five stages backwards as an exact finite-difference
`∂PureScore/∂x_i`; because `γ=3` and `δ=2` make worst-case gradients largest, "fix the binding
constraint" naturally ranks as the highest-leverage action.

### 5.2 Aggregate risk (worst-sensitive again)
```
            ⎛  Σ_k W_k · R_k^δ  ⎞^{1/δ}
 R_total =  ⎜ ───────────────── ⎟          δ = 2 (default)
            ⎝     Σ_k W_k        ⎠

 PureScore° = 100 · (1 − R_total)
```

### 5.3 Critical cascade (the safety cap)
A critical pillar must pull the headline number into the critical zone — it cannot be averaged
away by healthy pillars:

```
 if  ∃ k with status_k == critical:
       PureScore = min( PureScore° , PURE_CRIT_CAP )          # PURE_CRIT_CAP = 40
       overall_status = CRITICAL
       if any critical marker has escalation == emergency:  trigger emergency pathway (Doc 16)
 else:
       PureScore = PureScore°
       overall_status = (R_total ≥ 0.30 ? AT_RISK : ON_TRACK)
```

This guarantees: *a single life-threatening biomarker ⇒ its pillar critical ⇒ PureScore in the
critical band with escalation* — the explicit requirement.

### 5.4 Worked micro-example (critical cascade)
A 58-y-old man, otherwise green pillars, presents K⁺ = 6.3 mmol/L (REN critical-marker red,
`r ≈ 0.7`, escalation=emergency):
- REN: `R_REN^mark` jumps via `γ=3`; status=critical ⇒ `R_REN ← 0.60`.
- Even with 11 green pillars, `R_total` rises, but more importantly the **cascade** sets
  `PureScore ≤ 40`, `overall_status = CRITICAL`, and fires the emergency pathway. The man is not
  told "your PureScore is 88, you're great." ✔

### 5.5 Worked micro-example (Stage-2b responsiveness, non-critical)
The same loop running on a *green* marker — showing the number move *before any band is crossed*.
Take resting HR (`rhr`, SLP/CV, **not** a critical marker), optimum `m = 55 bpm`, yellow cut
`c_y = 70`, with a personal baseline `μ^p = 58, σ^p = 4`:

- **Today: rhr = 62.** Stage 1: `t = 62−55 = 7`, `a_y = 70−55 = 15` ⇒ `r^clin = 0.15·7/15 = 0.070`
  (mid-green). Stage 2 cohort blend leaves it (`r^cohort ≈ 0`), so safety `s = 0.070`.
- **A recovered week pulls rhr to 54** (`z = sign_adverse·(54−58)/4 = −1.0`, better than your normal):
  `r^pers = κ_resp·tanh(−0.5) = 0.10·(−0.462) = −0.046`. `band_clamp(0.070 − 0.046, s=0.070)` stays
  in green ⇒ `r ≈ 0.024`. The marker risk *fell*, `S_SLP`/`S_CV` tick **up**, Trajectory shows ↑ —
  all while the marker never left green.
- **Honest ceiling.** Even a huge `z` saturates at `κ_resp = 0.10` and `band_clamp` blocks any
  band flip: responsiveness is real but bounded, so short-term effort can never fake-clear a red.

---

## 6. Determinism, smoothness, and stability
- The pipeline is **deterministic** given inputs; identical inputs ⇒ identical score (auditable).
- All maps are continuous except the intentional **critical** step functions (safety cliffs);
  these are *designed* discontinuities and are surfaced in the explanation ("crossed critical
  threshold for K⁺").
- **Anti-flap:** status transitions (esp. critical→non-critical) use hysteresis and require either
  a confirming measurement or sustained reservoir drainage (Doc 04 λ), so the headline doesn't
  oscillate on noise. Acute-mode entry/exit hysteresis is in Doc 09.

### 6.1 Responsiveness & the member feedback loop
The score is engineered to **move with behaviour** so the member gets feedback, while staying
clinically honest:
1. **Every modifiable action has a non-zero, continuous `ΔPureScore`.** Because Stages 1–2b are
   smooth, the exact finite-difference recompute the nudge engine uses (Doc 11 §2.5) returns a real
   gradient — not a lookup, not zero-until-a-threshold. The **top-5 actions are selected to each
   carry a strictly positive expected `ΔPureScore@h`** (Doc 11 §3): doing them moves the number up.
2. **Negative behaviour trends down.** A missed-sleep streak, a sedentary week, rising stress, or a
   regressing wearable metric pushes `z_i` adverse → `r_i` up → PureScore down, and shows as
   **↓ Trajectory** on the affected pillars and, if it accelerates, an **Early-warning** flag
   (Doc 05 §4–§5) — before any band is crossed.
3. **Honest ceiling.** Responsiveness is bounded by `κ_resp` and `band_clamp`: fixed/irreversible burden
   (genetics, age, established disease) does **not** fake-improve from short-term effort — it shows
   as low **Modifiability** (Doc 05 §4) so the engine never sells false hope (D5/D16).
4. **The loop:** measure → personal-baseline `z` (2b) → continuous score + companion trends →
   top-5 easiest positive-`Δ` actions (Doc 11) → member acts → next measurement moves `z` → score
   and arrows update. The interactive demonstration of this loop is the `tech/` feedback-loop page.

### 6.2 Prose ↔ engine reconciliation (this spec **is** the shared scorer)

The math above is **JSON-canonical** (D32): `data/calc-graph.json` + `pillar-weights.json` +
`constants.json` resolve into `assets/calc-data.js`, consumed by the single shared scorer
`assets/engine.js`. An earlier engine had drifted from this spec; it was reconciled so the live
**Calculation Explorer**, **feedback-loop** demo and this document compute the same thing:

| Stage | This doc | Engine (`engine.js`) |
|-------|----------|----------------------|
| 1 | continuous four-branch curve | `markerR` → `contRisk` (continuous; replaced the old 3-value quantiser) |
| 2 | `max(r_clin, φ·r_cohort)` | applied in `markerEval` (raise-only) |
| 2b | `+κ·tanh(z/2)`, `band_clamp` | implemented; on a *static* profile `z=0` (no history) so the term is **inert on a snapshot** but live in the temporal feedback-loop demo |
| 3 | `γ`-power-mean + `ρ·B̃_k` | `Math.pow(Σŵr^γ/Σŵ, 1/γ)` (replaced the old arithmetic mean); empty-pillar rule excludes no-data pillars |
| 4 | critical-marker red ⇒ floor | `rclin ≥ 0.50` fires; `R_k ← R_crit` |
| 5 | `δ`-power-mean + cap 40 | unchanged (already matched); no-data pillars dropped from `ΣW` |

Zone cuts are now applied consistently: **markers** `0.15 / 0.50` (`markerZone`), **pillars**
`0.30 / 0.60` (`pillarZone`). `γ` and `κ_resp` are exported into `calc-data.js` so the explorer's
constants panel and the scorer share one source.

## 7. Explainability output (every score ships with this)
For any score the engine emits:
1. PureScore, overall_status, and the **binding constraint** (which pillar/marker capped it).
2. Per-pillar `S_k`, `status_k`, `cov_k`, and the **top contributors** (largest `ŵ_i·r_i`).
3. Reservoir contributions `ρ_k·B̃_k` ("chronic sleep debt is adding 6 pts of risk to CV").
4. The personalization weights `W_k` and *why* they were set (cohort/goal/acute).
5. Confidence/coverage caveats and any literature-fallback markers.

This object is the substrate for the nudge engine (Doc 11) and the clinician view (Doc 10).

## 8. Default constants (all tunable, all versioned)
| Symbol | Meaning | Default |
|--------|---------|---------|
| `φ` | cohort-blend weight (Stage 2) | 0.60 |
| `κ_resp` | personal-baseline responsiveness cap (Stage 2b) | 0.10 |
| `r_i^green` | within-green optimum-centering gradient cap (the green branch of Stage 1) | <0.15 |
| `γ` | pillar power-mean exponent | 3 |
| `δ` | PureScore power-mean exponent | 2 |
| `ρ_k` | reservoir contribution cap to pillar | 0.20 |
| `R_crit` | critical pillar risk floor | 0.60 |
| `PURE_CRIT_CAP` | PureScore cap when any pillar critical | 40 |
| marker zone cuts | green/yellow/red on `r` | 0.15 / 0.50 |
| pillar zone cuts | green/yellow/red on `R_k` | 0.30 / 0.60 |

Constants are stored in a versioned config; any change is a model-version bump requiring
re-validation (Doc 13) and is recorded in the audit trail (Doc 16).

> ⚠️ **Calibration status — read before trusting any number.** Every constant on this page
> (`φ, κ_resp, γ, δ, ρ_k, R_crit, PURE_CRIT_CAP`, the pillar weights `W_k`, and the reservoir
> `λ_j / κ_jl / u_{kj}`) is an **expert-prior design default** anchored in clinical heuristic and
> literature precedent — **not** fitted to outcome data. *No guideline assigns these values.* The
> calibration plan is to fit them to all-cause / CV mortality and competing risks on the local
> cohort, with ethnicity/region strata, via the cohort percentiles (Doc 13) and the
> validation & calibration harness (Doc 14) **before any clinical use** (Doc 17 §K). Until then the
> formula is *structurally* sound (safety dominance, worst-case sensitivity, bounded responsiveness)
> but **numerically illustrative**. Locked decisions behind this math: **D6** (baseline shrinkage),
> **D13** (log-scale heavy-tailed), **D14** (critical-value confirmation), **D23** (continuous
> personalized scoring), **D32** (JSON-canonical engine), **D33** (cold-start imputation).
