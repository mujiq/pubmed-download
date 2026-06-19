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

Each marker has guideline bands (Doc 02). Define the **clinical risk** `r_i^clin ∈ [0,1]` as a
continuous, monotone, two-sided penalty so the score is smooth (no cliff at a threshold) but
anchored to the guideline cut-points.

Let the optimal band be `[L, U]`, the yellow outer edges `[L_y, U_y]`, and the red outer edges
`[L_r, U_r]` (for one-sided markers the unused side is `−∞`/`+∞`). Define signed distance into
the adverse region, normalized by band width. For a value `x` above optimum:

```
            ⎧ 0,                                          L ≤ x ≤ U        (green core)
            ⎪ 0.15 · (x − U)/(U_y − U),                   U < x ≤ U_y      (green→yellow)
 r_i^clin = ⎨ 0.15 + 0.35 · (x − U_y)/(U_r − U_y),        U_y < x ≤ U_r    (yellow→red edge)
            ⎪ 0.50 + 0.50 · (1 − e^{−(x − U_r)/U_r}),     x > U_r          (red, saturating →1)
            ⎩ (mirror of the above for x < L on the low side)
```

Properties: `r=0` across the optimal core; `r=0.15` exactly at the yellow boundary; `r=0.5`
exactly at the red boundary; saturates toward `1` deep in the red so a single extreme value
dominates but cannot make `r>1`. The colour zones in README §3.5 fall out directly
(`<0.15` green, `<0.5` yellow, `≥0.5` red).

**Continuity is a hard requirement, not a side-effect.** Every map in Stage 1 is `C⁰`-continuous
(and `C¹` except at the designed critical step, §6): an arbitrarily small change in `x_i` yields an
arbitrarily small change in `r_i`. There are **no discrete band jumps in the number** — the
green/yellow/red bands are *labels read off the continuous curve* (`r<0.15`/`<0.5`/`≥0.5`), never
the generator of the score. This is what lets a single day's action register as a real, visible
score change (§6.1; Doc 07; Doc 12).

**Within-band optimum-centering is ON by default.** The true best value sits at the band centre
`m_i` (e.g., HDL keeps improving above the lower edge), so even *inside green* there is a gentle
continuous gradient `r_i^green ∈ [0, 0.15)` toward `m_i`. A patient already in the green band still
sees the score rise as they move toward their optimum and dip if they drift toward the edge — the
score is never "flat until you cross a line." Magnitude is small and capped below the yellow cut so
it can never, by itself, change a band or a status.

---

## 2. Stage 2 — Blend with cohort percentile (safety-dominant)

The cohort percentile (Doc 01 §4.2) personalizes the score and powers stack-ranking, but it may
only **raise** concern, never lower it below the clinical anchor:

```
 r_i^cohort = g(q_i)        # maps an adverse percentile to risk; g(0.5)=0, rises toward tails
 r_i        = max( r_i^clin ,  φ · r_i^cohort )           # φ = 0.6 default  (Doc 01 §4.4)
```

- `g(q)` is `0` near the healthy side and rises as the value becomes unusually adverse *for the
  cohort* (e.g., a "normal" value sitting at the cohort's adverse 5th percentile earns some risk).
- For a **healthy** reference cohort, `g` flags being an outlier on the bad side even while still
  technically in-range — an early-warning property.
- For a **diseased** cohort, the `max(...)` guarantees the clinical anchor cannot be diluted by
  looking "good for a sick group" (the HbA1c-7.5%-in-diabetics example, Doc 01 §4.4).

**Stack-ranking (for display and the actuarial layer)** uses `q_i` and pillar percentiles
directly; it is presentation/analytics, not a relaxation of `r_i`.

---

## 2b. Stage 2b — Personal-baseline responsiveness (the feedback term)

Stage 2 personalizes *across people*; Stage 2b personalizes *across time for one person*, so the
score visibly tracks short-term action. Each marker carries a robust **personal baseline**
`μ_i^p, σ_i^p` (empirical-Bayes, log-scaled for heavy-tailed markers; cold-start shrinks to cohort
— Doc 12 §5.1, Doc 14 ignition), giving a **personal z-score** oriented so *adverse = positive*:

```
 z_i = sign_adverse · (x_i − μ_i^p) / σ_i^p
```

A bounded, smooth gradient turns recent personal movement into a small score change:

```
 r_i^pers = κ_resp · tanh( z_i / 2 )                       # κ_resp = 0.10 cap; continuous, monotone
 r_i      = band_clamp(  max(r_i^clin, φ·r_i^cohort) + r_i^pers · 𝟙[non-critical]  )
```

- **`band_clamp`** keeps the personalized term *within the current non-critical band*: Stage 2b can
  move the number continuously, but it can never relax a red, clear a critical, or by itself flip a
  band. Safety (clinical anchor + cohort `max`) always dominates (README §5.2).
- **Direction = feedback.** Better-than-your-baseline (`z_i<0`) lowers `r_i` a little → PureScore
  ticks **up**; worse-than-baseline (`z_i>0`) raises it → PureScore ticks **down**. A good night's
  sleep, a 9 000-step day, an in-range glucose curve, a calmer week all move the number *today*,
  before any band is crossed.
- **Cadence.** `μ_i^p, σ_i^p` and `z_i` update on each new reading: daily for wearables/behaviours
  (sleep, steps, HRV, resting HR, CGM, stress check-ins), per-measurement for labs. The fast
  channels are exactly the modifiable ones the nudge engine acts on (Doc 07), closing the loop.
- Stage 2b is also the substrate for the **Trajectory/momentum** and **Early-warning** companion
  dimensions (Doc 12 §4–§5): the same `z_i` stream that nudges the number drives the arrows.

---

## 3. Stage 3 — Pillar risk (worst-sensitive aggregation, confidence- and reservoir-aware)

A pillar must not let many mediocre markers average away one dangerous one. We use a **weighted
power mean with exponent γ > 1** (a soft-max over risks), with weights scaled by each marker's
confidence, plus an additive **reservoir term** carrying chronic burden (Doc 04).

```
 ŵ_i = w_i · confidence_i           # low-confidence/imputed markers contribute less (Doc 01 §2)

           ⎛  Σ_{i∈k} ŵ_i · r_i^γ  ⎞^{1/γ}
 R_k^mark = ⎜ ───────────────────── ⎟           γ = 3 (default; higher ⇒ more worst-case-sensitive)
           ⎝     Σ_{i∈k} ŵ_i        ⎠

 R_k = clamp_{[0,1]}(  R_k^mark  +  ρ_k · B̃_k  )      # B̃_k = normalized reservoir load for pillar k
```

- `γ = 3` makes the worst one or two markers dominate while still reflecting breadth.
- `B̃_k ∈ [0,1]` is the aggregate of the reservoirs linked to pillar `k` (Doc 04 §5), e.g. chronic
  sleep debt raises SLP, MCS, CV, MET even when their spot markers look acceptable. `ρ_k` (≈0.2)
  bounds how much chronic burden can move a pillar on top of its current markers.
- **Coverage** `cov_k = Σ ŵ_i over observed / Σ w_i over all` is reported alongside `R_k`. Low
  coverage caps how *green* a pillar may be displayed (a pillar that is green only via imputed
  medians is shown as *low-coverage green*, Doc 01 §4.3).

`S_k = 100 · (1 − R_k)`.

---

## 4. Stage 4 — Pillar status and the critical override

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
(Doc 11); reserve-deficit reds drive **priority improvement** plans (Doc 06) and high-leverage
nudges (Doc 07). The marker catalogue tags each critical marker with `escalation ∈ {emergency,
urgent, routine}`.

---

## 5. Stage 5 — PureScore (personalized weights + critical cascade)

### 5.1 Personalized pillar weights
```
 W_k = W_k^base · m_k^cohort · m_k^goal · m_k^acute ,   then normalized so Σ_k W_k = 1
```
- `W_k^base`: default importance (longevity/all-cause-mortality contribution; CV, MET, FIT, MCS,
  SLP carry the largest defaults).
- `m_k^cohort`: raises pillars central to the patient's conditions/meds (diabetic ⇒ MET, REN, CV
  up; CKD ⇒ REN up; on statin ⇒ CV interpretation adjusts).
- `m_k^goal`: raises pillars tied to the patient's stated goals (Doc 06/07).
- `m_k^acute`: spikes during an acute event (Doc 06); reverts with hysteresis on recovery.

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
       if any critical marker has escalation == emergency:  trigger emergency pathway (Doc 11)
 else:
       PureScore = PureScore°
       overall_status = (R_total ≥ 0.30 ? AT_RISK : ON_TRACK)
```

This guarantees: *a single life-threatening biomarker ⇒ its pillar critical ⇒ PureScore in the
critical band with escalation* — the explicit requirement.

### 5.4 Worked micro-example
A 58-y-old man, otherwise green pillars, presents K⁺ = 6.3 mmol/L (REN critical-marker red,
`r ≈ 0.7`, escalation=emergency):
- REN: `R_REN^mark` jumps via `γ=3`; status=critical ⇒ `R_REN ← 0.60`.
- Even with 11 green pillars, `R_total` rises, but more importantly the **cascade** sets
  `PureScore ≤ 40`, `overall_status = CRITICAL`, and fires the emergency pathway. The man is not
  told "your PureScore is 88, you're great." ✔

---

## 6. Determinism, smoothness, and stability
- The pipeline is **deterministic** given inputs; identical inputs ⇒ identical score (auditable).
- All maps are continuous except the intentional **critical** step functions (safety cliffs);
  these are *designed* discontinuities and are surfaced in the explanation ("crossed critical
  threshold for K⁺").
- **Anti-flap:** status transitions (esp. critical→non-critical) use hysteresis and require either
  a confirming measurement or sustained reservoir drainage (Doc 04 λ), so the headline doesn't
  oscillate on noise. Acute-mode entry/exit hysteresis is in Doc 06.

### 6.1 Responsiveness & the patient feedback loop
The score is engineered to **move with behaviour** so the patient gets feedback, while staying
clinically honest:
1. **Every modifiable action has a non-zero, continuous `ΔPureScore`.** Because Stages 1–2b are
   smooth, the exact finite-difference recompute the nudge engine uses (Doc 07 §2.5) returns a real
   gradient — not a lookup, not zero-until-a-threshold. The **top-5 actions are selected to each
   carry a strictly positive expected `ΔPureScore@h`** (Doc 07 §3): doing them moves the number up.
2. **Negative behaviour trends down.** A missed-sleep streak, a sedentary week, rising stress, or a
   regressing wearable metric pushes `z_i` adverse → `r_i` up → PureScore down, and shows as
   **↓ Trajectory** on the affected pillars and, if it accelerates, an **Early-warning** flag
   (Doc 12 §4–§5) — before any band is crossed.
3. **Honest ceiling.** Responsiveness is bounded by `κ_resp` and `band_clamp`: fixed/irreversible burden
   (genetics, age, established disease) does **not** fake-improve from short-term effort — it shows
   as low **Modifiability** (Doc 12 §4) so the engine never sells false hope (D5/D16).
4. **The loop:** measure → personal-baseline `z` (2b) → continuous score + companion trends →
   top-5 easiest positive-`Δ` actions (Doc 07) → patient acts → next measurement moves `z` → score
   and arrows update. The interactive demonstration of this loop is the `tech/` feedback-loop page.

## 7. Explainability output (every score ships with this)
For any score the engine emits:
1. PureScore, overall_status, and the **binding constraint** (which pillar/marker capped it).
2. Per-pillar `S_k`, `status_k`, `cov_k`, and the **top contributors** (largest `ŵ_i·r_i`).
3. Reservoir contributions `ρ_k·B̃_k` ("chronic sleep debt is adding 6 pts of risk to CV").
4. The personalization weights `W_k` and *why* they were set (cohort/goal/acute).
5. Confidence/coverage caveats and any literature-fallback markers.

This object is the substrate for the nudge engine (Doc 07) and the clinician view (Doc 08).

## 8. Default constants (all tunable, all versioned)
| Symbol | Meaning | Default |
|--------|---------|---------|
| `φ` | cohort-blend weight (Stage 2) | 0.60 |
| `κ_resp` | personal-baseline responsiveness cap (Stage 2b) | 0.10 |
| `r_i^green` | within-green optimum-centering gradient cap | <0.15 |
| `γ` | pillar power-mean exponent | 3 |
| `δ` | PureScore power-mean exponent | 2 |
| `ρ_k` | reservoir contribution cap to pillar | 0.20 |
| `R_crit` | critical pillar risk floor | 0.60 |
| `PURE_CRIT_CAP` | PureScore cap when any pillar critical | 40 |
| zone cuts | green/yellow/red on `r` | 0.15 / 0.50 |

Constants are stored in a versioned config; any change is a model-version bump requiring
re-validation (Doc 09) and is recorded in the audit trail (Doc 11).
