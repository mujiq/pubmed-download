# 03 — The Core Scoring Formula

> Binding conventions: `README.md §3`. This document defines the full deterministic computation
> **marker → pillar → PureScore**, including the critical cascade. The dynamic reservoir terms
> that feed `r_i` and `R_k` are defined in Doc 04 and referenced here as `B_j`.

The pipeline has five stages:

```
 raw value x_i ─▶ (1) marker risk r_i ─▶ (2) cohort blend ─▶ (3) pillar risk R_k
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

A small **optimum-centering** refinement places the true best value at the band centre `m_i`
(e.g., HDL keeps improving above the lower edge), so within-green we set
`r_i^clin = 0` only within a tolerance of `m_i` and allow a tiny gradient toward band edges; this
lets the nudge engine still reward moving toward the centre of green. (Tunable; off by default.)

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
| `γ` | pillar power-mean exponent | 3 |
| `δ` | PureScore power-mean exponent | 2 |
| `ρ_k` | reservoir contribution cap to pillar | 0.20 |
| `R_crit` | critical pillar risk floor | 0.60 |
| `PURE_CRIT_CAP` | PureScore cap when any pillar critical | 40 |
| zone cuts | green/yellow/red on `r` | 0.15 / 0.50 |

Constants are stored in a versioned config; any change is a model-version bump requiring
re-validation (Doc 09) and is recorded in the audit trail (Doc 11).
