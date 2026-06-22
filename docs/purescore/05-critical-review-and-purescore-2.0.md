# 05 — Beyond the Number: Context, Companion Vector & Early Warning

> Binding conventions: `README.md §3`. This document (a) critically reviews where the v0.1
> formula (Docs 03–04) misbehaves across the age × sex × lifestyle × persona × disease ×
> medication space, and (b) specifies **PureScore 2.0** — a context-aware score shipped with a
> **companion meta-dimension vector** and a **personal-baseline early-warning layer**. It refines
> Docs 03/04; it does not replace their safety rules.

PureScore 2.0 design decisions (locked):
1. Single PureScore **+ full companion vector + early-warning** (§4–5).
2. Early-warning is **tiered Watch → Advisory → Alert** (§5.3).
3. Context-aware interpretation: **Managed state + confounder down-weighting** (§3).
4. **Hybrid age-bands + special frames + actionability modifier**; absolute anchors kept for
   acute-danger markers (§3.3).
5. **Dual framing**: absolute score + *progress to your attainable best* + competing-risk
   weighting (§6).
6. **Empirical-Bayes personal baseline** (§5.1).
7. **Defensive bias handling**: low representativeness lowers confidence, suppresses unreliable
   cohort stats, fairness mechanics (§7).

---

## 1. The core failure mode

> **The same value means opposite things in different contexts, and a level-based single number
> cannot tell them apart.**

Resting HR 50 is *elite* in an endurance athlete and *iatrogenic bradycardia* in a sedentary
elder on a β-blocker. eGFR 65 is *normal-for-age* at 85 and *early CKD* at 35. ApoB 75 is
*excellent* untreated and *barely-controlled latent risk* on a statin. v0.1 scores all of these
identically. PureScore 2.0 fixes this by interpreting every marker **through context** (meds, age,
physiologic state, personal baseline) and by shipping the number **with its own uncertainty and
direction**.

## 2. Critical edge-case catalogue (and the 2.0 behaviour)

`✗` = v0.1 misbehaviour · `✓` = 2.0 fix.

| Combination | ✗ v0.1 behaviour | ✓ PureScore 2.0 behaviour |
|---|---|---|
| **β-blocker** + wearables | RHR 50 reads "elite green"; HRV/VO₂max/HR-recovery corrupted → FIT flattered/penalized | Confounded markers **down-weighted + confidence-reduced** (§3.2); not scored as fitness |
| **Statin / treated FH**, ApoB 75 | "Green / optimal" → implies no CV problem | **MANAGED** tag (§3.1); pillar keeps disease-weight; ATH reservoir holds lifetime burden; never "cured" |
| **SGLT2i / ACE-i** | Expected benign eGFR dip + mild K⁺ rise → false REN red / **false critical cascade** | Drug-expected shifts are **anticipated** (§3.2); flagged Watch not Alert; no spurious cascade |
| **Steroids / chemo** | Glucose↑, WBC↑, BP↑ / pancytopenia → multiple false reds | Treatment-expected derangements **contextualized**; criticality tier reflects expected vs unexpected |
| **GLP-1 rapid loss** | BMI improves → BCM looks better | **Muscle/sarcopenia** (ALMI, grip) surfaced; modifiability flags lean-mass loss |
| **Athlete / muscular** | Low RHR/glucose, BMI 28 (muscle), high creatinine → false eGFR drop, BCM penalty | Athlete persona + **cystatin-C-preferred eGFR**; personal baseline; BMI deferred to body-comp |
| **"Skinny-fat" / TOFI** | Normal BMI → **false green**, missed insulin resistance | Early-warning (§5) on waist/HOMA-IR/visceral; low coverage flags "measure this" |
| **Normal pregnancy** (flag missing) | Physiologic anemia/eGFR rise/glucose shift → multi-pillar false red | Pregnancy frame (Doc 08); **but pre-eclampsia/GDM never masked** (absolute anchors retained) |
| **Multimorbid / very elderly** | Very low worst-sensitive score; non-actionable; demoralizing | **Dual framing** (§6): absolute + progress-to-attainable; competing-risk weighting mutes irrelevant pillars |
| **FH / high Lp(a)** | Correct red, but nudges imply lifestyle fixes it | **Modifiability** dimension (§4) marks fixed vs modifiable; nudges route to clinical/med, not blame |
| **Dialysis (ESRD)** | eGFR ~0 → REN permanently "critical", uninformative | **ESRD-on-RRT reference frame** (§3.3): dialysis-adequacy, not eGFR |
| **Hemolyzed K⁺ 6.5 / bad-contact SpO₂ 88** | Single unconfirmed value → instant emergency cascade → alarm fatigue | **Confirmation logic** (§3.4): plausibility + reconfirm for criticals; Watch first unless corroborated |
| **Heavy-tailed marker** (CRP, TG, ferritin) | Naive percentile misleads | **Robust/log transforms** + skew reporting (§7) |
| **Out-of-cohort ancestry / small cell** | Biased percentiles silently used | **Defensive bias handling** (§7): suppress cohort stat, lower confidence, race-free equations |

## 3. Context-aware interpretation (accuracy layer)

### 3.1 The "Managed" state
A marker is **Managed** when its value sits in green/yellow *and* an active medication class
targets it (`drug_targets(i, Mx) = true`).

- The marker keeps its low risk `r_i` (we **credit control** — we chose the managed-state model,
  not counterfactual untreated-risk scoring), but is **labelled `MANAGED`**, never `optimal`.
- The pillar retains the elevated weight from the disease flag (`m_k^cohort`, Doc 03 §5), so a
  controlled diabetic's MET pillar stays prominent.
- The relevant burden **reservoir is not zeroed** (Doc 04): atherogenic/glycemic burden persists,
  preserving lifetime-risk memory. Stopping the drug re-derangess the marker, so the score depends
  on continued control — which is the truth.
- Display: "ApoB 75 mg/dL — **Managed** (on statin). Controlled, not resolved."

### 3.2 Confounder down-weighting
Some drugs **distort** a marker without reflecting health (β-blocker → resting HR, HRV, VO₂max,
HR-recovery). A `confounds(drug, marker)` table drives:

```
 ŵ_i ← ŵ_i · (1 − c_conf)          # c_conf ∈ [0,1], e.g. 0.7 for β-blocker→HRV
 confidence_i ← confidence_i · (1 − c_conf)
```

The marker is **neither rewarded nor penalized**; the pillar leans on its other markers, and the
explanation states "HRV down-weighted: on β-blocker." Drug-**expected** transient shifts
(SGLT2i eGFR dip, ACE-i K⁺ rise) are encoded as **anticipated trajectories**: the deviation is
expected, so it routes to *Watch* (internal) rather than firing the critical cascade, unless it
exceeds the drug-expected envelope.

### 3.3 Age & physiologic frames (hybrid)
Bands become functions of age/state **only where guidelines do**, with absolute anchors retained
for acute danger:

| Marker | Age/state adaptation | Anchor retained? |
|---|---|---|
| BMI | optimal band rises in ≥65 (e.g., 23–28 protective) | — |
| Systolic BP | **J-curve**: widen optimal and add a low-side caution in frail elderly (avoid over-treatment harm) | upper red anchor kept |
| eGFR | mild age-expected decline contextualized; **ESRD-on-dialysis → dialysis-adequacy frame**, not eGFR | <30 still flagged |
| VO₂max / fitness | age- and sex-normed percentiles (already) | — |
| Pediatric | **out of v0.1 scope** — requires percentile/Tanner frames; explicitly gated, not scored on adult bands | — |
| **K⁺, SpO₂, glucose extremes, suicidality** | **never age-relaxed** | absolute |

**Actionability / expected-benefit modifier.** For the frail/elderly/competing-risk patient, a
truthful flag whose intervention has low expected benefit is **de-prioritized in nudges and in the
`m_k^acute/goal` weighting** — the flag is shown and explained, not hidden. This prevents both
alarmism and under-treatment.

### 3.4 Critical-value confirmation (anti-alarm-fatigue, pro-safety)
A single critical value runs a gate before firing the emergency cascade (Doc 03 §5.3):

```
 if r_i ≥ 0.5 and critical(i):
    if implausible(x_i) or device_low_quality or drug_expected:   → Watch + request reconfirm
    elif corroborated (a second marker, symptom, or prior trend):  → Alert / emergency cascade now
    else:                                                          → Urgent + expedited reconfirm
```

True emergencies with corroboration are **never delayed**; isolated implausible spikes get a fast
reconfirm instead of a false alarm. (Hemolyzed K⁺ → Watch+reconfirm; K⁺ 6.5 *with* peaked-T-wave
symptom report → immediate.)

**Wearable-tiered critical gate (D22, Doc 07 §2).** `device_low_quality` above is set by the
wearable trust tier: a **consumer-validated or inferential** wearable anomaly maps to
`Watch / Advisory` and **cannot reach red/critical without a clinical-grade confirmation**
(CGM / validated cuff / single-lead ECG, or a lab). Clinical-grade wearables *can* corroborate and
fire the cascade like any other marker. This keeps continuous monitoring sensitive for
early-warning without letting a consumer sensor trigger an emergency alone.

## 4. The companion meta-dimension vector

PureScore 2.0 ships the number as **"a score with direction and error bars."** Each dimension is
computed per-pillar and rolled up.

| Dim | Symbol | Definition (per pillar k, rolled up by `W_k`) | Range |
|---|---|---|---|
| **Confidence** | `Cf_k` | `cov_k · meanConf_k · stability_k` (coverage × source/recency quality × inverse volatility); `meanConf_k` carries the per-input trust flags & wearable tier (Doc 06 §2, Doc 07 §1–2) | 0–100 |
| **Data sufficiency** | `Su_k` | weighted fraction of pillar backed by **fresh patient** data; `Su_k < τ_su` ⇒ **INSUFFICIENT** flag (score provisional) | 0–100 |
| **Criticality** | `Cr` | independent badge: `(max escalation tier, count of red/critical markers)` — *not* derivable from the number | tier+count |
| **Trajectory / momentum** | `Tr_k` | robust slope of `S_k` over a window → ↑improving / →stable / ↓worsening + rate | signed |
| **Early-warning** | `Ew_k` | tiered anomaly state from §5 (none / Watch / Advisory / Alert) | categorical |
| **Representativeness** | `Rp_k` | inverse OOD distance from cohort + cell-size credibility (§7) | 0–100 |
| **Skew / robustness** | `Sk_i` | per-marker skew; drives transform choice; surfaced when heavy-tailed | descriptor |
| **Volatility** | `Vo_k` | short-window within-patient variance (damps `Cf_k` and `Tr_k`) | 0–100 |
| **Modifiability** | `Mo_k` | share of `R_k` from modifiable (lifestyle/med-responsive) vs fixed (genetic/age/irreversible) inputs | 0–100 |
| **Stress-load** | `St` | autonomic/allostatic load (higher = more loaded): `σ(a·B̄_ALLO + b·z_auto)` — `B̄_ALLO` = normalized allostatic-load reservoir (Doc 04), `z_auto` = trust-weighted personal-baseline deviation of HRV↓ / resting-HR↑ / sleep-disruption. **Read-only; no PureScore weight** (§4.1, D24) | 0–100 + tier |

**Confidence vs sufficiency are distinct:** sufficiency = *is the data present?*; confidence =
*is present data trustworthy/stable?* A pillar can be sufficient but low-confidence (noisy device)
or high-quality but insufficient (one pristine lab, everything else imputed).

**Headline example:**
> **PureScore 72 ↓** · Confidence 61% · Data 74% · Criticality: none · *2 pillars trending down* ·
> Representativeness: low (out-of-cohort) · 81% of risk modifiable

Critical-cascade headline is unchanged from Doc 03, but now always paired with the **Criticality
badge** so "40 from one emergency red" reads differently from "40 from diffuse yellows."

### 4.1 Stress-load — a companion dimension, **not** a 13th pillar (D24)

Users intuitively want a "Stress" number, so PureScore 2.0 surfaces **Stress-load** (`St`) prominently
in the companion vector — but it is **read-only and carries no weight in the score**. It models the
*physiological* stress users feel day-to-day, blended from two parts:

- **Chronic** — the normalized **ALLO allostatic/stress reservoir** `B̄_ALLO = B_ALLO / B_ALLO^max`
  (Doc 04 §2/§5), which already integrates PSS/PHQ/GAD, cortisol slope and life events over days–months.
- **Acute** — a trust-weighted **autonomic deviation** `z_auto`: personal-baseline depression of HRV,
  resting-HR elevation and sleep disruption, each down-weighted by its wearable tier (D22).

`St = 100·σ(a·B̄_ALLO + b·z_auto)`, always paired with **Confidence** so the noisy, inferential
autonomic signal is shown honestly, and reported as a tier **low / elevated / high**.

**Why stress is not taken as a pillar:**
1. **No double-count.** `St` *reads* HRV/resting-HR/cortisol/sleep markers that stay owned by
   **CV / ENDO / SLP** for scoring, and the **ALLO** reservoir already propagates stress into
   CV/MET/SLP/MCS/ENDO through the `κ` interference matrix (Doc 04). A weighted Stress pillar on top
   would count the same allostatic burden twice.
2. **Trust.** Inferred stress/readiness is **inferential/consumer-tier** — already ruled
   *informational-only, never a band* (D22). A companion score honours that; a scored pillar would not.
3. **Taxonomy.** Pillars are organ/functional domains; stress is an *upstream, cross-cutting driver*,
   best modelled as a reservoir (Doc 04) plus this companion readout.

**What `St` may do:** raise the **Early-warning** ladder (§5) to Watch/Advisory, and re-rank
stress-reducing actions in the nudge engine (Doc 11). **What it never does:** move the headline
PureScore, set or clear a band, or trigger a critical.

## 5. Early-detection layer (cross-pillar visibility)

The shift: **stop relying only on population-band levels; add personal baselines, motion, and
multivariate patterns** — because v0.1 gives *zero* signal until a marker crosses a threshold.

### 5.1 Personal baseline (empirical Bayes)
Each marker's personal mean/variance shrinks from the cohort prior toward the patient as data
accumulates:

```
 μ_i^base = (n/(n+k_i))·x̄_i^personal + (k_i/(n+k_i))·μ_i^cohort
 σ_i^base similarly (k_i larger for noisy/volatile markers)
 z_i = (x_i − μ_i^base) / σ_i^base
```

Sensitivity grows with `n`; cold-start leans on population, so no hard switch. (Consistent with
Doc 13 shrinkage.) This catches "your RHR 52→60, still green" before any band is crossed.

**This `z_i` is the engine's feedback term.** The same personal z drives **Stage 2b** of the core
score (Doc 03 §2b): better-than-baseline nudges PureScore up, worse-than-baseline trends it down —
continuously, bounded by `κ_resp` and `band_clamp` so it never relaxes a clinical anchor or clears a
critical. One signal, three surfaces: it moves **the number**, the **Trajectory** arrow (§5.2), and
the **Early-warning** ladder (§5.3) — the substrate of the patient feedback loop (Doc 11 §3.4).

### 5.2 Motion & within-green gradient
- **Trajectory**: robust slope + acceleration of each marker, pillar, and PureScore.
- **Within-green gradient** (the optimum-centering left off in Doc 03 §1, now **on**): a small
  non-zero risk gradient *inside* green toward the band edge, so drift is visible pre-threshold.
- **Leading-indicator weighting**: earliness-weighted markers — insulin/HOMA-IR before glucose;
  **UACR before eGFR**; HRV/reservoir fill before events.
- **Time-to-threshold forecast**: project trajectory → estimated days to yellow/red per pillar.

### 5.3 Multivariate anomaly + the tiered ladder
- **Mahalanobis distance** `D_M` from the personal/cohort *joint* baseline catches simultaneous
  small shifts no single marker flags (early sepsis, decompensation, metabolic-syndrome onset).
- **Syndromic detectors**: named cross-pillar patterns (metabolic syndrome, frailty phenotype,
  CKD-progression) as pattern alerts.
- **Tiered surfacing** (locked decision 2):

```
 Watch    (internal/clinician queue only): single-marker |z|>2 or rising reservoir or mild D_M
 Advisory (gentle patient nudge):          multivariate D_M high OR trend confirmed over ≥2 reads
 Alert    (clinician, may escalate):       clinical threshold crossed OR high D_M + corroboration
```

The patient is **never** shown a raw single-marker wobble; soft signals sit in Watch until
multivariate- or multi-measurement-**confirmed**, protecting against alarm fatigue while keeping
sensitivity (β-blocker/cycle/athlete normal variation stays in Watch and resolves).

## 6. Dual framing — honest *and* motivating

Worst-sensitive scoring (safe) means the headline won't move for patients with fixed burden, and
multimorbid scores are demoralizing/non-actionable. So 2.0 shows **two coordinates**:

1. **Absolute PureScore** — unchanged truth (with all context fixes above).
2. **Progress to *your* attainable best** —
```
 PureScore_max(p) = score with all MODIFIABLE inputs at optimum, FIXED inputs (genetic/age/
                    irreversible) held at their realistic value
 Progress = clamp( (PureScore − PureScore_floor) / (PureScore_max − PureScore_floor) , 0, 1 )
```
   "You're at 84% of what's achievable for you" rewards effort without lying about absolute risk.
3. **Competing-risk / prognostic weighting** down-weights pillars irrelevant to the patient's
   dominant prognosis (don't optimize omega-3 in end-stage disease), reducing non-actionable noise.

`Modifiability` (Mo, §4) is the connective tissue: it tells the patient and the nudge engine how
much of the gap is theirs to close vs fixed — and ensures un-fixable risk (FH, age) is never
framed as personal failure.

## 7. Bias, skew & representativeness (defensive)

- **Representativeness `Rp`**: OOD distance from the cohort distribution + cell-size credibility.
- **When `Rp` is low** (out-of-distribution, tiny cell, or the marker is heavy-tailed): the
  **cohort-percentile blend is suppressed** (`φ → 0`, Doc 03 §2) so the score leans on **clinical
  anchors + personal baseline**, and **Confidence is reduced** — we never lean on unreliable cohort
  statistics.
- **Fairness mechanics**: race-free eGFR/CKD-EPI (2021); **robust/log transforms** for skewed
  markers (CRP, TG, ferritin) with `Sk` surfaced; **partial pooling** for small cells (Doc 13);
  **subgroup calibration & error-rate parity** as a release gate (Doc 13 §5, Doc 16).
- No protected-class attribute or proxy may worsen a score, price, or access (README §5.5,
  Doc 19, Doc 16) — representativeness adjusts *confidence*, never *penalty*.

## 8. Is PureScore actually useful? (honest verdict)

- **v0.1 alone**: a useful organizing/triage/engagement layer, but level-based, worst-sensitive,
  and over-confident — **weak on early detection and prone to being confidently wrong** in the
  edge cases of §2.
- **PureScore 2.0**: becomes genuinely useful **iff** it ships with (a) context-aware
  interpretation (§3), (b) the companion vector so the number carries its own uncertainty and
  direction (§4), (c) the personal-baseline early-warning layer (§5), and (d) dual framing (§6).
  The single number is necessary but never sufficient — its value comes from the vector around it.
- **Still required before any real use**: prospective calibration, fairness audit, and the
  validation gates of Doc 13; clinician-in-the-loop and the governance of Doc 16. These dimensions
  are *design*, not evidence — they must be validated, not asserted (the Babylon lesson, Doc 01).

## 9. New/changed constants (versioned; calibrate per Doc 13)
| Symbol | Meaning | Default |
|---|---|---|
| `c_conf` | confounder down-weight (per drug×marker) | 0.5–0.8 |
| `τ_su` | data-sufficiency threshold for INSUFFICIENT flag | 0.50 |
| `k_i` | empirical-Bayes shrinkage constant (per marker) | marker-specific |
| `z*` | personal-baseline Watch threshold | 2.0 |
| `D_M*` | multivariate-anomaly Advisory threshold | χ²-based, per cohort |
| within-green gradient max | risk at green edge | 0.10 |
| `Rp*` | representativeness floor → suppress cohort blend | cohort-specific |

All changes are model-version bumps requiring re-validation (Doc 03 §8, Doc 13, Doc 16).

## 10. Implementation status (interactive calculator)
The `purescore-architecture.html` calculator now **computes** (not just illustrates) the 2.0 layer,
so the behaviour is inspectable against the edge-case personas:

| Capability | Status in calculator | Notes |
|---|---|---|
| Context-aware: Managed / confounder down-weighting / age & special frames | **Live** | `MANAGES`/`CONFOUNDS` tables, confidence-weighted aggregation, per-persona band frames, eGFR exclusion |
| Companion vector: Confidence, Data sufficiency, Criticality, Representativeness, Modifiability | **Live** | computed from coverage × quality × `rep`, present-data fraction, red-marker scan, modifiable-risk share |
| Trajectory, Volatility, personal-baseline z, within-green drift, time-to-threshold | **Live (synthetic)** | from a **seeded synthetic 90-day history** per marker (decision **D9**); production uses real series |
| Early-warning tiers (Watch/Advisory) | **Live** | band-ramp + filling-reservoir + polarity-aware personal-drift signals; Advisory on multivariate `D`/≥3 signals |
| Multivariate anomaly `D` | **Live (diagonal-covariance approx.)** | `D≈√Σzᵢ²` (decision **D10**); production uses full Mahalanobis |
| Dual framing: absolute + progress-to-attainable-best + competing-risk muting | **Live** | modifiable-optimum ceiling (decision **D11**); `crmute` per persona |
| Syndromic detectors (metabolic syndrome, frailty, CKD-progression) | **Live** | decision **D12** |
| Skew/robustness: log-scale baseline for heavy-tailed markers | **Live** | CRP/UACR/ALT/FIB-4/bilirubin (decision **D13**) |
| Critical-value confirmation: isolated implausible → reconfirm, not Alert | **Live** | data-quality flag, never auto-suppresses a confirmed critical (decision **D14**) |

The synthetic-history items are **demonstrations of the method**, not evidence; production requires
real longitudinal data and the validation gates of Doc 13. Every decision behind this design is
recorded in [`decisions.md`](./decisions.md) (see the Decision log).
