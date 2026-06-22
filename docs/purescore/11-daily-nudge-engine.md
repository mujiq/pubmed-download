# 11 — The Daily Nudge Engine & Delivery (Top-5 Easiest Actions, Impact-Attributed)

**Care delivery:** the preventive reminders, intervention catalogue and alerts of the care pathways ([Prevention & engagement](prevention-engagement.html)) route through this delivery engine (§9) — one sender, not a parallel one.

> Binding conventions: `README.md §3`. This document defines how PureScore converts the
> explainability object (Doc 03 §7) and the reservoir dynamics (Doc 04) into a **daily action
> plan**: the **top-5 easiest** actions for this patient today, each carrying an honest,
> attributed **ΔPureScore** trickled down to pillars `S_k` and individual markers `s_i`.
>
> It consumes: the scored state (Doc 03), reservoirs `B_j(t)` and their `λ_j, κ, u_{kj}` (Doc 04),
> the marker catalogue and critical/escalation tags (Doc 02), acute-mode weights `m_k^acute`
> (Doc 09). It feeds: care plans (Doc 09), validation/recalibration (Doc 13), governance/escalation
> (Doc 16). It is bound by the anti-Babylon principle (Doc 01 §3.1): **no overpromising, no dark
> patterns, no triage-to-reassurance.**
>
> **§9 (delivery)** specifies how the selected action reaches the patient — channels, send-time,
> quiet hours, frequency caps, consent and PHI-safe payloads — and how **safety-critical** alerts
> (Doc 16) are delivered with guaranteed escalation, firewalled from best-effort engagement.

The engine is a four-stage pipeline run on the daily clock `Δ = 1 day`:

```
 scored state + reservoirs ─▶ (1) candidate actions A_p (library, filtered by safety/contraindication)
                            ─▶ (2) impact ΔPureScore_a(h)  via sensitivity through scoring + dynamics
                            ─▶ (3) utility U_a (ease-weighted) + diversity + safety  ─▶ rank
                            ─▶ (4) present top-5 nudge card  ─▶ (6) measure realized Δ ─▶ recalibrate
```

---

## 1. The action library `A`

`A` is a versioned catalogue of **candidate actions**. Each action `a` is a typed record. It is
not a free-text suggestion: it is a structured object whose fields let stages 2–5 compute, filter,
and rank deterministically.

### 1.1 Action schema

| Field | Symbol | Meaning |
|-------|--------|---------|
| id, class | — | stable id; `class_a ∈ {SLEEP, NUTR, ACT, STRESS, ADHERE, MEASURE, CLINICAL}` |
| valve target | `valve_a` | which Doc 04 valve(s) it turns: an inflow/decay term of reservoir `j`, or a direct marker `i` |
| effect vector | `e_a` | nominal per-unit effect on inflows/markers (signed; see §2) |
| effort | `E_a ∈ [0,1]` | normalized effort/burden (0 = trivial, 1 = very hard) |
| latency | `τ_a` | time-to-effect class (immediate / days / weeks / months) — must match `λ_j` of its target |
| evidence | `ε_a ∈ [0,1]` | evidence strength (RCT/meta-analysis = high; mechanistic/observational = low) |
| dose | `d_a` | prescribed dose/frequency (e.g., "+1500 steps/day", "+20 g fiber") |
| safety tags | `safe_a` | contraindication keys (cohort `D`, meds `Mx`, acute flags) it must be screened against |
| cohort tags | `cohort_a` | life-stage / sex / age-band applicability (Doc 08/09) |
| horizon shape | `g_a(h)` | the fraction of full effect realized by horizon `h` (§2.3) |

`tier`-style economy applies: **`MEASURE` and `CLINICAL` actions** are first-class — "measure the
marker that is currently imputed and dominating your uncertainty" is a legitimate, often
highest-yield nudge (Doc 01 §2.5 cheapest-high-yield-first; Kaiser care-gap closure, Doc 01 §3.2).

### 1.2 Library excerpt (illustrative, literature-anchored — re-verify per README §5.6)

| id | class | dose `d_a` | valve / target | moves | `E_a` | `τ_a` | `ε_a` | contraindication `safe_a` |
|----|-------|-----------|----------------|-------|------:|-------|------:|---------------------------|
| SLP-WIND | SLEEP | fixed wind-down + 30 min earlier lights-out | drain `B_SLD` (sleep-hygiene) | SLP, →MCS,MET,CV | 0.25 | days | 0.7 | none |
| SLP-REG | SLEEP | constant sleep/wake time ±30 min | `B_SLD` regularity inflow ↓ | SLP, →ALLO | 0.35 | days–wk | 0.6 | shift-worker (re-tag) |
| ACT-STEPS | ACT | +1500 steps/day | fill `B_CRF`; ↓`B_GLY` (κ) | FIT, MET, CV | 0.20 | weeks | 0.8 | unstable angina, acute MSK injury |
| ACT-Z2 | ACT | 2× zone-2 cardio 30 min/wk | fill `B_CRF` strongly | FIT, CV, MET | 0.55 | weeks–mo | 0.8 | acute cardiac event (Doc 09) |
| ACT-RES | ACT | 2× resistance 20 min/wk | fill `B_MUS`, `B_BON` | BCM, FIT, MET | 0.55 | weeks–mo | 0.7 | severe osteoporosis → supervised |
| NUT-FIBER | NUTR | +10 g fiber/day | ↓`B_GLY` inflow, diet-quality `s_i`↑ | NUT, MET, →INFL | 0.30 | weeks | 0.7 | active flare IBD/stricture |
| NUT-O3 | NUTR | omega-3 to index ≥8 | ↓`B_INFL` inflow; omega-3 `s_i`↑ | NUT, INF, →CV | 0.20 | weeks–mo | 0.6 | anticoagulant (bleeding) |
| NUT-NAK | NUTR | reduce added sugar/refined carb | ↓`B_GLY`,`B_HEPF` inflow | MET, HEP, →CV | 0.40 | weeks | 0.7 | none |
| NUT-PROT | NUTR | protein to 1.2 g/kg | fill `B_MUS` | BCM, MET | 0.35 | weeks | 0.6 | **CKD stage ≥3b: capped/blocked** |
| STR-BREATH | STRESS | 5-min daily breathing / mindfulness | drain `B_ALLO` | MCS, →SLP,CV | 0.15 | days–wk | 0.5 | active psychosis → clinical |
| STR-CHECK | STRESS | daily 1-tap mood/stress check-in | observe `B_ALLO`; raises coverage | MCS | 0.05 | immediate | n/a | suicidality item → escalate |
| MED-STATIN | ADHERE | take prescribed statin daily | ↓ApoB `s_i` ⇒ ↓`B_ATH` inflow | CV | 0.10 | weeks (ApoB), years (`B_ATH`) | 0.9 | prescribed only |
| MED-BP | ADHERE | take prescribed antihypertensive | ↓SBP `s_i` ⇒ ↓vascular load | CV, REN | 0.10 | days–wk | 0.9 | prescribed only; hypotension watch |
| MSR-APOB | MEASURE | order ApoB (currently imputed) | ↑coverage `cov_CV`; resolves `r_i` | CV (uncertainty) | 0.30 | immediate | n/a | none |
| MSR-VITD | MEASURE | order 25-OH vit D | ↑coverage `cov_NUT` | NUT | 0.30 | immediate | n/a | none |
| CLN-FU | CLINICAL | book overdue clinician follow-up | care-gap close; gate red markers | (pillar of gap) | 0.45 | immediate | n/a | red-flag ⇒ urgent route |

This is an excerpt; the production library is cohort-partitioned (Doc 09 care-plan templates supply
the per-cohort action sets and doses).

---

## 2. Impact estimation — sensitivity through scoring **and** reservoir dynamics

The honest impact of an action is **not** a lookup. PureScore is a composition

```
 PureScore = 100·(1 − R_total(  {R_k(  {r_i},  B̃_k({B_j}) )},  {W_k} ))
```

so an action that turns a valve propagates along two coupled paths:

- **Fast / marker path:** `valve_a → r_i → R_k^mark → R_total → PureScore` (Doc 03 §1–§5).
- **Slow / reservoir path:** `valve_a → inflow_j/λ_j → B_j(t) over horizon → B̃_k → R_k → PureScore`
  (Doc 04 §3,§5). Some tanks drain in days (`B_SLD`), some barely at all (`B_ATH`, CAC), so impact
  **must be quoted at a horizon `h`** (default report set: 7 / 30 / 90 days).

We therefore define expected impact as a **horizon-indexed total derivative**.

### 2.1 The chain rule (marker path, instantaneous)

For an action moving marker `i` by `Δx_i = d_a · e_{a,i}` (its dose times per-unit effect), the
first-order PureScore sensitivity is the product of the local gradients along the binding chain:

```
 ∂PureScore        ∂PureScore   ∂R_total    ∂R_k     ∂r_i
 ──────────  =  100·──────────·─────────·────────·──────
   ∂x_i             ∂R_total    ∂R_k       ∂r_i      ∂x_i
                     └──(a)──┘  └──(b)──┘  └──(c)──┘ └─(d)─┘
```

with each factor read straight off Docs 02–03:

- **(a)** `∂PureScore/∂R_total = −100`.
- **(b)** from the δ-power-mean (Doc 03 §5.2):
  `∂R_total/∂R_k = (W_k R_k^{δ−1}) / (Σ_l W_l R_l^δ)^{(δ−1)/δ} · (1/Σ_l W_l)`.
  *This is why the same marker change is worth more PureScore points in a high-`W_k`, high-`R_k`
  pillar* — worst-sensitive aggregation makes improving the dominant pillar pay more.
- **(c)** from the γ-power-mean (Doc 03 §3):
  `∂R_k/∂r_i = (ŵ_i r_i^{γ−1}) / (Σ_l ŵ_l r_l^γ)^{(γ−1)/γ} · (1/Σ_l ŵ_l)`.
  *The marker that is currently the worst in its pillar has the largest gradient* — fixing the
  binding marker pays most, exactly the explainability "top contributor" (Doc 03 §7.2).
- **(d)** `∂r_i/∂x_i` is the slope of the piecewise band map (Doc 03 §1). It is **zero inside the
  green core** (already optimal ⇒ no marker-path credit, only reservoir-path credit) and steepest
  on the yellow→red edge. Two-sided markers use the active side.

The marker-path action effect is then `Δ_mark PureScore_a = Σ_i (∂PureScore/∂x_i)·Δx_i`, summed
over every marker the action touches, with a second-order guard (§2.5) for large moves.

### 2.2 The reservoir path (horizon `h`)

A behavioural valve usually does **not** move a marker today; it changes a reservoir's inflow or
drain, and the tank moves over `h` per the Doc 04 §3 state equation. Linearize the discrete
dynamics around the patient's current reservoir vector `B(t)`:

```
 B(t+Δ) ≈ B(t) + Δ·[ f(B(t)) + valve_a ],     J = ∂f/∂B = (−Λ + K)      # Doc 04 Hurwitz Jacobian
```

where `Λ = diag(λ_j)`, `K` is the (saturated-linearized) interference matrix `κ`. A sustained
valve `v_a` (a constant change to inflow/drain for the relevant tanks) produces, at horizon `h`,
a reservoir displacement

```
 ΔB(h) = ( J^{-1} ( e^{J h} − I ) ) · v_a            # response of a linear system to a step input
```

Read this honestly: for a **fast tank** (large `−λ_j`, e.g. `B_SLD`) `ΔB_j(h)` saturates within
days; for a **near-permanent burden** (`B_ATH`, `λ→0`) `ΔB_j(h) ≈ −v_a·h` grows only linearly and
the 90-day number is small even for a perfectly-adhered statin — the model refuses false hope
(Doc 04 §4.4). The cross-terms in `e^{Jh}` are exactly the **interference contagion**: a sleep
action also drains `B_INFL`, `B_GLY` through `κ`.

The reservoir displacement maps to pillar risk through Doc 04 §5:

```
 ΔB̃_k(h) = Σ_j u_{kj} · (ΔB_j(h) / B_j^max)          (asset signs inverted: depletion = risk)
```

and then through the **same** chain-rule tail (a)+(b) plus `∂R_k/∂B̃_k = ρ_k` (Doc 03 §3):

```
 Δ_res PureScore_a(h) = Σ_k [ (a)·(b)·ρ_k ] · ΔB̃_k(h)
```

### 2.3 Total horizoned impact

```
 ΔPureScore_a(h) = ε_a · g_a(h) · [ Δ_mark PureScore_a + Δ_res PureScore_a(h) ]
```

- `g_a(h) ∈ [0,1]` is the action's realization curve (a marker that needs 6 weeks contributes
  little of its effect at `h = 7 d`); it is consistent with the target tank's `λ_j`.
- `ε_a` (evidence strength) **down-weights the claim** for weakly-evidenced actions — a
  conservative-claims tax (Doc 01 §2.7). Low evidence ⇒ smaller promised number, never a bigger one.

### 2.4 Attribution (what the card must show)

Because every term above is additive over pillars and markers, the engine emits the **same
decomposition** the explainability object uses, so the patient sees where the points come from:

```
 ΔPureScore_a(h)  =  Σ_k  ΔS-credit_k(h)        # per-pillar credit (sums to the headline Δ)
 ΔS-credit_k(h)   =  Σ_{i∈k} (marker-path credit_i) + (reservoir credit via B̃_k)
```

rendered as e.g. **"+1.8 PureScore @90d — CV +3.0 (ApoB −5 mg/dL, ↓atherogenic burden), MET +1.2
(↓glycemic burden)"**.

### 2.5 Second-order guard & honesty bounds

- For a **large** `Δx_i` (not infinitesimal) we use the exact band map, not just the local slope:
  `Δ_mark = PureScore(x+Δx) − PureScore(x)` recomputed through Docs 03 — the linearization is only
  for ranking/explanation; the **quoted** number uses the exact recomputation to avoid overstating.
- **Diminishing returns are preserved:** moving an already-green marker yields ~0 (slope 0 in green),
  so the engine never nudges "improve the thing that is already perfect."
- **No double counting:** a marker that feeds a reservoir is credited once on the fast path and once
  on the slow path *only because they act on different time-scales*; the `g_a(h)` curve and the
  reservoir state equation share the same `λ_j`, so the 90-day total equals the converged recompute.

### 2.6 Worked example — `ACT-STEPS` for a pre-diabetic 52-y-old

State: MET `R_MET=0.41` (HbA1c 6.0% yellow, dominant), FIT `R_FIT=0.38` (steps 4,800/day red-ish,
`B_CRF` depleted), CV `R_CV=0.22`. `W_MET=0.16, W_FIT=0.11, W_CV=0.15`. Action dose +1500 steps/day.

- **Marker path:** steps `s_i` moves out of red toward green; `∂r_i/∂x` steep there. Small direct
  `Δ_mark ≈ +0.3` (steps is a modest-weight FIT marker).
- **Reservoir path (90 d):** valve fills `B_CRF` (asset). `e^{Jh}` propagates: `κ_{GLY,CRF}<0` so
  `B_GLY` drains too. `ΔB̃_FIT(90)`, `ΔB̃_MET(90)`, small `ΔB̃_CV(90)` all negative (risk down).
- **Total:** `ΔPureScore_ACT-STEPS(90) ≈ +1.4`, attributed **FIT +0.7, MET +0.5, CV +0.2**.
  Quoted to the patient: *"+1.4 over 90 days — fitness, blood sugar, and heart all benefit; HbA1c
  est. −0.2%."* `ε_a=0.8`, `g_a(7d)≈0.15` ⇒ the 7-day card shows only **+0.2** — honest about latency.

---

## 3. Ranking — an **ease-weighted** utility with safety and diversity

The brief is *top-5 **easiest***. Ease/feasibility is weighted heavily; raw impact is necessary but
deliberately not sufficient (a huge-impact action the patient won't do is worthless).

### 3.1 Utility

For each safety-passing action (§5), define the daily utility

```
 U_a = [ ΔPureScore_a(h*) ]^{α} · [ p̂_a ]^{β} · (1 − E_a)^{η} · ν_a
        └── impact ──┘        └adherence┘   └── ease ──┘
```

- `h*` — the patient's planning horizon (default 30 d; goal-/cohort-set).
- `p̂_a ∈ [0,1]` — **predicted adherence probability** for *this* patient (§3.2). This is what makes
  the list "easiest *for you*."
- `(1−E_a)^{η}` — explicit ease preference; `η > 1` (default 1.5) makes the list bias toward
  low-effort actions, satisfying the brief literally.
- `α, β` default `1.0`; raising `β` and `η` over `α` is the *ease-first* posture. All versioned (§7).
- `ν_a` — novelty/timing factor: small bonus for an action that closes the binding constraint or a
  care-gap, small penalty for one shown-and-ignored many times (anti-nag; §4.4). It **also carries a
  goal-alignment bonus**: the patient's declared **GOAL stream** (Doc 07 §1, §7) sets `h*` and lifts
  actions that advance the goal (e.g. "lower HbA1c" → glycemic actions rank up), within the safety
  rails — the goal steers ranking but never overrides a critical or a contraindication (§1).

**Why multiplicative:** if either expected impact or adherence is ~0, `U_a→0`. We never surface a
high-impact action nobody will do, nor a trivially-easy action that does nothing.

### 3.2 Personalized adherence / feasibility model `p̂_a`

`p̂_a = σ( θ·z_{p,a} )`, a per-patient logistic over features `z_{p,a}`:

| Feature | Source |
|---------|--------|
| effort `E_a`, latency `τ_a` | action record |
| **personal history with this action/class** — completion rate, streak length, recency | nudge feedback log (§6) |
| similar-patient base rate (empirical-Bayes shrinkage to cohort) | Doc 13 |
| context fit — time-of-day, calendar load, current acute mode, weather/season for ACT | device/context |
| friction — does it need a new device/purchase/appointment | action record |
| momentum — current streak in the same `class_a`, habit-stacking opportunity | feedback log |

Cold-start (no history): `p̂_a` falls back to the **cohort base rate** with wide uncertainty, and we
shrink toward it (empirical Bayes, Doc 13). As the patient acts, the personal terms dominate.
The model is **recalibrated** against realized completion (§6) — a real probability, not a guess.

### 3.3 Selection: greedy, diverse, safe (the actual top-5)

Sorting by `U_a` alone yields *five nutrition tips*. We select with a **diversity constraint** via
greedy submodular pick (a small facility-location / determinantal flavour):

```
 selected = ∅
 repeat 5×:
   a* = argmax_{a∉selected, passes safety §5}  U_a − μ · redundancy(a, selected)
   selected ← selected ∪ {a*}
```

- `redundancy(a, S)` is high if `a` shares `class_a`, the same valve/reservoir, or the same pillar
  as actions already chosen (so we don't credit overlapping mechanisms twice — also avoids the §2.5
  double-count at the *recommendation* level).
- **Class cap:** ≤2 actions per `class_a` in the top-5 (default), guaranteeing a mix (e.g. one
  sleep, one activity, one nutrition, one stress, one measure/clinical).
- **At least one near-zero-effort action** is always included if available (a "free win" anchors the
  habit; honest because its `ΔPureScore` is shown truthfully, even when small).
- **Constraint-aware override:** if a pillar is *binding/critical-adjacent* (large `(a)·(b)` gradient,
  §2.1), one slot is reserved for the highest-leverage **safe** action on it, even at higher effort,
  flagged as "biggest lever" — so ease-first never buries the one thing that matters most.
- **Positive-`Δ` guarantee.** Every action admitted to the top-5 must have a **strictly positive
  expected `ΔPureScore@h`** on the continuous score (Doc 03 §2b/§6.1) — selection filters out
  zero-impact actions (e.g. an already-optimal managed marker, D16). So *acting on the list always
  moves the number up*; the patient gets immediate, honest feedback. If no positive-`Δ` modifiable
  action exists (everything green/at-optimum, or all remaining risk is fixed), the engine says so
  rather than inventing a nudge (Doc 05 Modifiability; D5).

### 3.4 Negative feedback (the other direction)
The loop is symmetric. Regression — a missed-sleep streak, a sedentary week, rising stress, a
worsening wearable trend — pushes the personal z-score adverse (Doc 03 §2b), so PureScore **trends
down** and the affected pillars show **↓ Trajectory**, escalating to an **Early-warning** flag if it
accelerates (Doc 05 §4–§5). The engine surfaces this as a *gentle, non-alarming* "slipping" signal
with the single easiest recovery action — never a scold, never a discrete cliff, and never masking a
real clinical change behind "you're improving vs your own bad week" (safety dominates, Doc 03 §2b
`band_clamp`).

---

## 4. Presentation — the daily nudge card (honest by construction)

### 4.1 Card anatomy (per action)

Each of the 5 cards shows:
1. **Plain-language action + dose** ("Walk 1,500 more steps — about a 15-minute walk").
2. **Impact, attributed and horizoned** ("+1.4 PureScore over 90 days · helps Fitness, Blood Sugar,
   Heart") — the §2.4 decomposition in patient language, with the **7-day** number shown too so
   latency is never hidden.
3. **Effort badge** (`E_a` → "tiny / small / moderate / big effort") and **latency badge**.
4. **"Why this"** — the explainability trace: *which marker/reservoir/binding-constraint it targets*
   and why it's on *your* list ("your sleep debt is the biggest hidden drag on three pillars").
5. **Confidence** — evidence strength `ε_a` as "strong / moderate / emerging evidence", and a
   coverage note if it's a `MEASURE` action ("we're guessing your ApoB — measuring it sharpens your
   whole heart score").

### 4.2 Streaks & habit formation (non-manipulative)

- **Streaks** and **habit-stacking** suggestions (attach a new micro-action to an existing routine)
  support formation, *because* the reservoir model rewards sustained behaviour (Doc 04 §4.2: one good
  day ≠ cured; the tank only drains under sustained input). Streaks here are **mechanistically true**,
  not a slot-machine.
- Progress is shown as **realized reservoir drainage / score movement**, tying the streak to the
  actual `B_j(t)` trajectory the patient is changing.

### 4.3 Anti-Babylon honesty rules (binding — Doc 01 §3.1)

- **No overpromising.** The quoted `ΔPureScore_a(h)` is the `ε_a`-discounted, exactly-recomputed,
  horizoned number (§2.3/§2.5). We never inflate, never quote a 90-day number as if it were today's.
- **No dark patterns.** No artificial scarcity, no manufactured urgency, no guilt loops, no streak
  held hostage. A missed day is reported neutrally; the patient may dismiss/snooze any nudge freely.
- **No triage-to-reassurance.** A good nudge list **never** implies "you're fine." If anything in the
  state is red/critical, the card leads with escalation, not with tips (§5).
- **Explainable or absent.** If an action's impact can't be traced through §2, it is not shown
  (Doc 01 §2.2).

### 4.4 Anti-nag / fatigue control

`ν_a` decays an action repeatedly surfaced and ignored; the engine rotates and respects a daily nudge
**budget** (default ≤5 surfaced, and a weekly "big lever" cadence) so the product is sustainable to
attend to (Doc 01 §2.5 burden control extends to attention, not just measurement cost).

---

## 5. Safety — never nudge into harm (binding)

Safety is a **hard pre-filter on `A` before ranking**, plus mode-reweighting and red-flag routing.

### 5.1 Contraindication filter (precedes §3 entirely)

For patient `p` with cohort `c(p)=(age, sex, D, Mx)`, action `a` is **eligible** only if `safe_a`
clears against `D`, `Mx`, and current acute flags. Worked guards:

- **CKD (eGFR<45 / stage ≥3b):** `NUT-PROT` (protein load) and any potassium-raising nutrition nudge
  are **blocked or capped**, regardless of how good it looks for BCM/MET — the brief's explicit case.
  REN reservoir/marker state vetoes the MET/BCM-optimizing action.
- **Anticoagulated / bleeding risk:** high-dose `NUT-O3`, certain supplements blocked.
- **Acute cardiac / unstable angina:** `ACT-Z2`, high-intensity ACT blocked (Doc 09 acute mode).
- **Osteoporosis / recent fracture:** high-impact ACT → supervised-only variant.
- **Hypotension / falls:** aggressive BP-lowering behavioural stacking flagged.
- **Disordered-eating history:** calorie-restriction / weight-framed nudges suppressed; reframed.
- **Pregnancy / postpartum / menopause stage:** Doc 08 toggles eligibility & doses.

A blocked action is **logged with reason** (auditable, Doc 16) and never silently re-surfaces.

### 5.2 Red-flag routing (escalation, not nudges)

If the scored state contains any **acute-danger red** with `escalation ∈ {emergency, urgent}`
(Doc 03 §4.1 / Doc 02 critical markers — e.g. K⁺ 6.3, SpO2 88%, PHQ-9 item-9 > 0), the engine does
**not** produce a tips card. It surfaces the **escalation pathway** (Doc 16) as the only action and
suppresses lifestyle nudges. *A suicidality item routes to crisis support, never to a breathing
exercise.* This is the anti-Babylon missed-red-flag countermeasure (Doc 01 §3.1).

### 5.3 Acute-mode reweighting (Doc 09)

When acute mode is active (`m_k^acute` spiked, Doc 03 §5.1), the engine **reweights nudges toward
recovery and the acute pillar**: effort caps tighten (favor very-low-`E_a` recovery actions), the
diversity cap yields to acute-relevant actions, and contraindications from the acute event apply.
On recovery (hysteresis, Doc 09), weights revert and the normal library re-opens.

### 5.4 Reserve-deficit reds → priority nudges (not alarms)

A **reserve-deficit red** (e.g. very low VO2max, Doc 03 §4.1) is *not* an emergency; it becomes a
**high-leverage but safe** nudge (the §3.3 "biggest lever" slot), framed as priority improvement, at
a feasible starting dose — never an alarm, never an unsafe jump.

---

## 6. Feedback loop — realized vs predicted, recalibration (ties to Doc 13)

The engine logs, for every surfaced action, a closed loop:

```
 logged: (action a, predicted p̂_a, predicted ΔPureScore_a(h), date)
 observed: completion ∈ {done, partial, dismissed, snoozed},  streak,
           and — at horizon h — the realized ΔPureScore and per-pillar/marker movement
```

### 6.1 Two calibration targets

1. **Adherence model (`p̂_a`).** Compare predicted vs realized completion; refit `θ` (§3.2) per
   patient and pool to cohort (empirical Bayes, Doc 13). Brier score / calibration curve tracked.
2. **Sensitivity / impact model.** Compare **predicted `ΔPureScore_a(h)`** against the **realized**
   change attributable to the action (de-confounded against other actions and natural drift using the
   reservoir state equations as the counterfactual baseline). Systematic over-prediction ⇒ shrink the
   relevant `e_a`, `g_a(h)`, or `κ`/`λ` estimates — i.e. the **valve efficacies and reservoir
   parameters are themselves recalibrated** (Doc 04 §7 versioned params; Doc 13 §5 stability re-check
   so the Jacobian stays Hurwitz).

### 6.2 Honesty enforcement via the loop

Because realized vs predicted is measured and surfaced (and audited, Doc 16), the engine is
**structurally prevented from overpromising**: a consistently-optimistic action self-corrects
downward. This is the quantitative anti-Babylon guarantee — claims are forced to track evidence
(Doc 01 §3.1, §2.7), and any parameter change is a model-version bump requiring Doc 13
re-validation.

---

## 7. Default constants (all tunable, all versioned)

| Symbol | Meaning | Default |
|--------|---------|---------|
| `Δ` | nudge clock | 1 day |
| horizons `h` | reported impact horizons | 7 / 30 / 90 days |
| `h*` | ranking horizon | 30 days |
| `α` | impact exponent in `U_a` | 1.0 |
| `β` | adherence exponent in `U_a` | 1.0 |
| `η` | **ease exponent** in `U_a` (ease-first ⇒ >1) | 1.5 |
| `μ` | diversity/redundancy penalty | tuned |
| class cap | max actions per `class_a` in top-5 | 2 |
| nudge budget | max surfaced/day | 5 |
| `ε_a` tax | evidence-strength discount on quoted Δ | applied always |

Constants live in the versioned config; any change is a model-version bump requiring re-validation
(Doc 13) and an audit-trail entry (Doc 16). Valve efficacies `e_a`, realization curves `g_a(h)`, and
the reservoir parameters they ride on (`λ_j, κ, u_{kj}`) are **calibrated, not asserted** (§6, Doc 13).

---

## 8. End-to-end worked example — a daily card

**Patient:** 52-y-old man, pre-diabetic (HbA1c 6.0%), sleep 5.8 h (chronic `B_SLD` high), steps
4,800/day, ApoB imputed (low `cov_CV`), on no meds, no red-flags. Goal: "more energy, avoid diabetes."

Engine run:
1. **Library filtered** (no contraindications; not acute; no red-flags ⇒ tips card allowed).
2. **Impact** computed at 7/30/90 d through §2.
3. **Utility + diversity** select across classes; ease-first `η=1.5`.

**Top-5 card produced:**

| # | Action | Impact (30d / 90d) | Effort | Why this |
|---|--------|--------------------|--------|----------|
| 1 | 30-min earlier lights-out (`SLP-WIND`) | +1.1 / +1.6 | tiny | Sleep debt is the biggest hidden drag on 3 pillars (SLP, MET, MCS) |
| 2 | +1,500 steps/day (`ACT-STEPS`) | +0.7 / +1.4 | small | Builds fitness reserve; lowers glycemic burden — HbA1c est. −0.2% |
| 3 | Measure ApoB (`MSR-APOB`) | sharpens CV score | small | We're estimating your heart's key number; one test removes the guess |
| 4 | +10 g fiber/day (`NUT-FIBER`) | +0.5 / +0.9 | small | Smooths blood sugar; feeds anti-inflammatory benefit |
| 5 | 5-min breathing (`STR-CHECK/BREATH`) | +0.3 / +0.5 | tiny | Lowers stress load, which is feeding your short sleep |

Headline: *"Do these 5 — about +3.5 PureScore over 90 days. Most of it is sleep and movement.
Nothing here is urgent; your numbers are not in the danger zone, and the one number we're guessing
(ApoB) is easy to measure."* — diverse (SLEEP/ACT/MEASURE/NUTR/STRESS), ease-weighted, attributed,
honest about horizon and uncertainty, no overpromise.

---

## 9. Delivery engine — getting the selected nudge to the patient

> §1–§8 decide *which* action to surface and *what* honest number to attach. This section defines
> *how* it reaches the patient on a real mobile app — channels, timing, consent, privacy — and the
> firewall between best-effort **engagement** and guaranteed **safety-critical** delivery. It reuses
> the adherence model (§3.2) and feedback loop (§6) and is bound by the anti-Babylon rules (§4.3).

### 9.1 Two delivery classes (the firewall)

Every outbound message is exactly one class; they never mix.

| Property | ENGAGEMENT | SAFETY-CRITICAL |
|---|---|---|
| Source | top-5 nudges (§3), streaks / re-engagement (§9.9), digests | acute-danger red, escalation emergency/urgent (Doc 03 §4.1, Doc 16) |
| Legal basis | explicit opt-in (marketing-adjacent) | duty-of-care / vital-interest / contract (transactional) |
| Quiet hours · caps · opt-out | **respected** | **bypassed** |
| Channels | in-app + push (→ fallback) | **all reachable at once** |
| Acknowledgement | not required | **required**; no ack within TTL → human escalation |
| Patient can silence | yes (per-category, §9.6) | **no** (legal duty, not a toggle) |

**Hard rules.** (1) Only a Doc 16 escalation tag mints a critical — the engine may **never** promote an engagement nudge to critical to bypass limits (a dark pattern). (2) A critical can **never** be suppressed by opt-out, quiet hours, or a cap. (3) Crisis *content/pathway* is owned by Doc 16; this section owns only its *delivery mechanics*. (4) **Dependents / household (Doc 07):** a dependent's safety-critical routes to the responsible caregiver/proxy per consent, in addition to the patient.

### 9.2 Channels & the fallback ladder

| Channel | Tier | Used for | Constraints |
|---|---|---|---|
| In-app inbox | system-of-record | every message, persisted | no OS permission; seen only on app open |
| Push (APNs/FCM) | primary engagement | daily digest, time-sensitive criticals | needs OS permission; **best-effort, no delivery guarantee** |
| SMS | fallback + critical | critical fan-out, push-off patients | telecom; PHI-safe only; STOP handling (§9.6) |
| WhatsApp (Business API) | fallback + critical | MENA-prevalent reach | pre-approved PHI-safe templates; opt-in |
| Email | records / digest | weekly digest, receipts, exports | not for time-critical |

Per-patient, per-channel **reachability** = granted / denied / provisional / undetermined, plus deliverability health (token validity, recent SMS/WhatsApp success). Channel choice is a function of (class, priority, reachability).

- **Engagement ladder:** push (if granted) → else in-app inbox only. SMS/WhatsApp are **reserved for critical** (fatigue + cost), never used for ordinary nudges.
- **Critical ladder:** in-app takeover **+** push **+** SMS **+** WhatsApp fired *together* (speed dominates); collect acks; no ack within `ack_TTL` → **human escalation** (care-team / on-call clinician, or the consented emergency contact) + audit (Doc 16). A critical is **never silently dropped** — total channel failure also escalates to a human. (Criticals are confirmation-gated upstream — Doc 05 §3.4 / Doc 16 — so fan-out is never triggered by unconfirmed noise.)

### 9.3 Scheduling & send-time

- **Clock.** Engagement runs on the daily nudge clock `Δ` (§1); the day's top-5 are coalesced into **one** digest push, not five pings (attention budget, §4.4).
- **Send-time.** Pick the time in the allowed window that maximises `p̂_a` — §3.2 already carries a time-of-day / calendar-load feature, so delivery **reuses** the adherence model rather than inventing a second one.
- **Circadian / context.** Sleep wind-down in the evening, movement when typically active, measurement in clinic hours.
- **Ramadan / fasting (Doc 18).** During fasting hours, suppress food/med-timing nudges and shift to *suhoor/iftar* windows.
- **Timezone & travel.** Local-time, DST- and travel-aware.

### 9.4 Quiet hours & Do-Not-Disturb

Default engagement quiet window **21:00–07:00 local** (patient-configurable); OS DND/Focus respected where exposed; engagement due in quiet hours defers to the next window (or drops if stale). **Critical overrides** quiet hours and DND (iOS time-sensitive / critical-alert entitlement; Android high-importance) — a genuine emergency must wake the patient.

### 9.5 Frequency caps, arbitration & cross-channel dedup

- **Caps (engagement only):** ≤1 daily digest push + the weekly "big-lever" cadence (§4.4); a per-day push ceiling (default 2) incl. reminders; cooldown after a dismiss; the `ν_a` anti-nag decay (§4.4) extends to delivery (a repeatedly-ignored nudge stops being pushed).
- **Arbitration** when messages compete: critical > care-gap/appointment/result-ready > daily digest > streak/milestone > re-engagement > weekly digest. Lower items yield or merge. **Criticals are never batched, coalesced or rate-limited.**
- **Cross-channel dedup:** an engagement message is delivered on **one** channel — never push *and* SMS the same nudge. (Critical fan-out is deliberate, not duplication.)

### 9.6 Consent, opt-in & OS permissions (regulatory)

- **Transactional vs marketing.** Safety-critical, appointment and result-ready messages are **transactional / duty-of-care** (UAE PDPL & GDPR vital-interest/contract) — not gated by marketing consent. Daily nudges, streaks, tips and re-engagement are **engagement** and need **explicit opt-in**.
- **Granular categories** (patient toggles): *Safety alerts* (always on, not disableable), *Daily plan*, *Streaks & milestones*, *Care reminders*, *Weekly digest*, *Research/product* — each mapped to a class + legal basis.
- **OS permission priming:** an in-context pre-prompt precedes the system push dialog; if push is denied, engagement degrades to the in-app inbox and **safety still reaches the patient** via SMS/WhatsApp.
- **STOP / unsubscribe** (SMS/WhatsApp/email) disables **engagement on that channel only** and **never** safety-critical (separate legal basis) — disclosed at opt-in.
- All consent & preference changes are **versioned and audited** (Doc 16).

### 9.7 PHI-safe payloads (privacy by construction)

- **Default: no health specifics in any payload** — no marker, value or diagnosis. Generic teaser + deep-link only (*"Your PureScore plan is ready"*; critical: *"Urgent health alert — open PureScore now"*). The marker/value is **never** on a lock screen, watch or synced/mirrored surface.
- Health detail is revealed **only after in-app authentication** (biometric/passcode) — delivery therefore depends on the on-device-security work (see *Production readiness — gaps*).
- **Opt-in richer previews** let a patient consciously accept the lock-screen trade-off.
- SMS/WhatsApp/email bodies follow the same rule (PHI-free body, auth-gated deep-link); WhatsApp templates are pre-approved PHI-safe; all notification copy is reviewed so health detail can't leak.
- Because payloads are PHI-safe, third-party channel processors (APNs/FCM, SMS, WhatsApp) **never handle PHI** — which also satisfies processor / data-residency constraints (PDPL/GDPR).

### 9.8 Deep-links & in-notification actions

- Every message **deep-links** to its exact destination via a route registry keyed by message type / `class_a`: nudge → its card; critical → the Doc 16 escalation/crisis screen; result-ready → score detail; appointment → booking. A locked app lands on auth first, then routes through (no PHI pre-auth).
- **Quick actions** feed the §6 feedback log directly: engagement → *Done / Snooze / Remind tonight / Dismiss*; critical → *I'm safe / I need help / Call now* (dials local emergency services — 999/112 in the UAE).

### 9.9 Streaks, lifecycle & re-engagement (non-manipulative)

- **Streaks** reuse §4.2 (mechanistically true, tied to real reservoir drainage — never a slot machine). Delivery adds an **honest** streak-at-risk reminder and milestone acknowledgement; a streak is **never** held hostage, a missed day reported neutrally (§4.3).
- **Lifecycle / dormancy:** declining engagement (no opens for N days) triggers re-engagement at a **decreasing** cadence that eventually **stops** (no infinite win-back spam); win-backs lead with real value — the easiest positive-`Δ` action or a high-yield *measure-this* — never guilt.
- **Negative-trend** signals (§3.4) are delivered gently as a "slipping" nudge with one easy recovery action — alarming wording is reserved for genuine criticals.

### 9.10 Measurement & the feedback loop (extends §6; validated per Doc 14)

- **Per-message telemetry:** queued → sent → delivered (receipt) → displayed → opened/tapped → action (done/partial/snooze/dismiss) → realized ΔPureScore @h. Event payloads are **PHI-safe** (IDs/enums only).
- This **closes the §6 loop**: channel/time efficacy recalibrates send-time and `p̂_a` (§3.2); deliverability (token validity, SMS/WhatsApp rates) is monitored.
- **Critical-path SLOs** (validated per Doc 14, governed per Doc 16): time-to-deliver, time-to-ack, unacknowledged-escalation rate, and **false-alarm rate** — alarm fatigue is an explicit outcome metric, not an afterthought (§4.4).
- Delivery experiments (send-time, copy, channel) are confined to **engagement**; safety-critical delivery is **never A/B-tested**.

### 9.11 Reliability & safety guarantees

- **Idempotency** keys prevent duplicate sends on retry; per-patient ordering; a durable outbox.
- **Retry / backoff** on transient channel failure, then the fallback ladder (§9.2).
- **Criticals are guaranteed-attempt:** persisted until acknowledged; all-channel failure or no-ack-within-TTL → human escalation + audit (Doc 16). Never silently lost.
- **Localization (Doc 18):** RTL/Arabic templates, locale formatting, culturally-appropriate timing.
- **Accessibility:** payloads never convey critical meaning by colour/sound alone; the in-app inbox is screen-reader- and dynamic-type-friendly (see the accessibility gap).

### 9.12 Default constants (versioned)

| Symbol | Meaning | Default |
|---|---|---|
| quiet hours | engagement no-send window (local) | 21:00–07:00 |
| push cap | engagement pushes/day (incl. reminders) | 2 |
| digest | coalesce daily top-5 into one push | on |
| `ack_TTL` | critical no-ack → human escalation | 15 min (emergency) / 4 h (urgent) |
| re-engage | dormancy → win-back cadence → stop | 7 d → weekly → stop after 3 |
| big-lever | weekly high-effort cadence (§4.4) | 1 / week |

All tunable and versioned; any change is a model-version bump (Doc 14) with an audit entry (Doc 16).

### 9.13 Worked example — one critical, one digest

**02:10 local — K⁺ 6.4 mmol/L, confirmed (Doc 16 emergency).** Class = SAFETY-CRITICAL. Quiet hours (21:00–07:00) are **overridden**; in-app takeover **+** push (time-sensitive) **+** SMS **+** WhatsApp fire together, every payload PHI-safe: *"Urgent health alert — open PureScore now."* The patient taps at 02:14 → ack logged, routed (post-auth) to the Doc 16 crisis screen (*Call 999 / I'm safe / I need help*). Had no ack arrived by 02:25 (`ack_TTL` 15 min), the on-call clinician / care-team is paged and the event audited. No marker or value ever appeared on the lock screen.

**19:30 local (next day) — daily top-5 ready.** Class = ENGAGEMENT. One **digest** push (not five) at the send-time that maximises `p̂` (early evening, before the 21:00 quiet start), PHI-safe teaser *"Your PureScore plan is ready"* deep-linking to the plan. SMS/WhatsApp are **not** used. Had push been disabled, the digest would simply wait in the in-app inbox. Quick actions (*Done / Snooze / Dismiss*) feed the §6 loop.

The two messages share **nothing** — different class, legal basis, channel set, quiet-hours behaviour and ack policy — which is the firewall (§9.1) made concrete.

## 10. Implementation status (interactive calculator) — decision D16

The calculator implements the daily-nudge pipeline live:

| Spec concept | Calculator realization |
|---|---|
| Action library §1 (typed records: class, valve/marker targets, effort `E_a`, evidence `ε_a`, safety) | `NUDGES[]` (SLEEP/NUTR/ACT/STRESS/ADHERE/CLINICAL) |
| Impact §2 — exact recompute, not lookup (§2.5 honesty bound) | `nudgeImpact()` = finite-difference `uncapped()` after applying the action's marker moves + reservoir deltas (30-day horizon) |
| Diminishing returns in green; no false hope on fixed burdens | `improveToward()` yields 0 for an already-optimal marker; **Modifiability gate** zeroes lifestyle effect on a fixed/genetic marker (D5/D11) |
| Ranking §3.1 — `U=[ΔPure·ε]^α·p̂^β·(1−E)^η·ν` | `nudgeRank()` (α=β=1, η=1.5; `p̂` cold-start cohort proxy from effort; `ν` bonus for the binding/worst pillar — Trajectory-aware) |
| Safety/contraindication §5; diversity | CKD protein cap, pregnancy/lactation no-deficit caveat, anticoagulant ω-3 caveat; per-class cap of 2 |
| Adherence-only / clinical-gap nudges first-class | `MED-*` shown only when prescribed (`rx`); **fixed/medication-responsive red routes to a clinician referral** instead of a fabricated lifestyle fix |
| **Delivery engine (§9)** — channels, send-time, quiet hours, caps, consent, PHI-safe payloads, critical escalation | **Spec only** — production server + app + 3rd-party channels (push/SMS/WhatsApp); not modelled in the illustrative calculator |

Demonstrated: prediabetic → "cut refined carbs +5.7" on the binding MET pillar; elderly → resistance +
protein (sarcopenia); CKD → BP-adherence + sodium (protein gated out); **FH → lifestyle cannot move
genetic ApoB (gated to ~0) → clinician referral surfaces.** All ΔPureScore values are illustrative
(synthetic engine), pending Doc 14 validation of realized-vs-predicted impact (the §6 feedback loop).
