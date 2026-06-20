# 04 — MONIAC Reservoir Dynamics (Stocks, Flows, Decay, Interference)

> Binding conventions: `README.md §3`. This document gives PureScore its **memory and physics**.
> It defines the latent reservoirs `B_j`, their time-decay `λ_j`, the cross-pillar interference
> matrix `κ`, and how reservoirs feed back into pillar risk (`B̃_k` in Doc 03 §3).

## 1. The MONIAC analogy

The MONIAC (Phillips hydraulic economic computer, 1949) modelled an economy as **water in
tanks**: reservoirs (stocks of money), pipes (flows between sectors), valves (policy knobs), and
leakage (taxes/savings draining a tank). PureScore models the body the same way:

| MONIAC element | PureScore element |
|----------------|-------------------|
| Tank / reservoir (stock) | accumulated **burden** or **reserve** `B_j(t)` |
| Inflow pipe | adverse markers + behaviours raising a burden |
| Outflow / leakage | **healing/decay** `λ_j` when inputs normalize |
| Cross-pipe between tanks | **interference** coupling `κ_{jl}` (one burden feeding another) |
| Valve / knob | **intervention** (medication, behaviour change, nudge) |
| Float gauge | the reservoir's contribution `B̃_k` to pillar risk |

Two reservoir polarities:
- **Burden** (water you don't want): glycemic burden, atherogenic burden, inflammatory load,
  sleep debt, adiposity, hepatic fat, allostatic/stress load, vascular/BP load. High = bad.
- **Reserve / asset** (water you *do* want): cardiorespiratory reserve, muscle/strength reserve,
  bone reserve, renal reserve, micronutrient reserve, oxygen-delivery reserve, iron reserve.
  High = good; depletion = risk.

This is the "biomoniac asset reservoir and burdens" the brief asked for, connected to
cohort-specific lifestyle goals (Doc 09).

## 2. Reservoir catalogue

| `j` | Reservoir | Polarity | Primary inflows | Decay/heal `λ_j` (time-scale) | Feeds pillars (via `κ`/`B̃`) |
|----|-----------|----------|-----------------|-------------------------------|------------------------------|
| GLY | Glycemic burden | burden | high glucose/HbA1c/CGM-TIR, refined-carb intake | weeks–months | MET, CV, REN, HEP, INF |
| ATH | Atherogenic burden | burden | ApoB/LDL/Lp(a)·time, BP·time, smoking | years (CAC ~ irreversible) | CV |
| INFL | Inflammatory load | burden | hsCRP, IL-6, poor diet, visceral fat, infection, sleep debt | days–weeks (acute), months (chronic) | INF, CV, MET, HEP, MCS |
| SLD | Sleep debt | burden | nightly deficit vs need, irregularity, OSA | days (acute), weeks (chronic) | SLP, MCS, MET, CV, INF |
| ADI | Adiposity (esp. visceral) | burden | energy surplus, low activity | months–years | BCM, MET, CV, INF |
| HEPF | Hepatic fat | burden | fructose/alcohol, surplus, insulin resistance | weeks–months | HEP, MET |
| ALLO | Allostatic / stress load | burden | PSS/PHQ/GAD, cortisol slope, life events | days–months | MCS, ENDO, CV, SLP |
| CRF | Cardiorespiratory reserve | **asset** | MVPA, VO2max training, steps | depletes over weeks of inactivity | FIT, CV, MET |
| MUS | Muscle / strength reserve | **asset** | resistance training, protein, grip/ALMI | depletes over weeks–months | BCM, FIT, MET |
| BON | Bone reserve | **asset** | weight-bearing, vit D/Ca, hormones | very slow (years) | BCM |
| RENR | Renal reserve | **asset** | inverse of GLY/BP/UACR damage | slow, partly irreversible | REN |
| MICR | Micronutrient reserve | **asset** | vit D/B12/iron/omega-3 intake | weeks–months | NUT, HEM, MCS |
| VBP | Vascular / BP load | burden | systolic/diastolic BP·time, arterial stiffness | months–years (partly structural) | CV, REN |
| OXD | Oxygen-delivery reserve | **asset** | hemoglobin, SpO₂, iron sufficiency, cardiorespiratory fitness | weeks–months | HEM, CV, FIT |
| IRON | Iron reserve | **asset** | dietary iron, ferritin / transferrin-sat, menstrual/GI losses | weeks–months | HEM, NUT |

(IDs are stable; the table is extensible per cohort — e.g., a pregnancy reservoir set in Doc 08.
VBP/OXD/IRON were promoted from the Appendix E question-bank mapping so the LIFE-stream weights
target a single canonical reservoir set.)

## 3. State equation (discrete stock-and-flow)

Each reservoir evolves on the update interval `Δ` (e.g., daily):

```
 B_j(t+Δ) = B_j(t)
            + Δ · [  inflow_j( markers(t), behaviours(t) )           # source
                    − λ_j · ( B_j(t) − B_j^* )                       # leakage toward set-point
                    + Σ_{l≠j} κ_{jl} · sat(B_l(t)) ]                  # interference from other tanks
```

clamped to `B_j ∈ [0, B_j^max]`. Components:

- **`inflow_j`** — a saturating (logistic) function of the relevant adverse markers and behaviours
  so a single bad day cannot blow up the tank:
  `inflow_j = κ_j^in · σ( Σ a · (marker excess) + Σ b · (behaviour) )`, `σ` logistic.
  For **assets**, "inflow" is the *training/intake stimulus* that raises the reserve.
- **`λ_j (B_j − B_j^*)`** — **time-decay / healing**. `B_j^*` is the healthy set-point (0 for pure
  burdens; a target reserve for assets). Large `λ_j` (sleep debt) ⇒ fast recovery when behaviour
  normalizes; tiny `λ_j` (atherogenic burden, CAC) ⇒ near-permanent — *exposure integrates over
  years*. This is the explicit **time-decay** requirement.
- **`κ_{jl}`** — **interference / multivariate dependency**: how reservoir `l` feeds `j`. `sat()`
  bounds the coupling. Examples (sign and rough magnitude; calibrated in Doc 13):
  - `κ_{INFL,SLD} > 0` — chronic sleep debt raises inflammatory load.
  - `κ_{GLY,SLD} > 0` — sleep debt worsens glycemic control.
  - `κ_{GLY,CRF} < 0` — cardiorespiratory reserve *lowers* glycemic burden (asset protects).
  - `κ_{ATH,GLY} > 0`, `κ_{ATH,INFL} > 0` — glycemic + inflammatory load accelerate atherogenesis.
  - `κ_{ALLO,SLD} > 0` and `κ_{SLD,ALLO} > 0` — stress and sleep debt are bidirectional.
  - `κ_{MET via ADI,…}` — adiposity raises GLY, INFL, HEPF.

The full `κ` matrix is a **12×12-ish signed, sparse, literature-initialized** coupling matrix
(initial values from physiology/epidemiology), then **calibrated and regularized** against
outcomes (Doc 13). Stability requirement: the linearized system (matrix `−Λ + K`) must be
**Hurwitz/contractive** (all eigenvalues negative real part) so reservoirs converge rather than
diverge — checked at every calibration (Doc 13 §5).

## 4. Why this matters (the four behaviours the brief demanded)

1. **Accumulation reflects on the pillar/PureScore.** "If sleep debt is consistently accumulated,
   it should reflect" — `B_SLD` integrates nightly deficits; via `B̃_SLP` (and `κ` into CV/MET/INF)
   it raises those pillars' `R_k` (Doc 03 §3) even when a single night looks fine.
2. **One good day ≠ cured.** Because the tank only drains at rate `λ`, a single good night barely
   moves chronic `B_SLD`; sustained behaviour change drains it.
3. **Cross-pillar contagion.** A red glycemic state raises `B_GLY`, which via `κ` raises
   `B_INFL` and `B_ATH`, nudging INF and CV risk up — *multivariate dependency across pillars*.
4. **Reversibility realism.** Sleep debt and inflammation recover fast; atherogenic burden and CAC
   barely reverse — the model's `λ` encodes prognosis honestly (no false hope, no false alarm).

## 5. Mapping reservoirs back to pillar risk (`B̃_k`)

Doc 03 §3 uses `B̃_k ∈ [0,1]`, the normalized burden load on pillar `k`. Define link weights
`u_{kj}` (from the "feeds pillars" column) and normalize each reservoir by its `B_j^max`:

```
 B̃_k = clamp_{[0,1]}(  Σ_j u_{kj} · ( B_j / B_j^max )          for burdens j
                       + Σ_j u_{kj} · ( 1 − B_j / B_j^target )  for assets j (depletion = risk) )
```

So a depleted asset (low `B_CRF`) *adds* risk to FIT/CV/MET, while a full burden tank adds risk to
its linked pillars. `ρ_k` (Doc 03 §8) caps the total contribution.

## 6. Valves (knobs) = interventions → links to nudges and care plans
Every modifiable inflow/decay term is a **valve** an intervention can turn:
- A behaviour nudge (Doc 11) opens a drain on `B_SLD` (sleep-hygiene) or a fill on `B_CRF`
  (zone-2 cardio).
- A medication changes a marker directly (statin ↓ApoB ⇒ ↓`B_ATH` inflow) — modelled as a valve
  with its own efficacy/latency.
- Care plans (Doc 09) are **bundles of valve settings** tied to the cohort's lifestyle goals.

The nudge engine computes each action's effect by differentiating PureScore through this dynamic
system (Doc 11 §3 sensitivities), giving an honest "this action drains X reservoir by Y over Z
weeks ⇒ +ΔPureScore."

## 7. Numerical/operational notes
- Update `Δ` = 1 day for fast reservoirs (SLD, INFL-acute, ALLO), with slower tanks updated on
  the same clock but tiny `λ`. Wearable streams update daily; labs update on measurement.
- **Missing data:** when an inflow's marker is stale, inflow uses the last value with decayed
  confidence (it does **not** reset the tank — burdens persist through missing measurements;
  Doc 06 §5 anti-gaming).
- **Acute events** (Doc 09) inject a transient high-`λ` reservoir and temporarily raise relevant
  `κ`/weights, then drain on recovery — the mechanism behind acute-mode entry/exit.
- All `λ`, `κ`, `u`, set-points are **versioned parameters** calibrated and stability-checked in
  Doc 13; none may be changed without a model-version bump and re-validation (Doc 16).
