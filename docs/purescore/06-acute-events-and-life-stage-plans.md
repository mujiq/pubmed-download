# 06 — Acute-Event Override & Life-Stage Care / Nutrition / Exercise Plans

> Binding conventions: `README.md §3`. This document defines two coupled mechanisms:
> **(A)** the *acute-event override* — how PureScore detects an acute event, spikes `m_k^acute`
> (Doc 03 §5.1), injects a transient high-`λ` acute reservoir (Doc 04 §7), re-prioritizes toward
> recovery, and then *reverts* with hysteresis to long-term goals once healed; and **(B)** the
> **life-stage care/nutrition/exercise plans** that set the default valves (Doc 04 §6) and the
> `m_k^goal` weights (Doc 03 §5.1) for each stage. Reproductive stages (cycle, fertility,
> pregnancy, post-partum, menopause/andropause) are **deferred to Doc 05**, which is authoritative;
> this document references them but does not redefine them.
>
> All illustrative clinical numbers below are **literature/guideline-anchored examples and must be
> re-verified on a fixed cadence and cohort-adjusted** before any production use (README §5.6).

---

## Part A — The Acute-Event Override

### A.1 Principle

PureScore is normally a **long-term optimization** engine: it stack-ranks chronic burdens and
nudges toward longevity goals. But when an **acute event** happens, the right behaviour is the
opposite of optimization — *triage and recover*. The override implements the brief's requirement:

> "If an acute event happens, PureScore prioritizes it; once healed it reverts to long-term goals."

The override is built entirely from primitives already defined:

| Need | Mechanism | Source |
|------|-----------|--------|
| Make the acute pillar dominate the score | spike `m_k^acute` for affected pillar(s) | Doc 03 §5.1 |
| Give the event *memory* and a drain curve | inject transient high-`λ` **acute reservoir** `B_ACU` | Doc 04 §7 |
| Re-aim nudges/care at recovery | swap `m_k^goal` weights + acute valve bundle | Doc 04 §6, Doc 07 |
| Don't let healthy pillars hide a danger | existing critical cascade | Doc 03 §5.3 |
| Don't self-manage true emergencies | emergency escalation, NOT acute mode | Doc 03 §4.1, Doc 11 |
| Avoid flapping in/out of acute mode | entry/exit **hysteresis** | Doc 03 §6, A.6 below |

Acute mode is a **display + weighting state**, never a relaxation of safety. The clinical optimal
band still dominates the cohort percentile (README §5.2), and any emergency red still escalates.

### A.2 Taxonomy of acute events

| Class | Examples | Primary affected pillars | Default acute reservoir(s) | Typical drain `λ` |
|-------|----------|--------------------------|----------------------------|-------------------|
| **Acute medical — infectious** | infection, influenza/COVID, sepsis screen-positive | INF, HEM (SpO2), MET | `B_ACU` + spike on `B_INFL` | days–weeks |
| **Acute medical — injury** | fracture, sprain, fall, soft-tissue injury | BCM, FIT (deload) | `B_ACU` (musculoskeletal) | weeks–months |
| **Acute medical — surgical / post-op** | elective or emergency surgery, post-op recovery | BCM, FIT, INF, NUT | `B_ACU` + catabolic/`B_INFL` | weeks |
| **Acute medical — cardiac** | MI, unstable angina, new arrhythmia, decompensated HF | CV (critical) | `B_ACU` + permanent `B_ATH` step | event = emergency; recovery weeks–months |
| **Acute medical — renal** | AKI (KDIGO stage), contrast nephropathy | REN (critical) | `B_ACU` + possible permanent `RENR` loss | days–weeks if reversible |
| **Acute medical — chronic-disease flare** | COPD/asthma exacerbation, IBD/RA flare, gout, CKD-on-AKI, glycemic crisis (DKA/HHS) | the chronic-condition pillar(s) | `B_ACU` superimposed on the chronic reservoir | days–weeks |
| **Acute mental-health crisis** | acute suicidality, panic/crisis, acute psychosis, substance crisis | MCS (critical) | `B_ACU` + spike on `B_ALLO` | crisis = emergency; stabilization weeks |
| **Acute life event** | bereavement, job loss, divorce/separation, caregiving shock, displacement | MCS, SLP, ENDO | `B_ACU` + spike on `B_ALLO`, `B_SLD` | weeks–months |
| **Pregnancy complications** | pre-eclampsia, GDM crisis, hyperemesis, post-partum events | **see Doc 05** (authoritative) | Doc 05 pregnancy reservoir set | per Doc 05 |

Notes:
- A single event may light up multiple pillars (sepsis → INF + HEM + REN + MET). The override
  spikes `m_k^acute` for **each** affected pillar, weighted by severity (A.4).
- **Acute life events** are first-class: bereavement/job-loss/divorce/caregiving are scored through
  MCS + the `B_ALLO` (allostatic load) and `B_SLD` (sleep debt) reservoirs, not dismissed as
  "non-medical." They open a recovery plan exactly like a physical event.

### A.3 Detection (multi-source, with confirmation)

An event can be opened by any of four sources; the engine fuses them and records the trigger in the
explainability object (Doc 03 §7).

| Source | Example signals | Confidence | Confirmation needed? |
|--------|-----------------|------------|----------------------|
| **Markers / labs** | hsCRP spike >10 mg/L, WBC >11 or <3.5, SpO2 <92%, ALT/AST 3× ULN, K⁺/eGFR shift (AKI), Tn rise | high (if recent) | clinician for emergency reds |
| **Wearables** | resting-HR step-up vs baseline, HRV collapse, skin-temperature anomaly, SpO2 desaturation, **activity collapse** (steps/MVPA drop ≫ personal variance), sleep fragmentation | medium | corroboration or PRO |
| **Patient-reported (PRO)** | "I had surgery", "fever 3 days", "bereavement", symptom check-in, pain score | medium–high | self-attested; flag for review |
| **EHR encounter** | ED visit, admission, procedure code, new diagnosis, discharge summary | high | authoritative when present |

**Fusion rule.** Open *provisional* acute mode on any single medium+ source; **confirm** to *active*
acute mode when (a) an EHR encounter exists, **or** (b) ≥2 independent sources agree, **or** (c) a
critical-marker red fires. A wearable-only anomaly without corroboration stays **provisional**
(softer re-weighting, no display takeover) to avoid false triggers from a hard workout or a flight.

> **Anti-gaming / missing-data note (Doc 04 §7):** acute reservoirs do **not** reset when a marker
> goes stale; a provisional event that is never corroborated decays out on its own `λ` rather than
> being silently dropped.

### A.4 Entry criteria and severity tiers

Acute mode is entered per-event with a **severity tier** that sets the size of the `m_k^acute` spike,
the injected `B_ACU` magnitude, and the routing.

| Tier | Definition (entry criteria) | `m_k^acute` spike (affected pillar) | `B_ACU` inject | Routing |
|------|------------------------------|-------------------------------------|----------------|---------|
| **A0 — provisional** | single uncorroborated medium-confidence source | ×1.2 (soft) | small, fast `λ` | watch; PRO prompt; **no** display takeover |
| **A1 — mild** | corroborated, self-managed (e.g., minor URI, minor sprain, acute grief week 1) | ×1.5 | moderate | acute-mode display; recovery nudges |
| **A2 — moderate** | corroborated, needs clinical follow-up (flare needing meds, post-op week 1–2, AKI stage 1 resolving) | ×2.0 | large | acute-mode display; **care-team task** (Doc 11) |
| **A3 — severe / emergency** | any **emergency** acute-danger red (Doc 03 §4.1): sepsis screen, SpO2 <92%, K⁺ ≥5.6, Tn rise, suicidality, MI/AKI-acute | n/a — **bypasses** self-managed acute mode | n/a | **immediate human escalation** (Doc 11); cascade caps PureScore ≤ 40 |

**Hard safety boundary (A3).** Emergency acute-danger reds **do not** enter self-managed acute mode.
They route straight to the emergency pathway (Doc 11) via the existing cascade (Doc 03 §5.3): the
pillar is critical, `PureScore ≤ PURE_CRIT_CAP`, `overall_status = CRITICAL`, and a human is engaged.
Acute mode (A0–A2) is for **sub-emergency** recovery the patient can self-manage *with* care-team
visibility — never a substitute for escalation. The reserve-deficit reds of Doc 03 §4.1 (e.g., very
low VO2max) are **not** acute events; they drive priority-improvement plans in Part B.

### A.5 Mechanism while active (priority logic)

While an event is **active** (A1/A2), the override applies four coupled changes:

1. **Weight spike — acute dominates optimization.** For each affected pillar `k`,
   `m_k^acute` is set per A.4 and `W_k = W_k^base · m_k^cohort · m_k^goal · m_k^acute` is
   renormalized (Doc 03 §5.1). The affected pillar(s) now carry the bulk of the score's attention;
   far-from-the-event pillars are de-emphasized so the patient is not nagged about, say, Lp(a) the
   week after surgery.

2. **Acute reservoir injection (Doc 04 §7).** Inject `B_ACU` — a transient, high-`λ` reservoir
   linked to the affected pillar(s) via `u_{k,ACU}`. Because `λ_ACU` is large, `B_ACU` drains as the
   patient heals, providing the *recovery clock* and the revert trigger (A.6). Where the event also
   feeds a chronic tank (sepsis → `B_INFL`; MI → permanent step on `B_ATH`; AKI → possible permanent
   `RENR` loss), that coupling persists after `B_ACU` drains — honest reversibility (Doc 04 §4.4).

3. **Acute valve bundle (Doc 04 §6) + re-aimed nudges (Doc 07).** Swap the long-term valve settings
   for an **acute recovery bundle**: rest/deload, hydration, protein for catabolic events, sleep
   protection, medication adherence, wound/symptom monitoring, graded return. `m_k^goal` is
   temporarily repointed from long-term goals to **recovery goals**. The nudge engine's top-5
   (Doc 07) becomes recovery-first.

4. **Acute-mode display.** The headline communicates *"recovering from an acute event"* rather than a
   bare number drop, shows the recovery trajectory (`B_ACU` drain curve), suppresses long-term
   stack-rank guilt, and surfaces the care-team contact for A2. The score still reflects reality
   (it may legitimately dip), but the *frame* is recovery, not failure.

**Priority logic (one line):** while any event is active, **acute recovery > long-term optimization**
— the acute pillar's `m_k^acute` and the recovery `m_k^goal` outrank chronic-burden nudges, *except*
that any emergency red still preempts everything via the cascade (Doc 03 §5.3).

### A.6 Revert logic (recovery + hysteresis → back to long-term goals)

The switch-back is the crux of "once healed it reverts." It is **gated**, not abrupt, to prevent
flapping (Doc 03 §6).

**Recovery is tracked on two signals that must *both* clear:**

```
 (R1) marker recovery:  the markers that opened the event have returned to (and held in) their
                        non-red band for a confirmation window  (e.g., hsCRP < 3 mg/L, SpO2 ≥ 96%,
                        ALT < ULN, eGFR back to baseline) — re-verify thresholds on cadence.
 (R2) reservoir drain:  B_ACU(t) < θ_exit · B_ACU^peak     (default θ_exit = 0.25)
```

**Hysteresis (anti-flap), mirroring Doc 03 §6:**

| Guard | Rule | Default |
|-------|------|---------|
| Asymmetric thresholds | exit threshold `θ_exit` is *lower* than the entry trigger (enter high, leave low) | enter at `B_ACU ≥ θ_enter·B^max`, leave at `< 0.25·B^peak` |
| Dwell / confirmation window | R1 and R2 must hold for a sustained window, not a single reading | `τ_dwell` (e.g., 48–72 h fast events; longer for surgical) |
| Confirming measurement | for events opened by a lab, require a confirming in-band lab or sustained wearable normalization before exit | per event |
| One-way step-down | mode steps **A3→A2→A1→resolved**, never skipping upward silently; a relapse re-spikes rather than half-exiting | enforced |

**On exit (resolved):**
1. `m_k^acute → 1` for the affected pillars (ramped down over `τ_ramp`, not snapped, so the headline
   eases back); `W_k` renormalized (Doc 03 §5.1).
2. `m_k^goal` is **restored to the patient's long-term life-stage goals** (Part B) — the long-term
   nudge stream resumes.
3. The valve bundle reverts from *recovery* to the **life-stage default bundle** (Part B), often with
   a *graded-return* sub-plan (e.g., return-to-exercise ramp after an injury or post-op).
4. Any **permanent** sequelae remain: `B_ACU` is gone, but a post-MI step on `B_ATH`, a post-AKI
   `RENR` deficit, or a new chronic-disease flag stays in the model and updates `m_k^cohort`. The
   event leaves an honest scar, not a reset to "as if it never happened."
5. The whole transition is logged in the explainability object (Doc 03 §7): *"acute event (post-op,
   A2) opened 2026-04-10, resolved 2026-05-01; recovery goals → long-term goals restored;
   residual: +renal-reserve deficit."*

### A.7 Safety summary (non-negotiable)

- Emergency acute-danger reds (Doc 03 §4.1) → **immediate human escalation** (Doc 11), never
  self-managed acute mode.
- Acute mode never lowers a clinical-anchor risk and never averages away a critical pillar.
- Uncertainty defaults to caution (README §5.4): a borderline wearable-only signal stays provisional
  (softer) — but a borderline *emergency* signal escalates, not waits.
- Every entry/exit and weight change is explainable and audited (Doc 03 §7, Doc 11).

---

## Part B — Life-Stage Care / Nutrition / Exercise Plans

### B.1 What a plan is, mechanically

A **life-stage plan** is a default configuration of three things the rest of the spec already
defines, chosen for the patient's stage and cohort:

1. **`m_k^goal` weights** (Doc 03 §5.1) — which pillars the stage emphasizes.
2. **Valve bundles** (Doc 04 §6) — the standing intervention/nudge settings that fill assets
   (`B_CRF`, `B_MUS`, `B_BON`, `B_MICR`) and drain burdens (`B_SLD`, `B_INFL`, `B_ADI`, `B_ALLO`).
3. **Measurement cadence** by tier (Core/Peripheral/Comprehensive, Doc 01 §3) — how often each
   marker class is refreshed, which also drives **coverage/confidence** (Doc 03 §3).

Plans are **adaptive**: when a pillar goes yellow/red, the plan auto-escalates that pillar's cadence,
opens the relevant valves harder, and raises its `m_k^goal` (B.7). Reproductive-stage plans
(pregnancy, post-partum, peri/menopause, andropause) are **deferred to Doc 05**.

**Named good practices borrowed (Doc 00):**
- **Kaiser Permanente** — *panel management* (every patient on a panel has a known care-gap list) and
  *care-gap closure* (proactive outreach when a Tier-1/2 measurement or screening is overdue). In
  PureScore, the cadence tables below *are* the panel/care-gap list; an overdue cell is a care gap.
- **Mayo Clinic** — *evidence-based, team-based* care: every plan element cites a guideline class and
  is owned by a care-team role, not just the app.
- **Intermountain / Geisinger ProvenCare** — *standardized pathways*: each stage and each acute
  recovery is a versioned, auditable pathway (a "bundle of valve settings", Doc 04 §6), reducing
  unwarranted variation while still personalized by cohort.

### B.2 Life stages (binding for this doc)

| Stage | Typical band | Dominant `m_k^goal` pillars | Headline reservoir focus |
|-------|--------------|------------------------------|--------------------------|
| **Adolescent** | ~12–17 | MCS, SLP, FIT, NUT, BCM (peak bone) | build `B_BON`, `B_CRF`; protect `B_SLD`, `B_ALLO` |
| **Young adult** | ~18–34 | FIT, MCS, MET, SLP | build `B_CRF`/`B_MUS`; establish baselines |
| **Reproductive years** | ~18–45 | ENDO, NUT, MET, MCS (+ Doc 05) | **see Doc 05** (cycle/fertility/pregnancy) |
| **Midlife** | ~35–55 | CV, MET, MCS, SLP, FIT | first `B_ATH`/`B_GLY`/`B_ADI` accrual; catch early |
| **Older adult** | ~55–74 | CV, MET, BCM, FIT, MCS, REN | preserve `B_MUS`/`B_BON`/`B_CRF`; manage `B_ATH` |
| **Frail / geriatric** | ~75+ | BCM, FIT, MCS, NUT, SLP | **anti-sarcopenia/anti-fall**; protect reserves, deprescribe-aware |

Stage is one axis of the cohort `c(p)` (README §3.1); the plan is further modified by disease flags
`D` and medication classes `Mx` (B.8).

### B.3 CARE PLAN templates (cadence of Tier 1/2/3 + screening + goals)

Cadence is the **panel-management / care-gap** backbone (Kaiser). "Overdue" cells generate proactive
outreach. All intervals are **illustrative defaults — re-verify on cadence** and override by guideline
and cohort.

| Stage | Tier-1 (Core) cadence | Tier-2 (Peripheral) | Tier-3 (Comprehensive) | Stage-specific screening / goals |
|-------|------------------------|---------------------|------------------------|----------------------------------|
| **Adolescent** | annual: BP, BMI, mood (PHQ-A/GAD), sleep | as-indicated: lipids if risk, ferritin | rarely | immunizations, vision, substance/risk, **mental-health & sleep check-in every visit**; goal: bone-building activity, sleep regularity |
| **Young adult** | 1–2 yr: BP, BMI, glucose/A1c, mood, sleep, activity | 2–3 yr: lipids/ApoB, vit D, Lp(a) **once** | baseline body-comp if available | establish lifestyle baselines; CRF baseline; mental-health & sleep first-class |
| **Reproductive yrs** | per Doc 05 | per Doc 05 | per Doc 05 | **see Doc 05** (cycle, fertility, pregnancy, post-partum) |
| **Midlife** | annual: BP, A1c, lipids/ApoB, BMI/waist, mood, sleep, activity | 1–2 yr: HOMA-IR, ALT/FIB-4, UACR, TSH, omega-3 | 5–10 yr: CAC **once** if intermediate risk; DEXA if risk | cancer screening per guideline; ASCVD/SCORE2 (Doc 08); **sleep apnea screen** if signs |
| **Older adult** | annual: BP, A1c, lipids, eGFR/UACR, BMI, mood, sleep, **gait speed/grip** | 1–2 yr: FIB-4, B12, vit D, TSH | DEXA (FRAX, Doc 08); body-comp/ALMI | cancer + bone screening; **fall-risk & cognition check**; polypharmacy review |
| **Frail / geriatric** | 6–12 mo: BP (orthostatic), A1c (relaxed targets), eGFR, **grip/gait/ALMI**, mood, nutrition (albumin, weight trend) | as-tolerated | sparing (burden-aware) | **fall-risk, cognition, nutrition (sarcopenia), deprescribing, goals-of-care**; avoid overtreatment |

Every cell is owned by a **care-team role** (Mayo team-based): app/nudge engine for self-measured
wearable/PRO items; nurse/care-manager for outreach and gap closure; clinician for labs and
escalation. The cadence table is the **ProvenCare standardized pathway** for the stage.

### B.4 NUTRITION PLAN templates (evidence-based pattern + modifications)

Base pattern by default, then **cohort/disease/medication-aware modifications**. Patterns are
literature-anchored (re-verify on cadence). Diet-quality and fiber are scored in **NUT** (Doc 02);
the plan opens valves on `B_MICR` (fill) and `B_INFL`/`B_ADI`/`B_GLY` (drain).

| Stage | Base pattern | Key targets | Modifications (cohort / disease / med) |
|-------|--------------|-------------|----------------------------------------|
| **Adolescent** | balanced, adequate-energy Mediterranean-style | Ca + vit D (bone), iron (esp. menstruating), protein for growth | disordered-eating screen before any restriction; iron if ferritin low (NUT/HEM) |
| **Young adult** | Mediterranean | fiber ≥25–30 g, omega-3 index ≥8%, limit ultra-processed/alcohol | athlete: periodized fueling; vegan: B12/iron/omega-3 supplementation |
| **Reproductive yrs** | per Doc 05 | folate/iron pre-conception (Doc 05) | **see Doc 05** (pregnancy/lactation nutrition) |
| **Midlife** | Mediterranean / DASH if BP-yellow | sodium ↓ (DASH) if BP↑; protein ~1.0–1.2 g/kg; weight regulation | **MET-yellow/red**: lower glycemic-load, time-restricted eating consideration; **HEP fat**: cut alcohol/fructose; **CKD**: protein/K⁺/phosphate per KDIGO |
| **Older adult** | Mediterranean + **higher protein** | protein **1.2–1.5 g/kg** (anti-sarcopenia), vit D/Ca/B12, fiber | **on metformin**: monitor B12; **on diuretics/ACEi**: watch K⁺/Na⁺; **anticoagulant**: vit-K-consistent intake |
| **Frail / geriatric** | **energy- & protein-dense**, texture-appropriate | protein **≥1.2–1.5 g/kg**, leucine-rich, vit D, hydration; **liberalize** restrictive diets | de-emphasize tight glycemic/lipid restriction (overtreatment risk); address appetite, dentition, social eating; screen malnutrition (albumin, weight loss) |

**Medication-aware examples (apply across stages):** statin → CV interpretation already adjusted
(Doc 03 §5.1); SGLT2i/diuretic → hydration + electrolytes; PPI → B12/Mg; corticosteroid (e.g., during
a flare) → glycemic + bone watch. These are **valve interactions**, surfaced as plan footnotes.

### B.5 EXERCISE PLAN templates (zone-2 + resistance + balance, dosed by FIT & reserves)

Dose is set by the **FIT** pillar and the **CRF / MUS / BON** reserves (Doc 04), and is
**contraindication-aware** (acute event, cardiac/renal/orthopedic limits). The plan fills `B_CRF`
(zone-2/MVPA), `B_MUS` (resistance), `B_BON` (weight-bearing/impact), and trains **balance** to drain
fall risk in older stages.

| Stage | Aerobic (zone-2 + vigorous) | Resistance | Balance / mobility | Dosing & contraindication notes |
|-------|------------------------------|-----------|--------------------|---------------------------------|
| **Adolescent** | ≥60 min/day MVPA, varied; some vigorous | bodyweight/light, technique-first | sport/skill-based | build `B_BON` (impact/jumping); avoid early overspecialization |
| **Young adult** | ≥150 (≥300 ideal) min/wk; build VO2max | 2–3×/wk full-body | as desired | establish `B_CRF`/`B_MUS` baseline; progressive overload |
| **Reproductive yrs** | per Doc 05 (pregnancy modifications) | per Doc 05 | per Doc 05 | **see Doc 05** |
| **Midlife** | 150–300 min/wk zone-2 + 1–2 vigorous (VO2max) | 2–3×/wk progressive | core/mobility | titrate vigorous by **CRF reserve** & CV status; symptom-limited if CV-yellow |
| **Older adult** | 150 min/wk zone-2, intervals as tolerated | **2–3×/wk, protein-paired (anti-sarcopenia)** | **2–3×/wk balance** (fall prevention) | dose by `B_MUS`/`B_BON`; pre-screen CV; osteoporosis → avoid high-impact/flexion loading |
| **Frail / geriatric** | short, frequent, low-intensity (chair/walk) | **resistance is the priority valve** (power training), supervised | **balance/gait first-class** (Otago-style) | start low, progress slow; supervise; fall-history → balance before vigorous; pain/orthopedic-aware |

**Reserve-deficit reds drive this plan, not emergencies.** A very low VO2max (Doc 03 §4.1 reserve
deficit) is a **priority-improvement** target here: raise `m_FIT^goal`, open the `B_CRF` fill valve,
and put graded zone-2 in the Doc 07 top-5 — never an emergency escalation.

### B.6 Sleep and mental-health as first-class plan components

Not afterthoughts. Every stage's plan carries a **sleep block** and a **mental-health block**, each
with its own cadence, goals, and valves:

| Component | Cadence (all stages) | Goals / valves | Escalation |
|-----------|----------------------|----------------|------------|
| **Sleep (SLP)** | wearable nightly; 14-day review; **OSA screen** if signs | 7–9 h, regularity, efficiency; drain `B_SLD`; protect from shift/jet-lag | suspected mod–severe OSA → sleep-medicine referral (Doc 02 SLP) |
| **Mental health (MCS)** | PHQ-9/GAD-7 + WHO-5 on a fixed cadence (more often in adolescents, midlife stress, older-adult isolation) | mood, anxiety, **loneliness/social connection**, stress; drain `B_ALLO` | PHQ-9 item-9 / acute crisis → **immediate escalation** (Doc 02 MCS, Doc 11), and Part A acute-life-event plan for sub-crisis stressors |

These blocks also interlock with Part A: an **acute life event** (bereavement, job loss, caregiving)
opens the MCS recovery plan and protects sleep, then reverts to the stage's long-term mental-health
goals on recovery (A.6).

### B.7 How plans adjust automatically when a pillar is yellow/red

Plans are not static. The same engine that scores also re-tunes the plan (Kaiser care-gap logic +
ProvenCare pathway branching):

| Pillar status | Automatic plan changes |
|---------------|------------------------|
| **Yellow** | (1) raise that pillar's Tier-2/3 **cadence** (close the measurement gap); (2) open its valves harder (Doc 04 §6) — e.g., MET-yellow → glycemic-load nutrition + zone-2 fill on `B_CRF`; (3) bump `m_k^goal` so the nudge engine (Doc 07) prioritizes it; (4) outreach if self-measures are overdue |
| **Red (reserve-deficit)** | priority-improvement plan: strong valve bundle + high-leverage nudges + clinician task (Doc 03 §4.1) — e.g., low VO2max, osteoporosis, sarcopenia |
| **Red (acute-danger)** | **Part A**: emergency → escalation (Doc 11); sub-emergency corroborated → acute mode (A.4) |
| **Recovered** | step cadence/valves/`m_k^goal` back down with the same **hysteresis** as Part A so the plan doesn't flap (Doc 03 §6) |

This is the explicit tie between **status → plan**: the plan is a function of pillar state, so a
yellow MET pillar quietly tightens the metabolic nutrition + zone-2 dose without the patient having to
ask, and relaxes again when MET returns to green.

### B.8 Cohort-specific lifestyle goals → valves (the through-line)

Each plan ultimately resolves to **valve settings tied to cohort-specific lifestyle goals**
(Doc 04 §6): the stage + disease flags `D` + medication classes `Mx` select which assets to fill and
which burdens to drain, at what intensity, with what cadence, owned by which care-team role. That is
the same object the nudge engine differentiates through (Doc 04 §6, Doc 07 §3) to attribute
"+ΔPureScore" to each action — so a life-stage plan and a daily nudge are the same machinery at
different time-scales, and an acute event (Part A) is that machinery temporarily re-pointed at
recovery and then handed back.

---

### Cross-references
- Acute reservoir mechanics & valves: **Doc 04 §6, §7**.
- `m_k^acute`, `m_k^goal`, critical cascade, hysteresis: **Doc 03 §5, §6**.
- Emergency escalation vs reserve-deficit reds: **Doc 03 §4.1**, **Doc 11**.
- Reproductive-stage care/nutrition/exercise & pregnancy complications: **Doc 05 (authoritative)**.
- Daily nudges & impact attribution: **Doc 07**. Clinical scores (ASCVD, KDIGO, FRAX, FIB-4): **Doc 08**.

> Reminder (README §5.6): every illustrative threshold, interval, dose, and reservoir constant here is
> **literature-anchored and must be re-verified on a fixed cadence** and cohort-adjusted before
> production; all constants are versioned and changes require re-validation (Doc 09) and audit (Doc 11).
