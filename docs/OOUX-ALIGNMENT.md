# Wiki ↔ OOUX System Map — Alignment Register

**Status:** Analysis complete (4-agent review). Recommendations **proposed, not yet applied.**
**Date:** 2026-06-26
**Inputs:** `ooux-system-map.html` (the whole-app OOUX object model — ~70 objects, 15 layers,
10 features) vs the PureScore wiki (canonical JSON + Docs 01–19 + glossary rulings).

> **Headline:** the two models describe the **same system** and agree in substance ~85%.
> The gaps are three kinds: **(A) word collisions** (the same term meaning two things, or two
> terms for one thing), **(B) stale numbers** in the OOUX map (a snapshot of the older 9-reservoir
> sim), and **(C) a few genuine product/architecture decisions** still open. Almost nothing is a
> true contradiction of the engine. PureScore content should **not** be rewritten — the work is
> vocabulary reconciliation + an orientation layer.

---

## Top reconciliations (prioritized)

| # | Issue | Type | Recommended resolution |
|---|---|---|---|
| 1 | **"Pillar" collision** — OOUX "PureScore Pillar" = 4 backend themes (Risk/Reserve/Lifestyle/Vitality, off-UI); wiki = 12 weighted clinical pillars (CV/MET/REN…), engine-load-bearing | A | The **12 keep the word "Pillar"** (calc-graph + evidence-registry canonical; `_engine_guard` enforces weights=1). OOUX's 4 → rename **"score facets / dimensions,"** modelled as an *optional roll-up ABOVE* the 12, never a replacement. Don't let "pillars = 4 backend" leak into the engine. |
| 2 | **Confidence / Accuracy naming collision** — OOUX "Confidence" = the headline banded number = wiki **"Accuracy"**; the wiki's own "Confidence" is a narrower sub-term (trustworthiness only) | A | Pick **one word for the headline band**. Highest-confusion fix: the same number is called two things across app and engine. Recommend: headline = **Accuracy** (wiki), with Confidence + Sufficiency as its named sub-components; have OOUX adopt that. |
| 3 | **Marker vs Biomarker** — OOUX makes **Biomarker** the canonical object; glossary ruling #9 makes **Marker** canonical | A | Keep **Marker** canonical in clinical/engine prose (it correctly spans lab + wearable + PRO; "biomarker" mis-scopes wearable/PRO). Register **Biomarker** as the **My-Health product-surface alias** (bidirectional). Adopt OOUX's object split **MarkerDef / MeasuredValue / ReferenceRange** — the wiki already mirrors it (`MarkerDef`/`Measurement`/`ReferenceDist`). |
| 4 | **Care Plan / Protocol / Care pathway** — OOUX adds the **prescriber axis** and a **Protocol** object (member's living plan) that collides with ruling #5's prose calling the clinical template a "protocol" | A/C | Adopt three sharply-named constructs: **Care pathway** = clinical template (backend) → **Care plan** = clinician-prescribed instance ‖ **Protocol** = member-authored lifestyle plan-per-goal. **Reword ruling #5** to stop calling the template a "protocol." |
| 5 | **Action-pipeline objects unnamed in wiki** — wiki has anonymous selection stages; OOUX names them | A | Pure relabel (no math change): §3 selection → **Action Funnel**; nudge-card payload → **Insight**; MEASURE/CLINICAL nudges → **EnhancementAction**; ΔAccuracy-per-effort ranking → **Value of Information**. |
| 6 | **Daily contract: 1 hero vs top-5** — OOUX surfaces **one** Daily Action/day (rest = NBAs); wiki surfaces a **top-5** card | C | Genuine product decision. Recommend distinguishing **Daily Action** (the one hero) from **NBAs** (the rest of the ranked set) in Doc 11 §3.3/§4. |
| 7 | **Daily Summary gap** — wiki argues PureScore is the *slow* outcome and *not* the immediate reward, but never models the immediate-feedback surface | C | OOUX makes **Daily Summary** the acknowledgement/reinforcement surface PureScore can't be. Either adopt it or explicitly scope it out of the scoring spec. |
| 8 | **Whole-app positioning** — wiki treats PureScore as the system; OOUX: **My Health owns** the centralised Biomarker Engine + Timeline + KnowledgeLevel that every feature reads; PureScore is the embedded "present read" | C | Invert biomarker ownership to **My Health** in Ch.2 + Doc 06; position PureScore as one of 10 features (present read; the Twin is the future read on the same substrate). Add a "Where PureScore sits" orientation (see Structural recs). |
| 9 | **Stale numbers in the OOUX map** | B | Re-sync OOUX text to JSON canon: **reservoirs 9 → 15**, **knobs 16 → 19** (16 behavioural + 3 Rx; the 3 Rx already match exactly), **outcome horizons** (wiki 7/30/90 d vs OOUX 30/90/365), and **"13 systems / 66 diseases"** (wiki = 12 systems / 26 diseases — trace or correct). |

---

## Layer-by-layer findings

### A · Object model & terminology
The two agree structurally; conflicts are the three word-collisions above (#1 Pillar, #3 Marker/Biomarker, #4 Protocol). OOUX's genuine contributions the wiki should absorb: the **prescriber axis** (Care Plan vs Protocol vs PersonalGoal) and the **Marker object decomposition** (Marker-def / MeasuredValue / ReferenceRange). `MeasuredValue` and `ReferenceRange` are already in the wiki implicitly — name them to match.

### B · Scoring engine & Moniac substrate
**Same machine.** Knob → Reservoir → Marker → Score cascade and the outcome overlay correspond cleanly. Reservoir mapping: OOUX's named 5 = GLY, VBP, INFL, CRF, and "Recovery" (a lossy merge of **ALLO + SLD**); its "9 in the sim" = the original cardiometabolic core; the wiki added 6 asset tanks (MUS, BON, RENR, MICR, OXD, IRON) → **15 canonical**. The 3 Rx knobs match exactly (statin / antihypertensive / GLP-1). **OutcomeProjection ≈ the wiki's 5 outcomes** (ASCVD/T2DM/MORT/ENERGY/BIOAGE) near 1:1. The only true conceptual divergence is the **Pillar layer (#1)**; the **RewardFunction** is stated additively in OOUX (.45/.20/.20/.10/.05) but multiplicatively in the wiki (`U_a`, harm = hard pre-filter) — pick one canonical form and document it.

### C · Action / engagement / daily-loop / trust
The wiki models the **outbound** half of the daily loop (select → nudge → deliver) thoroughly but is light on the **inbound/reflective** half OOUX centres on. Strongest overlap is the **trust layer** (KnowledgeLevel ↔ Sufficiency + cohort fallback; ValueOfInformation ↔ ΔAccuracy ranking; Provenance/ContextualisedStatus shared) — but it carries the **Confidence/Accuracy naming collision (#2)**. **Check-in** matches ruling #4 well (OOUX adds binary / max-3-day constraints; the wiki allows graded responses — reconcile). **Adherence vs Outcome** (did-they-do-it vs did-it-work) is computed by the wiki but not named the way OOUX makes load-bearing — adopt the split in Doc 11 §6. Deliberate non-adoptions (record as decisions, not gaps): **FitCoin / Care Credit / Patterns card / Log** objects — outside the honesty-first scoring scope.

### D · Whole-app scope & IA
PureScore = **1 of 10 features**, embedded in **My Health**. Coverage: **Full** — PureScore, Care & clinical. **Partial** — Shared spine, My Health, Digital Twin, Goals, Engagement. **Absent** — **Nutrition & Fitness** (logging/meals/workouts/streaks — largest gap), **Family** (Household/FamilyLink/ConsentGrant/AttentionItem), **Homescreen/Daily Summary**. These absences should become *acknowledged boundaries*, not silent gaps.

**Structural / IA recommendations** (orientation layer, not a content rewrite):
1. Add a **"Where PureScore sits"** orientation (new page or a top section of `purescore-system.html`): Pura = 10 features; this wiki documents PureScore + the shared substrate; link `ooux-system-map.html` as the canonical whole-app map. One-line scope banner on `index.html`.
2. Relabel the data/intake module in `index.html` + `purescore-system.html` as **"My Health — Biomarker Engine + Timeline + KnowledgeLevel (shared store)"**, with PureScore + Twin as two consumers.
3. Make **"one biomarker, one value everywhere"** a first-class principle (`conventions.html` + Doc 06 lead).
4. Add a thin **OOUX-feature annotation** to NAV chapters (`CHAPTERS`/`NAV_BLURB`, no new pages): ch.2 ↔ My Health/Shared spine, ch.1+4 ↔ PureScore, ch.4 substrate ↔ Digital Twin, ch.5 ↔ Goals/Engagement, ch.6 ↔ Care.
5. Add an **"out of scope (covered elsewhere)"** stub naming the 5 absent features, each linking `ooux-system-map.html`.

---

## Terminology rulings — reconciliation against OOUX

| Ruling | Verdict | Action |
|---|---|---|
| #1 Onboarding | **Stands** | none |
| #2 Intake | **Confirmed** | none |
| #3 Progressive profiling | **Confirmed** | none |
| #4 Nudge / Check-in | **Semantics confirmed; object-hood challenged** | Add a cross-walk note: at the member surface a Nudge is a `Notification(type=nudge)`; the "nudge engine" = Action Funnel + RewardFunction + ClinicalIntelligence. Add the max-3/day check-in budget. |
| #5 Care pathway / Care plan | **Challenged (highest priority)** | Reword: template = "clinical **pathway**", never "protocol"; reserve **Protocol** for the member's lifestyle plan; state the 3 constructs explicitly. |
| #6 Goal / Target | **Confirmed** | Minor: keep capital **Target** for engine setpoints; lowercase "target value" on a goal is fine. |
| #7 Instrument / Screener / PRO | **Confirmed** | none (OOUX uses PRO as a channel — consistent) |
| #8 Pillar | **Challenged (biggest tension)** | Ruling #8 **wins the word**; OOUX's 4 → "score facets," modelled as a roll-up above the 12. Settle the surfacing question (12 on-UI vs off) separately. |
| #9 Marker / biomarker | **Label challenged, structure confirmed** | Keep **Marker** canonical; **Biomarker** = My-Health alias; adopt the Marker-def / MeasuredValue / ReferenceRange split. |

**Net:** #1, #2, #3, #6, #7 stand. #4 needs a note. **#5, #8, #9 need ruling edits** — proposed above, awaiting approval before the glossary `rulings` are updated.
