# 00 — Vision, Design Principles, and Institutional Lessons

> Read `README.md` first for binding conventions.

## 1. Vision

PureScore is a lifelong, sex-specific, multi-pillar wellbeing operating system. It turns a
patient's biomarkers, wearables, and lifestyle into **one explainable number with twelve honest
parts**, tracks it at a sustainable visit cadence, and converts it into **the five easiest things
to do today**. It remembers accumulated burdens, reacts immediately to acute life events, and
hands off to a clinician the moment something looks dangerous.

The product promise is modest by design and large in aggregate:
> *"We will measure what matters, show you exactly where you stand and why, give you the smallest
> next step that helps the most, and never pretend to be your doctor."*

## 2. Design principles

1. **Safety dominates personalization.** Personalization (cohort percentiles, "normal for you")
   makes the score feel relevant; it must never make a dangerous value look safe. The absolute
   clinical anchor wins every tie (README §5.2).
2. **Explainability is a feature, not a report.** Every pillar score and the top-line PureScore
   decompose to the markers, weights, reservoirs, and life events that produced them. If we
   cannot explain a number, we do not show it.
3. **Memory and dynamics over snapshots.** Health is stocks and flows, not a single blood draw.
   The MONIAC reservoir layer (Doc 04) gives PureScore memory, decay, and cross-pillar coupling.
4. **Always-answerable, honestly-caveated.** We can always produce a score via literature
   fallback, but we always annotate **coverage/confidence**. A score built mostly from population
   priors is labelled as such.
5. **Cheapest high-yield first.** Tiering (Core/Peripheral/Comprehensive) packages the
   highest-information, lowest-cost, lowest-burden measurements into a single regular visit, and
   reserves expensive deep phenotyping for cadence or trigger.
6. **Aligned economics.** When PureScore is used by payers, the incentive must be to *improve*
   the patient's score (value-based care), never to deny or to cherry-pick risk.
7. **Conservative claims.** We under-promise. Every capability is bounded by what has been
   prospectively validated for the population in front of us.
8. **Equity by construction.** Cohorting must reduce, not encode, disparity. No protected-class
   proxy may worsen access, price, or care (Doc 09 fairness, Doc 11 governance).

## 3. Lessons we are explicitly engineering around

### 3.1 Babylon Health — what *not* to do
Babylon scaled an AI "symptom checker"/triage product with **bold accuracy claims that outran
its clinical validation**, opaque evidence, regulator and clinician pushback over safety (missed
red-flag presentations), and unsustainable unit economics; it collapsed into insolvency in 2023.

**Engineered countermeasures in PureScore:**
- **No autonomous diagnosis or triage-to-reassurance.** PureScore is wellness-grade; red-flags
  escalate to humans (Doc 11). We never tell a patient they are fine in a way that could mask an
  emergency.
- **Validate before scale.** Prospective, population-specific calibration and fairness audits are
  *gates*, not afterthoughts (Doc 09). No claim ships ahead of its evidence.
- **Transparency over mystique.** Explainable scoring, published methodology, external audit.
- **Sustainable unit economics.** Tiering controls per-patient measurement cost; the actuarial
  layer (Doc 10) targets *portfolio loss ratios with reinsurance*, not clairvoyant individual
  pricing. **"Impossible to lose money" is itself a Babylon-class overpromise and is rejected as
  a design goal** — see §4.

### 3.2 Kaiser Permanente — what to emulate
Integrated payer-provider with population-health management: registries, panel management,
proactive **care-gap closure**, and **value-based incentives** that reward keeping members well.

**Adopted into PureScore:**
- Cohort registries drive proactive outreach and the visit cadence.
- Care-gap logic (overdue Tier measurements, uncontrolled pillars) feeds the nudge engine (Doc 07)
  and care plans (Doc 06).
- The economic model rewards score *improvement* and prevention (Doc 10 value-based section).

### 3.3 Mayo Clinic — what to emulate
Evidence-based, **team-based** care; conservative, guideline-anchored decision support
(AskMayoExpert-style); a destination model for complex cases; cautious public claims.

**Adopted into PureScore:**
- Guideline-anchored bands and clinician decision-support content (Doc 02, Doc 08).
- Team-based escalation: PureScore is one input to a care team, never the decision-maker.
- Conservative communication standards for patient-facing language.

### 3.4 Others worth borrowing from
- **Intermountain / Geisinger (ProvenCare):** standardized care pathways and outcome
  accountability → care-plan templates (Doc 06).
- **UK Biobank / NHANES / Framingham:** population reference distributions and validated risk
  equations → reference ranges (Doc 01) and clinical scores (Doc 08).
- **STRAW+10, ACC/AHA, ADA, KDIGO, AASLD, Endocrine Society, USPSTF, NSF:** the guideline
  backbone for bands and staging.

## 4. On "impossible to lose money"

This goal, taken literally, is unachievable and dangerous — it is exactly the kind of overclaim
that destroyed Babylon and that invites regulatory and ethical failure (adverse selection,
proxy discrimination, denial of care to the sick). PureScore instead pursues a **defensible,
legal, and durable** financial posture:

- Manage risk at the **portfolio** level (loss-ratio targeting, Bühlmann credibility, reinsurance
  / stop-loss), not by trying to perfectly price every individual (Doc 10).
- Make money by **making people healthier** (value-based shared savings), not by excluding them.
- Treat fairness, anti-adverse-selection, and regulatory compliance as **hard constraints**, not
  optimizations to be traded away.

The honest version of the goal is: *"a financially sustainable model whose profit comes from
measurable health improvement and disciplined portfolio risk management."* That is what Doc 10
designs, with the legal and fairness landmines flagged in red.

## 5. What success looks like
- Patients understand their score and act on it (adherence to nudges).
- Red/critical states reach a clinician fast, with no false reassurance failures.
- Scores are **calibrated** to real outcomes and **fair** across sex, age, race/ethnicity, and SES.
- The score moves *before* hard events when an upstream burden accumulates (early warning).
- The business is sustainable without ever profiting from denial of care.
