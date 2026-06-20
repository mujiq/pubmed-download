# 16 — Safety, Governance, and Regulatory

> Binding conventions: `README.md §3`, and especially the **hard safety non-negotiables**
> (`README.md §5`). This document is the **control document** for everything that keeps PureScore
> safe, legal, fair, and honest. It does not introduce new scoring mathematics; it governs the
> mathematics defined elsewhere — the critical cascade and escalation tags (Doc 03 §4–§5), the hard
> suicidality rule (Doc 02 MCS), the cohort/fairness/calibration machinery (Doc 13), the gated
> clinician layer (Doc 10), the actuarial/pricing layer (Doc 19), the sex-specific reproductive
> models (Doc 08), and the genetic/aging inputs (Doc 04, Doc 19).
>
> **Re-verify caveat.** Regulatory texts, guidance numbers, and statutory thresholds named below are
> stated as of the cited frameworks and **must be re-verified with qualified regulatory and legal
> counsel for each jurisdiction and product configuration** before any production use (README §5.6).
> Classification is **feature-dependent**: a single change to a claim, output, or audience can move
> PureScore across a regulatory line, so §1 and §8 are re-run on every release.

---

## 1. Regulatory positioning — wellness-grade, clinician-in-the-loop, NOT a medical device

### 1.1 The core claim

PureScore is a **general-wellness product**: it helps a person understand and improve everyday
health, fitness, sleep, nutrition, and wellbeing. It is **not** a diagnostic medical device, makes
**no autonomous diagnostic or treatment claim**, and **never** substitutes for a clinician
(README §1, §5.1; Doc 01 §3.1 anti-Babylon). Every critical/red state escalates to a human
(Doc 03 §4–§5). This is a *design constraint*, not a marketing posture: the system is built so that
the patient-facing surface cannot make a device-grade claim.

### 1.2 Where the line is — US FDA

Two safe-harbours keep the patient-facing product out of medical-device regulation. Both are
**conditional**; the conditions are engineering requirements, not aspirations.

**(a) General Wellness policy (low-risk wellness).** A product is low-risk general wellness when it
(i) makes only **general-wellness claims** (maintain/encourage a healthy lifestyle) and (ii) does
**not** reference a specific disease/condition in a diagnostic, curative, mitigating, or
preventive way. PureScore's patient surface stays here:

| Allowed (general-wellness) claim | Forbidden (device-grade) claim |
|----------------------------------|--------------------------------|
| "Your sleep and activity habits put you in the top quartile of your cohort." | "You do not have sleep apnea." |
| "Some markers are worth discussing with your clinician." | "You have / are at high risk of chronic kidney disease." |
| "Improving fiber may help your metabolic pillar." | "This will prevent / treat type-2 diabetes." |
| "Your wellness score is in the *at-risk* band — see a clinician." | "Your ASCVD 10-yr risk is 14% — start a statin." |
| "We detected a value your clinician should review urgently." | "This is a hypertensive emergency; do X." |

The patient-facing app renders, at most, a **wellness-level prompt to consult a clinician**
(Doc 10 §1.1). Numerical clinical risk estimates (ASCVD, KDIGO, FIB-4, FRAX, KFRE, PhenoAge …)
live **only** in the gated clinician layer (Doc 10) and are never surfaced to the patient as a
diagnosis (§1.5).

**(b) Clinical Decision Support (CDS) exemption — for the clinician layer only.** Software that
supports a clinician can be **non-device CDS** when **all four** hold: (1) it is **not** intended to
acquire/process/analyze a signal from a *signal-acquisition device* (e.g. raw ECG/imaging
interpretation); (2) it displays/analyzes medical information about a patient; (3) it provides
**recommendations** (options) to a *healthcare professional*; and (4) it enables that professional
to **independently review the basis** of the recommendation (it does not rely *primarily* on the
software's output). The Doc 10 clinician layer is engineered to satisfy (3) and especially (4): every
clinical score ships with inputs, source guideline, model version, bands, and OOD flags so the
clinician can independently reproduce and overrule it (Doc 10 §3–§4). **A score the clinician cannot
inspect, or one a clinician is expected to follow without independent review, fails criterion (4)
and becomes a regulated device.** That is an explicit non-goal.

### 1.3 Where the line is — EU MDR / MDCG

Under EU MDR (2017/745), software has a **medical purpose** (diagnosis, prevention, monitoring,
prediction, prognosis, treatment of a disease) is a **medical device**, and **MDCG 2019-11**
guidance plus the *Rule 11* classification logic push most diagnostic/decision-driving software to
**Class IIa or higher** (rising to IIb/III as the information drives serious or critical decisions).
General-wellness/lifestyle software *without* a medical purpose is out of scope. PureScore's
patient surface is positioned as **lifestyle/wellness software, no medical purpose**; the clinician
layer, if/where it provides information *used to take decisions with diagnosis or therapeutic
purposes*, is treated as **potentially MDR-regulated** and is gated and governed accordingly. GDPR
**Art. 9** (health = special-category data) and, for the EU AI Act, the **high-risk** classification
of health/safety AI are assessed per release (§4, §5).

### 1.4 Consequences if a feature crosses into SaMD

| If a feature… | …then it crosses into | Consequence (gate, do not ship until met) |
|----------------|----------------------|-------------------------------------------|
| Tells a patient they have / don't have a condition | FDA device / MDR medical purpose | Full device pathway: QMS (ISO 13485), clinical validation, 510(k)/De Novo or MDR conformity + Notified Body, post-market surveillance |
| Gives an autonomous treatment/medication instruction | Device + practice-of-medicine | Prohibited on the patient surface; clinician-layer only as inspectable CDS |
| Drives a clinician decision the clinician can't independently review | Loses CDS exemption → device | Re-architect for inspectability (Doc 10 §4) or pursue device pathway |
| Interprets a raw signal-acquisition stream (ECG morphology, image) | Device (criterion 1 fails) | Out of scope for this product; route to a cleared device |
| Markets a wellness feature with a disease claim | Device by **intended use** | Block at claims review (§7); intended use is set by claims, not internals |

**Intended use governs classification.** A change in *marketing copy* alone can convert a wellness
feature into a device. Claims review (§7.3) is therefore a release gate (§8).

### 1.5 How the clinician layer is gated separately

The Doc 10 clinical-scores layer is a **separate product surface** with separate controls:
(1) access restricted to **credentialed clinicians** (and, where lawful and separately governed,
payers per Doc 19) via role-based access (§4.3); (2) **never** rendered to patients as diagnosis;
(3) every output **inspectable** to preserve the CDS posture (§1.2b); (4) its own validation,
OOD-flagging, and audit (Doc 10 §6, Doc 13, §5 here). Patient and clinician layers have **distinct
intended-use statements, distinct claims, and distinct regulatory determinations**.

---

## 2. Clinical safety — escalation, fail-safe defaults, human override

### 2.1 The three escalation tiers (binding on Doc 03 §4.1 tags)

Every critical marker in Doc 02 carries an `escalation ∈ {emergency, urgent, routine}` tag
(Doc 03 §4.1). The pathway is determined by the tag, not by the headline number.

| Tier | Examples (Doc 02) | Patient-facing action | System action | Target time-to-clinician |
|------|-------------------|----------------------|---------------|--------------------------|
| **emergency** (acute-danger red) | K⁺ ≥5.6/≤3.2, SpO2 <92, Hb <11, hsCRP/WBC sepsis screen, suicidality (MCS) | Unambiguous *"seek emergency care now / call your clinician now"*; show emergency + crisis resources; **no reassuring framing** | Fire emergency pathway (Doc 03 §5.3); alert named on-call clinician; create high-priority case; log | **Immediate** (minutes); confirm receipt |
| **urgent** | New eGFR <30, ALT/AST acute >3× ULN, BP ≥180/120 region, moderate–severe OSA flag | *"Contact your clinician within 24–72h"*; book/escalate | Open priority case to care team; track acknowledgement; re-prompt if unacknowledged | **24–72 h** |
| **routine** (reserve-deficit red) | Very low VO2max, osteoporosis (FRAX), sarcopenia, chronic sub-optimal | Priority-improvement plan (Doc 09) + high-leverage nudges (Doc 11) | Flag for next visit; care-gap registry (Doc 01 §3.2) | **Next scheduled visit** |

`emergency` and `urgent` are **acute-danger reds**; `routine` reds are **reserve-deficit reds**
(Doc 03 §4.1). Both floor the pillar and cap PureScore (≤40, Doc 03 §5.3); only acute-danger reds
fire the immediate clinician/crisis pathway.

### 2.2 The hard suicidality rule and crisis pathway (Doc 02 MCS)

Any positive suicidality signal — **PHQ-9 item 9 > 0**, or any equivalent (Doc 02 MCS hard rule;
Doc 10 §2.6) — forces MCS to **red/critical irrespective of the total**, is **never averaged away**
(Doc 03 §4 hard floor), and triggers the **crisis pathway**:

1. **Immediate in-app crisis resources** (region-appropriate crisis line / emergency number),
   shown **before** any score, with no reassuring or dismissive framing.
2. **Immediate human escalation** to the named on-call clinician / crisis protocol; C-SSRS is the
   **structured referral instrument the clinician applies** — never an in-app autonomous triage
   (Doc 10 §2.6).
3. **No autonomous risk-stratification or reassurance.** The app does not tell the patient they are
   "low risk." Uncertainty here defaults hard to escalation (README §5.4).
4. **Audit + duty-of-care logging** (§4.5) and warm-handoff confirmation that a human received it.

This is the single most safety-critical pathway in PureScore and is exercised in every release's
safety test suite (§8).

### 2.3 Red-flag symptom handoff to humans

Beyond marker thresholds, declared **red-flag symptoms** (e.g. chest pain, focal neuro deficit,
syncope, severe dyspnea, suicidal ideation, signs of sepsis, reproductive red-flags per Doc 08 such
as severe pregnancy hypertension/bleeding) trigger an **immediate human handoff** and emergency
guidance. PureScore does **not** run an autonomous symptom-checker/triage (the Babylon failure mode,
Doc 01 §3.1): it routes red-flags to humans rather than adjudicating them.

### 2.4 Fail-safe defaults — uncertainty ⇒ caution, never false reassurance

This is README §5.4 made operational. The system is **asymmetric by construction**:

- **Missing / low-coverage data** never produces a green reassurance. Low coverage caps how *green*
  a pillar may display (Doc 03 §3; Doc 06 §4.3); a pillar green only via imputed medians is shown as
  *low-coverage green*, not "you're fine."
- **The cohort term can only raise concern**, never lower it below the clinical anchor
  (Doc 03 §2 `max(...)`; README §5.2). "Normal for a sick cohort" never reads as healthy.
- **Validated clinical scores feed the `max`**, never relax `r_i`, never clear a critical pillar,
  never lift `PURE_CRIT_CAP` (Doc 10 §3 invariants).
- **Stale data** (beyond a marker's validity window) is treated as missing/low-confidence, not as a
  reassuring last-known-good.
- **No silent imputation into clinical scores** — an under-determined clinical score is *not
  computable*, never guessed (Doc 10 §3.1).

The forbidden state — *telling a patient they are fine in a way that could mask an emergency* — is
designed out (Doc 01 §3.1). Where the model is unsure, it **says so and escalates / recommends
measurement** (the highest-yield nudge is often "measure the marker dominating your uncertainty",
Doc 11 §1.1).

### 2.5 Human override and accountability

- A clinician can **override** any PureScore output within their domain (raise concern, re-weight,
  annotate); overrides are first-class, recorded with clinician identity, rationale, and timestamp
  (§4.5, §5.6). The validated clinical score is the higher-authority anchor within its domain
  (Doc 10 §5).
- An override can **add caution** freely; an override that would **reduce** a safety floor (clear a
  critical pillar, lift the crit cap) requires the override to itself be a **clinical act by a
  credentialed clinician**, fully logged, and never performed by the automated system alone.
- **The system never overrides a human.** PureScore is one input to a care team, never the
  decision-maker (Doc 01 §3.3 Mayo team-based model).

---

## 3. Failure-mode analysis (FMEA)

Severity (S), Likelihood (L), Detectability (D) on 1–5 (5 = worst / least detectable). Risk
Priority = S×L×D (qualitative; recomputed with real telemetry in production). Every mitigation
maps to an enforced control elsewhere in the spec.

| # | Failure mode | How a patient is harmed | S | L | D | Mitigations (enforced) |
|---|--------------|-------------------------|---|---|---|------------------------|
| F1 | **False reassurance** (Babylon mode) | Real danger masked; patient doesn't seek care | 5 | 2 | 4 | Critical cascade + crit cap (Doc 03 §5.3); safety-dominant `max` (Doc 03 §2); low-coverage caps green (§2.4); no patient-facing "you're fine" near reds |
| F2 | **Missed red-flag / under-escalation** | Emergency not routed to a human in time | 5 | 2 | 3 | Escalation tags + pathways (§2.1); crisis pathway (§2.2); acknowledgement tracking + re-prompt; hard floors (Doc 03 §4) |
| F3 | **Alert fatigue** | Real alerts ignored because too many fire | 4 | 3 | 3 | Tiered escalation (§2.1); anti-flap hysteresis (Doc 03 §6); top-5 nudge cap (Doc 11); monitor alert-acknowledgement rate (§5.4) |
| F4 | **Automation bias** | Clinician/patient over-trusts the number | 4 | 3 | 4 | Inspectable CDS (§1.2b, Doc 10 §4); explainability shipped with every score (Doc 03 §7); discrepancy banner (Doc 10 §5); human-override primacy (§2.5) |
| F5 | **Miscalibration / drift** | Bands wrong for this cohort → wrong risk | 4 | 3 | 3 | Calibration + drift monitoring (Doc 13); OOD flagging (Doc 10 §6); version bump + re-validation on any constant change (§5.1) |
| F6 | **Data error** (unit/transcription/device) | Spurious red (anxiety, over-treatment) or spurious green | 4 | 3 | 3 | Confirming-measurement requirement before critical→non-critical transition (Doc 03 §6); plausibility/unit checks (Doc 06); confidence-weighting (Doc 03 §3); **wearable trust-tiering — a consumer/inferential wearable cannot drive red/critical without clinical-grade confirmation (D22; Doc 05 §3.4; Doc 07 §2)** |
| F7 | **Equity harm** | Worse score/access/pricing for a protected class | 5 | 2 | 4 | No-proxy rule (README §5.5; §6); fairness audit gate (Doc 13); cohorting reduces not encodes disparity (Doc 01 §2.8); appeal rights (§6.4) |
| F8 | **Privacy / security breach** | Sensitive health (incl. genetic, reproductive) exposed | 5 | 2 | 3 | Encryption, RBAC, audit logging, minimization (§4); genetic (GINA) + reproductive special handling (§4.4–§4.5); breach response (§4.6) |
| F9 | **Reproductive-data weaponization** | Pregnancy/fertility data used against the patient | 5 | 2 | 4 | Heightened minimization/consent for Doc 08 data (§4.5); legal-process resistance; opt-out; default no third-party sharing |
| F10 | **Over-claiming / scope creep into SaMD** | Patient acts on an unvalidated "diagnosis" | 5 | 2 | 3 | Claims review gate (§1.4, §7.3); intended-use control; clinician-layer gating (§1.5); validation-before-scale (Doc 13) |
| F11 | **Acute-event mishandling** | Wrong re-prioritization during a real event | 4 | 2 | 3 | Acute-mode hysteresis + revert (Doc 09); clinician confirmation for care actions; escalation unaffected by acute weights |
| F12 | **Nudge harm / dark pattern** | Unsafe or manipulative recommendation | 4 | 2 | 3 | Contraindication screening on actions (Doc 11 §1); anti-overpromise (Doc 01 §3.1); evidence-weighted, attributed Δ (Doc 11) |

The FMEA is a **living register**: each release re-scores rows against real telemetry and adds new
modes found in incident review (§5.3). Any S=5 row with no green-status mitigation is a launch
blocker (§8).

---

## 4. Privacy and security

### 4.1 Legal basis

| Regime | What it requires of PureScore |
|--------|-------------------------------|
| **HIPAA** (US) | PHI safeguards (Privacy/Security/Breach-Notification Rules); BAAs with every processor; minimum-necessary access; breach notice (individuals/HHS, and media if ≥500) without unreasonable delay (≤60 days) |
| **GDPR** (EU/UK) | Health = **Art. 9 special-category** data; explicit lawful basis + Art. 9 condition (usually explicit consent); data-subject rights (access, erasure, portability, objection); **DPIA**; purpose limitation; ≤72 h breach notice to supervisory authority |
| **GINA** (US) | Genetic info may **not** be used in health-insurance underwriting or employment decisions (§4.4) |
| **State / sectoral** | State genetic-privacy and reproductive-health-data laws; consumer-health-data laws (e.g. WA My Health My Data-class); EU AI Act high-risk obligations (§5) |

### 4.2 Data minimization, purpose limitation, consent

- **Collect only what a pillar/marker or a consented feature needs** (Core/Peripheral/Comprehensive
  tiering, Doc 06, already limits collection to high-yield measurements).
- **Purpose limitation:** scoring data is not repurposed (e.g. into Doc 19 pricing) without a
  **separate, explicit, revocable consent**; payer/actuarial use is separately gated (§1.5, Doc 19).
- **Granular consent** per data class (wearable, labs, **genetic**, **reproductive**, mental-health),
  with plain-language explanation, easy withdrawal, and no loss of core wellness function as
  coercion. Children/dependents and incapacity handled per jurisdiction.
- **Household accounts (Doc 07 §6)** hold individual scores per member under one shared view with
  **role-based access** (e.g. a caregiver monitors an elder's in-home stream). Each member's data
  stays member-scoped; a household view **never merges or cross-uses** members' protected data, and
  the same per-data-class consent, special-category handling (§4.4–§4.5), and RBAC (§4.3) apply
  per member. Dependent/guardian and elder-caregiver access follows the incapacity rules above.
- **Product surfaces (Doc 07 §5)** — AI scribe, AI chat agent, scheduling, in-home tracking,
  insurance-auth — are all **inside these firewalls**: the agent never diagnoses and escalates on the
  §2 hard rules; insurance-auth runs under the payer separation (§1.5, Doc 19); in-home passive
  sensing requires its own explicit consent.

### 4.3 Encryption, access control, audit logging

- **Encryption** in transit (TLS 1.2+/mTLS) and at rest (AES-256); key management with rotation and
  separation of duties; field-level encryption for the most sensitive classes (§4.4–§4.5).
- **Role-based / attribute-based access control:** patient (own data), clinician (assigned patients,
  Doc 10 gated layer), payer (separately governed, de-identified/aggregated where possible, Doc 19),
  engineer (no clear PHI in routine ops). **Minimum-necessary** by default; break-glass access is
  logged and reviewed.
- **Audit logging:** every access to and change of PHI, every escalation, every override, every
  model-version application, every consent change is logged immutably with who/what/when/why (ties
  to §5.6 accountability and §2.5 override records).

### 4.4 Genetic data — special handling (GINA; tie to Doc 19)

Genetic inputs (epigenetic clocks/DNAm, any germline data feeding Doc 04 aging reservoirs or Doc 10
BioAge) get the **strictest** controls:

- **GINA firewall:** genetic information is **structurally walled off from the actuarial/pricing
  layer (Doc 19)** and from any employment-related use. It may inform *wellness and clinical
  interpretation* with consent, but **must not** flow into underwriting, pricing, or access decisions
  (README §5.5; Doc 19 fairness flags). This is enforced by access control, not policy alone.
- **Separate explicit consent**, separate storage, field-level encryption, and the right to delete
  genetic data without losing other functionality.
- Genetic results that are clinically actionable route through a **clinician** (and, where relevant,
  genetic counseling), never as an autonomous patient-facing diagnosis (§1).

### 4.5 Reproductive data — special handling (tie to Doc 08)

Sex-specific reproductive data (cycle, fertility, **pregnancy**, post-partum, menopause; Doc 08) is
**heightened-risk** given its potential for misuse:

- **Heightened minimization and consent;** default **no third-party sharing**; clear separation from
  any payer/marketing use.
- **Legal-process resistance:** policies to minimize retention, resist over-broad legal demands where
  lawful, and notify the data subject where permitted; prefer on-device/ephemeral handling for the
  most sensitive cycle/pregnancy signals where feasible.
- **Easy, complete deletion and export** (GDPR portability/erasure analogue applied universally).
- Reproductive **red-flags** (Doc 08) still escalate to humans (§2.3) — safety is never traded for
  privacy, but the data is not retained or shared beyond that purpose.

### 4.6 Breach response

A standing incident-response plan: **detect → contain → assess → notify → remediate → review.**
Notifications meet HIPAA (≤60 days; HHS/media as required) and GDPR (≤72 h to the supervisory
authority; affected individuals when high risk) timelines. Breaches involving **genetic or
reproductive** data trigger the highest-severity path and a board review (§5.7). Every breach is a
mandatory FMEA and incident-register entry (§3, §5.3).

---

## 5. Model governance / MLOps

### 5.1 Versioning and change control (binding on Doc 03 §8, Doc 13)

**Every constant, band, weight, model, and dataset is versioned.** Per Doc 03 §8 and README §5.6:

> **Any change to a constant, band, weight, equation, reference range, model, or training/cohort
> dataset is a version bump that requires re-validation (Doc 13) before production and is recorded
> in the audit trail.**

| Versioned artifact | Examples | Change ⇒ |
|--------------------|----------|----------|
| **Parameters/constants** | `φ, γ, δ, ρ_k, R_crit, PURE_CRIT_CAP`, zone cuts (Doc 03 §8) | Version bump + targeted re-validation |
| **Bands / reference ranges** | Doc 02 green/yellow/red, Doc 06 cohort ranges | Bump + clinical sign-off + calibration check (Doc 13) |
| **Clinical-score equations** | ASCVD/KDIGO/FIB-4/FRAX versions (Doc 10) | Bump + OOD-envelope re-check (Doc 10 §6) |
| **Cohort/training datasets** | NHANES/Biobank refresh, percentile tables (Doc 13) | Bump + re-fit + fairness + drift re-audit |
| **Action library** | Doc 11 actions, effects, contraindications | Bump + safety screen |

Releases follow **change control**: proposal → review (clinical + technical + fairness) → validation
gates (Doc 13) → sign-off by named owners (§5.6) → staged rollout → monitoring. No "silent" constant
edits.

### 5.2 Production monitoring

Continuously monitor (ties to Doc 13 drift/calibration):
calibration vs realized outcomes; **input drift** and population shift; **OOD rate** (Doc 10 §6);
escalation volume and **acknowledgement/closure latency** (§2.1); **alert-acknowledgement rate**
(F3 alert fatigue); override frequency and direction; coverage/confidence distribution; subgroup
performance for **fairness** (§6, Doc 13); error and missing-data rates. Thresholds trip alerts to
the named owners and, for safety-relevant regressions, can **auto-roll-back** to the last validated
version.

### 5.3 Incident reporting

A standing process: any safety event (missed escalation, false reassurance, crisis-pathway failure,
breach, fairness regression) is logged in an **incident register**, triaged by severity, root-caused,
fixed, re-validated, and fed back into the FMEA (§3) and test suite (§8). Reportable events follow
the applicable regulatory reporting obligation for the product's then-current classification (§1).

### 5.4 Model card / "nutrition label"

Each model/version ships a public-facing **model card**:
intended use and **explicit out-of-scope uses**; populations it was validated on and **where it is
not validated** (OOD envelope); inputs and data sources; **calibration and fairness results by
subgroup** (Doc 13); known limitations and failure modes (§3); confidence/coverage semantics
(Doc 03 §3); version, date, and owners (§5.6). This is the anti-Babylon transparency artifact
(§7, Doc 01 §3.1): claims are bounded by validated evidence, in writing.

### 5.5 (reserved — see §5.4 nutrition label and §7 transparency)

### 5.6 Human accountability — named owners

No model runs ownerless. Each production model/version has:

- a **named Clinical Owner** (qualified clinician) accountable for clinical safety, bands,
  escalation, and the override pathway (§2.5);
- a **named Technical Owner** accountable for the pipeline, versioning, monitoring, and rollback;
- a **named Privacy/Security Owner** (DPO-equivalent) for §4;
- recorded in the audit trail; accountable for sign-off at each gate (§5.1, §8).

### 5.7 Oversight / ethics board

A standing **Clinical & Ethics Oversight Board** (clinicians, ethicist, patient advocate, privacy/
legal, fairness/biostatistics) reviews: new claims and intended-use changes (§1, §7.3); validation
and fairness results before launch (Doc 13, §8); serious incidents and breaches (§4.6, §5.3); the
genetic/reproductive/actuarial firewall (§4.4–§4.5, Doc 19); and the appeal/contest process (§6.4).
The board can **block a launch** (§8) and is independent of commercial targets (Doc 01 §4 rejects
"impossible to lose money" as a goal).

---

## 6. Equity and access governance

### 6.1 The no-protected-class-proxy rule (enforced across scoring and pricing)

README §5.5 and Doc 01 §2.8 are binding: **no protected-class attribute (race, ethnicity, sex where
unlawful, disability, genetic info, reproductive status, …) or its proxy may be used to worsen
access, pricing, or care.** Enforcement spans:

- **Scoring (Doc 13):** cohorting must **reduce, not encode** disparity; fairness audits (calibration
  and error parity across sex, age, race/ethnicity, SES) are **gates**, not afterthoughts; proxy
  detection is run on features.
- **Pricing (Doc 19):** the actuarial layer carries **heavy regulatory/fairness flags**; genetic info
  is firewalled from underwriting (§4.4, GINA); profit comes from **health improvement**, never from
  denial or proxy discrimination (Doc 01 §4).

Where guideline equations legitimately use sex/age/ancestry as *clinical* inputs (e.g. ASCVD,
sex-specific bands Doc 08), they are used **only** to improve clinical accuracy within the
clinician layer, never to worsen access/price, and OOD/fairness-checked (Doc 10 §6, Doc 13).

### 6.2 Accessibility

WCAG-conformant interfaces; screen-reader, contrast, font-scaling, motor and cognitive
accessibility; multi-language; offline/low-bandwidth and low-cost device support so access does not
track wealth.

### 6.3 Health-literacy-appropriate communication

Patient-facing language is plain, at an appropriate reading level, culturally aware, and conservative
(Doc 01 §3.3 Mayo communication standard). Numbers come with meaning ("at-risk band — worth a
clinician visit"), never alarmist and never falsely reassuring (§2.4).

### 6.4 Appeal / contest rights

A patient (or clinician on their behalf) can **contest** a score, an escalation, a data point, or a
pricing/access decision: request the explanation (Doc 03 §7), correct erroneous data (re-scores
deterministically, Doc 03 §6), request **human review**, and appeal to the oversight board (§5.7).
Decisions and rationales are logged (§4.3). For any consequential automated decision, a
**human-review route** is guaranteed (GDPR Art. 22 analogue applied universally).

---

## 7. Transparency (anti-Babylon)

### 7.1 Explainability to patients and clinicians

Every score ships with the Doc 03 §7 explainability object: the binding constraint, per-pillar
contributors, reservoir contributions, personalization rationale, and coverage/confidence caveats.
**If we cannot explain a number, we do not show it** (Doc 01 §2.2). Clinicians additionally get the
inspectable CDS surface (Doc 10 §4) that preserves the device exemption (§1.2b).

### 7.2 Published methodology and external audit

This specification (Docs 01–19) is the **published methodology**. Bands, formulas, constants,
validation, fairness results, and model cards (§5.4) are documented and subject to **external
audit** (clinical, statistical, security, fairness). Re-verification of guideline thresholds runs on
a fixed cadence (README §5.6).

### 7.3 Honest communication standard (claims control)

The anti-Babylon rule (Doc 01 §3.1): **no claim ships ahead of its evidence.** Every patient- or
market-facing claim passes a **claims-review gate** (§1.4, §5.7) checking that it is (a) bounded by
prospectively validated evidence for the population in front of us, (b) a general-wellness claim
(not a device/disease claim, §1.2), and (c) not an overpromise (explicitly including
"impossible to lose money", Doc 01 §4). Intended use is set by claims; claims review is therefore a
**regulatory** control, not just marketing.

---

## 8. Go / No-Go launch checklist

A production deployment (or any version bump, §5.1) is **blocked** unless **every** gate is green and
**signed by its named owner** (§5.6) and, for launches and claim changes, by the Oversight Board
(§5.7). Any single No-Go blocks the release.

| # | Gate | Pass criterion | Owner | Ref |
|---|------|----------------|-------|-----|
| G1 | **Validation** | Prospective calibration meets target for the deployed population; no un-validated claim | Clinical + Technical | Doc 13; Doc 01 §3.1 |
| G2 | **Safety — critical cascade** | Critical cascade, crit cap, hard floors verified by test suite (incl. K⁺ / SpO2 / suicidality cases) | Clinical | Doc 03 §4–§5; Doc 02 MCS |
| G3 | **Safety — escalation** | emergency/urgent/routine pathways fire, acknowledge, and re-prompt; crisis pathway exercised | Clinical | §2.1–§2.3 |
| G4 | **Fail-safe defaults** | No false-reassurance path; low-coverage caps green; uncertainty ⇒ caution proven | Clinical + Technical | §2.4; README §5.4 |
| G5 | **Regulatory positioning** | Wellness/CDS classification confirmed for this config; clinician layer gated; counsel sign-off | Privacy/Legal + Clinical | §1 |
| G6 | **Claims review** | Every patient/market claim general-wellness, evidence-bounded, no overpromise | Oversight Board | §7.3; §1.4 |
| G7 | **Fairness** | No protected-class proxy; calibration/error parity across subgroups within tolerance | Fairness + Technical | §6; Doc 13; Doc 19 |
| G8 | **Privacy & security** | DPIA/risk assessment done; encryption, RBAC, audit, consent in place; genetic/reproductive firewalls verified; breach plan live | Privacy/Security | §4 |
| G9 | **Governance / MLOps** | Versioning, change control, monitoring, rollback, incident process, **model card** published; owners named | Technical | §5 |
| G10 | **FMEA clearance** | No S=5 failure mode without a green-status mitigation; register current | Clinical + Technical | §3 |
| G11 | **Explainability** | Every shipped score emits the Doc 03 §7 object; clinician CDS inspectable | Technical + Clinical | §7.1; Doc 10 §4 |
| G12 | **Override & appeal** | Human-override and appeal/contest routes live and logged | Clinical + Privacy | §2.5; §6.4 |

**Default is No-Go.** Gates are re-run on every release; a previously-green gate does not carry over
across a version bump.

---

*Cross-references: README §5 (hard non-negotiables); Doc 01 §3.1 (anti-Babylon), §4 (no
"impossible to lose money"); Doc 02 (critical markers, MCS suicidality rule); Doc 03 §4–§5 (critical
cascade, escalation tags, crit cap), §6 (hysteresis), §7 (explainability), §8 (versioned constants);
Doc 04 (reservoirs, aging/genetic burdens); Doc 08 (reproductive/sex-specific data and red-flags);
Doc 09 (acute-mode); Doc 11 (nudge safety); Doc 10 (gated clinician CDS layer, OOD, inspectability);
Doc 13 (cohorts, calibration, fairness, drift, validation gates); Doc 19 (actuarial/pricing
firewall, GINA, fairness flags).*
