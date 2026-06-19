# 14 — Evidence Registry, Provenance & the Cold-Start "Ignition" Model

> Binding conventions: `README.md §3`. This document defines how **every band, threshold, critical
> rule, and recommended action in the engine is bound to its clinical evidence**, how that evidence
> is kept current by an external crawler under a human-in-loop gate, and how an effective reference
> range **ignites from a clinical anchor and warms into a personalized baseline over time**. It
> operationalizes the maintainability requirement: *the rules will change; the engine must track
> them without silent drift.*
>
> Decisions: **D19** (registry + ignition provenance), **D20** (crawler change-control gate), with
> **D6** (empirical-Bayes personal baseline), **D9** (shrinkage/validation), **D7** (representativeness),
> **D18** (ethnicity context). Machine-readable registry: `assets/evidence-registry.json`.
>
> **Honesty:** the registry is *seeded* from authoritative bodies; URLs/DOIs and currency are
> maintained by the crawler (§6). No band here is evidence until clinically signed off and validated
> (Doc 13). Nothing relaxes the hard non-negotiables (README §5).

---

## 1. Why an evidence layer exists (separation of concerns)

The **engine** computes; the **evidence registry** justifies. They are deliberately decoupled:

- A clinician/auditor can ask of *any* number on a card — "says who, and from when?" — and get a
  citation with body, year, version, and `last_verified`.
- When ADA/KDIGO/ESC/WHO (or UAE: MoHAP/DHA/DoH-Abu-Dhabi/Emirates Cardiac Society) revise a
  threshold, the change lands in **one place** (the registry), is **reviewed**, and re-validates —
  not scattered across code.
- The engine references **evidence IDs**, never inlined prose. A band with no evidence ID is a bug.

## 2. The "diesel ignition" provenance model

A reference range is not static and is not purely personal. It **ignites cold on guideline evidence
and warms into a personalized baseline** as the patient's own data accrues — exactly how a diesel is
cranked with glow-plugs/starter and then runs on its own combustion.

| Stage | "Engine" analogue | PureScore source | When it dominates |
|---|---|---|---|
| **Crank** | glow-plug + starter | **Clinical guideline anchor** (registry band) | cold start: no/О little patient data |
| **Warm-up** | idling on starter assist | **Real-world cohort range** (NHANES/UK-Biobank-class + local cohort percentile) | some cohort data; sparse personal data |
| **Running** | self-sustaining combustion | **Personal baseline** (z-score vs the patient's own history) | personal series established (Doc 12 §5) |

The **effective optimum/center** used for interpretation and anomaly detection is a weighted blend:

```
 effRef = θ_clin·anchor_clin  +  θ_coh·range_coh  +  θ_pers·baseline_pers
 θ_clin + θ_coh + θ_pers = 1
```

with weights that **shift over time by data accrual** (empirical-Bayes, D6/D9):

```
 θ_pers = n_pers /(n_pers + k_pers)         # personal evidence grows with measurements
 θ_coh  = (1−θ_pers)· n_coh /(n_coh + k_coh)
 θ_clin = (1−θ_pers)·(1 − n_coh/(n_coh+k_coh))   # the guideline anchor is never fully discarded
```

**Safety invariants (why the anchor never fully leaves):**
- The **clinical anchor floors the safety side**: a personal baseline can *tighten* a band but may
  **not move a critical/acute-danger anchor** (K⁺, SpO₂, glucose extremes) — those stay absolute
  regardless of `θ` (Doc 03 §4, Doc 12 §3.4). The crank is always available.
- `θ_pers` is gated by **personal-baseline quality** (volatility, coverage); a noisy baseline keeps
  more weight on the cohort/anchor (prevents over-fitting to noise).
- Low **representativeness** (D7) suppresses `θ_coh` (`φ→0`) — don't warm up on an ill-fitting cohort;
  fall back to anchor + personal.

This is the single mechanism behind several earlier decisions: D6 (EB blend), D9 (shrinkage), and the
calculator's personal-baseline z-scores are all **the same ignition curve** viewed from different docs.

## 3. The per-band provenance object

Every interpreted quantity carries provenance so the card and the auditor see the full lineage:

```json
{
  "marker": "apob",
  "evidence_ids": ["EVD-CVD-APOB-001"],
  "anchor_clin": { "opt_hi": 79, "unit": "mg/dL" },
  "range_coh":   { "p50": 88, "p20_p80": [70, 110], "cohort": "GULF-SOUTHASIAN-M-40s", "n": 2140 },
  "baseline_pers": { "mu": 92, "sd": 6, "n": 7 },
  "theta": { "clin": 0.55, "coh": 0.28, "pers": 0.17 },
  "effRef": 81.4,
  "stage": "warm-up",
  "confidence": 0.61
}
```

The companion-vector **Confidence** (Doc 12 §4) is partly a read-out of `θ` quality and evidence grade;
**Representativeness** gates `θ_coh`; **Volatility** gates `θ_pers`.

## 4. Evidence registry schema (`assets/evidence-registry.json`)

| Field | Meaning |
|---|---|
| `id` | stable key `EVD-<DOMAIN>-<SHORT>-<n>` (e.g. `EVD-REN-EGFR-RACEFREE-002`) — **never reused/renumbered** |
| `domain` | CVD, MET, REN, HEP, INFL, HEM, ENDO, BCM, NUT, SLP, FIT, MCS, PREG, SEX, ETHN, GEN, MED, UAE |
| `claim` | one-line statement of what the evidence supports |
| `bodies` | issuing guideline body/bodies |
| `title` | guideline/paper title |
| `year`, `version` | edition/year; version string |
| `jurisdiction` | `["US","EU","INTL","UAE",…]` |
| `grade` | evidence strength (A/B/C or GRADE) → feeds `ε_a`/Confidence |
| `applies_to` | engine objects this governs: `marker:…`, `rule:…`, `band:…`, `equation:…`, `action:…`, `cohort:…` |
| `url`, `doi` | canonical source (crawler-maintained; may be null at seed) |
| `last_verified` | ISO date the crawler last confirmed the source (null = seed, unverified) |
| `review_status` | `seed` → `verified` → `change-proposed` → `approved` → `superseded` |
| `supersedes` / `superseded_by` | version lineage |

ID convention is append-only: a superseded guideline is **kept** (audit trail) and linked, never deleted.

## 5. How the engine references evidence
- Each **marker band** (Doc 02), **critical rule** (Doc 03 §4), **sex/stage modifier** (Doc 05),
  **risk-equation choice** (Doc 08), and **action** (Doc 07/16) declares `evidence_ids[]`.
- A CI check (Doc 13 release gate) **fails the build if any band/rule/action lacks a resolvable
  evidence ID**, or references an `id` whose `review_status` is `change-proposed` without sign-off.
- The calculator demonstrates this: representative markers/actions carry `evd` arrays surfaced in the
  pillar breakdown and the provenance readout.

## 6. The external-crawler contract (D20)
The crawler keeps the registry **current**; it never silently changes the engine.

```
 (1) MONITOR   each registry source (body guideline index, DOI, URL) on a cadence (default 30 d).
 (2) DETECT    new edition / changed threshold / dead URL / superseding statement / retraction.
 (3) STAGE     write a change-proposal: evidence diff, old→new value, affected applies_to[], severity.
 (4) GATE      a clinician/governance reviewer MUST approve (Doc 11). review_status: change-proposed→approved.
 (5) APPLY     approval = model-version bump → re-run Doc 13 validation + fairness → ship via shadow→prod.
 (6) REFRESH   last_verified updates automatically every cycle; VALUES never auto-change (safety).
```

- **Auto-allowed without review:** `last_verified` refresh, URL/DOI repair, metadata (title typo).
- **Never auto-applied:** any change to a `value`, `band`, `rule`, `grade`, or `applies_to` — these
  are safety-relevant and require human sign-off + re-validation (D20).
- **Dead source / retraction** raises the affected bands' uncertainty (Confidence ↓) and alerts
  governance until re-anchored.

## 7. Coverage — bodies tracked
International: **ADA, ACC/AHA, ESC/EAS/ESH, KDIGO, WHO, IDF, Endocrine Society, ATA, AASLD/EASL,
NICE, USPSTF, ACSM, WPATH, STRAW+10, ACOG, IADPSG**. UAE/regional: **MoHAP** (Ministry of Health &
Prevention), **DHA** (Dubai Health Authority), **DoH-Abu-Dhabi**, **Emirates Cardiac Society**,
**Emirates Diabetes & Endocrine Society**, **IDF-DAR** (Diabetes & Ramadan), UAE **premarital genetic
screening** program. (Seed set in `assets/evidence-registry.json`; the crawler expands/verifies it.)

## 8. Implementation status
- `assets/evidence-registry.json` — seeded registry (this commit) covering the engine's current
  bands/rules + UAE-specific entries; `review_status:"seed"`, `last_verified:null` pending crawler.
- Calculator — representative markers and nudges carry `evd` IDs; the pillar breakdown shows the
  governing citation and the **ignition stage** (crank/warm-up/running) for the selected pillar.
- CI evidence-completeness check and the live crawler are **specified, not yet built** (design, not
  evidence — Doc 00).
