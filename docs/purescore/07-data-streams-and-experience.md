# 07 — Data Streams, Devices & the Effortless Experience

> Binding conventions: `README.md §3`. This document defines the **input taxonomy** (lab biomarkers,
> wearable metrics, user goals, lifestyle/PRO), how **consumer wearables are trust-tiered** into the
> engine (decision **D22**), how wearable signals map across pillars, and the **product surfaces** that
> make the member journey effortless (AI scribe, AI chat agent, scheduling, in-home tracking,
> insurance auth, household accounts). It feeds the companion vector (Doc 05 §4) and the nudge engine
> (Doc 11/12). **Illustrative; consumer-grade ≠ medical-grade — flagged, not assumed (Doc 01).**

Decisions: **D22** (wearable tiering), D1 (output model), D6/D9 (baselines/shrinkage), D16 (nudges),
D18/D21 (UAE), D14 (reconfirm). Companion **Confidence** carries every reliability flag.

---

## 1. Four data streams — flagged by type and trust
| Flag | Stream | Reliability | Examples (most used) | Primary use |
|---|---|---|---|---|
| 🧪 **LAB** | Lab biomarker | clinical-grade | HbA1c, ApoB, eGFR, TSH, hsCRP, ferritin | **anchors the score's clinical bands** |
| ⌚ **WEAR** | Wearable metric | **tiered (D22)** | resting HR, HRV, SpO₂, sleep, steps, CGM glucose | trajectory + early-warning; **device-weighted** into score |
| 🎯 **GOAL** | User goal | declared intent | "lower HbA1c", "sleep better", "fertility" | steers pillar weights & nudge ranking |
| 📝 **LIFE** | Lifestyle / PRO | self-report | diet, alcohol, shisha, stress, PHQ-9, GAD-7 | fills pillars without labs (lower confidence) |

Every value the engine stores carries its **stream flag, source/device, timestamp, and a reliability
tier** — surfaced in the breakdown and folded into Confidence.

## 2. Wearable trust tiering (D22)
Consumer wearables are rich but not uniformly trustworthy. They **fully power trajectory,
early-warning, and nudges**, and they **feed the score weighted by device grade**:

| Tier | What | Score weight | Can it trigger…? |
|---|---|---|---|
| **Clinical-grade** | CGM, validated BP cuff, single-lead ECG/AFib | ≈ lab weight | Watch → Alert (with confirmation) |
| **Consumer-validated** | resting HR, steps, sleep duration, HR zones | discounted | Watch / Advisory |
| **Inferential / derived** | "readiness", "stress", sleep stages, VO₂max est. | **informational only** | nothing critical — context only |

**Hard rule:** a wearable-only anomaly may raise **Watch/Advisory**, but a **clinical-grade
confirmation is required before it drives a red/critical** (ties to the D14 reconfirm path). An
inferred metric never sets a band.

## 3. Individual → aggregated → derived
| Layer | Definition | Examples |
|---|---|---|
| **Individual (raw)** | instantaneous signal | beat-to-beat HR, SpO₂, skin temp, accelerometer, CGM every 5 min |
| **Aggregated (summary)** | daily/weekly rollup | resting HR, sleep duration/efficiency, CGM time-in-range, weekly MVPA, daily steps |
| **Derived (inferred)** | model estimate | recovery/readiness, stress, sleep stages, VO₂max estimate, respiratory rate, rhythm alerts |

Aggregation is where most clinical value lives (resting HR, TIR, sleep regularity); derived metrics
are coaching colour, never diagnostic.

## 4. Wearables across the pillars (most-frequent routing)
| Metric | Pillars it feeds |
|---|---|
| Resting HR | CV, FIT, MCS |
| HRV | CV, MCS, SLP, ENDO |
| SpO₂ | HEM |
| Steps / MVPA | FIT, MET, CV |
| VO₂max (est.) | FIT, CV |
| Sleep dur/eff/reg | SLP, MCS |
| CGM glucose / TIR | MET |
| Skin temp | ENDO (cycle), INF (illness) |
| BP (cuff) | CV, REN |
| Weight / body-comp | BCM, MET |
| ECG / rhythm | CV |

Daily backbone of Trajectory & Early-warning: **resting HR, HRV, sleep, steps, CGM time-in-range.**
Devices: Apple Watch, Samsung, Oura, Whoop, CGM (Libre/Dexcom), Omron cuff, smart scale, in-home
passive sensors (sleep mat, motion, fall detection).

## 5. The effortless experience (reduce member load)
| Surface | What it removes |
|---|---|
| **AI scribe** | ambient visit notes → structured data; the score updates itself — no forms |
| **AI chat agent** | 24/7 conversational coach: explains the score, answers, books actions; **escalates on hard safety rules, never diagnoses** (README §5) |
| **Frictionless scheduling** | books the exact lab/clinician/scan the engine flags, at the right interval, with reminders |
| **In-home tracking** | passive scale, BP cuff, sleep mat, fall detection — care between visits, zero effort (elders/chronic) |
| **Insurance auth** | auto-prepares pre-authorization & e-claims (UAE: DHA / Shafafiya) — fewer denials, no paperwork |

All surfaces honour the firewalls: no autonomous diagnosis, payer/actuarial separation (Doc 19/16),
hard suicidality/critical rules unchanged.

## 6. Household accounts
A household holds **individual scores per member under one shared view**, with role-based access
(e.g., a caregiver monitors an elder's in-home stream). Sensitive data stays member-scoped and gated
(Doc 16); a household view never merges or cross-uses members' protected data.

## 7. Goals → recommended next actions
The engine matches each member's **declared goal** to a ranked plan (nudge engine D16 + screening +
scheduling). Examples (illustrative): *"lower HbA1c / avoid insulin"* → Ramadan-safe med timing,
cut refined carbs, CGM time-in-range target, book HbA1c+UACR; *"stay independent"* → fall-risk plan,
protein + resistance, polypharmacy/renal med review, nephrology follow-up. Goals also re-weight the
pillars the member cares about (Doc 09), within the safety rails.

## 8. Status
Design only — device integrations, the scribe/agent surfaces, and payer connectors are **specified,
not built**; wearable weights and confidence mappings await real-data calibration (Doc 14). The
honesty discipline (Doc 01) applies: consumer signals are used where they are strong and flagged
where they are weak.
