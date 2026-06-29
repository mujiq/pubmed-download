# 17 — Clinician Red-Team & Blind-Spot Register

> Binding conventions: `README.md §3`. This is the project's **adversarial self-review**: the
> objections a sceptical clinician, statistician, ethicist, or regulator would raise, mapped to
> **how the design handles each** and the **honest residual**. It is a *living* register — every new
> attack vector is added, not argued away. It is the consolidated answer to "where can this engine be
> poked?" Nothing here claims validation; the strongest defence is bounded scope + human-in-loop +
> evidence provenance + defensive uncertainty + the discipline that this is **design, not evidence**
> (Doc 01, the Babylon lesson).

Each row: **Attack** → **Handled by** → **Residual (honest)**.

---

## A. Statistical validity & calibration
| Attack | Handled by | Residual |
|---|---|---|
| "Your number isn't a calibrated probability." | Correct — PureScore is a **wellness index**, not a probability. Doc 14 mandates isotonic/Platt **recalibration** before any risk read; the executable harness shows raw ECE→recalibrated ECE. | Recalibration must be fit on **real** outcomes; synthetic ≠ evidence. |
| "Where's the validation?" | Doc 14 gates (discrimination/calibration/early-warning PPV/fairness/drift) with hard pass/fail; release-gate + shadow-mode (Doc 16). | **No gate is satisfied yet** — needs prospective linked cohorts. Stated everywhere. |
| "Weights/bands are made up." | Every band/rule binds to an **evidence ID** (Doc 15); CI fails on an uncited band. | Numeric values illustrative pending clinical sign-off. |

## B. Race/ethnicity & equity
| Attack | Handled by | Residual |
|---|---|---|
| "You use race in eGFR." | **No.** Race-free CKD-EPI 2021 (`EVD-REN-EGFR-RACEFREE-002`), which explicitly supersedes the retracted race-coefficient. | — |
| "Ethnicity penalizes minorities." | D18: ethnicity adjusts **reference frame & screening only**; **no ethnicity term in any risk multiplier**; absence → Confidence↓, never penalty; excluded from the payer layer (Doc 19/16). | Gulf-Arab cut-point evidence is **emerging** (vs well-validated South-Asian) — flagged, conservative. |
| "Ignoring ethnicity is also harmful." | We do **not** ignore it — WHO Asian BMI/IDF waist + heritable screening (G6PD, thalassemia, FH) for the South-Asian-majority UAE population (Doc 18). | Live calculator applies cut-points to the BMI/waist subset; full catalogue in Doc 18/12. |

## C. Medication & physiologic confounding
| Attack | Handled by | Residual |
|---|---|---|
| "β-blocker makes my athlete look unfit." | Confounder **down-weighting** + confidence reduction (D3); `EVD-MED-BB-CONFOUND-002`. | Confound table is seeded, not exhaustive. |
| "A statin-controlled ApoB reads 'cured'." | **Managed** state — green-but-tagged, disease weight retained, burden reservoir not zeroed (D3). | — |
| "SGLT2i eGFR dip / ACE-i K⁺ rise fires a false emergency." | Drug-**expected** envelope → Watch not Alert (Doc 05 §3.2; `EVD-MED-RAASI-001`). | Envelope encoded for common classes; expand via registry. |

## D. Alarm fatigue & false criticals
| Attack | Handled by | Residual |
|---|---|---|
| "A hemolyzed K⁺ will trigger a crisis." | Confirmation logic (D14): isolated **implausible/low-quality** critical → **reconfirm**, not cascade; a *confirmed* critical still fires. | Plausibility/device-quality inputs needed from the data layer. |
| "Early-warning will bury people in alerts." | Tiered **Watch→Advisory→Alert** (D2); member sees only multivariate/confirmed signals; Doc 14 enforces **PPV-at-realistic-prevalence** + an alarm budget (the Babylon false-positive lesson). | Thresholds need prevalence-specific calibration. |
| "Worst-sensitive aggregation hides diffuse risk / or over-reacts." | Critical **cascade** + independent **Criticality badge** distinguish "40 from one red" vs "40 from many yellows"; companion vector carries the decomposition. | — |

## E. Personalization & the cold start
| Attack | Handled by | Residual |
|---|---|---|
| "Personal baselines will over-fit noise." | Ignition model (D19): `θ_pers` gated by **volatility/coverage**; empirical-Bayes shrinkage (D6/D9); **clinical anchor floors the safety side** and is never discarded. | Shrinkage constants need calibration. |
| "A new user with no data gets a bad score." | **Crank stage** = guideline anchor + cohort prior; Confidence/sufficiency flagged low; provisional. | — |
| "Personal tightening could hide a real deviation." | Anchors and **acute-danger thresholds are absolute**, independent of `θ` (Doc 05 §3.4). | — |

## F. Sex, pregnancy, gender
| Attack | Handled by | Residual |
|---|---|---|
| "Binary sex mis-scores trans/pregnancy/menopause." | Hormonal-milieu model (D15, Doc 08): natal-sex baseline + gonadal-milieu override + life-stage modifiers; race-/sex-appropriate references. | Trans/intersex **cohort data sparse** → Confidence lowered honestly, not faked. |
| "Pregnancy masks pre-eclampsia/GDM." | Trimester frames shift physiologic bands **but pre-eclampsia/GDM are absolute anchors** (Doc 08 §2.3.4; `EVD-PREG-PREE-001`). | — |

## G. Age, multimorbidity, actionability
| Attack | Handled by | Residual |
|---|---|---|
| "You'll alarm healthy 85-year-olds / under-treat them." | Hybrid age-bands + special frames + **actionability/expected-benefit modifier** (D4); J-curve for frail BP. | Expected-benefit weighting needs outcome data. |
| "Multimorbid scores are demoralizing & non-actionable." | **Dual framing** (D5): absolute + progress-to-attainable-best + competing-risk muting. | — |
| "Genetic risk (FH/Lp(a)) framed as personal failure." | **Modifiability** dimension; lifestyle nudges **gated** on fixed markers → route to clinician/medication (D16). | — |

## H. Data quality, units, missingness, skew
| Attack | Handled by | Residual |
|---|---|---|
| "A unit error (mmol↔mg/dL) breaks it." | Plausibility gate + reconfirm (D14); Doc 14 robustness suite specifies unit-error/out-of-range detection. | Unit-detection **specified, not yet built** in the live tool. |
| "Heavy-tailed markers (CRP, ferritin) give false anomalies." | **Log-scale** personal baseline for skew-tagged markers (D13/Doc 05 §7). | — |
| "Imputed markers masquerade as measured." | **Data-sufficiency** flag + coverage-weighted Confidence; MEASURE nudges close the gap. | — |

## I. Governance, scope, explainability
| Attack | Handled by | Residual |
|---|---|---|
| "This is unlicensed diagnosis." | **Wellness-grade, clinician-in-the-loop**; every red/critical escalates to a human; **no autonomous diagnosis** (README §5; Doc 16). | — |
| "It's a black box." | Full marker→pillar→score **decomposition**, evidence IDs, provenance/ignition readout, attributed nudges. | — |
| "Guidelines change; you'll go stale." | Evidence registry + **crawler with human-in-loop gate** (D20); a band change is a version bump → re-validate. | Crawler/CI **specified, not built**. |
| "Could be used to deny insurance / price by health." | Hard firewall: protected attributes and the wellness score **never** worsen access/pricing; actuarial layer separated & gated (Doc 19/16). | Enforcement is governance, not code, in this design. |

## J. Model realism
| Attack | Handled by | Residual |
|---|---|---|
| "Reservoir dynamics are hand-wavy." | MONIAC reservoirs give *memory*/latency realism (Doc 04); honest about near-permanent burdens (no false hope). | Coupling matrix `κ`, decay `λ` need **empirical calibration**. |
| "Pediatrics?" | **Explicitly out of scope** (gated, not scored on adult bands) — Doc 08. | Pediatric frames are future work. |
| "Mental-health safety?" | **Hard suicidality rule** (PHQ-9 item-9 / EPDS item-10) forces MCS critical + crisis escalation, never averaged away. | — |

---

## K. Consolidated honest residuals (the things that are genuinely *not done*)
1. **No prospective validation** — every gate in Doc 14 is unmet; all bands are design, not evidence.
2. **Live tool covers a marker subset** — the full Doc 02/18 catalogue (ferritin, hematocrit, urate,
   HDL, creatinine/eGFR, BMD, …) and all ethnicity cut-points are specified, not all wired live.
3. **Crawler, CI evidence-completeness check, and unit-error detection are specified, not built.**
4. **Calibration of constants** (shrinkage k, reservoir κ/λ, anomaly thresholds, expected-benefit
   weights) awaits real cohort data.
5. **Sparse cohorts** (trans/intersex, some UAE subgroups) → honestly lower Confidence, not fabricated.
6. **Some ethnicity cut-points** (Gulf-Arab) rest on emerging rather than settled evidence — flagged.

The engine is defensible **because** it states these plainly, bounds its claims, keeps a human in the
loop on every critical and every guideline change, and binds every number to a citation that an
auditor can challenge. A score it cannot justify, it does not assert.
