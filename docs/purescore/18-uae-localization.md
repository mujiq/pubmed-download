# 18 — UAE Localization (Ethnicity-Aware Frames, Regional Epidemiology, Ramadan)

**Care delivery:** the UAE operational specifics — IFHAS screening, Malaffi EMR coding, Ramadan dosing, BAME/South-Asian cuts — localise the [condition care pathways](care-pathways.html).

> Binding conventions: `README.md §3`. This document localizes PureScore for the **United Arab
> Emirates** population: ethnicity-aware reference frames and screening (decision **D18** — *context
> & screening, never penalty*), regionally prevalent disease/medication priorities, heritable-condition
> screening, near-universal vitamin-D deficiency, and **Ramadan-fasting** safety. It binds bands/rules
> to the evidence registry (Doc 15; D19) and feeds the action catalogue (Doc 12).
>
> Decisions: **D18** (ethnicity), **D15** (hormonal-milieu sex model), **D19** (evidence/provenance),
> **D21** (UAE-localization scope & Ramadan handling). **All cut-points are illustrative, guideline-
> anchored, re-verify on cadence; every red/critical still escalates to a human; no autonomous
> diagnosis (Doc 01/16).** Acute-danger anchors are never relaxed.

---

## 1. Why localize — the UAE population & burden
The UAE is **majority-expatriate**, dominated by a **South-Asian** population (Indian, Pakistani,
Bangladeshi, Sri Lankan), with **Emirati/Gulf-Arab**, other-Arab, **SE-Asian/Filipino**, Western, and
African communities. Regional burden is heavily **cardiometabolic**: among the world's highest
age-adjusted **type-2 diabetes and obesity** prevalence (IDF), early **ASCVD**, **CKD**, **NAFLD/MASLD**,
**near-universal vitamin-D deficiency**, and — with high **consanguinity** — elevated rates of
**hemoglobinopathies (thalassemia/sickle), G6PD deficiency, and familial hypercholesterolemia**.
A flat Western-default engine systematically **under-calls** risk here.

## 2. Ethnicity model — context & screening, never penalty (D18)
Ethnicity is **self-reported and optional**. It changes the **reference frame and screening**, never
the score's penalty; absence lowers **Confidence**, never the score (D7).

| Use | Mechanism | Evidence |
|---|---|---|
| **Cut-points** | UAE-prevalent higher-risk groups (South-Asian, SE-Asian/Filipino, Gulf/other-Arab) use **WHO Asian BMI** (overweight ≥23, obese ≥27.5) and **IDF ethnic-specific waist** (♂ ≥90, ♀ ≥80 cm) | `EVD-BCM-BMI-WAIST-001` (WHO 2004; IDF) |
| **Risk equation** | ASCVD/pooled-cohort **under-predicts** in South-Asian/Gulf ancestry → use an **ethnicity-aware equation (QRISK3)**; consider earlier statin/screening | `EVD-ETHN-ASCVD-001` |
| **Screening** | raise screening for high-prevalence heritable conditions (§3) | `EVD-GEN-*`, `EVD-UAE-*` |
| **eGFR** | **race-free CKD-EPI 2021** — no race coefficient, ever | `EVD-REN-EGFR-RACEFREE-002` |
| **Absent ethnicity** | standard cut-points + **lower Confidence**, no penalty | D7/D18 |

**Hard rule (Doc 19/16):** ethnicity, nationality, or migration status may **never** worsen a score,
price, or access. The engine has *no* ethnicity term in any risk multiplier; it only selects which
*validated, biology-based* reference applies and what to *screen*.

## 3. Heritable-condition screening (high regional relevance)
| Condition | Trigger | Action | Evidence |
|---|---|---|---|
| **G6PD deficiency** | Gulf-Arab / South-Asian ancestry, before oxidant drugs | screen; **avoid oxidant triggers** (primaquine, rasburicase, dapsone, some sulfonamides, fava) | `EVD-GEN-G6PD-001` |
| **Thalassemia / hemoglobinopathy** | regional ancestry; **UAE mandatory premarital screening** | interpret Hgb/MCV with carrier status; counsel | `EVD-GEN-THAL-002` |
| **Familial hypercholesterolemia** | high ApoB/LDL, family history, consanguinity (incl. homozygous FH) | **cascade screening**; early lipid clinic | `EVD-GEN-FH-003` |
| **Consanguinity-linked recessive** | Gulf-Arab, known consanguinity | genetic counselling route | `EVD-GEN-THAL-002` |

Screening flags are **recommendations routed to a clinician**, never autonomous diagnoses, and never
score penalties.

## 4. Vitamin D
Vitamin-D deficiency is **highly prevalent across UAE ages/sexes** (clothing, indoor lifestyle, heat
avoidance) despite abundant sun. The engine raises vitamin-D **measurement + repletion** salience for
all UAE cohorts (`EVD-NUT-VITD-001`, `EVD-UAE-VITD-003`).

## 5. Ramadan-aware logic (IDF-DAR)
When `ramadan = fasting`, the engine applies **IDF-DAR** practical guidance (`EVD-UAE-RAMADAN-002`):

- **Risk stratification** for fasting in diabetes (very-high/high/moderate/low); **very-high/high risk
  → fasting medically inadvisable** → clinician route.
- **Medication timing:** shift glucose-lowering agents toward **Iftar/Suhoor**; dose adjustments are
  clinician-set; **SGLT2i caution** (dehydration / euglycemic DKA) — hold if at risk.
- **Hydration:** front-load fluids **Iftar→Suhoor**; avoid strenuous daytime activity.
- **Safety override (absolute):** **break the fast** if blood glucose **<70 mg/dL** (or >300, or
  hypo/dehydration/DKA symptoms) — *medically and religiously permitted*. This is a hard safety rule,
  not a nudge.
- Nudge tone and timing adapt (no daytime caloric/exercise nudges that conflict with the fast).

## 6. Prevalent diseases & medications (priority map → Doc 12)
| Priority | Conditions | Prevalent medications |
|---|---|---|
| Very high | T2D / prediabetes, obesity/metabolic syndrome, dyslipidemia/ASCVD, vitamin-D deficiency | metformin, **SGLT2i, GLP-1 RA**, statins/ezetimibe, ACE-i/ARB |
| High | hypertension, CKD, NAFLD/MASLD, hemoglobinopathy/anemia | CCB, diuretics, β-blockers, insulin, DPP-4i, iron |
| Notable | thyroid, gout/hyperuricemia, OSA, depression/anxiety, asthma/COPD (sandstorm/AQ), osteoporosis | levothyroxine, urate-lowering, SSRIs, inhalers, vitamin-D/Ca, antiplatelets, PPIs |

The full, evidence-tagged action set per condition is **Doc 12**.

## 7. Guideline bodies tracked (registry)
International (ADA, ACC/AHA, ESC/EAS/ESH, KDIGO, WHO, IDF, Endocrine Society, ATA, AASLD/EASL, NICE,
USPSTF, ACSM, EWGSOP2, WPATH, STRAW+10, ACOG, IADPSG) **and UAE/regional**: **MoHAP, DHA,
DoH-Abu-Dhabi, Emirates Cardiac Society, Emirates Diabetes & Endocrine Society, IDF-DAR**, and the
UAE **premarital genetic-screening program**. The external crawler (Doc 15 §6) keeps these current
under the human-in-loop gate (D20).

## 8. Equity & data protection
- Ethnicity, nationality, genetic-carrier, and pregnancy data are **sensitive**: strictest access
  controls, **excluded from the actuarial/payer layer** (Doc 19), never used to worsen access/pricing
  (README §5.5; Doc 16). Data-minimization — collected only when it improves care.
- Under-represented groups lower **Confidence/representativeness** (D7) and **suppress unreliable
  cohort percentiles** — they never fabricate certainty or penalty.

## 9. Implementation status (calculator)
- Header **Ethnicity** + **Ramadan** selectors → `CTX.ethnicity`, `CTX.ramadan`.
- `ethTier()` applies **WHO Asian BMI / IDF waist** cut-points for the higher-risk groups in `band()`;
  eGFR stays race-free; ethnicity adjusts only **Confidence** (`ethRep()`), never penalty.
- **Localization readout**: cut-point frame, race-free-eGFR note, ethnicity-aware risk-equation note,
  raised **screening** list, and the **Ramadan-IDF-DAR** safety advisory.
- Personas: `southasian` (WHO Asian cut-points flag cardiometabolic risk a normal-range BMI misses) and
  `ramadan_dm` (Gulf-Arab T2D fasting → med-timing/hypoglycemia safety).
- The production system applies the full §3/§6 sets across the Doc 02 catalogue; numbers are
  illustrative pending clinical sign-off + Doc 14 validation (the Babylon lesson, Doc 01).
