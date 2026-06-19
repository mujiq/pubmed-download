# PureScore Question-Bank — canonical schema & vocabulary (v1)

This is the **contract** every question-bank chunk must follow so the chunks merge
cleanly and render in the wiki. All values are *illustrative, literature-anchored*
and carry `"flag":"illustrative-reverify"` (README §5.6) — they are NOT validated.

A chunk file is a JSON object:

```json
{ "domain": "<DOMAIN_ID>", "questions": [ <question>, … ] }
```

---

## Controlled vocabularies (use these EXACT tokens)

**Pillars (12):** `CV` `MET` `REN` `HEP` `INF` `HEM` `ENDO` `BCM` `NUT` `SLP` `FIT` `MCS`
- CV cardiovascular/vascular · MET metabolic/glycemic · REN renal · HEP hepatic ·
  INF inflammation/immune · HEM hematologic/oxygen · ENDO endocrine/hormonal ·
  BCM body-composition/musculoskeletal · NUT nutrition/micronutrient ·
  SLP sleep/circadian · FIT fitness/cardiorespiratory · MCS mental/cognitive/social

**Reservoirs (15)** — MONIAC stocks (Doc 04). Two polarities:
- *Burden* reservoirs (higher = worse): `atherogenic_burden` `vascular_bp_load`
  `glycemic_burden` `adiposity` `hepatic_fat` `inflammatory_load` `allostatic_load`
  `sleep_debt`
- *Reserve* reservoirs (higher = better): `cardiorespiratory_reserve` `renal_reserve`
  `oxygen_delivery_reserve` `iron_reserve` `bone_reserve` `muscle_reserve`
  `micronutrient_reserve`

**Sign convention for numeric weights**
- `pillars`: signed float, `+` = *raises pillar risk*, `−` = *protective*. Magnitude bands:
  strong `0.20–0.30` · moderate `0.10–0.18` · weak `0.03–0.08` · none `0`.
- `reservoirs`: signed float, `+` = *adds to that stock*, `−` = *drains it*.
  (So `inflammatory_load:+0.1` is bad; `micronutrient_reserve:+0.1` is good;
  `cardiorespiratory_reserve:-0.1` is bad.) Use `0.05 / 0.10 / 0.20` tiers.

**direction:** `increase_risk` · `decrease_risk` · `neutral`
**magnitude:** `strong` · `moderate` · `weak` · `none`

**axis_tags** (which lifestyle axes the question informs — for filtering & persona):
`activity` `diet` `sleep` `substance` `stress` `social` `lifestage` `condition_load`
`metabolic` `environment` `occupation` `anthropometric` `genetic_familial` `cognition`
`reproductive` `aesthetic` `pain`

**question type:** `single_select` · `multi_select` · `boolean` · `numeric` · `scale` · `composite`

**cadence:** `Core` (every visit) · `quarterly` · `annual` · `biennial` · `once` · `on-trigger`
**refresh:** `static` (immutable: ethnicity, prenatal, childhood) · `slow` (annual/biennial:
education, family history) · `periodic` (quarterly/annual lifestyle) · `dynamic`
(Core/on-trigger symptoms & behaviours) · `event` (fires on clinical trigger)

**priority:** integer `1`–`5` (1 = ask first / highest yield, 5 = peripheral).
Heuristic: lower number when it informs many or *critical* conditions, is highly
modifiable, or gates branching.

**perceived_actual.kind:** `anchor` (immutable demographic) · `perceived`
(self-report belief/symptom — can be wrong) · `actual` (objective self-report fact)
**corroborated_by:** array of objective signals that confirm/contradict the answer,
e.g. `"lab.HbA1c"`, `"lab.omega3_index"`, `"wearable.MVPA"`, `"wearable.sleep_duration"`,
`"lab.ALT"`, `"bcm.waist"`. Used to compute the **perceived-vs-actual gap**.

---

## Question object (all fields required unless marked optional)

```json
{
  "id": "Q_DIET_FISH",                       // UNIQUE, UPPER_SNAKE, prefix Q_<DOMAIN>_
  "text": "How often do you eat fish?",
  "category": "Diet & Nutrition",            // human-readable theme
  "domain": "NUTRITION_GI",                  // = chunk domain id
  "conditions": ["acne","longevity","atherosclerosis","cognitive_decline"], // conditions informed
  "stream": "LIFE",                          // LIFE | GOAL (almost always LIFE)
  "type": "single_select",
  "responses": [
    { "label": "Never/Rarely",
      "direction": "increase_risk", "magnitude": "moderate",
      "pillars": { "NUT": 0.15, "CV": 0.08, "MCS": 0.05 },
      "reservoirs": { "micronutrient_reserve": -0.10, "inflammatory_load": 0.08 } },
    { "label": "1–2×/week",
      "direction": "neutral", "magnitude": "none", "pillars": {}, "reservoirs": {} },
    { "label": "3+×/week",
      "direction": "decrease_risk", "magnitude": "moderate",
      "pillars": { "NUT": -0.12, "CV": -0.06 },
      "reservoirs": { "micronutrient_reserve": 0.10, "inflammatory_load": -0.06 } }
  ],
  "dimensions": {
    "age": "all",                            // "all" or {"min":18,"max":120}
    "sex": ["all"],                          // ["all"] | ["male"] | ["female"]
    "axis_tags": ["diet"],
    "personas": ["all"]                       // ["all"] or specific persona keys
  },
  "dependencies": {
    "show_if": [],                           // [{"q":"Q_SEX","in":["female"]}] — display gate
    "skip_if": [],
    "triggers": [],                          // [{"if_response":"Never/Rarely","ask":"Q_..."}]
    "validated_by": "DIET"                    // optional: id of a validated PRO that supersedes this crude item
  },
  "priority": 2,
  "cadence": "annual",
  "refresh": "periodic",
  "perceived_actual": { "kind": "perceived", "corroborated_by": ["lab.omega3_index"] },
  "source": "OMICS Introductory Report; PREDIMED",
  "flag": "illustrative-reverify"
}
```

### Rules
1. **Reuse, don't duplicate** core/global questions. Demographics, anthropometrics,
   smoking, alcohol, family-history-generic, prenatal, childhood-adversity,
   environment/occupation, and self-perception live in `qb-00-core.json`
   (ids `Q_CORE_*`). If your domain needs one, reference its id in a `triggers`/`show_if`
   instead of re-defining it. Only define questions specific to YOUR domain's conditions.
2. Every `single_select`/`scale` response needs direction+magnitude+pillars+reservoirs
   (use `{}` and `magnitude:"none"` for the neutral option).
3. Keep weights internally consistent: a "strong increase" answer should have larger
   magnitudes than a "weak" one; protective answers are negative.
4. Map each question to the **pillar(s) it actually drives** — most map to 1–3 pillars.
5. Prefer `validated_by` links when a crude lifestyle item is really a rough proxy for a
   validated PRO already in the wiki: `PHQ-9 PHQ-2 GAD-7 GAD-2 AUDIT-C PSS-4 UCLA-3
   WHO-5 ISI IPAQ-SF DIET SUBSTANCE`.
6. Aim for ~25–32 questions per domain, grouped to cover that domain's conditions.
7. Output **valid JSON only** to your target file. No comments, no trailing commas.
