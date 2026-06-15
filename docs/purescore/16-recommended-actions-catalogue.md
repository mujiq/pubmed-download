# 16 — Recommended-Actions Catalogue (Exhaustive Nudge Library, UAE-Prioritized)

> Binding conventions: `README.md §3`. This document is the **exhaustive recommended-action /
> nudge library** that Doc 07 §1.2 calls an *excerpt* of. It supplies the per-condition, typed
> action records (same schema as Doc 07 §1.1) that the daily nudge engine filters for safety
> (Doc 07 §5), scores for impact (Doc 07 §2), and ranks by ease-weighted utility
> `U_a = [ΔPureScore_a(h*)]^α · p̂_a^β · (1−E_a)^η · ν_a` (Doc 07 §3.1). It is organized by the
> conditions and medication classes **most prevalent in the UAE**, plus screening and adherence
> actions.
>
> **Decisions.** This catalogue is governed by **D16** (nudge-engine ranking, Modifiability gate,
> impact = exact finite-difference ΔPureScore — no lookup, no false hope on fixed burden) and
> **D18** (ethnicity = *context & screening selector*, never a score penalty; absent ethnicity →
> lower confidence, never penalty; eGFR uses the race-free CKD-EPI 2021). Every red/critical state
> still escalates to a human clinician (README §5.1; Doc 07 §5.2; Doc 11). PureScore makes **no
> autonomous diagnosis** (Doc 00 §3.1; Doc 11).
>
> **Honesty mandate.** Every dose, threshold, and target below is **illustrative,
> literature/guideline-anchored as of the cited body and year, and MUST be re-verified on the fixed
> cadence (README §5.6) before any production use.** Medication actions are surfaced **only when
> the drug is prescribed** (`rx`); the engine never initiates or titrates therapy — it supports
> **adherence**, **measurement**, and **clinician follow-up**. Doses shown are the *prescribed*
> regimen the patient is to take, not a recommendation to start.

---

## 0. How to read each action row

Each row carries the Doc 07 §1.1 fields, compacted:

| Column | Meaning |
|--------|---------|
| `id` | stable action id |
| `class` | `∈ {SLEEP, NUTR, ACT, STRESS, ADHERE, MEASURE, CLINICAL, VACCINE, SCREEN}` |
| `dose` | prescribed dose/frequency `d_a` |
| `valve / target` | Doc 04 valve(s) / reservoir(s) or direct marker `i` it moves |
| `pillars` | pillars moved (binding order, Doc 02; `→` = secondary via interference `κ`) |
| `E_a` | effort/burden `∈[0,1]` (0 trivial, 1 very hard) |
| `τ_a` | latency class (immediate / days / weeks / months) |
| `ε_a` | evidence strength `∈[0,1]` + **citation tag** (guideline body + year) |
| `safe_a` | contraindication / safety tags screened in Doc 07 §5.1 |
| `cohort_a` | cohort / ethnicity / Ramadan applicability |

`VACCINE`, `SCREEN`, `MEASURE`, and `CLINICAL` are first-class (Doc 07 §1.1): "close the imputed
marker dominating your uncertainty" and "book the overdue screen" are legitimate, often
highest-yield nudges (Doc 00 §2.5 cheapest-high-yield-first; Kaiser care-gap closure). Adherence
(`ADHERE`) actions carry `ε_a` high because adherence efficacy is RCT-anchored, but the **quoted
ΔPureScore is still the exact finite-difference recompute** (Doc 07 §2.5), so an already-green
managed marker yields ~0 and the engine refuses false hope (D16).

---

## 1. Type 2 diabetes / prediabetes

Among the highest-prevalence conditions in the UAE/GCC (IDF MENA region); high consanguinity and
South-Asian/Emirati cardiometabolic phenotype raise risk at lower BMI. Marker home: **MET** pillar
(HbA1c, fasting/postprandial glucose, HOMA-IR, CGM time-in-range), with **REN/CV/HEM** linkages.

### 1.1 Glycemic lifestyle & monitoring

| id | class | dose | valve / target | pillars | E_a | τ_a | ε_a | safe_a | cohort_a |
|----|-------|------|----------------|---------|----:|-----|-----|--------|----------|
| T2D-CARB | NUTR | cut refined carb / added sugar; low-GI swaps | ↓`B_GLY` inflow; HbA1c `s_i` | MET, HEP, →CV | 0.40 | weeks | 0.8 ADA 2024 | active hypoglycemia risk on insulin/SU → pair with MEASURE | all; **Ramadan: shift carb to suhoor/iftar, avoid sugary qamar al-din at iftar** |
| T2D-STEP | ACT | +2000 steps/day; post-meal 10-min walk | fill `B_CRF`; ↓`B_GLY` (κ) | FIT, MET, →CV | 0.20 | weeks | 0.85 ADA 2024 | unstable angina, active foot ulcer | all; Ramadan: walk post-iftar, not midday heat |
| T2D-RES | ACT | 2× resistance 20 min/wk | fill `B_MUS`; insulin sensitivity | BCM, MET | 0.55 | weeks–mo | 0.7 ADA 2024 | proliferative retinopathy → avoid Valsalva-heavy lifts | all |
| T2D-CGM | MEASURE | wear CGM; target time-in-range >70% | ↑`cov_MET`; resolve glucose `r_i` | MET (uncertainty) | 0.30 | immediate | n/a IDF/ADA 2024 | none | insulin/SU users prioritized; **Ramadan: CGM is core safety tool** |
| T2D-HBA1C | MEASURE | order HbA1c (imputed/overdue) | ↑`cov_MET` | MET (uncertainty) | 0.25 | immediate | n/a ADA 2024 | hemoglobinopathy → HbA1c unreliable, use fructosamine/CGM (see §8) | thalassemia/G6PD carriers: flag HbA1c caveat |

### 1.2 T2D medication adherence (surfaced only when prescribed)

| id | class | dose | valve / target | pillars | E_a | τ_a | ε_a | safe_a | cohort_a |
|----|-------|------|----------------|---------|----:|-----|-----|--------|----------|
| T2D-MET | ADHERE | take prescribed metformin daily | ↓HbA1c `s_i` ⇒ ↓`B_GLY` | MET | 0.10 | weeks | 0.9 ADA 2024 | rx only; eGFR<30 contraindicated; hold for contrast/illness (GI tolerance) | all; Ramadan: split dose to suhoor + iftar |
| T2D-SGLT2 | ADHERE | take prescribed SGLT2i daily | ↓`B_GLY`; ↓`B_ATH`; renal/CV protection | MET, REN, CV | 0.10 | weeks–mo | 0.9 ADA/KDIGO 2024 | rx only; **euglycemic DKA risk — hold when fasting/ill/peri-op**; genital mycotic infection, volume depletion | **Ramadan: dehydration/DKA risk — clinician must reassess fasting safety** |
| T2D-GLP1 | ADHERE | take/inject prescribed GLP-1 RA per schedule | ↓`B_GLY`; weight; ↓`B_ATH` | MET, BCM, CV | 0.15 | weeks–mo | 0.9 ADA 2024 | rx only; pancreatitis hx, MTC/MEN2 (GLP-1); pair with muscle-preservation (§2) | all; Ramadan: weekly agents unaffected by meal timing |
| T2D-INS | ADHERE | take prescribed insulin per regimen; rotate sites | glucose `s_i` ↓ | MET | 0.20 | immediate–days | 0.95 ADA 2024 | rx only; **hypoglycemia — never auto-adjust; red lows escalate** | **Ramadan: basal/bolus dose reduction is clinician-directed; high hypo risk** |
| T2D-SU | ADHERE | take prescribed sulfonylurea per schedule | glucose `s_i` ↓ | MET | 0.10 | days | 0.8 ADA 2024 | rx only; **highest hypoglycemia risk class when fasting** | **Ramadan: often switched/reduced — clinician review mandatory** |

### 1.3 Diabetes complication screening (care-gap closure)

| id | class | dose | valve / target | pillars | E_a | τ_a | ε_a | safe_a | cohort_a |
|----|-------|------|----------------|---------|----:|-----|-----|--------|----------|
| T2D-RETINA | SCREEN | annual dilated retinal / fundus photo | care-gap close; gate retinopathy red | CV/MET (gap) | 0.40 | immediate | n/a ADA 2024/MoHAP | red flag → urgent ophthalmology route | all T2D ≥ diagnosis |
| T2D-FOOT | SCREEN | annual monofilament + pulse foot exam | care-gap close; neuropathy/PAD | BCM/CV (gap) | 0.35 | immediate | n/a IDF/ADA 2024 | active ulcer → urgent podiatry/vascular | all T2D |
| T2D-UACR | SCREEN | annual UACR (spot urine ACR) | ↑`cov_REN`; leading-indicator (UACR before eGFR) | REN (gap) | 0.30 | immediate | n/a KDIGO 2024 | none | all T2D; see §5 CKD |
| T2D-FU | CLINICAL | book overdue diabetes review | care-gap close; gate red markers | (pillar of gap) | 0.45 | immediate | n/a DHA/DoH-AbuDhabi | any red → urgent route | all |

**Ramadan-safe glycemic plan (cross-cutting).** Pre-Ramadan risk stratification (IDF-DAR 2021)
gates fasting eligibility: recent severe hypoglycemia, T1D, pregnancy, advanced CKD, or
poorly-controlled T2D are **medically inadvisable to fast** → routed to clinician, never an
autonomous "you may fast." For eligible patients the engine raises `T2D-CGM`, suhoor/iftar dose
timing nudges, hydration, and a hypoglycemia break-fast rule (glucose <70 mg/dL ⇒ break fast).

---

## 2. Obesity / metabolic syndrome

UAE has among the world's highest obesity prevalence (MoHAP/WHO). **South-Asian and Arab waist
cutoffs are lower** (IDF ethnicity-specific): waist ≥90 cm (men) / ≥80 cm (women) for South-Asian/
Chinese/Arab ancestry vs ≥94/≥80 European. This is a **cutoff selection** (D18), never a penalty.
Marker home: **BCM/MET**, with CV/HEP linkage.

| id | class | dose | valve / target | pillars | E_a | τ_a | ε_a | safe_a | cohort_a |
|----|-------|------|----------------|---------|----:|-----|-----|--------|----------|
| OB-WAIST | MEASURE | measure waist circumference | ↑`cov_BCM`; ethnicity cutoff applied | BCM, MET | 0.10 | immediate | n/a IDF | none | **South-Asian/Arab: use ≥90/≥80 cm cutoffs (D18)** |
| OB-DEFICIT | NUTR | modest sustained caloric deficit; high-protein | ↓`B_GLY`,`B_HEPF`; weight | MET, BCM, HEP | 0.45 | weeks–mo | 0.8 WHO/Endocrine Society 2015 | **disordered-eating hx → suppressed/reframed (Doc 07 §5.1); pregnancy/lactation no-deficit** | all |
| OB-PROT | NUTR | protein 1.2–1.6 g/kg to preserve lean mass | fill `B_MUS` | BCM, MET | 0.35 | weeks | 0.7 Endocrine Society | **CKD ≥3b: capped/blocked (Doc 07 §5.1)** | **GLP-1/bariatric patients: muscle-preservation priority** |
| OB-RES | ACT | 2–3× resistance/wk (muscle preservation) | fill `B_MUS`,`B_BON` | BCM, FIT, MET | 0.55 | weeks–mo | 0.75 ACSM/WHO | severe osteoporosis → supervised | rapid-weight-loss (GLP-1) patients prioritized |
| OB-GLP1 | ADHERE | take prescribed anti-obesity GLP-1/dual agonist | weight; ↓`B_GLY` | MET, BCM, CV | 0.15 | weeks–mo | 0.9 Endocrine Society/WHO | rx only; pair OB-PROT/OB-RES vs lean-mass loss | all; Ramadan: weekly dosing meal-time independent |
| OB-BARI | CLINICAL | bariatric/metabolic surgery referral | care-gap; durable weight/MET | BCM, MET, CV | 0.50 | immediate | 0.85 ASMBS/IFSO | BMI ≥35 w/ comorbidity or ≥40 (ethnicity-adjusted lower in Asians) | **clinician-gated; never autonomous** |

---

## 3. Hypertension

High UAE prevalence; often undiagnosed. Marker home: **CV** (SBP/DBP, home BP), with **REN**.

| id | class | dose | valve / target | pillars | E_a | τ_a | ε_a | safe_a | cohort_a |
|----|-------|------|----------------|---------|----:|-----|-----|--------|----------|
| HTN-HBPM | MEASURE | home BP, 7-day AM/PM averaged | ↑`cov_CV`; resolve SBP `r_i` | CV (uncertainty) | 0.25 | immediate | n/a ACC/AHA 2017 / ESC 2024 | validated cuff; correct size | all |
| HTN-DASH | NUTR | DASH dietary pattern | ↓vascular load; SBP `s_i` | CV, REN, MET | 0.40 | weeks | 0.8 ACC/AHA 2017 | CKD ≥3b → K⁺-aware (limit high-K⁺ DASH foods) | all; Ramadan: maintain pattern across iftar/suhoor |
| HTN-NA | NUTR | sodium <2 g/day (<5 g salt) | ↓SBP `s_i` | CV, REN | 0.35 | weeks | 0.8 WHO/ESC 2024 | none | all |
| HTN-WT | ACT | aerobic 150 min/wk + weight loss if high | fill `B_CRF`; ↓SBP | CV, FIT, MET | 0.45 | weeks–mo | 0.75 ACC/AHA 2017 | acute cardiac event → Doc 06 | all |
| HTN-ACEI | ADHERE | take prescribed ACE-i/ARB daily | ↓SBP `s_i`; renal protection | CV, REN | 0.10 | days–wk | 0.9 ESC 2024 / ACC/AHA 2017 | rx only; **pregnancy contraindicated (teratogen)**; hyperkalemia, cough (ACE-i); monitor K⁺/Cr | women of childbearing potential → flag |
| HTN-CCB | ADHERE | take prescribed CCB daily | ↓SBP `s_i` | CV | 0.10 | days–wk | 0.9 ESC 2024 | rx only; edema watch | all; first-line in many Emirati regimens |
| HTN-DIUR | ADHERE | take prescribed thiazide/loop daily | ↓SBP; volume | CV, REN | 0.10 | days | 0.85 ACC/AHA 2017 | rx only; **hypokalemia/dehydration — Ramadan caution**; gout (thiazide) | **Ramadan: dehydration risk; clinician may shift to suhoor** |

---

## 4. Dyslipidemia / ASCVD prevention

**South-Asian ancestry under-predicted by pooled-cohort/SCORE2 risk equations** → ethnicity-aware
risk enhancement (D18: raises screening, never penalizes). High regional **consanguinity** raises
familial hypercholesterolemia (FH) prevalence → **cascade screening** of first-degree relatives.
Marker home: **CV** (ApoB, LDL-C, Lp(a)).

| id | class | dose | valve / target | pillars | E_a | τ_a | ε_a | safe_a | cohort_a |
|----|-------|------|----------------|---------|----:|-----|-----|--------|----------|
| LIP-APOB | MEASURE | order ApoB (imputed/discordant LDL) | ↑`cov_CV`; resolve `r_i` | CV (uncertainty) | 0.25 | immediate | n/a ACC/AHA 2018 / ESC 2021 | none | **South-Asian: prioritize (discordance common)** |
| LIP-LPA | MEASURE | order Lp(a) once (lifetime) | ↑`cov_CV`; fixed-risk marker | CV (uncertainty) | 0.25 | immediate | n/a ESC 2021 | none | high in South-Asian/Arab cohorts; **Modifiability=fixed → routes to clinical, not lifestyle (D16)** |
| LIP-DIET | NUTR | ↓saturated fat; +soluble fiber; +ω-3 food | ↓`B_ATH` inflow; ApoB `s_i` | CV, MET, →INF | 0.35 | weeks–mo | 0.7 ACC/AHA 2018 | anticoagulant → high-dose ω-3 caution | all |
| LIP-STATIN | ADHERE | take prescribed statin daily | ↓ApoB `s_i` ⇒ ↓`B_ATH` inflow | CV | 0.10 | weeks (ApoB), years (`B_ATH`) | 0.95 ACC/AHA 2018 / ESC 2021 | rx only; **pregnancy contraindicated**; myalgia watch; check w/ G6PD? (no — statins safe) | all; Ramadan: evening dose unaffected |
| LIP-EZE | ADHERE | take prescribed ezetimibe daily | ↓ApoB `s_i` | CV | 0.10 | weeks | 0.85 ESC 2021 | rx only | add-on when statin insufficient |
| LIP-PCSK9 | ADHERE | inject prescribed PCSK9i per schedule | ↓ApoB strongly; ↓`B_ATH` | CV | 0.20 | weeks | 0.9 ESC 2021 | rx only; FH/very-high-risk | **FH and high-Lp(a) patients (lifestyle gated, D16)** |
| LIP-FHCASC | SCREEN | cascade-screen first-degree relatives (FH) | care-gap; family lipid panel + genetics | CV (gap) | 0.40 | immediate | n/a ESC 2021 / EAS | index FH case | **high regional consanguinity → strong yield (D18)** |

---

## 5. Chronic kidney disease (CKD)

Driven by the high diabetes/HTN burden. **eGFR uses race-free CKD-EPI 2021** (D18; README §5.5).
Leading indicator: **UACR before eGFR** (Doc 12 §5.2). Marker home: **REN** (eGFR, UACR, K⁺).

| id | class | dose | valve / target | pillars | E_a | τ_a | ε_a | safe_a | cohort_a |
|----|-------|------|----------------|---------|----:|-----|-----|--------|----------|
| CKD-UACR | MEASURE | UACR + eGFR (CKD-EPI 2021) | ↑`cov_REN`; resolve `r_i` | REN (uncertainty) | 0.30 | immediate | n/a KDIGO 2024 | none | **race-free eGFR for all ancestries (D18)** |
| CKD-K | MEASURE | serum K⁺ monitoring (on RAASi/SGLT2i) | ↑`cov_REN`; safety | REN | 0.25 | immediate | n/a KDIGO 2024 | **hemolyzed sample → reconfirm, not Alert (D14)** | all CKD on RAASi |
| CKD-SGLT2 | ADHERE | take prescribed SGLT2i daily | renal protection; ↓UACR | REN, CV, MET | 0.10 | weeks–mo | 0.9 KDIGO 2024 | rx only; euglycemic DKA, volume; **hold when fasting/ill** | **Ramadan: dehydration/DKA — reassess fasting** |
| CKD-RAASI | ADHERE | take prescribed ACE-i/ARB daily | ↓UACR; BP; renal protection | REN, CV | 0.10 | days–wk | 0.9 KDIGO 2024 | rx only; **pregnancy contraindicated**; monitor K⁺/Cr | women of childbearing potential → flag |
| CKD-NSAID | ADHERE | **avoid NSAIDs** (OTC ibuprofen/diclofenac) | protect eGFR; avoid AKI | REN | 0.15 | immediate | 0.8 KDIGO 2024 | nephrotoxic — counsel OTC/pharmacy purchases | all CKD; common OTC in region |
| CKD-PROT | NUTR | moderate protein 0.8 g/kg (not high-protein) | ↓hyperfiltration | REN, BCM | 0.35 | weeks–mo | 0.6 KDIGO 2024 | **overrides BCM-optimizing high-protein nudges (Doc 07 §5.1)** | CKD ≥3b: protein cap binds |
| CKD-NEPH | CLINICAL | nephrology referral (eGFR<30 or rapid decline) | care-gap; gate red | REN (gap) | 0.45 | immediate | n/a KDIGO 2024 | red → urgent route | all advanced CKD |

---

## 6. NAFLD / MASLD (metabolic-associated steatotic liver disease)

Tracks the obesity/T2D burden; very prevalent regionally. Marker home: **HEP** (ALT/AST, FIB-4).

| id | class | dose | valve / target | pillars | E_a | τ_a | ε_a | safe_a | cohort_a |
|----|-------|------|----------------|---------|----:|-----|-----|--------|----------|
| NAFLD-WT | NUTR | 7–10% body-weight loss | ↓`B_HEPF`; ALT `s_i` | HEP, MET, BCM | 0.45 | weeks–mo | 0.85 AASLD 2023 | disordered-eating hx → reframe; pregnancy no-deficit | all |
| NAFLD-FIB4 | MEASURE | compute FIB-4 (age, AST, ALT, platelets) | ↑`cov_HEP`; fibrosis risk-stratify | HEP (uncertainty) | 0.15 | immediate | n/a AASLD 2023 / EASL | **heavy-tailed → log handling (D13)**; high FIB-4 → hepatology | all metabolic patients |
| NAFLD-ALC | NUTR | minimize/avoid alcohol | ↓`B_HEPF` | HEP | 0.30 | weeks | 0.75 EASL/WHO | — | most UAE residents abstain; still screened |
| NAFLD-MET | NUTR | optimize glycemia + lipids (see §1, §4) | ↓`B_HEPF`,`B_GLY` (κ) | HEP, MET, CV | 0.40 | weeks–mo | 0.7 AASLD 2023 | as per §1/§4 | all |

---

## 7. Vitamin D deficiency

**Near-universal in the UAE** (sun avoidance, indoor lifestyle, clothing, skin pigmentation —
high latitude of melanin, not latitude of sun). Marker home: **NUT** (25-OH vit D), with **BCM**.

| id | class | dose | valve / target | pillars | E_a | τ_a | ε_a | safe_a | cohort_a |
|----|-------|------|----------------|---------|----:|-----|-----|--------|----------|
| VITD-MSR | MEASURE | order 25-OH vitamin D | ↑`cov_NUT`; resolve `r_i` | NUT (uncertainty) | 0.20 | immediate | n/a Endocrine Society 2024 | none | **near-universal deficiency → high prior** |
| VITD-REPL | ADHERE | take prescribed repletion (e.g. 50,000 IU/wk ×6–8 wk if deficient), then maintenance ~1000–2000 IU/d | 25-OH vit D `s_i`↑ | NUT, BCM | 0.10 | weeks–mo | 0.7 Endocrine Society 2024 | rx/clinician dose; **granulomatous disease, hypercalcemia, sarcoid → caution** | all; safe in Ramadan (weekly/daily) |
| VITD-SUN | NUTR | safe incidental sun + dietary D sources | 25-OH vit D `s_i` (modest) | NUT | 0.20 | weeks–mo | 0.5 WHO | **skin-cancer/heat caveat — not midday UAE summer sun** | supplementation is primary route regionally |

---

## 8. Anemia / iron deficiency + hemoglobinopathy / G6PD context

Iron deficiency is common (esp. menstruating women, pregnancy). The region has **high carrier
rates of thalassemia, sickle trait, and G6PD deficiency** (consanguinity; UAE has a premarital
screening program). This is a **screening & safety context** (D18), never a penalty. Marker home:
**HEM** (Hgb, ferritin, MCV), with **NUT**.

| id | class | dose | valve / target | pillars | E_a | τ_a | ε_a | safe_a | cohort_a |
|----|-------|------|----------------|---------|----:|-----|-----|--------|----------|
| ANE-FER | MEASURE | order ferritin + CBC/MCV | ↑`cov_HEM`; iron status | HEM, NUT | 0.20 | immediate | n/a WHO/NICE | inflammation raises ferritin (pair CRP) | menstruating/pregnant prioritized |
| ANE-IRON | ADHERE | take prescribed iron; +vit C; avoid w/ tea | ferritin/Hgb `s_i`↑ | HEM, NUT | 0.15 | weeks–mo | 0.8 WHO/NICE | rx; **GI tolerance**; alternate-day dosing improves uptake | **iron only if truly deficient — not in thalassemia trait without ID** |
| HGB-SCREEN | SCREEN | hemoglobinopathy screen (Hgb electrophoresis) | carrier status; HbA1c caveat | HEM (gap) | 0.30 | immediate | n/a WHO/MoHAP premarital | — | **high regional carrier rate; premarital program (D18)** |
| HGB-A1C-FLAG | MEASURE | flag: HbA1c unreliable in hemoglobinopathy → use CGM/fructosamine | corrects MET `cov` | MET, HEM | 0.10 | immediate | n/a ADA 2024 | — | thalassemia/variant-Hgb carriers |
| G6PD-SCREEN | SCREEN | G6PD assay (esp. before oxidant drugs) | carrier status; drug safety | HEM (gap) | 0.30 | immediate | n/a WHO | — | **high regional prevalence (D18)** |
| G6PD-AVOID | ADHERE | **avoid G6PD-unsafe drugs/foods** (primaquine, dapsone, nitrofurantoin, sulfonamides, rasburicase, fava beans, high-dose vit C, naphthalene) | prevent hemolysis | HEM | 0.15 | immediate | 0.85 WHO | **hard safety filter on med/nudge library for G6PD-deficient patients** | G6PD-deficient cohort |

---

## 9. Mental health (depression / anxiety)

Marker home: **MCS** (PHQ-9, GAD-7). **Hard suicidality rule (binding, never relaxed):** PHQ-9
item-9 > 0 (or EPDS item-10 > 0 postpartum, Doc 05 §2.4) forces MCS critical and routes to **crisis
support, never a breathing exercise** (Doc 07 §5.2; Doc 02 MCS). The nudge card is suppressed in
favor of the escalation pathway (Doc 11).

| id | class | dose | valve / target | pillars | E_a | τ_a | ε_a | safe_a | cohort_a |
|----|-------|------|----------------|---------|----:|-----|-----|--------|----------|
| MH-PHQ9 | SCREEN | PHQ-9 depression screen | MCS coverage; risk-stratify | MCS | 0.10 | immediate | n/a NICE/USPSTF | **item-9 > 0 ⇒ CRITICAL, crisis escalation (hard rule)** | all; culturally-adapted Arabic instrument |
| MH-GAD7 | SCREEN | GAD-7 anxiety screen | MCS coverage | MCS | 0.10 | immediate | n/a NICE | — | all |
| MH-CHECK | STRESS | daily 1-tap mood/stress check-in | observe `B_ALLO`; coverage | MCS | 0.05 | immediate | n/a | **suicidality item → escalate, not nudge** | all |
| MH-BREATH | STRESS | 5-min daily breathing/mindfulness | drain `B_ALLO` | MCS, →SLP,CV | 0.15 | days–wk | 0.5 NICE | **active psychosis/suicidality → clinical, not this** | non-crisis only |
| MH-ACT | ACT | aerobic 150 min/wk (mood benefit) | fill `B_CRF`; ↓`B_ALLO` | MCS, FIT | 0.40 | weeks | 0.6 NICE | as per ACT contraindications | adjunct, non-crisis |
| MH-REFER | CLINICAL | mental-health clinician referral | care-gap; gate red | MCS (gap) | 0.45 | immediate | n/a NICE/DHA | **moderate–severe / any red → urgent** | all |

---

## 10. Respiratory (asthma / COPD)

Regional triggers: **sandstorms / dust, high PM air quality, indoor smoking, shisha, vaping.**
Marker home: **CV/FIT** linkage + PRO symptom/peak-flow; smoking is a cross-pillar burden.

| id | class | dose | valve / target | pillars | E_a | τ_a | ε_a | safe_a | cohort_a |
|----|-------|------|----------------|---------|----:|-----|-----|--------|----------|
| RESP-AQ | STRESS | air-quality–triggered avoidance (mask, indoor, filtration) on high-PM/sandstorm days | reduce exposure inflow | FIT, INF | 0.20 | immediate | 0.6 WHO | — | **UAE sandstorm/PM season** |
| RESP-INH | ADHERE | take prescribed controller inhaler daily; check technique | airway control; symptom `s_i` | FIT, CV | 0.15 | days–wk | 0.9 GINA 2024 / GOLD 2024 | rx only; spacer/technique | asthma & COPD |
| RESP-SMOKE | ADHERE | smoking cessation (+ pharmacotherapy if rx) | drain cumulative-exposure burden | CV, FIT, HEP, →all | 0.55 | weeks–mo | 0.9 WHO/USPSTF | pregnancy → behavioral-first | **shisha & vaping counted as tobacco exposure** |
| RESP-SHISHA | NUTR | shisha/waterpipe cessation | reduce CO/tar exposure | CV, FIT | 0.50 | weeks–mo | 0.7 WHO | — | **high regional shisha prevalence** |
| RESP-PEF | MEASURE | home peak-flow / symptom diary | ↑`cov_FIT` (respiratory) | FIT (uncertainty) | 0.25 | immediate | n/a GINA 2024 | — | asthma |
| RESP-FLU | VACCINE | annual influenza vaccine | reduce exacerbation burden | INF, FIT | 0.20 | immediate | 0.8 WHO/MoHAP | egg-allergy variants | COPD/asthma prioritized |

---

## 11. Bone health / osteoporosis

Vit-D deficiency + sedentary lifestyle + postmenopausal estrogen withdrawal (Doc 05 §2.5) raise
fracture risk. Marker home: **BCM** (BMD/T-score, FRAX), with **NUT**.

| id | class | dose | valve / target | pillars | E_a | τ_a | ε_a | safe_a | cohort_a |
|----|-------|------|----------------|---------|----:|-----|-----|--------|----------|
| BONE-FRAX | SCREEN | compute FRAX ± DXA (BMD/T-score) | fracture risk-stratify; gap | BCM (gap) | 0.35 | immediate | n/a NICE/FRAX | T-score ≤ −2.5 → treat pathway | **postmenopausal women, ≥65** |
| BONE-VITD | ADHERE | vitamin D repletion (see §7) + calcium 1000–1200 mg/d (diet-first) | fill `B_BON` | BCM, NUT | 0.15 | weeks–mo | 0.7 Endocrine Society 2024 | hypercalcemia/stones → diet-first calcium | all deficient |
| BONE-RES | ACT | weight-bearing + resistance 2–3×/wk | fill `B_BON`,`B_MUS` | BCM, FIT | 0.55 | weeks–mo | 0.7 ACSM/WHO | severe osteoporosis/recent fracture → **supervised-only (Doc 07 §5.1)** | postmenopausal, elderly |
| BONE-RX | ADHERE | take prescribed bisphosphonate/anti-resorptive | ↓bone-loss rate | BCM | 0.15 | months | 0.85 NICE/Endocrine Society | rx only; dental (ONJ), renal, dosing posture | osteoporosis dx |

---

## 12. Thyroid · gout/hyperuricemia · sleep apnea (OSA) · immunization & cancer screening

### 12.1 Thyroid (ENDO)

| id | class | dose | valve / target | pillars | E_a | τ_a | ε_a | safe_a | cohort_a |
|----|-------|------|----------------|---------|----:|-----|-----|--------|----------|
| THY-TSH | MEASURE | order TSH (± FT4) | ↑`cov_ENDO` | ENDO (uncertainty) | 0.20 | immediate | n/a ATA/Endocrine Society | **preconception target 0.5–2.5; pregnancy trimester-specific (Doc 05)** | symptomatic, preconception, postpartum |
| THY-RX | ADHERE | take prescribed levothyroxine, empty stomach | TSH `s_i` | ENDO | 0.10 | weeks | 0.9 ATA | rx only; separate from iron/calcium/coffee | all; Ramadan: take at suhoor, fasting-state preserved |

### 12.2 Gout / hyperuricemia (MET/CV)

| id | class | dose | valve / target | pillars | E_a | τ_a | ε_a | safe_a | cohort_a |
|----|-------|------|----------------|---------|----:|-----|-----|--------|----------|
| GOUT-URATE | MEASURE | order serum urate | ↑coverage; risk | MET (uncertainty) | 0.20 | immediate | n/a ACR 2020 | — | hyperuricemia, gout hx |
| GOUT-DIET | NUTR | ↓purine/alcohol/fructose; hydrate | urate `s_i` | MET | 0.30 | weeks | 0.6 ACR 2020 | thiazide/loop raise urate (§3) | Ramadan: hydration window narrow |
| GOUT-ULT | ADHERE | take prescribed urate-lowering (allopurinol) | urate `s_i` ↓ | MET | 0.10 | weeks | 0.85 ACR 2020 | rx only; **HLA-B*58:01 hypersensitivity (esp. Asian ancestry) — screen before start** | **Asian-ancestry → HLA screen flag (D18)** |

### 12.3 Obstructive sleep apnea (SLP) — high with obesity

| id | class | dose | valve / target | pillars | E_a | τ_a | ε_a | safe_a | cohort_a |
|----|-------|------|----------------|---------|----:|-----|-----|--------|----------|
| OSA-STOP | SCREEN | STOP-BANG questionnaire | OSA risk-stratify; gap | SLP (gap) | 0.10 | immediate | n/a AASM | high score → sleep study | **obesity cohort prioritized** |
| OSA-CPAP | ADHERE | use prescribed CPAP nightly (>4 h) | drain `B_SLD`; ↓SBP, glucose (κ) | SLP, CV, MET | 0.40 | days–wk | 0.85 AASM | rx only; mask/adherence support | OSA dx |
| OSA-WT | NUTR | weight loss (see §2) | reduce AHI | SLP, BCM, MET | 0.45 | weeks–mo | 0.7 AASM | as §2 | obesity-related OSA |

### 12.4 Immunization & cancer screening (preventive)

| id | class | dose | valve / target | pillars | E_a | τ_a | ε_a | safe_a | cohort_a |
|----|-------|------|----------------|---------|----:|-----|-----|--------|----------|
| VAC-FLU | VACCINE | annual influenza | ↓infection burden | INF | 0.20 | immediate | 0.8 WHO/MoHAP | egg-allergy variant | all; elderly/chronic prioritized |
| VAC-PNEU | VACCINE | pneumococcal per age/risk schedule | ↓infection burden | INF | 0.20 | immediate | 0.8 WHO/MoHAP | — | ≥65, chronic disease |
| VAC-HPV | VACCINE | HPV per national schedule | ↓cervical-cancer risk | INF (gap) | 0.20 | immediate | 0.85 WHO | — | adolescents/eligible |
| SCR-COLO | SCREEN | colorectal screening (FIT/colonoscopy) per age | care-gap; cancer | (gap) | 0.40 | immediate | n/a USPSTF/MoHAP | — | **age ≥45–50 per protocol** |
| SCR-MAMMO | SCREEN | mammography per age/sex protocol | care-gap; cancer | (gap) | 0.40 | immediate | n/a USPSTF/DHA | sex/organ-inventory gated (Doc 05 §1.1) | **women per national age band** |
| SCR-CERV | SCREEN | cervical (Pap/HPV) per protocol | care-gap; cancer | (gap) | 0.40 | immediate | n/a USPSTF/DHA | organ-inventory gated (Doc 05 §1.1) | women with cervix per age band |

---

## 13. Cross-cutting actions

### 13.1 MEASURE — close imputed-marker gaps (first-class, Doc 07 §1.1)

When a pillar's dominant risk rides on an **imputed** marker (low `cov_k`, literature fallback,
README §1), measuring it is often the **highest-yield nudge** (it resolves uncertainty rather than
moving a value). Per-condition MEASURE rows above (`*-MSR`, `*-HBA1C`, `LIP-APOB`, `CKD-UACR`,
`VITD-MSR`, `ANE-FER`, `THY-TSH`, …) all carry `ε_a = n/a` (they don't claim a `ΔPureScore` from a
value move; they sharpen `cov_k` and the companion **Confidence/Data-sufficiency** vector, Doc 12
§4). They are surfaced when the imputed marker is the binding constraint.

### 13.2 ADHERE — most prevalent UAE drug classes

| id | class | drug class | pillars | E_a | ε_a | safe_a | cohort_a |
|----|-------|------------|---------|----:|-----|--------|----------|
| ADH-MET | ADHERE | metformin | MET | 0.10 | 0.9 ADA 2024 | eGFR<30, contrast hold | Ramadan split-dose |
| ADH-SGLT2 | ADHERE | SGLT2 inhibitor | MET, REN, CV | 0.10 | 0.9 KDIGO 2024 | DKA/volume; fasting reassess | Ramadan DKA caution |
| ADH-STATIN | ADHERE | statin | CV | 0.10 | 0.95 ACC/AHA 2018 | pregnancy contraindicated | evening dose |
| ADH-RAASI | ADHERE | ACE-i / ARB | CV, REN | 0.10 | 0.9 ESC 2024 | pregnancy contraindicated; K⁺ | childbearing flag |
| ADH-CCB | ADHERE | calcium-channel blocker | CV | 0.10 | 0.9 ESC 2024 | edema | — |
| ADH-LEVO | ADHERE | levothyroxine | ENDO | 0.10 | 0.9 ATA | empty stomach | suhoor timing |
| ADH-INH | ADHERE | inhaled controller | FIT, CV | 0.15 | 0.9 GINA/GOLD 2024 | technique | — |
| ADH-PPI | ADHERE | proton-pump inhibitor | HEP/NUT context | 0.10 | 0.7 NICE | long-term Mg/B12/bone caveat | deprescribe-review nudge |

Each `ADH-*` row is surfaced **only when prescribed** (`rx`), and the quoted `ΔPureScore` is the
exact recompute (D16): an already-controlled (`MANAGED`, Doc 12 §3.1) marker yields ~0, so the
engine credits *sustained* adherence via the reservoir path (Doc 07 §2.2), not a phantom value move.
A **fixed/medication-responsive red that lifestyle cannot move routes to a clinician** rather than a
fabricated lifestyle fix (D16; Doc 07 §8 status).

### 13.3 Ramadan-fasting adjustments (cross-cutting safety)

| id | class | dose | valve / target | pillars | E_a | τ_a | ε_a | safe_a | cohort_a |
|----|-------|------|----------------|---------|----:|-----|-----|--------|----------|
| RAM-RISK | CLINICAL | pre-Ramadan fasting risk stratification | gate fasting eligibility | (all) | 0.30 | immediate | n/a IDF-DAR 2021 | **high-risk (T1D, severe hypo hx, advanced CKD, pregnancy) → fasting medically inadvisable, clinician route** | diabetic/CKD/pregnant patients |
| RAM-TIME | ADHERE | shift med timing to suhoor/iftar (clinician plan) | preserve adherence under fast | (drug pillar) | 0.15 | immediate | 0.7 IDF-DAR 2021 | **never auto-adjust insulin/SU dose** | fasting patients on rx |
| RAM-HYPO | MEASURE | hypoglycemia/dehydration self-monitor; break-fast rule | safety; glucose `s_i` | MET, REN | 0.20 | immediate | 0.8 IDF-DAR 2021 | **glucose <70 mg/dL ⇒ break fast (binding); red lows escalate** | fasting diabetics |
| RAM-HYDR | NUTR | hydrate across the eating window; limit caffeine/salt at suhoor | reduce dehydration/AKI risk | REN, CV | 0.20 | immediate | 0.6 WHO | CKD/diuretic/SGLT2i → high caution | fasting patients |

---

## 14. Ethnicity & cohort applicability (D18 — binding)

Implementing **D18** across this catalogue:

1. **Ethnicity selects validated cutoffs / instruments, never a penalty.** South-Asian/Arab waist
   thresholds (≥90 cm men / ≥80 cm women, §2), South-Asian ASCVD risk-enhancement (§4),
   HLA-B*58:01 pre-allopurinol screen in Asian ancestry (§12.2). These **raise screening or change
   a band**; they **never** add risk to the score for belonging to a group (README §5.5).
2. **Ethnicity raises screening salience.** G6PD and thalassemia/hemoglobinopathy screening (§8),
   FH cascade screening under high regional consanguinity (§4) — ethnicity moves these care-gap
   nudges up the ranking via `ν_a` (Doc 07 §3.1), not via any penalty term.
3. **Absent ethnicity → lower confidence, never penalty.** When ancestry is unknown the engine
   widens to the least-penalizing validated band and **lowers the companion Confidence/
   Representativeness vector** (Doc 12 §4, §7; D7), exactly as for absent cohort data — it never
   substitutes a worse score.
4. **Race-free renal function.** eGFR uses **CKD-EPI 2021 (race-free)** for every patient (§5;
   README §5.5; Doc 12 §7) — no race coefficient, ever.
5. **No protected-class proxy may worsen a score, price, or access** (README §5.5; Doc 10; Doc 11).
   Ethnicity adjusts *context and screening*, the same way representativeness adjusts *confidence*,
   never *penalty*.

---

## 15. Coverage & maintenance

This catalogue is a **living artifact**, maintained against the project's **evidence registry**: an
external citation crawler keeps the cited guideline bodies/years (ADA, KDIGO, ACC/AHA, ESC, WHO,
IDF/IDF-DAR, Endocrine Society, NICE, ATA, AASLD/EASL, GINA/GOLD, AASM, ACR, USPSTF, and the UAE
bodies MoHAP, DHA, DoH-AbuDhabi, Emirates Cardiac Society) **fresh on the fixed cadence** (README
§5.6); a guideline update triggers a model-version bump and re-validation (Doc 09/11).

- **Conditions covered (16 sections):** T2D/prediabetes, obesity/metabolic syndrome, hypertension,
  dyslipidemia/ASCVD, CKD, NAFLD/MASLD, vitamin D deficiency, anemia/iron + hemoglobinopathy/G6PD,
  mental health, respiratory, bone health, thyroid, gout, OSA, immunization/cancer screening — plus
  cross-cutting MEASURE, ADHERE, and Ramadan-fasting actions.
- **Every dose/threshold is illustrative** and guideline-anchored as of the cited body+year;
  re-verify on cadence before any production use (README §5.6).
- **Every red/critical escalates to a human** (README §5.1; Doc 07 §5.2). The hard suicidality rule
  (§9) and the medically-inadvisable-to-fast rule (§13.3) are never relaxed.
- **No autonomous diagnosis or therapy initiation/titration** (Doc 00 §3.1; Doc 11). Medication
  rows act on **adherence** to an existing prescription only.
- **Pending clinical sign-off and Doc 13 validation** of realized-vs-predicted impact (Doc 07 §6
  feedback loop). Until then, all `ΔPureScore` contributions are illustrative (synthetic engine),
  and the catalogue carries the Babylon discipline: synthetic ≠ evidence (Doc 00).
