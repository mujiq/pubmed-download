# -*- coding: utf-8 -*-
"""Data + appendix/admin page builders for the PureScore tech wiki.
All values are illustrative, literature/guideline-anchored, and must be re-verified
(README §5.6) before production. Compiled from docs 00-18."""

import json, os
_HERE = os.path.dirname(os.path.abspath(__file__))
def _load(name):
    with open(os.path.join(_HERE, "data", name), encoding="utf-8") as f:
        return json.load(f)

# ----------------------------------------------------------------- per-doc summaries
SUMMARY = {
 "00":"Vision, design principles, and the institutional lessons (Babylon / Kaiser / Mayo) PureScore is engineered around.",
 "01":"Canonical FHIR-aligned data model, the four flagged input streams, measurement tiers, provenance/confidence, and literature reference-range strategy.",
 "02":"The 12 pillars and the full marker catalogue — tiers, two-sidedness, green/yellow/red bands, within-pillar weights and sources.",
 "03":"The deterministic marker→pillar→PureScore math, continuous personalized scoring (Stage 2b), the critical cascade and the feedback loop.",
 "04":"MONIAC reservoir dynamics: stocks, leakage, cross-pillar interference and valves that give PureScore memory.",
 "05":"Sex-specific models: cycle, fertility, pregnancy, post-partum, menopause, andropause, and hormone-therapy-aware ranges.",
 "06":"Acute-event override & revert, and care/nutrition/exercise plans by life stage.",
 "07":"The daily top-5 nudge engine: impact-attributed, ease-weighted, diverse, safe — every action a positive ΔPureScore.",
 "08":"Validated clinical scores (FINDRISC, ASCVD/SCORE2, KDIGO, FIB-4, FRAX, PhenoAge) integrated for clinicians — feeding max risk, never relaxing it.",
 "09":"Cohort construction, empirical-Bayes shrinkage, calibration, fairness slices and drift monitoring.",
 "10":"The actuarial/pricing layer — gated, firewalled, and heavily flagged for regulatory & fairness risk.",
 "11":"Clinician-in-the-loop safety, escalation tiers, the crisis pathway, FMEA, privacy/consent and model governance.",
 "12":"The critical review and PureScore 2.0: context-aware interpretation, the companion meta-vector, personal-baseline early-warning, dual framing.",
 "13":"Validation & calibration gates for 2.0 — discrimination, calibration, early-warning PPV/lead-time, fairness, drift, release gates.",
 "14":"Machine-readable evidence registry, per-band provenance and the cold-start ‘ignition’ model (guideline → cohort → personal).",
 "15":"UAE localization: ethnicity-aware cut-points & screening (context, never penalty), regional epidemiology and Ramadan (IDF-DAR) safety.",
 "16":"The exhaustive, UAE-prioritized recommended-action / nudge library by condition, with adherence & screening actions.",
 "17":"Adversarial self-review: every clinician/statistician/ethicist/regulator objection → how handled → honest residual.",
 "18":"Input taxonomy & trust flags, wearable trust-tiering (D22), wearables×pillars, the effortless product surfaces and household accounts.",
}

# ----------------------------------------------------------------- per-doc mermaid
MERMAID = {
 "00":("Lessons → principles", """flowchart LR
  B["Babylon<br/>overclaim, opaque"] -->|avoid| P(("PureScore"))
  K["Kaiser<br/>care-gap closure"] -->|emulate| P
  M["Mayo<br/>clinician-in-loop"] -->|emulate| P
  P --> S["Safety dominates"]
  P --> X["Explainable"]
  P --> E["Effortless"]"""),
 "01":("Four streams → measurement", """flowchart TD
  L["LAB · clinical-grade"] --> M["Measurement<br/>source + confidence"]
  W["WEAR · tiered (D22)"] --> M
  G["GOAL · intent"] --> M
  F["LIFE · self-report"] --> M
  M --> C{"own recent data?"}
  C -->|yes| U["use value"]
  C -->|no| R["literature fallback<br/>low confidence"]"""),
 "02":("PureScore → 12 pillars", """flowchart TB
  PS(("PureScore")) --> CV & MET & REN & HEP
  PS --> INF & HEM & ENDO & BCM
  PS --> NUT & SLP & FIT & MCS"""),
 "03":("The scoring pipeline", """flowchart LR
  X["raw x_i"] --> R1["1 · clinical risk<br/>r_i^clin (continuous)"]
  R1 --> R2["2 · cohort blend<br/>max(clin, φ·cohort)"]
  R2 --> R2b["2b · personal z<br/>κ·tanh(z_i/2)"]
  R2b --> RK["3 · pillar risk<br/>γ-mean + reservoir"]
  RK --> CR{"critical?"}
  CR -->|yes| CAP["cap ≤ 40 + escalate"]
  CR -->|no| PSc["5 · PureScore"]"""),
 "04":("Reservoir hydraulics", """flowchart LR
  IN["inflows<br/>behaviours / values"] --> RES[("Reservoir B_j<br/>stock + memory")]
  V["valves<br/>interventions"] --> IN
  RES -->|leak λ| H["heal / decay"]
  RES -->|interference κ| RES2[("coupled reservoir")]
  RES --> RK["pillar risk + ρ·B̃"]"""),
 "05":("Hormonal milieu → ranges", """flowchart TD
  N["natal sex"] --> B["physiology baseline"]
  H["hormone milieu<br/>cycle / preg / meno / HRT"] --> B
  B --> R["phase-aware reference ranges"]"""),
 "06":("Acute mode lifecycle", """stateDiagram-v2
  [*] --> Baseline
  Baseline --> AcuteMode : life event (override weights)
  AcuteMode --> Baseline : recovery + hysteresis
  AcuteMode --> AcuteMode : escalation unaffected"""),
 "07":("The daily nudge loop", """flowchart LR
  SC["score + companion vector"] --> CAND["candidate actions (Doc 16)"]
  CAND --> SAFE["safety / contraindication filter"]
  SAFE --> U["U_a = impact · adherence · ease"]
  U --> TOP["top-5 · diverse · positive Δ"]
  TOP --> ACT["patient acts"] --> SC"""),
 "08":("Clinical scores feed max", """flowchart LR
  M["markers"] --> CS["FINDRISC · ASCVD/SCORE2<br/>KDIGO · FIB-4 · FRAX"]
  CS --> MX["feed the max risk"]
  MX -.never.-> RX["relax r_i / clear critical"]"""),
 "09":("Cohort & shrinkage", """flowchart TD
  P["patient"] --> C["cohort stratum<br/>age × sex × D × Mx"]
  C --> EB["empirical-Bayes shrinkage"]
  EB --> CAL["calibration · fairness · drift"]"""),
 "10":("Gated actuarial firewall", """flowchart LR
  S["PureScore (wellness)"] -. separate consent .-> ACT["actuarial layer"]
  ACT --> FW["firewall: no protected-class proxy<br/>may worsen price/access"]"""),
 "11":("Escalation tiers", """flowchart TD
  R["red / critical marker"] --> E{"escalation tier"}
  E -->|emergency| ER["seek care now + on-call clinician"]
  E -->|urgent| UR["expedited reconfirm"]
  E -->|routine| RT["clinician follow-up"]"""),
 "12":("Number → companion vector", """flowchart LR
  N["PureScore number"] --> CV["companion vector"]
  CV --> CF["Confidence"] & TR["Trajectory"] & EW["Early-warning"]
  CV --> MO["Modifiability"] & CRb["Criticality"] & RP["Representativeness"]"""),
 "13":("Release gates", """flowchart LR
  M["model + config change"] --> G["gates:<br/>discrimination · calibration<br/>PPV/lead-time · fairness · drift"]
  G -->|all pass| REL["release"]
  G -->|any fail| BLK["blocked + audit"]"""),
 "14":("Cold-start ignition", """flowchart LR
  GUIDE["guideline band"] --> COH["cohort distribution"] --> PERS["personal baseline"]
  GUIDE --> REG[("evidence registry<br/>provenance + vintage")]
  COH --> REG"""),
 "15":("Ethnicity = context", """flowchart TD
  E["ethnicity"] --> CTX["context + screening selector"]
  CTX -.never.-> PEN["score penalty"]
  R["Ramadan"] --> SAFE["IDF-DAR med timing<br/>+ fast-break safety rule"]"""),
 "16":("Condition → ranked actions", """flowchart LR
  COND["condition / med class"] --> LIB["typed action library"]
  GOAL["goal stream (Doc 18)"] --> RANK["rank by U_a"]
  LIB --> RANK --> TOP["top-5"]"""),
 "17":("Objection → residual", """flowchart LR
  OBJ["objection<br/>(clinician / stats / ethics / reg)"] --> H["how handled"] --> RES["honest residual + register"]"""),
 "18":("Streams → score → surfaces", """flowchart TD
  LAB["LAB · anchor"] --> SC(("PureScore"))
  WEAR["WEAR · tiered"] --> SC
  GOAL["GOAL · steer"] --> SC
  LIFE["LIFE · fill"] --> SC
  SC --> EXP["scribe · chat · scheduling<br/>in-home · insurance-auth"]"""),
}

# ----------------------------------------------------------------- biomarker catalogue (Doc 02)
# (marker, tier, two_sided, green, yellow, red, w, source, critical?)
PILLARS = [
 ("CV","Cardiovascular & Vascular","atherogenic burden · vascular/BP load · cardiorespiratory reserve",[
   ("Systolic BP","mmHg","C",1,"90–119","120–139 (or <90)","≥140 or <85",".15","ACC/AHA 2017",1),
   ("Diastolic BP","mmHg","C",1,"60–79","80–89","≥90 or <55",".10","ACC/AHA",0),
   ("ApoB","mg/dL","P",0,"<80 (<65 high-risk)","80–99","≥100",".18","ESC/EAS",1),
   ("LDL-C","mg/dL","C",0,"<100 (<70 high-risk)","100–159","≥160",".12","ACC/AHA",1),
   ("HDL-C","mg/dL","C",1,"≥50 F / ≥40 M","40–49","<40 (very low)",".06","ACC/AHA",0),
   ("Lp(a)","nmol/L","P",0,"<75","75–125","≥125",".10","ESC (lifetime, once)",0),
   ("Resting HR","bpm","C",1,"50–69","70–84 (or <45)","≥85 or <40",".07","wearable/literature",0),
   ("HRV (RMSSD, age-adj)","ms","C",0,"cohort p50+","p20–p50","<p10",".07","wearable norms",0),
   ("CAC","Agatston","X",0,"0","1–99","≥100 (≥300 high)",".15","MESA",1),
 ]),
 ("MET","Metabolic & Glycemic","glycemic burden · adiposity · hepatic fat",[
   ("HbA1c","%","C",0,"<5.4","5.4–6.4","≥6.5",".22","ADA",1),
   ("Fasting glucose","mg/dL","C",1,"70–99","100–125 (or <65)","≥126 or <55",".15","ADA",1),
   ("Fasting insulin","µIU/mL","P",0,"<8","8–14",">14",".10","literature",0),
   ("HOMA-IR","index","P",0,"<1.5","1.5–2.9","≥3.0",".12","literature",0),
   ("Triglycerides","mg/dL","C",0,"<90","90–149","≥150 (≥500 acute)",".10","ADA/AHA",1),
   ("Waist circumference","cm","C",0,"<94 M / <80 F","up to 102 M / 88 F","≥102 M / ≥88 F",".12","IDF",0),
   ("CGM time-in-range","%","X",0,"≥90","70–89","<70",".10","consensus TIR",0),
   ("CGM glycemic variability","CV%","X",0,"<25","25–36",">36",".09","consensus",0),
 ]),
 ("REN","Renal","renal reserve · KDIGO eGFR×UACR heatmap (Doc 08)",[
   ("eGFR (cystatin-C)","mL/min/1.73m²","P",0,"≥90","60–89","<60 (<30 severe)",".35","KDIGO 2024",1),
   ("eGFR (creatinine)","mL/min/1.73m²","C",0,"≥90","60–89","<60",".15","KDIGO",1),
   ("UACR","mg/g","P",0,"<30","30–299","≥300",".25","KDIGO",1),
   ("Potassium","mmol/L","C",1,"3.6–5.0","5.1–5.5 / 3.3–3.5","≥5.6 or ≤3.2",".15","lab",1),
   ("Sodium","mmol/L","C",1,"136–144","130–135 / 145–148","<130 or >148",".10","lab",0),
 ]),
 ("HEP","Hepatic","hepatic fat · inflammatory load",[
   ("ALT","U/L","C",0,"<30 M / <20 F","up to 50",">50 (3× ULN acute)",".22","AASLD",1),
   ("AST","U/L","C",0,"<30","30–45",">45",".15","AASLD",1),
   ("GGT","U/L","P",0,"<30","30–60",">60",".12","literature",0),
   ("FIB-4 index","index","P",0,"<1.3","1.3–2.67",">2.67",".25","AASLD fibrosis",1),
   ("Liver fat (CAP / MRI-PDFF)","—","X",0,"normal","mild steatosis","mod–severe",".16","AASLD",0),
   ("Bilirubin","mg/dL","C",0,"<1.2","1.2–2.0",">2.0",".10","lab",0),
 ]),
 ("INF","Inflammation & Immune","inflammatory load (central hub, Doc 04)",[
   ("hsCRP","mg/L","C",0,"<1.0","1.0–3.0",">3.0 (>10 acute)",".30","AHA/CDC",1),
   ("WBC","×10⁹/L","C",1,"4.0–9.0","9.1–11 / 3.5–3.9",">11 or <3.5",".18","lab",1),
   ("Ferritin (as inflammation)","ng/mL","P",1,"sex-specific opt","high-normal","very high/low",".14","see NUT/HEM",0),
   ("Neutrophil:Lymphocyte ratio","ratio","P",0,"<2","2–3",">3",".12","literature",0),
   ("IL-6","pg/mL","X",0,"<1.5","1.5–3",">3",".14","research",0),
   ("Albumin (inverse APR)","g/dL","C",1,"4.0–5.0","3.5–3.9","<3.5",".12","lab",0),
 ]),
 ("HEM","Hematologic & Oxygen Transport","oxygen-delivery reserve · iron reserve",[
   ("Hemoglobin","g/dL","C",1,"13.5–17 M / 12–15.5 F","mild low/high","<11 or >18",".25","WHO",1),
   ("Hematocrit","%","C",1,"40–50 M / 36–46 F","borderline","extremes",".12","lab",0),
   ("RDW","%","C",0,"<13.5","13.5–15",">15",".10","lab/prognostic",0),
   ("Platelets","×10⁹/L","C",1,"150–400","100–149 / 401–500","<100 or >500",".13","lab",1),
   ("Iron saturation","%","P",1,"25–45","15–24 / 46–55","<15 or >55",".12","lab",0),
   ("SpO₂","%","C",0,"≥96","92–95","<92",".18","clinical",1),
   ("Ferritin (iron stores)","ng/mL","P",1,"50–150","30–49 / 151–300","<30 or >300",".10","lab",0),
 ]),
 ("ENDO","Endocrine & Hormonal (sex-specific, Doc 05)","allostatic/stress load · metabolic · bone reserve",[
   ("TSH","mIU/L","P",1,"0.5–2.5","2.5–4.5 / 0.3–0.5",">4.5 or <0.3",".22","ATA",0),
   ("Free T4","ng/dL","P",1,"0.9–1.6","borderline","out of range",".10","ATA",0),
   ("Cortisol rhythm (diurnal slope)","AUC","X",0,"healthy slope","flattening","flat/inverted",".14","research",0),
   ("Sex hormones (E2/Prog/FSH/LH/T/SHBG/AMH)","—","P/X",1,"see Doc 05 by phase/stage","—","—",".34","Doc 05",0),
   ("Fasting cortisol","µg/dL","P",1,"5–15","15–20",">20 or <3",".10","lab",0),
   ("Prolactin","ng/mL","P",0,"sex-specific","borderline","high",".10","lab",0),
 ]),
 ("BCM","Body Composition & Musculoskeletal","adiposity (burden) · muscle/strength · bone reserve",[
   ("BMI","kg/m²","C",1,"18.5–24.9","25–29.9 / 17–18.4","≥30 or <17",".12","WHO (ethnicity-adj)",0),
   ("Body fat % (DEXA)","%","X",1,"sex/age optimal","borderline","high/very low",".14","ISCD",0),
   ("ALMI / lean mass index","kg/m²","X",0,"≥ sex cut-off","low-normal","sarcopenic",".16","EWGSOP2",1),
   ("Grip strength","kg","P",0,"≥ sex/age norm","low-normal","weak (frailty)",".14","EWGSOP2",0),
   ("Gait speed","m/s","P",0,">1.0","0.8–1.0","<0.8",".08","EWGSOP2",0),
   ("BMD T-score (DEXA)","SD","X",0,"≥ −1.0","−1.0 to −2.5","≤ −2.5",".20","ISCD/WHO",1),
   ("Visceral adipose","—","X",0,"low","moderate","high",".16","literature",0),
 ]),
 ("NUT","Nutrition & Micronutrients","micronutrient reserve · inflammatory load",[
   ("25-OH Vitamin D","ng/mL","C",1,"30–60","20–29 / 60–80","<20 or >100",".18","Endocrine Soc.",1),
   ("Vitamin B12","pg/mL","P",1,"400–900","200–399","<200 or >1100",".12","lab",0),
   ("Folate","ng/mL","P",0,">5","3–5","<3",".08","lab",0),
   ("Ferritin (stores)","ng/mL","P",1,"50–150","see HEM","extremes",".10","lab",0),
   ("Omega-3 index","%","P",0,"≥8","4–8","<4",".14","literature",0),
   ("Magnesium","mg/dL","P",1,"1.8–2.4","borderline","<1.5 or >2.6",".10","lab",1),
   ("Diet-quality (HEI/Mediterranean)","score","C",0,"high","moderate","poor",".16","survey",0),
   ("Fiber intake","g/day","C",0,"≥25 F / ≥30 M","15–24","<15",".12","survey",0),
 ]),
 ("SLP","Sleep & Circadian Recovery","sleep debt (central burden) · allostatic load · inflammation",[
   ("Sleep duration (14-day mean)","h","C",1,"7–9","6–7 / 9–10","<6 or >10 chronic",".24","NSF/AASM",1),
   ("Sleep regularity index","SRI","C",0,"high","moderate","low (shift-like)",".16","literature",0),
   ("Sleep efficiency","%","C",0,"≥90","85–89","<85",".12","wearable",0),
   ("Deep + REM proportion","%","C",0,"age-normal","low-normal","low",".12","wearable",0),
   ("Overnight SpO₂ / ODI (apnea)","ODI","X",1,"normal","mild","mod–severe OSA",".20","HSAT",1),
   ("Nighttime HR/HRV recovery","—","C",0,"good recovery","borderline","poor",".16","wearable",0),
 ]),
 ("FIT","Physical Activity & Cardiorespiratory Fitness","cardiorespiratory reserve · muscle reserve · glycemic burden (−)",[
   ("VO₂max (est.)","mL/kg/min","P",0,"≥ sex/age p60","p20–p60","< p10",".30","Cooper/ACSM",1),
   ("MVPA","min/week","C",0,"≥150 (≥300 ideal)","75–149","<75",".20","WHO",0),
   ("Steps/day (14-day mean)","steps","C",0,"≥8000","5000–7999","<5000",".14","literature",0),
   ("Sedentary time","h/day","C",0,"<6","6–9",">9",".10","literature",0),
   ("Grip strength (shared w/ BCM)","kg","P",0,"≥ norm","low-normal","weak",".12","EWGSOP2",0),
   ("HR recovery (wearable)","bpm","C",0,"fast","moderate","slow",".14","literature",0),
 ]),
 ("MCS","Mental, Cognitive & Social Health","allostatic/stress load (central) · sleep debt (bidirectional)",[
   ("PHQ-9 (depression)","score","C",0,"0–4","5–14","≥15 (item-9>0 ⇒ red)",".24","validated",1),
   ("GAD-7 (anxiety)","score","C",0,"0–4","5–14","≥15",".18","validated",0),
   ("Perceived stress (PSS)","score","C",0,"low","moderate","high",".14","validated",0),
   ("Loneliness (UCLA-3)","score","C",0,"connected","some isolation","isolated",".14","validated",0),
   ("Cognitive screen (age-adj)","score","X",1,"normal","borderline","impaired",".16","validated",0),
   ("Subjective wellbeing (WHO-5)","score","C",0,"high","moderate","low",".14","validated",0),
 ]),
]

MODIFIERS = [
 ("Smoking / nicotine","PRO + cotinine","multiplies CV, HEM/respiratory, INF risk"),
 ("Alcohol","PRO","modifies HEP, MET, MCS, CV"),
 ("Respiratory (SpO₂, FEV1, smoking)","mixed","folds into HEM oxygen-transport + CV; severe values escalate independently"),
 ("Adherence / engagement","behavioural","not scored as health; feeds the nudge feasibility model (Doc 07) and reservoir inflows (Doc 04)"),
]

# ----------------------------------------------------------------- wearables (Doc 18 / 01)
# (metric, layer, tier, q_source, pillars, devices)
WEARABLES = [
 ("Resting HR","aggregated","consumer-validated","0.6–0.8","CV · FIT · MCS","Apple Watch · Oura · Whoop · Fitbit"),
 ("HRV (RMSSD)","aggregated","consumer-validated","0.6–0.8","CV · MCS · SLP · ENDO","Oura · Whoop · Apple Watch"),
 ("Steps / MVPA","aggregated","consumer-validated","0.6–0.8","FIT · MET · CV","all wrist/phone"),
 ("Sleep duration / efficiency / regularity","aggregated","consumer-validated","0.6–0.8","SLP · MCS","Oura · Whoop · Apple Watch"),
 ("SpO₂ (spot/overnight)","aggregated","consumer-validated","0.6–0.8","HEM · SLP","Apple Watch · Oura"),
 ("Skin / body temperature","aggregated","consumer-validated","0.6–0.8","ENDO (cycle) · INF (illness)","Oura · Whoop"),
 ("CGM glucose / time-in-range","aggregated","clinical-grade","0.85–1.0","MET","Freestyle Libre · Dexcom"),
 ("Blood pressure (cuff)","aggregated","clinical-grade","0.85–1.0","CV · REN","Omron · Withings BPM"),
 ("Single-lead ECG / rhythm","derived→confirmed","clinical-grade","0.85–1.0","CV","Apple Watch · KardiaMobile"),
 ("Weight / body composition","aggregated","consumer-validated","0.6–0.8","BCM · MET","smart scales (Withings/Garmin)"),
 ("VO₂max (estimate)","derived","inferential","informational only","FIT · CV","Apple Watch · Garmin"),
 ("Readiness / recovery","derived","inferential","informational only","SLP · MCS · FIT","Oura · Whoop · Garmin"),
 ("Stress score","derived","inferential","informational only","MCS","Garmin · Fitbit · Whoop"),
 ("Sleep stages (REM/deep/light)","derived","inferential","informational only","SLP","all wearables"),
 ("Respiratory rate","derived","inferential","informational only","HEM · SLP","Oura · Whoop · Apple Watch"),
]

# ----------------------------------------------------------------- instruments (Doc 02 MCS, Doc 18)
LIKERT_0_3 = '0 = not at all · 1 = several days · 2 = more than half the days · 3 = nearly every day'
INSTRUMENTS = [
 {"id":"PHQ-9","name":"PHQ-9 — Patient Health Questionnaire (depression)","pillar":"MCS","cadence":"quarterly / on-trigger",
  "stem":"Over the last 2 weeks, how often have you been bothered by any of the following problems?","scale":LIKERT_0_3,
  "items":["Little interest or pleasure in doing things","Feeling down, depressed, or hopeless",
   "Trouble falling or staying asleep, or sleeping too much","Feeling tired or having little energy",
   "Poor appetite or overeating","Feeling bad about yourself — or that you are a failure",
   "Trouble concentrating on things","Moving or speaking slowly, or being fidgety/restless",
   "Thoughts that you would be better off dead, or of hurting yourself"],
  "scoring":"Sum 0–27. 0–4 none/minimal · 5–9 mild · 10–14 moderate · 15–19 mod-severe · 20–27 severe.",
  "bands":"green 0–4 · yellow 5–14 · red ≥15","safety":"Item 9 > 0 forces MCS red/critical irrespective of total and fires the crisis pathway (Doc 11). Never averaged away.",
  "src":"Kroenke 2001 (public domain)"},
 {"id":"PHQ-2","name":"PHQ-2 — ultra-brief depression screen","pillar":"MCS","cadence":"Core (every visit)",
  "stem":"Over the last 2 weeks, how often bothered by…","scale":LIKERT_0_3,
  "items":["Little interest or pleasure in doing things","Feeling down, depressed, or hopeless"],
  "scoring":"Sum 0–6; ≥3 → administer full PHQ-9.","bands":"≥3 triggers PHQ-9","safety":"Positive screen escalates to PHQ-9.","src":"Kroenke 2003"},
 {"id":"GAD-7","name":"GAD-7 — Generalized Anxiety Disorder","pillar":"MCS","cadence":"quarterly / on-trigger",
  "stem":"Over the last 2 weeks, how often have you been bothered by the following problems?","scale":LIKERT_0_3,
  "items":["Feeling nervous, anxious, or on edge","Not being able to stop or control worrying",
   "Worrying too much about different things","Trouble relaxing","Being so restless that it is hard to sit still",
   "Becoming easily annoyed or irritable","Feeling afraid as if something awful might happen"],
  "scoring":"Sum 0–21. 0–4 minimal · 5–9 mild · 10–14 moderate · 15–21 severe.","bands":"green 0–4 · yellow 5–14 · red ≥15",
  "safety":"—","src":"Spitzer 2006 (public domain)"},
 {"id":"GAD-2","name":"GAD-2 — ultra-brief anxiety screen","pillar":"MCS","cadence":"Core (every visit)","scale":LIKERT_0_3,
  "stem":"Over the last 2 weeks, how often bothered by…",
  "items":["Feeling nervous, anxious, or on edge","Not being able to stop or control worrying"],
  "scoring":"Sum 0–6; ≥3 → administer full GAD-7.","bands":"≥3 triggers GAD-7","safety":"—","src":"Kroenke 2007"},
 {"id":"AUDIT-C","name":"AUDIT-C — alcohol use","pillar":"HEP / MET / MCS","cadence":"Core","scale":"item-specific 0–4",
  "stem":"Alcohol consumption screen (WHO).",
  "items":["How often do you have a drink containing alcohol? (0 never … 4 ≥4×/week)",
   "How many standard drinks on a typical drinking day? (0:1–2 … 4:≥10)",
   "How often do you have ≥6 drinks on one occasion? (0 never … 4 daily/almost)"],
  "scoring":"Sum 0–12.","bands":"at-risk ≥4 M / ≥3 F","safety":"High scores route to brief intervention + LFTs.","src":"Bush 1998 (WHO)"},
 {"id":"PSS-4","name":"PSS-4 — Perceived Stress Scale (short)","pillar":"MCS / ENDO","cadence":"quarterly","scale":"0 never … 4 very often",
  "stem":"In the last month, how often have you felt…",
  "items":["…unable to control the important things in your life?","…confident about your ability to handle personal problems? (reverse)",
   "…that things were going your way? (reverse)","…difficulties piling up so high you could not overcome them?"],
  "scoring":"Reverse items 2–3, sum 0–16; higher = more stress.","bands":"green low · yellow moderate · red high","safety":"—","src":"Cohen 1983"},
 {"id":"UCLA-3","name":"UCLA-3 — loneliness / social connection","pillar":"MCS","cadence":"quarterly","scale":"1 hardly ever · 2 some of the time · 3 often",
  "stem":"How often do you feel…",
  "items":["…that you lack companionship?","…left out?","…isolated from others?"],
  "scoring":"Sum 3–9; ≥6 ≈ lonely.","bands":"green ≤4 · yellow 5 · red ≥6","safety":"—","src":"Hughes 2004"},
 {"id":"WHO-5","name":"WHO-5 — Well-Being Index","pillar":"MCS","cadence":"quarterly","scale":"0 at no time … 5 all of the time",
  "stem":"Over the last 2 weeks…",
  "items":["I have felt cheerful and in good spirits","I have felt calm and relaxed","I have felt active and vigorous",
   "I woke up feeling fresh and rested","My daily life has been filled with things that interest me"],
  "scoring":"Sum ×4 → 0–100; <50 low well-being, <28 screen for depression.","bands":"green ≥50 · yellow 28–49 · red <28","safety":"<28 → PHQ-9.","src":"WHO 1998"},
 {"id":"ISI","name":"ISI — Insomnia Severity Index","pillar":"SLP","cadence":"on-trigger","scale":"0–4 per item",
  "stem":"Rate the severity of your insomnia problems in the last 2 weeks.",
  "items":["Difficulty falling asleep","Difficulty staying asleep","Problem waking too early",
   "Satisfaction with current sleep pattern","Interference with daily functioning",
   "Noticeability to others of impairment","Worry/distress about sleep"],
  "scoring":"Sum 0–28.","bands":"0–7 none · 8–14 sub-threshold · 15–21 moderate · 22–28 severe","safety":"Pairs with overnight SpO₂/ODI → OSA referral.","src":"Morin 2011"},
 {"id":"IPAQ-SF","name":"IPAQ-SF — activity (self-report, cross-checks wearable)","pillar":"FIT","cadence":"Core","scale":"days/week × min/day",
  "stem":"In the last 7 days, time spent in vigorous, moderate, and walking activity (+ sitting).",
  "items":["Vigorous-intensity days & minutes","Moderate-intensity days & minutes","Walking days & minutes","Sitting time on a weekday"],
  "scoring":"Convert to MET-min/week; map to MVPA band.","bands":"≥150 min MVPA green","safety":"Self-report cross-checked vs wearable steps/MVPA for plausibility (Doc 01).","src":"IPAQ 2002"},
 {"id":"DIET","name":"Diet-quality screener (Mediterranean / HEI-style)","pillar":"NUT / MET","cadence":"Core","scale":"frequency per item",
  "stem":"Usual weekly intake across food groups.",
  "items":["Vegetables & fruit servings/day","Whole grains vs refined","Legumes/nuts per week","Fish per week",
   "Olive oil as main fat","Red/processed meat per week","Sugary drinks/day","Ultra-processed food frequency"],
  "scoring":"Composite 0–14 (Mediterranean) → diet-quality band.","bands":"high / moderate / poor","safety":"—","src":"PREDIMED-style"},
 {"id":"SUBSTANCE","name":"Smoking / nicotine / shisha (cross-cutting modifier)","pillar":"CV · HEM · INF","cadence":"Core","scale":"status + pack-years",
  "stem":"Tobacco / nicotine / waterpipe use.",
  "items":["Current status (never / former / current)","Cigarettes/day & pack-years","Waterpipe (shisha) sessions/week",
   "Vaping/nicotine pouches","Cotinine (if available, objective)"],
  "scoring":"Risk multiplier on CV, HEM/respiratory, INF (Doc 02 modifiers).","bands":"never / former / current","safety":"Current use raises CV/respiratory weighting and screening.","src":"Doc 02"},
 # --- respiratory / atopy (resolve question-bank validated_by) ---
 {"id":"TNSS","name":"TNSS — Total Nasal Symptom Score (allergic rhinitis)","pillar":"INF","cadence":"on-trigger","scale":"0 none … 3 severe per symptom",
  "stem":"Rate each nasal symptom over the past 24 h / typical day.",
  "items":["Nasal congestion","Rhinorrhoea (runny nose)","Sneezing","Nasal itching"],
  "scoring":"Sum 0–12.","bands":"0–3 mild · 4–7 moderate · 8–12 severe","safety":"—","src":"Downie 2004 (illustrative)"},
 {"id":"ACQ","name":"ACQ — Asthma Control Questionnaire","pillar":"INF","cadence":"on-trigger",
  "scale":"0 (none) … 6 (severe) per item; reliever item banded","stem":"Over the past week…",
  "items":["Night waking from asthma","Symptoms on waking","Activity limitation","Shortness of breath","Wheeze","Reliever puffs/day"],
  "scoring":"Mean of items 0–6.","bands":"<0.75 well-controlled · 0.75–1.5 grey · ≥1.5 uncontrolled","safety":"Uncontrolled → clinician review; pairs with peak-flow.","src":"Juniper 1999"},
 {"id":"POEM","name":"POEM — Patient-Oriented Eczema Measure","pillar":"INF","cadence":"on-trigger","scale":"0 no days … 4 every day per item",
  "stem":"Over the last week, how many days did your skin…",
  "items":["Itch","Disturb sleep","Bleed","Weep/ooze","Crack","Flake","Feel dry/rough"],
  "scoring":"Sum 0–28.","bands":"0–2 clear · 3–7 mild · 8–16 moderate · 17–24 severe · 25–28 very severe","safety":"—","src":"Charman 2004"},
 {"id":"MRC_DYSPNOEA","name":"MRC Dyspnoea Scale (breathlessness)","pillar":"CV · HEM","cadence":"on-trigger","scale":"grade 1–5",
  "stem":"Which best describes your breathlessness?",
  "items":["1 — only on strenuous exertion","2 — hurrying / slight hill","3 — walks slower / stops on the level","4 — stops after ~100 m","5 — too breathless to leave the house / on dressing"],
  "scoring":"Single grade 1–5.","bands":"3–5 significant functional limitation","safety":"Grade ≥4 with desaturation → expedited review (pairs with SpO₂).","src":"Fletcher 1959 (mMRC)"},
 # --- mental / cognitive ---
 {"id":"EPDS","name":"EPDS — Edinburgh Postnatal Depression Scale","pillar":"MCS","cadence":"pregnancy / postpartum",
  "scale":"0–3 per item (10 items)","stem":"In the past 7 days (perinatal mood)…",
  "items":["Able to laugh / see the funny side","Looked forward with enjoyment","Blamed myself unnecessarily","Anxious or worried","Scared / panicky","Things getting on top of me","Unhappy → difficulty sleeping","Sad or miserable","So unhappy I have been crying","Thoughts of harming myself"],
  "scoring":"Sum 0–30.","bands":"green <10 · yellow 10–12 · red ≥13 (likely depression)","safety":"Item 10 > 0 (self-harm) forces MCS red/critical and fires the crisis pathway (Doc 11), like PHQ-9 item 9.","src":"Cox 1987"},
 {"id":"MINI-COG","name":"Mini-Cog / cognitive screen (age-adjusted)","pillar":"MCS","cadence":"annual ≥65 / on-trigger",
  "scale":"recall 0–3 + clock 0 or 2","stem":"Three-word recall + clock-drawing test.",
  "items":["Register & recall 3 words","Draw a clock to a set time (normal = 2)","Total = recall + clock"],
  "scoring":"0–5.","bands":"≥3 likely normal · <3 screen-positive → MoCA/MMSE + clinician","safety":"Screen-positive routes to formal cognitive assessment, never an autonomous diagnosis.","src":"Borson 2000"},
 {"id":"PHQ-15","name":"PHQ-15 — somatic symptom burden","pillar":"MCS","cadence":"quarterly / on-trigger","scale":"0 not bothered … 2 bothered a lot",
  "stem":"Over the last 4 weeks, bothered by… (15 somatic symptoms)",
  "items":["Stomach pain","Back pain","Pain in arms/legs/joints","Headaches","Chest pain","Dizziness","Palpitations","Shortness of breath","Bowel symptoms","Fatigue / low energy","Trouble sleeping"],
  "scoring":"Sum 0–30.","bands":"0–4 minimal · 5–9 low · 10–14 medium · 15–30 high","safety":"High somatic burden cross-checks MCS and the relevant organ pillars.","src":"Kroenke 2002"},
 {"id":"DAST-10","name":"DAST-10 — Drug Abuse Screening Test","pillar":"MCS","cadence":"on-trigger","scale":"yes / no (10 items)",
  "stem":"In the past 12 months, regarding non-medical drug use…",
  "items":["Used more than intended","Unable to stop","Neglected obligations","Guilt about use","Withdrawal symptoms","Medical / social / legal problems from use"],
  "scoring":"Sum 0–10.","bands":"0 none · 1–2 low · 3–5 moderate · 6–8 substantial · 9–10 severe","safety":"Moderate+ routes to brief intervention / referral.","src":"Skinner 1982"},
 {"id":"AUDIT","name":"AUDIT — Alcohol Use Disorders Identification Test (full)","pillar":"HEP · MCS","cadence":"on-trigger (AUDIT-C positive)","scale":"0–4 per item (10 items)",
  "stem":"Full alcohol screen when AUDIT-C is positive.",
  "items":["AUDIT-C consumption items 1–3","Impaired control over drinking","Failed normal expectations","Morning drinking","Guilt after drinking","Blackouts","Injury from drinking","Others concerned"],
  "scoring":"Sum 0–40.","bands":"0–7 low · 8–15 hazardous · 16–19 harmful · ≥20 likely dependence","safety":"≥20 → assess dependence/withdrawal; LFTs; brief intervention.","src":"Saunders 1993 (WHO)"},
 # --- frailty / sleep / sex-specific ---
 {"id":"SARC-F","name":"SARC-F — sarcopenia / frailty screen","pillar":"BCM","cadence":"annual (≥65)","scale":"0–2 per item",
  "stem":"Difficulty with strength, ambulation, rising, stairs, falls.",
  "items":["Lifting / carrying ~4.5 kg","Walking across a room","Rising from a chair / bed","Climbing 10 stairs","Falls in the past year"],
  "scoring":"Sum 0–10.","bands":"≥4 suggests sarcopenia → grip / gait + ALMI (Doc 02 BCM)","safety":"Positive routes to functional assessment + resistance-training plan.","src":"Malmstrom 2013"},
 {"id":"PSQI","name":"PSQI — Pittsburgh Sleep Quality Index","pillar":"SLP","cadence":"on-trigger","scale":"7 components, 0–3 each",
  "stem":"Sleep quality over the last month (7 components).",
  "items":["Subjective sleep quality","Sleep latency","Sleep duration","Habitual efficiency","Disturbances","Sleep-medication use","Daytime dysfunction"],
  "scoring":"Global 0–21.","bands":">5 = poor sleep quality","safety":"Pairs with ISI + wearable sleep; high score + snoring → STOP-BANG / OSA.","src":"Buysse 1989"},
 {"id":"STOP-BANG","name":"STOP-BANG — OSA risk screen","pillar":"SLP","cadence":"on-trigger","scale":"yes / no (8 items)",
  "stem":"Snoring, Tiredness, Observed apnoea, Pressure (BP), BMI, Age, Neck, Gender.",
  "items":["Loud Snoring","Daytime Tiredness","Observed apnoea","high blood Pressure","BMI > 35","Age > 50","Neck > 40 cm","male sex"],
  "scoring":"Sum 0–8.","bands":"0–2 low · 3–4 intermediate · 5–8 high OSA risk","safety":"High risk → overnight SpO₂/ODI or HSAT (Doc 02 SLP); cross-checks ISI.","src":"Chung 2008"},
 {"id":"IIEF-5","name":"IIEF-5 — erectile function (male)","pillar":"ENDO","cadence":"on-trigger","scale":"1–5 per item (5 items)",
  "stem":"Over the past 6 months (male sexual function)…",
  "items":["Confidence in getting an erection","Erections firm enough for penetration","Maintaining after penetration","Maintaining to completion","Satisfaction with intercourse"],
  "scoring":"Sum 5–25.","bands":"22–25 none · 17–21 mild · 12–16 mild-mod · 8–11 moderate · 5–7 severe ED","safety":"ED can be an early CV/endothelial and low-testosterone marker — cross-check CV/ENDO.","src":"Rosen 1999 (male)"},
 {"id":"MENQOL","name":"MENQOL — Menopause-specific Quality of Life (female)","pillar":"ENDO","cadence":"on-trigger","scale":"Likert; higher = more bothersome",
  "stem":"Bother from menopausal symptoms across four domains.",
  "items":["Vasomotor (hot flushes, sweats)","Psychosocial","Physical","Sexual"],
  "scoring":"Domain mean scores (higher = worse QoL).","bands":"descriptor by domain","safety":"Severe vasomotor/sleep impact → ENDO/SLP attention; HRT decision is clinician-led.","src":"Hilditch 1996 (female)"},
]

# ----------------------------------------------------------------- personas (from the live calculator)
PERSONAS = [
 ("healthy","Healthy adult",40,"—","0.90","—","Well-represented reference cohort."),
 ("prediabetic","Pre-diabetes",52,"—","0.85","MET 1.6 · CV 1.3 · REN 1.1","Glycemic burden accumulating; metabolic markers trending up."),
 ("ckd","CKD-3 on ACE-i",60,"ACE-i","0.80","REN 2.0 · CV 1.4 · MET 1.2","BP/UACR managed; mild K⁺ rise drug-expected (Watch, not Alert); eGFR declining."),
 ("athlete","Endurance athlete",30,"—","0.40","—","RHR 42 & low glucose are adaptive; out-of-cohort → cohort stats suppressed, confidence lowered."),
 ("betablocker","On β-blocker",58,"β-blocker","0.70","CV 1.3","RHR & VO₂max confounded (down-weighted, not scored as fitness); BP managed."),
 ("elderly","Older adult (84)",84,"ACE-i","0.60","REN 1.2 · BCM 1.3 · FIT 1.1","Age-adjusted BMI/BP(J-curve)/eGFR/VO₂max frames; low-yield flags de-prioritized; sarcopenic drift; NUT competing-risk-muted."),
 ("dialysis","ESRD on dialysis",64,"—","0.50","REN 1.5 · CV 1.4","eGFR excluded (adequacy frame); structural damage fixed; renal anemia expected; NUT/SLP competing-risk-muted."),
 ("pregnancy","Pregnancy (T2)",31,"—","0.50","ENDO 1.3 · HEM 1.2 · CV 1.2","Physiologic anemia & eGFR-rise expected (bands shifted); pre-eclampsia/GDM absolute anchors retained."),
 ("menopause","Postmenopausal",56,"—","0.82","CV 1.2 · BCM 1.3","Estrogen-loss CV & bone-loss risk (BMD red sooner); ApoB drift; vasomotor sleep impact."),
 ("southasian","South-Asian male",45,"—","1.0","MET 1.4 · CV 1.3","WHO Asian BMI/waist cut-points; ASCVD under-predicts → ethnicity-aware equation; G6PD/FH/vit-D screening raised (D18)."),
 ("ramadan_dm","Gulf-Arab T2D, Ramadan",52,"metformin · SGLT2i","1.0","MET 1.5","Med-timing & hypo/dehydration safety (IDF-DAR); SGLT2i held on dehydration risk; clinician-set fast-break rule."),
 ("fh","Familial hypercholesterolemia",38,"—","0.75","CV 1.5","ApoB genetic, NOT lifestyle-modifiable (needs medication); modifiability low."),
 ("labartifact","Lab artifact (hemolysis)",55,"—","0.85","—","Isolated implausible K⁺ 6.6 → reconfirm, not emergency (Doc 12 §3.4); corroboration would escalate."),
]

# ----------------------------------------------------------------- pillar base weights + constants (Doc 03)
PILLAR_W = [("CV",.13),("MET",.12),("REN",.07),("HEP",.06),("INF",.07),("HEM",.06),
            ("ENDO",.06),("BCM",.07),("NUT",.06),("SLP",.10),("FIT",.10),("MCS",.10)]
CONSTANTS = [
 ("φ","cohort-blend weight (Stage 2)","0.60","Doc 03 §2"),
 ("κ_resp","personal-baseline responsiveness cap (Stage 2b)","0.10","Doc 03 §2b"),
 ("γ","pillar power-mean exponent","3","Doc 03 §3"),
 ("δ","PureScore power-mean exponent","2","Doc 03 §5"),
 ("ρ_k","reservoir contribution cap to a pillar","0.20","Doc 03 §3"),
 ("R_crit","critical-pillar risk floor","0.60","Doc 03 §4"),
 ("PURE_CRIT_CAP","PureScore cap when any pillar critical","40","Doc 03 §5"),
 ("zone cuts","green / yellow / red on r","0.15 / 0.50","README §3.5"),
]
QSOURCE = [
 ("Lab (venous)","1.0","clinical-grade reference"),
 ("Wearable — clinical-grade (CGM, validated cuff, single-lead ECG)","0.85–1.0","near-lab; can corroborate a critical"),
 ("Wearable — consumer-validated (resting HR, steps, sleep duration)","0.6–0.8","feeds score, discounted; Watch/Advisory only"),
 ("Wearable — inferential/derived (readiness, stress, sleep stages)","informational only","never sets a band"),
 ("Consumer survey / PRO","0.5–0.7","self-report, plausibility-checked"),
 ("Literature fallback","0.2–0.4","cohort median when no own data; lowers coverage"),
]

# =================================================================== HTML helpers
def _esc(s):
    return str(s).replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")

def _chip(text, cls):
    return '<span class="chip %s">%s</span>' % (cls, _esc(text))

def _rngbar(two_sided):
    if two_sided:
        return ('<span class="range-mini" title="low red · yellow · green · yellow · high red">'
                '<i class="rg-lo" style="width:14%"></i><i class="rg-y1" style="width:18%"></i>'
                '<i class="rg-g" style="width:36%"></i><i class="rg-y2" style="width:18%"></i>'
                '<i class="rg-hi" style="width:14%"></i></span>')
    return ('<span class="range-mini" title="green · yellow · red">'
            '<i class="rg-g" style="width:54%"></i><i class="rg-y2" style="width:28%"></i>'
            '<i class="rg-hi" style="width:18%"></i></span>')

ILLUS = ('<div class="callout note"><div class="ct">Illustrative</div>All bands, weights and cut-points '
         'on this page are literature/guideline-anchored examples and <b>must be re-verified and '
         'cohort-adjusted before any production use</b> (README §5.6). This is design documentation, '
         'not a validated medical device.</div>')

# =================================================================== typed source channel per marker (closes F7)
_CHANNELS = ["biomarker-lab", "wearable-clinical", "wearable-consumer", "wearable-inferential", "self-report", "derived"]
_CHAN_META = {  # channel -> (short label, chip class, accuracy note)
    "biomarker-lab":        ("lab",          "b-green",  "venous/clinical assay or imaging — highest accuracy"),
    "wearable-clinical":    ("wear-clinical","b-green",  "CGM / validated cuff / ECG / HSAT — clinical-grade (D22)"),
    "wearable-consumer":    ("wear-consumer","b-acc",    "consumer sensor — trajectory & early-warning, Watch-capped (D22)"),
    "wearable-inferential": ("wear-inferred","b-yellow", "device-inferred (VO₂max, sleep-stages) — informational only (D22)"),
    "self-report":          ("PRO",          "b-yellow", "patient-reported / administered instrument"),
    "derived":              ("derived",      "b-mut",    "computed index (eGFR, HOMA-IR, FIB-4, BMI…)"),
}
def _has_kw(s, kws):
    s = s.lower(); return any(k in s for k in kws)
def marker_channel(name, src):
    """Deterministic typed source-channel for a marker (name + guideline source). Total over the catalogue."""
    n = name.lower(); s = (src or "").lower()
    if _has_kw(n, ["phq", "gad", "pss", "ucla", "who-5", "loneliness", "perceived stress", "cognitive screen",
                   "diet-quality", "fiber intake"]) or s in ("survey", "validated"):
        return "self-report"
    if _has_kw(n, ["cgm", "single-lead", "ecg", "systolic bp", "diastolic bp", "hsat"]) or s in ("consensus tir", "consensus", "hsat"):
        return "wearable-clinical"
    if _has_kw(n, ["vo₂max", "vo2max", "deep + rem", "rem proportion", "readiness"]):
        return "wearable-inferential"
    if _has_kw(n, ["homa", "egfr", "fib-4", "almi", "lean mass", "neutrophil", "iron saturation", "diurnal slope", "cortisol rhythm"]) or n.startswith("bmi"):
        return "derived"
    if "wearable" in s or _has_kw(n, ["resting hr", "hrv", "sleep duration", "sleep regularity", "sleep efficiency",
                                      "spo", "steps", "mvpa", "sedentary", "hr recovery", "gait speed", "nighttime hr"]):
        return "wearable-consumer"
    return "biomarker-lab"
def _chan_chip(ch):
    lbl, cls, note = _CHAN_META.get(ch, (ch, "b-mut", ""))
    return '<span class="chip %s" title="%s">%s</span>' % (cls, _esc(note), _esc(lbl))

# =================================================================== APPENDIX BUILDERS
def build_biomarkers():
    h = ['<div class="crumbs"><a href="index.html">Home</a> › Reference › Biomarker catalogue</div>',
         '<h1>Appendix A — Biomarker Catalogue</h1>',
         '<p class="lead">Every marker across the 12 pillars: tier, two-sidedness, green/yellow/red bands, '
         'within-pillar weight and source. Compiled from <a class="xref" href="02-pillars-and-marker-catalog.html">Doc 02</a>; '
         'the scoring math is <a class="xref" href="03-scoring-formula.html">Doc 03</a>.</p>', ILLUS,
         '<div class="tagrow" style="margin:10px 0 18px">'
         '<span class="tier C">C</span> Core <span class="tier P">P</span> Peripheral '
         '<span class="tier X">X</span> Comprehensive &nbsp; · &nbsp; <span class="chip b-mut">2s</span> two-sided '
         '(low <i>and</i> high adverse)</div>']
    for pid, pname, res, rows in PILLARS:
        h.append('<h2 id="%s">%s · %s</h2>' % (pid, pid, _esc(pname)))
        h.append('<p class="small muted">Reservoir links: %s</p>' % _esc(res))
        h.append('<div class="tablewrap"><table><thead><tr>'
                 '<th>Marker</th><th>Unit</th><th>T</th><th>Channel</th><th>Band shape</th>'
                 '<th>Green</th><th>Yellow</th><th>Red</th><th>w</th><th>Source</th></tr></thead><tbody>')
        for (mk, unit, t, ts, g, y, r, w, src, crit) in rows:
            star = ' <span title="critical marker — can make its pillar critical" style="color:var(--red)">★</span>' if crit else ''
            h.append('<tr><td><b>%s</b>%s</td><td class="small muted">%s</td>'
                     '<td><span class="tier %s">%s</span></td><td>%s</td><td>%s</td>'
                     '<td><span class="chip b-green">%s</span></td>'
                     '<td><span class="chip b-yellow">%s</span></td>'
                     '<td><span class="chip b-red">%s</span></td>'
                     '<td class="mono">%s</td><td class="small muted">%s</td></tr>'
                     % (_esc(mk), star, _esc(unit), t, t.replace("/","/"), _chan_chip(marker_channel(mk, src)),
                        _rngbar(ts), _esc(g), _esc(y), _esc(r), _esc(w), _esc(src)))
        h.append('</tbody></table></div>')
    h.append('<h2 id="modifiers">Cross-cutting modifiers</h2>')
    h.append('<div class="tablewrap"><table><thead><tr><th>Modifier</th><th>Source</th><th>Effect</th></tr></thead><tbody>')
    for nm, sr, ef in MODIFIERS:
        h.append('<tr><td><b>%s</b></td><td class="small muted">%s</td><td>%s</td></tr>' % (_esc(nm), _esc(sr), _esc(ef)))
    h.append('</tbody></table></div>')
    h.append('<p class="small muted">★ = critical marker — a red value can make its pillar critical and cascade '
             'PureScore into the critical band (<a class="xref" href="03-scoring-formula.html">Doc 03</a> §4). '
             'Sex-specific overrides: <a class="xref" href="05-sex-specific-models.html">Doc 05</a>.</p>')
    return "Appendix A · Biomarkers", "".join(h)

def build_wearables():
    h = ['<div class="crumbs"><a href="index.html">Home</a> › Reference › Wearable metrics</div>',
         '<h1>Appendix B — Wearable Metrics &amp; Trust Tiers</h1>',
         '<p class="lead">Every wearable signal, its abstraction layer (raw → aggregated → derived), '
         'its <b>trust tier</b> (D22), the confidence weight <code>q_source</code> it earns, the pillars it feeds and '
         'representative devices. See <a class="xref" href="18-data-streams-and-experience.html">Doc 18</a> and '
         '<a class="xref" href="01-data-model-and-reference-ranges.html">Doc 01</a> §2.</p>', ILLUS,
         '<div class="tagrow" style="margin:10px 0 16px">'
         '<span class="chip b-green">clinical-grade ≈ lab</span>'
         '<span class="chip b-acc">consumer-validated · discounted</span>'
         '<span class="chip b-yellow">inferential · informational only</span></div>',
         '<div class="callout safety"><div class="ct">Safety rule (D22)</div>A consumer or inferential wearable '
         'anomaly may raise <b>Watch/Advisory</b> but <b>cannot drive a red/critical without a clinical-grade '
         'confirmation</b> (CGM / validated cuff / single-lead ECG, or a lab). '
         '(<a class="xref" href="12-critical-review-and-purescore-2.0.html">Doc 12</a> §3.4)</div>',
         '<div class="tablewrap"><table><thead><tr><th>Metric</th><th>Layer</th><th>Trust tier</th>'
         '<th>q_source</th><th>Pillars</th><th>Devices</th></tr></thead><tbody>']
    tcls = {"clinical-grade":"b-green","consumer-validated":"b-acc","inferential":"b-yellow"}
    for (m, layer, tier, q, pil, dev) in WEARABLES:
        h.append('<tr><td><b>%s</b></td><td class="small muted">%s</td>'
                 '<td><span class="chip %s">%s</span></td><td class="mono small">%s</td>'
                 '<td class="small">%s</td><td class="small muted">%s</td></tr>'
                 % (_esc(m), _esc(layer), tcls.get(tier,"b-mut"), _esc(tier), _esc(q), _esc(pil), _esc(dev)))
    h.append('</tbody></table></div>')
    return "Appendix B · Wearables", "".join(h)

def build_questions():
    h = ['<div class="crumbs"><a href="index.html">Home</a> › Questions &amp; intake › Screeners & PROs</div>',
         '<h1>Appendix C — Clinical Screeners &amp; PROs</h1>',
         '<p class="lead">The <b>validated, standardized patient-reported-outcome (PRO) instruments and clinical '
         'screeners</b> HikmaEngine administers — full item text, scoring and band cut-offs. These are the '
         'literature-validated instruments (PHQ-9, GAD-7, AUDIT-C, ISI…), distinct from the broad lifestyle/condition '
         '<a class="xref" href="appendix-question-bank.html">Question bank (E)</a> and the '
         '<a class="xref" href="appendix-lifestyles.html">Lifestyles (F)</a> model — the bank\'s '
         '<code>validated_by</code> links escalate a crude item to the matching instrument here. They are the '
         '<span class="flag fl-life">LIFE</span> stream (<a class="xref" href="18-data-streams-and-experience.html">'
         'Doc 18</a> §1) and the MCS/behavioural markers of '
         '<a class="xref" href="02-pillars-and-marker-catalog.html">Doc 02</a>.</p>', ILLUS,
         '<p class="small muted"><b>%d instruments</b> across mental-health, sleep, respiratory/atopy, '
         'metabolic/activity, substance, frailty, cognition and sex-specific domains.</p>' % len(INSTRUMENTS)]
    for q in INSTRUMENTS:
        h.append('<div class="panel"><h3 id="%s">%s</h3>' % (_esc(q["id"]), _esc(q["name"])))
        h.append('<div class="tagrow" style="margin-bottom:8px">'
                 '<span class="chip b-teal">%s</span><span class="chip b-mut">cadence: %s</span>'
                 '<span class="chip b-mut">source: %s</span></div>' % (_esc(q["pillar"]), _esc(q["cadence"]), _esc(q["src"])))
        if q.get("stem"): h.append('<p class="small muted"><i>%s</i></p>' % _esc(q["stem"]))
        if q.get("scale"): h.append('<p class="small"><b>Response scale:</b> %s</p>' % _esc(q["scale"]))
        h.append('<ol class="small">')
        for it in q["items"]: h.append('<li>%s</li>' % _esc(it))
        h.append('</ol>')
        h.append('<table style="margin-top:6px"><tbody>'
                 '<tr><td style="width:120px" class="small muted">Scoring</td><td class="small">%s</td></tr>'
                 '<tr><td class="small muted">Bands</td><td class="small">%s</td></tr>' % (_esc(q["scoring"]), _esc(q["bands"])))
        if q.get("safety") and q["safety"] != "—":
            h.append('<tr><td class="small muted">Safety</td><td class="small" style="color:#ff9bae">%s</td></tr>' % _esc(q["safety"]))
        h.append('</tbody></table></div>')
    return "Appendix C · Screeners & PROs", "".join(h)

def build_personas():
    h = ['<div class="crumbs"><a href="index.html">Home</a> › Reference › Personas</div>',
         '<h1>Appendix D — Personas &amp; Edge Cases</h1>',
         '<p class="lead">The cohort/edge-case personas that exercise the engine — each with age, medications, '
         'representativeness <code>rep</code> (drives Confidence), pillar weight multipliers, and the interpretation '
         'frame they test. Used by <a class="xref" href="05-sex-specific-models.html">Doc 05</a>, '
         '<a class="xref" href="15-uae-localization.html">Doc 15</a> and the live calculator.</p>', ILLUS,
         '<div class="tablewrap"><table><thead><tr><th>Persona</th><th>Age</th><th>Meds</th>'
         '<th>rep</th><th>Weight multipliers</th><th>Frame it tests</th></tr></thead><tbody>']
    for (k, label, age, meds, rep, wm, note) in PERSONAS:
        h.append('<tr><td><b>%s</b><br><span class="small muted mono">%s</span></td><td>%s</td>'
                 '<td class="small">%s</td><td class="mono">%s</td><td class="small">%s</td>'
                 '<td class="small muted">%s</td></tr>'
                 % (_esc(label), _esc(k), age, _esc(meds), _esc(rep), _esc(wm), _esc(note)))
    h.append('</tbody></table></div>')
    return "Appendix D · Personas", "".join(h)

# =================================================================== APPENDIX E — question bank
_BURDEN = {"atherogenic_burden", "vascular_bp_load", "glycemic_burden", "adiposity",
           "hepatic_fat", "inflammatory_load", "allostatic_load", "sleep_debt"}

def _dir_chip(d):
    return {"increase_risk": '<span class="chip b-red">↑ risk</span>',
            "decrease_risk": '<span class="chip b-green">↓ risk</span>',
            "neutral": '<span class="chip b-mut">·</span>'}.get(d, '<span class="chip b-mut">·</span>')

def _pdeltas(d):
    if not d: return '<span class="small muted">—</span>'
    out = []
    for k, v in d.items():
        cls = "b-red" if (v or 0) > 0 else ("b-green" if (v or 0) < 0 else "b-mut")
        out.append('<span class="chip %s mono">%s %+.2f</span>' % (cls, _esc(k), v))
    return " ".join(out)

def _rdeltas(d):
    if not d: return '<span class="small muted">—</span>'
    out = []
    for k, v in d.items():
        v = v or 0
        # burden ↑ = bad (red); reserve ↑ = good (green)
        if k in _BURDEN:
            cls = "b-red" if v > 0 else ("b-green" if v < 0 else "b-mut")
        else:
            cls = "b-green" if v > 0 else ("b-red" if v < 0 else "b-mut")
        out.append('<span class="chip %s mono">%s %+.2f</span>' % (cls, _esc(k), v))
    return " ".join(out)

def _deps_line(dep):
    if not dep: return ""
    bits = []
    for cond in dep.get("show_if", []):
        bits.append("show if %s ∈ %s" % (cond.get("q", "?"), cond.get("in", cond.get("op", "?"))))
    for cond in dep.get("skip_if", []):
        bits.append("skip if %s ∈ %s" % (cond.get("q", "?"), cond.get("in", cond.get("op", "?"))))
    for t in dep.get("triggers", []):
        bits.append("if “%s” → ask %s" % (t.get("if_response", "?"), t.get("ask", "?")))
    if dep.get("validated_by"):
        bits.append('validated by <b>%s</b>' % _esc(dep["validated_by"]))
    if not bits: return ""
    return '<p class="small muted"><b>Dependencies:</b> %s</p>' % " · ".join(_esc(b) if "<b>" not in b else b for b in bits)

def _reflink(r):
    return '<a class="mono" href="#%s">%s</a>' % (_esc(r), _esc(r)) if r else ""

def build_question_bank():
    qb = _load("question-bank.json")
    m = qb["meta"]; qs = qb["questions"]
    SECTION_ORDER = ["demographics", "medical_history", "lifestyle", "symptoms", "adherence"]
    SECTION_LABELS = {"demographics": "Demographics", "medical_history": "Medical history & conditions",
                      "lifestyle": "Lifestyle & behaviour", "symptoms": "Symptoms & PROs", "adherence": "Adherence"}
    PHASE_LABELS = {"onboarding": "① Onboarding · one-time intake",
                    "progressive": "② Progressive profiling · unlocked by earlier answers",
                    "ongoing": "③ Ongoing tracking"}
    PHASE_RANK = {"onboarding": 0, "progressive": 1, "ongoing": 2}
    by_sec = {}
    for q in qs: by_sec.setdefault(q.get("section", "lifestyle"), []).append(q)
    dom_codes = m["domain_codes"]                      # retained for the per-question domain badge
    n_sec = len([s for s in SECTION_ORDER if by_sec.get(s)])
    total = len(qs)
    h = ['<div class="crumbs"><a href="index.html">Home</a> › Questions &amp; intake › Question bank</div>',
         '<h1>Appendix E — Lifestyle &amp; Condition Question Bank</h1>',
         '<p class="lead">The full engine-ready intake bank: <b>%d questions</b> grouped by <b>%d content sections × 3 phases</b> '
         '(onboarding → progressive → ongoing), covering <b>%d conditions</b>. Each question has a stable reference code <code>%s</code>, a place in '
         'the progressive <b>chain</b> (prerequisites → this → unlocks, with prev/next links), an '
         '<b>applicability vector</b> (sex · age · life-stage · persona) and signed weights onto the 12 '
         '<a class="xref" href="02-pillars-and-marker-catalog.html">pillars</a> and MONIAC '
         '<a class="xref" href="04-moniac-reservoir-dynamics.html">reservoirs</a>. The <span class="flag fl-life">'
         'LIFE</span> stream (<a class="xref" href="18-data-streams-and-experience.html">Doc 18</a>); '
         'validated PROs are in <a class="xref" href="appendix-questions.html">Appendix C</a>.</p>'
         % (m["total_questions"], n_sec, m["total_conditions"], m.get("ref_scheme", "DOM-NNN")), ILLUS]
    h.append('<div class="diagram"><div class="dt">Answer → score → nudge → adherence, and the question chain</div>'
             '<pre class="mermaid">flowchart LR\n'
             '  PRQ["prerequisite Qs"] --> Q["this question (ref)"] --> UNL["unlocked Qs"]\n'
             '  Q --> M["response: direction · magnitude · signed weight"]\n'
             '  M --> P["pillar Δrisk (Doc 03)"]\n  M --> R["reservoir inflow (Doc 04)"]\n'
             '  P --> PS(("PureScore"))\n  R --> PS\n'
             '  PS --> N["top-5 nudges (Doc 07/16)"] --> AD["adherence"] --> R\n'
             '  PS --> PA["perceived vs actual (Appendix F)"]</pre></div>')
    h.append('<div class="callout note"><div class="ct">Reference & chain</div>'
             '<code>ref</code> = 3-letter section code + sequence (e.g. <code>CMB-001</code>); '
             '<code>order</code> = global intake position; <code>prerequisites</code>/<code>unlocks</code> = the '
             'dependency DAG (sex/age/gender gating + show_if + triggers); <code>applicability</code> = '
             '{sex · age · life-stage · personas}. Machine-readable source: '
             '<code>data/question-bank.json</code> (rebuild via <code>data/enrich_bank.py</code>).</div>')
    h.append('<div class="tagrow" style="margin:10px 0 6px">'
             '<span class="chip b-red">↑ risk / adds burden</span>'
             '<span class="chip b-green">↓ risk / builds reserve</span>'
             '<span class="chip b-mut">neutral</span>'
             '<span class="chip b-teal">P1 = ask first</span>'
             '<span class="chip b-acc">♀/♂ = sex-specific</span>'
             '<span class="chip b-mut">⛓ prerequisite chain</span></div>')
    # interactive filter bar — text · pillar · axis · priority · gender · age · persona
    pills = m["pillars"]; axes = m["axes"]; personas = m.get("personas", [])
    opt = lambda v: '<option value="%s">%s</option>' % (_esc(v), _esc(v))
    h.append('<div class="panel" style="position:sticky;top:54px;z-index:5">'
             '<div class="tagrow" style="gap:8px;align-items:center">'
             '<input id="qbq" type="search" placeholder="filter questions…" '
             'style="flex:1;min-width:150px;padding:6px 10px;border-radius:8px;border:1px solid var(--line,#333);'
             'background:transparent;color:inherit">'
             '<select id="qbg"><option value="">any gender</option>'
             '<option value="male">male</option><option value="female">female</option></select>'
             '<select id="qbage"><option value="">any age</option>'
             '<option value="25">&lt;40</option><option value="50">40–59</option><option value="70">60+</option></select>'
             '<select id="qbper"><option value="">any persona</option>%s</select>'
             '<select id="qbp"><option value="">all pillars</option>%s</select>'
             '<select id="qba"><option value="">all axes</option>%s</select>'
             '<select id="qbr"><option value="">all priorities</option>'
             '<option value="1">P1</option><option value="2">P2</option><option value="3">P3</option>'
             '<option value="4">P4</option><option value="5">P5</option></select>'
             '<span id="qbn" class="small muted"></span></div></div>'
             % ("".join(opt(p) for p in personas), "".join(opt(p) for p in pills), "".join(opt(a) for a in axes)))
    # per content-section panels, sub-grouped by intake phase, ordered along the chain
    for sec in SECTION_ORDER:
        items = sorted(by_sec.get(sec, []), key=lambda x: (PHASE_RANK.get(x.get("phase", "ongoing"), 9), x.get("order", 999)))
        if not items: continue
        h.append('<h2 id="sec-%s">%s <span class="small muted mono">· %d questions</span></h2>'
                 % (_esc(sec), _esc(SECTION_LABELS.get(sec, sec)), len(items)))
        cur_ph = None
        for q in items:
            ph = q.get("phase", "ongoing")
            if ph != cur_ph:
                cur_ph = ph
                _pn = sum(1 for x in items if x.get("phase", "ongoing") == ph)
                h.append('<div class="section-h" style="margin:16px 0 6px;font-size:13px">%s <span class="small muted">· %d</span></div>'
                         % (_esc(PHASE_LABELS.get(ph, ph)), _pn))
            qp = sorted({k for r in q.get("responses", []) for k in (r.get("pillars") or {})})
            ax = q.get("dimensions", {}).get("axis_tags", [])
            pa = q.get("perceived_actual", {})
            ap = q.get("applicability", {})
            sex = ap.get("sex", ["all"]); ls = ap.get("life_stage", [])
            per = ap.get("personas", ["all"])
            data = ('data-text="%s" data-pillars="%s" data-axes="%s" data-prio="%s" '
                    'data-sex="%s" data-agemin="%s" data-agemax="%s" data-personas="%s"'
                    % (_esc((q["ref"] + " " + q["text"] + " " + " ".join(q.get("conditions", []))).lower()),
                       _esc(" ".join(qp)), _esc(" ".join(ax)), q.get("priority", ""),
                       _esc(" ".join(sex)), ap.get("age_min", 0), ap.get("age_max", 120),
                       _esc(" ".join(per))))
            h.append('<div class="panel qbq" %s>' % data)
            h.append('<h3 id="%s"><span class="mono" style="color:var(--acc,#6cf)">%s</span> · %s'
                     '<span id="%s"></span></h3>' % (_esc(q["ref"]), _esc(q["ref"]), _esc(q["text"]), _esc(q["id"])))
            # chip row
            chips = ['<span class="chip b-teal">P%s</span>' % q.get("priority", "?"),
                     '<span class="chip b-gold">phase: %s</span>' % _esc(q.get("phase", "ongoing")),
                     '<span class="chip b-mut mono">%s</span>' % _esc(q.get("domain", "")),
                     '<span class="chip b-mut">%s</span>' % _esc(q.get("category", "")),
                     '<span class="chip b-mut">%s</span>' % _esc(q.get("type", "")),
                     '<span class="chip b-mut">%s</span>' % _esc(ap.get("stream", "LIFE")),
                     '<span class="chip b-mut">cadence: %s · %s</span>' % (_esc(q.get("cadence", "")), _esc(q.get("refresh", ""))),
                     '<span class="chip b-mut">%s</span>' % _esc(pa.get("kind", ""))]
            if sex != ["all"]:
                sym = {"female": "♀ female-only", "male": "♂ male-only"}.get(sex[0], " / ".join(sex))
                chips.append('<span class="chip b-acc">%s</span>' % _esc(sym))
            if (ap.get("age_min", 0), ap.get("age_max", 120)) != (0, 120):
                chips.append('<span class="chip b-acc">age %s–%s</span>' % (ap.get("age_min"), ap.get("age_max")))
            chips += ['<span class="chip b-acc">⌖ %s</span>' % _esc(x) for x in ls]
            chips += ['<span class="chip b-acc mono">%s</span>' % _esc(p) for p in qp]
            chips += ['<span class="chip b-mut">#%s</span>' % _esc(a) for a in ax]
            h.append('<div class="tagrow" style="margin-bottom:8px">%s</div>' % "".join(chips))
            # chain line: prev / order / next  + prerequisites / unlocks
            chain = ['<span class="small muted">chain:</span> ']
            chain.append(('← %s' % _reflink(q["prev_ref"])) if q.get("prev_ref") else '<span class="small muted">← start</span>')
            chain.append(' <span class="small muted mono">[#%d/%d]</span> ' % (q.get("order", 0), total))
            chain.append(('%s →' % _reflink(q["next_ref"])) if q.get("next_ref") else '<span class="small muted">end →</span>')
            if q.get("prerequisites"):
                chain.append(' &nbsp;·&nbsp; <span class="small muted">⛓ prerequisites:</span> ' + ", ".join(_reflink(r) for r in q["prerequisites"]))
            if q.get("unlocks"):
                chain.append(' &nbsp;·&nbsp; <span class="small muted">unlocks:</span> ' + ", ".join(_reflink(r) for r in q["unlocks"]))
            h.append('<p class="small">%s</p>' % "".join(chain))
            if q.get("conditions"):
                h.append('<p class="small muted">Informs: %s</p>' % _esc(", ".join(q["conditions"])))
            h.append('<div class="tablewrap"><table><thead><tr><th>Response</th><th>Δrisk</th>'
                     '<th>Mag</th><th>Pillar Δ</th><th>Reservoir Δ</th></tr></thead><tbody>')
            for r in q.get("responses", []):
                h.append('<tr><td class="small"><b>%s</b></td><td>%s</td><td class="small muted">%s</td>'
                         '<td>%s</td><td>%s</td></tr>'
                         % (_esc(r.get("label", "")), _dir_chip(r.get("direction")), _esc(r.get("magnitude", "")),
                            _pdeltas(r.get("pillars")), _rdeltas(r.get("reservoirs"))))
            h.append('</tbody></table></div>')
            h.append(_deps_line(q.get("dependencies")))
            aline = 'applies to — sex: %s · age: %s · life-stage: %s · personas: %s' % (
                _esc(", ".join(sex)),
                _esc("all" if (ap.get("age_min", 0), ap.get("age_max", 120)) == (0, 120) else "%s–%s" % (ap.get("age_min"), ap.get("age_max"))),
                _esc(", ".join(ls) if ls else "any"),
                _esc(", ".join(per)))
            if pa.get("corroborated_by"):
                aline += ' · corroborated by: %s' % _esc(", ".join(pa["corroborated_by"]))
            h.append('<p class="small muted"><b>Applicability:</b> %s</p>' % aline)
            h.append('<p class="small muted" style="opacity:.7">%s · <i>%s</i> · internal id <span class="mono">%s</span></p>'
                     % (_esc(q.get("source", "")), _esc(q.get("flag", "")), _esc(q["id"])))
            h.append('</div>')
    # filter script (page-local; independent of the topbar search in wiki.js)
    h.append('<script>(function(){'
             'var ids=["qbq","qbg","qbage","qbper","qbp","qba","qbr"].map(function(i){return document.getElementById(i);});'
             'var s=ids[0],g=ids[1],ag=ids[2],pe=ids[3],p=ids[4],a=ids[5],r=ids[6],n=document.getElementById("qbn");'
             'var cards=[].slice.call(document.querySelectorAll(".panel.qbq"));'
             'function has(d,v){return (" "+d+" ").indexOf(" "+v+" ")>-1;}'
             'function f(){var t=(s.value||"").toLowerCase(),gv=g.value,av=ag.value,pev=pe.value,pv=p.value,xv=a.value,rv=r.value,c=0;'
             'cards.forEach(function(el){var d=el.dataset;var ok=(!t||d.text.indexOf(t)>-1)'
             '&&(!gv||has(d.sex,"all")||has(d.sex,gv))'
             '&&(!av||(+d.agemin<=+av&&+d.agemax>=+av))'
             '&&(!pev||has(d.personas,"all")||has(d.personas,pev))'
             '&&(!pv||has(d.pillars,pv))&&(!xv||has(d.axes,xv))&&(!rv||d.prio===rv);'
             'el.style.display=ok?"":"none";if(ok)c++;});'
             'n.textContent=c+" / "+cards.length+" shown";}'
             'ids.forEach(function(e){e.addEventListener("input",f);e.addEventListener("change",f);});f();})();</script>')
    return "Appendix E · Question Bank", "".join(h)

# =================================================================== QUESTIONS HUB
_Q_SEC = ["demographics", "medical_history", "lifestyle", "symptoms", "adherence"]
_Q_SECL = {"demographics": "Demographics", "medical_history": "Medical history & conditions",
           "lifestyle": "Lifestyle & behaviour", "symptoms": "Symptoms & PROs", "adherence": "Adherence"}
_Q_PH = ["onboarding", "progressive", "ongoing"]
_Q_PHL = {"onboarding": "Onboarding", "progressive": "Progressive", "ongoing": "Ongoing"}

def build_questions_hub():
    qs = _load("question-bank.json")["questions"]
    adh = _load("adherence.json")["items"]
    cnt = {s: {p: 0 for p in _Q_PH} for s in _Q_SEC}
    for q in qs:
        s = q.get("section", "lifestyle"); p = q.get("phase", "ongoing")
        if s in cnt: cnt[s][p] += 1
    for a in adh:  # adherence check-ins fire on cadence → ongoing (the no-EHR screener is onboarding)
        cnt["adherence"]["ongoing"] += 1
    tot = len(qs) + len(adh)
    h = ['<div class="crumbs"><a href="index.html">Home</a> › Questions &amp; intake › Overview</div>',
         '<h1>Questions &amp; Intake — overview</h1>',
         '<p class="lead">Every question PureScore can ask, unified into <b>5 content sections × 3 phases</b> and '
         'gated so a patient is <b>only ever asked what is relevant</b>. The raw items live in the bank '
         '(<a class="xref" href="appendix-question-bank.html">Appendix E</a>, %d) and the adherence check-ins '
         '(<a class="xref" href="appendix-adherence.html">Appendix H</a>, %d); validated PROs in '
         '<a class="xref" href="appendix-questions.html">Appendix C</a>. The <a class="xref" href="eligibility-gating.html">'
         'Eligibility &amp; gating</a> page shows the rules that connect onboarding answers to which questions unlock.</p>'
         % (len(qs), len(adh)), ILLUS]
    h.append('<div class="diagram"><div class="dt">Content section × intake phase</div>'
             '<pre class="mermaid">flowchart LR\n'
             '  ON["① Onboarding\\none-time intake"] --> PR["② Progressive\\nunlocked by answers"] --> OG["③ Ongoing\\ntracking + adherence"]\n'
             '  ON -.gates.-> PR</pre></div>')
    # matrix
    h.append('<div class="tablewrap"><table><thead><tr><th>Content section</th>'
             + "".join('<th>%s</th>' % _Q_PHL[p] for p in _Q_PH) + '<th>Total</th></tr></thead><tbody>')
    for s in _Q_SEC:
        row = cnt[s]; rt = sum(row.values())
        h.append('<tr><td><b>%s</b></td>%s<td class="mono"><b>%d</b></td></tr>'
                 % (_Q_SECL[s], "".join('<td class="mono">%d</td>' % row[p] for p in _Q_PH), rt))
    coltot = {p: sum(cnt[s][p] for s in _Q_SEC) for p in _Q_PH}
    h.append('<tr><td class="muted">Total</td>%s<td class="mono"><b>%d</b></td></tr>'
             % ("".join('<td class="mono muted">%d</td>' % coltot[p] for p in _Q_PH), tot))
    h.append('</tbody></table></div>')
    # entry points
    h.append('<div class="section-h">The question library</div><div class="grid c3">')
    for href, t, dsc in [
        ("appendix-question-bank.html", "Question bank (Appendix E)", "The %d-item engine-ready bank, grouped by section × phase" % len(qs)),
        ("appendix-questions.html", "Validated PROs (Appendix C)", "PHQ-9, GAD-7, AUDIT-C, ISI, PSS-4… full instruments"),
        ("appendix-adherence.html", "Adherence check-ins (Appendix H)", "%d EHR-triggered micro check-ins (Patient360)" % len(adh)),
        ("appendix-lifestyles.html", "Lifestyles & axes (Appendix F)", "Axes, archetypes, perceived-vs-actual"),
        ("appendix-persona-matrix.html", "Persona matrix (Appendix I)", "Signals → persona posterior"),
        ("appendix-goals.html", "Goals (Appendix J)", "Goal catalogue keyed by applicability"),
        ("eligibility-gating.html", "Eligibility & gating", "Rules that decide who is asked each question"),
    ]:
        h.append('<a class="card" href="%s"><h3>%s</h3><p>%s</p></a>' % (href, _esc(t), _esc(dsc)))
    h.append('</div>')
    h.append('<div class="callout note"><div class="ct">Connected to onboarding</div>'
             'Onboarding (phase ①) captures identity, baseline medical history and the screeners (sex, BMI, smoking, '
             'alcohol). Those answers <b>gate</b> phases ② and ③ — see <a class="xref" href="eligibility-gating.html">Eligibility '
             '&amp; gating</a> and the data-streams experience (<a class="xref" href="18-data-streams-and-experience.html">Doc 18</a>).</div>')
    return "Questions & intake · overview", "".join(h)

# =================================================================== ELIGIBILITY & GATING
def build_eligibility():
    qs = _load("question-bank.json")["questions"]
    byid = {q["id"]: q for q in qs}
    def ap(q): return q.get("applicability", {}) or {}
    sexr = [q for q in qs if ap(q).get("sex") not in (None, ["all"], [])]
    lsr = [q for q in qs if ap(q).get("life_stage")]
    ager = [q for q in qs if (ap(q).get("age_min", 0), ap(q).get("age_max", 120)) != (0, 120)]
    showif = [q for q in qs if (q.get("dependencies", {}) or {}).get("show_if")]
    prereq = [q for q in qs if q.get("prerequisites")]
    h = ['<div class="crumbs"><a href="index.html">Home</a> › Questions &amp; intake › Eligibility &amp; gating</div>',
         '<h1>Eligibility &amp; Gating Rules <span class="small muted">· don\'t ask irrelevant questions</span></h1>',
         '<p class="lead">A patient is only ever asked what is <b>valid and relevant</b> for them. Onboarding answers '
         '(sex, BMI, smoking, alcohol, conditions) gate the rest: a male is never asked menopause questions; smoking '
         'follow-ups appear only if smoking was reported. Rules are real data on each question '
         '(<code>applicability</code> · <code>dependencies.show_if</code> · <code>prerequisites</code> · <code>unlocks</code>).</p>', ILLUS]
    h.append('<div class="diagram"><div class="dt">Onboarding answers gate the question set</div>'
             '<pre class="mermaid">flowchart TD\n'
             '  SEX["Q_CORE_SEX"] -->|male| HF["hide female-only Qs:\\nmenopause · PCOS · pregnancy"]\n'
             '  SEX -->|female| SF["unlock female reproductive Qs"]\n'
             '  BMI["Q_CORE_BMI"] -->|overweight / obese| WA["ask waist + metabolic follow-ups"]\n'
             '  SMK["Q_CORE_SMOKING"] -->|current / former| SQ["ask smoking-detail Qs"]\n'
             '  SMK -->|never| SK["skip smoking follow-ups"]\n'
             '  ALC["Q_CORE_ALCOHOL"] -->|drinks| AQ["ask alcohol-pattern Qs"]\n'
             '  classDef on fill:#0c1726,stroke:#2b5a86,color:#cfe3ff;\n'
             '  class SEX,BMI,SMK,ALC on</pre></div>')
    h.append('<div class="tagrow" style="margin:6px 0 12px">'
             '<span class="chip b-acc">%d sex-gated</span><span class="chip b-acc">%d life-stage-gated</span>'
             '<span class="chip b-acc">%d age-gated</span><span class="chip b-teal">%d show-if</span>'
             '<span class="chip b-mut">%d with prerequisites</span></div>'
             % (len(sexr), len(lsr), len(ager), len(showif), len(prereq)))

    def _tbl(title, anchor, rows, head):
        h.append('<h2 id="%s">%s <span class="small muted">· %d</span></h2>' % (anchor, title, len(rows)))
        h.append('<div class="tablewrap"><table><thead><tr>%s</tr></thead><tbody>'
                 % "".join("<th>%s</th>" % c for c in head))
        h.extend(rows)
        h.append('</tbody></table></div>')

    sym = {"female": "♀ female-only", "male": "♂ male-only"}
    rows = ['<tr><td class="mono small">%s</td><td><span class="chip b-acc">%s</span></td><td class="small">%s</td></tr>'
            % (_esc(q["ref"]), _esc(sym.get((ap(q)["sex"] or ["?"])[0], " / ".join(ap(q)["sex"]))), _esc(q["text"][:80]))
            for q in sexr]
    _tbl("Sex-restricted questions", "sex", rows, ["Ref", "Valid for", "Question"])

    rows = ['<tr><td class="mono small">%s</td><td>%s</td><td class="small">%s</td></tr>'
            % (_esc(q["ref"]), "".join('<span class="chip b-acc">⌖ %s</span>' % _esc(x) for x in ap(q)["life_stage"]), _esc(q["text"][:80]))
            for q in lsr]
    _tbl("Life-stage-gated questions", "lifestage", rows, ["Ref", "Life-stage", "Question"])

    rows = ['<tr><td class="mono small">%s</td><td class="small">%s</td><td class="small">%s</td></tr>'
            % (_esc(q["ref"]), _esc("%s–%s" % (ap(q).get("age_min", 0), ap(q).get("age_max", 120))), _esc(q["text"][:80]))
            for q in ager]
    _tbl("Age-restricted questions", "age", rows, ["Ref", "Age range", "Question"])

    rows = []
    for q in showif:
        for cond in q["dependencies"]["show_if"]:
            gate = byid.get(cond.get("q"))
            gref = gate["ref"] if gate else cond.get("q", "?")
            rows.append('<tr><td class="mono small">%s</td><td class="small">shown only if <b>%s</b> ∈ %s</td><td class="small">%s</td></tr>'
                        % (_esc(q["ref"]), _esc(gref), _esc(", ".join(cond.get("in", []))), _esc(q["text"][:70])))
    _tbl("Conditional (show-if) questions", "showif", rows, ["Ref", "Shown when", "Question"])

    h.append('<div class="callout spec"><div class="ct">How rules connect to onboarding</div>'
             'The gate questions above are all <b>phase-① onboarding</b> items. Their answers set the patient\'s '
             'applicability vector, which the engine evaluates before serving any later question — so phases ② and ③ '
             'only ever surface valid, relevant items. New gates are added by editing <code>data/question-bank.json</code> '
             '(re-run <code>migrate_questions_sections.py</code> to gap-fill systematically).</div>')
    return "Eligibility & gating", "".join(h)

# =================================================================== PURESCORE CALCULATION — DATA FLOW
def build_purescore_dataflow():
    L0 = """flowchart LR
  PT["Patient / consumer"] -->|survey · self-report| SYS
  WE["Wearables (D22 tiered)"] -->|streams| SYS
  LB["Labs (venous · DTC)"] -->|biomarkers| SYS
  EHR["Patient360 (EHR/EMR)"] -->|conditions · meds| SYS
  SYS(["PureScore engine"]) --> SCORE["PureScore 0–100 + bands"]
  SYS --> COMP["Companion vector (Doc 12)"]
  SYS --> NUD["Top-5 nudges (Doc 07)"]
  SYS --> ESC["Crisis escalation (Doc 11)"]
  CLIN["Clinician"] -->|review · sign-off| SYS
  classDef ext fill:#0c1726,stroke:#2b5a86,color:#cfe3ff;
  class PT,WE,LB,EHR,CLIN ext"""
    L1 = """flowchart TB
  IN["1 · Ingest + confidence"] --> MK["2 · Marker risk r_i"]
  MK --> PL["3 · Pillar risk R_k"]
  PL --> SC["4 · PureScore + critical cascade"]
  SC --> OUT["5 · Outputs (score · bands · companion)"]
  REFD[("Reference distributions — NHANES · guidelines")] -.-> MK
  BASE[("Personal baseline — empirical Bayes")] -.-> MK
  RES[("MONIAC reservoirs B_j — Doc 04")] -.-> PL
  WTS[("Pillar weights W_k · δ — admin")] -.-> PL
  WTS -.-> SC
  CRIT[("Critical-marker set — Doc 02")] -.-> SC
  classDef store fill:#10151e,stroke:#8f9bff,color:#cdd6ff;
  class REFD,BASE,RES,WTS,CRIT store"""
    L2 = """flowchart TB
  A["measurement x_i"] --> G1{"confidence ≥ floor?"}
  G1 -->|missing / stale| FB["impute cohort median — low conf → low coverage"]
  G1 -->|ok| G2{"wearable tier?"}
  G2 -->|inferential| INFO["informational only — cannot set a band"]
  G2 -->|clinical / consumer / lab| CL["clinical band risk r_clin (green / yellow / red bands)"]
  FB --> CL
  CL --> MX["r = max(r_clin, 0.6·r_cohort) — cohort can only RAISE"]
  MX --> G3{"critical marker?"}
  G3 -->|no| PERS["+ personalization, then band_clamp (can't relax red / flip band)"]
  G3 -->|yes| RI["r (personalization disabled)"]
  PERS --> RI
  RI --> AGG["pillar R_k = confidence-weighted δ-power-mean"]
  AGG --> RESV["R_k = clamp01(R_k + ρ·reservoir load) — Doc 04"]
  RESV --> G4{"critical-marker red in pillar?"}
  G4 -->|yes| FLOOR["status CRITICAL · R_k ← max(R_k, 0.60) — never averaged away"]
  G4 -->|no| STAT["status from R_k bands"]
  FLOOR --> G5{"acute-danger red?"}
  G5 -->|yes| ESCAL["crisis / clinician escalation — Doc 11"]
  G5 -->|no| TOT["R_total = δ-power-mean over W_k pillars"]
  STAT --> TOT
  ESCAL --> TOT
  TOT --> G6{"acute event active?"}
  G6 -->|yes| ACUTE["apply acute weights m_k_acute · override→revert — Doc 06"]
  G6 -->|no| FINAL["PureScore = 100·(1 − R_total)"]
  ACUTE --> FINAL
  FINAL --> G7{"pillar green only via imputed medians?"}
  G7 -->|yes| LCG["label 'low-coverage green' — suppress over-confident green"]
  G7 -->|no| DONE["PureScore + bands + companion vector (Doc 12)"]
  LCG --> DONE
  classDef gate fill:#1d1a0c,stroke:#edb14a,color:#ffe6b0;
  classDef danger fill:#1a0f15,stroke:#f0606e,color:#ffd0d6;
  class G1,G2,G3,G4,G5,G6,G7 gate
  class ESCAL,FLOOR danger"""
    h = ['<div class="crumbs"><a href="index.html">Home</a> › Diagrams &amp; system maps › PureScore calculation — data flow</div>',
         '<h1>PureScore Calculation — Data Flow Diagram <span class="small muted">· all decision gates &amp; rules</span></h1>',
         '<p class="lead">The end-to-end data flow of a PureScore computation — ingestion → marker risk → pillar risk → '
         'score → outputs — with <b>every decision gate and rule</b> made explicit. Leveled: <b>L0</b> context, '
         '<b>L1</b> pipeline + data stores, <b>L2</b> the full gate flow; the table below enumerates each rule with its '
         'source §. Derived from <a class="xref" href="01-data-model-and-reference-ranges.html">Doc 01</a>, '
         '<a class="xref" href="02-pillars-and-marker-catalog.html">02</a>, '
         '<a class="xref" href="03-scoring-formula.html">03</a>, '
         '<a class="xref" href="04-moniac-reservoir-dynamics.html">04</a>, '
         '<a class="xref" href="06-acute-events-and-life-stage-plans.html">06</a>, '
         '<a class="xref" href="11-safety-governance-and-regulatory.html">11</a>, '
         '<a class="xref" href="12-critical-review-and-purescore-2.0.html">12</a>.</p>', ILLUS]
    h.append('<div class="callout spec"><div class="ct">Reading the levels</div>'
             'Rounded boxes are <b>processes</b>, cylinders are <b>data stores</b>, diamonds are <b>decision gates</b>. '
             '<span class="b-yellow">Amber</span> = a gate; <span class="b-red">red</span> = a safety floor / escalation that '
             'can never be averaged or relaxed away. Every gate is numbered in the rules table.</div>')
    h.append('<h2 id="l0">L0 · Context</h2>')
    h.append('<div class="diagram"><div class="dt">External entities → engine → outputs</div><pre class="mermaid">%s</pre></div>' % L0)
    h.append('<h2 id="l1">L1 · Pipeline &amp; data stores</h2>')
    h.append('<div class="diagram"><div class="dt">Five stages, with the stores each reads</div><pre class="mermaid">%s</pre></div>' % L1)
    h.append('<h2 id="l2">L2 · Full decision-gate flow</h2>')
    h.append('<div class="diagram"><div class="dt">One marker → its pillar → the score, every gate explicit</div><pre class="mermaid">%s</pre></div>' % L2)

    h.append('<h2 id="rules">Decision gates &amp; rules <span class="small muted">· exhaustive</span></h2>')
    h.append('<div class="tablewrap"><table><thead><tr><th>#</th><th>Stage</th><th>Gate / rule</th>'
             '<th>Condition → action</th><th>Source</th></tr></thead><tbody>')
    rules = [
      ("G0", "Upstream", "Eligibility gating", "only valid/relevant questions captured (sex/age/show_if) before any input reaches scoring", "Eligibility &amp; gating"),
      ("G1", "Ingest", "Confidence floor", "confidence = q_source · recency-decay; if &lt; per-marker floor → mark stale, blend literature fallback at low confidence", "Doc 01 §2"),
      ("G2", "Ingest", "Missing data", "no usable measurement → impute cohort median, source=literature_fallback, reduce pillar coverage", "Doc 01 §4.3"),
      ("G3", "Ingest", "Wearable trust tier (D22)", "inferential metric → informational only (cannot set a band); clinical-grade may drive a band with confirmation", "Doc 18 §2"),
      ("G4", "Marker", "Continuous band", "risk read off a continuous curve; green &lt;0.15 / yellow &lt;0.5 / red ≥0.5 (labels, not cliffs)", "Doc 03 §1"),
      ("G5", "Marker", "Cohort max rule", "r_i = max(r_i^clin, φ·r_i^cohort), φ=0.6 — cohort can only RAISE concern, never dilute the clinical anchor", "Doc 03 §2 · Doc 01 §4.4"),
      ("G6", "Marker", "Personalization clamp", "r_i = band_clamp(… + r_i^pers); can't relax a red, clear a critical, or by itself flip a band", "Doc 03 §2b"),
      ("G7", "Marker", "Critical exclusion", "personalization disabled for critical markers (𝟙[non-critical])", "Doc 03 §2b"),
      ("G8", "Pillar", "Confidence weighting", "each marker's weight is scaled by its confidence — low-confidence inputs contribute less", "Doc 03 §3"),
      ("G9", "Pillar", "δ-power-mean (within)", "R_k^mark aggregates marker risks via a δ-power-mean with within-pillar weights w_i", "Doc 03 §5.2"),
      ("G10", "Pillar", "Reservoir add", "R_k = clamp₀₁(R_k^mark + ρ_k·B̃_k); MONIAC stocks, leakage λ, cross-pillar interference κ", "Doc 03 §3 · Doc 04"),
      ("G11", "Pillar", "Critical-marker override", "∃ critical-marker red → status critical; R_k ← max(R_k, 0.60) hard floor", "Doc 03 §4/§6 · Doc 02"),
      ("G12", "Score", "δ-power-mean (across)", "R_total = δ-power-mean over pillars with weights W_k", "Doc 03 §5.2"),
      ("G13", "Score", "Critical cascade", "a single critical marker floors its pillar and is never averaged away by the formula", "Doc 03 §6 · Doc 02"),
      ("G14", "Safety", "Acute-danger escalation", "acute-danger red → immediate clinician / crisis pathway (independent of the scalar score)", "Doc 11 · Doc 03 §6"),
      ("G15", "Modifier", "Acute-event override", "acute event active → apply acute weights m_k^acute, override then revert on resolution", "Doc 06"),
      ("G16", "Modifier", "Sex / life-stage model", "select hormone-/cycle-/pregnancy-/menopause-aware ranges before banding", "Doc 05 · Doc 06"),
      ("G17", "Output", "Coverage suppression", "pillar 'green' only via imputed medians → label low-coverage green, never a clean bill", "Doc 01 §4.3 · Doc 03"),
      ("G18", "Output", "Companion vector", "emit Confidence / Trajectory / early-warning alongside the scalar score", "Doc 12"),
    ]
    for gid, stage, rule, cond, src in rules:
        h.append('<tr><td class="mono small">%s</td><td class="small">%s</td><td class="small"><b>%s</b></td>'
                 '<td class="small">%s</td><td class="small muted">%s</td></tr>' % (gid, _esc(stage), _esc(rule), cond, src))
    h.append('</tbody></table></div>')

    # ================= DEEPENINGS =================
    # A · MONIAC reservoir internals (gate G10)
    RESV = """flowchart LR
  MKB["adverse markers + behaviours"] --> INF["inflow_j = κ_in·σ(Σ a·excess + Σ b·behaviour)"]
  INF --> TANK[("reservoir stock B_j")]
  TANK -->|"heal: − λ_j·(B_j − B_j*)"| TANK
  OTHER[("other reservoirs B_l")] -->|"+ κ_jl·sat(B_l)"| TANK
  TANK -->|"+ κ_lj·sat(B_j)"| OTHER
  TANK --> NORM["B̃_k normalized load"]
  NORM -->|"ρ_k coupling"| RK["R_k = clamp01(R_k_mark + ρ_k·B̃_k)"]
  classDef store fill:#10151e,stroke:#8f9bff,color:#cdd6ff;
  class TANK,OTHER store"""
    h.append('<h2 id="reservoir">Deep-dive · MONIAC reservoir internals <span class="small muted">· gate G10</span></h2>')
    h.append('<p class="small">Zoom into the reservoir term of pillar risk (<a class="xref" href="04-moniac-reservoir-dynamics.html">Doc 04</a>): each latent stock <code>B_j</code> integrates exposure, heals toward a set-point, and couples to other tanks before contributing <code>B̃_k</code> to its pillar.</p>')
    h.append('<div class="diagram"><div class="dt">Stock-and-flow for one reservoir B_j</div><pre class="mermaid">%s</pre></div>' % RESV)
    h.append('<pre class="code"><code>B_j(t+Δ) = B_j(t) + Δ · [ inflow_j(markers, behaviours)       # source (logistic σ)\n                         − λ_j · (B_j − B_j*)                # leakage toward set-point\n                         + Σ κ_jl · sat(B_l) ]               # cross-tank interference\n           clamped to  B_j ∈ [0, B_j^max]</code></pre>')
    h.append('<div class="tablewrap"><table><thead><tr><th>Coupling</th><th>Sign</th><th>Meaning</th></tr></thead><tbody>'
             '<tr><td class="mono small">κ(INFL←SLD)</td><td>+</td><td class="small">chronic sleep debt raises inflammatory load</td></tr>'
             '<tr><td class="mono small">κ(GLY←SLD)</td><td>+</td><td class="small">sleep debt worsens glycemic control</td></tr>'
             '<tr><td class="mono small">κ(GLY←CRF)</td><td>−</td><td class="small">cardiorespiratory reserve lowers glycemic burden (asset protects)</td></tr>'
             '<tr><td class="mono small">κ(ATH←GLY), κ(ATH←INFL)</td><td>+</td><td class="small">glycemic + inflammatory load accelerate atherogenesis</td></tr>'
             '<tr><td class="mono small">κ(ALLO↔SLD)</td><td>±</td><td class="small">stress and sleep debt are bidirectional</td></tr>'
             '<tr><td class="mono small">λ_j</td><td>—</td><td class="small">large = fast heal (sleep debt) · tiny = near-permanent (atherogenic, CAC)</td></tr>'
             '</tbody></table></div>')

    # B · Worked numeric example
    h.append('<h2 id="worked">Deep-dive · Worked numeric example <span class="small muted">· illustrative</span></h2>')
    h.append('<div class="callout note"><div class="ct">Illustrative only</div>A 52-year-old male, Cardiovascular pillar. r-values are hand-chosen to show the gates firing, not calibrated outputs.</div>')
    h.append('<div class="tablewrap"><table><thead><tr><th>#</th><th>Input</th><th>Gate</th><th>Result</th></tr></thead><tbody>')
    wrows = [
      ("1", "ApoB 124 mg/dL (≥100)", "G4 band", "red → r_clin ≈ 0.62"),
      ("2", "ApoB cohort ≈ p70", "G5 cohort max", "max(0.62, 0.6·0.50) = 0.62"),
      ("3", "ApoB is a critical marker", "G7 critical exclusion", "personalization disabled → r = 0.62"),
      ("4", "Systolic BP 134 (120–139)", "G4 band", "yellow → r ≈ 0.38 (critical marker, not red)"),
      ("5", "LDL 150 · HDL 42 · RHR 72 · HRV p30 · CAC 40", "G4 band", "yellow → r ≈ 0.30–0.42 each"),
      ("6", "9 CV markers · weights w_i", "G8 + G9 conf-weighted δ-power-mean", "R_CV_mark ≈ 0.48"),
      ("7", "atherogenic reservoir elevated (B̃ ≈ 0.60, ρ ≈ 0.15)", "G10 reservoir add", "R_CV = clamp01(0.48 + 0.09) = 0.57"),
      ("8", "ApoB red AND critical", "G11 critical override", "status CRITICAL · R_CV ← max(0.57, 0.60) = 0.60"),
      ("9", "pillars aggregated (W_k)", "G12 cross-pillar δ-power-mean", "R_total ≈ 0.34 (CV floored at 0.60 not averaged away — G13)"),
      ("10", "SBP not ≥180; no acute-danger red", "G14 escalation", "no crisis pathway, but CV flagged critical → priority action"),
      ("11", "final", "—", "<b>PureScore = 100·(1 − 0.34) = 66</b>"),
      ("12", "companion", "G18", "Confidence high · Trajectory ↑ if ApoB improving"),
    ]
    for n, inp, gate, res in wrows:
        h.append('<tr><td class="mono small">%s</td><td class="small">%s</td><td class="small">%s</td><td class="small">%s</td></tr>'
                 % (n, _esc(inp), _esc(gate), res))
    h.append('</tbody></table></div>')

    # C · Per-stage L2 sub-diagrams
    ING = """flowchart LR
  S["source: lab / wearable / survey"] --> Q["q_source lookup"]
  Q --> D["× recency decay exp(−Δt/τ)"]
  D --> C{"confidence ≥ floor?"}
  C -->|no / missing| F["stale → blend literature fallback (↓coverage)"]
  C -->|yes| T{"wearable tier?"}
  T -->|inferential| I["informational only"]
  T -->|clinical / consumer / lab| OK["usable x_i + confidence"]
  F --> OK"""
    MRK = """flowchart LR
  X["x_i + confidence"] --> CB["r_clin off band curve"]
  X --> CO["r_cohort = F_ic percentile"]
  CB --> MX["r = max(r_clin, 0.6·r_cohort)"]
  CO --> MX
  MX --> K{"critical marker?"}
  K -->|no| P["+ r_pers, band_clamp"]
  K -->|yes| R["r_i"]
  P --> R"""
    PIL = """flowchart LR
  RS["marker risks r_i"] --> W["× confidence weights w_i"]
  W --> PM["δ-power-mean → R_k_mark"]
  PM --> RZ["+ ρ_k·B̃_k → clamp01"]
  RZ --> OV{"critical-marker red?"}
  OV -->|yes| FL["status critical · max(R_k, 0.60)"]
  OV -->|no| ST["status from bands"]"""
    SCO = """flowchart LR
  RK["pillar risks R_k"] --> PM2["δ-power-mean over W_k → R_total"]
  PM2 --> AC{"acute event?"}
  AC -->|yes| AW["apply acute weights (Doc 06)"]
  AC -->|no| PS["PureScore = 100·(1 − R_total)"]
  AW --> PS
  PS --> CG{"low coverage?"}
  CG -->|yes| LC["low-coverage green label"]
  CG -->|no| OUT["score + bands + companion"]
  LC --> OUT"""
    h.append('<h2 id="stages">Deep-dive · Per-stage sub-flows</h2>')
    for title, dia in [("Stage 1 · Ingest + confidence", ING), ("Stage 2 · Marker risk", MRK),
                       ("Stage 3 · Pillar risk", PIL), ("Stage 4–5 · Score + output", SCO)]:
        h.append('<div class="diagram"><div class="dt">%s</div><pre class="mermaid">%s</pre></div>' % (_esc(title), dia))

    # D · Representative per-pillar DFD (Cardiovascular)
    CVP = """flowchart TB
  SBP["Systolic BP · w.15 · CRITICAL"] --> AGG
  DBP["Diastolic BP · w.10"] --> AGG
  APOB["ApoB · w.18 · CRITICAL"] --> AGG
  LDL["LDL-C · w.12 · CRITICAL"] --> AGG
  HDL["HDL-C · w.06"] --> AGG
  LPA["Lp(a) · w.10"] --> AGG
  RHR["Resting HR · w.07"] --> AGG
  HRV["HRV · w.07"] --> AGG
  CAC["CAC · w.15 · CRITICAL"] --> AGG
  AGG["confidence-weighted δ-power-mean → R_CV_mark"] --> RES["+ reservoirs: atherogenic · vascular/BP · cardiorespiratory"]
  RES --> OV{"any CRITICAL marker red?"}
  OV -->|yes| CR["status CRITICAL · R_CV ← max(R_CV, 0.60)"]
  OV -->|no| ST["status from R_CV bands"]
  classDef crit fill:#1a0f15,stroke:#f0606e,color:#ffd0d6;
  class SBP,APOB,LDL,CAC crit"""
    h.append('<h2 id="cv-pillar">Deep-dive · Per-pillar template (Cardiovascular)</h2>')
    h.append('<p class="small">The marker→pillar pattern for one pillar (<a class="xref" href="02-pillars-and-marker-catalog.html">Doc 02 · CV</a>); the other 11 pillars follow the same shape with their own markers, weights and critical set.</p>')
    h.append('<div class="diagram"><div class="dt">Cardiovascular: 9 markers → pillar risk → critical cascade</div><pre class="mermaid">%s</pre></div>' % CVP)

    h.append('<div class="callout note"><div class="ct">Keep this in sync</div>'
             'This diagram is <b>derived</b> from the scoring docs. Per the wiki guide (<code>CLAUDE.md</code> · diagram '
             'dependency map), any change to Docs 01–06, 11, 12 or the admin weights <b>must</b> trigger a review of this '
             'page and the other system diagrams, with the impact noted.</div>')
    return "PureScore calculation — data flow", "".join(h)

# =================================================================== PURESCORE UBER MAP (interactive)
def build_purescore_uber():
    body = r"""<div class="crumbs"><a href="index.html">Home</a> &rsaquo; Diagrams &amp; system maps &rsaquo; PureScore uber-map</div>
<h1>PureScore Uber-Map <span class="small muted">&middot; interactive &middot; 12 pillars &middot; 15 reservoirs</span></h1>
<p class="lead">The entire PureScore lifecycle on one pan/zoom canvas, with the full <b>12-pillar &times; 15-reservoir</b> calculation.
Pick a sample profile and press <b>Play</b> to watch values flow box-by-box. <b>Click</b> a box for its formula, conditions and live value;
<b>double-click</b> to zoom to it. Illustrative numbers (README &sect;5.6).</p>

<div class="um-tools" id="umTools">
  <select id="umProfile" class="um-sel"></select>
  <button class="um-btn" id="umPlay">&#9654; Play</button>
  <button class="um-btn" id="umStep">Step &raquo;</button>
  <button class="um-btn" id="umReset">Reset</button>
  <label class="um-lab">speed <input type="range" id="umSpeed" min="120" max="900" value="380" step="60"></label>
  <span class="um-grow"></span>
  <label class="um-lab"><input type="checkbox" id="umPath" checked> highlight path</label>
  <label class="um-lab"><input type="checkbox" id="umGates"> firing gates only</label>
  <button class="um-btn" id="umFit">Fit</button>
  <button class="um-btn" id="umFs">&#10530; Fullscreen</button>
  <span id="umScoreBadge" class="um-score">&mdash;</span>
</div>
<div class="um-areas" id="umAreas"></div>
<div class="um-wrap" id="umWrap">
  <svg id="umSvg" width="100%" height="100%"></svg>
  <div class="um-hint" id="umHint">drag = pan &middot; scroll = zoom &middot; click = detail &middot; dbl-click = zoom to box</div>
</div>
<div class="um-modal" id="umModal"><div class="um-card">
  <button class="um-x" id="umX">&times;</button>
  <div id="umModalBody"></div>
</div></div>

<style>
.um-tools{display:flex;gap:8px;align-items:center;flex-wrap:wrap;margin:10px 0}
.um-sel,.um-btn{font:inherit;font-size:13px;padding:6px 10px;border-radius:8px;border:1px solid var(--line,#2a3340);background:var(--bg2,#0c1320);color:inherit;cursor:pointer}
.um-btn:hover{background:var(--line,#1a2230)}
.um-lab{font-size:12px;color:var(--dim,#9bb0c5);display:flex;align-items:center;gap:5px}
.um-grow{flex:1}
.um-score{font-weight:800;font-size:18px;padding:4px 12px;border-radius:9px;background:#0c1726;border:1px solid #2b5a86;min-width:46px;text-align:center}
.um-areas{display:flex;gap:6px;flex-wrap:wrap;margin-bottom:8px}
.um-areas span{font-size:11px;padding:3px 8px;border-radius:6px;border:1px solid;cursor:pointer;user-select:none}
.um-areas span.off{opacity:.32}
.um-wrap{position:relative;width:100%;height:600px;border:1px solid var(--line,#222c3a);border-radius:14px;overflow:hidden;background:radial-gradient(1200px 700px at 30% 0,#0e1726,#080d16)}
.um-wrap.fs{position:fixed;inset:0;height:100vh;width:100vw;z-index:9999;border-radius:0}
.um-hint{position:absolute;left:12px;bottom:10px;font-size:11px;color:#6b7d92;pointer-events:none}
#umSvg{display:block;cursor:grab;touch-action:none}
#umSvg.drag{cursor:grabbing}
.um-node{cursor:pointer}
.um-node rect{transition:opacity .2s}
.um-node text{font:600 11.5px Inter,system-ui,sans-serif;fill:#e8eef6;pointer-events:none}
.um-node .um-val{font-weight:800;font-size:11px;fill:#fff}
.um-node.on rect{stroke-width:3}
.um-node.dim{opacity:.16}
.um-edge{fill:none;stroke:#36424f;stroke-width:1.3}
.um-edge.on{stroke:#2ee6c9;stroke-width:2.4}
.um-edge.loop{stroke-dasharray:5 4}
.um-areabg{opacity:.5}
.um-arealabel{font:700 11px Inter,sans-serif;letter-spacing:.04em;text-transform:uppercase}
.um-modal{display:none;position:fixed;inset:0;background:rgba(4,8,14,.6);z-index:10000;align-items:center;justify-content:center}
.um-modal.show{display:flex}
.um-card{background:#0d1422;border:1px solid #2b5a86;border-radius:14px;max-width:560px;width:90%;max-height:80vh;overflow:auto;padding:18px 20px;position:relative}
.um-x{position:absolute;right:12px;top:10px;background:none;border:none;color:#9bb0c5;font-size:22px;cursor:pointer}
.um-card h3{margin:0 6px 0 0;display:inline}
.um-card .um-tag{font-size:11px;padding:2px 8px;border-radius:6px;border:1px solid}
.um-card pre{background:#080d18;border:1px solid #1c2636;border-radius:8px;padding:10px 12px;font-size:12.5px;overflow:auto;color:#bcd2f0;white-space:pre-wrap}
.um-card ul{margin:8px 0 0;padding-left:18px;font-size:13px}
.um-card .um-live{margin-top:12px;padding:10px 12px;border-radius:8px;background:#0c1726;border:1px solid #2b5a86;font-size:13px}
</style>

<script>
(function(){
var AR=[
 {k:'onboard',l:'Onboarding',c:'#8f9bff'},{k:'capture',l:'Data capture',c:'#49c6d8'},
 {k:'gate',l:'Eligibility & gating',c:'#2ee6c9'},{k:'marker',l:'Marker scoring',c:'#5ad1b0'},
 {k:'reservoir',l:'MONIAC reservoirs (15)',c:'#7c83e8'},{k:'pillar',l:'Pillars (12)',c:'#3ad6a0'},
 {k:'score',l:'Score & cascade',c:'#edb14a'},{k:'safety',l:'Safety',c:'#f0606e'},
 {k:'companion',l:'Companion',c:'#9aa7bd'},{k:'nudge',l:'Nudges',c:'#f59e4b'},
 {k:'adherence',l:'Adherence',c:'#e6c84a'},{k:'recal',l:'Recalibration',c:'#b08bff'}];
var ACOL={};AR.forEach(function(a){ACOL[a.k]=a.c;});

// base (non-pillar/reservoir) nodes
var N=[
 {id:'onb',a:'onboard',l:'Onboarding intake',c:0,r:6,k:'process',f:'first-run capture: demographics, baseline Hx, screeners',conds:['phase ① one-time','sets the applicability vector'],s:'Onboarding & first-run'},
 {id:'lab',a:'capture',l:'Labs (venous/DTC)',c:1,r:4,k:'store',f:'biomarker Observations',conds:['clinical-grade anchors the bands'],s:'Doc 01'},
 {id:'wear',a:'capture',l:'Wearables (D22)',c:1,r:5,k:'store',f:'trust-tiered streams',conds:['inferential = informational only'],s:'Doc 18 §2'},
 {id:'ehr',a:'capture',l:'Patient360 EHR',c:1,r:6,k:'store',f:'conditions (ICD-10) + meds (ATC)',conds:['drives adherence triggers'],s:'Doc 01'},
 {id:'life',a:'capture',l:'Lifestyle / PRO',c:1,r:7,k:'store',f:'self-report questionnaires',conds:['lower confidence; fills gaps'],s:'Doc 18 §1'},
 {id:'gate',a:'gate',l:'Eligibility & gating',c:2,r:5.5,k:'gate',f:'only valid/relevant questions reach scoring',conds:['sex gate (menopause→female)','show_if (smoking detail if smoker)'],s:'Eligibility & gating'},
 {id:'conf',a:'marker',l:'Confidence + decay',c:3,r:3.5,k:'gate',f:'confidence = q_source · exp(−Δt/τ)',conds:['<floor → stale → fallback'],s:'Doc 01 §2'},
 {id:'band',a:'marker',l:'Clinical band r_clin',c:3,r:4.5,k:'process',f:'r off continuous curve (green/yellow/red)',conds:['guideline cut-points'],s:'Doc 03 §1'},
 {id:'cohort',a:'marker',l:'Cohort percentile',c:3,r:5.5,k:'process',f:'r_cohort = F_ic(x) percentile',conds:['NHANES / UK-Biobank'],s:'Doc 01 §4.2'},
 {id:'mmax',a:'marker',l:'max(r_clin, φ·r_cohort)',c:3,r:6.5,k:'gate',f:'φ=0.6 — cohort can only RAISE',conds:['clinical anchor never diluted'],s:'Doc 03 §2'},
 {id:'pers',a:'marker',l:'Personalization + clamp',c:3,r:7.5,k:'gate',f:'band_clamp(… + r_pers·𝟙[non-critical])',conds:['disabled for critical markers'],s:'Doc 03 §2b'},
 {id:'agg',a:'score',l:'δ-power-mean → R_total',c:6,r:5,k:'process',f:'R_total = sqrt(Σ W_k·R_k²)',conds:['worst-pillar leaning'],s:'Doc 03 §5.2'},
 {id:'crit',a:'score',l:'Critical cascade',c:6,r:7,k:'gate',f:'critical-marker red → R_k ← max(R_k,0.60)',conds:['never averaged away'],s:'Doc 03 §6'},
 {id:'score',a:'score',l:'PureScore = 100(1−R)',c:7,r:5,k:'output',f:'round(100·(1−R_total))',conds:['0–100, banded'],s:'Doc 03'},
 {id:'esc',a:'safety',l:'Crisis escalation',c:7,r:8,k:'gate',f:'acute-danger red → clinician pathway',conds:['independent of scalar score'],s:'Doc 11'},
 {id:'comp',a:'companion',l:'Companion vector',c:8,r:3.5,k:'output',f:'Confidence · Trajectory · early-warning',conds:['carries reliability flags'],s:'Doc 12'},
 {id:'nud',a:'nudge',l:'Top-5 nudges',c:8,r:5.5,k:'process',f:'U_a = impact · p̂_a · ease',conds:['every action positive ΔPureScore'],s:'Doc 07'},
 {id:'adh',a:'adherence',l:'Adherence check-ins',c:9,r:5.5,k:'process',f:'adherence ∈ [0,1] → reservoir inflow',conds:['EHR-triggered; 7-day EWMA'],s:'Appendix H'},
 {id:'recal',a:'recal',l:'Recalibration / re-score',c:10,r:5.5,k:'process',f:'realized Δ refits p̂_a + baseline',conds:['closes the loop'],s:'Doc 09'}
];

// 12 pillars (col5) and 15 reservoirs (col4) generated
var PILLMETA=[['cv','CV · cardiovascular',['SBP','ApoB','LDL','RHR','HDL','HRV'],'SBP, ApoB, LDL'],
 ['met','MET · metabolic',['HbA1c','glucose','TG','waist'],'HbA1c, glucose, TG'],
 ['ren','REN · renal',['eGFR','UACR','K+'],'eGFR, UACR, K+'],
 ['hep','HEP · hepatic',['ALT','FIB-4'],'ALT, FIB-4'],
 ['inf','INF · inflammation',['hsCRP','WBC'],'hsCRP (acute)'],
 ['hem','HEM · hematologic',['Hb','platelets','SpO₂','ferritin'],'Hb, platelets, SpO₂'],
 ['endo','ENDO · endocrine',['TSH','vit-D'],'severe'],
 ['bcm','BCM · body-comp/MSK',['body-fat','ALMI','T-score'],'severe sarcopenia/osteoporosis'],
 ['nut','NUT · nutrition',['vit-D','B12','omega-3'],'severe deficiency'],
 ['slp','SLP · sleep',['sleep dur','efficiency','OSA'],'severe OSA'],
 ['fit','FIT · fitness',['VO₂max','steps','MVPA'],'very low VO₂ (priority)'],
 ['mcs','MCS · mental/cognitive',['PHQ-9','GAD-7','ISI'],'PHQ-9 severe']];
var RESMETA=[['ath','ATH · atherogenic','burden','cv'],['vbp','VBP · vascular/BP','burden','cv'],
 ['gly','GLY · glycemic','burden','met'],['adi','ADI · adiposity','burden','met'],
 ['hepf','HEPF · hepatic fat','burden','hep'],['infl','INFL · inflammatory','burden','inf'],
 ['allo','ALLO · allostatic','burden','mcs'],['sld','SLD · sleep debt','burden','slp'],
 ['crf','CRF · cardioresp','asset','fit'],['mus','MUS · muscle','asset','bcm'],
 ['bon','BON · bone','asset','bcm'],['renr','RENR · renal','asset','ren'],
 ['micr','MICR · micronutrient','asset','nut'],['oxd','OXD · oxygen','asset','hem'],
 ['iron','IRON · iron','asset','hem']];
RESMETA.forEach(function(m,i){N.push({id:m[0],a:'reservoir',l:m[1],c:4,r:i,k:'store',
 f:'reservoir load L_j ∈ [0,1] · polarity: '+m[2]+' · feeds '+m[3].toUpperCase(),
 conds:['B̃_k coupled into pillar via ρ_k','leakage λ_j, interference κ (Doc 04)'],s:'Doc 04'});});
PILLMETA.forEach(function(m,i){N.push({id:m[0],a:'pillar',l:m[1],c:5,r:i,k:'pillar',
 f:'R_k = clamp01(conf-weighted δ-power-mean of ['+m[2].join(', ')+'] + ρ·reservoir load)',
 conds:['critical: '+m[3]],s:'Doc 02'});});

var E=[['onb','gate'],['lab','gate'],['wear','gate'],['ehr','gate'],['life','gate'],
 ['gate','conf'],['conf','band'],['conf','cohort'],['band','mmax'],['cohort','mmax'],['mmax','pers'],
 ['agg','crit'],['crit','score'],['crit','esc'],['score','comp'],['score','nud'],
 ['nud','adh'],['adh','recal'],['recal','gly',1],['recal','score',1]];
PILLMETA.forEach(function(m){E.push(['pers',m[0]]);E.push([m[0],'agg']);});
RESMETA.forEach(function(m){E.push([m[0],m[3]]);});  // reservoir → primary pillar
var STEP=['onb','lab','wear','ehr','life','gate','conf','band','cohort','mmax','pers']
 .concat(RESMETA.map(function(m){return m[0];}))
 .concat(PILLMETA.map(function(m){return m[0];}))
 .concat(['agg','crit','score','esc','comp','nud','adh','recal']);

// ---------------- scorer (12 pillars × 15 reservoirs) ----------------
var DEF={sbp:110,apob:70,ldl:90,rhr:60,hdl:55,hrv:55,hba1c:5.2,glu:88,tg:100,waist:85,
 egfr:100,uacr:5,k:4.2,alt:22,fib4:0.9,crp:0.6,wbc:6,hb:14.5,plt:250,spo2:98,ferr:80,
 tsh:2.0,vitd:40,bodyfat:18,almi:8.5,tscore:0,b12:450,omega3:9,sleepdur:7.5,sleepeff:90,
 osa:0,vo2:45,steps:10000,mvpa:200,phq:2,gad:1,isi:3};
var TH={sbp:[120,140,'hi'],apob:[80,100,'hi'],ldl:[100,160,'hi'],rhr:[70,85,'hi'],hdl:[50,40,'lo'],hrv:[40,25,'lo'],
 hba1c:[5.7,6.5,'hi'],glu:[100,126,'hi'],tg:[150,500,'hi'],waist:[94,102,'hi'],
 egfr:[60,45,'lo'],uacr:[30,300,'hi'],k:[5.0,6.0,'hi'],alt:[40,200,'hi'],fib4:[1.3,2.67,'hi'],
 crp:[1,10,'hi'],wbc:[11,20,'hi'],hb:[13,10,'lo'],plt:[150,50,'lo'],spo2:[95,90,'lo'],ferr:[30,15,'lo'],
 tsh:[4.5,10,'hi'],vitd:[30,20,'lo'],bodyfat:[25,32,'hi'],almi:[7,5.5,'lo'],tscore:[-1,-2.5,'lo'],
 b12:[300,200,'lo'],omega3:[8,4,'lo'],sleepdur:[7,6,'lo'],sleepeff:[85,75,'lo'],osa:[0.5,1,'hi'],
 vo2:[40,30,'lo'],steps:[8000,5000,'lo'],mvpa:[150,75,'lo'],phq:[5,20,'hi'],gad:[5,15,'hi'],isi:[8,15,'hi']};
var PILL={cv:['sbp','apob','ldl','rhr','hdl','hrv'],met:['hba1c','glu','tg','waist'],ren:['egfr','uacr','k'],
 hep:['alt','fib4'],inf:['crp','wbc'],hem:['hb','plt','spo2','ferr'],endo:['tsh','vitd'],
 bcm:['bodyfat','almi','tscore'],nut:['vitd','b12','omega3'],slp:['sleepdur','sleepeff','osa'],
 fit:['vo2','steps','mvpa'],mcs:['phq','gad','isi']};
var CRIT={cv:['sbp','apob','ldl'],met:['hba1c','glu','tg'],ren:['egfr','uacr','k'],hep:['alt','fib4'],
 inf:['crp'],hem:['hb','plt','spo2'],bcm:['almi','tscore'],slp:['osa'],mcs:['phq']};
var PRES={cv:['ath','vbp','crf'],met:['gly','adi'],ren:['renr','vbp'],hep:['hepf'],inf:['infl'],
 hem:['oxd','iron'],endo:['allo','micr'],bcm:['mus','bon','adi'],nut:['micr'],slp:['sld','allo'],
 fit:['crf','mus'],mcs:['allo']};
var PILLS=PILLMETA.map(function(m){return m[0];});
var W={cv:.14,met:.12,ren:.08,hep:.06,inf:.07,hem:.06,endo:.07,bcm:.08,nut:.06,slp:.08,fit:.08,mcs:.10};
function clamp(x){return Math.max(0,Math.min(1,x));}
function avg(){var a=arguments,s=0;for(var i=0;i<a.length;i++)s+=a[i];return s/a.length;}
function nrm(v,good,bad){if(good===bad)return 0;return clamp((v-good)/(bad-good));}
function rMark(m,v){var t=TH[m];if(!t)return .1;if(t[2]==='hi'){if(v>=t[1])return .66;if(v>=t[0])return .40;return .10;}else{if(v<=t[1])return .66;if(v<=t[0])return .40;return .10;}}
function reservoirs(mk){
 var R={};
 R.gly=avg(nrm(mk.hba1c,5.4,7),nrm(mk.glu,90,140),nrm(mk.waist,85,105));
 R.ath=avg(nrm(mk.apob,70,140),nrm(mk.ldl,90,170),nrm(mk.sbp,115,160));
 R.infl=avg(nrm(mk.crp,0.5,8),nrm(mk.bodyfat,18,34));
 R.sld=avg(nrm(mk.sleepdur,8,5),nrm(mk.sleepeff,92,72));
 R.adi=avg(nrm(mk.waist,85,110),nrm(mk.bodyfat,18,34));
 R.hepf=avg(nrm(mk.alt,25,120),nrm(mk.fib4,0.9,3));
 R.allo=avg(nrm(mk.phq,3,20),nrm(mk.gad,3,15),nrm(mk.isi,4,18));
 R.crf=avg(nrm(mk.vo2,45,28),nrm(mk.steps,9000,4000));
 R.mus=nrm(mk.almi,8,5.5);R.bon=nrm(mk.tscore,0,-2.5);
 R.renr=avg(nrm(mk.egfr,90,40),nrm(mk.uacr,10,300));
 R.micr=avg(nrm(mk.vitd,40,18),nrm(mk.b12,400,200));
 R.vbp=nrm(mk.sbp,115,160);R.oxd=avg(nrm(mk.hb,14.5,10),nrm(mk.spo2,98,90));R.iron=nrm(mk.ferr,80,15);
 var c={};for(var key in R)c[key]=R[key];
 c.ath=clamp(R.ath+0.15*R.gly+0.15*R.infl);
 c.infl=clamp(R.infl+0.15*R.sld+0.10*R.adi);
 c.gly=clamp(R.gly+0.10*R.sld-0.08*(1-R.crf));
 c.allo=clamp(R.allo+0.12*R.sld);c.sld=clamp(R.sld+0.12*R.allo);
 return c;
}
function scoreProfile(p){
 var mk={};for(var key in DEF)mk[key]=DEF[key];for(var k2 in p.mk)mk[k2]=p.mk[k2];
 var RES=reservoirs(mk),rk={},crit=[];
 PILLS.forEach(function(pk){var ms=PILL[pk],s=0,n=0,cr=false;
  ms.forEach(function(m){var r=rMark(m,mk[m]);s+=r;n++;if((CRIT[pk]||[]).indexOf(m)>=0&&r>=0.66)cr=true;});
  var Rm=s/n,res=PRES[pk]||[],bt=0;res.forEach(function(rid){bt+=RES[rid];});bt=res.length?bt/res.length:0;
  var R=clamp(Rm+0.15*bt);if(cr){R=Math.max(R,0.60);crit.push(pk);}rk[pk]=R;});
 var num=0;PILLS.forEach(function(pk){num+=W[pk]*rk[pk]*rk[pk];});
 var Rtot=Math.sqrt(num);  // ΣW = 1
 return {rk:rk,res:RES,crit:crit,Rtot:Rtot,score:Math.round(100*(1-Rtot)),
  adher:(p.adh!=null?p.adh:(Rtot>0.4?0.65:0.85)),preg:p.preg,
  comp:(crit.length?'Confidence high · early-warning ON':'Confidence high · stable'),
  nudges:(crit.length?('focus: '+crit.join(',').toUpperCase()):'optimization nudges')};
}
var P=[
 {id:'healthy',name:'1 · Healthy young (28F)',mk:{}},
 {id:'prediab',name:'2 · Prediabetic (45M)',mk:{sbp:124,apob:95,ldl:120,rhr:72,hba1c:6.0,glu:110,waist:97,bodyfat:26,vo2:38,steps:7000}},
 {id:'metsyn',name:'3 · Metabolic syndrome (52M)',mk:{sbp:138,apob:130,ldl:150,rhr:78,hba1c:6.4,glu:118,tg:260,waist:106,bodyfat:31,crp:3.5,vo2:32,steps:5500,sleepdur:6}},
 {id:'ckd3',name:'4 · CKD stage 3 (60M)',mk:{sbp:146,apob:110,ldl:140,hba1c:6.2,glu:115,egfr:45,uacr:60,k:5.1,waist:101,crp:2.2}},
 {id:'postmi',name:'5 · Post-MI on meds (58M)',mk:{sbp:124,apob:70,ldl:65,rhr:60,hba1c:5.6,bodyfat:24,vo2:34,steps:7000,phq:8,gad:7},adh:0.6},
 {id:'hypothy',name:'6 · Hypothyroid (40F)',mk:{tsh:8.5,vitd:18,ldl:125,apob:95,bodyfat:28,sleepdur:6.5,phq:9,gad:8,b12:260}},
 {id:'preg',name:'7 · Pregnant (31F)',mk:{sbp:112,apob:90,ldl:120,rhr:84,glu:96,hb:11.5,ferr:22,waist:90},preg:1},
 {id:'frail',name:'8 · Elderly frail (78M)',mk:{sbp:142,apob:100,ldl:135,egfr:55,uacr:40,almi:5.8,tscore:-2.6,vo2:22,steps:3500,vitd:22,hb:12,sleepeff:74,bodyfat:20}},
 {id:'deprx',name:'9 · Depression / anxiety (35F)',mk:{phq:22,gad:14,isi:17,sleepdur:5.5,sleepeff:70,crp:1.8}},
 {id:'athlete',name:'10 · Athlete optimizer (33M)',mk:{sbp:108,apob:62,ldl:80,rhr:44,hdl:68,hrv:90,hba1c:4.9,glu:82,waist:80,bodyfat:11,almi:9.4,vo2:58,steps:14000,mvpa:420,sleepdur:8.2,sleepeff:94}}
];
function nodeValue(id,R,p){
 var mk={};for(var key in DEF)mk[key]=DEF[key];for(var k2 in p.mk)mk[k2]=p.mk[k2];
 if(id==='score')return R.score+'';
 if(R.rk[id]!=null)return 'R='+R.rk[id].toFixed(2)+(R.crit.indexOf(id)>=0?' ⚑':'');
 if(R.res[id]!=null)return 'L='+R.res[id].toFixed(2);
 if(id==='agg')return 'R_tot='+R.Rtot.toFixed(2);
 if(id==='crit')return R.crit.length?('crit: '+R.crit.join(',')):'none';
 if(id==='gate')return p.preg?'pregnancy gates':'standard set';
 if(id==='esc')return R.crit.length?'review':'none';
 if(id==='comp')return R.comp;
 if(id==='nud')return R.nudges;
 if(id==='adh')return 'adher '+R.adher.toFixed(2);
 if(id==='recal')return 're-score 30d';
 if(id==='conf')return 'conf ok';
 if(id==='band'||id==='cohort'||id==='mmax'||id==='pers')return '—';
 if(id==='onb')return 'intake ✓';
 return '';
}

// ---------------- layout ----------------
var COLW=210,ROWH=64,PADX=110,PADY=54,NW=164,NH=44;
var byId={};N.forEach(function(n){n.x=PADX+n.c*COLW;n.y=PADY+n.r*ROWH;byId[n.id]=n;});
var maxRow=0,maxCol=0;N.forEach(function(n){maxRow=Math.max(maxRow,n.r);maxCol=Math.max(maxCol,n.c);});
var W=PADX*2+maxCol*COLW+NW, H=PADY*2+maxRow*ROWH+NH;

var svg=document.getElementById('umSvg'),wrap=document.getElementById('umWrap');
var NS='http://www.w3.org/2000/svg';
function el(t,a){var e=document.createElementNS(NS,t);for(var k in a)e.setAttribute(k,a[k]);return e;}
var scene=el('g',{id:'umScene'});svg.appendChild(scene);
var areaG=el('g',{});scene.appendChild(areaG);
(function(){var groups={};N.forEach(function(n){(groups[n.a]=groups[n.a]||[]).push(n);});
 AR.forEach(function(a){var ns=groups[a.k];if(!ns)return;var x0=1e9,y0=1e9,x1=-1e9,y1=-1e9;
  ns.forEach(function(n){x0=Math.min(x0,n.x);y0=Math.min(y0,n.y);x1=Math.max(x1,n.x+NW);y1=Math.max(y1,n.y+NH);});
  var pad=14;areaG.appendChild(el('rect',{x:x0-pad,y:y0-pad-15,width:(x1-x0)+pad*2,height:(y1-y0)+pad*2+15,rx:13,fill:a.c,'fill-opacity':0.06,stroke:a.c,'stroke-opacity':0.30,'class':'um-areabg','data-area':a.k}));
  var tx=el('text',{x:x0-pad+6,y:y0-pad-3,fill:a.c,'class':'um-arealabel','data-area':a.k});tx.textContent=a.l;areaG.appendChild(tx);});})();
var edgeG=el('g',{});scene.appendChild(edgeG);var edgeEls={};
E.forEach(function(e){var s=byId[e[0]],t=byId[e[1]];if(!s||!t)return;
 var x1=s.x+NW,y1=s.y+NH/2,x2=t.x,y2=t.y+NH/2;if(t.x<=s.x){x1=s.x+NW/2;y1=s.y+NH;x2=t.x+NW/2;y2=t.y;}
 var mx=(x1+x2)/2;edgeG.appendChild(el('path',{d:'M'+x1+','+y1+' C'+mx+','+y1+' '+mx+','+y2+' '+x2+','+y2,'class':'um-edge'+(e[2]?' loop':''),'data-from':e[0],'data-to':e[1]}));
 edgeEls[e[0]+'>'+e[1]]=edgeG.lastChild;});
var nodeG=el('g',{});scene.appendChild(nodeG);var nodeEls={};
N.forEach(function(n){var g=el('g',{'class':'um-node','data-id':n.id,'data-area':n.a});
 var fill=n.k==='gate'?'#1d1a0c':(n.k==='output'?'#0c1726':(n.k==='store'?'#10151e':(n.k==='pillar'?'#10211a':'#121a26')));
 g.appendChild(el('rect',{x:n.x,y:n.y,width:NW,height:NH,rx:n.k==='gate'?12:8,fill:fill,stroke:ACOL[n.a],'stroke-width':1.5}));
 var t1=el('text',{x:n.x+10,y:n.y+18});t1.textContent=n.l;g.appendChild(t1);
 var tv=el('text',{x:n.x+10,y:n.y+34,'class':'um-val','data-val':n.id});g.appendChild(tv);
 g.addEventListener('click',function(ev){ev.stopPropagation();if(!moved)openModal(n.id);});
 g.addEventListener('dblclick',function(ev){ev.stopPropagation();zoomToNode(n);});
 nodeG.appendChild(g);nodeEls[n.id]=g;});

// ---------------- pan / zoom (no pointer-capture so clicks reach nodes) ----------------
var tx=0,ty=0,k=1,down=false,moved=false,lx=0,ly=0;
function apply(){scene.setAttribute('transform','translate('+tx+','+ty+') scale('+k+')');}
function fit(){var bb=wrap.getBoundingClientRect();k=Math.max(0.18,Math.min(1.3,Math.min(bb.width/W,bb.height/H)*0.96));tx=(bb.width-W*k)/2;ty=(bb.height-H*k)/2;apply();}
function zoomToNode(n){closeModal();var bb=wrap.getBoundingClientRect();k=1.3;tx=bb.width/2-(n.x+NW/2)*k;ty=bb.height/2-(n.y+NH/2)*k;apply();}
svg.addEventListener('wheel',function(ev){ev.preventDefault();var bb=svg.getBoundingClientRect();var mx=ev.clientX-bb.left,my=ev.clientY-bb.top;var f=ev.deltaY<0?1.12:1/1.12;var nk=Math.max(0.15,Math.min(3,k*f));tx=mx-(mx-tx)*(nk/k);ty=my-(my-ty)*(nk/k);k=nk;apply();},{passive:false});
svg.addEventListener('pointerdown',function(ev){down=true;moved=false;lx=ev.clientX;ly=ev.clientY;});
svg.addEventListener('pointermove',function(ev){if(!down)return;var dx=ev.clientX-lx,dy=ev.clientY-ly;if(!moved&&Math.abs(dx)+Math.abs(dy)>4){moved=true;svg.classList.add('drag');}if(moved){tx+=dx;ty+=dy;lx=ev.clientX;ly=ev.clientY;apply();}});
window.addEventListener('pointerup',function(){down=false;svg.classList.remove('drag');});
svg.addEventListener('click',function(){if(!moved)closeModal();});

// ---------------- legend / filters ----------------
var hidden={};var ab=document.getElementById('umAreas');
AR.forEach(function(a){var s=document.createElement('span');s.textContent=a.l;s.style.color=a.c;s.style.borderColor=a.c;
 s.onclick=function(){hidden[a.k]=!hidden[a.k];s.classList.toggle('off',hidden[a.k]);applyFilter();};ab.appendChild(s);});
function applyFilter(){var go=document.getElementById('umGates').checked;
 N.forEach(function(n){nodeEls[n.id].style.display=(hidden[n.a]||(go&&n.k!=='gate'))?'none':'';});
 [].forEach.call(areaG.querySelectorAll('[data-area]'),function(e){e.style.display=hidden[e.getAttribute('data-area')]?'none':'';});
 E.forEach(function(e){var p=edgeEls[e[0]+'>'+e[1]];if(!p)return;p.style.display=(hidden[byId[e[0]].a]||hidden[byId[e[1]].a]||(go&&byId[e[0]].k!=='gate'&&byId[e[1]].k!=='gate'))?'none':'';});}
document.getElementById('umGates').onchange=applyFilter;

// ---------------- profiles / step-through ----------------
var sel=document.getElementById('umProfile');
P.forEach(function(p,i){var o=document.createElement('option');o.value=i;o.textContent=p.name;sel.appendChild(o);});
var curR=null,curP=null,stepIdx=-1,timer=null;
function setProfile(i){curP=P[i];curR=scoreProfile(curP);resetAnim();document.getElementById('umScoreBadge').textContent='?';}
function clearVals(){N.forEach(function(n){nodeEls[n.id].querySelector('[data-val]').textContent='';nodeEls[n.id].classList.remove('on','dim');});E.forEach(function(e){var p=edgeEls[e[0]+'>'+e[1]];if(p)p.classList.remove('on');});}
function resetAnim(){if(timer){clearInterval(timer);timer=null;}stepIdx=-1;clearVals();document.getElementById('umPlay').innerHTML='&#9654; Play';}
function showNode(id){nodeEls[id].querySelector('[data-val]').textContent=nodeValue(id,curR,curP);nodeEls[id].classList.add('on');
 E.forEach(function(e){if(e[1]===id&&nodeEls[e[0]]&&nodeEls[e[0]].classList.contains('on')){var p=edgeEls[e[0]+'>'+e[1]];if(p)p.classList.add('on');}});
 if(document.getElementById('umPath').checked)N.forEach(function(m){nodeEls[m.id].classList.toggle('dim',STEP.indexOf(m.id)>stepIdx);});
 if(id==='score')document.getElementById('umScoreBadge').textContent=curR.score;}
function stepNext(){if(!curR)setProfile(+sel.value);if(stepIdx>=STEP.length-1)return false;stepIdx++;showNode(STEP[stepIdx]);return stepIdx<STEP.length-1;}
function play(){if(!curR)setProfile(+sel.value);if(timer){resetAnim();return;}document.getElementById('umPlay').innerHTML='&#10073;&#10073; Pause';
 var sp=+document.getElementById('umSpeed').value;timer=setInterval(function(){if(!stepNext()){clearInterval(timer);timer=null;document.getElementById('umPlay').innerHTML='&#9654; Replay';}},sp);}
sel.onchange=function(){setProfile(+sel.value);};
document.getElementById('umPlay').onclick=play;
document.getElementById('umStep').onclick=function(){if(timer){clearInterval(timer);timer=null;document.getElementById('umPlay').innerHTML='&#9654; Play';}stepNext();};
document.getElementById('umReset').onclick=resetAnim;
document.getElementById('umPath').onchange=function(){if(stepIdx>=0)showNode(STEP[stepIdx]);if(!this.checked)N.forEach(function(m){nodeEls[m.id].classList.remove('dim');});};
document.getElementById('umFit').onclick=fit;
document.getElementById('umFs').onclick=function(){wrap.classList.toggle('fs');setTimeout(fit,60);};
document.addEventListener('keydown',function(e){if(e.key==='Escape'&&wrap.classList.contains('fs')){wrap.classList.remove('fs');setTimeout(fit,60);}});

// ---------------- modal ----------------
function openModal(id){var n=byId[id],m=document.getElementById('umModal'),b=document.getElementById('umModalBody');
 var live=curR?('<div class="um-live"><b>'+curP.name+'</b><br>live value: <b>'+(nodeValue(id,curR,curP)||'—')+'</b></div>'):'';
 b.innerHTML='<h3>'+n.l+'</h3> <span class="um-tag" style="color:'+ACOL[n.a]+';border-color:'+ACOL[n.a]+'">'+AR.filter(function(a){return a.k===n.a;})[0].l+'</span>'+
  '<pre>'+n.f+'</pre><div class="small" style="margin-top:6px;color:#9bb0c5">conditions / gates:</div><ul>'+
  n.conds.map(function(c){return '<li>'+c+'</li>';}).join('')+'</ul><div class="small" style="margin-top:8px;color:#6b7d92">source: '+n.s+'</div>'+live;
 m.classList.add('show');}
function closeModal(){document.getElementById('umModal').classList.remove('show');}
document.getElementById('umX').onclick=closeModal;
document.getElementById('umModal').onclick=function(e){if(e.target===this)closeModal();};

applyFilter();fit();setProfile(0);
})();
</script>"""
    return "PureScore uber-map", body

# =================================================================== APPENDIX F — lifestyles & personas
def build_lifestyles():
    pa = _load("persona-axes.json")
    m = pa["meta"]
    h = ['<div class="crumbs"><a href="index.html">Home</a> › Questions &amp; intake › Lifestyles</div>',
         '<h1>Appendix F — Lifestyles, Axes &amp; Perceived-vs-Actual</h1>',
         '<p class="lead">%s</p>' % _esc(m["idea"]), ILLUS]
    h.append('<div class="diagram"><div class="dt">Perceived vs actual → strategy</div>'
             '<pre class="mermaid">flowchart LR\n'
             '  SR["self-report (perceived)"] --> GAP{"gap"}\n'
             '  OBJ["labs + wearables + PRO (actual)"] --> GAP\n'
             '  GAP --> NUDGE["strategy + companion vector (Doc 12)"]\n'
             '  NUDGE --> ACT["nudges (Doc 07/16)"] --> AD["adherence"] --> OBJ</pre></div>')
    # axes
    h.append('<h2 id="axes">Lifestyle axes</h2>')
    h.append('<p class="small muted">Any person is a vector over these independent axes. Each axis names the '
             'self-report signal (perceived) and the objective signal (actual) used to place them, and the '
             'pillars it drives.</p>')
    h.append('<div class="tablewrap"><table><thead><tr><th>Axis</th><th>Levels</th><th>Pillars</th>'
             '<th>Perceived signal</th><th>Actual signal</th></tr></thead><tbody>')
    for ax in pa["axes"]:
        h.append('<tr><td><b>%s</b><br><span class="small muted mono">%s</span></td>'
                 '<td class="small">%s</td><td class="small">%s</td><td class="small muted">%s</td>'
                 '<td class="small muted">%s</td></tr>'
                 % (_esc(ax["name"]), _esc(ax["id"]),
                    " → ".join('<span class="chip b-mut">%s</span>' % _esc(l) for l in ax["levels"]),
                    ", ".join(_esc(p) for p in ax["pillars"]),
                    ", ".join(_esc(x) for x in ax["perceived_signal"]),
                    ", ".join(_esc(x) for x in ax["actual_signal"])))
    h.append('</tbody></table></div>')
    # archetypes
    h.append('<h2 id="archetypes">Named archetypes <span class="small muted">· %d presets</span></h2>' % len(pa["archetypes"]))
    h.append('<p class="small muted">Common presets = the nearest vector over the axes above. Each links to a '
             'clinical persona (<a class="xref" href="appendix-personas.html">Appendix D</a>) and carries pillar '
             'weight multipliers.</p>')
    h.append('<div class="tablewrap"><table><thead><tr><th>Archetype</th><th>Clinical persona</th>'
             '<th>Axis profile</th><th>Weight multipliers</th><th>Typical conditions</th><th>Frame</th></tr></thead><tbody>')
    for a in pa["archetypes"]:
        prof = " · ".join("%s:%s" % (_esc(k), _esc(v)) for k, v in a.get("axis_profile", {}).items())
        h.append('<tr><td><b>%s</b><br><span class="small muted mono">%s</span></td>'
                 '<td class="small mono">%s</td><td class="small">%s</td><td class="small mono">%s</td>'
                 '<td class="small">%s</td><td class="small muted">%s</td></tr>'
                 % (_esc(a["name"]), _esc(a["key"]), _esc(a.get("links_clinical_persona", "—")),
                    prof, _esc(a.get("weight_multipliers", "—")),
                    _esc(", ".join(a.get("typical_conditions", []))), _esc(a.get("frame", ""))))
    h.append('</tbody></table></div>')
    # perceived vs actual gaps
    pv = pa["perceived_vs_actual"]
    h.append('<h2 id="gaps">Perceived-vs-actual gaps → nudge strategy</h2>')
    h.append('<div class="callout note"><div class="ct">Method</div>%s</div>' % _esc(pv["method"]))
    h.append('<div class="tablewrap"><table><thead><tr><th>Gap</th><th>Axis</th><th>Perceived</th>'
             '<th>Actual</th><th>Detecting signal</th><th>Strategy</th><th>Companion vector</th></tr></thead><tbody>')
    for g in pv["gaps"]:
        h.append('<tr><td><b>%s</b></td><td class="small mono">%s</td><td class="small">%s</td>'
                 '<td class="small">%s</td><td class="small muted">%s</td><td class="small">%s</td>'
                 '<td class="small muted">%s</td></tr>'
                 % (_esc(g["id"]), _esc(g["axis"]), _esc(g["perceived"]), _esc(g["actual"]),
                    _esc(g["signal"]), _esc(g["strategy"]), _esc(g.get("companion_effect", ""))))
    h.append('</tbody></table></div>')
    h.append('<p class="small muted">Machine-readable source: <code>data/persona-axes.json</code>. '
             'Links: %s.</p>' % _esc(", ".join(m.get("links", []))))
    return "Appendix F · Lifestyles", "".join(h)

# =================================================================== APPENDIX G — coverage audit (versioned)
_PNAME = {p[0]: p[1] for p in PILLARS}                       # pillar id -> full name
_WEAR_HINTS = ("wear", "hrv", "rmssd", "cgm", "glucose monitor", "step", "actig", "spo2",
               "oximet", "resting hr", "resting heart", "sleep stage", "sleep track", "ring",
               "watch", "accelerom", "vo2", "cadence", "readiness", "skin temp")

def _is_wear(sigs):
    j = " ".join(str(s).lower() for s in (sigs or []))
    return any(hint in j for hint in _WEAR_HINTS)

def _audit_bar(frac, color, label=""):
    pct = max(2, min(100, int(round(frac * 100))))
    return ('<div style="display:flex;align-items:center;gap:8px">'
            '<div style="flex:1;background:#0c1322;border:1px solid var(--line);border-radius:5px;height:9px;overflow:hidden">'
            '<i style="display:block;height:100%%;width:%d%%;background:%s"></i></div>'
            '<span class="small mono" style="min-width:58px;text-align:right">%s</span></div>'
            % (pct, color, _esc(label)))

import re as _re
def _slug_metric(name):
    return _re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")

# ---- source channels & the canonical fusion scenarios -------------------------------
_CH = {                                                       # id -> (label, chip-class, note)
 "L":  ("biomarker · lab", "b-green", "highest accuracy · low frequency"),
 "Wc": ("wearable · clinical-grade", "b-green", "≈ lab · may drive red (D22)"),
 "Ww": ("wearable · consumer", "b-acc", "trajectory / early-warning · Watch-capped"),
 "Wi": ("wearable · inferential", "b-yellow", "informational only (D22)"),
 "S":  ("self-report · PRO", "b-yellow", "subjective · screening / context"),
}
_PILLAR_SRC = {                                               # pillar -> {channel: [signals]}
 "CV":  {"L": ["ApoB", "LDL-C", "Lp(a)", "clinic BP"], "Wc": ["validated BP cuff", "single-lead ECG"],
         "Ww": ["resting HR", "HRV"], "S": ["smoking PRO", "activity recall"]},
 "MET": {"L": ["HbA1c", "fasting glucose", "fasting insulin"], "Wc": ["CGM"],
         "Ww": ["weight (smart scale)"], "S": ["diet-quality", "IPAQ-SF"]},
 "REN": {"L": ["eGFR (creat/cystatin)", "UACR", "K⁺"], "Wc": ["validated BP cuff"],
         "S": ["NSAID-use PRO", "symptom PRO"]},
 "HEP": {"L": ["ALT/AST", "FIB-4", "GGT"], "S": ["AUDIT-C", "metabolic PRO"]},
 "INF": {"L": ["hs-CRP", "IL-6", "ferritin", "WBC"], "Ww": ["skin temp", "resting HR (illness)"],
         "S": ["symptom / illness PRO"]},
 "HEM": {"L": ["Hgb", "ferritin", "B12/folate"], "Wc": ["overnight SpO₂ (validated)"],
         "Ww": ["SpO₂ (spot)", "respiratory rate"], "S": ["fatigue / dyspnea PRO"]},
 "ENDO":{"L": ["TSH", "E2/FSH/LH/T", "cortisol"], "Ww": ["skin temp (cycle)", "HRV"],
         "S": ["PSS-4", "vasomotor PRO"]},
 "BCM": {"L": ["DEXA BMD", "ALMI"], "Wc": ["grip dynamometer"],
         "Ww": ["body-comp (smart scale)", "gait speed"], "S": ["falls PRO", "function PRO"]},
 "NUT": {"L": ["vit-D", "B12", "ferritin", "omega-3 index"], "S": ["diet-quality screener"]},
 "SLP": {"Wc": ["PSG (clinic)"], "Ww": ["sleep duration / efficiency / regularity", "overnight SpO₂"],
         "S": ["ISI", "sleep-hygiene PRO"]},
 "FIT": {"L": ["VO₂max (CPET)"], "Ww": ["steps", "VO₂max est.", "HR-recovery"],
         "Wi": ["readiness / recovery"], "S": ["IPAQ-SF"]},
 "MCS": {"Ww": ["HRV", "sleep"], "Wi": ["stress score"],
         "S": ["PHQ-9", "GAD-7", "UCLA-3", "WHO-5", "PSS-4"]},
}
# canonical scenarios: id, required-channel predicate, name, value rule, confidence, dominant, guardrail
_SCN = [
 ("Lonly",  lambda c: "L" in c,
  "Lab only", "take the lab value as the level", "high", "lab", "—"),
 ("Wconly", lambda c: "Wc" in c,
  "Clinical-wearable only", "take the device value (CGM / cuff / ECG)", "high", "wearable-clinical", "D22: may drive red"),
 ("Wwonly", lambda c: "Ww" in c and "L" not in c and "Wc" not in c,
  "Consumer-wearable only", "trend vs personal baseline; level provisional", "medium", "wearable-consumer",
  "D22: Watch/Advisory cap — no red without lab/clinical confirm"),
 ("Sonly",  lambda c: "S" in c and "L" not in c and "Wc" not in c and "Ww" not in c,
  "Self-report only", "screening estimate; flag low coverage", "low–med", "self-report",
  "never a band alone; prompts measurement"),
 ("LWw",    lambda c: "L" in c and "Ww" in c,
  "Lab + consumer-wearable", "lab anchors the level; wearable supplies trajectory + early-warning", "high",
  "lab (level) + wearable (trend)", "—"),
 ("WcWw",   lambda c: "Wc" in c and "Ww" in c,
  "Clinical + consumer wearable", "clinical anchors; consumer corroborates with denser sampling", "high",
  "wearable-clinical", "—"),
 ("LS",     lambda c: "L" in c and "S" in c,
  "Lab + self-report", "lab dominates the level; PRO adds adherence / symptom context", "high", "lab", "—"),
 ("ALL",    lambda c: ("L" in c or "Wc" in c) and "Ww" in c and "S" in c,
  "All sources present", "precision-weighted: lab/clinical level + wearable trajectory + PRO context", "very high",
  "lab/clinical (level)", "—"),
 ("CONF",   lambda c: ("L" in c or "Wc" in c) and ("Ww" in c or "Wi" in c),
  "Conflict (consumer red, lab green)", "trust hierarchy — lab/clinical wins; consumer anomaly → reconfirm, raise Watch only",
  "high", "lab/clinical", "Doc 12 §3.4 · D22 — no false red"),
]
_CONF_COL = {"high": "b-green", "very high": "b-green", "medium": "b-acc", "low–med": "b-yellow"}

# ---- wearable cadence × provider (illustrative design targets) ----------------------
_PROVIDERS = ["Terra", "Apple HealthKit", "Android Health Connect", "Samsung Health",
              "Fitbit", "Garmin", "Oura", "Whoop"]
_CADENCE = {
 # metric: [Terra, HealthKit, HealthConnect, Samsung, Fitbit, Garmin, Oura, Whoop]
 "Resting HR":      ["daily + intraday*", "per-sample 5–10 min + daily", "per-sample + daily", "intraday", "1-min intraday (API)", "intraday 15 min", "nightly + daytime", "continuous → cycle"],
 "HRV (RMSSD)":     ["nightly + samples", "beat-to-beat (SDNN) overnight", "HRV samples", "intraday", "nightly", "nightly + stress", "nightly", "continuous → cycle"],
 "Sleep stages":    ["per-session", "per-session (asleep/stages)", "per-session", "per-session", "per-session", "per-session", "per-session (gold-ish)", "per-cycle"],
 "Overnight SpO₂":  ["nightly avg/min", "per-sample (on-demand)", "per-sample", "spot/nightly", "nightly est.", "Pulse Ox nightly", "nightly avg/min", "nightly"],
 "Steps":           ["daily + intraday", "per-sample + daily", "per-sample + daily", "intraday", "15-min intraday", "intraday", "daily activity", "—"],
 "Skin temp":       ["nightly deviation", "wrist temp nightly", "nightly", "nightly", "nightly deviation", "—", "nightly deviation", "nightly"],
 "ECG / rhythm":    ["event (on-demand)", "single-lead on-demand", "—", "on-demand", "on-demand", "on-demand", "—", "—"],
 "VO₂max (est.)":   ["on-update", "cardio-fitness periodic", "—", "periodic", "periodic", "per-activity", "—", "—"],
 "Readiness/Stress":["daily score", "—", "—", "stress intraday", "daily readiness", "Body Battery intraday", "readiness daily", "recovery daily"],
 "Respiratory rate":["nightly", "nightly (sleep)", "nightly", "nightly", "nightly", "nightly", "nightly", "nightly"],
}
# ---- freshness SLA per metric (the contract F8 asked for: expected cadence · fresh-within · stale-after · drives) ----
_CADENCE_SLA = {
 "Resting HR":       ("daily", "24 h", "48 h", "stale-wearable state; RHR-trend Confidence"),
 "HRV (RMSSD)":      ("nightly", "24 h", "72 h", "autonomic/stress companion freshness"),
 "Sleep stages":     ("per night", "24 h", "48 h", "SLP pillar; sleep-debt reservoir"),
 "Overnight SpO₂":   ("per night (screen)", "7 d", "14 d", "OSA screen; HEM"),
 "Steps":            ("daily", "24 h", "48 h", "FIT activity; adherence corroboration"),
 "Skin temp":        ("nightly", "24 h", "72 h", "illness / cycle early-warning"),
 "ECG / rhythm":     ("on event", "last event", "event-based (no decay)", "AFib check — confirm vs 12-lead"),
 "VO₂max (est.)":    ("periodic", "30 d", "90 d", "FIT trend (informational, D22)"),
 "Readiness/Stress": ("daily", "24 h", "72 h", "informational companion only (D22)"),
 "Respiratory rate": ("nightly", "24 h", "72 h", "INF / SLP early-warning"),
}

# ---- metric calculation reference (individual / aggregated / derived / baseline) ----
_CALCS = {
 "HRV (RMSSD)": ("Single overnight RMSSD from beat-to-beat RR intervals during stable sleep; ms; reject motion/arrhythmia windows.",
                 "Nightly value = median of clean 5-min windows; report a 7-day EWMA to damp night-to-night noise.",
                 "RMSSD = √(mean(ΔRR²)); ln-transform for scoring; autonomic-load companion blends with resting HR.",
                 "Personal baseline = trimmed 30-night mean ± SD; z = (x−μ)/σ feeds Stage-2b (κ·tanh(z/2), Doc 03 §2b)."),
 "Resting HR": ("Lowest stable HR during sleep/inactivity; bpm; exclude wake/motion.",
                "Daily = sleeping-HR minimum or 10th-pct; 7-day median for trend.",
                "Used raw; combined with HRV for the autonomic/stress companion (Doc 12 §4.1).",
                "30-day personal baseline; β-blocker/illness flagged as confounders (down-weighted)."),
 "Sleep efficiency": ("Per-session = time-asleep ÷ time-in-bed; %.",
                "Nightly value; 7-night mean + regularity (onset-time SD) tracked separately.",
                "Composite of stage timing; regularity = SD of mid-sleep time over the window.",
                "Personal baseline per night-type (work vs free day); ISI corroborates (Appendix C)."),
 "Overnight SpO₂": ("Per-sample reflectance SpO₂; %; consumer-tier accuracy.",
                "Nightly mean + minimum + desaturation index (ODI).",
                "ODI = desaturation events/hr; flags possible OSA → ISI / clinical PSG.",
                "Personal nightly baseline; sustained low → clinical confirmation before any red (D22)."),
 "Steps / MVPA": ("Per-epoch accelerometer counts → steps; cadence ≥100 spm ≈ moderate.",
                "Daily steps; weekly MVPA minutes (bouts ≥10 min).",
                "MVPA derived from cadence/HR zones; cross-checked vs IPAQ-SF self-report.",
                "Personal 28-day baseline; goal pacing vs target (Goals catalogue)."),
 "VO₂max (est.)": ("Not measured directly on consumer devices; inferred — inferential tier.",
                "Periodic estimate; report trend not absolute.",
                "Derived from HR–pace during runs (firmware model); CPET is the lab reference.",
                "Personal trend baseline; absolute level informational only (D22)."),
 "eGFR": ("Computed from serum creatinine (± cystatin-C), age, sex; mL/min/1.73m².",
                "Per-draw; trend across draws (slope = decline rate).",
                "CKD-EPI 2021 (race-free); cystatin-C variant when available.",
                "Personal slope vs prior draws; pregnancy/elderly frames shift expectation (Doc 05)."),
 "ApoB / non-HDL": ("Single immunoassay (ApoB) or calculated non-HDL = TC − HDL; mg/dL.",
                "Per-draw; trend across draws under therapy.",
                "ApoB preferred particle-count proxy; med-responsiveness modelled (modifiability).",
                "Personal target vs guideline (e.g. <80 high-risk); FH = low modifiability (Doc 12)."),
 "Blood pressure": ("Single cuff reading; mmHg; clinical-grade cuff = clinical tier.",
                "Average of ≥2 readings/sitting; 7-day home-BP mean (preferred).",
                "MAP, pulse-pressure derived; white-coat / masked detected vs clinic.",
                "Personal home baseline; medication context (treated-to-target, D3)."),
 "HbA1c": ("Single lab assay; % (mmol/mol); reflects ~90-day glycemia.",
                "Per-draw; trend across draws; CGM TIR cross-check when present.",
                "Estimates mean glucose; discordance with CGM flags hemoglobinopathy/turnover.",
                "Personal trajectory; pre-DM/T2D thresholds; Ramadan/anemia frames adjust."),
}
_CALC_KEYS = {_slug_metric(m) for m in _CALCS}

def _stat_chip(kind):
    return {"ok": '<span class="chip b-green">✅ closes loop</span>',
            "warn": '<span class="chip b-gold">⚠️ partial</span>',
            "gap": '<span class="chip b-red">❌ gap</span>'}[kind]

_SEVCHIP = {"P0": '<span class="chip b-red">P0</span>', "P1": '<span class="chip b-gold">P1</span>',
            "P2": '<span class="chip b-mut">P2</span>'}
_STATUS_META = {"open": ("Open", "b-red"), "in_progress": ("In progress", "b-gold"),
                "addressed": ("Addressed", "b-green")}

def _eval_auto(rule, stats):
    if not rule:
        return None
    try:
        return bool(eval(rule, {"__builtins__": {}}, stats))
    except Exception:
        return None

def build_coverage_audit():
    qb = _load("question-bank.json"); qs = qb["questions"]; qm = qb["meta"]
    pa = _load("persona-axes.json")
    log = _load("audit-log.json")
    total = len(qs)
    from collections import Counter
    pillar_q = Counter(); pillar_wear = Counter(); res_q = Counter(); axis_q = Counter(); axis_wear = Counter()
    corr_n = 0; wear_n = 0; adher_n = 0; goal_n = 0; persona_det_n = 0
    for q in qs:
        tp = set(); tr = set()
        for r in q.get("responses", []):
            for p in (r.get("pillars") or {}): tp.add(p)
            for rv in (r.get("reservoirs") or {}): tr.add(rv)
        cb = q.get("perceived_actual", {}).get("corroborated_by", [])
        if cb: corr_n += 1
        wear = _is_wear(cb)
        if wear: wear_n += 1
        for p in tp:
            pillar_q[p] += 1
            if wear: pillar_wear[p] += 1
        for rv in tr: res_q[rv] += 1
        for a in q.get("dimensions", {}).get("axis_tags", []):
            axis_q[a] += 1
            if wear: axis_wear[a] += 1
        cat = (q.get("category", "") + " " + q.get("type", "")).lower()
        if "adher" in cat: adher_n += 1
        if q.get("goal") or "goal" in cat: goal_n += 1
        if q.get("persona_signal") or q.get("determines_persona"): persona_det_n += 1
    # closure artifacts (Appendices H/I/J) — their presence auto-closes F1/F2/F3
    def _safe_count(fname, key):
        try:
            return len(_load(fname).get(key, []))
        except Exception:
            return 0
    adher_cat = _safe_count("adherence.json", "items")
    persona_sig = _safe_count("persona-matrix.json", "signals")
    goal_struct = _safe_count("goals.json", "goals")
    try:
        _onb = _load("onboarding.json")
    except Exception:
        _onb = {}
    source_typed = sum(1 for _p in PILLARS for _m in _p[3] if marker_channel(_m[0], _m[8]))
    stats = {"total": total, "corr_n": corr_n, "wear_n": wear_n,
             "adher_n": adher_n + adher_cat,
             "goal_n": goal_n, "goal_struct": goal_struct,
             "persona_det_n": persona_det_n + persona_sig, "source_typed": source_typed,
             "onboarding_steps": len(_onb.get("steps", [])), "demographics_n": len(_onb.get("demographics", [])),
             "coldstart_items": len((_onb.get("cold_start", {}) or {}).get("bootstrap_items", [])),
             "consent_gates": len(_onb.get("consent_gating", [])),
             "wear_corr_typed": _safe_count("wearable-corroboration.json", "metrics"),
             "cadence_sla": len(_CADENCE_SLA),
             "hep_ren_symptoms": sum(1 for q in qs if q.get("id") in (
                 "Q_RENHEP_REN_SYMPTOMS", "Q_RENHEP_REN_NSAID", "Q_RENHEP_REN_URINE",
                 "Q_RENHEP_HEP_SYMPTOMS", "Q_RENHEP_HEP_RISK", "Q_RENHEP_HEP_TOXIN"))}
    pillars = qm.get("pillars", list(_PNAME))
    reservoirs = qm.get("reservoirs", sorted(res_q))
    pmax = max(pillar_q.values()) if pillar_q else 1
    rmax = max(res_q.values()) if res_q else 1
    n_clin = len(PERSONAS); n_arch = len(pa.get("archetypes", []))
    union_personas = n_clin + n_arch

    # resolve effective status for every item (auto may promote to addressed)
    items = log["items"]
    for it in items:
        eff = it.get("status", "open")
        auto = _eval_auto(it.get("auto_rule"), stats)
        if auto is True:
            eff = "addressed"
        it["_eff"] = eff
        it["_auto"] = auto
    buckets = {"open": [], "in_progress": [], "addressed": []}
    for it in items:
        buckets[it["_eff"]].append(it)

    meta = log["meta"]
    h = ['<div class="crumbs"><a href="index.html">Home</a> › Reference › Coverage audit</div>',
         '<h1>Appendix G — Questionnaire Coverage Audit <span class="small muted">(product-loop lens · cumulative)</span></h1>',
         '<p class="lead">A <b>cumulative, versioned</b> evaluation of the lifestyle intake — the validated PROs '
         '(<a class="xref" href="appendix-questions.html">Appendix C</a>), the %d-item question bank '
         '(<a class="xref" href="appendix-question-bank.html">Appendix E</a>) and the axes/archetypes '
         '(<a class="xref" href="appendix-lifestyles.html">Appendix F</a>) — judged on whether the captured data '
         '<b>closes the product loop</b>: capture → score → wearable-match → persona → goals → nudge → adherence → back. '
         'Findings are dated; each keeps its full history (open a finding’s <b>history</b> to view &amp; select older '
         'dated versions). Numbers compute live from <code>data/question-bank.json</code>.</p>' % total, ILLUS]
    # revision strip
    h.append('<div class="tagrow" style="margin:2px 0 8px">'
             + " ".join('<span class="chip b-mut">%s · %s — %s</span>' % (_esc(r["date"]), _esc(r["tag"]), _esc(r["note"]))
                        for r in meta.get("revisions", [])) + '</div>')

    # ===== status header (pinned) =====
    h.append('<div class="panel" style="position:sticky;top:54px;z-index:6">')
    h.append('<div class="tagrow" style="gap:10px;align-items:center">'
             '<b>Status</b>'
             '<span class="chip b-red">%d open</span>'
             '<span class="chip b-gold">%d in progress</span>'
             '<span class="chip b-green">%d addressed</span>'
             '<span class="small muted">· current %s</span></div>'
             % (len(buckets["open"]), len(buckets["in_progress"]), len(buckets["addressed"]), _esc(meta.get("current_version", ""))))
    # compact item chips grouped
    for key, lbl in [("open", "Open"), ("in_progress", "In progress"), ("addressed", "Addressed")]:
        if not buckets[key]:
            continue
        cls = _STATUS_META[key][1]
        bx = "☑ " if key == "addressed" else "☐ "
        chips = " ".join('<a class="chip %s" href="#%s">%s%s · %s</a>' % (cls, _esc(it["id"]), bx, _esc(it["id"]), _esc(it["title"]))
                         for it in buckets[key])
        h.append('<div class="small" style="margin-top:7px"><span class="muted">%s:</span> %s</div>' % (lbl, chips))
    h.append('</div>')

    # ===== findings register (versioned) =====
    h.append('<h2 id="findings">Findings register <span class="small muted">· dated &amp; versioned</span></h2>')
    h.append('<p class="small muted">Each finding shows its latest version. The status auto-advances to '
             '<b>Addressed</b> when its live rule is met (e.g. adherence count &gt; 0); otherwise it is set in '
             '<code>data/audit-log.json</code>. Click <b>history</b> for older dated versions.</p>')
    pop_blocks = []
    for it in items:
        latest = it["versions"][-1]
        slbl, scls = _STATUS_META[it["_eff"]]
        box = "☑" if it["_eff"] == "addressed" else "☐"   # completed items render checked + green
        autotag = ""
        if it.get("auto_rule"):
            autotag = (' <span class="chip b-green">auto ✓</span>' if it["_auto"]
                       else ' <span class="chip b-mut mono" title="auto-rule (not yet met)">auto: %s</span>' % _esc(it["auto_rule"]))
        h.append('<div class="panel" id="%s" style="margin:10px 0">' % _esc(it["id"]))
        h.append('<div class="tagrow" style="gap:8px;align-items:center;margin-bottom:6px">'
                 '%s<b class="mono">%s</b><span style="flex:1">%s</span>'
                 '<span class="chip %s">%s %s</span>'
                 '<span class="chip b-mut">stage %s</span>'
                 '<span class="chip b-mut">%s · %s</span>%s'
                 '<button class="chip b-acc" style="cursor:pointer" onclick="auditPop(\'%s\')">history (%d) ▾</button>'
                 '</div>'
                 % (_SEVCHIP[it["severity"]], _esc(it["id"]), _esc(it["title"]), scls, box, _esc(slbl),
                    _esc(it["stage"]), _esc(latest["date"]), _esc(latest["tag"]), autotag,
                    _esc(it["id"]), len(it["versions"])))
        h.append('<div class="small">%s</div>' % latest["body"])
        h.append('</div>')
        # popup source block: each version selectable
        vt = []
        for vi, v in enumerate(it["versions"]):
            vlbl, vcls = _STATUS_META.get(v["status"], ("?", "b-mut"))
            vt.append('<button class="chip %s av-tab" data-item="%s" data-v="%d" style="cursor:pointer">%s · %s · %s</button>'
                      % (vcls, _esc(it["id"]), vi, _esc(v["date"]), _esc(v["tag"]), _esc(vlbl)))
        bodies = []
        for vi, v in enumerate(it["versions"]):
            bodies.append('<div class="av-body" data-item="%s" data-v="%d" style="%s">'
                          '<div class="small muted" style="margin-bottom:6px">%s · %s · status: %s</div>%s</div>'
                          % (_esc(it["id"]), vi, "" if vi == len(it["versions"]) - 1 else "display:none",
                             _esc(v["date"]), _esc(v["tag"]), _esc(v["status"]), v["body"]))
        pop_blocks.append('<div class="popsrc" id="pop-%s"><h3 style="margin-top:0">%s · %s</h3>'
                          '<div class="tagrow" style="margin-bottom:10px">%s</div>%s</div>'
                          % (_esc(it["id"]), _esc(it["id"]), _esc(it["title"]), "".join(vt), "".join(bodies)))

    # ===== coverage analysis (v1) =====
    h.append('<h2 id="analysis">Coverage analysis <span class="small muted">· as of 2026-06-19 · v1</span></h2>')
    h.append('<div class="diagram"><div class="dt">The capture→adherence loop — and where it is open</div>'
             '<pre class="mermaid">flowchart LR\n'
             '  CAP["1 Capture<br/>%d Q · 12 domains<br/>catalogue OK · flow PARTIAL"] --> SCORE["2 Score<br/>12 pillars · 15 reservoirs<br/>OK"]\n'
             '  SCORE --> MATCH["3 Wearable-match<br/>%d/%d corroborated<br/>PARTIAL"]\n'
             '  MATCH --> PER["4 Persona determine<br/>no input→persona map<br/>GAP"]\n'
             '  PER --> GOAL["5 Goals<br/>no catalogue<br/>GAP"]\n'
             '  GOAL --> NUDGE["6 Nudge (Doc 07/16)<br/>OK"]\n'
             '  NUDGE --> ADH["7 Adherence<br/>%d check-ins<br/>GAP"]\n'
             '  ADH -.loop broken.-> CAP</pre></div>' % (total, corr_n, total, adher_n))
    h.append('<h3 id="scorecard">Loop-stage scorecard</h3>')
    h.append('<div class="tablewrap"><table><thead><tr><th>#</th><th>Loop stage</th><th>What exists</th>'
             '<th>Verdict</th><th>Where it breaks</th></tr></thead><tbody>')
    rows = [
      ("1", "Capture (multi-dimensional intake)",
       "%d questions · 12 domains · 17 axes · validated PROs for mental-health/anxiety (PHQ-9/2, GAD-7/2, PSS-4, UCLA-3, WHO-5), sleep (ISI), activity (IPAQ-SF), diet, substance" % total,
       "warn", "Question <i>catalogue</i> is broad, but the first-run <b>experience</b> is undocumented — onboarding flow, demographic capture, cold-start/progressive-profiling, consent + device-pairing (F9–F12, <a href=\"#onboarding\">matrix</a>)"),
      ("2", "Score (pillars + reservoirs)",
       "All 12 pillars and all 15 reservoirs reachable from ≥1 self-report item (signed Δrisk + reservoir inflow)",
       "ok", "Thin on HEP/REN (lab-dominated) — see source-fusion (F5)"),
      ("3", "Wearable-match (perceived ↔ actual)",
       "%d/%d questions carry <code>corroborated_by</code>; %d reference a wearable/CGM signal" % (corr_n, total, wear_n),
       "warn", "Free-text corroboration; %d uncorroborated — now partly advanced by source-tags + cadence (F4)" % (total - corr_n)),
      ("4", "Persona determination",
       "Archetypes (%d) link <i>to</i> clinical personas (%d); applicability gates questions <i>by</i> persona" % (n_arch, n_clin),
       "gap", "No <i>input→persona</i> matrix (F2); %d determination weights" % persona_det_n),
      ("5", "User goals", "Goals referenced in prose; no structured catalogue",
       "gap", "Nothing for the UserGoals stream to bind to (F3)"),
      ("6", "Nudge (Doc 07/16)", "Actions catalogue + daily top-5 engine consume pillar/reservoir Δ",
       "ok", "— well specified upstream"),
      ("7", "Adherence check-ins", "%d adherence items" % adher_n,
       "gap", "Loop never closes (F1): completed/skipped nudges don't feed reservoirs (Doc 04)"),
    ]
    for num, stage, exists, kind, brk in rows:
        h.append('<tr><td class="small mono">%s</td><td><b>%s</b></td><td class="small">%s</td>'
                 '<td>%s</td><td class="small muted">%s</td></tr>'
                 % (num, _esc(stage), exists, _stat_chip(kind), brk))
    h.append('</tbody></table></div>')

    # ----- onboarding / first-run coverage (v4 front-of-loop pass) -----
    h.append('<h3 id="onboarding">Onboarding / first-run coverage <span class="small muted">· v4 · front-of-loop pass</span></h3>')
    h.append('<p class="small muted">Stage-1 Capture rated <b>ok</b> on question <i>breadth</i>, but does any doc own the first-run <b>experience</b>? '
             'Each dimension maps to where it is defined and whether an onboarding flow ties it together. '
             '<span class="b-green">ok</span> = covered &amp; owned · <span class="b-gold">warn</span> = exists but not anchored to intake · '
             '<span class="b-red">gap</span> = undocumented.</p>')
    h.append('<div class="tablewrap"><table><thead><tr><th>Onboarding dimension</th><th>Where defined / owner</th>'
             '<th>Read</th><th>Finding</th></tr></thead><tbody>')
    ob_rows = [
      ("First-run flow &amp; sequence", "— <i>no owning doc</i> (Doc 18 lists effortless surfaces, not an intake flow)", "gap", "F9"),
      ("Demographics — sex · gender · DOB", "Doc 01 §1.1 <code>Patient</code>; drives physiology (Doc 05)", "ok", "—"),
      ("Demographics — ethnicity · locale · occupation · language · household role", "implicit in Doc 01 (trailing <code>…</code>); ethnicity used in §4.2 + Doc 15", "warn", "F10"),
      ("Lifestyle / PRO question content", "Appx C (PROs) · Appx E (bank) · Appx F (axes)", "ok", "—"),
      ("Goals capture", "Appx J · Doc 18 §7", "ok", "F3"),
      ("Persona inference from intake", "Appx I (signal→persona matrix)", "ok", "F2"),
      ("Cold-start — marker priors", "Doc 01 §4.3 (median fallback) · Doc 14 (ignition)", "ok", "—"),
      ("Cold-start — question order / progressive profiling", "— <i>undocumented</i>", "gap", "F11"),
      ("Consent / privacy in first-run", "Doc 11 owns consent; not sequenced into intake", "warn", "F12"),
      ("Device / wearable pairing in first-run", "Doc 18 §2/§4 lists devices; not sequenced into intake", "warn", "F12"),
    ]
    for dim, where, kind, find in ob_rows:
        h.append('<tr><td><b>%s</b></td><td class="small">%s</td><td>%s</td><td class="small mono">%s</td></tr>'
                 % (dim, where, _stat_chip(kind), find))
    h.append('</tbody></table></div>')

    h.append('<h3 id="pillars">Pillar coverage — self-report reach &amp; wearable corroboration</h3>')
    h.append('<div class="tablewrap"><table><thead><tr><th>Pillar</th><th>Self-report reach</th>'
             '<th>Wearable-corroborated</th><th>Read</th></tr></thead><tbody>')
    for p in sorted(pillars, key=lambda x: -pillar_q[x]):
        q = pillar_q[p]; w = pillar_wear[p]; wfrac = (w / q) if q else 0
        reach_color = "var(--green)" if q >= 0.45 * pmax else ("var(--gold)" if q >= 0.2 * pmax else "var(--red)")
        wcol = "var(--green)" if wfrac >= 0.33 else ("var(--gold)" if wfrac >= 0.12 else "var(--red)")
        read = "thin (lab-led)" if q < 0.2 * pmax else ("heard, unverified" if wfrac < 0.12 else "closed")
        h.append('<tr><td><b>%s</b> <span class="small muted">%s</span></td><td style="min-width:150px">%s</td>'
                 '<td style="min-width:150px">%s</td><td class="small muted">%s</td></tr>'
                 % (_esc(p), _esc(_PNAME.get(p, "")), _audit_bar(q / pmax, reach_color, "%d Q" % q),
                    _audit_bar(wfrac, wcol, "%d%%" % round(wfrac * 100)), _esc(read)))
    h.append('</tbody></table></div>')

    h.append('<h3 id="reservoirs">Reservoir reach (MONIAC, Doc 04)</h3>')
    h.append('<div class="tablewrap"><table><thead><tr><th>Reservoir</th><th>Questions feeding it</th></tr></thead><tbody>')
    for rv in sorted(reservoirs, key=lambda x: -res_q[x]):
        c = res_q[rv]
        col = "var(--green)" if c >= 0.45 * rmax else ("var(--gold)" if c >= 0.15 * rmax else "var(--red)")
        h.append('<tr><td class="small mono">%s</td><td style="min-width:220px">%s</td></tr>'
                 % (_esc(rv.replace("_", " ")), _audit_bar(c / rmax, col, "%d" % c)))
    h.append('</tbody></table></div>')

    h.append('<h3 id="dimensions">Dimensional coverage — captured <i>and</i> matched?</h3>')
    h.append('<div class="tablewrap"><table><thead><tr><th>Axis</th><th>Questions</th>'
             '<th>Wearable-matched?</th></tr></thead><tbody>')
    for a in sorted(axis_q, key=lambda x: -axis_q[x]):
        wm = axis_wear[a]
        chip = ('<span class="chip b-green">yes (%d)</span>' % wm) if wm else '<span class="chip b-red">self-report only</span>'
        h.append('<tr><td><b>%s</b></td><td class="small mono">%d</td><td>%s</td></tr>' % (_esc(a), axis_q[a], chip))
    h.append('</tbody></table></div>')

    # ===== source tagging + fusion scenarios (v2) =====
    h.append('<h2 id="fusion">Per-pillar source tagging &amp; fusion scenarios <span class="small muted">· added 2026-06-19 · v2</span></h2>')
    h.append('<p class="small muted">Each pillar’s inputs are tagged by <b>channel</b>; where ≥2 channels cover the '
             'same construct, the value is fused by <b>accuracy</b>. Channel legend:</p>')
    h.append('<div class="tagrow" style="margin-bottom:8px">'
             + " ".join('<span class="chip %s">%s — %s</span>' % (c[1], _esc(c[0]), _esc(c[2])) for c in _CH.values())
             + '</div>')
    h.append('<div class="callout note"><div class="ct">Now typed (F7)</div>Every one of the <b>%d</b> markers now '
             'carries a <b>typed source channel</b> — <code>marker_channel()</code> resolves each to one of '
             '{biomarker-lab · wearable-clinical · wearable-consumer · wearable-inferential · self-report · derived}, '
             'shown as the <b>Channel</b> column in <a class="xref" href="appendix-biomarkers.html">Appendix A</a> '
             '(filterable on the <a class="xref" href="grid-biomarkers.html">grid</a>). The fusion rules below are '
             'therefore <b>programmatically enforceable</b>, not just illustrative.</div>'
             % source_typed)
    # master scenario rule table
    h.append('<h3 id="fusion-rules">Fusion rules (master) — accuracy hierarchy</h3>')
    h.append('<div class="tablewrap"><table><thead><tr><th>Scenario</th><th>Value rule</th><th>Confidence</th>'
             '<th>Dominant source</th><th>Guardrail</th></tr></thead><tbody>')
    for _id, _pred, name, rule, conf, dom, guard in _SCN:
        h.append('<tr><td class="small"><b>%s</b></td><td class="small">%s</td>'
                 '<td><span class="chip %s">%s</span></td><td class="small">%s</td>'
                 '<td class="small muted">%s</td></tr>'
                 % (_esc(name), _esc(rule), _CONF_COL.get(conf, "b-mut"), _esc(conf), _esc(dom), _esc(guard)))
    h.append('</tbody></table></div>')
    # per-pillar: channels present + reachable scenarios
    h.append('<h3 id="fusion-pillars">Per-pillar channels &amp; reachable scenarios</h3>')
    for p in pillars:
        src = _PILLAR_SRC.get(p, {})
        present = set(src.keys())
        chips = " ".join('<span class="chip %s">%s: %s</span>' % (_CH[ch][1], ch, _esc(", ".join(src[ch])))
                         for ch in ["L", "Wc", "Ww", "Wi", "S"] if ch in src)
        reach = [s for s in _SCN if s[1](present)]
        h.append('<div class="panel" style="margin:8px 0"><div style="margin-bottom:6px"><b>%s</b> '
                 '<span class="small muted">%s</span></div>' % (_esc(p), _esc(_PNAME.get(p, ""))))
        h.append('<div class="tagrow" style="margin-bottom:7px">%s</div>' % chips)
        h.append('<div class="tablewrap"><table><thead><tr><th>Reachable scenario</th><th>Dominant</th>'
                 '<th>Confidence</th></tr></thead><tbody>')
        for _id, _pred, name, rule, conf, dom, guard in reach:
            h.append('<tr><td class="small">%s</td><td class="small muted">%s</td>'
                     '<td><span class="chip %s">%s</span></td></tr>'
                     % (_esc(name), _esc(dom), _CONF_COL.get(conf, "b-mut"), _esc(conf)))
        h.append('</tbody></table></div></div>')

    # ===== wearable cadence × provider (v2) =====
    h.append('<h2 id="cadence">Wearable cadence × provider <span class="small muted">· added 2026-06-19 · v2 · illustrative</span></h2>')
    h.append('<div class="callout note"><div class="ct">Illustrative</div>Typical sampling / ingest cadence per '
             'metric and source. <b>Design targets, re-verify per SDK/device version</b> — Terra is an aggregator; '
             'HealthKit / Health Connect / Samsung Health are OS platforms; the rest are device brands. '
             '<code>*</code> = intraday depends on the source device. Metric calc behind each row → '
             '<a href="#calcs">calculation reference</a>.</div>')
    h.append('<div class="tablewrap"><table><thead><tr><th>Metric</th>'
             + "".join('<th class="small">%s</th>' % _esc(pr) for pr in _PROVIDERS)
             + '</tr></thead><tbody>')
    for met, cells in _CADENCE.items():
        link = ('<a href="#" onclick="auditPop(\'calc-%s\');return false">%s</a>' % (_slug_metric(met), _esc(met))
                if _slug_metric(met) in _CALC_KEYS else _esc(met))
        h.append('<tr><td class="small"><b>%s</b></td>%s</tr>'
                 % (link, "".join('<td class="small mono muted">%s</td>' % _esc(c) for c in cells)))
    h.append('</tbody></table></div>')
    # freshness SLA (closes F8)
    h.append('<h3 id="cadence-sla">Freshness SLA <span class="small muted">· per-metric contract (closes F8)</span></h3>')
    h.append('<div class="callout note"><div class="ct">Staleness contract</div>The per-metric freshness contract the '
             'cadence trace was missing: <b>fresh-within</b> (data newer than this is trusted at full weight), '
             '<b>stale-after</b> (older → the <a class="xref" href="states.html">Wearables → stale</a> state fires and '
             'Confidence decays, Doc 12 §4). Provider determines <i>how</i> a metric is delivered (matrix above); the '
             'SLA window is the <i>clinical</i> staleness bound, mostly metric-driven.</div>')
    h.append('<div class="tablewrap"><table><thead><tr><th>Metric</th><th>Expected cadence</th>'
             '<th>Fresh within</th><th>Stale after</th><th>Drives</th></tr></thead><tbody>')
    for met, (exp, fresh, stale, drives) in _CADENCE_SLA.items():
        h.append('<tr><td class="small"><b>%s</b></td><td class="small">%s</td><td class="mono small" style="color:var(--green)">%s</td>'
                 '<td class="mono small" style="color:var(--gold)">%s</td><td class="small muted">%s</td></tr>'
                 % (_esc(met), _esc(exp), _esc(fresh), _esc(stale), _esc(drives)))
    h.append('</tbody></table></div>')

    # ===== metric calculation reference (popups) =====
    h.append('<h2 id="calcs">Metric calculation reference <span class="small muted">· added 2026-06-19 · v2</span></h2>')
    h.append('<p class="small muted">Click a metric to open its calculation across four levels — '
             '<b>individual</b> reading, <b>aggregated</b> rollup, <b>derived</b> formula/composite, and '
             '<b>baseline</b> (personal-baseline method, Doc 03 §2b).</p>')
    h.append('<div class="tagrow">')
    for met in _CALCS:
        h.append('<button class="chip b-acc" style="cursor:pointer" onclick="auditPop(\'calc-%s\')">%s ▾</button>'
                 % (_slug_metric(met), _esc(met)))
    h.append('</div>')
    for met, (ind, agg, der, base) in _CALCS.items():
        pop_blocks.append('<div class="popsrc" id="pop-calc-%s"><h3 style="margin-top:0">%s — calculation</h3>'
                          '<table><tbody>'
                          '<tr><td class="small muted" style="width:96px">Individual</td><td class="small">%s</td></tr>'
                          '<tr><td class="small muted">Aggregated</td><td class="small">%s</td></tr>'
                          '<tr><td class="small muted">Derived</td><td class="small">%s</td></tr>'
                          '<tr><td class="small muted">Baseline</td><td class="small">%s</td></tr>'
                          '</tbody></table></div>'
                          % (_slug_metric(met), _esc(met), _esc(ind), _esc(agg), _esc(der), _esc(base)))

    # ===== comprehensive persona set =====
    h.append('<h2 id="personas">Comprehensive persona set <span class="small muted">· determination target (F6)</span></h2>')
    h.append('<p class="small muted">The canonical union the persona matrix resolves to — %d clinical personas '
             '(<a class="xref" href="appendix-personas.html">Appendix D</a>) × %d lifestyle archetypes '
             '(<a class="xref" href="appendix-lifestyles.html">Appendix F</a>) = up to %d frames before dedup.</p>'
             % (n_clin, n_arch, union_personas))
    h.append('<div class="tagrow" style="gap:6px">')
    for pid, pname, *rest in PERSONAS:
        h.append('<span class="chip b-teal">%s</span>' % _esc(pname))
    for a in pa.get("archetypes", []):
        h.append('<span class="chip b-mut">%s</span>' % _esc(a.get("name", a.get("key", ""))))
    h.append('</div>')

    # ===== spec sketches =====
    h.append('<h2 id="sketches">Spec sketches (ready-to-build)</h2>')
    h.append('<h3 id="sk-adherence">Adherence check-in micro-instrument</h3>')
    h.append('<pre class="code"><code>{\n'
             '  "ref": "ADH-014", "nudge_id": "NUD-sleep-winddown",\n'
             '  "stem": "Did you do tonight’s wind-down?", "cadence": "daily",\n'
             '  "responses": [\n'
             '    {"label":"Yes",      "adherence":1.0, "reservoirs":{"sleep_debt":-0.4}},\n'
             '    {"label":"Partly",   "adherence":0.5, "reservoirs":{"sleep_debt":-0.2}},\n'
             '    {"label":"No — too busy", "adherence":0.0, "reason":"time"},\n'
             '    {"label":"No — forgot",   "adherence":0.0, "reason":"salience"}\n'
             '  ],\n'
             '  "feeds": ["reservoir_inflow(Doc04)","engagement(Doc12)","feasibility(Doc07)"],\n'
             '  "corroborated_by": ["wearable: sleep onset time","HRV overnight"]\n}</code></pre>')
    h.append('<h3 id="sk-goals">User-goals catalogue (keyed by applicability)</h3>')
    h.append('<div class="tablewrap"><table><thead><tr><th>goal_id</th><th>Title</th><th>Pillar / axis</th>'
             '<th>Target metric → goal</th><th>Applies to (persona · life-stage · age · sex · condition)</th>'
             '<th>Modifiability</th></tr></thead><tbody>')
    for gid, t, pa_, tgt, app, mod in [
      ("GL-MET-01", "Lower fasting glucose", "MET / metabolic", "FPG 112 → &lt;100 mg/dL @12wk (CGM TIR↑)",
       "prediabetic, southasian, ramadan_dm · adult · 35–65 · any · pre-DM/T2D", "high"),
      ("GL-FIT-03", "Build aerobic base", "FIT / activity", "VO₂max p35 → p55 @16wk (wearable est.)",
       "desk_sedentary, weekend_warrior · adult · 18–60 · any · —", "high"),
      ("GL-SLP-02", "Stabilize sleep timing", "SLP / sleep", "Onset SD 95→&lt;45 min @8wk (wearable)",
       "shift_worker, traveler, new_parent · any · any · any · insomnia", "high"),
      ("GL-BCM-04", "Preserve bone &amp; muscle", "BCM / anthropometric", "Grip+ALMI↑; BMD hold @26wk (DEXA)",
       "menopause, frail_elderly · peri/post-meno, 60+ · F-led · osteopenia", "moderate"),
      ("GL-CV-07", "Hit ApoB target (med-led)", "CV / metabolic", "ApoB 124 → &lt;80 mg/dL (statin+diet)",
       "fh, southasian · adult · any · any · FH / high-Lp(a)", "low (genetic) → clinician"),
      ("GL-MCS-05", "Reduce perceived stress", "MCS / stress", "PSS-4 11 → &lt;6; HRV↑ @8wk",
       "stress_eater, caregiver_burnout · adult · any · any · —", "high")]:
        h.append('<tr><td class="small mono">%s</td><td class="small"><b>%s</b></td><td class="small">%s</td>'
                 '<td class="small">%s</td><td class="small muted">%s</td><td class="small">%s</td></tr>'
                 % (gid, _esc(t), _esc(pa_), tgt, _esc(app), _esc(mod)))
    h.append('</tbody></table></div>')
    h.append('<h3 id="sk-persona">Input→persona determination matrix (many-to-many)</h3>')
    sig_rows = [
      ("AUDIT-C ≥ 8", {"social_drinker_pro": "+3", "corporate_stress_eater": "+1"}),
      ("IPAQ high + steps &gt;10k/d", {"endurance_athlete": "+3", "weekend_warrior": "+1", "desk_sedentary": "−3"}),
      ("Fasting + on SGLT2i/metformin", {"ramadan_faster_t2d": "+3", "prediabetic_unaware": "+1"}),
      ("South-Asian + waist↑ + FH-Hx", {"south_asian_metabolic": "+3", "fh_genetic_risk": "+2"}),
      ("Night-shift ≥3×/wk + onset SD↑", {"shift_worker": "+3", "anxious_insomniac": "+1"}),
      ("Peri/post-meno + BMD↓", {"postmenopausal_bone": "+3", "frail_elderly": "+1"}),
      ("PSS-4↑ + caregiving hrs↑", {"caregiver_burnout": "+3", "corporate_stress_eater": "+1"}),
    ]
    cols = ["endurance_athlete", "desk_sedentary", "shift_worker", "south_asian_metabolic",
            "ramadan_faster_t2d", "postmenopausal_bone", "caregiver_burnout", "social_drinker_pro"]
    h.append('<div class="tablewrap"><table><thead><tr><th>Signal \\ persona</th>%s</tr></thead><tbody>'
             % "".join('<th class="small mono" style="writing-mode:vertical-rl;transform:rotate(180deg);white-space:nowrap">%s</th>' % _esc(c) for c in cols))
    for sig, weights in sig_rows:
        cells = ""
        for c in cols:
            v = weights.get(c, "")
            cls = "" if not v else ("color:var(--green)" if v.startswith("+") else "color:var(--red)")
            cells += '<td class="small mono" style="text-align:center;%s">%s</td>' % (cls, _esc(v))
        h.append('<tr><td class="small"><b>%s</b></td>%s</tr>' % (_esc(sig), cells))
    h.append('</tbody></table></div>')
    h.append('<p class="small muted">Persona posterior = softmax over column sums; sex/age/life-stage act as hard '
             'priors (zeroing impossible columns) before soft evidence is summed.</p>')

    # ===== popup modal + sources + JS =====
    h.append('<div id="amodal" class="amodal"><div class="amodal-card">'
             '<button class="amodal-x" onclick="auditClose()">×</button>'
             '<div id="amodal-body"></div></div></div>')
    h.append('<div style="display:none">' + "".join(pop_blocks) + '</div>')
    h.append("""<style>
      .amodal{display:none;position:fixed;inset:0;z-index:50;background:rgba(4,7,14,.72);
        align-items:flex-start;justify-content:center;padding:6vh 16px;overflow:auto}
      .amodal.on{display:flex}
      .amodal-card{position:relative;max-width:680px;width:100%;background:var(--panel,#10182a);
        border:1px solid var(--line,#26314c);border-radius:14px;padding:20px 22px;box-shadow:0 20px 60px rgba(0,0,0,.5)}
      .amodal-x{position:absolute;top:10px;right:12px;background:transparent;border:0;color:var(--mut);
        font-size:24px;line-height:1;cursor:pointer}
      .av-tab{opacity:.6}.av-tab.on{opacity:1;outline:1px solid var(--acc)}
    </style>
    <script>(function(){
      function $(s,r){return (r||document).querySelector(s);}
      window.auditPop=function(id){
        var src=document.getElementById('pop-'+id); if(!src)return;
        var body=document.getElementById('amodal-body'); body.innerHTML=src.innerHTML;
        var tabs=[].slice.call(body.querySelectorAll('.av-tab'));
        var bodies=[].slice.call(body.querySelectorAll('.av-body'));
        function show(v){bodies.forEach(function(b){b.style.display=(b.dataset.v===v)?'':'none';});
          tabs.forEach(function(t){t.classList.toggle('on',t.dataset.v===v);});}
        tabs.forEach(function(t){t.classList.toggle('on',t.dataset.v===String(tabs.length-1));
          t.addEventListener('click',function(){show(t.dataset.v);});});
        document.getElementById('amodal').classList.add('on');
      };
      window.auditClose=function(){document.getElementById('amodal').classList.remove('on');};
      document.getElementById('amodal').addEventListener('click',function(e){if(e.target.id==='amodal')auditClose();});
      document.addEventListener('keydown',function(e){if(e.key==='Escape')auditClose();});
    })();</script>""")

    h.append('<p class="small muted" style="margin-top:18px">Verdict: capture and scoring are strong and genuinely '
             'multi-dimensional; the loop opens at <b>persona determination</b>, <b>goals</b> and <b>adherence</b> '
             '(F1–F3). Source-fusion and cadence (v2) make the wearable-match stage auditable; closing the three open '
             'P0/P1 items turns a rich intake into a learning loop.</p>')
    return "Appendix G · Coverage audit", "".join(h)


# =================================================================== APPENDIX H — adherence (closes F1)
def _res_delta_chips(d):
    if not d:
        return '<span class="small muted">—</span>'
    out = []
    for k, v in d.items():
        good = (v < 0) if ("reserve" not in k) else (v > 0)
        cls = "b-green" if good else "b-red"
        out.append('<span class="chip %s mono">%s %+.2f</span>' % (cls, _esc(k.replace("_", " ")), v))
    return " ".join(out)

def build_adherence():
    d = _load("adherence.json"); m = d["meta"]; items = d["items"]
    h = ['<div class="crumbs"><a href="index.html">Home</a> › Questions &amp; intake › Adherence</div>',
         '<h1>Appendix H — Adherence Micro Check-ins <span class="small muted">· closes F1</span></h1>',
         '<p class="lead">%s</p>' % _esc(m["idea"]), ILLUS]
    h.append('<div class="diagram"><div class="dt">How an adherence answer closes the loop</div>'
             '<pre class="mermaid">flowchart LR\n'
             '  NUD["nudge (Doc 07/16)"] --> ASK["micro check-in (this appendix)"]\n'
             '  ASK --> R["response · adherence ∈ [0,1] + reason"]\n'
             '  R --> RES["reservoir inflow (Doc 04)"]\n  R --> ENG["engagement / Trajectory (Doc 12)"]\n'
             '  R --> FEAS["nudge feasibility (Doc 07 §3)"] --> NUD</pre></div>')
    # gating / timing / source model (how we avoid asking irrelevant questions)
    h.append('<div class="callout spec"><div class="ct">Activation — only relevant check-ins fire</div>%s</div>' % _esc(m.get("activation_model", "")))
    h.append('<div class="callout note"><div class="ct">Timing — captured at onboarding</div>%s</div>' % _esc(m.get("timing_model", "")))
    h.append('<div class="callout note"><div class="ct">Patient360 (EHR/EMR graph)</div>%s</div>' % _esc(m.get("patient360", "")))
    h.append('<div class="callout note"><div class="ct">Scoring</div>%s</div>' % _esc(m["scoring"]))
    h.append('<div class="tagrow" style="margin:8px 0"><span class="small muted">reason taxonomy:</span> '
             + " ".join('<span class="chip b-mut">%s</span>' % _esc(r) for r in m["reason_taxonomy"]) + '</div>')
    h.append('<p class="small muted">%s</p>' % _esc(m.get("coding", "")))

    by_type = {}
    for it in items:
        by_type.setdefault(it.get("type", "lifestyle"), []).append(it)
    n_life = len(by_type.get("lifestyle", [])); n_med = len(by_type.get("medication", [])); n_dis = len(by_type.get("disease-management", []))
    h.append('<p class="small muted">%d check-ins · %d lifestyle · %d medication (EHR-triggered) · %d disease-management. '
             'Source: <code>data/adherence.json</code>.</p>' % (len(items), n_life, n_med, n_dis))

    def _codes(trig):
        out = []
        if trig.get("atc"): out.append('<span class="chip b-mut mono">ATC %s</span>' % _esc(", ".join(trig["atc"])))
        if trig.get("icd10"): out.append('<span class="chip b-mut mono">ICD-10 %s</span>' % _esc(", ".join(trig["icd10"])))
        return "".join(out)

    def _applies(ap):
        bits = []
        if ap.get("conditions"): bits.append("conditions: " + ", ".join(ap["conditions"]))
        if ap.get("med_classes"): bits.append("meds: " + ", ".join(ap["med_classes"]))
        if ap.get("personas"): bits.append("personas: " + ", ".join(ap["personas"]))
        if ap.get("age_range"): bits.append("age %s–%s" % (ap["age_range"][0], ap["age_range"][1]))
        if ap.get("sex") and ap["sex"] != "any": bits.append("sex: " + ap["sex"])
        if ap.get("life_stage") and ap["life_stage"] != "any": bits.append("life-stage: " + ap["life_stage"])
        return " · ".join(bits)

    GROUPS = [
        ("lifestyle", "Lifestyle &amp; behavioural", "Gated by the active nudge / goal (persona-applicable) — fired on the nudge's cadence."),
        ("medication", "Medication adherence — EHR-triggered (Patient360)", "Auto-activated by a coded <code>ehr_trigger</code> from the EHR med list; the onboarding screener is the fallback when no record exists. We never ask what the chart already tells us."),
        ("disease-management", "Disease-management routines", "Condition-triggered self-management (monitoring, technique, device use)."),
    ]
    for tkey, tlabel, tdesc in GROUPS:
        group = by_type.get(tkey, [])
        if not group:
            continue
        h.append('<h2 id="grp-%s">%s <span class="small muted">· %d</span></h2>' % (tkey, tlabel, len(group)))
        h.append('<p class="small muted">%s</p>' % tdesc)
        for it in group:
            h.append('<div class="panel" id="%s" style="margin:10px 0">' % _esc(it["ref"]))
            uae = ' <span class="chip b-gold">UAE priority</span>' if it.get("uae_priority") else ""
            h.append('<h3 style="margin:0"><span class="mono" style="color:var(--acc)">%s</span> · %s%s</h3>'
                     % (_esc(it["ref"]), _esc(it["nudge_family"]), uae))
            chips = ['<span class="chip b-acc mono">%s</span>' % _esc(p) for p in it.get("pillars", [])]
            chips.append('<span class="chip b-mut">reservoir: %s</span>' % _esc(it.get("reservoir") or "—"))
            chips.append('<span class="chip b-mut">cadence: %s</span>' % _esc(it.get("cadence", "")))
            h.append('<div class="tagrow" style="margin:6px 0">%s</div>' % "".join(chips))
            act = it.get("activation")
            if act:
                trig = act.get("ehr_trigger", {})
                lbls = ", ".join(trig.get("labels", []))
                h.append('<p class="small"><b>Activates when</b> Patient360 shows <i>%s</i> &nbsp;%s</p>' % (_esc(lbls), _codes(trig)))
                if act.get("onboarding_fallback"):
                    h.append('<p class="small muted">↳ no-EHR fallback: %s</p>' % _esc(act["onboarding_fallback"]))
            tm = it.get("timing")
            if tm:
                seg = ["anchor: %s" % tm.get("anchor", "—")]
                if tm.get("schedule_from_onboarding"): seg.append("onboarding capture: " + tm["schedule_from_onboarding"])
                if tm.get("window"): seg.append("fire window: " + tm["window"])
                if tm.get("ema"): seg.append("EMA")
                h.append('<p class="small"><b>Timing</b> · %s</p>' % _esc(" · ".join(seg)))
            ap = it.get("applicability")
            if ap and _applies(ap):
                h.append('<p class="small muted">Applies to — %s</p>' % _esc(_applies(ap)))
            h.append('<p class="small"><b>Q:</b> <i>%s</i></p>' % _esc(it["stem"]))
            h.append('<div class="tablewrap"><table><thead><tr><th>Response</th><th>Adherence</th>'
                     '<th>Reservoir Δ</th><th>Reason</th></tr></thead><tbody>')
            for r in it["responses"]:
                flag = ' <span class="chip b-red">⚑ clinician</span>' if r.get("flag") == "clinician" else ""
                h.append('<tr><td class="small"><b>%s</b>%s</td><td class="mono small">%.2f</td>'
                         '<td>%s</td><td class="small muted">%s</td></tr>'
                         % (_esc(r["label"]), flag, r.get("adherence", 0), _res_delta_chips(r.get("reservoirs")),
                            _esc(r.get("reason", "—"))))
            h.append('</tbody></table></div>')
            if it.get("corroborated_by"):
                h.append('<p class="small muted">Corroborated by: %s</p>' % _esc(", ".join(it["corroborated_by"])))
            h.append('</div>')
    return "Appendix H · Adherence", "".join(h)

# =================================================================== APPENDIX I — persona matrix (closes F2)
def build_persona_matrix():
    d = _load("persona-matrix.json"); m = d["meta"]; personas = d["personas"]; signals = d["signals"]
    pname = {p["id"]: p["name"] for p in personas}
    _nclin = sum(1 for p in personas if p.get("type") == "clinical"); _narch = len(personas) - _nclin
    h = ['<div class="crumbs"><a href="index.html">Home</a> › Questions &amp; intake › Persona determination</div>',
         '<h1>Appendix I — Input → Persona Determination Matrix <span class="small muted">· closes F2</span></h1>',
         '<p class="lead">The matrix that lets the engine <b>infer</b> a persona from the answers, not just gate '
         'questions by one. %d personas (%d clinical + %d lifestyle archetypes) × %d signals.</p>'
         % (len(personas), _nclin, _narch, len(signals)), ILLUS]
    h.append('<div class="callout spec"><div class="ct">Method</div>%s</div>' % _esc(m["method"]))
    h.append('<div class="diagram"><div class="dt">Answers → persona posterior</div>'
             '<pre class="mermaid">flowchart LR\n'
             '  A["answers + derived flags + wearable/lab thresholds"] --> S["matched signals"]\n'
             '  P["hard priors: sex · age · life-stage · requires"] --> Z["zero impossible columns"]\n'
             '  S --> SUM["Σ signed weights per persona"] --> Z --> SM["softmax → posterior"]\n'
             '  SM --> FRAME["persona frame (Doc 05/12) · cohort · nudge tilt"]</pre></div>')
    h.append('<div class="callout note"><div class="ct">Worked example</div>A 52-y male, South-Asian, waist above '
             'the Asian cut-point, HbA1c 6.1%, &lt;5k steps/day → signals <code>south_asian_ancestry</code> (+3 '
             'south_asian_metabolic), <code>hba1c_pre</code> (+3 prediabetic), <code>waist_high_asian_cut</code> '
             '(+2), <code>sedentary_job_low_steps</code> (+3 desk_sedentary). Posterior peaks on '
             '<b>south_asian_metabolic / prediabetic</b>; pregnancy &amp; menopause columns are zeroed by sex/age priors.</div>')
    srccls = {"biomarker-lab": "b-green", "wearable-clinical": "b-green", "wearable-consumer": "b-acc",
              "self-report": "b-yellow", "derived": "b-mut"}
    h.append('<h2 id="signals">Signals → persona weights</h2>')
    h.append('<div class="tablewrap"><table><thead><tr><th>Signal</th><th>Source</th>'
             '<th>Persona evidence (signed weight)</th></tr></thead><tbody>')
    for s in signals:
        ws = sorted(s["weights"].items(), key=lambda kv: -kv[1])
        chips = " ".join('<span class="chip %s">%s %+d</span>'
                         % ("b-green" if v > 0 else "b-red", _esc(pname.get(k, k)), v) for k, v in ws)
        h.append('<tr><td class="small"><b>%s</b><br><span class="small muted mono">%s</span></td>'
                 '<td><span class="chip %s">%s</span></td><td>%s</td></tr>'
                 % (_esc(s["label"]), _esc(s["id"]), srccls.get(s.get("source"), "b-mut"),
                    _esc(s.get("source", "")), chips))
    h.append('</tbody></table></div>')
    h.append('<h2 id="personas">Persona index &amp; hard priors</h2>')
    h.append('<div class="tablewrap"><table><thead><tr><th>Persona</th><th>Type</th><th>Hard priors</th>'
             '<th>Top determining signals</th></tr></thead><tbody>')
    sig_for = {}
    for s in signals:
        for k, v in s["weights"].items():
            sig_for.setdefault(k, []).append((v, s["label"]))
    for p in personas:
        pr = p.get("priors", {})
        prtxt = ", ".join("%s: %s" % (k, v) for k, v in pr.items()) if pr else "—"
        tops = sorted(sig_for.get(p["id"], []), reverse=True)[:3]
        topt = ", ".join("%s (+%d)" % (lbl, w) for w, lbl in tops) if tops else "—"
        tcls = "b-teal" if p["type"] == "clinical" else "b-mut"
        h.append('<tr><td><b>%s</b> <span class="small muted mono">%s</span></td>'
                 '<td><span class="chip %s">%s</span></td><td class="small muted">%s</td>'
                 '<td class="small">%s</td></tr>'
                 % (_esc(p["name"]), _esc(p["id"]), tcls, _esc(p["type"]), _esc(prtxt), _esc(topt)))
    h.append('</tbody></table></div>')
    h.append('<p class="small muted">Source: <code>data/persona-matrix.json</code>. Targets reconcile the clinical '
             'personas (<a class="xref" href="appendix-personas.html">Appendix D</a>) and lifestyle archetypes '
             '(<a class="xref" href="appendix-lifestyles.html">Appendix F</a>) — the comprehensive set from '
             '<a class="xref" href="appendix-coverage-audit.html">Appendix G</a> (F6).</p>')
    return "Appendix I · Persona matrix", "".join(h)

# =================================================================== APPENDIX J — goals (closes F3)
def build_goals():
    d = _load("goals.json"); m = d["meta"]; goals = d["goals"]
    h = ['<div class="crumbs"><a href="index.html">Home</a> › Questions &amp; intake › Goals</div>',
         '<h1>Appendix J — User-Goals Catalogue <span class="small muted">· closes F3</span></h1>',
         '<p class="lead">%s</p>' % _esc(m["idea"]), ILLUS]
    h.append('<div class="tagrow" style="margin:6px 0"><span class="small muted">lifecycle:</span> '
             + " → ".join('<span class="chip b-mut">%s</span>' % _esc(s) for s in m["lifecycle"]) + '</div>')
    h.append('<p class="small muted">%d goals · ≥2 per pillar · each keyed by an applicability vector and a '
             'wearable/lab/PRO target. Source: <code>data/goals.json</code>. Drives the UserGoals lifecycle on the '
             '<a class="xref" href="states.html">state machine</a>; nudges from '
             '<a class="xref" href="appendix-adherence.html">Appendix H</a> / Doc 16.</p>' % len(goals))
    by_p = {}
    for g in goals:
        by_p.setdefault(g["pillar"], []).append(g)
    for pid in ["CV", "MET", "REN", "HEP", "INF", "HEM", "ENDO", "BCM", "NUT", "SLP", "FIT", "MCS"]:
        gs = by_p.get(pid, [])
        if not gs:
            continue
        h.append('<h2 id="%s">%s <span class="small muted">· %s · %d goals</span></h2>'
                 % (_esc(pid), _esc(_PNAME.get(pid, pid)), _esc(pid), len(gs)))
        h.append('<div class="tablewrap"><table><thead><tr><th>Goal</th><th>Target metric</th>'
                 '<th>Applies to</th><th>Modifiability</th><th>Nudges</th></tr></thead><tbody>')
        for g in gs:
            met = g["metric"]
            tgt = '%s → <b>%s</b> <span class="small muted">@%dwk · %s</span>' % (
                _esc(met.get("baseline_eg", "")), _esc(met.get("target", "")), met.get("horizon_wk", 0),
                _esc(met.get("source", "")))
            ap = g["applicability"]
            who = []
            if ap.get("personas"): who.append("personas: " + ", ".join(ap["personas"]))
            if ap.get("archetypes"): who.append("archetypes: " + ", ".join(ap["archetypes"]))
            seg = "age %s–%s · %s · %s" % (ap.get("age_range", [0, 120])[0], ap.get("age_range", [0, 120])[1],
                                            ap.get("sex", "any"), ap.get("life_stage", "any"))
            cond = (" · conditions: " + ", ".join(ap["conditions"])) if ap.get("conditions") else ""
            nud = " ".join('<span class="chip b-acc mono">%s</span>' % _esc(n) for n in g.get("linked_nudges", []))
            h.append('<tr id="%s"><td class="small"><span class="mono" style="color:var(--acc)">%s</span> <b>%s</b>'
                     '<br><span class="small muted">%s</span></td>'
                     '<td class="small">%s</td><td class="small muted">%s<br>%s%s</td>'
                     '<td class="small">%s</td><td>%s</td></tr>'
                     % (_esc(g["id"]), _esc(g["id"]), _esc(g["title"]), _esc(g.get("axis", "")), tgt,
                        _esc("; ".join(who)), _esc(seg), _esc(cond), _esc(g.get("modifiability", "")), nud))
        h.append('</tbody></table></div>')
    return "Appendix J · Goals", "".join(h)

# =================================================================== BEHEMOTH — uber class diagram (data-driven)
# Subject areas: (key, label, fill, stroke). Colour each area so subsystems are visible.
_BM_AREAS = [
 ("core",       "Core",                     "#5b21b6", "#a78bfa"),
 ("intake",     "Data & Streams",           "#1d4ed8", "#93c5fd"),
 ("markers",    "Markers & Bands",          "#0e7490", "#67e8f9"),
 ("scoring",    "Scoring Engine",           "#4338ca", "#a5b4fc"),
 ("context",    "Context (2.0)",            "#7e22ce", "#d8b4fe"),
 ("reservoir",  "Reservoirs",               "#15803d", "#86efac"),
 ("sexacute",   "Sex / Acute / Life-stage", "#b45309", "#fcd34d"),
 ("nudge",      "Nudges & Actions",         "#a16207", "#fde68a"),
 ("governance", "Governance & Validation",  "#b91c1c", "#fca5a5"),
 ("questions",  "Question Bank",            "#0891b2", "#22d3ee"),
 ("persona",    "Personas & Lifestyles",    "#be185d", "#f9a8d4"),
 ("infra",      "Tooling / Calc / UI",      "#475569", "#cbd5e1"),
]
_BM_AREA_NODE = {"core":"Core","intake":"DataStreams","markers":"Markers","scoring":"ScoringEngine",
 "context":"Context","reservoir":"Reservoirs","sexacute":"SexAcute","nudge":"Nudges",
 "governance":"Governance","questions":"Questions","persona":"Personas","infra":"Tooling"}

# (name, area, stereotype, [bare members])
_BM_CLASSES = [
 ("Patient","core","«core»",["id","age","sex","gender","ethnicity"]),
 ("InputStream","intake","«Doc01»",["flag","q_source"]),
 ("LabStream","intake","«LAB»",[]),
 ("WearableStream","intake","«WEAR»",[]),
 ("GoalStream","intake","«GOAL»",[]),
 ("LifeStream","intake","«LIFE»",[]),
 ("Measurement","intake","«Doc01»",["x_i","source","confidence","tier","timestamp"]),
 ("TrustTier","intake","«D22»",["tier","q_source"]),
 ("WearableMetric","intake","«AppxB»",["metric","layer","pillars","devices"]),
 ("Marker","markers","«Doc02»",["name","unit","tier","twoSided","w_i","critical","escalation","r_clin"]),
 ("Band","markers","«Doc02»",["L","U","Ly","Uy","Lr","Ur","optimum"]),
 ("Modifier","markers","«Doc02»",["name","effect"]),
 ("CohortStat","markers","«Doc01»",["q_i","g","phi"]),
 ("PersonalBaseline","markers","«Doc03_2b»",["mu","sigma","z_i","k_i","kappa_resp"]),
 ("ClinicalScore","markers","«Doc08»",["FINDRISC","ASCVD","KDIGO","FIB4","FRAX","PhenoAge"]),
 ("Pillar","scoring","«Doc03»",["code","R_mark","R_k","S_k","status","cov","W_k","gamma"]),
 ("ScoringConstants","scoring","«Doc03»",["phi","kappa_resp","gamma","delta","rho_k","R_crit","cap"]),
 ("PureScore","scoring","«Doc03_12»",["R_total","value","overall_status","delta","progressToBest"]),
 ("CriticalCascade","scoring","«Doc03_4»",["criticalMarkers","R_crit","cap40","escalation"]),
 ("Explanation","scoring","«Doc03_7»",["bindingConstraint","topContributors","weights"]),
 ("CompanionVector","context","«Doc12_4»",["Confidence","DataSufficiency","Criticality","Trajectory","EarlyWarning","Representativeness","Skew","Volatility","Modifiability","StressLoad"]),
 ("EarlyWarning","context","«Doc12_5»",["tier","D_M","syndromic","timeToThreshold"]),
 ("ManagedState","context","«Doc12_3»",["drugTargets","tag"]),
 ("ConfounderRule","context","«Doc12_3»",["confounds","c_conf"]),
 ("AgeFrame","context","«Doc12_3»",["ageBand","anchorRetained"]),
 ("ReservoirSystem","reservoir","«Doc04»",["deltaT","stateEq"]),
 ("Reservoir","reservoir","«Doc04»",["id","polarity","B_j","setpoint","B_max","lambda","B_tilde"]),
 ("InterferenceMatrix","reservoir","«Doc04»",["kappa_jl","hurwitz"]),
 ("Valve","reservoir","«Doc04_6»",["intervention","efficacy","latency"]),
 ("HormonalMilieu","sexacute","«Doc05»",["phase","rangeShift"]),
 ("AcuteEvent","sexacute","«Doc06»",["m_acute","hysteresis"]),
 ("CarePlan","sexacute","«Doc06»",["valveBundle","lifeStage","goals"]),
 ("PatientState","sexacute","«states»",["Baseline","AcuteMode"]),
 ("NudgeEngine","nudge","«Doc07»",["top5","safetyFilter","deltaScore","rank()"]),
 ("Action","nudge","«Doc16»",["id","condition","impact","ease"]),
 ("Adherence","nudge","«Doc07»",["completion","feedsInflow"]),
 ("Goal","nudge","«Doc18»",["stated","m_goal"]),
 ("Cohort","governance","«Doc09»",["stratum","shrinkage","calibration","fairness","drift"]),
 ("ValidationHarness","governance","«Doc13»",["discrimination","calibration","PPV","gates"]),
 ("EvidenceRegistry","governance","«Doc14»",["entry","provenance","ignition"]),
 ("Governance","governance","«Doc11»",["escalationTiers","crisisPathway","FMEA","consent","audit"]),
 ("ModelVersion","governance","«Doc11»",["version","configHash","bump"]),
 ("ActuarialLayer","governance","«Doc10»",["gated","firewall"]),
 ("UAELocalization","governance","«Doc15»",["ethnicityCutpoints","ramadan"]),
 ("QuestionBank","questions","«AppxE»",["meta"]),
 ("Question","questions","«AppxE»",["ref","seq","order","prev","next","priority","cadence","refresh","stream","type"]),
 ("Response","questions","«AppxE»",["label","direction","magnitude","pillars","reservoirs"]),
 ("Dependency","questions","«AppxE»",["showIf","skipIf","triggers","validatedBy","prerequisites","unlocks"]),
 ("Applicability","questions","«AppxE»",["sex","ageMin","ageMax","lifeStage","personas","gated"]),
 ("QuestionDomain","questions","«AppxE»",["code"]),
 ("Category","questions","«AppxE»",["theme"]),
 ("ValidatedPRO","questions","«AppxC»",["instrument","scoring","bands","cadence"]),
 ("Persona","persona","«abstract»",[]),
 ("ClinicalPersona","persona","«AppxD»",["key","age","meds","rep","weightMult","frame"]),
 ("LifestyleArchetype","persona","«AppxF»",["key","axisProfile","weightMult"]),
 ("LifestyleAxis","persona","«AppxF»",["id","levels","pillars","perceivedSignal","actualSignal"]),
 ("AxisVector","persona","«AppxF»",["levels"]),
 ("PerceivedVsActual","persona","«AppxF»",["perceived","actual","gap","strategy","companionEffect"]),
 ("WikiGenerator","infra","«gen»",["md_to_html()","page()","sidebar()"]),
 ("WikiContent","infra","«gen»",["PILLARS","INSTRUMENTS","PERSONAS","builders()"]),
 ("WikiAdmin","infra","«gen»",["ADMIN_PAGES"]),
 ("Calculator","infra","«impl»",["computes2_0()"]),
 ("DataArtifact","infra","«data»",["questionBank","personaAxes","evidence"]),
 ("AdminBoard","infra","«ui»",["labRanges","weights","lifestyle","personas"]),
 ("InteractiveDemo","infra","«ui»",["feedbackLoop","stateMachine"]),
]
_BM_RELATIONS = [
 ("Patient","*--","InputStream",""),("Patient","*--","PureScore",""),("Patient","*--","ReservoirSystem",""),
 ("Patient","-->","Persona",""),("Patient","-->","CarePlan",""),("Patient","-->","PatientState",""),
 ("Patient","o--","Measurement",""),
 ("InputStream","<|--","LabStream",""),("InputStream","<|--","WearableStream",""),
 ("InputStream","<|--","GoalStream",""),("InputStream","<|--","LifeStream",""),
 ("WearableStream","o--","WearableMetric",""),("WearableMetric","-->","TrustTier",""),
 ("WearableMetric","-->","Pillar",""),("Measurement","-->","TrustTier",""),
 ("Measurement","..>","Marker","feeds x_i"),("GoalStream","-->","Goal",""),("LifeStream","-->","QuestionBank",""),
 ("Marker","-->","Band",""),("Marker","-->","Modifier",""),("Marker","-->","PersonalBaseline",""),
 ("Marker","-->","CohortStat",""),("Marker","..>","ManagedState",""),("Marker","..>","ConfounderRule",""),
 ("HormonalMilieu","-->","Band",""),("AgeFrame","-->","Band",""),("UAELocalization","-->","Band",""),
 ("EvidenceRegistry","-->","Band","provenance"),
 ("Pillar","o--","Marker",""),("Pillar","-->","ScoringConstants",""),
 ("PureScore","o--","Pillar",""),("PureScore","-->","ScoringConstants",""),("PureScore","-->","CriticalCascade",""),
 ("PureScore","-->","CompanionVector",""),("PureScore","-->","Explanation",""),
 ("CriticalCascade","-->","Governance","escalates"),("CompanionVector","-->","EarlyWarning",""),
 ("CompanionVector","..>","Reservoir","StressLoad reads ALLO"),("ClinicalScore","..>","Pillar","feeds max"),
 ("PersonalBaseline","-->","CompanionVector","z drives"),("PersonalBaseline","..>","PureScore","Stage2b"),
 ("ReservoirSystem","o--","Reservoir",""),("ReservoirSystem","-->","InterferenceMatrix",""),
 ("Reservoir","-->","Pillar","B_tilde feeds"),("Reservoir","<--","Valve",""),
 ("Valve","<..","Action",""),("Valve","<..","NudgeEngine",""),
 ("AcuteEvent","-->","Pillar","m_acute"),("AcuteEvent","-->","PatientState",""),("AcuteEvent","..>","ReservoirSystem",""),
 ("CarePlan","o--","Valve",""),("CarePlan","-->","Goal",""),("Goal","-->","Pillar","m_goal"),
 ("NudgeEngine","-->","Action","top5"),("NudgeEngine","..>","PureScore","deltaScore"),
 ("Action","-->","Adherence",""),("Adherence","-->","Reservoir","inflow"),
 ("Cohort","-->","CohortStat",""),("Cohort","-->","PersonalBaseline","prior"),
 ("ValidationHarness","..>","ModelVersion","gates"),("Governance","o--","ModelVersion",""),
 ("ActuarialLayer","..>","PureScore","gated"),
 ("QuestionBank","o--","Question",""),("Question","o--","Response",""),("Question","-->","Dependency",""),
 ("Question","-->","Applicability",""),("Question","-->","QuestionDomain",""),("Question","-->","Category",""),
 ("Question","..>","ValidatedPRO","validatedBy"),("Response","-->","Pillar","weights"),
 ("Response","-->","Reservoir","weights"),("Applicability","-->","ClinicalPersona",""),
 ("Dependency","..>","Question","prereq"),
 ("Persona","<|--","ClinicalPersona",""),("Persona","<|--","LifestyleArchetype",""),
 ("LifestyleArchetype","-->","ClinicalPersona","links"),("ClinicalPersona","-->","AxisVector",""),
 ("LifestyleArchetype","-->","AxisVector",""),("AxisVector","o--","LifestyleAxis",""),
 ("PerceivedVsActual","-->","AxisVector",""),("PerceivedVsActual","..>","NudgeEngine","strategy"),
 ("LifestyleAxis","-->","Pillar",""),("ClinicalPersona","-->","Pillar","weightMult"),
 ("QuestionDomain","..>","Pillar","routesTo"),("Reservoir","..>","Pillar","feeds"),
 ("Category","..>","Question","groups"),
 ("WikiGenerator","..>","DataArtifact","reads"),("WikiGenerator","..>","WikiContent",""),
 ("WikiGenerator","..>","WikiAdmin",""),("WikiContent","..>","QuestionBank","renders"),
 ("WikiContent","..>","Pillar","PILLARS"),("WikiAdmin","..>","AdminBoard",""),
 ("Calculator","..>","PureScore","computes"),("Calculator","..>","CompanionVector",""),
 ("DataArtifact","..>","QuestionBank",""),("DataArtifact","..>","PerceivedVsActual",""),
 ("InteractiveDemo","..>","PureScore","demoLoop"),("AdminBoard","..>","ScoringConstants","configures"),
 ("AdminBoard","..>","Band","labRanges"),("AdminBoard","..>","ClinicalPersona",""),
]
_BM_EXPECT = {"reservoirs":15,"pillars":12,"domains":12,"axes":12,"clin":13,"arch":24}

def _bm_counts():
    try: m = _load("question-bank.json").get("meta", {})
    except Exception: m = {}
    try: pa = _load("persona-axes.json")
    except Exception: pa = {}
    return {"reservoirs": len(m.get("reservoirs", [])), "questions": m.get("total_questions", 0),
            "domains": len(m.get("domain_codes", [])), "pillars": len(PILLAR_W),
            "clin": len(PERSONAS), "arch": len(pa.get("archetypes", [])), "axes": len(pa.get("axes", []))}

def _bm_drift(c):
    return ["%s: baseline %d -> data now %s" % (k, v, c.get(k)) for k, v in _BM_EXPECT.items() if c.get(k) != v]

def _bm_classdefs():
    return ["classDef %s fill:%s,stroke:%s,color:#fff,stroke-width:1px;" % (k, f, s) for k, _l, f, s in _BM_AREAS]

def _bm_diagram(level, c):
    extra = {"Reservoir": ["catalogue %d" % c["reservoirs"]], "QuestionBank": ["total %d" % c["questions"]],
             "QuestionDomain": ["count %d" % c["domains"]], "Pillar": ["count %d" % c["pillars"]],
             "ClinicalPersona": ["count %d" % c["clin"]], "LifestyleArchetype": ["count %d" % c["arch"]],
             "LifestyleAxis": ["count %d" % c["axes"]]}
    L = ["classDiagram", "direction LR"] + _bm_classdefs()
    area_of = {cl[0]: cl[1] for cl in _BM_CLASSES}
    if level == "areas":
        for k, _l, _f, _s in _BM_AREAS:
            L.append("class %s" % _BM_AREA_NODE[k]); L.append('cssClass "%s" %s' % (_BM_AREA_NODE[k], k))
        seen = set()
        for s, _op, d, _lab in _BM_RELATIONS:
            a, b = area_of.get(s), area_of.get(d)
            if not a or not b or a == b or (a, b) in seen: continue
            seen.add((a, b)); L.append("%s --> %s" % (_BM_AREA_NODE[a], _BM_AREA_NODE[b]))
        return "\n".join(L)
    by_area = {}
    for name, area, stereo, members in _BM_CLASSES:
        by_area.setdefault(area, []).append(name)
        if level == "fields":
            L.append("class %s {" % name); L.append(stereo)
            for mm in members + extra.get(name, []): L.append("+%s" % mm)
            L.append("}")
        else:
            L.append("class %s" % name)
    for k, _l, _f, _s in _BM_AREAS:
        names = by_area.get(k, [])
        if names: L.append('cssClass "%s" %s' % (",".join(names), k))
    for s, op, d, lab in _BM_RELATIONS:
        L.append("%s %s %s%s" % (s, op, d, (" : " + lab) if lab else ""))
    return "\n".join(L)

_BM_JS = """<style>
#bm-frame:fullscreen{height:100vh!important;width:100vw!important;border-radius:0}
.bm-tool button{cursor:pointer;border:1px solid var(--line,#334);background:transparent;color:inherit;border-radius:7px;padding:4px 9px}
.bm-tool button.on{background:var(--acc,#6cf);color:#04121f;border-color:var(--acc,#6cf);font-weight:600}
#bm-legend span[data-area]{cursor:pointer;border-radius:7px;padding:3px 9px;font-size:12px}
</style>
<script type="module">
import mermaid from "https://cdn.jsdelivr.net/npm/mermaid@11/dist/mermaid.esm.min.mjs";
mermaid.initialize({startOnLoad:false,theme:"dark",securityLevel:"loose",flowchart:{htmlLabels:true,curve:"basis"},themeVariables:{fontSize:"13px"}});
(function(){
 var frame=document.getElementById("bm-frame"),stage=document.getElementById("bm-stage"),hud=document.getElementById("bm-hud");
 if(!frame||!stage)return;
 var srcs={fields:document.getElementById("bm-src-fields").textContent,
           names:document.getElementById("bm-src-names").textContent,
           areas:document.getElementById("bm-src-areas").textContent};
 var level=localStorage.getItem("bmLevel")||"fields",z=1,tx=0,ty=0,area=null,n=0,natW=0,natH=0;
 function clamp(v,a,b){return Math.max(a,Math.min(b,v));}
 function apply(){stage.style.transform="translate("+tx+"px,"+ty+"px) scale("+z+")";if(hud)hud.textContent=Math.round(z*100)+"%";}
 var AREAS=window.BM_AREA_COLORS||{};
 function nodeArea(g){for(var k in AREAS){if(g.classList.contains(k))return k;}return null;}
 function colorize(){var s=stage.querySelector("svg");if(!s)return;
   s.querySelectorAll("g.node").forEach(function(g){var k=nodeArea(g);if(!k)return;var col=AREAS[k];
     g.querySelectorAll("path,rect,polygon").forEach(function(sh){
       var f=sh.getAttribute("fill")||getComputedStyle(sh).fill;
       if(f&&f!=="none"&&f!=="rgba(0, 0, 0, 0)"){sh.style.fill=col;}});
     g.querySelectorAll("text,tspan,.nodeLabel").forEach(function(t){t.style.fill="#fff";});});}
 function wireNav(){if(level==="areas")return;var s=stage.querySelector("svg");if(!s)return;
   s.querySelectorAll("g.node").forEach(function(g){var m=(g.id||"").match(/classId-(.+?)-\\d+$/);if(!m)return;
     g.style.cursor="pointer";g.setAttribute("title","double-click → "+m[1]+" dossier");});}
 function nodeName(t){var g=t&&t.closest&&t.closest("g.node");if(!g)return null;var m=(g.id||"").match(/classId-(.+?)-\\d+$/);return m?m[1]:null;}
 frame.addEventListener("dblclick",function(e){if(level==="areas")return;var nm=nodeName(e.target);if(nm){e.preventDefault();location.href="class-explorer.html#"+nm;}});
 function highlight(){var s=stage.querySelector("svg");if(!s)return;
   s.querySelectorAll("g.node").forEach(function(g){g.style.opacity=(!area||g.classList.contains(area))?"1":"0.18";});}
 function fit(){if(!natW||!natH)return;var fw=frame.clientWidth-24,fh=frame.clientHeight-24;
   z=clamp(Math.min(fw/natW,(fh-60)/natH),0.05,4);tx=Math.max(8,(frame.clientWidth-natW*z)/2);ty=66;apply();}
 function setActive(){["fields","names","areas"].forEach(function(k){var b=document.getElementById("bm-"+k);if(b)b.classList.toggle("on",k===level);});}
 function render(){mermaid.render("bmG"+(++n),srcs[level]).then(function(r){stage.innerHTML=r.svg;
     var s=stage.querySelector("svg");
     if(s){var vb=(s.viewBox&&s.viewBox.baseVal)?s.viewBox.baseVal:null;
       if(vb&&vb.width){natW=vb.width;natH=vb.height;}
       else{try{var bb=s.getBBox();natW=bb.width;natH=bb.height;}catch(e){natW=1400;natH=900;}}
       s.setAttribute("width",natW);s.setAttribute("height",natH);
       s.style.maxWidth="none";s.style.width=natW+"px";s.style.height=natH+"px";}
     setActive();fit();colorize();wireNav();highlight();}).catch(function(e){stage.innerHTML='<pre style="color:#ff8a8a;padding:14px;white-space:pre-wrap">'+(e&&e.message||e)+'</pre>';});}
 ["fields","names","areas"].forEach(function(k){var b=document.getElementById("bm-"+k);if(b)b.addEventListener("click",function(){level=k;localStorage.setItem("bmLevel",k);render();});});
 function zoomAt(cx,cy,f){var nz=clamp(z*f,0.08,6);tx=cx-((cx-tx)/z)*nz;ty=cy-((cy-ty)/z)*nz;z=nz;apply();}
 var byId=function(i){return document.getElementById(i);};
 if(byId("bm-zin"))byId("bm-zin").addEventListener("click",function(){zoomAt(frame.clientWidth/2,frame.clientHeight/2,1.2);});
 if(byId("bm-zout"))byId("bm-zout").addEventListener("click",function(){zoomAt(frame.clientWidth/2,frame.clientHeight/2,1/1.2);});
 if(byId("bm-reset"))byId("bm-reset").addEventListener("click",function(){z=1;tx=0;ty=0;apply();});
 if(byId("bm-fit"))byId("bm-fit").addEventListener("click",fit);
 if(byId("bm-full"))byId("bm-full").addEventListener("click",function(){try{if(!document.fullscreenElement){if(frame.requestFullscreen){var pr=frame.requestFullscreen();if(pr&&pr.catch)pr.catch(function(){});}}else if(document.exitFullscreen){document.exitFullscreen();}}catch(_){}});
 document.addEventListener("fullscreenchange",function(){setTimeout(fit,90);});
 frame.addEventListener("wheel",function(e){e.preventDefault();var r=frame.getBoundingClientRect();zoomAt(e.clientX-r.left,e.clientY-r.top,e.deltaY<0?1.1:1/1.1);},{passive:false});
 var gx=0,gy=0,sx=0,sy=0,pid=null,drag=false,moved=false;
 frame.addEventListener("pointerdown",function(e){if(e.target.closest&&e.target.closest("#bm-overlay"))return;drag=true;moved=false;pid=e.pointerId;sx=e.clientX;sy=e.clientY;gx=e.clientX-tx;gy=e.clientY-ty;});
 frame.addEventListener("pointermove",function(e){if(!drag)return;if(!moved&&Math.abs(e.clientX-sx)+Math.abs(e.clientY-sy)>4){moved=true;try{frame.setPointerCapture(pid);}catch(_){}frame.style.cursor="grabbing";}if(moved){tx=e.clientX-gx;ty=e.clientY-gy;apply();}});
 frame.addEventListener("pointerup",function(){drag=false;moved=false;frame.style.cursor="grab";try{frame.releasePointerCapture(pid);}catch(_){}});
 document.querySelectorAll("#bm-legend [data-area]").forEach(function(el){el.addEventListener("click",function(){var k=el.getAttribute("data-area");area=(area===k)?null:k;
   document.querySelectorAll("#bm-legend [data-area]").forEach(function(x){x.style.outline=(x===el&&area)?"2px solid #fff":"none";});highlight();});});
 render();
})();
</script>"""

def build_behemoth():
    c = _bm_counts(); warns = _bm_drift(c)
    if warns:
        try: print("  [behemoth] drift vs baseline: " + "; ".join(warns))
        except Exception: pass
    h = ['<div class="crumbs"><a href="index.html">Home</a> &rsaquo; Diagrams & system maps &rsaquo; Behemoth class diagram</div>',
         '<h1>Behemoth Class Diagram</h1>',
         '<p class="lead">A single uber class diagram mapping <b>every concept across <code>docs/</code></b> &mdash; '
         'scoring (<a class="xref" href="03-scoring-formula.html">Doc 03</a>), reservoirs '
         '(<a class="xref" href="04-moniac-reservoir-dynamics.html">Doc 04</a>), the 2.0 companion vector '
         '(<a class="xref" href="12-critical-review-and-purescore-2.0.html">Doc 12</a>), streams, sex/acute, '
         'nudges, governance, the question bank (<a class="xref" href="appendix-question-bank.html">Appx E</a>), '
         'lifestyles/personas (<a class="xref" href="appendix-lifestyles.html">Appx F</a>) and the tooling/'
         'calculator/UI. <b>%d classes</b> across <b>%d colour-coded subject areas</b>; live counts: '
         '%d reservoirs &middot; %d pillars &middot; %d question-domains &middot; %d questions &middot; %d clinical personas &middot; %d archetypes.</p>'
         % (len(_BM_CLASSES), len(_BM_AREAS), c["reservoirs"], c["pillars"], c["domains"], c["questions"], c["clin"], c["arch"])]
    toolbar = ('<div class="bm-tool" style="display:flex;gap:6px;flex-wrap:wrap;align-items:center;pointer-events:auto">'
               '<span class="small muted">detail:</span>'
               '<button id="bm-fields">Fields</button><button id="bm-names">Names</button><button id="bm-areas">Areas</button>'
               '<span style="width:10px"></span><span class="small muted">view:</span>'
               '<button id="bm-zout">&minus;</button><button id="bm-reset">reset</button><button id="bm-fit">fit</button>'
               '<button id="bm-zin">&plus;</button><button id="bm-full">&#9974; fullscreen</button>'
               '<span id="bm-hud" class="small muted mono" style="margin-left:6px">100%</span>'
               '<span class="small muted">wheel = zoom &middot; drag = pan &middot; <b>double-click a class &rarr; its dossier</b></span></div>')
    sfx = {"reservoir": " &middot;%d" % c["reservoirs"], "scoring": " &middot;%d pillars" % c["pillars"],
           "questions": " &middot;%dQ" % c["questions"], "persona": " &middot;%d+%d" % (c["clin"], c["arch"])}
    chips = ['<span data-area="%s" style="background:%s;color:#fff">%s%s</span>'
             % (k, f, _esc(lbl), sfx.get(k, "")) for k, lbl, f, s in _BM_AREAS]
    legend = ('<div id="bm-legend" class="tagrow" style="gap:6px;margin-top:6px;pointer-events:auto">'
              '<span class="small muted">areas (click to spotlight):</span>%s</div>' % "".join(chips))
    # controls live INSIDE the frame as a top overlay, so they survive fullscreen
    h.append('<div id="bm-frame" tabindex="0" style="position:relative;height:76vh;overflow:hidden;'
             'border:1px solid var(--line,#334);border-radius:10px;background:#0b0f17;cursor:grab">'
             '<div id="bm-overlay" style="position:absolute;top:0;left:0;right:0;z-index:4;padding:8px 10px;'
             'pointer-events:none;background:linear-gradient(180deg,rgba(11,15,23,.92),rgba(11,15,23,0))">%s%s</div>'
             '<div id="bm-stage" style="position:absolute;top:0;left:0;transform-origin:0 0"></div></div>'
             % (toolbar, legend))
    h.append('<p class="small muted" style="margin-top:8px">Generated from the <code>BEHEMOTH</code> spec in '
             '<code>wiki_content.py</code> (areas &middot; classes &middot; relations); the three views and colours regenerate on '
             'every <code>build_wiki.py</code> run and counts are pulled live from the data files, so the map stays '
             'in sync.</p>')
    # Illustrative + Reconciliation notes sit at the bottom of the page (below the diagram)
    h.append(ILLUS)
    h.append('<div class="callout note"><div class="ct">Reconciliations baked in</div>'
             '<b>Reservoirs</b> 15 canonical (Doc 04, incl. VBP/OXD/IRON). <b>Symbol</b> responsiveness cap = '
             '<code>kappa_resp</code> vs interference <code>kappa_jl</code>. <b>Scope</b> everything under <code>docs/</code>.</div>')
    if warns:
        h.append('<div class="callout safety"><div class="ct">Diagram drift</div>The data files moved away from the '
                 'diagram baseline &mdash; counts above auto-updated; review the spec: %s.</div>' % _esc("; ".join(warns)))
    for lv in ("fields", "names", "areas"):
        h.append('<script type="application/x-mermaid" id="bm-src-%s">%s</script>' % (lv, _bm_diagram(lv, c)))
    colors = "{" + ",".join('"%s":"%s"' % (k, f) for k, _l, f, _s in _BM_AREAS) + "}"
    h.append('<script>window.BM_AREA_COLORS=%s;</script>' % colors)
    h.append(_BM_JS)
    return "Behemoth Class Diagram", "".join(h)


# =================================================================== ENGINEERING DOSSIER (per-class explorer + 5 index pages)
def _dossier():
    return _load("dossier.json")

_CE_CSS = ('<style>#ce-list a{display:block;padding:2px 7px;border-radius:6px;text-decoration:none;color:inherit}'
           '#ce-list a:hover{background:rgba(255,255,255,.06)}#ce-list a.on{background:var(--acc,#6cf);color:#04121f}'
           '#ce-main .panel{margin:10px 0}</style>')

_CE_JS = """<script>
(function(){
 var el=document.getElementById("ce-data"); if(!el)return;
 var D=JSON.parse(el.textContent);
 var seqT={}; D.sequences.forEach(function(s){seqT[s.id]=s.title;});
 var apiT={}; D.api.forEach(function(a){apiT[a.id]=a;});
 var main=document.getElementById("ce-main"),listEl=document.getElementById("ce-list"),search=document.getElementById("ce-search");
 function esc(s){return (s==null?"":String(s)).replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;");}
 function chip(t,bg){return '<span class="chip" style="background:'+bg+';color:#fff">'+esc(t)+'</span>';}
 function rel(r,dir){var o=dir==='out'?r.to:r.from;return '<a href="#'+o+'">'+esc(o)+'</a>'+(r.label?' <span class="small muted">('+esc(r.label)+')</span>':'');}
 function render(name){
  var c=D.classes[name]; if(!c){main.innerHTML='<p class="small muted">Pick a class.</p>';return;}
  var h=[];
  h.push('<div class="tagrow" style="gap:8px;align-items:center"><h2 style="margin:0">'+esc(c.name)+'</h2>'+chip(c.areaLabel,c.areaColor)+'<span class="chip b-mut">«'+esc(c.doc)+'»</span><span class="chip '+(c.tier==='core'?'b-teal':'b-mut')+'">'+c.tier+'</span></div>');
  if(c.purpose) h.push('<p class="lead" style="margin:8px 0">'+esc(c.purpose)+'</p>');
  var ql=['<a class="chip b-acc" href="behemoth-class-diagram.html">← diagram</a>'];
  if(c.seqRefs.length) ql.push('<a class="chip b-acc" href="dossier-sequences.html">sequences ('+c.seqRefs.length+')</a>');
  if(c.apiRefs.length) ql.push('<a class="chip b-acc" href="dossier-api.html">API ('+c.apiRefs.length+')</a>');
  ql.push('<a class="chip b-acc" href="dossier-erd.html#'+name+'">data</a>');
  if(c.stories.length) ql.push('<a class="chip b-acc" href="dossier-stories.html#'+name+'">stories ('+c.stories.length+')</a>');
  ql.push('<a class="chip b-acc" href="dossier-c4.html">C4</a>');
  h.push('<div class="tagrow" style="gap:6px;margin:6px 0 14px">'+ql.join('')+'</div>');
  h.push('<div class="panel"><h3>Overview</h3>');
  if(c.fields.length) h.push('<p class="small"><b>Fields:</b> '+c.fields.map(esc).join(' · ')+'</p>');
  if(c.relationsOut.length) h.push('<p class="small"><b>Uses →</b> '+c.relationsOut.map(function(r){return rel(r,'out');}).join(', ')+'</p>');
  if(c.relationsIn.length) h.push('<p class="small"><b>← Used by</b> '+c.relationsIn.map(function(r){return rel(r,'in');}).join(', ')+'</p>');
  h.push('<p class="small muted"><b>C4:</b> '+esc(c.c4||'—')+(c.state?' &nbsp;·&nbsp; <b>State:</b> '+esc(c.state)+' (<a href="states.html">states</a>)':'')+'</p></div>');
  if(c.table){var t=c.table;h.push('<div class="panel"><h3>Data model — <span class="mono">'+esc(t.name)+'</span></h3><div class="tablewrap"><table><thead><tr><th>column</th><th>type</th><th>key</th><th>note</th></tr></thead><tbody>');
   t.columns.forEach(function(col){h.push('<tr><td class="mono small">'+esc(col.name)+'</td><td class="small">'+esc(col.type)+'</td><td class="small">'+esc(col.key||'')+'</td><td class="small muted">'+esc(col.note||'')+'</td></tr>');});
   h.push('</tbody></table></div>'+(t.indexes&&t.indexes.length?'<p class="small muted">indexes: '+t.indexes.map(esc).join(', ')+'</p>':'')+(t.notes?'<p class="small muted">'+esc(t.notes)+'</p>':'')+'</div>');}
  if(c.apiRefs.length){h.push('<div class="panel"><h3>API</h3><ul class="small">');
   c.apiRefs.forEach(function(id){var a=apiT[id];if(a)h.push('<li><a href="dossier-api.html#'+id+'"><span class="mono">'+a.method+' '+esc(a.path)+'</span></a> — '+esc(a.desc)+'</li>');});h.push('</ul></div>');}
  if(c.seqRefs.length){h.push('<div class="panel"><h3>Sequences</h3><ul class="small">');
   c.seqRefs.forEach(function(id){h.push('<li><a href="dossier-sequences.html#'+id+'">'+esc(seqT[id]||id)+'</a></li>');});h.push('</ul></div>');}
  if(c.stories.length){h.push('<div class="panel"><h3>User stories <span class="small muted">('+c.stories.length+')</span></h3>');
   c.stories.forEach(function(s){h.push('<div style="border-left:3px solid '+c.areaColor+';padding-left:10px;margin:10px 0">');
    h.push('<p class="small"><b class="mono">'+esc(s.id)+'</b> — As a <b>'+esc(s.role)+'</b>, I want '+esc(s.want)+' so that '+esc(s.soThat)+'.</p>');
    if(s.acceptance&&s.acceptance.length)h.push('<p class="small muted" style="margin:2px 0">Acceptance:</p><ul class="small">'+s.acceptance.map(function(a){return '<li>'+esc(a)+'</li>';}).join('')+'</ul>');
    if(s.edge&&s.edge.length)h.push('<p class="small" style="color:#ffb38a;margin:2px 0">Edge/failure: '+s.edge.map(esc).join(' · ')+'</p>');
    h.push('</div>');});h.push('</div>');}
  if(c.invariants.length)h.push('<div class="panel"><h3>Invariants &amp; safety</h3><ul class="small">'+c.invariants.map(function(i){return '<li>'+esc(i)+'</li>';}).join('')+'</ul></div>');
  if(c.decisions.length)h.push('<div class="panel"><h3>Decisions</h3><div class="tagrow">'+c.decisions.map(function(d){return '<a class="chip b-mut" href="decisions.html">'+esc(d)+'</a>';}).join('')+'</div></div>');
  main.innerHTML=h.join('');
  [].forEach.call(listEl.querySelectorAll('a'),function(a){a.classList.toggle('on',a.getAttribute('data-c')===name);});
 }
 var byArea={}; Object.keys(D.classes).forEach(function(k){var c=D.classes[k];(byArea[c.areaLabel]=byArea[c.areaLabel]||[]).push(c);});
 var lh=[]; Object.keys(byArea).forEach(function(al){lh.push('<div class="small muted" style="margin:8px 0 2px">'+esc(al)+'</div>');
  byArea[al].forEach(function(c){lh.push('<a data-c="'+c.name+'" href="#'+c.name+'"><span style="display:inline-block;width:8px;height:8px;border-radius:2px;background:'+c.areaColor+';margin-right:6px"></span>'+esc(c.name)+(c.tier==='core'?' <span style="color:#6cf">●</span>':'')+'</a>');});});
 listEl.innerHTML=lh.join('');
 function fromHash(){var n=decodeURIComponent((location.hash||'').replace('#',''));render(D.classes[n]?n:Object.keys(D.classes)[0]);}
 window.addEventListener('hashchange',fromHash);
 if(search)search.addEventListener('input',function(){var q=search.value.toLowerCase();[].forEach.call(listEl.querySelectorAll('a'),function(a){if(a.hasAttribute('data-c'))a.style.display=a.getAttribute('data-c').toLowerCase().indexOf(q)>-1?'block':'none';});});
 fromHash();
})();
</script>"""

def build_class_explorer():
    d = _dossier(); m = d["meta"]
    blob = json.dumps(d, ensure_ascii=False).replace("</", "<\\/")
    h = ['<div class="crumbs"><a href="index.html">Home</a> &rsaquo; Engineering &rsaquo; Class explorer</div>',
         '<h1>Class Explorer</h1>',
         '<p class="lead">The per-class engineering dossier for all <b>%d classes</b> (<b>%d</b> fully curated): '
         'purpose, fields &amp; relations, data-model table, API, sequences, user stories, invariants and decisions. '
         '<b>Double-click any node in the <a class="xref" href="behemoth-class-diagram.html">Behemoth diagram</a></b> '
         'to land on that class here. %d user stories &middot; %d endpoints &middot; %d sequences.</p>'
         % (m["total_classes"], m["core_classes"], sum(len(c["stories"]) for c in d["classes"].values()), m["api"], m["sequences"]),
         ILLUS, _CE_CSS,
         '<div style="display:flex;gap:14px;align-items:flex-start">'
         '<aside style="flex:0 0 230px;position:sticky;top:54px;max-height:84vh;overflow:auto">'
         '<input id="ce-search" type="search" placeholder="filter classes…" style="width:100%;padding:6px 9px;'
         'border-radius:8px;border:1px solid var(--line,#334);background:transparent;color:inherit;margin-bottom:6px">'
         '<div id="ce-list"></div></aside>'
         '<main id="ce-main" style="flex:1;min-width:0"></main></div>',
         '<script type="application/json" id="ce-data">%s</script>' % blob,
         _CE_JS]
    return "Class Explorer", "".join(h)

def build_sequences():
    d = _dossier()
    h = ['<div class="crumbs"><a href="index.html">Home</a> &rsaquo; Engineering &rsaquo; Sequences</div>',
         '<h1>Runtime Sequence Diagrams</h1>',
         '<p class="lead">The dynamic choreography behind the static model — how data and control actually flow at '
         'runtime. Each names the classes it touches (open them in the <a class="xref" href="class-explorer.html">Class '
         'Explorer</a>).</p>', ILLUS]
    for s in d["sequences"]:
        h.append('<div class="panel"><h3 id="%s">%s</h3>' % (_esc(s["id"]), _esc(s["title"])))
        h.append('<pre class="mermaid">%s</pre>' % s["mermaid"])
        links = ", ".join('<a href="class-explorer.html#%s">%s</a>' % (_esc(cn), _esc(cn)) for cn in s.get("classes", []))
        h.append('<p class="small muted">Classes: %s</p></div>' % links)
    return "Sequences", "".join(h)

def build_erd():
    d = _dossier()
    h = ['<div class="crumbs"><a href="index.html">Home</a> &rsaquo; Engineering &rsaquo; Data model</div>',
         '<h1>Data Model (ERD)</h1>',
         '<p class="lead">Production persistence sketch: one table per class (core classes hand-modelled; the rest '
         'auto-skeletoned from the spec). Time-series tables are partitioned; audit tables append-only. '
         '<span class="flag fl-life">LIFE</span>-stream and reservoir history are the main time-series stores.</p>', ILLUS]
    by_area = {}
    for cn, c in d["classes"].items(): by_area.setdefault((c["area"], c["areaLabel"], c["areaColor"]), []).append(c)
    for (ak, al, ac), cs in by_area.items():
        h.append('<h2 style="border-left:5px solid %s;padding-left:8px">%s</h2>' % (ac, _esc(al)))
        for c in sorted(cs, key=lambda x: (0 if x["tier"] == "core" else 1, x["name"])):
            t = c["table"]
            badge = '<span class="chip b-teal">core</span>' if c["tier"] == "core" else '<span class="chip b-mut">skeleton</span>'
            h.append('<div class="panel"><h3 id="%s"><span class="mono">%s</span> %s <span class="small muted">(%s)</span></h3>'
                     % (_esc(c["name"]), _esc(t["name"]), badge, _esc(c["name"])))
            h.append('<div class="tablewrap"><table><thead><tr><th>column</th><th>type</th><th>key</th><th>note</th></tr></thead><tbody>')
            for col in t["columns"]:
                h.append('<tr><td class="mono small">%s</td><td class="small">%s</td><td class="small">%s</td><td class="small muted">%s</td></tr>'
                         % (_esc(col["name"]), _esc(col["type"]), _esc(col.get("key", "")), _esc(col.get("note", ""))))
            h.append('</tbody></table></div>')
            if t.get("indexes"): h.append('<p class="small muted">indexes: %s</p>' % _esc(", ".join(t["indexes"])))
            if t.get("notes"): h.append('<p class="small muted">%s</p>' % _esc(t["notes"]))
            h.append('</div>')
    return "Data model (ERD)", "".join(h)

def build_c4():
    d = _dossier(); c4 = d["c4"]
    flow = ("flowchart LR\n"
            "  PA[\"Patient App\"] --> API[\"API Gateway\"]\n  CB[\"Doctor's Board\"] --> API\n"
            "  WH[\"Wearable webhooks\"] --> ING[\"Ingestion\"]\n  LF[\"Lab feed FHIR\"] --> ING\n  ING --> API\n"
            "  API --> SC[\"Scoring Engine\"]\n  API --> QS[\"Question Service\"]\n  API --> NS[\"Nudge Service\"]\n"
            "  SC --> RS[\"Reservoir Service\"]\n  SC --> REG[\"Model Registry\"]\n  SC --> DS[(\"Datastore\")]\n"
            "  RS --> DS\n  QS --> DS\n  NS --> DS\n  SC --> AUD[(\"Audit Store\")]\n"
            "  API -. consent .-> ACT[\"Actuarial firewalled\"]")
    h = ['<div class="crumbs"><a href="index.html">Home</a> &rsaquo; Engineering &rsaquo; C4 architecture</div>',
         '<h1>C4 Architecture (Container / Component)</h1>',
         '<p class="lead">How the domain model runs as a system: external actors, services (containers), their '
         'components, and the datastores. Each class maps to a component (see its Class Explorer page).</p>', ILLUS,
         '<div class="panel"><h3>Container diagram</h3><pre class="mermaid">%s</pre></div>' % flow,
         '<h2>Containers &amp; components</h2>']
    comps = c4.get("components", {})
    for ct in c4["containers"]:
        cc = comps.get(ct["id"], [])
        h.append('<div class="panel"><h3 id="%s">%s <span class="small muted">(%s)</span></h3>'
                 '<p class="small">%s</p>%s</div>'
                 % (_esc(ct["id"]), _esc(ct["name"]), _esc(ct["tech"]), _esc(ct["desc"]),
                    ('<div class="tagrow">' + "".join('<span class="chip b-mut">%s</span>' % _esc(x) for x in cc) + '</div>') if cc else ""))
    return "C4 architecture", "".join(h)

def build_api():
    d = _dossier()
    h = ['<div class="crumbs"><a href="index.html">Home</a> &rsaquo; Engineering &rsaquo; API</div>',
         '<h1>API Contracts</h1>',
         '<p class="lead">Illustrative service seams (re-verify before production): method, path, auth, idempotency, '
         'returns, errors, and the classes each endpoint touches.</p>', ILLUS,
         '<div class="tablewrap"><table><thead><tr><th>endpoint</th><th>auth</th><th>idem</th><th>summary</th></tr></thead><tbody>']
    for a in d["api"]:
        h.append('<tr><td class="small"><a href="#%s"><span class="mono">%s %s</span></a></td><td class="small">%s</td>'
                 '<td class="small">%s</td><td class="small muted">%s</td></tr>'
                 % (_esc(a["id"]), a["method"], _esc(a["path"]), _esc(a.get("auth", "")),
                    "yes" if a.get("idempotent") else "no", _esc(a["desc"])))
    h.append('</tbody></table></div>')
    for a in d["api"]:
        cls = ", ".join('<a href="class-explorer.html#%s">%s</a>' % (_esc(cn), _esc(cn)) for cn in a.get("classes", []))
        h.append('<div class="panel"><h3 id="%s"><span class="mono">%s %s</span></h3><p class="small">%s</p>'
                 '<p class="small muted">auth: %s &middot; idempotent: %s &middot; returns: <span class="mono">%s</span></p>'
                 '<p class="small" style="color:#ffb38a">errors: %s</p><p class="small muted">classes: %s</p></div>'
                 % (_esc(a["id"]), a["method"], _esc(a["path"]), _esc(a["desc"]), _esc(a.get("auth", "")),
                    "yes" if a.get("idempotent") else "no", _esc(a.get("returns", "")),
                    _esc(" · ".join(a.get("errors", []))), cls))
    return "API contracts", "".join(h)

def build_stories():
    d = _dossier()
    total = sum(len(c["stories"]) for c in d["classes"].values())
    h = ['<div class="crumbs"><a href="index.html">Home</a> &rsaquo; Engineering &rsaquo; User stories</div>',
         '<h1>User Stories &amp; Acceptance Criteria</h1>',
         '<p class="lead">The buildable backlog: <b>%d stories</b> across the curated core classes, with Gherkin '
         'acceptance criteria and edge/failure cases. Grouped by subject area; open any class in the '
         '<a class="xref" href="class-explorer.html">Class Explorer</a>.</p>' % total, ILLUS]
    by_area = {}
    for cn, c in d["classes"].items():
        if c["stories"]: by_area.setdefault((c["areaLabel"], c["areaColor"]), []).append(c)
    for (al, ac), cs in by_area.items():
        h.append('<h2 style="border-left:5px solid %s;padding-left:8px">%s</h2>' % (ac, _esc(al)))
        for c in cs:
            h.append('<div class="panel"><h3 id="%s">%s</h3>' % (_esc(c["name"]), _esc(c["name"])))
            for s in c["stories"]:
                h.append('<div style="border-left:3px solid %s;padding-left:10px;margin:10px 0">'
                         '<p class="small"><b class="mono">%s</b> &mdash; As a <b>%s</b>, I want %s so that %s.</p>'
                         % (ac, _esc(s.get("id", "")), _esc(s.get("role", "")), _esc(s.get("want", "")), _esc(s.get("soThat", ""))))
                if s.get("acceptance"):
                    h.append('<p class="small muted" style="margin:2px 0">Acceptance:</p><ul class="small">%s</ul>'
                             % "".join('<li>%s</li>' % _esc(a) for a in s["acceptance"]))
                if s.get("edge"):
                    h.append('<p class="small" style="color:#ffb38a;margin:2px 0">Edge/failure: %s</p>' % _esc(" · ".join(s["edge"])))
                h.append('</div>')
            h.append('</div>')
    return "User stories", "".join(h)


# =================================================================== SPREADSHEET GRIDS (reusable)
import re as _re_grid
def _gstrip(v): return _re_grid.sub("<[^>]+>", "", str(v)).strip()
def _gattr(v): return _esc(_gstrip(v)).replace('"', "&quot;")

_GRID_ASSETS = """<style>
/* grid pages use the full horizontal space (these overrides only ship on grid pages) */
.shell{max-width:none}
.main{max-width:none;padding-left:22px;padding-right:22px}
.dgwrap{margin:14px 0}
.dgbar{display:flex;gap:10px;align-items:center;flex-wrap:wrap;margin-bottom:8px}
.dgbar input.dgq{flex:1;min-width:200px;padding:7px 11px;border-radius:9px;border:1px solid var(--line);background:#0a1120;color:var(--ink)}
.dgbtn{cursor:pointer;border:1px solid var(--line);background:#0e1626;color:var(--ink);border-radius:8px;padding:6px 11px;font-size:12.5px;white-space:nowrap}
.dgbtn:hover{border-color:var(--acc);color:#fff}
.dgcount{font-size:12px;color:var(--mut);white-space:nowrap}
.dgwrap:fullscreen{background:var(--bg,#0a0e18);padding:16px}
/* all rows on one continuous page (no inner vertical scrollbar); the page scrolls */
.dgscroll{overflow:visible;border:1px solid var(--line);border-radius:12px}
.dgbar{position:sticky;top:53px;z-index:6;background:var(--bg,#0a0e18);padding:8px 0}
table.dgrid{border-collapse:separate;border-spacing:0;width:100%;font-size:12.5px}
table.dgrid th,table.dgrid td{padding:7px 10px;border-bottom:1px solid var(--line);text-align:left;vertical-align:top}
table.dgrid td{max-width:380px}
/* sticky headers stick to the page, just below the top bar (≈53px) + the filter bar */
table.dgrid thead th{position:sticky;top:100px;background:#0e1626;z-index:2;cursor:pointer;user-select:none;white-space:nowrap}
table.dgrid thead th:hover{background:#16223a}
table.dgrid thead tr.dgf th{position:sticky;top:134px;background:#0b1322;z-index:1;cursor:auto;padding:5px 7px}
/* fullscreen: the wrap is its own scroll context — pin headers to the top of it */
.dgwrap:fullscreen .dgscroll{overflow:auto;max-height:calc(100vh - 120px)}
.dgwrap:fullscreen .dgbar{top:0}
.dgwrap:fullscreen table.dgrid thead th{top:0}
.dgwrap:fullscreen table.dgrid thead tr.dgf th{top:34px}
table.dgrid thead tr.dgf input,table.dgrid thead tr.dgf select{width:100%;box-sizing:border-box;padding:4px 6px;border-radius:6px;border:1px solid var(--line);background:#0a1120;color:var(--ink);font-size:11.5px}
table.dgrid th .ar{opacity:.35;font-size:10px;margin-left:5px}
table.dgrid th.asc .ar,table.dgrid th.desc .ar{opacity:1;color:var(--acc)}
table.dgrid tbody tr:hover{background:#11192b}
table.dgrid td.num{text-align:right;font-variant-numeric:tabular-nums;white-space:nowrap}
</style>
<script>
(function(){
 function txt(el){var v=el.getAttribute("data-v");return (v!==null?v:el.textContent).trim();}
 document.querySelectorAll("table.dgrid").forEach(function(tbl){
   var gid=tbl.id, heads=[].slice.call(tbl.tHead.rows[0].cells);
   var fcells=tbl.tHead.rows[1]?[].slice.call(tbl.tHead.rows[1].cells):[];
   var body=tbl.tBodies[0], rows=[].slice.call(body.rows);
   var q=document.getElementById(gid+"-q"), count=document.getElementById(gid+"-count");
   var sortCol=-1,sortDir=0;
   function apply(){
     var gv=(q&&q.value||"").toLowerCase();
     var fv=fcells.map(function(c){var i=c.querySelector("input,select");return i?i.value.toLowerCase():"";});
     var shown=0;
     rows.forEach(function(r){
       var cells=r.cells, ok=true, i;
       if(gv){ ok=false; for(i=0;i<cells.length;i++){ if(txt(cells[i]).toLowerCase().indexOf(gv)>-1){ok=true;break;} } }
       if(ok){ for(var j=0;j<fv.length;j++){ if(fv[j]){ var cv=txt(cells[j]).toLowerCase(), sel=fcells[j].querySelector("select");
           if(sel){ if(cv!==fv[j]){ok=false;break;} } else if(cv.indexOf(fv[j])<0){ok=false;break;} } } }
       r.style.display=ok?"":"none"; if(ok)shown++;
     });
     if(count)count.textContent=shown+" of "+rows.length+" rows";
   }
   function sortBy(ci){
     var th=heads[ci], num=th.getAttribute("data-type")==="num";
     if(sortCol===ci){ sortDir = sortDir===1?-1:(sortDir===-1?0:1); } else { sortCol=ci; sortDir=1; }
     heads.forEach(function(h){h.classList.remove("asc","desc");var a=h.querySelector(".ar");if(a)a.textContent="↕";});
     if(sortDir!==0){ th.classList.add(sortDir===1?"asc":"desc"); var a=th.querySelector(".ar"); if(a)a.textContent=sortDir===1?"▲":"▼";
       var arr=rows.slice();
       arr.sort(function(a,b){ var x=txt(a.cells[ci]),y=txt(b.cells[ci]);
         if(num){ x=parseFloat(x.replace(/[^0-9.\\-]/g,""))||0; y=parseFloat(y.replace(/[^0-9.\\-]/g,""))||0; return (x-y)*sortDir; }
         return x.localeCompare(y,undefined,{numeric:true})*sortDir; });
       arr.forEach(function(r){body.appendChild(r);});
     } else { rows.forEach(function(r){body.appendChild(r);}); }
   }
   heads.forEach(function(th,ci){ th.addEventListener("click",function(){sortBy(ci);}); });
   fcells.forEach(function(c){ var i=c.querySelector("input,select"); if(i){i.addEventListener("input",apply);i.addEventListener("change",apply);} });
   if(q)q.addEventListener("input",apply);
   // download the current (filtered + sorted) view as an Excel-openable .xls
   function xesc(s){return String(s).replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;");}
   function exportXls(){
     var th="<tr>"; heads.forEach(function(h){th+="<th>"+xesc(h.textContent.replace(/[↕▲▼]/g,"").trim())+"</th>";}); th+="</tr>";
     var tb=""; [].slice.call(body.rows).forEach(function(r){ if(r.style.display==="none")return;
       var tr="<tr>"; for(var i=0;i<r.cells.length;i++){tr+="<td>"+xesc(txt(r.cells[i]))+"</td>";} tb+=tr+"</tr>"; });
     var doc='<html xmlns:x="urn:schemas-microsoft-com:office:excel"><head><meta charset="utf-8">'
       +'<style>table{border-collapse:collapse}td,th{border:1px solid #ccc;padding:3px 6px;mso-number-format:"\\\\@"}th{background:#eee;font-weight:bold}</style>'
       +'</head><body><table>'+th+tb+'</table></body></html>';
     var blob=new Blob(["\\ufeff"+doc],{type:"application/vnd.ms-excel"});
     var a=document.createElement("a"); a.href=URL.createObjectURL(blob); a.download=gid+".xls";
     document.body.appendChild(a); a.click(); setTimeout(function(){URL.revokeObjectURL(a.href);a.remove();},0);
   }
   var xb=document.getElementById(gid+"-xls"); if(xb)xb.addEventListener("click",exportXls);
   var fb=document.getElementById(gid+"-full"), wrap=tbl.closest(".dgwrap");
   if(fb&&wrap)fb.addEventListener("click",function(){ try{ if(!document.fullscreenElement){ if(wrap.requestFullscreen){var p=wrap.requestFullscreen();if(p&&p.catch)p.catch(function(){});} } else if(document.exitFullscreen){document.exitFullscreen();} }catch(_){}});
   apply();
 });
})();
</script>"""

def _grid(gid, columns, rows):
    for col in columns:
        if "filter" not in col:
            if col.get("type") == "num":
                col["filter"] = "text"
            else:
                vals = set(_gstrip(r.get(col["key"], "")) for r in rows)
                col["filter"] = "select" if (1 < len(vals) <= 16 and max((len(x) for x in vals), default=0) <= 26) else "text"
    h = ['<div class="dgwrap"><div class="dgbar">'
         '<input class="dgq" id="%s-q" type="search" placeholder="search all columns…">'
         '<span class="dgcount" id="%s-count"></span>'
         '<button class="dgbtn" id="%s-xls" type="button">&#11015; XLS</button>'
         '<button class="dgbtn" id="%s-full" type="button">&#9974; Fullscreen</button></div>'
         '<div class="dgscroll"><table class="dgrid" id="%s"><thead><tr>' % (gid, gid, gid, gid, gid)]
    for col in columns:
        h.append('<th data-type="%s">%s<span class="ar">↕</span></th>' % (col.get("type", "text"), _esc(col["label"])))
    h.append('</tr><tr class="dgf">')
    for col in columns:
        if col["filter"] == "select":
            vals = sorted(set(_gstrip(r.get(col["key"], "")) for r in rows if _gstrip(r.get(col["key"], "")) != ""))
            opts = '<option value="">all</option>' + "".join('<option value="%s">%s</option>' % (_gattr(v).lower(), _esc(v)) for v in vals)
            h.append('<th><select>%s</select></th>' % opts)
        elif col["filter"] == "none":
            h.append('<th></th>')
        else:
            h.append('<th><input type="text" placeholder="filter…"></th>')
    h.append('</tr></thead><tbody>')
    for r in rows:
        h.append('<tr>')
        for col in columns:
            val = r.get(col["key"], "")
            cls = ' class="num"' if col.get("type") == "num" else ''
            disp = val if (isinstance(val, str) and "<" in str(val)) else _esc(str(val))
            h.append('<td%s data-v="%s">%s</td>' % (cls, _gattr(val), disp))
        h.append('</tr>')
    h.append('</tbody></table></div></div>')
    return "".join(h)

def _grid_page(crumb, h1, lead, source_href, source_label, gridhtml, tab):
    h = ['<div class="crumbs"><a href="index.html">Home</a> › Spreadsheets › %s</div>' % _esc(crumb),
         '<h1>%s</h1>' % _esc(h1),
         '<p class="lead">%s <a class="xref" href="%s">%s ↗</a></p>' % (lead, source_href, _esc(source_label)),
         gridhtml, _GRID_ASSETS]
    return tab, "".join(h)

# ----- 1. biomarkers -----
def build_grid_biomarkers():
    rows = []
    for pid, pname, res, markers in PILLARS:
        for (mk, unit, tier, two, gr, ye, rd, w, src, crit) in markers:
            rows.append({"Pillar": pid, "Marker": mk, "Unit": unit, "Tier": tier, "Channel": marker_channel(mk, src),
                         "2-sided": "yes" if two else "—", "Green": gr, "Yellow": ye, "Red": rd,
                         "Weight": w, "Source": src, "Critical": "yes" if crit else "—"})
    cols = [{"key":"Pillar","label":"Pillar"},{"key":"Marker","label":"Marker","filter":"text"},{"key":"Unit","label":"Unit"},
            {"key":"Tier","label":"Tier"},{"key":"Channel","label":"Channel"},{"key":"2-sided","label":"2-sided"},
            {"key":"Green","label":"Green","filter":"text"},{"key":"Yellow","label":"Yellow","filter":"text"},
            {"key":"Red","label":"Red","filter":"text"},{"key":"Weight","label":"Weight","type":"num"},{"key":"Source","label":"Source"},
            {"key":"Critical","label":"Critical"}]
    return _grid_page("Biomarkers grid", "Biomarkers — spreadsheet", "Every marker across the 12 pillars, flat &amp; sortable. Source:",
                      "appendix-biomarkers.html", "Appendix A · Biomarkers", _grid("g-bio", cols, rows), "Biomarkers grid")

# ----- 2. wearables -----
def build_grid_wearables():
    rows = [{"Metric":m,"Layer":layer,"Trust tier":tier,"q_source":q,"Pillars":pil,"Devices":dev} for (m,layer,tier,q,pil,dev) in WEARABLES]
    cols = [{"key":"Metric","label":"Metric","filter":"text"},{"key":"Layer","label":"Layer"},{"key":"Trust tier","label":"Trust tier"},
            {"key":"q_source","label":"q_source"},{"key":"Pillars","label":"Pillars","filter":"text"},{"key":"Devices","label":"Devices","filter":"text"}]
    return _grid_page("Wearables grid", "Wearables — spreadsheet", "Every wearable metric, layer, trust tier &amp; pillars. Source:",
                      "appendix-wearables.html", "Appendix B · Wearables", _grid("g-wear", cols, rows), "Wearables grid")

# ----- 3. personas -----
def build_grid_personas():
    rows = [{"Id":pid,"Persona":name,"Age":age,"Meds":meds,"Coverage φ":cov,"Multipliers":mult,"Notes":notes}
            for (pid,name,age,meds,cov,mult,notes) in PERSONAS]
    cols = [{"key":"Id","label":"Id"},{"key":"Persona","label":"Persona","filter":"text"},{"key":"Age","label":"Age","type":"num"},
            {"key":"Meds","label":"Meds","filter":"text"},{"key":"Coverage φ","label":"Coverage φ","type":"num"},
            {"key":"Multipliers","label":"Pillar multipliers","filter":"text"},{"key":"Notes","label":"Notes","filter":"text"}]
    return _grid_page("Personas grid", "Clinical personas — spreadsheet", "The clinical persona frames, flat &amp; sortable. Source:",
                      "appendix-personas.html", "Appendix D · Personas", _grid("g-pers", cols, rows), "Personas grid")

# ----- 4. lifestyles (axes + archetypes) -----
def build_grid_lifestyles():
    pa = _load("persona-axes.json")
    axrows = [{"Axis":a["name"],"Id":a["id"],"Levels":" → ".join(a.get("levels",[])),"Pillars":", ".join(a.get("pillars",[])),
               "Perceived":", ".join(a.get("perceived_signal",[])),"Actual":", ".join(a.get("actual_signal",[]))} for a in pa.get("axes",[])]
    axcols = [{"key":"Axis","label":"Axis","filter":"text"},{"key":"Id","label":"Id"},{"key":"Levels","label":"Levels","filter":"text"},
              {"key":"Pillars","label":"Pillars","filter":"text"},{"key":"Perceived","label":"Perceived signal","filter":"text"},{"key":"Actual","label":"Actual signal","filter":"text"}]
    arrows = [{"Key":a["key"],"Name":a["name"],"Clinical persona":a.get("links_clinical_persona","—"),
               "Axis profile":" · ".join("%s:%s"%(k,v) for k,v in a.get("axis_profile",{}).items()),
               "Multipliers":a.get("weight_multipliers","—"),"Conditions":", ".join(a.get("typical_conditions",[])),"Frame":a.get("frame","")} for a in pa.get("archetypes",[])]
    arcols = [{"key":"Key","label":"Key"},{"key":"Name","label":"Name","filter":"text"},{"key":"Clinical persona","label":"Clinical persona"},
              {"key":"Axis profile","label":"Axis profile","filter":"text"},{"key":"Multipliers","label":"Weight multipliers","filter":"text"},
              {"key":"Conditions","label":"Typical conditions","filter":"text"},{"key":"Frame","label":"Frame","filter":"text"}]
    body = '<h2>Lifestyle axes</h2>' + _grid("g-lf-ax", axcols, axrows) + '<h2>Named archetypes</h2>' + _grid("g-lf-ar", arcols, arrows)
    return _grid_page("Lifestyles grid", "Lifestyles — spreadsheet", "Lifestyle axes and named archetypes, flat &amp; sortable. Source:",
                      "appendix-lifestyles.html", "Appendix F · Lifestyles", body, "Lifestyles grid")

# ----- 5. adherence -----
def build_grid_adherence():
    d = _load("adherence.json"); rows = []
    for it in d["items"]:
        resp = " / ".join(r.get("label","") for r in it.get("responses",[]))
        rows.append({"Ref":'<a href="appendix-adherence.html#%s">%s</a>'%(_esc(it["ref"]),_esc(it["ref"])),
                     "Nudge family":it.get("nudge_family",""),"Pillars":", ".join(it.get("pillars",[])),
                     "Reservoir":it.get("reservoir") or "—","Cadence":it.get("cadence",""),"Stem":it.get("stem",""),
                     "Responses":resp,"Corroborated by":", ".join(it.get("corroborated_by",[]))})
    cols = [{"key":"Ref","label":"Ref"},{"key":"Nudge family","label":"Nudge family","filter":"text"},{"key":"Pillars","label":"Pillars","filter":"text"},
            {"key":"Reservoir","label":"Reservoir"},{"key":"Cadence","label":"Cadence"},{"key":"Stem","label":"Stem","filter":"text"},
            {"key":"Responses","label":"Responses","filter":"text"},{"key":"Corroborated by","label":"Corroborated by","filter":"text"}]
    return _grid_page("Adherence grid", "Adherence check-ins — spreadsheet", "Every adherence micro check-in, flat &amp; sortable. Source:",
                      "appendix-adherence.html", "Appendix H · Adherence", _grid("g-adh", cols, rows), "Adherence grid")

# ----- 6. goals -----
def build_grid_goals():
    d = _load("goals.json"); rows = []
    for g in d["goals"]:
        m = g.get("metric",{}); ap = g.get("applicability",{}); ar = ap.get("age_range",[0,120])
        rows.append({"Goal":'<a href="appendix-goals.html#%s">%s</a>'%(_esc(g["id"]),_esc(g["id"])),"Title":g.get("title",""),
                     "Pillar":g.get("pillar",""),"Axis":g.get("axis",""),"Metric":m.get("name",""),
                     "Target":"%s → %s"%(m.get("baseline_eg",""),m.get("target","")),"Horizon (wk)":m.get("horizon_wk",""),"Source":m.get("source",""),
                     "Personas":", ".join(ap.get("personas",[])),"Archetypes":", ".join(ap.get("archetypes",[])),
                     "Age":"%s–%s"%(ar[0],ar[1]),"Sex":ap.get("sex","any"),"Life-stage":ap.get("life_stage","any"),
                     "Conditions":", ".join(ap.get("conditions",[])),"Modifiability":g.get("modifiability",""),"Nudges":", ".join(g.get("linked_nudges",[]))})
    cols = [{"key":"Goal","label":"Goal"},{"key":"Title","label":"Title","filter":"text"},{"key":"Pillar","label":"Pillar"},{"key":"Axis","label":"Axis"},
            {"key":"Metric","label":"Metric","filter":"text"},{"key":"Target","label":"Target","filter":"text"},{"key":"Horizon (wk)","label":"Horizon (wk)","type":"num"},
            {"key":"Source","label":"Source","filter":"text"},{"key":"Personas","label":"Personas","filter":"text"},{"key":"Archetypes","label":"Archetypes","filter":"text"},
            {"key":"Age","label":"Age"},{"key":"Sex","label":"Sex"},{"key":"Life-stage","label":"Life-stage"},{"key":"Conditions","label":"Conditions","filter":"text"},
            {"key":"Modifiability","label":"Modifiability","filter":"text"},{"key":"Nudges","label":"Nudges","filter":"text"}]
    return _grid_page("Goals grid", "User goals — spreadsheet", "Every goal keyed by applicability, flat &amp; sortable. Source:",
                      "appendix-goals.html", "Appendix J · Goals", _grid("g-goal", cols, rows), "Goals grid")

# ----- 7. persona-matrix (long: signal × persona) -----
def build_grid_persona_matrix():
    d = _load("persona-matrix.json"); pname = {p["id"]:p["name"] for p in d["personas"]}; ptype = {p["id"]:p.get("type","") for p in d["personas"]}
    rows = []
    for s in d["signals"]:
        for pid, w in s.get("weights",{}).items():
            rows.append({"Signal":s["label"],"Signal id":s["id"],"Source":s.get("source",""),
                         "Persona":pname.get(pid,pid),"Type":ptype.get(pid,""),"Weight":w})
    cols = [{"key":"Signal","label":"Signal","filter":"text"},{"key":"Signal id","label":"Signal id","filter":"text"},{"key":"Source","label":"Source"},
            {"key":"Persona","label":"Persona","filter":"text"},{"key":"Type","label":"Type"},{"key":"Weight","label":"Weight","type":"num"}]
    return _grid_page("Persona-matrix grid", "Persona determination — spreadsheet", "Every signal→persona weight in long form, sortable by weight. Source:",
                      "appendix-persona-matrix.html", "Appendix I · Persona matrix", _grid("g-pm", cols, rows), "Persona-matrix grid")

# ----- 8. question bank (one row per question) -----
def build_grid_questions():
    qb = _load("question-bank.json"); rows = []
    for q in qb["questions"]:
        ap = q.get("applicability",{}) or {}
        pil = sorted({k for r in q.get("responses",[]) for k in (r.get("pillars") or {})})
        ax = q.get("dimensions",{}).get("axis_tags",[])
        rows.append({"Ref":'<a href="appendix-question-bank.html#%s">%s</a>'%(_esc(q["ref"]),_esc(q["ref"])),
                     "Domain":q.get("domain","").replace("_"," ").title(),"Order":q.get("order",""),"Priority":q.get("priority",""),
                     "Category":q.get("category",""),"Type":q.get("type",""),"Cadence":q.get("cadence",""),
                     "Pillars":", ".join(pil),"Axes":", ".join(ax),"Sex":", ".join(ap.get("sex",["all"])),
                     "Age min":ap.get("age_min",0),"Age max":ap.get("age_max",120),"Personas":", ".join(ap.get("personas",["all"])),"Text":q.get("text","")})
    cols = [{"key":"Ref","label":"Ref"},{"key":"Domain","label":"Domain"},{"key":"Order","label":"Order","type":"num"},{"key":"Priority","label":"Priority","type":"num"},
            {"key":"Category","label":"Category","filter":"text"},{"key":"Type","label":"Type"},{"key":"Cadence","label":"Cadence"},
            {"key":"Pillars","label":"Pillars","filter":"text"},{"key":"Axes","label":"Axes","filter":"text"},{"key":"Sex","label":"Sex"},
            {"key":"Age min","label":"Age min","type":"num"},{"key":"Age max","label":"Age max","type":"num"},{"key":"Personas","label":"Personas","filter":"text"},
            {"key":"Text","label":"Question text","filter":"text"}]
    return _grid_page("Question-bank grid", "Question bank — spreadsheet", "All %d questions, one row each, sortable &amp; filterable. Source:" % len(qb["questions"]),
                      "appendix-question-bank.html", "Appendix E · Question bank", _grid("g-q", cols, rows), "Question-bank grid")

# ----- 9. ERD (long: class × column) -----
def build_grid_erd():
    d = _dossier(); rows = []
    for cn, c in d["classes"].items():
        t = c.get("table",{}) or {}
        for col in t.get("columns",[]):
            rows.append({"Area":c.get("areaLabel",""),"Class":c.get("name",""),"Tier":c.get("tier",""),"Table":t.get("name",""),
                         "Column":col.get("name",""),"Type":col.get("type",""),"Key":col.get("key",""),"Note":col.get("note","")})
    cols = [{"key":"Area","label":"Area"},{"key":"Class","label":"Class","filter":"text"},{"key":"Tier","label":"Tier"},{"key":"Table","label":"Table","filter":"text"},
            {"key":"Column","label":"Column","filter":"text"},{"key":"Type","label":"Type","filter":"text"},{"key":"Key","label":"Key"},{"key":"Note","label":"Note","filter":"text"}]
    return _grid_page("Data-model (ERD) grid", "Data model (ERD) — spreadsheet", "Every entity column across the data model, flat &amp; sortable. Source:",
                      "dossier-erd.html", "Data model (ERD)", _grid("g-erd", cols, rows), "Data-model grid")


# =================================================================== ONBOARDING & first-run (closes F9–F12)
def build_onboarding():
    d = _load("onboarding.json"); m = d["meta"]
    h = ['<div class="crumbs"><a href="index.html">Home</a> › Questions & intake › Onboarding & first-run</div>',
         '<h1>Appendix K — Onboarding &amp; First-Run Intake Flow</h1>',
         '<p class="lead">%s</p>' % _esc(m["idea"]), ILLUS]
    # flow diagram
    nodes = " --> ".join('%s["%s"]' % (s["id"], s["phase"]) for s in d["steps"])
    h.append('<div class="diagram"><div class="dt">First-run sequence — install → first score → progressive profiling</div>'
             '<pre class="mermaid">flowchart LR\n  %s</pre></div>' % nodes)
    h.append('<div class="callout safety"><div class="ct">Consent first</div>No PII or health data is captured before the '
             'privacy &amp; data-use consent step; device-pairing and EHR-connect gate the streams they unlock (Doc 11 · Doc 18 · D22). '
             'The actuarial layer is firewalled and off by default (Doc 10).</div>')
    # steps
    h.append('<h2 id="steps">First-run steps <span class="small muted">· <a class="xref" href="grid-onboarding.html">spreadsheet ↗</a></span></h2>')
    h.append('<div class="tablewrap"><table><thead><tr><th>#</th><th>Phase</th><th>Screen</th><th>Captures</th>'
             '<th>Gating</th><th>Why</th></tr></thead><tbody>')
    for s in d["steps"]:
        flags = []
        if s.get("consent"): flags.append('<span class="chip b-red">consent</span>')
        if s.get("pairs_device"): flags.append('<span class="chip b-acc">device</span>')
        gate = _esc(s.get("gated_until", "—")) + (" " + "".join(flags) if flags else "")
        h.append('<tr><td class="small mono">%d</td><td class="small"><b>%s</b></td><td class="small">%s</td>'
                 '<td class="small muted">%s</td><td class="small">%s</td><td class="small muted">%s</td></tr>'
                 % (s["order"], _esc(s["phase"]), _esc(s["screen"]), _esc(s.get("captures", "")), gate, _esc(s["why"])))
    h.append('</tbody></table></div>')
    # demographics (F10)
    h.append('<h2 id="demographics">Demographic field-set <span class="small muted">· closes F10</span></h2>')
    h.append('<p class="small muted">The full first-run demographic capture — what each field drives downstream.</p>')
    h.append('<div class="tablewrap"><table><thead><tr><th>Field</th><th>Type</th><th>Required</th><th>Drives</th><th>Source</th></tr></thead><tbody>')
    for f in d["demographics"]:
        h.append('<tr><td class="small mono">%s</td><td class="small">%s</td><td class="small">%s</td>'
                 '<td class="small muted">%s</td><td class="small muted">%s</td></tr>'
                 % (_esc(f["field"]), _esc(f["type"]), "✔" if f.get("required") else "—", _esc(f["drives"]), _esc(f["source"])))
    h.append('</tbody></table></div>')
    # cold-start (F11)
    cs = d["cold_start"]
    h.append('<h2 id="cold-start">Question-side cold-start &amp; progressive profiling <span class="small muted">· closes F11</span></h2>')
    h.append('<div class="callout note"><div class="ct">Rule</div>%s</div>' % _esc(cs["rule"]))
    h.append('<div class="tablewrap"><table><thead><tr><th>Bootstrap item</th><th>Pillar</th><th>Why it earns its place</th></tr></thead><tbody>')
    for it in cs["bootstrap_items"]:
        h.append('<tr><td class="small"><b>%s</b></td><td class="small mono">%s</td><td class="small muted">%s</td></tr>'
                 % (_esc(it["ref"]), _esc(it.get("pillar", "")), _esc(it["why"])))
    h.append('</tbody></table></div>')
    pp = cs["progressive_profiling"]
    h.append('<ul class="small">' + "".join('<li><b>%s:</b> %s</li>' % (_esc(k.replace("_", " ")), _esc(v)) for k, v in pp.items()) + '</ul>')
    # consent & device (F12)
    h.append('<h2 id="consent">Consent &amp; device-pairing sequence <span class="small muted">· closes F12</span></h2>')
    h.append('<div class="tablewrap"><table><thead><tr><th>Item</th><th>When</th><th>Gates</th><th>Owner</th></tr></thead><tbody>')
    for c in d["consent_gating"]:
        h.append('<tr><td class="small"><b>%s</b></td><td class="small">%s</td><td class="small muted">%s</td><td class="small muted">%s</td></tr>'
                 % (_esc(c["item"]), _esc(c["when"]), _esc(c["gates"]), _esc(c["doc"])))
    h.append('</tbody></table></div>')
    h.append('<p class="small muted">Source: <code>data/onboarding.json</code>. Hands off to the question bank '
             '(<a class="xref" href="appendix-question-bank.html">Appendix E</a>), persona determination '
             '(<a class="xref" href="appendix-persona-matrix.html">Appendix I</a>), goals '
             '(<a class="xref" href="appendix-goals.html">Appendix J</a>) and the Onboarding sub-machine on the '
             '<a class="xref" href="states.html">patient state machine</a>. Closes audit F9–F12 '
             '(<a class="xref" href="appendix-coverage-audit.html">Appendix G</a>).</p>')
    return "Onboarding & first-run", "".join(h)

def build_grid_onboarding():
    d = _load("onboarding.json")
    rows = [{"Order": s["order"], "Id": s["id"], "Phase": s["phase"], "Screen": s["screen"], "Captures": s.get("captures", ""),
             "Consent": "yes" if s.get("consent") else "—", "Device": "yes" if s.get("pairs_device") else "—",
             "Gated until": s.get("gated_until", "—"), "Feeds": s.get("feeds", ""), "Why": s["why"]} for s in d["steps"]]
    cols = [{"key": "Order", "label": "Order", "type": "num"}, {"key": "Id", "label": "Id"}, {"key": "Phase", "label": "Phase"},
            {"key": "Screen", "label": "Screen", "filter": "text"}, {"key": "Captures", "label": "Captures", "filter": "text"},
            {"key": "Consent", "label": "Consent"}, {"key": "Device", "label": "Device"}, {"key": "Gated until", "label": "Gated until", "filter": "text"},
            {"key": "Feeds", "label": "Feeds", "filter": "text"}, {"key": "Why", "label": "Why", "filter": "text"}]
    return _grid_page("Onboarding grid", "Onboarding first-run — spreadsheet", "The ordered first-run steps, flat &amp; sortable. Source:",
                      "appendix-onboarding.html", "Onboarding & first-run", _grid("g-onb", cols, rows), "Onboarding grid")


# =================================================================== PURESCORE SECTION (overview, wearable baselines, male, female)
def _ps_qcounts():
    try: qs = _load("question-bank.json")["questions"]
    except Exception: qs = []
    cnt = {}
    for q in qs:
        ps = set()
        for r in q.get("responses", []):
            for p in (r.get("pillars") or {}): ps.add(p)
        for p in ps: cnt[p] = cnt.get(p, 0) + 1
    return cnt, len(qs)

def _ps_wcounts():
    wc = {}
    for row in WEARABLES:
        for p in [x.strip() for x in row[4].split("·")]:
            wc[p] = wc.get(p, 0) + 1
    return wc

def _tier_counts(rows):
    c = {"C": 0, "P": 0, "X": 0}
    for r in rows:
        t = r[2]
        for k in c:
            if k in t: c[k] += 1
    return c

def _sexsplit(g):
    if "/" not in g: return None
    parts = [p.strip() for p in g.split("/")]
    male = next((p for p in parts if "M" in p), None)
    fem = next((p for p in parts if "F" in p), None)
    return (male, fem) if (male and fem and male != fem) else None

def build_purescore_overview():
    qcnt, nq = _ps_qcounts(); wc = _ps_wcounts(); wp = dict(PILLAR_W)
    h = ['<div class="crumbs"><a href="index.html">Home</a> &rsaquo; PureScore &rsaquo; Overview</div>',
         '<h1>PureScore &mdash; the scoring engine</h1>',
         '<p class="lead"><b>PureScore</b> is the scoring subsystem of HikmaEngine. It turns three input streams '
         '&mdash; <b>biomarkers</b> (labs, <a class="xref" href="appendix-biomarkers.html">Appendix A</a>), '
         '<b>wearables</b> (<a class="xref" href="appendix-wearables.html">Appendix B</a>) and '
         '<b>questions</b> (<a class="xref" href="appendix-question-bank.html">Appendix E</a>, the LIFE stream) '
         '&mdash; into 12 <a class="xref" href="02-pillars-and-marker-catalog.html">pillars</a> and the MONIAC '
         '<a class="xref" href="04-moniac-reservoir-dynamics.html">reservoirs</a>, then a single score via the '
         '<a class="xref" href="03-scoring-formula.html">scoring formula</a> with its '
         '<a class="xref" href="admin-weights.html">weights &amp; constants</a> and the 2.0 '
         '<a class="xref" href="12-critical-review-and-purescore-2.0.html">companion vector</a>. '
         'Scored by sex: <a class="xref" href="purescore-male.html">PureScore &mdash; Male</a> &middot; '
         '<a class="xref" href="purescore-female.html">PureScore &mdash; Female</a>.</p>', ILLUS]
    h.append('<div class="diagram"><div class="dt">Inputs &rarr; pillars &amp; reservoirs &rarr; score</div>'
             '<pre class="mermaid">flowchart LR\n'
             '  LAB["Biomarkers (C/P/X)"] --> P["12 Pillars"]\n  WEAR["Wearables (+baselines)"] --> P\n'
             '  LIFE["Questions (LIFE)"] --> P\n  LIFE --> R[("Reservoirs")]\n  WEAR --> R\n  LAB --> R\n'
             '  R -->|rho*B_tilde| P\n  P -->|gamma-mean, W_k| PS(("PureScore"))\n'
             '  PS --> CV["Companion vector"]\n  PS --> SEX["by sex: Male / Female"]</pre></div>')
    h.append('<div class="tagrow" style="margin:6px 0"><span class="tier C">C</span> Core '
             '<span class="tier P">P</span> Peripheral <span class="tier X">X</span> Comprehensive '
             '&nbsp;&middot;&nbsp; metrics tiers drive coverage &amp; confidence (Doc 01 §4.3)</div>')
    h.append('<div class="tablewrap"><table><thead><tr><th>Pillar</th><th>base W_k</th><th>Reservoirs</th>'
             '<th>Metrics C/P/X</th><th>Wearables</th><th>Questions</th></tr></thead><tbody>')
    for pid, pname, res, rows in PILLARS:
        tc = _tier_counts(rows)
        h.append('<tr><td><a href="appendix-biomarkers.html#%s"><b>%s</b></a> <span class="small muted">%s</span></td>'
                 '<td class="mono">%.2f</td><td class="small muted">%s</td>'
                 '<td class="mono">%d / %d / %d</td><td class="mono">%d</td><td class="mono">%d</td></tr>'
                 % (pid, pid, _esc(pname), wp.get(pid, 0), _esc(res), tc["C"], tc["P"], tc["X"],
                    wc.get(pid, 0), qcnt.get(pid, 0)))
    h.append('</tbody></table></div>')
    h.append('<p class="small muted">Totals: %d markers across the catalogue, %d wearable signals, %d LIFE questions '
             '&mdash; all weighted into pillars &amp; reservoirs. Constants (φ, κ_resp, γ, δ, ρ_k, R_crit, cap) and '
             'per-marker weights live in <a class="xref" href="admin-weights.html">Weights &amp; constants</a>; '
             'per-metric baselines in <a class="xref" href="purescore-wearable-baselines.html">Wearable baselines</a>.</p>'
             % (sum(len(r) for _a, _b, _c, r in PILLARS), len(WEARABLES), nq))
    return "PureScore · Overview", "".join(h)

_WB_BASELINE = {
 "Resting HR": ("personal EB μ/σ over 14d; lower = fitter", "50–70 bpm (adult)", "daily"),
 "HRV (RMSSD)": ("personal EB μ/σ; ↓ vs baseline = strain", "age-declining; track Δ", "daily"),
 "Steps / MVPA": ("14-day rolling mean vs WHO target", "≥8000 steps; ≥150 min MVPA/wk", "daily"),
 "Sleep duration / efficiency / regularity": ("14-day mean + regularity index", "7–9 h; SRI high", "nightly"),
 "SpO₂ (spot/overnight)": ("overnight nadir + ODI vs personal", "≥95%; ODI low", "nightly"),
 "Skin / body temperature": ("deviation from personal nightly baseline", "± personal band", "nightly"),
 "CGM glucose / time-in-range": ("TIR vs consensus + personal mean", "TIR ≥70%; CV <36%", "continuous"),
 "Blood pressure (cuff)": ("home mean vs guideline", "<120/80 optimal", "per reading"),
 "Single-lead ECG / rhythm": ("rhythm classification vs sinus baseline", "sinus; no AF", "on demand"),
 "Weight / body composition": ("trend vs personal baseline", "stable; BMI band", "daily/weekly"),
 "VO₂max (estimate)": ("age/sex-normed percentile", "≥ sex/age p60", "weekly"),
 "Readiness / recovery": ("composite vs personal baseline (informational)", "personal band", "daily"),
 "Stress score": ("autonomic composite vs baseline (informational)", "personal band", "daily"),
 "Sleep stages (REM/deep/light)": ("proportion vs age-norm (informational)", "age-normal", "nightly"),
 "Respiratory rate": ("deviation from personal nightly baseline", "12–20 /min", "nightly"),
}

def build_wearable_baselines():
    tcls = {"clinical-grade": "b-green", "consumer-validated": "b-acc", "inferential": "b-yellow"}
    h = ['<div class="crumbs"><a href="index.html">Home</a> &rsaquo; PureScore &rsaquo; Wearable baselines</div>',
         '<h1>Wearable Baselines</h1>',
         '<p class="lead">Each wearable metric is scored against a <b>personal baseline</b> (empirical-Bayes μ/σ that '
         'shrinks from the cohort prior toward the patient as data accrues &mdash; '
         '<a class="xref" href="12-critical-review-and-purescore-2.0.html">Doc 12 §5.1</a>), not a fixed cut-point. '
         'The personal z-score it produces drives Stage 2b of the <a class="xref" href="03-scoring-formula.html">'
         'score</a>, the Trajectory arrow and the Early-warning ladder. Trust tier (D22) caps how far a signal can '
         'move a band.</p>', ILLUS,
         '<div class="tablewrap"><table><thead><tr><th>Metric</th><th>Trust tier</th><th>Baseline method</th>'
         '<th>Expected range</th><th>Cadence</th><th>Pillars</th></tr></thead><tbody>']
    for (m, layer, tier, q, pil, dev) in WEARABLES:
        meth, rng, cad = _WB_BASELINE.get(m, ("personal EB μ/σ", "personal band", "daily"))
        h.append('<tr><td><b>%s</b></td><td><span class="chip %s">%s</span></td><td class="small">%s</td>'
                 '<td class="small mono">%s</td><td class="small muted">%s</td><td class="small">%s</td></tr>'
                 % (_esc(m), tcls.get(tier, "b-mut"), _esc(tier), _esc(meth), _esc(rng), _esc(cad), _esc(pil)))
    h.append('</tbody></table></div>')
    h.append('<div class="callout note"><div class="ct">How a baseline is built</div>'
             '<code>μ_i = (n/(n+k))·x̄_personal + (k/(n+k))·μ_cohort</code>; <code>z = (x−μ)/σ</code>. Cold-start '
             'leans on the cohort prior; sensitivity grows with <code>n</code>. Consumer/inferential tiers are '
             'informational-only and never set a red/critical without clinical-grade confirmation (D22).</div>')
    return "PureScore · Wearable baselines", "".join(h)

def _sex_gated(sex):
    try: qs = _load("question-bank.json")["questions"]
    except Exception: qs = []
    return [q for q in qs if q.get("applicability", {}).get("sex") == [sex]]

_MALE_MK = [("Total testosterone", "ng/dL", "300–1000", "ENDO — hypogonadism/andropause if low"),
 ("Free testosterone", "pg/mL", "age-adjusted", "ENDO — symptomatic low-T"),
 ("PSA", "ng/mL", "age-banded (≈<4)", "prostate screening"),
 ("Hematocrit / Hemoglobin", "g/dL", "13.5–17", "HEM — polycythemia (T-therapy) / anemia"),
 ("HDL-C", "mg/dL", "≥40", "CV — protective threshold (male)"),
 ("Waist circumference", "cm", "<94 (raised ≥102)", "MET — central adiposity")]
_FEMALE_MK = [("Estradiol (E2)", "pg/mL", "cycle/menopause-dependent", "ENDO — menopause transition"),
 ("FSH", "mIU/mL", "↑ in menopause", "ENDO — ovarian reserve / menopause"),
 ("AMH", "ng/mL", "age-declining", "ENDO — ovarian reserve / fertility"),
 ("Progesterone", "ng/mL", "luteal-phase dependent", "ENDO — cycle"),
 ("Hemoglobin / Ferritin", "g/dL · ng/mL", "12–15.5; ferritin low-sensitive", "HEM/NUT — menstrual/GI iron loss"),
 ("HDL-C", "mg/dL", "≥50", "CV — protective threshold (female)"),
 ("Waist circumference", "cm", "<80 (raised ≥88)", "MET — central adiposity"),
 ("BMD T-score", "SD", "≥ −1.0; red sooner post-menopause", "BCM — bone reserve")]

def _build_purescore_sex(sex, label, markers, emphasis, lifestage):
    gated = _sex_gated(sex)
    h = ['<div class="crumbs"><a href="index.html">Home</a> &rsaquo; PureScore &rsaquo; %s</div>' % _esc(label),
         '<h1>PureScore &mdash; %s</h1>' % _esc(label),
         '<p class="lead">The %s view of the scoring engine: sex-specific reference cut-points, %s-specific markers, '
         'pillar-weight emphases and the %d %s-gated questions. The shared engine (formula, reservoirs, companion '
         'vector) is in the <a class="xref" href="purescore-overview.html">Overview</a>; sex handling is '
         '<a class="xref" href="05-sex-specific-models.html">Doc 05</a>.</p>' % (label.lower(), label.lower(), len(gated), label.lower()),
         ILLUS]
    h.append('<div class="callout note"><div class="ct">Pillar emphasis (%s)</div>%s</div>' % (_esc(label), _esc(emphasis)))
    # sex-specific markers (curated)
    h.append('<h2>%s-specific markers</h2><div class="tablewrap"><table><thead><tr><th>Marker</th><th>Unit</th>'
             '<th>%s reference</th><th>Pillar / note</th></tr></thead><tbody>' % (_esc(label), _esc(label)))
    for mk, unit, rng, note in markers:
        h.append('<tr><td><b>%s</b></td><td class="small muted">%s</td><td class="small mono">%s</td>'
                 '<td class="small">%s</td></tr>' % (_esc(mk), _esc(unit), _esc(rng), _esc(note)))
    h.append('</tbody></table></div>')
    # shared markers with sex-specific cut-points parsed from the catalogue
    idx = 0 if sex == "male" else 1
    rowsout = []
    for pid, pname, res, rows in PILLARS:
        for r in rows:
            ss = _sexsplit(r[4])
            if ss: rowsout.append((pid, r[0], r[1], ss[idx]))
    if rowsout:
        h.append('<h2>Shared markers, %s cut-points</h2><div class="tablewrap"><table><thead><tr><th>Pillar</th>'
                 '<th>Marker</th><th>Unit</th><th>%s green band</th></tr></thead><tbody>' % (_esc(label), _esc(label)))
        for pid, mk, unit, val in rowsout:
            h.append('<tr><td class="mono">%s</td><td><b>%s</b></td><td class="small muted">%s</td>'
                     '<td class="small mono">%s</td></tr>' % (pid, _esc(mk), _esc(unit), _esc(val)))
        h.append('</tbody></table></div>')
    h.append('<h2>Life-stage frames</h2><p class="small">%s</p>' % _esc(lifestage))
    # sex-gated questions
    h.append('<h2>%s-gated questions <span class="small muted">(%d)</span></h2>'
             '<p class="small muted">From the question bank, served only when sex = %s:</p><ul class="small">'
             % (_esc(label), len(gated), _esc(label.lower())))
    for q in gated[:40]:
        h.append('<li><span class="mono">%s</span> — %s</li>' % (_esc(q.get("ref", "")), _esc(q.get("text", ""))))
    h.append('</ul>')
    return "PureScore — %s" % label, "".join(h)

def build_purescore_male():
    return _build_purescore_sex("male", "Male", _MALE_MK,
        "CV carries earlier baseline risk; ENDO tracks testosterone/andropause; prostate (PSA) screening; HEM watches "
        "polycythemia. Hemoglobin/eGFR/urate read against male reference.",
        "Andropause — gradual testosterone decline from midlife; no cyclical hormonal frame.")

def build_purescore_female():
    return _build_purescore_sex("female", "Female", _FEMALE_MK,
        "ENDO dominates across cycle → perimenopause → menopause; BCM bone loss post-menopause (BMD red sooner); HEM "
        "iron/menstrual-loss sensitivity; CV risk rises post-menopause. Hemoglobin/HDL/waist read against female reference.",
        "Menstrual cycle (follicular/luteal phase-aware ranges) · pregnancy frame (physiologic shifts; pre-eclampsia/GDM "
        "anchors retained) · perimenopause/menopause (vasomotor, bone, CV).")


# =================================================================== WEARABLE CORROBORATION (closes F4)
def _wear_corr_counts():
    """Per-metric count of question-bank questions whose wearable corroborations resolve to it."""
    try:
        reg = _load("wearable-corroboration.json"); qb = _load("question-bank.json")["questions"]
    except Exception:
        return {}, {}
    alias2id = {a: m["id"] for m in reg["metrics"] for a in m["aliases"]}
    from collections import Counter
    mc = Counter()
    for q in qb:
        seen = set()
        for s in (q.get("perceived_actual", {}).get("corroborated_by", []) or []):
            mid = alias2id.get(str(s).strip())
            if mid:
                seen.add(mid)
        for mid in seen:
            mc[mid] += 1
    return dict(mc), reg

def build_wearable_corroboration():
    counts, reg = _wear_corr_counts(); m = reg["meta"]
    h = ['<div class="crumbs"><a href="index.html">Home</a> › Questions & intake › Wearable corroboration</div>',
         '<h1>Appendix L — Wearable Corroboration <span class="small muted">— question → metric → tolerance (closes F4)</span></h1>',
         '<p class="lead">%s</p>' % _esc(m["idea"]), ILLUS]
    h.append('<div class="diagram"><div class="dt">Perceived ↔ actual, with a typed tolerance</div>'
             '<pre class="mermaid">flowchart LR\n'
             '  SR["self-report answer (perceived)"] --> CMP{"|Δ| &gt; tolerance?"}\n'
             '  WM["wearable metric (actual, Appendix B)"] --> CMP\n'
             '  CMP -->|yes| GAP["perceived↔actual gap (Appendix F)\\nlower Confidence (Doc 12)"]\n'
             '  CMP -->|no| OK["corroborated → higher Confidence"]</pre></div>')
    h.append('<div class="callout note"><div class="ct">Tolerance</div>%s</div>' % _esc(m["tolerance_semantics"]))
    h.append('<div class="callout note"><div class="ct">Normalized</div>%s</div>' % _esc(m["normalizes"]))
    h.append('<p class="small muted">%d canonical metrics resolve all 23 of the bank\'s <code>wearable.*</code> '
             'corroboration refs · <a class="xref" href="grid-wearable-corroboration.html">spreadsheet ↗</a></p>' % len(reg["metrics"]))
    h.append('<div class="tablewrap"><table><thead><tr><th>Metric</th><th>Canonical (Appx B)</th><th>Channel</th>'
             '<th>Tolerance</th><th>Questions</th><th>Perceived ↔ actual</th><th>Aliases (normalized)</th></tr></thead><tbody>')
    for mt in sorted(reg["metrics"], key=lambda x: -counts.get(x["id"], 0)):
        h.append('<tr><td class="small"><b>%s</b><br><span class="small muted mono">%s</span></td>'
                 '<td class="small muted">%s</td><td>%s</td><td class="small mono">%s</td>'
                 '<td class="mono small">%d</td><td class="small muted">%s</td><td class="small mono">%s</td></tr>'
                 % (_esc(mt["name"]), _esc(mt["id"]), _esc(mt["appendix_b"]), _chan_chip(mt["channel"]),
                    _esc(mt["tolerance"]), counts.get(mt["id"], 0), _esc(mt["perceived_actual"]),
                    _esc(", ".join(a.replace("wearable.", "") for a in mt["aliases"]))))
    h.append('</tbody></table></div>')
    h.append('<p class="small muted">Source: <code>data/wearable-corroboration.json</code>. Closes audit <b>F4</b> '
             '(<a class="xref" href="appendix-coverage-audit.html">Appendix G</a>) — the wearable-match stage is now '
             'typed &amp; auditable: every <code>wearable.*</code> corroboration links to a canonical metric '
             '(<a class="xref" href="appendix-wearables.html">Appendix B</a>) with a tolerance the perceived-vs-actual '
             'gap engine (<a class="xref" href="appendix-lifestyles.html">Appendix F</a>) applies.</p>')
    return "Wearable corroboration", "".join(h)

def build_grid_wearable_corroboration():
    counts, reg = _wear_corr_counts()
    rows = [{"Metric": mt["name"], "Id": mt["id"], "Canonical (Appx B)": mt["appendix_b"], "Channel": mt["channel"],
             "Tolerance": mt["tolerance"], "Questions": counts.get(mt["id"], 0),
             "Perceived↔actual": mt["perceived_actual"], "Aliases": ", ".join(a.replace("wearable.", "") for a in mt["aliases"])}
            for mt in reg["metrics"]]
    cols = [{"key": "Metric", "label": "Metric", "filter": "text"}, {"key": "Id", "label": "Id"},
            {"key": "Canonical (Appx B)", "label": "Canonical (Appx B)", "filter": "text"}, {"key": "Channel", "label": "Channel"},
            {"key": "Tolerance", "label": "Tolerance", "filter": "text"}, {"key": "Questions", "label": "Questions", "type": "num"},
            {"key": "Perceived↔actual", "label": "Perceived ↔ actual", "filter": "text"}, {"key": "Aliases", "label": "Aliases", "filter": "text"}]
    return _grid_page("Wearable-corroboration grid", "Wearable corroboration — spreadsheet",
                      "Every wearable corroboration metric, tolerance &amp; question count, flat &amp; sortable. Source:",
                      "appendix-wearable-corroboration.html", "Wearable corroboration", _grid("g-wc", cols, rows), "Wearable-corroboration grid")
