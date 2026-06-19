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
 ("transfem","Trans woman (estrogen)",34,"—","0.55","—","Hgb/lipids/urate/E2 read against female (gonadal) reference; natal-sex organ baseline retained; confidence lowered."),
 ("transmasc","Trans man (testosterone)",29,"—","0.55","—","Hgb/creatinine-eGFR/urate/T read against male (gonadal) reference."),
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
                 '<th>Marker</th><th>Unit</th><th>T</th><th>Band shape</th>'
                 '<th>Green</th><th>Yellow</th><th>Red</th><th>w</th><th>Source</th></tr></thead><tbody>')
        for (mk, unit, t, ts, g, y, r, w, src, crit) in rows:
            star = ' <span title="critical marker — can make its pillar critical" style="color:var(--red)">★</span>' if crit else ''
            h.append('<tr><td><b>%s</b>%s</td><td class="small muted">%s</td>'
                     '<td><span class="tier %s">%s</span></td><td>%s</td>'
                     '<td><span class="chip b-green">%s</span></td>'
                     '<td><span class="chip b-yellow">%s</span></td>'
                     '<td><span class="chip b-red">%s</span></td>'
                     '<td class="mono">%s</td><td class="small muted">%s</td></tr>'
                     % (_esc(mk), star, _esc(unit), t, t.replace("/","/"), _rngbar(ts),
                        _esc(g), _esc(y), _esc(r), _esc(w), _esc(src)))
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
    h = ['<div class="crumbs"><a href="index.html">Home</a> › Reference › Questionnaires</div>',
         '<h1>Appendix C — Lifestyle &amp; PRO Questionnaires</h1>',
         '<p class="lead">The standardized patient-reported instruments PureScore administers, with full item '
         'text, scoring and band cut-offs. These are the <span class="flag fl-life">LIFE</span> stream '
         '(<a class="xref" href="18-data-streams-and-experience.html">Doc 18</a> §1) and the MCS/behavioural markers of '
         '<a class="xref" href="02-pillars-and-marker-catalog.html">Doc 02</a>.</p>', ILLUS]
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
    return "Appendix C · Questionnaires", "".join(h)

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
    by_dom = {}
    for q in qs: by_dom.setdefault(q["domain"], []).append(q)
    dom_codes = m["domain_codes"]                      # [{domain,code,count}]
    code_of = {d["domain"]: d["code"] for d in dom_codes}
    total = len(qs)
    h = ['<div class="crumbs"><a href="index.html">Home</a> › Reference › Question bank</div>',
         '<h1>Appendix E — Lifestyle &amp; Condition Question Bank</h1>',
         '<p class="lead">The full engine-ready intake bank: <b>%d questions</b> across <b>%d sections</b> '
         'covering <b>%d conditions</b>. Each question has a stable reference code <code>%s</code>, a place in '
         'the progressive <b>chain</b> (prerequisites → this → unlocks, with prev/next links), an '
         '<b>applicability vector</b> (sex · age · life-stage · persona) and signed weights onto the 12 '
         '<a class="xref" href="02-pillars-and-marker-catalog.html">pillars</a> and MONIAC '
         '<a class="xref" href="04-moniac-reservoir-dynamics.html">reservoirs</a>. The <span class="flag fl-life">'
         'LIFE</span> stream (<a class="xref" href="18-data-streams-and-experience.html">Doc 18</a>); '
         'validated PROs are in <a class="xref" href="appendix-questions.html">Appendix C</a>.</p>'
         % (m["total_questions"], len(dom_codes), m["total_conditions"], m.get("ref_scheme", "DOM-NNN")), ILLUS]
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
    # per-section panels, ordered by the chain sequence
    for dc in dom_codes:
        dom = dc["domain"]; code = dc["code"]
        items = sorted(by_dom.get(dom, []), key=lambda x: x.get("seq", 999))
        if not items: continue
        h.append('<h2 id="%s">%s <span class="small muted mono">· %s · %d questions</span></h2>'
                 % (_esc(code), _esc(dom.replace("_", " ").title()), _esc(code), len(items)))
        for q in items:
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

# =================================================================== APPENDIX F — lifestyles & personas
def build_lifestyles():
    pa = _load("persona-axes.json")
    m = pa["meta"]
    h = ['<div class="crumbs"><a href="index.html">Home</a> › Reference › Lifestyles</div>',
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
    stats = {"total": total, "corr_n": corr_n, "wear_n": wear_n,
             "adher_n": adher_n + adher_cat,
             "goal_n": goal_n, "goal_struct": goal_struct,
             "persona_det_n": persona_det_n + persona_sig, "source_typed": 0}
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
        chips = " ".join('<a class="chip %s" href="#%s">%s · %s</a>' % (cls, _esc(it["id"]), _esc(it["id"]), _esc(it["title"]))
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
        autotag = ""
        if it.get("auto_rule"):
            autotag = (' <span class="chip b-green">auto ✓</span>' if it["_auto"]
                       else ' <span class="chip b-mut mono" title="auto-rule (not yet met)">auto: %s</span>' % _esc(it["auto_rule"]))
        h.append('<div class="panel" id="%s" style="margin:10px 0">' % _esc(it["id"]))
        h.append('<div class="tagrow" style="gap:8px;align-items:center;margin-bottom:6px">'
                 '%s<b class="mono">%s</b><span style="flex:1">%s</span>'
                 '<span class="chip %s">%s</span>'
                 '<span class="chip b-mut">stage %s</span>'
                 '<span class="chip b-mut">%s · %s</span>%s'
                 '<button class="chip b-acc" style="cursor:pointer" onclick="auditPop(\'%s\')">history (%d) ▾</button>'
                 '</div>'
                 % (_SEVCHIP[it["severity"]], _esc(it["id"]), _esc(it["title"]), scls, _esc(slbl),
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
             '  CAP["1 Capture<br/>%d Q · 12 domains<br/>OK"] --> SCORE["2 Score<br/>12 pillars · 15 reservoirs<br/>OK"]\n'
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
       "ok", "— capture is broad and explicitly multi-dimensional"),
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
    h = ['<div class="crumbs"><a href="index.html">Home</a> › Reference › Adherence</div>',
         '<h1>Appendix H — Adherence Micro Check-ins <span class="small muted">· closes F1</span></h1>',
         '<p class="lead">%s</p>' % _esc(m["idea"]), ILLUS]
    h.append('<div class="diagram"><div class="dt">How an adherence answer closes the loop</div>'
             '<pre class="mermaid">flowchart LR\n'
             '  NUD["nudge (Doc 07/16)"] --> ASK["micro check-in (this appendix)"]\n'
             '  ASK --> R["response · adherence ∈ [0,1] + reason"]\n'
             '  R --> RES["reservoir inflow (Doc 04)"]\n  R --> ENG["engagement / Trajectory (Doc 12)"]\n'
             '  R --> FEAS["nudge feasibility (Doc 07 §3)"] --> NUD</pre></div>')
    h.append('<div class="callout note"><div class="ct">Scoring</div>%s</div>' % _esc(m["scoring"]))
    h.append('<div class="tagrow" style="margin:8px 0"><span class="small muted">reason taxonomy:</span> '
             + " ".join('<span class="chip b-mut">%s</span>' % _esc(r) for r in m["reason_taxonomy"]) + '</div>')
    h.append('<p class="small muted">%d check-ins · one per nudge family. Source: <code>data/adherence.json</code>.</p>'
             % len(items))
    for it in items:
        h.append('<div class="panel" id="%s" style="margin:10px 0">' % _esc(it["ref"]))
        h.append('<h3 style="margin:0"><span class="mono" style="color:var(--acc)">%s</span> · %s</h3>'
                 % (_esc(it["ref"]), _esc(it["nudge_family"])))
        chips = ['<span class="chip b-acc mono">%s</span>' % _esc(p) for p in it.get("pillars", [])]
        chips.append('<span class="chip b-mut">reservoir: %s</span>' % _esc(it.get("reservoir") or "—"))
        chips.append('<span class="chip b-mut">cadence: %s</span>' % _esc(it.get("cadence", "")))
        h.append('<div class="tagrow" style="margin:6px 0">%s</div>' % "".join(chips))
        h.append('<p class="small"><b>Q:</b> <i>%s</i></p>' % _esc(it["stem"]))
        h.append('<div class="tablewrap"><table><thead><tr><th>Response</th><th>Adherence</th>'
                 '<th>Reservoir Δ</th><th>Reason</th></tr></thead><tbody>')
        for r in it["responses"]:
            h.append('<tr><td class="small"><b>%s</b></td><td class="mono small">%.2f</td>'
                     '<td>%s</td><td class="small muted">%s</td></tr>'
                     % (_esc(r["label"]), r.get("adherence", 0), _res_delta_chips(r.get("reservoirs")),
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
    h = ['<div class="crumbs"><a href="index.html">Home</a> › Reference › Persona determination</div>',
         '<h1>Appendix I — Input → Persona Determination Matrix <span class="small muted">· closes F2</span></h1>',
         '<p class="lead">The matrix that lets the engine <b>infer</b> a persona from the answers, not just gate '
         'questions by one. %d personas (15 clinical + 25 lifestyle archetypes) × %d signals.</p>'
         % (len(personas), len(signals)), ILLUS]
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
    h = ['<div class="crumbs"><a href="index.html">Home</a> › Reference › Goals</div>',
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
            h.append('<tr><td class="small"><span class="mono" style="color:var(--acc)">%s</span> <b>%s</b>'
                     '<br><span class="small muted">%s</span></td>'
                     '<td class="small">%s</td><td class="small muted">%s<br>%s%s</td>'
                     '<td class="small">%s</td><td>%s</td></tr>'
                     % (_esc(g["id"]), _esc(g["title"]), _esc(g.get("axis", "")), tgt,
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
_BM_EXPECT = {"reservoirs":15,"pillars":12,"domains":12,"axes":12,"clin":15,"arch":25}

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
 var level=localStorage.getItem("bmLevel")||"fields",z=1,tx=0,ty=0,area=null,n=0;
 function clamp(v,a,b){return Math.max(a,Math.min(b,v));}
 function apply(){stage.style.transform="translate("+tx+"px,"+ty+"px) scale("+z+")";if(hud)hud.textContent=Math.round(z*100)+"%";}
 function highlight(){var s=stage.querySelector("svg");if(!s)return;
   s.querySelectorAll("g.node").forEach(function(g){g.style.opacity=(!area||g.classList.contains(area))?"1":"0.12";});}
 function fit(){var s=stage.querySelector("svg");if(!s)return;var bb;try{bb=s.getBBox();}catch(e){return;}
   var fw=frame.clientWidth-24,fh=frame.clientHeight-24;
   if(bb.width&&bb.height){z=clamp(Math.min(fw/bb.width,fh/bb.height),0.08,4);tx=Math.max(12,(frame.clientWidth-bb.width*z)/2);ty=12;}apply();}
 function setActive(){["fields","names","areas"].forEach(function(k){var b=document.getElementById("bm-"+k);if(b)b.classList.toggle("on",k===level);});}
 function render(){mermaid.render("bmG"+(++n),srcs[level]).then(function(r){stage.innerHTML=r.svg;
     var s=stage.querySelector("svg");if(s){s.removeAttribute("height");s.removeAttribute("width");s.style.maxWidth="none";s.style.height="auto";}
     setActive();fit();highlight();}).catch(function(e){stage.innerHTML='<pre style="color:#ff8a8a;padding:14px;white-space:pre-wrap">'+(e&&e.message||e)+'</pre>';});}
 ["fields","names","areas"].forEach(function(k){var b=document.getElementById("bm-"+k);if(b)b.addEventListener("click",function(){level=k;localStorage.setItem("bmLevel",k);render();});});
 function zoomAt(cx,cy,f){var nz=clamp(z*f,0.08,6);tx=cx-((cx-tx)/z)*nz;ty=cy-((cy-ty)/z)*nz;z=nz;apply();}
 var byId=function(i){return document.getElementById(i);};
 if(byId("bm-zin"))byId("bm-zin").addEventListener("click",function(){zoomAt(frame.clientWidth/2,frame.clientHeight/2,1.2);});
 if(byId("bm-zout"))byId("bm-zout").addEventListener("click",function(){zoomAt(frame.clientWidth/2,frame.clientHeight/2,1/1.2);});
 if(byId("bm-reset"))byId("bm-reset").addEventListener("click",function(){z=1;tx=0;ty=0;apply();});
 if(byId("bm-fit"))byId("bm-fit").addEventListener("click",fit);
 if(byId("bm-full"))byId("bm-full").addEventListener("click",function(){if(!document.fullscreenElement){if(frame.requestFullscreen)frame.requestFullscreen();}else if(document.exitFullscreen){document.exitFullscreen();}});
 document.addEventListener("fullscreenchange",function(){setTimeout(fit,90);});
 frame.addEventListener("wheel",function(e){e.preventDefault();var r=frame.getBoundingClientRect();zoomAt(e.clientX-r.left,e.clientY-r.top,e.deltaY<0?1.1:1/1.1);},{passive:false});
 var gx=0,gy=0,drag=false;
 frame.addEventListener("pointerdown",function(e){if(e.target.closest&&e.target.closest("#bm-legend"))return;drag=true;gx=e.clientX-tx;gy=e.clientY-ty;try{frame.setPointerCapture(e.pointerId);}catch(_){}frame.style.cursor="grabbing";});
 frame.addEventListener("pointermove",function(e){if(!drag)return;tx=e.clientX-gx;ty=e.clientY-gy;apply();});
 frame.addEventListener("pointerup",function(){drag=false;frame.style.cursor="grab";});
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
         % (len(_BM_CLASSES), len(_BM_AREAS), c["reservoirs"], c["pillars"], c["domains"], c["questions"], c["clin"], c["arch"]),
         ILLUS]
    h.append('<div class="callout note"><div class="ct">Reconciliations baked in</div>'
             '<b>Reservoirs</b> 15 canonical (Doc 04, incl. VBP/OXD/IRON). <b>Symbol</b> responsiveness cap = '
             '<code>kappa_resp</code> vs interference <code>kappa_jl</code>. <b>Scope</b> everything under <code>docs/</code>.</div>')
    if warns:
        h.append('<div class="callout safety"><div class="ct">Diagram drift</div>The data files moved away from the '
                 'diagram baseline &mdash; counts above auto-updated; review the spec: %s.</div>' % _esc("; ".join(warns)))
    h.append('<div class="bm-tool" style="display:flex;gap:6px;flex-wrap:wrap;align-items:center;margin:10px 0 6px">'
             '<span class="small muted">detail:</span>'
             '<button id="bm-fields">Fields</button><button id="bm-names">Names</button><button id="bm-areas">Areas</button>'
             '<span style="width:14px"></span><span class="small muted">view:</span>'
             '<button id="bm-zout">&minus;</button><button id="bm-reset">reset</button><button id="bm-fit">fit</button>'
             '<button id="bm-zin">&plus;</button><button id="bm-full">&#9974; fullscreen</button>'
             '<span id="bm-hud" class="small muted mono" style="margin-left:6px">100%</span>'
             '<span class="small muted" style="margin-left:auto">wheel = zoom &middot; drag = pan</span></div>')
    sfx = {"reservoir": " &middot;%d" % c["reservoirs"], "scoring": " &middot;%d pillars" % c["pillars"],
           "questions": " &middot;%dQ" % c["questions"], "persona": " &middot;%d+%d" % (c["clin"], c["arch"])}
    chips = ['<span data-area="%s" style="background:%s;color:#fff">%s%s</span>'
             % (k, f, _esc(lbl), sfx.get(k, "")) for k, lbl, f, s in _BM_AREAS]
    h.append('<div id="bm-legend" class="tagrow" style="gap:6px;margin:0 0 10px">'
             '<span class="small muted">subject areas (click to spotlight):</span>%s</div>' % "".join(chips))
    h.append('<div id="bm-frame" tabindex="0" style="position:relative;height:74vh;overflow:hidden;'
             'border:1px solid var(--line,#334);border-radius:10px;background:#0b0f17;cursor:grab">'
             '<div id="bm-stage" style="position:absolute;top:0;left:0;transform-origin:0 0"></div></div>')
    h.append('<p class="small muted" style="margin-top:8px">Generated from the <code>BEHEMOTH</code> spec in '
             '<code>wiki_content.py</code> (areas &middot; classes &middot; relations); the three views and colours regenerate on '
             'every <code>build_wiki.py</code> run and counts are pulled live from the data files, so the map stays '
             'in sync.</p>')
    for lv in ("fields", "names", "areas"):
        h.append('<script type="application/x-mermaid" id="bm-src-%s">%s</script>' % (lv, _bm_diagram(lv, c)))
    h.append(_BM_JS)
    return "Behemoth Class Diagram", "".join(h)
