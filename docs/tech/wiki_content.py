# -*- coding: utf-8 -*-
"""Data + appendix/admin page builders for the PureScore tech wiki.
All values are illustrative, literature/guideline-anchored, and must be re-verified
(README §5.6) before production. Compiled from docs 00-18."""

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
 ("κ","personal-baseline responsiveness cap (Stage 2b)","0.10","Doc 03 §2b"),
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
