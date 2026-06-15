# 05 — Sex-Specific & Reproductive-Stage Models

> Binding conventions: `README.md §3`. This document is authoritative for the **ENDO** sex-hormone
> bands deferred from Doc 02, and for how **sex at birth × reproductive stage** modifies pillar
> weights `W_k` (Doc 03 §5), ENDO marker bands (`[L_i^opt, U_i^opt]`), the reservoir set (Doc 04),
> and escalation thresholds (Doc 03 §4 / Doc 11). Positioning is **wellness-grade,
> clinician-in-the-loop** (README §1). Every red/critical state still escalates to a human; nothing
> here relaxes the hard non-negotiables (README §5).
>
> **All bands in this document are illustrative, literature/guideline-anchored, and must be
> re-verified and cohort-adjusted on the fixed cadence (README §5.6) before any production use.**
> Units, assay platforms, and trimester cut-points vary by lab; the engine stores assay-specific
> ranges, not the printed numbers here.

---

## 1. Two orthogonal attributes: `sex_at_birth` vs `gender_identity`

PureScore separates **physiology** from **communication**. Conflating them is both a safety bug
(wrong reference ranges) and a dignity failure.

| Field | Type | Drives | Never drives |
|-------|------|--------|--------------|
| `sex_at_birth ∈ {female, male, intersex, unknown}` | physiological | reference-range selection, organ-specific markers (e.g. PSA, AMH), reservoir set, eGFR sex coefficient, hemoglobin/ferritin bands | pronouns, tone, framing |
| `gender_identity` (free/standard list) | social | pronouns, addressing the patient, nudge tone, imagery (Doc 07) | any band, threshold, or weight |
| `organ_inventory` (set: uterus, ovaries, prostate, …) | physiological | *which* sex-specific markers are even applicable | communication |
| `hormone_therapy` (class, dose, route, start date) | physiological | **range shifting** (see §1.2) | — |

**No crude binary fallback.** When `sex_at_birth = unknown` the engine does **not** guess from
gender, name, or any proxy (README §5.5). It instead (a) uses sex-neutral bands where they exist,
(b) marks affected ENDO/HEM/REN markers low-confidence (Doc 03 §3 `confidence_i`), and (c) raises a
clinician prompt to capture `sex_at_birth` and `organ_inventory`. Uncertainty defaults to caution
(README §5.4), never to a reassuring "in range."

### 1.1 Organ-inventory gating
A marker is scored only if the relevant organ/axis is present. PSA is omitted (not scored as
"green") for a patient without a prostate; AMH/antral context is omitted without ovaries. This
prevents both false reassurance and irrelevant red flags. Post-surgical/absent organs set the
marker to `not-applicable`, excluded from `cov_k` denominators (Doc 03 §3).

### 1.2 Hormone-therapy-aware ranges (transgender & gender-diverse patients)
For a patient on gender-affirming hormone therapy (GAHT), the **target reference range follows the
affirmed hormonal milieu**, anchored to Endocrine Society 2017 GAHT guidance (illustrative,
re-verify):

| Scenario | Markers shifted toward | Notes |
|----------|------------------------|-------|
| Transfeminine on estradiol ± anti-androgen | estradiol → premenopausal-female target ~100–200 pg/mL; total T → <50 ng/dL suppressed | Hemoglobin/hematocrit, creatinine/eGFR drift toward **female** reference over months; lipid/VTE surveillance up |
| Transmasculine on testosterone | total T → male physiologic 320–1000 ng/dL trough-dependent; estradiol low | Hemoglobin/hematocrit drift toward **male** reference; monitor erythrocytosis (HEM red if Hct high); pelvic organs if retained still need stage logic |
| Post-gonadectomy, on stable GAHT | gonadotropins (FSH/LH) **not** interpreted as menopausal | flag only if HT interrupted |

Implementation: `effective_endocrine_profile = f(sex_at_birth, GAHT class, time-on-therapy,
organ_inventory)`. Hematologic and renal bands **transition gradually** (use time-on-therapy to
interpolate, not a step change), because erythropoiesis and creatinine generation track the
dominant sex steroid over 6–12 months. Communication always uses `gender_identity`.

### 1.3 Intersex / DSD handling
Intersex / differences of sex development are **not** forced into a binary. The engine uses the
patient's actual `organ_inventory`, measured baseline hormone profile, and any
clinician-specified reference frame; where no validated population range exists, markers are scored
against the **individual's own longitudinal baseline** (intra-individual reference change value)
rather than a mismatched population band, and flagged low-confidence with a clinician note. This is
a first-class path, not an exception handler.

---

## 2. Female model — phase- and stage-dependent bands

The female ENDO axis is **cyclical and stage-structured**. The core design rule:

> **Phase-aware banding.** Normal cyclical variation in estradiol, progesterone, LH/FSH, BBT,
> resting HR and HRV **must not** be scored as risk. The band `[L_i^opt, U_i^opt]` for these
> markers is a **function of cycle phase / trimester / STRAW+10 stage**, supplied to Doc 03 Stage 1
> as `[L_i^opt(phase), U_i^opt(phase)]`. Penalty (`r_i`) accrues only for values out-of-range
> *for the current phase*, or for **loss of expected cyclicity** (see §2.1.3).

Stage is determined from: patient-reported LMP / cycle tracking, wearable temperature/HR signatures,
pregnancy status, age, and labs — with a confidence weight. When stage is uncertain, the engine
widens bands (least-penalizing across plausible phases) and lowers confidence rather than guessing.

### 2.1 Menstrual cycle (reproductive years)

#### 2.1.1 Phase-dependent hormone bands (illustrative, re-verify; serum, conventional assays)

| Marker | Follicular (early) | Ovulatory (peri-LH-surge) | Luteal (mid) | Source frame |
|--------|--------------------|---------------------------|--------------|--------------|
| Estradiol (pg/mL) | 20–150 | 150–400 (peak) | 50–250 | Endocrine Society / lab |
| Progesterone (ng/mL) | <1.0 | 1–3 (rising) | **>3 (≥10 confirms ovulation, mid-luteal)** | ESHRE/ACOG ovulation |
| LH (IU/L) | 2–12 | **surge ~15–80** | 1–12 | lab |
| FSH (IU/L) | 3–12 (day-3 reference) | mid-rise | 1.5–8 | ESHRE/STRAW+10 |

Mid-luteal progesterone >3 ng/mL is the score's **ovulation-confirmation** signal; sustained
anovulation (no luteal rise across cycles) is a yellow flag routed to §2.1.3.

#### 2.1.2 Wearable correlates across the cycle (do not penalize the normal swing)
Phase-expected wearable shifts are **baselined per phase** so the score reads them as *signal of a
healthy cycle*, not as deterioration:

| Wearable | Follicular | Luteal (post-ovulation) | Engine handling |
|----------|------------|--------------------------|-----------------|
| BBT / wrist skin temp | lower baseline | **+0.3–0.5 °C sustained shift** | rise = ovulation marker, **not** fever/illness; gated against absolute fever threshold |
| Resting HR | lower | **+2–4 bpm** | luteal elevation expected; compared to *luteal* baseline |
| HRV (RMSSD) | higher | **lower** in luteal | compared to *phase* baseline; not a recovery red |
| Respiratory rate | lower | slightly higher | phase-aware |

The SLP/FIT/CV pillars receive **phase-adjusted** resting-HR and HRV baselines for menstruating
patients, so a normal luteal HRV dip is not mis-scored as poor recovery (Doc 02 SLP/CV). Absolute
safety thresholds (e.g. true fever, tachycardia) still apply on top of the phase baseline.

#### 2.1.3 Flagging irregularity (the score's real job here)
Cyclicity itself is a health signal. The engine computes a **cycle-regularity feature** (cycle
length mean/variance, ovulation confirmation rate, luteal-phase adequacy) and flags patterns:

| Pattern | Signature | Routes to | Zone |
|---------|-----------|-----------|------|
| Oligo/anovulation, hyperandrogenism (PCOS phenotype) | irregular/long cycles, ↑LH:FSH, ↑free androgens/SHBG-low, often ↑HOMA-IR | ENDO + MET (insulin link via GLY reservoir); refer per ESHRE/AE-PCOS | yellow→red, **routine/urgent**, clinician-confirmed |
| Functional hypothalamic amenorrhea | low LH/FSH, low estradiol, low energy availability, often high training load/low body fat | ENDO + BCM (BON reserve) + NUT; **not** "good low hormones" | yellow→red, urgent (bone/cardiac risk) |
| Thyroid-driven irregularity | abnormal TSH/FT4 with menstrual change | ENDO (thyroid sub-axis) | per Doc 02 TSH bands |

**Safety note:** PureScore *flags* these patterns and explains the contributing markers; it does
**not** diagnose PCOS/FHA/thyroid disease — diagnosis is clinician-only (README §5.1). Hypothalamic
amenorrhea's low estradiol must never read as benign because the number is "low" — it is penalized
via the BON reserve depletion path (§4.3).

### 2.2 Fertility / preconception

Activated by goal flag `goal = conception` (Doc 06) or clinician order. Adds targeted markers and
shifts thyroid targets:

| Marker | Optimal (illustrative, re-verify) | Notes / source |
|--------|-----------------------------------|----------------|
| AMH (ng/mL) | age-referenced; ~1.0–4.0 typical reproductive | ovarian-reserve **context, not a fertility verdict** (ESHRE/ASRM caution) |
| Antral follicle count (context) | clinician-entered | imaging, not scored autonomously |
| Ovulation confirmation | mid-luteal progesterone >3 ng/mL (§2.1.1) | ESHRE |
| TSH (conception target) | **0.5–2.5 mIU/L** (tighter than general 0.5–4.5) | ATA/Endocrine Society preconception |
| 25-OH Vitamin D | 30–60 ng/mL (NUT) | sufficiency before conception |
| Ferritin / iron (HEM/NUT) | ferritin ≥30, sat 25–45% | replete iron stores |
| Folate / RBC folate | replete; **periconceptional folic acid 400 µg/day** (≥4 mg if prior NTD) | ACOG/CDC NTD prevention |

AMH is communicated with an explicit caveat (it predicts ovarian *response*, not natural
conception). The preconception TSH target tightening is a **band override**, not a new pillar.

### 2.3 Pregnancy (trimester-structured) — dedicated bands, reservoirs, and cascade tuning

Pregnancy is a distinct physiological state, not a perturbation of the non-pregnant cohort. When
`pregnant = true`, the engine swaps in **trimester-specific bands**, activates the **pregnancy
reservoir set** (§2.3.3), raises `W_k` for CV/HEM/MET/ENDO/MCS, and **tunes the critical cascade**
(§2.3.4). Gestational age (weeks) drives band selection.

#### 2.3.1 Trimester-specific reference bands (illustrative, re-verify; ACOG / Endocrine Society)

| Marker | Non-pregnant | T1 (0–13 wk) | T2 (14–27 wk) | T3 (28 wk–term) | Notes |
|--------|--------------|--------------|----------------|------------------|-------|
| Hemoglobin (g/dL) — green floor | ≥12 | ≥11.0 | **≥10.5** | ≥11.0 | physiologic hemodilution; anemia thresholds shift (ACOG/CDC) |
| TSH (mIU/L) | 0.5–4.5 | ~0.1–2.5 | ~0.2–3.0 | ~0.3–3.0 | trimester/assay-specific; **use lab kit ranges** (Endocrine Society / ATA pregnancy) |
| Systolic / Diastolic BP | <120/<80 opt | <140/<90 | <140/<90 | <140/<90 | **≥140/≥90 = surveillance trigger; see §2.3.4** |
| Fasting glucose (GDM screen) | 70–99 | — | **≥92 mg/dL abnormal** | (per OGTT) | ADA/IADPSG one-step thresholds |
| 1-h 75 g OGTT | — | — | **≥180 mg/dL** | — | IADPSG/ADA (any one value abnormal ⇒ GDM) |
| 2-h 75 g OGTT | — | — | **≥153 mg/dL** | — | IADPSG/ADA |
| Proteinuria (UACR/PCR or dipstick) | <30 mg/g | watch | watch | **≥300 mg/24h or PCR ≥0.3 = abnormal** | pre-eclampsia criterion (ACOG) |
| Platelets | 150–400 | mild ↓ normal | mild ↓ normal | **<100 = HELLP concern** | feeds §2.3.4 cascade |

Gestational-diabetes thresholds support both the **one-step 75 g** (IADPSG/ADA: fasting ≥92, 1-h
≥180, 2-h ≥153) and **two-step** (50 g screen → 100 g OGTT) pathways; the engine records which
protocol the clinician used and scores against the matching thresholds.

#### 2.3.2 Gestational weight-gain bands (IOM/NASEM, by pre-pregnancy BMI — illustrative, re-verify)

| Pre-pregnancy BMI | Total recommended gain | Engine handling |
|-------------------|------------------------|-----------------|
| <18.5 (under) | 12.5–18 kg | trajectory vs week, not a single weigh-in |
| 18.5–24.9 (normal) | 11.5–16 kg | green corridor by gestational week |
| 25–29.9 (over) | 7–11.5 kg | — |
| ≥30 (obese) | 5–9 kg | low gain not auto-penalized if fetal growth normal (clinician context) |

Weight is scored as a **corridor over gestational age** (BCM pillar), not against a fixed BMI band;
deviation above/below corridor is yellow and routed to clinician, never an autonomous red.

#### 2.3.3 Pregnancy reservoir set (extends Doc 04 §2; activated only when `pregnant`)

| `j` | Reservoir | Polarity | Primary inflows | Decay `λ` | Feeds |
|-----|-----------|----------|-----------------|-----------|-------|
| PEB | Pre-eclampsia burden | burden | BP·time, proteinuria, uric acid, platelet drop, sFlt-1/PlGF (if available) | does **not** self-decay antepartum; resolves post-delivery | CV, REN, ENDO; arms §2.3.4 cascade |
| GLYP | Gestational glycemic burden | burden | GDM thresholds, CGM-TIR-pregnancy | resolves postpartum (re-screen) | MET, CV |
| IRON-P | Pregnancy iron/oxygen demand | asset (depleting) | fetal/placental iron draw, dietary iron | slow refill | HEM, NUT |
| FETAL | Fetal-growth surveillance | context | fundal height, growth scans (clinician) | — | not autonomously scored |

PEB is intentionally **low-leakage antepartum**: pre-eclampsia risk accumulates and is not "averaged
away" by a good day — mirroring Doc 04's reversibility realism. It drains only after delivery.

#### 2.3.4 Critical-cascade tuning — pre-eclampsia / HELLP escalation
The pre-eclampsia critical-cascade is the headline pregnancy safety rule and is **emergency-tagged**
(Doc 03 §4.1):

```
 if pregnant and gestational_age ≥ 20 wk:
    if  SBP ≥ 160 or DBP ≥ 110:                       # severe-range BP
         ENDO/CV → critical, escalation = EMERGENCY
    elif (SBP ≥ 140 or DBP ≥ 90) and proteinuria ≥ 0.3:   # classic pre-eclampsia dyad
         critical, escalation = EMERGENCY
    elif (SBP ≥ 140 or DBP ≥ 90) and severe-feature*:     # *plt<100, ↑LFTs(2× ULN), Cr↑,
         critical, escalation = EMERGENCY                  #  visual/CNS sx, pulmonary edema
    elif (SBP ≥ 140 or DBP ≥ 90):                          # new gestational HTN
         yellow→red, escalation = URGENT (clinician review)
```

This realizes the brief's requirement: **BP + proteinuria → pre-eclampsia emergency escalation**,
with `PURE_CRIT_CAP = 40` (Doc 03 §5.3) and immediate human handoff (Doc 11). Severe-range BP alone
escalates even without proteinuria (current ACOG criteria — illustrative, re-verify). The hard
suicidality rule (Doc 02 MCS) and acute-sepsis rule remain active throughout pregnancy.

### 2.4 Post-partum

Activated for ~12 months post-delivery (the "fourth trimester" extending to a year, per ACOG).

- **Recovery corridor:** BP, weight, hemoglobin tracked back toward non-pregnant bands; PEB and GLYP
  reservoirs drain (re-screen GDM 4–12 wk postpartum with 75 g OGTT — abnormal ⇒ MET flag, future
  T2D risk).
- **Mood — EPDS in addition to PHQ-9.** The MCS pillar adds the **Edinburgh Postnatal Depression
  Scale (EPDS)**; EPDS ≥10 = yellow, **≥13 = red** (illustrative, re-verify). **EPDS item-10
  (self-harm) > 0** forces MCS critical with crisis escalation — the **same hard suicidality rule
  as PHQ-9 item-9 (Doc 02 MCS) still applies, additively, never relaxed.**
- **Post-partum thyroiditis:** transient hyper- then hypothyroid; TSH/FT4 watched 3–12 months;
  abnormal ⇒ ENDO yellow→red per thyroid bands, clinician-routed.
- **Anemia:** post-partum hemoglobin recovery tracked; persistent low ⇒ HEM, iron repletion (NUT).
- **Lactation & nutrition pillar:** if breastfeeding, NUT raises targets (energy, iodine, choline,
  vitamin D, continued folate; hydration) and the nudge engine (Doc 07) avoids aggressive
  caloric-deficit nudges; certain medication/alcohol nudges gain lactation-safety caveats. Iodine
  and vitamin D bands shift to lactation reference.

### 2.5 Perimenopause / menopause — STRAW+10 staging

Staging follows **STRAW+10** (Stages of Reproductive Aging Workshop). The engine assigns a stage
from menstrual-cycle criteria (primary) plus supportive FSH:

| STRAW+10 stage | Cycle criterion | FSH / estradiol trend | Engine focus |
|----------------|-----------------|------------------------|--------------|
| −3 to −1 (late reproductive → late menopausal transition) | subtle then ≥7-day cycle variation; ≥60-day amenorrhea (−1) | FSH rising/variable, estradiol variable | phase-bands widen; flag VMS, sleep, mood |
| +1 (early postmenopause) | ≥12 mo amenorrhea | FSH high (>25–40 IU/L), estradiol low | bone-loss acceleration window |
| +2 (late postmenopause) | — | FSH high, estradiol low/stable | long-term CV/bone |

Stage-driven interpretation and surveillance (illustrative, re-verify):

- **FSH/estradiol trajectory:** in the transition FSH is **variable** — a single high FSH is *not*
  scored as red; the engine reads the *trajectory* and cycle pattern, not one draw.
- **Vasomotor / sleep / mood:** VMS frequency, sleep fragmentation (SLP), and mood (MCS) tracked;
  these are expected and routed to support/plans, not pathologized — but severe mood still obeys the
  hard MCS rules.
- **Accelerated bone loss → BON reserve.** Postmenopausal estrogen withdrawal accelerates bone loss;
  the engine **increases the BON reserve depletion rate** (Doc 04 `λ`/inflow for BON) and raises BCM
  weight; BMD/T-score and FRAX (Doc 08) gain salience. Low estradiol here is interpreted via bone &
  CV risk, not as a benign number.
- **Cardiovascular risk inflection:** CV weight `W_CV` rises across the transition (loss of
  estrogen's vascular protection); lipid/BP bands unchanged but pillar salience up.
- **MHT-aware interpretation:** if on menopausal hormone therapy (MHT), estradiol is interpreted
  against the **treated** target and VMS/bone benefits credited; the engine does not flag
  therapeutic estradiol as abnormal. MHT route/type recorded; CV/VTE/breast surveillance per
  clinician.

---

## 3. Male model

Driven by `sex_at_birth = male` (and GAHT logic §1.2 for transmasculine patients with male
hormonal target).

### 3.1 Testosterone / SHBG / free-T with age decline (illustrative, re-verify; Endocrine Society 2018)

| Marker | Optimal | Yellow | Red | Notes |
|--------|---------|--------|-----|-------|
| Total testosterone (ng/dL) | 300–1000 (morning, fasting) | 230–299 | <230 (confirmed ×2) | **morning, two confirmed low draws** before any low-T flag (Endocrine Society) |
| SHBG (nmol/L) | 20–60 | borderline | extremes | needed to interpret free-T |
| Calculated free T (ng/dL) | ~5–21 | low-normal | low | use when SHBG abnormal (obesity, age, illness) |
| LH/FSH | normal | — | — | distinguishes primary vs secondary hypogonadism (clinician) |

**Age decline is normalized, not pathologized.** Total T declines ~1%/yr after ~30–40; the
age-banded cohort percentile (Doc 03 §2) means age-typical decline is not over-penalized, while a
*symptomatic* + biochemically low pattern is flagged. PureScore never diagnoses hypogonadism — it
surfaces the pattern (low morning T ×2 + symptoms via PRO) for clinician evaluation.

### 3.2 Andropause / sexual & prostate context
- **Andropause (late-onset hypogonadism):** scored as the T-trajectory + symptom pattern above;
  routed to ENDO yellow with clinician referral, **routine** escalation (reserve-style, not
  emergency).
- **Sexual health:** PRO items (libido, erectile function as a vascular/endocrine sentinel) feed MCS
  and can prompt CV evaluation (ED as early vascular signal).
- **PSA — strong caveats.** PSA is **context, not a score-it-and-forget marker**. Age-referenced;
  influenced by BPH, prostatitis, recent ejaculation, instrumentation, finasteride (halves PSA).
  The engine treats elevated/rising PSA as a **clinician-routed flag with explicit
  false-positive/overdiagnosis caveat** (per USPSTF shared-decision framing), never an autonomous
  red, and only if a prostate is present (§1.1).

### 3.3 Male-specific reservoir notes
No new male reservoir set is required; the standard Doc 04 reservoirs apply. Notes: lower estrogen
means the **BON** reserve depletes more slowly than in postmenopausal females (different `λ`);
testosterone supports **MUS** reserve (low T accelerates MUS depletion); erythrocytosis risk on
exogenous testosterone links to **HEM** (high hematocrit red).

---

## 4. How sex / stage modifies the four engine surfaces

This section is the precise mapping back to Docs 02–04. All multipliers are **defaults, versioned,
re-verify on cadence** (README §5.6; Doc 09 calibrates).

### 4.1 Pillar weights `W_k` — life-stage multipliers `m_k^{life-stage}` (Doc 03 §5.1)
`m_k^{life-stage}` is a new factor in `W_k = W_k^base · m_k^cohort · m_k^{life-stage} · m_k^goal ·
m_k^acute` (extends Doc 03 §5.1; renormalized to Σ=1).

| Stage | Raised pillars (`m > 1`) | Rationale |
|-------|--------------------------|-----------|
| Cycle (reproductive) | ENDO, MCS (cyclical mood), MET (if PCOS phenotype) | phase-aware ENDO salience |
| Preconception | ENDO, NUT, HEM | folate/iron/vit D, TSH target |
| Pregnancy | **CV, HEM, MET, ENDO, MCS** | pre-eclampsia, anemia, GDM, thyroid, mood |
| Post-partum | MCS (EPDS), HEM, ENDO (thyroid), NUT (lactation) | fourth-trimester risks |
| Perimenopause/menopause | **CV, BCM (bone), SLP, MCS** | CV inflection, bone loss, VMS/sleep/mood |
| Male age decline | ENDO, BCM (MUS), CV | T decline, sarcopenia, vascular |

### 4.2 ENDO marker bands
Sex-hormone bands become **functions of phase/trimester/STRAW+10 stage/HT** (§2–§3 tables), fed to
Doc 03 Stage 1 as `[L_i^opt(stage), U_i^opt(stage)]`. Thyroid TSH targets shift for preconception
(0.5–2.5) and pregnancy (trimester-specific). This is the realization of the deferral noted in
Doc 02 Pillar 7.

### 4.3 Reservoir set
- Pregnancy activates **PEB, GLYP, IRON-P, FETAL** (§2.3.3).
- **BON** reserve `λ`/inflow modified by estrogen status: accelerated depletion postmenopause;
  functional hypothalamic amenorrhea and low energy availability also **drain BON** (and MUS) — so a
  low-estradiol athlete is correctly penalized via reserve depletion, not reassured by a low number.
- **ALLO** (allostatic load) couples to ENDO across all stages (Doc 04 `κ_{ALLO,ENDO}`).

### 4.4 Escalation thresholds
- Pregnancy adds the **pre-eclampsia/HELLP emergency cascade** (§2.3.4).
- Post-partum adds **EPDS** as a second depression instrument with its own self-harm hard rule
  (§2.4), additive to the PHQ-9 item-9 rule.
- All sex-specific reds still route through Doc 11 with the correct `escalation ∈ {emergency,
  urgent, routine}` tag (Doc 03 §4.1).

---

## 5. Reference-band appendix (compact, illustrative — **re-verify on cadence**)

> Single-source-of-truth quick table. **Every value here is illustrative and literature-anchored as
> of the cited bodies (ACOG, Endocrine Society, STRAW+10, ESHRE/ASRM, ATA, ADA/IADPSG, IOM/NASEM,
> USPSTF) and MUST be re-verified and assay-matched before production (README §5.6).** Assays differ;
> the engine stores lab-kit ranges, not these print values.

### 5.1 Female cycle (serum)
| Marker | Follicular | Ovulatory | Luteal |
|--------|-----------|-----------|--------|
| Estradiol (pg/mL) | 20–150 | 150–400 | 50–250 |
| Progesterone (ng/mL) | <1 | 1–3 | >3 (mid-luteal ≥10 confirms ovulation) |
| LH (IU/L) | 2–12 | 15–80 (surge) | 1–12 |
| FSH (IU/L) | 3–12 (d3) | mid-rise | 1.5–8 |

### 5.2 Pregnancy
| Marker | T1 | T2 | T3 |
|--------|----|----|----|
| Hemoglobin green floor (g/dL) | ≥11.0 | ≥10.5 | ≥11.0 |
| TSH (mIU/L) | ~0.1–2.5 | ~0.2–3.0 | ~0.3–3.0 (assay-specific) |
| GDM (75 g) | — | fasting ≥92 / 1h ≥180 / 2h ≥153 = abnormal | — |
| Pre-eclampsia | BP ≥140/90 **+** protein ≥0.3 ⇒ EMERGENCY; BP ≥160/110 alone ⇒ EMERGENCY | | |
| Weight gain (normal BMI) | corridor → total 11.5–16 kg | | |

### 5.3 Menopause (STRAW+10) & male
| Marker | Postmenopause | Male (morning) |
|--------|---------------|----------------|
| FSH (IU/L) | >25–40 (high) | normal |
| Estradiol (pg/mL) | low/stable (or MHT target) | — |
| Total T (ng/dL) | — | 300–1000; <230 confirmed ×2 = low |
| SHBG (nmol/L) | — | 20–60 |
| PSA | — | age-referenced, **caveated**, clinician-routed |

### 5.4 Preconception overrides
| Marker | Target |
|--------|--------|
| TSH | 0.5–2.5 mIU/L |
| Folic acid | 400 µg/day (≥4 mg if prior NTD) |
| 25-OH vit D | 30–60 ng/mL |
| Ferritin / iron sat | ≥30 ng/mL / 25–45% |

---

## 6. Equity & safety notes for reproductive data (ties to Doc 11)

1. **Data minimization / do not over-collect.** Pregnancy, fertility, cycle, and gender data are
   collected **only** when they materially improve the score or care, with explicit purpose, and
   are **not** retained beyond need. The engine never *requires* reproductive disclosure to produce
   a score — it degrades to lower confidence (§1) instead.
2. **Heightened privacy.** Pregnancy and fertility status are sensitive in many jurisdictions
   (employment, insurance, legal). These fields get the strictest access controls, are excluded
   from the actuarial/payer layer (Doc 10) and from any pricing/access decision, and are never used
   to *worsen* access, pricing, or care (README §5.5). Granular consent and revocation are
   honored per Doc 11.
3. **Gender dignity.** `gender_identity` controls all communication; misgendering is treated as a
   defect. Physiological bands are explained in clinical terms tied to `organ_inventory`/hormonal
   milieu, never as a verdict on identity.
4. **No autonomous reproductive diagnosis.** PCOS, FHA, hypogonadism, pre-eclampsia, GDM, thyroid
   disease, and prostate concerns are **flagged with explanation and escalated**, never diagnosed by
   the score (README §5.1). Uncertainty defaults to caution (README §5.4).
5. **Equity of reference ranges.** Where population ranges were derived in non-representative
   cohorts, affected bands are flagged for re-derivation (Doc 09 fairness/drift); intersex and
   gender-diverse patients use individual-baseline scoring rather than a forced population band
   (§1.3).
6. **All thresholds re-verified on cadence** before production (README §5.6); reproductive guidance
   changes (e.g. ACOG/ADA updates) trigger a model-version bump (Doc 09/11).

---

## 7. Implementation status (interactive calculator) — decision D15

This document is the **authoritative production reference**; the calculator
(`purescore-architecture.html`) implements the resolution *mechanism* live on a marker subset so the
behaviour is inspectable. Mapping to the spec above:

| Spec concept (this doc) | Calculator realization |
|---|---|
| `sex_at_birth` × `gender_identity` × `hormone_therapy` × stage (§1) | `CTX = {natal, hrt, stage}` + header selectors (Natal sex · Hormones · Life stage) |
| Hormone-therapy-aware ranges (§1.2); affirmed-milieu resolution | `gsex()` (governing milieu) drives `bySex` band selection; established-HRT assumed for the demo |
| Trimester / menopause / andropause modifiers (§2.3, §2.5, §3) | `STAGE_MODS` (preg T1/T2/T3, postpartum, peri/postmenopause, andropause) applied in `band()` |
| Per-marker axis {none, gonadal, natal} (§1) | `bySex` presence marks gonadal markers (Hgb, waist, grip, VO₂max, sex hormones in the subset) |
| Pre-eclampsia/GDM **absolute anchors** (§2.3.4) | acute-danger anchors enforced in `pillarRisk` regardless of stage |
| Confidence reduction for under-represented cohorts (§1.2, §6) | `rep` lowers Confidence for trans/intersex/pregnancy personas (D7) |

**Demonstration personas:** `pregnancy` (T2 frame), `transfem` (feminizing HRT → female gonadal
ranges; Hgb 12.8 reads normal-female but a binary-male toggle would false-flag it anemic),
`transmasc` (masculinizing HRT → male ranges; erythrocytosis watch), `menopause` (postmenopausal
CV/bone up-weight, BMD red sooner). The full production system applies the §3 classification and
§4–5 tables across the entire Doc 02 catalogue. Every numeric band remains **illustrative pending
clinical sign-off and Doc 13 validation** — the Babylon lesson (Doc 00).
