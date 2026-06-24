-- ============================================================================
-- Cohort percentiles — ClickHouse schema for the PureScore cohort-median engine.
-- Each row = one (cohort cell) × (marker) → percentile distribution + cell size.
-- The engine reads p50 as the cohort median for graceful-degradation imputation
-- (degradation-model.json), and the p5..p95 spread for the percentile-CDF risk
-- and OOD / k-anonymity gating (cohort-governance.json).
--
-- Sample data is loaded from data/cohort-percentiles.json (generated, ILLUSTRATIVE).
-- Re-derive from NHANES / UK-Biobank-class + local UAE cohorts before production.
-- ============================================================================

CREATE DATABASE IF NOT EXISTS purescore;

-- ---- main fact table -------------------------------------------------------
CREATE TABLE IF NOT EXISTS purescore.cohort_percentiles
(
    cohort_version  LowCardinality(String),                 -- e.g. '2026.06-demo' (pin per score for reproducibility)
    age_band        LowCardinality(String),                 -- child | adolescent | 18-39 | 40-64 | 65-79 | 80+
    sex             LowCardinality(String),                 -- male | female
    life_stage      LowCardinality(String),                 -- general | pregnancy
    ethnicity       LowCardinality(String),                 -- emirati_gulf_arab | south_asian | other_arab | filipino_se_asian | african | iranian | western
    marker          LowCardinality(String),                 -- apob | hba1c | egfr | ...
    unit            LowCardinality(String),
    p5              Float64,
    p25             Float64,
    p50             Float64,                                 -- cohort MEDIAN (the imputation value)
    p75             Float64,
    p95             Float64,
    n               UInt32,                                  -- cell size; k-anonymity floor n >= 20
    source          LowCardinality(String),                 -- illustrative-demo | illustrative-pediatric | <dataset>
    updated_at      DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)                     -- newest version of a cell wins
ORDER BY (cohort_version, marker, age_band, sex, life_stage, ethnicity);

-- ---- load the sample JSON (one object per row) -----------------------------
-- clickhouse-client --query "INSERT INTO purescore.cohort_percentiles FORMAT JSONEachRow" \
--   < <(jq -c '.rows[]' data/cohort-percentiles.json)
-- (the .rows[] array matches the column names exactly; updated_at defaults to now()).

-- ---- a k-anonymous, usable-cells view (engine reads this) ------------------
CREATE VIEW IF NOT EXISTS purescore.cohort_percentiles_usable AS
SELECT * FROM purescore.cohort_percentiles
WHERE n >= 20;                                              -- below the k-anon floor → not servable (back off / OOD)

-- ---- engine median lookup with HIERARCHICAL BACK-OFF -----------------------
-- Resolve the cohort median for a (marker, age_band, sex, life_stage, ethnicity),
-- falling back age×sex×life×eth → age×sex×eth(general) → age×sex → sex → global,
-- always honouring the k-anon floor; the coarsest level that has n>=20 wins.
-- Parameterised example (the app supplies {marker,age,sex,ls,eth}):
--
-- SELECT marker, p50, n, age_band, sex, life_stage, ethnicity,
--        multiIf(
--          age_band={age:String} AND sex={sex:String} AND life_stage={ls:String} AND ethnicity={eth:String}, 5,
--          age_band={age:String} AND sex={sex:String} AND life_stage='general'  AND ethnicity={eth:String}, 4,
--          age_band={age:String} AND sex={sex:String}, 3,
--          sex={sex:String}, 2, 1) AS specificity
-- FROM purescore.cohort_percentiles_usable
-- WHERE marker={marker:String}
-- ORDER BY specificity DESC
-- LIMIT 1 BY marker;                                       -- most-specific usable cell per marker

-- ---- drift monitor feed (PSI/KL vs incoming real data) ---------------------
-- A scheduled job compares each new cohort_version against the prior to flag
-- variance shrinkage / autophagy (cohort-governance §1, degradation-integrity).
