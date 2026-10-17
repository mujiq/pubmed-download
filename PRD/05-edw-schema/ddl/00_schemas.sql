-- =============================================================================
-- HEALIX EDW · module 05 · 00_schemas.sql
-- Creates every EDW schema (database) for the four layers and the nine
-- subject-area groups G1–G9 (01-architecture-overview.md §4, EDW-00 §3).
--
-- Dialect: portable ANSI SQL. Teradata-specific rendering is given in
-- "-- TD:" hint comments; the generated Teradata variant lives in
-- ddl/dialects/teradata/ (04-physical-model-and-ddl.md §1).
--
-- Apply order for the reference DDL (FK targets must exist first):
--   00_schemas.sql → g8_reference.sql → g9_governance.sql → g1_party.sql
--   → g3_finance.sql → g4_supply.sql → g2_clinical.sql
-- The single circular dependency (G3 fact_charge → G4 dim_item) is added by
-- ALTER TABLE at the end of g4_supply.sql.
--
-- Idempotency: the migration runner guards each statement against the
-- catalog (04-physical-model-and-ddl.md MIG-2); plain statements are kept
-- here so the file parses on every target.
-- =============================================================================

-- TD: CREATE DATABASE edw_root FROM dbc AS PERM = 50e12, SPOOL = 10e12, FALLBACK;
-- TD: every schema below is "CREATE DATABASE <name> FROM edw_root AS PERM = <n>";

-- ---------------------------------------------------------------------------
-- Cross-layer schemas (EDW-ARCH-001)
-- ---------------------------------------------------------------------------
CREATE SCHEMA edw_stg;            -- landing / staging: stg_* append-only Kafka copies (90-day retention)
-- TD: CREATE DATABASE edw_stg FROM edw_root AS PERM = 5e12;

CREATE SCHEMA edw_int;            -- integration: int_* 3NF, hist_int_* bitemporal, source grain
-- TD: CREATE DATABASE edw_int FROM edw_root AS PERM = 15e12;

CREATE SCHEMA edw_sem;            -- semantic: generated v_* views and as-of table functions only
-- TD: CREATE DATABASE edw_sem FROM edw_root AS PERM = 1e9;

-- ---------------------------------------------------------------------------
-- G1 · Party & Identity (owns conformed dim_patient, dim_practitioner,
--      dim_organization, dim_location, dim_payer)
-- ---------------------------------------------------------------------------
CREATE SCHEMA edw_g1_party;
-- TD: CREATE DATABASE edw_g1_party FROM edw_root AS PERM = 2e12;

-- ---------------------------------------------------------------------------
-- G2 · Clinical Care
-- ---------------------------------------------------------------------------
CREATE SCHEMA edw_g2_clinical;    -- encounter-centric facts, diagnoses, procedures, orders, documents
-- TD: CREATE DATABASE edw_g2_clinical FROM edw_root AS PERM = 6e12;

CREATE SCHEMA edw_g2_medication;  -- medication order → dispense → administration lifecycle
-- TD: CREATE DATABASE edw_g2_medication FROM edw_root AS PERM = 3e12;

CREATE SCHEMA edw_g2_diagnostics; -- observations, diagnostic reports, specimens, imaging-study metadata
-- TD: CREATE DATABASE edw_g2_diagnostics FROM edw_root AS PERM = 8e12;

-- ---------------------------------------------------------------------------
-- G3 · Revenue Cycle & Finance
-- ---------------------------------------------------------------------------
CREATE SCHEMA edw_g3_revenue;     -- patient accounting, charges, coverage, claims, remittance, payments
-- TD: CREATE DATABASE edw_g3_revenue FROM edw_root AS PERM = 4e12;

CREATE SCHEMA edw_g3_finance;     -- GL, AP, AR, budget, fixed assets, projects; owns conformed dim_cost_center
-- TD: CREATE DATABASE edw_g3_finance FROM edw_root AS PERM = 3e12;

-- ---------------------------------------------------------------------------
-- G4 · Supply Chain & Operations
-- ---------------------------------------------------------------------------
CREATE SCHEMA edw_g4_supply;      -- items, suppliers, procurement, inventory; owns conformed dim_item
-- TD: CREATE DATABASE edw_g4_supply FROM edw_root AS PERM = 3e12;

CREATE SCHEMA edw_g4_operations;  -- beds, assets, schedules, appointments, transport
-- TD: CREATE DATABASE edw_g4_operations FROM edw_root AS PERM = 1e12;

-- ---------------------------------------------------------------------------
-- G5 · Workforce
-- ---------------------------------------------------------------------------
CREATE SCHEMA edw_g5_workforce;
-- TD: CREATE DATABASE edw_g5_workforce FROM edw_root AS PERM = 1e12;

-- ---------------------------------------------------------------------------
-- G6 · Population Health & Quality
-- ---------------------------------------------------------------------------
CREATE SCHEMA edw_g6_pophealth;
-- TD: CREATE DATABASE edw_g6_pophealth FROM edw_root AS PERM = 2e12;

-- ---------------------------------------------------------------------------
-- G7 · Research & Analytics
-- ---------------------------------------------------------------------------
CREATE SCHEMA edw_g7_research;
-- TD: CREATE DATABASE edw_g7_research FROM edw_root AS PERM = 2e12;

-- ---------------------------------------------------------------------------
-- G8 · Reference & Terminology (owns conformed dim_date, dim_time, dim_code,
--      dim_source_system)
-- ---------------------------------------------------------------------------
CREATE SCHEMA edw_g8_reference;
-- TD: CREATE DATABASE edw_g8_reference FROM edw_root AS PERM = 1e12;

-- ---------------------------------------------------------------------------
-- G9 · Metadata, Quality & Governance
-- ---------------------------------------------------------------------------
CREATE SCHEMA edw_g9_governance;
-- TD: CREATE DATABASE edw_g9_governance FROM edw_root AS PERM = 4e12;
