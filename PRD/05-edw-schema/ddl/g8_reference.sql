-- =============================================================================
-- HEALIX EDW · module 05 · g8_reference.sql
-- G8 Reference & Terminology · schema edw_g8_reference
-- Tables: dim_date, dim_time, ref_code_system, dim_code, ref_concept,
--         ref_concept_hierarchy, ref_value_set, ref_value_set_member,
--         ref_concept_map, ref_concept_map_element, dim_ucum_unit,
--         dim_geography, dim_service_line, dim_source_system
-- Specs:  01-subject-area-groups.md §G8 · 05-conformed-dimensions.md §5, §9, §10
-- Dialect: portable ANSI; Teradata hints as "-- TD:" comments.
-- Ontology base IRI: https://ontology.healix.example.org/
-- Column comment format: '<phi_class> | <ontology_property_iri>' (EDW-DICT-003)
-- =============================================================================

-- ---------------------------------------------------------------------------
-- dim_source_system (conformed; owned by G8 per ARC-01 §4; referenced by
-- every presentation table's technical block). Created first because every
-- other table's source_system_sk references it.
-- ---------------------------------------------------------------------------
CREATE TABLE edw_g8_reference.dim_source_system (
    source_system_sk        INTEGER       NOT NULL,
    source_system_bk        VARCHAR(128)  NOT NULL,   -- <vendor>:<product>:<instance>
    source_system_code      VARCHAR(30)   NOT NULL,   -- e.g. CERNER_PROD, OFC_ERP_PROD
    source_system_name      VARCHAR(255)  NOT NULL,
    vendor_name             VARCHAR(100),
    product_name            VARCHAR(100),
    product_version         VARCHAR(50),
    system_category_code    VARCHAR(20)   NOT NULL,   -- ehr/erp/hcm/scm/lis/ris/pharmacy/rcm/hie/portal/device/payer/platform
    environment_code        VARCHAR(10)   NOT NULL,   -- prod/uat/test/dev
    default_timezone        VARCHAR(64)   NOT NULL,   -- IANA zone for local date derivation (EDW-CONF-009)
    kafka_domain_codes      VARCHAR(255),
    owner_steward           VARCHAR(100),
    registry_iri            VARCHAR(512),             -- module-03 Source Registry IRI
    active_flag             CHAR(1)       NOT NULL DEFAULT 'Y',
    onboarded_date          DATE,
    decommissioned_date     DATE,
    -- valid-time block (Type 2)
    valid_from              TIMESTAMP     NOT NULL,
    valid_to                TIMESTAMP,
    is_current_flag         CHAR(1)       NOT NULL DEFAULT 'Y',
    -- technical block
    source_system_sk_origin INTEGER       NOT NULL,   -- platform component that loaded this row (self-reference)
    load_batch_id           BIGINT        NOT NULL,
    record_hash             CHAR(64)      NOT NULL,
    recorded_at             TIMESTAMP     NOT NULL,
    prov_activity_iri       VARCHAR(512)  NOT NULL,
    CONSTRAINT pk_dim_source_system PRIMARY KEY (source_system_sk),
    CONSTRAINT fk_dim_src_sys_origin FOREIGN KEY (source_system_sk_origin)
        REFERENCES edw_g8_reference.dim_source_system (source_system_sk)
);
-- TD: CREATE SET TABLE edw_g8_reference.dim_source_system, FALLBACK (...) UNIQUE PRIMARY INDEX (source_system_sk)
CREATE UNIQUE INDEX ux_dim_source_system_bk ON edw_g8_reference.dim_source_system (source_system_bk, valid_from);
COMMENT ON TABLE edw_g8_reference.dim_source_system IS 'https://ontology.healix.example.org/governance/SourceSystem';
COMMENT ON COLUMN edw_g8_reference.dim_source_system.source_system_bk IS 'none | https://ontology.healix.example.org/governance/sourceSystemIdentifier';
COMMENT ON COLUMN edw_g8_reference.dim_source_system.default_timezone IS 'none | https://ontology.healix.example.org/governance/defaultTimeZone';
COMMENT ON COLUMN edw_g8_reference.dim_source_system.registry_iri IS 'none | https://ontology.healix.example.org/governance/sourceRegistryEntry';

INSERT INTO edw_g8_reference.dim_source_system
    (source_system_sk, source_system_bk, source_system_code, source_system_name, system_category_code, environment_code,
     default_timezone, active_flag, valid_from, is_current_flag, source_system_sk_origin, load_batch_id, record_hash,
     recorded_at, prov_activity_iri)
VALUES
    (-1, 'UNKNOWN', 'UNKNOWN', 'Unknown source system', 'platform', 'prod', 'UTC', 'N',
     TIMESTAMP '1900-01-01 00:00:00', 'Y', -1, 0, 'SEED', TIMESTAMP '1900-01-01 00:00:00',
     'https://ontology.healix.example.org/prov/activity/seed/dim_source_system'),
    (-2, 'NOT_APPLICABLE', 'N/A', 'Not applicable', 'platform', 'prod', 'UTC', 'N',
     TIMESTAMP '1900-01-01 00:00:00', 'Y', -1, 0, 'SEED', TIMESTAMP '1900-01-01 00:00:00',
     'https://ontology.healix.example.org/prov/activity/seed/dim_source_system'),
    (1, 'healix:platform:edw-loader', 'HEALIX_EDW', 'HEALIX EDW loader (derived rows)', 'platform', 'prod', 'UTC', 'Y',
     TIMESTAMP '1900-01-01 00:00:00', 'Y', 1, 0, 'SEED', TIMESTAMP '1900-01-01 00:00:00',
     'https://ontology.healix.example.org/prov/activity/seed/dim_source_system');

-- ---------------------------------------------------------------------------
-- dim_date (conformed, Type 0). date_sk = yyyymmdd. Rows 1900-01-01..2099-12-31
-- plus -1 unknown and -2 not applicable are generated by ddl/seed/gen_dim_date
-- (EDW-CONF-008); only the reserved members are inserted here.
-- ---------------------------------------------------------------------------
CREATE TABLE edw_g8_reference.dim_date (
    date_sk                     INTEGER       NOT NULL,
    calendar_date               DATE,
    day_of_week                 SMALLINT,     -- 1 = Monday (ISO)
    day_name                    VARCHAR(9),
    day_of_month                SMALLINT,
    day_of_year                 SMALLINT,
    week_of_year_iso            SMALLINT,
    month_number                SMALLINT,
    month_name                  VARCHAR(9),
    quarter_number              SMALLINT,
    calendar_year               SMALLINT,
    is_weekend_flag             CHAR(1)       NOT NULL DEFAULT 'N',
    is_us_federal_holiday_flag  CHAR(1)       NOT NULL DEFAULT 'N',
    holiday_name                VARCHAR(50),
    fiscal_year                 SMALLINT,
    fiscal_quarter              SMALLINT,
    fiscal_period               SMALLINT,
    fiscal_period_start_date    DATE,
    fiscal_period_end_date      DATE,
    cms_reporting_year          SMALLINT,
    hedis_measurement_year      SMALLINT,
    payer_period_label          VARCHAR(20),
    is_last_day_of_month_flag   CHAR(1)       NOT NULL DEFAULT 'N',
    days_in_month               SMALLINT,
    -- technical block (Type 0: no valid-time block)
    source_system_sk            INTEGER       NOT NULL DEFAULT 1,
    load_batch_id               BIGINT        NOT NULL,
    record_hash                 CHAR(64)      NOT NULL,
    recorded_at                 TIMESTAMP     NOT NULL,
    prov_activity_iri           VARCHAR(512)  NOT NULL,
    CONSTRAINT pk_dim_date PRIMARY KEY (date_sk),
    CONSTRAINT fk_dim_date_src FOREIGN KEY (source_system_sk)
        REFERENCES edw_g8_reference.dim_source_system (source_system_sk)
);
-- TD: CREATE SET TABLE edw_g8_reference.dim_date, FALLBACK (...) UNIQUE PRIMARY INDEX (date_sk)
-- TD: COLLECT STATISTICS COLUMN (date_sk), COLUMN (calendar_date), COLUMN (fiscal_year, fiscal_period) ON edw_g8_reference.dim_date;
CREATE UNIQUE INDEX ux_dim_date_calendar ON edw_g8_reference.dim_date (calendar_date);
COMMENT ON TABLE edw_g8_reference.dim_date IS 'https://ontology.healix.example.org/reference/CalendarDate';
COMMENT ON COLUMN edw_g8_reference.dim_date.calendar_date IS 'none | https://ontology.healix.example.org/reference/calendarDate';
COMMENT ON COLUMN edw_g8_reference.dim_date.fiscal_period IS 'none | https://ontology.healix.example.org/reference/fiscalPeriod';

INSERT INTO edw_g8_reference.dim_date
    (date_sk, calendar_date, day_name, month_name, source_system_sk, load_batch_id, record_hash, recorded_at, prov_activity_iri)
VALUES
    (-1, NULL, 'Unknown', 'Unknown', 1, 0, 'SEED', TIMESTAMP '1900-01-01 00:00:00',
     'https://ontology.healix.example.org/prov/activity/seed/dim_date'),
    (-2, NULL, 'N/A', 'N/A', 1, 0, 'SEED', TIMESTAMP '1900-01-01 00:00:00',
     'https://ontology.healix.example.org/prov/activity/seed/dim_date');

-- ---------------------------------------------------------------------------
-- dim_time (conformed, Type 0). time_sk = hhmm; 1,440 rows + -1 (EDW-CONF-008)
-- ---------------------------------------------------------------------------
CREATE TABLE edw_g8_reference.dim_time (
    time_sk                 SMALLINT      NOT NULL,
    time_of_day             TIME,
    hour_24                 SMALLINT,
    hour_12                 SMALLINT,
    minute_of_hour          SMALLINT,
    am_pm                   CHAR(2),
    shift_code              VARCHAR(10),   -- day / evening / night (enterprise default; G5 dim_shift_type for local)
    is_business_hours_flag  CHAR(1)       NOT NULL DEFAULT 'N',
    half_hour_slot          SMALLINT,
    quarter_hour_slot       SMALLINT,
    source_system_sk        INTEGER       NOT NULL DEFAULT 1,
    load_batch_id           BIGINT        NOT NULL,
    record_hash             CHAR(64)      NOT NULL,
    recorded_at             TIMESTAMP     NOT NULL,
    prov_activity_iri       VARCHAR(512)  NOT NULL,
    CONSTRAINT pk_dim_time PRIMARY KEY (time_sk),
    CONSTRAINT fk_dim_time_src FOREIGN KEY (source_system_sk)
        REFERENCES edw_g8_reference.dim_source_system (source_system_sk)
);
-- TD: UNIQUE PRIMARY INDEX (time_sk)
COMMENT ON TABLE edw_g8_reference.dim_time IS 'https://ontology.healix.example.org/reference/TimeOfDay';
COMMENT ON COLUMN edw_g8_reference.dim_time.shift_code IS 'none | https://ontology.healix.example.org/reference/shiftCode';

INSERT INTO edw_g8_reference.dim_time
    (time_sk, shift_code, source_system_sk, load_batch_id, record_hash, recorded_at, prov_activity_iri)
VALUES
    (-1, 'unknown', 1, 0, 'SEED', TIMESTAMP '1900-01-01 00:00:00',
     'https://ontology.healix.example.org/prov/activity/seed/dim_time');

-- ---------------------------------------------------------------------------
-- ref_code_system: one row per code system × version (EDW-G8-001)
-- ---------------------------------------------------------------------------
CREATE TABLE edw_g8_reference.ref_code_system (
    code_system_version_sk  BIGINT        NOT NULL,
    code_system_bk          VARCHAR(128)  NOT NULL,   -- canonical URI + '|' + version
    code_system_uri         VARCHAR(255)  NOT NULL,   -- e.g. http://snomed.info/sct, http://loinc.org
    code_system_name        VARCHAR(100)  NOT NULL,
    code_system_version     VARCHAR(50)   NOT NULL,
    release_date            DATE,
    publisher               VARCHAR(100),
    licence_class           VARCHAR(30),              -- open / affiliate-licence / commercial (SNOMED, CPT)
    is_local_flag           CHAR(1)       NOT NULL DEFAULT 'N',   -- Cerner code_set / Oracle lookup type
    hierarchy_meaning_code  VARCHAR(20),              -- is-a / part-of / grouped-by / classified-with
    concept_count           INTEGER,
    terminology_service_uri VARCHAR(512),             -- FHIR CodeSystem resource location
    skos_scheme_iri         VARCHAR(512),             -- Ontology Registry concept scheme
    is_current_flag         CHAR(1)       NOT NULL DEFAULT 'Y',
    source_system_sk        INTEGER       NOT NULL,
    load_batch_id           BIGINT        NOT NULL,
    record_hash             CHAR(64)      NOT NULL,
    recorded_at             TIMESTAMP     NOT NULL,
    prov_activity_iri       VARCHAR(512)  NOT NULL,
    CONSTRAINT pk_ref_code_system PRIMARY KEY (code_system_version_sk),
    CONSTRAINT fk_ref_code_system_src FOREIGN KEY (source_system_sk)
        REFERENCES edw_g8_reference.dim_source_system (source_system_sk)
);
-- TD: UNIQUE PRIMARY INDEX (code_system_version_sk)
CREATE UNIQUE INDEX ux_ref_code_system_bk ON edw_g8_reference.ref_code_system (code_system_bk);
COMMENT ON TABLE edw_g8_reference.ref_code_system IS 'https://ontology.healix.example.org/reference/CodeSystemVersion';
COMMENT ON COLUMN edw_g8_reference.ref_code_system.code_system_uri IS 'none | https://ontology.healix.example.org/reference/codeSystemUri';

-- ---------------------------------------------------------------------------
-- dim_code (conformed): one row per (code_system_uri, code); code_sk is stable
-- across terminology releases (EDW-CONF-013). Version-specific attributes are
-- in ref_concept. Latest-release attributes are denormalised here for BI.
-- ---------------------------------------------------------------------------
CREATE TABLE edw_g8_reference.dim_code (
    code_sk                 BIGINT        NOT NULL,
    code_bk                 VARCHAR(128)  NOT NULL,   -- <code_system_uri>|<code>
    code_system_uri         VARCHAR(255)  NOT NULL,
    code_system_name        VARCHAR(100)  NOT NULL,
    code                    VARCHAR(100)  NOT NULL,
    display                 VARCHAR(500),
    preferred_label         VARCHAR(500),             -- enterprise SKOS prefLabel when bound
    definition              VARCHAR(1000),
    concept_iri             VARCHAR(512),             -- SKOS concept IRI in the Ontology Registry
    concept_status          VARCHAR(10)   NOT NULL DEFAULT 'active',   -- active / inactive / retired (latest release)
    effective_from          DATE,
    effective_to            DATE,
    is_abstract_flag        CHAR(1)       NOT NULL DEFAULT 'N',
    parent_code_sk          BIGINT,                   -- primary parent; closure in ref_concept_hierarchy
    sensitive_category_code VARCHAR(30),              -- healix:scheme/sensitive-categories (drives phi_sensitive)
    latest_version_sk       BIGINT,                   -- ref_code_system row of the latest release seen
    source_system_sk        INTEGER       NOT NULL,
    load_batch_id           BIGINT        NOT NULL,
    record_hash             CHAR(64)      NOT NULL,
    recorded_at             TIMESTAMP     NOT NULL,
    prov_activity_iri       VARCHAR(512)  NOT NULL,
    CONSTRAINT pk_dim_code PRIMARY KEY (code_sk),
    CONSTRAINT fk_dim_code_parent FOREIGN KEY (parent_code_sk)
        REFERENCES edw_g8_reference.dim_code (code_sk),
    CONSTRAINT fk_dim_code_version FOREIGN KEY (latest_version_sk)
        REFERENCES edw_g8_reference.ref_code_system (code_system_version_sk),
    CONSTRAINT fk_dim_code_src FOREIGN KEY (source_system_sk)
        REFERENCES edw_g8_reference.dim_source_system (source_system_sk)
);
-- TD: CREATE SET TABLE edw_g8_reference.dim_code, FALLBACK (...) UNIQUE PRIMARY INDEX (code_sk)
-- TD: INDEX (code_system_uri, code); INDEX (concept_iri)
-- TD: COLLECT STATISTICS COLUMN (code_sk), COLUMN (code_system_uri, code), COLUMN (concept_iri) ON edw_g8_reference.dim_code;
CREATE UNIQUE INDEX ux_dim_code_bk ON edw_g8_reference.dim_code (code_bk);
CREATE INDEX ix_dim_code_system_code ON edw_g8_reference.dim_code (code_system_uri, code);
COMMENT ON TABLE edw_g8_reference.dim_code IS 'http://www.w3.org/2004/02/skos/core#Concept';
COMMENT ON COLUMN edw_g8_reference.dim_code.code IS 'none | http://www.w3.org/2004/02/skos/core#notation';
COMMENT ON COLUMN edw_g8_reference.dim_code.preferred_label IS 'none | http://www.w3.org/2004/02/skos/core#prefLabel';
COMMENT ON COLUMN edw_g8_reference.dim_code.concept_iri IS 'none | https://ontology.healix.example.org/reference/conceptIri';
COMMENT ON COLUMN edw_g8_reference.dim_code.sensitive_category_code IS 'none | https://ontology.healix.example.org/governance/sensitiveCategory';

INSERT INTO edw_g8_reference.dim_code
    (code_sk, code_bk, code_system_uri, code_system_name, code, display, concept_status, source_system_sk,
     load_batch_id, record_hash, recorded_at, prov_activity_iri)
VALUES
    (-1, 'healix:unknown|UNK', 'https://ontology.healix.example.org/codesystem/reserved', 'HEALIX reserved', 'UNK',
     'Unknown', 'active', 1, 0, 'SEED', TIMESTAMP '1900-01-01 00:00:00',
     'https://ontology.healix.example.org/prov/activity/seed/dim_code'),
    (-2, 'healix:reserved|NA', 'https://ontology.healix.example.org/codesystem/reserved', 'HEALIX reserved', 'NA',
     'Not applicable', 'active', 1, 0, 'SEED', TIMESTAMP '1900-01-01 00:00:00',
     'https://ontology.healix.example.org/prov/activity/seed/dim_code'),
    (-3, 'healix:reserved|PENDING', 'https://ontology.healix.example.org/codesystem/reserved', 'HEALIX reserved',
     'PENDING', 'Pending terminology resolution', 'active', 1, 0, 'SEED', TIMESTAMP '1900-01-01 00:00:00',
     'https://ontology.healix.example.org/prov/activity/seed/dim_code');

-- ---------------------------------------------------------------------------
-- ref_concept: one row per concept × code-system version
-- ---------------------------------------------------------------------------
CREATE TABLE edw_g8_reference.ref_concept (
    concept_version_sk      BIGINT        NOT NULL,
    code_sk                 BIGINT        NOT NULL,
    code_system_version_sk  BIGINT        NOT NULL,
    code                    VARCHAR(100)  NOT NULL,
    display                 VARCHAR(500),
    definition              VARCHAR(1000),
    concept_status          VARCHAR(10)   NOT NULL,
    effective_from          DATE,
    effective_to            DATE,
    is_abstract_flag        CHAR(1)       NOT NULL DEFAULT 'N',
    parent_code_sk          BIGINT,
    concept_iri             VARCHAR(512),
    property_json           VARCHAR(4000),            -- FHIR CodeSystem.concept.property[] as JSON text
    source_system_sk        INTEGER       NOT NULL,
    load_batch_id           BIGINT        NOT NULL,
    record_hash             CHAR(64)      NOT NULL,
    recorded_at             TIMESTAMP     NOT NULL,
    prov_activity_iri       VARCHAR(512)  NOT NULL,
    CONSTRAINT pk_ref_concept PRIMARY KEY (concept_version_sk),
    CONSTRAINT fk_ref_concept_code FOREIGN KEY (code_sk)
        REFERENCES edw_g8_reference.dim_code (code_sk),
    CONSTRAINT fk_ref_concept_version FOREIGN KEY (code_system_version_sk)
        REFERENCES edw_g8_reference.ref_code_system (code_system_version_sk),
    CONSTRAINT fk_ref_concept_parent FOREIGN KEY (parent_code_sk)
        REFERENCES edw_g8_reference.dim_code (code_sk),
    CONSTRAINT fk_ref_concept_src FOREIGN KEY (source_system_sk)
        REFERENCES edw_g8_reference.dim_source_system (source_system_sk)
);
-- TD: CREATE MULTISET TABLE edw_g8_reference.ref_concept, FALLBACK (...) PRIMARY INDEX (code_sk)
-- TD: COLLECT STATISTICS COLUMN (code_sk), COLUMN (code_system_version_sk, code) ON edw_g8_reference.ref_concept;
CREATE UNIQUE INDEX ux_ref_concept_version ON edw_g8_reference.ref_concept (code_system_version_sk, code);
COMMENT ON TABLE edw_g8_reference.ref_concept IS 'https://ontology.healix.example.org/reference/ConceptVersion';

-- ---------------------------------------------------------------------------
-- ref_concept_hierarchy: transitive closure (EDW-G8-004)
-- ---------------------------------------------------------------------------
CREATE TABLE edw_g8_reference.ref_concept_hierarchy (
    code_system_version_sk  BIGINT        NOT NULL,
    ancestor_code_sk        BIGINT        NOT NULL,
    descendant_code_sk      BIGINT        NOT NULL,
    depth                   SMALLINT      NOT NULL,   -- 0 = self
    source_system_sk        INTEGER       NOT NULL,
    load_batch_id           BIGINT        NOT NULL,
    recorded_at             TIMESTAMP     NOT NULL,
    prov_activity_iri       VARCHAR(512)  NOT NULL,
    CONSTRAINT pk_ref_concept_hierarchy PRIMARY KEY (code_system_version_sk, ancestor_code_sk, descendant_code_sk),
    CONSTRAINT fk_ref_hier_version FOREIGN KEY (code_system_version_sk)
        REFERENCES edw_g8_reference.ref_code_system (code_system_version_sk),
    CONSTRAINT fk_ref_hier_ancestor FOREIGN KEY (ancestor_code_sk)
        REFERENCES edw_g8_reference.dim_code (code_sk),
    CONSTRAINT fk_ref_hier_descendant FOREIGN KEY (descendant_code_sk)
        REFERENCES edw_g8_reference.dim_code (code_sk),
    CONSTRAINT fk_ref_hier_src FOREIGN KEY (source_system_sk)
        REFERENCES edw_g8_reference.dim_source_system (source_system_sk)
);
-- TD: PRIMARY INDEX (descendant_code_sk)
CREATE INDEX ix_ref_concept_hier_anc ON edw_g8_reference.ref_concept_hierarchy (ancestor_code_sk);
COMMENT ON TABLE edw_g8_reference.ref_concept_hierarchy IS 'http://www.w3.org/2004/02/skos/core#broaderTransitive';

-- ---------------------------------------------------------------------------
-- ref_value_set / ref_value_set_member (expanded; EDW-G8-002)
-- ---------------------------------------------------------------------------
CREATE TABLE edw_g8_reference.ref_value_set (
    value_set_version_sk    BIGINT        NOT NULL,
    value_set_bk            VARCHAR(128)  NOT NULL,   -- canonical URL + '|' + version
    value_set_url           VARCHAR(255)  NOT NULL,
    value_set_name          VARCHAR(255)  NOT NULL,
    value_set_version       VARCHAR(50)   NOT NULL,
    value_set_iri           VARCHAR(512),             -- Ontology Registry scheme IRI (<base>scheme/<slug>)
    compose_hash            CHAR(64),                 -- SHA-256 of ValueSet.compose
    binding_strength_default VARCHAR(12),             -- required / extensible / preferred / example
    expansion_ts            TIMESTAMP,
    member_count            INTEGER,
    is_current_flag         CHAR(1)       NOT NULL DEFAULT 'Y',
    source_system_sk        INTEGER       NOT NULL,
    load_batch_id           BIGINT        NOT NULL,
    record_hash             CHAR(64)      NOT NULL,
    recorded_at             TIMESTAMP     NOT NULL,
    prov_activity_iri       VARCHAR(512)  NOT NULL,
    CONSTRAINT pk_ref_value_set PRIMARY KEY (value_set_version_sk),
    CONSTRAINT fk_ref_value_set_src FOREIGN KEY (source_system_sk)
        REFERENCES edw_g8_reference.dim_source_system (source_system_sk)
);
-- TD: UNIQUE PRIMARY INDEX (value_set_version_sk)
CREATE UNIQUE INDEX ux_ref_value_set_bk ON edw_g8_reference.ref_value_set (value_set_bk);
COMMENT ON TABLE edw_g8_reference.ref_value_set IS 'http://www.w3.org/2004/02/skos/core#Collection';

CREATE TABLE edw_g8_reference.ref_value_set_member (
    value_set_version_sk    BIGINT        NOT NULL,
    code_sk                 BIGINT        NOT NULL,
    code_system_version_sk  BIGINT        NOT NULL,   -- version of the code system used in the expansion
    is_abstract_flag        CHAR(1)       NOT NULL DEFAULT 'N',
    is_inactive_flag        CHAR(1)       NOT NULL DEFAULT 'N',
    expansion_ts            TIMESTAMP     NOT NULL,
    source_system_sk        INTEGER       NOT NULL,
    load_batch_id           BIGINT        NOT NULL,
    recorded_at             TIMESTAMP     NOT NULL,
    prov_activity_iri       VARCHAR(512)  NOT NULL,
    CONSTRAINT pk_ref_value_set_member PRIMARY KEY (value_set_version_sk, code_sk),
    CONSTRAINT fk_ref_vsm_value_set FOREIGN KEY (value_set_version_sk)
        REFERENCES edw_g8_reference.ref_value_set (value_set_version_sk),
    CONSTRAINT fk_ref_vsm_code FOREIGN KEY (code_sk)
        REFERENCES edw_g8_reference.dim_code (code_sk),
    CONSTRAINT fk_ref_vsm_cs_version FOREIGN KEY (code_system_version_sk)
        REFERENCES edw_g8_reference.ref_code_system (code_system_version_sk),
    CONSTRAINT fk_ref_vsm_src FOREIGN KEY (source_system_sk)
        REFERENCES edw_g8_reference.dim_source_system (source_system_sk)
);
-- TD: PRIMARY INDEX (value_set_version_sk)
CREATE INDEX ix_ref_vsm_code ON edw_g8_reference.ref_value_set_member (code_sk);
COMMENT ON TABLE edw_g8_reference.ref_value_set_member IS 'http://www.w3.org/2004/02/skos/core#member';

-- ---------------------------------------------------------------------------
-- ref_concept_map / ref_concept_map_element
-- ---------------------------------------------------------------------------
CREATE TABLE edw_g8_reference.ref_concept_map (
    concept_map_version_sk  BIGINT        NOT NULL,
    concept_map_bk          VARCHAR(128)  NOT NULL,   -- canonical URL + '|' + version
    concept_map_url         VARCHAR(255)  NOT NULL,
    concept_map_name        VARCHAR(255)  NOT NULL,
    concept_map_version     VARCHAR(50)   NOT NULL,
    source_code_system_uri  VARCHAR(255)  NOT NULL,
    target_code_system_uri  VARCHAR(255)  NOT NULL,
    source_value_set_url    VARCHAR(255),
    target_value_set_url    VARCHAR(255),
    steward_agent_id        VARCHAR(50),              -- agent.terminology_steward
    is_current_flag         CHAR(1)       NOT NULL DEFAULT 'Y',
    source_system_sk        INTEGER       NOT NULL,
    load_batch_id           BIGINT        NOT NULL,
    record_hash             CHAR(64)      NOT NULL,
    recorded_at             TIMESTAMP     NOT NULL,
    prov_activity_iri       VARCHAR(512)  NOT NULL,
    CONSTRAINT pk_ref_concept_map PRIMARY KEY (concept_map_version_sk),
    CONSTRAINT fk_ref_concept_map_src FOREIGN KEY (source_system_sk)
        REFERENCES edw_g8_reference.dim_source_system (source_system_sk)
);
-- TD: UNIQUE PRIMARY INDEX (concept_map_version_sk)
CREATE UNIQUE INDEX ux_ref_concept_map_bk ON edw_g8_reference.ref_concept_map (concept_map_bk);
COMMENT ON TABLE edw_g8_reference.ref_concept_map IS 'https://ontology.healix.example.org/reference/ConceptMapVersion';

CREATE TABLE edw_g8_reference.ref_concept_map_element (
    concept_map_element_sk  BIGINT        NOT NULL,
    concept_map_version_sk  BIGINT        NOT NULL,
    source_code_sk          BIGINT        NOT NULL,
    target_code_sk          BIGINT        NOT NULL,   -- -2 when unmapped
    equivalence_code        VARCHAR(20)   NOT NULL,   -- equivalent / wider / narrower / inexact / unmatched / disjoint
    depends_on_json         VARCHAR(2000),
    mapping_comment         VARCHAR(1000),
    confidence_score        DECIMAL(5,4),             -- agent-proposed mappings carry evaluator confidence
    approved_by             VARCHAR(100),
    valid_from              TIMESTAMP     NOT NULL,
    valid_to                TIMESTAMP,
    source_system_sk        INTEGER       NOT NULL,
    load_batch_id           BIGINT        NOT NULL,
    record_hash             CHAR(64)      NOT NULL,
    recorded_at             TIMESTAMP     NOT NULL,
    superseded_at           TIMESTAMP,
    prov_activity_iri       VARCHAR(512)  NOT NULL,
    CONSTRAINT pk_ref_concept_map_element PRIMARY KEY (concept_map_element_sk),
    CONSTRAINT fk_ref_cme_map FOREIGN KEY (concept_map_version_sk)
        REFERENCES edw_g8_reference.ref_concept_map (concept_map_version_sk),
    CONSTRAINT fk_ref_cme_source FOREIGN KEY (source_code_sk)
        REFERENCES edw_g8_reference.dim_code (code_sk),
    CONSTRAINT fk_ref_cme_target FOREIGN KEY (target_code_sk)
        REFERENCES edw_g8_reference.dim_code (code_sk),
    CONSTRAINT fk_ref_cme_src FOREIGN KEY (source_system_sk)
        REFERENCES edw_g8_reference.dim_source_system (source_system_sk)
);
-- TD: PRIMARY INDEX (source_code_sk)
CREATE INDEX ix_ref_cme_map_source ON edw_g8_reference.ref_concept_map_element (concept_map_version_sk, source_code_sk);
COMMENT ON TABLE edw_g8_reference.ref_concept_map_element IS 'http://www.w3.org/2004/02/skos/core#mappingRelation';
COMMENT ON COLUMN edw_g8_reference.ref_concept_map_element.equivalence_code IS 'none | https://ontology.healix.example.org/reference/mappingEquivalence';

-- ---------------------------------------------------------------------------
-- dim_ucum_unit
-- ---------------------------------------------------------------------------
CREATE TABLE edw_g8_reference.dim_ucum_unit (
    ucum_unit_sk            BIGINT        NOT NULL,
    ucum_code               VARCHAR(50)   NOT NULL,   -- case-sensitive UCUM code, e.g. mg/dL
    ucum_display            VARCHAR(100),
    dimension_vector        VARCHAR(50),              -- e.g. L-3.M1 (mass per volume)
    canonical_ucum_unit_sk  BIGINT,                   -- canonical unit for the dimension (e.g. g/L)
    conversion_factor       DECIMAL(30,15),           -- value_canonical = value * factor (+ offset)
    conversion_offset       DECIMAL(30,15),
    code_sk                 BIGINT        NOT NULL,   -- dim_code row in the UCUM code system
    is_canonical_flag       CHAR(1)       NOT NULL DEFAULT 'N',
    source_system_sk        INTEGER       NOT NULL,
    load_batch_id           BIGINT        NOT NULL,
    record_hash             CHAR(64)      NOT NULL,
    recorded_at             TIMESTAMP     NOT NULL,
    prov_activity_iri       VARCHAR(512)  NOT NULL,
    CONSTRAINT pk_dim_ucum_unit PRIMARY KEY (ucum_unit_sk),
    CONSTRAINT fk_dim_ucum_canonical FOREIGN KEY (canonical_ucum_unit_sk)
        REFERENCES edw_g8_reference.dim_ucum_unit (ucum_unit_sk),
    CONSTRAINT fk_dim_ucum_code FOREIGN KEY (code_sk)
        REFERENCES edw_g8_reference.dim_code (code_sk),
    CONSTRAINT fk_dim_ucum_src FOREIGN KEY (source_system_sk)
        REFERENCES edw_g8_reference.dim_source_system (source_system_sk)
);
-- TD: UNIQUE PRIMARY INDEX (ucum_unit_sk)
CREATE UNIQUE INDEX ux_dim_ucum_code ON edw_g8_reference.dim_ucum_unit (ucum_code);
COMMENT ON TABLE edw_g8_reference.dim_ucum_unit IS 'https://ontology.healix.example.org/reference/UcumUnit';
COMMENT ON COLUMN edw_g8_reference.dim_ucum_unit.ucum_code IS 'none | https://ontology.healix.example.org/reference/ucumCode';

INSERT INTO edw_g8_reference.dim_ucum_unit
    (ucum_unit_sk, ucum_code, ucum_display, code_sk, source_system_sk, load_batch_id, record_hash, recorded_at, prov_activity_iri)
VALUES
    (-1, 'UNKNOWN', 'Unknown unit', -1, 1, 0, 'SEED', TIMESTAMP '1900-01-01 00:00:00',
     'https://ontology.healix.example.org/prov/activity/seed/dim_ucum_unit'),
    (-2, 'N/A', 'Not applicable (unitless or non-numeric)', -2, 1, 0, 'SEED', TIMESTAMP '1900-01-01 00:00:00',
     'https://ontology.healix.example.org/prov/activity/seed/dim_ucum_unit');

-- ---------------------------------------------------------------------------
-- dim_geography (Type 2): country → state → county → city → ZIP/ZCTA → tract
-- ---------------------------------------------------------------------------
CREATE TABLE edw_g8_reference.dim_geography (
    geography_sk            BIGINT        NOT NULL,
    geography_bk            VARCHAR(128)  NOT NULL,   -- <level>:<fips-or-zip-or-tract>
    geography_level_code    VARCHAR(10)   NOT NULL,   -- country / state / county / city / zip / zcta / tract
    country_code            CHAR(2),
    state_code              CHAR(2),
    state_fips              CHAR(2),
    county_fips             CHAR(5),
    county_name             VARCHAR(100),
    city_name               VARCHAR(100),
    postal_code             VARCHAR(10),
    zcta_code               CHAR(5),
    census_tract_fips       CHAR(11),
    parent_geography_sk     BIGINT,
    latitude                DECIMAL(9,6),
    longitude               DECIMAL(9,6),
    area_deprivation_index  DECIMAL(6,2),             -- ADI national percentile
    social_vulnerability_idx DECIMAL(6,4),            -- CDC SVI
    rural_urban_code        VARCHAR(5),               -- RUCA
    valid_from              TIMESTAMP     NOT NULL,
    valid_to                TIMESTAMP,
    is_current_flag         CHAR(1)       NOT NULL DEFAULT 'Y',
    source_system_sk        INTEGER       NOT NULL,
    load_batch_id           BIGINT        NOT NULL,
    record_hash             CHAR(64)      NOT NULL,
    recorded_at             TIMESTAMP     NOT NULL,
    prov_activity_iri       VARCHAR(512)  NOT NULL,
    CONSTRAINT pk_dim_geography PRIMARY KEY (geography_sk),
    CONSTRAINT fk_dim_geography_parent FOREIGN KEY (parent_geography_sk)
        REFERENCES edw_g8_reference.dim_geography (geography_sk),
    CONSTRAINT fk_dim_geography_src FOREIGN KEY (source_system_sk)
        REFERENCES edw_g8_reference.dim_source_system (source_system_sk)
);
-- TD: UNIQUE PRIMARY INDEX (geography_sk)
CREATE UNIQUE INDEX ux_dim_geography_bk ON edw_g8_reference.dim_geography (geography_bk, valid_from);
COMMENT ON TABLE edw_g8_reference.dim_geography IS 'https://ontology.healix.example.org/reference/GeographicArea';
COMMENT ON COLUMN edw_g8_reference.dim_geography.postal_code IS 'phi_quasi | https://ontology.healix.example.org/reference/postalCode';

INSERT INTO edw_g8_reference.dim_geography
    (geography_sk, geography_bk, geography_level_code, valid_from, is_current_flag, source_system_sk, load_batch_id,
     record_hash, recorded_at, prov_activity_iri)
VALUES
    (-1, 'UNKNOWN', 'unknown', TIMESTAMP '1900-01-01 00:00:00', 'Y', 1, 0, 'SEED', TIMESTAMP '1900-01-01 00:00:00',
     'https://ontology.healix.example.org/prov/activity/seed/dim_geography'),
    (-2, 'NOT_APPLICABLE', 'n/a', TIMESTAMP '1900-01-01 00:00:00', 'Y', 1, 0, 'SEED', TIMESTAMP '1900-01-01 00:00:00',
     'https://ontology.healix.example.org/prov/activity/seed/dim_geography');

-- ---------------------------------------------------------------------------
-- dim_service_line (Type 2)
-- ---------------------------------------------------------------------------
CREATE TABLE edw_g8_reference.dim_service_line (
    service_line_sk         BIGINT        NOT NULL,
    service_line_bk         VARCHAR(128)  NOT NULL,
    service_line_code       VARCHAR(30)   NOT NULL,
    service_line_name       VARCHAR(100)  NOT NULL,
    sub_service_line_code   VARCHAR(30),
    sub_service_line_name   VARCHAR(100),
    parent_service_line_sk  BIGINT,
    assignment_rule_iri     VARCHAR(512),             -- rule (DRG/CPT/specialty) in Ontology Registry
    valid_from              TIMESTAMP     NOT NULL,
    valid_to                TIMESTAMP,
    is_current_flag         CHAR(1)       NOT NULL DEFAULT 'Y',
    source_system_sk        INTEGER       NOT NULL,
    load_batch_id           BIGINT        NOT NULL,
    record_hash             CHAR(64)      NOT NULL,
    recorded_at             TIMESTAMP     NOT NULL,
    prov_activity_iri       VARCHAR(512)  NOT NULL,
    CONSTRAINT pk_dim_service_line PRIMARY KEY (service_line_sk),
    CONSTRAINT fk_dim_service_line_parent FOREIGN KEY (parent_service_line_sk)
        REFERENCES edw_g8_reference.dim_service_line (service_line_sk),
    CONSTRAINT fk_dim_service_line_src FOREIGN KEY (source_system_sk)
        REFERENCES edw_g8_reference.dim_source_system (source_system_sk)
);
-- TD: UNIQUE PRIMARY INDEX (service_line_sk)
CREATE UNIQUE INDEX ux_dim_service_line_bk ON edw_g8_reference.dim_service_line (service_line_bk, valid_from);
COMMENT ON TABLE edw_g8_reference.dim_service_line IS 'https://ontology.healix.example.org/reference/ServiceLine';

INSERT INTO edw_g8_reference.dim_service_line
    (service_line_sk, service_line_bk, service_line_code, service_line_name, valid_from, is_current_flag,
     source_system_sk, load_batch_id, record_hash, recorded_at, prov_activity_iri)
VALUES
    (-1, 'UNKNOWN', 'UNK', 'Unknown service line', TIMESTAMP '1900-01-01 00:00:00', 'Y', 1, 0, 'SEED',
     TIMESTAMP '1900-01-01 00:00:00', 'https://ontology.healix.example.org/prov/activity/seed/dim_service_line'),
    (-2, 'NOT_APPLICABLE', 'NA', 'Not applicable', TIMESTAMP '1900-01-01 00:00:00', 'Y', 1, 0, 'SEED',
     TIMESTAMP '1900-01-01 00:00:00', 'https://ontology.healix.example.org/prov/activity/seed/dim_service_line');
