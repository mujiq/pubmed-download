-- =============================================================================
-- HEALIX EDW · module 05 · g9_governance.sql
-- G9 Metadata, Quality & Governance · schema edw_g9_governance
-- Tables: fact_load_batch, fact_lineage_activity, bridge_lineage_activity_entity,
--         dim_dq_dimension, dim_data_steward, dim_dq_rule, ref_dq_weight,
--         dim_dq_certification, fact_dq_result, fact_dq_issue, fact_dq_profile,
--         agg_dq_result_day
-- Specs:  01-subject-area-groups.md §G9 · module 09 (DQ-03 §6 result/issue
--         contracts and DQ-04 scorecard tables are authoritative, EDW-G9-001)
--         · module 08 TKG-01 §9 (bitemporal block) · module 11 (PROV-O, PHI tags)
-- Depends on: g8_reference.sql (dim_source_system, dim_date, dim_time, dim_code)
-- Dialect: portable ANSI; Teradata hints as "-- TD:" comments.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- fact_load_batch: one row per load batch into any EDW table (EDW-G9-002).
-- Every presentation row's load_batch_id references this table (soft FK,
-- validated nightly by the FK-closure job; not declared to avoid the
-- cross-file cycle with dim_source_system).
-- ---------------------------------------------------------------------------
CREATE TABLE edw_g9_governance.fact_load_batch (
    load_batch_id           BIGINT        NOT NULL,
    load_batch_bk           VARCHAR(128)  NOT NULL,   -- ULID of the batch
    target_schema_name      VARCHAR(30)   NOT NULL,
    target_table_name       VARCHAR(30)   NOT NULL,
    source_system_sk        INTEGER       NOT NULL,
    kafka_topic             VARCHAR(255),
    kafka_partition_no      INTEGER,
    offset_from             BIGINT,
    offset_to               BIGINT,
    batch_start_ts          TIMESTAMP     NOT NULL,
    batch_end_ts            TIMESTAMP,
    batch_start_date_sk     INTEGER       NOT NULL,
    batch_start_time_sk     SMALLINT      NOT NULL,
    rows_read               BIGINT,
    rows_inserted           BIGINT,
    rows_superseded         BIGINT,
    rows_rejected           BIGINT,
    rows_inferred_members   BIGINT,
    content_checksum        CHAR(64),                 -- XOR-fold of record_hash over inserted rows
    batch_status_code       VARCHAR(12)   NOT NULL,   -- running / committed / failed / replayed
    is_replay_flag          CHAR(1)       NOT NULL DEFAULT 'N',
    hash_algo_version       VARCHAR(10)   NOT NULL,   -- EDW-LOG-012
    loader_version          VARCHAR(30),
    -- bitemporal block (valid_from = batch_start_ts; a replayed batch supersedes the original row)
    valid_from              TIMESTAMP     NOT NULL,
    valid_to                TIMESTAMP,
    recorded_at             TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    superseded_at           TIMESTAMP,
    change_kind             CHAR(12),                     -- TKG-01 C6/C10: why this version was closed; NULL while current, NOT NULL when superseded_at is set (new-version / correction / retraction / merge / unmerge)
    change_reason_code      VARCHAR(30),
    event_time              TIMESTAMP     NOT NULL,       -- TKG-01 C7: hc-event-time of the producing record (UTC)
    source_time             TIMESTAMP,                    -- TKG-01 C7: hc-source-time, the source system clock (UTC)
    decision_time           TIMESTAMP,                    -- TKG-01 C8: when the real-world decision was taken; NULL = not available
    source_tz               VARCHAR(64),                  -- TKG-01 C12: IANA zone of the source timestamps (storage is UTC)
    -- technical / provenance
    record_hash             CHAR(64)      NOT NULL,
    prov_activity_iri       VARCHAR(512)  NOT NULL,   -- = fact_lineage_activity.prov_activity_iri
    CONSTRAINT pk_fact_load_batch PRIMARY KEY (load_batch_id),
    CONSTRAINT fk_load_batch_src FOREIGN KEY (source_system_sk)
        REFERENCES edw_g8_reference.dim_source_system (source_system_sk),
    CONSTRAINT fk_load_batch_date FOREIGN KEY (batch_start_date_sk)
        REFERENCES edw_g8_reference.dim_date (date_sk),
    CONSTRAINT fk_load_batch_time FOREIGN KEY (batch_start_time_sk)
        REFERENCES edw_g8_reference.dim_time (time_sk)
);
-- TD: CREATE MULTISET TABLE edw_g9_governance.fact_load_batch, FALLBACK (...) PRIMARY INDEX (load_batch_id)
-- TD: PARTITION BY RANGE_N(CAST(batch_start_ts AS DATE) BETWEEN DATE '2025-01-01' AND DATE '2040-12-31' EACH INTERVAL '1' MONTH, NO RANGE)
CREATE UNIQUE INDEX ux_fact_load_batch_bk ON edw_g9_governance.fact_load_batch (load_batch_bk);
CREATE INDEX ix_fact_load_batch_target ON edw_g9_governance.fact_load_batch (target_schema_name, target_table_name, batch_start_ts);
COMMENT ON TABLE edw_g9_governance.fact_load_batch IS 'https://ontology.healix.example.org/governance/LoadBatch';
COMMENT ON COLUMN edw_g9_governance.fact_load_batch.prov_activity_iri IS 'internal | http://www.w3.org/ns/prov#wasGeneratedBy';
COMMENT ON COLUMN edw_g9_governance.fact_load_batch.content_checksum IS 'internal | https://ontology.healix.example.org/governance/contentChecksum';

-- ---------------------------------------------------------------------------
-- fact_lineage_activity: one row per PROV-O activity (load, transform, merge,
-- correction, DDL migration, agent action, retention purge)
-- ---------------------------------------------------------------------------
CREATE TABLE edw_g9_governance.fact_lineage_activity (
    lineage_activity_sk     BIGINT        NOT NULL,
    prov_activity_iri       VARCHAR(512)  NOT NULL,   -- business key; resolvable prov:Activity in the KG
    activity_type_code      VARCHAR(30)   NOT NULL,   -- load / transform / merge / correction / ddl-migration / agent-action / retention-purge / reconciliation
    pipeline_name           VARCHAR(100),
    pipeline_version        VARCHAR(30),
    agent_id                VARCHAR(50),              -- agent.<role> (module 10 roster) or NULL for human/system
    agent_model_id          VARCHAR(100),
    triggered_by            VARCHAR(100),             -- schedule / kafka / change-request id / human principal
    change_request_id       VARCHAR(64),
    source_system_sk        INTEGER       NOT NULL,
    load_batch_id           BIGINT,                   -- when the activity is a load
    started_ts              TIMESTAMP     NOT NULL,
    ended_ts                TIMESTAMP,
    started_date_sk         INTEGER       NOT NULL,
    started_time_sk         SMALLINT      NOT NULL,
    duration_seconds        DECIMAL(12,3),
    outcome_code            VARCHAR(12)   NOT NULL,   -- success / partial / failed / cancelled
    openlineage_run_id      VARCHAR(64),
    was_informed_by_iri     VARCHAR(512),             -- prov:wasInformedBy (e.g. correction → original load)
    input_entity_count      INTEGER,
    output_entity_count     INTEGER,
    message_text            VARCHAR(1000),
    -- bitemporal block (activities are events; valid = occurrence time)
    valid_from              TIMESTAMP     NOT NULL,
    valid_to                TIMESTAMP,
    recorded_at             TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    superseded_at           TIMESTAMP,
    change_kind             CHAR(12),                     -- TKG-01 C6/C10: why this version was closed; NULL while current, NOT NULL when superseded_at is set (new-version / correction / retraction / merge / unmerge)
    change_reason_code      VARCHAR(30),
    event_time              TIMESTAMP     NOT NULL,       -- TKG-01 C7: hc-event-time of the producing record (UTC)
    source_time             TIMESTAMP,                    -- TKG-01 C7: hc-source-time, the source system clock (UTC)
    decision_time           TIMESTAMP,                    -- TKG-01 C8: when the real-world decision was taken; NULL = not available
    source_tz               VARCHAR(64),                  -- TKG-01 C12: IANA zone of the source timestamps (storage is UTC)
    record_hash             CHAR(64)      NOT NULL,
    CONSTRAINT pk_fact_lineage_activity PRIMARY KEY (lineage_activity_sk),
    CONSTRAINT fk_lineage_src FOREIGN KEY (source_system_sk)
        REFERENCES edw_g8_reference.dim_source_system (source_system_sk),
    CONSTRAINT fk_lineage_batch FOREIGN KEY (load_batch_id)
        REFERENCES edw_g9_governance.fact_load_batch (load_batch_id),
    CONSTRAINT fk_lineage_date FOREIGN KEY (started_date_sk)
        REFERENCES edw_g8_reference.dim_date (date_sk),
    CONSTRAINT fk_lineage_time FOREIGN KEY (started_time_sk)
        REFERENCES edw_g8_reference.dim_time (time_sk)
);
-- TD: PRIMARY INDEX (prov_activity_iri)
-- TD: PARTITION BY RANGE_N(CAST(started_ts AS DATE) BETWEEN DATE '2025-01-01' AND DATE '2040-12-31' EACH INTERVAL '1' MONTH, NO RANGE)
-- TD: COLLECT STATISTICS COLUMN (prov_activity_iri), COLUMN (activity_type_code), COLUMN (PARTITION) ON edw_g9_governance.fact_lineage_activity;
CREATE UNIQUE INDEX ux_fact_lineage_activity_iri ON edw_g9_governance.fact_lineage_activity (prov_activity_iri, valid_from);
COMMENT ON TABLE edw_g9_governance.fact_lineage_activity IS 'http://www.w3.org/ns/prov#Activity';
COMMENT ON COLUMN edw_g9_governance.fact_lineage_activity.started_ts IS 'internal | http://www.w3.org/ns/prov#startedAtTime';
COMMENT ON COLUMN edw_g9_governance.fact_lineage_activity.ended_ts IS 'internal | http://www.w3.org/ns/prov#endedAtTime';
COMMENT ON COLUMN edw_g9_governance.fact_lineage_activity.agent_id IS 'internal | http://www.w3.org/ns/prov#wasAssociatedWith';
COMMENT ON COLUMN edw_g9_governance.fact_lineage_activity.was_informed_by_iri IS 'internal | http://www.w3.org/ns/prov#wasInformedBy';

-- ---------------------------------------------------------------------------
-- bridge_lineage_activity_entity: activity × entity × role (prov:used /
-- prov:generated); entity = table, topic, file, named graph, ontology release
-- ---------------------------------------------------------------------------
CREATE TABLE edw_g9_governance.bridge_lineage_activity_entity (
    lineage_activity_sk     BIGINT        NOT NULL,
    entity_urn              VARCHAR(512)  NOT NULL,   -- OpenLineage dataset URN or named-graph IRI
    entity_role_code        VARCHAR(10)   NOT NULL,   -- used / generated / invalidated
    entity_type_code        VARCHAR(20)   NOT NULL,   -- table / topic / file / graph / ontology / rule-pack
    row_count               BIGINT,
    content_checksum        CHAR(64),
    valid_from              TIMESTAMP     NOT NULL,
    valid_to                TIMESTAMP,
    recorded_at             TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    superseded_at           TIMESTAMP,
    change_kind             CHAR(12),                     -- TKG-01 C6/C10: why this version was closed; NULL while current, NOT NULL when superseded_at is set (new-version / correction / retraction / merge / unmerge)
    change_reason_code      VARCHAR(30),                  -- EDW loader reason for this version (06-history-and-bitemporality.md §1)
    event_time              TIMESTAMP     NOT NULL,       -- TKG-01 C7: hc-event-time of the producing record (UTC)
    source_time             TIMESTAMP,                    -- TKG-01 C7: hc-source-time, the source system clock (UTC)
    decision_time           TIMESTAMP,                    -- TKG-01 C8: when the real-world decision was taken; NULL = not available
    source_tz               VARCHAR(64),                  -- TKG-01 C12: IANA zone of the source timestamps (storage is UTC)
    source_system_sk        INTEGER       NOT NULL,
    load_batch_id           BIGINT        NOT NULL,
    record_hash             CHAR(64)      NOT NULL,
    prov_activity_iri       VARCHAR(512)  NOT NULL,
    CONSTRAINT pk_bridge_lineage_entity PRIMARY KEY (lineage_activity_sk, entity_urn, entity_role_code),
    CONSTRAINT fk_bridge_lineage_activity FOREIGN KEY (lineage_activity_sk)
        REFERENCES edw_g9_governance.fact_lineage_activity (lineage_activity_sk),
    CONSTRAINT fk_bridge_lineage_src FOREIGN KEY (source_system_sk)
        REFERENCES edw_g8_reference.dim_source_system (source_system_sk)
);
-- TD: PRIMARY INDEX (lineage_activity_sk)
CREATE INDEX ix_bridge_lineage_entity_urn ON edw_g9_governance.bridge_lineage_activity_entity (entity_urn);
COMMENT ON TABLE edw_g9_governance.bridge_lineage_activity_entity IS 'http://www.w3.org/ns/prov#Usage';

-- ---------------------------------------------------------------------------
-- dim_dq_dimension (Type 1): the ten module-09 dimensions (DQ-01 §2 / DQ-FW-001):
-- completeness, validity, accuracy, consistency, uniqueness, timeliness,
-- integrity, temporal, lineage, semantic
-- ---------------------------------------------------------------------------
CREATE TABLE edw_g9_governance.dim_dq_dimension (
    dq_dimension_sk         SMALLINT      NOT NULL,
    dq_dimension_code       VARCHAR(20)   NOT NULL,   -- lower-case token; upper-cased it is the <DIMENSION> part of the rule id
    dq_dimension_name       VARCHAR(50)   NOT NULL,
    dq_dimension_definition VARCHAR(500),
    code_sk                 BIGINT        NOT NULL,   -- dim_code row in healix:scheme/dq-dimension
    source_system_sk        INTEGER       NOT NULL,
    load_batch_id           BIGINT        NOT NULL,
    record_hash             CHAR(64)      NOT NULL,
    recorded_at             TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    prov_activity_iri       VARCHAR(512)  NOT NULL,
    CONSTRAINT pk_dim_dq_dimension PRIMARY KEY (dq_dimension_sk),
    CONSTRAINT fk_dim_dq_dimension_code FOREIGN KEY (code_sk)
        REFERENCES edw_g8_reference.dim_code (code_sk),
    CONSTRAINT fk_dim_dq_dimension_src FOREIGN KEY (source_system_sk)
        REFERENCES edw_g8_reference.dim_source_system (source_system_sk)
);
-- TD: UNIQUE PRIMARY INDEX (dq_dimension_sk)
CREATE UNIQUE INDEX ux_dim_dq_dimension_code ON edw_g9_governance.dim_dq_dimension (dq_dimension_code);
COMMENT ON TABLE edw_g9_governance.dim_dq_dimension IS 'https://ontology.healix.example.org/governance/DataQualityDimension';
COMMENT ON COLUMN edw_g9_governance.dim_dq_dimension.dq_dimension_code IS 'internal | https://ontology.healix.example.org/governance/dqDimensionCode';

-- ---------------------------------------------------------------------------
-- dim_data_steward (Type 2): steward assignments (module 09 DQ-01 §6, DQ-FW-008).
-- steward_bk is the rule-pack `owner` value (DQ-02 §2.1, rule.schema.json).
-- ---------------------------------------------------------------------------
CREATE TABLE edw_g9_governance.dim_data_steward (
    steward_sk              BIGINT        NOT NULL,
    steward_bk              VARCHAR(128)  NOT NULL,   -- steward.<slug>, e.g. steward.clinical; agent.<role> for agent stewards
    steward_name            VARCHAR(200)  NOT NULL,
    steward_role_code       VARCHAR(30)   NOT NULL,   -- dq-lead / group-steward / source-owner / clinical-informaticist / revenue-integrity / agent
    group_code              CHAR(2),                  -- G1..G9 for group stewards; NULL for enterprise roles
    owned_source_system_sk  INTEGER,                  -- → dim_source_system for source owners
    agent_id                VARCHAR(50),              -- agent.<role> (module 10 roster) when the steward is an agent
    principal_id            VARCHAR(100),             -- directory principal of the human steward
    email_work              VARCHAR(255),
    escalation_steward_sk   BIGINT,                   -- → dim_data_steward (manager / escalation target, DQ-04 §5.1)
    active_flag             CHAR(1)       NOT NULL DEFAULT 'Y',
    -- valid-time block (Type 2)
    valid_from              TIMESTAMP     NOT NULL,
    valid_to                TIMESTAMP,
    is_current_flag         CHAR(1)       NOT NULL DEFAULT 'Y',
    -- technical block
    source_system_sk        INTEGER       NOT NULL,
    load_batch_id           BIGINT        NOT NULL,
    record_hash             CHAR(64)      NOT NULL,
    recorded_at             TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    prov_activity_iri       VARCHAR(512)  NOT NULL,
    CONSTRAINT pk_dim_data_steward PRIMARY KEY (steward_sk),
    CONSTRAINT fk_dim_steward_owned_src FOREIGN KEY (owned_source_system_sk)
        REFERENCES edw_g8_reference.dim_source_system (source_system_sk),
    CONSTRAINT fk_dim_steward_escalation FOREIGN KEY (escalation_steward_sk)
        REFERENCES edw_g9_governance.dim_data_steward (steward_sk),
    CONSTRAINT fk_dim_steward_src FOREIGN KEY (source_system_sk)
        REFERENCES edw_g8_reference.dim_source_system (source_system_sk)
);
-- TD: UNIQUE PRIMARY INDEX (steward_sk)
CREATE UNIQUE INDEX ux_dim_data_steward_bk ON edw_g9_governance.dim_data_steward (steward_bk, valid_from);
COMMENT ON TABLE edw_g9_governance.dim_data_steward IS 'https://ontology.healix.example.org/governance/DataSteward';
COMMENT ON COLUMN edw_g9_governance.dim_data_steward.steward_bk IS 'internal | https://ontology.healix.example.org/governance/stewardIdentifier';
COMMENT ON COLUMN edw_g9_governance.dim_data_steward.steward_name IS 'pii:workforce | https://ontology.healix.example.org/governance/stewardName';
COMMENT ON COLUMN edw_g9_governance.dim_data_steward.principal_id IS 'pii:workforce | https://ontology.healix.example.org/governance/principalIdentifier';
COMMENT ON COLUMN edw_g9_governance.dim_data_steward.email_work IS 'pii:workforce | https://ontology.healix.example.org/party/workEmail';
COMMENT ON COLUMN edw_g9_governance.dim_data_steward.agent_id IS 'internal | http://www.w3.org/ns/prov#SoftwareAgent';

-- ---------------------------------------------------------------------------
-- dim_dq_rule (Type 2): one row per DQ rule version, compiled from rules/*.yaml
-- by the Rule Compiler (module 09 DQ-02 §2.2, DQ-03 §2).
-- Rule ids follow CONVENTIONS §3: DQR-<DIMENSION>-<DOMAIN>-<NNN>
-- ---------------------------------------------------------------------------
CREATE TABLE edw_g9_governance.dim_dq_rule (
    dq_rule_sk              BIGINT        NOT NULL,
    dq_rule_bk              VARCHAR(128)  NOT NULL,   -- rule id, e.g. DQR-COMPLETENESS-PATIENT-001
    dq_rule_version         VARCHAR(20)   NOT NULL,   -- semver; immutable once active
    pack_name               VARCHAR(50)   NOT NULL,   -- rules/<pack>.yaml
    pack_version            VARCHAR(20)   NOT NULL,   -- pack release that compiled this rule version
    dq_rule_name            VARCHAR(255)  NOT NULL,
    dq_rule_description     VARCHAR(1000),
    dq_dimension_sk         SMALLINT      NOT NULL,
    domain_code             VARCHAR(30)   NOT NULL,   -- Kafka domain: party / clinical / finance / supply / workforce / pophealth / research / reference / governance
    group_code              VARCHAR(4)    NOT NULL,   -- G1..G9 or XSYS (cross-system reconciliation)
    source_systems_text     VARCHAR(255)  NOT NULL,   -- comma-separated module-09 source_systems enum values
    cde_paths_text          VARCHAR(1000),            -- canonical element paths protected (drives CDE coverage)
    target_planes_text      VARCHAR(30)   NOT NULL,   -- comma-separated subset of stream,graph,warehouse
    authoritative_plane_code VARCHAR(10)  NOT NULL,   -- stream / graph / warehouse (DQ-04 §2.3)
    target_schema_name      VARCHAR(30),              -- warehouse target
    target_table_name       VARCHAR(30),
    target_column_name      VARCHAR(30),
    target_resource_type    VARCHAR(50),              -- stream target (FHIR resource / HEALIX-ERP profile)
    target_fhir_path        VARCHAR(255),
    rule_language_code      VARCHAR(10)   NOT NULL,   -- fhirpath / shacl / sparql / sql (first target)
    rule_expression         VARCHAR(4000),            -- FHIRPath, SPARQL or SQL text of the first target
    shacl_shape_iri         VARCHAR(512),
    derivation_code         VARCHAR(12)   NOT NULL,   -- ontology / terminology / profiling / policy
    derived_from_constraint_iri VARCHAR(512),         -- shape/property IRI, ValueSet URL, baseline_ref URN or policy URI
    ontology_property_iri   VARCHAR(512),             -- property measured
    severity_code           VARCHAR(10)   NOT NULL,   -- blocker / critical / major / minor (DQ-FW-005)
    quarantine_flag         CHAR(1)       NOT NULL DEFAULT 'N',
    is_critical_data_element_flag CHAR(1) NOT NULL DEFAULT 'N',   -- = module-09 is_cde
    threshold_metric_code   VARCHAR(10)   NOT NULL,   -- pass_rate / fail_count / value
    threshold_min           DECIMAL(18,6),
    threshold_max           DECIMAL(18,6),
    threshold_window        VARCHAR(20)   NOT NULL,   -- ISO 8601 duration (PT1H, P1D, P7D)
    issue_grain_code        VARCHAR(12),              -- record / hourly / window (DQ-03 §6.2)
    remediation_action_code VARCHAR(12)   NOT NULL,   -- auto_fix / steward / source_fix / accept
    remediation_hint        VARCHAR(1000),
    owner_steward_sk        BIGINT        NOT NULL,   -- → dim_data_steward (DQ-FW-008)
    authored_by_agent_id    VARCHAR(50),              -- agent.dq_author
    approved_by             VARCHAR(100),
    rule_status_code        VARCHAR(12)   NOT NULL,   -- draft / review / active / tuning / deprecated / retired (DQ-01 §7)
    tags_text               VARCHAR(255),
    -- valid-time block (Type 2)
    valid_from              TIMESTAMP     NOT NULL,
    valid_to                TIMESTAMP,
    is_current_flag         CHAR(1)       NOT NULL DEFAULT 'Y',
    -- technical block
    source_system_sk        INTEGER       NOT NULL,
    load_batch_id           BIGINT        NOT NULL,
    record_hash             CHAR(64)      NOT NULL,
    recorded_at             TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    prov_activity_iri       VARCHAR(512)  NOT NULL,
    CONSTRAINT pk_dim_dq_rule PRIMARY KEY (dq_rule_sk),
    CONSTRAINT fk_dim_dq_rule_dimension FOREIGN KEY (dq_dimension_sk)
        REFERENCES edw_g9_governance.dim_dq_dimension (dq_dimension_sk),
    CONSTRAINT fk_dim_dq_rule_owner FOREIGN KEY (owner_steward_sk)
        REFERENCES edw_g9_governance.dim_data_steward (steward_sk),
    CONSTRAINT fk_dim_dq_rule_src FOREIGN KEY (source_system_sk)
        REFERENCES edw_g8_reference.dim_source_system (source_system_sk)
);
-- TD: UNIQUE PRIMARY INDEX (dq_rule_sk)
CREATE UNIQUE INDEX ux_dim_dq_rule_bk ON edw_g9_governance.dim_dq_rule (dq_rule_bk, dq_rule_version, valid_from);
COMMENT ON TABLE edw_g9_governance.dim_dq_rule IS 'https://ontology.healix.example.org/governance/DataQualityRule';
COMMENT ON COLUMN edw_g9_governance.dim_dq_rule.dq_rule_bk IS 'internal | https://ontology.healix.example.org/governance/dqRuleIdentifier';
COMMENT ON COLUMN edw_g9_governance.dim_dq_rule.rule_expression IS 'internal | https://ontology.healix.example.org/governance/dqRuleExpression';
COMMENT ON COLUMN edw_g9_governance.dim_dq_rule.shacl_shape_iri IS 'internal | http://www.w3.org/ns/shacl#shape';
COMMENT ON COLUMN edw_g9_governance.dim_dq_rule.derived_from_constraint_iri IS 'internal | http://www.w3.org/ns/prov#wasDerivedFrom';
COMMENT ON COLUMN edw_g9_governance.dim_dq_rule.owner_steward_sk IS 'internal | https://ontology.healix.example.org/governance/ownedBy';

-- ---------------------------------------------------------------------------
-- ref_dq_weight: composite DQ index weights (module 09 DQ-04 §2.1):
-- w_sev blocker 8 · critical 4 · major 2 · minor 1; w_cde 3 if the rule
-- protects a CDE, else 1. Versioned so historical scorecards reproduce
-- (DQ-SCORE-007). Module-09 SQL names: severity = severity_code, is_cde =
-- is_critical_data_element_flag.
-- ---------------------------------------------------------------------------
CREATE TABLE edw_g9_governance.ref_dq_weight (
    dq_weight_sk            SMALLINT      NOT NULL,
    severity_code           VARCHAR(10)   NOT NULL,   -- blocker / critical / major / minor
    is_critical_data_element_flag CHAR(1) NOT NULL,
    w_sev                   DECIMAL(5,2)  NOT NULL,
    w_cde                   DECIMAL(5,2)  NOT NULL,
    formula_version         VARCHAR(20)   NOT NULL,   -- version of DQ-04 §2.1 in force
    effective_from          TIMESTAMP     NOT NULL,
    effective_to            TIMESTAMP,                -- NULL = in force
    -- technical block
    source_system_sk        INTEGER       NOT NULL,
    load_batch_id           BIGINT        NOT NULL,
    record_hash             CHAR(64)      NOT NULL,
    recorded_at             TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    prov_activity_iri       VARCHAR(512)  NOT NULL,
    CONSTRAINT pk_ref_dq_weight PRIMARY KEY (dq_weight_sk),
    CONSTRAINT fk_ref_dq_weight_src FOREIGN KEY (source_system_sk)
        REFERENCES edw_g8_reference.dim_source_system (source_system_sk)
);
-- TD: UNIQUE PRIMARY INDEX (dq_weight_sk)
CREATE UNIQUE INDEX ux_ref_dq_weight ON edw_g9_governance.ref_dq_weight (severity_code, is_critical_data_element_flag, effective_from);
COMMENT ON TABLE edw_g9_governance.ref_dq_weight IS 'https://ontology.healix.example.org/governance/DataQualityIndexWeight';
COMMENT ON COLUMN edw_g9_governance.ref_dq_weight.w_sev IS 'internal | https://ontology.healix.example.org/governance/severityWeight';
COMMENT ON COLUMN edw_g9_governance.ref_dq_weight.w_cde IS 'internal | https://ontology.healix.example.org/governance/cdeWeight';

-- ---------------------------------------------------------------------------
-- dim_dq_certification (Type 2): downstream data-product certification per
-- scope (group, schema or data product). is_certified_flag is module-09
-- `dim_dq_certification.is_certified` (DQ-SCORE-009): set 'N' when the group
-- index falls to Red/Black, restored after 7 consecutive Amber-or-better days.
-- ---------------------------------------------------------------------------
CREATE TABLE edw_g9_governance.dim_dq_certification (
    dq_certification_sk     BIGINT        NOT NULL,
    dq_certification_bk     VARCHAR(128)  NOT NULL,   -- <scope_type_code>:<scope_bk>, e.g. group:G2, product:edw_sem.v_encounter
    scope_type_code         VARCHAR(12)   NOT NULL,   -- group / schema / data-product
    scope_bk                VARCHAR(128)  NOT NULL,
    group_code              CHAR(2),                  -- owning group G1..G9
    is_certified_flag       CHAR(1)       NOT NULL,
    certification_band_code VARCHAR(6)    NOT NULL,   -- green / amber / red / black (DQ-04 §2.4)
    dq_index_value          DECIMAL(5,2),             -- DQI at the time of this version
    consecutive_amber_days  SMALLINT,
    revoked_ts              TIMESTAMP,
    restored_ts             TIMESTAMP,
    certified_by_steward_sk BIGINT        NOT NULL,   -- → dim_data_steward
    certification_note      VARCHAR(1000),
    -- valid-time block (Type 2)
    valid_from              TIMESTAMP     NOT NULL,
    valid_to                TIMESTAMP,
    is_current_flag         CHAR(1)       NOT NULL DEFAULT 'Y',
    -- technical block
    source_system_sk        INTEGER       NOT NULL,
    load_batch_id           BIGINT        NOT NULL,
    record_hash             CHAR(64)      NOT NULL,
    recorded_at             TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    prov_activity_iri       VARCHAR(512)  NOT NULL,
    CONSTRAINT pk_dim_dq_certification PRIMARY KEY (dq_certification_sk),
    CONSTRAINT fk_dim_dq_cert_steward FOREIGN KEY (certified_by_steward_sk)
        REFERENCES edw_g9_governance.dim_data_steward (steward_sk),
    CONSTRAINT fk_dim_dq_cert_src FOREIGN KEY (source_system_sk)
        REFERENCES edw_g8_reference.dim_source_system (source_system_sk)
);
-- TD: UNIQUE PRIMARY INDEX (dq_certification_sk)
CREATE UNIQUE INDEX ux_dim_dq_certification_bk ON edw_g9_governance.dim_dq_certification (dq_certification_bk, valid_from);
COMMENT ON TABLE edw_g9_governance.dim_dq_certification IS 'https://ontology.healix.example.org/governance/DataProductCertification';
COMMENT ON COLUMN edw_g9_governance.dim_dq_certification.is_certified_flag IS 'internal | https://ontology.healix.example.org/governance/isCertified';
COMMENT ON COLUMN edw_g9_governance.dim_dq_certification.certification_band_code IS 'internal | https://ontology.healix.example.org/governance/dqBand';

-- ---------------------------------------------------------------------------
-- fact_dq_result: one row per hc.dq.result.v1 record (module 09 DQ-03 §6.1),
-- i.e. one evaluation of one rule version on one plane against one subject
-- (record / focus node / row) or one aggregate window. Module 09 is
-- authoritative; this table is regenerated when the contract changes
-- (EDW-G9-001). Contract field → column:
--   result_id → dq_result_bk · rule_id → dq_rule_bk · rule_version → dq_rule_version
--   pack_version → pack_version · plane → plane_code · outcome → outcome_code
--   grain → grain_code · severity → severity_code
--   subject.resource_type → subject_resource_type · subject.business_key → subject_bk
--   subject.resource_id → subject_resource_id · subject.named_graph → subject_named_graph_iri
--   subject.table → target_schema_name + target_table_name · subject.key_columns → subject_key_columns_text
--   source_system → result_source_system_sk · facility → facility_code
--   event_time → event_time · source_time → source_time · evaluated_at → evaluated_ts
--   valid_from / recorded_at / prov_activity_iri → same names (bitemporal block)
--   detail → detail_text · context → context_json
--   aggregate.{passed,failed,errored,denominator} → *_count · aggregate.window_* → window_*_ts
--   `backfill` flag (DQ-PIPE-013) → is_backfill_flag
-- Retention: record-grain rows 90 days, then agg_dq_result_day (DQ-SCORE-008).
-- ---------------------------------------------------------------------------
CREATE TABLE edw_g9_governance.fact_dq_result (
    dq_result_hk            CHAR(64)      NOT NULL,   -- SHA-256(dq_result_bk)
    dq_result_bk            VARCHAR(64)   NOT NULL,   -- result_id (ULID)
    dq_rule_sk              BIGINT        NOT NULL,
    dq_rule_bk              VARCHAR(128)  NOT NULL,   -- rule_id (degenerate, keeps results joinable across rule re-versioning)
    dq_rule_version         VARCHAR(20)   NOT NULL,
    pack_version            VARCHAR(20)   NOT NULL,
    dq_dimension_sk         SMALLINT      NOT NULL,
    plane_code              VARCHAR(10)   NOT NULL,   -- stream / graph / warehouse
    outcome_code            VARCHAR(8)    NOT NULL,   -- pass / fail / error / pending
    grain_code              VARCHAR(12)   NOT NULL,   -- record / focus_node / row / aggregate
    severity_code           VARCHAR(10)   NOT NULL,   -- copied from the rule version
    -- subject (never PHI values beyond the hashed business key, DQ-PIPE-009)
    subject_resource_type   VARCHAR(50),
    subject_bk              VARCHAR(128),             -- business key of the evaluated record; MRN hashed; NULL for aggregate
    subject_resource_id     VARCHAR(128),
    subject_named_graph_iri VARCHAR(512),
    target_schema_name      VARCHAR(30),
    target_table_name       VARCHAR(30),
    target_column_name      VARCHAR(30),
    subject_key_columns_text VARCHAR(255),            -- key column list for warehouse-plane detail rows
    result_source_system_sk INTEGER       NOT NULL,   -- source system of the evaluated data (technical source_system_sk = platform)
    facility_code           VARCHAR(50),
    kafka_topic             VARCHAR(255),
    evaluated_load_batch_id BIGINT,                   -- warehouse plane: batch whose rows were evaluated
    evaluated_ts            TIMESTAMP     NOT NULL,   -- evaluated_at
    evaluated_date_sk       INTEGER       NOT NULL,
    evaluated_time_sk       SMALLINT      NOT NULL,
    detail_text             VARCHAR(2000),            -- expression-specific detail, masked per module 11
    context_json            VARCHAR(4000),            -- valueset_version, ontology_version, baseline_ref, window ...
    -- aggregate (grain_code = 'aggregate')
    passed_count            BIGINT,
    failed_count            BIGINT,
    errored_count           BIGINT,
    denominator_count       BIGINT,
    window_start_ts         TIMESTAMP,
    window_end_ts           TIMESTAMP,
    pass_rate_pct           DECIMAL(7,4),             -- passed / (passed + failed); errors excluded (DQ-PIPE-003)
    -- routing / remediation
    quarantine_id           VARCHAR(64),              -- ULID of the quarantined record when the record was routed
    quarantine_topic        VARCHAR(255),             -- hc.dq.quarantine.<domain>.v1
    remediation_issue_bk    VARCHAR(64),              -- fact_dq_issue.dq_issue_bk when an issue was opened
    evaluator_agent_id      VARCHAR(50),              -- agent.dq_monitor or plane job id
    is_backfill_flag        CHAR(1)       NOT NULL DEFAULT 'N',
    terminology_release_id  VARCHAR(50),
    ontology_release_iri    VARCHAR(512),
    -- bitemporal block (TKG-01 §9; valid_from = evaluated_ts, event_time = subject record's hc-event-time)
    valid_from              TIMESTAMP     NOT NULL,
    valid_to                TIMESTAMP,
    recorded_at             TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    superseded_at           TIMESTAMP,
    change_kind             CHAR(12),                     -- TKG-01 C6/C10: why this version was closed; NULL while current, NOT NULL when superseded_at is set (new-version / correction / retraction / merge / unmerge)
    change_reason_code      VARCHAR(30),
    event_time              TIMESTAMP     NOT NULL,       -- TKG-01 C7: hc-event-time of the producing record (UTC)
    source_time             TIMESTAMP,                    -- TKG-01 C7: hc-source-time, the source system clock (UTC)
    decision_time           TIMESTAMP,                    -- TKG-01 C8: when the real-world decision was taken; NULL = not available
    source_tz               VARCHAR(64),                  -- TKG-01 C12: IANA zone of the source timestamps (storage is UTC)
    -- technical block
    source_system_sk        INTEGER       NOT NULL,
    load_batch_id           BIGINT        NOT NULL,
    record_hash             CHAR(64)      NOT NULL,
    prov_activity_iri       VARCHAR(512)  NOT NULL,
    CONSTRAINT pk_fact_dq_result PRIMARY KEY (dq_result_hk),
    CONSTRAINT fk_dq_result_rule FOREIGN KEY (dq_rule_sk)
        REFERENCES edw_g9_governance.dim_dq_rule (dq_rule_sk),
    CONSTRAINT fk_dq_result_dimension FOREIGN KEY (dq_dimension_sk)
        REFERENCES edw_g9_governance.dim_dq_dimension (dq_dimension_sk),
    CONSTRAINT fk_dq_result_result_src FOREIGN KEY (result_source_system_sk)
        REFERENCES edw_g8_reference.dim_source_system (source_system_sk),
    CONSTRAINT fk_dq_result_eval_batch FOREIGN KEY (evaluated_load_batch_id)
        REFERENCES edw_g9_governance.fact_load_batch (load_batch_id),
    CONSTRAINT fk_dq_result_date FOREIGN KEY (evaluated_date_sk)
        REFERENCES edw_g8_reference.dim_date (date_sk),
    CONSTRAINT fk_dq_result_time FOREIGN KEY (evaluated_time_sk)
        REFERENCES edw_g8_reference.dim_time (time_sk),
    CONSTRAINT fk_dq_result_src FOREIGN KEY (source_system_sk)
        REFERENCES edw_g8_reference.dim_source_system (source_system_sk)
);
-- TD: CREATE MULTISET TABLE edw_g9_governance.fact_dq_result, FALLBACK, BLOCKCOMPRESSION = AUTOTEMP (...) PRIMARY INDEX (dq_rule_sk, subject_bk)
-- TD: PARTITION BY (RANGE_N(CAST(evaluated_ts AS DATE) BETWEEN DATE '2025-01-01' AND DATE '2040-12-31' EACH INTERVAL '1' DAY, NO RANGE),
-- TD:               CASE_N(superseded_at IS NULL, NO CASE))                       -- DQ-PIPE-014: partition by evaluated_at, cluster by rule
-- TD: COLLECT STATISTICS COLUMN (dq_rule_sk), COLUMN (outcome_code), COLUMN (plane_code), COLUMN (target_schema_name, target_table_name), COLUMN (PARTITION) ON edw_g9_governance.fact_dq_result;
CREATE UNIQUE INDEX ux_fact_dq_result_bk ON edw_g9_governance.fact_dq_result (dq_result_bk);
CREATE INDEX ix_fact_dq_result_rule_day ON edw_g9_governance.fact_dq_result (dq_rule_sk, plane_code, evaluated_date_sk);
CREATE INDEX ix_fact_dq_result_target ON edw_g9_governance.fact_dq_result (target_schema_name, target_table_name, evaluated_date_sk);
CREATE INDEX ix_fact_dq_result_subject ON edw_g9_governance.fact_dq_result (subject_bk);
COMMENT ON TABLE edw_g9_governance.fact_dq_result IS 'https://ontology.healix.example.org/governance/DataQualityResult';
COMMENT ON COLUMN edw_g9_governance.fact_dq_result.dq_result_bk IS 'internal | https://ontology.healix.example.org/governance/dqResultIdentifier';
COMMENT ON COLUMN edw_g9_governance.fact_dq_result.outcome_code IS 'internal | https://ontology.healix.example.org/governance/dqOutcome';
COMMENT ON COLUMN edw_g9_governance.fact_dq_result.plane_code IS 'internal | https://ontology.healix.example.org/governance/dqPlane';
COMMENT ON COLUMN edw_g9_governance.fact_dq_result.subject_bk IS 'pseudonymised | https://ontology.healix.example.org/governance/evaluatedRecord';
COMMENT ON COLUMN edw_g9_governance.fact_dq_result.detail_text IS 'pseudonymised | https://ontology.healix.example.org/governance/dqFailureDetail';
COMMENT ON COLUMN edw_g9_governance.fact_dq_result.event_time IS 'internal | https://ontology.healix.example.org/time/eventTime';
COMMENT ON COLUMN edw_g9_governance.fact_dq_result.prov_activity_iri IS 'internal | http://www.w3.org/ns/prov#wasGeneratedBy';

-- ---------------------------------------------------------------------------
-- fact_dq_issue: one row per hc.dq.issue.v1 state version (module 09 DQ-03 §6.2,
-- DQ-05 §3). Every workflow transition appends a new bitemporal version of the
-- same dq_issue_bk and supersedes the previous one (DQ-REM-009); the current
-- state is the row with superseded_at IS NULL. Also opened by the EDW↔KG
-- reconciliation (EDW-REC-002).
-- ---------------------------------------------------------------------------
CREATE TABLE edw_g9_governance.fact_dq_issue (
    dq_issue_hk             CHAR(64)      NOT NULL,   -- SHA-256(dq_issue_bk, valid_from)
    dq_issue_bk             VARCHAR(64)   NOT NULL,   -- issue_id (ULID)
    dq_rule_sk              BIGINT        NOT NULL,
    dq_rule_bk              VARCHAR(128)  NOT NULL,
    dq_rule_version         VARCHAR(20)   NOT NULL,
    severity_code           VARCHAR(10)   NOT NULL,   -- blocker / critical / major / minor (may be escalated, DQ-04 §4.2)
    issue_state_code        VARCHAR(20)   NOT NULL,   -- detected / acknowledged / auto_fixing / steward_review / source_requested / escalated / verifying / resolved / accepted / reopened
    issue_grain_code        VARCHAR(12)   NOT NULL,   -- record / hourly / window
    issue_source_system_sk  INTEGER       NOT NULL,   -- source system of the failing data
    facility_code           VARCHAR(50),
    window_start_ts         TIMESTAMP,
    window_end_ts           TIMESTAMP,
    first_seen_ts           TIMESTAMP     NOT NULL,
    first_seen_date_sk      INTEGER       NOT NULL,
    first_seen_time_sk      SMALLINT      NOT NULL,
    last_seen_ts            TIMESTAMP     NOT NULL,
    occurrence_count        BIGINT        NOT NULL,   -- `count`
    pass_rate_in_window_pct DECIMAL(7,4),
    subjects_json           VARCHAR(4000),            -- ≤ 100 business keys, hashed where PHI
    quarantine_ids_json     VARCHAR(4000),
    owner_steward_sk        BIGINT        NOT NULL,   -- `owner` → dim_data_steward
    assignee_id             VARCHAR(100),             -- principal or agent.<role>
    remediation_action_code VARCHAR(12),              -- auto_fix / steward / source_fix / accept
    source_request_ref      VARCHAR(100),             -- ticket with the source owner (state source_requested)
    sla_ack_due_ts          TIMESTAMP,
    sla_fix_due_ts          TIMESTAMP,
    acknowledged_ts         TIMESTAMP,
    resolved_ts             TIMESTAMP,
    sla_ack_status_code     VARCHAR(10),              -- within / at-risk / breached
    sla_fix_status_code     VARCHAR(10),
    planes_agreeing_text    VARCHAR(30),              -- comma-separated planes that reproduce the failure
    reconciliation_status_code VARCHAR(20),           -- agree / stream-only / warehouse-only / graph-only / drift (DQ-03 §7)
    reconciliation_check_bk VARCHAR(128),             -- fact_reconciliation_check key when opened by EDW↔KG reconciliation
    transition_actor_id     VARCHAR(100),             -- who moved the issue into this state (DQ-05 §3)
    transition_note         VARCHAR(1000),
    -- bitemporal block (TKG-01 §9; valid_from = time the state became true, event_time = triggering result/transition event)
    valid_from              TIMESTAMP     NOT NULL,
    valid_to                TIMESTAMP,
    recorded_at             TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    superseded_at           TIMESTAMP,
    change_kind             CHAR(12),                     -- TKG-01 C6/C10: why this version was closed; NULL while current, NOT NULL when superseded_at is set (new-version / correction / retraction / merge / unmerge)
    change_reason_code      VARCHAR(30),
    event_time              TIMESTAMP     NOT NULL,       -- TKG-01 C7: hc-event-time of the producing record (UTC)
    source_time             TIMESTAMP,                    -- TKG-01 C7: hc-source-time, the source system clock (UTC)
    decision_time           TIMESTAMP,                    -- TKG-01 C8: when the steward/agent decision was taken; NULL = not available
    source_tz               VARCHAR(64),                  -- TKG-01 C12: IANA zone of the source timestamps (storage is UTC)
    -- technical block
    source_system_sk        INTEGER       NOT NULL,
    load_batch_id           BIGINT        NOT NULL,
    record_hash             CHAR(64)      NOT NULL,
    prov_activity_iri       VARCHAR(512)  NOT NULL,
    CONSTRAINT pk_fact_dq_issue PRIMARY KEY (dq_issue_hk),
    CONSTRAINT fk_dq_issue_rule FOREIGN KEY (dq_rule_sk)
        REFERENCES edw_g9_governance.dim_dq_rule (dq_rule_sk),
    CONSTRAINT fk_dq_issue_issue_src FOREIGN KEY (issue_source_system_sk)
        REFERENCES edw_g8_reference.dim_source_system (source_system_sk),
    CONSTRAINT fk_dq_issue_first_date FOREIGN KEY (first_seen_date_sk)
        REFERENCES edw_g8_reference.dim_date (date_sk),
    CONSTRAINT fk_dq_issue_first_time FOREIGN KEY (first_seen_time_sk)
        REFERENCES edw_g8_reference.dim_time (time_sk),
    CONSTRAINT fk_dq_issue_owner FOREIGN KEY (owner_steward_sk)
        REFERENCES edw_g9_governance.dim_data_steward (steward_sk),
    CONSTRAINT fk_dq_issue_src FOREIGN KEY (source_system_sk)
        REFERENCES edw_g8_reference.dim_source_system (source_system_sk)
);
-- TD: CREATE MULTISET TABLE edw_g9_governance.fact_dq_issue, FALLBACK (...) PRIMARY INDEX (dq_issue_bk)
-- TD: PARTITION BY (RANGE_N(CAST(first_seen_ts AS DATE) BETWEEN DATE '2025-01-01' AND DATE '2040-12-31' EACH INTERVAL '1' MONTH, NO RANGE),
-- TD:               CASE_N(superseded_at IS NULL, NO CASE))
CREATE UNIQUE INDEX ux_fact_dq_issue_bk ON edw_g9_governance.fact_dq_issue (dq_issue_bk, valid_from, recorded_at);
CREATE INDEX ix_fact_dq_issue_state ON edw_g9_governance.fact_dq_issue (issue_state_code, severity_code, owner_steward_sk);
CREATE INDEX ix_fact_dq_issue_rule ON edw_g9_governance.fact_dq_issue (dq_rule_sk, first_seen_date_sk);
COMMENT ON TABLE edw_g9_governance.fact_dq_issue IS 'https://ontology.healix.example.org/governance/DQIssue';
COMMENT ON COLUMN edw_g9_governance.fact_dq_issue.dq_issue_bk IS 'internal | https://ontology.healix.example.org/governance/dqIssueIdentifier';
COMMENT ON COLUMN edw_g9_governance.fact_dq_issue.issue_state_code IS 'internal | https://ontology.healix.example.org/governance/dqIssueState';
COMMENT ON COLUMN edw_g9_governance.fact_dq_issue.subjects_json IS 'pseudonymised | https://ontology.healix.example.org/governance/dqIssueSubjects';
COMMENT ON COLUMN edw_g9_governance.fact_dq_issue.owner_steward_sk IS 'internal | https://ontology.healix.example.org/governance/ownedBy';
COMMENT ON COLUMN edw_g9_governance.fact_dq_issue.prov_activity_iri IS 'internal | http://www.w3.org/ns/prov#wasGeneratedBy';

-- ---------------------------------------------------------------------------
-- fact_dq_profile: profiling baselines per (rule target, source system,
-- facility) written by agent.dq_monitor (module 09 DQ-01 §4.3, DQ-FW-009,
-- DQ-PIPE-011). Versioned; profiling-derived rules cite baseline_ref.
-- Grain: one row per baseline version (target × source × facility × window).
-- ---------------------------------------------------------------------------
CREATE TABLE edw_g9_governance.fact_dq_profile (
    dq_profile_hk           CHAR(64)      NOT NULL,   -- SHA-256(baseline_ref, baseline_version)
    baseline_ref            VARCHAR(255)  NOT NULL,   -- URN cited by profiling-derived rules (derived_from)
    baseline_version        VARCHAR(20)   NOT NULL,
    target_plane_code       VARCHAR(10)   NOT NULL,   -- stream / warehouse
    target_locator          VARCHAR(255)  NOT NULL,   -- <ResourceType>.<element path> or <schema>.<table>.<column>
    target_schema_name      VARCHAR(30),
    target_table_name       VARCHAR(30),
    target_column_name      VARCHAR(30),
    profiled_source_system_sk INTEGER     NOT NULL,   -- source system whose data was profiled
    facility_code           VARCHAR(50),
    window_start_ts         TIMESTAMP     NOT NULL,
    window_end_ts           TIMESTAMP     NOT NULL,
    profile_date_sk         INTEGER       NOT NULL,   -- day the window ended
    row_count               BIGINT,
    null_count              BIGINT,
    null_rate_pct           DECIMAL(7,4),
    distinct_count          BIGINT,
    min_value_numeric       DECIMAL(18,6),
    max_value_numeric       DECIMAL(18,6),
    mean_value_numeric      DECIMAL(18,6),
    stddev_value_numeric    DECIMAL(18,6),
    p01_value_numeric       DECIMAL(18,6),
    p05_value_numeric       DECIMAL(18,6),
    p50_value_numeric       DECIMAL(18,6),
    p95_value_numeric       DECIMAL(18,6),
    p99_value_numeric       DECIMAL(18,6),
    min_string_length       INTEGER,
    max_string_length       INTEGER,
    mean_string_length      DECIMAL(9,2),
    top_values_json         VARCHAR(4000),            -- code / value frequencies, masked per module 11
    new_code_count          INTEGER,                  -- codes unseen in the trailing 90 days
    inter_arrival_p50_seconds DECIMAL(12,3),
    inter_arrival_p99_seconds DECIMAL(12,3),
    psi_vs_prior            DECIMAL(9,6),             -- Population Stability Index against the previous baseline (DQ-03 §8)
    is_baseline_flag        CHAR(1)       NOT NULL,   -- Y = adopted as baseline; N = observation only
    profiler_agent_id       VARCHAR(50),              -- agent.dq_monitor
    -- bitemporal block (TKG-01 §9; valid_from = window_end_ts, event_time = profiling run time)
    valid_from              TIMESTAMP     NOT NULL,
    valid_to                TIMESTAMP,
    recorded_at             TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    superseded_at           TIMESTAMP,
    change_kind             CHAR(12),                     -- TKG-01 C6/C10: why this version was closed; NULL while current, NOT NULL when superseded_at is set (new-version / correction / retraction / merge / unmerge)
    change_reason_code      VARCHAR(30),
    event_time              TIMESTAMP     NOT NULL,       -- TKG-01 C7: hc-event-time of the producing record (UTC)
    source_time             TIMESTAMP,                    -- TKG-01 C7: hc-source-time, the source system clock (UTC)
    decision_time           TIMESTAMP,                    -- TKG-01 C8: when the baseline was adopted; NULL = not available
    source_tz               VARCHAR(64),                  -- TKG-01 C12: IANA zone of the source timestamps (storage is UTC)
    -- technical block
    source_system_sk        INTEGER       NOT NULL,
    load_batch_id           BIGINT        NOT NULL,
    record_hash             CHAR(64)      NOT NULL,
    prov_activity_iri       VARCHAR(512)  NOT NULL,
    CONSTRAINT pk_fact_dq_profile PRIMARY KEY (dq_profile_hk),
    CONSTRAINT fk_dq_profile_profiled_src FOREIGN KEY (profiled_source_system_sk)
        REFERENCES edw_g8_reference.dim_source_system (source_system_sk),
    CONSTRAINT fk_dq_profile_date FOREIGN KEY (profile_date_sk)
        REFERENCES edw_g8_reference.dim_date (date_sk),
    CONSTRAINT fk_dq_profile_src FOREIGN KEY (source_system_sk)
        REFERENCES edw_g8_reference.dim_source_system (source_system_sk)
);
-- TD: CREATE MULTISET TABLE edw_g9_governance.fact_dq_profile, FALLBACK (...) PRIMARY INDEX (target_locator)
-- TD: PARTITION BY RANGE_N(CAST(window_end_ts AS DATE) BETWEEN DATE '2025-01-01' AND DATE '2040-12-31' EACH INTERVAL '1' MONTH, NO RANGE)
CREATE UNIQUE INDEX ux_fact_dq_profile_ref ON edw_g9_governance.fact_dq_profile (baseline_ref, baseline_version);
CREATE INDEX ix_fact_dq_profile_target ON edw_g9_governance.fact_dq_profile (target_locator, profiled_source_system_sk, facility_code, profile_date_sk);
COMMENT ON TABLE edw_g9_governance.fact_dq_profile IS 'https://ontology.healix.example.org/governance/DataProfileBaseline';
COMMENT ON COLUMN edw_g9_governance.fact_dq_profile.baseline_ref IS 'internal | https://ontology.healix.example.org/governance/baselineReference';
COMMENT ON COLUMN edw_g9_governance.fact_dq_profile.top_values_json IS 'pseudonymised | https://ontology.healix.example.org/governance/topValueFrequencies';
COMMENT ON COLUMN edw_g9_governance.fact_dq_profile.prov_activity_iri IS 'internal | http://www.w3.org/ns/prov#wasGeneratedBy';

-- ---------------------------------------------------------------------------
-- agg_dq_result_day: daily roll-up of fact_dq_result by
-- (rule_id, rule_version, plane, source_system, facility, day) — module 09
-- DQ-04 §6, DQ-SCORE-007/008. Retained 7 years; record-grain results 90 days.
-- Recomputation (backfill, late results) supersedes the prior row so published
-- scorecards remain reproducible from the bitemporal history.
-- Module-09 SQL names: rule_id = dq_rule_bk, rule_version = dq_rule_version,
-- plane = plane_code, source_system = result_source_system_sk, day = day_date_sk,
-- passed/failed/errored/denominator = *_count.
-- ---------------------------------------------------------------------------
CREATE TABLE edw_g9_governance.agg_dq_result_day (
    agg_dq_result_day_hk    CHAR(64)      NOT NULL,   -- SHA-256(dq_rule_bk, dq_rule_version, plane_code, result_source_system_sk, facility_code, day_date_sk)
    dq_rule_sk              BIGINT        NOT NULL,
    dq_rule_bk              VARCHAR(128)  NOT NULL,
    dq_rule_version         VARCHAR(20)   NOT NULL,
    pack_version            VARCHAR(20)   NOT NULL,
    plane_code              VARCHAR(10)   NOT NULL,   -- stream / graph / warehouse
    result_source_system_sk INTEGER       NOT NULL,
    facility_code           VARCHAR(50),
    day_date_sk             INTEGER       NOT NULL,
    passed_count            BIGINT        NOT NULL,
    failed_count            BIGINT        NOT NULL,
    errored_count           BIGINT        NOT NULL,
    pending_count           BIGINT        NOT NULL,
    denominator_count       BIGINT        NOT NULL,   -- passed + failed (errors excluded, DQ-PIPE-003)
    pass_rate_pct           DECIMAL(7,4),
    evaluation_latency_p95_ms INTEGER,                -- plane evaluation latency
    detection_latency_p95_seconds DECIMAL(12,3),      -- event_time → first fail (MTTD input, DQ-PIPE-012)
    breach_flag             CHAR(1)       NOT NULL,   -- pass rate below threshold.min for the day
    is_backfill_flag        CHAR(1)       NOT NULL DEFAULT 'N',
    -- bitemporal block (valid_from = day start; recomputation supersedes)
    valid_from              TIMESTAMP     NOT NULL,
    valid_to                TIMESTAMP,
    recorded_at             TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    superseded_at           TIMESTAMP,
    change_kind             CHAR(12),                     -- TKG-01 C6/C10: why this version was closed; NULL while current, NOT NULL when superseded_at is set (new-version / correction / retraction / merge / unmerge)
    change_reason_code      VARCHAR(30),
    event_time              TIMESTAMP     NOT NULL,       -- TKG-01 C7: aggregation run time (UTC)
    source_time             TIMESTAMP,                    -- TKG-01 C7: latest evaluated_ts folded into the row
    decision_time           TIMESTAMP,                    -- TKG-01 C8: not applicable to aggregates
    source_tz               VARCHAR(64),                  -- TKG-01 C12: IANA zone of the facility day boundary
    -- technical block
    source_system_sk        INTEGER       NOT NULL,
    load_batch_id           BIGINT        NOT NULL,
    record_hash             CHAR(64)      NOT NULL,
    prov_activity_iri       VARCHAR(512)  NOT NULL,
    CONSTRAINT pk_agg_dq_result_day PRIMARY KEY (agg_dq_result_day_hk),
    CONSTRAINT fk_agg_dq_rule FOREIGN KEY (dq_rule_sk)
        REFERENCES edw_g9_governance.dim_dq_rule (dq_rule_sk),
    CONSTRAINT fk_agg_dq_result_src FOREIGN KEY (result_source_system_sk)
        REFERENCES edw_g8_reference.dim_source_system (source_system_sk),
    CONSTRAINT fk_agg_dq_day FOREIGN KEY (day_date_sk)
        REFERENCES edw_g8_reference.dim_date (date_sk),
    CONSTRAINT fk_agg_dq_src FOREIGN KEY (source_system_sk)
        REFERENCES edw_g8_reference.dim_source_system (source_system_sk)
);
-- TD: CREATE MULTISET TABLE edw_g9_governance.agg_dq_result_day, FALLBACK (...) PRIMARY INDEX (dq_rule_sk, day_date_sk)
-- TD: PARTITION BY RANGE_N(day_date_sk BETWEEN 20250101 AND 20401231 EACH 100, NO RANGE)
CREATE INDEX ix_agg_dq_result_day_rule ON edw_g9_governance.agg_dq_result_day (dq_rule_bk, dq_rule_version, plane_code, day_date_sk);
COMMENT ON TABLE edw_g9_governance.agg_dq_result_day IS 'https://ontology.healix.example.org/governance/DataQualityDailyAggregate';
COMMENT ON COLUMN edw_g9_governance.agg_dq_result_day.pass_rate_pct IS 'internal | https://ontology.healix.example.org/governance/passRate';
COMMENT ON COLUMN edw_g9_governance.agg_dq_result_day.prov_activity_iri IS 'internal | http://www.w3.org/ns/prov#wasGeneratedBy';
