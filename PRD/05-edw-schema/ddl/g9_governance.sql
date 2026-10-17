-- =============================================================================
-- HEALIX EDW · module 05 · g9_governance.sql
-- G9 Metadata, Quality & Governance · schema edw_g9_governance
-- Tables: fact_load_batch, fact_lineage_activity, bridge_lineage_activity_entity,
--         dim_dq_dimension, dim_dq_rule, fact_dq_result
-- Specs:  01-subject-area-groups.md §G9 · module 09 (DQ result contract is
--         authoritative for fact_dq_result, EDW-G9-001) · module 11 (PROV-O)
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
    recorded_at             TIMESTAMP     NOT NULL,
    superseded_at           TIMESTAMP,
    change_reason_code      VARCHAR(30),
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
COMMENT ON COLUMN edw_g9_governance.fact_load_batch.prov_activity_iri IS 'none | http://www.w3.org/ns/prov#wasGeneratedBy';
COMMENT ON COLUMN edw_g9_governance.fact_load_batch.content_checksum IS 'none | https://ontology.healix.example.org/governance/contentChecksum';

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
    recorded_at             TIMESTAMP     NOT NULL,
    superseded_at           TIMESTAMP,
    change_reason_code      VARCHAR(30),
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
COMMENT ON COLUMN edw_g9_governance.fact_lineage_activity.started_ts IS 'none | http://www.w3.org/ns/prov#startedAtTime';
COMMENT ON COLUMN edw_g9_governance.fact_lineage_activity.ended_ts IS 'none | http://www.w3.org/ns/prov#endedAtTime';
COMMENT ON COLUMN edw_g9_governance.fact_lineage_activity.agent_id IS 'none | http://www.w3.org/ns/prov#wasAssociatedWith';
COMMENT ON COLUMN edw_g9_governance.fact_lineage_activity.was_informed_by_iri IS 'none | http://www.w3.org/ns/prov#wasInformedBy';

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
    recorded_at             TIMESTAMP     NOT NULL,
    superseded_at           TIMESTAMP,
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
-- dim_dq_dimension (Type 1): completeness, conformity, consistency, timeliness,
-- uniqueness, accuracy, validity
-- ---------------------------------------------------------------------------
CREATE TABLE edw_g9_governance.dim_dq_dimension (
    dq_dimension_sk         SMALLINT      NOT NULL,
    dq_dimension_code       VARCHAR(20)   NOT NULL,
    dq_dimension_name       VARCHAR(50)   NOT NULL,
    dq_dimension_definition VARCHAR(500),
    code_sk                 BIGINT        NOT NULL,   -- dim_code row in healix:scheme/dq-dimension
    source_system_sk        INTEGER       NOT NULL,
    load_batch_id           BIGINT        NOT NULL,
    record_hash             CHAR(64)      NOT NULL,
    recorded_at             TIMESTAMP     NOT NULL,
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

-- ---------------------------------------------------------------------------
-- dim_dq_rule (Type 2): one row per DQ rule version
-- Rule ids follow CONVENTIONS §3: DQR-<DIMENSION>-<DOMAIN>-<NNN>
-- ---------------------------------------------------------------------------
CREATE TABLE edw_g9_governance.dim_dq_rule (
    dq_rule_sk              BIGINT        NOT NULL,
    dq_rule_bk              VARCHAR(128)  NOT NULL,   -- DQR-COMPLETENESS-PATIENT-001
    dq_rule_version         VARCHAR(20)   NOT NULL,
    dq_rule_name            VARCHAR(255)  NOT NULL,
    dq_rule_description     VARCHAR(1000),
    dq_dimension_sk         SMALLINT      NOT NULL,
    domain_code             VARCHAR(30)   NOT NULL,   -- PATIENT / ENCOUNTER / CLAIM ... (module 09 domains)
    group_code              CHAR(2)       NOT NULL,   -- G1..G9 of the target data
    target_schema_name      VARCHAR(30),
    target_table_name       VARCHAR(30),
    target_column_name      VARCHAR(30),
    target_fhir_path        VARCHAR(255),
    rule_type_code          VARCHAR(20)   NOT NULL,   -- shacl / sql / expression / statistical / referential
    rule_expression         VARCHAR(4000),            -- SHACL shape IRI, SQL predicate or expression text
    shacl_shape_iri         VARCHAR(512),
    derived_from_constraint_iri VARCHAR(512),         -- ontology constraint (OWL axiom / SHACL) the rule derives from
    ontology_property_iri   VARCHAR(512),             -- property measured
    severity_code           VARCHAR(10)   NOT NULL,   -- critical / high / medium / low
    threshold_pct           DECIMAL(5,2),             -- pass-rate threshold for batch-level evaluation
    is_critical_data_element_flag CHAR(1) NOT NULL DEFAULT 'N',
    evaluation_stage_code   VARCHAR(12)   NOT NULL,   -- std / canonical / edw / kg
    authored_by_agent_id    VARCHAR(50),              -- agent.dq_author
    approved_by             VARCHAR(100),
    rule_status_code        VARCHAR(12)   NOT NULL,   -- draft / active / suspended / retired
    -- valid-time block (Type 2)
    valid_from              TIMESTAMP     NOT NULL,
    valid_to                TIMESTAMP,
    is_current_flag         CHAR(1)       NOT NULL DEFAULT 'Y',
    -- technical block
    source_system_sk        INTEGER       NOT NULL,
    load_batch_id           BIGINT        NOT NULL,
    record_hash             CHAR(64)      NOT NULL,
    recorded_at             TIMESTAMP     NOT NULL,
    prov_activity_iri       VARCHAR(512)  NOT NULL,
    CONSTRAINT pk_dim_dq_rule PRIMARY KEY (dq_rule_sk),
    CONSTRAINT fk_dim_dq_rule_dimension FOREIGN KEY (dq_dimension_sk)
        REFERENCES edw_g9_governance.dim_dq_dimension (dq_dimension_sk),
    CONSTRAINT fk_dim_dq_rule_src FOREIGN KEY (source_system_sk)
        REFERENCES edw_g8_reference.dim_source_system (source_system_sk)
);
-- TD: UNIQUE PRIMARY INDEX (dq_rule_sk)
CREATE UNIQUE INDEX ux_dim_dq_rule_bk ON edw_g9_governance.dim_dq_rule (dq_rule_bk, dq_rule_version, valid_from);
COMMENT ON TABLE edw_g9_governance.dim_dq_rule IS 'https://ontology.healix.example.org/governance/DataQualityRule';
COMMENT ON COLUMN edw_g9_governance.dim_dq_rule.dq_rule_bk IS 'none | https://ontology.healix.example.org/governance/dqRuleIdentifier';
COMMENT ON COLUMN edw_g9_governance.dim_dq_rule.shacl_shape_iri IS 'none | http://www.w3.org/ns/shacl#shape';
COMMENT ON COLUMN edw_g9_governance.dim_dq_rule.derived_from_constraint_iri IS 'none | http://www.w3.org/ns/prov#wasDerivedFrom';

-- ---------------------------------------------------------------------------
-- fact_dq_result: one row per rule × evaluated record/batch × evaluation time.
-- Field set mirrors the module-09 result contract (hc.dq.* payload); module 09
-- is authoritative and this table is regenerated when it changes (EDW-G9-001).
-- ---------------------------------------------------------------------------
CREATE TABLE edw_g9_governance.fact_dq_result (
    dq_result_hk            CHAR(64)      NOT NULL,   -- SHA-256(dq_run_id, dq_rule_bk, record_bk, evaluated_ts)
    dq_run_id               VARCHAR(64)   NOT NULL,
    dq_rule_sk              BIGINT        NOT NULL,
    dq_dimension_sk         SMALLINT      NOT NULL,
    evaluation_stage_code   VARCHAR(12)   NOT NULL,   -- std / canonical / edw / kg
    kafka_topic             VARCHAR(255),
    target_schema_name      VARCHAR(30),
    target_table_name       VARCHAR(30),
    target_column_name      VARCHAR(30),
    record_bk               VARCHAR(128),             -- business key of the evaluated record; NULL for batch-level
    record_resource_type    VARCHAR(50),              -- FHIR resource type when canonical-stage
    evaluated_load_batch_id BIGINT,                   -- batch whose rows were evaluated
    evaluated_ts            TIMESTAMP     NOT NULL,
    evaluated_date_sk       INTEGER       NOT NULL,
    evaluated_time_sk       SMALLINT      NOT NULL,
    outcome_code            VARCHAR(8)    NOT NULL,   -- pass / fail / warn / error / skipped
    measured_value          DECIMAL(18,6),
    threshold_value         DECIMAL(18,6),
    records_evaluated       BIGINT,
    records_failed          BIGINT,
    pass_rate_pct           DECIMAL(5,2),
    failure_detail_json     VARCHAR(4000),            -- offending values (masked per phi_class), SHACL result path
    quarantine_topic        VARCHAR(255),
    remediation_issue_bk    VARCHAR(64),              -- fact_dq_issue business key when opened
    evaluator_agent_id      VARCHAR(50),              -- agent.dq_monitor
    terminology_release_id  VARCHAR(50),
    ontology_release_iri    VARCHAR(512),
    -- bitemporal block
    valid_from              TIMESTAMP     NOT NULL,   -- = evaluated_ts
    valid_to                TIMESTAMP,
    recorded_at             TIMESTAMP     NOT NULL,
    superseded_at           TIMESTAMP,
    change_reason_code      VARCHAR(30),
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
    CONSTRAINT fk_dq_result_eval_batch FOREIGN KEY (evaluated_load_batch_id)
        REFERENCES edw_g9_governance.fact_load_batch (load_batch_id),
    CONSTRAINT fk_dq_result_date FOREIGN KEY (evaluated_date_sk)
        REFERENCES edw_g8_reference.dim_date (date_sk),
    CONSTRAINT fk_dq_result_time FOREIGN KEY (evaluated_time_sk)
        REFERENCES edw_g8_reference.dim_time (time_sk),
    CONSTRAINT fk_dq_result_src FOREIGN KEY (source_system_sk)
        REFERENCES edw_g8_reference.dim_source_system (source_system_sk)
);
-- TD: CREATE MULTISET TABLE edw_g9_governance.fact_dq_result, FALLBACK, BLOCKCOMPRESSION = AUTOTEMP (...) PRIMARY INDEX (dq_rule_sk, record_bk)
-- TD: PARTITION BY (RANGE_N(CAST(evaluated_ts AS DATE) BETWEEN DATE '2025-01-01' AND DATE '2040-12-31' EACH INTERVAL '1' DAY, NO RANGE),
-- TD:               CASE_N(superseded_at IS NULL, NO CASE))
-- TD: COLLECT STATISTICS COLUMN (dq_rule_sk), COLUMN (outcome_code), COLUMN (target_schema_name, target_table_name), COLUMN (PARTITION) ON edw_g9_governance.fact_dq_result;
CREATE INDEX ix_fact_dq_result_target ON edw_g9_governance.fact_dq_result (target_schema_name, target_table_name, evaluated_date_sk);
CREATE INDEX ix_fact_dq_result_record ON edw_g9_governance.fact_dq_result (record_bk);
COMMENT ON TABLE edw_g9_governance.fact_dq_result IS 'https://ontology.healix.example.org/governance/DataQualityResult';
COMMENT ON COLUMN edw_g9_governance.fact_dq_result.outcome_code IS 'none | https://ontology.healix.example.org/governance/dqOutcome';
COMMENT ON COLUMN edw_g9_governance.fact_dq_result.record_bk IS 'none | https://ontology.healix.example.org/governance/evaluatedRecord';
COMMENT ON COLUMN edw_g9_governance.fact_dq_result.failure_detail_json IS 'phi_quasi | https://ontology.healix.example.org/governance/dqFailureDetail';
COMMENT ON COLUMN edw_g9_governance.fact_dq_result.prov_activity_iri IS 'none | http://www.w3.org/ns/prov#wasGeneratedBy';
