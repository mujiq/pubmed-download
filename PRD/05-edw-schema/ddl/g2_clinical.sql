-- =============================================================================
-- HEALIX EDW · module 05 · g2_clinical.sql
-- G2 Clinical Care · schemas edw_g2_clinical, edw_g2_diagnostics, edw_g2_medication
-- Tables (edw_g2_clinical):    dim_encounter_class, dim_encounter_type,
--         dim_admit_source, dim_discharge_disposition, dim_encounter_status_junk,
--         dim_diagnosis_type, dim_clinical_status, fact_encounter, fact_diagnosis,
--         bridge_encounter_diagnosis
-- Tables (edw_g2_diagnostics): dim_result_status, dim_abnormal_flag,
--         fact_observation_result
-- Tables (edw_g2_medication):  dim_route, dim_dose_form, dim_med_order_status,
--         fact_medication_order
-- Specs:  01-subject-area-groups.md §G2 · 03-logical-model.md §9.3–9.4
-- Depends on: g8_reference.sql, g1_party.sql, g4_supply.sql (dim_item)
-- Dialect: portable ANSI; Teradata hints as "-- TD:" comments.
-- =============================================================================

-- ===========================================================================
-- edw_g2_clinical · value-set dimensions (Type 1, bound to dim_code)
-- ===========================================================================
CREATE TABLE edw_g2_clinical.dim_encounter_class (
    encounter_class_sk      BIGINT        NOT NULL,
    code_sk                 BIGINT        NOT NULL,
    code                    VARCHAR(100)  NOT NULL,   -- v3-ActCode: IMP / AMB / EMER / OBSENC / VR / HH / SS
    display                 VARCHAR(500),
    code_system_uri         VARCHAR(255)  NOT NULL,
    value_set_iri           VARCHAR(512)  NOT NULL,   -- https://ontology.healix.example.org/scheme/encounter-class
    is_inpatient_flag       CHAR(1)       NOT NULL DEFAULT 'N',
    sort_order              SMALLINT,
    source_system_sk        INTEGER       NOT NULL,
    load_batch_id           BIGINT        NOT NULL,
    record_hash             CHAR(64)      NOT NULL,
    recorded_at             TIMESTAMP     NOT NULL,
    prov_activity_iri       VARCHAR(512)  NOT NULL,
    CONSTRAINT pk_dim_encounter_class PRIMARY KEY (encounter_class_sk),
    CONSTRAINT fk_dim_enc_class_code FOREIGN KEY (code_sk)
        REFERENCES edw_g8_reference.dim_code (code_sk),
    CONSTRAINT fk_dim_enc_class_src FOREIGN KEY (source_system_sk)
        REFERENCES edw_g8_reference.dim_source_system (source_system_sk)
);
-- TD: UNIQUE PRIMARY INDEX (encounter_class_sk)
COMMENT ON TABLE edw_g2_clinical.dim_encounter_class IS 'https://ontology.healix.example.org/clinical/EncounterClass';
COMMENT ON COLUMN edw_g2_clinical.dim_encounter_class.code IS 'public | https://ontology.healix.example.org/clinical/encounterClass';

CREATE TABLE edw_g2_clinical.dim_encounter_type (
    encounter_type_sk       BIGINT        NOT NULL,
    code_sk                 BIGINT        NOT NULL,
    code                    VARCHAR(100)  NOT NULL,
    display                 VARCHAR(500),
    code_system_uri         VARCHAR(255)  NOT NULL,
    value_set_iri           VARCHAR(512)  NOT NULL,
    sort_order              SMALLINT,
    source_system_sk        INTEGER       NOT NULL,
    load_batch_id           BIGINT        NOT NULL,
    record_hash             CHAR(64)      NOT NULL,
    recorded_at             TIMESTAMP     NOT NULL,
    prov_activity_iri       VARCHAR(512)  NOT NULL,
    CONSTRAINT pk_dim_encounter_type PRIMARY KEY (encounter_type_sk),
    CONSTRAINT fk_dim_enc_type_code FOREIGN KEY (code_sk)
        REFERENCES edw_g8_reference.dim_code (code_sk),
    CONSTRAINT fk_dim_enc_type_src FOREIGN KEY (source_system_sk)
        REFERENCES edw_g8_reference.dim_source_system (source_system_sk)
);
-- TD: UNIQUE PRIMARY INDEX (encounter_type_sk)
COMMENT ON TABLE edw_g2_clinical.dim_encounter_type IS 'https://ontology.healix.example.org/clinical/EncounterType';

CREATE TABLE edw_g2_clinical.dim_admit_source (
    admit_source_sk         BIGINT        NOT NULL,
    code_sk                 BIGINT        NOT NULL,
    code                    VARCHAR(100)  NOT NULL,
    display                 VARCHAR(500),
    code_system_uri         VARCHAR(255)  NOT NULL,
    value_set_iri           VARCHAR(512)  NOT NULL,
    sort_order              SMALLINT,
    source_system_sk        INTEGER       NOT NULL,
    load_batch_id           BIGINT        NOT NULL,
    record_hash             CHAR(64)      NOT NULL,
    recorded_at             TIMESTAMP     NOT NULL,
    prov_activity_iri       VARCHAR(512)  NOT NULL,
    CONSTRAINT pk_dim_admit_source PRIMARY KEY (admit_source_sk),
    CONSTRAINT fk_dim_admit_source_code FOREIGN KEY (code_sk)
        REFERENCES edw_g8_reference.dim_code (code_sk),
    CONSTRAINT fk_dim_admit_source_src FOREIGN KEY (source_system_sk)
        REFERENCES edw_g8_reference.dim_source_system (source_system_sk)
);
-- TD: UNIQUE PRIMARY INDEX (admit_source_sk)
COMMENT ON TABLE edw_g2_clinical.dim_admit_source IS 'https://ontology.healix.example.org/clinical/AdmitSource';

CREATE TABLE edw_g2_clinical.dim_discharge_disposition (
    discharge_disposition_sk BIGINT       NOT NULL,
    code_sk                 BIGINT        NOT NULL,
    code                    VARCHAR(100)  NOT NULL,   -- NUBC patient discharge status
    display                 VARCHAR(500),
    code_system_uri         VARCHAR(255)  NOT NULL,
    value_set_iri           VARCHAR(512)  NOT NULL,
    is_expired_flag         CHAR(1)       NOT NULL DEFAULT 'N',
    is_transfer_flag        CHAR(1)       NOT NULL DEFAULT 'N',
    sort_order              SMALLINT,
    source_system_sk        INTEGER       NOT NULL,
    load_batch_id           BIGINT        NOT NULL,
    record_hash             CHAR(64)      NOT NULL,
    recorded_at             TIMESTAMP     NOT NULL,
    prov_activity_iri       VARCHAR(512)  NOT NULL,
    CONSTRAINT pk_dim_discharge_disposition PRIMARY KEY (discharge_disposition_sk),
    CONSTRAINT fk_dim_disch_disp_code FOREIGN KEY (code_sk)
        REFERENCES edw_g8_reference.dim_code (code_sk),
    CONSTRAINT fk_dim_disch_disp_src FOREIGN KEY (source_system_sk)
        REFERENCES edw_g8_reference.dim_source_system (source_system_sk)
);
-- TD: UNIQUE PRIMARY INDEX (discharge_disposition_sk)
COMMENT ON TABLE edw_g2_clinical.dim_discharge_disposition IS 'https://ontology.healix.example.org/clinical/DischargeDisposition';

-- junk dimension: one row per observed combination of low-cardinality flags (03-logical-model.md §3.3)
CREATE TABLE edw_g2_clinical.dim_encounter_status_junk (
    encounter_status_junk_sk BIGINT       NOT NULL,
    encounter_status_code   VARCHAR(20)   NOT NULL,   -- planned / arrived / triaged / in-progress / onleave / finished / cancelled / entered-in-error
    hospitalization_flag    CHAR(1)       NOT NULL,
    readmission_flag        CHAR(1)       NOT NULL,
    newborn_flag            CHAR(1)       NOT NULL,
    trauma_flag             CHAR(1)       NOT NULL,
    isolation_flag          CHAR(1)       NOT NULL,
    left_without_being_seen_flag CHAR(1)  NOT NULL,
    confidential_flag       CHAR(1)       NOT NULL,
    source_system_sk        INTEGER       NOT NULL,
    load_batch_id           BIGINT        NOT NULL,
    record_hash             CHAR(64)      NOT NULL,
    recorded_at             TIMESTAMP     NOT NULL,
    prov_activity_iri       VARCHAR(512)  NOT NULL,
    CONSTRAINT pk_dim_encounter_status_junk PRIMARY KEY (encounter_status_junk_sk),
    CONSTRAINT fk_dim_enc_junk_src FOREIGN KEY (source_system_sk)
        REFERENCES edw_g8_reference.dim_source_system (source_system_sk)
);
-- TD: UNIQUE PRIMARY INDEX (encounter_status_junk_sk)
CREATE UNIQUE INDEX ux_dim_encounter_status_junk ON edw_g2_clinical.dim_encounter_status_junk
    (encounter_status_code, hospitalization_flag, readmission_flag, newborn_flag, trauma_flag, isolation_flag,
     left_without_being_seen_flag, confidential_flag);
COMMENT ON TABLE edw_g2_clinical.dim_encounter_status_junk IS 'https://ontology.healix.example.org/clinical/EncounterStatusIndicators';

CREATE TABLE edw_g2_clinical.dim_diagnosis_type (
    diagnosis_type_sk       BIGINT        NOT NULL,
    code_sk                 BIGINT        NOT NULL,
    code                    VARCHAR(100)  NOT NULL,   -- admitting / principal / secondary / chronic / working / billing / discharge
    display                 VARCHAR(500),
    code_system_uri         VARCHAR(255)  NOT NULL,
    value_set_iri           VARCHAR(512)  NOT NULL,
    sort_order              SMALLINT,
    source_system_sk        INTEGER       NOT NULL,
    load_batch_id           BIGINT        NOT NULL,
    record_hash             CHAR(64)      NOT NULL,
    recorded_at             TIMESTAMP     NOT NULL,
    prov_activity_iri       VARCHAR(512)  NOT NULL,
    CONSTRAINT pk_dim_diagnosis_type PRIMARY KEY (diagnosis_type_sk),
    CONSTRAINT fk_dim_dx_type_code FOREIGN KEY (code_sk)
        REFERENCES edw_g8_reference.dim_code (code_sk),
    CONSTRAINT fk_dim_dx_type_src FOREIGN KEY (source_system_sk)
        REFERENCES edw_g8_reference.dim_source_system (source_system_sk)
);
-- TD: UNIQUE PRIMARY INDEX (diagnosis_type_sk)
COMMENT ON TABLE edw_g2_clinical.dim_diagnosis_type IS 'https://ontology.healix.example.org/clinical/DiagnosisType';

CREATE TABLE edw_g2_clinical.dim_clinical_status (
    clinical_status_sk      BIGINT        NOT NULL,
    code_sk                 BIGINT        NOT NULL,
    code                    VARCHAR(100)  NOT NULL,   -- condition-clinical: active / recurrence / relapse / inactive / remission / resolved
    display                 VARCHAR(500),
    code_system_uri         VARCHAR(255)  NOT NULL,
    value_set_iri           VARCHAR(512)  NOT NULL,
    is_active_flag          CHAR(1)       NOT NULL DEFAULT 'Y',
    sort_order              SMALLINT,
    source_system_sk        INTEGER       NOT NULL,
    load_batch_id           BIGINT        NOT NULL,
    record_hash             CHAR(64)      NOT NULL,
    recorded_at             TIMESTAMP     NOT NULL,
    prov_activity_iri       VARCHAR(512)  NOT NULL,
    CONSTRAINT pk_dim_clinical_status PRIMARY KEY (clinical_status_sk),
    CONSTRAINT fk_dim_clin_status_code FOREIGN KEY (code_sk)
        REFERENCES edw_g8_reference.dim_code (code_sk),
    CONSTRAINT fk_dim_clin_status_src FOREIGN KEY (source_system_sk)
        REFERENCES edw_g8_reference.dim_source_system (source_system_sk)
);
-- TD: UNIQUE PRIMARY INDEX (clinical_status_sk)
COMMENT ON TABLE edw_g2_clinical.dim_clinical_status IS 'https://ontology.healix.example.org/clinical/ConditionClinicalStatus';

-- ===========================================================================
-- fact_encounter (accumulating snapshot) · 03-logical-model.md §9.3 · EDW-G2-001
-- ===========================================================================
CREATE TABLE edw_g2_clinical.fact_encounter (
    encounter_sk            BIGINT        NOT NULL,
    encounter_bk            VARCHAR(128)  NOT NULL,   -- <source>:Encounter/<id>
    patient_sk              BIGINT        NOT NULL,
    encounter_class_sk      BIGINT        NOT NULL,
    encounter_type_sk       BIGINT        NOT NULL,
    encounter_status_junk_sk BIGINT       NOT NULL,
    admit_source_sk         BIGINT        NOT NULL,
    discharge_disposition_sk BIGINT       NOT NULL,
    service_provider_org_sk BIGINT        NOT NULL,
    admit_location_sk       BIGINT        NOT NULL,
    discharge_location_sk   BIGINT        NOT NULL,
    attending_practitioner_sk BIGINT      NOT NULL,
    admitting_practitioner_sk BIGINT      NOT NULL,
    referring_practitioner_sk BIGINT      NOT NULL,
    primary_payer_sk        BIGINT        NOT NULL,
    service_line_sk         BIGINT        NOT NULL,
    drg_code_sk             BIGINT        NOT NULL,   -- MS-DRG
    admit_date_sk           INTEGER       NOT NULL,
    admit_time_sk           SMALLINT      NOT NULL,
    admit_ts                TIMESTAMP,
    discharge_date_sk       INTEGER       NOT NULL,
    discharge_time_sk       SMALLINT      NOT NULL,
    discharge_ts            TIMESTAMP,
    ed_arrival_ts           TIMESTAMP,
    ed_departure_ts         TIMESTAMP,
    coding_complete_ts      TIMESTAMP,
    final_bill_ts           TIMESTAMP,
    length_of_stay_days     DECIMAL(9,2),
    length_of_stay_hours    DECIMAL(12,2),
    observation_hours       DECIMAL(9,2),
    encounter_count         SMALLINT      NOT NULL DEFAULT 1,
    inpatient_flag          CHAR(1)       NOT NULL DEFAULT 'N',
    readmission_30d_flag    CHAR(1)       NOT NULL DEFAULT 'U',
    appointment_bk          VARCHAR(128),             -- → G4 fact_appointment
    episode_of_care_bk      VARCHAR(128),             -- → fact_episode_of_care
    account_bk              VARCHAR(128),             -- → G3 fact_account
    source_encounter_id     VARCHAR(64),              -- Cerner ENCNTR_ID / FIN
    visit_number            VARCHAR(64),              -- HL7 PV1-19
    orphan_flag             CHAR(1)       NOT NULL DEFAULT 'N',
    -- bitemporal block
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
    CONSTRAINT pk_fact_encounter PRIMARY KEY (encounter_sk),
    CONSTRAINT fk_enc_patient FOREIGN KEY (patient_sk)
        REFERENCES edw_g1_party.dim_patient (patient_sk),
    CONSTRAINT fk_enc_class FOREIGN KEY (encounter_class_sk)
        REFERENCES edw_g2_clinical.dim_encounter_class (encounter_class_sk),
    CONSTRAINT fk_enc_type FOREIGN KEY (encounter_type_sk)
        REFERENCES edw_g2_clinical.dim_encounter_type (encounter_type_sk),
    CONSTRAINT fk_enc_status_junk FOREIGN KEY (encounter_status_junk_sk)
        REFERENCES edw_g2_clinical.dim_encounter_status_junk (encounter_status_junk_sk),
    CONSTRAINT fk_enc_admit_source FOREIGN KEY (admit_source_sk)
        REFERENCES edw_g2_clinical.dim_admit_source (admit_source_sk),
    CONSTRAINT fk_enc_disch_disp FOREIGN KEY (discharge_disposition_sk)
        REFERENCES edw_g2_clinical.dim_discharge_disposition (discharge_disposition_sk),
    CONSTRAINT fk_enc_service_provider FOREIGN KEY (service_provider_org_sk)
        REFERENCES edw_g1_party.dim_organization (organization_sk),
    CONSTRAINT fk_enc_admit_location FOREIGN KEY (admit_location_sk)
        REFERENCES edw_g1_party.dim_location (location_sk),
    CONSTRAINT fk_enc_discharge_location FOREIGN KEY (discharge_location_sk)
        REFERENCES edw_g1_party.dim_location (location_sk),
    CONSTRAINT fk_enc_attending FOREIGN KEY (attending_practitioner_sk)
        REFERENCES edw_g1_party.dim_practitioner (practitioner_sk),
    CONSTRAINT fk_enc_admitting FOREIGN KEY (admitting_practitioner_sk)
        REFERENCES edw_g1_party.dim_practitioner (practitioner_sk),
    CONSTRAINT fk_enc_referring FOREIGN KEY (referring_practitioner_sk)
        REFERENCES edw_g1_party.dim_practitioner (practitioner_sk),
    CONSTRAINT fk_enc_primary_payer FOREIGN KEY (primary_payer_sk)
        REFERENCES edw_g1_party.dim_payer (payer_sk),
    CONSTRAINT fk_enc_service_line FOREIGN KEY (service_line_sk)
        REFERENCES edw_g8_reference.dim_service_line (service_line_sk),
    CONSTRAINT fk_enc_drg FOREIGN KEY (drg_code_sk)
        REFERENCES edw_g8_reference.dim_code (code_sk),
    CONSTRAINT fk_enc_admit_date FOREIGN KEY (admit_date_sk)
        REFERENCES edw_g8_reference.dim_date (date_sk),
    CONSTRAINT fk_enc_admit_time FOREIGN KEY (admit_time_sk)
        REFERENCES edw_g8_reference.dim_time (time_sk),
    CONSTRAINT fk_enc_discharge_date FOREIGN KEY (discharge_date_sk)
        REFERENCES edw_g8_reference.dim_date (date_sk),
    CONSTRAINT fk_enc_discharge_time FOREIGN KEY (discharge_time_sk)
        REFERENCES edw_g8_reference.dim_time (time_sk),
    CONSTRAINT fk_enc_src FOREIGN KEY (source_system_sk)
        REFERENCES edw_g8_reference.dim_source_system (source_system_sk)
);
-- TD: CREATE MULTISET TABLE edw_g2_clinical.fact_encounter, FALLBACK, BLOCKCOMPRESSION = AUTOTEMP (...) PRIMARY INDEX (patient_sk)
-- TD: PARTITION BY (RANGE_N(CAST(admit_ts AS DATE) BETWEEN DATE '2015-01-01' AND DATE '2040-12-31' EACH INTERVAL '1' MONTH, NO RANGE, UNKNOWN),
-- TD:               CASE_N(superseded_at IS NULL, NO CASE))
-- TD: UNIQUE INDEX (encounter_bk, valid_from)
-- TD: COLLECT STATISTICS COLUMN (patient_sk), COLUMN (encounter_bk), COLUMN (encounter_class_sk), COLUMN (service_provider_org_sk), COLUMN (admit_date_sk), COLUMN (PARTITION) ON edw_g2_clinical.fact_encounter;
CREATE UNIQUE INDEX ux_fact_encounter_bk ON edw_g2_clinical.fact_encounter (encounter_bk, valid_from);
CREATE INDEX ix_fact_encounter_patient ON edw_g2_clinical.fact_encounter (patient_sk, admit_date_sk);
COMMENT ON TABLE edw_g2_clinical.fact_encounter IS 'https://ontology.healix.example.org/clinical/Encounter';
COMMENT ON COLUMN edw_g2_clinical.fact_encounter.encounter_bk IS 'phi:direct | https://ontology.healix.example.org/clinical/encounterIdentifier';
COMMENT ON COLUMN edw_g2_clinical.fact_encounter.patient_sk IS 'internal | https://ontology.healix.example.org/clinical/subject';
COMMENT ON COLUMN edw_g2_clinical.fact_encounter.encounter_class_sk IS 'phi:clinical | https://ontology.healix.example.org/clinical/encounterClass';
COMMENT ON COLUMN edw_g2_clinical.fact_encounter.attending_practitioner_sk IS 'phi:clinical | https://ontology.healix.example.org/clinical/attendingPractitioner';
COMMENT ON COLUMN edw_g2_clinical.fact_encounter.admit_ts IS 'phi:quasi | https://ontology.healix.example.org/clinical/admitDateTime';
COMMENT ON COLUMN edw_g2_clinical.fact_encounter.discharge_ts IS 'phi:quasi | https://ontology.healix.example.org/clinical/dischargeDateTime';
COMMENT ON COLUMN edw_g2_clinical.fact_encounter.length_of_stay_days IS 'phi:clinical | https://ontology.healix.example.org/clinical/lengthOfStayDays';
COMMENT ON COLUMN edw_g2_clinical.fact_encounter.drg_code_sk IS 'phi:quasi | https://ontology.healix.example.org/clinical/diagnosisRelatedGroup';
COMMENT ON COLUMN edw_g2_clinical.fact_encounter.source_encounter_id IS 'phi:direct | https://ontology.healix.example.org/clinical/sourceEncounterIdentifier';
COMMENT ON COLUMN edw_g2_clinical.fact_encounter.valid_from IS 'internal | https://ontology.healix.example.org/time/validFrom';
COMMENT ON COLUMN edw_g2_clinical.fact_encounter.recorded_at IS 'internal | https://ontology.healix.example.org/time/recordedAt';
COMMENT ON COLUMN edw_g2_clinical.fact_encounter.superseded_at IS 'internal | https://ontology.healix.example.org/time/supersededAt';
COMMENT ON COLUMN edw_g2_clinical.fact_encounter.change_kind IS 'internal | https://ontology.healix.example.org/time/changeKind';
COMMENT ON COLUMN edw_g2_clinical.fact_encounter.event_time IS 'internal | https://ontology.healix.example.org/time/eventTime';
COMMENT ON COLUMN edw_g2_clinical.fact_encounter.source_time IS 'internal | https://ontology.healix.example.org/time/sourceTime';
COMMENT ON COLUMN edw_g2_clinical.fact_encounter.decision_time IS 'internal | https://ontology.healix.example.org/time/decisionTime';
COMMENT ON COLUMN edw_g2_clinical.fact_encounter.source_tz IS 'internal | https://ontology.healix.example.org/time/sourceTimeZone';
COMMENT ON COLUMN edw_g2_clinical.fact_encounter.prov_activity_iri IS 'internal | http://www.w3.org/ns/prov#wasGeneratedBy';

-- ===========================================================================
-- fact_diagnosis (transaction): one row per condition assertion (FHIR Condition)
-- ===========================================================================
CREATE TABLE edw_g2_clinical.fact_diagnosis (
    diagnosis_sk            BIGINT        NOT NULL,
    diagnosis_bk            VARCHAR(128)  NOT NULL,   -- <source>:Condition/<id>
    patient_sk              BIGINT        NOT NULL,
    encounter_bk            VARCHAR(128),             -- degenerate; NULL for problem-list conditions
    asserter_practitioner_sk BIGINT       NOT NULL,
    diagnosis_code_sk       BIGINT        NOT NULL,   -- ICD-10-CM or SNOMED CT
    diagnosis_code_src      VARCHAR(64),
    diagnosis_display_src   VARCHAR(255),
    diagnosis_type_sk       BIGINT        NOT NULL,
    clinical_status_sk      BIGINT        NOT NULL,
    verification_status_code_sk BIGINT    NOT NULL,   -- unconfirmed / provisional / differential / confirmed / refuted / entered-in-error
    category_code_sk        BIGINT        NOT NULL,   -- problem-list-item / encounter-diagnosis / health-concern
    severity_code_sk        BIGINT        NOT NULL,
    body_site_code_sk       BIGINT        NOT NULL,
    onset_date_sk           INTEGER       NOT NULL,
    onset_ts                TIMESTAMP,
    abatement_date_sk       INTEGER       NOT NULL,
    abatement_ts            TIMESTAMP,
    asserted_date_sk        INTEGER       NOT NULL,
    asserted_ts             TIMESTAMP,
    poa_code                CHAR(1),                  -- present on admission: Y / N / U / W / 1
    is_chronic_flag         CHAR(1)       NOT NULL DEFAULT 'U',
    sensitive_category_code VARCHAR(30),              -- derived from dim_code.sensitive_category_code (drives the phi:sensitive-category/* tag, module 11 §3)
    source_diagnosis_id     VARCHAR(64),
    orphan_flag             CHAR(1)       NOT NULL DEFAULT 'N',
    -- bitemporal block
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
    CONSTRAINT pk_fact_diagnosis PRIMARY KEY (diagnosis_sk),
    CONSTRAINT fk_dx_patient FOREIGN KEY (patient_sk)
        REFERENCES edw_g1_party.dim_patient (patient_sk),
    CONSTRAINT fk_dx_asserter FOREIGN KEY (asserter_practitioner_sk)
        REFERENCES edw_g1_party.dim_practitioner (practitioner_sk),
    CONSTRAINT fk_dx_code FOREIGN KEY (diagnosis_code_sk)
        REFERENCES edw_g8_reference.dim_code (code_sk),
    CONSTRAINT fk_dx_type FOREIGN KEY (diagnosis_type_sk)
        REFERENCES edw_g2_clinical.dim_diagnosis_type (diagnosis_type_sk),
    CONSTRAINT fk_dx_clinical_status FOREIGN KEY (clinical_status_sk)
        REFERENCES edw_g2_clinical.dim_clinical_status (clinical_status_sk),
    CONSTRAINT fk_dx_verification FOREIGN KEY (verification_status_code_sk)
        REFERENCES edw_g8_reference.dim_code (code_sk),
    CONSTRAINT fk_dx_category FOREIGN KEY (category_code_sk)
        REFERENCES edw_g8_reference.dim_code (code_sk),
    CONSTRAINT fk_dx_severity FOREIGN KEY (severity_code_sk)
        REFERENCES edw_g8_reference.dim_code (code_sk),
    CONSTRAINT fk_dx_body_site FOREIGN KEY (body_site_code_sk)
        REFERENCES edw_g8_reference.dim_code (code_sk),
    CONSTRAINT fk_dx_onset_date FOREIGN KEY (onset_date_sk)
        REFERENCES edw_g8_reference.dim_date (date_sk),
    CONSTRAINT fk_dx_abatement_date FOREIGN KEY (abatement_date_sk)
        REFERENCES edw_g8_reference.dim_date (date_sk),
    CONSTRAINT fk_dx_asserted_date FOREIGN KEY (asserted_date_sk)
        REFERENCES edw_g8_reference.dim_date (date_sk),
    CONSTRAINT fk_dx_src FOREIGN KEY (source_system_sk)
        REFERENCES edw_g8_reference.dim_source_system (source_system_sk)
);
-- TD: CREATE MULTISET TABLE edw_g2_clinical.fact_diagnosis, FALLBACK, BLOCKCOMPRESSION = AUTOTEMP (...) PRIMARY INDEX (patient_sk)
-- TD: PARTITION BY (RANGE_N(asserted_date_sk BETWEEN 20150101 AND 20401231 EACH 100, NO RANGE, UNKNOWN),
-- TD:               CASE_N(superseded_at IS NULL, NO CASE))
-- TD: COLLECT STATISTICS COLUMN (patient_sk), COLUMN (encounter_bk), COLUMN (diagnosis_code_sk), COLUMN (PARTITION) ON edw_g2_clinical.fact_diagnosis;
CREATE UNIQUE INDEX ux_fact_diagnosis_bk ON edw_g2_clinical.fact_diagnosis (diagnosis_bk, valid_from);
CREATE INDEX ix_fact_diagnosis_encounter ON edw_g2_clinical.fact_diagnosis (encounter_bk);
CREATE INDEX ix_fact_diagnosis_code ON edw_g2_clinical.fact_diagnosis (diagnosis_code_sk, asserted_date_sk);
COMMENT ON TABLE edw_g2_clinical.fact_diagnosis IS 'https://ontology.healix.example.org/clinical/Condition';
COMMENT ON COLUMN edw_g2_clinical.fact_diagnosis.diagnosis_code_sk IS 'phi:sensitive-category | https://ontology.healix.example.org/clinical/conditionCode';
COMMENT ON COLUMN edw_g2_clinical.fact_diagnosis.onset_ts IS 'phi:quasi | https://ontology.healix.example.org/clinical/onsetDateTime';
COMMENT ON COLUMN edw_g2_clinical.fact_diagnosis.poa_code IS 'phi:clinical | https://ontology.healix.example.org/clinical/presentOnAdmission';
COMMENT ON COLUMN edw_g2_clinical.fact_diagnosis.sensitive_category_code IS 'phi:clinical | https://ontology.healix.example.org/governance/sensitiveCategory';

-- ===========================================================================
-- bridge_encounter_diagnosis: encounter × diagnosis × rank (EDW-G2-002, EDW-LOG-015)
-- ===========================================================================
CREATE TABLE edw_g2_clinical.bridge_encounter_diagnosis (
    encounter_diagnosis_hk  CHAR(64)      NOT NULL,   -- SHA-256(encounter_bk, diagnosis_bk, valid_from)
    encounter_sk            BIGINT        NOT NULL,
    encounter_bk            VARCHAR(128)  NOT NULL,
    diagnosis_sk            BIGINT        NOT NULL,
    diagnosis_bk            VARCHAR(128)  NOT NULL,
    diagnosis_rank          SMALLINT      NOT NULL,   -- 1 = principal
    is_principal_flag       CHAR(1)       NOT NULL DEFAULT 'N',
    poa_code                CHAR(1),
    diagnosis_role_code     VARCHAR(20)   NOT NULL,   -- admitting / discharge / billing / chief-complaint / comorbidity
    -- bitemporal block
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
    CONSTRAINT pk_bridge_encounter_diagnosis PRIMARY KEY (encounter_diagnosis_hk),
    CONSTRAINT fk_bed_encounter FOREIGN KEY (encounter_sk)
        REFERENCES edw_g2_clinical.fact_encounter (encounter_sk),
    CONSTRAINT fk_bed_diagnosis FOREIGN KEY (diagnosis_sk)
        REFERENCES edw_g2_clinical.fact_diagnosis (diagnosis_sk),
    CONSTRAINT fk_bed_src FOREIGN KEY (source_system_sk)
        REFERENCES edw_g8_reference.dim_source_system (source_system_sk)
);
-- TD: PRIMARY INDEX (encounter_bk)
-- TD: COLLECT STATISTICS COLUMN (encounter_bk), COLUMN (diagnosis_sk) ON edw_g2_clinical.bridge_encounter_diagnosis;
CREATE INDEX ix_bridge_enc_dx_encounter ON edw_g2_clinical.bridge_encounter_diagnosis (encounter_sk, diagnosis_rank);
CREATE INDEX ix_bridge_enc_dx_diagnosis ON edw_g2_clinical.bridge_encounter_diagnosis (diagnosis_sk);
COMMENT ON TABLE edw_g2_clinical.bridge_encounter_diagnosis IS 'https://ontology.healix.example.org/clinical/EncounterDiagnosis';
COMMENT ON COLUMN edw_g2_clinical.bridge_encounter_diagnosis.diagnosis_rank IS 'phi:clinical | https://ontology.healix.example.org/clinical/diagnosisRank';
COMMENT ON COLUMN edw_g2_clinical.bridge_encounter_diagnosis.is_principal_flag IS 'phi:clinical | https://ontology.healix.example.org/clinical/isPrincipalDiagnosis';

-- ===========================================================================
-- edw_g2_diagnostics · value-set dimensions
-- ===========================================================================
CREATE TABLE edw_g2_diagnostics.dim_result_status (
    result_status_sk        BIGINT        NOT NULL,
    code_sk                 BIGINT        NOT NULL,
    code                    VARCHAR(100)  NOT NULL,   -- observation-status: registered / preliminary / final / amended / corrected / cancelled / entered-in-error
    display                 VARCHAR(500),
    code_system_uri         VARCHAR(255)  NOT NULL,
    value_set_iri           VARCHAR(512)  NOT NULL,
    is_final_flag           CHAR(1)       NOT NULL DEFAULT 'N',
    sort_order              SMALLINT,
    source_system_sk        INTEGER       NOT NULL,
    load_batch_id           BIGINT        NOT NULL,
    record_hash             CHAR(64)      NOT NULL,
    recorded_at             TIMESTAMP     NOT NULL,
    prov_activity_iri       VARCHAR(512)  NOT NULL,
    CONSTRAINT pk_dim_result_status PRIMARY KEY (result_status_sk),
    CONSTRAINT fk_dim_result_status_code FOREIGN KEY (code_sk)
        REFERENCES edw_g8_reference.dim_code (code_sk),
    CONSTRAINT fk_dim_result_status_src FOREIGN KEY (source_system_sk)
        REFERENCES edw_g8_reference.dim_source_system (source_system_sk)
);
-- TD: UNIQUE PRIMARY INDEX (result_status_sk)
COMMENT ON TABLE edw_g2_diagnostics.dim_result_status IS 'https://ontology.healix.example.org/clinical/ObservationStatus';

CREATE TABLE edw_g2_diagnostics.dim_abnormal_flag (
    abnormal_flag_sk        BIGINT        NOT NULL,
    code_sk                 BIGINT        NOT NULL,
    code                    VARCHAR(100)  NOT NULL,   -- v3-ObservationInterpretation: N / L / H / LL / HH / A / AA / POS / NEG ...
    display                 VARCHAR(500),
    code_system_uri         VARCHAR(255)  NOT NULL,
    value_set_iri           VARCHAR(512)  NOT NULL,
    is_abnormal_flag        CHAR(1)       NOT NULL DEFAULT 'N',
    is_critical_flag        CHAR(1)       NOT NULL DEFAULT 'N',
    sort_order              SMALLINT,
    source_system_sk        INTEGER       NOT NULL,
    load_batch_id           BIGINT        NOT NULL,
    record_hash             CHAR(64)      NOT NULL,
    recorded_at             TIMESTAMP     NOT NULL,
    prov_activity_iri       VARCHAR(512)  NOT NULL,
    CONSTRAINT pk_dim_abnormal_flag PRIMARY KEY (abnormal_flag_sk),
    CONSTRAINT fk_dim_abnormal_flag_code FOREIGN KEY (code_sk)
        REFERENCES edw_g8_reference.dim_code (code_sk),
    CONSTRAINT fk_dim_abnormal_flag_src FOREIGN KEY (source_system_sk)
        REFERENCES edw_g8_reference.dim_source_system (source_system_sk)
);
-- TD: UNIQUE PRIMARY INDEX (abnormal_flag_sk)
COMMENT ON TABLE edw_g2_diagnostics.dim_abnormal_flag IS 'https://ontology.healix.example.org/clinical/ObservationInterpretation';

-- ===========================================================================
-- fact_observation_result (transaction) · 03-logical-model.md §9.4 · EDW-G2-003
-- Grain: one row per observation component (component_sequence 0 = whole observation)
-- ===========================================================================
CREATE TABLE edw_g2_diagnostics.fact_observation_result (
    observation_result_sk   BIGINT        NOT NULL,
    observation_bk          VARCHAR(128)  NOT NULL,   -- <source>:Observation/<id>
    component_sequence      SMALLINT      NOT NULL DEFAULT 0,
    panel_observation_bk    VARCHAR(128),
    patient_sk              BIGINT        NOT NULL,
    encounter_bk            VARCHAR(128),
    order_bk                VARCHAR(128),
    specimen_bk             VARCHAR(128),
    diagnostic_report_bk    VARCHAR(128),
    performer_practitioner_sk BIGINT      NOT NULL,
    performing_organization_sk BIGINT     NOT NULL,
    location_sk             BIGINT        NOT NULL,
    observation_code_sk     BIGINT        NOT NULL,   -- LOINC
    observation_category_code_sk BIGINT   NOT NULL,   -- vital-signs / laboratory / imaging / social-history / ...
    observation_code_src    VARCHAR(64),
    observation_display_src VARCHAR(255),
    effective_date_sk       INTEGER       NOT NULL,
    effective_time_sk       SMALLINT      NOT NULL,
    effective_ts            TIMESTAMP,
    issued_ts               TIMESTAMP,
    result_status_sk        BIGINT        NOT NULL,
    value_type_code         VARCHAR(20)   NOT NULL,   -- quantity / coded / string / boolean / ratio / range / none
    value_numeric           DECIMAL(18,6),            -- canonical UCUM unit
    ucum_unit_sk            BIGINT        NOT NULL,
    value_numeric_src       DECIMAL(18,6),            -- as received
    unit_src_text           VARCHAR(50),
    comparator_code         VARCHAR(2),               -- < / <= / >= / >
    value_code_sk           BIGINT        NOT NULL,
    value_text              VARCHAR(1000),
    value_boolean_flag      CHAR(1),
    reference_range_low     DECIMAL(18,6),
    reference_range_high    DECIMAL(18,6),
    reference_range_text    VARCHAR(100),
    abnormal_flag_sk        BIGINT        NOT NULL,
    critical_flag           CHAR(1)       NOT NULL DEFAULT 'N',
    body_site_code_sk       BIGINT        NOT NULL,
    method_code_sk          BIGINT        NOT NULL,
    device_bk               VARCHAR(128),
    sensitive_category_code VARCHAR(30),
    source_result_id        VARCHAR(64),
    orphan_flag             CHAR(1)       NOT NULL DEFAULT 'N',
    -- bitemporal block
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
    CONSTRAINT pk_fact_observation_result PRIMARY KEY (observation_result_sk),
    CONSTRAINT fk_obs_patient FOREIGN KEY (patient_sk)
        REFERENCES edw_g1_party.dim_patient (patient_sk),
    CONSTRAINT fk_obs_performer FOREIGN KEY (performer_practitioner_sk)
        REFERENCES edw_g1_party.dim_practitioner (practitioner_sk),
    CONSTRAINT fk_obs_performing_org FOREIGN KEY (performing_organization_sk)
        REFERENCES edw_g1_party.dim_organization (organization_sk),
    CONSTRAINT fk_obs_location FOREIGN KEY (location_sk)
        REFERENCES edw_g1_party.dim_location (location_sk),
    CONSTRAINT fk_obs_code FOREIGN KEY (observation_code_sk)
        REFERENCES edw_g8_reference.dim_code (code_sk),
    CONSTRAINT fk_obs_category FOREIGN KEY (observation_category_code_sk)
        REFERENCES edw_g8_reference.dim_code (code_sk),
    CONSTRAINT fk_obs_effective_date FOREIGN KEY (effective_date_sk)
        REFERENCES edw_g8_reference.dim_date (date_sk),
    CONSTRAINT fk_obs_effective_time FOREIGN KEY (effective_time_sk)
        REFERENCES edw_g8_reference.dim_time (time_sk),
    CONSTRAINT fk_obs_result_status FOREIGN KEY (result_status_sk)
        REFERENCES edw_g2_diagnostics.dim_result_status (result_status_sk),
    CONSTRAINT fk_obs_ucum FOREIGN KEY (ucum_unit_sk)
        REFERENCES edw_g8_reference.dim_ucum_unit (ucum_unit_sk),
    CONSTRAINT fk_obs_value_code FOREIGN KEY (value_code_sk)
        REFERENCES edw_g8_reference.dim_code (code_sk),
    CONSTRAINT fk_obs_abnormal_flag FOREIGN KEY (abnormal_flag_sk)
        REFERENCES edw_g2_diagnostics.dim_abnormal_flag (abnormal_flag_sk),
    CONSTRAINT fk_obs_body_site FOREIGN KEY (body_site_code_sk)
        REFERENCES edw_g8_reference.dim_code (code_sk),
    CONSTRAINT fk_obs_method FOREIGN KEY (method_code_sk)
        REFERENCES edw_g8_reference.dim_code (code_sk),
    CONSTRAINT fk_obs_src FOREIGN KEY (source_system_sk)
        REFERENCES edw_g8_reference.dim_source_system (source_system_sk)
);
-- TD: CREATE MULTISET TABLE edw_g2_diagnostics.fact_observation_result, FALLBACK, BLOCKCOMPRESSION = AUTOTEMP (...) PRIMARY INDEX (patient_sk)
-- TD: PARTITION BY (RANGE_N(CAST(effective_ts AS DATE) BETWEEN DATE '2015-01-01' AND DATE '2040-12-31' EACH INTERVAL '1' MONTH, NO RANGE, UNKNOWN),
-- TD:               CASE_N(superseded_at IS NULL, NO CASE))
-- TD: UNIQUE INDEX (observation_bk, component_sequence, valid_from)
-- TD: COLLECT STATISTICS COLUMN (patient_sk), COLUMN (observation_code_sk), COLUMN (encounter_bk), COLUMN (effective_date_sk), COLUMN (PARTITION) ON edw_g2_diagnostics.fact_observation_result;
CREATE UNIQUE INDEX ux_fact_obs_result_bk ON edw_g2_diagnostics.fact_observation_result (observation_bk, component_sequence, valid_from);
CREATE INDEX ix_fact_obs_result_patient ON edw_g2_diagnostics.fact_observation_result (patient_sk, observation_code_sk, effective_date_sk);
CREATE INDEX ix_fact_obs_result_encounter ON edw_g2_diagnostics.fact_observation_result (encounter_bk);
COMMENT ON TABLE edw_g2_diagnostics.fact_observation_result IS 'https://ontology.healix.example.org/clinical/Observation';
COMMENT ON COLUMN edw_g2_diagnostics.fact_observation_result.observation_bk IS 'phi:direct | https://ontology.healix.example.org/clinical/observationIdentifier';
COMMENT ON COLUMN edw_g2_diagnostics.fact_observation_result.observation_code_sk IS 'phi:clinical | https://ontology.healix.example.org/clinical/observationCode';
COMMENT ON COLUMN edw_g2_diagnostics.fact_observation_result.effective_ts IS 'phi:quasi | https://ontology.healix.example.org/clinical/effectiveDateTime';
COMMENT ON COLUMN edw_g2_diagnostics.fact_observation_result.value_numeric IS 'phi:clinical | https://ontology.healix.example.org/clinical/valueQuantity';
COMMENT ON COLUMN edw_g2_diagnostics.fact_observation_result.ucum_unit_sk IS 'phi:clinical | https://ontology.healix.example.org/clinical/valueUnit';
COMMENT ON COLUMN edw_g2_diagnostics.fact_observation_result.value_numeric_src IS 'phi:clinical | https://ontology.healix.example.org/clinical/sourceValueQuantity';
COMMENT ON COLUMN edw_g2_diagnostics.fact_observation_result.value_code_sk IS 'phi:clinical | https://ontology.healix.example.org/clinical/valueCodeableConcept';
COMMENT ON COLUMN edw_g2_diagnostics.fact_observation_result.value_text IS 'phi:sensitive-category | https://ontology.healix.example.org/clinical/valueString';
COMMENT ON COLUMN edw_g2_diagnostics.fact_observation_result.abnormal_flag_sk IS 'phi:clinical | https://ontology.healix.example.org/clinical/interpretation';
COMMENT ON COLUMN edw_g2_diagnostics.fact_observation_result.prov_activity_iri IS 'internal | http://www.w3.org/ns/prov#wasGeneratedBy';

-- ===========================================================================
-- edw_g2_medication · value-set dimensions
-- ===========================================================================
CREATE TABLE edw_g2_medication.dim_route (
    route_sk                BIGINT        NOT NULL,
    code_sk                 BIGINT        NOT NULL,
    code                    VARCHAR(100)  NOT NULL,   -- SNOMED CT route of administration
    display                 VARCHAR(500),
    code_system_uri         VARCHAR(255)  NOT NULL,
    value_set_iri           VARCHAR(512)  NOT NULL,
    is_parenteral_flag      CHAR(1)       NOT NULL DEFAULT 'N',
    sort_order              SMALLINT,
    source_system_sk        INTEGER       NOT NULL,
    load_batch_id           BIGINT        NOT NULL,
    record_hash             CHAR(64)      NOT NULL,
    recorded_at             TIMESTAMP     NOT NULL,
    prov_activity_iri       VARCHAR(512)  NOT NULL,
    CONSTRAINT pk_dim_route PRIMARY KEY (route_sk),
    CONSTRAINT fk_dim_route_code FOREIGN KEY (code_sk)
        REFERENCES edw_g8_reference.dim_code (code_sk),
    CONSTRAINT fk_dim_route_src FOREIGN KEY (source_system_sk)
        REFERENCES edw_g8_reference.dim_source_system (source_system_sk)
);
-- TD: UNIQUE PRIMARY INDEX (route_sk)
COMMENT ON TABLE edw_g2_medication.dim_route IS 'https://ontology.healix.example.org/clinical/RouteOfAdministration';

CREATE TABLE edw_g2_medication.dim_dose_form (
    dose_form_sk            BIGINT        NOT NULL,
    code_sk                 BIGINT        NOT NULL,
    code                    VARCHAR(100)  NOT NULL,   -- SNOMED CT / RxNorm dose form
    display                 VARCHAR(500),
    code_system_uri         VARCHAR(255)  NOT NULL,
    value_set_iri           VARCHAR(512)  NOT NULL,
    sort_order              SMALLINT,
    source_system_sk        INTEGER       NOT NULL,
    load_batch_id           BIGINT        NOT NULL,
    record_hash             CHAR(64)      NOT NULL,
    recorded_at             TIMESTAMP     NOT NULL,
    prov_activity_iri       VARCHAR(512)  NOT NULL,
    CONSTRAINT pk_dim_dose_form PRIMARY KEY (dose_form_sk),
    CONSTRAINT fk_dim_dose_form_code FOREIGN KEY (code_sk)
        REFERENCES edw_g8_reference.dim_code (code_sk),
    CONSTRAINT fk_dim_dose_form_src FOREIGN KEY (source_system_sk)
        REFERENCES edw_g8_reference.dim_source_system (source_system_sk)
);
-- TD: UNIQUE PRIMARY INDEX (dose_form_sk)
COMMENT ON TABLE edw_g2_medication.dim_dose_form IS 'https://ontology.healix.example.org/clinical/DoseForm';

CREATE TABLE edw_g2_medication.dim_med_order_status (
    med_order_status_sk     BIGINT        NOT NULL,
    code_sk                 BIGINT        NOT NULL,
    code                    VARCHAR(100)  NOT NULL,   -- medicationrequest-status: active / on-hold / cancelled / completed / entered-in-error / stopped / draft / unknown
    display                 VARCHAR(500),
    code_system_uri         VARCHAR(255)  NOT NULL,
    value_set_iri           VARCHAR(512)  NOT NULL,
    is_active_flag          CHAR(1)       NOT NULL DEFAULT 'N',
    sort_order              SMALLINT,
    source_system_sk        INTEGER       NOT NULL,
    load_batch_id           BIGINT        NOT NULL,
    record_hash             CHAR(64)      NOT NULL,
    recorded_at             TIMESTAMP     NOT NULL,
    prov_activity_iri       VARCHAR(512)  NOT NULL,
    CONSTRAINT pk_dim_med_order_status PRIMARY KEY (med_order_status_sk),
    CONSTRAINT fk_dim_med_status_code FOREIGN KEY (code_sk)
        REFERENCES edw_g8_reference.dim_code (code_sk),
    CONSTRAINT fk_dim_med_status_src FOREIGN KEY (source_system_sk)
        REFERENCES edw_g8_reference.dim_source_system (source_system_sk)
);
-- TD: UNIQUE PRIMARY INDEX (med_order_status_sk)
COMMENT ON TABLE edw_g2_medication.dim_med_order_status IS 'https://ontology.healix.example.org/clinical/MedicationRequestStatus';

-- ===========================================================================
-- fact_medication_order (accumulating snapshot): one row per MedicationRequest
-- ===========================================================================
CREATE TABLE edw_g2_medication.fact_medication_order (
    medication_order_sk     BIGINT        NOT NULL,
    medication_order_bk     VARCHAR(128)  NOT NULL,   -- <source>:MedicationRequest/<id>
    patient_sk              BIGINT        NOT NULL,
    encounter_bk            VARCHAR(128),
    prescriber_practitioner_sk BIGINT     NOT NULL,
    location_sk             BIGINT        NOT NULL,
    item_sk                 BIGINT        NOT NULL,   -- drug product (conformed dim_item, G4)
    medication_code_sk      BIGINT        NOT NULL,   -- RxNorm
    medication_code_src     VARCHAR(64),
    medication_display_src  VARCHAR(255),
    route_sk                BIGINT        NOT NULL,
    dose_form_sk            BIGINT        NOT NULL,
    med_order_status_sk     BIGINT        NOT NULL,
    intent_code_sk          BIGINT        NOT NULL,   -- proposal / plan / order / original-order / reflex-order / filler-order / instance-order
    priority_code_sk        BIGINT        NOT NULL,   -- routine / urgent / asap / stat
    category_code_sk        BIGINT        NOT NULL,   -- inpatient / outpatient / community / discharge
    authored_date_sk        INTEGER       NOT NULL,
    authored_time_sk        SMALLINT      NOT NULL,
    authored_ts             TIMESTAMP,
    start_ts                TIMESTAMP,
    end_ts                  TIMESTAMP,
    first_dispense_ts       TIMESTAMP,
    first_administration_ts TIMESTAMP,
    discontinued_ts         TIMESTAMP,
    dose_quantity           DECIMAL(18,6),
    dose_ucum_unit_sk       BIGINT        NOT NULL,
    dose_frequency_text     VARCHAR(100),             -- timing code text (e.g. "BID"); structured timing in int layer
    dose_frequency_per_day  DECIMAL(6,2),
    dispense_quantity       DECIMAL(18,6),
    dispense_ucum_unit_sk   BIGINT        NOT NULL,
    refills_allowed         SMALLINT,
    days_supply             SMALLINT,
    is_prn_flag             CHAR(1)       NOT NULL DEFAULT 'N',
    is_controlled_substance_flag CHAR(1)  NOT NULL DEFAULT 'N',
    dea_schedule_code       VARCHAR(5),
    sensitive_category_code VARCHAR(30),
    source_order_id         VARCHAR(64),
    orphan_flag             CHAR(1)       NOT NULL DEFAULT 'N',
    -- bitemporal block
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
    CONSTRAINT pk_fact_medication_order PRIMARY KEY (medication_order_sk),
    CONSTRAINT fk_med_order_patient FOREIGN KEY (patient_sk)
        REFERENCES edw_g1_party.dim_patient (patient_sk),
    CONSTRAINT fk_med_order_prescriber FOREIGN KEY (prescriber_practitioner_sk)
        REFERENCES edw_g1_party.dim_practitioner (practitioner_sk),
    CONSTRAINT fk_med_order_location FOREIGN KEY (location_sk)
        REFERENCES edw_g1_party.dim_location (location_sk),
    CONSTRAINT fk_med_order_item FOREIGN KEY (item_sk)
        REFERENCES edw_g4_supply.dim_item (item_sk),
    CONSTRAINT fk_med_order_code FOREIGN KEY (medication_code_sk)
        REFERENCES edw_g8_reference.dim_code (code_sk),
    CONSTRAINT fk_med_order_route FOREIGN KEY (route_sk)
        REFERENCES edw_g2_medication.dim_route (route_sk),
    CONSTRAINT fk_med_order_dose_form FOREIGN KEY (dose_form_sk)
        REFERENCES edw_g2_medication.dim_dose_form (dose_form_sk),
    CONSTRAINT fk_med_order_status FOREIGN KEY (med_order_status_sk)
        REFERENCES edw_g2_medication.dim_med_order_status (med_order_status_sk),
    CONSTRAINT fk_med_order_intent FOREIGN KEY (intent_code_sk)
        REFERENCES edw_g8_reference.dim_code (code_sk),
    CONSTRAINT fk_med_order_priority FOREIGN KEY (priority_code_sk)
        REFERENCES edw_g8_reference.dim_code (code_sk),
    CONSTRAINT fk_med_order_category FOREIGN KEY (category_code_sk)
        REFERENCES edw_g8_reference.dim_code (code_sk),
    CONSTRAINT fk_med_order_authored_date FOREIGN KEY (authored_date_sk)
        REFERENCES edw_g8_reference.dim_date (date_sk),
    CONSTRAINT fk_med_order_authored_time FOREIGN KEY (authored_time_sk)
        REFERENCES edw_g8_reference.dim_time (time_sk),
    CONSTRAINT fk_med_order_dose_ucum FOREIGN KEY (dose_ucum_unit_sk)
        REFERENCES edw_g8_reference.dim_ucum_unit (ucum_unit_sk),
    CONSTRAINT fk_med_order_dispense_ucum FOREIGN KEY (dispense_ucum_unit_sk)
        REFERENCES edw_g8_reference.dim_ucum_unit (ucum_unit_sk),
    CONSTRAINT fk_med_order_src FOREIGN KEY (source_system_sk)
        REFERENCES edw_g8_reference.dim_source_system (source_system_sk)
);
-- TD: CREATE MULTISET TABLE edw_g2_medication.fact_medication_order, FALLBACK, BLOCKCOMPRESSION = AUTOTEMP (...) PRIMARY INDEX (patient_sk)
-- TD: PARTITION BY (RANGE_N(CAST(authored_ts AS DATE) BETWEEN DATE '2015-01-01' AND DATE '2040-12-31' EACH INTERVAL '1' MONTH, NO RANGE, UNKNOWN),
-- TD:               CASE_N(superseded_at IS NULL, NO CASE))
-- TD: COLLECT STATISTICS COLUMN (patient_sk), COLUMN (medication_code_sk), COLUMN (item_sk), COLUMN (encounter_bk), COLUMN (PARTITION) ON edw_g2_medication.fact_medication_order;
CREATE UNIQUE INDEX ux_fact_medication_order_bk ON edw_g2_medication.fact_medication_order (medication_order_bk, valid_from);
CREATE INDEX ix_fact_medication_order_enc ON edw_g2_medication.fact_medication_order (encounter_bk);
CREATE INDEX ix_fact_medication_order_item ON edw_g2_medication.fact_medication_order (item_sk, authored_date_sk);
COMMENT ON TABLE edw_g2_medication.fact_medication_order IS 'https://ontology.healix.example.org/clinical/MedicationRequest';
COMMENT ON COLUMN edw_g2_medication.fact_medication_order.medication_code_sk IS 'phi:sensitive-category | https://ontology.healix.example.org/clinical/medicationCode';
COMMENT ON COLUMN edw_g2_medication.fact_medication_order.item_sk IS 'phi:clinical | https://ontology.healix.example.org/clinical/medicationProduct';
COMMENT ON COLUMN edw_g2_medication.fact_medication_order.authored_ts IS 'phi:quasi | https://ontology.healix.example.org/clinical/authoredOn';
COMMENT ON COLUMN edw_g2_medication.fact_medication_order.dose_quantity IS 'phi:clinical | https://ontology.healix.example.org/clinical/doseQuantity';
COMMENT ON COLUMN edw_g2_medication.fact_medication_order.prov_activity_iri IS 'internal | http://www.w3.org/ns/prov#wasGeneratedBy';
