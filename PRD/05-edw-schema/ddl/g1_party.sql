-- =============================================================================
-- HEALIX EDW · module 05 · g1_party.sql
-- G1 Party & Identity · schema edw_g1_party
-- Tables: dim_party, dim_organization, dim_location, dim_practitioner, dim_payer,
--         dim_patient, hist_dim_patient, bridge_patient_link
-- Specs:  01-subject-area-groups.md §G1 · 03-logical-model.md §9.1–9.2 ·
--         05-conformed-dimensions.md §1–§4, §6 · 06-history-and-bitemporality.md
-- Depends on: g8_reference.sql (dim_source_system, dim_code, dim_geography)
-- hist_* variants for dim_organization, dim_location, dim_practitioner,
-- dim_payer and dim_party are generated with the identical pattern shown for
-- hist_dim_patient (EDW-HIST-001) and omitted here for brevity.
-- Dialect: portable ANSI; Teradata hints as "-- TD:" comments.
-- Column comment format: '<phi_class> | <ontology_property_iri>'; phi_class values are the module-11 taxonomy (phi:direct, phi:quasi, phi:clinical, phi:sensitive-category[/...], pii:workforce, confidential:*, internal, public, deidentified:*, pseudonymised)
-- =============================================================================

-- ---------------------------------------------------------------------------
-- dim_party (Type 2): EMPI golden record for persons and organisations;
-- conformed dimensions are role projections of this table (EDW-02 §2)
-- ---------------------------------------------------------------------------
CREATE TABLE edw_g1_party.dim_party (
    party_sk                BIGINT        NOT NULL,
    party_bk                VARCHAR(128)  NOT NULL,   -- EMPI golden identifier (persons) / resolved org id
    party_type_code         VARCHAR(12)   NOT NULL,   -- person / organization
    empi_golden_iri         VARCHAR(512),             -- module-08 resolved IRI
    display_name            VARCHAR(255),
    party_status_code       VARCHAR(12)   NOT NULL,   -- active / inactive / merged
    merged_into_party_sk    BIGINT,
    is_inferred_flag        CHAR(1)       NOT NULL DEFAULT 'N',
    valid_from              TIMESTAMP     NOT NULL,
    valid_to                TIMESTAMP,
    is_current_flag         CHAR(1)       NOT NULL DEFAULT 'Y',
    source_system_sk        INTEGER       NOT NULL,
    load_batch_id           BIGINT        NOT NULL,
    record_hash             CHAR(64)      NOT NULL,
    recorded_at             TIMESTAMP     NOT NULL,
    prov_activity_iri       VARCHAR(512)  NOT NULL,
    CONSTRAINT pk_dim_party PRIMARY KEY (party_sk),
    CONSTRAINT fk_dim_party_merged FOREIGN KEY (merged_into_party_sk)
        REFERENCES edw_g1_party.dim_party (party_sk),
    CONSTRAINT fk_dim_party_src FOREIGN KEY (source_system_sk)
        REFERENCES edw_g8_reference.dim_source_system (source_system_sk)
);
-- TD: CREATE SET TABLE edw_g1_party.dim_party, FALLBACK (...) UNIQUE PRIMARY INDEX (party_sk)
CREATE UNIQUE INDEX ux_dim_party_bk ON edw_g1_party.dim_party (party_bk, valid_from);
COMMENT ON TABLE edw_g1_party.dim_party IS 'https://ontology.healix.example.org/party/Party';
COMMENT ON COLUMN edw_g1_party.dim_party.party_bk IS 'phi:direct | https://ontology.healix.example.org/party/empiGoldenIdentifier';
COMMENT ON COLUMN edw_g1_party.dim_party.display_name IS 'phi:direct | https://ontology.healix.example.org/party/displayName';

-- ---------------------------------------------------------------------------
-- dim_organization (conformed, Type 2) · 05-conformed-dimensions.md §3
-- ---------------------------------------------------------------------------
CREATE TABLE edw_g1_party.dim_organization (
    organization_sk         BIGINT        NOT NULL,
    organization_bk         VARCHAR(128)  NOT NULL,
    party_sk                BIGINT        NOT NULL,
    organization_name       VARCHAR(255)  NOT NULL,
    organization_alias      VARCHAR(255),
    organization_type_code_sk BIGINT      NOT NULL,
    npi_type2               CHAR(10),
    ein_hash                CHAR(64),
    tax_id_type_code        VARCHAR(10),
    parent_organization_sk  BIGINT,
    hierarchy_level         SMALLINT,
    level1_name             VARCHAR(255),             -- health system
    level2_name             VARCHAR(255),             -- region
    level3_name             VARCHAR(255),             -- facility
    level4_name             VARCHAR(255),             -- division
    level5_name             VARCHAR(255),             -- department
    fusion_legal_entity_id  VARCHAR(30),
    fusion_business_unit_id VARCHAR(30),
    cerner_organization_id  VARCHAR(30),
    address_line1           VARCHAR(200),
    address_city            VARCHAR(100),
    address_state_code      CHAR(2),
    address_postal_code     VARCHAR(10),
    address_country_code    CHAR(2),
    geography_sk            BIGINT        NOT NULL,
    active_flag             CHAR(1)       NOT NULL DEFAULT 'Y',
    is_internal_flag        CHAR(1)       NOT NULL DEFAULT 'N',
    is_inferred_flag        CHAR(1)       NOT NULL DEFAULT 'N',
    valid_from              TIMESTAMP     NOT NULL,
    valid_to                TIMESTAMP,
    is_current_flag         CHAR(1)       NOT NULL DEFAULT 'Y',
    source_system_sk        INTEGER       NOT NULL,
    load_batch_id           BIGINT        NOT NULL,
    record_hash             CHAR(64)      NOT NULL,
    recorded_at             TIMESTAMP     NOT NULL,
    prov_activity_iri       VARCHAR(512)  NOT NULL,
    CONSTRAINT pk_dim_organization PRIMARY KEY (organization_sk),
    CONSTRAINT fk_dim_org_party FOREIGN KEY (party_sk)
        REFERENCES edw_g1_party.dim_party (party_sk),
    CONSTRAINT fk_dim_org_type FOREIGN KEY (organization_type_code_sk)
        REFERENCES edw_g8_reference.dim_code (code_sk),
    CONSTRAINT fk_dim_org_parent FOREIGN KEY (parent_organization_sk)
        REFERENCES edw_g1_party.dim_organization (organization_sk),
    CONSTRAINT fk_dim_org_geography FOREIGN KEY (geography_sk)
        REFERENCES edw_g8_reference.dim_geography (geography_sk),
    CONSTRAINT fk_dim_org_src FOREIGN KEY (source_system_sk)
        REFERENCES edw_g8_reference.dim_source_system (source_system_sk)
);
-- TD: UNIQUE PRIMARY INDEX (organization_sk); INDEX (organization_bk)
CREATE UNIQUE INDEX ux_dim_organization_bk ON edw_g1_party.dim_organization (organization_bk, valid_from);
COMMENT ON TABLE edw_g1_party.dim_organization IS 'https://ontology.healix.example.org/party/Organization';
COMMENT ON COLUMN edw_g1_party.dim_organization.organization_name IS 'public | https://ontology.healix.example.org/party/organizationName';
COMMENT ON COLUMN edw_g1_party.dim_organization.parent_organization_sk IS 'internal | https://ontology.healix.example.org/party/partOf';
COMMENT ON COLUMN edw_g1_party.dim_organization.npi_type2 IS 'public | https://ontology.healix.example.org/party/npi';

-- ---------------------------------------------------------------------------
-- dim_location (conformed, Type 2) · 05-conformed-dimensions.md §4
-- ---------------------------------------------------------------------------
CREATE TABLE edw_g1_party.dim_location (
    location_sk             BIGINT        NOT NULL,
    location_bk             VARCHAR(128)  NOT NULL,
    location_name           VARCHAR(255)  NOT NULL,
    location_type_code_sk   BIGINT        NOT NULL,   -- FHIR location-physical-type + virtual/warehouse/locator
    managing_organization_sk BIGINT       NOT NULL,
    parent_location_sk      BIGINT,
    hierarchy_level         SMALLINT,
    campus_name             VARCHAR(100),
    building_name           VARCHAR(100),
    floor_name              VARCHAR(50),
    unit_name               VARCHAR(100),
    room_name               VARCHAR(50),
    bed_name                VARCHAR(50),
    cerner_location_cd      VARCHAR(30),
    cerner_nurse_unit_cd    VARCHAR(30),
    fusion_inventory_org_id VARCHAR(30),
    fusion_subinventory_code VARCHAR(30),
    address_line1           VARCHAR(200),
    address_city            VARCHAR(100),
    address_state_code      CHAR(2),
    address_postal_code     VARCHAR(10),
    address_country_code    CHAR(2),
    geography_sk            BIGINT        NOT NULL,
    latitude                DECIMAL(9,6),
    longitude               DECIMAL(9,6),
    place_of_service_code_sk BIGINT       NOT NULL,   -- CMS POS
    telehealth_flag         CHAR(1)       NOT NULL DEFAULT 'N',
    active_flag             CHAR(1)       NOT NULL DEFAULT 'Y',
    is_inferred_flag        CHAR(1)       NOT NULL DEFAULT 'N',
    valid_from              TIMESTAMP     NOT NULL,
    valid_to                TIMESTAMP,
    is_current_flag         CHAR(1)       NOT NULL DEFAULT 'Y',
    source_system_sk        INTEGER       NOT NULL,
    load_batch_id           BIGINT        NOT NULL,
    record_hash             CHAR(64)      NOT NULL,
    recorded_at             TIMESTAMP     NOT NULL,
    prov_activity_iri       VARCHAR(512)  NOT NULL,
    CONSTRAINT pk_dim_location PRIMARY KEY (location_sk),
    CONSTRAINT fk_dim_loc_type FOREIGN KEY (location_type_code_sk)
        REFERENCES edw_g8_reference.dim_code (code_sk),
    CONSTRAINT fk_dim_loc_org FOREIGN KEY (managing_organization_sk)
        REFERENCES edw_g1_party.dim_organization (organization_sk),
    CONSTRAINT fk_dim_loc_parent FOREIGN KEY (parent_location_sk)
        REFERENCES edw_g1_party.dim_location (location_sk),
    CONSTRAINT fk_dim_loc_geography FOREIGN KEY (geography_sk)
        REFERENCES edw_g8_reference.dim_geography (geography_sk),
    CONSTRAINT fk_dim_loc_pos FOREIGN KEY (place_of_service_code_sk)
        REFERENCES edw_g8_reference.dim_code (code_sk),
    CONSTRAINT fk_dim_loc_src FOREIGN KEY (source_system_sk)
        REFERENCES edw_g8_reference.dim_source_system (source_system_sk)
);
-- TD: UNIQUE PRIMARY INDEX (location_sk); INDEX (location_bk)
CREATE UNIQUE INDEX ux_dim_location_bk ON edw_g1_party.dim_location (location_bk, valid_from);
COMMENT ON TABLE edw_g1_party.dim_location IS 'https://ontology.healix.example.org/party/Location';
COMMENT ON COLUMN edw_g1_party.dim_location.parent_location_sk IS 'internal | https://ontology.healix.example.org/party/locationPartOf';
COMMENT ON COLUMN edw_g1_party.dim_location.unit_name IS 'internal | https://ontology.healix.example.org/party/nursingUnitName';

-- ---------------------------------------------------------------------------
-- dim_practitioner (conformed, Type 2) · 03-logical-model.md §9.2
-- ---------------------------------------------------------------------------
CREATE TABLE edw_g1_party.dim_practitioner (
    practitioner_sk         BIGINT        NOT NULL,
    practitioner_bk         VARCHAR(128)  NOT NULL,
    party_sk                BIGINT        NOT NULL,
    npi                     CHAR(10),
    dea_number_hash         CHAR(64),
    state_license_number    VARCHAR(30),
    state_license_state_code CHAR(2),
    name_family             VARCHAR(100),
    name_given              VARCHAR(100),
    name_prefix             VARCHAR(20),
    name_suffix             VARCHAR(20),
    credential_text         VARCHAR(100),
    gender_code_sk          BIGINT        NOT NULL,
    practitioner_type_code_sk BIGINT      NOT NULL,
    primary_specialty_code_sk BIGINT      NOT NULL,   -- NUCC taxonomy
    primary_organization_sk BIGINT        NOT NULL,
    employed_flag           CHAR(1)       NOT NULL DEFAULT 'U',
    active_flag             CHAR(1)       NOT NULL DEFAULT 'Y',
    email_work              VARCHAR(255),
    phone_work              VARCHAR(30),
    is_inferred_flag        CHAR(1)       NOT NULL DEFAULT 'N',
    valid_from              TIMESTAMP     NOT NULL,
    valid_to                TIMESTAMP,
    is_current_flag         CHAR(1)       NOT NULL DEFAULT 'Y',
    source_system_sk        INTEGER       NOT NULL,
    load_batch_id           BIGINT        NOT NULL,
    record_hash             CHAR(64)      NOT NULL,
    recorded_at             TIMESTAMP     NOT NULL,
    prov_activity_iri       VARCHAR(512)  NOT NULL,
    CONSTRAINT pk_dim_practitioner PRIMARY KEY (practitioner_sk),
    CONSTRAINT fk_dim_prac_party FOREIGN KEY (party_sk)
        REFERENCES edw_g1_party.dim_party (party_sk),
    CONSTRAINT fk_dim_prac_gender FOREIGN KEY (gender_code_sk)
        REFERENCES edw_g8_reference.dim_code (code_sk),
    CONSTRAINT fk_dim_prac_type FOREIGN KEY (practitioner_type_code_sk)
        REFERENCES edw_g8_reference.dim_code (code_sk),
    CONSTRAINT fk_dim_prac_specialty FOREIGN KEY (primary_specialty_code_sk)
        REFERENCES edw_g8_reference.dim_code (code_sk),
    CONSTRAINT fk_dim_prac_org FOREIGN KEY (primary_organization_sk)
        REFERENCES edw_g1_party.dim_organization (organization_sk),
    CONSTRAINT fk_dim_prac_src FOREIGN KEY (source_system_sk)
        REFERENCES edw_g8_reference.dim_source_system (source_system_sk)
);
-- TD: CREATE SET TABLE edw_g1_party.dim_practitioner, FALLBACK (...) UNIQUE PRIMARY INDEX (practitioner_sk)
-- TD: INDEX (npi); INDEX (practitioner_bk)
-- TD: COLLECT STATISTICS COLUMN (practitioner_sk), COLUMN (practitioner_bk), COLUMN (npi), COLUMN (is_current_flag) ON edw_g1_party.dim_practitioner;
CREATE UNIQUE INDEX ux_dim_practitioner_bk ON edw_g1_party.dim_practitioner (practitioner_bk, valid_from);
CREATE INDEX ix_dim_practitioner_npi ON edw_g1_party.dim_practitioner (npi);
COMMENT ON TABLE edw_g1_party.dim_practitioner IS 'https://ontology.healix.example.org/party/Practitioner';
COMMENT ON COLUMN edw_g1_party.dim_practitioner.practitioner_bk IS 'internal | https://ontology.healix.example.org/party/practitionerIdentifier';
COMMENT ON COLUMN edw_g1_party.dim_practitioner.npi IS 'public | https://ontology.healix.example.org/party/npi';
COMMENT ON COLUMN edw_g1_party.dim_practitioner.name_family IS 'internal | https://ontology.healix.example.org/party/familyName';
COMMENT ON COLUMN edw_g1_party.dim_practitioner.primary_specialty_code_sk IS 'internal | https://ontology.healix.example.org/party/primarySpecialty';
COMMENT ON COLUMN edw_g1_party.dim_practitioner.primary_organization_sk IS 'internal | https://ontology.healix.example.org/party/primaryOrganization';

-- ---------------------------------------------------------------------------
-- dim_payer (conformed, Type 2) · 05-conformed-dimensions.md §6
-- ---------------------------------------------------------------------------
CREATE TABLE edw_g1_party.dim_payer (
    payer_sk                BIGINT        NOT NULL,
    payer_bk                VARCHAR(128)  NOT NULL,
    organization_sk         BIGINT        NOT NULL,   -- payer is an organisation in the payer role
    payer_name              VARCHAR(255)  NOT NULL,
    payer_type_code_sk      BIGINT        NOT NULL,   -- commercial / medicare / medicare-advantage / medicaid / ...
    naic_code               VARCHAR(10),
    cms_contract_number     VARCHAR(10),
    clearinghouse_payer_id  VARCHAR(30),
    cerner_health_plan_id   VARCHAR(30),
    fusion_customer_id      VARCHAR(30),
    financial_class_code_sk BIGINT        NOT NULL,
    parent_payer_sk         BIGINT,
    active_flag             CHAR(1)       NOT NULL DEFAULT 'Y',
    is_inferred_flag        CHAR(1)       NOT NULL DEFAULT 'N',
    valid_from              TIMESTAMP     NOT NULL,
    valid_to                TIMESTAMP,
    is_current_flag         CHAR(1)       NOT NULL DEFAULT 'Y',
    source_system_sk        INTEGER       NOT NULL,
    load_batch_id           BIGINT        NOT NULL,
    record_hash             CHAR(64)      NOT NULL,
    recorded_at             TIMESTAMP     NOT NULL,
    prov_activity_iri       VARCHAR(512)  NOT NULL,
    CONSTRAINT pk_dim_payer PRIMARY KEY (payer_sk),
    CONSTRAINT fk_dim_payer_org FOREIGN KEY (organization_sk)
        REFERENCES edw_g1_party.dim_organization (organization_sk),
    CONSTRAINT fk_dim_payer_type FOREIGN KEY (payer_type_code_sk)
        REFERENCES edw_g8_reference.dim_code (code_sk),
    CONSTRAINT fk_dim_payer_fin_class FOREIGN KEY (financial_class_code_sk)
        REFERENCES edw_g8_reference.dim_code (code_sk),
    CONSTRAINT fk_dim_payer_parent FOREIGN KEY (parent_payer_sk)
        REFERENCES edw_g1_party.dim_payer (payer_sk),
    CONSTRAINT fk_dim_payer_src FOREIGN KEY (source_system_sk)
        REFERENCES edw_g8_reference.dim_source_system (source_system_sk)
);
-- TD: UNIQUE PRIMARY INDEX (payer_sk)
CREATE UNIQUE INDEX ux_dim_payer_bk ON edw_g1_party.dim_payer (payer_bk, valid_from);
COMMENT ON TABLE edw_g1_party.dim_payer IS 'https://ontology.healix.example.org/party/Payer';
COMMENT ON COLUMN edw_g1_party.dim_payer.payer_type_code_sk IS 'internal | https://ontology.healix.example.org/finance/payerType';

-- ---------------------------------------------------------------------------
-- dim_patient (conformed, Type 2) · 03-logical-model.md §9.1 · EDW-G1-001
-- Current-belief projection of hist_dim_patient (06-history-and-bitemporality.md §2)
-- ---------------------------------------------------------------------------
CREATE TABLE edw_g1_party.dim_patient (
    patient_sk              BIGINT        NOT NULL,
    patient_bk              VARCHAR(128)  NOT NULL,   -- EMPI golden identifier
    party_sk                BIGINT        NOT NULL,
    empi_golden_iri         VARCHAR(512)  NOT NULL,   -- module-08 resolved person IRI
    mrn_primary             VARCHAR(64),
    mrn_primary_system      VARCHAR(255),
    name_family             VARCHAR(100),
    name_given              VARCHAR(100),
    name_middle             VARCHAR(100),
    name_prefix             VARCHAR(20),
    name_suffix             VARCHAR(20),
    birth_date              DATE,
    birth_date_estimated_flag CHAR(1)     NOT NULL DEFAULT 'N',
    deceased_flag           CHAR(1)       NOT NULL DEFAULT 'N',
    deceased_ts             TIMESTAMP,
    administrative_gender_code_sk BIGINT  NOT NULL,
    birth_sex_code_sk       BIGINT        NOT NULL,
    gender_identity_code_sk BIGINT        NOT NULL,
    race_primary_code_sk    BIGINT        NOT NULL,   -- multi-valued race in bridge_patient_race
    ethnicity_code_sk       BIGINT        NOT NULL,
    preferred_language_code_sk BIGINT     NOT NULL,
    marital_status_code_sk  BIGINT        NOT NULL,
    religion_code_sk        BIGINT        NOT NULL,
    interpreter_required_flag CHAR(1)     NOT NULL DEFAULT 'U',
    multiple_birth_flag     CHAR(1)       NOT NULL DEFAULT 'U',
    address_line1           VARCHAR(200),
    address_line2           VARCHAR(200),
    address_city            VARCHAR(100),
    address_state_code      CHAR(2),
    address_postal_code     VARCHAR(10),
    address_country_code    CHAR(2),
    geography_sk            BIGINT        NOT NULL,
    phone_home              VARCHAR(30),
    phone_mobile            VARCHAR(30),
    email                   VARCHAR(255),
    ssn_hash                CHAR(64),
    managing_organization_sk BIGINT       NOT NULL,
    primary_care_practitioner_sk BIGINT   NOT NULL,
    patient_status_code     VARCHAR(20)   NOT NULL DEFAULT 'active',   -- active / inactive / merged / deceased
    merged_into_patient_sk  BIGINT,
    test_patient_flag       CHAR(1)       NOT NULL DEFAULT 'N',
    is_inferred_flag        CHAR(1)       NOT NULL DEFAULT 'N',
    -- valid-time block
    valid_from              TIMESTAMP     NOT NULL,
    valid_to                TIMESTAMP,
    is_current_flag         CHAR(1)       NOT NULL DEFAULT 'Y',
    -- technical block
    source_system_sk        INTEGER       NOT NULL,
    load_batch_id           BIGINT        NOT NULL,
    record_hash             CHAR(64)      NOT NULL,
    recorded_at             TIMESTAMP     NOT NULL,
    prov_activity_iri       VARCHAR(512)  NOT NULL,
    CONSTRAINT pk_dim_patient PRIMARY KEY (patient_sk),
    CONSTRAINT fk_dim_pat_party FOREIGN KEY (party_sk)
        REFERENCES edw_g1_party.dim_party (party_sk),
    CONSTRAINT fk_dim_pat_admin_gender FOREIGN KEY (administrative_gender_code_sk)
        REFERENCES edw_g8_reference.dim_code (code_sk),
    CONSTRAINT fk_dim_pat_birth_sex FOREIGN KEY (birth_sex_code_sk)
        REFERENCES edw_g8_reference.dim_code (code_sk),
    CONSTRAINT fk_dim_pat_gender_identity FOREIGN KEY (gender_identity_code_sk)
        REFERENCES edw_g8_reference.dim_code (code_sk),
    CONSTRAINT fk_dim_pat_race FOREIGN KEY (race_primary_code_sk)
        REFERENCES edw_g8_reference.dim_code (code_sk),
    CONSTRAINT fk_dim_pat_ethnicity FOREIGN KEY (ethnicity_code_sk)
        REFERENCES edw_g8_reference.dim_code (code_sk),
    CONSTRAINT fk_dim_pat_language FOREIGN KEY (preferred_language_code_sk)
        REFERENCES edw_g8_reference.dim_code (code_sk),
    CONSTRAINT fk_dim_pat_marital FOREIGN KEY (marital_status_code_sk)
        REFERENCES edw_g8_reference.dim_code (code_sk),
    CONSTRAINT fk_dim_pat_religion FOREIGN KEY (religion_code_sk)
        REFERENCES edw_g8_reference.dim_code (code_sk),
    CONSTRAINT fk_dim_pat_geography FOREIGN KEY (geography_sk)
        REFERENCES edw_g8_reference.dim_geography (geography_sk),
    CONSTRAINT fk_dim_pat_managing_org FOREIGN KEY (managing_organization_sk)
        REFERENCES edw_g1_party.dim_organization (organization_sk),
    CONSTRAINT fk_dim_pat_pcp FOREIGN KEY (primary_care_practitioner_sk)
        REFERENCES edw_g1_party.dim_practitioner (practitioner_sk),
    CONSTRAINT fk_dim_pat_merged FOREIGN KEY (merged_into_patient_sk)
        REFERENCES edw_g1_party.dim_patient (patient_sk),
    CONSTRAINT fk_dim_pat_src FOREIGN KEY (source_system_sk)
        REFERENCES edw_g8_reference.dim_source_system (source_system_sk)
);
-- TD: CREATE SET TABLE edw_g1_party.dim_patient, FALLBACK (...) UNIQUE PRIMARY INDEX (patient_sk)
-- TD: UNIQUE INDEX (patient_bk, valid_from); INDEX (mrn_primary)
-- TD: COLLECT STATISTICS COLUMN (patient_sk), COLUMN (patient_bk), COLUMN (mrn_primary), COLUMN (is_current_flag), COLUMN (geography_sk) ON edw_g1_party.dim_patient;
CREATE UNIQUE INDEX ux_dim_patient_bk ON edw_g1_party.dim_patient (patient_bk, valid_from);
CREATE INDEX ix_dim_patient_mrn ON edw_g1_party.dim_patient (mrn_primary);
COMMENT ON TABLE edw_g1_party.dim_patient IS 'https://ontology.healix.example.org/party/Patient';
COMMENT ON COLUMN edw_g1_party.dim_patient.patient_bk IS 'phi:direct | https://ontology.healix.example.org/party/empiGoldenIdentifier';
COMMENT ON COLUMN edw_g1_party.dim_patient.empi_golden_iri IS 'phi:direct | https://ontology.healix.example.org/party/empiGoldenIri';
COMMENT ON COLUMN edw_g1_party.dim_patient.mrn_primary IS 'phi:direct | https://ontology.healix.example.org/party/medicalRecordNumber';
COMMENT ON COLUMN edw_g1_party.dim_patient.name_family IS 'phi:direct | https://ontology.healix.example.org/party/familyName';
COMMENT ON COLUMN edw_g1_party.dim_patient.name_given IS 'phi:direct | https://ontology.healix.example.org/party/givenName';
COMMENT ON COLUMN edw_g1_party.dim_patient.birth_date IS 'phi:direct | https://ontology.healix.example.org/party/birthDate';
COMMENT ON COLUMN edw_g1_party.dim_patient.deceased_ts IS 'phi:quasi | https://ontology.healix.example.org/party/deceasedDateTime';
COMMENT ON COLUMN edw_g1_party.dim_patient.administrative_gender_code_sk IS 'phi:quasi | https://ontology.healix.example.org/party/administrativeGender';
COMMENT ON COLUMN edw_g1_party.dim_patient.gender_identity_code_sk IS 'phi:sensitive-category | https://ontology.healix.example.org/party/genderIdentity';
COMMENT ON COLUMN edw_g1_party.dim_patient.race_primary_code_sk IS 'phi:quasi | https://ontology.healix.example.org/party/race';
COMMENT ON COLUMN edw_g1_party.dim_patient.ethnicity_code_sk IS 'phi:quasi | https://ontology.healix.example.org/party/ethnicity';
COMMENT ON COLUMN edw_g1_party.dim_patient.religion_code_sk IS 'phi:sensitive-category | https://ontology.healix.example.org/party/religiousAffiliation';
COMMENT ON COLUMN edw_g1_party.dim_patient.address_line1 IS 'phi:direct | https://ontology.healix.example.org/party/addressLine';
COMMENT ON COLUMN edw_g1_party.dim_patient.address_postal_code IS 'phi:quasi | https://ontology.healix.example.org/party/addressPostalCode';
COMMENT ON COLUMN edw_g1_party.dim_patient.geography_sk IS 'phi:quasi | https://ontology.healix.example.org/party/residesIn';
COMMENT ON COLUMN edw_g1_party.dim_patient.ssn_hash IS 'phi:direct | https://ontology.healix.example.org/party/nationalIdentifierHash';
COMMENT ON COLUMN edw_g1_party.dim_patient.managing_organization_sk IS 'phi:clinical | https://ontology.healix.example.org/party/managingOrganization';
COMMENT ON COLUMN edw_g1_party.dim_patient.primary_care_practitioner_sk IS 'phi:clinical | https://ontology.healix.example.org/party/generalPractitioner';
COMMENT ON COLUMN edw_g1_party.dim_patient.merged_into_patient_sk IS 'internal | https://ontology.healix.example.org/party/mergedInto';
COMMENT ON COLUMN edw_g1_party.dim_patient.valid_from IS 'internal | https://ontology.healix.example.org/time/validFrom';
COMMENT ON COLUMN edw_g1_party.dim_patient.recorded_at IS 'internal | https://ontology.healix.example.org/time/recordedAt';
COMMENT ON COLUMN edw_g1_party.dim_patient.prov_activity_iri IS 'internal | http://www.w3.org/ns/prov#wasGeneratedBy';

-- ---------------------------------------------------------------------------
-- hist_dim_patient: bitemporal history of dim_patient (EDW-HIST-001).
-- Same business columns + hist_sk, superseded_at, change_reason_code.
-- dim_patient == SELECT <business cols> FROM hist_dim_patient WHERE superseded_at IS NULL
-- ---------------------------------------------------------------------------
CREATE TABLE edw_g1_party.hist_dim_patient (
    hist_sk                 BIGINT        NOT NULL,
    patient_sk              BIGINT        NOT NULL,
    patient_bk              VARCHAR(128)  NOT NULL,
    party_sk                BIGINT        NOT NULL,
    empi_golden_iri         VARCHAR(512)  NOT NULL,
    mrn_primary             VARCHAR(64),
    mrn_primary_system      VARCHAR(255),
    name_family             VARCHAR(100),
    name_given              VARCHAR(100),
    name_middle             VARCHAR(100),
    name_prefix             VARCHAR(20),
    name_suffix             VARCHAR(20),
    birth_date              DATE,
    birth_date_estimated_flag CHAR(1)     NOT NULL,
    deceased_flag           CHAR(1)       NOT NULL,
    deceased_ts             TIMESTAMP,
    administrative_gender_code_sk BIGINT  NOT NULL,
    birth_sex_code_sk       BIGINT        NOT NULL,
    gender_identity_code_sk BIGINT        NOT NULL,
    race_primary_code_sk    BIGINT        NOT NULL,
    ethnicity_code_sk       BIGINT        NOT NULL,
    preferred_language_code_sk BIGINT     NOT NULL,
    marital_status_code_sk  BIGINT        NOT NULL,
    religion_code_sk        BIGINT        NOT NULL,
    interpreter_required_flag CHAR(1)     NOT NULL,
    multiple_birth_flag     CHAR(1)       NOT NULL,
    address_line1           VARCHAR(200),
    address_line2           VARCHAR(200),
    address_city            VARCHAR(100),
    address_state_code      CHAR(2),
    address_postal_code     VARCHAR(10),
    address_country_code    CHAR(2),
    geography_sk            BIGINT        NOT NULL,
    phone_home              VARCHAR(30),
    phone_mobile            VARCHAR(30),
    email                   VARCHAR(255),
    ssn_hash                CHAR(64),
    managing_organization_sk BIGINT       NOT NULL,
    primary_care_practitioner_sk BIGINT   NOT NULL,
    patient_status_code     VARCHAR(20)   NOT NULL,
    merged_into_patient_sk  BIGINT,
    test_patient_flag       CHAR(1)       NOT NULL,
    is_inferred_flag        CHAR(1)       NOT NULL,
    -- bitemporal block (full) · TKG-01 semantics
    valid_from              TIMESTAMP     NOT NULL,
    valid_to                TIMESTAMP,
    is_current_flag         CHAR(1)       NOT NULL,
    recorded_at             TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    superseded_at           TIMESTAMP,
    change_kind             CHAR(12),                     -- TKG-01 C6/C10: why this version was closed; NULL while current, NOT NULL when superseded_at is set (new-version / correction / retraction / merge / unmerge)
    change_reason_code      VARCHAR(30)   NOT NULL,   -- insert / source-update / correction / merge / late-arrival / retraction / reload
    event_time              TIMESTAMP     NOT NULL,       -- TKG-01 C7: hc-event-time of the producing record (UTC)
    source_time             TIMESTAMP,                    -- TKG-01 C7: hc-source-time, the source system clock (UTC)
    decision_time           TIMESTAMP,                    -- TKG-01 C8: when the real-world decision was taken; NULL = not available
    source_tz               VARCHAR(64),                  -- TKG-01 C12: IANA zone of the source timestamps (storage is UTC)
    -- technical block
    source_system_sk        INTEGER       NOT NULL,
    load_batch_id           BIGINT        NOT NULL,
    record_hash             CHAR(64)      NOT NULL,
    prov_activity_iri       VARCHAR(512)  NOT NULL,
    CONSTRAINT pk_hist_dim_patient PRIMARY KEY (hist_sk),
    CONSTRAINT fk_hist_pat_patient FOREIGN KEY (patient_sk)
        REFERENCES edw_g1_party.dim_patient (patient_sk),
    CONSTRAINT fk_hist_pat_party FOREIGN KEY (party_sk)
        REFERENCES edw_g1_party.dim_party (party_sk),
    CONSTRAINT fk_hist_pat_admin_gender FOREIGN KEY (administrative_gender_code_sk)
        REFERENCES edw_g8_reference.dim_code (code_sk),
    CONSTRAINT fk_hist_pat_birth_sex FOREIGN KEY (birth_sex_code_sk)
        REFERENCES edw_g8_reference.dim_code (code_sk),
    CONSTRAINT fk_hist_pat_gender_identity FOREIGN KEY (gender_identity_code_sk)
        REFERENCES edw_g8_reference.dim_code (code_sk),
    CONSTRAINT fk_hist_pat_race FOREIGN KEY (race_primary_code_sk)
        REFERENCES edw_g8_reference.dim_code (code_sk),
    CONSTRAINT fk_hist_pat_ethnicity FOREIGN KEY (ethnicity_code_sk)
        REFERENCES edw_g8_reference.dim_code (code_sk),
    CONSTRAINT fk_hist_pat_language FOREIGN KEY (preferred_language_code_sk)
        REFERENCES edw_g8_reference.dim_code (code_sk),
    CONSTRAINT fk_hist_pat_marital FOREIGN KEY (marital_status_code_sk)
        REFERENCES edw_g8_reference.dim_code (code_sk),
    CONSTRAINT fk_hist_pat_religion FOREIGN KEY (religion_code_sk)
        REFERENCES edw_g8_reference.dim_code (code_sk),
    CONSTRAINT fk_hist_pat_geography FOREIGN KEY (geography_sk)
        REFERENCES edw_g8_reference.dim_geography (geography_sk),
    CONSTRAINT fk_hist_pat_managing_org FOREIGN KEY (managing_organization_sk)
        REFERENCES edw_g1_party.dim_organization (organization_sk),
    CONSTRAINT fk_hist_pat_pcp FOREIGN KEY (primary_care_practitioner_sk)
        REFERENCES edw_g1_party.dim_practitioner (practitioner_sk),
    CONSTRAINT fk_hist_pat_merged FOREIGN KEY (merged_into_patient_sk)
        REFERENCES edw_g1_party.dim_patient (patient_sk),
    CONSTRAINT fk_hist_pat_src FOREIGN KEY (source_system_sk)
        REFERENCES edw_g8_reference.dim_source_system (source_system_sk)
);
-- TD: CREATE MULTISET TABLE edw_g1_party.hist_dim_patient, FALLBACK, BLOCKCOMPRESSION = AUTOTEMP (...) PRIMARY INDEX (patient_bk)
-- TD: PARTITION BY RANGE_N(CAST(recorded_at AS DATE) BETWEEN DATE '2015-01-01' AND DATE '2040-12-31' EACH INTERVAL '1' MONTH, NO RANGE)
-- TD: INDEX (patient_sk)
-- TD: COLLECT STATISTICS COLUMN (patient_bk), COLUMN (patient_sk), COLUMN (recorded_at), COLUMN (superseded_at), COLUMN (PARTITION) ON edw_g1_party.hist_dim_patient;
-- TD (optional generated variant, 06-history-and-bitemporality.md §3.2):
-- TD:   valid_period PERIOD(TIMESTAMP(6)) NOT NULL AS VALIDTIME, transaction_period PERIOD(TIMESTAMP(6)) NOT NULL AS TRANSACTIONTIME
CREATE INDEX ix_hist_dim_patient_bk ON edw_g1_party.hist_dim_patient (patient_bk, valid_from, recorded_at);
CREATE INDEX ix_hist_dim_patient_sk ON edw_g1_party.hist_dim_patient (patient_sk);
COMMENT ON TABLE edw_g1_party.hist_dim_patient IS 'https://ontology.healix.example.org/party/Patient';
COMMENT ON COLUMN edw_g1_party.hist_dim_patient.valid_from IS 'internal | https://ontology.healix.example.org/time/validFrom';
COMMENT ON COLUMN edw_g1_party.hist_dim_patient.valid_to IS 'internal | https://ontology.healix.example.org/time/validTo';
COMMENT ON COLUMN edw_g1_party.hist_dim_patient.recorded_at IS 'internal | https://ontology.healix.example.org/time/recordedAt';
COMMENT ON COLUMN edw_g1_party.hist_dim_patient.superseded_at IS 'internal | https://ontology.healix.example.org/time/supersededAt';
COMMENT ON COLUMN edw_g1_party.hist_dim_patient.change_reason_code IS 'internal | https://ontology.healix.example.org/time/changeReason';
COMMENT ON COLUMN edw_g1_party.hist_dim_patient.change_kind IS 'internal | https://ontology.healix.example.org/time/changeKind';
COMMENT ON COLUMN edw_g1_party.hist_dim_patient.event_time IS 'internal | https://ontology.healix.example.org/time/eventTime';
COMMENT ON COLUMN edw_g1_party.hist_dim_patient.source_time IS 'internal | https://ontology.healix.example.org/time/sourceTime';
COMMENT ON COLUMN edw_g1_party.hist_dim_patient.decision_time IS 'internal | https://ontology.healix.example.org/time/decisionTime';
COMMENT ON COLUMN edw_g1_party.hist_dim_patient.source_tz IS 'internal | https://ontology.healix.example.org/time/sourceTimeZone';
COMMENT ON COLUMN edw_g1_party.hist_dim_patient.prov_activity_iri IS 'internal | http://www.w3.org/ns/prov#wasGeneratedBy';

-- ---------------------------------------------------------------------------
-- bridge_patient_link: EMPI link decisions source record ↔ golden patient
-- (EDW-G1-003, EDW-CONF-002); mirrors module-08 sameAs/resolvedTo assertions
-- ---------------------------------------------------------------------------
CREATE TABLE edw_g1_party.bridge_patient_link (
    patient_link_hk         CHAR(64)      NOT NULL,   -- SHA-256(source_system_sk, source_record_id, valid_from)
    patient_sk              BIGINT        NOT NULL,
    patient_bk              VARCHAR(128)  NOT NULL,
    source_record_system_sk INTEGER       NOT NULL,   -- system that owns the source record
    source_record_id        VARCHAR(64)   NOT NULL,   -- e.g. Cerner PERSON_ID, HCM person id
    source_mrn              VARCHAR(64),
    match_score             DECIMAL(6,4),
    resolution_method_code  VARCHAR(20)   NOT NULL,   -- deterministic / probabilistic / manual / ml (module 08)
    link_status_code        VARCHAR(12)   NOT NULL,   -- linked / unlinked / pending / rejected
    decided_by              VARCHAR(100),             -- agent id or steward principal
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
    CONSTRAINT pk_bridge_patient_link PRIMARY KEY (patient_link_hk),
    CONSTRAINT fk_bpl_patient FOREIGN KEY (patient_sk)
        REFERENCES edw_g1_party.dim_patient (patient_sk),
    CONSTRAINT fk_bpl_record_system FOREIGN KEY (source_record_system_sk)
        REFERENCES edw_g8_reference.dim_source_system (source_system_sk),
    CONSTRAINT fk_bpl_src FOREIGN KEY (source_system_sk)
        REFERENCES edw_g8_reference.dim_source_system (source_system_sk)
);
-- TD: PRIMARY INDEX (patient_sk); INDEX (source_record_system_sk, source_record_id)
CREATE INDEX ix_bridge_patient_link_src ON edw_g1_party.bridge_patient_link (source_record_system_sk, source_record_id);
COMMENT ON TABLE edw_g1_party.bridge_patient_link IS 'https://ontology.healix.example.org/party/PatientSourceLink';
COMMENT ON COLUMN edw_g1_party.bridge_patient_link.source_record_id IS 'phi:direct | https://ontology.healix.example.org/party/sourceRecordIdentifier';
COMMENT ON COLUMN edw_g1_party.bridge_patient_link.match_score IS 'internal | https://ontology.healix.example.org/party/matchScore';
COMMENT ON COLUMN edw_g1_party.bridge_patient_link.resolution_method_code IS 'internal | https://ontology.healix.example.org/party/resolutionMethod';
