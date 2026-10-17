-- =============================================================================
-- HEALIX EDW · module 05 · g3_finance.sql
-- G3 Revenue Cycle & Finance · schemas edw_g3_finance, edw_g3_revenue
-- Tables (edw_g3_finance): dim_cost_center (conformed), dim_gl_account,
--         dim_ledger, dim_accounting_period, fact_gl_journal_line
-- Tables (edw_g3_revenue): dim_payer_plan, dim_claim_type, dim_claim_status,
--         fact_charge, fact_claim_header, fact_claim_line, bridge_claim_diagnosis
-- Specs:  01-subject-area-groups.md §G3 · 03-logical-model.md §9.5 ·
--         05-conformed-dimensions.md §8
-- Depends on: g8_reference.sql, g9_governance.sql, g1_party.sql
-- fact_charge.item_sk → edw_g4_supply.dim_item is added by ALTER TABLE in
-- g4_supply.sql (cross-group cycle).
-- Dialect: portable ANSI; Teradata hints as "-- TD:" comments.
-- =============================================================================

-- ===========================================================================
-- edw_g3_finance
-- ===========================================================================

-- ---------------------------------------------------------------------------
-- dim_cost_center (conformed, Type 2) · 05-conformed-dimensions.md §8
-- ---------------------------------------------------------------------------
CREATE TABLE edw_g3_finance.dim_cost_center (
    cost_center_sk          BIGINT        NOT NULL,
    cost_center_bk          VARCHAR(128)  NOT NULL,   -- <ledger>:<segment value>
    cost_center_code        VARCHAR(30)   NOT NULL,
    cost_center_name        VARCHAR(255)  NOT NULL,
    department_code         VARCHAR(30),
    department_name         VARCHAR(255),
    division_code           VARCHAR(30),
    division_name           VARCHAR(255),
    entity_code             VARCHAR(30),
    entity_name             VARCHAR(255),
    parent_cost_center_sk   BIGINT,
    organization_sk         BIGINT        NOT NULL,   -- legal entity / facility
    cost_center_type_code   VARCHAR(12)   NOT NULL,   -- revenue / expense / overhead / capital
    manager_worker_bk       VARCHAR(128),             -- G5 worker
    cerner_department_cd    VARCHAR(30),              -- crosswalk for charge attribution
    active_flag             CHAR(1)       NOT NULL DEFAULT 'Y',
    allow_posting_flag      CHAR(1)       NOT NULL DEFAULT 'Y',
    is_inferred_flag        CHAR(1)       NOT NULL DEFAULT 'N',
    valid_from              TIMESTAMP     NOT NULL,
    valid_to                TIMESTAMP,
    is_current_flag         CHAR(1)       NOT NULL DEFAULT 'Y',
    source_system_sk        INTEGER       NOT NULL,
    load_batch_id           BIGINT        NOT NULL,
    record_hash             CHAR(64)      NOT NULL,
    recorded_at             TIMESTAMP     NOT NULL,
    prov_activity_iri       VARCHAR(512)  NOT NULL,
    CONSTRAINT pk_dim_cost_center PRIMARY KEY (cost_center_sk),
    CONSTRAINT fk_dim_cc_parent FOREIGN KEY (parent_cost_center_sk)
        REFERENCES edw_g3_finance.dim_cost_center (cost_center_sk),
    CONSTRAINT fk_dim_cc_org FOREIGN KEY (organization_sk)
        REFERENCES edw_g1_party.dim_organization (organization_sk),
    CONSTRAINT fk_dim_cc_src FOREIGN KEY (source_system_sk)
        REFERENCES edw_g8_reference.dim_source_system (source_system_sk)
);
-- TD: CREATE SET TABLE edw_g3_finance.dim_cost_center, FALLBACK (...) UNIQUE PRIMARY INDEX (cost_center_sk)
CREATE UNIQUE INDEX ux_dim_cost_center_bk ON edw_g3_finance.dim_cost_center (cost_center_bk, valid_from);
COMMENT ON TABLE edw_g3_finance.dim_cost_center IS 'https://ontology.healix.example.org/finance/CostCenter';
COMMENT ON COLUMN edw_g3_finance.dim_cost_center.cost_center_code IS 'none | https://ontology.healix.example.org/finance/costCenterCode';
COMMENT ON COLUMN edw_g3_finance.dim_cost_center.parent_cost_center_sk IS 'none | https://ontology.healix.example.org/finance/costCenterPartOf';

-- ---------------------------------------------------------------------------
-- dim_gl_account (Type 2): natural account with roll-up hierarchy
-- ---------------------------------------------------------------------------
CREATE TABLE edw_g3_finance.dim_gl_account (
    gl_account_sk           BIGINT        NOT NULL,
    gl_account_bk           VARCHAR(128)  NOT NULL,   -- <chart of accounts>:<natural account>
    natural_account_code    VARCHAR(30)   NOT NULL,
    gl_account_name         VARCHAR(255)  NOT NULL,
    account_type_code       VARCHAR(12)   NOT NULL,   -- asset / liability / equity / revenue / expense
    account_subtype_code    VARCHAR(30),
    normal_balance_code     CHAR(1)       NOT NULL,   -- D / C
    parent_gl_account_sk    BIGINT,
    hierarchy_level         SMALLINT,
    level1_name             VARCHAR(255),             -- statement line
    level2_name             VARCHAR(255),             -- group
    level3_name             VARCHAR(255),             -- sub-group
    level4_name             VARCHAR(255),             -- account
    financial_statement_line VARCHAR(100),
    chart_of_accounts_name  VARCHAR(100)  NOT NULL,
    allow_posting_flag      CHAR(1)       NOT NULL DEFAULT 'Y',
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
    CONSTRAINT pk_dim_gl_account PRIMARY KEY (gl_account_sk),
    CONSTRAINT fk_dim_gl_account_parent FOREIGN KEY (parent_gl_account_sk)
        REFERENCES edw_g3_finance.dim_gl_account (gl_account_sk),
    CONSTRAINT fk_dim_gl_account_src FOREIGN KEY (source_system_sk)
        REFERENCES edw_g8_reference.dim_source_system (source_system_sk)
);
-- TD: UNIQUE PRIMARY INDEX (gl_account_sk)
CREATE UNIQUE INDEX ux_dim_gl_account_bk ON edw_g3_finance.dim_gl_account (gl_account_bk, valid_from);
COMMENT ON TABLE edw_g3_finance.dim_gl_account IS 'https://ontology.healix.example.org/finance/GeneralLedgerAccount';
COMMENT ON COLUMN edw_g3_finance.dim_gl_account.natural_account_code IS 'none | https://ontology.healix.example.org/finance/naturalAccountCode';
COMMENT ON COLUMN edw_g3_finance.dim_gl_account.account_type_code IS 'none | https://ontology.healix.example.org/finance/accountType';

-- ---------------------------------------------------------------------------
-- dim_ledger (Type 1)
-- ---------------------------------------------------------------------------
CREATE TABLE edw_g3_finance.dim_ledger (
    ledger_sk               BIGINT        NOT NULL,
    ledger_bk               VARCHAR(128)  NOT NULL,   -- Fusion ledger id
    ledger_name             VARCHAR(100)  NOT NULL,
    ledger_category_code    VARCHAR(12)   NOT NULL,   -- primary / secondary / reporting
    ledger_currency_code    CHAR(3)       NOT NULL,
    chart_of_accounts_name  VARCHAR(100)  NOT NULL,
    accounting_calendar_name VARCHAR(100) NOT NULL,
    legal_entity_organization_sk BIGINT   NOT NULL,
    source_system_sk        INTEGER       NOT NULL,
    load_batch_id           BIGINT        NOT NULL,
    record_hash             CHAR(64)      NOT NULL,
    recorded_at             TIMESTAMP     NOT NULL,
    prov_activity_iri       VARCHAR(512)  NOT NULL,
    CONSTRAINT pk_dim_ledger PRIMARY KEY (ledger_sk),
    CONSTRAINT fk_dim_ledger_org FOREIGN KEY (legal_entity_organization_sk)
        REFERENCES edw_g1_party.dim_organization (organization_sk),
    CONSTRAINT fk_dim_ledger_src FOREIGN KEY (source_system_sk)
        REFERENCES edw_g8_reference.dim_source_system (source_system_sk)
);
-- TD: UNIQUE PRIMARY INDEX (ledger_sk)
CREATE UNIQUE INDEX ux_dim_ledger_bk ON edw_g3_finance.dim_ledger (ledger_bk);
COMMENT ON TABLE edw_g3_finance.dim_ledger IS 'https://ontology.healix.example.org/finance/Ledger';

-- ---------------------------------------------------------------------------
-- dim_accounting_period (Type 1)
-- ---------------------------------------------------------------------------
CREATE TABLE edw_g3_finance.dim_accounting_period (
    accounting_period_sk    BIGINT        NOT NULL,
    accounting_period_bk    VARCHAR(128)  NOT NULL,   -- <calendar>:<period name>
    accounting_calendar_name VARCHAR(100) NOT NULL,
    period_name             VARCHAR(30)   NOT NULL,
    fiscal_year             SMALLINT      NOT NULL,
    period_number           SMALLINT      NOT NULL,
    quarter_number          SMALLINT,
    period_start_date       DATE          NOT NULL,
    period_end_date         DATE          NOT NULL,
    period_status_code      VARCHAR(20)   NOT NULL,   -- never-opened / open / closed / permanently-closed
    is_adjustment_period_flag CHAR(1)     NOT NULL DEFAULT 'N',
    source_system_sk        INTEGER       NOT NULL,
    load_batch_id           BIGINT        NOT NULL,
    record_hash             CHAR(64)      NOT NULL,
    recorded_at             TIMESTAMP     NOT NULL,
    prov_activity_iri       VARCHAR(512)  NOT NULL,
    CONSTRAINT pk_dim_accounting_period PRIMARY KEY (accounting_period_sk),
    CONSTRAINT fk_dim_acct_period_src FOREIGN KEY (source_system_sk)
        REFERENCES edw_g8_reference.dim_source_system (source_system_sk)
);
-- TD: UNIQUE PRIMARY INDEX (accounting_period_sk)
CREATE UNIQUE INDEX ux_dim_accounting_period_bk ON edw_g3_finance.dim_accounting_period (accounting_period_bk);
COMMENT ON TABLE edw_g3_finance.dim_accounting_period IS 'https://ontology.healix.example.org/finance/AccountingPeriod';

-- ---------------------------------------------------------------------------
-- fact_gl_journal_line (transaction): one row per GL journal line (EDW-G3-002)
-- ---------------------------------------------------------------------------
CREATE TABLE edw_g3_finance.fact_gl_journal_line (
    gl_journal_line_sk      BIGINT        NOT NULL,
    gl_journal_line_bk      VARCHAR(128)  NOT NULL,   -- OFC:<ledger>:<je_header_id>:<je_line_num>
    journal_header_bk       VARCHAR(128)  NOT NULL,   -- degenerate: journal header
    journal_name            VARCHAR(100),
    journal_line_number     INTEGER       NOT NULL,
    ledger_sk               BIGINT        NOT NULL,
    accounting_period_sk    BIGINT        NOT NULL,
    gl_account_sk           BIGINT        NOT NULL,
    cost_center_sk          BIGINT        NOT NULL,
    legal_entity_organization_sk BIGINT   NOT NULL,
    business_unit_organization_sk BIGINT  NOT NULL,
    journal_source_code_sk  BIGINT        NOT NULL,   -- dim_code (Fusion journal source value set)
    journal_category_code_sk BIGINT       NOT NULL,   -- dim_code (Fusion journal category value set)
    accounting_date_sk      INTEGER       NOT NULL,
    accounting_date         DATE          NOT NULL,
    posted_date_sk          INTEGER       NOT NULL,
    posted_ts               TIMESTAMP,
    entered_debit_amount    DECIMAL(18,4),
    entered_credit_amount   DECIMAL(18,4),
    entered_currency_code   CHAR(3)       NOT NULL,
    debit_ledger_amount     DECIMAL(18,4),
    credit_ledger_amount    DECIMAL(18,4),
    ledger_currency_code    CHAR(3)       NOT NULL,
    exchange_rate           DECIMAL(18,8),
    line_description        VARCHAR(255),
    reference_document_type VARCHAR(30),              -- charge / payment / ap-invoice / payroll / manual
    reference_document_bk   VARCHAR(128),             -- cross-group link (fact_charge, fact_payment, fact_ap_invoice)
    journal_status_code     VARCHAR(12)   NOT NULL,   -- unposted / posted / reversed
    is_reversal_flag        CHAR(1)       NOT NULL DEFAULT 'N',
    reversed_journal_bk     VARCHAR(128),
    source_journal_line_id  VARCHAR(64),
    orphan_flag             CHAR(1)       NOT NULL DEFAULT 'N',
    -- bitemporal block
    valid_from              TIMESTAMP     NOT NULL,
    valid_to                TIMESTAMP,
    recorded_at             TIMESTAMP     NOT NULL,
    superseded_at           TIMESTAMP,
    change_reason_code      VARCHAR(30),
    -- technical block
    source_system_sk        INTEGER       NOT NULL,
    load_batch_id           BIGINT        NOT NULL,
    record_hash             CHAR(64)      NOT NULL,
    prov_activity_iri       VARCHAR(512)  NOT NULL,
    CONSTRAINT pk_fact_gl_journal_line PRIMARY KEY (gl_journal_line_sk),
    CONSTRAINT fk_gl_line_ledger FOREIGN KEY (ledger_sk)
        REFERENCES edw_g3_finance.dim_ledger (ledger_sk),
    CONSTRAINT fk_gl_line_period FOREIGN KEY (accounting_period_sk)
        REFERENCES edw_g3_finance.dim_accounting_period (accounting_period_sk),
    CONSTRAINT fk_gl_line_account FOREIGN KEY (gl_account_sk)
        REFERENCES edw_g3_finance.dim_gl_account (gl_account_sk),
    CONSTRAINT fk_gl_line_cost_center FOREIGN KEY (cost_center_sk)
        REFERENCES edw_g3_finance.dim_cost_center (cost_center_sk),
    CONSTRAINT fk_gl_line_legal_entity FOREIGN KEY (legal_entity_organization_sk)
        REFERENCES edw_g1_party.dim_organization (organization_sk),
    CONSTRAINT fk_gl_line_business_unit FOREIGN KEY (business_unit_organization_sk)
        REFERENCES edw_g1_party.dim_organization (organization_sk),
    CONSTRAINT fk_gl_line_source_code FOREIGN KEY (journal_source_code_sk)
        REFERENCES edw_g8_reference.dim_code (code_sk),
    CONSTRAINT fk_gl_line_category_code FOREIGN KEY (journal_category_code_sk)
        REFERENCES edw_g8_reference.dim_code (code_sk),
    CONSTRAINT fk_gl_line_acct_date FOREIGN KEY (accounting_date_sk)
        REFERENCES edw_g8_reference.dim_date (date_sk),
    CONSTRAINT fk_gl_line_posted_date FOREIGN KEY (posted_date_sk)
        REFERENCES edw_g8_reference.dim_date (date_sk),
    CONSTRAINT fk_gl_line_src FOREIGN KEY (source_system_sk)
        REFERENCES edw_g8_reference.dim_source_system (source_system_sk)
);
-- TD: CREATE MULTISET TABLE edw_g3_finance.fact_gl_journal_line, FALLBACK, BLOCKCOMPRESSION = AUTOTEMP (...) PRIMARY INDEX (journal_header_bk)
-- TD: PARTITION BY (RANGE_N(accounting_date BETWEEN DATE '2015-01-01' AND DATE '2040-12-31' EACH INTERVAL '1' MONTH, NO RANGE),
-- TD:               CASE_N(superseded_at IS NULL, NO CASE))
-- TD: COLLECT STATISTICS COLUMN (journal_header_bk), COLUMN (gl_account_sk), COLUMN (cost_center_sk), COLUMN (accounting_period_sk), COLUMN (PARTITION) ON edw_g3_finance.fact_gl_journal_line;
CREATE UNIQUE INDEX ux_fact_gl_journal_line_bk ON edw_g3_finance.fact_gl_journal_line (gl_journal_line_bk, valid_from);
CREATE INDEX ix_fact_gl_journal_line_hdr ON edw_g3_finance.fact_gl_journal_line (journal_header_bk);
CREATE INDEX ix_fact_gl_journal_line_ref ON edw_g3_finance.fact_gl_journal_line (reference_document_type, reference_document_bk);
COMMENT ON TABLE edw_g3_finance.fact_gl_journal_line IS 'https://ontology.healix.example.org/finance/GeneralLedgerJournalLine';
COMMENT ON COLUMN edw_g3_finance.fact_gl_journal_line.debit_ledger_amount IS 'none | https://ontology.healix.example.org/finance/debitLedgerAmount';
COMMENT ON COLUMN edw_g3_finance.fact_gl_journal_line.credit_ledger_amount IS 'none | https://ontology.healix.example.org/finance/creditLedgerAmount';
COMMENT ON COLUMN edw_g3_finance.fact_gl_journal_line.accounting_date IS 'none | https://ontology.healix.example.org/finance/accountingDate';
COMMENT ON COLUMN edw_g3_finance.fact_gl_journal_line.reference_document_bk IS 'none | https://ontology.healix.example.org/finance/accountsFor';
COMMENT ON COLUMN edw_g3_finance.fact_gl_journal_line.prov_activity_iri IS 'none | http://www.w3.org/ns/prov#wasGeneratedBy';

-- ===========================================================================
-- edw_g3_revenue
-- ===========================================================================

-- ---------------------------------------------------------------------------
-- dim_payer_plan (Type 2): payer → plan hierarchy (EDW-G3-005, EDW-CONF-010)
-- ---------------------------------------------------------------------------
CREATE TABLE edw_g3_revenue.dim_payer_plan (
    payer_plan_sk           BIGINT        NOT NULL,
    payer_plan_bk           VARCHAR(128)  NOT NULL,
    payer_sk                BIGINT        NOT NULL,
    plan_code               VARCHAR(30)   NOT NULL,
    plan_name               VARCHAR(255)  NOT NULL,
    product_type_code_sk    BIGINT        NOT NULL,   -- HMO / PPO / EPO / POS / FFS / MA / Part D ...
    line_of_business_code_sk BIGINT       NOT NULL,
    group_number            VARCHAR(30),
    network_name            VARCHAR(100),
    plan_effective_date     DATE,
    plan_termination_date   DATE,
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
    CONSTRAINT pk_dim_payer_plan PRIMARY KEY (payer_plan_sk),
    CONSTRAINT fk_dim_payer_plan_payer FOREIGN KEY (payer_sk)
        REFERENCES edw_g1_party.dim_payer (payer_sk),
    CONSTRAINT fk_dim_payer_plan_product FOREIGN KEY (product_type_code_sk)
        REFERENCES edw_g8_reference.dim_code (code_sk),
    CONSTRAINT fk_dim_payer_plan_lob FOREIGN KEY (line_of_business_code_sk)
        REFERENCES edw_g8_reference.dim_code (code_sk),
    CONSTRAINT fk_dim_payer_plan_src FOREIGN KEY (source_system_sk)
        REFERENCES edw_g8_reference.dim_source_system (source_system_sk)
);
-- TD: UNIQUE PRIMARY INDEX (payer_plan_sk)
CREATE UNIQUE INDEX ux_dim_payer_plan_bk ON edw_g3_revenue.dim_payer_plan (payer_plan_bk, valid_from);
COMMENT ON TABLE edw_g3_revenue.dim_payer_plan IS 'https://ontology.healix.example.org/finance/PayerPlan';
COMMENT ON COLUMN edw_g3_revenue.dim_payer_plan.payer_sk IS 'none | https://ontology.healix.example.org/finance/offeredByPayer';

-- ---------------------------------------------------------------------------
-- Value-set dimensions (Type 1, bound to dim_code)
-- ---------------------------------------------------------------------------
CREATE TABLE edw_g3_revenue.dim_claim_type (
    claim_type_sk           BIGINT        NOT NULL,
    code_sk                 BIGINT        NOT NULL,
    code                    VARCHAR(100)  NOT NULL,   -- institutional / professional / dental / pharmacy / vision
    display                 VARCHAR(500),
    x12_transaction_code    VARCHAR(10),              -- 837I / 837P / 837D
    code_system_uri         VARCHAR(255)  NOT NULL,
    value_set_iri           VARCHAR(512)  NOT NULL,
    sort_order              SMALLINT,
    source_system_sk        INTEGER       NOT NULL,
    load_batch_id           BIGINT        NOT NULL,
    record_hash             CHAR(64)      NOT NULL,
    recorded_at             TIMESTAMP     NOT NULL,
    prov_activity_iri       VARCHAR(512)  NOT NULL,
    CONSTRAINT pk_dim_claim_type PRIMARY KEY (claim_type_sk),
    CONSTRAINT fk_dim_claim_type_code FOREIGN KEY (code_sk)
        REFERENCES edw_g8_reference.dim_code (code_sk),
    CONSTRAINT fk_dim_claim_type_src FOREIGN KEY (source_system_sk)
        REFERENCES edw_g8_reference.dim_source_system (source_system_sk)
);
-- TD: UNIQUE PRIMARY INDEX (claim_type_sk)
COMMENT ON TABLE edw_g3_revenue.dim_claim_type IS 'https://ontology.healix.example.org/finance/ClaimType';

CREATE TABLE edw_g3_revenue.dim_claim_status (
    claim_status_sk         BIGINT        NOT NULL,
    code_sk                 BIGINT        NOT NULL,
    code                    VARCHAR(100)  NOT NULL,   -- draft / submitted / accepted / rejected / adjudicated / paid / denied / appealed / void
    display                 VARCHAR(500),
    is_final_flag           CHAR(1)       NOT NULL DEFAULT 'N',
    code_system_uri         VARCHAR(255)  NOT NULL,
    value_set_iri           VARCHAR(512)  NOT NULL,
    sort_order              SMALLINT,
    source_system_sk        INTEGER       NOT NULL,
    load_batch_id           BIGINT        NOT NULL,
    record_hash             CHAR(64)      NOT NULL,
    recorded_at             TIMESTAMP     NOT NULL,
    prov_activity_iri       VARCHAR(512)  NOT NULL,
    CONSTRAINT pk_dim_claim_status PRIMARY KEY (claim_status_sk),
    CONSTRAINT fk_dim_claim_status_code FOREIGN KEY (code_sk)
        REFERENCES edw_g8_reference.dim_code (code_sk),
    CONSTRAINT fk_dim_claim_status_src FOREIGN KEY (source_system_sk)
        REFERENCES edw_g8_reference.dim_source_system (source_system_sk)
);
-- TD: UNIQUE PRIMARY INDEX (claim_status_sk)
COMMENT ON TABLE edw_g3_revenue.dim_claim_status IS 'https://ontology.healix.example.org/finance/ClaimStatus';

-- ---------------------------------------------------------------------------
-- fact_charge (transaction): one row per charge line (EDW-G3-003)
-- item_sk FK to edw_g4_supply.dim_item is declared in g4_supply.sql
-- ---------------------------------------------------------------------------
CREATE TABLE edw_g3_revenue.fact_charge (
    charge_sk               BIGINT        NOT NULL,
    charge_bk               VARCHAR(128)  NOT NULL,
    charge_number           VARCHAR(64),              -- degenerate
    patient_sk              BIGINT        NOT NULL,
    encounter_bk            VARCHAR(128),             -- degenerate → G2 fact_encounter
    account_bk              VARCHAR(128),             -- degenerate → fact_account
    performing_practitioner_sk BIGINT     NOT NULL,
    billing_practitioner_sk BIGINT        NOT NULL,
    location_sk             BIGINT        NOT NULL,
    service_provider_org_sk BIGINT        NOT NULL,
    cost_center_sk          BIGINT        NOT NULL,
    revenue_gl_account_sk   BIGINT        NOT NULL,
    procedure_code_sk       BIGINT        NOT NULL,   -- CPT / HCPCS
    revenue_code_sk         BIGINT        NOT NULL,   -- UB-04 revenue code
    item_sk                 BIGINT        NOT NULL,   -- supply / drug charged (-2 when service only)
    charge_code_src         VARCHAR(64),              -- charge master (CDM) code
    charge_description_src  VARCHAR(255),
    service_date_sk         INTEGER       NOT NULL,
    service_ts              TIMESTAMP,
    post_date_sk            INTEGER       NOT NULL,
    post_ts                 TIMESTAMP,
    quantity                DECIMAL(12,3) NOT NULL,
    unit_price_amount       DECIMAL(18,4),
    extended_amount         DECIMAL(18,4) NOT NULL,
    currency_code           CHAR(3)       NOT NULL DEFAULT 'USD',
    charge_status_code_sk   BIGINT        NOT NULL,
    is_credit_flag          CHAR(1)       NOT NULL DEFAULT 'N',
    reversal_of_charge_bk   VARCHAR(128),
    source_charge_id        VARCHAR(64),
    orphan_flag             CHAR(1)       NOT NULL DEFAULT 'N',
    -- bitemporal block
    valid_from              TIMESTAMP     NOT NULL,
    valid_to                TIMESTAMP,
    recorded_at             TIMESTAMP     NOT NULL,
    superseded_at           TIMESTAMP,
    change_reason_code      VARCHAR(30),
    -- technical block
    source_system_sk        INTEGER       NOT NULL,
    load_batch_id           BIGINT        NOT NULL,
    record_hash             CHAR(64)      NOT NULL,
    prov_activity_iri       VARCHAR(512)  NOT NULL,
    CONSTRAINT pk_fact_charge PRIMARY KEY (charge_sk),
    CONSTRAINT fk_charge_patient FOREIGN KEY (patient_sk)
        REFERENCES edw_g1_party.dim_patient (patient_sk),
    CONSTRAINT fk_charge_perf_prac FOREIGN KEY (performing_practitioner_sk)
        REFERENCES edw_g1_party.dim_practitioner (practitioner_sk),
    CONSTRAINT fk_charge_bill_prac FOREIGN KEY (billing_practitioner_sk)
        REFERENCES edw_g1_party.dim_practitioner (practitioner_sk),
    CONSTRAINT fk_charge_location FOREIGN KEY (location_sk)
        REFERENCES edw_g1_party.dim_location (location_sk),
    CONSTRAINT fk_charge_org FOREIGN KEY (service_provider_org_sk)
        REFERENCES edw_g1_party.dim_organization (organization_sk),
    CONSTRAINT fk_charge_cost_center FOREIGN KEY (cost_center_sk)
        REFERENCES edw_g3_finance.dim_cost_center (cost_center_sk),
    CONSTRAINT fk_charge_gl_account FOREIGN KEY (revenue_gl_account_sk)
        REFERENCES edw_g3_finance.dim_gl_account (gl_account_sk),
    CONSTRAINT fk_charge_procedure_code FOREIGN KEY (procedure_code_sk)
        REFERENCES edw_g8_reference.dim_code (code_sk),
    CONSTRAINT fk_charge_revenue_code FOREIGN KEY (revenue_code_sk)
        REFERENCES edw_g8_reference.dim_code (code_sk),
    CONSTRAINT fk_charge_service_date FOREIGN KEY (service_date_sk)
        REFERENCES edw_g8_reference.dim_date (date_sk),
    CONSTRAINT fk_charge_post_date FOREIGN KEY (post_date_sk)
        REFERENCES edw_g8_reference.dim_date (date_sk),
    CONSTRAINT fk_charge_status FOREIGN KEY (charge_status_code_sk)
        REFERENCES edw_g8_reference.dim_code (code_sk),
    CONSTRAINT fk_charge_src FOREIGN KEY (source_system_sk)
        REFERENCES edw_g8_reference.dim_source_system (source_system_sk)
);
-- TD: CREATE MULTISET TABLE edw_g3_revenue.fact_charge, FALLBACK, BLOCKCOMPRESSION = AUTOTEMP (...) PRIMARY INDEX (patient_sk)
-- TD: PARTITION BY (RANGE_N(CAST(service_ts AS DATE) BETWEEN DATE '2015-01-01' AND DATE '2040-12-31' EACH INTERVAL '1' MONTH, NO RANGE, UNKNOWN),
-- TD:               CASE_N(superseded_at IS NULL, NO CASE))
-- TD: COLLECT STATISTICS COLUMN (patient_sk), COLUMN (encounter_bk), COLUMN (cost_center_sk), COLUMN (procedure_code_sk), COLUMN (PARTITION) ON edw_g3_revenue.fact_charge;
CREATE UNIQUE INDEX ux_fact_charge_bk ON edw_g3_revenue.fact_charge (charge_bk, valid_from);
CREATE INDEX ix_fact_charge_encounter ON edw_g3_revenue.fact_charge (encounter_bk);
COMMENT ON TABLE edw_g3_revenue.fact_charge IS 'https://ontology.healix.example.org/finance/ChargeItem';
COMMENT ON COLUMN edw_g3_revenue.fact_charge.encounter_bk IS 'none | https://ontology.healix.example.org/finance/encounter';
COMMENT ON COLUMN edw_g3_revenue.fact_charge.extended_amount IS 'none | https://ontology.healix.example.org/finance/chargeAmount';
COMMENT ON COLUMN edw_g3_revenue.fact_charge.cost_center_sk IS 'none | https://ontology.healix.example.org/finance/attributedToCostCenter';
COMMENT ON COLUMN edw_g3_revenue.fact_charge.service_date_sk IS 'phi_quasi | https://ontology.healix.example.org/finance/serviceDate';

-- ---------------------------------------------------------------------------
-- fact_claim_header (accumulating snapshot): one row per claim version
-- ---------------------------------------------------------------------------
CREATE TABLE edw_g3_revenue.fact_claim_header (
    claim_header_sk         BIGINT        NOT NULL,
    claim_bk                VARCHAR(128)  NOT NULL,   -- <source>:<claim number>:<version>
    claim_number            VARCHAR(64)   NOT NULL,   -- degenerate
    claim_version_number    SMALLINT      NOT NULL DEFAULT 1,
    patient_sk              BIGINT        NOT NULL,
    account_bk              VARCHAR(128),
    encounter_bk            VARCHAR(128),
    billing_organization_sk BIGINT        NOT NULL,
    rendering_practitioner_sk BIGINT      NOT NULL,
    attending_practitioner_sk BIGINT      NOT NULL,
    payer_sk                BIGINT        NOT NULL,
    payer_plan_sk           BIGINT        NOT NULL,
    claim_type_sk           BIGINT        NOT NULL,
    claim_status_sk         BIGINT        NOT NULL,
    claim_frequency_code_sk BIGINT        NOT NULL,   -- original / replacement / void (CLM05-3)
    drg_code_sk             BIGINT        NOT NULL,
    bill_type_code_sk       BIGINT        NOT NULL,
    admission_type_code_sk  BIGINT        NOT NULL,
    statement_from_date_sk  INTEGER       NOT NULL,
    statement_to_date_sk    INTEGER       NOT NULL,
    submitted_date_sk       INTEGER       NOT NULL,
    submitted_ts            TIMESTAMP,
    adjudicated_date_sk     INTEGER       NOT NULL,
    adjudicated_ts          TIMESTAMP,
    paid_date_sk            INTEGER       NOT NULL,
    paid_ts                 TIMESTAMP,
    total_charge_amount     DECIMAL(18,4),
    total_allowed_amount    DECIMAL(18,4),
    total_paid_amount       DECIMAL(18,4),
    total_patient_resp_amount DECIMAL(18,4),
    total_denied_amount     DECIMAL(18,4),
    currency_code           CHAR(3)       NOT NULL DEFAULT 'USD',
    line_count              SMALLINT,
    clearinghouse_trace_number VARCHAR(50),
    payer_claim_control_number VARCHAR(50),
    original_claim_bk       VARCHAR(128),             -- for replacement / void claims
    orphan_flag             CHAR(1)       NOT NULL DEFAULT 'N',
    -- bitemporal block
    valid_from              TIMESTAMP     NOT NULL,
    valid_to                TIMESTAMP,
    recorded_at             TIMESTAMP     NOT NULL,
    superseded_at           TIMESTAMP,
    change_reason_code      VARCHAR(30),
    -- technical block
    source_system_sk        INTEGER       NOT NULL,
    load_batch_id           BIGINT        NOT NULL,
    record_hash             CHAR(64)      NOT NULL,
    prov_activity_iri       VARCHAR(512)  NOT NULL,
    CONSTRAINT pk_fact_claim_header PRIMARY KEY (claim_header_sk),
    CONSTRAINT fk_claim_hdr_patient FOREIGN KEY (patient_sk)
        REFERENCES edw_g1_party.dim_patient (patient_sk),
    CONSTRAINT fk_claim_hdr_billing_org FOREIGN KEY (billing_organization_sk)
        REFERENCES edw_g1_party.dim_organization (organization_sk),
    CONSTRAINT fk_claim_hdr_rendering FOREIGN KEY (rendering_practitioner_sk)
        REFERENCES edw_g1_party.dim_practitioner (practitioner_sk),
    CONSTRAINT fk_claim_hdr_attending FOREIGN KEY (attending_practitioner_sk)
        REFERENCES edw_g1_party.dim_practitioner (practitioner_sk),
    CONSTRAINT fk_claim_hdr_payer FOREIGN KEY (payer_sk)
        REFERENCES edw_g1_party.dim_payer (payer_sk),
    CONSTRAINT fk_claim_hdr_payer_plan FOREIGN KEY (payer_plan_sk)
        REFERENCES edw_g3_revenue.dim_payer_plan (payer_plan_sk),
    CONSTRAINT fk_claim_hdr_type FOREIGN KEY (claim_type_sk)
        REFERENCES edw_g3_revenue.dim_claim_type (claim_type_sk),
    CONSTRAINT fk_claim_hdr_status FOREIGN KEY (claim_status_sk)
        REFERENCES edw_g3_revenue.dim_claim_status (claim_status_sk),
    CONSTRAINT fk_claim_hdr_frequency FOREIGN KEY (claim_frequency_code_sk)
        REFERENCES edw_g8_reference.dim_code (code_sk),
    CONSTRAINT fk_claim_hdr_drg FOREIGN KEY (drg_code_sk)
        REFERENCES edw_g8_reference.dim_code (code_sk),
    CONSTRAINT fk_claim_hdr_bill_type FOREIGN KEY (bill_type_code_sk)
        REFERENCES edw_g8_reference.dim_code (code_sk),
    CONSTRAINT fk_claim_hdr_admission_type FOREIGN KEY (admission_type_code_sk)
        REFERENCES edw_g8_reference.dim_code (code_sk),
    CONSTRAINT fk_claim_hdr_stmt_from FOREIGN KEY (statement_from_date_sk)
        REFERENCES edw_g8_reference.dim_date (date_sk),
    CONSTRAINT fk_claim_hdr_stmt_to FOREIGN KEY (statement_to_date_sk)
        REFERENCES edw_g8_reference.dim_date (date_sk),
    CONSTRAINT fk_claim_hdr_submitted FOREIGN KEY (submitted_date_sk)
        REFERENCES edw_g8_reference.dim_date (date_sk),
    CONSTRAINT fk_claim_hdr_adjudicated FOREIGN KEY (adjudicated_date_sk)
        REFERENCES edw_g8_reference.dim_date (date_sk),
    CONSTRAINT fk_claim_hdr_paid FOREIGN KEY (paid_date_sk)
        REFERENCES edw_g8_reference.dim_date (date_sk),
    CONSTRAINT fk_claim_hdr_src FOREIGN KEY (source_system_sk)
        REFERENCES edw_g8_reference.dim_source_system (source_system_sk)
);
-- TD: CREATE MULTISET TABLE edw_g3_revenue.fact_claim_header, FALLBACK, BLOCKCOMPRESSION = AUTOTEMP (...) PRIMARY INDEX (patient_sk)
-- TD: PARTITION BY (RANGE_N(statement_from_date_sk BETWEEN 20150101 AND 20401231 EACH 100, NO RANGE, UNKNOWN),
-- TD:               CASE_N(superseded_at IS NULL, NO CASE))   -- EACH 100 on yyyymmdd ≈ monthly buckets
-- TD: UNIQUE INDEX (claim_bk, valid_from); INDEX (claim_number)
CREATE UNIQUE INDEX ux_fact_claim_header_bk ON edw_g3_revenue.fact_claim_header (claim_bk, valid_from);
CREATE INDEX ix_fact_claim_header_number ON edw_g3_revenue.fact_claim_header (claim_number);
COMMENT ON TABLE edw_g3_revenue.fact_claim_header IS 'https://ontology.healix.example.org/finance/Claim';
COMMENT ON COLUMN edw_g3_revenue.fact_claim_header.claim_number IS 'phi_quasi | https://ontology.healix.example.org/finance/claimNumber';
COMMENT ON COLUMN edw_g3_revenue.fact_claim_header.total_paid_amount IS 'none | https://ontology.healix.example.org/finance/totalPaidAmount';
COMMENT ON COLUMN edw_g3_revenue.fact_claim_header.drg_code_sk IS 'phi_quasi | https://ontology.healix.example.org/clinical/diagnosisRelatedGroup';

-- ---------------------------------------------------------------------------
-- fact_claim_line (transaction) · 03-logical-model.md §9.5 · EDW-G3-001
-- ---------------------------------------------------------------------------
CREATE TABLE edw_g3_revenue.fact_claim_line (
    claim_line_sk           BIGINT        NOT NULL,
    claim_line_bk           VARCHAR(128)  NOT NULL,
    claim_header_sk         BIGINT        NOT NULL,
    claim_number            VARCHAR(64)   NOT NULL,   -- degenerate
    line_number             SMALLINT      NOT NULL,   -- degenerate
    patient_sk              BIGINT        NOT NULL,
    rendering_practitioner_sk BIGINT      NOT NULL,
    place_of_service_location_sk BIGINT   NOT NULL,
    place_of_service_code_sk BIGINT       NOT NULL,   -- CMS POS
    payer_sk                BIGINT        NOT NULL,
    payer_plan_sk           BIGINT        NOT NULL,
    procedure_code_sk       BIGINT        NOT NULL,   -- CPT / HCPCS
    revenue_code_sk         BIGINT        NOT NULL,
    ndc_item_sk             BIGINT        NOT NULL,   -- → edw_g4_supply.dim_item (declared in g4_supply.sql)
    service_from_date_sk    INTEGER       NOT NULL,
    service_to_date_sk      INTEGER       NOT NULL,
    adjudication_date_sk    INTEGER       NOT NULL,
    units_of_service        DECIMAL(12,3),
    billed_amount           DECIMAL(18,4),
    allowed_amount          DECIMAL(18,4),
    paid_amount             DECIMAL(18,4),
    patient_responsibility_amount DECIMAL(18,4),
    deductible_amount       DECIMAL(18,4),
    coinsurance_amount      DECIMAL(18,4),
    copay_amount            DECIMAL(18,4),
    contractual_adjustment_amount DECIMAL(18,4),
    denied_amount           DECIMAL(18,4),
    currency_code           CHAR(3)       NOT NULL DEFAULT 'USD',
    line_status_sk          BIGINT        NOT NULL,
    diagnosis_pointer_1     SMALLINT,
    diagnosis_pointer_2     SMALLINT,
    diagnosis_pointer_3     SMALLINT,
    diagnosis_pointer_4     SMALLINT,
    primary_charge_bk       VARCHAR(128),             -- full M:N in bridge_claim_line_charge
    claim_line_match_status VARCHAR(20)   NOT NULL DEFAULT 'UNMATCHED',   -- MATCHED / UNMATCHED / PARTIAL
    encounter_bk            VARCHAR(128),
    orphan_flag             CHAR(1)       NOT NULL DEFAULT 'N',
    -- bitemporal block
    valid_from              TIMESTAMP     NOT NULL,
    valid_to                TIMESTAMP,
    recorded_at             TIMESTAMP     NOT NULL,
    superseded_at           TIMESTAMP,
    change_reason_code      VARCHAR(30),
    -- technical block
    source_system_sk        INTEGER       NOT NULL,
    load_batch_id           BIGINT        NOT NULL,
    record_hash             CHAR(64)      NOT NULL,
    prov_activity_iri       VARCHAR(512)  NOT NULL,
    CONSTRAINT pk_fact_claim_line PRIMARY KEY (claim_line_sk),
    CONSTRAINT fk_claim_line_header FOREIGN KEY (claim_header_sk)
        REFERENCES edw_g3_revenue.fact_claim_header (claim_header_sk),
    CONSTRAINT fk_claim_line_patient FOREIGN KEY (patient_sk)
        REFERENCES edw_g1_party.dim_patient (patient_sk),
    CONSTRAINT fk_claim_line_rendering FOREIGN KEY (rendering_practitioner_sk)
        REFERENCES edw_g1_party.dim_practitioner (practitioner_sk),
    CONSTRAINT fk_claim_line_pos_location FOREIGN KEY (place_of_service_location_sk)
        REFERENCES edw_g1_party.dim_location (location_sk),
    CONSTRAINT fk_claim_line_pos_code FOREIGN KEY (place_of_service_code_sk)
        REFERENCES edw_g8_reference.dim_code (code_sk),
    CONSTRAINT fk_claim_line_payer FOREIGN KEY (payer_sk)
        REFERENCES edw_g1_party.dim_payer (payer_sk),
    CONSTRAINT fk_claim_line_payer_plan FOREIGN KEY (payer_plan_sk)
        REFERENCES edw_g3_revenue.dim_payer_plan (payer_plan_sk),
    CONSTRAINT fk_claim_line_procedure FOREIGN KEY (procedure_code_sk)
        REFERENCES edw_g8_reference.dim_code (code_sk),
    CONSTRAINT fk_claim_line_revenue_code FOREIGN KEY (revenue_code_sk)
        REFERENCES edw_g8_reference.dim_code (code_sk),
    CONSTRAINT fk_claim_line_svc_from FOREIGN KEY (service_from_date_sk)
        REFERENCES edw_g8_reference.dim_date (date_sk),
    CONSTRAINT fk_claim_line_svc_to FOREIGN KEY (service_to_date_sk)
        REFERENCES edw_g8_reference.dim_date (date_sk),
    CONSTRAINT fk_claim_line_adjudicated FOREIGN KEY (adjudication_date_sk)
        REFERENCES edw_g8_reference.dim_date (date_sk),
    CONSTRAINT fk_claim_line_status FOREIGN KEY (line_status_sk)
        REFERENCES edw_g3_revenue.dim_claim_status (claim_status_sk),
    CONSTRAINT fk_claim_line_src FOREIGN KEY (source_system_sk)
        REFERENCES edw_g8_reference.dim_source_system (source_system_sk)
);
-- TD: CREATE MULTISET TABLE edw_g3_revenue.fact_claim_line, FALLBACK, BLOCKCOMPRESSION = AUTOTEMP (...) PRIMARY INDEX (claim_number)
-- TD: PARTITION BY (RANGE_N(service_from_date_sk BETWEEN 20150101 AND 20401231 EACH 100, NO RANGE, UNKNOWN),
-- TD:               CASE_N(superseded_at IS NULL, NO CASE))
-- TD: COLLECT STATISTICS COLUMN (claim_number), COLUMN (patient_sk), COLUMN (payer_sk), COLUMN (procedure_code_sk), COLUMN (PARTITION) ON edw_g3_revenue.fact_claim_line;
CREATE UNIQUE INDEX ux_fact_claim_line_bk ON edw_g3_revenue.fact_claim_line (claim_line_bk, valid_from);
CREATE INDEX ix_fact_claim_line_number ON edw_g3_revenue.fact_claim_line (claim_number, line_number);
CREATE INDEX ix_fact_claim_line_patient ON edw_g3_revenue.fact_claim_line (patient_sk, service_from_date_sk);
COMMENT ON TABLE edw_g3_revenue.fact_claim_line IS 'https://ontology.healix.example.org/finance/ClaimLine';
COMMENT ON COLUMN edw_g3_revenue.fact_claim_line.claim_number IS 'phi_quasi | https://ontology.healix.example.org/finance/claimNumber';
COMMENT ON COLUMN edw_g3_revenue.fact_claim_line.procedure_code_sk IS 'none | https://ontology.healix.example.org/finance/procedureCode';
COMMENT ON COLUMN edw_g3_revenue.fact_claim_line.billed_amount IS 'none | https://ontology.healix.example.org/finance/billedAmount';
COMMENT ON COLUMN edw_g3_revenue.fact_claim_line.allowed_amount IS 'none | https://ontology.healix.example.org/finance/allowedAmount';
COMMENT ON COLUMN edw_g3_revenue.fact_claim_line.paid_amount IS 'none | https://ontology.healix.example.org/finance/paidAmount';
COMMENT ON COLUMN edw_g3_revenue.fact_claim_line.service_from_date_sk IS 'phi_quasi | https://ontology.healix.example.org/finance/serviceFromDate';
COMMENT ON COLUMN edw_g3_revenue.fact_claim_line.prov_activity_iri IS 'none | http://www.w3.org/ns/prov#wasGeneratedBy';

-- ---------------------------------------------------------------------------
-- bridge_claim_diagnosis: claim × diagnosis code × pointer
-- ---------------------------------------------------------------------------
CREATE TABLE edw_g3_revenue.bridge_claim_diagnosis (
    claim_diagnosis_hk      CHAR(64)      NOT NULL,   -- SHA-256(claim_bk, diagnosis_pointer, valid_from)
    claim_header_sk         BIGINT        NOT NULL,
    claim_bk                VARCHAR(128)  NOT NULL,
    diagnosis_pointer       SMALLINT      NOT NULL,   -- 1..12
    diagnosis_code_sk       BIGINT        NOT NULL,   -- ICD-10-CM
    is_principal_flag       CHAR(1)       NOT NULL DEFAULT 'N',
    poa_code                CHAR(1),                  -- Y / N / U / W / 1
    diagnosis_type_code     VARCHAR(20),              -- principal / admitting / external-cause / patient-reason / other
    -- bitemporal block
    valid_from              TIMESTAMP     NOT NULL,
    valid_to                TIMESTAMP,
    recorded_at             TIMESTAMP     NOT NULL,
    superseded_at           TIMESTAMP,
    change_reason_code      VARCHAR(30),
    -- technical block
    source_system_sk        INTEGER       NOT NULL,
    load_batch_id           BIGINT        NOT NULL,
    record_hash             CHAR(64)      NOT NULL,
    prov_activity_iri       VARCHAR(512)  NOT NULL,
    CONSTRAINT pk_bridge_claim_diagnosis PRIMARY KEY (claim_diagnosis_hk),
    CONSTRAINT fk_bcd_claim FOREIGN KEY (claim_header_sk)
        REFERENCES edw_g3_revenue.fact_claim_header (claim_header_sk),
    CONSTRAINT fk_bcd_diagnosis_code FOREIGN KEY (diagnosis_code_sk)
        REFERENCES edw_g8_reference.dim_code (code_sk),
    CONSTRAINT fk_bcd_src FOREIGN KEY (source_system_sk)
        REFERENCES edw_g8_reference.dim_source_system (source_system_sk)
);
-- TD: PRIMARY INDEX (claim_bk)
CREATE INDEX ix_bridge_claim_diagnosis_claim ON edw_g3_revenue.bridge_claim_diagnosis (claim_header_sk, diagnosis_pointer);
COMMENT ON TABLE edw_g3_revenue.bridge_claim_diagnosis IS 'https://ontology.healix.example.org/finance/ClaimDiagnosis';
COMMENT ON COLUMN edw_g3_revenue.bridge_claim_diagnosis.diagnosis_pointer IS 'none | https://ontology.healix.example.org/finance/diagnosisPointer';
