-- =============================================================================
-- HEALIX EDW · module 05 · g4_supply.sql
-- G4 Supply Chain & Operations · schema edw_g4_supply
-- Tables: dim_supplier, dim_item (conformed), fact_purchase_order_line,
--         fact_inventory_balance_daily
-- Specs:  01-subject-area-groups.md §G4 · 05-conformed-dimensions.md §7
-- Depends on: g8_reference.sql, g1_party.sql, g3_finance.sql (dim_cost_center,
--         dim_gl_account). Closes the G3→G4 cycle at the end of this file by
--         adding the item FKs on fact_charge and fact_claim_line.
-- Dialect: portable ANSI; Teradata hints as "-- TD:" comments.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- dim_supplier (Type 2): supplier site; identity lives in G1 dim_organization
-- ---------------------------------------------------------------------------
CREATE TABLE edw_g4_supply.dim_supplier (
    supplier_sk             BIGINT        NOT NULL,
    supplier_bk             VARCHAR(128)  NOT NULL,   -- OFC:<supplier id>:<site id>
    organization_sk         BIGINT        NOT NULL,   -- legal entity in G1
    supplier_number         VARCHAR(30)   NOT NULL,
    supplier_name           VARCHAR(255)  NOT NULL,
    supplier_site_code      VARCHAR(30),
    duns_number             CHAR(9),
    supplier_type_code_sk   BIGINT        NOT NULL,   -- manufacturer / distributor / service / GPO
    diversity_class_code_sk BIGINT        NOT NULL,   -- minority-owned / women-owned / veteran-owned / none
    payment_terms_code      VARCHAR(30),
    is_gpo_contracted_flag  CHAR(1)       NOT NULL DEFAULT 'N',
    address_line1           VARCHAR(200),
    address_city            VARCHAR(100),
    address_state_code      CHAR(2),
    address_postal_code     VARCHAR(10),
    address_country_code    CHAR(2),
    active_flag             CHAR(1)       NOT NULL DEFAULT 'Y',
    hold_flag               CHAR(1)       NOT NULL DEFAULT 'N',
    is_inferred_flag        CHAR(1)       NOT NULL DEFAULT 'N',
    valid_from              TIMESTAMP     NOT NULL,
    valid_to                TIMESTAMP,
    is_current_flag         CHAR(1)       NOT NULL DEFAULT 'Y',
    source_system_sk        INTEGER       NOT NULL,
    load_batch_id           BIGINT        NOT NULL,
    record_hash             CHAR(64)      NOT NULL,
    recorded_at             TIMESTAMP     NOT NULL,
    prov_activity_iri       VARCHAR(512)  NOT NULL,
    CONSTRAINT pk_dim_supplier PRIMARY KEY (supplier_sk),
    CONSTRAINT fk_dim_supplier_org FOREIGN KEY (organization_sk)
        REFERENCES edw_g1_party.dim_organization (organization_sk),
    CONSTRAINT fk_dim_supplier_type FOREIGN KEY (supplier_type_code_sk)
        REFERENCES edw_g8_reference.dim_code (code_sk),
    CONSTRAINT fk_dim_supplier_diversity FOREIGN KEY (diversity_class_code_sk)
        REFERENCES edw_g8_reference.dim_code (code_sk),
    CONSTRAINT fk_dim_supplier_src FOREIGN KEY (source_system_sk)
        REFERENCES edw_g8_reference.dim_source_system (source_system_sk)
);
-- TD: UNIQUE PRIMARY INDEX (supplier_sk)
CREATE UNIQUE INDEX ux_dim_supplier_bk ON edw_g4_supply.dim_supplier (supplier_bk, valid_from);
COMMENT ON TABLE edw_g4_supply.dim_supplier IS 'https://ontology.healix.example.org/supply/Supplier';
COMMENT ON COLUMN edw_g4_supply.dim_supplier.supplier_number IS 'internal | https://ontology.healix.example.org/supply/supplierNumber';
COMMENT ON COLUMN edw_g4_supply.dim_supplier.duns_number IS 'internal | https://ontology.healix.example.org/supply/dunsNumber';

-- ---------------------------------------------------------------------------
-- dim_item (conformed, Type 2) · 05-conformed-dimensions.md §7 · EDW-CONF-011
-- ---------------------------------------------------------------------------
CREATE TABLE edw_g4_supply.dim_item (
    item_sk                 BIGINT        NOT NULL,
    item_bk                 VARCHAR(128)  NOT NULL,   -- enterprise item id
    item_description        VARCHAR(500)  NOT NULL,
    item_short_name         VARCHAR(100),
    item_type_code          VARCHAR(12)   NOT NULL,   -- supply / implant / drug / vaccine / device / equipment / service
    fusion_item_id          VARCHAR(30),
    fusion_item_number      VARCHAR(50),
    cerner_charge_item_id   VARCHAR(30),
    cerner_medication_id    VARCHAR(30),
    gtin                    VARCHAR(14),
    udi_device_identifier   VARCHAR(100),
    ndc_code                VARCHAR(11),              -- 11-digit normalised NDC
    rxnorm_code_sk          BIGINT        NOT NULL,
    cvx_code_sk             BIGINT        NOT NULL,
    hcpcs_code_sk           BIGINT        NOT NULL,
    unspsc_code_sk          BIGINT        NOT NULL,
    unspsc_segment_name     VARCHAR(100),
    unspsc_family_name      VARCHAR(100),
    unspsc_class_name       VARCHAR(100),
    unspsc_commodity_name   VARCHAR(100),
    manufacturer_organization_sk BIGINT   NOT NULL,
    manufacturer_part_number VARCHAR(50),
    primary_supplier_sk     BIGINT        NOT NULL,
    primary_supplier_part_number VARCHAR(50),
    unit_of_issue_ucum_sk   BIGINT        NOT NULL,
    unit_of_purchase_ucum_sk BIGINT       NOT NULL,
    purchase_to_issue_factor DECIMAL(12,4),
    strength_value          DECIMAL(18,6),
    strength_ucum_sk        BIGINT        NOT NULL,
    dose_form_code_sk       BIGINT        NOT NULL,
    route_code_sk           BIGINT        NOT NULL,
    latex_flag              CHAR(1)       NOT NULL DEFAULT 'U',
    hazardous_flag          CHAR(1)       NOT NULL DEFAULT 'N',
    implantable_flag        CHAR(1)       NOT NULL DEFAULT 'N',
    lot_tracked_flag        CHAR(1)       NOT NULL DEFAULT 'N',
    serial_tracked_flag     CHAR(1)       NOT NULL DEFAULT 'N',
    chargeable_flag         CHAR(1)       NOT NULL DEFAULT 'N',
    controlled_substance_flag CHAR(1)     NOT NULL DEFAULT 'N',
    dea_schedule_code       VARCHAR(5),
    standard_cost_amount    DECIMAL(18,4),
    list_price_amount       DECIMAL(18,4),
    currency_code           CHAR(3)       NOT NULL DEFAULT 'USD',
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
    CONSTRAINT pk_dim_item PRIMARY KEY (item_sk),
    CONSTRAINT fk_dim_item_rxnorm FOREIGN KEY (rxnorm_code_sk)
        REFERENCES edw_g8_reference.dim_code (code_sk),
    CONSTRAINT fk_dim_item_cvx FOREIGN KEY (cvx_code_sk)
        REFERENCES edw_g8_reference.dim_code (code_sk),
    CONSTRAINT fk_dim_item_hcpcs FOREIGN KEY (hcpcs_code_sk)
        REFERENCES edw_g8_reference.dim_code (code_sk),
    CONSTRAINT fk_dim_item_unspsc FOREIGN KEY (unspsc_code_sk)
        REFERENCES edw_g8_reference.dim_code (code_sk),
    CONSTRAINT fk_dim_item_manufacturer FOREIGN KEY (manufacturer_organization_sk)
        REFERENCES edw_g1_party.dim_organization (organization_sk),
    CONSTRAINT fk_dim_item_supplier FOREIGN KEY (primary_supplier_sk)
        REFERENCES edw_g4_supply.dim_supplier (supplier_sk),
    CONSTRAINT fk_dim_item_issue_ucum FOREIGN KEY (unit_of_issue_ucum_sk)
        REFERENCES edw_g8_reference.dim_ucum_unit (ucum_unit_sk),
    CONSTRAINT fk_dim_item_purchase_ucum FOREIGN KEY (unit_of_purchase_ucum_sk)
        REFERENCES edw_g8_reference.dim_ucum_unit (ucum_unit_sk),
    CONSTRAINT fk_dim_item_strength_ucum FOREIGN KEY (strength_ucum_sk)
        REFERENCES edw_g8_reference.dim_ucum_unit (ucum_unit_sk),
    CONSTRAINT fk_dim_item_dose_form FOREIGN KEY (dose_form_code_sk)
        REFERENCES edw_g8_reference.dim_code (code_sk),
    CONSTRAINT fk_dim_item_route FOREIGN KEY (route_code_sk)
        REFERENCES edw_g8_reference.dim_code (code_sk),
    CONSTRAINT fk_dim_item_src FOREIGN KEY (source_system_sk)
        REFERENCES edw_g8_reference.dim_source_system (source_system_sk)
);
-- TD: CREATE SET TABLE edw_g4_supply.dim_item, FALLBACK, BLOCKCOMPRESSION = AUTOTEMP (...) UNIQUE PRIMARY INDEX (item_sk)
-- TD: UNIQUE INDEX (item_bk, valid_from); INDEX (ndc_code); INDEX (gtin)
-- TD: COLLECT STATISTICS COLUMN (item_sk), COLUMN (item_bk), COLUMN (ndc_code), COLUMN (gtin), COLUMN (is_current_flag) ON edw_g4_supply.dim_item;
CREATE UNIQUE INDEX ux_dim_item_bk ON edw_g4_supply.dim_item (item_bk, valid_from);
CREATE INDEX ix_dim_item_ndc ON edw_g4_supply.dim_item (ndc_code);
CREATE INDEX ix_dim_item_gtin ON edw_g4_supply.dim_item (gtin);
COMMENT ON TABLE edw_g4_supply.dim_item IS 'https://ontology.healix.example.org/supply/Item';
COMMENT ON COLUMN edw_g4_supply.dim_item.item_bk IS 'internal | https://ontology.healix.example.org/supply/itemIdentifier';
COMMENT ON COLUMN edw_g4_supply.dim_item.gtin IS 'internal | https://ontology.healix.example.org/supply/gtin';
COMMENT ON COLUMN edw_g4_supply.dim_item.udi_device_identifier IS 'internal | https://ontology.healix.example.org/supply/udiDeviceIdentifier';
COMMENT ON COLUMN edw_g4_supply.dim_item.ndc_code IS 'internal | https://ontology.healix.example.org/supply/ndcCode';
COMMENT ON COLUMN edw_g4_supply.dim_item.rxnorm_code_sk IS 'internal | https://ontology.healix.example.org/supply/rxnormConcept';
COMMENT ON COLUMN edw_g4_supply.dim_item.unspsc_code_sk IS 'internal | https://ontology.healix.example.org/supply/unspscCategory';

INSERT INTO edw_g4_supply.dim_item
    (item_sk, item_bk, item_description, item_type_code, rxnorm_code_sk, cvx_code_sk, hcpcs_code_sk, unspsc_code_sk,
     manufacturer_organization_sk, primary_supplier_sk, unit_of_issue_ucum_sk, unit_of_purchase_ucum_sk, strength_ucum_sk,
     dose_form_code_sk, route_code_sk, valid_from, is_current_flag, source_system_sk, load_batch_id, record_hash,
     recorded_at, prov_activity_iri)
VALUES
    (-1, 'UNKNOWN', 'Unknown item', 'supply', -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
     TIMESTAMP '1900-01-01 00:00:00', 'Y', 1, 0, 'SEED', TIMESTAMP '1900-01-01 00:00:00',
     'https://ontology.healix.example.org/prov/activity/seed/dim_item'),
    (-2, 'NOT_APPLICABLE', 'Not applicable (service-only)', 'service', -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2,
     TIMESTAMP '1900-01-01 00:00:00', 'Y', 1, 0, 'SEED', TIMESTAMP '1900-01-01 00:00:00',
     'https://ontology.healix.example.org/prov/activity/seed/dim_item');
-- NOTE: the -1/-2 members of dim_organization and dim_supplier referenced above are
-- seeded by ddl/seed/ before this file runs (reserved members, 03-logical-model.md §2).

-- ---------------------------------------------------------------------------
-- fact_purchase_order_line (accumulating snapshot) · EDW-G4-002
-- ---------------------------------------------------------------------------
CREATE TABLE edw_g4_supply.fact_purchase_order_line (
    po_line_sk              BIGINT        NOT NULL,
    po_line_bk              VARCHAR(128)  NOT NULL,   -- OFC:<po_header_id>:<line>:<schedule>
    po_number               VARCHAR(30)   NOT NULL,   -- degenerate
    po_line_number          SMALLINT      NOT NULL,
    po_schedule_number      SMALLINT      NOT NULL DEFAULT 1,
    requisition_line_bk     VARCHAR(128),
    item_sk                 BIGINT        NOT NULL,
    supplier_sk             BIGINT        NOT NULL,
    supplier_contract_bk    VARCHAR(128),
    procurement_organization_sk BIGINT    NOT NULL,   -- procurement BU
    deliver_to_location_sk  BIGINT        NOT NULL,
    cost_center_sk          BIGINT        NOT NULL,
    gl_account_sk           BIGINT        NOT NULL,
    buyer_worker_bk         VARCHAR(128),
    po_status_code_sk       BIGINT        NOT NULL,   -- incomplete / approved / open / closed-for-receiving / closed / cancelled
    ordered_date_sk         INTEGER       NOT NULL,
    ordered_ts              TIMESTAMP,
    promised_date_sk        INTEGER       NOT NULL,
    need_by_date_sk         INTEGER       NOT NULL,
    approved_ts             TIMESTAMP,
    first_receipt_ts        TIMESTAMP,
    last_receipt_ts         TIMESTAMP,
    closed_ts               TIMESTAMP,
    quantity_ordered        DECIMAL(18,6) NOT NULL,
    quantity_received       DECIMAL(18,6),
    quantity_billed         DECIMAL(18,6),
    quantity_cancelled      DECIMAL(18,6),
    purchase_ucum_unit_sk   BIGINT        NOT NULL,
    unit_price_amount       DECIMAL(18,4),
    extended_amount         DECIMAL(18,4),
    currency_code           CHAR(3)       NOT NULL DEFAULT 'USD',
    is_contract_price_flag  CHAR(1)       NOT NULL DEFAULT 'U',
    days_late               SMALLINT,                 -- derived: first_receipt − promised
    source_po_line_id       VARCHAR(64),
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
    CONSTRAINT pk_fact_po_line PRIMARY KEY (po_line_sk),
    CONSTRAINT fk_po_line_item FOREIGN KEY (item_sk)
        REFERENCES edw_g4_supply.dim_item (item_sk),
    CONSTRAINT fk_po_line_supplier FOREIGN KEY (supplier_sk)
        REFERENCES edw_g4_supply.dim_supplier (supplier_sk),
    CONSTRAINT fk_po_line_proc_org FOREIGN KEY (procurement_organization_sk)
        REFERENCES edw_g1_party.dim_organization (organization_sk),
    CONSTRAINT fk_po_line_location FOREIGN KEY (deliver_to_location_sk)
        REFERENCES edw_g1_party.dim_location (location_sk),
    CONSTRAINT fk_po_line_cost_center FOREIGN KEY (cost_center_sk)
        REFERENCES edw_g3_finance.dim_cost_center (cost_center_sk),
    CONSTRAINT fk_po_line_gl_account FOREIGN KEY (gl_account_sk)
        REFERENCES edw_g3_finance.dim_gl_account (gl_account_sk),
    CONSTRAINT fk_po_line_status FOREIGN KEY (po_status_code_sk)
        REFERENCES edw_g8_reference.dim_code (code_sk),
    CONSTRAINT fk_po_line_ordered_date FOREIGN KEY (ordered_date_sk)
        REFERENCES edw_g8_reference.dim_date (date_sk),
    CONSTRAINT fk_po_line_promised_date FOREIGN KEY (promised_date_sk)
        REFERENCES edw_g8_reference.dim_date (date_sk),
    CONSTRAINT fk_po_line_need_by_date FOREIGN KEY (need_by_date_sk)
        REFERENCES edw_g8_reference.dim_date (date_sk),
    CONSTRAINT fk_po_line_ucum FOREIGN KEY (purchase_ucum_unit_sk)
        REFERENCES edw_g8_reference.dim_ucum_unit (ucum_unit_sk),
    CONSTRAINT fk_po_line_src FOREIGN KEY (source_system_sk)
        REFERENCES edw_g8_reference.dim_source_system (source_system_sk)
);
-- TD: CREATE MULTISET TABLE edw_g4_supply.fact_purchase_order_line, FALLBACK, BLOCKCOMPRESSION = AUTOTEMP (...) PRIMARY INDEX (po_number)
-- TD: PARTITION BY (RANGE_N(ordered_date_sk BETWEEN 20150101 AND 20401231 EACH 100, NO RANGE, UNKNOWN),
-- TD:               CASE_N(superseded_at IS NULL, NO CASE))
-- TD: COLLECT STATISTICS COLUMN (po_number), COLUMN (item_sk), COLUMN (supplier_sk), COLUMN (cost_center_sk), COLUMN (PARTITION) ON edw_g4_supply.fact_purchase_order_line;
CREATE UNIQUE INDEX ux_fact_po_line_bk ON edw_g4_supply.fact_purchase_order_line (po_line_bk, valid_from);
CREATE INDEX ix_fact_po_line_item ON edw_g4_supply.fact_purchase_order_line (item_sk, ordered_date_sk);
COMMENT ON TABLE edw_g4_supply.fact_purchase_order_line IS 'https://ontology.healix.example.org/supply/PurchaseOrderLine';
COMMENT ON COLUMN edw_g4_supply.fact_purchase_order_line.quantity_ordered IS 'confidential:financial | https://ontology.healix.example.org/supply/quantityOrdered';
COMMENT ON COLUMN edw_g4_supply.fact_purchase_order_line.quantity_received IS 'confidential:financial | https://ontology.healix.example.org/supply/quantityReceived';
COMMENT ON COLUMN edw_g4_supply.fact_purchase_order_line.extended_amount IS 'confidential:financial | https://ontology.healix.example.org/supply/purchaseAmount';
COMMENT ON COLUMN edw_g4_supply.fact_purchase_order_line.prov_activity_iri IS 'internal | http://www.w3.org/ns/prov#wasGeneratedBy';

-- ---------------------------------------------------------------------------
-- fact_inventory_balance_daily (periodic snapshot) · EDW-G4-001
-- Grain: item × stocking location (sub-inventory / locator) × day
-- ---------------------------------------------------------------------------
CREATE TABLE edw_g4_supply.fact_inventory_balance_daily (
    inventory_balance_hk    CHAR(64)      NOT NULL,   -- SHA-256(snapshot_date_sk, item_sk, location_sk)
    snapshot_date_sk        INTEGER       NOT NULL,
    snapshot_date           DATE          NOT NULL,
    item_sk                 BIGINT        NOT NULL,
    location_sk             BIGINT        NOT NULL,   -- dim_location of type warehouse / locator
    inventory_organization_sk BIGINT      NOT NULL,   -- Fusion inventory organisation
    cost_center_sk          BIGINT        NOT NULL,
    on_hand_qty             DECIMAL(18,6) NOT NULL,
    reserved_qty            DECIMAL(18,6),
    available_qty           DECIMAL(18,6),
    in_transit_qty          DECIMAL(18,6),
    issue_ucum_unit_sk      BIGINT        NOT NULL,
    unit_cost_amount        DECIMAL(18,4),
    on_hand_value_amount    DECIMAL(18,4),
    currency_code           CHAR(3)       NOT NULL DEFAULT 'USD',
    min_qty                 DECIMAL(18,6),
    max_qty                 DECIMAL(18,6),
    reorder_point_qty       DECIMAL(18,6),
    below_min_flag          CHAR(1)       NOT NULL DEFAULT 'N',
    days_on_hand            DECIMAL(9,2),             -- derived from trailing 90-day usage
    expiring_30d_qty        DECIMAL(18,6),
    lot_count               SMALLINT,
    net_transaction_qty     DECIMAL(18,6),            -- signed sum of fact_inventory_transaction for the day
    balance_reconciled_flag CHAR(1)       NOT NULL DEFAULT 'U',   -- EDW-G4-001 check result
    -- bitemporal block (valid_from = snapshot instant)
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
    CONSTRAINT pk_fact_inventory_balance PRIMARY KEY (inventory_balance_hk),
    CONSTRAINT fk_inv_bal_date FOREIGN KEY (snapshot_date_sk)
        REFERENCES edw_g8_reference.dim_date (date_sk),
    CONSTRAINT fk_inv_bal_item FOREIGN KEY (item_sk)
        REFERENCES edw_g4_supply.dim_item (item_sk),
    CONSTRAINT fk_inv_bal_location FOREIGN KEY (location_sk)
        REFERENCES edw_g1_party.dim_location (location_sk),
    CONSTRAINT fk_inv_bal_inv_org FOREIGN KEY (inventory_organization_sk)
        REFERENCES edw_g1_party.dim_organization (organization_sk),
    CONSTRAINT fk_inv_bal_cost_center FOREIGN KEY (cost_center_sk)
        REFERENCES edw_g3_finance.dim_cost_center (cost_center_sk),
    CONSTRAINT fk_inv_bal_ucum FOREIGN KEY (issue_ucum_unit_sk)
        REFERENCES edw_g8_reference.dim_ucum_unit (ucum_unit_sk),
    CONSTRAINT fk_inv_bal_src FOREIGN KEY (source_system_sk)
        REFERENCES edw_g8_reference.dim_source_system (source_system_sk)
);
-- TD: CREATE MULTISET TABLE edw_g4_supply.fact_inventory_balance_daily, FALLBACK, BLOCKCOMPRESSION = AUTOTEMP (...) PRIMARY INDEX (item_sk, location_sk)
-- TD: PARTITION BY (RANGE_N(snapshot_date BETWEEN DATE '2024-01-01' AND DATE '2026-12-31' EACH INTERVAL '1' DAY,
-- TD:                                     DATE '2027-01-01' AND DATE '2040-12-31' EACH INTERVAL '1' MONTH, NO RANGE),
-- TD:               CASE_N(superseded_at IS NULL, NO CASE))
-- TD: COLLECT STATISTICS COLUMN (item_sk), COLUMN (location_sk), COLUMN (snapshot_date_sk), COLUMN (PARTITION) ON edw_g4_supply.fact_inventory_balance_daily;
CREATE UNIQUE INDEX ux_fact_inv_bal_grain ON edw_g4_supply.fact_inventory_balance_daily (snapshot_date_sk, item_sk, location_sk, valid_from);
COMMENT ON TABLE edw_g4_supply.fact_inventory_balance_daily IS 'https://ontology.healix.example.org/supply/InventoryBalance';
COMMENT ON COLUMN edw_g4_supply.fact_inventory_balance_daily.on_hand_qty IS 'confidential:operational | https://ontology.healix.example.org/supply/onHandQuantity';
COMMENT ON COLUMN edw_g4_supply.fact_inventory_balance_daily.on_hand_value_amount IS 'confidential:operational | https://ontology.healix.example.org/supply/onHandValue';
COMMENT ON COLUMN edw_g4_supply.fact_inventory_balance_daily.snapshot_date IS 'internal | https://ontology.healix.example.org/supply/snapshotDate';

-- ---------------------------------------------------------------------------
-- Close the G3 → G4 cycle: item references from revenue facts
-- TD: ALTER TABLE ... ADD CONSTRAINT ... FOREIGN KEY ... REFERENCES WITH NO CHECK OPTION (soft RI)
-- NOTE: valid ANSI and Teradata syntax; parsed by sqlglot (generic + teradata) and
-- sqlfluff (postgres). sqlfluff 4.x's teradata grammar does not yet cover
-- ALTER TABLE ADD CONSTRAINT (EDW-PHY-001 note).
-- ---------------------------------------------------------------------------
ALTER TABLE edw_g3_revenue.fact_charge
    ADD CONSTRAINT fk_charge_item FOREIGN KEY (item_sk)
        REFERENCES edw_g4_supply.dim_item (item_sk);

ALTER TABLE edw_g3_revenue.fact_claim_line
    ADD CONSTRAINT fk_claim_line_ndc_item FOREIGN KEY (ndc_item_sk)
        REFERENCES edw_g4_supply.dim_item (item_sk);
